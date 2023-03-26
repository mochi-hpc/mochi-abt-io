
/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "abt-io-config.h"

#define _GNU_SOURCE

#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <fcntl.h>
#include <json-c/json.h>
#ifdef USE_LIBURING
    #include <liburing.h>
#endif

#include <abt.h>
#include "abt-io.h"
#include "abt-io-macros.h"

#define DEFAULT_BACKING_THREAD_COUNT 16
#define DEFAULT_URING_ENTRIES        64

enum abt_io_engine_type {
    ABT_IO_ENGINE_POSIX = 1,
    ABT_IO_ENGINE_LIBURING
};

struct abt_io_instance {
    ABT_pool            progress_pool;
    ABT_xstream*        progress_xstreams;
    int                 num_xstreams;
    struct json_object* json_cfg;
    int                 do_null_io_read;
    int                 do_null_io_write;
    int                 logging_enabled; /* debugging: log every i/o call? */
    double epoch_start; /* i/o logs will report time relative to this */
    enum abt_io_engine_type engine_type;
#ifdef USE_LIBURING
    ABT_task        uring_completion_task;
    struct io_uring ring;
    int             uring_shutdown_flag;
#endif
};

struct abt_io_op {
    ABT_eventual e;
    void*        state;
    void (*free_fn)(void*);
    struct iovec vec;
};

#ifdef USE_LIBURING
static void uring_completion_task_fn(void* foo);
static int  issue_pwrite_liburing(abt_io_instance_id aid,
                                  abt_io_op_t*       op,
                                  int                fd,
                                  const void*        buf,
                                  size_t             count,
                                  off_t              offset,
                                  ssize_t*           ret);
static int  issue_pread_liburing(abt_io_instance_id aid,
                                 abt_io_op_t*       op,
                                 int                fd,
                                 void*              buf,
                                 size_t             count,
                                 off_t              offset,
                                 ssize_t*           ret);
#endif /* USE_LIBURING */

/**
 * Validates the format of the configuration and fills default values
 * if they are not provided
 */
static int validate_and_complete_config(struct json_object* _config,
                                        ABT_pool            _progress_pool);

static int issue_pwrite_posix(abt_io_instance_id aid,
                              abt_io_op_t*       op,
                              int                fd,
                              const void*        buf,
                              size_t             count,
                              off_t              offset,
                              ssize_t*           ret);

static int (*issue_pwrite)(
    abt_io_instance_id, abt_io_op_t*, int, const void*, size_t, off_t, ssize_t*)
    = issue_pwrite_posix;

static int issue_pread_posix(abt_io_instance_id aid,
                             abt_io_op_t*       op,
                             int                fd,
                             void*              buf,
                             size_t             count,
                             off_t              offset,
                             ssize_t*           ret);
static int (*issue_pread)(
    abt_io_instance_id, abt_io_op_t*, int, void*, size_t, off_t, ssize_t*)
    = issue_pread_posix;

/**
 * Set up pool (creating if needed) for abt-io instance to use
 */
static int setup_pool(abt_io_instance_id  aid,
                      struct json_object* _config,
                      ABT_pool            _progress_pool);

/**
 * Tear down pool (if internally created)
 */
static void teardown_pool(abt_io_instance_id aid);

abt_io_instance_id abt_io_init(int backing_thread_count)
{
    char                    config[256];
    struct abt_io_init_info args = {0};

    if (backing_thread_count < 1) return ABT_IO_INSTANCE_NULL;

    snprintf(config, 256, "{ \"backing_thread_count\" : %d }",
             backing_thread_count);

    args.json_config = config;

    return abt_io_init_ext(&args);
}

abt_io_instance_id abt_io_init_ext(const struct abt_io_init_info* uargs)
{
    struct abt_io_init_info args   = {0};
    struct json_object*     config = NULL;
    struct abt_io_instance* aid    = NULL;
    int                     ret;
    struct json_object*     jengine = NULL;

    if (uargs) args = *uargs;

    if (args.json_config && strlen(args.json_config) > 0) {
        /* read JSON config from provided string argument */
        struct json_tokener*    tokener = json_tokener_new();
        enum json_tokener_error jerr;

        config = json_tokener_parse_ex(tokener, args.json_config,
                                       strlen(args.json_config));
        if (!config) {
            jerr = json_tokener_get_error(tokener);
            fprintf(stderr, "JSON parse error: %s",
                    json_tokener_error_desc(jerr));
            json_tokener_free(tokener);
            return ABT_IO_INSTANCE_NULL;
        }
        json_tokener_free(tokener);
    } else {
        /* create default JSON config */
        config = json_object_new_object();
    }

    /* validate and complete configuration */
    /* TODO: if liburing is enabled; validate that pool only has one ES (if
     * possible); it won't be helpful to have more than one dedicated to
     * abt-io in that case.
     * NOTE: maybe 2 ESs? We may want one to run posix operations that
     * aren't directly supported by liburing.
     */
    ret = validate_and_complete_config(config, args.progress_pool);
    if (ret != 0) {
        fprintf(stderr, "Could not validate and complete configuration");
        goto error;
    }

    aid = calloc(1, sizeof(*aid));
    if (aid == NULL) goto error;

    aid->do_null_io_write
        = json_object_get_int(json_object_object_get(config, "null_io_write"));
    aid->do_null_io_read
        = json_object_get_int(json_object_object_get(config, "null_io_read"));

    aid->epoch_start = ABT_get_wtime();
    aid->logging_enabled
        = json_object_get_int(json_object_object_get(config, "trace_io"));

    /* TODO: implement a second key "trace_file" that takes a format specifier
     * similar to valgrind's %p */
    if (aid->logging_enabled) {
        fprintf(
            stderr,
            "#Module\tRank\tOp\tSegment\tOffset\tLength\tStart(s)\tEnd(s)\n");
    }

    ret = setup_pool(aid, config, args.progress_pool);
    if (ret != 0) goto error;

    jengine = json_object_object_get(config, "engine");
    if (strcmp(json_object_get_string(jengine), "posix") == 0)
        aid->engine_type = ABT_IO_ENGINE_POSIX;
    else if (strcmp(json_object_get_string(jengine), "liburing") == 0) {
#ifndef USE_LIBURING
        fprintf(stderr,
                "Error: requested \"engine\"=\"liburing\" but abt-io was not "
                "compiled with liburing support.\n");
        goto error;
#else
        aid->engine_type = ABT_IO_ENGINE_LIBURING;
        /* initialize uring queue */
        ret = io_uring_queue_init(DEFAULT_URING_ENTRIES, &aid->ring, 0);
        if (ret != 0) {
            fprintf(stderr, "Error: io_uring_queue_init() failure.\n");
            goto error;
        }
        /* create a task that will wait for completion events */
        ret = ABT_task_create(aid->progress_pool, uring_completion_task_fn, aid,
                              &aid->uring_completion_task);
        if (ret != ABT_SUCCESS) {
            fprintf(
                stderr,
                "Error: unable to create uring_completion_task_fn() task.\n");
            goto error;
        }

        /* set function pointers for operations supported by liburing */
        issue_pwrite = issue_pwrite_liburing;
        issue_pread  = issue_pread_liburing;
#endif
    }

    aid->json_cfg = config;
    return aid;

error:
    if (config) json_object_put(config);
    if (aid) free(aid);

    return ABT_IO_INSTANCE_NULL;
}

abt_io_instance_id abt_io_init_pool(ABT_pool progress_pool)
{
    struct abt_io_init_info args = {0};

    args.progress_pool = progress_pool;

    return (abt_io_init_ext(&args));
}

void abt_io_finalize(abt_io_instance_id aid)
{
#ifdef USE_LIBURING
    if (aid->engine_type == ABT_IO_ENGINE_LIBURING) {
        /* stop the persistent completion queue fn */
        /* We set a shutdown flag so that the engine loop will see that we
         * are in shutdown mode.  Then submit a timeout operation to fire
         * immediately to break out of the cqe wait function.
         */
        struct __kernel_timespec ts  = {0};
        struct io_uring_sqe*     sqe = io_uring_get_sqe(&aid->ring);
        aid->uring_shutdown_flag     = 1;
        io_uring_prep_timeout(sqe, &ts, 0, 0);
        /* NULL user data to indicate progress loop should ignore content */
        io_uring_sqe_set_data(sqe, NULL);
        io_uring_submit(&aid->ring);
        ABT_task_join(aid->uring_completion_task);

        /* tear down uring queues */
        io_uring_queue_exit(&aid->ring);
    }
#endif
    teardown_pool(aid);
    json_object_put(aid->json_cfg);
    free(aid);
}

static void abt_io_log(abt_io_instance_id aid,
                       char*              op,
                       int64_t            offset,
                       int64_t            length,
                       double             start,
                       double             end)
{
    if (!aid->logging_enabled) return;
    fprintf(stderr, "X_ABTIO\t-1\t%s\t-1\t%ld\t%ld\t%f\t%f\n", op, offset,
            length, start - aid->epoch_start, end - aid->epoch_start);
}
struct abt_io_open_state {
    int*               ret;
    const char*        pathname;
    int                flags;
    mode_t             mode;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

#ifdef USE_LIBURING
static void uring_completion_task_fn(void* foo)
{
    struct abt_io_instance* aid = foo;
    int                     ret;
    struct io_uring_cqe*    cqe;
    struct abt_io_op*       op;

    while (!aid->uring_shutdown_flag) {
        /* NOTE: we intentionally do not user a timeout here.  If we need
         * to break out of this call then we will issue a timeout op from
         * another caller.
         */
        ret = io_uring_wait_cqe(&aid->ring, &cqe);
        if (ret == 0) {
            /* NOTE: if user_data isn't set, then we assume that it was just
             * an event (e.g., a timeout) injected to break out of the wait
             * call.
             */
            if (cqe->user_data) {
                op = (struct abt_io_op*)cqe->user_data;
                /* TODO: will this casting work for the return type on all
                 * supported operations? */
                int64_t* result = op->state;
                *result         = cqe->res;
                ABT_eventual_set(op->e, NULL, 0);
            }
            io_uring_cqe_seen(&aid->ring, cqe);
        }
    }

    return;
}
#endif

static void abt_io_open_fn(void* foo)
{
    struct abt_io_open_state* state = foo;
    double                    start = ABT_get_wtime();

    *state->ret = open(state->pathname, state->flags, state->mode);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "open", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_open(abt_io_instance_id aid,
                      abt_io_op_t*       op,
                      const char*        pathname,
                      int                flags,
                      mode_t             mode,
                      int*               ret)
{
    struct abt_io_open_state  state;
    struct abt_io_open_state* pstate = NULL;
    int                       rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->pathname = pathname;
    pstate->flags    = flags;
    pstate->mode     = mode;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_open_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_open(abt_io_instance_id aid,
                const char*        pathname,
                int                flags,
                mode_t             mode)
{
    int ret;
    issue_open(aid, NULL, pathname, flags, mode, &ret);
    return ret;
}

abt_io_op_t* abt_io_open_nb(abt_io_instance_id aid,
                            const char*        pathname,
                            int                flags,
                            mode_t             mode,
                            int*               ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_open(aid, op, pathname, flags, mode, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_pread_state {
    ssize_t*           ret;
    int                fd;
    void*              buf;
    size_t             count;
    off_t              offset;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_pread_fn(void* foo)
{
    struct abt_io_pread_state* state = foo;
    double                     start = ABT_get_wtime();

    if (state->aid->do_null_io_read)
        *state->ret = state->count; // uh-oh won't be able to detect end of file
    else {
        *state->ret = pread(state->fd, state->buf, state->count, state->offset);
        if (*state->ret < 0) *state->ret = -errno;
    }

    abt_io_log(state->aid, "pread", state->offset, state->count, start,
               ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_pread_posix(abt_io_instance_id aid,
                             abt_io_op_t*       op,
                             int                fd,
                             void*              buf,
                             size_t             count,
                             off_t              offset,
                             ssize_t*           ret)
{
    struct abt_io_pread_state  state;
    struct abt_io_pread_state* pstate = NULL;
    int                        rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->buf      = buf;
    pstate->count    = count;
    pstate->offset   = offset;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_pread_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_pread(
    abt_io_instance_id aid, int fd, void* buf, size_t count, off_t offset)
{
    ssize_t ret = -1;
    issue_pread(aid, NULL, fd, buf, count, offset, &ret);
    return ret;
}

abt_io_op_t* abt_io_pread_nb(abt_io_instance_id aid,
                             int                fd,
                             void*              buf,
                             size_t             count,
                             off_t              offset,
                             ssize_t*           ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_pread(aid, op, fd, buf, count, offset, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_read_state {
    ssize_t*           ret;
    int                fd;
    void*              buf;
    size_t             count;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_read_fn(void* foo)
{
    double                    start, end;
    struct abt_io_read_state* state = foo;
    start                           = ABT_get_wtime();

    if (state->aid->do_null_io_read)
        *state->ret = state->count;
    else {
        *state->ret = read(state->fd, state->buf, state->count);
        if (*state->ret < 0) *state->ret = -errno;
    }

    if (*state->ret < 0) *state->ret = -errno;

    end = ABT_get_wtime();
    abt_io_log(state->aid, "read", -1, state->count, start, end);
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_read(abt_io_instance_id aid,
                      abt_io_op_t*       op,
                      int                fd,
                      void*              buf,
                      size_t             count,
                      ssize_t*           ret)
{
    struct abt_io_read_state  state;
    struct abt_io_read_state* pstate = NULL;
    int                       rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->buf      = buf;
    pstate->count    = count;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_read_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_read(abt_io_instance_id aid, int fd, void* buf, size_t count)
{
    ssize_t ret = -1;
    issue_read(aid, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_read_nb(
    abt_io_instance_id aid, int fd, void* buf, size_t count, ssize_t* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_read(aid, op, fd, buf, count, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_pwrite_state {
    ssize_t*           ret;
    int                fd;
    const void*        buf;
    size_t             count;
    off_t              offset;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_pwrite_fn(void* foo)
{
    struct abt_io_pwrite_state* state = foo;
    double                      start = ABT_get_wtime();

    if (state->aid->do_null_io_write)
        *state->ret = state->count;
    else {
        *state->ret
            = pwrite(state->fd, state->buf, state->count, state->offset);
        if (*state->ret < 0) *state->ret = -errno;
    }

    abt_io_log(state->aid, "pwrite", state->offset, state->count, start,
               ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

#ifdef USE_LIBURING
static int issue_pwrite_liburing(abt_io_instance_id aid,
                                 abt_io_op_t*       arg_op,
                                 int                fd,
                                 const void*        buf,
                                 size_t             count,
                                 off_t              offset,
                                 ssize_t*           ret)
{
    int                  rc;
    struct io_uring_sqe* sqe      = io_uring_get_sqe(&aid->ring);
    struct abt_io_op     stack_op = {0};
    struct abt_io_op*    op;

    if (arg_op == NULL)
        op = &stack_op;
    else
        op = arg_op;

    rc = ABT_eventual_create(0, &op->e);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }
    op->state = ret;

    /* TODO: abt_io_log() support for uring operations */

    /* NOTE: some older kernels (at least 5.3.x do not support the WRITE
     * operation, but they do support WRITEV, so we use that here.  It's
     * also possible that older kernels require the iovec state to be stable
     * until completion according the NOTES section of the
     * io_uring_prep_writev() man page.  For maximum safety we therefore use
     * a field in the abt_io_op structure for this purpose.
     */
    op->vec.iov_base = (void*)buf;
    op->vec.iov_len  = count;
    io_uring_prep_writev(sqe, fd, &op->vec, 1, offset);
    io_uring_sqe_set_data(sqe, op);
    io_uring_submit(&aid->ring);

    if (arg_op == NULL) {
        rc = ABT_eventual_wait(op->e, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->free_fn = free;
    }

    if (arg_op == NULL) ABT_eventual_free(&op->e);
    return 0;
err:
    if (op->e != NULL) ABT_eventual_free(&op->e);
    return -1;
}
#endif /* USE_LIBURING */

static int issue_pwrite_posix(abt_io_instance_id aid,
                              abt_io_op_t*       op,
                              int                fd,
                              const void*        buf,
                              size_t             count,
                              off_t              offset,
                              ssize_t*           ret)
{
    struct abt_io_pwrite_state  state;
    struct abt_io_pwrite_state* pstate = NULL;
    int                         rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->buf      = buf;
    pstate->count    = count;
    pstate->offset   = offset;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_pwrite_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_pwrite(
    abt_io_instance_id aid, int fd, const void* buf, size_t count, off_t offset)
{
    ssize_t ret = -1;
    issue_pwrite(aid, NULL, fd, buf, count, offset, &ret);
    return ret;
}

abt_io_op_t* abt_io_pwrite_nb(abt_io_instance_id aid,
                              int                fd,
                              const void*        buf,
                              size_t             count,
                              off_t              offset,
                              ssize_t*           ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_pwrite(aid, op, fd, buf, count, offset, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_write_state {
    ssize_t*           ret;
    int                fd;
    const void*        buf;
    size_t             count;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_write_fn(void* foo)
{
    struct abt_io_write_state* state = foo;
    double                     start = ABT_get_wtime();

    if (state->aid->do_null_io_write)
        *state->ret = state->count;
    else {
        *state->ret = write(state->fd, state->buf, state->count);
        if (*state->ret < 0) *state->ret = -errno;
    }

    abt_io_log(state->aid, "write", -1, state->count, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_write(abt_io_instance_id aid,
                       abt_io_op_t*       op,
                       int                fd,
                       const void*        buf,
                       size_t             count,
                       ssize_t*           ret)
{
    struct abt_io_write_state  state;
    struct abt_io_write_state* pstate = NULL;
    int                        rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->buf      = buf;
    pstate->count    = count;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_write_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t
abt_io_write(abt_io_instance_id aid, int fd, const void* buf, size_t count)
{
    ssize_t ret = -1;
    issue_write(aid, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_write_nb(
    abt_io_instance_id aid, int fd, const void* buf, size_t count, ssize_t* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_write(aid, op, fd, buf, count, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_mkostemp_state {
    int*               ret;
    char*              tpl;
    int                flags;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_mkostemp_fn(void* foo)
{
    struct abt_io_mkostemp_state* state = foo;
    double                        start = ABT_get_wtime();

#ifdef HAVE_MKOSTEMP
    *state->ret = mkostemp(state->tpl, state->flags);
#else
    *state->ret = mkstemp(state->tpl);
#endif
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "mkostemp", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_mkostemp(
    abt_io_instance_id aid, abt_io_op_t* op, char* tpl, int flags, int* ret)
{
    struct abt_io_mkostemp_state  state;
    struct abt_io_mkostemp_state* pstate = NULL;
    int                           rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->tpl      = tpl;
    pstate->flags    = flags;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_mkostemp_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_mkostemp(abt_io_instance_id aid, char* tpl, int flags)
{
    int ret = -1;
    issue_mkostemp(aid, NULL, tpl, flags, &ret);
    return ret;
}

abt_io_op_t*
abt_io_mkostemp_nb(abt_io_instance_id aid, char* tpl, int flags, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_mkostemp(aid, op, tpl, flags, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_unlink_state {
    int*               ret;
    const char*        pathname;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_unlink_fn(void* foo)
{
    struct abt_io_unlink_state* state = foo;
    double                      start = ABT_get_wtime();

    *state->ret = unlink(state->pathname);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "unlink", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_unlink(abt_io_instance_id aid,
                        abt_io_op_t*       op,
                        const char*        pathname,
                        int*               ret)
{
    struct abt_io_unlink_state  state;
    struct abt_io_unlink_state* pstate = NULL;
    int                         rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->pathname = pathname;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_unlink_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_unlink(abt_io_instance_id aid, const char* pathname)
{
    int ret = -1;
    issue_unlink(aid, NULL, pathname, &ret);
    return ret;
}

abt_io_op_t*
abt_io_unlink_nb(abt_io_instance_id aid, const char* pathname, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_unlink(aid, op, pathname, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_close_state {
    int*               ret;
    int                fd;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_close_fn(void* foo)
{
    struct abt_io_close_state* state = foo;
    double                     start = ABT_get_wtime();

    *state->ret = close(state->fd);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "close", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int
issue_close(abt_io_instance_id aid, abt_io_op_t* op, int fd, int* ret)
{
    struct abt_io_close_state  state;
    struct abt_io_close_state* pstate = NULL;
    int                        rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_close_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_close(abt_io_instance_id aid, int fd)
{
    int ret = -1;
    issue_close(aid, NULL, fd, &ret);
    return ret;
}

abt_io_op_t* abt_io_close_nb(abt_io_instance_id aid, int fd, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_close(aid, op, fd, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_fdatasync_state {
    int*               ret;
    int                fd;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_fdatasync_fn(void* foo)
{
    struct abt_io_fdatasync_state* state = foo;
    double                         start = ABT_get_wtime();

    *state->ret = fdatasync(state->fd);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "sync", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int
issue_fdatasync(abt_io_instance_id aid, abt_io_op_t* op, int fd, int* ret)
{
    struct abt_io_fdatasync_state  state;
    struct abt_io_fdatasync_state* pstate = NULL;
    int                            rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_fdatasync_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_fdatasync(abt_io_instance_id aid, int fd)
{
    int ret = -1;
    issue_fdatasync(aid, NULL, fd, &ret);
    return ret;
}

abt_io_op_t* abt_io_fdatasync_nb(abt_io_instance_id aid, int fd, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fdatasync(aid, op, fd, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_fallocate_state {
    int*               ret;
    int                fd;
    int                mode;
    off_t              offset;
    off_t              len;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_fallocate_fn(void* foo)
{
    struct abt_io_fallocate_state* state = foo;
    double                         start = ABT_get_wtime();

#ifdef HAVE_FALLOCATE
    *state->ret = fallocate(state->fd, state->mode, state->offset, state->len);
    if (*state->ret < 0) *state->ret = -errno;
#else
    *state->ret = -ENOSYS;
#endif

    abt_io_log(state->aid, "fallocate", state->offset, state->len, start,
               ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_fallocate(abt_io_instance_id aid,
                           abt_io_op_t*       op,
                           int                fd,
                           int                mode,
                           off_t              offset,
                           off_t              len,
                           int*               ret)
{
    struct abt_io_fallocate_state  state;
    struct abt_io_fallocate_state* pstate = NULL;
    int                            rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->fd       = fd;
    pstate->mode     = mode;
    pstate->offset   = offset;
    pstate->len      = len;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }
    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_fallocate_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_fallocate(
    abt_io_instance_id aid, int fd, int mode, off_t offset, off_t len)
{
    int ret = -1;
    issue_fallocate(aid, NULL, fd, mode, offset, len, &ret);
    return ret;
}

abt_io_op_t* abt_io_fallocate_nb(
    abt_io_instance_id aid, int fd, int mode, off_t offset, off_t len, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fallocate(aid, op, fd, mode, offset, len, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_statfs_state {
    int*               ret;
    char*              pathname;
    struct statfs*     statfsbuf;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_statfs_fn(void* foo)
{
    struct abt_io_statfs_state* state = foo;
    double                      start = ABT_get_wtime();

    *state->ret = statfs(state->pathname, state->statfsbuf);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "statfs", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_statfs(abt_io_instance_id aid,
                        abt_io_op_t*       op,
                        char*              pathname,
                        struct statfs*     statfsbuf,
                        int*               ret)
{
    struct abt_io_statfs_state  state;
    struct abt_io_statfs_state* pstate = NULL;
    int                         rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret              = -ENOSYS;
    pstate->ret       = ret;
    pstate->pathname  = pathname;
    pstate->statfsbuf = statfsbuf;
    pstate->eventual  = NULL;
    pstate->aid       = aid;
    rc                = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_statfs_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }
    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_statfs(abt_io_instance_id aid,
                  char*              pathname,
                  struct statfs*     statfsbuf)
{
    int ret = -1;
    issue_statfs(aid, NULL, pathname, statfsbuf, &ret);
    return ret;
}

struct abt_io_stat_state {
    int*               ret;
    char*              pathname;
    struct stat*       statbuf;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_stat_fn(void* foo)
{
    struct abt_io_stat_state* state = foo;
    double                    start = ABT_get_wtime();

    *state->ret = stat(state->pathname, state->statbuf);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "stat", 0, 0, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_stat(abt_io_instance_id aid,
                      abt_io_op_t*       op,
                      char*              pathname,
                      struct stat*       statbuf,
                      int*               ret)
{
    struct abt_io_stat_state  state;
    struct abt_io_stat_state* pstate = NULL;
    int                       rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->pathname = pathname;
    pstate->statbuf  = statbuf;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_stat_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }
    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_stat(abt_io_instance_id aid, char* pathname, struct stat* statbuf)
{
    int ret = -1;
    issue_stat(aid, NULL, pathname, statbuf, &ret);
    return ret;
}

struct abt_io_truncate_state {
    int*               ret;
    char*              pathname;
    off_t              length;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_truncate_fn(void* foo)
{
    struct abt_io_truncate_state* state = foo;
    double                        start = ABT_get_wtime();

    *state->ret = truncate(state->pathname, state->length);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "truncate", 0, state->length, start,
               ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_truncate(abt_io_instance_id aid,
                          abt_io_op_t*       op,
                          char*              pathname,
                          off_t              offset,
                          int*               ret)
{
    struct abt_io_truncate_state  state;
    struct abt_io_truncate_state* pstate = NULL;
    int                           rc;

    if (op == NULL)
        pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret             = -ENOSYS;
    pstate->ret      = ret;
    pstate->pathname = pathname;
    pstate->length   = offset;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_truncate_fn, pstate, NULL);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = pstate->eventual;
        op->state   = pstate;
        op->free_fn = free;
    }
    if (op == NULL) ABT_eventual_free(&pstate->eventual);
    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_truncate(abt_io_instance_id aid, char* pathname, off_t offset)
{
    int ret = -1;
    issue_truncate(aid, NULL, pathname, offset, &ret);
    return ret;
}

int abt_io_op_wait(abt_io_op_t* op)
{
    int ret;

    ret = ABT_eventual_wait(op->e, NULL);
    return ret == ABT_SUCCESS ? 0 : -1;
}

void abt_io_op_free(abt_io_op_t* op)
{
    ABT_eventual_free(&op->e);
    op->free_fn(op->state);
    free(op);
}

size_t abt_io_get_pending_op_count(abt_io_instance_id aid)
{
    size_t size;
    int    ret;
    ret = ABT_pool_get_size(aid->progress_pool, &size);
    if (ret == ABT_SUCCESS)
        return size;
    else
        return -1;
}

char* abt_io_get_config(abt_io_instance_id aid)
{
    const char* content = json_object_to_json_string_ext(
        aid->json_cfg,
        JSON_C_TO_STRING_PRETTY | JSON_C_TO_STRING_NOSLASHESCAPE);
    return strdup(content);
}

static int validate_and_complete_config(struct json_object* _config,
                                        ABT_pool _custom_progress_pool)
{
    struct json_object* val;
    struct json_object* _internal_pool;
    int                 backing_thread_count = -1; /* sentinal value */

    /* ------- abt-io configuration examples ------
     *
     * tuning and debugging flags
     * --------------
     *  "null_io" is a trick we learned in PVFS:  operations return immediately
     *  allowing us to look for performance problems in the rest of the code
     *  paths.  I'm splitting "read" and "write" because some workloads, like
     *  writing to an HDF5 file, read a dataset before writing
     * {"null_io_write": false}
     * {"null_io_read": false}
     *
     * "trace_io" records every ABT-IO operation along with information like
     * offset, size, and duration
     * {"trace_io": false}
     *
     * optional input fields for convenience:
     * --------------
     * {"backing_thread_count": 16}
     *
     * canonical runtime json if pool is external:
     * --------------
     * {"internal_pool_flag": 0}
     *
     * canonical runtime json if pool is internal:
     * --------------
     * {"internal_pool_flag": 1,
     *    "internal_pool":{
     *       "kind":"fifo_wait",
     *       "access":"mpmc",
     *       "num_xstreams": 4
     *    }
     * }
     *
     * control over io engine type
     * --------------
     * Then engine field can be set to control what I/O engine will be used
     * to execute operations.  The current options are:
     * - "posix" - the default option; uses conventional open, close, pwrite,
     *    etc. functions executed within a dedicated Argobots pool
     * - "liburing" - uses the Linux specifc liburing library
     *
     * example:
     * {"engine"="posix"}
     */

    /* report version number for this component */
    CONFIG_OVERRIDE_STRING(_config, "version", PACKAGE_VERSION, "version", 1);

    if (!CONFIG_HAS(_config, "null_io_write", val)) {
        CONFIG_OVERRIDE_BOOL(_config, "null_io_write", 0, "null_io_write", 0);
    }

    if (!CONFIG_HAS(_config, "null_io_read", val)) {
        CONFIG_OVERRIDE_BOOL(_config, "null_io_read", 0, "null_io_read", 0);
    }

    {
        const char* trace_io_str     = getenv("ABT_IO_TRACE_IO");
        int         trace_io_enabled = (trace_io_str ? atoi(trace_io_str) : 0);
        if (!CONFIG_HAS(_config, "trace_io", val)) {
            CONFIG_OVERRIDE_BOOL(_config, "trace_io", 0, "trace_io", 0);
            if (trace_io_str)
                CONFIG_OVERRIDE_BOOL(_config, "trace_io", trace_io_enabled,
                                     "trace_io", trace_io_enabled);
        }
    }

    if (!CONFIG_HAS(_config, "trace_io", val)) {
        CONFIG_OVERRIDE_BOOL(_config, "trace_io", 0, "trace_io", 0);
    }
    /* check if thread count convenience field is set */
    if (CONFIG_HAS(_config, "backing_thread_count", val)) {
        backing_thread_count = json_object_get_int(val);
        /* delete it because we don't want this in the runtime json */
        json_object_object_del(_config, "backing_thread_count");
    }

    if (_custom_progress_pool != ABT_POOL_NULL
        && _custom_progress_pool != NULL) {
        /* explicit pool provided by user overrides anything in json */
        if (backing_thread_count > 0) {
            fprintf(stderr,
                    "abt-io warning: ignoring backing_thread_count because "
                    "explicit pool was provided.\n");
        }
        if (CONFIG_HAS(_config, "internal_pool", val)) {
            json_object_object_del(_config, "internal_pool");
            fprintf(stderr,
                    "abt-io warning: ignoring internal pool object because "
                    "explicit pool was provided.\n");
        }
        /* denote that abt-io is _not_ using internal pool */
        CONFIG_OVERRIDE_BOOL(_config, "internal_pool_flag", 0,
                             "internal_pool_flag", 1);
    } else {
        /* denote that abt-io is using it's own internal pool */
        CONFIG_OVERRIDE_BOOL(_config, "internal_pool_flag", 1,
                             "internal_pool_flag", 0);
        /* create obj to describe internal pool */
        CONFIG_HAS_OR_CREATE_OBJECT(_config, "internal_pool", "internal_pool",
                                    _internal_pool);
        if (CONFIG_HAS(_internal_pool, "num_xstreams", val)) {
            if (backing_thread_count > 0) {
                fprintf(stderr,
                        "abt-io warning: ignoring backing_thread_count in "
                        "favor of num_xstreams.\n");
            }
        } else {
            if (backing_thread_count < 0)
                backing_thread_count = DEFAULT_BACKING_THREAD_COUNT;
            CONFIG_OVERRIDE_INTEGER(_internal_pool, "num_xstreams",
                                    backing_thread_count,
                                    "internal_pool.num_xstreams", 0);
        }
        /* NOTE: abt-io only supports making one kind of pool.  If you want
         * something different, configure it externally and pass it in
         * rather than asking abt-io to do it.
         */
        CONFIG_OVERRIDE_STRING(_internal_pool, "kind", "fifo_wait",
                               "internal_pool.kind", 1);
        CONFIG_OVERRIDE_STRING(_internal_pool, "access", "mpmc",
                               "internal_pool.kind", 1);
    }

    /* I/O engine type */
    if (!CONFIG_HAS(_config, "engine", val)) {
        CONFIG_OVERRIDE_STRING(_config, "engine", "posix", "engine", 0);
    } else {
        struct json_object* jengine = json_object_object_get(_config, "engine");
        CONFIG_IS_IN_ENUM_STRING(jengine, "engine", "posix", "liburing");
    }

    return (0);
}

static int setup_pool(abt_io_instance_id  aid,
                      struct json_object* _config,
                      ABT_pool            _progress_pool)
{
    struct json_object* _internal_pool_cfg;
    ABT_pool            pool;
    ABT_xstream*        progress_xstreams = NULL;
    ABT_sched*          progress_scheds   = NULL;
    int                 i;
    int                 ret;

    if (_progress_pool) {
        /* using external pool */
        aid->progress_pool     = _progress_pool;
        aid->progress_xstreams = NULL;
        aid->num_xstreams      = 0;
        return (0);
    }

    /* create internal pool */
    /* NOTE: some asserts in here rather than error handling, because we
     * should be able to trust the json once it's passed validation
     */
    _internal_pool_cfg = json_object_object_get(_config, "internal_pool");
    assert(_internal_pool_cfg);
    aid->num_xstreams = json_object_get_int(
        json_object_object_get(_internal_pool_cfg, "num_xstreams"));

    progress_xstreams = malloc(aid->num_xstreams * sizeof(*progress_xstreams));
    if (progress_xstreams == NULL) { return -1; }
    progress_scheds = malloc(aid->num_xstreams * sizeof(*progress_scheds));
    if (progress_scheds == NULL) {
        free(progress_xstreams);
        return -1;
    }

    ret = ABT_pool_create_basic(ABT_POOL_FIFO_WAIT, ABT_POOL_ACCESS_MPMC,
                                ABT_TRUE, &pool);
    if (ret != ABT_SUCCESS) {
        free(progress_xstreams);
        free(progress_scheds);
        return -1;
    }

    for (i = 0; i < aid->num_xstreams; i++) {
        ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 1, &pool,
                                     ABT_SCHED_CONFIG_NULL,
                                     &progress_scheds[i]);
        if (ret != ABT_SUCCESS) {
            free(progress_xstreams);
            free(progress_scheds);
            return -1;
        }
        ret = ABT_xstream_create(progress_scheds[i], &progress_xstreams[i]);
        if (ret != ABT_SUCCESS) {
            free(progress_xstreams);
            free(progress_scheds);
            return -1;
        }
    }

    free(progress_scheds);
    aid->progress_pool     = pool;
    aid->progress_xstreams = progress_xstreams;

    return (0);
}

static void teardown_pool(abt_io_instance_id aid)
{
    int i;

    if (!aid->num_xstreams) {
        /* externally created; don't touch it */
        aid->progress_pool = NULL;
    } else {
        /* clean up our own pool */
        for (i = 0; i < aid->num_xstreams; i++) {
            ABT_xstream_join(aid->progress_xstreams[i]);
            ABT_xstream_free(&aid->progress_xstreams[i]);
        }
        free(aid->progress_xstreams);
        /* pool gets implicitly freed */
    }

    return;
}

#ifdef USE_LIBURING
static int issue_pread_liburing(abt_io_instance_id aid,
                                abt_io_op_t*       arg_op,
                                int                fd,
                                void*              buf,
                                size_t             count,
                                off_t              offset,
                                ssize_t*           ret)
{
    int                  rc;
    struct io_uring_sqe* sqe      = io_uring_get_sqe(&aid->ring);
    struct abt_io_op     stack_op = {0};
    struct abt_io_op*    op;

    if (arg_op == NULL)
        op = &stack_op;
    else
        op = arg_op;

    rc = ABT_eventual_create(0, &op->e);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }
    op->state = ret;

    /* TODO: abt_io_log() support for uring operations */

    /* NOTE: some older kernels (at least 5.3.x do not support the WRITE
     * operation, but they do support WRITEV, so we use that here.  It's
     * also possible that older kernels require the iovec state to be stable
     * until completion according the NOTES section of the
     * io_uring_prep_readv() man page.  For maximum safety we therefore use
     * a field in the abt_io_op structure for this purpose.
     */
    op->vec.iov_base = buf;
    op->vec.iov_len  = count;
    io_uring_prep_readv(sqe, fd, &op->vec, 1, offset);
    io_uring_sqe_set_data(sqe, op);
    io_uring_submit(&aid->ring);

    if (arg_op == NULL) {
        rc = ABT_eventual_wait(op->e, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->free_fn = free;
    }

    if (arg_op == NULL) ABT_eventual_free(&op->e);
    return 0;
err:
    if (op->e != NULL) ABT_eventual_free(&op->e);
    return -1;
}
#endif /* USE_LIBURING */
