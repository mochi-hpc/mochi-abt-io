/*
 * (C) 2015 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "abt-io-config.h"

#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#if defined(__APPLE__)
    #include <sys/mount.h>
#else
    #include <sys/vfs.h>
#endif
#include <fcntl.h>
#include <json-c/json.h>
#ifdef USE_LIBURING
    #include <liburing.h>
    #include <sys/eventfd.h>
    #include <poll.h>
#endif

#include <abt.h>
#include "abt-io.h"
#include "abt-io-macros.h"
#include "utlist.h"

#define DEFAULT_BACKING_THREAD_COUNT 16
#define DEFAULT_URING_ENTRIES        64

struct abt_io_io_state {
    ssize_t*           ret;
    int                fd;
    void*              buf;
    size_t             count;
    off_t              offset;
    struct iovec       vec;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

#ifdef USE_LIBURING
struct uring_op_state {
    enum io_uring_op       op_type;
    struct uring_op_state* next;
    struct uring_op_state* prev;
    union {
        struct abt_io_io_state io_state;
    } u;
};

struct uring_engine_state {
    struct io_uring         ring;
    struct abt_io_instance* aid;
    int                     efd;
    /* Each uring engine gets a dedicated pool, scheduler, execution stream,
     * and thread. This guarantees a) that each engine is always able to
     * execute and b) that each ring is only accessed from a single
     * operating system thread.
     */
    ABT_pool               pool;
    ABT_sched              sched;
    ABT_xstream            xstream;
    ABT_thread             tid;
    ABT_mutex              op_queue_mutex;
    struct uring_op_state* op_queue;
    struct uring_op_state* op_working_set;
};
#endif

struct abt_io_instance {
    ABT_pool     progress_pool;
    ABT_xstream* progress_xstreams;
    int          num_xstreams;
    int          num_urings;
#ifdef USE_LIBURING
    int                        uring_sqe_flags;
    int                        uring_setup_flags;
    struct io_uring_probe*     uring_probe;
    int                        uring_shutdown_flag;
    int                        uring_op_counter;
    ABT_barrier                uring_barrier;
    struct uring_engine_state* uring_state_array;
#endif
    struct json_object* json_cfg;
    int                 do_null_io_read;
    int                 do_null_io_write;
    int                 logging_enabled; /* debugging: log every i/o call? */
    double epoch_start; /* i/o logs will report time relative to this */
};

struct abt_io_op {
    ABT_eventual e;
    void*        state;
    void (*free_fn)(void*);
};

/**
 * Validates the format of the configuration and fills default values
 * if they are not provided
 */
static int validate_and_complete_config(struct json_object* _config,
                                        ABT_pool            _progress_pool);

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

static int issue_pwrite_default(abt_io_instance_id aid,
                                abt_io_op_t*       op,
                                int                fd,
                                const void*        buf,
                                size_t             count,
                                off_t              offset,
                                ssize_t*           ret);

static int (*issue_pwrite)(
    abt_io_instance_id, abt_io_op_t*, int, const void*, size_t, off_t, ssize_t*)
    = issue_pwrite_default;

static int issue_pread_default(abt_io_instance_id aid,
                               abt_io_op_t*       op,
                               int                fd,
                               void*              buf,
                               size_t             count,
                               off_t              offset,
                               ssize_t*           ret);

static int (*issue_pread)(
    abt_io_instance_id, abt_io_op_t*, int, void*, size_t, off_t, ssize_t*)
    = issue_pread_default;

#ifdef USE_LIBURING
static void uring_engine_fn(void* foo);
static int  issue_pwrite_uring(abt_io_instance_id aid,
                               abt_io_op_t*       op,
                               int                fd,
                               const void*        buf,
                               size_t             count,
                               off_t              offset,
                               ssize_t*           ret);
static int  issue_pread_uring(abt_io_instance_id aid,
                              abt_io_op_t*       op,
                              int                fd,
                              void*              buf,
                              size_t             count,
                              off_t              offset,
                              ssize_t*           ret);
#endif

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
    int                     json_idx = 0;
    struct json_object*     json_obj = NULL;

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

    aid->num_urings
        = json_object_get_int(json_object_object_get(config, "num_urings"));

#ifdef USE_LIBURING
    /* note any sqe flags that were requested */
    json_array_foreach(json_object_object_get(config, "liburing_flags"),
                       json_idx, json_obj)
    {
        const char* flag = json_object_get_string(json_obj);
        if (!strcmp(flag, "IOSQE_ASYNC")) {
            aid->uring_sqe_flags |= IOSQE_ASYNC;
        } else if (!strcmp(flag, "IORING_SETUP_SQPOLL")) {
            aid->uring_setup_flags |= IORING_SETUP_SQPOLL;
        } else if (!strcmp(flag, "IORING_SETUP_COOP_TASKRUN")) {
            aid->uring_setup_flags |= IORING_SETUP_COOP_TASKRUN;
        } else if (!strcmp(flag, "IORING_SETUP_SINGLE_ISSUER")) {
            aid->uring_setup_flags |= IORING_SETUP_SINGLE_ISSUER;
        } else if (!strcmp(flag, "IORING_SETUP_DEFER_TASKRUN")) {
            aid->uring_setup_flags |= IORING_SETUP_DEFER_TASKRUN;
        } else {
            fprintf(stderr, "Error: liburing_flag %s not supported.\n", flag);
        }
    }
#endif

    /* TODO: implement a second key "trace_file" that takes a format specifier
     * similar to valgrind's %p */
    if (aid->logging_enabled) {
        fprintf(
            stderr,
            "#Module\tRank\tOp\tSegment\tOffset\tLength\tStart(s)\tEnd(s)\n");
    }

    ret = setup_pool(aid, config, args.progress_pool);
    if (ret != 0) goto error;

#ifdef USE_LIBURING
    int i;
    /* start uring engines, if any */

    /* use barrier to wait until they are all ready before proceeding */
    ABT_barrier_create(aid->num_urings + 1, &aid->uring_barrier);
    if (aid->num_urings) {
        /* probe to see what capabilities we have */
        aid->uring_probe = io_uring_get_probe();
        aid->uring_state_array
            = calloc(aid->num_urings, sizeof(*aid->uring_state_array));
        if (!aid->uring_state_array) {
            perror("calloc");
            fprintf(stderr, "Error: unable to allocate uring state array.\n");
            goto error;
        }
        /* override issue fn pointers for any supported operations */
        issue_pwrite = issue_pwrite_uring;
        issue_pread  = issue_pread_uring;
    }
    for (i = 0; i < aid->num_urings; i++) {
        ABT_mutex_create(&aid->uring_state_array[i].op_queue_mutex);

        ret = ABT_pool_create_basic(ABT_POOL_FIFO_WAIT, ABT_POOL_ACCESS_PRIV,
                                    ABT_FALSE, &aid->uring_state_array[i].pool);
        if (ret != ABT_SUCCESS) {
            fprintf(stderr, "Error: unable to create uring pool.\n");
            goto error;
        }
        ret = ABT_sched_create_basic(
            ABT_SCHED_BASIC_WAIT, 1, &aid->uring_state_array[i].pool,
            ABT_SCHED_CONFIG_NULL, &aid->uring_state_array[i].sched);
        if (ret != ABT_SUCCESS) {
            fprintf(stderr, "Error: unable to create uring sched.\n");
            goto error;
        }
        ret = ABT_xstream_create(aid->uring_state_array[i].sched,
                                 &aid->uring_state_array[i].xstream);
        if (ret != ABT_SUCCESS) {
            fprintf(stderr, "Error: unable to create uring xstream.\n");
            goto error;
        }

        aid->uring_state_array[i].aid = aid;
        ret = ABT_thread_create(aid->uring_state_array[i].pool, uring_engine_fn,
                                &aid->uring_state_array[i],
                                ABT_THREAD_ATTR_NULL,
                                &aid->uring_state_array[i].tid);
        if (ret != ABT_SUCCESS) {
            fprintf(stderr, "Error: unable to create uring thread.\n");
            goto error;
        }
    }
    ABT_barrier_wait(aid->uring_barrier);
    ABT_barrier_free(&aid->uring_barrier);
#endif

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
    int i;

    aid->uring_shutdown_flag = 1;
    for (i = 0; i < aid->num_urings; i++) {
        eventfd_write(aid->uring_state_array[i].efd, 1);
        ABT_thread_join(aid->uring_state_array[i].tid);
        ABT_xstream_join(aid->uring_state_array[i].xstream);
        ABT_xstream_free(&aid->uring_state_array[i].xstream);
        ABT_pool_free(&aid->uring_state_array[i].pool);
        ABT_mutex_free(&aid->uring_state_array[i].op_queue_mutex);
    }
    if (aid->num_urings) free(aid->uring_state_array);
    if (aid->uring_probe) io_uring_free_probe(aid->uring_probe);
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

static void abt_io_pread_fn(void* foo)
{
    struct abt_io_io_state* state = foo;
    double                  start = ABT_get_wtime();

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

static int issue_pread_default(abt_io_instance_id aid,
                               abt_io_op_t*       op,
                               int                fd,
                               void*              buf,
                               size_t             count,
                               off_t              offset,
                               ssize_t*           ret)
{
    struct abt_io_io_state  state;
    struct abt_io_io_state* pstate = NULL;
    int                     rc;

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

static void abt_io_pwrite_fn(void* foo)
{
    struct abt_io_io_state* state = foo;
    double                  start = ABT_get_wtime();

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

static int issue_pwrite_default(abt_io_instance_id aid,
                                abt_io_op_t*       op,
                                int                fd,
                                const void*        buf,
                                size_t             count,
                                off_t              offset,
                                ssize_t*           ret)
{
    struct abt_io_io_state  state;
    struct abt_io_io_state* pstate = NULL;
    int                     rc;

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
    pstate->buf      = (void*)buf;
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

struct abt_io_ftruncate_state {
    int*               ret;
    int                fd;
    off_t              length;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_ftruncate_fn(void* foo)
{
    struct abt_io_ftruncate_state* state = foo;
    double                         start = ABT_get_wtime();

    *state->ret = ftruncate(state->fd, state->length);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "ftruncate", 0, state->length, start,
               ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);

    return;
}

static int issue_ftruncate(
    abt_io_instance_id aid, abt_io_op_t* op, int fd, off_t length, int* ret)
{
    struct abt_io_ftruncate_state  state;
    struct abt_io_ftruncate_state* pstate = NULL;
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
    pstate->length   = length;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_ftruncate_fn, pstate, NULL);
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

int abt_io_ftruncate(abt_io_instance_id aid, int fd, off_t length)
{
    int ret = -1;
    issue_ftruncate(aid, NULL, fd, length, &ret);
    return ret;
}

abt_io_op_t*
abt_io_ftruncate_nb(abt_io_instance_id aid, int fd, off_t length, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_ftruncate(aid, op, fd, length, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_lseek_state {
    off_t*             ret;
    int                fd;
    off_t              offset;
    int                whence;
    ABT_eventual       eventual;
    abt_io_instance_id aid;
};

static void abt_io_lseek_fn(void* foo)
{
    struct abt_io_lseek_state* state = foo;
    double                     start = ABT_get_wtime();

    *state->ret = lseek(state->fd, state->offset, state->whence);
    if (*state->ret < 0) *state->ret = -errno;

    abt_io_log(state->aid, "lseek", 0, state->offset, start, ABT_get_wtime());
    ABT_eventual_set(state->eventual, NULL, 0);

    return;
}

static int issue_lseek(abt_io_instance_id aid,
                       abt_io_op_t*       op,
                       int                fd,
                       off_t              offset,
                       int                whence,
                       off_t*             ret)
{
    struct abt_io_lseek_state  state;
    struct abt_io_lseek_state* pstate = NULL;
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
    pstate->offset   = offset;
    pstate->whence   = whence;
    pstate->eventual = NULL;
    pstate->aid      = aid;
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -EINVAL;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_lseek_fn, pstate, NULL);
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

off_t abt_io_lseek(abt_io_instance_id aid, int fd, off_t offset, int whence)
{
    off_t ret = -1;
    issue_lseek(aid, NULL, fd, offset, whence, &ret);
    return ret;
}

abt_io_op_t* abt_io_lseek_nb(
    abt_io_instance_id aid, int fd, off_t offset, int whence, off_t* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_lseek(aid, op, fd, offset, whence, ret);
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
    struct json_object* array;

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
     * "num_urings" indicates how many urings (if any) should be activated
     * to process supported operations.  Each uring will consume exactly one
     * xstream from the progress pool, so the progress pool must be at least
     * of size num_urings+1 in order to handle operations that cannot be
     * performed with liburing.
     * {"num_urings": 0}
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

    if (!CONFIG_HAS(_config, "num_urings", val)) {
        CONFIG_OVERRIDE_INTEGER(_config, "num_urings", 0, "num_urings", 0);
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

        /* NOTE: there is not an easy way to tell how many xstreams are
         * assoicated with an externally-provided pool.  We have to trust
         * that it has at least one more than the value of num_urings.
         */
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

    /* optional liburing flags */
    array = json_object_object_get(_config, "liburing_flags");
    if (array && !json_object_is_type(array, json_type_array)) {
        fprintf(stderr,
                "Error: \"liburing_flags\" is in configuration but is not an "
                "array.\n");
        return (-1);
    } else if (!array) {
        /* Create array with default flags.  Presently we set no flags
         * automatically, but this could change in the future. */
        /* Note that unlike uring opcodes, it is difficult to detect
         * supported flags at runtime, so be cautious when adding flags that
         * may not be supported in older Linux kernels.
         */
        array = json_object_new_array();
        json_object_object_add(_config, "liburing_flags", array);
    }

#ifndef USE_LIBURING
    int sanity_num_urings
        = json_object_get_int(json_object_object_get(_config, "num_urings"));
    if (sanity_num_urings > 0) {
        fprintf(stderr,
                "abt-io error: num_urings is non-zero but mochi-abt-io was not "
                "compiled with --enable-liburing\n");
        return (-1);
    }
#endif

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

static void uring_engine_fn(void* foo)
{
    struct uring_engine_state* ues = foo;
    int                        ret;
    struct io_uring_cqe*       cqe;
    struct io_uring_sqe*       sqe;
    struct uring_op_state*     op_state;
    struct uring_op_state*     tmp;
    int64_t*                   result;
    ABT_eventual               ev;

    /* initialize uring queue */
    ret = io_uring_queue_init(DEFAULT_URING_ENTRIES, &ues->ring,
                              ues->aid->uring_setup_flags);
    if (ret != 0) {
        fprintf(stderr, "Error: io_uring_queue_init() failure.\n");
        return;
    }

    /* create event fd that can be used to wake this engine */
    ues->efd = eventfd(0, EFD_NONBLOCK);
    sqe      = io_uring_get_sqe(&ues->ring);
    io_uring_prep_poll_add(sqe, ues->efd, POLLIN);
    io_uring_sqe_set_data(sqe, NULL);
    ret = io_uring_submit(&ues->ring);
    assert(ret >= 0);

    /* wait on barrier once we are ready to process operations */

    /* The purpose here is to make sure that all engines are ready before we
     * proceed.  The caller of the initialize function waits in the
     * same barrier so that initialization does not complete until all engines
     * are ready.
     */
    ABT_barrier_wait(ues->aid->uring_barrier);

    while (!ues->aid->uring_shutdown_flag) {
        /* NOTE: we intentionally do not use a timeout here.  Something
         * external (e.g., an eventfd signal) will break this
         * function out of the wait when needed.
         */

        /* drain operation queue */
        ABT_mutex_lock(ues->op_queue_mutex);
        DL_FOREACH_SAFE(ues->op_queue, op_state, tmp)
        {
            DL_DELETE(ues->op_queue, op_state);
            DL_APPEND(ues->op_working_set, op_state);
        }
        ABT_mutex_unlock(ues->op_queue_mutex);

        /* for each operation that we found above, submit to uring */
        /* NOTE that this is intentionally done in a separate loop so that
         * we don't have to hold the op_queue_mutex across the submissions
         */
        DL_FOREACH_SAFE(ues->op_working_set, op_state, tmp)
        {
            DL_DELETE(ues->op_working_set, op_state);
            sqe = io_uring_get_sqe(&ues->ring);
            /* prepare op */
            switch (op_state->op_type) {
            case IORING_OP_WRITE:
                io_uring_prep_write(
                    sqe, op_state->u.io_state.fd, op_state->u.io_state.buf,
                    op_state->u.io_state.count, op_state->u.io_state.offset);
                break;
            case IORING_OP_WRITEV:
                io_uring_prep_writev(sqe, op_state->u.io_state.fd,
                                     &op_state->u.io_state.vec, 1,
                                     op_state->u.io_state.offset);
                break;
            case IORING_OP_READ:
                io_uring_prep_read(
                    sqe, op_state->u.io_state.fd, op_state->u.io_state.buf,
                    op_state->u.io_state.count, op_state->u.io_state.offset);
                break;
            case IORING_OP_READV:
                io_uring_prep_readv(sqe, op_state->u.io_state.fd,
                                    &op_state->u.io_state.vec, 1,
                                    op_state->u.io_state.offset);
                break;
            default:
                assert(0);
            }
            /* same flags and data for all ops */
            io_uring_sqe_set_data(sqe, op_state);
            io_uring_sqe_set_flags(sqe, ues->aid->uring_sqe_flags);
            /* submit to ring */
            ret = io_uring_submit(&ues->ring);
            assert(ret >= 0);
        }

        /* Wait for operations to complete or for an external caller to wake
         * the engine up to find new operations to submit.
         */
        ret = io_uring_wait_cqe(&ues->ring, &cqe);
        if (ret == 0) {
            if (cqe->user_data) {
                op_state = (struct uring_op_state*)cqe->user_data;
                switch (op_state->op_type) {
                case IORING_OP_WRITE:
                case IORING_OP_READ:
                case IORING_OP_WRITEV:
                case IORING_OP_READV:
                    result = op_state->u.io_state.ret;
                    ev     = op_state->u.io_state.eventual;
                    ABT_mutex_lock(ues->op_queue_mutex);
                    ABT_mutex_unlock(ues->op_queue_mutex);
                    break;
                default:
                    assert(0);
                }
                *result = cqe->res;
                ABT_eventual_set(ev, NULL, 0);
            } else {
                uint64_t tmp_ev;
                /* If user data was not set then we assume that this was
                 * just a wake up event.
                 */

                /* Clear the eventfd to reset the counter and prepare to
                 * poll it again.  Note that the eventfd is in nonblocking
                 * mode to ensure that we don't block if this was a
                 * spurious event.  Ignore return code and event value.
                 */
                eventfd_read(ues->efd, &tmp_ev);
                sqe = io_uring_get_sqe(&ues->ring);
                io_uring_prep_poll_add(sqe, ues->efd, POLLIN);
                io_uring_sqe_set_data(sqe, NULL);
                ret = io_uring_submit(&ues->ring);
                assert(ret >= 0);
            }
            io_uring_cqe_seen(&ues->ring, cqe);
        }
    }

    io_uring_queue_exit(&ues->ring);

    return;
}

static int issue_pwrite_uring(abt_io_instance_id aid,
                              abt_io_op_t*       op,
                              int                fd,
                              const void*        buf,
                              size_t             count,
                              off_t              offset,
                              ssize_t*           ret)
{
    struct uring_op_state  state;
    struct uring_op_state* opstate = NULL;
    int                    rc;
    int                    uring_engine_index;

    if (op == NULL)
        opstate = &state;
    else {
        opstate = malloc(sizeof(*opstate));
        if (opstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    *ret = -ENOSYS;
    if (aid->uring_probe
        && io_uring_opcode_supported(aid->uring_probe, IORING_OP_WRITE)) {
        opstate->op_type          = IORING_OP_WRITE;
        opstate->u.io_state.count = count;
        opstate->u.io_state.buf   = (void*)buf;
    } else {
        /* older kernels only support WRITEV */
        opstate->op_type                 = IORING_OP_WRITEV;
        opstate->u.io_state.vec.iov_len  = count;
        opstate->u.io_state.vec.iov_base = (void*)buf;
    }
    opstate->u.io_state.offset   = offset;
    opstate->u.io_state.ret      = ret;
    opstate->u.io_state.fd       = fd;
    opstate->u.io_state.eventual = NULL;
    opstate->u.io_state.aid      = aid;
    rc = ABT_eventual_create(0, &opstate->u.io_state.eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = opstate->u.io_state.eventual;

    /* find ring to submit to; just round robin */
    uring_engine_index = aid->uring_op_counter++;
    uring_engine_index %= aid->num_urings;

    /* enqueue operation for engine to pick up */
    ABT_mutex_lock(aid->uring_state_array[uring_engine_index].op_queue_mutex);
    DL_APPEND(aid->uring_state_array[uring_engine_index].op_queue, opstate);
    ABT_mutex_unlock(aid->uring_state_array[uring_engine_index].op_queue_mutex);

    /* Signal the corresponding uring engine to break out of its cqe wait
     * and process this operation.
     */
    eventfd_write(aid->uring_state_array[uring_engine_index].efd, 1);

    /* wait for completion, if we are in blocking mode */
    if (op == NULL) {
        rc = ABT_eventual_wait(opstate->u.io_state.eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = opstate->u.io_state.eventual;
        op->state   = opstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&opstate->u.io_state.eventual);
    return 0;
err:
    if (opstate->u.io_state.eventual != NULL)
        ABT_eventual_free(&opstate->u.io_state.eventual);
    if (opstate != NULL && op != NULL) free(opstate);
    return -1;
}

static int issue_pread_uring(abt_io_instance_id aid,
                             abt_io_op_t*       op,
                             int                fd,
                             void*              buf,
                             size_t             count,
                             off_t              offset,
                             ssize_t*           ret)
{
    struct uring_op_state  state;
    struct uring_op_state* opstate = NULL;
    int                    rc;
    int                    uring_engine_index;

    if (op == NULL)
        opstate = &state;
    else {
        opstate = malloc(sizeof(*opstate));
        if (opstate == NULL) {
            *ret = -ENOMEM;
            goto err;
        }
    }

    /* TODO: replace this with liburing utility function instead of
     * accessing probe structure directly.
     */
    *ret = -ENOSYS;
    if (aid->uring_probe && aid->uring_probe->last_op >= IORING_OP_READ) {
        opstate->op_type          = IORING_OP_READ;
        opstate->u.io_state.buf   = buf;
        opstate->u.io_state.count = count;
    } else {
        /* older kernels only support READV */
        opstate->op_type                 = IORING_OP_READV;
        opstate->u.io_state.vec.iov_base = buf;
        opstate->u.io_state.vec.iov_len  = count;
    }
    opstate->u.io_state.ret      = ret;
    opstate->u.io_state.fd       = fd;
    opstate->u.io_state.offset   = offset;
    opstate->u.io_state.eventual = NULL;
    opstate->u.io_state.aid      = aid;
    rc = ABT_eventual_create(0, &opstate->u.io_state.eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = opstate->u.io_state.eventual;

    /* find ring to submit to; just round robin */
    uring_engine_index = aid->uring_op_counter++;
    uring_engine_index %= aid->num_urings;

    /* enqueue operation for engine to pick up */
    ABT_mutex_lock(aid->uring_state_array[uring_engine_index].op_queue_mutex);
    DL_APPEND(aid->uring_state_array[uring_engine_index].op_queue, opstate);
    ABT_mutex_unlock(aid->uring_state_array[uring_engine_index].op_queue_mutex);

    /* Signal the corresponding uring engine to break out of its cqe wait
     * and process this operation.
     */
    eventfd_write(aid->uring_state_array[uring_engine_index].efd, 1);

    /* wait for completion, if we are in blocking mode */
    if (op == NULL) {
        rc = ABT_eventual_wait(opstate->u.io_state.eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) {
            *ret = -EINVAL;
            goto err;
        }
    } else {
        op->e       = opstate->u.io_state.eventual;
        op->state   = opstate;
        op->free_fn = free;
    }

    if (op == NULL) ABT_eventual_free(&opstate->u.io_state.eventual);
    return 0;
err:
    if (opstate->u.io_state.eventual != NULL)
        ABT_eventual_free(&opstate->u.io_state.eventual);
    if (opstate != NULL && op != NULL) free(opstate);
    return -1;
}
#endif
