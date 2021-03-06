
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
#include <fcntl.h>
#include <json-c/json.h>

#include <abt.h>
#include "abt-io.h"
#include "abt-io-macros.h"

#define DEFAULT_BACKING_THREAD_COUNT 16

struct abt_io_instance {
    ABT_pool            progress_pool;
    ABT_xstream*        progress_xstreams;
    int                 num_xstreams;
    struct json_object* json_cfg;
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
    struct abt_io_instance* aid;
    int                     ret;

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

    aid = malloc(sizeof(*aid));
    if (aid == NULL) goto error;

    ret = setup_pool(aid, config, args.progress_pool);
    if (ret != 0) goto error;

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
    teardown_pool(aid);
    json_object_put(aid->json_cfg);
    free(aid);
}

struct abt_io_open_state {
    int*         ret;
    const char*  pathname;
    int          flags;
    mode_t       mode;
    ABT_eventual eventual;
};

static void abt_io_open_fn(void* foo)
{
    struct abt_io_open_state* state = foo;

    *state->ret = open(state->pathname, state->flags, state->mode);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_open(ABT_pool     pool,
                      abt_io_op_t* op,
                      const char*  pathname,
                      int          flags,
                      mode_t       mode,
                      int*         ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_open_fn, pstate, NULL);
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
    issue_open(aid->progress_pool, NULL, pathname, flags, mode, &ret);
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

    iret = issue_open(aid->progress_pool, op, pathname, flags, mode, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_pread_state {
    ssize_t*     ret;
    int          fd;
    void*        buf;
    size_t       count;
    off_t        offset;
    ABT_eventual eventual;
};

static void abt_io_pread_fn(void* foo)
{
    struct abt_io_pread_state* state = foo;

    *state->ret = pread(state->fd, state->buf, state->count, state->offset);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_pread(ABT_pool     pool,
                       abt_io_op_t* op,
                       int          fd,
                       void*        buf,
                       size_t       count,
                       off_t        offset,
                       ssize_t*     ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_pread_fn, pstate, NULL);
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
    issue_pread(aid->progress_pool, NULL, fd, buf, count, offset, &ret);
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

    iret = issue_pread(aid->progress_pool, op, fd, buf, count, offset, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_read_state {
    ssize_t*     ret;
    int          fd;
    void*        buf;
    size_t       count;
    ABT_eventual eventual;
};

static void abt_io_read_fn(void* foo)
{
    struct abt_io_read_state* state = foo;

    *state->ret = read(state->fd, state->buf, state->count);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_read(ABT_pool     pool,
                      abt_io_op_t* op,
                      int          fd,
                      void*        buf,
                      size_t       count,
                      ssize_t*     ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_read_fn, pstate, NULL);
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
    issue_read(aid->progress_pool, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_read_nb(
    abt_io_instance_id aid, int fd, void* buf, size_t count, ssize_t* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_read(aid->progress_pool, op, fd, buf, count, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_pwrite_state {
    ssize_t*     ret;
    int          fd;
    const void*  buf;
    size_t       count;
    off_t        offset;
    ABT_eventual eventual;
};

static void abt_io_pwrite_fn(void* foo)
{
    struct abt_io_pwrite_state* state = foo;

    *state->ret = pwrite(state->fd, state->buf, state->count, state->offset);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_pwrite(ABT_pool     pool,
                        abt_io_op_t* op,
                        int          fd,
                        const void*  buf,
                        size_t       count,
                        off_t        offset,
                        ssize_t*     ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_pwrite_fn, pstate, NULL);
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
    issue_pwrite(aid->progress_pool, NULL, fd, buf, count, offset, &ret);
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

    iret = issue_pwrite(aid->progress_pool, op, fd, buf, count, offset, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_write_state {
    ssize_t*     ret;
    int          fd;
    const void*  buf;
    size_t       count;
    ABT_eventual eventual;
};

static void abt_io_write_fn(void* foo)
{
    struct abt_io_write_state* state = foo;

    *state->ret = write(state->fd, state->buf, state->count);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_write(ABT_pool     pool,
                       abt_io_op_t* op,
                       int          fd,
                       const void*  buf,
                       size_t       count,
                       ssize_t*     ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_write_fn, pstate, NULL);
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
    issue_write(aid->progress_pool, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_write_nb(
    abt_io_instance_id aid, int fd, const void* buf, size_t count, ssize_t* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_write(aid->progress_pool, op, fd, buf, count, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_mkostemp_state {
    int*         ret;
    char*        tpl;
    int          flags;
    ABT_eventual eventual;
};

static void abt_io_mkostemp_fn(void* foo)
{
    struct abt_io_mkostemp_state* state = foo;

#ifdef HAVE_MKOSTEMP
    *state->ret = mkostemp(state->tpl, state->flags);
#else
    *state->ret = mkstemp(state->tpl);
#endif
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int
issue_mkostemp(ABT_pool pool, abt_io_op_t* op, char* tpl, int flags, int* ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_mkostemp_fn, pstate, NULL);
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
    issue_mkostemp(aid->progress_pool, NULL, tpl, flags, &ret);
    return ret;
}

abt_io_op_t*
abt_io_mkostemp_nb(abt_io_instance_id aid, char* tpl, int flags, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_mkostemp(aid->progress_pool, op, tpl, flags, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_unlink_state {
    int*         ret;
    const char*  pathname;
    ABT_eventual eventual;
};

static void abt_io_unlink_fn(void* foo)
{
    struct abt_io_unlink_state* state = foo;

    *state->ret = unlink(state->pathname);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int
issue_unlink(ABT_pool pool, abt_io_op_t* op, const char* pathname, int* ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_unlink_fn, pstate, NULL);
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
    issue_unlink(aid->progress_pool, NULL, pathname, &ret);
    return ret;
}

abt_io_op_t*
abt_io_unlink_nb(abt_io_instance_id aid, const char* pathname, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_unlink(aid->progress_pool, op, pathname, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_close_state {
    int*         ret;
    int          fd;
    ABT_eventual eventual;
};

static void abt_io_close_fn(void* foo)
{
    struct abt_io_close_state* state = foo;

    *state->ret = close(state->fd);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_close(ABT_pool pool, abt_io_op_t* op, int fd, int* ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_close_fn, pstate, NULL);
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
    issue_close(aid->progress_pool, NULL, fd, &ret);
    return ret;
}

abt_io_op_t* abt_io_close_nb(abt_io_instance_id aid, int fd, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_close(aid->progress_pool, op, fd, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_fdatasync_state {
    int*         ret;
    int          fd;
    ABT_eventual eventual;
};

static void abt_io_fdatasync_fn(void* foo)
{
    struct abt_io_fdatasync_state* state = foo;

    *state->ret = fdatasync(state->fd);
    if (*state->ret < 0) *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_fdatasync(ABT_pool pool, abt_io_op_t* op, int fd, int* ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_fdatasync_fn, pstate, NULL);
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
    issue_fdatasync(aid->progress_pool, NULL, fd, &ret);
    return ret;
}

abt_io_op_t* abt_io_fdatasync_nb(abt_io_instance_id aid, int fd, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fdatasync(aid->progress_pool, op, fd, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
}

struct abt_io_fallocate_state {
    int*         ret;
    int          fd;
    int          mode;
    off_t        offset;
    off_t        len;
    ABT_eventual eventual;
};

static void abt_io_fallocate_fn(void* foo)
{
    struct abt_io_fallocate_state* state = foo;

#ifdef HAVE_FALLOCATE
    *state->ret = fallocate(state->fd, state->mode, state->offset, state->len);
    if (*state->ret < 0) *state->ret = -errno;
#else
    *state->ret = -ENOSYS;
#endif

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_fallocate(ABT_pool     pool,
                           abt_io_op_t* op,
                           int          fd,
                           int          mode,
                           off_t        offset,
                           off_t        len,
                           int*         ret)
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
    rc               = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) {
        *ret = -ENOMEM;
        goto err;
    }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_fallocate_fn, pstate, NULL);
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
    issue_fallocate(aid->progress_pool, NULL, fd, mode, offset, len, &ret);
    return ret;
}

abt_io_op_t* abt_io_fallocate_nb(
    abt_io_instance_id aid, int fd, int mode, off_t offset, off_t len, int* ret)
{
    abt_io_op_t* op;
    int          iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fallocate(aid->progress_pool, op, fd, mode, offset, len, ret);
    if (iret != 0) {
        free(op);
        return NULL;
    } else
        return op;
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
