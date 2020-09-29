
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
#include <jansson.h>

#include <abt.h>
#include <mochi-cfg.h>
#include "abt-io.h"

# define ABT_IO_DEFAULT_CFG \
"{   \"abt-io\": {" \
"         \"version\": \"" PACKAGE_VERSION "\"," \
"         \"thread_count\": 16" \
"}}"

struct abt_io_instance
{
    ABT_pool progress_pool;
    ABT_xstream *progress_xstreams;
    ABT_sched *progress_scheds;
    int num_xstreams;
    json_t *component_cfg;
    int logging_enabled;    /* debugging: log every i/o call? */
    double epoch_start;     /* i/o logs will report time relative to this */
};

struct abt_io_op
{
    ABT_eventual e;
    void *state;
    void (*free_fn)(void*);
};

abt_io_instance_id abt_io_init_json(const char* json_cfg_string)
{
    struct abt_io_instance *aid;
    ABT_pool pool;
    ABT_xstream self_xstream;
    ABT_xstream *progress_xstreams = NULL;
    ABT_sched *progress_scheds = NULL;
    int ret;
    int i;
    int thread_count;

    aid = calloc(1, sizeof(*aid));
    if (aid == NULL) return ABT_IO_INSTANCE_NULL;

    aid->component_cfg = mochi_cfg_get_component(json_cfg_string, "abt-io", ABT_IO_DEFAULT_CFG);
    if(!aid->component_cfg)
    {
        fprintf(stderr, "Error: unable to set up abt-io config.\n");
        return(ABT_IO_INSTANCE_NULL);
    }

    ret = mochi_cfg_get_value_int(aid->component_cfg, "thread_count", &thread_count);
    if(ret < 0 || thread_count < 0)
    {
        mochi_cfg_release_component(aid->component_cfg);
        free(aid);
        return(ABT_IO_INSTANCE_NULL);
    }

    if (thread_count == 0) {
        aid->num_xstreams = 0;
        ret = ABT_xstream_self(&self_xstream);
        if (ret != ABT_SUCCESS) {
            mochi_cfg_release_component(aid->component_cfg);
            free(aid);
            return ABT_IO_INSTANCE_NULL;
        }
        ret = ABT_xstream_get_main_pools(self_xstream, 1, &pool);
        if (ret != ABT_SUCCESS) {
            mochi_cfg_release_component(aid->component_cfg);
            free(aid);
            return ABT_IO_INSTANCE_NULL;
        }
    }
    else {
        aid->num_xstreams = thread_count;
        progress_xstreams = malloc(
                thread_count * sizeof(*progress_xstreams));
        if (progress_xstreams == NULL) {
            mochi_cfg_release_component(aid->component_cfg);
            free(aid);
            return ABT_IO_INSTANCE_NULL;
        }
        progress_scheds = malloc(
                thread_count * sizeof(*progress_scheds));
        if (progress_scheds == NULL) {
            mochi_cfg_release_component(aid->component_cfg);
            free(progress_xstreams);
            free(aid);
            return ABT_IO_INSTANCE_NULL;
        }

        ret = ABT_pool_create_basic(ABT_POOL_FIFO_WAIT, ABT_POOL_ACCESS_MPMC, 
            ABT_TRUE, &pool);
        if(ret != ABT_SUCCESS)
        {
            mochi_cfg_release_component(aid->component_cfg);
            free(progress_xstreams);
            free(progress_scheds);
            free(aid);
            return ABT_IO_INSTANCE_NULL;
        }

        for(i=0; i<thread_count; i++)
        {
            ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 1, &pool,
               ABT_SCHED_CONFIG_NULL, &progress_scheds[i]);
            if (ret != ABT_SUCCESS) {
                mochi_cfg_release_component(aid->component_cfg);
                free(progress_xstreams);
                free(progress_scheds);
                free(aid);
                return ABT_IO_INSTANCE_NULL;
            }
            ret = ABT_xstream_create(progress_scheds[i], &progress_xstreams[i]);
            if (ret != ABT_SUCCESS) {
                mochi_cfg_release_component(aid->component_cfg);
                free(progress_xstreams);
                free(progress_scheds);
                free(aid);
                return ABT_IO_INSTANCE_NULL;
            }
        }
    }

    aid->progress_pool = pool;
    aid->progress_xstreams = progress_xstreams;
    aid->progress_scheds = progress_scheds;

    aid->epoch_start = ABT_get_wtime();
    aid->logging_enabled = 0;
    /* TODO: implement a second key "trace_file" that takes a format specifier
     * similar to valgrind's %p */
    ret = mochi_cfg_get_value_int(aid->component_cfg, "trace_enabled", &aid->logging_enabled);
    if (ret == 0)
            fprintf(aid->logfd, "#Module\tRank\tOp\tSegment\tOffset\tLength\tStart(s)\tEnd(s)\n");
    }

    return aid;
}

abt_io_instance_id abt_io_init(int thread_count)
{
    char cfg_string[64];

    /* NOTE: jansson could be used for more complex encodings, but this one
     * is trivial enough to just do with snprintf()
     */
    snprintf(cfg_string, 64, "{\"abt-io\": {\"thread_count\": %d }}", thread_count);

    return(abt_io_init_json(cfg_string));
}

char* abt_io_get_config(abt_io_instance_id aid)
{
    return(mochi_cfg_emit(aid->component_cfg, "abt-io"));
}

abt_io_instance_id abt_io_init_pool(ABT_pool progress_pool)
{
    struct abt_io_instance *aid;

    aid = malloc(sizeof(*aid));
    if(!aid) return(ABT_IO_INSTANCE_NULL);

    aid->progress_pool = progress_pool;
    aid->progress_xstreams = NULL;
    aid->progress_scheds = NULL;
    aid->num_xstreams = 0;

    return aid;
}

void abt_io_finalize(abt_io_instance_id aid)
{
    int i;

    if (aid->num_xstreams) {
        for (i = 0; i < aid->num_xstreams; i++) {
            ABT_xstream_join(aid->progress_xstreams[i]);
            ABT_xstream_free(&aid->progress_xstreams[i]);
        }
        free(aid->progress_xstreams);
        free(aid->progress_scheds);
        // pool gets implicitly freed
    }

    if(aid->component_cfg)
        mochi_cfg_release_component(aid->component_cfg);

    free(aid);
}

static void abt_io_log(abt_io_instance_id aid, char * op, int64_t offset, int64_t length, double start, double end)
{
    if (!aid->logging_enabled) return;
    fprintf(stderr, "X_ABTIO\t-1\t%s\t-1\t%ld\t%ld\t%f\t%f\n",
            op, offset, length, start-aid->epoch_start, end-aid->epoch_start);
}
struct abt_io_open_state
{
    int *ret;
    const char *pathname;
    int flags;
    mode_t mode;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_open_fn(void *foo)
{
    struct abt_io_open_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = open(state->pathname, state->flags, state->mode);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "open", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_open(abt_io_instance_id aid, abt_io_op_t *op, const char* pathname, int flags, mode_t mode, int *ret)
{
    struct abt_io_open_state state;
    struct abt_io_open_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->pathname = pathname;
    pstate->flags = flags;
    pstate->mode = mode;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_open_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_open(abt_io_instance_id aid, const char* pathname, int flags, mode_t mode)
{
    int ret;
    issue_open(aid, NULL, pathname, flags, mode, &ret);
    return ret;
}

abt_io_op_t* abt_io_open_nb(abt_io_instance_id aid, const char* pathname, int flags, mode_t mode, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_open(aid, op, pathname, flags, mode, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_pread_state
{
    ssize_t *ret;
    int fd;
    void *buf;
    size_t count;
    off_t offset;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_pread_fn(void *foo)
{
    struct abt_io_pread_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = pread(state->fd, state->buf, state->count, state->offset);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "pread", state->offset, state->count, start, ABT_get_wtime());
    return;
}

static int issue_pread(abt_io_instance_id aid, abt_io_op_t *op, int fd, void *buf,
        size_t count, off_t offset, ssize_t *ret)
{
    struct abt_io_pread_state state;
    struct abt_io_pread_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->buf = buf;
    pstate->count = count;
    pstate->offset = offset;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_pread_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_pread(abt_io_instance_id aid, int fd, void *buf, 
        size_t count, off_t offset)
{
    ssize_t ret = -1;
    issue_pread(aid, NULL, fd, buf, count, offset, &ret);
    return ret;
}

abt_io_op_t* abt_io_pread_nb(abt_io_instance_id aid, int fd, void *buf,
        size_t count, off_t offset, ssize_t *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_pread(aid, op, fd, buf, count, offset, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_read_state
{
    ssize_t *ret;
    int fd;
    void *buf;
    size_t count;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_read_fn(void *foo)
{
    double start, end;
    struct abt_io_read_state *state = foo;

    start = ABT_get_wtime();
    *state->ret = read(state->fd, state->buf, state->count);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    end = ABT_get_wtime();
    abt_io_log(state->aid, "read", -1, state->count, start, end);
    return;
}

static int issue_read(abt_io_instance_id aid, abt_io_op_t *op, int fd, void *buf,
        size_t count, ssize_t *ret)
{
    struct abt_io_read_state state;
    struct abt_io_read_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->buf = buf;
    pstate->count = count;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_read_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_read(abt_io_instance_id aid, int fd, void *buf, size_t count)
{
    ssize_t ret = -1;
    issue_read(aid, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_read_nb(abt_io_instance_id aid, int fd, void *buf,
        size_t count, ssize_t *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_read(aid, op, fd, buf, count, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_pwrite_state
{
    ssize_t *ret;
    int fd;
    const void *buf;
    size_t count;
    off_t offset;
    ABT_eventual eventual;
    abt_io_instance_id aid;

};

static void abt_io_pwrite_fn(void *foo)
{
    struct abt_io_pwrite_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = pwrite(state->fd, state->buf, state->count, state->offset);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "pwrite", state->offset, state->count, start, ABT_get_wtime());
    return;
}

static int issue_pwrite(abt_io_instance_id aid, abt_io_op_t *op, int fd, const void *buf,
        size_t count, off_t offset, ssize_t *ret)
{
    struct abt_io_pwrite_state state;
    struct abt_io_pwrite_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->buf = buf;
    pstate->count = count;
    pstate->offset = offset;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_pwrite_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_pwrite(abt_io_instance_id aid, int fd, const void *buf, 
        size_t count, off_t offset)
{
    ssize_t ret = -1;
    issue_pwrite(aid, NULL, fd, buf, count, offset, &ret);
    return ret;
}

abt_io_op_t* abt_io_pwrite_nb(abt_io_instance_id aid, int fd, const void *buf,
        size_t count, off_t offset, ssize_t *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_pwrite(aid, op, fd, buf, count, offset, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_write_state
{
    ssize_t *ret;
    int fd;
    const void *buf;
    size_t count;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_write_fn(void *foo)
{
    struct abt_io_write_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = write(state->fd, state->buf, state->count);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "write", -1, state->count, start, ABT_get_wtime());
    return;
}

static int issue_write(abt_io_instance_id aid, abt_io_op_t *op, int fd, const void *buf,
        size_t count, ssize_t *ret)
{
    struct abt_io_write_state state;
    struct abt_io_write_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->buf = buf;
    pstate->count = count;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_write_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

ssize_t abt_io_write(abt_io_instance_id aid, int fd, const void *buf, size_t count)
{
    ssize_t ret = -1;
    issue_write(aid, NULL, fd, buf, count, &ret);
    return ret;
}

abt_io_op_t* abt_io_write_nb(abt_io_instance_id aid, int fd, const void *buf,
        size_t count, ssize_t *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_write(aid, op, fd, buf, count, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}


struct abt_io_mkostemp_state
{
    int *ret;
    char *tpl;
    int flags;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_mkostemp_fn(void *foo)
{
    struct abt_io_mkostemp_state *state = foo;
    double start = ABT_get_wtime();

#ifdef HAVE_MKOSTEMP
    *state->ret = mkostemp(state->tpl, state->flags);
#else
    *state->ret = mkstemp(state->tpl);
#endif
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "mkostemp", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_mkostemp(abt_io_instance_id aid, abt_io_op_t *op, char* tpl, int flags, int *ret)
{
    struct abt_io_mkostemp_state state;
    struct abt_io_mkostemp_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->tpl = tpl;
    pstate->flags = flags;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_mkostemp_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_mkostemp(abt_io_instance_id aid, char *tpl, int flags)
{
    int ret = -1;
    issue_mkostemp(aid, NULL, tpl, flags, &ret);
    return ret;
}

abt_io_op_t* abt_io_mkostemp_nb(abt_io_instance_id aid, char *tpl, int flags, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_mkostemp(aid, op, tpl, flags, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_unlink_state
{
    int *ret;
    const char *pathname;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_unlink_fn(void *foo)
{
    struct abt_io_unlink_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = unlink(state->pathname);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "unlink", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_unlink(abt_io_instance_id aid, abt_io_op_t *op, const char* pathname, int *ret)
{
    struct abt_io_unlink_state state;
    struct abt_io_unlink_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else
    {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->pathname = pathname;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_unlink_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}


int abt_io_unlink(abt_io_instance_id aid, const char *pathname)
{
    int ret = -1;
    issue_unlink(aid, NULL, pathname, &ret);
    return ret;
}

abt_io_op_t* abt_io_unlink_nb(abt_io_instance_id aid, const char *pathname, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_unlink(aid, op, pathname, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_close_state
{
    int *ret;
    int fd;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_close_fn(void *foo)
{
    struct abt_io_close_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = close(state->fd);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "close", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_close(abt_io_instance_id aid, abt_io_op_t *op, int fd, int *ret)
{
    struct abt_io_close_state state;
    struct abt_io_close_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_close_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

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

abt_io_op_t* abt_io_close_nb(abt_io_instance_id aid, int fd, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_close(aid, op, fd, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_fdatasync_state
{
    int *ret;
    int fd;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_fdatasync_fn(void *foo)
{
    struct abt_io_fdatasync_state *state = foo;
    double start = ABT_get_wtime();

    *state->ret = fdatasync(state->fd);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "sync", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_fdatasync(abt_io_instance_id aid, abt_io_op_t *op, int fd, int *ret)
{
    struct abt_io_fdatasync_state state;
    struct abt_io_fdatasync_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_fdatasync_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

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

abt_io_op_t* abt_io_fdatasync_nb(abt_io_instance_id aid, int fd, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fdatasync(aid, op, fd, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_fallocate_state
{
    int *ret;
    int fd;
    int mode;
    off_t offset;
    off_t len;
    ABT_eventual eventual;
    abt_io_instance_id aid;
};

static void abt_io_fallocate_fn(void *foo)
{
    struct abt_io_fallocate_state *state = foo;
    double start = ABT_get_wtime();

#ifdef HAVE_FALLOCATE
    *state->ret = fallocate(state->fd, state->mode, state->offset, state->len);
    if(*state->ret < 0)
        *state->ret = -errno;
#else
    *state->ret = -ENOSYS;
#endif

    ABT_eventual_set(state->eventual, NULL, 0);
    abt_io_log(state->aid, "fallocate", 0, 0, start, ABT_get_wtime());
    return;
}

static int issue_fallocate(abt_io_instance_id aid, abt_io_op_t *op, int fd, int mode, off_t offset, off_t len, int *ret)
{
    struct abt_io_fallocate_state state;
    struct abt_io_fallocate_state *pstate = NULL;
    int rc;

    if (op == NULL) pstate = &state;
    else {
        pstate = malloc(sizeof(*pstate));
        if (pstate == NULL) { *ret = -ENOMEM; goto err; }
    }

    *ret = -ENOSYS;
    pstate->ret = ret;
    pstate->fd = fd;
    pstate->mode = mode;
    pstate->offset = offset;
    pstate->len = len;
    pstate->eventual = NULL;
    pstate->aid = aid;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(aid->progress_pool, abt_io_fallocate_fn, pstate, NULL);
    if(rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }

    if (op == NULL) {
        rc = ABT_eventual_wait(pstate->eventual, NULL);
        // what error should we use here?
        if (rc != ABT_SUCCESS) { *ret = -EINVAL; goto err; }
    }
    else {
        op->e = pstate->eventual;
        op->state = pstate;
        op->free_fn = free;
    }

    return 0;
err:
    if (pstate->eventual != NULL) ABT_eventual_free(&pstate->eventual);
    if (pstate != NULL && op != NULL) free(pstate);
    return -1;
}

int abt_io_fallocate(abt_io_instance_id aid, int fd, int mode, off_t offset, off_t len)
{
    int ret = -1;
    issue_fallocate(aid, NULL, fd, mode, offset, len, &ret);
    return ret;
}

abt_io_op_t* abt_io_fallocate_nb(abt_io_instance_id aid, int fd, int mode, off_t offset, off_t len, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_fallocate(aid, op, fd, mode, offset, len, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
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
    int ret;
    ret = ABT_pool_get_size(aid->progress_pool, &size);
    if (ret == ABT_SUCCESS)
        return size;
    else
        return -1;
}
