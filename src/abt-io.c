
/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE

#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <abt.h>
#include <abt-snoozer.h>

#include "abt-io.h"

struct abt_io_instance
{
    /* provided by caller */
    ABT_pool progress_pool;
};

struct abt_io_op
{
    ABT_eventual e;
    void *state;
    void (*free_fn)(void*);
};

abt_io_instance_id abt_io_init(ABT_pool progress_pool)
{
    struct abt_io_instance *aid;

    aid = malloc(sizeof(*aid));
    if(!aid) return(ABT_IO_INSTANCE_NULL);
    memset(aid, 0, sizeof(*aid));

    aid->progress_pool = progress_pool;

    return aid;
}

void abt_io_finalize(abt_io_instance_id aid)
{
    free(aid);
    return;
}

struct abt_io_open_state
{
    int *ret;
    const char *pathname;
    int flags;
    mode_t mode;
    ABT_eventual eventual;
};

static void abt_io_open_fn(void *foo)
{
    struct abt_io_open_state *state = foo;

    *state->ret = open(state->pathname, state->flags, state->mode);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_open(ABT_pool pool, abt_io_op_t *op, const char* pathname, int flags, mode_t mode, int *ret)
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
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_open_fn, pstate, NULL);
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
    issue_open(aid->progress_pool, NULL, pathname, flags, mode, &ret);
    return ret;
}

abt_io_op_t* abt_io_open_nb(abt_io_instance_id aid, const char* pathname, int flags, mode_t mode, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_open(aid->progress_pool, op, pathname, flags, mode, ret);
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
};

static void abt_io_pwrite_fn(void *foo)
{
    struct abt_io_pwrite_state *state = foo;

    *state->ret = pwrite(state->fd, state->buf, state->count, state->offset);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_pwrite(ABT_pool pool, abt_io_op_t *op, int fd, const void *buf,
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
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_pwrite_fn, pstate, NULL);
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
    issue_pwrite(aid->progress_pool, NULL, fd, buf, count, offset, &ret);
    return ret;
}

abt_io_op_t* abt_io_pwrite_nb(abt_io_instance_id aid, int fd, const void *buf,
        size_t count, off_t offset, ssize_t *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_pwrite(aid->progress_pool, op, fd, buf, count, offset, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_mkostemp_state
{
    int *ret;
    char *template;
    int flags;
    ABT_eventual eventual;
};

static void abt_io_mkostemp_fn(void *foo)
{
    struct abt_io_mkostemp_state *state = foo;

    *state->ret = mkostemp(state->template, state->flags);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_mkostemp(ABT_pool pool, abt_io_op_t *op, char* template, int flags, int *ret)
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
    pstate->template = template;
    pstate->flags = flags;
    pstate->eventual = NULL;
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_mkostemp_fn, pstate, NULL);
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

int abt_io_mkostemp(abt_io_instance_id aid, char *template, int flags)
{
    int ret = -1;
    issue_mkostemp(aid->progress_pool, NULL, template, flags, &ret);
    return ret;
}

abt_io_op_t* abt_io_mkostemp_nb(abt_io_instance_id aid, char *template, int flags, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_mkostemp(aid->progress_pool, op, template, flags, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_unlink_state
{
    int *ret;
    const char *pathname;
    ABT_eventual eventual;
};

static void abt_io_unlink_fn(void *foo)
{
    struct abt_io_unlink_state *state = foo;

    *state->ret = unlink(state->pathname);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_unlink(ABT_pool pool, abt_io_op_t *op, const char* pathname, int *ret)
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
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_unlink_fn, pstate, NULL);
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
    issue_unlink(aid->progress_pool, NULL, pathname, &ret);
    return ret;
}

abt_io_op_t* abt_io_unlink_nb(abt_io_instance_id aid, const char *pathname, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_unlink(aid->progress_pool, op, pathname, ret);
    if (iret != 0) { free(op); return NULL; }
    else return op;
}

struct abt_io_close_state
{
    int *ret;
    int fd;
    ABT_eventual eventual;
};

static void abt_io_close_fn(void *foo)
{
    struct abt_io_close_state *state = foo;

    *state->ret = close(state->fd);
    if(*state->ret < 0)
        *state->ret = -errno;

    ABT_eventual_set(state->eventual, NULL, 0);
    return;
}

static int issue_close(ABT_pool pool, abt_io_op_t *op, int fd, int *ret)
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
    rc = ABT_eventual_create(0, &pstate->eventual);
    if (rc != ABT_SUCCESS) { *ret = -ENOMEM; goto err; }

    if (op != NULL) op->e = pstate->eventual;

    rc = ABT_task_create(pool, abt_io_close_fn, pstate, NULL);
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
    issue_close(aid->progress_pool, NULL, fd, &ret);
    return ret;
}

abt_io_op_t* abt_io_close_nb(abt_io_instance_id aid, int fd, int *ret)
{
    abt_io_op_t *op;
    int iret;

    op = malloc(sizeof(*op));
    if (op == NULL) return NULL;

    iret = issue_close(aid->progress_pool, op, fd, ret);
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
