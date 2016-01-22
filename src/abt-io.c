
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

abt_io_instance_id abt_io_init(ABT_pool progress_pool)
{
    struct abt_io_instance *aid;

    aid = malloc(sizeof(*aid));
    if(!aid)
        return(ABT_IO_INSTANCE_NULL);
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
    int ret;
    const char *pathname;
    int flags;
    mode_t mode;
};

static void abt_io_open_fn(void *foo)
{
    struct abt_io_open_state *state = foo;

    state->ret = open(state->pathname, state->flags, state->mode);
    if(state->ret < 0)
        state->ret = -errno;

    return;
}

int abt_io_open(abt_io_instance_id aid, const char* pathname, int flags, mode_t mode)
{
    struct abt_io_open_state state;
    int ret;
    ABT_thread tid;

    state.ret = -ENOSYS;
    state.pathname = pathname;
    state.flags = flags;
    state.mode = mode;

    ret = ABT_thread_create(aid->progress_pool, abt_io_open_fn, &state,
        ABT_THREAD_ATTR_NULL, &tid);
    if(ret != 0)
    {
        return(-EINVAL);
    }

    ABT_thread_join(tid);
    ABT_thread_free(&tid);

    return(state.ret);
}

struct abt_io_pwrite_state
{
    ssize_t ret;
    int fd;
    const void *buf;
    size_t count;
    off_t offset;
};

static void abt_io_pwrite_fn(void *foo)
{
    struct abt_io_pwrite_state *state = foo;

    state->ret = pwrite(state->fd, state->buf, state->count, state->offset);
    if(state->ret < 0)
        state->ret = -errno;

    return;
}


ssize_t abt_io_pwrite(abt_io_instance_id aid, int fd, const void *buf, 
    size_t count, off_t offset)
{
    struct abt_io_pwrite_state state;
    int ret;
    ABT_thread tid;

    state.ret = -ENOSYS;
    state.fd = fd;
    state.buf = buf;
    state.count = count;
    state.offset = offset;

    ret = ABT_thread_create(aid->progress_pool, abt_io_pwrite_fn, &state,
        ABT_THREAD_ATTR_NULL, &tid);
    if(ret != 0)
    {
        return(-EINVAL);
    }

    ABT_thread_join(tid);
    ABT_thread_free(&tid);

    return(state.ret);
}

struct abt_io_mkostemp_state
{
    int ret;
    char *template;
    int flags;
};

static void abt_io_mkostemp_fn(void *foo)
{
    struct abt_io_mkostemp_state *state = foo;

    state->ret = mkostemp(state->template, state->flags);
    if(state->ret < 0)
        state->ret = -errno;

    return;
}

int abt_io_mkostemp(abt_io_instance_id aid, char *template, int flags)
{
    struct abt_io_mkostemp_state state;
    int ret;
    ABT_thread tid;

    state.ret = -ENOSYS;
    state.template = template;
    state.flags = flags;

    ret = ABT_thread_create(aid->progress_pool, abt_io_mkostemp_fn, &state,
        ABT_THREAD_ATTR_NULL, &tid);
    if(ret != 0)
    {
        return(-EINVAL);
    }

    ABT_thread_join(tid);
    ABT_thread_free(&tid);

    return(state.ret);
}

struct abt_io_unlink_state
{
    int ret;
    const char *pathname;
};

static void abt_io_unlink_fn(void *foo)
{
    struct abt_io_unlink_state *state = foo;

    state->ret = unlink(state->pathname);
    if(state->ret < 0)
        state->ret = -errno;

    return;
}

int abt_io_unlink(abt_io_instance_id aid, const char *pathname)
{
    struct abt_io_unlink_state state;
    int ret;
    ABT_thread tid;

    state.ret = -ENOSYS;
    state.pathname = pathname;

    ret = ABT_thread_create(aid->progress_pool, abt_io_unlink_fn, &state,
        ABT_THREAD_ATTR_NULL, &tid);
    if(ret != 0)
    {
        return(-EINVAL);
    }

    ABT_thread_join(tid);
    ABT_thread_free(&tid);

    return(state.ret);
}


