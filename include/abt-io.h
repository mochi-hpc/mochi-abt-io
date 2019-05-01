/*
 * (C) 2015 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __ABT_IO
#define __ABT_IO

#ifdef __cplusplus
extern "C" {
#endif

#include <abt.h>
#include <sys/types.h>
#include <stdlib.h>

struct abt_io_instance;
typedef struct abt_io_instance* abt_io_instance_id;

#define ABT_IO_INSTANCE_NULL ((abt_io_instance_id)NULL)

struct abt_io_op;
typedef struct abt_io_op abt_io_op_t;


/**
 * Initializes abt_io library, using the specified number of backing threads. A
 * count of zero currently indicates that concurrent I/O progress is not made
 * unless control is passed to blocking abt-io calls (or other blocking calls
 * w.r.t. argobots).
 * @param [in] progress_pool Argobots pool to drive I/O
 * @returns abt_io instance id on success, NULL upon error
 */
abt_io_instance_id abt_io_init(int backing_thread_count);

/**
 * Initializes abt_io library using the specified Argobots pool for operation
 * dispatch.
 * @param [in] progress_pool Argobots pool to drive I/O
 * @returns abt_io instance id on success, NULL upon error
 */
abt_io_instance_id abt_io_init_pool(ABT_pool progress_pool);

/**
 * Shuts down abt_io library and its underlying resources. Waits for underlying
 * operations to complete in the case abt_io_init was called, otherwise returns
 * immediately.
 * @param [in] aid abt-io instance
 */
void abt_io_finalize(abt_io_instance_id aid);

/** 
 * wrapper for open()
 */
int abt_io_open(
        abt_io_instance_id aid,
        const char* pathname,
        int flags,
        mode_t mode);

/** 
 * non-blocking wrapper for open()
 */
abt_io_op_t* abt_io_open_nb(
        abt_io_instance_id aid,
        const char* pathname,
        int flags,
        mode_t mode,
        int *ret);

/**
 * wrapper for pwrite()
 */
ssize_t abt_io_pwrite(
        abt_io_instance_id aid,
        int fd,
        const void *buf,
        size_t count,
        off_t offset);

/**
 * non-blocking wrapper for pwrite()
 */
abt_io_op_t* abt_io_pwrite_nb(
        abt_io_instance_id aid,
        int fd,
        const void *buf,
        size_t count,
        off_t offset,
        ssize_t *ret);

/**
 * wrapper for write()
 */
ssize_t abt_io_write(
        abt_io_instance_id aid,
        int fd,
        const void *buf,
        size_t count);

/**
 * non-blocking wrapper for write()
 */
abt_io_op_t* abt_io_write_nb(
        abt_io_instance_id aid,
        int fd,
        const void *buf,
        size_t count,
        ssize_t *ret);

/**
 * wrapper for pread()
 */
ssize_t abt_io_pread(
        abt_io_instance_id aid,
        int fd,
        void *buf,
        size_t count,
        off_t offset);

/**
 * non-blocking wrapper for pread()
 */
abt_io_op_t* abt_io_pread_nb(
        abt_io_instance_id aid,
        int fd,
        void *buf,
        size_t count,
        off_t offset,
        ssize_t *ret);

/**
 * wrapper for read()
 */
ssize_t abt_io_read(
        abt_io_instance_id aid,
        int fd,
        void *buf,
        size_t count);

/**
 * non-blocking wrapper for read()
 */
abt_io_op_t* abt_io_read_nb(
        abt_io_instance_id aid,
        int fd,
        void *buf,
        size_t count,
        ssize_t *ret);

/**
 * wrapper for mkostemp()
 */
int abt_io_mkostemp(abt_io_instance_id aid, char *tpl, int flags);

/**
 * non-blocking wrapper for mkostemp()
 */
abt_io_op_t* abt_io_mkostemp_nb(
        abt_io_instance_id aid,
        char *tpl,
        int flags,
        int *ret);

/** 
 * wrapper for unlink()
 */
int abt_io_unlink(abt_io_instance_id aid, const char *pathname);

/** 
 * non-blocking wrapper for unlink()
 */
abt_io_op_t* abt_io_unlink_nb(
        abt_io_instance_id aid,
        const char *pathname,
        int *ret);

/**
 * wrapper for close()
 */
int abt_io_close(abt_io_instance_id aid, int fd);

/**
 * non-blocking wrapper for close()
 */
abt_io_op_t* abt_io_close_nb(abt_io_instance_id aid, int fd, int *ret);

/**
 * wait on an abt-io operation
 * return: 0 if success, non-zero on failure
 */
int abt_io_op_wait(abt_io_op_t* op);

/**
 * release resources comprising the op. DO NOT call until the op has been
 * successfully waited on
 */
void abt_io_op_free(abt_io_op_t* op);

#ifdef __cplusplus
}
#endif

#endif /* __ABT_IO */
