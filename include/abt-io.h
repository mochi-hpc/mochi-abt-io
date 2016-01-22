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

/**
 * Initializes abt_io library.
 * @param [in] progress_pool Argobots pool to drive I/O
 * @returns abt_io instance id on success, NULL upon error
 */
abt_io_instance_id abt_io_init(ABT_pool progress_pool);

/**
 * Shuts down abt_io library and its underlying resources
 * @param [in] aid abt-io instance
 */
void abt_io_finalize(abt_io_instance_id aid);

/** 
 * wrapper for open()
 */
int abt_io_open(abt_io_instance_id aid, const char* pathname, int flags, mode_t mode);

/**
 * wrapper for pwrite()
 */
ssize_t abt_io_pwrite(abt_io_instance_id aid, int fd, const void *buf, size_t count,
    off_t offset);

/**
 * wrapper for mkostemp()
 */
int abt_io_mkostemp(abt_io_instance_id aid, char *template, int flags);

/** 
 * wrapper for unlink()
 */
int abt_io_unlink(abt_io_instance_id aid, const char *pathname);

/**
 * wrapper for close()
 */
int abt_io_close(abt_io_instance_id aid, int fd);

#ifdef __cplusplus
}
#endif

#endif /* __ABT_IO */
