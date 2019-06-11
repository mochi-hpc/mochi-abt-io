#define  _GNU_SOURCE

#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <float.h>
#include <errno.h>

#include <abt.h>
#include <abt-io.h>

/* test program to exercise a few basic abt-io functions */

int main(int argc, char **argv) 
{
    int ret;
    ABT_sched self_sched;
    ABT_xstream self_xstream;
    abt_io_instance_id aid;
    int fd;
    char template[64];

    ABT_init(argc, argv);

    if(argc != 1)
    {
        fprintf(stderr, "Error: this program accepts no arguments.\n");
        return(-1);
    }

    /* set caller (self) ES to sleep when idle by using SCHED_BASIC_WAIT */
    ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 0, NULL,
        ABT_SCHED_CONFIG_NULL, &self_sched);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_self(&self_xstream);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_set_main_sched(self_xstream, self_sched);
    assert(ret == ABT_SUCCESS);

    /* start up abt-io */
    aid = abt_io_init(2);
    assert(aid != NULL);

    sprintf(template, "/tmp/XXXXXX");
    fd = abt_io_mkostemp(aid, template, 0);
    assert(ret >= 0);

    ret = abt_io_pwrite(aid, fd, &fd, sizeof(fd), 0);
    assert(ret == sizeof(fd));

    ret = abt_io_fallocate(aid, fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE,
        0, sizeof(fd));
    assert(ret == 0);

    ret = abt_io_close(aid, fd);
    assert(ret == 0);

    ret  = abt_io_unlink(aid, template);
    assert(ret == 0);

    abt_io_finalize(aid);

    ABT_finalize();

    return(0);
}

