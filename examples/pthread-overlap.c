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
#include <openssl/rand.h>
#include <errno.h>
#include <pthread.h>

#include "abt-io-config.h"

#ifndef HAVE_ODIRECT
#define O_DIRECT 0
#endif

struct worker_pthread_common
{
    int opt_io;
    int opt_compute;
    int opt_unit_size;
    int opt_num_units;
    pthread_cond_t cond;
    pthread_mutex_t mutex;
    int completed;
    int inflight_threads;
};

struct worker_pthread_arg
{
    struct worker_pthread_common *common;
    void* buffer;
};

static void *worker_pthread(void *_arg);
static double wtime(void);

int main(int argc, char **argv) 
{
    int ret;
    double seconds;
    double end, start;
    int i;
    struct worker_pthread_arg *arg_array;
    struct worker_pthread_common common;
    pthread_attr_t attr;
    pthread_t tid;

    if(argc != 6)
    {
        fprintf(stderr, "Usage: pthread-overlap <compute> <io> <unit_size> <num_units> <inflight_threads>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &common.opt_compute);
    assert(ret == 1);
    ret = sscanf(argv[2], "%d", &common.opt_io);
    assert(ret == 1);
    ret = sscanf(argv[3], "%d", &common.opt_unit_size);
    assert(ret == 1);
    assert(common.opt_unit_size % 4096 == 0);
    ret = sscanf(argv[4], "%d", &common.opt_num_units);
    assert(ret == 1);
    ret = sscanf(argv[5], "%d", &common.inflight_threads);
    assert(ret == 1);

    pthread_cond_init(&common.cond, NULL);
    pthread_mutex_init(&common.mutex, NULL);
    common.completed = 0;

    arg_array = malloc(sizeof(*arg_array)*common.opt_num_units);
    assert(arg_array);

    for(i=0; i<common.opt_num_units; i++)
    {
        arg_array[i].common = &common;
        ret = posix_memalign(&arg_array[i].buffer, 4096, common.opt_unit_size);
        assert(ret == 0);
        memset(arg_array[i].buffer, 0, common.opt_unit_size);
    }

    start = wtime();

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    for(i=0; i<common.opt_num_units; i++)
    {
        pthread_mutex_lock(&common.mutex);
        while((i + 1 - common.completed) >= common.inflight_threads)
            pthread_cond_wait(&common.cond, &common.mutex);
        pthread_mutex_unlock(&common.mutex);

        /* create threads */
        ret = pthread_create(&tid, &attr, worker_pthread, &arg_array[i]);
        assert(ret == 0);
    }

    pthread_mutex_lock(&common.mutex);
    while(common.completed < common.opt_num_units)
        pthread_cond_wait(&common.cond, &common.mutex);
    pthread_mutex_unlock(&common.mutex);

    end = wtime();
   
    seconds = end-start;

    pthread_mutex_destroy(&common.mutex);
    pthread_cond_destroy(&common.cond);

    for(i=0; i<common.opt_num_units; i++)
        free(arg_array[i].buffer);
    free(arg_array);

    assert(common.opt_num_units == common.completed);
    printf("#<opt_compute>\t<opt_io>\t<opt_unit_size>\t<opt_num_units>\t<time (s)>\t<bytes/s>\t<ops/s>\n");
    printf("%d\t%d\t%d\t%d\t%f\t%f\t%f\n", common.opt_compute, common.opt_io, 
        common.opt_unit_size, common.opt_num_units, seconds, ((double)common.opt_unit_size* (double)common.opt_num_units)/seconds, (double)common.opt_num_units/seconds);

    return(0);
}

static void *worker_pthread(void *_arg)
{
    struct worker_pthread_arg* arg = _arg;
    struct worker_pthread_common *common = arg->common;
    void *buffer = arg->buffer;
    size_t ret;
    char template[256];
    int fd;

    if(common->opt_compute)
    {
        ret = RAND_bytes(buffer, common->opt_unit_size);
        assert(ret == 1);
    }

    sprintf(template, "./data-XXXXXX");

    if(common->opt_io)
    {
#ifdef HAVE_MKOSTEMP
        fd = mkostemp(template, O_DIRECT|O_SYNC);
#else
        fd = mkstemp(template);
#endif
        if(fd < 0)
        {
            perror("mkostemp");
            fprintf(stderr, "errno: %d\n", errno);
        }
        assert(fd >= 0);

        ret = pwrite(fd, buffer, common->opt_unit_size, 0);
        assert(ret == common->opt_unit_size);

        ret = close(fd);
        assert(ret == 0);

        ret = unlink(template);
        assert(ret == 0);
    }

    pthread_mutex_lock(&common->mutex);
    common->completed++;
    pthread_cond_signal(&common->cond);
    pthread_mutex_unlock(&common->mutex);

    return(NULL);
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

