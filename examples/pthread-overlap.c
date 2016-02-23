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

#define INFLIGHT_LIMIT 64

struct worker_pthread_arg
{
    int opt_io;
    int opt_compute;
    int opt_unit_size;
    int opt_num_units;
    pthread_cond_t cond;
    pthread_mutex_t mutex;
    int inflight;
    int completed;
    pthread_cond_t done_cond;
    pthread_mutex_t done_mutex;
    int done;
};

static void *worker_pthread(void *_arg);
static double wtime(void);

int main(int argc, char **argv) 
{
    int ret;
    double seconds;
    double end, start;
    int i;
    struct worker_pthread_arg arg;
    pthread_attr_t attr;
    pthread_t tid;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: pthread-overlap <compute> <io> <unit_size> <num_units>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &arg.opt_compute);
    assert(ret == 1);
    ret = sscanf(argv[2], "%d", &arg.opt_io);
    assert(ret == 1);
    ret = sscanf(argv[3], "%d", &arg.opt_unit_size);
    assert(ret == 1);
    assert(arg.opt_unit_size % 4096 == 0);
    ret = sscanf(argv[4], "%d", &arg.opt_num_units);
    assert(ret == 1);

    pthread_cond_init(&arg.cond, NULL);
    pthread_mutex_init(&arg.mutex, NULL);
    pthread_cond_init(&arg.done_cond, NULL);
    pthread_mutex_init(&arg.done_mutex, NULL);

    arg.inflight = 0;
    arg.done = 0;

    start = wtime();

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    for(i=0; i<arg.opt_num_units; i++)
    {
        /* create ULTs */
        ret = pthread_create(&tid, &attr, worker_pthread, &arg);
        assert(ret == 0);
    }

#if 0
    ABT_eventual_wait(arg.eventual, (void**)&done);
#endif

    end = wtime();
   
    seconds = end-start;

#if 0
    /* wait on the compute ESs to complete */
    for(i=0; i<compute_es_count; i++)
    {
        ABT_xstream_join(compute_xstreams[i]);
        ABT_xstream_free(&compute_xstreams[i]);
    }

    ABT_cond_free(&arg.cond);
    ABT_mutex_free(&arg.mutex);
#endif

    printf("#<opt_compute>\t<opt_io>\t<opt_unit_size>\t<opt_num_units>\t<time (s)>\t<bytes/s>\t<ops/s>\n");
    printf("%d\t%d\t%d\t%d\t%f\t%f\t%f\n", arg.opt_compute, arg.opt_io, 
        arg.opt_unit_size, arg.opt_num_units, seconds, ((double)arg.opt_unit_size* (double)arg.opt_num_units)/seconds, (double)arg.opt_num_units/seconds);

    return(0);
}

static void *worker_pthread(void *_arg)
{
    struct worker_pthread_arg* arg = _arg;
    void *buffer;
    size_t ret;
    char template[256];
    int fd;
    int done = 0;

#if 0
    ABT_mutex_lock(arg->mutex);
    while(arg->inflight >= INFLIGHT_LIMIT) 
    {
        ABT_cond_wait(arg->cond, arg->mutex);
    }
    arg->inflight++;
    ABT_mutex_unlock(arg->mutex);

    //fprintf(stderr, "start\n");
    ret = posix_memalign(&buffer, 4096, arg->opt_unit_size);
    assert(ret == 0);
    memset(buffer, 0, arg->opt_unit_size);

    if(arg->opt_compute)
    {
        ret = RAND_bytes(buffer, arg->opt_unit_size);
        assert(ret == 1);
    }

    sprintf(template, "./data-XXXXXX");

    if(arg->opt_io)
    {
        if(arg->opt_abt_io)
        {
            fd = abt_io_mkostemp(arg->aid, template, O_DIRECT|O_SYNC);
            if(fd < 0)
            {
                fprintf(stderr, "abt_io_mkostemp: %d\n", fd);
            }
            assert(fd >= 0);

            ret = abt_io_pwrite(arg->aid, fd, buffer, arg->opt_unit_size, 0);
            assert(ret == arg->opt_unit_size);

            ret = abt_io_close(arg->aid, fd);
            assert(ret == 0);

            ret = abt_io_unlink(arg->aid, template);
            assert(ret == 0);
        }
        else
        {
            fd = mkostemp(template, O_DIRECT|O_SYNC);
            if(fd < 0)
            {
                perror("mkostemp");
                fprintf(stderr, "errno: %d\n", errno);
            }
            assert(fd >= 0);

            ret = pwrite(fd, buffer, arg->opt_unit_size, 0);
            assert(ret == arg->opt_unit_size);

            ret = close(fd);
            assert(ret == 0);

            ret = unlink(template);
            assert(ret == 0);
        }
    }

    free(buffer);
    //fprintf(stderr, "end\n");

    ABT_mutex_lock(arg->mutex);
    arg->inflight--;
    ABT_cond_signal(arg->cond);
    arg->completed++;
    ABT_mutex_unlock(arg->mutex);

    if(arg->completed == arg->opt_num_units)
        ABT_eventual_set(arg->eventual, &done, sizeof(done));
#endif

    return(NULL);
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

