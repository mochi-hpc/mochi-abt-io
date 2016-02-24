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

#include <abt.h>
#include <abt-io.h>
#include <abt-snoozer.h>

#define INFLIGHT_LIMIT 64

struct worker_ult_common
{
    int opt_io;
    int opt_compute;
    int opt_abt_io;
    int opt_abt_snoozer;
    int opt_unit_size;
    int opt_num_units;
    abt_io_instance_id aid;
    ABT_cond cond;
    ABT_mutex mutex;
    int completed;
};

struct worker_ult_arg
{
    struct worker_ult_common *common;
    void* buffer;
};

static void worker_ult(void *_arg);
static double wtime(void);
static int ABT_nosnoozer_xstream_create(int num_xstreams, ABT_pool *newpool, ABT_xstream *newxstreams);

int main(int argc, char **argv) 
{
    int ret;
    double seconds;
    double end, start;
    int i;
    ABT_xstream *io_xstreams;
    ABT_pool io_pool;
    ABT_xstream *compute_xstreams;
    ABT_pool compute_pool;
    int io_es_count = -1;
    int compute_es_count = -1;
    struct worker_ult_arg *arg_array;
    struct worker_ult_common common;
    int *done;

    if(argc != 9)
    {
        fprintf(stderr, "Usage: abt-io-overlap <compute> <io> <abt_io 0|1> <abt_snoozer 0|1> <unit_size> <num_units> <compute_es_count> <io_es_count>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &common.opt_compute);
    assert(ret == 1);
    ret = sscanf(argv[2], "%d", &common.opt_io);
    assert(ret == 1);
    ret = sscanf(argv[3], "%d", &common.opt_abt_io);
    assert(ret == 1);
    ret = sscanf(argv[4], "%d", &common.opt_abt_snoozer);
    assert(ret == 1);
    ret = sscanf(argv[5], "%d", &common.opt_unit_size);
    assert(ret == 1);
    assert(common.opt_unit_size % 4096 == 0);
    ret = sscanf(argv[6], "%d", &common.opt_num_units);
    assert(ret == 1);
    ret = sscanf(argv[7], "%d", &compute_es_count);
    assert(ret == 1);
    ret = sscanf(argv[8], "%d", &io_es_count);
    assert(ret == 1);

    io_xstreams = malloc(io_es_count * sizeof(*io_xstreams));
    assert(io_xstreams);

    compute_xstreams = malloc(compute_es_count * sizeof(*compute_xstreams));
    assert(compute_xstreams);

    /* set up argobots */
    ret = ABT_init(argc, argv);
    assert(ret == 0);

    if(common.opt_abt_snoozer)
    {
        /* set primary ES to idle without polling */
        ret = ABT_snoozer_xstream_self_set();
        assert(ret == 0);

        /* create dedicated pool for computation */
        ret = ABT_snoozer_xstream_create(compute_es_count, &compute_pool, compute_xstreams);
        assert(ret == 0);
    }
    else
    {
        ret = ABT_nosnoozer_xstream_create(compute_es_count, &compute_pool, compute_xstreams);
        assert(ret == 0);
    }

    if(common.opt_abt_io)
    {
        if(common.opt_abt_snoozer)
        {
            /* create dedicated pool drive IO */
            ret = ABT_snoozer_xstream_create(io_es_count, &io_pool, io_xstreams);
            assert(ret == 0);
        }
        else
        {
            ret = ABT_nosnoozer_xstream_create(io_es_count, &io_pool, io_xstreams);
            assert(ret == 0);
        }

        /* initialize abt_io */
        common.aid = abt_io_init(io_pool);
        assert(common.aid != NULL);
    }

    ABT_cond_create(&common.cond);
    ABT_mutex_create(&common.mutex);
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

    for(i=0; i<common.opt_num_units; i++)
    {
        ABT_mutex_lock(common.mutex);
        while((i + 1 - common.completed) >= INFLIGHT_LIMIT)
            ABT_cond_wait(common.cond, common.mutex);
        ABT_mutex_unlock(common.mutex);

        /* create ULTs */
        ret = ABT_thread_create(compute_pool, worker_ult, &arg_array[i], ABT_THREAD_ATTR_NULL, NULL);
        assert(ret == 0);
    }

    ABT_mutex_lock(common.mutex);
    while(common.completed < common.opt_num_units)
        ABT_cond_wait(common.cond, common.mutex);
    ABT_mutex_unlock(common.mutex);

    end = wtime();
   
    seconds = end-start;

    /* wait on the compute ESs to complete */
    for(i=0; i<compute_es_count; i++)
    {
        ABT_xstream_join(compute_xstreams[i]);
        ABT_xstream_free(&compute_xstreams[i]);
    }

    if(common.opt_abt_io)
    {
        abt_io_finalize(common.aid);

        /* wait on IO ESs to complete */
        for(i=0; i<io_es_count; i++)
        {
            ABT_xstream_join(io_xstreams[i]);
            ABT_xstream_free(&io_xstreams[i]);
        }

    }

    ABT_cond_free(&common.cond);
    ABT_mutex_free(&common.mutex);

    for(i=0; i<common.opt_num_units; i++)
        free(arg_array[i].buffer);
    free(arg_array);
    
    ABT_finalize();

    free(io_xstreams);
    free(compute_xstreams);

    assert(common.opt_num_units == common.completed);
    printf("#<opt_compute>\t<opt_io>\t<opt_abt_io>\t<opt_abt_snoozer>\t<opt_unit_size>\t<opt_num_units>\t<opt_compute_es_count>\t<opt_io_es_count>\t<time (s)>\t<bytes/s>\t<ops/s>\n");
    printf("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%f\t%f\t%f\n", common.opt_compute, common.opt_io, common.opt_abt_io, common.opt_abt_snoozer,
        common.opt_unit_size, common.opt_num_units, compute_es_count, io_es_count, seconds, ((double)common.opt_unit_size* (double)common.opt_num_units)/seconds, (double)common.opt_num_units/seconds);

    return(0);
}

static void worker_ult(void *_arg)
{
    struct worker_ult_arg* arg = _arg;
    struct worker_ult_common *common = arg->common;
    void *buffer = arg->buffer;
    size_t ret;
    char template[256];
    int fd;
    int done = 0;

    if(common->opt_compute)
    {
        ret = RAND_bytes(buffer, common->opt_unit_size);
        assert(ret == 1);
    }

    sprintf(template, "./data-XXXXXX");

    if(common->opt_io)
    {
        if(common->opt_abt_io)
        {
            fd = abt_io_mkostemp(common->aid, template, O_DIRECT|O_SYNC);
            if(fd < 0)
            {
                fprintf(stderr, "abt_io_mkostemp: %d\n", fd);
            }
            assert(fd >= 0);

            ret = abt_io_pwrite(common->aid, fd, buffer, common->opt_unit_size, 0);
            assert(ret == common->opt_unit_size);

            ret = abt_io_close(common->aid, fd);
            assert(ret == 0);

            ret = abt_io_unlink(common->aid, template);
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

            ret = pwrite(fd, buffer, common->opt_unit_size, 0);
            assert(ret == common->opt_unit_size);

            ret = close(fd);
            assert(ret == 0);

            ret = unlink(template);
            assert(ret == 0);
        }
    }

    ABT_mutex_lock(common->mutex);
    common->completed++;
    ABT_cond_signal(common->cond);
    ABT_mutex_unlock(common->mutex);

    return;
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}

static int ABT_nosnoozer_xstream_create(int num_xstreams, ABT_pool *newpool, ABT_xstream *newxstreams)
{
    ABT_sched *scheds;
    int i;

    scheds = malloc(num_xstreams * sizeof(*scheds));
    if(!scheds)
        return(-1);

    /* Create a shared pool */
    ABT_pool_create_basic(ABT_POOL_FIFO, ABT_POOL_ACCESS_MPMC,
        ABT_TRUE, newpool);

    /* create schedulers */
    for (i = 0; i < num_xstreams; i++) {
        ABT_sched_create_basic(ABT_SCHED_DEFAULT, 1, newpool,
            ABT_SCHED_CONFIG_NULL, &scheds[i]);
    }
    
    /* create ESs */
    for (i = 0; i < num_xstreams; i++) {
        ABT_xstream_create(scheds[i], &newxstreams[i]);
    }

    free(scheds);

    return(0);
}

