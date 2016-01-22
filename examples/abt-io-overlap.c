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

#include <abt.h>
#include <abt-io.h>
#include <abt-snoozer.h>

struct worker_ult_arg
{
    int opt_abt_io;
    int opt_abt_snoozer;
    int opt_unit_size;
    int opt_num_units;
    abt_io_instance_id aid;
};

static void worker_ult(void *_arg);
static double wtime(void);
static int ABT_nosnoozer_xstream_create(int num_xstreams, ABT_pool *newpool, ABT_xstream *newxstreams);

int main(int argc, char **argv) 
{
    int ret;
    double seconds;
    ABT_thread *tid_array = NULL;
    double end, start;
    int i;
    ABT_xstream *io_xstreams;
    ABT_pool io_pool;
    ABT_xstream *compute_xstreams;
    ABT_pool compute_pool;
    int io_es_count = 4;
    int compute_es_count = 16;
    struct worker_ult_arg arg;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: abt-io-overlap <abt_io 0|1> <abt_snoozer 0|1> <unit_size> <num_units>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%d", &arg.opt_abt_io);
    assert(ret == 1);
    ret = sscanf(argv[2], "%d", &arg.opt_abt_snoozer);
    assert(ret == 1);
    ret = sscanf(argv[3], "%d", &arg.opt_unit_size);
    assert(ret == 1);
    assert(arg.opt_unit_size % 4096 == 0);
    ret = sscanf(argv[4], "%d", &arg.opt_num_units);
    assert(ret == 1);

    tid_array = malloc(arg.opt_num_units * sizeof(*tid_array));
    assert(tid_array);

    io_xstreams = malloc(io_es_count * sizeof(*io_xstreams));
    assert(io_xstreams);

    compute_xstreams = malloc(compute_es_count * sizeof(*compute_xstreams));
    assert(compute_xstreams);

    /* set up argobots */
    ret = ABT_init(argc, argv);
    assert(ret == 0);

    if(arg.opt_abt_snoozer)
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

    if(arg.opt_abt_io)
    {
        if(arg.opt_abt_snoozer)
        {
            /* create dedicated pool drive IO */
            ret = ABT_snoozer_xstream_create(io_es_count, &io_pool, io_xstreams);
            assert(ret == 0);
        }
        else
        {
            assert(0);
        }

        /* initialize abt_io */
        arg.aid = abt_io_init(io_pool);
        assert(arg.aid != NULL);
    }

    start = wtime();

    for(i=0; i<arg.opt_num_units; i++)
    {
        /* create ULTs */
        ret = ABT_thread_create(compute_pool, worker_ult, &arg, ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    for(i=0; i<arg.opt_num_units; i++)
        ABT_thread_join(tid_array[i]);

    end = wtime();
 
    for(i=0; i<arg.opt_num_units; i++)
        ABT_thread_free(&tid_array[i]);
   
    seconds = end-start;

    /* wait on the compute ESs to complete */
    for(i=0; i<compute_es_count; i++)
    {
        ABT_xstream_join(compute_xstreams[i]);
        ABT_xstream_free(&compute_xstreams[i]);
    }

    if(arg.opt_abt_io)
    {
        abt_io_finalize(arg.aid);

        /* wait on IO ESs to complete */
        for(i=0; i<io_es_count; i++)
        {
            ABT_xstream_join(io_xstreams[i]);
            ABT_xstream_free(&io_xstreams[i]);
        }

    }

    ABT_finalize();

    free(tid_array);
    free(io_xstreams);
    free(compute_xstreams);

    printf("#<opt_abt_io>\t<opt_abt_snoozer>\t<opt_unit_size>\t<opt_num_units>\t<time (s)>\t<bytes/s>\t<ops/s>\n");
    printf("%d\t%d\t%d\t%d\t%f\t%f\t%f\n", arg.opt_abt_io, arg.opt_abt_snoozer,
        arg.opt_unit_size, arg.opt_num_units, seconds, ((double)arg.opt_unit_size* (double)arg.opt_num_units)/seconds, (double)arg.opt_num_units/seconds);

    return(0);
}

static void worker_ult(void *_arg)
{
    struct worker_ult_arg* arg = _arg;
    void *buffer;
    size_t ret;
    char template[256];
    int fd;

    //fprintf(stderr, "start\n");
    ret = posix_memalign(&buffer, 4096, arg->opt_unit_size);
    assert(ret == 0);
    memset(buffer, 0, arg->opt_unit_size);

    ret = RAND_bytes(buffer, arg->opt_unit_size);
    assert(ret == 1);

    sprintf(template, "./XXXXXX");

    if(arg->opt_abt_io)
    {
#if 0
        ret = abt_io_pwrite(arg->aid, arg->fd, buffer, arg->size, my_offset);
        assert(ret == arg->size);
#endif

        assert(0);
    }
    else
    {
        fd = mkostemp(template, O_DIRECT|O_SYNC);
        assert(fd >= 0);

        ret = pwrite(fd, buffer, arg->opt_unit_size, 0);
        assert(ret == arg->opt_unit_size);

        ret = unlink(template);
        assert(ret == 0);
    }

    free(buffer);
    //fprintf(stderr, "end\n");

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

