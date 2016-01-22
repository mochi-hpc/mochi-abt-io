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

static void worker_ult(void *_arg);
static double wtime(void);

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
    abt_io_instance_id aid;
    int io_es_count = 4;
    int compute_es_count = 16;
    int target_ops = 100;

    tid_array = malloc(target_ops * sizeof(*tid_array));
    assert(tid_array);

    io_xstreams = malloc(io_es_count * sizeof(*io_xstreams));
    assert(io_xstreams);

    compute_xstreams = malloc(compute_es_count * sizeof(*compute_xstreams));
    assert(compute_xstreams);

    /* set up argobots */
    ret = ABT_init(argc, argv);
    assert(ret == 0);

    /* set primary ES to idle without polling */
    ret = ABT_snoozer_xstream_self_set();
    assert(ret == 0);

    /* create dedicated pool drive IO */
    ret = ABT_snoozer_xstream_create(io_es_count, &io_pool, io_xstreams);
    assert(ret == 0);

    /* create dedicated pool for computatcomputen */
    ret = ABT_snoozer_xstream_create(compute_es_count, &compute_pool, compute_xstreams);
    assert(ret == 0);

    /* initialize abt_io */
    aid = abt_io_init(io_pool);
    assert(aid != NULL);

    start = wtime();

    for(i=0; i<target_ops; i++)
    {
        /* create ULTs */
        ret = ABT_thread_create(compute_pool, worker_ult, NULL, ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    for(i=0; i<target_ops; i++)
        ABT_thread_join(tid_array[i]);

    end = wtime();
 
    for(i=0; i<target_ops; i++)
        ABT_thread_free(&tid_array[i]);
   
    seconds = end-start;

    abt_io_finalize(aid);

    /* wait on the ESs to complete */
    for(i=0; i<io_es_count; i++)
    {
        ABT_xstream_join(io_xstreams[i]);
        ABT_xstream_free(&io_xstreams[i]);
    }

    /* wait on the ESs to complete */
    for(i=0; i<compute_es_count; i++)
    {
        ABT_xstream_join(compute_xstreams[i]);
        ABT_xstream_free(&compute_xstreams[i]);
    }

    ABT_finalize();

    free(tid_array);
    free(io_xstreams);
    free(compute_xstreams);

    return(0);
}

static void worker_ult(void *_arg)
{
    struct write_abt_arg* arg = _arg;
    void *buffer;
    size_t ret;
    int size = 1024*1024*4;

    ret = posix_memalign(&buffer, 4096, size);
    assert(ret == 0);
    memset(buffer, 0, size);

    ret = RAND_bytes(buffer, size);
    assert(ret == 1);

#if 0
    double now = wtime();
    while((now-arg->start_time) < arg->duration) 
    {
        ABT_mutex_lock(*arg->mutex);
        my_offset = *arg->next_offset;
        (*arg->next_offset) += arg->size;
        ABT_mutex_unlock(*arg->mutex);

        ret = abt_io_pwrite(arg->aid, arg->fd, buffer, arg->size, my_offset);
        assert(ret == arg->size);

        now = wtime();
    }
#endif

    free(buffer);

    return;
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}
