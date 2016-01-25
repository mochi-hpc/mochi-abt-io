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

#include <abt.h>
#include <abt-io.h>
#include <abt-snoozer.h>

/* This is a simple benchmark that measures the
 * streaming, concurrent, sequentially-issued write throughput for a 
 * specified number of concurrent operations.  It includes an abt-io version and
 * a pthread version.
 *
 * Both tests use O_DIRECT and O_SYNC if available.
 */

/* abt data types and fn prototypes */
struct write_abt_arg
{
    double start_time;
    size_t size;
    ABT_mutex *mutex;
    off_t *next_offset;
    int fd;
    double duration;
    abt_io_instance_id aid;
};

static void write_abt_bench(void *_arg);
static void abt_bench(int argc, char **argv, unsigned int concurrency, size_t size, 
    double duration, const char* filename, unsigned int* ops_done, double *seconds);

/* pthread data types and fn prototypes */
struct write_pthread_arg
{
    double start_time;
    size_t size;
    pthread_mutex_t *mutex;
    off_t *next_offset;
    int fd;
    double duration;
};

static void* write_pthread_bench(void *_arg);
static void pthread_bench(unsigned int concurrency, size_t size, 
    double duration, const char* filename, unsigned int* ops_done, double *seconds);


static double wtime(void);

int main(int argc, char **argv) 
{
    int ret;
    unsigned abt_ops_done, pthread_ops_done;
    double abt_seconds, pthread_seconds;
    size_t size;
    unsigned int concurrency;
    double duration;

    if(argc != 5)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%zu", &size);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file>\n");
        return(-1);
    }

    ret = sscanf(argv[2], "%u", &concurrency);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file>\n");
        return(-1);
    }

    ret = sscanf(argv[3], "%lf", &duration);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file>\n");
        return(-1);
    }

    /* run benchmarks */
    printf("# Running ABT benchmark...\n");
    abt_bench(argc, argv, concurrency, size, duration, argv[4], &abt_ops_done, &abt_seconds);
    printf("# ...abt benchmark done.\n");

    sleep(1);

    printf("# Running pthread benchmark...\n");
    pthread_bench(concurrency, size, duration, argv[4], &pthread_ops_done, &pthread_seconds);
    printf("# ...pthread benchmark done.\n");


    /* output */
    printf("#<type>\t<concurrency>\t<write_size>\t<ops>\t<seconds>\t<MiB/s>\n");
    printf("abt\t%u\t%zu\t%u\t%f\t%f\n",
        concurrency, size, abt_ops_done, abt_seconds, 
        ((((double)size*(double)abt_ops_done))/abt_seconds)/(1024.0*1024.0));
    printf("pthread\t%u\t%zu\t%u\t%f\t%f\n",
        concurrency, size, pthread_ops_done, pthread_seconds, 
        ((((double)size*(double)pthread_ops_done))/pthread_seconds)/(1024.0*1024.0));

    return(0);
}

static void abt_bench(int argc, char **argv, unsigned int concurrency, size_t size, double duration,
    const char *filename, unsigned int* ops_done, double *seconds)
{
    ABT_thread *tid_array = NULL;
    ABT_mutex mutex;
    struct write_abt_arg arg;
    off_t next_offset = 0;
    int ret;
    double end;
    int i;
    ABT_xstream *progress_xstreams;
    ABT_pool progress_pool;
    ABT_xstream xstream;
    ABT_pool pool;
    abt_io_instance_id aid;

    arg.fd = open(filename, O_WRONLY|O_CREAT|O_DIRECT|O_SYNC, S_IWUSR|S_IRUSR);
    if(!arg.fd)
    {
        perror("open");
        assert(0);
    }

    tid_array = malloc(concurrency * sizeof(*tid_array));
    assert(tid_array);

    progress_xstreams = malloc(concurrency * sizeof(*progress_xstreams));
    assert(progress_xstreams);

    printf("WARNING: this benchmark may have an issue with one core busy spinning, see comments.\n");
    /* set up argobots */
    ret = ABT_init(argc, argv);
    assert(ret == 0);

    /* set primary ES to idle without polling */
    ret = ABT_snoozer_xstream_self_set();
    assert(ret == 0);

    /* create a dedicated ES drive Mercury progress */
    /* NOTE: for now we are going to use the same number of execution streams
     * in the io pool as the desired level of issue concurrency, but this
     * doesn't need to be the case in general.
     */
    ret = ABT_snoozer_xstream_create(concurrency, &progress_pool, progress_xstreams);
    assert(ret == 0);

    /* retrieve current pool to use for ULT concurrency */
    ret = ABT_xstream_self(&xstream);
    assert(ret == 0);
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    assert(ret == 0);

    /* initialize abt_io */
    aid = abt_io_init(progress_pool);
    assert(aid != NULL);

    ABT_mutex_create(&mutex);

    arg.mutex = &mutex;
    arg.size = size;
    arg.next_offset = &next_offset;
    arg.duration = duration;
    arg.aid = aid;

    arg.start_time = wtime();

    for(i=0; i<concurrency; i++)
    {
        /* create ULTs */
        ret = ABT_thread_create(pool, write_abt_bench, &arg, ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    arg.start_time = wtime();

    /* TODO: this is likely to be causing the main thread to busy spin
     * waiting for the ULTs to complete.  Need to confirm.  Can be addressed
     * in Argobots possibly or by working around with an eventual wait here.
     */
    for(i=0; i<concurrency; i++)
        ABT_thread_join(tid_array[i]);

    end = wtime();
 
    for(i=0; i<concurrency; i++)
        ABT_thread_free(&tid_array[i]);
   
    *seconds = end-arg.start_time;
    *ops_done = next_offset/size;

    abt_io_finalize(aid);

    /* wait on the ESs to complete */
    for(i=0; i<4; i++)
    {
        ABT_xstream_join(progress_xstreams[i]);
        ABT_xstream_free(&progress_xstreams[i]);
    }

    ABT_finalize();

    ABT_mutex_free(&mutex);
    free(tid_array);
    free(progress_xstreams);

    close(arg.fd);
    unlink(filename);

    return;
}

static void pthread_bench(unsigned int concurrency, size_t size, double duration,
    const char *filename, unsigned int* ops_done, double *seconds)
{
    pthread_t *id_array = NULL;
    pthread_mutex_t mutex;
    struct write_pthread_arg arg;
    off_t next_offset = 0;
    int ret;
    double end;
    int i;

    arg.fd = open(filename, O_WRONLY|O_CREAT|O_DIRECT|O_SYNC, S_IWUSR|S_IRUSR);
    if(!arg.fd)
    {
        perror("open");
        assert(0);
    }

    id_array = malloc(concurrency * sizeof(*id_array));
    assert(id_array);

    pthread_mutex_init(&mutex, NULL);

    arg.mutex = &mutex;
    arg.size = size;
    arg.next_offset = &next_offset;
    arg.duration = duration;

    arg.start_time = wtime();

    for(i=0; i<concurrency; i++)
    {
        ret = pthread_create(&id_array[i], NULL, write_pthread_bench, &arg);
        assert(ret == 0);
    }

    for(i=0; i<concurrency; i++)
    {
        ret = pthread_join(id_array[i], NULL);
        assert(ret == 0);
    }

    end = wtime();
    
    *seconds = end-arg.start_time;
    *ops_done = next_offset/size;

    pthread_mutex_destroy(&mutex);
    free(id_array);

    close(arg.fd);
    unlink(filename);

    return;
}

static void write_abt_bench(void *_arg)
{
    struct write_abt_arg* arg = _arg;
    off_t my_offset;
    void *buffer;
    size_t ret;

    ret = posix_memalign(&buffer, 4096, arg->size);
    assert(ret == 0);
    memset(buffer, 0, arg->size);

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
    return;
}

static void *write_pthread_bench(void *_arg)
{
    struct write_pthread_arg* arg = _arg;
    off_t my_offset;
    void *buffer;
    size_t ret;

    ret = posix_memalign(&buffer, 4096, arg->size);
    assert(ret == 0);
    memset(buffer, 0, arg->size);

    double now = wtime();
    while((now-arg->start_time) < arg->duration) 
    {
        pthread_mutex_lock(arg->mutex);
        my_offset = *arg->next_offset;
        (*arg->next_offset) += arg->size;
        pthread_mutex_unlock(arg->mutex);

        ret = pwrite(arg->fd, buffer, arg->size, my_offset);
        assert(ret == arg->size);

        now = wtime();
    }

    return(NULL);
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}
