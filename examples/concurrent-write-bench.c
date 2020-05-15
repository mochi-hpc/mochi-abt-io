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

#include "abt-io-config.h"
#include <abt.h>
#include <abt-io.h>

#ifndef HAVE_ODIRECT
#define O_DIRECT 0
#endif

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
    void* buffer;
};

static void write_abt_bench(void *_arg);
static void abt_bench(int buffer_per_thread, unsigned int concurrency, size_t size, 
    double duration, const char* filename, unsigned int* ops_done, double *seconds);
static void abt_bench_nb(int buffer_per_thread, unsigned int concurrency, size_t size, 
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
    void* buffer;
};

static void* write_pthread_bench(void *_arg);
static void pthread_bench( int buffer_per_thread, unsigned int concurrency, size_t size, 
    double duration, const char* filename, unsigned int* ops_done, double *seconds);


static double wtime(void);

int main(int argc, char **argv) 
{
    int ret;
    unsigned abt_ops_done, abt_nb_ops_done, pthread_ops_done;
    double abt_seconds, abt_nb_seconds, pthread_seconds;
    size_t size;
    unsigned int concurrency;
    double duration;
    int buffer_per_thread = 0;
    ABT_sched self_sched;
    ABT_xstream self_xstream;

    ABT_init(argc, argv);

    /* set caller (self) ES to sleep when idle by using SCHED_BASIC_WAIT */
    ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 0, NULL,
        ABT_SCHED_CONFIG_NULL, &self_sched);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_self(&self_xstream);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_set_main_sched(self_xstream, self_sched);
    assert(ret == ABT_SUCCESS);

    if(argc != 6)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file> <buffer_per_thread (0|1)>\n");
        return(-1);
    }

    ret = sscanf(argv[1], "%zu", &size);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file> <buffer_per_thread (0|1)>\n");
        return(-1);
    }

    ret = sscanf(argv[2], "%u", &concurrency);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file> <buffer_per_thread (0|1)>\n");
        return(-1);
    }

    ret = sscanf(argv[3], "%lf", &duration);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file> <buffer_per_thread (0|1)>\n");
        return(-1);
    }

    ret = sscanf(argv[5], "%d", &buffer_per_thread);
    if(ret != 1)
    {
        fprintf(stderr, "Usage: concurrent-write-bench <write_size> <concurrency> <duration> <file> <buffer_per_thread (0|1)>\n");
        return(-1);
    }
    buffer_per_thread = !!buffer_per_thread;

    /* run benchmarks */
    printf("# Running ABT benchmark...\n");
    abt_bench(buffer_per_thread, concurrency, size, duration, argv[4], &abt_ops_done, &abt_seconds);
    printf("# ...ABT benchmark done.\n");

    printf("# Running ABT (nonblocking) benchmark...\n");
    abt_bench_nb(buffer_per_thread, concurrency, size, duration, argv[4], &abt_nb_ops_done, &abt_nb_seconds);
    printf("# ...ABT (nonblocking) benchmark done.\n");

    ABT_finalize();

    sleep(1);

    printf("# Running pthread benchmark...\n");
    pthread_bench(buffer_per_thread, concurrency, size, duration, argv[4], &pthread_ops_done, &pthread_seconds);
    printf("# ...pthread benchmark done.\n");


    /* output */
    printf("#<type>\t<concurrency>\t<write_size>\t<ops>\t<seconds>\t<MiB/s>\n");
    printf("abt\t%u\t%zu\t%u\t%f\t%f\n",
        concurrency, size, abt_ops_done, abt_seconds, 
        ((((double)size*(double)abt_ops_done))/abt_seconds)/(1024.0*1024.0));
    printf("abt_nb\t%u\t%zu\t%u\t%f\t%f\n",
        concurrency, size, abt_nb_ops_done, abt_nb_seconds, 
        ((((double)size*(double)abt_nb_ops_done))/abt_nb_seconds)/(1024.0*1024.0));
    printf("pthread\t%u\t%zu\t%u\t%f\t%f\n",
        concurrency, size, pthread_ops_done, pthread_seconds, 
        ((((double)size*(double)pthread_ops_done))/pthread_seconds)/(1024.0*1024.0));

    return(0);
}

static void abt_bench(int buffer_per_thread, unsigned int concurrency, size_t size, double duration,
    const char *filename, unsigned int* ops_done, double *seconds)
{
    ABT_thread *tid_array = NULL;
    ABT_mutex mutex;
    struct write_abt_arg* args;
    off_t next_offset = 0;
    int ret;
    double end;
    unsigned int i;
    ABT_xstream xstream;
    ABT_pool pool;
    abt_io_instance_id aid;
    int fd;
    void *buffer;
    double start;

    fd = open(filename, O_WRONLY|O_CREAT|O_SYNC|O_DIRECT, S_IWUSR|S_IRUSR);
    if(!fd)
    {
        perror("open");
        assert(0);
    }

    tid_array = malloc(concurrency * sizeof(*tid_array));
    assert(tid_array);

    /* retrieve current pool to use for ULT concurrency */
    ret = ABT_xstream_self(&xstream);
    assert(ret == 0);
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    assert(ret == 0);

    /* initialize abt_io */
    /* NOTE: for now we are going to use the same number of execution streams
     * in the io pool as the desired level of issue concurrency, but this
     * doesn't need to be the case in general.
     */
    aid = abt_io_init(concurrency);
    assert(aid != NULL);

    ABT_mutex_create(&mutex);

    if (buffer_per_thread)
        buffer = NULL;
    else
    {
        ret = posix_memalign(&buffer, 4096, size);
        assert(ret == 0);
        memset(buffer, 0, size);
    }

    args = malloc(concurrency*sizeof(*args));
    assert(args != NULL);

    for (i = 0; i < concurrency; i++)
    {
        args[i].mutex = &mutex;
        args[i].size = size;
        args[i].next_offset = &next_offset;
        args[i].duration = duration;
        args[i].aid = aid;
        args[i].fd = fd;
        if (buffer == NULL)
        {
            ret = posix_memalign(&args[i].buffer, 4096, size);
            assert(ret == 0);
            memset(args[i].buffer, 0, size);
        }
        else
            args[i].buffer = buffer;
    }


    for(i=0; i<concurrency; i++)
    {
        /* create ULTs */
        ret = ABT_thread_create(pool, write_abt_bench, &args[i], ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    for(i=0; i<concurrency; i++)
        ABT_thread_join(tid_array[i]);

    end = wtime();

    for(i=0; i<concurrency; i++)
        ABT_thread_free(&tid_array[i]);

    /* compute min start time */
    start = DBL_MAX;
    for (i = 0; i < concurrency; i++)
        start = args[i].start_time < start ? args[i].start_time : start;

    *seconds = end-start;
    *ops_done = next_offset/size;

    abt_io_finalize(aid);

    ABT_mutex_free(&mutex);
    free(tid_array);

    if(buffer_per_thread)
    {
        for (i = 0; i < concurrency; i++)
            free(args[i].buffer);
    }
    else
        free(buffer);

    free(args);

    close(fd);
    unlink(filename);

    return;
}

static void abt_bench_nb(int buffer_per_thread, unsigned int concurrency, size_t size, double duration,
    const char *filename, unsigned int* ops_done, double *seconds)
{
    int fd;
    off_t next_offset = 0;
    int ret;
    double end;
    unsigned int i;
    abt_io_instance_id aid;
    void **buffers = NULL;
    unsigned int num_buffers = 0;
    double start_time;
    abt_io_op_t **ops;
    ssize_t *wrets;

    fd = open(filename, O_WRONLY|O_CREAT|O_SYNC, S_IWUSR|S_IRUSR);
    if(!fd)
    {
        perror("open");
        assert(0);
    }

    /* initialize abt_io */
    /* NOTE: for now we are going to use the same number of execution streams
     * in the io pool as the desired level of issue concurrency, but this
     * doesn't need to be the case in general.
     */
    aid = abt_io_init(concurrency);
    assert(aid != NULL);

    /* set up buffers */
    num_buffers = buffer_per_thread ? concurrency : 1;
    buffers = malloc(num_buffers*sizeof(*buffers));
    assert(buffers);
    for (i = 0; i < num_buffers; i++) {
        ret = posix_memalign(&buffers[i], 4096, size);
        assert(ret == 0);
        memset(buffers[i], 0, size);
    }

    /* set up async contexts */
    ops = calloc(concurrency, sizeof(*ops));
    assert(ops);
    wrets = malloc(concurrency * sizeof(*wrets));
    assert(wrets);

    /* start the benchmark */
    start_time = wtime();

    /* in the absence of a waitany, just going through one-by-one */
    for(i = 0; ; i = (i+1) % concurrency)
    {
        if (ops[i] != NULL)
        {
            ret = abt_io_op_wait(ops[i]);
            assert(ret == 0 && wrets[i] > 0 && (size_t)wrets[i] == size);
            abt_io_op_free(ops[i]);
        }

        if (wtime() - start_time < duration)
        {
            ops[i] = abt_io_pwrite_nb(aid, fd, buffers[i*buffer_per_thread],
                    size, next_offset, wrets+i);
            assert(ops[i]);
            next_offset += size;
        }
        else if (ops[i] == NULL)
            break;
        else
            ops[i] = NULL;
    }

    end = wtime();

    *seconds = end-start_time;
    *ops_done = next_offset/size;

    abt_io_finalize(aid);

    for (i = 0; i < num_buffers; i++)
        free(buffers[i]);
    free(buffers);

    free(wrets);

    close(fd);
    unlink(filename);

    return;
}

static void pthread_bench(int buffer_per_thread, unsigned int concurrency, size_t size, double duration,
    const char *filename, unsigned int* ops_done, double *seconds)
{
    pthread_t *id_array = NULL;
    pthread_mutex_t mutex;
    struct write_pthread_arg* args;
    off_t next_offset = 0;
    int ret;
    double end;
    unsigned int i;
    int fd;
    void *buffer;
    double start;

    fd = open(filename, O_WRONLY|O_CREAT|O_SYNC, S_IWUSR|S_IRUSR);
    if(!fd)
    {
        perror("open");
        assert(0);
    }

    id_array = malloc(concurrency * sizeof(*id_array));
    assert(id_array);

    pthread_mutex_init(&mutex, NULL);

    if (buffer_per_thread)
        buffer = NULL;
    else
    {
        ret = posix_memalign(&buffer, 4096, size);
        assert(ret == 0);
        memset(buffer, 0, size);
    }

    args = malloc(concurrency * sizeof(*args));
    assert(args != NULL);

    for (i = 0; i < concurrency; i++) {
        args[i].mutex = &mutex;
        args[i].size = size;
        args[i].next_offset = &next_offset;
        args[i].duration = duration;
        args[i].fd = fd;
        if(buffer == NULL)
        {
            ret = posix_memalign(&args[i].buffer, 4096, size);
            assert(ret == 0);
            memset(args[i].buffer, 0, size);
        }
        else
            args[i].buffer = buffer;
    }

    for(i=0; i<concurrency; i++)
    {
        ret = pthread_create(&id_array[i], NULL, write_pthread_bench, &args[i]);
        assert(ret == 0);
    }

    for(i=0; i<concurrency; i++)
    {
        ret = pthread_join(id_array[i], NULL);
        assert(ret == 0);
    }

    end = wtime();

    start = DBL_MAX;
    for (i = 0; i < concurrency; i++)
        start = args[i].start_time < start ? args[i].start_time : start;

    *seconds = end-start;
    *ops_done = next_offset/size;

    pthread_mutex_destroy(&mutex);
    free(id_array);

    if(buffer_per_thread)
    {
        for (i = 0; i < concurrency; i++)
            free(args[i].buffer);
    }
    else
        free(buffer);

    free(args);

    close(fd);
    unlink(filename);

    return;
}

static void write_abt_bench(void *_arg)
{
    struct write_abt_arg* arg = _arg;
    off_t my_offset;
    size_t ret;

    arg->start_time = wtime();
    while((wtime()-arg->start_time) < arg->duration) 
    {
        ABT_mutex_lock(*arg->mutex);
        my_offset = *arg->next_offset;
        (*arg->next_offset) += arg->size;
        ABT_mutex_unlock(*arg->mutex);

        ret = abt_io_pwrite(arg->aid, arg->fd, arg->buffer, arg->size, my_offset);
        assert(ret == arg->size);
    }

    return;
}

static void *write_pthread_bench(void *_arg)
{
    struct write_pthread_arg* arg = _arg;
    off_t my_offset;
    size_t ret;

    arg->start_time = wtime();
    while((wtime()-arg->start_time) < arg->duration) 
    {
        pthread_mutex_lock(arg->mutex);
        my_offset = *arg->next_offset;
        (*arg->next_offset) += arg->size;
        pthread_mutex_unlock(arg->mutex);

        ret = pwrite(arg->fd, arg->buffer, arg->size, my_offset);
        assert(ret == arg->size);
    }

    return(NULL);
}

static double wtime(void)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return((double)t.tv_sec + (double)t.tv_usec / 1000000.0);
}
