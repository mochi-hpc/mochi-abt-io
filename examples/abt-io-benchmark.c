/*
 * Copyright (c) 2023 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#define _GNU_SOURCE
#include "abt-io-config.h"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <zlib.h>
#include <float.h>

#include <json-c/json.h>

#include <abt.h>
#include <abt-io.h>

#include "src/abt-io-macros.h"

/* record up to 8 million (power of 2) samples per thread. This will take 64 MiB
 * of RAM per thread.
 */
#define MAX_SAMPLES_PER_THREAD (8 * 1024 * 1024)

/* benchmark types to perform */
#define BENCHMARK_OP_WRITE 1
#define BENCHMARK_OP_READ  2

struct options {
    char json_file[256];
    char output_file[256];
};

struct sample_statistics {
    double min;
    double q1;
    double median;
    double q3;
    double max;
    double mean;
};

/* abt data types and fn prototypes */
struct abt_thread_arg {
    int                benchmark_op;
    double             start_time;
    double             end_time;
    size_t             access_size_bytes;
    ABT_mutex*         mutex;
    ABT_barrier*       barrier;
    off_t*             global_next_offset;
    int                fd;
    double             duration_seconds;
    double             elapsed_seconds;
    abt_io_instance_id aid;
    int                ops_done;
    int                unique_files_flag;
    double*            samples;
};

static void abt_thread_fn(void* _arg);

static void abt_bench(int                benchmark_op,
                      abt_io_instance_id aid,
                      unsigned int       concurrency,
                      size_t             access_size_bytes,
                      double             duration_seconds,
                      const char*        data_file_name,
                      int                open_flags,
                      int                unique_files_flag,
                      int                fallocate_flag,
                      unsigned int*      ops_done,
                      double*            elapsed_seconds,
                      double*            samples);

static int  parse_args(int                  argc,
                       char**               argv,
                       struct options*      opts,
                       struct json_object** json_cfg);
static void usage(void);
static int  sample_compare(const void* p1, const void* p2);

int main(int argc, char** argv)
{
    int                      ret;
    struct options           opts;
    struct json_object*      json_cfg;
    int                      duration_seconds;
    const char*              data_file_name;
    int                      concurrency;
    int                      access_size_bytes;
    double*                  samples;
    int                      sample_index = 0;
    gzFile                   f            = NULL;
    int                      i, j;
    int                      trace_flag        = 0;
    int                      benchmark_op      = BENCHMARK_OP_WRITE;
    const char*              benchmark_op_str  = NULL;
    int                      open_flags        = 0;
    int                      unique_files_flag = 0;
    int                      fallocate_flag    = 0;
    struct sample_statistics stats             = {0};
    struct json_object*      abt_io_config     = NULL;
    struct abt_io_init_info  aii               = {0};
    abt_io_instance_id       aid               = ABT_IO_INSTANCE_NULL;
    char*                    cli_cfg_str       = NULL;
    ABT_sched                self_sched;
    ABT_xstream              self_xstream;
    int                      json_idx = 0;
    struct json_object*      json_obj = NULL;
    unsigned int             ops_done;
    double                   elapsed_seconds;
    long unsigned            total_bytes;
    double                   this_ts;

    ret = parse_args(argc, argv, &opts, &json_cfg);
    if (ret < 0) {
        usage();
        exit(EXIT_FAILURE);
    }

    ABT_init(argc, argv);

    /* set caller (self) ES to sleep when idle by using SCHED_BASIC_WAIT */
    /* NOTE: this is handled automatically by Margo in most Mochi programs */
    ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 0, NULL,
                                 ABT_SCHED_CONFIG_NULL, &self_sched);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_self(&self_xstream);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_set_main_sched(self_xstream, self_sched);
    assert(ret == ABT_SUCCESS);

    /* If there is an "abt-io" section in the json configuration, then
     * serialize it into a string to pass to abt_io_init_ext().
     */
    abt_io_config = json_object_object_get(json_cfg, "abt-io");
    if (abt_io_config)
        aii.json_config = json_object_to_json_string_ext(
            abt_io_config, JSON_C_TO_STRING_PLAIN);
    aid = abt_io_init_ext(&aii);
    if (!aid) {
        fprintf(stderr, "Error: failed to initialize abt-io.\n");
        ret = -1;
        goto err_cleanup;
    }

    duration_seconds = json_object_get_int(
        json_object_object_get(json_cfg, "duration_seconds"));
    concurrency
        = json_object_get_int(json_object_object_get(json_cfg, "concurrency"));
    access_size_bytes = json_object_get_int(
        json_object_object_get(json_cfg, "access_size_bytes"));
    data_file_name = json_object_get_string(
        json_object_object_get(json_cfg, "data_file_name"));
    trace_flag
        = json_object_get_boolean(json_object_object_get(json_cfg, "trace"));
    fallocate_flag = json_object_get_boolean(
        json_object_object_get(json_cfg, "fallocate"));
    benchmark_op_str = json_object_get_string(
        json_object_object_get(json_cfg, "benchmark_op"));
    if (!strcmp(benchmark_op_str, "write")) {
        benchmark_op = BENCHMARK_OP_WRITE;
        open_flags   = O_WRONLY | O_CREAT;
    } else if (!strcmp(benchmark_op_str, "read")) {
        benchmark_op = BENCHMARK_OP_READ;
        /* note that we can't open read only; the benchmark will create its
         * own file and fallocate it so it must be writeable
         */
        open_flags = O_RDWR | O_CREAT;
        if (!fallocate_flag) {
            fprintf(stderr,
                    "Error: \"benchmark_op\":\"read\" requires that "
                    "\"fallocate\":true also be set.\n");
            goto err_cleanup;
        }
    } else {
        fprintf(stderr, "Error: unknown benchmark_op specified: \"%s\"\n",
                benchmark_op_str);
        goto err_cleanup;
    }
    unique_files_flag = json_object_get_boolean(
        json_object_object_get(json_cfg, "unique_files"));
    fallocate_flag = json_object_get_boolean(
        json_object_object_get(json_cfg, "fallocate"));
    json_array_foreach(json_object_object_get(json_cfg, "open_flags"), json_idx,
                       json_obj)
    {
        const char* flag = json_object_get_string(json_obj);
        if (!strcmp(flag, "O_DIRECT")) {
#ifndef HAVE_ODIRECT
            printf(
                "# WARNING: O_DIRECT not supported on this platform.  Disabled "
                "for this test.\n");
#else
            open_flags |= O_DIRECT;
#endif
        } else if (!strcmp(flag, "O_SYNC"))
            open_flags |= O_SYNC;
        else {
            fprintf(stderr, "Error: open_flag %s not supported by benchmark.\n",
                    flag);
        }
    }

    /* allocate with mmap rather than malloc just so we can use the
     * MAP_POPULATE flag to get the paging out of the way before we start
     * measurements
     */
    samples = mmap(NULL, MAX_SAMPLES_PER_THREAD * concurrency * sizeof(double),
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, 0, 0);
    if (!samples) {
        perror("mmap");
        ret = -1;
        goto err_cleanup;
    }

    abt_bench(benchmark_op, aid, concurrency, access_size_bytes,
              duration_seconds, data_file_name, open_flags, unique_files_flag,
              fallocate_flag, &ops_done, &elapsed_seconds, samples);

    /* store results */
    f = gzopen(opts.output_file, "w");
    if (!f) {
        fprintf(stderr, "Error opening %s\n", opts.output_file);
        goto err_cleanup;
    }

    /* report configuration */
    struct json_tokener*    tokener;
    enum json_tokener_error jerr;

    /* retrieve local abt-io configuration */
    cli_cfg_str = abt_io_get_config(aid);

    /* parse abt-io config and injected into the benchmark config */
    tokener = json_tokener_new();
    abt_io_config
        = json_tokener_parse_ex(tokener, cli_cfg_str, strlen(cli_cfg_str));
    if (!abt_io_config) {
        jerr = json_tokener_get_error(tokener);
        fprintf(stderr, "JSON parse error: %s\n",
                json_tokener_error_desc(jerr));
        json_tokener_free(tokener);
        return -1;
    }
    json_tokener_free(tokener);
    /* delete existing abt-io object, if present */
    json_object_object_del(json_cfg, "abt-io");
    /* add new one, derived at run time */
    json_object_object_add(json_cfg, "abt-io", abt_io_config);

    gzprintf(f, "\"abt-io-benchmark\" : %s\n",
             json_object_to_json_string_ext(
                 json_cfg,
                 JSON_C_TO_STRING_PRETTY | JSON_C_TO_STRING_NOSLASHESCAPE));

    /* if requested, report every sample */
    if (trace_flag) {
        gzprintf(f, "# sample_trace\t<start>\t<end>\t<elapsed>\n");
        for (j = 0; j < concurrency; j++) {
            this_ts = 0;
            for (i = j * MAX_SAMPLES_PER_THREAD;
                 i < j * MAX_SAMPLES_PER_THREAD + MAX_SAMPLES_PER_THREAD
                 && samples[i] != 0;
                 i++) {
                gzprintf(f, "sample_trace\t%f\t%f\t%f\n", this_ts,
                         (this_ts + samples[i]), samples[i]);
                this_ts += samples[i];
            }
        }
    }

    /* now that samples have been written out individually in the order they
     * occurred (if requested), we can locally sort and generate some
     * statistics
     */
    qsort(samples, MAX_SAMPLES_PER_THREAD * concurrency, sizeof(double),
          sample_compare);
    /* sample_compare will put all zero samples at the end so that we can
     * truncate them off.  Walk backwards to find the index of the last
     * non-zero value.
     */
    sample_index = MAX_SAMPLES_PER_THREAD * concurrency - 1;
    while (sample_index >= 0 && samples[sample_index] == 0) sample_index--;

    /* there should be a lot of samples; we aren't going to bother
     * interpolating between points if there isn't a precise sample for the
     * medians or quartiles
     */
    stats.min    = samples[0];
    stats.q1     = samples[sample_index / 4];
    stats.median = samples[sample_index / 2];
    stats.q3     = samples[3 * (sample_index / 4)];
    stats.max    = samples[sample_index - 1];
    for (i = 0; i < sample_index; i++) stats.mean += samples[i];
    stats.mean /= (double)sample_index;
    gzprintf(f, "# sample_stats\t<min>\t<q1>\t<median>\t<q3>\t<max>\t<mean>\n");
    gzprintf(f, "sample_stats\t%f\t%f\t%f\t%f\t%f\t%f\n", stats.min, stats.q1,
             stats.median, stats.q3, stats.max, stats.mean);

    /* calculate aggregate throughput */
    total_bytes = ops_done;
    total_bytes *= access_size_bytes;
    gzprintf(f,
             "# "
             "aggregate_stats\t<total_ops>\t<total_seconds>\t<total_ops/"
             "s>\t<total_bytes>\t<total_bytes/s>\t<total_MiB/s>\n");
    gzprintf(f, "aggregate_stats\t%d\t%f\t%f\t%lu\t%f\t%f\n", ops_done,
             elapsed_seconds, (double)ops_done / elapsed_seconds, total_bytes,
             (double)total_bytes / elapsed_seconds,
             (double)total_bytes / (elapsed_seconds * 1024.0 * 1024.0));

    if (f) {
        gzclose(f);
        f = NULL;
    }

err_cleanup:
    if (cli_cfg_str) free(cli_cfg_str);
    if (aid) abt_io_finalize(aid);
    if (f) gzclose(f);
    if (samples)
        munmap(samples, MAX_SAMPLES_PER_THREAD * concurrency * sizeof(double));
    if (json_cfg) json_object_put(json_cfg);
    ABT_finalize();

    return ret;
}

static int parse_json(const char* json_file, struct json_object** json_cfg)
{
    struct json_tokener*    tokener;
    enum json_tokener_error jerr;
    char*                   json_cfg_str = NULL;
    FILE*                   f;
    long                    fsize;
    struct json_object*     val;
    struct json_object*     array;

    /* open json file */
    f = fopen(json_file, "r");
    if (!f) {
        perror("fopen");
        fprintf(stderr, "Error: could not open json file %s\n", json_file);
        return (-1);
    }

    /* check size */
    fseek(f, 0, SEEK_END);
    fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    /* allocate space to hold contents and read it in */
    json_cfg_str = malloc(fsize + 1);
    if (!json_cfg_str) {
        perror("malloc");
        return (-1);
    }
    fread(json_cfg_str, 1, fsize, f);
    fclose(f);
    json_cfg_str[fsize] = 0;

    /* parse json */
    tokener = json_tokener_new();
    *json_cfg
        = json_tokener_parse_ex(tokener, json_cfg_str, strlen(json_cfg_str));
    if (!(*json_cfg)) {
        jerr = json_tokener_get_error(tokener);
        fprintf(stderr, "JSON parse error: %s\n",
                json_tokener_error_desc(jerr));
        json_tokener_free(tokener);
        free(json_cfg_str);
        return -1;
    }
    json_tokener_free(tokener);
    free(json_cfg_str);

    /* set defaults if not present */
    CONFIG_HAS_OR_CREATE(*json_cfg, int, "duration_seconds", 10, val);
    CONFIG_HAS_OR_CREATE(*json_cfg, string, "data_file_name", "/tmp/foo.dat",
                         val);
    CONFIG_HAS_OR_CREATE(*json_cfg, int, "concurrency", 16, val);
    CONFIG_HAS_OR_CREATE(*json_cfg, int, "access_size_bytes", 4096, val);
    CONFIG_HAS_OR_CREATE(*json_cfg, boolean, "trace", 1, val);
    CONFIG_HAS_OR_CREATE(*json_cfg, string, "benchmark_op", "write", val);
    CONFIG_HAS_OR_CREATE(*json_cfg, boolean, "unique_files", 1, val);
    CONFIG_HAS_OR_CREATE(*json_cfg, boolean, "fallocate", 1, val);
    array = json_object_object_get(*json_cfg, "open_flags");
    if (array && !json_object_is_type(array, json_type_array)) {
        fprintf(
            stderr,
            "Error: \"open_flags\" is in configuration but is not an array.\n");
        return (-1);
    } else if (!array) {
        /* create array with default flags */
        array = json_object_new_array();
        json_object_object_add(*json_cfg, "open_flags", array);
        json_object_array_add(array, json_object_new_string("O_DIRECT"));
        json_object_array_add(array, json_object_new_string("O_SYNC"));
    }

    return (0);
}

static int parse_args(int                  argc,
                      char**               argv,
                      struct options*      opts,
                      struct json_object** json_cfg)
{
    int opt;
    int ret;

    memset(opts, 0, sizeof(*opts));

    while ((opt = getopt(argc, argv, "j:o:")) != -1) {
        switch (opt) {
        case 'j':
            ret = sscanf(optarg, "%s", opts->json_file);
            if (ret != 1) return (-1);
            break;
        case 'o':
            ret = sscanf(optarg, "%s", opts->output_file);
            if (ret != 1) return (-1);
            break;
        default:
            return (-1);
        }
    }

    if (strlen(opts->json_file) == 0) return (-1);
    if (strlen(opts->output_file) == 0) return (-1);

    /* add .gz on to the output file name if it isn't already there */
    if ((strlen(opts->output_file) < 3)
        || (strcmp(".gz", &opts->output_file[strlen(opts->output_file) - 3])
            != 0)) {
        strcat(opts->output_file, ".gz");
    }

    return (parse_json(opts->json_file, json_cfg));
}

static void usage(void)
{
    fprintf(stderr,
            "Usage: "
            "abt-io-benchmark -j <configuration json> -o "
            "<output file>\n");
    return;
}

static int sample_compare(const void* p1, const void* p2)
{
    double d1 = *((double*)p1);
    double d2 = *((double*)p2);

    /* push zero values to the back */
    if (d1 == 0) return 1;
    if (d2 == 0) return -1;

    if (d1 > d2) return 1;
    if (d1 < d2) return -1;
    return 0;
}

static void abt_bench(int                benchmark_op,
                      abt_io_instance_id aid,
                      unsigned int       concurrency,
                      size_t             access_size_bytes,
                      double             duration_seconds,
                      const char*        data_file_name,
                      int                open_flags,
                      int                unique_files_flag,
                      int                fallocate_flag,
                      unsigned int*      ops_done,
                      double*            elapsed_seconds,
                      double*            samples)
{
    ABT_thread*            tid_array = NULL;
    ABT_mutex              mutex;
    struct abt_thread_arg* args;
    off_t                  global_next_offset = 0;
    int                    ret;
    double                 end;
    unsigned int           i;
    ABT_xstream            xstream;
    ABT_pool               pool;
    double                 start;
    char                   filename[256] = {0};
    ABT_barrier            barrier;

    tid_array = malloc(concurrency * sizeof(*tid_array));
    assert(tid_array);

    /* retrieve current pool to use for ULT concurrency */
    ret = ABT_xstream_self(&xstream);
    assert(ret == 0);
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    assert(ret == 0);

    ABT_mutex_create(&mutex);
    ABT_barrier_create(concurrency, &barrier);

    args = calloc(concurrency, sizeof(*args));
    assert(args != NULL);

    /* open file(s) up front */
    for (i = 0; i < concurrency; i++) {
        if (unique_files_flag)
            snprintf(filename, 128, "%s.%d", data_file_name, i);
        else
            snprintf(filename, 128, "%s", data_file_name);
        if (unique_files_flag || i == 0) {
            args[i].fd = open(filename, open_flags, S_IWUSR | S_IRUSR);
            if (args[i].fd < 0) {
                perror("open");
                assert(0);
            }
            if (fallocate_flag) {
#ifdef HAVE_FALLOCATE
                ret = fallocate(args[i].fd, 0, 0, 10737418240UL);
                assert(ret == 0);
#else
                printf(
                    "# WARNING: fallocate not supported on this platform.  "
                    "Disabled "
                    "for this test.\n");
#endif
            }
        } else
            args[i].fd = args[0].fd;
    }

    for (i = 0; i < concurrency; i++) {
        args[i].benchmark_op       = benchmark_op;
        args[i].mutex              = &mutex;
        args[i].barrier            = &barrier;
        args[i].access_size_bytes  = access_size_bytes;
        args[i].global_next_offset = &global_next_offset;
        args[i].duration_seconds   = duration_seconds;
        args[i].aid                = aid;
        args[i].unique_files_flag  = unique_files_flag;
        /* let each thread record its own sample space */
        args[i].samples = &samples[MAX_SAMPLES_PER_THREAD * i];
    }

    for (i = 0; i < concurrency; i++) {
        /* create ULTs */
        ret = ABT_thread_create(pool, abt_thread_fn, &args[i],
                                ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    for (i = 0; i < concurrency; i++) ABT_thread_join(tid_array[i]);
    for (i = 0; i < concurrency; i++) ABT_thread_free(&tid_array[i]);

    /* compute min start time, max end time, and cumulative op count */
    start     = DBL_MAX;
    end       = 0;
    *ops_done = 0;
    for (i = 0; i < concurrency; i++) {
        start = args[i].start_time < start ? args[i].start_time : start;
        end   = args[i].end_time > end ? args[i].end_time : end;
        *ops_done += args[i].ops_done;
    }

    *elapsed_seconds = end - start;

    /* close and unlink data files */
    for (i = 0; i < concurrency; i++) {
        if (unique_files_flag)
            snprintf(filename, 128, "%s.%d", data_file_name, i);
        else
            snprintf(filename, 128, "%s", data_file_name);
        if (unique_files_flag || i == 0) {
            close(args[i].fd);
            unlink(filename);
        }
    }

    ABT_mutex_free(&mutex);
    ABT_barrier_free(&barrier);
    free(tid_array);
    free(args);

    return;
}

static void abt_thread_fn(void* _arg)
{
    struct abt_thread_arg* arg       = _arg;
    off_t                  my_offset = 0;
    size_t                 ret;
    void*                  buffer;
    double                 prev_ts, this_ts;

    ret = posix_memalign(&buffer, 4096, arg->access_size_bytes);
    assert(ret == 0);
    memset(buffer, 0, arg->access_size_bytes);

    /* wait until all ULTs are ready */
    ABT_barrier_wait(*arg->barrier);

    arg->start_time = ABT_get_wtime();
    do {
        if (!arg->unique_files_flag) {
            /* if we are sharing a file (and file descriptor) then we have
             * to retrieve a unique offset
             */
            ABT_mutex_lock(*arg->mutex);
            my_offset = *arg->global_next_offset;
            (*arg->global_next_offset) += arg->access_size_bytes;
            ABT_mutex_unlock(*arg->mutex);
        }
        if (arg->benchmark_op == BENCHMARK_OP_WRITE) {
            ret = abt_io_pwrite(arg->aid, arg->fd, buffer,
                                arg->access_size_bytes, my_offset);
            assert(ret == arg->access_size_bytes);
        } else if (arg->benchmark_op == BENCHMARK_OP_READ) {
            ret = abt_io_pread(arg->aid, arg->fd, buffer,
                               arg->access_size_bytes, my_offset);
            assert(ret == arg->access_size_bytes || ret == 0);
            if (ret == 0) {
                /* We hit EOF. End benchmark here. */
                printf("# Warning: read benchmark hit EOF; stopping early.\n");
                break;
            }
        } else {
            fprintf(stderr, "Error: invalid benchmark_op.\n");
            assert(0);
        }
        this_ts = ABT_get_wtime() - arg->start_time;
        if (arg->ops_done < MAX_SAMPLES_PER_THREAD)
            arg->samples[arg->ops_done] = this_ts - prev_ts;
        prev_ts = this_ts;
        arg->ops_done++;

        if (arg->unique_files_flag) {
            /* track our own local offset, bump for next time */
            my_offset += arg->access_size_bytes;
        }
    } while (this_ts < arg->duration_seconds);

    arg->end_time = ABT_get_wtime();

    free(buffer);

    return;
}

