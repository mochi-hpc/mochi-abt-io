/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

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

char* readfile(const char* filename) {
    FILE *f = fopen(filename, "r");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* string = malloc(fsize + 1);
    fread(string, 1, fsize, f);
    fclose(f);
    string[fsize] = 0;
    return string;
}

int main(int argc, char** argv)
{
    abt_io_instance_id aid;
    struct abt_io_init_info args = { 0 };
    args.json_config = argc > 1 ? readfile(argv[1]) : NULL;

    ABT_init(argc, argv);

    /* start up abt-io */
    aid = abt_io_init_ext(&args);
    assert(aid != NULL);

    char* config = abt_io_get_config(aid);
    fprintf(stderr, "----------------------------\n");
    fprintf(stderr, "%s\n", config);
    free(config);

    abt_io_finalize(aid);
    ABT_finalize();

    if(args.json_config)
        free((char*)args.json_config);

    return (0);
}

