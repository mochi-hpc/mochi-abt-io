#include <stdio.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
//#include <sys/statfs.h>
#include <fcntl.h>
#include <float.h>
#include <errno.h>

#include <abt.h>
#include <abt-io.h>
#include <abt-io-config.h>

char* readfile(const char* filename) {
    FILE *f = fopen(filename, "r");
    int ret;
    if(!f) {
        perror("fopen");
        fprintf(stderr, "\tCould not open json file \"%s\"\n", filename);
        exit(EXIT_FAILURE);
    }
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* string = malloc(fsize + 1);
    ret = fread(string, 1, fsize, f);
    if(ret < 0) {
        perror("fread");
        fprintf(stderr, "\tCould not read json file \"%s\"\n", filename);
        exit(EXIT_FAILURE);
    }
    fclose(f);
    string[fsize] = 0;
    return string;
}


/* test program to exercise a few basic abt-io functions */

int main(int argc, char** argv)
{
    int                ret;
    ABT_sched          self_sched;
    ABT_xstream        self_xstream;
    abt_io_instance_id aid;
    struct abt_io_init_info args = { 0 };
    int                fd;
    int                fd2;
    char template[64];

    ABT_init(argc, argv);

    args.json_config = argc > 1 ? readfile (argv[1]) : NULL;

    /* set caller (self) ES to sleep when idle by using SCHED_BASIC_WAIT */
    ret = ABT_sched_create_basic(ABT_SCHED_BASIC_WAIT, 0, NULL,
                                 ABT_SCHED_CONFIG_NULL, &self_sched);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_self(&self_xstream);
    assert(ret == ABT_SUCCESS);
    ret = ABT_xstream_set_main_sched(self_xstream, self_sched);
    assert(ret == ABT_SUCCESS);

    /* start up abt-io */
    if (args.json_config == NULL)
        aid = abt_io_init(2);
    else {
        aid = abt_io_init_ext(&args);
        free((char*)args.json_config);
    }

    assert(aid != NULL);

    sprintf(template, "/tmp/XXXXXX");
    fd = abt_io_mkostemp(aid, template, 0);
    assert(ret >= 0);

    ret = abt_io_pwrite(aid, fd, &fd, sizeof(fd), 0);
    assert(ret == sizeof(fd));

    ret = abt_io_pread(aid, fd, &fd2, sizeof(fd2), 0);
    assert(ret == sizeof(fd2));
    assert(fd == fd2);

#ifdef HAVE_FALLOCATE
    ret = abt_io_fallocate(aid, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                           sizeof(fd), sizeof(fd));
    assert(ret == 0);
#endif

    ret = abt_io_truncate(aid, template, 1024);
    assert(ret == 0);

    ret = abt_io_ftruncate(aid, fd, 512);
    assert(ret == 0);

    ret = abt_io_lseek(aid, fd, 0, SEEK_SET);
    assert(ret == 0);
    ret = abt_io_lseek(aid, fd, 256, SEEK_SET);
    assert(ret == 256);

    ret = abt_io_close(aid, fd);
    assert(ret == 0);

    struct stat statbuf;
    ret = abt_io_stat(aid, template, &statbuf);
    assert(ret == 0);

    struct statfs statfsbuf;
    ret = abt_io_statfs(aid, template, &statfsbuf);
    assert(ret == 0);

    ret = abt_io_unlink(aid, template);
    assert(ret == 0);

    abt_io_finalize(aid);

    ABT_finalize();

    return (0);
}
