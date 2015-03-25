#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <fcntl.h>

#define DROP_CACHE "/proc/sys/vm/drop_caches"

char *target;
int block_size, rndseed;
long long int file_size;
int file_number;
long long int bitmask;
char *ops;

static unsigned long next = 1;

struct rbstat
{
    float usage;        // in Mo
    float first_write;  // in Mo/s
    float second_write; // in Mo/s
    float read;         // in Mo/s
} *rbstats;

int stat_count=0;

void mysrand(unsigned long seed) {
    next = seed;
}

long int myrand(void) {
   next = next * 1103515245 + 12345;
   return next;
}

long long int now()
{
    struct timeval tv;
    struct timezone tz;
    int res=gettimeofday(&tv, &tz);
    if (res==-1) perror("gettimeofday");
    return tv.tv_sec*1000*1000+tv.tv_usec;
}

void free_cache(char *op)
{
    int fd, res;
    fd=open(DROP_CACHE, O_WRONLY);
    if (fd==-1)
    {
        perror(DROP_CACHE);
        return;
    }
    res=write(fd, op, strlen(op));
    if (res!=strlen(op))
    {
        if (res==-1) perror(op);
        fprintf(stderr, "incomplete write to %s.", DROP_CACHE);
    }
    close(fd);
}

int main(int argc, char *argv[])
{
    int i, res;
    char filename[1024];
    long long int start, end;

    if (argc<5)
    {
        printf("usage: %s target-dir  block-size-in-ko  file-size-in-mo  numbers-of-file [rnd-seed=0]  [bit-mask=0x0] [ops=12R]\n", argv[0]);
        printf("\n");
        printf("generate numbers-of-file files in directory target-dir of size = file-size-in-mb Mo\n");
        printf("writing blocks of block-size-in-ko Ko at a time.\n");
        printf("content is random and initialized per file with rnd-seed+file_number.\n");
        printf("You can increase compressibility of the random data using a OR bit-mask. Here are some samples:\n");
        printf("bitmask 0x00000000 = 100.0%% using gzip -1\n");
        printf("bitmask 0x60606060 =  77.4%% using gzip -1\n");
        printf("bitmask 0x60e0e0e0 =  73.9%% using gzip -1\n");
        printf("bitmask 0xe0e0e0e0 =  68.1%% using gzip -1\n");
        printf("\n");
        printf("ops=1 first write\n");
        printf("ops=2 second write (for testing deduplication)\n");
        printf("ops=R test read\n");
        printf("\n");
        exit(1);
    }

    target=argv[1];
    block_size=atoi(argv[2])*1024LL;
    file_size=atoi(argv[3])*1024LL*1024;
    file_number=atoi(argv[4]);
    if (argc>=6) rndseed=atoi(argv[5]);
    else rndseed=0;
    if (argc>=7) bitmask=strtoll(argv[6], NULL, 0);
    else bitmask=0x0;
    if (argc>=8) ops=argv[7];
    else ops="12R";

    printf("seed=%d bitmaks=0x%llx ops=%s\n", rndseed, bitmask, ops);

    stat_count=file_number;
    rbstats=malloc(sizeof(struct rbstat)*stat_count);

    long int *buf=malloc(block_size);
    if (buf==NULL)
    {
        perror("allocating buffer");
        exit(1);
    }

    res=chdir(target);
    if (res==-1)
    {
        perror(target);
        exit(1);
    }

    int err=0;
    int n=block_size/sizeof(long int);
    int loop=1;


    if (strchr(ops, '2')!=NULL) loop=2;

//    printf("ops=%s %p %p\n", ops, strchr(ops, '1'), strchr(ops, '2'));

    for (loop=(strchr(ops, '1')!=NULL? 0 : 1); loop<(strchr(ops, '2')!=NULL? 2 : 1); loop++)
    {
        if (loop==0) printf("== first write\n");
        if (loop==1) printf("== second write\n");

        for (i=0; !err && (i<file_number || file_number==0); i++)
        {
            mysrand(i+rndseed);
            start=now();
            sprintf(filename, "data%06d-%d.bin", i, loop);
            int fd=open(filename, O_WRONLY|O_CREAT, 0644);
            if (fd==-1)
            {
                perror(filename);
                exit(1);
            }

            int size=0;
            while (size<file_size && !err)
            {
                int j;
                long int *p;
                for (j=0, p=buf; j<n; p++, j++) *p=bitmask | myrand();
                int len=write(fd, buf, block_size);
                err=(len!=block_size);
                if (err)
                {
                    if (len==-1) perror(filename);
                    else printf("partial write on %s, %d bytes instead of %d bytes\n", filename, len, block_size);
                }
                size+=block_size;
            }
            fsync(fd);
            close(fd);
            end=now();
            float tp=1000000.0/1048576.0*size/(end-start);
            printf("%6d\t%5.2f\t%5.2f\n", i, (end-start)/1000000.0, tp);
            if (stat_count)
            {
                if (loop==0) rbstats[i].first_write=tp;
                if (loop==1) rbstats[i].second_write=tp;
            }
        }
    }

    if (strchr(ops, 'R')!=NULL)
    {
//        free_cache("1\n");
        printf("== read\n");
        for (i=0; !err && (i<file_number || file_number==0); i++)
        {
            start=now();
            sprintf(filename, "data%06d-%d.bin", i, 0);
            int fd=open(filename, O_RDONLY, 0644);
            if (fd==-1)
            {
                break;
            }

            int size=0;
            while (size<file_size && !err)
            {
                int len=read(fd, buf, block_size);
                err=(len!=block_size);
                size+=block_size;
            }
            close(fd);
            end=now();
            float tp=1000000.0/1048576.0*size/(end-start);
            printf("%6d\t%5.2f\t%5.2f\n", i, (end-start)/1000000.0, tp);
            if (stat_count) rbstats[i].read=tp;
        }
    }

    if (stat_count)
    {
        printf("== stats\n");
        for (i=0; i<file_number; i++)
        {
            printf("%6d\t%5.2f", i, rbstats[i].first_write);
            if (strchr(ops, '2')!=NULL) printf("\t%5.2f", rbstats[i].second_write);
            if (strchr(ops, 'R')!=NULL) printf("\t%5.2f", rbstats[i].read);
            printf("\n");
        }
    }


    return 0;
}
