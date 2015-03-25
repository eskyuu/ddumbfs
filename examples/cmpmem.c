#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include <sys/mman.h>


#define NOW_PER_SEC 1000
#define NBR_SEC        2// run 10 sec each test

long long int mem_size=1024*1024*1024;
long long int block_size=32*1024*1024;

int *mem;
int *block;
char *filename;

long long int now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000LL+tv.tv_usec/1000LL;
}

void test_block(int idx)
{
    int i;
    long long int start, end, stop;
    start=end=now();
    stop=start+NBR_SEC*NOW_PER_SEC;

    int n=block_size/sizeof(int);
    int *block=mem+(idx*n);
    int *to=block+n;

    int count;

    i=0;
    while (end<stop)
    {
        int *p;
        for (p=block; p<to; p++)
        {
            if (*p==0) count++;
        }
        i++;
        end=now();
    }
    fprintf(stderr, "%3d time: %7.1f block/s %7.2f Mo/s\n", idx, (float)i*NOW_PER_SEC/(end-start), (float)i*block_size*NOW_PER_SEC/(end-start)/1024/1024);
}


int main(int argc, char *argv[])
{
    int i;

    if (argc<2)
    {
        printf("usage %s filename memsize_in_Mo block_size_in_Mo\n", argv[0]);
        return 1;
    }

    filename=argv[1];

    mem_size=strtol(argv[2], NULL, 10);
    if (mem_size<16 || mem_size>65536)
    {
        printf("mem size must be between 16(16Mo) and 65536(64Go): %lld\n", mem_size);
        return 1;
    }

    block_size=strtol(argv[3], NULL, 10);
    if (block_size<1 || block_size>1024)
    {
        printf("block size must be between 1(1Mo) and 1024(1Go): %lld\n", block_size);
        return 1;
    }
    if (block_size>=mem_size)
    {
        printf("block size must be smaller than mem size\n");
        return 1;
    }
    mem_size*=1024*1024;
    block_size*=1024*1024;

    int fd=open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd==-1)
    {
        perror(filename);
        exit(1);
    }

    printf("mem=%lldMo block=%lldMo\n", mem_size/1024/1024, block_size/1024/1024);

    block=malloc(block_size);
    if (block==NULL)
    {
        perror("malloc");
        return 1;
    }
    for(i=0; i<block_size/sizeof(int); i++) block[i]=i;

    for (i=0; i<mem_size/block_size; i++)
    {
        int len=write(fd, block, block_size);
        if (len!=block_size)
        {
            if (len==-1) perror(filename);
            else fprintf(stderr, "short write %d/%lld\n", len, block_size);
            exit(1);
        }
    }

    mem=mmap(NULL, mem_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    int res=mlock(mem, mem_size);
    if (res==-1) {
        perror("cannot lock file in memory\n");
    }

    for (i=0; i<mem_size/block_size; i++)
    {
        test_block(i);
    }
    munmap(mem, mem_size);
    close(fd);
    return 0;
}
