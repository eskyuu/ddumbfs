#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>


#include "gsha1.h"

#define NOW_PER_SEC 1000
#define NBR_SEC       10 // run 10 sec each test

int c_block_size=0;
char *block, *block2;

unsigned char sha1a[20];
unsigned char sha1b[20];
unsigned char hash[32];


#include <mhash.h>

long long int now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000LL+tv.tv_usec/1000LL;
}

static void mhash_tiger(unsigned char *hash, const char *block)
{
   MHASH td;

   td=mhash_init(MHASH_TIGER);
   if (td==MHASH_FAILED) exit(1);
   mhash(td, block, c_block_size);
   mhash_deinit(td, hash);
}

static void mhash_sha1(unsigned char *hash, const char *block)
{
   MHASH td;

   td=mhash_init(MHASH_SHA1);
   if (td==MHASH_FAILED) exit(1);
   mhash(td, block, c_block_size);
   mhash_deinit(td, hash);
}

static void gnu_hash_sha1(unsigned char *hash, const char *block)
{
    sha1_buffer(block, c_block_size, hash);
}

static void test_memcpy(unsigned char *hash, const char *block)
{
    memcpy(block2, block, c_block_size);
}

void test_hash(char *name, void (*func)(unsigned char *hash, const char *block), unsigned char *hash, const char *block)
{
    int i;

    long long int start, end, stop;
    start=end=now();
    stop=start+NBR_SEC*NOW_PER_SEC;

    i=0;
    while (end<stop)
    {
        func(hash, block);
        i++;
        end=now();
    }

    fprintf(stderr, "%s time: %7.1f hash/s %7.2f Mo/s hash[0..8]=%016llx\n", name, (float)i*NOW_PER_SEC/(end-start), (float)i*c_block_size*NOW_PER_SEC/(end-start)/1024/1024, *(long long unsigned *)hash);
}


int main(int argc, char *argv[])
{
    int i;

    if (argc<2)
    {
        printf("usage %s block_size_in_k\n", argv[0]);
        return 1;
    }
    c_block_size=strtol(argv[1], NULL, 10);
    if (c_block_size<1 || c_block_size>65536)
    {
        printf("block size must be between 1(1K) and 65536(64Mo): %d\n", c_block_size);
        return 1;
    }
    c_block_size*=1024;

    printf("block size %d (%dK)\n", c_block_size, c_block_size/1024);
    block=malloc(c_block_size);
    block2=malloc(c_block_size);
    if (block==NULL || block2==NULL)
    {
        perror("malloc");
        return 1;
    }
    for(i=0; i<c_block_size; i++) block[i]=i%256;

    test_hash("memcpy       ", test_memcpy, hash, block);
    test_hash("mhash sha1   ", mhash_sha1, hash, block);
    test_hash("gnu hash sha1", gnu_hash_sha1, hash, block);
    test_hash("mhash tiger  ", mhash_tiger, hash, block);
    printf("block_size=%d (%dK)\n", c_block_size, c_block_size/1024);
}
