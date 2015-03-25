/**
 * @testddumbfs.c
 * @author  Alain Spineux <alain.spineux@gmail.com>
 * @version 1.0
 *
 * @section LICENSE
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details at
 * http://www.gnu.org/copyleft/gpl.html
 *
 * @section DESCRIPTION
 *
 * testddumbfs is tool to test filesystem speed and integrity
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <mhash.h>

#include <fcntl.h>

#define DROP_CACHE "/proc/sys/vm/drop_caches"

struct rbstat
{
    float usage;        // in Mo
    float first_write;  // in Mo/s
    float second_write; // in Mo/s
    float read;         // in Mo/s
    float first_write_cpu;  // in %
    float second_write_cpu; // in %
    float read_cpu;         // in %
} *rbstats;

static struct option long_options[] =
{
       {"help",                 no_argument,       0, 'h'},
       {"verbose",              no_argument,       0, 'v'},
       {"socket-dump",          no_argument,       0, 'd'},

       {"count",                required_argument, 0, 'c'},
       {"block-size",           required_argument, 0, 'B'},
       {"file-size",            required_argument, 0, 'S'},
       {"mask",                 required_argument, 0, 'm'},
       {"operation",            required_argument, 0, 'o'},
       {"seed",                 required_argument, 0, 's'},

       {0, 0, 0, 0}
};

void usage()
{
    printf("Usage: testddumbfs [options] target-directory\n"
            "\n    set of tests for filesystem.\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         be more verbose and display progress\n"
            "  -d, --socket-dump     request a dump all using the socket interface\n"
            "  -c, --count=COUNT     repeat the operation COUNT time, default is 4\n"
            "  -B, --block-size=SIZE size of block write/read, default is 128K\n"
            "  -S, --file-size=SIZE  size of file to generate, default 1024M\n"
            "  -m, --mask=MASK       mask to increase compressibility of blocks\n"
            "                        0x00000000 = 100.0%% using gzip -1\n (default)"
            "                        0x60606060 =  77.4%% using gzip -1\n"
            "                        0x60e0e0e0 =  73.9%% using gzip -1\n"
            "                        0xe0e0e0e0 =  68.1%% using gzip -1\n"
            "  -s, --seed=SEED       random seed, default is 0\n"
            "  -f, --fast            fast random, rewrite only one int32 in each block\n"
            "  -o, --operation=OPS  list of operation to run, must be in \"12R\" order\n"
            "                        1 = first write\n"
            "                        2 = second write (for testing deduplication)\n"
            "                        R = read test\n"
            "                          display time, throughput, and CPU usage\n"
            "                        F = create a single file of specified name and size\n"
            "                        C = compare the specified file, 'seed', 'size'"
            "                            and 'fast' must match parameters used for the 'F'"
            "                            operation at creation time\n"
            "                        B = check r/w block race condition\n"
            "                        default is 12R\n"
    );
}

char *target;
int issocket=0;
int verbose_flag=0;
int socket_dump_flag=0;
int count=4;
int block_size=128*1024;
long long int file_size=1024*1024*1024;
long int bitmask=0x0;
char *operation="12R";
int rndseed=0;
int fast_random=0;
long sc_clk_tck;
int cpu=1;

unsigned long *block_buf=NULL;
unsigned long *block_buf_aux=NULL;
unsigned char *block_hash=NULL;


static unsigned long next=1;

void mysrand(unsigned long seed)
{
    next=seed;
}

inline unsigned long int myrand(void)
{
   next=next * 1103515245 + 12345;
//   next = (214013*next+2531011); // intel
   return next;
}

long long int micronow()
{
    struct timeval tv;
    struct timezone tz;
    int res=gettimeofday(&tv, &tz);
    if (res==-1) perror("gettimeofday");
    return tv.tv_sec*1000*1000+tv.tv_usec;
}

#include <sys/socket.h>
#include <sys/un.h>

enum socket_operation_command { sop_noop, sop_open, sop_read, sop_write, sop_fsync, sop_flush, sop_close, sop_dump };

struct socket_operation
{
    int command;
    long long int offset;
    long long int size;
    long long int aux;
};

int sock_init(char *path)
{
    int sock, len;
    struct sockaddr_un remote;

    if ((sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    remote.sun_family = AF_UNIX;
    strcpy(remote.sun_path, path);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family);
    if (connect(sock, (struct sockaddr *)&remote, len) == -1) {
        perror("connect");
        exit(1);
    }
    return sock;
}

ssize_t socket_send(int sockfd, const void *buf, size_t len, int flags)
{
//    fprintf(stderr,"socket_send %d\n", len);
    int sz=0;
    int n;
    while (sz<len)
    {
//        fprintf(stderr,"send %d %d\n", len, sz);
        n=send(sockfd, buf+sz, len-sz, flags);
//        fprintf(stderr,"sent %d %d %d\n", len, sz, n);
        if (n==0) return 0;
        if (n==-1)
        {
            perror("socket_send");
            return n;
        }
        sz+=n;
    }
    return len;
}

ssize_t socket_recv(int sockfd, void *buf, size_t len, int flags)
{
//    fprintf(stderr,"socket_recv %d\n", len);
    int sz=0;
    int n;
    while (sz<len)
    {
//        fprintf(stderr,"recv %d %d\n", len, sz);
        n=recv(sockfd, buf+sz, len-sz, flags);
//        fprintf(stderr,"recv %d %d %d\n", len, sz, n);
        if (n==0) return 0;
        if (n==-1)
        {
            perror("socket_recv");
            return n;
        }
        sz+=n;
    }
    return len;
}

int sock_open(int sock, char *filename, int flag, int mode)
{   // open a file
    int res=0;
    struct socket_operation sop;
    sop.command=sop_open;
    sop.offset=flag;
    sop.aux=mode;
    sop.size=strlen(filename);
//    fprintf(stderr,"Open %s\n", filename);
    res=socket_send(sock, &sop, sizeof(struct socket_operation), 0);
    if (res==-1)
    {
        perror("Open send ops");
        exit(1);
    }
    res=socket_send(sock, filename, sop.size, 0);
    if (res==-1)
    {
        perror("Open send filename:");
        exit(1);
    }
    res=socket_recv(sock, &res, sizeof(res), 0);
    if (res==-1)
    {
        perror("Open recv");
        exit(1);
    }
    return res;
}

int sock_write(int sock, void *buf, long long int size, long long int offset)
{   // write some data
    int res=0;
    struct socket_operation sop;
    sop.command=sop_write;
    sop.size=size;
    sop.offset=offset;
//    fprintf(stderr,"Write\n");
    if (socket_send(sock, &sop, sizeof(struct socket_operation), 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_send(sock, buf, sop.size, 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_recv(sock, &res, sizeof(res), 0)==-1)
    {
        perror("recv");
        exit(1);
    }
    return size;
}

int sock_read(int sock, void *buf, long long int size, long long int offset)
{   // write some data
    int res=0;
    struct socket_operation sop;
    sop.command=sop_read;
    sop.size=size;
    sop.offset=offset;
//    fprintf(stderr,"Read\n");
    if (socket_send(sock, &sop, sizeof(struct socket_operation), 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_recv(sock, &res, sizeof(res), 0)==-1)
    {
        perror("recv");
        exit(1);
    }
    int sz=socket_recv(sock, buf, size, 0);
    return sz;
}

int sock_fsync(int sock)
{   // close
    int res=0;
    struct socket_operation sop;
    sop.command=sop_fsync;
//    fprintf(stderr,"sync\n");
    if (socket_send(sock, &sop, sizeof(struct socket_operation), 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_recv(sock, &res, sizeof(res), 0)==-1)
    {
        perror("recv");
        exit(1);
    }
    return res;
}

int sock_close(int sock)
{   // close
    int res=0;
    struct socket_operation sop;
    sop.command=sop_close;
//    fprintf(stderr,"close\n");
    if (socket_send(sock, &sop, sizeof(struct socket_operation), 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_recv(sock, &res, sizeof(res), 0)==-1)
    {
        perror("recv");
        exit(1);
    }
    return res;
}

int sock_dump(int sock)
{   // dump
    int res=0;
    struct socket_operation sop;
    sop.command=sop_dump;
//    fprintf(stderr,"dump\n");
    if (socket_send(sock, &sop, sizeof(struct socket_operation), 0) == -1)
    {
        perror("send");
        exit(1);
    }
    if (socket_recv(sock, &res, sizeof(res), 0)==-1)
    {
        perror("recv");
        exit(1);
    }
    return res;
}

int test_socket(void)
{
    int sock=sock_init(target);
    fprintf(stderr,"Connected.\n");
    int res=sock_open(sock, "/hello", O_RDWR|O_CREAT|O_TRUNC, 0644);
    int size=128*1024;
    char *data=malloc(size);
    memset(data, 'A', size);
    res=sock_write(sock, data, size, 0);
    res=sock_fsync(sock);
    res=sock_close(sock);

    close(sock);
    return res;
}

int test_dump(void)
{
    int sock=sock_init(target);
    fprintf(stderr,"Connected.\n");
    int res=sock_dump(sock);
    close(sock);
    return res;
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

void read_cpu(long long int *total, long long int *widel, long long int *proc)
{
    long long int user, nice, system, idle;
    long long int iowait=0;
    long long int irq=0;
    long long int softirq=0;
    long long int stolen=0;
    long long int guest=0;
    int len;

    char buf[512];
    int fd=open("/proc/stat", O_RDONLY);
    len=read(fd, buf, sizeof(buf));
    (void)len;
    close(fd);

    char *p=buf+4;
    user=strtol(p, &p, 10);
    nice=strtol(p, &p, 10);
    system=strtol(p, &p, 10);
    idle=strtol(p, &p, 10);
    if (*p!='\n') iowait=strtol(p, &p, 10);
    if (*p!='\n') irq=strtol(p, &p, 10);
    if (*p!='\n') softirq=strtol(p, &p, 10);
    if (*p!='\n') stolen=strtol(p, &p, 10);
    if (*p!='\n') guest=strtol(p, &p, 10);
//    fprintf(stderr, "%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\t%lld\n", user, nice, system, idle, iowait, irq, softirq, stolen, guest);

    *total=user+nice+system+idle+iowait+irq+softirq+stolen+guest;
    *widel=idle+iowait;

    fd=open("/proc/self/stat", O_RDONLY);
    len=read(fd, buf, sizeof(buf));
    close(fd);
    // pid %d comm %s state %c ppid %d pgrp %d session %d tty_nr %d tpgid %d flags %u (%lu<2.6.22)
    // minflt %lu cminflt %lu  majflt %lu cmajflt %lu utime %lu stime %lu cutime %ld cstime %ld

    long unsigned utime, stime;
    sscanf(buf, "%*d %*s %*c %*d %*d %*d %*d %*d %*d %*u %*u %*u %*u %lu %lu", &utime, &stime);
//    fprintf(stderr, "%lu %lu\n", utime, stime);
    *proc=utime+stime;
}

int genfile(unsigned long seed, char *filename, unsigned char *hash, int compare)
{
    char sfilename[4096];
    int err=0;
    MHASH td=MHASH_FAILED;
    if (hash)
    {
        td=mhash_init(MHASH_MD5);
        if (td==MHASH_FAILED) exit(1);
    }

    int fd;
    if (compare) fd=open(filename, O_RDONLY);
    else
    {
        if (issocket)
        {
            fd=issocket;
            strncpy(sfilename+1, filename, sizeof(sfilename)-1);
            sfilename[0]='/';
            sock_open(fd, sfilename, O_RDWR|O_CREAT|O_TRUNC, 0644);
        }
        else
        {
            fd=open(filename, O_WRONLY|O_CREAT, 0644);
        }
    }
    if (fd==-1)
    {
        perror(filename);
        exit(1);
    }
    int n=block_size/sizeof(unsigned long);
    long long int size=0;
    int fast_ixd=0;
    while (size<file_size && !err)
    {
        unsigned long *to=block_buf+n;
        unsigned long *p;

        if (!fast_random || size==0)
        {
            for (p=block_buf; p<to; p++)
            {
                seed=seed * 1103515245 + 12345;
                *p=seed|bitmask;
            }
        }
        if (fast_random && size!=0)
        {
            seed=seed * 1103515245 + 12345;
            *(block_buf+(fast_ixd%n))=seed|bitmask;
            fast_ixd++;
            seed=seed * 1103515245 + 12345;
            *(block_buf+(fast_ixd%n))=seed|bitmask;
            fast_ixd++;
            seed=seed * 1103515245 + 12345;
            *(block_buf+(fast_ixd%n))=seed|bitmask;
            fast_ixd++;
        }
        int len;
        if (compare)
        {
            if (issocket)
            {
                len=sock_read(fd, block_buf_aux, block_size, size);
            }
            else
            {
                len=read(fd, block_buf_aux, block_size);
            }
            err=memcmp(block_buf, block_buf_aux, len)!=0;
            if (err) printf("difference in block starting at: %lld\n", size);
        }
        else
        {
            if (issocket)
            {
                len=sock_write(fd, block_buf, block_size, size);
            }
            else
            {
                len=write(fd, block_buf, block_size);
            }
        }
        if (hash) mhash(td, block_buf, block_size);
        err=(len!=block_size || err);
        size+=block_size;
    }

    if (issocket)
    {
        sock_fsync(fd);
        sock_close(fd);
    }
    else
    {
        fsync(fd);
        close(fd);
    }

    if (hash)
    {
        mhash_deinit(td, hash);
    }
    return err;
}

void print_hash(unsigned char *hash)
{
    int i;
    for (i=0; i<mhash_get_block_size(MHASH_MD5); i++)
    {
        printf("%.2x", hash[i]);
    }
}

int speed_test()
{
    int i, res;
    char filename[1024];
    long long int start, end;
    long long int total1, widel1, total2, widel2, proc1, proc2;


    struct stat stats;
    if (stat(target, &stats))
    {
        fprintf(stderr, "target not found: %s\n", target);
        exit(1);
    }
    issocket=S_ISSOCK(stats.st_mode);
    fprintf(stderr, "issocket=%d %s\n", issocket, target);

    if (issocket)
    {
        issocket=sock_init(target);
    }
    else
    {
        res=chdir(target);
        if (res==-1)
        {
            perror(target);
            exit(1);
        }
    }

    int err=0;
    int loop=1;

    int stat_count=count;

    if (strchr(operation, '2')!=NULL) loop=2;

//    printf("operation=%s %p %p\n", operation, strchr(operation, '1'), strchr(operation, '2'));

    for (loop=(strchr(operation, '1')!=NULL? 0 : 1); loop<(strchr(operation, '2')!=NULL? 2 : 1); loop++)
    {
        if (loop==0) printf("== first write\n");
        if (loop==1) printf("== second write\n");

        long long int totaltime=0;
        float totalcpu=0.0;
        float totaltp=0.0;
        for (i=0; !err && (i<count || count==0); i++)
        {
            mysrand(i+rndseed);
            sprintf(filename, "data%06d-%d.bin", i, loop);
            start=micronow();
            read_cpu(&total1, &widel1, &proc1);
            genfile(i+rndseed, filename, NULL, 0);
            end=micronow();
            read_cpu(&total2, &widel2, &proc2);
            float tp=1000000.0/1048576.0*file_size/(end-start);
            float cpu=100.0-100.0*(proc2-proc1+widel2-widel1)/(total2-total1);
            printf("%6d    %6.2f s    %6.2f Mo/s    %6.2f %%\n", i+1, (end-start)/1000000.0, tp, cpu);
            totaltime+=(end-start);
            totaltp+=tp;
            totalcpu+=cpu;
            if (stat_count)
            {
                if (loop==0) rbstats[i].first_write=tp;
                if (loop==0) rbstats[i].first_write_cpu=cpu;
                if (loop==1) rbstats[i].second_write=tp;
                if (loop==1) rbstats[i].second_write_cpu=cpu;
            }
        }
        printf("  avg:    %6.2f s    %6.2f Mo/s    %6.2f %%\n", totaltime/(1000000.0*count), totaltp/count, totalcpu/count);
        if (stat_count)
        {
            if (loop==0) rbstats[i].first_write=totaltp/count;
            if (loop==0) rbstats[i].first_write_cpu=totalcpu/count;
            if (loop==1) rbstats[i].second_write=totaltp/count;
            if (loop==1) rbstats[i].second_write_cpu=totalcpu/count;
        }
    }

    if (strchr(operation, 'R')!=NULL)
    {
//        free_cache("1\n");
        printf("== read\n");
        long long int totaltime=0;
        float totalcpu=0.0;
        float totaltp=0.0;
        for (i=0; !err && (i<count || count==0); i++)
        {
            start=micronow();
            read_cpu(&total1, &widel1, &proc1);
            sprintf(filename, "data%06d-%d.bin", i, 0);
            int fd=open(filename, O_RDONLY, 0644);
            if (fd==-1)
            {
                break;
            }

            long long int size=0;
            while (size<file_size && !err)
            {
                int len=read(fd, block_buf, block_size);
                err=(len!=block_size);
                size+=block_size;
            }
            close(fd);
            end=micronow();
            read_cpu(&total2, &widel2, &proc2);
            float tp=1000000.0/1048576.0*size/(end-start);
            float cpu=100.0-100.0*(proc2-proc1+widel2-widel1)/(total2-total1);
            printf("%6d    %6.2f s    %6.2f Mo/s    %6.2f %%\n", i, (end-start)/1000000.0, tp, cpu);
            totaltime+=(end-start);
            totaltp+=tp;
            totalcpu+=cpu;
            if (stat_count) rbstats[i].read=tp;
            if (stat_count) rbstats[i].read_cpu=cpu;
        }
        printf("  avg:    %6.2f s    %6.2f Mo/s    %6.2f %%\n", totaltime/(1000000.0*count), totaltp/count, totalcpu/count);
        if (stat_count)
        {
            if (stat_count) rbstats[i].read=totaltp/count;
            if (stat_count) rbstats[i].read_cpu=totalcpu/count;
        }

    }

    if (stat_count)
    {
        printf("== stats\n");
        for (i=0; i<count+1; i++)
        {
            if (i==count) printf("  avg:");
            else printf("%6d", i+1);
            if (strchr(operation, '1')!=NULL) printf("    %6.2f %6.2f", rbstats[i].first_write, rbstats[i].first_write_cpu);
            if (strchr(operation, '2')!=NULL) printf("    %6.2f %6.2f", rbstats[i].second_write, rbstats[i].second_write_cpu);
            if (strchr(operation, 'R')!=NULL) printf("    %6.2f %6.2f", rbstats[i].read, rbstats[i].read_cpu);
            printf("\n");
        }
    }
    return 0;
}

int block_racecond_done=0;
long long int brl_i=0;
long long int brl_n=1;
pthread_t brl_write1_pthread;
pthread_t brl_write2_pthread;
pthread_t brl_read_pthread;
pthread_barrier_t brl_write1_barrier;
pthread_barrier_t brl_write2_barrier;
pthread_barrier_t brl_read_barrier;

void *brl_write1(void *ptr)
{
    unsigned long seed=1;
    int len;
    unsigned long *p;
    int n=block_size/sizeof(unsigned long);
    unsigned long *to=block_buf+n;

    char *filename="blockracecond1.bin";
    brl_n=file_size/block_size;
    brl_i=0;
    for (p=block_buf; p<to; p++)
    {
        *p=seed;
        seed=seed * 1103515245 + 12345;
    }

    int fd=open(filename, O_RDWR|O_CREAT|O_TRUNC, 0644);
    if (fd==-1)
    {
        perror(filename);
        exit(1);
    }

    pthread_barrier_wait(&brl_read_barrier);
    while (!block_racecond_done)
    {
        pthread_barrier_wait(&brl_read_barrier);
        if (brl_i%brl_n==0) lseek(fd, 0, SEEK_SET);
        pthread_barrier_wait(&brl_write1_barrier);
        len=write(fd, block_buf, block_size);
        if (len==-1)
        {
            perror(filename);
            exit(1);
        }
        if (len!=block_size)
        {
            fprintf(stderr, "short write: %s\n", filename);
            exit(1);
        }

        pthread_barrier_wait(&brl_read_barrier);
        block_buf[brl_i++%n]=seed;
        seed=seed * 1103515245 + 12345;
        if (brl_i%100==0) fprintf(stderr, "%lld\r", brl_i);
    }
    pthread_exit(NULL);
}

void *brl_write2(void *ptr)
{
    char *filename="blockracecond2.bin";
    int len;
    int fd=open(filename, O_RDWR|O_CREAT|O_TRUNC, 0644);
    if (fd==-1)
    {
        perror(filename);
        exit(1);
    }

    pthread_barrier_wait(&brl_read_barrier);
    while (!block_racecond_done)
    {
        pthread_barrier_wait(&brl_read_barrier);
        if (brl_i%brl_n==0) lseek(fd, 0, SEEK_SET);
        pthread_barrier_wait(&brl_write1_barrier);
        len=write(fd, block_buf, block_size);
        pthread_barrier_wait(&brl_write2_barrier);
        if (len==-1)
        {
            perror(filename);
            exit(1);
        }
        if (len!=block_size)
        {
            fprintf(stderr, "short write %d: %s\n", len, filename);
            exit(1);
        }
        pthread_barrier_wait(&brl_read_barrier);
    }
    pthread_exit(NULL);
}

void *brl_read(void *ptr)
{
    char *filename="blockracecond2.bin";
    int len;
    pthread_barrier_wait(&brl_read_barrier);
    int fd=open(filename, O_RDONLY);
    if (fd==-1)
    {
        perror(filename);
        exit(1);
    }

    while (!block_racecond_done)
    {
        pthread_barrier_wait(&brl_read_barrier);
        if (brl_i%brl_n==0) lseek(fd, 0, SEEK_SET);
        long long i=brl_i;
        pthread_barrier_wait(&brl_write1_barrier);
        pthread_barrier_wait(&brl_write2_barrier);
        len=read(fd, block_buf_aux, block_size);
        if (len==-1)
        {
            perror(filename);
            exit(1);
        }
        if (len!=block_size)
        {
            fprintf(stderr, "short write: %s\n", filename);
            exit(1);
        }
        if (0!=memcmp(block_buf, block_buf_aux, block_size))
        {
            fprintf(stderr, "block don't match %lld %lld: %s\n", brl_i, i, filename);
            exit(1);
        }
        pthread_barrier_wait(&brl_read_barrier);
    }
    pthread_exit(NULL);
}

int test_block_racecond()
{
    int res=chdir(target);
    if (res==-1)
    {
        perror(target);
        exit(1);
    }
    pthread_barrier_init(&brl_write1_barrier, NULL, 3);
    pthread_barrier_init(&brl_write2_barrier, NULL, 2);
    pthread_barrier_init(&brl_read_barrier, NULL, 3);

    pthread_create(&brl_write1_pthread, NULL, brl_write1, NULL);
    pthread_create(&brl_write2_pthread, NULL, brl_write2, NULL);
    pthread_create(&brl_read_pthread, NULL, brl_read, NULL);

    pthread_join(brl_read_pthread, NULL);
    return 0;
}


long long int unitvalue(char *u)
{
    if (u)
    {
        switch (*u)
        {
            case 'k':
            case 'K':
                return 1024;
            case 'm':
            case 'M':
                return 1048576;
            case 'g':
            case 'G':
                return 1073741824;
            case 't':
            case 'T':
                return 1099511627776LL;
        }
    }
    return 1;
}

int cpu_count()
{
    int count=0;
    char line[256];
    char *cpuinfo="/proc/cpuinfo";
    FILE *cpufile=fopen(cpuinfo, "r");
    if (!cpufile)
    {
        perror(cpuinfo);
        return 1;
    }

    while (fgets(line, sizeof(line), cpufile))
    {
        if (strncmp("processor", line, 9)==0) count++;
    }
    fclose(cpufile);

    return count;
}


int main(int argc, char *argv[])
{
    int c;
    char *unit;

    cpu=cpu_count();
    sc_clk_tck=sysconf(_SC_CLK_TCK);

    while (1)
    {
        // getopt_long stores the option index here.
        int option_index=0;
        char *optparse;

        c=getopt_long(argc, argv, "hvdc:B:S:m:o:s:f", long_options, &option_index);

        // Detect the end of the options.
        if (c==-1) break;

        switch (c)
        {
            case 'h':
                usage();
                return 0;
                break;

            case 'v':
                verbose_flag=1;
                break;

            case 'd':
                socket_dump_flag=1;
                break;

            case 'f':
                fast_random=1;
                break;

            case 'c':
                count=atoi(optarg);
                break;

            case 'B':
                block_size=strtol(optarg, &unit, 10);
                block_size*=unitvalue(unit);
                break;

            case 'S':
                file_size=strtol(optarg, &unit, 10);
                file_size*=unitvalue(unit);
                break;

            case 'm':
                bitmask=strtoll(optarg, &optparse, 0);
                break;

            case 'o':
                operation=strdup(optarg);
                break;

            case 's':
                rndseed=atoi(optarg);
                break;

            default:
                abort();
        }
    }

    if (optind>=argc)
    {
        fprintf(stderr, "Target directory is missing !\n");
        return 1;
    }

    target=argv[optind];

    if (socket_dump_flag)
    {
        test_dump();
        exit(0);
    }

    rbstats=malloc(sizeof(struct rbstat)*(count+1));

    block_buf=malloc(block_size);
    block_buf_aux=malloc(block_size);
    block_hash=malloc(mhash_get_block_size(MHASH_MD5));
    if (block_buf==NULL || block_buf_aux==NULL || block_hash==NULL)
    {
        perror("allocating buffer");
        exit(1);
    }

    int ret=0;

    if (operation[0]=='1' || operation[0]=='2' || operation[0]=='R')
    {
        fprintf(stderr, "seed=%d bitmaks=0x%lx operation=%s\n", rndseed, bitmask, operation);
        ret=speed_test();
    }
    else if (operation[0]=='F' || operation[0]=='C')
    {
        ret=genfile(rndseed, target, NULL, operation[0]=='C');
    }
    else if (operation[0]=='S')
    {
        ret=test_socket();
    }
    else if (operation[0]=='B')
    {
        ret=test_block_racecond();
    }
    else if (operation[0]=='M')
    {
        printf("int: %d\n", (int)sizeof(int));
        printf("long int: %d\n", (int)sizeof(long int));
        printf("long long int: %d\n", (int)sizeof(long long int));
        printf("size_t: %d\n", (int)sizeof(size_t));
        printf("off_t: %d\n", (int)sizeof(off_t));
    }

    return ret;
}
