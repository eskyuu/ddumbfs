/**
 * @ddumbfs.c
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
 * ddumbfs.c provides the FUSE interface for the ddumbfs filesystem
 *
 */

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE

#define FUSE_USE_VERSION 26
//#define  HAVE_SETXATTR

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#include <fuse.h>
#include <ulockmgr.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include <stddef.h>
#include <inttypes.h>

#include <ctype.h>

#include <pthread.h>

#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <assert.h>
#include <ftw.h>
#include <search.h>

#define SOCKET_INTERFACE
#ifdef SOCKET_INTERFACE
    #define SOCKET_PATH "../socket"
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

#include "bits.h"
#include "ddfslib.h"
#include "ddfschkrep.h"

//#define pthread_mutex_lock_d(x)   { DDFS_LOG(LOG_NOTICE, "[%lu]++ pthread_mutex_lock   %p %s %d l=%d c=%d u=%d\n", thread_id(), (void*) (x), __func__, __LINE__, (x)->__data.__lock, (x)->__data.__count, (x)->__data.__nusers); pthread_mutex_lock(x); }
//#define pthread_mutex_unlock_d(x) { DDFS_LOG(LOG_NOTICE, "[%lu]-- pthread_mutex_unlock %p %s %d l=%d c=%d u=%d\n", thread_id(), (void*) (x), __func__, __LINE__, (x)->__data.__lock, (x)->__data.__count, (x)->__data.__nusers); pthread_mutex_unlock(x); }
//#define pthread_cond_wait_d(c, x) { DDFS_LOG(LOG_NOTICE, "[%lu]-- pthread_cond_wait    %p %s %d l=%d c=%d u=%d\n", thread_id(), (void*) (x), __func__, __LINE__, (x)->__data.__lock, (x)->__data.__count, (x)->__data.__nusers); pthread_cond_wait(c, x); DDFS_LOG_DEBUG("[%lu]++ pthread_cond_wait    %p %s %d l=%d c=%d u=%d\n", thread_id(), (void*) (x), __func__, __LINE__, (x)->__data.__lock, (x)->__data.__count, (x)->__data.__nusers); }

#define pthread_mutex_lock_d(x)   { pthread_mutex_lock(x); }
#define pthread_mutex_unlock_d(x) { pthread_mutex_unlock(x); }
#define pthread_cond_wait_d(c, x) { pthread_cond_wait(c, x); }

#define XSTAT_FH_SZ     8  // (is dynamic now) at least 12 to backup VMware ESX via NFS, because vmkfstools open 8 simultaneous connection to the same file
pthread_mutex_t ifile_mutex=PTHREAD_MUTEX_INITIALIZER;

long long int r_file_count;
long long int r_addr_count;
long long int r_frag_count;
long long int r_not_found;

// cannot start reclaim() when in ddumb_buf_write() or reclaim() itself, this require a pthread_cond_t and its mutex
pthread_mutex_t reclaim_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t reclaim_ddumb_buf_write_is_unused=PTHREAD_COND_INITIALIZER;
int reclaim_ddumb_buf_write_is_in_use=0;
int reclaim_could_find_free_blocks=1;
int no_more_free_block_warning=0;
int low_free_block_warning=0;

// I need to know when reclaim() is running and maintain a list of recently allocated block
pthread_spinlock_t reclaim_spinlock;
struct bit_array ba_found_in_files;
int reclaim_enable=0;

// handle save of block list at regulare interval
long long int used_block_saved=-1;
int ddumbfs_terminate=0;
pthread_t ddumbfs_background_pthread;
pthread_mutex_t ddumb_background_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t ddumb_background_cond=PTHREAD_COND_INITIALIZER;

typedef struct ddumb_param
{
    char  *parent;
    int   pool;
    int   check_at_start;
    int   lock_index;
    int   direct_io;
    int   fuse_default;
    int   reclaim;
    char  *command_args;
    char  *ext_command_args;
    double attr_timeout;
} struct_ddumb_param;

struct_ddumb_param ddumb_param = { NULL, -100, 0, 1, 2, 1, 95, NULL, NULL, 1.0L };

int next_reclaim=100;

struct ddumb_fh;

// simultaneous access to a file are protected by zones
struct xzone
{
    long long int start;
    long long int end;
    char op;
    char right;
};

// xstat is for extra stat (currently only file size)
struct xstat
{


    long long int ino;      // only one xstat per ino

    struct file_header h;   // the payload
    int saved;              // does the payload need to be saved ?

    struct ddumb_fh **fhs;               // ddumb_fh pointing to this xstat, used to search for ddumb_fh->buf
    int fhs_max;                         // size of fhs[]
    int fhs_n;
    pthread_mutex_t xstat_lock;
    pthread_cond_t  zone_cond;
    pthread_cond_t  buf_cond;
};

#define DDFS_BUF_EMPTY  0
#define DDFS_BUF_RDONLY 1
#define DDFS_BUF_RDWR   2

enum poolstatus { // status of ddumb_fh->pool_status
                  ps_empty,     // don't contain any data or can be loaded by a writer_pool_load()
                  ps_loading,   // being processed by writer_pool_load()
                  ps_ready,     // ready to write, waiting to be processed by writer_pool_loop()
                  ps_busy       // being processed by writer_pool_loop()
                  };

// this is the info that are attached to fuse struct fuse_file_info *fi
struct ddumb_fh
{   // one is created by open, the same can be used simultaneously by multiple thread
    int fd;
    int rdonly;
    struct xstat *xstat;
    char *filename;         // the filename (could be NULL)
    int pool_writer;        // number of "writer" holding this fh
    char *buf;              // attached buffer, used only for write operation
    int buf_loaded;         // do this buffer contains relevant data that must be saved
    int buf_firstwrite;     // when this "loaded" buffer has been written for the first time
//    long long int buf_off;  // the offset of this buffer inside the file
    off_t buf_off;  // the offset of this buffer inside the file
    pthread_mutex_t lock;
    int pool_status;
    pthread_cond_t pool_cond  ;
    int special;
    int delayed_write_error_code;
    struct ddumb_fh *fh_src;
    struct xzone zone;

};

// maintains a tree sorted by ino with all currently open files
struct xstat_root
{
    void *root;
    pthread_mutex_t mutex;
    struct xstat* xstat;
} xstat_root;


struct ddumb_statistic
{   // contains some usefull stat about the running ddumbfs
    long long int header_load;
    long long int header_save;

    long long int hash;

    long long int block_write;
    long long int read_before_write; // write not on a block boundary, requiring a read
    long long int eof_write;         // write after eof
    long long int ghost_write;       // block already exist, just reuse the block address
    long long int block_write_try_next_node; // node already used, try next
    long long int block_write_slide;  // slide inside node block
    long long int block_read;
    long long int block_read_zero;    // read zero block, not from disk
    long long int write_save;         // the write is not sequential, save an "uncompleted" buffer
    long long int block_locked_max;   // max in block_locked[]
    long long int getattr;
    long long int fgetattr;
    long long int do_truncate;
    long long int ftruncate;
    long long int truncate;
    long long int create;
    long long int open;
    long long int read;
    long long int write;
    long long int flush;
    long long int release;
    long long int fsync;
    long long int inode_counter;     // number of inode simultaneously open (<=fh_counter)
    long long int fh_counter;        // number of ddumb_fh simultaneously open

    long long int lock_zone;
    long long int wait_for_zone;

    long long int wait_buf_write;    // wait for end of buffer flush before to write
    long long int wait_buf_truncate; // wait for end of buffer flush before to truncate
    long long int wait_writer_pool_on_flush;
    long long int wait_writer_pool_on_submit;
    long long int wait_writer_pool_on_ready;
    long long int wait_writer_pool_try_active_wait;
    long long int wait_writer_pool_active_wait;
    long long int wait_on_submit;   // in mirco_sec
    long long int calc_hash;         // in mirco_sec
    long long int xstat_resize;
    long long int reclaim;

    long long int counter1;
    long long int counter2;
    long long int counter3;
    long long int counter4;

} ddumb_statistic;


struct ddumb_fh **writers_fh;
int writers_fh_next_empty=0;
int writers_fh_next_ready=0;
int writers_fh_count;
int writers_fh_n_empty;
int writers_fh_n_ready;
pthread_mutex_t writer_pool_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t writer_pool_cond_empty=PTHREAD_COND_INITIALIZER;  // at least one writer is empty
pthread_cond_t writer_pool_cond_ready=PTHREAD_COND_INITIALIZER;  // at least one writer is ready
pthread_t *writer_pool_threads=NULL;

// early declaration
static int ddumb_buffer_flush(struct ddumb_fh *fh);
static int _ddumb_write(const char *path, const char *buf, size_t size, off_t offset, struct ddumb_fh *fh);
void ddumb_test(FILE *file);


#define WRITE_FIELD(file, field, unit) { if (ddumb_statistic.field>0) fprintf(file, "%-30s %9lld%s\n", #field, ddumb_statistic.field, unit); }

static void ddumb_write_statistic(FILE *file)
{   // write ddumbfs statistics to FILE *file
    WRITE_FIELD(file, header_load,"");
    WRITE_FIELD(file, header_save,"");

    WRITE_FIELD(file, hash,"");

    WRITE_FIELD(file, block_write,"");
    WRITE_FIELD(file, read_before_write,"");
    WRITE_FIELD(file, ghost_write,"");
    WRITE_FIELD(file, write_save,"");
    WRITE_FIELD(file, eof_write,"");

    WRITE_FIELD(file, block_write_try_next_node,"");
    WRITE_FIELD(file, block_write_slide,"");

    WRITE_FIELD(file, block_read,"");
    WRITE_FIELD(file, block_read_zero,"");

    WRITE_FIELD(file, block_locked_max,"");

    WRITE_FIELD(file, getattr,"");
    WRITE_FIELD(file, fgetattr,"");
    WRITE_FIELD(file, do_truncate,"");
    WRITE_FIELD(file, ftruncate,"");
    WRITE_FIELD(file, truncate,"");
    WRITE_FIELD(file, create,"");
    WRITE_FIELD(file, open,"");
    WRITE_FIELD(file, read,"");
    WRITE_FIELD(file, write,"");
    WRITE_FIELD(file, flush,"");
    WRITE_FIELD(file, release,"");
    WRITE_FIELD(file, fsync,"");
    WRITE_FIELD(file, inode_counter,"");
    WRITE_FIELD(file, fh_counter,"");

    WRITE_FIELD(file, lock_zone,"");
    WRITE_FIELD(file, wait_for_zone,"");
    WRITE_FIELD(file, wait_buf_write,"");
    WRITE_FIELD(file, wait_buf_truncate,"");
    WRITE_FIELD(file, wait_writer_pool_on_flush,"");
    WRITE_FIELD(file, wait_writer_pool_on_submit,"");
    WRITE_FIELD(file, wait_writer_pool_on_ready,"");
    WRITE_FIELD(file, wait_writer_pool_try_active_wait,"");
    WRITE_FIELD(file, wait_writer_pool_active_wait,"");
    WRITE_FIELD(file, wait_on_submit,"mms");
    WRITE_FIELD(file, calc_hash,"mms");
    WRITE_FIELD(file, xstat_resize,"");
    WRITE_FIELD(file, counter1,"");
    WRITE_FIELD(file, counter2,"");
    WRITE_FIELD(file, counter3,"");
    WRITE_FIELD(file, counter4,"");

    long long int s, u;
    pthread_mutex_lock(&ifile_mutex);
    bit_array_count(&ddfs->ba_usedblocks, &s, &u); // no need to lock ddfs->ba_usedblocks here
    pthread_mutex_unlock(&ifile_mutex);
    fprintf(file, "%-30s %9lld\n", "block_allocated", s);
    fprintf(file, "%-30s %9lld\n", "block_free", u);
    fprintf(file, "%-30s %9.2f\n", "overflow", ddfs->c_node_overflow);
    fprintf(file, "%-30s %9d\n", "direct_io", ddfs->direct_io);
    fprintf(file, "%-30s %9d\n", "align", ddfs->align);
    fprintf(file, "%-30s %9d\n", "lock_index", ddfs->lock_index);
    fprintf(file, "%-30s %9s\n", "hash", ddfs->c_hash);
    fprintf(file, "%-30s %9d\n", "writer_pool", ddumb_param.pool);
    fprintf(file, "%-30s %9d\n", "reclaim", ddumb_param.reclaim);
    fprintf(file, "%-30s %9d\n", "next_reclaim", next_reclaim);
    fprintf(file, "%-30s %9s\n", "command_args", ddumb_param.command_args);
    fprintf(file, "%-30s %9s\n", "ext_command_args", ddumb_param.ext_command_args);
}

unsigned long int thread_id(void)
{
    return (unsigned long int) pthread_self();
}

void preload_node(long long int node_idx)
{  // make a read in nodes to load the page, usually just before to lock the mutex
    volatile char ch=*(ddfs->nodes+(node_idx*ddfs->c_node_size));
    (void)ch;
}

#define ddumb_get_fh(fi) ((struct ddumb_fh *)(uintptr_t)(fi)->fh)

/*
 * xstat_*
 */
static int xstat_load(int fd, struct xstat *xstat, const char* filename)
{   // load extra stat data from file header
    ddumb_statistic.header_load++;

    int len=file_header_set(fd, &xstat->h);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "xstat_load pread: %s (%s)\n", filename, strerror(errno));
        return -errno;
    }
    else if (len==0)
    {
        DDFS_LOG(LOG_ERR, "xstat_load, not a valid ddumbfs file: %s\n", filename);
        return -EIO;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        DDFS_LOG(LOG_ERR, "xstat_load read only %d/%d bytes: %s\n", len, ddfs->c_file_header_size, filename);
        return -EIO;
    }

    xstat->saved=1;
    DDFS_LOG_DEBUG("[%lu]++  xstat_load fd=%d len=%d xstat=%p size=%llx(%lld) %s\n", thread_id(), fd , len, (void*)xstat, (long long int)xstat->h.size, (long long int)xstat->h.size, filename);
    return 0;
}

static int xstat_save(int fd, struct xstat *xstat, const char* filename)
{   // save extra stat data into file header
    ddumb_statistic.header_save++;
    DDFS_LOG_DEBUG("[%lu]++  xstat_save fd=%d size=%lld %s\n", thread_id(), fd, (long long int)xstat->h.size, filename);

    int len=file_header_get(fd, &xstat->h);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "xstat_save pwrite: %s (%s)\n", filename, strerror(errno));
        return -errno;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        DDFS_LOG(LOG_ERR, "xstat_save wrote only %d/%d bytes: %s\n", len, ddfs->c_file_header_size, filename);
        return -EIO;
    }
    xstat->saved=1;
    return 0;
}

static int xstat_save_fh(struct ddumb_fh *fh)
{   // save extra stat data into file header
    DDFS_LOG_DEBUG("[%lu]++  xstat_save_fi fd=%d fh=%p rdonly=%d %s\n", thread_id(), fh->fd, (void*)fh, fh->rdonly, fh->filename);
    if (!fh->rdonly) return xstat_save(fh->fd, fh->xstat, fh->filename);
    return 0;
}

int xstat_compare(const void *a, const void* b)
{
    long long int res=((struct xstat *)a)->ino - ((struct xstat *)b)->ino;
    if (res==0) return 0;
    if (res<0) return -1;
    else return 1;
}

int xstat_counter=0;
void xstat_display(const void *nodep, const VISIT which, const int depth)
{
    struct xstat *xstat=*(struct xstat **)nodep;

    if (which==postorder || which==leaf)
    {
        DDFS_LOG(LOG_NOTICE, "xstat %d %p ino=%lld #%d lock<l=%d c=%d u=%d>\n", xstat_counter, xstat, xstat->ino, xstat->fhs_n, xstat->xstat_lock.__data.__lock, xstat->xstat_lock.__data.__count, xstat->xstat_lock.__data.__nusers);
        int i;
        if (pthread_mutex_trylock(&xstat->xstat_lock))
        {
            DDFS_LOG(LOG_NOTICE, "xstat already locked skip\n");
        }
        else
        {
            for (i=0; i<xstat->fhs_n; i++)
            {
                struct ddumb_fh *fh=xstat->fhs[i];
                DDFS_LOG(LOG_NOTICE, "    %d fh=%p buf_loaded=%d writer=%d lock<l=%d c=%d u=%d> %s\n", i, fh, fh->buf_loaded, fh->pool_writer, fh->lock.__data.__lock, fh->lock.__data.__count, fh->lock.__data.__nusers, fh->filename);
            }
            pthread_mutex_unlock(&xstat->xstat_lock);
        }
        xstat_counter++;
    }
}

void xstat_dump(struct xstat_root *root)
{
    if (pthread_mutex_trylock(&root->mutex))
    {
        DDFS_LOG(LOG_NOTICE, "xstat_dump: root->mutex already locked, dump skipped\n");
    }
    else
    {
        xstat_counter=0;
        twalk(xstat_root.root, xstat_display);
        pthread_mutex_unlock(&root->mutex);
    }
}


#define FLUSH_FH_SIZE   128
struct ddumb_fh *flush_fh[FLUSH_FH_SIZE];
int flush_fh_n=0;

int xstat_time_limit;
void xstat_flush_oldbuf(const void *nodep, const VISIT which, const int depth)
{
    struct xstat *xstat=*(struct xstat **)nodep;

    if (flush_fh_n>=FLUSH_FH_SIZE) return;

    if (which==postorder || which==leaf)
    {
        pthread_mutex_lock(&xstat->xstat_lock);
        int i;
        for (i=0; i<xstat->fhs_n; i++)
        {
            struct ddumb_fh *fh=xstat->fhs[i];
            if (flush_fh_n<FLUSH_FH_SIZE && fh->buf_loaded==DDFS_BUF_RDWR && fh->buf_firstwrite<=xstat_time_limit)
            {
                if (0==pthread_mutex_trylock(&fh->lock))
                {
                    flush_fh[flush_fh_n++]=fh;
                }
                xstat_counter++;
            }
        }
        pthread_mutex_unlock(&xstat->xstat_lock);
    }
}

void xstat_flush_all_buf(int time_limit)
{
    flush_fh_n=0;
    xstat_time_limit=time_limit;
    pthread_mutex_lock(&xstat_root.mutex);
    xstat_counter=0;
    twalk(xstat_root.root, xstat_flush_oldbuf);
    pthread_mutex_unlock(&xstat_root.mutex);
    int i;
    for (i=0; i<flush_fh_n; i++)
    {
        struct ddumb_fh *fh=flush_fh[i];
        int res=ddumb_buffer_flush(fh);
        if (fh->delayed_write_error_code==0) fh->delayed_write_error_code=res;
        pthread_mutex_unlock(&fh->lock);
    }
    if (xstat_counter) DDFS_LOG(LOG_INFO, "flush long living buffer %d/%d\n", flush_fh_n, xstat_counter);
}


int dump_all()
{
    xstat_counter=0;

    DDFS_LOG(LOG_NOTICE, "====== dump all\n");
    DDFS_LOG(LOG_NOTICE, "--- xstat ---\n");
    xstat_dump(&xstat_root);
    int i;
    if (ddumb_param.pool)
    {
        DDFS_LOG(LOG_NOTICE, "--- writers ---\n");
        pthread_mutex_lock_d(&writer_pool_mutex);
        DDFS_LOG(LOG_NOTICE, "   writers_fh_count=%3d writers_fh_availabel=%3d writers_fh_n_ready=%3d\n", writers_fh_count, writers_fh_n_empty, writers_fh_n_ready);
        for (i=0; i<writers_fh_count; i++)
        {
            struct ddumb_fh *fh=writers_fh[i];
            DDFS_LOG(LOG_NOTICE, "   writers[%2d] fh=%p status=%d buf_loaded=%d fh_src=%p lock<l=%d c=%d u=%d>\n", i, fh, fh->pool_status, fh->buf_loaded, fh->fh_src, fh->lock.__data.__lock, fh->lock.__data.__count, fh->lock.__data.__nusers);
        }
        pthread_mutex_unlock_d(&writer_pool_mutex);
    }
    if (pthread_mutex_trylock(&ifile_mutex))
    {
        DDFS_LOG(LOG_NOTICE, "ifile_mutex already locked\n");
    }
    else
    {
        pthread_mutex_unlock(&ifile_mutex);
        DDFS_LOG(LOG_NOTICE, "ifile_mutex not locked\n");
    }

    if (pthread_mutex_trylock(&reclaim_mutex))
    {
        DDFS_LOG(LOG_NOTICE, "reclaim_mutex already locked\n");
    }
    else
    {
        pthread_mutex_unlock(&reclaim_mutex);
        DDFS_LOG(LOG_NOTICE, "reclaim_mutex not locked\n");
    }

    if (pthread_mutex_trylock(&ddumb_background_mutex))
    {
        DDFS_LOG(LOG_NOTICE, "ddumb_background_mutex already locked\n");
    }
    else
    {
        pthread_mutex_unlock(&ddumb_background_mutex);
        DDFS_LOG(LOG_NOTICE, "ddumb_background_mutex not locked\n");
    }
    DDFS_LOG(LOG_NOTICE, "dump all ======\n");
    return 0;
}


int _xstat_register(struct xstat_root *root, struct ddumb_fh *fh)
{
    int res=0;

//    DDFS_LOG_DEBUG("[%lu]++  xstat_register fd=%d fh=%p root->xstat=%p\n", thread_id(), fh->fd, fh, (void*)root->xstat);
    if (!root->xstat)
    {
        root->xstat=malloc(sizeof(struct xstat));
        if (root->xstat)
        {
            root->xstat->fhs_max=XSTAT_FH_SZ;
            root->xstat->fhs=malloc(XSTAT_FH_SZ*sizeof(struct ddumb_fh *));
        }
        if (root->xstat==NULL || root->xstat->fhs==NULL)
        {
            if (root->xstat) free(root->xstat);
            root->xstat=NULL;
            DDFS_LOG(LOG_ERR, "xstat_register: xstat malloc failed: %s\n", fh->filename);
            return -ENOMEM;
        }
    }

    struct stat stbuf;
    if (fstat(fh->fd, &stbuf)==-1)
    {
        DDFS_LOG(LOG_ERR, "xstat_register: cannot fstat the file: %s (%s)\n", fh->filename, strerror(errno));
        return -errno;
    }
    root->xstat->ino=stbuf.st_ino;

    void *val=tsearch((const void *)root->xstat, &(root->root), xstat_compare);
//    DDFS_LOG_DEBUG("[%lu]++  tsearch root=%p xtstat=%p  -> xstat=%p\n", thread_id(), (void *)root->root, (void *)root->xstat, (void*)*(struct xstat**)val);
    if (val==NULL)
    {
        DDFS_LOG(LOG_ERR, "xstat_register: tsearch cannot allocate a new item: %s\n", fh->filename);
        fh->xstat=NULL;
        return -ENOMEM;
    }

    struct xstat *xstat=fh->xstat=*(struct xstat**)val;
    if (fh->xstat==root->xstat)
    {
        ddumb_statistic.inode_counter++;
        root->xstat=NULL;
//        DDFS_LOG_DEBUG("[%lu]** xstat_register xstat=%p memeset size=%d\n\n", thread_id(), fh->xstat, sizeof(xstat->fhs));
        xstat->fhs[0]=fh;
        xstat->fhs_n=1;
        DDFS_LOG_DEBUG("[%lu]pthread_mutex_init xstat %p %s\n", thread_id(), (void*)&xstat->xstat_lock, fh->filename);
        pthread_mutex_init(&xstat->xstat_lock, 0);
        pthread_cond_init(&xstat->zone_cond, NULL);
        pthread_cond_init(&xstat->buf_cond, NULL);

        res=xstat_load(fh->fd, fh->xstat, fh->filename);
        if (res<0)
        {
            tdelete(&(fh->xstat), &(root->root), xstat_compare);
            root->xstat=fh->xstat;
            fh->xstat=NULL;
//DDFS_LOG(LOG_NOTICE, "[%lu]++  xstat_register failed load delete xstat=%p ino=%lld\n", thread_id(), fh->xstat, xstat->ino);
        }
    }
    else
    {
        pthread_mutex_lock_d(&xstat->xstat_lock);
        // add fh to fh->xstat->fhs
        if (xstat->fhs_n>=xstat->fhs_max)
        {   // double the size of fhs
            ddumb_statistic.xstat_resize++;
            struct ddumb_fh **fhs=realloc(xstat->fhs, 2*xstat->fhs_max*sizeof(struct ddumb_fh *));
            if (fhs==NULL)
            {
                pthread_mutex_unlock_d(&xstat->xstat_lock);
                DDFS_LOG(LOG_ERR, "xstat_register realloc failed: %s\n", fh->filename);
                return -ENOMEM;
            }
            xstat->fhs=fhs;
            xstat->fhs_max*=2;
        }

        xstat->fhs[xstat->fhs_n++]=fh;
        pthread_mutex_unlock_d(&xstat->xstat_lock);
    }
    DDFS_LOG_DEBUG("[%lu]++  xstat_register fd=%d fh=%p xstat=%p ino=%lld fhs_n=%d root->xstat=%p %s\n", thread_id(), fh->fd, (void*)fh, (void*)fh->xstat, xstat->ino, xstat->fhs_n, (void*)root->xstat, fh->filename);
    return res;
}

int xstat_register(struct xstat_root *root, struct ddumb_fh *fh)
{
    int res;
    pthread_mutex_lock_d(&root->mutex);
    res=_xstat_register(root, fh);
    pthread_mutex_unlock_d(&root->mutex);
//    xstat_dump(root);
    return res;
}

int xstat_subscribe(struct ddumb_fh *fh_src, struct ddumb_fh *fh_dst)
{
    struct xstat *xstat=fh_dst->xstat=fh_src->xstat;
    char *buf=fh_dst->buf;
    fh_dst->fd=fh_src->fd;
    fh_dst->buf=fh_src->buf;
    fh_dst->buf_off=fh_src->buf_off;
    fh_dst->fh_src=fh_src;

    // add fh_dst to fh_src->xstat->fhs
    pthread_mutex_lock_d(&xstat->xstat_lock);
    fh_src->buf=buf;
    if (xstat->fhs_n>=xstat->fhs_max)
    {   // double the size of fhs
        ddumb_statistic.xstat_resize++;
        struct ddumb_fh **fhs=realloc(xstat->fhs, 2*xstat->fhs_max*sizeof(struct ddumb_fh *));
        if (fhs==NULL)
        {
            pthread_mutex_unlock_d(&xstat->xstat_lock);
            DDFS_LOG(LOG_ERR, "xstat_subscribe not enough memory: %s\n", fh_src->filename);
            return -ENOMEM;
        }
        xstat->fhs=fhs;
        xstat->fhs_max*=2;
    }
    xstat->fhs[xstat->fhs_n++]=fh_dst;

    fh_dst->buf_loaded=DDFS_BUF_RDONLY;
    fh_src->buf_loaded=DDFS_BUF_EMPTY;

    pthread_mutex_unlock_d(&xstat->xstat_lock);
    DDFS_LOG_DEBUG("[%lu]++  xstat_subscribe fd=%d fh=%p fh_src=%p xstat=%p ino=%lld fhs_n=%d %s %s\n", thread_id(), fh_dst->fd, fh_dst, fh_src, fh_dst->xstat, fh_dst->xstat->ino, fh_dst->xstat->fhs_n, fh_dst->filename, fh_src->filename);
    return 0;
}

static void xstat_unsubscribe(struct ddumb_fh *fh)
{
    struct xstat *xstat=fh->xstat;
    DDFS_LOG_DEBUG("[%lu]++  xstat_unsubscribe fd=%d fh=%p fh_src=%p xstat=%p ino=%lld %s\n", thread_id(), fh->fd, fh, fh->fh_src, fh->xstat, xstat->ino, fh->filename);


    int i, found;
    pthread_mutex_lock_d(&xstat->xstat_lock);
    for(i=0, found=0; i<xstat->fhs_n; i++)
    {
        if (xstat->fhs[i]==fh)
        {   // move last entry into the free one
            found=1;
            xstat->fhs_n--;
            if (i<xstat->fhs_n) xstat->fhs[i]=xstat->fhs[xstat->fhs_n];
            break;
        }
    }
    assert(xstat->fhs_n>0); // fh->fh_src is still in fh->xstat->fhs
    pthread_mutex_unlock_d(&xstat->xstat_lock);
    assert(found);
}

static int xstat_get(struct xstat_root *root, struct xstat *xstat, const char *path)
{
    // get xstat from tsearch tree or from header file if not found
    // idem xstat_load but don't need xstat_release, use tfind instead of tsearch, this is read only
    int res=0;
    pthread_mutex_lock_d(&(root->mutex));

    void *val=tfind((const void *)xstat, &(root->root), xstat_compare);
//  DDFS_LOG_DEBUG("[%lu]++  xstat_get tsearch root=%p xtstat=%p  -> xstat=%p\n", thread_id(), (void *)root->root, (void *)root->xstat, (void*)*(struct xstat**)val);
    if (val==NULL)
    {
        int fd=open(path, O_RDONLY);
        if (fd==-1)
        {
            DDFS_LOG(LOG_ERR, "xstat_get: cannot open %s to read header: %s\n", path, strerror(errno));
            res=-errno;
        }
        else
        {
            res=xstat_load(fd, xstat, path);
            close(fd);
        }
    }
    else
    {
        file_header_copy(&xstat->h, &(*(struct xstat**)val)->h);
    }
    pthread_mutex_unlock_d(&(root->mutex));
    return res;
}

int xstat_release(struct xstat_root *root, struct ddumb_fh *fh)
{
    int res=0;
    void *p;

    struct xstat *xstat=fh->xstat;
    DDFS_LOG_DEBUG("[%lu]++  xstat_release fd=%d fh=%p xstat=%p ino=%lld root->xstat=%p %s\n", thread_id(), fh->fd, fh, fh->xstat, xstat->ino, root->xstat, fh->filename);
//DDFS_LOG(LOG_NOTICE, "[%lu]++  xstat_release fd=%d fh=%p xstat=%p ino=%lld root->xstat=%p %s\n", thread_id(), fh->fd, fh, fh->xstat, xstat->ino, root->xstat, fh->filename);

    pthread_mutex_lock_d(&(root->mutex));

    if (xstat->fhs_n==1)
    {
        // cannot update header if file was open readonly
        if (!fh->rdonly && !xstat->saved) res=xstat_save(fh->fd, fh->xstat, fh->filename);
        p=tdelete(fh->xstat, &(root->root), xstat_compare);
        ddumb_statistic.inode_counter--;
        if (p==NULL)
        {
            DDFS_LOG(LOG_ERR, "xstat_release: xstat not found, continue: fh=%p xstat=%p %s\n", fh, fh->xstat, fh->filename);
//            xstat_dump(root);
        }

        // double check if the last one is fh
        assert(xstat->fhs[0]==fh);

        pthread_mutex_destroy(&xstat->xstat_lock);
        pthread_cond_destroy(&xstat->zone_cond);
        pthread_cond_destroy(&xstat->buf_cond);

        // reuse this xstat pointer if possible, else free it
        if (root->xstat==NULL) root->xstat=fh->xstat;
        else
        {
            free(xstat->fhs);
            xstat->fhs=NULL;
            free(xstat);
            fh->xstat=NULL;
        }
    }
    else
    {
        int i, found;
        pthread_mutex_lock_d(&xstat->xstat_lock);
        for(i=0, found=0; i<xstat->fhs_n; i++)
        {
            if (xstat->fhs[i]==fh)
            {   // move last entry into the free one
                found=1;
                xstat->fhs_n--;
                if (i<xstat->fhs_n) xstat->fhs[i]=xstat->fhs[xstat->fhs_n];
                break;
            }
        }
        pthread_mutex_unlock_d(&xstat->xstat_lock);
        assert(found);
    }

//xstat_dump(root);
    pthread_mutex_unlock_d(&(root->mutex));
    return res;
}

/*
static int zone_overlap(long long int offset1, long long int size1, long long int offset2, long long int size2)
{   // never used/tested !!!!
    return ((offset1<=offset2 && offset2<offset1+size1) ||
            (offset2<=offset1 && offset1<offset2+size2));
}
*/

static void xzone_lock(struct ddumb_fh *fh, long long int offset, long long int size, char op)
{   // lock a zone
    struct xstat *xstat=fh->xstat;
    char right='n';

    ddumb_statistic.lock_zone++;

    pthread_mutex_lock_d(&xstat->xstat_lock);
    fh->zone.op=op;

    int busy=1; // zone is already in use
    while (busy)
    {
        if (op=='W')
        {   // write
            if (offset<xstat->h.size) fh->zone.start=(offset & ddfs->block_boundary_mask);
            else fh->zone.start=(xstat->h.size & ddfs->block_boundary_mask);
            if (offset+size>xstat->h.size) fh->zone.end=-1;
            else fh->zone.end=((offset+size+ddfs->c_block_size-1) & ddfs->block_boundary_mask);
            right='w';
        }
        else if (op=='R')
        {   // read
            fh->zone.start=(offset & ddfs->block_boundary_mask);
            fh->zone.end=((offset+size+ddfs->c_block_size-1) & ddfs->block_boundary_mask);
            right='r';
        }
        else if (op=='T')
        {   // truncate
            if (offset<xstat->h.size) fh->zone.start=(offset & ddfs->block_boundary_mask);
            else fh->zone.start=(xstat->h.size & ddfs->block_boundary_mask);
            fh->zone.end=-1;
            right='w';
        }
        else // unknown operation
        {
            DDFS_LOG(LOG_ERR, "[%lu]ZONE LOCK unknow operation '%c' : fh=%p fd=%d ino=%lld start=0x%llx(%lld) end=0x%llx(%lld) %s\n", thread_id(), fh->zone.op, fh, fh->fd, (long long int)xstat->ino, fh->zone.start, fh->zone.start, fh->zone.end, fh->zone.end, fh->filename);
            assert(0);
        }
        int i;
        struct ddumb_fh *xfh;
        for(i=0, busy=0; !busy && i<xstat->fhs_n; i++)
        {
            xfh=xstat->fhs[i];
            if (xfh->zone.right=='n') continue;

            busy=((xfh->zone.start<=fh->zone.start && (fh->zone.start<xfh->zone.end || xfh->zone.end==-1)) ||
                  (fh->zone.start<=xfh->zone.start && (xfh->zone.start<xfh->zone.end || fh->zone.end==-1))) &&
                  (right!='r' || xfh->zone.right!='r');
        }
        if (busy)
        {
            ddumb_statistic.wait_for_zone++;
            DDFS_LOG_DEBUG("[%lu]ZONE WAIT op=%c fd=%d ino=%lld start=0x%llx(%lld) end=0x%llx(%lld) fh=%p xfh=%p xop=%c xstart=0x%llx xend=0x%llx %s\n",
                    thread_id(), fh->zone.op, fh->fd, (long long int)xstat->ino, fh->zone.start, fh->zone.start, fh->zone.end, fh->zone.end, fh, xfh, xfh->zone.op, xfh->zone.start, xfh->zone.end, fh->filename);
            pthread_cond_wait_d(&xstat->zone_cond, &xstat->xstat_lock);
        }

    }
    fh->zone.right=right;
    pthread_mutex_unlock_d(&xstat->xstat_lock);
    DDFS_LOG_DEBUG("[%lu]ZONE LOCK fh=%p op=%c fd=%d ino=%lld start=0x%llx(%lld) end=0x%llx(%lld) %s\n", thread_id(), fh, fh->zone.op, fh->fd, (long long int)xstat->ino, fh->zone.start, fh->zone.start, fh->zone.end, fh->zone.end, fh->filename);
}

static void xzone_unlock(struct ddumb_fh *fh)
{   // unlock a zone
    pthread_mutex_lock_d(&fh->xstat->xstat_lock);
    DDFS_LOG_DEBUG("[%lu]ZONE UNLOCK fh=%p op=%c ino=%lld start=0x%llx(%lld) end=0x%llx(%lld) %s\n", thread_id(), fh, fh->zone.op, (long long int)fh->xstat->ino, fh->zone.start, fh->zone.start, fh->zone.end, fh->zone.end, fh->filename);
    fh->zone.op='N';
    fh->zone.right='n';
    pthread_cond_broadcast(&fh->xstat->zone_cond);
    pthread_mutex_unlock_d(&fh->xstat->xstat_lock);
}

#define BLOCK_MAX 128
long long int block_locked[BLOCK_MAX];
long long int block_locked_n=0;
pthread_mutex_t block_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t block_cond=PTHREAD_COND_INITIALIZER;

static void block_lock(long long int baddr)
{
    pthread_mutex_lock_d(&block_mutex);
    while (block_locked_n>=BLOCK_MAX) pthread_cond_wait_d(&block_cond, &block_mutex);
    block_locked[block_locked_n++]=baddr;
    if (block_locked_n>ddumb_statistic.block_locked_max) ddumb_statistic.block_locked_max=block_locked_n;
    DDFS_LOG_DEBUG("block_lock %lld\n", baddr);
    pthread_mutex_unlock_d(&block_mutex);
}

static void block_unlock(long long int baddr)
{
    pthread_mutex_lock_d(&block_mutex);
    int found=0;
    int i;
    for (i=0; i<block_locked_n; i++)
    {
        if (block_locked[i]==baddr)
        {
            found=1;
            block_locked_n--;
            if (i!=block_locked_n) block_locked[i]=block_locked[block_locked_n];
            pthread_cond_broadcast(&block_cond);
            break;
        }
    }
    assert(found!=0);
    DDFS_LOG_DEBUG("block_unlock %lld\n", baddr);
    pthread_mutex_unlock_d(&block_mutex);
}

static void block_wait(long long int baddr)
{
    pthread_mutex_lock_d(&block_mutex);
    int i;
    for (i=0; i<block_locked_n; i++)
    {
        if (block_locked[i]==baddr)
        {
            DDFS_LOG(LOG_WARNING, "block_wait: I WAS RIGHT TO CHECK FOR THIS VERY UNLIKELY RACE CONDITION! block=%lld\n", baddr);
            pthread_cond_wait_d(&block_cond, &block_mutex);
            i=-1;
        }
    }
    pthread_mutex_unlock_d(&block_mutex);
}


/*
 * ddumb_*_fh
 */
struct ddumb_fh *ddumb_alloc_fh(struct fuse_file_info *fi, int fd, int rdonly, int special, const char* filename)
{   // alloc a new ddumb_fh and attache it to the fi
    int res=0;

    struct ddumb_fh *fh=malloc(sizeof(struct ddumb_fh));
    if (fh)
    {
        fh->fd=fd;
        fh->buf_loaded=DDFS_BUF_EMPTY;
        fh->pool_status=ps_empty;
        fh->buf_off=-1;
        fh->pool_writer=0;
        fh->rdonly=rdonly;
        fh->special=special;
        fh->buf=NULL;
        fh->xstat=NULL;
        fh->delayed_write_error_code=0;
        fh->zone.op='N';
        fh->zone.right='n';
        fh->filename=strdup(filename); // don't care about the failure, just for friendly error message
        if (!special)
        {
            if (ddfs->direct_io || ddfs->align)
            {
                res=posix_memalign((void *)&fh->buf, BLOCK_ALIGMENT, ddfs->c_block_size);
                if (res)
                {
                    DDFS_LOG(LOG_ERR, "ddumb_alloc_fh: cannot allocate aligned buffer: %s (%s)\n", filename, strerror(res));
                }
            }
            else
            {
                fh->buf=malloc(ddfs->c_block_size);
            }

            if (fd!=-1)
            {   // not attached to the writer_pool
                if (fh->buf) res=xstat_register(&xstat_root, fh);
                else DDFS_LOG(LOG_ERR, "ddumb_alloc_fh: buffer malloc failed: %s\n", filename);
            }

            if (fh->buf==NULL || (fd!=-1 && fh->xstat==NULL) || res<0)
            {
                free(fh->buf);
                if (fh->filename) free(fh->filename);
                free(fh);
                fh=NULL;
            }
            else
            {
                ddumb_statistic.fh_counter++;
                pthread_mutex_init(&fh->lock, 0);
                pthread_cond_init(&fh->pool_cond, NULL);

                DDFS_LOG_DEBUG("[%lu]pthread_mutex_init lock %p\n", thread_id(), (void*)&fh->lock);
            }
        }
    }
    else DDFS_LOG(LOG_ERR, "ddumb_alloc_fh: malloc failed: %s\n", filename);

//DDFS_LOG(LOG_NOTICE, "[%lu]ddumb_alloc_fh: fh=%p (%s)\n", thread_id(), fh, filename);
    fi->fh=(uintptr_t)fh;
    // FIXME: res must be reorted as an error
    return fh;
}

static int ddumb_free_fh(struct fuse_file_info *fi)
{   // free the ddumb_fh attached to the fi
    int res=0;

    struct ddumb_fh *fh=ddumb_get_fh(fi);
    if (fh)
    {
//DDFS_LOG(LOG_NOTICE, "[%lu]ddumb_free_fh: fh=%p (%s)\n", thread_id(), fh, fh->filename);
        char *filename=fh->filename; // for debugging to work free it at the end
        if (!fh->special)
        {
            res=xstat_release(&xstat_root, fh);
            free(fh->buf);
            fh->buf=NULL;
            pthread_mutex_destroy(&fh->lock);
            pthread_cond_destroy(&fh->pool_cond);
            free(fh);
            ddumb_statistic.fh_counter--;
        }
        if (filename) free(filename);
    }
    fi->fh=(unsigned long)NULL;
    return res;
}


int tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{   // called by nftw to walk the tree
    int res, len;
    unsigned char node[NODE_SIZE];
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr;
    long long int prev_addr=-1;

    if (typeflag!=FTW_F || ! S_ISREG(sb->st_mode)) return 0;
    if (0==strncmp(fpath+ddfs->rdir_len, SPECIAL_DIR, ddfs->special_dir_len))
    {   // skip SPECIAL_DIR
        return 0;
    }

    r_file_count++;

    FILE *file=fopen(fpath, "r");  // FILE* are faster here
    if (file==NULL)
    {
        DDFS_LOG(LOG_ERR, "reclaim cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    uint64_t file_size;
    len=file_header_set_conv(fileno(file), &file_size);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "reclaim cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        DDFS_LOG(LOG_ERR, "reclaim invalid ddumbfs header, skip file: %s\n", fpath);
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "reclaim cannot seek after header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    long long int offset=ddfs->c_file_header_size;
    len=fread(node, 1, ddfs->c_node_size, file);
    while (len==ddfs->c_node_size)
    {
        addr=ddfs_get_node_addr(node);

        if (prev_addr!=-1 && prev_addr+1!=addr && addr!=0) r_frag_count++;
        if (addr!=0) prev_addr=addr;

        if (addr>ddfs->c_block_count)
        {
            DDFS_LOG(LOG_WARNING, "reclaim invalid block address %lld: %s\n", addr, fpath);
        }
        else if (addr!=0)
        {
            // fprintf(stderr,"addr=%lld\n", addr);
            r_addr_count++;

            // search the index, if the same hash can be found for another block
            // this will avoid to maybe loose useful information for next fsck
            blockaddr baddr=addr;

            // Only search the index if we have it locked in memory
            if(ddfs->lock_index) {
                pthread_mutex_lock_d(&ifile_mutex);
                nodeidx node_idx=ddfs_search_hash(hash, &baddr);
                if (node_idx<-1) r_not_found++; // >=0 means found, -1 means == zeroes block
                pthread_mutex_unlock_d(&ifile_mutex);
            }

            pthread_spin_lock(&reclaim_spinlock);
            bit_array_set(&ba_found_in_files, addr);
            // if another block was found, keep it too. I don't want to loose information
            // that could be verified and maybe useful for fsck.
            if (baddr!=addr) bit_array_set(&ba_found_in_files, baddr); // FYI block 0 can be set here when node_idx==-1
            pthread_spin_unlock(&reclaim_spinlock);
        }
        offset+=len;
        len=fread(node, 1, ddfs->c_node_size, file);
    }

    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "reclaim cannot read: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    if (len && len!=ddfs->c_node_size)
    {   // maybe the file has been truncated during the read
        // get the new file size and compare with faulty offset
        struct stat st;
        if (fstat(fileno(file), &st)==0) file_size=st.st_size;
        if (offset!=file_size)
        {
            DDFS_LOG(LOG_WARNING, "reclaim short read node (%d/%d): %s:%lld/%lld\n", len, ddfs->c_node_size, fpath, offset, (long long int)sb->st_size);
        }
        // the opposite situation is not a problem because new blocks
        // are registered into ba_found_in_files when reclaim is running.
    }
    fclose(file);
    return 0;
}

int ddumbfs_save_usedblocks(int limit)
{   // if required, save used block list
    int res=0;

//    DDFS_LOG_DEBUG("save used block %lld-%lld>%d\n", ddfs->usedblock, used_block_saved, limit);
    if (llabs(ddfs->usedblock-used_block_saved)>=limit)
    {
	time_t start_time=time(NULL);
        res=ddfs_save_usedblocks();
        if (res==0)
        {
            used_block_saved=ddfs->usedblock;
            DDFS_LOG(LOG_INFO, "save used block list in %d seconds: %lld blocks in use\n", (int)(time(NULL)-start_time), ddfs->usedblock);
        }
    }

//    printf(fstderr, "ddfs->usedblock=%lld next_reclaim=%d\n", ddfs->usedblock, next_reclaim);
    if (ddfs->usedblock*100LL>=next_reclaim*ddfs->c_block_count && reclaim_could_find_free_blocks)
    {   // need to call reclaim()
        pthread_cond_signal(&ddumb_background_cond);
    }

    return res;
}

void reclaim(FILE *output)
{   // the reclaim() process
    int res;
    int success=0;

    DDFS_LOG(LOG_INFO, "reclaim started (online).\n");
    L_SYS(LOG_INFO, "online reclaim started on: %s\n", ddfs->pdir);

    // paranoid test
    char *specialdir=SPECIAL_DIR;
    if (!isdir(specialdir+1))
    {
        DDFS_LOG(LOG_ERR, "system directory %s not found in root!\n", SPECIAL_DIR);
        return;
    }

    // reclaim() must wait for everybody to have left ddumb_buf_write()
    // before to enter this part (up to unlock reclaim_mutex)
    pthread_mutex_lock_d(&reclaim_mutex);
    while (reclaim_ddumb_buf_write_is_in_use>0) pthread_cond_wait_d(&reclaim_ddumb_buf_write_is_unused, &reclaim_mutex);
    reclaim_ddumb_buf_write_is_in_use++; // don't enter in reclaim() when it is already running

    pthread_spin_lock(&reclaim_spinlock);
    reclaim_enable=1;
    bit_array_reset(&ba_found_in_files, 0);
    bit_array_set(&ba_found_in_files, 0); // 0 is reserved
    bit_array_set(&ba_found_in_files, 1); // 1 is reserved
    pthread_spin_unlock(&reclaim_spinlock);

    pthread_mutex_unlock_d(&reclaim_mutex);

    // Collect blocks from file (but also from index, when index has a different blockidx for the same hash)
    r_file_count=r_addr_count=r_frag_count=r_not_found=0;
    long long int start=now();
    res=nftw(ddfs->rdir, tree_explore, 10, FTW_MOUNT | FTW_PHYS);
    long long int end=now();

    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "reclaim error, cannot run, correct and check filesystem\n");
        L_SYS(LOG_ERR, "reclaim error, cannot run, correct and check filesystem\n");
        if (output) fprintf(output, "error: reclaim cannot run, correct errors and check the filesystem\n");
    }
    else
    {
        long long int block_allocated, block_not_allocated;
        long long int block_in_use, block_not_in_use;
        DDFS_LOG(LOG_INFO, "reclaim read used blocks from %lld files in %.1fs\n", r_file_count, (end-start)*1.0/NOW_PER_SEC);
        if (output) fprintf(output, "== Read used blocks from files in %.1fs\n", (end-start)*1.0/NOW_PER_SEC);

        int cmp, cmp_res;

        pthread_mutex_lock_d(&ifile_mutex);
        pthread_spin_lock(&reclaim_spinlock);
        cmp_res=bit_array_cmp(&ba_found_in_files, &ddfs->ba_usedblocks, &cmp);
        bit_array_count(&ddfs->ba_usedblocks, &block_allocated, &block_not_allocated);
        bit_array_count(&ba_found_in_files, &block_in_use, &block_not_in_use);
        pthread_spin_unlock(&reclaim_spinlock);
        pthread_mutex_unlock_d(&ifile_mutex);

        if (!cmp_res || cmp>0)
        {
            DDFS_LOG(LOG_ERR, "reclaim inconsistency between allocated and used blocks, filesystem check required\n");
            L_SYS(LOG_ERR, "reclaim inconsistency between allocated and used blocks, filesystem check required\n");
            if (output) fprintf(output, "Critical error: inconsistency between allocated and used blocks, filesystem check required\n");
        }
        else
        {
            long long int r_addr_count_nz=r_addr_count;
            if (r_addr_count_nz==0) r_addr_count_nz=1;

            DDFS_LOG(LOG_INFO, "reclaim   block_allocated=%9lld  block_not_allocated=%9lld\n", block_allocated, block_not_allocated);
            DDFS_LOG(LOG_INFO, "reclaim      block_in_use=%9lld        block_to_free=%9lld\n", block_in_use, block_allocated-block_in_use);
            DDFS_LOG(LOG_INFO, "reclaim    hash_not_found=%9lld                files=%9lld\n", r_not_found, r_file_count);
            DDFS_LOG(LOG_INFO ,"reclaim block_references=%10lld        fragmentation=%8.1f%%\n", r_addr_count, r_frag_count*100.0/r_addr_count_nz);
            if (output) fprintf(output, "%-30s %9lld\n", "block_allocated", block_allocated);
            if (output) fprintf(output, "%-30s %9lld\n", "block_not_allocated", block_not_allocated);
            if (output) fprintf(output, "%-30s %9lld\n", "block_in_use", block_in_use);
            if (output) fprintf(output, "%-30s %9lld\n", "block_not_in_use", block_not_in_use);
            if (output) fprintf(output, "%-30s %9lld\n", "hash_not_found", r_not_found);
            if (output) fprintf(output, "%-30s %9lld\n", "files", r_file_count);
            if (output) fprintf(output, "%-30s %9lld\n", "block_references", r_addr_count);
            if (output) fprintf(output, "%-30s %8.1f%%\n", "fragmentation", r_frag_count*100.0/r_addr_count_nz);

            start=now();

            long long int node_count=0;
            long long int node_deleted=0;
            long long int node_idx=0;
            // read all nodes, delete unused blocks
            while (node_idx<ddfs->c_node_count)
            {
                if (!ddfs->lock_index)
                {
                    preload_node(node_idx);  // pre-load into cache before to lock
                }
                pthread_mutex_lock_d(&ifile_mutex);
                long long int addr=ddfs_get_node_addr(ddfs->nodes+(node_idx*ddfs->c_node_size));
                if (addr==0)
                {
                    node_idx++;
                }
                else
                {
                    pthread_spin_lock(&reclaim_spinlock);
                    int found=bit_array_get(&ba_found_in_files, addr);
                    pthread_spin_unlock(&reclaim_spinlock);
                    if (found)
                    {
                        node_count++;
                        node_idx++;
                    }
                    else
                    {
                        node_delete(node_idx);
                        bit_array_unset(&ddfs->ba_usedblocks, addr);
                        node_deleted++;
                    }
                }
                // don't increment node_idx, because nodes have moved up and current node is a new node
                pthread_mutex_unlock_d(&ifile_mutex);
            }
            end=now();
            success=1;
            ddumb_statistic.reclaim++;
            if (output) fprintf(output, "== Index cleanup in %.1fs\n", (end-start)*1.0/NOW_PER_SEC);
            L_SYS(LOG_INFO, "reclaim     node_in_index=%9lld         node_deleted=%9lld\n", node_count, node_deleted);
            DDFS_LOG(LOG_INFO, "reclaim     node_in_index=%9lld         node_deleted=%9lld\n", node_count, node_deleted);
            if (output) fprintf(output, "%-30s %9lld\n", "nodes_in_index", node_count);
            if (output) fprintf(output, "%-30s %9lld\n", "node_deleted", node_deleted);
        }
    }
    pthread_spin_lock(&reclaim_spinlock);
    reclaim_enable=0;
    pthread_spin_unlock(&reclaim_spinlock);

    pthread_mutex_lock_d(&ifile_mutex);
    long long int _u;
    bit_array_count(&ddfs->ba_usedblocks, &ddfs->usedblock, &_u);
    if (ddfs->usedblock*100LL>=next_reclaim*ddfs->c_block_count)
    {   // still bigger then
        if (next_reclaim==99) next_reclaim=100;
        else next_reclaim=(next_reclaim+100)/2;
    }
    else
    {   // reduce next_reclaim
        next_reclaim=ddumb_param.reclaim;
        while (ddfs->usedblock>=next_reclaim*ddfs->c_block_count/100) next_reclaim=(next_reclaim+100)/2;
    }
    reclaim_could_find_free_blocks=0;

    ddumbfs_save_usedblocks(0);
    if (success && ddfs->c_reuse_asap)
    {
        ddfs->ba_usedblocks.index=DDFS_LAST_RESERVED_BLOCK+1; // reuse recently freed blocks ASAP
    }
    pthread_mutex_unlock_d(&ifile_mutex);

    pthread_mutex_lock_d(&reclaim_mutex);
    reclaim_ddumb_buf_write_is_in_use--; // you can call reclaim() again
    pthread_mutex_unlock_d(&reclaim_mutex);

}

static int ddumb_simple_block_read(struct ddumb_fh *fh, char *buf, long long int offset, long long int size)
{
    unsigned char baddr[ADDR_SIZE];
    long long int gap=(offset & ddfs->block_gap_mask);

    DDFS_LOG_DEBUG("[%lu]++  ddumb_simple_block_read fd=%d fh=%p offset=0x%llx(%lld) size=0x%llx(%lld) gap=%lld %s\n", thread_id(), fh->fd, (void*)fh, offset, offset, size, size, gap, fh->filename);

    // read the address of the block in the block file
    long long int idx_off=(offset>>ddfs->block_size_shift)*ddfs->c_node_size+ddfs->c_file_header_size;

    int len=pread(fh->fd, baddr, ddfs->c_addr_size, idx_off);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "ddumb_simple_block_read pread %s (%s)\n", fh->filename, strerror(errno));
        return -errno;
    }
    else if (len==0)
    {
        DDFS_LOG(LOG_ERR, "ddumb_simple_block_read read 0 bytes, maybe end of file. file_size=%lld rdonly=%d op=%c(%lld-%lld) %s:%lld\n", (long long int)fh->xstat->h.size, fh->rdonly, fh->zone.op, fh->zone.start, fh->zone.end, fh->filename, offset);
        return -EIO;
    }
    else if (len!=ddfs->c_addr_size)
    {
        DDFS_LOG(LOG_ERR, "ddumb_simple_block_read short read %d/%d %s:%lld\n", len, ddfs->c_addr_size, fh->filename, offset);
        return -EIO;
    }

    long long int block_addr=ddfs_get_node_addr(baddr);

    if (block_addr==0)
    {   // addr==0 means block '\0......\0',  ddfs_read_block() is optimized for such block
        ddumb_statistic.block_read_zero++;
        DDFS_LOG_DEBUG("[%lu]-- ddumb_simple_block_read read ZERO block fd=%d fh=%p\n", thread_id(), fh->fd, (void*)fh);
        DDFS_LOG_DEBUG("[%lu]-- ddumb_simple_block_read MEMSET fd=%d fh=%p from=0x%llx(%lld) to=0x%llx(%lld)\n", thread_id(), fh->fd, (void*)fh, offset, offset, offset+size, offset+size );
    }

    // just to avoid a very improbable race condition with ddfs_write_block2()
    block_wait(block_addr); // if this block is being written, wait for the end of the write

    len=ddfs_read_block(block_addr, buf, size, gap);

    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "ddumb_simple_block_read block off=%lld in file %s:%lld (%s)\n", block_addr*ddfs->c_block_size+gap, fh->filename, offset, strerror(errno));
        return -errno;
    }
    else if (len!=size)
    {
        DDFS_LOG(LOG_ERR, "ddumb_simple_block_read short read block off=%lld %d/%lld in file %s:%lld\n", block_addr*ddfs->c_block_size+gap, len, size, fh->filename, offset);
        return -EIO;
    }

    if (block_addr!=0)
    {
        DDFS_LOG_DEBUG("[%lu]--  ddumb_simple_block_read fd=%d fh=%p offset=0x%llx(%lld) size=0x%llx(%lld) gap=%lld data=0x%llx %s\n", thread_id(), fh->fd, (void*)fh, offset, offset, size, size, gap, *(long long int*)fh->buf, fh->filename);
    }

    return 0;
}

static int ddumb_block_read(struct ddumb_fh *fh, char *buf, long long int offset, long long int size)
{
    long long int gap=(offset & ddfs->block_gap_mask);
    long long int block_boundary=(offset & ddfs->block_boundary_mask);
    struct xstat *xstat=fh->xstat;
    ddumb_statistic.block_read++;

    DDFS_LOG_DEBUG("[%lu]++  ddumb_block_read fd=%d offset=0x%llx(%lld) size=0x%llx(%lld) gap=%lld fhs_n=%d %s\n", thread_id(), fh->fd, offset, offset, size, size, gap, xstat->fhs_n, fh->filename);

    // search if the required block is already somewhere in one open fh
    int i, found;
    pthread_mutex_lock_d(&xstat->xstat_lock);
    for(i=0, found=0; !found && i<xstat->fhs_n; i++)
    {
        struct ddumb_fh *xfh=xstat->fhs[i];
//        DDFS_LOG_DEBUG("[%lu]>>>>ddumb_block_read fh=%p xfh=%p buf_loaded=%d buf_off=0x%llx(%lld) I=%d \n", thread_id(), (void*)fh, (void*)xfh, xfh->buf_loaded, xfh->buf_off, xfh->buf_off, i);
        if (xfh->buf_loaded && xfh->buf_off==block_boundary)
        {
            found=1;
            memcpy(buf, xfh->buf+gap, size);
            DDFS_LOG_DEBUG("[%lu]--  ddumb_block_read fd=%d fh=%p FOUND in buffer xstat=%p xfd=%d xfh=%p I=%d %s\n", thread_id(), fh->fd, (void*)fh, (void*)fh->xstat, xfh->fd, (void*)xfh, i, fh->filename);
        }
    }
    pthread_mutex_unlock_d(&xstat->xstat_lock);
    if (found) return 0;

    return ddumb_simple_block_read(fh, buf, offset, size);
}

/**
 * write a block in the filesystem
 *
 * identical to ddfs_write_block, but handle statistics and multi-threading stuff
 *
 * @param block the block
 * @param bhash return the hash of the block
 * @return the address of the block or <0 for error
 */
long long int ddfs_write_block2(const char *block, unsigned char *bhash, struct ddumb_fh *fh)
{
    long long int addr;
    long long int node_idx;

    ddfs_hash(block, bhash);
    ddumb_statistic.hash++;

    if (memcmp(bhash, ddfs->zero_block_hash, ddfs->c_hash_size)==0) return 0;

    if (!ddfs->lock_index)
    {
        // preload the page where the node is before to lock to reduce the lock time
        // node_idx is calculated twice, but....
        preload_node(ddfs_hash2idx(bhash));
    }

    pthread_mutex_lock_d(&ifile_mutex);

    int res=ddfs_locate_hash(bhash, &addr, &node_idx);

    if (res<0)
    {
        pthread_mutex_unlock_d(&ifile_mutex);
        return res;
    }

    if (res==0)
    {   // the block is already stored, return its current address
        pthread_mutex_unlock_d(&ifile_mutex);
        ddumb_statistic.ghost_write++;
        return addr;
    }

    // update statistic
    ddumb_statistic.block_write_try_next_node+=res-1; // should use the number of try in ddfs_locate_hash

    // save the used block list at regular interval
    ddumbfs_save_usedblocks(16000 * (131072 / ddfs->c_block_size));

    // the hash is not found, search a free block in the BlockFile
    long long int baddr=ddfs_alloc_block();
    if (baddr<0)
    {
        pthread_mutex_unlock_d(&ifile_mutex);
        if (!no_more_free_block_warning)
        {
            no_more_free_block_warning=1;
            DDFS_LOG(LOG_ERR, "no more free block, you must start \"reclaim\" procedure !\n");
        }
        return baddr;
    }
    no_more_free_block_warning=0;

    // I'm using this block, don't free it if reclaim is running
    // this is the first place where ba_found_in_files is updated
    pthread_spin_lock(&reclaim_spinlock);
    if (reclaim_enable && baddr!=-1) bit_array_set(&ba_found_in_files, baddr);
    pthread_spin_unlock(&reclaim_spinlock);

    long long int free_node_idx=-1;
    if (addr!=0)
    {   // we must insert the node, search for a free node
        free_node_idx=ddfs_search_free_node(node_idx+1, ddfs->c_node_count);
        if (free_node_idx<0)
        {   // this should NEVER NERVER NEVER append
            pthread_mutex_unlock_d(&ifile_mutex);
            return -EIO;
        }
        memmove(ddfs->nodes+((node_idx+1)*ddfs->c_node_size), ddfs->nodes+(node_idx*ddfs->c_node_size), (free_node_idx-node_idx)*ddfs->c_node_size);
        ddumb_statistic.block_write_slide++;
    }

    // now node_idx is ready to receive new node
    ddfs_set_node(node_idx, baddr, bhash);

    block_lock(baddr);  // avoid this block to be read when being written

    pthread_mutex_unlock_d(&ifile_mutex);

    // now you can read the index, but not yet the blockfile because data are not yet in sync with the index
    // hopefully the block is locked by block_lock.
    // Else a unlikely race condition is possible :
    // process 0 is "stopped" here
    // process 1 search for the same hash, find it, and write the address in another file.
    // process 2 read the file of process 1, get the address and read the block before ...
    // process 0 has written the block

    baddr=ddfs_store_block(block, baddr);

    block_unlock(baddr);

    return baddr;
}


static int ddumb_buf_write(struct ddumb_fh *fh)
{
    unsigned char node[NODE_SIZE];

    int len;
    int ret=0;

    // I cannot enter this function when reclaim() is _starting_ and reclaim()
    // must wait for everybody to have left this function before to start
    pthread_mutex_lock_d(&reclaim_mutex);
    reclaim_ddumb_buf_write_is_in_use++;
    pthread_mutex_unlock_d(&reclaim_mutex);

    ddumb_statistic.block_write++;

    long long int addr=ddfs_write_block2(fh->buf, node+ddfs->c_addr_size, fh);
    if (addr<0)
    {
        ret=addr;
    }
    else
    {
        // addr is already registered into ba_found_in_files by index_new_block

        DDFS_LOG_DEBUG("[%lu]++  ddumb_buf_write fh=%p fd=%d offset=0x%llx(%lld) addr=%lld data=0x%llx %s\n", thread_id(), (void*)fh, fh->fd, (long long int)fh->buf_off, (long long int)fh->buf_off, addr, *(long long int*)fh->buf, fh->filename);

        ddfs_convert_addr(addr, node);

        off_t addr_off=(fh->buf_off>>ddfs->block_size_shift)*ddfs->c_node_size+ddfs->c_file_header_size;

        len=pwrite(fh->fd, node, ddfs->c_node_size, addr_off);

        pthread_spin_lock(&reclaim_spinlock);
        // This is 2nd place where ba_found_in_files is updated
        if (reclaim_enable) bit_array_set(&ba_found_in_files, addr);
        pthread_spin_unlock(&reclaim_spinlock);

        if (len==-1)
        {
            DDFS_LOG(LOG_ERR, "ddumb_buf_write addr offset=%lld %s (%s)\n", (long long int)addr_off, fh->filename, strerror(errno));
            ret=-errno;
        }
        else if (len!=ddfs->c_node_size)
        {
            DDFS_LOG(LOG_ERR, "ddumb_buf_write addr offset=%lld wrote only %d/%d %s\n", (long long int)addr_off, len, ddfs->c_node_size, fh->filename);
            ret=-EIO;
        }
    }

    pthread_mutex_lock_d(&fh->xstat->xstat_lock);
    // buffer has been written (or not) and don't contain anything useful now
    fh->buf_loaded=DDFS_BUF_EMPTY;
    // warn everybody about the change
    pthread_cond_broadcast(&fh->xstat->buf_cond);
    pthread_mutex_unlock_d(&fh->xstat->xstat_lock);

    pthread_mutex_lock_d(&reclaim_mutex);
    reclaim_ddumb_buf_write_is_in_use--;
    if (reclaim_ddumb_buf_write_is_in_use==0) pthread_cond_signal(&reclaim_ddumb_buf_write_is_unused);
    pthread_mutex_unlock_d(&reclaim_mutex);

    return ret;
}

void pool_dump()
{
    int i;
    DDFS_LOG(LOG_NOTICE, "[%lu]   writers_fh_count=%3d writers_fh_availabel=%3d writers_fh_n_ready=%3d\n", thread_id(), writers_fh_count, writers_fh_n_empty, writers_fh_n_ready);
    for (i=0; i<writers_fh_count; i++)
    {
        DDFS_LOG(LOG_NOTICE, "[%lu]   writers[%2d] fh=%p status=%d buf=%d fh_src=%p\n", thread_id(), i, writers_fh[i], writers_fh[i]->pool_status, writers_fh[i]->buf_loaded, writers_fh[i]->fh_src);
    }
}

static void *writer_pool_loop(void *ptr)
{
    pthread_mutex_lock_d(&writer_pool_mutex);

    while (1)
    {
        while (writers_fh_n_ready==0)
        {
            ddumb_statistic.wait_writer_pool_on_ready++;
            pthread_cond_wait_d(&writer_pool_cond_ready, &writer_pool_mutex);
        }
//        pool_dump();
        while (writers_fh[writers_fh_next_ready]->pool_status!=ps_ready) writers_fh_next_ready=(writers_fh_next_ready+1)%writers_fh_count;
        struct ddumb_fh *fh=writers_fh[writers_fh_next_ready];
        struct ddumb_fh *fh_src=fh->fh_src;
        writers_fh_next_ready=(writers_fh_next_ready+1)%writers_fh_count;
        writers_fh_n_ready--;
        fh->pool_status=ps_busy; // don't steal it to me
        pthread_mutex_unlock_d(&writer_pool_mutex);
//        DDFS_LOG(LOG_NOTICE, "[%lu]**  writer_pool_loop TAKE fh=%p fh_src=%p writer=%d fd=%d offset=0x%llx(%lld) data=0x%llx %s\n", thread_id(), fh, fh_src, fh_src->pool_writer, fh->fd, fh->buf_off, fh->buf_off, *(long long int*)fh->buf, fh->filename);

        int write_error_code=ddumb_buf_write(fh);
        xstat_unsubscribe(fh);

        // Update fh_src
        pthread_mutex_lock_d(&writer_pool_mutex);
        if (write_error_code && fh_src->delayed_write_error_code==0)
        {
            fh_src->delayed_write_error_code=write_error_code;
        }
        fh_src->pool_writer--;
//        DDFS_LOG(LOG_NOTICE, "[%lu]**  writer_pool_loop DONE fh=%p fh_src=%p writer=%d fd=%d offset=0x%llx(%lld) data=0x%llx %s\n", thread_id(), fh, fh_src, fh_src->pool_writer, fh->fd, fh->buf_off, fh->buf_off, *(long long int*)fh->buf, fh->filename);
        assert(fh_src->pool_writer>=0);
        if (fh_src->pool_writer==0)
        {
            pthread_cond_signal(&fh_src->pool_cond);
        }
        fh_src=NULL;

//        writers_fh_n_empty++;
        fh->pool_status=ps_empty;
        writers_fh_n_empty++;
        pthread_cond_signal(&writer_pool_cond_empty);
    }

    pthread_mutex_unlock_d(&writer_pool_mutex);
    return NULL;
}

static int writer_pool_load(struct ddumb_fh *fh)
{   // load fh in writers_fh[] to be picked up by writer_pool_loop()

    assert(!fh->rdonly);
    assert(!fh->special);

    DDFS_LOG_DEBUG("[%lu]++  writer_pool_load fh=%p available=%d fd=%d offset=0x%llx(%lld) delayed_err=%d %s\n", thread_id(), fh, writers_fh_n_empty, fh->fd, (long long int)fh->buf_off, (long long int)fh->buf_off, fh->delayed_write_error_code, fh->filename);
    // wait for an empty slots
    pthread_mutex_lock_d(&writer_pool_mutex);
    while (writers_fh_n_empty==0)
    {
        ddumb_statistic.wait_writer_pool_on_submit++;
        long long int start=micronow();
        pthread_cond_wait_d(&writer_pool_cond_empty, &writer_pool_mutex);
        ddumb_statistic.wait_on_submit+=micronow()-start;
    }
    // search for the empty one
    struct ddumb_fh *fh_dst=writers_fh[writers_fh_next_empty];
    while (fh_dst->pool_status!=ps_empty)
    {
        writers_fh_next_empty=(writers_fh_next_empty+1)%writers_fh_count;
        fh_dst=writers_fh[writers_fh_next_empty];
    }
    writers_fh_next_empty=(writers_fh_next_empty+1)%writers_fh_count;
    writers_fh_n_empty--;
    fh_dst->pool_status=ps_loading; // don't steal it to me while unlocked
    pthread_mutex_unlock_d(&writer_pool_mutex);
    int res=xstat_subscribe(fh, fh_dst);
    pthread_mutex_lock_d(&writer_pool_mutex);
    if (res)
    {   // roll-back
        writers_fh_n_empty++;
        fh_dst->pool_status=ps_empty;
    }
    else
    {
//        DDFS_LOG(LOG_NOTICE, "[%lu]++  writer_pool_load fh=%p fh_dst=%p writer=%d+1 pool_status=%d\n", thread_id(), fh, fh_dst, fh->pool_writer, fh_dst->pool_status);
        fh_dst->pool_status=ps_ready; // let it go (to a writer_pool_loop())
        writers_fh_n_ready++;
        fh->pool_writer++;
        res=fh->delayed_write_error_code;
        fh->delayed_write_error_code=0;
        pthread_cond_signal(&writer_pool_cond_ready);
    }
    pthread_mutex_unlock_d(&writer_pool_mutex);
    return res;
}

static int init_writer_pool()
{
    struct fuse_file_info fi;
    char thread_name[64];
    int i;

    writers_fh_count=2*ddumb_param.pool;
    writers_fh_n_empty=writers_fh_count;
    writers_fh_n_ready=0;

    writers_fh=malloc(writers_fh_count*sizeof(struct ddumb_fh*));
    writer_pool_threads=malloc(ddumb_param.pool*sizeof(pthread_t));

    if (writers_fh==NULL || writer_pool_threads==NULL)
    {
        DDFS_LOG(LOG_ERR, "init_writer_pool not enough memory for the writers pool\n");
        return -ENOMEM;
    }

    for (i=0; i<writers_fh_count; i++)
    {
        snprintf(thread_name, sizeof(thread_name), "writer-%d", i);

        struct ddumb_fh *fh=ddumb_alloc_fh(&fi, -1, 0, 0, thread_name);
        if (fh==NULL)
        {
            DDFS_LOG(LOG_ERR, "init_writer_pool not enough memory for ddumb_alloc_fh()\n");
            return -ENOMEM;
        }
        writers_fh[i]=fh;
        fh->zone.op='F';
        fh->zone.right='n';
    }

    for (i=0; i<ddumb_param.pool; i++)
    {
        int ret=pthread_create(writer_pool_threads+i, NULL, &writer_pool_loop, NULL);
        if (ret)
        {
            DDFS_LOG(LOG_ERR, "init_writer_pool pthread_create: (%s)\n", strerror(errno));
            return -errno;
        }
    }
    DDFS_LOG(LOG_INFO, "writer pool: %d threads\n", ddumb_param.pool);
    L_SYS(LOG_INFO, "writer pool: %d threads\n", ddumb_param.pool);
    return 0;
}

static int ddumb_buffer_flush(struct ddumb_fh *fh)
{
    int res=0;

    // I should not lock using a spin lock here, because we have to wait for
    // end of disk write, but nobody else should try to get the resource
    // except VMware backup in 2gbparse in some cases

    if (fh->buf_loaded)
    {
        DDFS_LOG_DEBUG("[%lu]    ddumb_buffer_flush fd=%d offset=0x%llx(%lld) data=0x%llx %s\n", thread_id(), fh->fd, (long long int)fh->buf_off, (long long int)fh->buf_off, *(long long int*)fh->buf, fh->filename);
        if (ddumb_param.pool)
        {
            res=writer_pool_load(fh);
        }
        else
        {
            pthread_mutex_lock_d(&fh->xstat->xstat_lock);
            fh->buf_loaded=DDFS_BUF_RDONLY; // will be flushed
            pthread_mutex_unlock_d(&fh->xstat->xstat_lock);
            res=ddumb_buf_write(fh);
        }
    }
    return res;
}

static int ddumb_getattr(const char *path, struct stat *stbuf)
{
    int res;

    ddumb_statistic.getattr++;
    int special=(0==strncmp(path, SPECIAL_DIR, ddfs->special_dir_len));

    if (strcmp(path, "/")==0) path="/.";

    res=lstat(path+1, stbuf);
    if (res==-1) return -errno;

    if (S_ISREG(stbuf->st_mode) && !special)
    {
        // retrieve size
        struct xstat xstat;
        xstat.ino=stbuf->st_ino;
        res=xstat_get(&xstat_root, &xstat, path+1);
        if (res>=0)
        {
            stbuf->st_size=xstat.h.size;
            stbuf->st_blksize=ddfs->c_block_size;
            stbuf->st_blocks=(xstat.h.size+ddfs->c_block_size-1)>>ddfs->block_size_shift<<(ddfs->block_size_shift-9);
        }

        if (res<0) return res;
    }

    //DDFS_LOG(LOG_NOTICE, "[%lu]++  ddumb_getattr %s size=%lld\n", thread_id(), path, (long long int)stbuf->st_size);
    return 0;
}

static int ddumb_fgetattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi)
{
    (void) path;
    int res;

    ddumb_statistic.fgetattr++;
    struct ddumb_fh *fh=ddumb_get_fh(fi);

    res=fstat(fh->fd, stbuf);
    if (res==-1) return -errno;

    if (!fh->special)
    {
        stbuf->st_size=fh->xstat->h.size;
        stbuf->st_blocks=(fh->xstat->h.size+ddfs->c_block_size-1)>>ddfs->block_size_shift<<(ddfs->block_size_shift-9);
        stbuf->st_blksize=ddfs->c_block_size;
    }
//    DDFS_LOG_DEBUG("[%lu]++  ddumb_fgetattr size=%lld xstat=%p\n", thread_id(), (long long int)stbuf->st_size, (void*)fh->xstat);
    return 0;
}

static int ddumb_access(const char *path, int mask)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = access(path+1, mask);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_readlink(const char *path, char *buf, size_t size)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = readlink(path+1, buf, size - 1);
    if (res == -1)
        return -errno;

    buf[res] = '\0';
    return 0;
}

struct ddumb_dirp {
    DIR *dp;
    struct dirent *entry;
    off_t offset;
};

static int ddumb_opendir(const char *path, struct fuse_file_info *fi)
{
    int res;
    struct ddumb_dirp *d = malloc(sizeof(struct ddumb_dirp));
    if (d == NULL)
        return -ENOMEM;

    if (strcmp(path, "/")==0) path="/.";

    d->dp = opendir(path+1);
    if (d->dp == NULL) {
        res = -errno;
        free(d);
        return res;
    }
    d->offset = 0;
    d->entry = NULL;

    fi->fh = (unsigned long) d;
    return 0;
}

static inline struct ddumb_dirp *get_dirp(struct fuse_file_info *fi)
{
    return (struct ddumb_dirp *) (uintptr_t) fi->fh;
}

static int ddumb_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    struct ddumb_dirp *d = get_dirp(fi);

    (void) path;
    if (offset != d->offset) {
        seekdir(d->dp, offset);
        d->entry = NULL;
        d->offset = offset;
    }
    while (1) {
        struct stat st;
        off_t nextoff;

        if (!d->entry) {
            d->entry = readdir(d->dp);
            if (!d->entry)
                break;
        }

        memset(&st, 0, sizeof(st));
        st.st_ino = d->entry->d_ino;
        st.st_mode = d->entry->d_type << 12;
        nextoff = telldir(d->dp);
        if (filler(buf, d->entry->d_name, &st, nextoff))
            break;

        d->entry = NULL;
        d->offset = nextoff;
    }

    return 0;
}

static int ddumb_releasedir(const char *path, struct fuse_file_info *fi)
{
    struct ddumb_dirp *d = get_dirp(fi);
    (void) path;
    closedir(d->dp);
    free(d);
    return 0;
}

static int ddumb_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int res;

    DDFS_LOG_DEBUG("[%lu]++  ddumb_mknod %s mode=%o fifo=%d sock=%d\n", thread_id(), path, mode, mode&S_IFIFO, mode&S_IFSOCK);
    if (strcmp(path, "/")==0) path="/.";
    if (S_ISFIFO(mode))
        res = mkfifo(path+1, mode);
    else
        res = mknod(path+1, mode, rdev);
    if (res == -1)
        return -errno;
    if (-1==lchown(path+1, fuse_get_context()->uid, fuse_get_context()->gid))
        return -errno;

    return 0;
}

static int ddumb_mkdir(const char *path, mode_t mode)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = mkdir(path+1, mode);
    if (res == -1)
        return -errno;
    if (-1==lchown(path+1, fuse_get_context()->uid, fuse_get_context()->gid))
        return -errno;

    return 0;
}

static int ddumb_unlink(const char *path)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = unlink(path+1);
    if (res == -1)
        return -errno;
    reclaim_could_find_free_blocks=1;
    return 0;
}

static int ddumb_rmdir(const char *path)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = rmdir(path+1);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_symlink(const char *from, const char *to)
{
    int res;

    if (strcmp(to, "/")==0) to="/.";
    res = symlink(from, to+1);
    if (res == -1)
        return -errno;
    if (-1==lchown(to+1, fuse_get_context()->uid, fuse_get_context()->gid))
        return -errno;

    return 0;
}

static int ddumb_rename(const char *from, const char *to)
{
    int res;

    if (strcmp(from, "/")==0) from="/.";
    if (strcmp(to, "/")==0) to="/.";
    res = rename(from+1, to+1);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_link(const char *from, const char *to)
{
    int res;

    if (strcmp(from, "/")==0) from="/.";
    if (strcmp(to, "/")==0) to="/.";
    res = link(from+1, to+1);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_chmod(const char *path, mode_t mode)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = chmod(path+1, mode);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_chown(const char *path, uid_t uid, gid_t gid)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res = lchown(path+1, uid, gid);
    if (res == -1)
        return -errno;

    return 0;
}

int do_truncate(struct ddumb_fh *fh, long long int size)
{// used by other functions to truncate file
    int res;
    struct xstat *xstat=fh->xstat;
    DDFS_LOG_DEBUG("[%lu]++  do_truncate size=0x%llx(%lld) -> 0x%llx(%lld) %s\n", thread_id(), (long long int)xstat->h.size, (long long int)xstat->h.size, size, size, fh->filename);

    ddumb_statistic.do_truncate++;

    if (size==xstat->h.size) return 0;
    else if (size<xstat->h.size)
    {   // truncating down

        // I must drop all buffers after the new size

        // When truncating down I don't modify the block on the limit, because
        // it will be filled of zeros when truncating up or rewritten by next write

        long long int block_boundary=(size & ddfs->block_boundary_mask);

        pthread_mutex_lock_d(&xstat->xstat_lock);
        int i;
        for(i=0; i<xstat->fhs_n; i++)
        {
            struct ddumb_fh *xfh=xstat->fhs[i];
            // DDFS_LOG_DEBUG("[%lu]++  do_truncate fh=%p xstat=%p xfh=%p I=%d\n", thread_id(), (void*)fh, (void*)fh->xstat, (void*)xfh, i);
            if (xfh->buf_loaded && (xfh->buf_off>block_boundary || xfh->buf_off==size))
            {
                if (xfh->buf_loaded==DDFS_BUF_RDONLY)
                {   // the buffer is being flushed, wait for the end
                    ddumb_statistic.wait_buf_truncate++;
                    pthread_cond_wait_d(&xstat->buf_cond, &xstat->xstat_lock);
                    // and retry from beginning
                    i=-1;
                }
                else
                {   // the buffer is now useless
                    xfh->buf_loaded=DDFS_BUF_EMPTY;
                }
            }
        }
        pthread_mutex_unlock_d(&xstat->xstat_lock);
    }
    else
    {   // truncating up
        long long int gap=(xstat->h.size & ddfs->block_gap_mask);

        if (gap)
        {
            // fill in unused space at end of last block
            DDFS_LOG_DEBUG("[%lu]++  do_truncate write zeroes to end of last block boundary=0x%llx(%lld) gap=0x%llx(%lld) size=%lld %s\n", thread_id(), xstat->h.size-gap, xstat->h.size-gap, gap, gap, ddfs->c_block_size-gap, fh->filename);
            _ddumb_write(NULL, NULL, ddfs->c_block_size-gap, xstat->h.size, fh);
        }

        // truncate fill the file with addr=0x0.....0 (using sparse file technique on unix)
        // and this address is reserved for block"\0......\0"
    }

    // size of the file after truncate
    off_t sz=((size+ddfs->c_block_size-1)>>ddfs->block_size_shift)*ddfs->c_node_size+ddfs->c_file_header_size;

    res=ftruncate(fh->fd, sz);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "do_truncate %lld -> %lld %s: (%s)\n", (long long int)xstat->h.size, size, fh->filename, strerror(errno));
        return -errno;
    }

    xstat->h.size=size;
    xstat->saved=0;
    return 0;
}

static void _ddumb_flush(struct ddumb_fh *fh);

static int ddumb_truncate(const char *path, off_t size)
{
    int res;

    ddumb_statistic.truncate++;

    DDFS_LOG_DEBUG("[%lu]++  ddumb_truncate %s size=0x%llx(%lld)\n", thread_id(), path, (long long int)size, (long long int)size);
    if (strcmp(path, "/")==0) path="/.";

    int fd=open(path+1, O_RDWR);
    if (fd==-1) return -errno;

    struct fuse_file_info fi;
    struct ddumb_fh *fh=ddumb_alloc_fh(&fi, fd, 0, 0, path);
    if (fh==NULL)
    {
        close(fd);
        DDFS_LOG(LOG_ERR, "ddumb_truncate not enough memory for ddumb_alloc_fh(): %s\n", path);
        return -ENOMEM;
    }

    pthread_mutex_lock_d(&fh->lock); // useless because fh cannot be "owned" by another thread
    xzone_lock(fh, size, 0, 'T');
    res=do_truncate(fh, size);
    xzone_unlock(fh);
    pthread_mutex_unlock_d(&fh->lock); // useless because fh cannot be "owned" by another thread
    _ddumb_flush(fh);
    ddumb_free_fh(&fi);

    close(fd);

    return res;
}

static int ddumb_ftruncate(const char *path, off_t size, struct fuse_file_info *fi)
{
    (void) path;
    struct ddumb_fh *fh=ddumb_get_fh(fi);

    pthread_mutex_lock_d(&fh->lock);
    xzone_lock(fh, size, 0, 'T');

    ddumb_statistic.ftruncate++;

    DDFS_LOG_DEBUG("[%lu]++  ddumb_ftruncate %s size=0x%llx(%lld)\n", thread_id(), path, (long long int)size, (long long int)size);

    int res=do_truncate(fh, size);
    if (res==0)
    {   // I think this is important to save the stat now
        res=xstat_save(fh->fd, fh->xstat, fh->filename);
    }

    xzone_unlock(fh);
    pthread_mutex_unlock_d(&fh->lock);

    return res;
}

static int ddumb_utimens(const char *path, const struct timespec ts[2])
{
    int res;
    struct timeval tv[2];

    tv[0].tv_sec = ts[0].tv_sec;
    tv[0].tv_usec = ts[0].tv_nsec / 1000;
    tv[1].tv_sec = ts[1].tv_sec;
    tv[1].tv_usec = ts[1].tv_nsec / 1000;

    if (strcmp(path, "/")==0) path="/.";
    res = utimes(path+1, tv);
    if (res == -1)
        return -errno;

    return 0;
}

static int ddumb_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    ddumb_statistic.create++;
//    DDFS_LOG_DEBUG("[%lu]++  ddumb_create %s flags=%o\n", thread_id(), path, fi->flags);

    fi->fh=(uintptr_t)NULL;
    int special=(0==strncmp(path, SPECIAL_DIR, ddfs->special_dir_len));
    if (special)
    {
        return -EACCES;
    }

    if (strcmp(path, "/")==0) path="/.";

    int flags=fi->flags;
    if (flags & O_WRONLY) flags=(flags^O_WRONLY)|O_RDWR;
    flags=flags&~O_APPEND;

    int fd=open(path+1, flags, mode);
    if (fd==-1)
    {
        // ddumb_flush() will be called anyway
        DDFS_LOG(LOG_ERR, "ddumb_create open %s flags=%o mode=%o : %s\n", path, fi->flags, mode, strerror(errno));
        return -errno;
    }

    if (-1==lchown(path+1, fuse_get_context()->uid, fuse_get_context()->gid))
        return -errno;

    struct ddumb_fh *fh=ddumb_alloc_fh(fi, fd, !(flags&(O_WRONLY|O_RDWR)), 0, path);
    if (fh==NULL)
    {
        // ddumb_flush() will be called anyway
        close(fd);
        DDFS_LOG(LOG_ERR, "ddumb_create not enough memory for ddumb_alloc_fh(): %s\n", path);
        return -ENOMEM;
    }

    xstat_save_fh(fh); // at least write the header, to be reconized as valid if unexpected shutdown

    DDFS_LOG_DEBUG("[%lu]++  ddumb_create %s flags=%o fd=%d fh=%p\n", thread_id(), path, fi->flags, fd, (void*)fh);

    return 0;
}

static int ddumb_open(const char *path, struct fuse_file_info *fi)
{
    ddumb_statistic.open++;
    DDFS_LOG_DEBUG("[%lu]++  ddumb_open %s flags=%o rdonly=%d\n", thread_id(), path, fi->flags, !(fi->flags&(O_WRONLY|O_RDWR)));

    fi->fh=(uintptr_t)NULL;
    int special=(0==strncmp(path, SPECIAL_DIR, ddfs->special_dir_len));

    if (special)
    {
        if ((fi->flags & O_WRONLY) || (fi->flags & O_RDWR)) return -EACCES;
    }

    if (strcmp(path, "/")==0) path="/.";

    int flags=fi->flags;
    if (flags & O_WRONLY) flags=(flags^O_WRONLY)|O_RDWR;
    flags=flags&~O_APPEND;

    int fd=open(path+1, flags);
    if (fd==-1)
    {
        // ddumb_flush() will be called anyway
        DDFS_LOG(LOG_ERR, "ddumb_open open %s flags=%o : %s\n", path, fi->flags, strerror(errno));
        return -errno;
    }

    struct ddumb_fh *fh=ddumb_alloc_fh(fi, fd, !(flags&(O_WRONLY|O_RDWR)), special, path);
    if (fh==NULL)
    {
        // ddumb_flush() will be called anyway
        close(fd);
        DDFS_LOG(LOG_ERR, "ddumb_open not enough memory for ddumb_alloc_fh(): %s\n", path);
        return -ENOMEM;
    }

    if (special)
    {
        long long int start=now();
        if (strcmp(path, STATS_FILE)==0 || strcmp(path, STATS0_FILE)==0)
        {
            FILE *file=fopen(path+1, "w");
            ddumb_write_statistic(file);
            fclose(file);
            if (strcmp(path, STATS0_FILE)==0) memset(&ddumb_statistic, '\0', sizeof(struct ddumb_statistic));
        }
        else if (strcmp(path, RECLAIM_FILE)==0)
        {
            FILE *file=fopen(path+1, "w");
            reclaim(file);
            fclose(file);
        }
        else if (strcmp(path, TEST_FILE)==0)
        {
            FILE *file=fopen(path+1, "w");
            ddumb_test(file);
            fclose(file);
        }
        // wait for "attr_timeout", default 1.0s, this will force fuse to reload file size to get true size.
        long long int end=now();
        double wait=ddumb_param.attr_timeout-(end-start)/1000.0;
        // fprintf(stderr, "****** attr_timeout=%lf wait=%lf %lld %lld\n", ddumb_param.attr_timeout, wait, start, end);
        if (wait>0.0 && wait<=ddumb_param.attr_timeout) dsleep(wait+0.1);
    }

    DDFS_LOG_DEBUG("[%lu]++  ddumb_open %s flags=%o fd=%d fh=%p rdonly=%d %d\n", thread_id(), path, fi->flags, fd, (void*)fh, fh->rdonly, !(flags&(O_WRONLY|O_RDWR)));
/* Be careful because truncate can be called just after open
 * see http://marc.info/?l=fuse-devel&m=129972717512852&w=2
 * This is no more a problem now
 */
    return 0;
}

static int _ddumb_read(const char *path, char *buf, size_t size, off_t offset, struct ddumb_fh *fh)
{
    (void) path;
    int res=0;
    ddumb_statistic.read++;

    DDFS_LOG_DEBUG("[%lu]++  ddumb_read fd=%d offset=0x%llx(%lld) size=0x%llx(%lld) %s\n", thread_id(), fh->fd, (long long int)offset, (long long int)offset, (long long int)size, (long long int)size, path);

    if (offset > fh->xstat->h.size)
    {
        return 0;
    }

    if (offset+size > fh->xstat->h.size)
    {   // don't read more than EOF
        size=fh->xstat->h.size-offset;
    }

    size_t remain=size;
    char *pbuf=buf;
    long long int off=offset;
    long long int gap=(off & ddfs->block_gap_mask);
    // long long int block_boundary=(off & ddfs->block_boundary_mask);
    while (remain>0)
    { // split read into multiple aligned block read
        long long int sz=ddfs->c_block_size-gap;
        if (remain<sz) sz=remain;

        res=ddumb_block_read(fh, pbuf, off, sz);
        if (res<0) return res;

        pbuf+=sz;
        remain-=sz;
        off+=sz;

        gap=0;
        // block_boundary=off;
    }
    return size;
}

static int ddumb_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    (void) path;
    int res;
    struct ddumb_fh *fh=ddumb_get_fh(fi);

    if (fh->special)
    {
        res=pread(fh->fd, buf, size, offset);
        if (res==-1) res=-errno;
    }
    else
    {
        pthread_mutex_lock_d(&fh->lock);
        // This lock don't allow simultaneous access in read to this fh
        // To allow simultaneous access, I must mel the xzone dynamic
        // to allow each request to lock its own zone (because here the zone is attached
        // to the fh

        xzone_lock(fh, (long long int)offset, (long long int)size, 'R');
        res=_ddumb_read(path, buf, size, offset, fh);
        xzone_unlock(fh);

        pthread_mutex_unlock_d(&fh->lock);
    }

    return res;
}

static int _ddumb_write(const char *path, const char *buf, size_t size, off_t offset, struct ddumb_fh *fh)
{   // be careful _ddumb_write can call itself via do_truncate
    (void) path;
    int res=0;
    struct xstat *xstat=fh->xstat;
    ddumb_statistic.write++;
    DDFS_LOG_DEBUG("[%lu]++  ddumb_write fd=%d fh=%p from=0x%llx(%lld) to=0x%llx(%lld) size=0x%llx(%lld) buf_off=%llx fhs_n=%d %s\n", thread_id(), fh->fd, (void*)fh, (long long int)offset, (long long int)offset, (long long int)(offset+size-1), (long long int)(offset+size-1), (long long int)size, (long long int)size, (long long int)(fh->buf_loaded?fh->buf_off:-1), xstat->fhs_n, path);

    long long int gap=(offset & ddfs->block_gap_mask);
    long long int block_boundary=(offset & ddfs->block_boundary_mask);

    if (block_boundary > xstat->h.size)
    {   // write further than the end of last block ?
        ddumb_statistic.eof_write++;
        DDFS_LOG_DEBUG("[%lu]--  ddumb_write write after last block, file size=0x%llx(%lld) offset=0x%llx(%lld)\n", thread_id(), (long long int)xstat->h.size, (long long int)xstat->h.size, (long long int)offset, (long long int)offset);
        res=do_truncate(fh, offset);
        if (res<0) return res;
    }
    else if (block_boundary < xstat->h.size)
    {
        reclaim_could_find_free_blocks=1; // we can run reclaim
    }

    long long int remain=size;
    long long int off=offset;
    while (remain>0)
    {   // split write into multiple aligned block writes
        long long int sz=ddfs->c_block_size-gap;
        if (remain<sz) sz=remain;

        // search if buf can be written into any available buffers registered in xstat
        int found=0;
        int i=0;
        pthread_mutex_lock_d(&xstat->xstat_lock);
        for(; i<xstat->fhs_n; i++)
        {
            struct ddumb_fh *xfh=xstat->fhs[i];
            // DDFS_LOG_DEBUG("[%lu]++  ddumb_write fh=%p xstat=%p xfh=%p I=%d\n", thread_id(), (void*)fh, (void*)fh->xstat, (void*)xfh, i);
            if (xfh->buf_loaded && xfh->buf_off==block_boundary)
            {
                if (xfh->buf_loaded==DDFS_BUF_RDONLY)
                {
                    ddumb_statistic.wait_buf_write++;
                    pthread_cond_wait_d(&xstat->buf_cond, &xstat->xstat_lock);
                    i=-1;
                }
                else
                {
                    found=1;
                    // if (xfh!=fh) DDFS_LOG_DEBUG("[%lu]--  ddumb_write write into another fh=%p xfh=%p\n", thread_id(), (void*)fh, (void*)xfh);

                    if (off > xstat->h.size)
                    { // handle write after EOF
                        DDFS_LOG_DEBUG("[%lu]--  ddumb_write write after EOF but inside last block, file size=0x%llx(%lld) offset=0x%llx(%lld)\n", thread_id(), (long long int)xstat->h.size, (long long int)xstat->h.size, (long long int)off, (long long int)off);
                        memset(xfh->buf+(xstat->h.size-block_boundary), '\0', off-xstat->h.size);
                        DDFS_LOG_DEBUG("[%lu]--  ddumb_write eof MEMSET from=0x%llx(%lld) to=0x%llx(%lld) \n", thread_id(), (long long int)xstat->h.size, (long long int)xstat->h.size, off, off);

                    }
                    if (buf) memcpy(xfh->buf+gap, buf, sz);
                    else {
                        memset(xfh->buf+gap, '\0', sz);
                        DDFS_LOG_DEBUG("[%lu]--  ddumb_write zero MEMSET from=0x%llx(%lld) to=0x%llx(%lld) \n", thread_id(), off, off, off+sz-1, off+sz-1);
                    }
                    break;
                }
            }
        }
        pthread_mutex_unlock_d(&xstat->xstat_lock);

        if (!found)
        {

            if (fh->buf_loaded && fh->buf_off!=block_boundary)
            {
                ddumb_statistic.write_save++;
                res=ddumb_buffer_flush(fh);
                if (res<0)
                {
                    return res;
                }
            }

            // fh->buf_loaded is EMPTY now, then no need to lock fh->buf_lock to access fh->buf
            if ((remain<ddfs->c_block_size || gap!=0))
            {
                DDFS_LOG_DEBUG("[%lu]++  ddumb_write short write gap=0x%llx(%lld) remain=%lld file_size=%lld loaded=%d %llx!=%llx\n", thread_id(), gap, gap, remain, (long long int)xstat->h.size, fh->buf_loaded, (long long int)fh->buf_off, (long long int)(off-gap));
                long long int psize=xstat->h.size-block_boundary;
                if (psize>0)
                {
                    ddumb_statistic.read_before_write++;
                    DDFS_LOG_DEBUG("[%lu]--  ddumb_write read_before_write fd=%d fh=%p offset=0x%llx(%lld)\n", thread_id(), fh->fd, (void*)fh, (long long int)block_boundary, (long long int)block_boundary);
                    if (psize>ddfs->c_block_size)
                    {
                        psize=ddfs->c_block_size;
                    }
                    res=ddumb_block_read(fh, fh->buf, block_boundary, psize);
                    if (res<0)
                    {
                        return res;
                    }
                }

                if (off > xstat->h.size)
                {   // if end of block > file size, fill block with zeros from end of file up to start of offset
                    memset(fh->buf+(xstat->h.size-block_boundary), '\0', off-xstat->h.size);
                    DDFS_LOG_DEBUG("[%lu]--  ddumb_write eof2 MEMSET from=0x%llx(%lld) to=0x%llx(%lld) \n", thread_id(), (long long int)xstat->h.size, (long long int)xstat->h.size, off, off);
                }
            }

            if (buf) memcpy(fh->buf+gap, buf, sz);
            else {
                memset(fh->buf+gap, '\0', sz);
                DDFS_LOG_DEBUG("[%lu]--  ddumb_write zero2 MEMSET from=0x%llx(%lld) to=0x%llx(%lld) \n", thread_id(), off, off, off+sz, off+sz);
            }
            fh->buf_off=block_boundary;
//            pthread_mutex_lock_d(&xstat->xstat_lock);
            fh->buf_firstwrite=time(NULL);
            fh->buf_loaded=DDFS_BUF_RDWR; // the locked zone "protect" this fh
//            pthread_mutex_unlock_d(&xstat->xstat_lock);

        }

        if (fh->buf_loaded && fh->buf_off==block_boundary && gap+sz==ddfs->c_block_size)
        {
            res=ddumb_buffer_flush(fh);
            if (res<0) return res;
        }

        if (buf) buf+=sz;
        remain-=sz;
        off+=sz;
        gap=0;
        block_boundary+=ddfs->c_block_size;

        // update xstat->h.size (protected by the locked zone)
        if (off > xstat->h.size)
        {
            xstat->h.size=off;
            xstat->saved=0;
        }
    }
    if (!xstat->saved) xstat_save_fh(fh);
    return size;
}

static int ddumb_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	if (ddfs->c_block_count-ddfs->usedblock<1000)
	{
		if (!low_free_block_warning)
		{
            DDFS_LOG(LOG_ERR, "free blocks are too low, returning preventive error to avoid serious corruption !\n");
            low_free_block_warning=1;
		}
		return -ENOSPC;
	}
	else low_free_block_warning=0;

    struct ddumb_fh *fh=ddumb_get_fh(fi);
    assert(!fh->rdonly);
    assert(!fh->special); // special file are RO (until now) and have no fh->lock

    pthread_mutex_lock_d(&fh->lock);

    xzone_lock(fh, (long long int)offset, (long long int)size, 'W');
    int res=_ddumb_write(path, buf, size, offset, fh);
    xzone_unlock(fh);

    pthread_mutex_unlock_d(&fh->lock);
    return res;
}

static int ddumb_statfs(const char *path, struct statvfs *stbuf)
{
    int res;

    if (strcmp(path, "/")==0) path="/.";
    res=statvfs(path+1, stbuf);
    if (res==-1) return -errno;

    long long int s, u;
    pthread_mutex_lock_d(&ifile_mutex);
    //bit_array_count(&ddfs->ba_usedblocks, &s, &u);
    u = ddfs->ba_usedblocks.size - ddfs->usedblock;
    pthread_mutex_unlock_d(&ifile_mutex);
    stbuf->f_bfree=stbuf->f_bfree*(stbuf->f_frsize/ddfs->c_addr_size);
    if (u<stbuf->f_bfree) stbuf->f_bfree=u;
    stbuf->f_bavail=stbuf->f_bavail*(stbuf->f_frsize/ddfs->c_addr_size);
    if (u<stbuf->f_bavail) stbuf->f_bavail=u;
    stbuf->f_bsize=ddfs->c_block_size;
    stbuf->f_frsize=ddfs->c_block_size;
    stbuf->f_blocks=ddfs->c_block_count;
    return 0;
}


static void _ddumb_flush(struct ddumb_fh *fh)
{
    // be careful not to flush special file that are RDONLY and don't have buffer nor buf_lock
    if (fh->buf)
    {
        pthread_mutex_lock_d(&fh->lock);
        ddumb_buffer_flush(fh);
        pthread_mutex_unlock_d(&fh->lock);
    }

    if (ddumb_param.pool)
    {   // wait for all related writers of the pool
        pthread_mutex_lock_d(&writer_pool_mutex);
        while (fh->pool_writer>0)
        {
            ddumb_statistic.wait_writer_pool_on_flush++;
            pthread_cond_wait_d(&fh->pool_cond, &writer_pool_mutex);
        }
        pthread_mutex_unlock_d(&writer_pool_mutex);
    }
    if (fh->xstat && !fh->xstat->saved) xstat_save_fh(fh);

}

static int ddumb_flush(const char *path, struct fuse_file_info *fi)
{
    int res;

    (void) path;
    /* This is called from every close on an open file (not sure if even read only),
       so call the close on the underlying filesystem. But since flush may be
       called multiple times for an open file, this must not really
       close the file.  This is important if used on a network
       filesystem like NFS which flush the data/metadata on close().
       this is not called for the last close(), release() is called instead  */
    struct ddumb_fh *fh=ddumb_get_fh(fi);
    DDFS_LOG_DEBUG("[%lu]++  ddumb_flush fd=%d fh=%p %s\n", thread_id(), fh?fh->fd:-1, (void*)fh, path);
    ddumb_statistic.flush++;

    if (!fh)
    {
        _ddumb_flush(fh);
        int fd=dup(fh->fd);
        res=close(fd);
        if (res==-1)
        {
            DDFS_LOG(LOG_ERR, "ddumb_flush close(dup) %s (%s)\n", path, strerror(errno));
            return -errno;
        }
    }
    else; // open() or create() has failed, ddumb_flush is called anyway

    return 0;
}

static int ddumb_release(const char *path, struct fuse_file_info *fi)
{
    int res=0;
    (void) path;
    ddumb_statistic.release++;
    struct ddumb_fh *fh=ddumb_get_fh(fi);
    if (fh)
    {
        DDFS_LOG_DEBUG("[%lu]++  ddumb_release fd=%d fh=%p buf_loaded=%d xstat->saved=%d %s\n", thread_id(), fh?fh->fd:-1, (void*)fh, fh->buf?fh->buf_loaded:99, fh->xstat?fh->xstat->saved:99, path);
        _ddumb_flush(fh);
        assert(!fh || !fh->buf || !fh->buf_loaded);
        assert(!fh->xstat || fh->xstat->saved || fh->rdonly);

        int fd=fh->fd;
        res=ddumb_free_fh(fi);
        if (close(fd)==-1)
        {
            DDFS_LOG(LOG_ERR, "ddumb_release close() %s (%s)\n", path, strerror(errno));
            return -errno;
        }
    }

    return res;
}

static int ddumb_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
    int res;
    (void) path;

    struct ddumb_fh *fh=ddumb_get_fh(fi);
    DDFS_LOG_DEBUG("[%lu]++  ddumb_fsync fd=%d fh=%p %s\n", thread_id(), fh->fd, (void*)fh, path);
    ddumb_statistic.fsync++;

    _ddumb_flush(fh);

#ifndef HAVE_FDATASYNC
    (void) isdatasync;
#else
    if (isdatasync)
        res = fdatasync(fh->fd);
    else
#endif
        res = fsync(fh->fd);

    if (res==-1) return -errno;
#if 1
    res=fsync(ddfs->bfile);
    if (res==-1) return -errno;
    res=fsync(ddfs->ifile);
    if (res==-1) return -errno;
#endif
    return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ddumb_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    if (strcmp(path, "/")==0) path="/.";
    int res=lsetxattr(path+1, name, value, size, flags);
    if (res==-1) return -errno;
    return 0;
}

static int ddumb_getxattr(const char *path, const char *name, char *value, size_t size)
{
    if (strcmp(path, "/")==0) path="/.";
    int res=lgetxattr(path+1, name, value, size);
    if (res==-1) return -errno;
    return res;
}

static int ddumb_listxattr(const char *path, char *list, size_t size)
{
    if (strcmp(path, "/")==0) path="/.";
    int res=llistxattr(path+1, list, size);
    if (res==-1) return -errno;
    return res;
}

static int ddumb_removexattr(const char *path, const char *name)
{
    if (strcmp(path, "/")==0) path="/.";
    int res=lremovexattr(path+1, name);
    if (res==-1)return -errno;
    return 0;
}
#endif /* HAVE_SETXATTR */

static int ddumb_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *lock)
{
    (void) path;

    struct ddumb_fh *fh=ddumb_get_fh(fi);
    int res=ulockmgr_op(fh->fd, cmd, lock, &fi->lock_owner, sizeof(fi->lock_owner));
    return res;
}


void *ddumbfs_background(void *ptr)
{
    struct timeval now;
    struct timespec timeout;
    long int next_sync;

    pthread_mutex_lock_d(&ddumb_background_mutex);

    next_sync=time(NULL)+ddfs->c_auto_sync;

    while (!ddumbfs_terminate)
    {
        //DDFS_LOG(LOG_NOTICE, "ddumbfs_background writers_fh_count=%d writers_fh_n_empty=%d writers_fh_n_ready=%d\n", writers_fh_count, writers_fh_n_empty, writers_fh_n_ready);
        gettimeofday(&now, NULL);
        timeout.tv_sec=now.tv_sec+60;
        timeout.tv_nsec=now.tv_usec*1000;
        int res=pthread_cond_timedwait(&ddumb_background_cond, &ddumb_background_mutex, &timeout);

        // dump_all();

        // save long living buffer ?
        if (res==ETIMEDOUT)
        {
            xstat_flush_all_buf(time(NULL)-ddfs->c_auto_buffer_flush);
        }

        // save_usedblocks ?
        pthread_mutex_lock_d(&ifile_mutex);
        if (res==ETIMEDOUT) ddumbfs_save_usedblocks(1000 * (131072 / ddfs->c_block_size));
        else ddumbfs_save_usedblocks(0);
        pthread_mutex_unlock_d(&ifile_mutex);

        // reclaim ?
        if (ddfs->usedblock>=ddfs->c_block_count/100*next_reclaim && reclaim_could_find_free_blocks)
        {
            reclaim(NULL);
        }

        if (time(NULL)>next_sync)
        {
            fsync(ddfs->bfile);
            fsync(ddfs->ifile);
            next_sync=time(NULL)+ddfs->c_auto_sync;
        }

    }
    pthread_mutex_unlock_d(&ddumb_background_mutex);
    pthread_exit(NULL);
}

#ifdef SOCKET_INTERFACE

pthread_t ddumbfs_socket_pthread;

enum socket_operation_command { sop_noop, sop_open, sop_read, sop_write, sop_fsync, sop_flush, sop_close, sop_dump };

struct socket_operation
{
    int command;
    long long int offset;
    long long int size;
    long long int aux;
};

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
void *ddumbfs_socket(void *ptr)
{

    int s_srv, s_cli, len, sz;
    struct sockaddr_un local, remote;
    char *buffer;
    struct fuse_file_info fi;
    char filename[4096];

    int res=posix_memalign((void *)&buffer, BLOCK_ALIGMENT, ddfs->c_block_size);
    if (res)
    {
        fprintf(stderr,"ERROR: cannot allocate buffer: %s\n", strerror(res));
        exit(1);
    }

    if ((s_srv = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    local.sun_family = AF_UNIX;
    strcpy(local.sun_path, SOCKET_PATH);
    unlink(local.sun_path);
    len = strlen(local.sun_path) + sizeof(local.sun_family);
    if (bind(s_srv, (struct sockaddr *)&local, len) == -1) {
        perror("bind");
        exit(1);
    }

    if (listen(s_srv, 5) == -1) {
        perror("listen");
        exit(1);
    }

    while(!ddumbfs_terminate) {

        fprintf(stderr, "Waiting for a connection...\n");
        socklen_t t = sizeof(remote);
        if ((s_cli = accept(s_srv, (struct sockaddr *)&remote, &t)) == -1) {
            perror("accept");
            exit(1);
        }

        fprintf(stderr, "Connected.\n");
        while (!ddumbfs_terminate)
        {
            struct socket_operation sop;
            int n;
            n=socket_recv(s_cli, &sop, sizeof(struct socket_operation), 0);
            if (n<0)
            {
                perror("recv");
                break;
            }
            if (n==0) break;
            switch (sop.command)
            {
                case sop_noop:
                    // to shutdown cleanly
                    break;
                case sop_open:
                    // fprintf(stderr, "socket: open\n");
                    n=socket_recv(s_cli, &filename, sop.size, 0);
                    if (n!=sop.size)
                    {
                        fprintf(stderr, "short read reading filename\n");
                        exit(1);
                    }
                    fi.flags=sop.offset;
                    if (pathexists(filename+1))
                    {
                        fprintf(stderr, "socket: open %s\n", filename);
                        res=ddumb_open(filename, &fi);
                        fprintf(stderr, "socket: open = %d %s\n", res, filename);
                    }
                    else
                    {
                        struct fuse_context *ctx=fuse_get_context();
                        ctx->gid=0;
                        ctx->uid=0;
                        fprintf(stderr, "socket: create %s\n", filename);
                        res=ddumb_create(filename, sop.aux, &fi);
                        struct ddumb_fh *fh=ddumb_get_fh(&fi);
                        fprintf(stderr, "buf=%p\n", fh->buf);
                        fh->buf[0]='H';
                        fprintf(stderr, "socket: create = %d %s\n", res, filename);
                    }
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "short write writing res\n");
                        exit(1);
                    }
                    break;
                case sop_write:
                    // fprintf(stderr, "socket: write %lld\n", sop.size);
                    sz=0;
                    while (sz<sop.size)
                    {
                        n=socket_recv(s_cli, buffer+sz, sop.size-sz, 0);
                        if (n==-1)
                        {
                            perror("sop_write, recv error:");
                            exit(1);
                        }
                        sz+=n;
                    }
                    res=ddumb_write(filename, buffer, sop.size, sop.offset, &fi);
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "short write writing buffer\n");
                        exit(1);
                    }
                    break;
                case sop_read:
                    // fprintf(stderr, "socket: read %lld\n", sop.size);
                    res=ddumb_read(filename, buffer, sop.size, sop.offset, &fi);
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "short write writing buffer\n");
                        exit(1);
                    }
                    sz=0;
                    while (sz<sop.size)
                    {
                        n=socket_send(s_cli, buffer+sz, sop.size-sz, 0);
                        if (n==-1)
                        {
                            perror("sop_read, send error:");
                            exit(1);
                        }
                        sz+=n;
                    }
                    break;
                case sop_fsync:
                    // fprintf(stderr, "socket: fsync\n");
                    res=ddumb_fsync(filename, sop.offset, &fi);
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "socket fsync: short write writing res\n");
                        exit(1);
                    }
                    break;
                case sop_flush:
                    // fprintf(stderr, "socket: flush\n");
                    res=ddumb_flush(filename, &fi);
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "socket flush: short write writing res\n");
                        exit(1);
                    }
                    break;
                case sop_close:
                    // fprintf(stderr, "socket: close\n");
                    res=ddumb_flush(filename, &fi);
                    res=ddumb_release(filename, &fi);
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    if (res!=sizeof(res))
                    {
                        fprintf(stderr, "socket close: short write writing res\n");
                        exit(1);
                    }
                    break;
                case sop_dump:
                    // fprintf(stderr, "socket: dump\n");
                    dump_all();
                    res=0;
                    res=socket_send(s_cli, &res, sizeof(res), 0);
                    break;
            }
        }
        close(s_cli);
    }
    pthread_exit(NULL);
}
#endif

static void* ddumb_init(struct fuse_conn_info *conn)
{
    int res;
    res=chdir(ddfs->rdir); // must be done here in ddumb_init, don't work in main()
    assert(res==0); // TODO: do it better

    // reset all stats to 0
    memset(&ddumb_statistic, 0x00, sizeof(struct ddumb_statistic));


    if (ddfs->lock_index)
    {  // lock index component into memory
        int res1=mlock(ddfs->usedblocks_map, ddfs->c_node_offset-ddfs->c_freeblock_offset);
        if (res1==-1) {
            DDFS_LOG(LOG_ERR, "cannot lock used block into memory\n");
        }
        int res2=mlock(ddfs->nodes, ddfs->c_node_block_count*ddfs->c_index_block_size);
        if (res2==-1) {
            DDFS_LOG(LOG_ERR, "cannot lock index into memory\n");
        }
        if (res1==0 && res2==0)
        {
            DDFS_LOG(LOG_INFO, "index locked into memory: %.1fMo\n", (ddfs->c_node_block_count*ddfs->c_index_block_size+ddfs->c_node_offset-ddfs->c_freeblock_offset)/1024.0/1024.0);
        }
        else ddfs->lock_index=0;
    }

    DDFS_LOG(LOG_INFO, "[%d] filesystem %s mounted\n", (int)getpid(), ddumb_param.parent);
    L_SYS(LOG_INFO, "[%d] filesystem %s mounted\n", (int)getpid(), ddumb_param.parent);

    // ddfs_save_usedblocks(); // is done by ddumbfs_background at startup

    pthread_create(&ddumbfs_background_pthread, NULL, ddumbfs_background, NULL);

#ifdef SOCKET_INTERFACE
    pthread_create(&ddumbfs_socket_pthread, NULL, ddumbfs_socket, NULL);
#endif

    if (ddumb_param.pool)
    {
        res=init_writer_pool();
        assert(res==0); // TODO: do it better
    }

    next_reclaim=ddumb_param.reclaim;

    return NULL;
}

void free_xstat_node(void *nodep)
{  // used by ddumb_destroy to report unclosed file
    DDFS_LOG(LOG_ERR, "free_node %p (should not append when all file are closed before unmount)\n", nodep)
}

static void ddumb_destroy(void* nothing)
{ // called just after umount

    // display statistics
    ddumb_write_statistic(stderr);

    // destroy xstat to be sure no one was left behind (except still open file)
    tdestroy(xstat_root.root, free_xstat_node);

    pthread_mutex_lock_d(&ddumb_background_mutex);
    ddumbfs_terminate=1;
    pthread_cond_signal(&ddumb_background_cond);
    pthread_mutex_unlock_d(&ddumb_background_mutex);

    pthread_join(ddumbfs_background_pthread, NULL);
#ifdef SOCKET_INTERFACE
    int s, len;
    struct sockaddr_un remote;
    struct socket_operation sop;
    if ((s=socket(AF_UNIX, SOCK_STREAM, 0))!=-1)
    {
        remote.sun_family = AF_UNIX;
        strcpy(remote.sun_path, SOCKET_PATH);
        len=strlen(remote.sun_path) + sizeof(remote.sun_family);
        if (connect(s, (struct sockaddr *)&remote, len)!=-1)
        {
            sop.command=sop_noop;
            socket_send(s, &sop, sizeof(struct socket_operation), 0);
            close(s);
        }
    }

    pthread_join(ddumbfs_socket_pthread, NULL);
#endif
    ddfs_close();

    // ok cleanly unmounted
    ddfs_unlock(".autofsck");
    L_SYS(LOG_INFO, "filesystem %s unmounted\n", ddumb_param.parent);
    DDFS_LOG(LOG_INFO, "filesystem %s unmounted\n", ddumb_param.parent);
}

static unsigned long * block_buf=NULL;
long long int file_size=2LL*1073741824LL;

int genfile(unsigned long seed, char *filename)
{
    int err=0;
    int fast_random=1;
    int block_size=ddfs->c_block_size;
	int res;

    struct fuse_file_info fi;
    fi.flags=O_RDWR|O_CREAT|O_TRUNC;
    if (pathexists(filename+1))
    {
        res=ddumb_open(filename, &fi);
    }
    else
    {
        struct fuse_context *ctx=fuse_get_context();
        ctx->gid=0;
        ctx->uid=0;
        res=ddumb_create(filename, 0644, &fi);
    }
    (void)res; // silent warning

    if (block_buf==NULL) block_buf=malloc(block_size);

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
                *p=seed=seed * 1103515245 + 12345;
            }
        }
        if (fast_random && size!=0)
        {
            *(block_buf+(fast_ixd%n))=seed=seed * 1103515245 + 12345;
            fast_ixd++;
            *(block_buf+(fast_ixd%n))=seed=seed * 1103515245 + 12345;
            fast_ixd++;
            *(block_buf+(fast_ixd%n))=seed=seed * 1103515245 + 12345;
            fast_ixd++;
        }
        int len;
        len=ddumb_write(filename, (char *)block_buf, block_size, size, &fi);
        err=(len!=block_size || err);
        size+=block_size;
    }

    res=ddumb_flush(filename, &fi);
    res=ddumb_fsync(filename, 0, &fi);
    res=ddumb_flush(filename, &fi);
    res=ddumb_release(filename, &fi);

    return err;
}

void ddumb_test(FILE *file)
{
    int i;
    int rndseed=0;
    int count=4;
    char filename[256];

    long long int totaltime=0;
    float totaltp=0;
    for (i=0; i<count; i++)
    {
        sprintf(filename, "/data%06d-0.bin", i);
        long long int start=micronow();
        genfile(i+rndseed, filename);
        long long int end=micronow();
        float tp=1000000.0/1048576.0*file_size/(end-start);
        totaltp+=tp;
        totaltime+=end-start;
        fprintf(file, "%6d    %6.2f s    %6.2f Mo/s\n", i, (end-start)/1000000.0, tp);
    }
    fprintf(file, "avg       %6.2f s    %6.2f Mo/s\n", totaltime/(count*1000000.0), totaltp/count);
}

static struct fuse_operations ddumb_ops = {
    .getattr        = ddumb_getattr,
    .fgetattr       = ddumb_fgetattr,
    .access         = ddumb_access,
    .readlink       = ddumb_readlink,
    .opendir        = ddumb_opendir,
    .readdir        = ddumb_readdir,
    .releasedir     = ddumb_releasedir,
    .mknod          = ddumb_mknod,
    .mkdir          = ddumb_mkdir,
    .symlink        = ddumb_symlink,
    .unlink         = ddumb_unlink,
    .rmdir          = ddumb_rmdir,
    .rename         = ddumb_rename,
    .link           = ddumb_link,
    .chmod          = ddumb_chmod,
    .chown          = ddumb_chown,
    .ftruncate      = ddumb_ftruncate,
    .truncate       = ddumb_truncate,
    .utimens        = ddumb_utimens,
    .create         = ddumb_create,
    .open           = ddumb_open,
    .read           = ddumb_read,
    .write          = ddumb_write,
    .statfs         = ddumb_statfs,
    .flush          = ddumb_flush,
    .release        = ddumb_release,
    .fsync          = ddumb_fsync,
#ifdef HAVE_SETXATTR
    .setxattr       = ddumb_setxattr,
    .getxattr       = ddumb_getxattr,
    .listxattr      = ddumb_listxattr,
    .removexattr    = ddumb_removexattr,
#endif
    .lock           = ddumb_lock,
    .init           = ddumb_init,
    .destroy        = ddumb_destroy,
#if FUSE_VERSION >= 28
    .flag_nullpath_ok = 1,
#endif
};


enum {
     KEY_HELP,
     KEY_VERSION,
     KEY_DEBUG,
};

char *fuse_default_options="-ouse_ino,readdir_ino,default_permissions,allow_other,big_writes,max_read=131072,max_write=131072,negative_timeout=0,entry_timeout=0,attr_timeout=0";

#define DDUMB_OPT(t, p, v) { t, offsetof(struct_ddumb_param, p), v }

static const struct fuse_opt ddumb_opts[] = {
        DDUMB_OPT("parent=%s", parent, 0),
        DDUMB_OPT("pool=%i", pool, 0),
        DDUMB_OPT("reclaim=%i", reclaim, 0),
        DDUMB_OPT("check", check_at_start, 1),
        DDUMB_OPT("lock_index", lock_index, 1),
        DDUMB_OPT("nolock_index", lock_index, 0),
        DDUMB_OPT("dio", direct_io, 1),
        DDUMB_OPT("nodio", direct_io, 0),
        DDUMB_OPT("fuse_default", fuse_default, 1),
        DDUMB_OPT("nofuse_default", fuse_default, 0),
//        DDUMB_OPT("attr_timeout=%lf", attr_timeout, 0), // handled by ddumb_opt_proc() tokeep order of arguments

        FUSE_OPT_KEY("-d", KEY_DEBUG),

        FUSE_OPT_KEY("-h", KEY_HELP),
        FUSE_OPT_KEY("--help", KEY_HELP),

        FUSE_OPT_KEY("-V", KEY_VERSION),
        FUSE_OPT_KEY("--version", KEY_VERSION),

        FUSE_OPT_END
};

static int ddumb_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    // This function is called ONLY with arguments recognized by FUSE
//    struct ddumb_param *param = data;

    char line[4096];
    (void) arg;
    (void) outargs;

//    fprintf(stderr,"ddumbr_opt_proc key=%d arg=%s\n", key, arg);

    switch (key) {
    case KEY_HELP:
            fuse_opt_add_arg(outargs, "-h");
            fuse_main(outargs->argc, outargs->argv, &ddumb_ops, NULL);
            fprintf(stderr,
//                    "usage: %s mountpoint [options]\n"
                    "\n"
                    "ddumbfs options:\n"
                    "\n"
                    "    -o parent=DIR      directory to mount as a ddumbfs\n"
                    "    -o pool=NUM        number of writer in the pool, 0=disable, >0 = CPUs, <0 = CPUs*NUM/100\n"
                    "    -o [no]lock_index  lock index into memory (default on)\n"
                    "    -o [no]dio         use direct_io for internal access (default auto)\n"
                    "    -o [no]fuse_default enable default fuse options* (default on)\n"
                    "    -o check           force filesystem check at startup\n"
                    "    -o reclaim=NUM     a reclaim() is started when disk usage is above this value in %%\n"
                    "\n\n"
                    "    fuse_default_options = \"%s\"\n"
//                    , outargs->argv[0]
                      , fuse_default_options
                                    );
            exit(1);

    case KEY_VERSION:
            fprintf(stderr, "ddumbfs version %s\n", PACKAGE_VERSION);
            fuse_opt_add_arg(outargs, "--version");
            fuse_main(outargs->argc, outargs->argv, &ddumb_ops, NULL);
            exit(0);

    case KEY_DEBUG:
            fprintf(stderr, "debug ON\n");
            ddfs_debug=1;
            return 1;
    case -1:
            // just to get attr_timeout, give it back to fuse
            strncpy(line, arg, sizeof(line));
            char *key=trim(strtok(line, "="));
            char *value=trim(strtok(NULL, "="));
            if (0==strcmp(key, "attr_timeout"))
            {
                ddumb_param.attr_timeout=strtod(value, NULL);
            }
            break;
    }
    return 1;
}

struct ddfs_ctx ddfsctx;

int main(int argc, char *argv[])
{
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    char line[4096];
    long long int i;

    ddfs=&ddfsctx;
    umask(0);

    // generate the original command line argument (for information only)
    for (i=0, line[0]='\0'; i<argc; i++)
    {
        if (i) strncat(line, " ", sizeof(line)-1);
        strncat(line, argv[i], sizeof(line)-1);
    }
    ddumb_param.command_args=strdup(line);

    if (fuse_opt_parse(&args, &ddumb_param, ddumb_opts, ddumb_opt_proc)==1)
    {   // error  parsing options
        return -1;
    }

    if (!ddumb_param.parent) {
        fprintf(stderr, "ERROR: parent directory is missing\n");
        return 1;
    }

    snprintf(line, sizeof(line), "-osubtype=ddumbfs,fsname=%s", ddumb_param.parent);
    fuse_opt_insert_arg(&args, 1, line);
    if (ddumb_param.fuse_default)
    {   // insert fuse default option at the begining to allow overwriting
        fuse_opt_insert_arg(&args, 1, fuse_default_options);
    }

    // generate the extended command line argument (for information only)
    for (i=0, line[0]='\0'; i<args.argc; i++)
    {
        if (i) strncat(line, " ", sizeof(line)-1);
        strncat(line, args.argv[i], sizeof(line)-1);
    }
    ddumb_param.ext_command_args=strdup(line);

    char pdir[FILENAME_MAX];

    if (ddfs_find_parent(ddumb_param.parent, pdir))
    {
        fprintf(stderr, "Not a valid parent directory: %s\n", ddumb_param.parent);
        return -1;
    }
    strcpy(ddumb_param.parent, pdir); // I can copy back pdir because len(pdir)<=len(ddumb_param.parent)

    if (is_mounted(ddumb_param.parent))
    {
        fprintf(stderr, "filesystem is already mounted: %s\n", ddumb_param.parent);
        return 1;
    }

    //
    // load config file
    //
    if (ddfs_loadcfg(ddumb_param.parent, stderr))
    {
        return 1;
    }

    if (ddfs->rebuild_fsck)
    {
        fprintf(stderr, "Last file system rebuild didn't finished, 'rebuild' your filesystem before mounting it !\n");
        return 1 ;
    }


    fprintf(stderr,"hash:      %s\n", ddfs->c_hash);
    fprintf(stderr,"direct_io: %d %s\n", ddumb_param.direct_io, ddumb_param.direct_io?(ddumb_param.direct_io==2?"auto":"enable"):"disable");
    fprintf(stderr,"reclaim:   %d\n", ddumb_param.reclaim);
    //
    // writers pool
    //
    if (ddumb_param.pool<0)
    {   // pool=auto, search number of cpu

    	int count=ddfs_cpu_count();
    	if (count<=0) return 1;
        ddumb_param.pool=-0.01*ddumb_param.pool*count;
        if (ddumb_param.pool<=0) ddumb_param.pool=1;
    }

    if (ddumb_param.pool)
    {
        fprintf(stderr, "writer pool: %d cpus\n", ddumb_param.pool);
    }
    else
    {
        fprintf(stderr, "writer pool: disable\n");
    }

    //
    // init ddfs
    //
    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, ddumb_param.direct_io, DDFS_NOLOCKINDEX, stderr))
    {
        exit(1);
    }
    ddfs->lock_index=ddumb_param.lock_index; // lock the index later

    if (ddfs_debug)
    {
        ddfs_logger->level=LOG_DEBUG;
        ddfs_logger->handlers[0].level=LOG_DEBUG;
    }

    //
    // init some aux bit array
    //
    bit_array_init(&ba_found_in_files, ddfs->c_block_count, 0);

    // init xstat root
    xstat_root.root=NULL;
    pthread_mutex_init(&xstat_root.mutex, NULL);
    xstat_root.xstat=NULL;

    pthread_spin_init(&reclaim_spinlock, 0);

    // force all index block to be read/loaded at statup
    for (i=0; i<ddfs->c_node_block_count; i++)
    {
    	char ch=ddfs->nodes[i*ddfs->c_index_block_size];
    	(void)ch; // silent the warning
    }

    if (chdir(ddfs->rdir)==-1)
    {
        fprintf(stderr, "ERROR changing to directory %s (%s)\n", ddfs->rdir, strerror(errno));
        return 1;
    }

    if (!isdir(".ddumbfs"))
    {
        char cmdbuf[1024];
        fprintf(stderr, "ERROR system directory .ddumbfs not found in %s!\n", getcwd(cmdbuf, sizeof(cmdbuf)));
        return 1;
    }

//    snfprintf(stderr,subdir_root, sizeof(subdir_root), "-omodules=subdir,subdir=/%s/%s", param.root, TOP_DIR);
//    fuse_opt_add_arg(&args, subdir_root);

    /* be careful, fuse can generate multiple simultaneous read. ddumbfs_read()
     * in direct_io mode use a unique aligned buffer for its read, it must be protected
     * to avoid simultaneous access, or "-osync_read" could be used to disable this feature
     * fuse_opt_add_arg(&args, "-osync_read");
     */

    if (ddfs_lock(".autofsck"))
    {
    	fprintf(stderr, "cannot create .autofsck file: %s\n", strerror(errno));
        return 1;
    }

    if (ddfs->auto_fsck || ddumb_param.check_at_start)
    {
        DDFS_LOG(LOG_INFO, "check filesystem %s\n", ddumb_param.parent);
        L_SYS(LOG_INFO, "check filesystem %s\n", ddumb_param.parent);
        if (ddfs_fsck(DDFS_NORELAXED, DDFS_NOVERBOSE, DDFS_NOPROGRESS))
        {
            fprintf(stderr, "ERROR filesystem corrupted. Run fsck !\n");
            return 1;
        }
        reclaim_could_find_free_blocks=0;
    }

    return fuse_main(args.argc, args.argv, &ddumb_ops, NULL);
}
