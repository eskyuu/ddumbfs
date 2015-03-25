/**
 * @ddfslib.c
 * @author  Alain Spineux <alain.spineux@gmail.com>
 * @version 0.0
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
 * ddfslib handle basic components of the ddumbfs filesystem
 *
 */

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE
#define _POSIX_C_SOURCE 199309L  // to get nanonsleep

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <stddef.h>
#include <mntent.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <mhash.h>

#include "ddfslib.h"

int ddfs_debug=0;
char *ddfs_logname="ddumbfs";

// Always define a swap64 function, as it's useful.
// from http://www.boxbackup.org
inline uint64_t ddumbfs_swap64(uint64_t x)
{
        return ((x & 0xff) << 56 |
                (x & 0xff00LL) << 40 |
                (x & 0xff0000LL) << 24 |
                (x & 0xff000000LL) << 8 |
                (x & 0xff00000000LL) >> 8 |
                (x & 0xff0000000000LL) >> 24 |
                (x & 0xff000000000000LL) >> 40 |
                (x & 0xff00000000000000LL) >> 56);
}

typedef struct ddfs_ctx struct_ddfs_ctx;

struct cfgfile cfg[]={
                    { "file_header_size", 'I', offsetof(struct_ddfs_ctx, c_file_header_size) },
                    { "hash", 'S', offsetof(struct_ddfs_ctx, c_hash) },
                    { "hash_size", 'I', offsetof(struct_ddfs_ctx, c_hash_size) },
                    { "block_size", 'I', offsetof(struct_ddfs_ctx, c_block_size) },
                    { "index_block_size", 'I', offsetof(struct_ddfs_ctx, c_index_block_size) },
                    { "node_overflow", 'D', offsetof(struct_ddfs_ctx, c_node_overflow) },
                    { "reuse_asap", 'I', offsetof(struct_ddfs_ctx, c_reuse_asap) },
                    { "auto_buffer_flush", 'I', offsetof(struct_ddfs_ctx, c_auto_buffer_flush) },
                    { "auto_sync", 'I', offsetof(struct_ddfs_ctx, c_auto_sync) },

                    { "partition_size", 'L', offsetof(struct_ddfs_ctx, c_partition_size) },
                    { "block_count", 'L', offsetof(struct_ddfs_ctx, c_block_count) },
                    { "addr_size", 'I', offsetof(struct_ddfs_ctx, c_addr_size) },
                    { "node_size", 'I', offsetof(struct_ddfs_ctx, c_node_size) },
                    { "node_count", 'L', offsetof(struct_ddfs_ctx, c_node_count) },
                    { "node_block_count", 'L', offsetof(struct_ddfs_ctx, c_node_block_count) },

                    { "freeblock_offset", 'L', offsetof(struct_ddfs_ctx, c_freeblock_offset) },
                    { "freeblock_size", 'L', offsetof(struct_ddfs_ctx, c_freeblock_size) },
                    { "node_offset", 'L', offsetof(struct_ddfs_ctx, c_node_offset) },
                    { "index_size", 'L', offsetof(struct_ddfs_ctx, c_index_size) },
                    { "index_block_count", 'L', offsetof(struct_ddfs_ctx, c_index_block_count) },

                    { "root_directory", 'S', offsetof(struct_ddfs_ctx, c_root_directory) },
                    { "block_filename", 'S', offsetof(struct_ddfs_ctx, c_block_filename) },
                    { "index_filename", 'S', offsetof(struct_ddfs_ctx, c_index_filename) },
                    { NULL, '\0', 0 },
};

struct ddfs_ctx *ddfs=NULL;

char *special_filenames[]={ "stats", "stats0", "reclaim", "test", NULL};


/**
 * return the value corresponding with the unit in K, M, G, or T
 *
 * @param u the character in K, M, G, or T and lowercase
 * @return the 1024 power matching the unit
 */
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

/**
 * trim space in front and at end of a string
 *
 * The string is modified in place
 *
 * @param s the string
 * @return the same address as the argument
 */
char *trim(char *s)
{
    if (s!=NULL)
    {
        char *p=s;
        int l=strlen(p);

        while(isspace(p[l-1])) p[--l]=0;
        while(*p && isspace(*p)) ++p, --l;

        memmove(s, p, l+1);
    }
    return s;
}

/**
 * return current time in mili sec
 *
 * @return time in mili seconds
 */
long long int now()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*1000LL+tv.tv_usec/1000LL;
}

/**
 * return current time in micro sec
 *
 * @return time in micro seconds
 */
long long int micronow()
{
    struct timeval tv;
    struct timezone tz;
    gettimeofday(&tv, &tz);
    return tv.tv_sec*1000*1000+tv.tv_usec;
}

/**
 * return current time in micro sec
 *
 * @return time in micro seconds
 */
void dsleep(double delay)
{
    struct timespec req, rem;
    req.tv_sec=delay;
    req.tv_nsec=(delay-req.tv_sec)*1000000000.0L;
    while (1)
    {   // run until the end whatever interrupt
        if (nanosleep(&req, &rem)==0) break;
        if (nanosleep(&rem, &req)==0) break;
    }
}


/**
 * return true if path exist
 *
 * @param path the path
 * @return true if path is a regular file
 */
int pathexists(const char *path)
{
  struct stat stats;
  return stat (path, &stats)==0;
}

/**
 * return true if path is a directory
 *
 * @param path the path
 * @return true if path is a directory
 */
int isdir(const char *path)
{
  struct stat stats;
  return stat (path, &stats)==0 && S_ISDIR(stats.st_mode);
}

/**
 * return true if path is a regular file
 *
 * @param path the path
 * @return true if path is a regular file
 */
int isregfile(const char *path)
{
  struct stat stats;
  int res=stat(path, &stats);
  return res==0 && S_ISREG(stats.st_mode);
}

/**
 * check if a device/source is already mounted
 *
 * @param device_name the name of the device/source
 * @return True if already mounted
 */
int is_mounted(const char *device_name)
{
    FILE * f;
    struct mntent * mnt;

    if ((f = setmntent ("/etc/mtab", "r")) == NULL) return 0;
    while ((mnt = getmntent (f)) != NULL)
    {
        if (strcmp (device_name, mnt->mnt_fsname) == 0)
            break;

    }
    endmntent (f);
    if (!mnt) return 0;

    return 1;
}

/**
 * remove node from the index
 *
 * @param node_idx the node to remove
 */
void node_delete(nodeidx node_idx)
{
    DDFS_LOG_DEBUG("node_delete node_idx=%lld\n", node_idx);
    nodeidx idx=node_idx+1;

    // search for all nodes after node_idx that should be moved up
    while (idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr==0)
        {
            break;
        }
        // what is the expected normal place for this node ?
        nodeidx cidx=ddfs_hash2idx(node+ddfs->c_addr_size);
        if (cidx<idx)
        {
        }
        else if (cidx==idx)
        {
            break;
        }
        else // idx<cidx
        {   // node before its ideal place, ignore it, it should be solved by check_node_order() later
            // stop here, the process will continue when check_node_order() will delete this one
            break;
        }
        idx++;
    }
    idx--;
    if (idx>node_idx) memmove(ddfs->nodes+(node_idx*ddfs->c_node_size), ddfs->nodes+((node_idx+1)*ddfs->c_node_size), (idx-node_idx)*ddfs->c_node_size);
    memset(ddfs->nodes+(idx*ddfs->c_node_size), '\0', ddfs->c_node_size);
}

/**
 * node_add()
 * add a node to the index at the right place,
 * node_add() handle duplicate hash and order such node regarding the addr
 * the node must be valid: addr!= 0 and 1 and hash != zero and null
 *
 * @param new_node
 * @param unique
 * @return 0 for success or if node already exists
 */
int node_add(const unsigned char* new_node, int unique)
{
    const unsigned char *new_hash=new_node+ddfs->c_addr_size;
    blockaddr new_addr=ddfs_get_node_addr(new_node);
    nodeidx node_idx;

    //DDFS_LOG_DEBUG("node_add addr=%lld\n", new_addr);
    for (node_idx=ddfs_hash2idx(new_hash); node_idx<ddfs->c_node_count; node_idx++)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr==0)
        {
            memcpy(node, new_node, ddfs->c_node_size);
            return 0;
        }
        else
        {
            int res=memcmp(node+ddfs->c_addr_size, new_hash, ddfs->c_hash_size);
            if (res<0)
            {
                continue;
            }
            if (res==0)
            {	// hash matches
            	if (unique==na_duplicate_hash_allowed)
            	{
					if (new_addr<addr)
					{
						continue;
					}
					if (new_addr==addr)
					{   // the node is already in the index, do nothing
						return 0;
					}
            	}
            	else
            	{
            		ddfs_convert_addr(new_addr, node);
            		node_idx++;
            		if (node_idx>=ddfs->c_node_count) return 0;
					node=ddfs->nodes+(node_idx*ddfs->c_node_size);
					while (0==memcmp(node+ddfs->c_addr_size, new_hash, ddfs->c_hash_size)) node_delete(node_idx);
					return 0;
            	}
            }
            else
            {
                // add new_node before this node
            }
            // insert node before current, search for a free node
            nodeidx free_node_idx=ddfs_search_free_node(node_idx+1, ddfs->c_node_count);
            if (free_node_idx<0)
            {   // this should NEVER NERVER NEVER append
                break;
            }
            memmove(node+ddfs->c_node_size, node, (free_node_idx-node_idx)*ddfs->c_node_size);
            memcpy(node, new_node, ddfs->c_node_size);
            return 0;
        }
    }
    ddfs->error_nodes_exausted++;
    DDFS_LOG(LOG_ERR, "unexpected error in node_add, no more free nodes.\n");
    return 1;
}

int node_fix(nodeidx node_idx)
{
    DDFS_LOG_DEBUG("node_fix node_idx=%lld\n", node_idx);
    unsigned char node[NODE_SIZE];
    memcpy(node, ddfs->nodes+(node_idx*ddfs->c_node_size), ddfs->c_node_size);
    node_delete(node_idx);
    return node_add(node, na_duplicate_hash_allowed);
}


/**
 * create a "lock" file in parent directory (like the ".autofsck")
 *
 * @param name the name of the lock file
 * @return 0 for success
 */
int ddfs_lock(const char *name)
{
    char filename[FILENAME_MAX];

    snprintf(filename, sizeof(filename), "%s/%s", ddfs->pdir, name);
    int fd=open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd!=-1) close(fd);
    return fd==-1;

}

/**
 * remove a "lock" file in parent directory (like the ".autofsck")
 *
 * @param name the name of the lock file
 * @return -1 if unlink failed, 0 for nothing, 1 for yes I removed it
 */
int ddfs_unlock(const char *name)
{
    char filename[FILENAME_MAX];

    snprintf(filename, sizeof(filename), "%s/%s", ddfs->pdir, name);
    if (isregfile(filename))
    {
        if (unlink(filename)==-1) return -1;
        return 1;
    }
    return 0;
}

/**
 * test if "lock" file exist (like the ".autofsck")
 *
 * @param name the name of the lock file
 * @return 0 don't exist,
 */
int ddfs_testlock(const char *name)
{
    char filename[FILENAME_MAX];

    snprintf(filename, sizeof(filename), "%s/%s", ddfs->pdir, name);
    return isregfile(filename);
}

/**
 * calculate the hash of a block
 *
 * @param hash where to store the hash
 * @param block the block of data
 */
void ddfs_hash(const char *block, unsigned char *hash)
{
    MHASH td;

    td=mhash_init(ddfs->c_hash_id);
    assert(td!=MHASH_FAILED);
    mhash(td, block, ddfs->c_block_size);
    mhash_deinit(td, hash);
}

/**
 * convert integer address into node address of c_addr_size byte
 *
 * @param addr the address
 * @param node_addr where to write the address
 */
inline void ddfs_convert_addr(blockaddr addr, unsigned char *node_addr)
{
    uint64_t baddr=addr;
    // address are in little endian in node
#if defined(WORDS_BIGENDIAN)
    baddr=ddumbfs_swap64(baddr)
#endif
    memcpy(node_addr, &baddr, ddfs->c_addr_size);
}
/**
 * initialize a node
 *
 * @param node_idx the position of the node
 * @param addr the node address
 * @param hash the node hash
 */
void ddfs_set_node(nodeidx node_idx, blockaddr addr, const unsigned char *hash)
{
    unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);

    ddfs_convert_addr(addr, node);
    memcpy(node+ddfs->c_addr_size, hash, ddfs->c_hash_size);
}

/**
 * extract address part of a node
 *
 * address are the first c_addre_size byte of a node
 *
 * @param node the address of the node
 * @return the extracted address
 */
blockaddr ddfs_get_node_addr(const unsigned char *node)
{
    uint64_t baddr=0;
    memcpy(&baddr, node, ddfs->c_addr_size);
    // address are in little endian in node
#if defined(WORDS_BIGENDIAN)
    baddr=ddumbfs_swap64(baddr)
#endif
    return baddr;
}

/**
 * calculate expected position of node inside the index
 *
 * the position is calculated using first bits of the hash
 * double have only 52 bits (instead of 64)
 * 52bits of 4K block = 16384 To I thing this is enough
 *
 * @param hash the hash of the node
 * @return position
*/

nodeidx ddfs_hash2idx(const unsigned char *hash)
{
    nodeidx idx=ddfs_ntoh64(*(uint64_t *)hash)*ddfs->coef_hash2idx;
    assert(idx<ddfs->c_node_count);
    return idx;
}

/**
 * search hash inside index
 *
 * @param hash the hash to search
 * @param addr the address of the matching block hash to search
 * @return index of matching node or <0 if not found
 */
nodeidx ddfs_search_hash(const unsigned char *hash, blockaddr *addr)
{
    nodeidx node_idx=ddfs_hash2idx(hash);
    if ((node_idx==ddfs->zero_block_cidx && 0==memcmp(hash, ddfs->zero_block_hash, ddfs->c_hash_size))
      ||(node_idx==ddfs->null_block_cidx && 0==memcmp(hash, ddfs->null_block_hash, ddfs->c_hash_size)))
    {
      *addr=0;
      return -1;  // ZERO BLOCK
    }

    while (node_idx<ddfs->c_node_count)
    {
        unsigned char*node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        *addr=ddfs_get_node_addr(node);
        if (*addr==0)
        {
            *addr=1;
            return -2;  // NODE EMPTY
        }
        // the node is not empty, compare
        int res=memcmp(node+ddfs->c_addr_size, hash, ddfs->c_hash_size);
        if (res==0)
        {
            return node_idx; // FOUND
        }
        else if (res>0)
        {
            // the new node should be before this one
            *addr=1;
            return -3;  // NOT FOUND
        }
        node_idx++;
    }
    return -4; // UNEXPECTED !!!
}

/**
 * search first free node from idx
 *
 * @param idx from here
 * @param stop_before but before here (use c_node_count for end)
 * @return return first free node address or -1 if not more free node
 */
nodeidx ddfs_search_free_node(nodeidx idx, nodeidx stop_before)
{
    blockaddr addr;

    while (idx<stop_before)
    {
        addr=ddfs_get_node_addr(ddfs->nodes+(idx*ddfs->c_node_size));
        if (addr==0) return idx;
        idx+=1;
    }
    ddfs->error_nodes_exausted++;
    return -1;
}

/**
 * locate the insertion position of the hash in the index or the hash itself
 *
 * return 0 if the hash is already in the index, this avoid to
 * double-check the hash. We don't have its position, but have the
 * block address, this is enough for what we need.
 *
 * @param hash the hash
 * @param addr the addr of the matching block in the BlockFile
 * @param node_idx the position of the matching node
 * @return <0 for error, 0 if hash is found, else the insertion position
 */
int ddfs_locate_hash(unsigned char *hash, blockaddr *addr, nodeidx *node_idx)
{
    *node_idx=ddfs_hash2idx(hash);
    int i=1;

//    preload_node(*node_idx);
    while (*node_idx<ddfs->c_node_count)
    {
        *addr=ddfs_get_node_addr(ddfs->nodes+(*node_idx*ddfs->c_node_size));
        if (*addr==0)
        {   // the node is free, use it to store the new block
            return i;
        }
        else
        {
            // the node is not free, compare ?
            int res=memcmp(ddfs->nodes+(*node_idx*ddfs->c_node_size+ddfs->c_addr_size), hash, ddfs->c_hash_size);
            if (res==0)
            {   // the hash is found, this is done
                return 0; //FOUND
            }
            else if (res<0)
            {
                // the hash must go after, continue to search
//                ddumb_statistic.block_write_try_next_node++;
            }
            else
            {
                // the hash should be inserted before this node
                return i;
            }
        }
        i++;
        (*node_idx)++;
    }
    // this should NEVER NEVER NEVER append
    ddfs->error_nodes_exausted++;
    DDFS_LOG(LOG_ERR, "ddfs_locate_hash CRITICAL ERROR no more free nodes !\n");
    return -ENOSPC;
}

/**
 * convert a raw filer header into a file_header
 *
 * check if the MAGIC of file header is valid
 *
 * @param fh destination
 * @param raw source
 * @return 0 if the MAGIC of file header is valid
 */
int file_header_set(int fd, struct file_header *fh)
{
    return file_header_set_conv(fd, &(fh->size));
}

int file_header_set_conv(int fd, uint64_t *size)
{
    char header[FILE_HEADER_SIZE];
    int len;

    len=pread(fd, header, ddfs->c_file_header_size, 0);
    if (len==ddfs->c_file_header_size)
    {
        if (0==memcmp(header, DDFS_MAGIC_FILE, DDFS_MAGIC_FILE_LEN))
        {
            *size=ddfs_ntoh64(*(uint64_t *)(header+DDFS_MAGIC_FILE_LEN));
            return len;
        }
        else return 0;
    }
    else if (len==0)
    {
        *size=0;
        return ddfs->c_file_header_size;
    }
    return len;
}

/**
 * convert file_header into a raw file header
 *
 * @param fh the source
 * @param raw the destination
 */
int file_header_get(int fd, struct file_header *fh)
{
    return file_header_get_conv(fd, fh->size);
}

int file_header_get_conv(int fd, uint64_t size)
{
    char header[FILE_HEADER_SIZE];

    memcpy(header, DDFS_MAGIC_FILE, DDFS_MAGIC_FILE_LEN);
    *(uint64_t *)(header+DDFS_MAGIC_FILE_LEN)=ddfs_hton64(size);
    return pwrite(fd, header, ddfs->c_file_header_size, 0);
}

/**
 * copy the data from one file_header to another
 *
 * @param dst the destination
 * @param src the source
 */
void file_header_copy(struct file_header *dst, struct file_header *src)
{
    dst->size=src->size;
}

blockaddr ddfs_alloc_block()
{
    blockaddr addr=bit_array_alloc(&ddfs->ba_usedblocks);
    // block 0 & 1 are reserver this is a mistake to allocate them
    while (addr==0 || addr==1) addr=bit_array_alloc(&ddfs->ba_usedblocks);
    if (addr>0) ddfs->usedblock++;
    return addr;
}
/**
 * read part of a block from block file
 *
 * @param addr the address of the block
 * @param buf where to write the data
 * @param size how many byte to read
 * @param gap the offset inside the block
 * @return how many byte have been read or -1 for error
 */
int ddfs_read_block(blockaddr addr, char *buf, int size, int gap)
{
    if (addr>=ddfs->c_block_count) return 0;
    if (addr==0 || addr==1)
    {
        memset(buf, '\0', size);
        return size;
    }

    long long int offset=(addr<<ddfs->block_size_shift)+gap;
    return pread(ddfs->bfile_ro, buf, size, offset);
}
/**
 * read a block from block file
 *
 * @param addr the address of the block
 * @param buf where to write the data
 * @return how many bytes have been read or -1 for error
 */
int ddfs_read_full_block(blockaddr addr, char *buf)
{
    return ddfs_read_block(addr, buf, ddfs->c_block_size, 0);
}

void ddfs_forced_read_full_block(blockaddr addr, char *buf, int hw_block_size)
{
    int i;

    memset(buf, '\0', ddfs->c_block_size);
    if (addr==0 || addr==1 || addr>=ddfs->c_block_count) return ;
    for (i=0; i<ddfs->c_block_size; i+=hw_block_size)
    {
        if (hw_block_size!=pread(ddfs->bfile_ro, buf, hw_block_size, (addr<<ddfs->block_size_shift)+i*hw_block_size))
        { // I don't care, I'm trying to read most of the data from a failing block
        }
    }
    return;
}

blockaddr ddfs_store_block(const char *block, blockaddr force_addr)
{
    if (force_addr==0)
    {
        force_addr=ddfs_alloc_block();
        if (force_addr<0)
        {
            DDFS_LOG(LOG_ERR, "ddfs_store_block no more free blocks !\n");
            return -ENOSPC;
        }
    }
    int len=pwrite(ddfs->bfile, block, ddfs->c_block_size, force_addr<<ddfs->block_size_shift);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "ddfs_store_block cannot write block: %s\n", strerror(errno));
        return -errno;
    }
    else if (len!=ddfs->c_block_size)
    {
        DDFS_LOG(LOG_ERR, "ddfs_store_block short write, only %d/%d bytes\n", len, ddfs->c_block_size);
        return -EIO;
    }
    return force_addr;
}

/**
 * write a block in the filesystem
 *
 * if the block is new, write it in the BlockFile and add it to the IndexFile
 * note: ddfs_write_block2 in ddumbfs.c handle stats and multi-threading
 *
 * @param block the block
 * @param bhash return the hash of the block
 * @return the address of the block or <0 for error
 */
blockaddr ddfs_write_block(const char *block, unsigned char *bhash)
{
    blockaddr addr;
    nodeidx node_idx;

    ddfs_hash(block, bhash);
    if (0==memcmp(bhash, ddfs->zero_block_hash, ddfs->c_hash_size)) return 0;

    int res=ddfs_locate_hash(bhash, &addr, &node_idx);

    if (res<0) return res;

    if (res==0)
    {
        return addr;
    }
    // the hash is not found
    // store the block in the BlockFile
    blockaddr baddr=ddfs_alloc_block();
    if (baddr<0) return baddr;

    if (addr!=0)
    {   // we must insert the node; search for a free node
        nodeidx free_node_idx=ddfs_search_free_node(node_idx+1, ddfs->c_node_count);
        if (free_node_idx<0)
        {   // this should NEVER NERVER NEVER append
            DDFS_LOG(LOG_ERR, "ddfs_write_block CRITICAL ERROR no more free nodes !\n");
            return -ENOSPC;
        }
        memmove(ddfs->nodes+((node_idx+1)*ddfs->c_node_size), ddfs->nodes+(node_idx*ddfs->c_node_size), (free_node_idx-node_idx)*ddfs->c_node_size);
    }
    // now node_idx is ready to receive new node
    ddfs_set_node(node_idx, baddr, bhash);

    baddr=ddfs_store_block(block, baddr);

    return baddr;
}

/**
 * keep a copy of usedblock and rollout previous one
 *
 * used for recovery to know witch block where recently allocated
 * @return 0 for success, -errno for error
 */
int ddfs_save_usedblocks()
{
    char filename[FILENAME_MAX];
    char filename0[FILENAME_MAX];

    snprintf(filename, sizeof(filename), "%s/%s", ddfs->pdir, DDFS_BACKUP_USEDBLOCK);
    snprintf(filename0, sizeof(filename0), "%s/%s.0", ddfs->pdir, DDFS_BACKUP_USEDBLOCK);

    int res=bit_array_save(&ddfs->ba_usedblocks, filename0, DDFS_SYNC);
    if (res!=0)
    {
        if (res<0)
        {
            DDFS_LOG(LOG_ERR, "cannot save usedblock in %s (%s)\n", filename0, strerror(-res));
        }
        else
        {
            DDFS_LOG(LOG_ERR, "cannot save usedblock in %s\n", filename0);
        }
        unlink(filename0);
        return res;
    }

    if (pathexists(filename))
    {
        // Now I must SYNC blockfile and indexfile,
    	// this is the smart place to sync them without lock
    	// because I must have bfile and ifile newer than usedblock
    	fsync(ddfs->bfile);
    	fsync(ddfs->ifile);
    	// Now both are synced, the old usedblock is still valid but now,
    	// the new one too, I can rename it and make it the reference

        res=unlink(filename);
        if (res==-1)
        {
            DDFS_LOG(LOG_ERR, "cannot delete %s (%s)\n", filename, strerror(errno));
            return -errno;
        }
    }

    res=rename(filename0, filename);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot rename %s in %s (%s)\n", filename0, filename, strerror(errno));
        return -errno;
    }
    return 0;
}

/**
 * load keep a copy of usedblocks and rollout previous one
 *
 * used for recovery to know wich block where recently allocated
 * @return 0 for success, -errno for error
 */
int ddfs_load_usedblocks(struct bit_array *ba)
{
    char filename[FILENAME_MAX];
    char filename0[FILENAME_MAX];

    snprintf(filename, sizeof(filename), "%s/%s", ddfs->pdir, DDFS_BACKUP_USEDBLOCK);
    snprintf(filename0, sizeof(filename0), "%s/%s.0", ddfs->pdir, DDFS_BACKUP_USEDBLOCK);

    struct stat stats;
    struct stat stats0;
    int res=stat(filename, &stats);
    int res0=stat(filename0, &stats0);

/*
    fprintf(stderr, "stat=%dd\n", stat(filename, &stats));
    fprintf(stderr, "size=%lld %lld\n", (long long int)stats.st_size, (long long int)(ba->isize*sizeof(bit_int)));
    fprintf(stderr, "load=%d\n", bit_array_load(ba, filename));
*/
    if (   (res ==0 && stats.st_size ==ba->isize*sizeof(bit_int) && bit_array_load(ba, filename)==0)
        || (res0==0 && stats0.st_size==ba->isize*sizeof(bit_int) && bit_array_load(ba, filename0)==0))
    {
        return 0;
    }
    else
    {
    	DDFS_LOG(LOG_ERR, "Cannot load last recently added block backup.\n");
        return -1;
    }
}


/**
 * search upper level ddumbfs parent directory
 *
 * giving a path inside an offline ddumbfs underlying filesystem
 * search the most upper directory having a ddfs.cfg file and ddfsroot directory
 *
 * @param path  the path to search in
 * @param parent copy the parent directory if found, must be large enough
 * @return 0 if found
 */
int ddfs_find_parent(char *path, char *parent)
{
    int len;
    char p1[FILENAME_MAX];

    strcpy(p1, path);
    len=strlen(p1);
    while (len)
    {
        while (len>0 && p1[len-1]=='/') p1[--len]='\0';
        if (isdir(p1))
        {
            sprintf(p1+len, "/%s", CFG_FILENAME);
            if (isregfile(p1))
            {
                sprintf(p1+len, "/%s", ROOT_DIR);
                if (isdir(p1))
                {
                    strncpy(parent, p1, len);
                    parent[len]='\0';
                    return 0;
                }
            }
        }
        while (len>0 && p1[len-1]!='/') p1[--len]='\0';
    }
    return 1;
}

/**
 * load the configuration file in c_* variables
 *
 * @param ddfs_parent the parent directory
 * @return 0 if ok, else error code
 */
int ddfs_loadcfg(char *ddfs_parent, FILE *output)
{
    char cfgfilename[FILENAME_MAX];
    char line[1024];

    ddfs->pdir=strdup(ddfs_parent);
    if (!ddfs->pdir)
    {
        fprintf(stderr,"cannot allocate memory for parent directory: %s\n", ddfs_parent);
        return 4;
    }

    snprintf(cfgfilename, sizeof(cfgfilename), "%s/%s", ddfs->pdir, CFG_FILENAME);
    FILE *cfgfile=fopen(cfgfilename, "r");
    if (!cfgfile)
    {
        perror(cfgfilename);
        return 1;
    }

    struct cfgfile *c;
    for (c=cfg; c->name!=NULL; c++) c->misc=0;

    while (fgets(line, sizeof(line), cfgfile))
    {
        char *key=trim(strtok(line, ":"));
        char *value=trim(strtok(NULL, "\n"));

        for (c=cfg; c->name!=NULL; c++)
        {
            if (strcmp(c->name, key)==0)
            {
                c->misc=1;
                void *p=((char*)ddfs)+c->offset;
                switch (c->type)
                {
                    case 'I':
                        *(int*)p=strtol(value, NULL, 10);
                        if (output) fprintf(output,"\t%s %d\n", c->name, *(int*)p);
                        break;
                    case 'L':
                        *(long long int*)p=strtoll(value, NULL, 10);
                        if (output) fprintf(output,"\t%s %lld\n", c->name, *(long long int*)p);
                        break;
                    case 'D':
                        *(double*)p=strtod(value, NULL);
                        if (output) fprintf(output,"\t%s %.2f\n", c->name, *(double*)p);
                        break;
                    case 'S':
                        *(char **)p=strdup(value);
                        if (output) fprintf(output,"\t%s %s\n", c->name, *(char **)p);
                        break;
                }
                continue;
            }
        }
    }
    fclose(cfgfile);

    // use bits shift and mask instead of div and modulo
    ddfs->block_gap_mask=ddfs->c_block_size-1;
    ddfs->block_boundary_mask=~ddfs->block_gap_mask;
    ddfs->block_size_shift=0;
    int shift=ddfs->c_block_size;
    while (shift>1)
    {
        shift>>=1;
        ddfs->block_size_shift++;
    }

    for (c=cfg; c->name!=NULL; c++)
    {
        if (c->misc==0) fprintf(stderr,"missing: %s\n", c->name);
    }

    if (0==strcmp(ddfs->c_hash, "SHA1")) ddfs->c_hash_id=MHASH_SHA1;
    else if (0==strcmp(ddfs->c_hash, "TIGER") || 0==strcmp(ddfs->c_hash, "TIGER192")) ddfs->c_hash_id=MHASH_TIGER192;
    else if (0==strcmp(ddfs->c_hash, "TIGER128")) ddfs->c_hash_id=MHASH_TIGER128;
    else if (0==strcmp(ddfs->c_hash, "TIGER160")) ddfs->c_hash_id=MHASH_TIGER160;
    else
    {
        fprintf(stderr,"HASH unknown: %s\n", ddfs->c_hash);
        return 2;
    }

    // check if some value are compatible with size of allocated array
    if (HASH_SIZE<ddfs->c_hash_size)
    {
        fprintf(stderr,"ERROR: hash_size > hard coded HASH_SIZE (%d>%d)\n", ddfs->c_hash_size, HASH_SIZE);
        return 3;
    }

    if (ADDR_SIZE<ddfs->c_addr_size)
    {
        fprintf(stderr,"ERROR: addr_size > hard coded ADDR_SIZE (%d>%d)\n", ddfs->c_addr_size, ADDR_SIZE);
        return 3;
    }

    if (INDEX_BLOCK_SIZE<ddfs->c_index_block_size)
    {
        fprintf(stderr,"ERROR: node_idx_size > hard coded INDEX_BLOCK_SIZE (%d>%d)\n", ddfs->c_index_block_size, INDEX_BLOCK_SIZE);
        return 3;
    }

    if (FILE_HEADER_SIZE<ddfs->c_file_header_size)
    {
        fprintf(stderr,"ERROR: file_header_size > hard coded FILE_HEADER_SIZE (%d>%d)\n", ddfs->c_file_header_size, FILE_HEADER_SIZE);
        return 3;
    }

    ddfs->auto_fsck=ddfs_testlock(".autofsck");
    ddfs->rebuild_fsck=ddfs_testlock(".rebuildfsck");

    return 0;
}

struct xlogger ddfs_logger[1];
struct xloghandler ddfs_loghandler[10];

int ddfs_init_logging()
{
    int ret=0;
    char filename[FILENAME_MAX];
    snprintf(filename, sizeof(filename), "%s/%s", ddfs->rdir, DDFS_LOG_FILE);

    xloghandler_init(ddfs_loghandler+0,  1, LOG_INFO, "%F %T %%{LEVEL} ", strdup(filename), DDFS_LOG_FILE, "a", 10*1024*1024, 10, NULL);
    xloghandler_init(ddfs_loghandler+1,  1, LOG_DEBUG, "%T %%{LEVEL} ", NULL, "stderr", NULL, 0, 0, stderr);
    xloghandler_init(ddfs_loghandler+2, -1, 0, NULL, NULL, NULL, NULL, 0, 0, NULL);

    ddfs_logger->level=LOG_DEBUG;
    ddfs_logger->handlers=ddfs_loghandler;

    ret=xlog(ddfs_logger, LOG_EMERG, NULL); // open files
    return ret;
}


/**
 * initialize the ddfs context
 *
 * direct_io==2 for auto
 * @return 0 if ok, else error code
 */
int ddfs_init(int force, int rebuild, int direct_io, int lock_index, FILE *output)
{
    int res;
    char filename[FILENAME_MAX];
	struct stat sbuf;

    ddfs->lock_index=lock_index;
    ddfs->coef_hash2idx=ddfs->c_block_count*ddfs->c_node_overflow/(65536.0*65536.0*65536.0*65536.0);

    ddfs->special_dir_len=strlen(SPECIAL_DIR);

    //
    // calculate root dir
    //
    ddfs->rdir_len=strlen(ddfs->pdir)+1+strlen(ROOT_DIR);
    ddfs->rdir=malloc(ddfs->rdir_len+1);
    if (!ddfs->rdir)
    {
        fprintf(stderr, "cannot allocate memory for root directory.\n");
        return 4;
    }
    sprintf(ddfs->rdir, "%s/%s", ddfs->pdir, ROOT_DIR);
    if (output) fprintf(output, "root directory: %s\n", ddfs->rdir);

    if (ddfs_init_logging())
    {
        fprintf(stderr, "cannot initialize logging.\n");
        return 9;
    }

    //
    // open block file
    //
    if (ddfs->c_block_filename[0]=='/') sprintf(filename, "%s", ddfs->c_block_filename);
    else sprintf(filename, "%s/%s", ddfs->pdir, ddfs->c_block_filename);
    ddfs->blockfile=strdup(filename);
    if (output) fprintf(output, "blockfile: %s\n", ddfs->blockfile);

    if (direct_io==2) // auto
    {
    	ddfs->direct_io=(stat(ddfs->blockfile, &sbuf)==0 && S_ISBLK(sbuf.st_mode));
    }
    else ddfs->direct_io=direct_io;

	ddfs->align=0;
    if (ddfs->direct_io)
    {
        // I don't want bfile to be cached by the system, because this is only for
        // big files written or read only once. ifile is a better choice for cache !
        // I can use O_DIRECT or posix_fadvise
    	ddfs->align=1;
        ddfs->bfile=open(ddfs->blockfile, O_RDWR | O_DIRECT);
        if (ddfs->bfile==-1)
        {
            perror(ddfs->blockfile);
            return 1;
        }
        // open the block file twice : READ ONLY To speed up read
        ddfs->bfile_ro=open(ddfs->blockfile, O_RDONLY);
        if (ddfs->bfile_ro==-1)
        {
            perror(ddfs->blockfile);
            return 1;
        }

    }
    else
    {
        ddfs->bfile=open(ddfs->blockfile, O_RDWR);
        if (ddfs->bfile==-1)
        {
            perror(ddfs->blockfile);
            return 2;
        }
        ddfs->bfile_ro=ddfs->bfile;
    }

    res=fstat(ddfs->bfile, &sbuf);
    if (res==-1)
    {
    	perror(ddfs->blockfile);
        return 1;
    }
    ddfs->bfile_isblk=S_ISBLK(sbuf.st_mode);
    ddfs->bfile_size=sbuf.st_size;
    ddfs->bfile_last=ddfs->bfile_size/ddfs->c_block_size;

    // allocate buffers for block file
    if (ddfs->align)
    {
        // allocate aligned buffers required by direct_io
        res=posix_memalign((void *)&ddfs->bfile_read_buffer, BLOCK_ALIGMENT, ddfs->c_block_size);
        if (res)
        {
            fprintf(stderr,"ERROR: cannot allocate aligned bfile_read_buffer, (try with -nodio): %s\n", strerror(res));
            return 1;
        }
        res=posix_memalign((void *)&ddfs->aux_buffer, BLOCK_ALIGMENT, ddfs->c_block_size);
        if (res)
        {
            fprintf(stderr,"ERROR: cannot allocate aligned aux_buffer (try with -nodio): %s\n", strerror(res));
            return 1;
        }
    }
    else
    {
        // allocate some buffer
        ddfs->bfile_read_buffer=malloc(ddfs->c_block_size);
        if (ddfs->bfile_read_buffer==NULL)
        {
            fprintf(stderr,"ERROR: cannot allocate bfile_read_buffer: %s\n", strerror(errno));
            return 2;
        }
        ddfs->aux_buffer=malloc(ddfs->c_block_size);
        if (ddfs->aux_buffer==NULL)
        {
            fprintf(stderr,"ERROR: cannot allocate aux_buffer: %s\n", strerror(errno));
            return 2;
        }

        if (0)
        {   // I don't think anymore that fadvise can improve speed
            // try to use posix_fadvise to limit cache use  http://insights.oetiker.ch/linux/fadvise.html

            // I would have to tune  ddfs->bfile_ro too

            res=fdatasync(ddfs->bfile);
            if (res==-1)
            {
                perror(ddfs->blockfile);
                return 2;
            }

            //  POSIX_FADV_NOREUSE looks to be NOOP in linux
            res=posix_fadvise(ddfs->bfile, 0, ddfs->c_partition_size, POSIX_FADV_DONTNEED);
            if (res==-1)
            {
                perror(ddfs->blockfile);
                return 2;
            }
        }
    }

    // check if block file start with appropriate MAGIC
    int len=read(ddfs->bfile_ro, ddfs->bfile_read_buffer, 4096);
    if (len!=4096)
    {
        if (len==-1) perror(ddfs->blockfile);
        else if (len!=4096) fprintf(stderr, "read only len=%d/4096 from %s\n", len, ddfs->blockfile);
        return 5;
    }

    if (0!=strncmp(ddfs->bfile_read_buffer, DDFS_MAGIC_BLOCK, DDFS_MAGIC_BLOCK_LEN))
    {
        fprintf(stderr, "magic not found in blockfile: %s\n", ddfs->blockfile);
        if (!force)
        {
            fprintf(stderr, "try using --force\n");
            return 5;
        }
        else
        {
            printf("Repair BlockFile header: %s\n", ddfs->blockfile);
            // first block is header, second is unused
            memset(ddfs->bfile_read_buffer, '\0', ddfs->c_block_size);
            memcpy(ddfs->bfile_read_buffer, DDFS_MAGIC_BLOCK, DDFS_MAGIC_BLOCK_LEN);
            len=pwrite(ddfs->bfile, ddfs->bfile_read_buffer, ddfs->c_block_size, 0);
            if (len!=ddfs->c_block_size)
            {
                if (len==-1) perror(ddfs->blockfile);
                fprintf(stderr, "Cannot write header in blockfile: %s\n", ddfs->blockfile);
                return 5;
            }
            memset(ddfs->bfile_read_buffer, '\0', ddfs->c_block_size);
            len=pwrite(ddfs->bfile, ddfs->bfile_read_buffer, ddfs->c_block_size, ddfs->c_block_size);
            if (len!=ddfs->c_block_size)
            {
                if (len==-1) perror(ddfs->blockfile);
                fprintf(stderr, "Cannot write second reserved block in blockfile: %s\n", ddfs->blockfile);
                return 5;
            }
            printf("BlockFile header initialized: %s\n", ddfs->blockfile);
        }
    }

    //
    // index file
    //
    if (ddfs->c_index_filename[0]=='/')
    {
       sprintf(filename, "%s", ddfs->c_index_filename);
    }
    else
    {
       sprintf(filename, "%s/%s", ddfs->pdir, ddfs->c_index_filename);
    }
    ddfs->indexfile=strdup(filename);

    if (output) fprintf(output, "indexfile: %s\n", ddfs->indexfile);

    ddfs->ifile=open(ddfs->indexfile, O_RDWR);
    if (ddfs->ifile==-1)
    {
        perror(ddfs->indexfile);
        if (!(rebuild && errno==ENOENT))
        {
            perror(ddfs->indexfile);
            fprintf(stderr, "try to rebuild the index\n");
            return 6;
        }
        else
        {
            fprintf(stderr, "repair indexfile: %s\n", ddfs->indexfile);

            ddfs->ifile=open(ddfs->indexfile, O_RDWR|O_CREAT|O_TRUNC, 0600);
            if (ddfs->ifile==-1)
            {
                perror(ddfs->indexfile);
                return 6;
            }
            // first block is header
            memset(ddfs->aux_buffer, '\0', ddfs->c_index_block_size);
            memcpy(ddfs->aux_buffer, DDFS_MAGIC_INDEX, DDFS_MAGIC_INDEX_LEN);
            len=write(ddfs->ifile, ddfs->aux_buffer, ddfs->c_index_block_size);
            if (len!=ddfs->c_index_block_size)
            {
                if (len==-1) perror(ddfs->indexfile);
                fprintf(stderr, "Cannot write header in IndexFile: %s\n", ddfs->indexfile);
                return 6;
            }

            // used block list (bit field)
            // first and second block are reserved => '\x80'
            if (lseek64(ddfs->ifile, ddfs->c_freeblock_offset, SEEK_SET)==-1)
            {
                perror(ddfs->indexfile);
                return 6;
            }
            memset(ddfs->aux_buffer, '\0', ddfs->c_index_block_size);
            ddfs->aux_buffer[0]=0xC0;
            len=write(ddfs->ifile, ddfs->aux_buffer, ddfs->c_index_block_size);
            ddfs->aux_buffer[0]=0;
            long int i;
            for (i=1; i<(ddfs->c_node_offset-ddfs->c_freeblock_offset)/ddfs->c_index_block_size; i++)
            {
                len=write(ddfs->ifile, ddfs->aux_buffer, ddfs->c_index_block_size);
                if (len!=ddfs->c_index_block_size)
                {
                    if (len==-1) perror(ddfs->indexfile);
                    fprintf(stderr, "Error writing to IndexFile: %s\n", ddfs->indexfile);
                    return 6;
                }
            }
            // nodes
            if (lseek64(ddfs->ifile, ddfs->c_node_offset, SEEK_SET)==-1)
            {
                perror(ddfs->indexfile);
                return 6;
            }
            for (i=0; i<ddfs->c_node_block_count; i++)
            {
                len=write(ddfs->ifile, ddfs->aux_buffer, ddfs->c_index_block_size);
                if (len!=ddfs->c_index_block_size)
                {
                    if (len==-1) perror(ddfs->indexfile);
                    fprintf(stderr, "Error writing to IndexFile: %s\n", ddfs->indexfile);
                    return 6;
                }
            }

            assert(lseek64(ddfs->ifile, 0, SEEK_CUR)==ddfs->c_index_size);
        }
    }


    if (0)
    {   // try to optimize index file, useless and maybe counterproductive
        // if index file can fit into memory cache
        res=posix_fadvise(ddfs->ifile, 0, ddfs->c_partition_size, POSIX_FADV_WILLNEED);
        if (res==-1)
        {
            perror(ddfs->blockfile);
            return 7;
        }
    }

    // check if indexfile has the appropriate size
    long long int sz=lseek64(ddfs->ifile, 0, SEEK_END);
    if (sz<ddfs->c_index_size)
    {
        fprintf(stderr, "the indexfile size is too small: %s (%lld<%lld)\n", ddfs->indexfile, sz, ddfs->c_index_size);
        if (rebuild)
        {
            fprintf(stderr, "repair the indexfile size: %s\n", ddfs->indexfile);
            res=ftruncate(ddfs->ifile, ddfs->c_index_size);
            if (res==-1)
            {
                perror(ddfs->indexfile);
                return 6;
            }
        }
        else return 6;
    }
    lseek(ddfs->ifile, 0, SEEK_SET);

    // check if indexfile start with appropriate MAGIC
    len=read(ddfs->ifile, ddfs->bfile_read_buffer, 4096);
    if (len!=4096)
    {
       if (len==-1) perror(ddfs->indexfile);
       else if (len!=4096) fprintf(stderr, "read only len=%d/4096 from %s\n", len, ddfs->indexfile);
       close(ddfs->ifile);
       return 8;
    }

    if (0!=strncmp(ddfs->bfile_read_buffer, DDFS_MAGIC_INDEX, DDFS_MAGIC_INDEX_LEN))
    {
       fprintf(stderr, "magic not found: %s\n", ddfs->indexfile);
       if (rebuild)
       {
           // first block is header
           memset(ddfs->aux_buffer, '\0', ddfs->c_index_block_size);
           memcpy(ddfs->aux_buffer, DDFS_MAGIC_INDEX, DDFS_MAGIC_INDEX_LEN);
           len=pwrite(ddfs->ifile, ddfs->aux_buffer, ddfs->c_index_block_size, 0);
           if (len!=ddfs->c_index_block_size)
           {
               if (len==-1) perror(ddfs->indexfile);
               fprintf(stderr, "Cannot write header in IndexFile: %s\n", ddfs->indexfile);
               return 8;
           }
       }
       else
       {
           close(ddfs->ifile);
           return 8;
       }
    }

    //
    // map all stuff in memory
    //
    ddfs->usedblocks_map=mmap(NULL, ddfs->c_node_offset-ddfs->c_freeblock_offset, PROT_READ|PROT_WRITE, MAP_SHARED, ddfs->ifile, ddfs->c_freeblock_offset);
    bit_array_init2(&ddfs->ba_usedblocks, ddfs->c_block_count, ddfs->usedblocks_map);
    long long int _u;
    bit_array_count(&ddfs->ba_usedblocks, &ddfs->usedblock, &_u);

    ddfs->nodes=mmap(NULL, ddfs->c_node_block_count*ddfs->c_index_block_size, PROT_READ|PROT_WRITE, MAP_SHARED, ddfs->ifile, ddfs->c_node_offset);

    if (ddfs->lock_index)
    {  // lock index component into memory
        int res1=mlock(ddfs->usedblocks_map, ddfs->c_node_offset-ddfs->c_freeblock_offset);
        if (res1==-1) {
            perror("cannot lock used block into memory\n");
        }
        int res2=mlock(ddfs->nodes, ddfs->c_node_block_count*ddfs->c_index_block_size);
        if (res2==-1) {
            perror("cannot lock index into memory\n");
        }
        if (res1==0 && res2==0 && output)
        {
            fprintf(output, "index locked into memory: %.1fMo\n", (ddfs->c_node_block_count*ddfs->c_index_block_size+ddfs->c_node_offset-ddfs->c_freeblock_offset)/1024.0/1024.0);
        }
        else ddfs->lock_index=0;

    }

    if(!ddfs->lock_index) {
	madvise(ddfs->nodes, ddfs->c_node_block_count*ddfs->c_index_block_size, MADV_RANDOM);
    }

    long long int i;
    unsigned char c=0;
    for (i=0; i<ddfs->c_node_block_count; i++)
    {
        c=ddfs->nodes[i*ddfs->c_index_block_size];
        ddfs->nodes[i*ddfs->c_index_block_size]=c;

    }
    for (i=0; i<(ddfs->c_node_offset-ddfs->c_freeblock_offset)/ddfs->c_index_block_size; i++) c=c+((unsigned char *)ddfs->usedblocks_map)[i*ddfs->c_index_block_size];

    // calculate hash for block full of 0
    ddfs->zero_block_hash=malloc(ddfs->c_hash_size);
    if (!ddfs->zero_block_hash)
    {
        fprintf(stderr,"ERROR: cannot allocate zero_block_hash\n");
        return 4;
    }
    memset(ddfs->bfile_read_buffer, '\0', ddfs->c_block_size);
    ddfs_hash(ddfs->bfile_read_buffer, ddfs->zero_block_hash);
    ddfs->zero_block_cidx=ddfs_hash2idx(ddfs->zero_block_hash);

    // init null_block_hash
    ddfs->null_block_hash=malloc(ddfs->c_hash_size);
    if (!ddfs->null_block_hash)
    {
        fprintf(stderr,"ERROR: cannot allocate null_block_hash\n");
        return 4;
    }
    memset(ddfs->null_block_hash, '\0', ddfs->c_hash_size);
    ddfs->null_block_cidx=ddfs_hash2idx(ddfs->null_block_hash);

    //
    ddfs->error_nodes_exausted=0;
    ddfs->error_block_write=0;
    ddfs->error_block_read=0;

    return 0;
}

/**
 * close the ddfs context
 *
 * @return 0 if ok
 */
int ddfs_close()
{
    int res1=munmap(ddfs->nodes, ddfs->c_node_block_count*ddfs->c_index_block_size);
    int res2=munmap(ddfs->usedblocks_map, ddfs->c_node_offset-ddfs->c_freeblock_offset);

    int res3=close(ddfs->bfile);
    int res4=0;
    if (!ddfs->direct_io) res4=close(ddfs->bfile_ro);
    int res5=close(ddfs->ifile);

    return res1 || res2 || res3 || res4 || res5;
}

int ddfs_cpu_count()
{
    char line[1024];
	int count=0;
	char *cpuinfo="/proc/cpuinfo";
	FILE *cpufile=fopen(cpuinfo, "r");
	if (!cpufile)
	{
		perror(cpuinfo);
		return -1;
	}

	while (fgets(line, sizeof(line), cpufile))
	{
		char *key=trim(strtok(line, ":"));
		//char *value=trim(strtok(NULL, "\n"));

		if (strcmp("processor", key)==0)
		{
			count++;
		}
	}
	fclose(cpufile);
	return count;
}
