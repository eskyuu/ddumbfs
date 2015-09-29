/**
 * @ddfslib.h
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
 * ddfslib handle basic commponents of the ddumbfs filesystem
 *
 */

#ifndef DDFSLIB_H_
#define DDFSLIB_H_

#include <stdio.h>
#include <endian.h>
#include <stdint.h>

#include "bits.h"
#include "xlog.h"

#ifdef WORDS_BIGENDIAN
        #define ddfs_hton64(x) (x)
        #define ddfs_ntoh64(x) (x)
#else
        #ifdef HAVE_SYS_ENDIAN_H
                #include <sys/endian.h>
                // betoh64 (OpenBSD) is sometimes called be64toh (FreeBSD, NetBSD).
                // Rather than check for it just reuse htobe64 since they are symmetrical
                #define ddfs_hton64(x) htobe64(x)
                #define ddfs_ntoh64(x) htobe64(x)
        #elif HAVE_ASM_BYTEORDER_H
                #include <asm/byteorder.h>
                #define ddfs_hton64(x) __cpu_to_be64(x)
                #define ddfs_ntoh64(x) __be64_to_cpu(x)
        #else
                #define ddfs_hton64(x) ddumbfs_swap64(x)
                #define ddfs_ntoh64(x) ddumbfs_swap64(x)
        #endif
#endif

#ifdef HAVE_SYSLOG_H
    #include<syslog.h>
#endif

#define FACILITY LOG_LOCAL0
extern char *ddfs_logname;
extern int ddfs_debug;
extern struct xlogger ddfs_logger[];

#ifdef DDFS_DEBUG
    #define DDFS_LOG_DEBUG(f...)    { xlog(ddfs_logger, LOG_DEBUG, f); }
#else
    #define DDFS_LOG_DEBUG(f...)    { }
#endif

#define DDFS_LOG(prio, f...)    { xlog(ddfs_logger, prio, f); }

#ifdef HAVE_SYSLOG_H
    #define L_SYS(prio, f...) { openlog(ddfs_logname,LOG_PID,FACILITY);syslog(prio ,f);closelog();}
#else
    #define L_SYS(prio, f...) {  }
#endif


#define DDFS_MAGIC_BLOCK     "DDUMBFSB"
#define DDFS_MAGIC_INDEX     "DDUMBFSI"
#define DDFS_MAGIC_FILE      "DDUMBFSF"
#define DDFS_MAGIC_BLOCK_LEN  8
#define DDFS_MAGIC_INDEX_LEN  8
#define DDFS_MAGIC_FILE_LEN   8

#define DDFS_FORCE          1
#define DDFS_NOFORCE        0
#define DDFS_REBUILD        1
#define DDFS_NOREBUILD      0
#define DDFS_NODIRECTIO     0
#define DDFS_DIRECTIO       1
#define DDFS_DIRECTIOAUTO   2
#define DDFS_NODIRECTIO     0
#define DDFS_LOCKINDEX      1
#define DDFS_NOLOCKINDEX    0
#define DDFS_COPYMAGIC      0
#define DDFS_NOCOPYMAGIC    0
#define DDFS_VERBOSE        1
#define DDFS_NOVERBOSE      0
#define DDFS_PROGRESS       1
#define DDFS_NOPROGRESS     0
#define DDFS_SYNC           1
#define DDFS_NOSYNC         0
#define DDFS_RELAXED        1
#define DDFS_NORELAXED      0
#define DDFS_      1
#define DDFS_NO    0

#define DDFS_ALREADY_LOCKED_RW     2
#define DDFS_ALREADY_LOCKED        1
#define DDFS_NOT_LOCKED            0

#define DDFS_LAST_RESERVED_BLOCK 1

/*
 * These are MAX value !!!!
 */
#define HASH_SIZE              64
#define ADDR_SIZE               8
#define NODE_SIZE           (HASH_SIZE+ADDR_SIZE)
#define INDEX_BLOCK_SIZE     4096
#define FILE_HEADER_SIZE       16

#define BLOCK_ALIGMENT   1024

#define DDFS_BACKUP_USEDBLOCK   "ddfsusedblocks"
#define BLOCK_FILENAME          "ddfsblocks"
#define INDEX_FILENAME          "ddfsidx"
#define ROOT_DIR                "ddfsroot"
#define CFG_FILENAME            "ddfs.cfg"
#define SPECIAL_DIR             "/.ddumbfs/"
#define RECLAIM_FILE            "/.ddumbfs/reclaim"
#define STATS_FILE              "/.ddumbfs/stats"
#define STATS0_FILE             "/.ddumbfs/stats0"
#define TEST_FILE               "/.ddumbfs/test"
#define DDFS_LOG_FILE           "/.ddumbfs/ddumbfs.log"
#define DDFS_CORRUPTED_LIST     "/.ddumbfs/corrupted.txt"
#define RECLAIM_BUF_SIZE        4096
#define DDFS_CORRUPTED_BACKUP_COUNT 5
#define NOW_PER_SEC             1000
#define FSCK_INDEX_PRELOAD	10000
#define NODE_FSCK_PREFETCH	1000

typedef long long int blockaddr;
typedef long long int nodeidx;

struct cfgfile
{
    char *name;
    char type;
    size_t offset;
    char misc;
};

extern struct cfgfile cfg[];
extern char *special_filenames[];

struct ddfs_ctx
{
    char *pdir;
    char *rdir;
    int rdir_len;
    int special_dir_len;
    int direct_io;
    int align;
    int lock_index;

    char *blockfile;
    char *indexfile;

    int bfile;
    int bfile_ro; // if direct_io, this is a read-only fd  else equal bfile
    int ifile;
    int bfile_isblk; // if bfile is a block device
    long long int bfile_size;  // size of the blockfile in byte; this variable is initialized but not kept up2date
    long long int bfile_last;  // last block in blockfile; this variable is initialized but not kept up2date

    unsigned char *nodes;
    void *usedblocks_map;
    struct bit_array ba_usedblocks; // block in use, live
    long long int usedblock;        // "maintained" number of block in use

    char *bfile_read_buffer; // aligned buffer, usage protected by bfile_mutex
    char *aux_buffer;        // aligned block buffer

    unsigned char *zero_block_hash;  // hash of block full of zero
    unsigned char *null_block_hash;  // hash full of zero
    nodeidx        zero_block_cidx;  // normal position of the hash of block full of zero
    nodeidx        null_block_cidx;  // normal position of the hash full of zero

    double coef_hash2idx;

    /** the c_* variables */
    int c_file_header_size;
    int c_hash_size;
    int c_block_size;
    int c_index_block_size;
    double c_node_overflow;

    long long int c_partition_size;
    blockaddr c_block_count;
    int c_addr_size;
    int c_node_size;
    int c_hash_id;
    nodeidx c_node_count;
    long long int c_node_block_count;

    long long int c_freeblock_offset;
    long long int c_freeblock_size;
    long long int c_node_offset;
    long long int c_index_size;
    long long int c_index_block_count;

    char *c_hash;
    char *c_block_filename;
    char *c_index_filename;
    char *c_root_directory;

    int c_reuse_asap;
    int c_auto_buffer_flush;
    int c_auto_sync;

    long long int block_boundary_mask;
    long long int block_gap_mask;
    int block_size_shift;

    int background_index_changed_flag;

    int auto_fsck_clean;
    int auto_fsck;
    int rebuild_fsck;

    int error_nodes_exausted;
    int error_block_write;
    int error_block_read;

};

struct file_header
{
    uint64_t size;
};

extern struct ddfs_ctx *ddfs;

extern inline uint64_t ddumbfs_swap64(uint64_t x);

long long int unitvalue(char *u);
char *trim(char *s);
long long int now();
long long int micronow();
void dsleep(double delay);

int pathexists(const char *path);
int isdir(const char *path);
int isregfile(const char *path);
int is_mounted(const char *device_name);

// node_add() unique parameter
enum { na_duplicate_hash_allowed, na_unique_hash }; // node must stay unique even when na_duplicate_hash_allowed

void node_delete(nodeidx node_idx);
int node_add(const unsigned char* new_node, int unique);
int node_fix(nodeidx node_idx);

int ddfs_lock(const char *filename);
int ddfs_unlock(const char *filename);
int ddfs_testlock(const char *filename);

void ddfs_hash(const char *block, unsigned char *hash);
void ddfs_convert_addr(blockaddr addr, unsigned char *node_addr);
void ddfs_set_node(nodeidx node_idx, blockaddr addr, const unsigned char *hash);
blockaddr ddfs_get_node_addr(const unsigned char *node);
nodeidx ddfs_hash2idx(const unsigned char *hash);
nodeidx ddfs_search_hash(const unsigned char *hash, blockaddr *addr);
nodeidx ddfs_search_free_node(nodeidx idx, nodeidx stop_before);
int ddfs_locate_hash(unsigned char *hash, blockaddr *addr, nodeidx *node_idx);

int file_header_set(int fd, struct file_header *fh);
int file_header_set_conv(int fd, uint64_t *size);
int file_header_get(int fd, struct file_header *fh);
int file_header_get_conv(int fd, uint64_t size);
void file_header_copy(struct file_header *dst, struct file_header *src);

blockaddr ddfs_alloc_block();
int ddfs_read_full_block(blockaddr addr, char *buf);
void ddfs_forced_read_full_block(blockaddr addr, char *buf, int block_size);
int ddfs_read_block(blockaddr addr, char *buf, int size, int gap);
blockaddr ddfs_store_block(const char *block, blockaddr force_addr);
blockaddr ddfs_write_block(const char *block, unsigned char *bhash);

int ddfs_save_usedblocks();
int ddfs_load_usedblocks(struct bit_array *ba);
int ddfs_find_parent(char *path, char *parent);
int ddfs_loadcfg(char *ddfs_parent, FILE *output);
int ddfs_init(int force, int rebuild, int direct_io, int lock_index, FILE *output);
int ddfs_close();
int ddfs_need_fsck();

int ddfs_cpu_count();

#endif
