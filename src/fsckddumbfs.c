/**
 * @fsckddumbfs.c
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
 * fsckddumbfs check and repair an offline ddumbfs filesystem
 *
 */

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/mman.h>

#include "ddfslib.h"
#include "ddfschkrep.h"

static struct option long_options[] =
{
       {"help",           no_argument,       0, 'h'},
       {"verbose",        no_argument,       0, 'v'},
       {"lock_index",     no_argument,       0, 'l'},
       {"force",          no_argument,       0, 'f'},
       {"progress",       no_argument,       0, 'p'},
       {"check",          no_argument,       0, 'c'},
       {"check-block",    no_argument,       0, 'C'},
       {"normal",         no_argument,       0, 'n'},
       {"normal-relaxed", no_argument,       0, 'N'},
       {"rebuild",        no_argument,       0, 'r'},
       {"rebuild-block",  no_argument,       0, 'R'},
       {"pack",           no_argument,       0, 'k'},
       {"debug",          no_argument,       0, 'd'},
       {0, 0, 0, 0}
};

int verbose_flag=0;
int progress_flag=0;
int check_flag=0;
int pack_flag=0;

void usage()
{
    printf("Usage: fsckddumbfs [options] parent-directory\n"
            "\n    check and/or repair an offline ddumbfs filesystem\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         be more verbose\n"
            "  -l, --lock_index      lock index into memory (increase speed)\n"
            "  -f, --force           bypass some guardrail\n"
            "  -p, --progress        display progress when possible\n"
            "  -c, --check           read-only check: index and files (very fast)\n"
            "  -C, --check-block     read-only check: index, file and block hashes (slow)\n"
            "  -n, --repair          automatically repair, verify all entries from file(fast)\n"
            "  -N, --repair-relaxed  automatically repair, don't verify new non colliding\n"
    		"           entries from file, useful to rebuild index from files without re-hash\n"
            "  -r, --rebuild         re-hash known blocks and build new index (slow)\n"
            "  -R, --rebuild-block   re-hash ALL blocks and build new index (slowest)\n"
            "  -k, --pack            pack the block file. READ the manual before to use this!\n"

    );
}

enum bb_state { bb_empty, bb_reading, bb_read, bb_hashing, bb_hashed };

struct blocks_buffer
{
	int state;
	char *buffer;
	blockaddr addr;
	unsigned char hash[HASH_SIZE];
};

char *pack_buf;

struct blocks_buffer *blocks_buffers=NULL;
pthread_t ddfs_background_reader_thread;
pthread_t *ddfs_background_hasher_threads;
pthread_mutex_t ddfs_background_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t ddfs_background_ba_ready=PTHREAD_COND_INITIALIZER;
pthread_cond_t ddfs_background_empty_ready=PTHREAD_COND_INITIALIZER;
pthread_cond_t ddfs_background_read_ready=PTHREAD_COND_INITIALIZER;
pthread_cond_t ddfs_background_hash_ready=PTHREAD_COND_INITIALIZER;
int ddfs_background_buf_count=-1;
int ddfs_background_hasher_count=-1;
int ddfs_background_empty;
int ddfs_background_read;
int ddfs_background_hashed;
int ddfs_background_next_empty;
int ddfs_background_next_read;
int ddfs_background_next_hashed;
int ddfs_background_read_done=0;
long long int ddfs_background_read_error;
long long int ddfs_background_block_count;
struct bit_array *ddfs_background_ba=NULL;
long long int ddfs_background_ba_start;

/**
 * Read the block file in background and load buffers
 */
void *ddfs_background_reader(void *ptr)
{

	while (1)
	{
		pthread_mutex_lock(&ddfs_background_mutex);
		while (ddfs_background_ba==NULL) pthread_cond_wait(&ddfs_background_ba_ready, &ddfs_background_mutex);
		struct bit_array *ba=ddfs_background_ba;
		ddfs_background_ba=NULL;
		pthread_mutex_unlock(&ddfs_background_mutex);

		blockaddr addr=bit_array_search_first_set(ba, ddfs_background_ba_start);
		while (addr>0)
		{
	//		DDFS_LOG(LOG_NOTICE, "reader addr=%lld\n", addr);
			// search an empty block
			pthread_mutex_lock(&ddfs_background_mutex);
			while (ddfs_background_empty==0) pthread_cond_wait(&ddfs_background_empty_ready, &ddfs_background_mutex);
			while (blocks_buffers[ddfs_background_next_empty].state!=bb_empty) ddfs_background_next_empty=(ddfs_background_next_empty+1)%ddfs_background_buf_count;

			struct blocks_buffer *bb=blocks_buffers+ddfs_background_next_empty;
			bb->state=bb_reading;
			bb->addr=addr;
			ddfs_background_empty--;
			pthread_mutex_unlock(&ddfs_background_mutex);
			ddfs_background_next_empty=(ddfs_background_next_empty+1)%ddfs_background_buf_count;
			int len=pread(ddfs->bfile_ro, bb->buffer, ddfs->c_block_size, addr<<ddfs->block_size_shift);
			if (len!=ddfs->c_block_size)
			{
				pthread_mutex_lock(&ddfs_background_mutex);
				bb->state=bb_empty;
				ddfs_background_empty++;
				pthread_mutex_unlock(&ddfs_background_mutex);
				if (len==-1)
				{
					ddfs_background_read_error++;
					if (!check_flag)
					{
						DDFS_LOG(LOG_ERR, "block %lld, cannot read: %s\n", addr, strerror(errno));
					}
					else if (verbose_flag) printf("block %lld, cannot read: %s\n", addr, strerror(errno));
				}
				else if (len!=0)
				{
					ddfs_background_read_error++;
					if(!check_flag)
					{
						DDFS_LOG(LOG_ERR, "block %lld, cannot read: %d/%d\n", addr, len, ddfs->c_block_size);
					}
					else if (verbose_flag) printf("block %lld, cannot read: %d/%d\n", addr, len, ddfs->c_block_size);
				}
				else // len==0
				{ // EOF
					//DDFS_LOG(LOG_NOTICE, "reader EOF addr=%lld\n", addr);
					break;
				}
			}
			else
			{
				pthread_mutex_lock(&ddfs_background_mutex);
				bb->state=bb_read;
				ddfs_background_read++;
				pthread_cond_signal(&ddfs_background_read_ready);
				pthread_mutex_unlock(&ddfs_background_mutex);
			}
			addr=bit_array_search_first_set(ba, addr+1);
			// DDFS_LOG(LOG_NOTICE, "reader ba=0x%x %x\n", ddfs->ba_usedblocks.array[0], ddfs->ba_usedblocks.array[1]);
		}
		// DDFS_LOG(LOG_NOTICE, "reader END addr=%lld\n", addr);

		// Wake up main thread and warn about the end
		pthread_mutex_lock(&ddfs_background_mutex);
		ddfs_background_read_done=1;
		pthread_cond_signal(&ddfs_background_hash_ready);
		pthread_mutex_unlock(&ddfs_background_mutex);
	}
    pthread_exit(NULL);
}

/**
 * Calculate buffers hashes
 */
void *ddfs_background_hasher(void *ptr)
{
//    DDFS_LOG(LOG_NOTICE, "start ddfs_background_hasher\n");
    while (1)
    {
        // search an empty block
        pthread_mutex_lock(&ddfs_background_mutex);
        while (ddfs_background_read==0) pthread_cond_wait(&ddfs_background_read_ready, &ddfs_background_mutex);
        while (blocks_buffers[ddfs_background_next_read].state!=bb_read) ddfs_background_next_read=(ddfs_background_next_read+1)%ddfs_background_buf_count;

        struct blocks_buffer *bb=blocks_buffers+ddfs_background_next_read;
        bb->state=bb_hashing;
        ddfs_background_read--;
        ddfs_background_next_read=(ddfs_background_next_read+1)%ddfs_background_buf_count; // must be in the lock, because multiple hash
        pthread_mutex_unlock(&ddfs_background_mutex);
        ddfs_hash(bb->buffer, bb->hash);
        pthread_mutex_lock(&ddfs_background_mutex);
        bb->state=bb_hashed;
        ddfs_background_hashed++;
        pthread_cond_signal(&ddfs_background_hash_ready);
        pthread_mutex_unlock(&ddfs_background_mutex);
    }
    pthread_exit(NULL);
}

int ddfs_background_init(int buf_count, int hasher_count)
{
	//  DDFS_LOG(LOG_NOTICE, "start ddfs_background_init\n");
	int i;
	if (ddfs_background_buf_count!=-1)
	{
      DDFS_LOG(LOG_ERR, "ddfs_background_init: already initialized\n");
      return -1;
	}

	blocks_buffers=malloc(buf_count*sizeof(struct blocks_buffer));
	if (blocks_buffers==NULL)
	{
		DDFS_LOG(LOG_ERR, "cannot allocate %d background buffers\n", buf_count);
		return -1;
	}

	for(i=0; i<buf_count; i++)
	{
		blocks_buffers[i].buffer=malloc(ddfs->c_block_size);
		if (blocks_buffers[i].buffer==NULL)
		{
			DDFS_LOG(LOG_ERR, "cannot allocate %d/%d background buffers\n", i+1, buf_count);
			return -1;
		}
	}

	ddfs_background_hasher_threads=malloc(hasher_count*sizeof(pthread_t));
	if (ddfs_background_hasher_threads==NULL)
	{
		DDFS_LOG(LOG_ERR, "cannot allocate the thread list\n");
		return -1;
	}

	for(i=0; i<hasher_count; i++)
	{
	    if (pthread_create(ddfs_background_hasher_threads+i, NULL, ddfs_background_hasher, &ddfs->ba_usedblocks))
	    {
			DDFS_LOG(LOG_ERR, "cannot allocate hasher threads\n");
	    	return -1;
	    }
	}

	ddfs_background_buf_count=buf_count;
	ddfs_background_hasher_count=hasher_count;
	ddfs_background_ba=NULL;

	if (pthread_create(&ddfs_background_reader_thread, NULL, ddfs_background_reader, NULL))
    {
		DDFS_LOG(LOG_ERR, "cannot allocate reader threads\n");
    	return -1;
    }

	return 0;
}

void ddfs_background_start(struct bit_array *ba, long long int ba_start)
{
	int i;
	ddfs_background_read_error=0;
	ddfs_background_read_done=0;
	ddfs_background_empty=ddfs_background_buf_count;
	ddfs_background_read=ddfs_background_hashed=0;
	ddfs_background_next_empty=ddfs_background_next_read=ddfs_background_next_hashed=0;
	ddfs_background_block_count=0;
	for(i=0; i<ddfs_background_buf_count; i++) blocks_buffers[i].state=bb_empty;
	pthread_mutex_lock(&ddfs_background_mutex);
	ddfs_background_ba=ba;
	ddfs_background_ba_start=ba_start;
	pthread_cond_signal(&ddfs_background_ba_ready);
	pthread_mutex_unlock(&ddfs_background_mutex);
}

/*
 * Hash all block in 'ba' and call 'bb_process'() with the hashed result
 * reading and hashing are done by threads in background
 */
int ddfs_hash_blocks(struct bit_array* ba, long long int ba_start, int (*bb_process)(struct blocks_buffer*, void*), void *ctx)
{
    long long int block_count, u;

    bit_array_count(ba, &block_count, &u);
    if (!ddfs->bfile_isblk)
    {
    	off_t file_size=lseek(ddfs->bfile, 0L, SEEK_END);
    	blockaddr count=file_size/ddfs->c_block_size;
        if (count<block_count) block_count=count;
    }

    ddfs_background_start(ba, ba_start);

	long long int start=now();
    long long int last=start;
    long long int i=0;
    while (1)
    {
        if (progress_flag && now()-last>NOW_PER_SEC)
        {
			last=now();
			printf("block %5.1f%% in %llds\r", i*100.0/block_count, (last-start)/NOW_PER_SEC);
			fflush(stdout);
        }

        // search an empty block
        pthread_mutex_lock(&ddfs_background_mutex);
        while (ddfs_background_hashed==0)
        {
        	if (ddfs_background_read_done && ddfs_background_empty==ddfs_background_buf_count)
        	{
        		pthread_mutex_unlock(&ddfs_background_mutex);
        		return 0;
        	}
        	pthread_cond_wait(&ddfs_background_hash_ready, &ddfs_background_mutex);
            //DDFS_LOG(LOG_NOTICE, "ddfs_background_read_done=%d  ddfs_background_hashed=%d ddfs_background_empty=%d!\n",ddfs_background_read_done, ddfs_background_hashed, ddfs_background_empty);
        }

        while (blocks_buffers[ddfs_background_next_hashed].state!=bb_hashed) ddfs_background_next_hashed=(ddfs_background_next_hashed+1)%ddfs_background_buf_count;

        struct blocks_buffer *bb=blocks_buffers+ddfs_background_next_hashed;
		// bb->state=bb_indexing;
        ddfs_background_hashed--;
        pthread_mutex_unlock(&ddfs_background_mutex);
        ddfs_background_next_hashed=(ddfs_background_next_hashed+1)%ddfs_background_buf_count;

        ddfs_background_block_count++;
        if (bb_process(bb, ctx))
        {
			// TODO: I should cleanly terminate all threads, hopefully functions are called only once
        	return 1;
        }

        pthread_mutex_lock(&ddfs_background_mutex);
        bb->state=bb_empty;
        ddfs_background_empty++;
        pthread_cond_signal(&ddfs_background_empty_ready);

        pthread_mutex_unlock(&ddfs_background_mutex);

        i++;
    }
	return 0;
}

/**
 * check the index for errors
 *
 * @param ba_found_in_index return each block used in the index
 * @param ba_found_in_index_twice return each block used more than once in the index
 * @param na_node_to_check
 * @return the number of errors found in index
 */
long long int ddfs_chk_index(struct bit_array *ba_found_in_index, struct bit_array *ba_found_in_index_twice, struct bit_array *na_node_to_check)
{
    nodeidx node_idx;
    long long int errors=0;

    bit_array_reset(ba_found_in_index, 0);
    bit_array_reset(ba_found_in_index_twice, 0);

    bit_array_set(ba_found_in_index, 0);
    bit_array_set(ba_found_in_index, 1);

    long long int start=now();
    long long int last=start;

    if(!ddfs->lock_index)
	ddfs_chk_preload_node(0, FSCK_INDEX_PRELOAD * 1.5);

    for (node_idx=0; node_idx<ddfs->c_node_count; node_idx++)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        unsigned char *hash=node+ddfs->c_addr_size;
        blockaddr addr=ddfs_get_node_addr(node);


	if(!ddfs->lock_index && !(node_idx%FSCK_INDEX_PRELOAD))
	    ddfs_chk_preload_node(node_idx+(FSCK_INDEX_PRELOAD/2), FSCK_INDEX_PRELOAD);

        if (progress_flag && now()-last>NOW_PER_SEC)
        {	// display progress
			last=now();
			printf("check index node %.1f%% in %llds lock=%d\r", node_idx*100.0/ddfs->c_node_count, (last-start)/NOW_PER_SEC, ddfs->lock_index);
			fflush(stdout);
        }

        if (addr==0)
        {
            if (0!=memcmp(ddfs->null_block_hash, hash, ddfs->c_hash_size) && 0!=memcmp(ddfs->zero_block_hash, hash, ddfs->c_hash_size))
            {
                errors++;
                if (verbose_flag) printf("node %lld (addr=%lld) has a wrong hash\n", node_idx, addr);
            }
        }
        else if (addr==1)
        {
            errors++;
            if (verbose_flag) printf("node %lld (addr=%lld) unexpected address in the index\n", node_idx, addr);
        }
        else
        {
            if (bit_array_get(ba_found_in_index, addr)) bit_array_set(ba_found_in_index_twice, addr);
            else bit_array_set(ba_found_in_index, addr);

            bit_array_set(na_node_to_check, node_idx);
            nodeidx cidx=ddfs_hash2idx(hash);
            if (node_idx<cidx)
            {
                errors++;
                if (verbose_flag) printf("node %lld (addr=%lld) before its expected position %lld\n", node_idx, addr, cidx);
            }
            else if (node_idx==cidx)
            {
            }
            else
            {
                if (node_idx!=0)
                {
                    if (ddfs_get_node_addr(node-ddfs->c_node_size)==0)
                    {
                        errors++;
                        if (verbose_flag) printf("node %lld (addr=%lld) is preceded by an empty node and is not at its optimum position\n", node_idx, addr);
                    }
                    else
                    {
                        int res=memcmp(hash-ddfs->c_node_size, hash, ddfs->c_hash_size);
                        if (res==0)
                        {
                            errors++;
                            if (verbose_flag) printf("node %lld (addr=%lld) hash equal previous hash (addr=%lld), this is unexpected\n", node_idx, addr, ddfs_get_node_addr(node-ddfs->c_node_size));
                        }
                        else if (res>0)
                        {
                            errors++;
                            if (verbose_flag) printf("node %lld (addr=%lld) hash order mismatch with previous node (addr=%lld)\n", node_idx, addr, ddfs_get_node_addr(node-ddfs->c_node_size));
                        }
                    }
                }
            }

            if (addr>=ddfs->c_node_count)
            {
                errors++;
                if (verbose_flag) printf("node %lld (addr=%lld) address out of range\n", node_idx, addr);
            }
            else
            {   // this is the good place to check block, but it would be checked in
                // non sequential order and would be very slow
                // errors+=ddfs_chk_block(node_idx, addr);
            }
        }
    }
    // if (chk_block) ddfs_chk_block(ddfs->c_node_count, 0); // flush
    bit_array_unset(ba_found_in_index_twice, 0);
    bit_array_unset(ba_found_in_index_twice, 1);

    return errors;
}

long long int ddfs_chk_unmatching_blocks(struct bit_array *na_list)
{
    long long int errors=0;
    long long int block_count, _u;
    long long int start=now();
    long long int last=start;

    bit_array_count(na_list, &block_count, &_u);

    long long int i=0;
    nodeidx node_idx=bit_array_search_first_set(na_list, 0);
    while (node_idx>0)
    {
        if (verbose_flag)
        {
            long long int end=now();
            if (end-last>NOW_PER_SEC)
            {
                last=end;
                printf("block %5.1f%% in %llds\r", i*100.0/block_count, (end-start)/NOW_PER_SEC);
                fflush(stdout);
            }
        }
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        unsigned char *hash=node+ddfs->c_addr_size;
        blockaddr addr=ddfs_get_node_addr(node);
        assert(addr!=0);
        assert(addr!=1);
        // I use the synchronous way, no need t be async, here the speed up should be nuts
        int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
        if (len==-1)
        {
            errors++;
            if (verbose_flag) printf("cannot read block %lld: %s\n", addr, strerror(errno));
        }
        else if (len!=ddfs->c_block_size)
        {
            errors++;
            if (verbose_flag) printf("cannot read block %lld: %d/%d\n", addr, len, ddfs->c_block_size);
        }
        else
        {
            unsigned char bhash[HASH_SIZE];
            ddfs_hash(ddfs->aux_buffer, bhash);
            if (0!=memcmp(bhash, hash, ddfs->c_hash_size))
            {
                errors++;
                if (verbose_flag) printf("block %lld don't match node %lld hash\n", addr, node_idx);
            }
        }
        i++;
        node_idx=bit_array_search_first_set(na_list, node_idx+1);
    }
    return errors;
}

int bb_check_hash(struct blocks_buffer *bb, void *ctx)
{
    struct bit_array *na_node_to_check=(struct bit_array *)ctx;

    if (0==memcmp(bb->hash, ddfs->zero_block_hash, ddfs->c_hash_size))
    {
        ddfs_background_read_error++;
        if (verbose_flag) printf("block %lld is the zero block, should not be referenced by a node\n", bb->addr);
    }
    else
    {
        blockaddr baddr;
        nodeidx node_idx=ddfs_search_hash(bb->hash, &baddr);
        if (node_idx<0 || bb->addr!=baddr)
        {
            if (verbose_flag) printf("block %lld hash not found or bad address\n", bb->addr);
        }
        else
        {
            bit_array_unset(na_node_to_check, node_idx);
        }
    }
    return 0;
}





struct bit_array *ddfs_chk_te_block_found;

int ddfs_chk_tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int res, len, pcount;
    unsigned char node[NODE_SIZE*NODE_FSCK_PREFETCH];
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr;
    uint64_t size;

    if (typeflag!=FTW_F || ! S_ISREG(sb->st_mode)) return 0;
    if (0==strncmp(fpath+ddfs->rdir_len, SPECIAL_DIR, ddfs->special_dir_len))
    {   // skip SPECIAL_DIR
        return 0;
    }

    te_file_count++;
    if (te_progress && now()-te_progress>NOW_PER_SEC)
    {
		te_progress=now();
		printf("Read file: %lld in %llds\r", te_file_count, (te_progress-te_start)/NOW_PER_SEC);
		fflush(stdout);
    }

    FILE *file=fopen(fpath, "rb");
    if (file==NULL)
    {
        te_errors++;
        if (verbose_flag) printf("cannot open: %s (%s)\n", fpath, strerror(errno));
        return 0;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file), &size);
    if (len!=ddfs->c_file_header_size)
    {
        te_errors++;
        if (verbose_flag)
        {
            if (len==-1) printf("cannot read: %s (%s)\n", fpath, strerror(errno));
            else printf("corrupted header: %s\n", fpath);
        }
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        te_errors++;
        if (verbose_flag) printf("cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 0;
    }

    int lost_block=0;
    long long int i=0;

    len=fread(node, 1, ddfs->c_node_size*NODE_FSCK_PREFETCH, file);
    pcount=len/ddfs->c_node_size;
    len=len%ddfs->c_node_size;

    if (!ddfs->lock_index)
    {
	int pnum=0;
	while(pnum<pcount) {
	    ddfs_chk_preload_node(ddfs_hash2idx(node+(pnum*ddfs->c_node_size)+ddfs->c_addr_size), 1);
	    pnum++;
	}
    }

    while (pcount)
    {
        addr=ddfs_get_node_addr(node);
        if (ddfs_chk_te_block_found) bit_array_set(ddfs_chk_te_block_found, addr);
        blockaddr baddr=addr;
        if (addr==0)
        {
            if (0!=memcmp(ddfs->null_block_hash, hash, ddfs->c_hash_size) && 0!=memcmp(ddfs->zero_block_hash, hash, ddfs->c_hash_size))
            {
                te_errors++;
                if (verbose_flag) printf("wrong hash for block zero      : %s\n", fpath);
            }
        }
        else if (addr==1)
        {
            te_lost_block++;
            if (!lost_block)
            {
                lost_block=1;
                if (verbose_flag) printf("file has lost blocks           : %s\n", fpath);
            }
            ddfs_search_hash(hash, &addr);
            if (baddr!=addr)
            {
                te_errors++;
                if (verbose_flag) printf("addr==1 but hash found in index: %s\n", fpath);
            }
        }
        else
        {
            ddfs_search_hash(hash, &addr);

	    //printf("Hash found in node index position %lld\n", node_idx);

            if (baddr!=addr)
            {
                te_errors++;
                if (verbose_flag)
                {
                    if (addr==1) printf("block not found in index       : %s (%lld -> %lld)\n", fpath, baddr, addr);
                    else printf("found another block for hash   : %s (%lld -> %lld)\n", fpath, baddr, addr);
                }
            }

        }
        i++;

	if(pcount>1)
	    memmove(node, node+ddfs->c_node_size, ddfs->c_node_size*(pcount-1));

	if(pcount == NODE_FSCK_PREFETCH)
	{
	    len=fread(node+(ddfs->c_node_size*(pcount-1)), 1, ddfs->c_node_size, file);
	    if(len != ddfs->c_node_size)
		pcount--;
	    else
	        ddfs_chk_preload_node(ddfs_hash2idx(node+((pcount-1)*ddfs->c_node_size)+ddfs->c_addr_size), 1);
	}
	else
	{
	    pcount--;
	}
    }

    if (len!=0)
    {
        te_errors++;
        if (verbose_flag)
        {
            if (len==-1) printf("cannot read file: %s (%s)\n", fpath, strerror(errno));
            else printf("incomplete node in file: %s\n", fpath);
        }
        fclose(file);
        return 0;
    }

    if (i!=(size+ddfs->c_block_size-1)/ddfs->c_block_size)
    {   // size mismatch
        te_errors++;
        if (verbose_flag) printf("size mismatch in file: %s\n", fpath);
        fclose(file);
        return 0;
    }

    fclose(file);
    return 0;
}

/**
 * verify hash of nodes from list
 *
 * @param list the list of blocks to check
 * @param verbose if true report all wrong nodes
 * @return number of errors
 */
long long int ddfs_chk_node_hash(struct bit_array *list)
{
    long long int total, _u;
    long long int count=0;
    long long int errors=0;
    long long int start=now();
    long long int last=start;

    bit_array_count(list, &total, &_u);

    nodeidx node_idx=0;
    while (total!=0 && node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        unsigned char *hash=node+ddfs->c_addr_size;
        if (addr==0)
        {
        }
        else if (addr==1)
        {
        }
        else if (bit_array_get(list, addr))
        {   // this is a node to check
            count++;
            if (progress_flag && now()-last>NOW_PER_SEC)
            {
            	last=now();
                printf("block %.1f%% in %llds\r",  count*100.0/total, (last-start)/NOW_PER_SEC);
                fflush(stdout);
            }
            int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
            if (len!=ddfs->c_block_size)
            {
                errors++;
                if (verbose_flag) printf("node %lld cannot read block %lld (%s).\n", node_idx, addr, len==-1?strerror(errno):"");
            }
            else
            {
                unsigned char bhash[HASH_SIZE];
                ddfs_hash(ddfs->aux_buffer, bhash);
                if (0!=memcmp(bhash, hash, ddfs->c_hash_size))
                {
                    errors++;
                    if (verbose_flag) printf("node %lld (addr=%lld) has a wrong hash.\n", node_idx, addr);
                }
            }
        }
        node_idx++;
    }
    return errors;
}

int ddfs_check(int chk_block)
{
    long long int errors_in_index=0;
    long long int errors_in_block=0;
    long long int wrong_hash;
    long long int block_in_index, block_twice_in_index, block_in_files, used_blocks, free_blocks, block_unmatching;
    long long int suspect_count;
    long long int u;
    struct bit_array ba_found_in_files;
    struct bit_array ba_found_in_index;
    struct bit_array ba_found_in_index_twice;
    struct bit_array ba_aux;
    struct bit_array ba_backup;
    struct bit_array ba_suspect;
    struct bit_array na_node_to_check;

    bit_array_init(&ba_found_in_files, ddfs->c_block_count, 0);
    bit_array_init(&ba_found_in_index, ddfs->c_block_count, 0);
    bit_array_init(&ba_found_in_index_twice, ddfs->c_block_count, 0);
    bit_array_init(&ba_aux, ddfs->c_block_count, 0);
    bit_array_init(&ba_backup, ddfs->c_block_count, 0);
    bit_array_init(&ba_suspect, ddfs->c_block_count, 0);

    bit_array_init(&na_node_to_check, ddfs->c_node_count, 0);

    printf("== Check index\n");
    long long int start=now();
    errors_in_index=ddfs_chk_index(&ba_found_in_index, &ba_found_in_index_twice, &na_node_to_check);
    long long int end=now();
    printf("== Checked %lld nodes in %.1fs\n", ddfs->c_node_count, (end-start)*1.0/NOW_PER_SEC);

    bit_array_count(&ba_found_in_index, &block_in_index, &u);
    bit_array_count(&ba_found_in_index_twice, &block_twice_in_index, &u);
    if (chk_block)
    {
        printf("== Check block hash\n");
        start=now();
        if (ddfs_hash_blocks(&ba_found_in_index, DDFS_LAST_RESERVED_BLOCK+1, bb_check_hash, &na_node_to_check)) return 1;
        errors_in_block=ddfs_background_read_error;
        end=now();
        if (errors_in_block<0) return 1;
        printf("== Checked %lld blocks in %.1fs\n", block_in_index, (end-start)*1.0/NOW_PER_SEC);

        bit_array_count(&na_node_to_check, &block_unmatching, &u);
        printf("== Check unmatching %lld block\n", block_unmatching);
        start=now();
        errors_in_block+=ddfs_chk_unmatching_blocks(&na_node_to_check);
        end=now();
        printf("== Checked %lld unmatching blocks in %.1fs\n", block_unmatching, (end-start)*1.0/NOW_PER_SEC);

    }

    int res;
    bit_array_reset(&ba_found_in_files, 0);
    bit_array_set(&ba_found_in_files, 0);
    bit_array_set(&ba_found_in_files, 1);

    te_errors=te_lost_block=te_file_count=0;
    ddfs_chk_te_block_found=&ba_found_in_files;

    printf("== Read files\n");
    te_progress=te_start=start=now();
    if (!progress_flag) te_progress=0;
    res=nftw(ddfs->rdir, ddfs_chk_tree_explore, 10, FTW_MOUNT | FTW_PHYS);
    end=now();
    printf("== Read %lld files in %.1fs\n", te_file_count, (end-start)*1.0/NOW_PER_SEC);

    // I add to ba_suspect all blocks that where recently allocated (since last backup) to
    // be sure these block where "committed"
    wrong_hash=suspect_count=0;
    if (ddfs->auto_fsck)
    {
        if (ddfs_load_usedblocks(&ba_backup)==0)
        {
            // ba_suspect=ba_suspect | (~ba_backup ^ ba_found_in_nodes)
            bit_array_plus_diff(&ba_suspect, &ba_found_in_files, &ba_backup);
            bit_array_count(&ba_suspect, &suspect_count, &u);
            printf("== Check 'last recently added' blocks: %lld.\n", suspect_count);
            wrong_hash=ddfs_chk_node_hash(&ba_suspect);
        }
    }
    else
    {
    	printf("filesystem cleanly shut down: skip 'last recently added' blocks check\n");
    }


    bit_array_count(&ba_found_in_files, &block_in_files, &u);
    bit_array_count(&ddfs->ba_usedblocks, &used_blocks, &free_blocks);
    long long int last_block=bit_array_search_nth_set(&ddfs->ba_usedblocks, 0, used_blocks);

    printf("== Summary   \n");
    printf("Used blocks              : %10lld\n", used_blocks);
    printf("Free blocks              : %10lld\n", free_blocks);
    printf("Last used blocks         : %10lld\n", last_block);
    printf("Total blocks             : %10lld\n", ddfs->c_block_count);
    printf("Blocks usage             : ");

    // Generate a usage bar, per 10% per 10%. '.'=none, "*"=full, 0 means between 1<= and <10%, 1 is for 10%<= and <20%
    int i;
    for (i=0; i<10; i++)
    {
    	long long int from=i*ddfs->c_block_count/10;
    	long long int to=(i+1)*ddfs->c_block_count/10-1;
    	long long int count=bit_array_count_zone(&ddfs->ba_usedblocks, from, to);
    	if (count==0) printf(".");
    	else if (count==to-from+1) printf("*");
    	else printf("%lld", 10*count/(to-from+1));
    }
    printf("\n");

    printf("Index errors             : %10lld %s\n", errors_in_index, errors_in_index!=0?"ERR":"OK");
    printf("Index blocks             : %10lld\n", block_in_index);
    printf("Index blocks dup         : %10lld %s\n", block_twice_in_index, block_twice_in_index!=0?"ERR":"OK");
    printf("Block errors             : %10lld %s\n", errors_in_block, errors_in_block!=0?"ERR":"OK");

    printf("Files                    : %10lld\n", te_file_count);
    printf("Files errors             : %10lld %s\n", te_errors, te_errors!=0?"ERR":"OK");
    printf("Files blocks             : %10lld\n", block_in_files);
    printf("Files lost blocks        : %10lld %s\n", te_lost_block, te_lost_block!=0?"WAR":"OK");

    printf("Tested hashes            : %10lld\n", suspect_count);
    printf("Wrong hashes             : %10lld %s\n", wrong_hash, wrong_hash!=0?"ERR":"OK");

    long long int only_index_vs_file, only_file_vs_index, both_index_file;
    bit_array_cmp_count(&ba_found_in_index, &ba_found_in_files, &only_index_vs_file, &only_file_vs_index, &both_index_file);
    printf("in index & not in files  : %10lld %s\n", only_index_vs_file, only_index_vs_file!=0?"OK can be reclaimed":"OK");
    printf("in files & not in index  : %10lld %s\n", only_file_vs_index, only_file_vs_index!=0?"ERR corrupted index":"OK");
    printf("in files & in index      : %10lld\n", both_index_file);

    long long int only_block_vs_file, only_file_vs_block, both_block_file;
    bit_array_cmp_count(&ddfs->ba_usedblocks, &ba_found_in_files, &only_block_vs_file, &only_file_vs_block, &both_block_file);
    printf("in blocks & not in files : %10lld %s\n", only_block_vs_file, only_block_vs_file!=0?"OK can be reclaimed":"OK");
    printf("in files & not in blocks : %10lld %s\n", only_file_vs_block, only_file_vs_block!=0?"corrupted files":"OK");

    res=(used_blocks==block_in_index \
        && errors_in_index==0 \
        && block_twice_in_index==0 \
        && te_errors==0 \
        && wrong_hash==0 \
        && errors_in_block==0 \
        && only_file_vs_index==0 \
        && only_file_vs_block==0);

    if (res && te_lost_block==0)
    {
        res=0;
        printf("Filesystem status        : OK %s\n", (only_index_vs_file || only_block_vs_file)?"blocks can be reclaimed":"");
    }
    else if (res && te_lost_block!=0)
    {
        res=2;
        printf("Filesystem status        : WARNING (files were already corrupted)%s\n", (only_index_vs_file || only_block_vs_file)?", blocks can be reclaimed":"");
    }
    else
    {
        res=1;
        printf("Filesystem status        : ERROR\n");

    }
    return res;
}


/**
 * used by ddfs_rebuild() and ddfs_hash_blocks() fill in the index that has been previously reset
 */
int bb_fill_index(struct blocks_buffer *bb, void *ctx)
{
    if (0!=memcmp(bb->hash, ddfs->zero_block_hash, ddfs->c_hash_size) && 0!=memcmp(bb->hash, ddfs->null_block_hash, ddfs->c_hash_size))
    {
        unsigned char node[NODE_SIZE];
        memcpy(node+ddfs->c_addr_size, bb->hash, ddfs->c_hash_size);
        ddfs_convert_addr(bb->addr, node);
        if (node_add(node, na_duplicate_hash_allowed)!=0)
        {   // should never happens
            DDFS_LOG(LOG_ERR, "critical error, cannot add more nodes, please correct by hand !\n");
            return 1; // emergency exit
        }
    }
    return 0;
}


/**
 * rebuild the index from scratch
 *
 * calculate hash from block used by files
 *
 * @param all_blocks
 * @param verbose
 * @return
 */
int ddfs_rebuild(int all_blocks)
{
    int res=0;
    long long int start, end;
    long long int u;

    //
    // make list of block to re-hash
    //
    if (all_blocks)
    {   // re-hash all blocks
        fprintf(stderr, "Re-Hash all blocks...\n");
        bit_array_reset(&ddfs->ba_usedblocks, 1);
    }
    else
    {   // take the list from Files
        bit_array_reset(&ddfs->ba_usedblocks, 0);
        bit_array_set(&ddfs->ba_usedblocks, 0);  // block ZERO is always used
        bit_array_set(&ddfs->ba_usedblocks, 1);  // block    1 is always used

        if (verbose_flag) printf("read block list from files...\n");
        te_file_count=te_addr_count=te_frag_count=te_corrupted_count=0;
        te_verbose=verbose_flag;
        te_block_found_in_files=&ddfs->ba_usedblocks;
        te_update_index=NULL;
        te_update_index_relaxed=0;
        te_corrupted_stream=NULL;
        te_fix_file=0;


        te_progress=te_start=start=now();
        if (!progress_flag) te_progress=0;
        res=nftw(ddfs->rdir, ddfs_fsck_tree_explore, 10, FTW_MOUNT | FTW_PHYS);
        end=now();
        if (res)
        {
            DDFS_LOG(LOG_ERR, "nftw is unable to read all files, correct and check filesystem again !\n");
            return 1;
        }

        long long int block_count;
        bit_array_count(&ddfs->ba_usedblocks, &block_count, &u);
        if (verbose_flag) printf("read %lld files in %.1fs. %lld blocks in use.\n", te_file_count, (end-start)*1.0/NOW_PER_SEC, block_count);
    }

    // reset all nodes
    memset(ddfs->nodes, '\0', ddfs->c_node_block_count*ddfs->c_index_block_size);

    //
    // re-hash blocks and update index
    //

    start=now();
    res=ddfs_hash_blocks(&ddfs->ba_usedblocks, DDFS_LAST_RESERVED_BLOCK+1, bb_fill_index, NULL);
    end=now();
    if (res) return 1;

    DDFS_LOG(LOG_INFO, "Read errors: %lld\n", ddfs_background_read_error);
    DDFS_LOG(LOG_INFO, "Read, hashed and updated index for %lld blocks in %.1fs\n", ddfs_background_block_count, (end-start)*1.0/NOW_PER_SEC);

    //
    // update files regarding new index
    //
    res=ddfs_fsck_fix_files_and_usedblocks(verbose_flag, progress_flag);

    return res;
}

long long int te_median_addr;
struct quick_bit_array te_q_count_zone, te_q_nth_unset;
/**
 * update block addresses after packing
 *
 */
int ddfs_pack_tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int res, len;
    unsigned char node[NODE_SIZE];
    blockaddr addr;
    uint64_t size;

    if (typeflag!=FTW_F || ! S_ISREG(sb->st_mode)) return 0;
    if (0==strncmp(fpath+ddfs->rdir_len, SPECIAL_DIR, ddfs->special_dir_len))
    {   // skip SPECIAL_DIR
        return 0;
    }

    te_file_count++;
    if (te_progress && now()-te_progress>NOW_PER_SEC)
    {
		te_progress=now();
		printf("Update file: %lld in %llds\r", te_file_count, (te_progress-te_start)/NOW_PER_SEC);
		fflush(stdout);
    }

    FILE *file;
    file=fopen(fpath, "r+");
    if (file==NULL)
    {
        fprintf(stderr, "cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file), &size);
    if (len==-1)
    {
        fprintf(stderr, "cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        if (te_verbose) fprintf(stderr, "invalid header, ignore file : %s\n", fpath);
        te_corrupted_count++;
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        fprintf(stderr, "cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    long long int block_pos=0;
    int cannot_fix_this_file=0;

    len=fread(node, 1, ddfs->c_node_size, file);

    while (len==ddfs->c_node_size)
    {
        addr=ddfs_get_node_addr(node);
        if (addr>=te_median_addr)
        {

        	long long int cnt=quick_bit_array_count_zone(&te_q_count_zone, addr);
        	blockaddr baddr=quick_bit_array_search_nth_unset(&te_q_nth_unset, cnt);
        	// assert(cnt==bit_array_count_zone(&ddfs->ba_usedblocks, te_median_addr, addr));
        	// assert(baddr==bit_array_search_nth_unset(&ddfs->ba_usedblocks, 0, cnt));
            ddfs_convert_addr(baddr, node);

			// set file position before to write
			res=fseek(file, ddfs->c_file_header_size+block_pos*ddfs->c_node_size, SEEK_SET);
			if (res==-1)
			{
				fprintf(stderr, "cannot update file: %s, %s\n", fpath, strerror(errno));
				cannot_fix_this_file=1;
			}
			else
			{
				len=fwrite(node, 1, ddfs->c_node_size, file);
				if (len!=ddfs->c_node_size)
				{
					if (len==-1)
					{
						fprintf(stderr, "cannot update file: %s, %s\n", fpath, strerror(errno));
						cannot_fix_this_file=1;
					}
					else
					{
						fprintf(stderr, "cannot update file: %s %d\n", fpath, len);
						cannot_fix_this_file=1;
					}
				}
				else
				{
					// if (te_verbose) fprintf(stderr, "update file: %s #%lld %lld --> %lld \n", fpath, block_pos, baddr, addr);
				}
			}
			// set file position before next read, required by ANSI C before to switch between read/write
			res=fseek(file, ddfs->c_file_header_size+(block_pos+1)*ddfs->c_node_size, SEEK_SET);
			if (res==-1)
			{
				fprintf(stderr, "cannot reposition file: %s, %s\n", fpath, strerror(errno));
				fclose(file);
				return 1;
			}
		}

        block_pos++;
        len=fread(node, 1, ddfs->c_node_size, file);
    }

    if (len==-1)
    {
        fprintf(stderr, "cannot read file: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    if (len!=0)
    {   // incomplete block
        fprintf(stderr, "file length problem: %s (%s)\n", fpath, strerror(errno));
    }

    if (block_pos!=(size+ddfs->c_block_size-1)/ddfs->c_block_size)
    {   // size mismatch
        fprintf(stderr, "size mismatch: %s (%s)\n", fpath, strerror(errno));
    }

    fclose(file);
    return 0;
}

/**
 * pack
 *
 * try to reduce the size of the blockfile by moving last blocks in the free spaces
 * available at the beginning.
 *
 */
int pack()
{
	long long int free_idx;
	long long int used_idx;
	long long int used_block;
	long long int to_move;
	long long int _u;
	long long int start, end, last;

	int buffer_size=4*1048576;
	int buffer_off=0;
	char *buffer;
	int res=0;

	fprintf(stderr, "Pack\n");
    bit_array_count(&ddfs->ba_usedblocks, &used_block, &_u);
    if (used_block==DDFS_LAST_RESERVED_BLOCK+1) goto TRUNCATE;
    to_move=used_block-bit_array_count_zone(&ddfs->ba_usedblocks, 0, used_block);
    if (to_move==0) goto TRUNCATE;

	buffer=malloc(buffer_size);
	if (buffer==NULL)
	{
		perror("malloc pack buffer");
		return 1;
	}

	// initialize the "quick" structure
    if (quick_bit_array_init(&te_q_count_zone, &ddfs->ba_usedblocks, -100, used_block, ddfs->c_block_count, qba_count_zone) ||
        quick_bit_array_init(&te_q_nth_unset,  &ddfs->ba_usedblocks, -100, 0, used_block, qba_nth_unset))
    {
    	perror("malloc quick structure");
    	exit(1);
    }

	fprintf(stderr, "== Move %lld (used=%lld, total=%lld) blocks to the beginning.\n", to_move, used_block, ddfs->c_block_count);

	free_idx=bit_array_search_first_unset(&ddfs->ba_usedblocks, DDFS_LAST_RESERVED_BLOCK+1);
    used_idx=bit_array_search_first_set(&ddfs->ba_usedblocks, used_block);

    long long int count=0;
    start=last=now();
    while (used_idx>=0)
    {
        blockaddr addr=used_idx;
        // No direct_io on 'bfile_ro'
		int len=pread(ddfs->bfile_ro, buffer+buffer_off, ddfs->c_block_size, addr<<ddfs->block_size_shift);
		if (len!=ddfs->c_block_size)
		{
			if (len==-1)
			{
				fprintf(stderr, "block %lld, cannot read: %s\n", addr, strerror(errno));
			}
			else if (len!=0)
			{
				fprintf(stderr, "block %lld, cannot read: %d/%d\n", addr, len, ddfs->c_block_size);
			}
			else // len==0
			{ // EOF
				fprintf(stderr, "block %lld, unexpected EOF\n", addr);
				break;
			}
		}

        used_idx=bit_array_search_first_set(&ddfs->ba_usedblocks, used_idx+1);
		buffer_off+=ddfs->c_block_size;
		if (buffer_off>=buffer_size || used_idx<0)
		{
			// need to flush
			long int off=0;
			while (off<buffer_off)
			{
				addr=free_idx;
		        // buffer must be aligned when using direct_io, then use double buffering
		        memcpy(ddfs->aux_buffer, buffer+off, ddfs->c_block_size);
		        int len=pwrite(ddfs->bfile, ddfs->aux_buffer, ddfs->c_block_size, addr<<ddfs->block_size_shift);
		        count++;
		        if (progress_flag && now()-last>NOW_PER_SEC)
				{
					last=now();
					printf("moving blocks %5.1f%% in %llds\r", count*100.0/to_move, (last-start)/NOW_PER_SEC);
					fflush(stdout);
				}

		        if (len!=ddfs->c_block_size)
				{
					if (len==-1)
					{
						fprintf(stderr, "block %lld, cannot write: %s\n", addr, strerror(errno));
					}
					else
					{
						fprintf(stderr, "block %lld, cannot write: %d/%d\n", addr, len, ddfs->c_block_size);
					}
				}
		        free_idx=bit_array_search_first_unset(&ddfs->ba_usedblocks, free_idx+1);
		        off+=ddfs->c_block_size;
			}
			buffer_off=0;
		}

    }

    fprintf(stderr, "== Update files\n");

    te_errors=te_lost_block=te_file_count=0;
    te_median_addr=used_block;
    te_progress=te_start=start=now();
    if (!progress_flag) te_progress=0;
    res=nftw(ddfs->rdir, ddfs_pack_tree_explore, 10, FTW_MOUNT | FTW_PHYS);
    end=now();
    fprintf(stderr, "== Updated %lld files in %.1fs\n", te_file_count, (end-start)*1.0/NOW_PER_SEC);

    fprintf(stderr, "Update index\n");

    nodeidx node_idx=0;

    while (node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr>=used_block)
        {
        	long long int cnt=quick_bit_array_count_zone(&te_q_count_zone, addr);
        	blockaddr baddr=quick_bit_array_search_nth_unset(&te_q_nth_unset, cnt);
        	// assert(cnt==bit_array_count_zone(&ddfs->ba_usedblocks, used_block, addr));
        	// assert(baddr==bit_array_search_nth_unset(&ddfs->ba_usedblocks, 0, cnt));
            ddfs_convert_addr(baddr, node);
        }
        node_idx++;
    }

    fprintf(stderr, "Update free block list\n");
    bit_array_reset_zone(&ddfs->ba_usedblocks, 0, used_block-1, 1);
    bit_array_reset_zone(&ddfs->ba_usedblocks, used_block, ddfs->c_block_count-1, 0);

    free(buffer);

TRUNCATE:
	if (!ddfs->bfile_isblk)
	{
		res=ftruncate(ddfs->bfile, used_block*ddfs->c_block_size);
		if (res==-1) perror(ddfs->blockfile);
		else
		{
		    ddfs->bfile_size=used_block*ddfs->c_block_size;
		    ddfs->bfile_last=used_block;
		}
	}

    return res;

}


struct ddfs_ctx ddfsctx;

int main(int argc, char *argv[])
{
    int res=0;
    int c;
    int force_flag=0;
    int lock_index_flag=0;
    int check_block_flag=0;
    int normal_flag=0;
    int normal_relaxed_flag=0;
    int rebuild_flag=0;
    int rebuild_block_flag=0;

    ddfs=&ddfsctx;
    while (1)
    {
        // getopt_long stores the option index here.
        int option_index=0;

        c=getopt_long(argc, argv, "hvlfpcCnNrRdk", long_options, &option_index);

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

            case 'l':
                lock_index_flag=1;
                break;

            case 'f':
                force_flag=1;
                break;

            case 'p':
                progress_flag=1;
                break;

            case 'C':
                check_block_flag=1;
                break;
            case 'c':
                check_flag=1;
                break;

            case 'n':
                normal_flag=1;
                break;
            case 'N':
                normal_relaxed_flag=1;
                break;

            case 'R':
                rebuild_block_flag=1;
                break;
            case 'r':
                rebuild_flag=1;
                break;

            case 'k':
                pack_flag=1;
                break;

            case 'd':
                ddfs_debug=1;
                break;

            case '?':
                // getopt_long already printed an error message.
                break;

            default:
                abort();
        }
    }

    if (1!=check_block_flag+check_flag+rebuild_block_flag+rebuild_flag+normal_flag+normal_relaxed_flag)
    {
        fprintf(stderr, "One of the options c, C, p, P, r, R must be selected, and only one !\n");
        return 1;
    }

    if (optind>=argc)
    {
        fprintf(stderr, "Target directory is missing !\n");
        return 1;
    }

    char pdir[FILENAME_MAX];

    if (ddfs_find_parent(argv[optind], pdir))
    {
        fprintf(stderr, "Not a valid ddumbfs filesystem parent directory: %s\n", argv[optind]);
        return -1;
    }

    if (is_mounted(pdir))
    {
        if (check_flag || check_block_flag) printf("This is not accurate to check an online file system !\n");
        else
        {
            fprintf(stderr, "filesystem is online: %s\n", pdir);
            return 1;
        }
    }

    if (ddfs_loadcfg(pdir, NULL))
    {
        return 1;
    }

    if (ddfs->rebuild_fsck && !(rebuild_flag || rebuild_block_flag))
    {
        fprintf(stderr, "The filesystem need to be rebuild using option '-r' or '-R' !\n");
        return 1;
    }

    if (ddfs_init(force_flag, (rebuild_flag || rebuild_block_flag), DDFS_NODIRECTIO, lock_index_flag, stderr))
    {
        return 1;
    }

    if (rebuild_flag || rebuild_block_flag || check_block_flag)
    {
    	int cpu=ddfs_cpu_count();
    	if (cpu<=0) cpu=2;
    	// read at least 1Mo in buffers
    	int buffers=1048576/ddfs->c_block_size;
    	if (2+2*cpu>buffers) buffers=2+2*cpu;
        if (ddfs_background_init(buffers, cpu)) return 1;
        printf("started %d hashing thread(s)\n", cpu);
    }

    if (check_flag || check_block_flag)
    {
        res=ddfs_check(check_block_flag);
    }
    else if (rebuild_flag || rebuild_block_flag)
    {
        ddfs_lock(".autofsck"); // I don't care if it works or not
        if (ddfs_lock(".rebuildfsck")) perror(".rebuildfsck");
        res=ddfs_rebuild(rebuild_block_flag);
        if (0==res)
        {
            if (ddfs_unlock(".rebuildfsck")==-1) perror(".rebuildfsck");
            if (pack_flag) res=pack();
            if (0==res)
            {
            	if (ddfs_unlock(".autofsck")==-1) perror(".autofsck");
            }
        }
    }
    else if (normal_flag || normal_relaxed_flag)
    {
        ddfs_lock(".autofsck"); // I don't care if it works or not
        res=ddfs_fsck(normal_relaxed_flag, verbose_flag, progress_flag);
        if (0==res)
        {
            if (pack_flag) res=pack();
            if (0==res)
            {
            	if (ddfs_unlock(".autofsck")==-1) perror(".autofsck");
            }
        }
    }
    else
    {
        usage();
    }

    if (verbose_flag) printf("%s\n", res==0?"OK":(res==2?"WARNING":"ERROR"));

    ddfs_close();
    return res;

}
