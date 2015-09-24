/*
 * ddfschkrep.c
 *
 *  Created on: Jun 4, 2011
 *      Author: asx
 */

#define _XOPEN_SOURCE 500

#define _LARGEFILE64_SOURCE
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <ftw.h>

#include "ddfschkrep.h"


/*
 *  tree_explore... variables
 */
long long int te_file_count;
long long int te_addr_count;
long long int te_frag_count;
long long int te_corrupted_count;
long long int te_errors;
long long int te_hash_counter;
long long int te_block_counter;
long long int te_lost_block;
long long int te_progress;
long long int te_start;

long long int te_fix_file;

int te_verbose;
int te_update_index_relaxed;
FILE *te_corrupted_stream=NULL;

struct bit_array *te_block_found_in_files=NULL;
struct bit_array *te_update_index=NULL;
struct bit_array *te_usedblocks=NULL;


/**
 * check and fix the order of nodes inside the index.
 * remove node with addresses out of ranges.
 * don't calculate any hash.
 *
 * @param wrong return the list of blocks that were at wrong place in the index
 * @param verbose if true report all errors
 * @return number of errors
 */
long long int ddfs_fsck_repair_node_order(struct bit_array *wrong, int verbose)
{
    long long int errors=0;
    nodeidx node_idx=0;

    while (node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        unsigned char *hash=node+ddfs->c_addr_size;
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr==0)
        {
        	if (0!=memcmp(hash, ddfs->null_block_hash, ddfs->c_hash_size))
        	{	// the memcmp() is useful to avoid unnecessary WRITE to disk
        		memset(hash, '\0', ddfs->c_hash_size);
        	}
        }
        else if (addr<=DDFS_LAST_RESERVED_BLOCK || ddfs->c_block_count<=addr)
        {
	    // The address is before the reserved block, or after the last block
            errors++;
            if (verbose) DDFS_LOG(LOG_ERR, "repair_node_order: node %lld (addr=%lld) has an invalid address, delete\n", node_idx, addr);
            node_delete(node_idx);
            continue;
        }
        else
        {
	    // Get the correct index position for the hash
            nodeidx cidx=ddfs_hash2idx(hash);
            if (node_idx<cidx)
            {
		// If the node is before its ideal position, fix
                errors++;
                if (verbose) DDFS_LOG(LOG_ERR, "repair_node_order: node %lld (addr=%lld) before it ideal position %lld, fix\n", node_idx, addr, cidx);
                if (wrong!=NULL) bit_array_set(wrong, addr);
                node_fix(node_idx);
                continue;
            }
            else if (cidx==node_idx)
            {
            }
            else // (cidx<node_idx)
            {
		// The node is after its ideal position - fix if the previous node is empty
                blockaddr baddr=ddfs_get_node_addr(node-ddfs->c_node_size);
                if (baddr==0 || memcmp(node+(ddfs->c_addr_size-ddfs->c_node_size), hash, ddfs->c_hash_size)>0)
                {
                    errors++;
                    if (verbose) DDFS_LOG(LOG_ERR, "repair_node_order: nodes %lld (addr=%lld) and %lld (addr=%lld) in wrong order, fix.\n", node_idx-1, baddr, node_idx, addr);
                    if (wrong!=NULL) bit_array_set(wrong, addr);
                    node_fix(node_idx);
                    node_idx=cidx;
                    continue;
                }
            }
        }
        node_idx++;
    }
    return errors;
}

void ddfs_chk_preload_node(nodeidx node_idx, int num_nodes)
{
    //printf("Preloading %d nodes starting at index position %lld\n", num_nodes, node_idx);

    long long node_offset=(node_idx*ddfs->c_node_size) & (0-getpagesize());
    int numpages=1 + ((num_nodes*ddfs->c_node_size) / getpagesize());
    madvise(ddfs->nodes+node_offset, getpagesize() * numpages, MADV_WILLNEED);
}

/**
 * verify hashes inside the index for a given list of nodes and remove bad nodes
 *
 * @param list the list of blocks to check
 * @param verbose if true report all wrong nodes
 * @return number of errors
 */
long long int ddfs_fsck_check_node_hash(struct bit_array *list, int verbose)
{
    long long int errors=0;
    long long int hash_counter=0;
    nodeidx node_idx=0;

    while (node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        unsigned char *hash=node+ddfs->c_addr_size;
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr==0)
        {
        }
        else if (addr<=DDFS_LAST_RESERVED_BLOCK)
        {
            errors++;
            if (verbose) DDFS_LOG(LOG_ERR, "check_node_hash: node %lld (addr=%lld) has an invalid address, deleted.\n", node_idx, addr);
            node_delete(node_idx);
            continue;
        }
        else if (bit_array_get(list, addr))
        {   // this is a node to check
            int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
            if (len!=ddfs->c_block_size)
            {
                ddfs->error_block_read++;
                errors++;
                DDFS_LOG(LOG_ERR, "cannot read block %lld (%s), delete node %lld from index.\n", addr, len==-1?strerror(errno):"", node_idx);
                // if the block is unreadable, remove it from the index !
                node_delete(node_idx);
                continue;
            }
            // check the hash
            unsigned char bhash[HASH_SIZE];
            // DDFS_LOG(LOG_INFO, "calculate hash for block addr=%lld\n", addr);
            ddfs_hash(ddfs->aux_buffer, bhash);
            hash_counter++;
            if (hash_counter%1000==0) DDFS_LOG(LOG_INFO, "calculated %lld hashes, continuing.\n", hash_counter);
            if (0==memcmp(bhash, hash, ddfs->c_hash_size))
            {
                if (node_idx>0 && 0==memcmp(hash, hash-ddfs->c_node_size, ddfs->c_hash_size))
                {
                    errors++;
                    if (verbose) DDFS_LOG(LOG_ERR, "check_node_hash: node %lld (addr=%lld) is identical to previous one, remove.\n", node_idx, addr);
                    node_delete(node_idx); // if node are ordered regarding the addr too, remove node with the highest addr
                    continue;
                }
            }
            else
            {
                errors++;
                if (verbose) DDFS_LOG(LOG_ERR, "check_node_hash: node %lld (addr=%lld) has a wrong hash, deleted.\n", node_idx, addr);
                node_delete(node_idx);
                continue;
            }
        }
        node_idx++;
    }
    return errors;
}

/**
 * calculate hashes of a list of blocks and add them to the index
 *
 * @param list the list of blocks to hash
 * @param verbose if true report all wrong nodes
 * @param progress if true display progress sec by sec
 * @return number of errors
 */
long long int ddfs_update_index_from_blocks(struct bit_array *list, int verbose, int progress)
{
    long long int errors=0;
    long long int block_count, _u;
    long long int start=now();
    long long int last=start;
    unsigned char node[NODE_SIZE];
    unsigned char *hash=node+ddfs->c_addr_size;

    bit_array_count(list, &block_count, &_u);

    long long int i=0;
    blockaddr addr=bit_array_search_first_set(list, DDFS_LAST_RESERVED_BLOCK+1);
    while (addr>0)
    {
        if (progress && now()-last>NOW_PER_SEC)
		{
			last=now();
			printf("re-hashing blocks: %5.1f%% in %llds\r", i*100.0/block_count, (last-start)/NOW_PER_SEC);
			fflush(stdout);
		}

        int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
        if (len==-1)
        {
            DDFS_LOG(LOG_ERR, "cannot read block %lld, skip (%s)\n", addr, strerror(errno));
            errors++;
        }
        else if (len==0 && addr>ddfs->bfile_last)
        {
        	// do nothing
        }
        else if (len!=ddfs->c_block_size)
        {
            DDFS_LOG(LOG_ERR, "cannot read block %lld, skip\n", addr);
            errors++;
        }
        else
        {
            ddfs_hash(ddfs->aux_buffer, hash);
            ddfs_convert_addr(addr, node);
            // node_add() will add this node and remove other nodes having identical hashes
            if (node_add(node, na_unique_hash)!=0) return 1;
        }
    	addr=bit_array_search_first_set(list, addr+1);
    }
    return errors;
}


/**
 * report block addresses that are in use and that are referenced more than once
 *
 * foundblocks2 will contains addresses that are referenced twice
 * or more (different hash) or that are duplicate (same hash) with other
 *
 * @param foundblocks return list of blocks found in the index
 * @param foundblocks2 return list of blocks over-referenced
 */
long long int ddfs_fsck_node_accounting(struct bit_array *foundblocks, struct bit_array *foundblocks2)
{
    nodeidx node_idx;
    blockaddr addr, paddr=0;
    unsigned char* node=ddfs->nodes;
    unsigned char* pnode=NULL;
    long long int errors=0;

    for (node_idx=0; node_idx<ddfs->c_node_count; node_idx++)
    {
        addr=ddfs_get_node_addr(node);
        int err=0;
        if (addr!=0)
        { 	// node is not empty
			// check if address was already used as reference for another hash
			if (bit_array_get(foundblocks, addr))
			{
				bit_array_set(foundblocks2, addr);
				errors++;
				err=1;
			}
			else bit_array_set(foundblocks, addr);

			// check if hash is identical to previous one
			if (node_idx>0 && paddr!=0 && 0==memcmp(node+ddfs->c_addr_size, pnode+ddfs->c_addr_size, ddfs->c_hash_size))
			{
				bit_array_set(foundblocks2, addr);
				bit_array_set(foundblocks2, paddr);
				errors+=2-err;
			}
        }
        paddr=addr;
        pnode=node;
        node+=ddfs->c_node_size;
    }
    return errors;
}

/**
 * delete useless node
 *
 * references to unused blocks can be removed
 *
 * @param keep the list of blocks to keep
 * @return number of deleted nodes
 */
long long int ddfs_fsck_remove_useless_node(struct bit_array *keep)
{
    long long int deleted=0;
    nodeidx node_idx=0;

    while (node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        if (addr!=0 && !bit_array_get(keep, addr))
        {
            deleted++;
            node_delete(node_idx);
            continue;

        }
        node_idx++;
    }
    return deleted;
}

/**
 * function called by nftw to walk through the directory tree
 *
 */
int ddfs_fsck_tree_explore_old(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int res, len;
    unsigned char node[NODE_SIZE];
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr;
    blockaddr prev_addr=-1;
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

    FILE *file;
    if (te_fix_file) file=fopen(fpath, "r+");
    else file=fopen(fpath, "r");
    if (file==NULL)
    {
        DDFS_LOG(LOG_ERR, "cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file), &size);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        if (te_verbose) DDFS_LOG(LOG_INFO, "invalid header, ignore file : %s\n", fpath);
        if (te_corrupted_stream)
        {
            fprintf(te_corrupted_stream, " H   %s\n", fpath);
            DDFS_LOG(LOG_WARNING,  " H   %s\n", fpath);
        }
        te_corrupted_count++;
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    long long int block_pos=0;
    int corrupted=0;
    int updated=0;
    int len_mismatch=' ';
    int size_mismatch=' ';
    int cannot_fix_this_file=0;

    len=fread(node, 1, ddfs->c_node_size, file);

    while (len==ddfs->c_node_size)
    {
        addr=ddfs_get_node_addr(node);

        if (prev_addr!=-1 && prev_addr+1!=addr && addr!=0) te_frag_count++;
        if (addr!=0) prev_addr=addr;

        te_addr_count++;

        if (te_update_index || te_update_index_relaxed)
        {
            if (addr<=DDFS_LAST_RESERVED_BLOCK || ddfs->c_block_count<=addr)
            {
            }
            else
            {
                blockaddr baddr;
                nodeidx node_idx=ddfs_search_hash(hash, &baddr);
                if ((node_idx<0 || addr!=baddr) && !(te_update_index && bit_array_get(te_update_index, addr)))
                {
                    // the block has not been already checked
                    if (te_update_index_relaxed)
                    {
                        if (node_add(node, na_duplicate_hash_allowed)!=0) return 1;
                        if (te_update_index) bit_array_set(te_update_index, addr);
                    }
                    else
                    {


                        te_hash_counter++;
                        if (te_hash_counter%1000==0) DDFS_LOG(LOG_INFO, "calculated %lld hashes, continuing.\n", te_hash_counter);

                        int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
                        if (len==-1)
                        {
                            DDFS_LOG(LOG_ERR, "cannot read block %lld, skip (%s)\n", addr, strerror(errno));
                        }
                        else if (len==0 && addr>ddfs->bfile_last)
                        {
                        	// do nothing
                        }
                        else if (len!=ddfs->c_block_size)
                        {
                            DDFS_LOG(LOG_ERR, "cannot read block %lld, skip\n", addr);
                        }
                        else
                        {
                            unsigned char bhash[HASH_SIZE];
                            ddfs_hash(ddfs->aux_buffer, bhash);
                            if (0==memcmp(bhash, hash, ddfs->c_hash_size))
                            {
                                if (node_add(node, na_duplicate_hash_allowed)!=0) return 1;
                                bit_array_set(te_update_index, addr);
                            }
                            // don't fix the file here
                        }
                    }
                }
            }
        }

        if (te_fix_file && !cannot_fix_this_file)
        {
            blockaddr baddr=addr;
            if (addr==0)
            {
                if (0!=memcmp(ddfs->null_block_hash, hash, ddfs->c_hash_size) && 0!=memcmp(ddfs->zero_block_hash, hash, ddfs->c_hash_size))
                {
                    addr=1;
                    memcpy(hash, ddfs->null_block_hash, ddfs->c_hash_size);
                }
            }
            else
            {
                ddfs_search_hash(hash, &addr);
            }

            if (baddr!=addr)
            {   // address has changed, must update file
                ddfs_convert_addr(addr, node);
                // set file position before to write
                res=fseek(file, ddfs->c_file_header_size+block_pos*ddfs->c_node_size, SEEK_SET);
                if (res==-1)
                {
                    DDFS_LOG(LOG_ERR, "cannot fix file: %s, %s\n", fpath, strerror(errno));
                    cannot_fix_this_file=1;
                }
                else
                {
                    len=fwrite(node, 1, ddfs->c_node_size, file);
                    if (len!=ddfs->c_node_size)
                    {
                        if (len==-1)
                        {
                            DDFS_LOG(LOG_ERR, "cannot fix file: %s, %s\n", fpath, strerror(errno));
                            cannot_fix_this_file=1;
                        }
                        else
                        {
                            DDFS_LOG(LOG_ERR, "cannot fix file: %s %d\n", fpath, len);
                            cannot_fix_this_file=1;
                        }
                    }
                    else
                    {
                        updated=1;
                        if (te_verbose) DDFS_LOG(LOG_INFO, "update file: %s #%lld %lld --> %lld \n", fpath, block_pos, baddr, addr);
                    }
                }
                // set file position before next read, required by ANSI C before to switch between read/write
                res=fseek(file, ddfs->c_file_header_size+(block_pos+1)*ddfs->c_node_size, SEEK_SET);
                if (res==-1)
                {
                    DDFS_LOG(LOG_ERR, "cannot reposition file: %s, %s\n", fpath, strerror(errno));
                    fclose(file);
                    return 1;
                }
            }

            if (addr==1 || cannot_fix_this_file) corrupted=1;
        }

        if (te_block_found_in_files && addr<ddfs->c_block_count) bit_array_set(te_block_found_in_files, addr); // use the correct address

        block_pos++;
        len=fread(node, 1, ddfs->c_node_size, file);
    }

    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read file: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    if (len!=0)
    {   // incomplete block
        //fprintf(stderr, "incomplete block: %s %d\n", fpath, len);
        len_mismatch='L';
        if (te_fix_file)
        {   // fix the file length
            res=ftruncate(fileno(file), ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            if (res==-1)
            {
                DDFS_LOG(LOG_ERR, "cannot fix underneath file length: %s (%s)\n", fpath, strerror(errno));
                cannot_fix_this_file=1;
            }
            else
            {
                len_mismatch='l';
                updated=1;
                if (te_verbose) DDFS_LOG(LOG_INFO, "fixed underneath file length mismatch: %s %lld -> %lld\n", fpath,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size+len,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            }
        }
    }

    if (block_pos!=(size+ddfs->c_block_size-1)/ddfs->c_block_size)
    {   // size mismatch
        size_mismatch='S';
        if (te_fix_file)
        {
            size=block_pos*ddfs->c_block_size;
            len=file_header_get_conv(fileno(file), size);
            if (len==-1)
            {
                DDFS_LOG(LOG_ERR, "cannot fix file size mismatch: %s (%s)\n", fpath, strerror(errno));
                cannot_fix_this_file=1;
            }
            else if (len!=ddfs->c_file_header_size)
            {
                DDFS_LOG(LOG_ERR, "cannot fix file size mismatch: %s len=%d/%d\n", fpath, len, ddfs->c_node_size);
                cannot_fix_this_file=1;
            }
            else
            {
                size_mismatch='s';
                updated=1;
                if (te_verbose) DDFS_LOG(LOG_INFO, "fixed file size mismatch: %s %lld -> %lld\n", fpath,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size+len,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            }
        }
    }

    char status[5];
    memset(status, ' ', sizeof(status));
    status[4]='\0';
    int bad=size_mismatch=='S' || len_mismatch=='L' || corrupted || cannot_fix_this_file;
    if (updated && !bad)
    {
        status[0]='F'; // FIXED
        te_fix_file++;
    }
    if (bad) te_corrupted_count++;
    if (corrupted) status[1]='C';
    status[2]=size_mismatch;
    status[3]=len_mismatch;

    if (updated || bad)
    {
        if (te_corrupted_stream) fprintf(te_corrupted_stream, "%s %s\n", status, fpath);
        if (te_fix_file) DDFS_LOG(LOG_WARNING, "File was fixed as follows: %s %s\n", status, fpath);
    }

    fclose(file);
    return 0;
}

/**
 * function called by nftw to build the usedblocks list
 *
 */
int ddfs_fsck_tree_find_usedblocks(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int len, res;
    blockaddr addr;
    uint64_t size;

    // Reset the global variable
    te_block_counter=0;

    // We need a place to store one node at a time
    unsigned char node[NODE_SIZE];

    if(!te_usedblocks) {
        DDFS_LOG(LOG_ERR, "ddfs_fsck_tree_find_usedblocks() called without te_usedblocks being set\n");
	return 1;
    }

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

    FILE *file;
    file=fopen(fpath, "r");
    if (file==NULL)
    {
        DDFS_LOG(LOG_ERR, "cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file), &size);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        if (te_verbose) DDFS_LOG(LOG_INFO, "invalid header, ignore file : %s\n", fpath);
        if (te_corrupted_stream)
        {
            fprintf(te_corrupted_stream, " H   %s\n", fpath);
            DDFS_LOG(LOG_WARNING,  " H   %s\n", fpath);
        }
        te_corrupted_count++;
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    len=fread(node, 1, ddfs->c_node_size, file);
    while(len == ddfs->c_node_size) {
	addr=ddfs_get_node_addr(node);

	// Mark the block as used if it's in the right range
	if (addr>=0 && addr <ddfs->c_block_count) {
	    bit_array_set(te_usedblocks, addr);
	}

	// Read the next block from the file
	len=fread(node, 1, ddfs->c_node_size, file);
    }

    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read file: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

/**
 * function called by nftw to walk through the directory tree
 *
 */
int ddfs_fsck_tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int res, len, pcount;
    unsigned char node[NODE_SIZE*NODE_FSCK_PREFETCH];
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr;
    blockaddr prev_addr=-1;
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

    FILE *file;
    if (te_fix_file) file=fopen(fpath, "r+");
    else file=fopen(fpath, "r");
    if (file==NULL)
    {
        DDFS_LOG(LOG_ERR, "cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file), &size);
    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        if (te_verbose) DDFS_LOG(LOG_INFO, "invalid header, ignore file : %s\n", fpath);
        if (te_corrupted_stream)
        {
            fprintf(te_corrupted_stream, " H   %s\n", fpath);
            DDFS_LOG(LOG_WARNING,  " H   %s\n", fpath);
        }
        te_corrupted_count++;
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    long long int block_pos=0;
    int corrupted=0;
    int updated=0;
    int len_mismatch=' ';
    int size_mismatch=' ';
    int cannot_fix_this_file=0;

    len=fread(node, 1, ddfs->c_node_size*NODE_FSCK_PREFETCH, file);
    pcount=len/ddfs->c_node_size;
    len=len%ddfs->c_node_size;

    if (!ddfs->lock_index)
    {
	int pnum;
	for(pnum=0;pnum<pcount;pnum++) {
	    ddfs_chk_preload_node(ddfs_hash2idx(node+(pnum*ddfs->c_node_size)+ddfs->c_addr_size), 1);
	}
    }

    while (pcount)
    {
        addr=ddfs_get_node_addr(node);

        if (prev_addr!=-1 && prev_addr+1!=addr && addr!=0) te_frag_count++;
        if (addr!=0) prev_addr=addr;

        te_addr_count++;

        if (te_update_index || te_update_index_relaxed)
        {
            if (addr<=DDFS_LAST_RESERVED_BLOCK || ddfs->c_block_count<=addr)
            {
            }
            else
            {
                blockaddr baddr;
                nodeidx node_idx=ddfs_search_hash(hash, &baddr);
                if ((node_idx<0 || addr!=baddr) && !(te_update_index && bit_array_get(te_update_index, addr)))
                {
                    // the block has not been already checked
                    if (te_update_index_relaxed)
                    {
                        if (node_add(node, na_duplicate_hash_allowed)!=0) return 1;
                    }
                    // remember the addr for further work
                    if (te_update_index) bit_array_set(te_update_index, addr);
                    te_block_counter++;
                }
            }
        }

        if (te_fix_file && !cannot_fix_this_file)
        {
            blockaddr baddr=addr;
            // search hash and update addr to 0, 1, or >0 depending the hash
            ddfs_search_hash(hash, &addr);

            if (baddr!=addr)
            {   // address has changed, must update file
                ddfs_convert_addr(addr, node);
                // set file position before to write
                res=fseek(file, ddfs->c_file_header_size+block_pos*ddfs->c_node_size, SEEK_SET);
                if (res==-1)
                {
                    DDFS_LOG(LOG_ERR, "cannot fix file: %s, %s\n", fpath, strerror(errno));
                    cannot_fix_this_file=1;
                }
                else
                {
                    len=fwrite(node, 1, ddfs->c_node_size, file);
                    if (len!=ddfs->c_node_size)
                    {
                        if (len==-1)
                        {
                            DDFS_LOG(LOG_ERR, "cannot fix file: %s, %s\n", fpath, strerror(errno));
                            cannot_fix_this_file=1;
                        }
                        else
                        {
                            DDFS_LOG(LOG_ERR, "cannot fix file: %s %d\n", fpath, len);
                            cannot_fix_this_file=1;
                        }
                    }
                    else
                    {
                        updated=1;
                        if (te_verbose) DDFS_LOG(LOG_INFO, "update file: %s #%lld %lld --> %lld \n", fpath, block_pos, baddr, addr);
                    }
                }
                // set file position before next read, required by ANSI C before to switch between read/write
                res=fseek(file, ddfs->c_file_header_size+(block_pos+pcount)*ddfs->c_node_size, SEEK_SET);
                if (res==-1)
                {
                    DDFS_LOG(LOG_ERR, "cannot reposition file: %s, %s\n", fpath, strerror(errno));
                    fclose(file);
                    return 1;
                }
            }

            if (addr==1 || cannot_fix_this_file) corrupted=1;
        }

        if (te_block_found_in_files && addr<ddfs->c_block_count) bit_array_set(te_block_found_in_files, addr); // use the correct address

        block_pos++;

	// Move the next node to the start of the array
	if(pcount>1)
	    memmove(node, node+ddfs->c_node_size, ddfs->c_node_size*(pcount-1));

	// If we had a full buffer, read the next node
	if(pcount == NODE_FSCK_PREFETCH)
	{
	    len=fread(node+(ddfs->c_node_size*(pcount-1)), 1, ddfs->c_node_size, file);

	    // If we did not read a full node, reduce the node count, else preload the new node
	    if(len != ddfs->c_node_size)
		pcount--;
	    else
		ddfs_chk_preload_node(ddfs_hash2idx(node+((pcount-1)*ddfs->c_node_size)+ddfs->c_addr_size), 1);
	}
	else
	{
	    // We do not have a full buffer, so we reduce the count until we run out of nodes
	    pcount--;
	}
    }

    if (len==-1)
    {
        DDFS_LOG(LOG_ERR, "cannot read file: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    if (len!=0)
    {   // incomplete block
        //fprintf(stderr, "incomplete block: %s %d\n", fpath, len);
        len_mismatch='L';
        if (te_fix_file)
        {   // fix the file length
            res=ftruncate(fileno(file), ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            if (res==-1)
            {
                DDFS_LOG(LOG_ERR, "cannot fix underneath file length: %s (%s)\n", fpath, strerror(errno));
                cannot_fix_this_file=1;
            }
            else
            {
                len_mismatch='l';
                updated=1;
                if (te_verbose) DDFS_LOG(LOG_INFO, "fixed underneath file length mismatch: %s %lld -> %lld\n", fpath,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size+len,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            }
        }
    }

    if (block_pos!=(size+ddfs->c_block_size-1)/ddfs->c_block_size)
    {   // size mismatch
        size_mismatch='S';
        if (te_fix_file)
        {
            size=block_pos*ddfs->c_block_size;
            len=file_header_get_conv(fileno(file), size);
            if (len==-1)
            {
                DDFS_LOG(LOG_ERR, "cannot fix file size mismatch: %s (%s)\n", fpath, strerror(errno));
                cannot_fix_this_file=1;
            }
            else if (len!=ddfs->c_file_header_size)
            {
                DDFS_LOG(LOG_ERR, "cannot fix file size mismatch: %s len=%d/%d\n", fpath, len, ddfs->c_node_size);
                cannot_fix_this_file=1;
            }
            else
            {
                size_mismatch='s';
                updated=1;
                if (te_verbose) DDFS_LOG(LOG_INFO, "fixed file size mismatch: %s %lld -> %lld\n", fpath,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size+len,  ddfs->c_file_header_size+block_pos*ddfs->c_node_size);
            }
        }
    }

    char status[5];
    memset(status, ' ', sizeof(status));
    status[4]='\0';
    int bad=size_mismatch=='S' || len_mismatch=='L' || corrupted || cannot_fix_this_file;
    if (updated && !bad)
    {
        status[0]='F'; // FIXED
        te_fix_file++;
    }
    if (bad) te_corrupted_count++;
    if (corrupted) status[1]='C';
    status[2]=size_mismatch;
    status[3]=len_mismatch;

    if (updated || bad)
    {
        if (te_corrupted_stream) fprintf(te_corrupted_stream, "%s %s\n", status, fpath);
        if (te_fix_file) DDFS_LOG(LOG_WARNING, "%s %s\n", status, fpath);
    }

    fclose(file);
    return 0;
}

/**
 * use valid index to check and fix files, update ba_usedblocks too
 *
 * @return 0 for success
 */
int ddfs_fsck_fix_files_and_usedblocks(int verbose, int progress)
{
    long long int start, end;
    long long int block_count, block_free;
    char filename[FILENAME_MAX];
    int res=0;

    te_file_count=te_addr_count=te_frag_count=te_corrupted_count=0;
    te_verbose=verbose;
    te_progress=te_start=now();

    if (!progress) te_progress=0;

    bit_array_reset(&ddfs->ba_usedblocks, 0x0);
    bit_array_set(&ddfs->ba_usedblocks, 0);  // block ZERO is always used
    bit_array_set(&ddfs->ba_usedblocks, 1);  // block    1 is always used
    te_block_found_in_files=&ddfs->ba_usedblocks;
    te_update_index=NULL;
    te_update_index_relaxed=0;
    sprintf(filename, "%s/%s", ddfs->rdir, DDFS_CORRUPTED_LIST);
    xlog_rollout(filename, DDFS_CORRUPTED_BACKUP_COUNT);
    te_corrupted_stream=fopen(filename, "w");
    te_fix_file=1;
    te_hash_counter=0;

    DDFS_LOG(LOG_INFO, "Fix files.\n");
    start=now();
    res=nftw(ddfs->rdir, ddfs_fsck_tree_explore, 10, FTW_MOUNT | FTW_PHYS);
    end=now();
    fclose(te_corrupted_stream);
    DDFS_LOG(LOG_INFO, "Calculated %lld hashes.\n", te_hash_counter);

    if (res)
    {
        res=1;
        DDFS_LOG(LOG_ERR, "nftw was unable to fix all files, correct and check filesystem again !\n");
    }
    else
    {
        DDFS_LOG(LOG_INFO, "Fixed:%lld  Corrupted:%lld  Total:%lld files in %.1fs.\n", te_fix_file-1, te_corrupted_count, te_file_count, (end-start)*1.0/NOW_PER_SEC);
        //
        // Clean up the extra nodes
        //
        long long int deleted=ddfs_fsck_remove_useless_node(&ddfs->ba_usedblocks);
        DDFS_LOG(LOG_INFO, "Deleted %lld useless nodes.\n", deleted);
        bit_array_count(&ddfs->ba_usedblocks, &block_count, &block_free);
        DDFS_LOG(LOG_INFO, "blocks in use: %lld   blocks free: %lld.\n", block_count, block_free);
    }
    return res;
}

/*
 * Remove node of the index not in ba_list
 */
long long int ddfs_fsck_cleanup_index_from_extra_blocks(struct bit_array *ba_list)
{
	long long int node_deleted=0;
    nodeidx node_idx;
    unsigned char* node=ddfs->nodes;
    for (node_idx=0; node_idx<ddfs->c_node_count; node_idx++)
    {
    	blockaddr addr=ddfs_get_node_addr(node);
        if (addr!=0 && !bit_array_get(ba_list, addr))
		{
			node_delete(node_idx);
			node_deleted++;
		}
        node+=ddfs->c_node_size;
    }
    return node_deleted;
}

/* Fix errors using the following logic:
 - Fix the index
 - Fix the files to match the index
*/

int ddfs_fsck(int relaxed, int verbose, int progress)
{
    int res;
    long long int errors;
    long long int start, end;
    long long int s, u;
    struct bit_array ba_found_in_files;
    struct bit_array ba_found_in_nodes;
    struct bit_array ba_suspect_need_rehash;
    struct bit_array ba_backup;

    // Create some bit arrays to keep track of things during the check
    bit_array_init(&ba_found_in_files, ddfs->c_block_count, 0x0);
    bit_array_init(&ba_found_in_nodes, ddfs->c_block_count, 0x0);
    bit_array_init(&ba_suspect_need_rehash, ddfs->c_block_count, 0x0);
    bit_array_init(&ba_backup, ddfs->c_block_count, 0x0);

    // Reset the suspect need rehash array
    bit_array_reset(&ba_suspect_need_rehash, 0);

    //
    // be sure that nodes in the index are well ordered and are valid
    // fix or remove wrong node
    // This don't remove duplicate hash
    //
    errors=ddfs_fsck_repair_node_order(&ba_suspect_need_rehash, verbose);
    DDFS_LOG(LOG_INFO, "Check and repair node order in index: fixed %lld errors.\n", errors);

    //
    // read files to retrieve block numbers in use
    //
    te_file_count=te_addr_count=te_frag_count=te_corrupted_count=0;
    te_verbose=verbose;
    te_progress=te_start=now();

    if (!progress) te_progress=0;

    bit_array_reset(&ba_found_in_files, 0);
    bit_array_set(&ba_found_in_files, 0);  // block ZERO is always used
    bit_array_set(&ba_found_in_files, 1);  // block    1 is always used
    te_usedblocks=&ba_found_in_files;

    DDFS_LOG(LOG_INFO, "Search files to find all used blocks.\n");
    start=now();
    res=nftw(ddfs->rdir, ddfs_fsck_tree_find_usedblocks, 10, FTW_MOUNT | FTW_PHYS);
    end=now();
    if (res)
    {
        DDFS_LOG(LOG_ERR, "nftw is unable to read all files, correct and check filesystem again !\n");
        res=1;
        goto END;
    }
    bit_array_count(&ba_found_in_files, &s, &u);
    DDFS_LOG(LOG_INFO, "Read %lld files in %.1fs.\n", te_file_count, (end-start)*1.0/NOW_PER_SEC);
    DDFS_LOG(LOG_INFO, "%lld blocks used in files.\n", s);
//    DDFS_LOG(LOG_INFO, "%lld more suspect blocks.\n", te_block_counter);
//    bit_array_count(&ba_rehash, &s, &u);
//    DDFS_LOG(LOG_INFO, "%lld blocks must be re-hashed and added to the index.\n", s);

    // Get a list of all blocks referenced by Index
    // ! ba_suspect_need_rehash will be extended
    bit_array_reset(&ba_found_in_nodes, 0);
    errors=ddfs_fsck_node_accounting(&ba_found_in_nodes, &ba_suspect_need_rehash);
    DDFS_LOG(LOG_INFO, "Found %lld duplicate addresses in index.\n", errors);


    // I add all blocks that where recently allocated to ba_suspect_need_rehash
    // (since last backup of ba_used_block) to be sure these block have been
    // written to disk with the right data.

    if (ddfs->auto_fsck)
    {
        if (ddfs_load_usedblocks(&ba_backup)==0)
        {
            long long int suspect_count1, suspect_count2, suspect_count3, u;

	    // Save the current count of bits set
            bit_array_count(&ba_suspect_need_rehash, &suspect_count1, &u);

	    // Merge the difference between the current used block list and saved list with the suspect list
            bit_array_plus_diff(&ba_suspect_need_rehash, &ba_found_in_files, &ba_backup);
            bit_array_count(&ba_suspect_need_rehash, &suspect_count2, &u);
            DDFS_LOG(LOG_INFO, "Check also last recently added blocks: %lld.\n", suspect_count2-suspect_count1);

	    // Merge the difference between the current used block list and index blocks with the suspect list
            bit_array_plus_diff(&ba_suspect_need_rehash, &ba_found_in_files, &ba_found_in_nodes);
            bit_array_count(&ba_suspect_need_rehash, &suspect_count3, &u);
            DDFS_LOG(LOG_INFO, "Check also blocks in files not in nodes: %lld.\n", suspect_count3-suspect_count2);
        }
    }
    else
    {
    	DDFS_LOG(LOG_INFO, "filesystem cleanly shut down: skip 'last recently added' blocks check\n");
    }

    // I will re-hash all suspect blocks and update the index
    // because node_add(na_unique_hash) remove all duplicate the index should be clean
    bit_array_count(&ba_suspect_need_rehash, &s, &u);
    DDFS_LOG(LOG_INFO, "Re-hash all %lld suspect blocks.\n", s);
    errors=ddfs_update_index_from_blocks(&ba_suspect_need_rehash, verbose, progress);
    DDFS_LOG(LOG_INFO, "Re-hash errors: %lld\n", errors);

    // The index is now accurate (in the correct order, with no duplicates and
    // all referenced blocks indexed

    // But the index could still contains unreferenced block
    // And the files must be updated with the index references to the blocks.

    //
    // update files regarding new index
    //
    res=ddfs_fsck_fix_files_and_usedblocks(verbose, progress);

    //
    // Now cleanup the index to match ba_used_block
    //
//    ddfs_fsck_cleanup_index_from_extra_blocks(&ddfs->ba_usedblocks);

    //
    // Update used_block list backup
    //
    ddfs_save_usedblocks();


    // blocks in ba_suspect_need_rehash are referenced twice or more and need to be verified
    bit_array_set(&ba_found_in_nodes, 0);  // block ZERO and 1
    bit_array_set(&ba_found_in_nodes, 1);  // must be referenced by Index
    bit_array_unset(&ba_suspect_need_rehash, 0);  // block ZERO and 1
    bit_array_unset(&ba_suspect_need_rehash, 1);  // cannot be suspected

    bit_array_count(&ba_found_in_nodes, &s, &u);
    DDFS_LOG(LOG_INFO, "%lld blocks used in nodes.\n", s);
    bit_array_count(&ba_suspect_need_rehash, &s, &u);
    DDFS_LOG(LOG_INFO, "Resolve Index conflicts and re-hash %lld suspect blocks.\n", s);
    errors=ddfs_fsck_check_node_hash(&ba_suspect_need_rehash, verbose);
    DDFS_LOG(LOG_INFO, "%lld nodes fixed.\n", errors);


END:
    bit_array_release(&ba_found_in_files);
    bit_array_release(&ba_found_in_nodes);
    bit_array_release(&ba_suspect_need_rehash);
    bit_array_release(&ba_backup);

    return res;
}
