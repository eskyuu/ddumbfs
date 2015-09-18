/*
 * ddfschkrep.h
 *
 *  Created on: Jun 4, 2011
 *      Author: asx
 */

#ifndef DDFSCHKREP_H_
#define DDFSCHKREP_H_

#include <stdio.h>
#include <ftw.h>

#include "ddfslib.h"

int ddfs_fsck(int relaxed, int verbose, int progress);

extern long long int te_file_count;
extern long long int te_addr_count;
extern long long int te_frag_count;
extern long long int te_corrupted_count;
extern long long int te_errors;
extern long long int te_fix_file;
extern long long int te_lost_block;

extern int te_verbose;
extern long long int te_progress;
extern long long int te_start;
extern int te_update_index_relaxed;
extern FILE *te_corrupted_stream;

extern struct bit_array *te_block_found_in_files;
extern struct bit_array *te_update_index;

int ddfs_fsck_tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf);

int ddfs_fsck_fix_files_and_usedblocks(int verbose, int progress);

void ddfs_chk_preload_node(nodeidx node_idx, int num_nodes);

#endif /* DDFSCHKREP_H_ */
