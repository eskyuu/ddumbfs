/**
 * @migrateddumbfs.c
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
 * migrateddumbfs allows to resize a ddumbfs filesytem by migrating
 * data data between thow fs of different size
 *
 */

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ftw.h>
#include <fcntl.h>
#include <assert.h>

#include "ddfslib.h"

#define MAX_SIZE 0xFFFFFFFFFFFFFFFFll;

int verbose_flag=0;
int check_integrity=0;
int lock_index_flag=0;
int force_flag=0;
int progress_flag=0;

static struct option long_options[] =
{
       /* These options set a flag. */
       {"help",          no_argument,       0, 'h'},
       {"verbose",       no_argument,       0, 'v'},
       {"force",         no_argument,       0, 'f'},
       {"progress",      no_argument,       0, 'p'},

       {0, 0, 0, 0}
};

void usage()
{
    printf("Usage: migrateddumbfs [options] parent_source parent_destination\n"
            "\n    copy from and to an offline ddumbfs filesystem\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         display addresses and hashes from file (download only)\n"
            "  -p, --progress        display progression\n"
            "  -f, --force           download a file even with a corrupted MAGIC in the header\n"
            "\n  migrate nodes of the index and the files from source to destination\n"
            "  directory. Destination must exists. Blockfile will not be migrated. "
    		"  Its format don't need to be changed for a filesystem resize. You"
            "  must manually edit ddfs.cfg file to link the blockfile to the old one.\n"
            "\nSamples:\n"
            "  migratedumbfs /data/ddumbfs /data/ddumbfs.new\n"
    );
}

int ddfs_index_migrate(struct ddfs_ctx *src, struct ddfs_ctx *dst)
{
    nodeidx node_idx, dst_idx;

    long long int start=now();
    long long int last=start;

    ddfs=src;
    for (node_idx=dst_idx=0; node_idx<src->c_node_count; node_idx++)
    {
        unsigned char *node=src->nodes+(node_idx*src->c_node_size);
        unsigned char *hash=node+src->c_addr_size;

        blockaddr addr=ddfs_get_node_addr(node);

        if (progress_flag && node_idx%10000==0)
        {	// display progress
            long long int end=now();
            if (end-last>NOW_PER_SEC)
            {
                last=end;
                printf("migrating index nodes: %.1f%% in %llds\r", node_idx*100.0/src->c_node_count, (end-start)/NOW_PER_SEC);
                fflush(stdout);
            }
        }

        if (addr!=0)
        {
        	ddfs=dst;
            nodeidx cidx=ddfs_hash2idx(hash);
            if (cidx<dst_idx) cidx=dst_idx;
            dst_idx=cidx+1;

            unsigned char *node_dst=dst->nodes+(cidx*dst->c_node_size);
            unsigned char *hash_dst=node_dst+dst->c_addr_size;

			ddfs_convert_addr(addr, node_dst);
			memcpy(hash_dst, hash, dst->c_hash_size);
        }
    }

    // copy ba_usedblocks
    printf("migrate bit list\n");
    node_idx=bit_array_search_first_set(&src->ba_usedblocks, 0);
    while (node_idx>=0)
    {
        bit_array_set(&dst->ba_usedblocks, node_idx);
        node_idx=bit_array_search_first_set(&src->ba_usedblocks, node_idx+1);
    }

    return 0;
}

struct ddfs_ctx ddfs_src;
struct ddfs_ctx ddfs_dst;
long long int ddfs_tree_migrate_te_file_count;
long long int ddfs_tree_migrate_te_errors;
long long int ddfs_tree_migrate_te_last;

int ddfs_tree_migrate(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    int res, len;
    char filename_dst[FILENAME_MAX];
    unsigned char node_src[NODE_SIZE];
    unsigned char node_dst[NODE_SIZE];
    unsigned char *hash_src=node_src+ddfs_src.c_addr_size;
    unsigned char *hash_dst=node_dst+ddfs_dst.c_addr_size;
    FILE *file_dst=NULL;
    FILE *file_src=NULL;
    uint64_t size;

    if (typeflag!=FTW_F || ! S_ISREG(sb->st_mode)) return 0;

    if (0==strncmp(fpath+ddfs_src.rdir_len, SPECIAL_DIR, ddfs_src.special_dir_len))
    {   // skip SPECIAL_DIR
        return 0;
    }

    ddfs_tree_migrate_te_file_count++;
    if (verbose_flag)
    {
        long long int end=now();
        if (end-ddfs_tree_migrate_te_last>NOW_PER_SEC)
        {
            ddfs_tree_migrate_te_last=end;
            printf("Read file: %lld\r", ddfs_tree_migrate_te_file_count);
            fflush(stdout);
        }
    }

    file_src=fopen(fpath, "rb");
    if (file_src==NULL)
    {
        ddfs_tree_migrate_te_errors++;
        printf("cannot open: %s (%s)\n", fpath, strerror(errno));
        return 0;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    len=file_header_set_conv(fileno(file_src), &size);
    if (len!=ddfs_src.c_file_header_size)
    {
        ddfs_tree_migrate_te_errors++;
        if (verbose_flag)
        {
            if (len==-1) printf("cannot read: %s (%s)\n", fpath, strerror(errno));
            else printf("corrupted header: %s\n", fpath);
        }
        goto END;
    }

    res=fseek(file_src, ddfs_src.c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        ddfs_tree_migrate_te_errors++;
        printf("cannot seek at first block: %s (%s)\n", fpath, strerror(errno));
        goto END;
    }

    // Destination
    snprintf(filename_dst, FILENAME_MAX, "%s/%s", ddfs_dst.rdir, fpath+ddfs_src.rdir_len+1);

//    fprintf(stderr, "src: %s\n", fpath);
//    fprintf(stderr, "dst: %s\n", filename_dst);

    file_dst=fopen(filename_dst, "wb");
    if (file_dst==NULL)
    {
        ddfs_tree_migrate_te_errors++;
        printf("cannot open: %s (%s)\n", filename_dst, strerror(errno));
        goto END;
    }

    res=fseek(file_dst, ddfs_dst.c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        ddfs_tree_migrate_te_errors++;
        printf("cannot seek at first block: %s (%s)\n", filename_dst, strerror(errno));
        goto END;
    }


    long long int i=0;

    len=fread(node_src, 1, ddfs_src.c_node_size, file_src);
    while (len==ddfs_src.c_node_size)
    {
		i++;
    	if (ddfs_src.c_node_size==ddfs_dst.c_node_size)
    	{
			res=fwrite(node_src, 1, ddfs_dst.c_node_size, file_dst);
    	}
    	else
    	{
    		ddfs=&ddfs_src;
			blockaddr addr=ddfs_get_node_addr(node_src);
    		ddfs=&ddfs_dst;
			ddfs_convert_addr(addr, node_dst);
			memcpy(hash_dst, hash_src, ddfs_src.c_hash_size);
			res=fwrite(node_dst, 1, ddfs_dst.c_node_size, file_dst);
			if (res!=ddfs->c_node_size)
			{
		        ddfs_tree_migrate_te_errors++;
				fprintf(stderr, "error writing file: %s\n", filename_dst);
		        goto END;
			}
    		ddfs=&ddfs_src;
    	}

		len=fread(node_src, 1, ddfs_src.c_node_size, file_src);
    }

    if (len!=0)
    {
        ddfs_tree_migrate_te_errors++;
		if (len==-1) printf("cannot read file: %s (%s)\n", fpath, strerror(errno));
		else printf("incomplete node in file: %s %d\n", fpath, len);
        goto END;
    }

    if (i!=(size+ddfs_src.c_block_size-1)/ddfs_src.c_block_size)
    {   // size mismatch
        ddfs_tree_migrate_te_errors++;
        printf("size mismatch in file: %s\n", fpath);
        goto END;
    }

    // write size
    ddfs=&ddfs_dst;
    fflush(file_dst);
    len=file_header_get_conv(fileno(file_dst), size);
    if (len==-1)
    {
        perror(filename_dst);
        goto END;
    }
    else if (len!=ddfs_dst.c_file_header_size)
    {
        fprintf(stderr, "cannot write file header\n");
        goto END;
    }

END:
	ddfs=&ddfs_src;
    if (file_src!=NULL) fclose(file_src);
    if (file_dst!=NULL) fclose(file_dst);
    return 0;
}


struct ddfs_ctx ddfs_src;
struct ddfs_ctx ddfs_dst;

int main(int argc, char *argv[])
{
    int res=0;
    int c;
    long long int start, end;

    while (1)
    {
        // getopt_long stores the option index here.
        int option_index = 0;

        c=getopt_long(argc, argv, "hvfp", long_options, &option_index);

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

            case 'p':
                progress_flag=1;
                break;

            case 'f':
                force_flag=1;
                break;

            case '?':
                // getopt_long already printed an error message.
                break;

            default:
                abort();
        }
    }

    if (optind+1>=argc)
    {
        fprintf(stderr, "missing source or destination parent directory %d %d!\n", optind, argc );
        return 1;
    }

    char pdir_src[FILENAME_MAX];
    char pdir_dst[FILENAME_MAX];

    if (ddfs_find_parent(argv[optind], pdir_src)!=0)
    {
        fprintf(stderr, "Source directory not found !\n");
        return 1;
    }

    if (ddfs_find_parent(argv[optind+1], pdir_dst)!=0)
    {
        fprintf(stderr, "Destination directory not found: %s\n", argv[optind+1]);
        return 1;
    }

    if (is_mounted(pdir_src))
    {
        fprintf(stderr, "source filesystem already mounted: %s\n", pdir_src);
        return 1;
    }

    if (is_mounted(pdir_dst))
    {
        fprintf(stderr, "destination filesystem already mounted: %s\n", pdir_dst);
        return 1;
    }

    ddfs=&ddfs_dst;
    printf("mounting destination: %s\n", pdir_dst);
    if (ddfs_loadcfg(pdir_dst, stderr))
    {
        return 1;
    }

    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, DDFS_DIRECTIOAUTO, DDFS_NOLOCKINDEX, NULL))
    {
        return 1;
    }

    ddfs=&ddfs_src;
    printf("mounting source: %s\n", pdir_src);
    if (ddfs_loadcfg(pdir_src, NULL))
    {
        return 1;
    }

    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, DDFS_DIRECTIOAUTO, DDFS_NOLOCKINDEX, NULL))
    {
        return 1;
    }

    printf("check source and destination compatibility\n");
    if (ddfs_src.c_block_size!=ddfs_dst.c_block_size)
    {
        printf("blocksize mismatch !\n");
        res=1;
        goto END;
    }

    if (0!=strcmp(ddfs_src.c_hash, ddfs_dst.c_hash))
    {
        printf("HASH mismatch !\n");
        res=1;
        goto END;
    }
    // compare used block
    long long int src_block_count, src_block_free;
    long long int dst_block_count, dst_block_free;

    bit_array_count(&ddfs_src.ba_usedblocks, &src_block_count, &src_block_free);
    bit_array_count(&ddfs_dst.ba_usedblocks, &dst_block_count, &dst_block_free);

    if (dst_block_count!=DDFS_LAST_RESERVED_BLOCK+1)
    {
        printf("Destination must be empty, please format destination !\n");
        res=1;
        goto END;
    }

    if (dst_block_free<src_block_count)
    {
        printf("Not enough free blocks in destination %lld < %lld!\n", dst_block_free, src_block_count);
        res=1;
        goto END;
    }

    printf("== migrating index\n");
    ddfs_index_migrate(&ddfs_src, &ddfs_dst);

    printf("migrating free block list\n");


    printf("== migrating file\n");
    start=now();
    ddfs_tree_migrate_te_file_count=0;
    ddfs_tree_migrate_te_last=start;
    res=nftw(ddfs_src.rdir, ddfs_tree_migrate, 10, FTW_MOUNT | FTW_PHYS);
    end=now();
    printf("== Migrated %lld files in %.1fs\n", ddfs_tree_migrate_te_file_count, (end-start)*1.0/NOW_PER_SEC);


    printf("don't forget to handle the blockfile manually\n");
END:
	ddfs=&ddfs_src;
    ddfs_close();
    ddfs=&ddfs_dst;
    ddfs_close();

    return res;

}
