/**
 * @queryddumbfs.c
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
 * queryddumbfs a tool to query and debug a ddumbfs volume
 *
 */

#define _XOPEN_SOURCE 600
#define _LARGEFILE64_SOURCE

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <assert.h>
#include <time.h>
#include <ftw.h>

#include "ddfslib.h"

int verbose_flag=0;

static struct option long_options[] =
{
       {"help",                 no_argument,       0, 'h'},
       {"verbose",              no_argument,       0, 'v'},
       {"force",                no_argument,       0, 'f'},
       {"lock_index",           no_argument,       0, 'l'},

       {"block",                required_argument, 0, 'b'},
       {"hash",                 required_argument, 0, 'H'},
       {"node",                 required_argument, 0, 'n'},
       {0, 0, 0, 0}
};

void usage()
{
    printf("Usage: queryddumbfs [options] parent-directory\n"
            "\n    queryddumbfs a tool to query and debug a ddumbfs volume\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         be more verbose and display progress\n"
            "  -f, --force           run even if filesystem is already mounted\n"
            "  -l, --lock_index      lock index into memory (increase speed)\n"
            "\n"
            "  -n, --node=ID         get info from node, related address and hash\n"
            "  -b, --block=ID        get info about block\n"
            "  -h, --hash=HASH       get info about hash\n"
    );
}

void print_hash(FILE *file, unsigned char *hash)
{
    int i;
    for (i=0; i<ddfs->c_hash_size; i++) fprintf(file, "%.2x", hash[i]);
}

unsigned char *te_bhash;
blockaddr te_baddr;
int te_count;

int tree_explore(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{   // called by nftw to walk the tree
    int res, len;
    unsigned char node[NODE_SIZE];
    unsigned char *hash=node+ddfs->c_addr_size;

    if (typeflag!=FTW_F || ! S_ISREG(sb->st_mode)) return 0;
    if (0==strncmp(fpath+ddfs->rdir_len, SPECIAL_DIR, ddfs->special_dir_len))
    {   // skip SPECIAL_DIR
        return 0;
    }

    FILE *file=fopen(fpath, "r");  // FILE* are faster here
    if (file==NULL)
    {
        fprintf(stderr, "tree_explore cannot open: %s (%s)\n", fpath, strerror(errno));
        return 1;
    }

    // be careful, file_header_set_conv return c_file_header_size for empty file
    uint64_t file_size;
    len=file_header_set_conv(fileno(file), &file_size);
    if (len==-1)
    {
        fprintf(stderr, "tree_explore cannot read header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        fprintf(stderr, "tree_explore invalid ddumbfs header, skip file: %s\n", fpath);
        fclose(file);
        return 0;
    }

    res=fseek(file, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        fprintf(stderr, "tree_explore cannot seek after header: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    long long int pos=0;
    len=fread(node, 1, ddfs->c_node_size, file);
    while (len==ddfs->c_node_size)
    {
        blockaddr addr=ddfs_get_node_addr(node);

        if (te_baddr==addr || (te_bhash && 0==memcmp(te_bhash, hash, ddfs->c_hash_size)))
        {
            printf("file: %8lld  addr=%8lld  ", pos, addr);
            print_hash(stdout, hash);
            printf(" %s\n", fpath);
            te_count++;
        }
        len=fread(node, 1, ddfs->c_node_size, file);
        pos++;
    }

    if (len==-1)
    {
        fprintf(stderr, "tree_explore cannot read: %s (%s)\n", fpath, strerror(errno));
        fclose(file);
        return 1;
    }

    if (len && len!=ddfs->c_node_size)
    {
        fprintf(stderr, "tree_explore read incomplete node (%d/%d): %s\n", len, ddfs->c_node_size, fpath);
    }
    fclose(file);
    return 0;
}

int search_files(unsigned char *bhash, blockaddr baddr)
{
    te_bhash=bhash;
    te_baddr=baddr;
    te_count=0;
    nftw(ddfs->rdir, tree_explore, 10, FTW_MOUNT | FTW_PHYS);
    return te_count;
}

int search_index(unsigned char *bhash, blockaddr baddr)
{
    int count=0;
    nodeidx node_idx=0;
    while (node_idx<ddfs->c_node_count)
    {
        unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
        blockaddr addr=ddfs_get_node_addr(node);
        unsigned char *hash=node+ddfs->c_addr_size;

        if (baddr==addr || (bhash && 0==memcmp(bhash, hash, ddfs->c_hash_size)))
        {
            printf("index:%8lld  addr=%8lld  ", node_idx, addr);
            print_hash(stdout, hash);
            printf("\n");
            count++;
        }
        node_idx++;
    }
    return count;
}

int query_block(blockaddr baddr)
{
	int len;
    unsigned char bhash[HASH_SIZE];
    printf("block:          addr=%8lld", baddr);
    len=ddfs_read_full_block(baddr, ddfs->aux_buffer);
    if (len!=ddfs->c_block_size)
    {
    	if (len==-1) printf("  %s ", strerror(errno));
    	else printf("  short read: %d bytes. ", len);
    }
    else
    {
		ddfs_hash(ddfs->aux_buffer, bhash);
		printf("  ");
		print_hash(stdout, bhash);
		printf(" ");
    }
    if (bit_array_get(&ddfs->ba_usedblocks, baddr)) printf("is allocated\n");
    else printf("is unallocated\n");
    if (len!=ddfs->c_block_size)
    {
		int count=search_index(bhash, baddr);
		if (count==0) printf("baddr and hash not found in index\n");
		count=search_files(bhash, baddr);
		if (count==0) printf("baddr and hash not found in files\n");
    }
    return 0;
}

int query_hash(unsigned char *hash)
{
    printf("hash: %8lld                 ", ddfs_hash2idx(hash));
    print_hash(stdout, hash);
    printf("\n");
    int count=search_index(hash, -1);
    if (count==0) printf("hash not found in index\n");
    count=search_files(hash, -1);
    if (count==0) printf("hash not found in files\n");
    return 0;
}

int query_node(nodeidx node_idx)
{
    unsigned char *node=ddfs->nodes+(node_idx*ddfs->c_node_size);
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr=ddfs_get_node_addr(node);
    fprintf(stderr, "node: %8lld  addr=%8lld  ", node_idx, addr);
    print_hash(stdout, hash);

    // display next free/used node
    nodeidx cidx=node_idx+1;
    while (cidx<ddfs->c_node_count)
    {
        node=ddfs->nodes+(cidx*ddfs->c_node_size);
        blockaddr baddr=ddfs_get_node_addr(node);
        // unsigned char *bhash=node+ddfs->c_addr_size;

        if (addr==0 && baddr!=0)
        {
            printf(" --> next used node is %lld\n", cidx);
            break;
        }
        if (addr!=0 && baddr==0)
        {
            printf(" --> next free node is %lld\n", cidx);
            break;
        }
        cidx++;
    }

    if (addr!=0)
    {
        unsigned char bhash[HASH_SIZE];
        blockaddr baddr=addr;
        if (addr!=1)
        {
            printf("block:          addr=%8lld", addr);
            int len=ddfs_read_full_block(addr, ddfs->aux_buffer);
            if (len!=ddfs->c_block_size)
            {
            	if (len==-1) printf("  %s ", strerror(errno));
            	else printf("  short read: %d bytes. ", len);
            }
            else
            {
				ddfs_hash(ddfs->aux_buffer, bhash);
				printf("  ");
				print_hash(stdout, bhash);
				printf(" ");
            }
            if (bit_array_get(&ddfs->ba_usedblocks, addr)) printf("is allocated\n");
            else printf("is unallocated\n");
        }
        else
        {
            baddr=-1;
        }
        int count=search_index(hash, baddr);
        if (count==0) printf("addr and hash not found in index\n");
        count=search_files(hash, baddr);
        if (count==0) printf("addr and hash not found in files\n");
    }

    return 0;
}


void invalid_argument(int option, char *value, int option_index)
{
    if (option_index) fprintf(stderr, "invalid option --%s %s\n", long_options[option_index].name, value);
    else fprintf(stderr, "invalid option -%c %s\n", option, value);
    exit(1);
}

struct ddfs_ctx ddfsctx;

int main(int argc, char *argv[])
{

    int force_flag=0;
    int lock_index_flag=0;
    int c;
    int res=0;
    nodeidx node_idx=-1;
    blockaddr baddr=-1;
    unsigned char hashbuf[HASH_SIZE];
    unsigned char *hash=NULL;

    ddfs=&ddfsctx;
    while (1)
    {
        // getopt_long stores the option index here.
        int option_index=0;
        char *optparse;

        c=getopt_long(argc, argv, "hvflb:n:H:", long_options, &option_index);

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

            case 'f':
                force_flag=1;
                break;

            case 'l':
                lock_index_flag=1;
                break;

            case 'b':
                baddr=strtoll(optarg, &optparse, 10);
                break;
            case 'n':
                node_idx=strtoll(optarg, &optparse, 10);
                break;
            case 'H':
                memset(hashbuf, '\0', HASH_SIZE);
                char *p;
                int i;
                for (p=optarg, i=0; *p!='\0'; p++, i++)
                {
                    int v=0;
                    if ('0'<=*p && *p<='9') v=*p-'0';
                    else if ('a'<=*p && *p<='f') v=*p-'a'+10;
                    else if ('A'<=*p && *p<='F') v=*p-'A'+10;
                    else invalid_argument(c, optarg, option_index);

                    if (i%2==0) hashbuf[i/2]=v<<4; // HI
                    else hashbuf[i/2]=hashbuf[i/2] | v; // LOW

                }
                hash=hashbuf;
                break;

            case '?':
                // getopt_long already printed an error message.
                break;

            default:
                abort();
        }
    }

    char pdir[FILENAME_MAX];

    if (ddfs_find_parent(argv[optind], pdir))
    {
        fprintf(stderr, "Not a valid ddumbfs filesystem parent directory: %s\n", argv[optind]);
        return 1;
    }

    if (is_mounted(pdir))
    {
        if (force_flag)
        {
            fprintf(stderr, "Warning filesystem is already mounted, use -f to force: %s", pdir);
        }
        else
        {
            fprintf(stderr, "filesystem already mounted: %s\n", pdir);
            return 1;
        }
    }

    if (ddfs_loadcfg(pdir, NULL))
    {
        return 1;
    }

    // NO_DIRECTIO to be able to read/write anywhere in the block file
    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, DDFS_NODIRECTIO, lock_index_flag, NULL))
    {
        return 1;
    }

    if (node_idx!=-1)
    {
        if (node_idx>ddfs->c_node_count)
        {
            fprintf(stderr, "node_idx %lld >= last node %lld\n", node_idx, ddfs->c_node_count);
            return 1;
        }
        query_node(node_idx);
    }

    if (baddr!=-1)
    {
        query_block(baddr);
    }


    if (hash)
    {
        query_hash(hash);
    }

    ddfs_close();
    return res;

}
