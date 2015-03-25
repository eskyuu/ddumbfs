/**
 * @alterddumbfs.c
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
 * alterddumbfs alter a working ddumbfs to test fsckddumbfs
 *
 */

#define _XOPEN_SOURCE 500
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

#include "ddfslib.h"

int verbose_flag=0;
int rebuild_flag=0;
int all_flag=0;

static struct option long_options[] =
{
       {"help",                 no_argument,       0, 'h'},
       {"verbose",              no_argument,       0, 'v'},
       {"lock_index",           no_argument,       0, 'l'},
       {"unexpected_shutdown",  no_argument,       0, 'u'},

       {"funny-files",          no_argument, 0,       'f'},

       {"index",                required_argument, 0, 'i'},
       {"swap-consecutive",     required_argument, 0, 's'},
       {"swap-random",          required_argument, 0, 'S'},
       {"reset-node",           required_argument, 0, 'r'},
       {"duplicate-node",       required_argument, 0, 'd'},
       {"duplicate-inplace",    required_argument, 0, 'p'},
       {"swap-addr",            required_argument, 0, 'a'},
       {"corrupt-node",         required_argument, 0, 'c'},
       {"random-seed",          required_argument, 0, 'n'},
       {"index-ops",            required_argument, 0, 'I'},
       {"block-ops",            required_argument, 0, 'B'},
       {"shake-file",           required_argument, 0, 'k'},
       {0, 0, 0, 0}
};

void usage()
{
    printf("Usage: alterddumbfs [options] parent-directory\n"
            "\n    create anomalies in an offline ddumbfs filesystem\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         be more verbose and display progress\n"
            "  -l, --lock_index      lock index into memory (increase speed)\n"
            "  -f, --funny-files     create some funny file\n"
            "  -n, --random-seed=VAL initialize random seed\n"
            "  -i, --index=NUM       alter index\n"
            "  -s, --swap-consecutive=NUM\n"
            "                        swap non empty consecutive nodes in the index\n"
            "  -S, --swap-random=NUM\n"
            "                        swap non empty random nodes in the index\n"
            "  -r, --reset-node=NUM\n"
            "                        reset to zero non empty nodes in the index\n"
            "  -d, --duplicate-node=NUM\n"
            "                        duplicate non empty nodes in empty random place\n"
            "  -p, --duplicate-inplace=NUM\n"
            "                        duplicate non empty nodes just after itself\n"
            "  -a, --swap-addr=NUM\n"
            "                        exchange addresses of two non empty nodes in the index\n"
            "  -c, --corrupt-node=NUM\n"
            "                        write one random byte anywhere in the index\n"
            "  -u, --unexpected-shutdown\n"
            "                        simulate a crash or a power-cut\n"
            "  -I, --index-ops=OPS  alter index file\n"
            "                        OPS=delete:   delete the file\n"
            "                        OPS=magic:    put MAGIC at blank\n"
            "                        OPS=truncate: truncate the file at mid size\n"
            "                        OPS=empty:    truncate file\n"
            "  -B, --block-ops=OPS  alter block file\n"
            "                        see --index-ops for possible operations\n"
            "  -k, --shake-file=FILE alter a file, replace addresses by randoms\n"
    );
}

void random_block(char *block)
{
    int j;
    unsigned long int *p;
    for (j=0, p=(unsigned long int *)block; j<ddfs->c_block_size; p++, j+=sizeof(unsigned long int)) *p=rand();
}

int funny_file(char *name, blockaddr addr, unsigned char *hash, int size, int extra)
{
    char filename[FILENAME_MAX];
    unsigned char node[NODE_SIZE];
    int len;

    // create a file with one invalid address
    sprintf(filename, "%s/%s", ddfs->rdir, name);

    int fd=open(filename, O_RDWR | O_TRUNC | O_CREAT, 0600);
    if (fd==-1)
    {
        perror(filename);
        return 1;
    }

    len=file_header_get_conv(fd, size);
    if (len!=ddfs->c_file_header_size)
    {
        fprintf(stderr, "error writing %s\n", filename);
        return 1;
    }

    len=lseek(fd, ddfs->c_file_header_size, SEEK_SET);
    if (len!=ddfs->c_file_header_size)
    {
        perror(filename);
        return 1;
    }

    // 0 bad hash=AAA addr=1
    memcpy(node+ddfs->c_addr_size, hash, ddfs->c_hash_size);
    ddfs_convert_addr(addr, node);
    len=write(fd, node, ddfs->c_node_size);
    while (extra>0 && len!=0)
    {
        len=extra;
        if (len>ddfs->c_node_size) len=ddfs->c_node_size;
        len=write(fd, node, len);
        extra-=len;
    }
    close(fd);
    return len!=ddfs->c_node_size;
}

int funny_files()
{
    unsigned char hash[HASH_SIZE];
    blockaddr used_addr=3;
    blockaddr addr;
    int i;

    printf("Disturb.\n");

    // bad: hash=AAA addr=1
    memset(hash, 'A', ddfs->c_hash_size);
    funny_file("ff_already_corrupted", 1, hash, ddfs->c_block_size, 0);

    // bad: hash=AAA addr=<used>
    memset(hash, 'A', ddfs->c_hash_size);
    funny_file("ff_block_not_found_1", used_addr, hash, ddfs->c_block_size, 0);

    // ok block=<random> hash=~ addr=~
    random_block(ddfs->aux_buffer);
    addr=ddfs_write_block(ddfs->aux_buffer, hash);
    funny_file("ff_ok_bb", addr, hash, ddfs->c_block_size, 0);

    // file with size in header too big
    funny_file("ff_size_in_header_too_big", addr, hash, ddfs->c_block_size+ddfs->c_block_size/2, 0);

    // file with size in header too small
    funny_file("ff_size_in_header_too_small", addr, hash, ddfs->c_block_size/2, 2*ddfs->c_node_size);

    // file with extra data at end
    funny_file("ff_extra_data", addr, hash, ddfs->c_block_size, ddfs->c_node_size/2);

    // recoverable idem BBBB addr=1
    funny_file("ff_recoverable_1", 1, hash, ddfs->c_block_size, 0);

    // recoverable idem BBBB addr=<used>
    funny_file("ff_recoverable_2", used_addr, hash, ddfs->c_block_size, 0);

    // bad hash=CCC addr=~
    memset(hash, 'C', ddfs->c_hash_size);
    funny_file("ff_block_not_found_2", addr, hash, ddfs->c_block_size, 0);

    // ok block zero with null hash
    funny_file("ff_ok_zero_null", 0, ddfs->null_block_hash, ddfs->c_block_size, 0);

    // ok block zero with ZERO hash
    funny_file("ff_ok_zero_zero", 0, ddfs->zero_block_hash, ddfs->c_block_size, 0);

    // bad block zero with bad hash
    memset(hash, 'D', ddfs->c_hash_size);
    funny_file("ff_bad_zero", 0, hash, ddfs->c_block_size, 0);


    // add some useless block
    for (i='W'; i<='Z'; i++)
    {
        memset(ddfs->aux_buffer, i, ddfs->c_block_size);
        addr=ddfs_write_block(ddfs->aux_buffer, hash);
    }

    char filename[FILENAME_MAX];
    int len;
    (void)len;

    // empty file
    sprintf(filename, "%s/%s", ddfs->rdir, "ff_empty");

    int fd=open(filename, O_RDWR | O_TRUNC | O_CREAT, 0600);
    if (fd==-1)
    {
        perror(filename);
    }
    else close(fd);

    // bad header short
    sprintf(filename, "%s/%s", ddfs->rdir, "ff_bad_header_short");

    fd=open(filename, O_RDWR | O_TRUNC | O_CREAT, 0600);
    if (fd==-1)
    {
        perror(filename);
    }
    else
    {
        len=write(fd, "wrong short header", 18);
        close(fd);
    }

    // bad header long
    sprintf(filename, "%s/%s", ddfs->rdir, "ff_bad_header_long");

    fd=open(filename, O_RDWR | O_TRUNC | O_CREAT, 0600);
    if (fd==-1)
    {
        perror(filename);
    }
    else
    {
        len=write(fd, filename, ddfs->c_file_header_size+ddfs->c_node_size);
        close(fd);
    }

    return 0;
}

void node_swap(nodeidx node1_idx, nodeidx node2_idx)
{
    unsigned char node_buf[NODE_SIZE];
    memcpy(node_buf, ddfs->nodes+(node1_idx*ddfs->c_node_size), ddfs->c_node_size);
    memcpy(ddfs->nodes+(node1_idx*ddfs->c_node_size), ddfs->nodes+(node2_idx*ddfs->c_node_size), ddfs->c_node_size);
    memcpy(ddfs->nodes+(node2_idx*ddfs->c_node_size), node_buf, ddfs->c_node_size);
}

void node_reset(nodeidx node1_idx)
{
    memset(ddfs->nodes+(node1_idx*ddfs->c_node_size), '\0', ddfs->c_node_size);
}

void node_cpy(nodeidx dst_idx, nodeidx src_idx)
{
    memcpy(ddfs->nodes+(dst_idx*ddfs->c_node_size), ddfs->nodes+(src_idx*ddfs->c_node_size), ddfs->c_node_size);
}

int swap_consecutive_non_empty_nodes()
{
    //
    // find two non empty consecutive nodes and swap them
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node_idx=random()%(ddfs->c_node_count-1);
        if (ddfs_get_node_addr(ddfs->nodes+(node_idx*ddfs->c_node_size)) && ddfs_get_node_addr(ddfs->nodes+((node_idx+1)*ddfs->c_node_size)))
        {
            node_swap(node_idx, node_idx+1);
            return 0;
        }
    }
    return 1;
}

int swap_random_non_empty_nodes()
{
    //
    // find two non empty nodes and swap them
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node1_idx=random()%(ddfs->c_node_count);
        nodeidx node2_idx=random()%(ddfs->c_node_count);
        if (ddfs_get_node_addr(ddfs->nodes+(node1_idx*ddfs->c_node_size)) && ddfs_get_node_addr(ddfs->nodes+(node2_idx*ddfs->c_node_size))
            //&& node1_idx>0 && ddfs_get_node_addr(ddfs->nodes+((node1_idx-1)*ddfs->c_node_size))==0 && node2_idx>0 && ddfs_get_node_addr(ddfs->nodes+((node2_idx-1)*ddfs->c_node_size))==0
            )
        {
            node_swap(node1_idx, node2_idx);
            return 0;
        }
    }
    return 1;
}

int reset_random_non_empty_nodes()
{
    //
    // reset a non empty node
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node1_idx=random()%(ddfs->c_node_count);
        if (ddfs_get_node_addr(ddfs->nodes+(node1_idx*ddfs->c_node_size)))
        {
            node_reset(node1_idx);
            return 0;
        }
    }
    return 1;
}

int corrupt_any_nodes()
{
    //
    // write a random byte anywhere in the index
    //
    nodeidx node1_idx=random()%(ddfs->c_node_count);
    int offset=random()%(ddfs->c_node_size);
    int value=random()%0xFF;

    ddfs->nodes[node1_idx*ddfs->c_node_size+offset]=value;
    return 1;
}

int duplicate_non_empty_node()
{
    //
    // copy one non empty node into one empty node
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node1_idx=random()%(ddfs->c_node_count);
        nodeidx node2_idx=random()%(ddfs->c_node_count);
        if (ddfs_get_node_addr(ddfs->nodes+(node1_idx*ddfs->c_node_size)) && 0==ddfs_get_node_addr(ddfs->nodes+(node2_idx*ddfs->c_node_size)))
        {
            node_cpy(node2_idx, node1_idx);
            return 0;
        }
    }
    return 1;
}

int duplicate_non_empty_node_inplace()
{
    //
    // duplicate one non empty node just after itself
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node_idx=random()%(ddfs->c_node_count);
        if (ddfs_get_node_addr(ddfs->nodes+(node_idx*ddfs->c_node_size)))
        {
            nodeidx free_node_idx=ddfs_search_free_node(node_idx, ddfs->c_node_count);
            if (free_node_idx>=0)
            {
                if (verbose_flag) printf("duplicate node in place %lld (addr=%lld)\n", node_idx, ddfs_get_node_addr(ddfs->nodes+(node_idx*ddfs->c_node_size)));
                memmove(ddfs->nodes+((node_idx+1)*ddfs->c_node_size), ddfs->nodes+(node_idx*ddfs->c_node_size), (free_node_idx-node_idx)*ddfs->c_node_size);
                return 0;
            }
        }
    }
    return 1;
}


int swap_node_addr()
{
    //
    // find two non empty nodes and swap block addresses
    //
    int i;
    for (i=0; i<10000; i++)
    {
        nodeidx node1_idx=random()%(ddfs->c_node_count);
        nodeidx node2_idx=random()%(ddfs->c_node_count);
        blockaddr addr1=ddfs_get_node_addr(ddfs->nodes+(node1_idx*ddfs->c_node_size));
        blockaddr addr2=ddfs_get_node_addr(ddfs->nodes+(node2_idx*ddfs->c_node_size));
        if (addr1 && addr2)
        {
            ddfs_convert_addr(addr1, ddfs->nodes+(node2_idx*ddfs->c_node_size));
            ddfs_convert_addr(addr2, ddfs->nodes+(node1_idx*ddfs->c_node_size));
            return 0;
        }
    }
    return 1;
}

int unexpected_shutdown()
{
    //
    // simulate an  unexpected shutdown, in the hope the recovery process will
    // repair the files that can be repaired
    // 1.

    unsigned char node[NODE_SIZE];
    unsigned char *hash=node+ddfs->c_addr_size;
    blockaddr addr;

    // save block now
    ddfs_save_usedblocks();

    // add a block to the block file and in files, but don't update index
    // can be repaired
    random_block(ddfs->aux_buffer);
    addr=ddfs_write_block(ddfs->aux_buffer, hash);
    funny_file("ff_unexpected_shutdown_recoverable", addr, hash, ddfs->c_block_size, 0);

    // add a block in index and files, but don't write it in the block file
    // cannot be repaired
    random_block(ddfs->aux_buffer);
    ddfs_hash(ddfs->aux_buffer, hash);
    addr=ddfs_alloc_block();
    ddfs_convert_addr(addr, node);
    node_add(node, na_duplicate_hash_allowed);
    funny_file("ff_unexpected_shutdown_bad", addr, hash, ddfs->c_block_size, 0);
    return 1;
}

int alter_file(char *what, char *op)
{
    char *filename=NULL;
    int fd=-1;
    int res=0;

    if (0==strcmp(what, "index"))
    {
        fd=ddfs->ifile;
        filename=ddfs->indexfile;
    }
    else if (0==strcmp(what, "block"))
    {
        fd=ddfs->bfile;
        filename=ddfs->blockfile;
    }

    struct stat sbuf;
    res=stat(filename, &sbuf);
    if (res==-1) filename=NULL;
    if (S_ISBLK(sbuf.st_mode)) filename=NULL;

    if (0==strcmp(op, "delete"))
    {
        if (filename) res=unlink(filename);
    }
    else if (0==strcmp(op, "magic"))
    {
        res=pwrite(fd, "WRONG", 5, 0);
    }
    else if (0==strcmp(op, "truncate"))
    {
        res=ftruncate(fd, 2048);
    }
    else if (0==strcmp(op, "empty"))
    {
        res=ftruncate(fd, 0);
    }
    return res;
}


int shake_file(char *filename)
{
    unsigned char node[NODE_SIZE];
    // unsigned char *hash=node+ddfs->c_addr_size;
	FILE *file=fopen(filename, "r+");
	if (file==NULL) return -1;
	long long int sz=lseek64(fileno(file), 0, SEEK_END);
	long long int offset=FILE_HEADER_SIZE;
	int len=ddfs->c_addr_size;
	while (offset<sz && len==ddfs->c_addr_size)
	{
	    ddfs_convert_addr(random(), node);
		fseek(file, offset, SEEK_SET);
		len=fwrite(node, 1, ddfs->c_addr_size, file);
		offset+=ddfs->c_node_size;
	}
	fclose(file);
	if (len!=ddfs->c_addr_size)
	{
		if (len==-1) perror(filename);
		else fprintf(stderr, "error shaking file %s %d\n", filename, len);
	}
	return len!=ddfs->c_addr_size;
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
    int res=0;
    long int rnd=time(NULL);
    int c;
    int lock_index_flag=0;
    int funny_files_flag=0;
    int unexpected_shutdown_flag=0;

    int index_num=0;
    int swap_consecutiv_num=0;
    int swap_random_num=0;
    int reset_node_num=0;
    int duplicate_node_num=0;
    int duplicate_inplace_num=0;
    int swap_addr_num=0;
    int corrupt_node_num=0;
    char *index_op=NULL;
    char *block_op=NULL;
    char *shake_filename=NULL;

    ddfs=&ddfsctx;
    while (1)
    {
        // getopt_long stores the option index here.
        int option_index=0;
        char *optparse;

        c=getopt_long(argc, argv, "hvlfui:s:S:r:d:p:a:c:m:n:I:B:k:", long_options, &option_index);

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
                funny_files_flag=1;
                break;

            case 'i':
                index_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 's':
                swap_consecutiv_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'S':
                swap_random_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'r':
                reset_node_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'd':
                duplicate_node_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'p':
                duplicate_inplace_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'a':
                swap_addr_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'c':
                corrupt_node_num=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                break;
            case 'u':
                unexpected_shutdown_flag=1;
                break;
            case 'n':
                rnd=strtol(optarg, &optparse, 10);
                if (optparse==optarg || *optparse!='\0') invalid_argument(c, optarg, option_index);
                srand(rnd);
                break;
            case 'I':
                index_op=strdup(optarg);
                break;

            case 'B':
                block_op=strdup(optarg);
                break;

            case 'k':
                shake_filename=strdup(optarg);
                break;

            case '?':
                // getopt_long already printed an error message.
                break;

            default:
                abort();
        }
    }

    srand(rnd);

    if (optind>=argc)
    {
        fprintf(stderr, "Target directory is missing !\n");
        return 1;
    }

    char pdir[FILENAME_MAX];

    if (ddfs_find_parent(argv[optind], pdir))
    {
        fprintf(stderr, "Not a valid ddumbfs filesystem parent directory: %s\n", argv[optind]);
        return 1;
    }

    if (is_mounted(pdir))
    {
        fprintf(stderr, "filesystem already mounted: %s\n", pdir);
        return 1;
    }

    if (ddfs_loadcfg(pdir, NULL))
    {
        return 1;
    }

    // NO_DIRECTIO to be able to write anywhere in the block file
    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, DDFS_NODIRECTIO, lock_index_flag, NULL))
    {
        return 1;
    }

    if (index_num)
    {
        swap_consecutiv_num=swap_random_num=reset_node_num=duplicate_node_num=duplicate_inplace_num=swap_addr_num=corrupt_node_num=index_num;
    }


    if (ddfs_lock(".autofsck"))
    {
        fprintf(stderr, "cannot create .autofsck file, abort (%s)\n", strerror(errno));
    }

    int i=0;
    for (i=0; i<swap_consecutiv_num; i++) swap_consecutive_non_empty_nodes();
    for (i=0; i<swap_random_num; i++) swap_random_non_empty_nodes();
    for (i=0; i<reset_node_num; i++) reset_random_non_empty_nodes();
    for (i=0; i<duplicate_node_num; i++) duplicate_non_empty_node();
    for (i=0; i<duplicate_inplace_num; i++) duplicate_non_empty_node_inplace();
    for (i=0; i<swap_addr_num; i++) swap_node_addr();
    for (i=0; i<corrupt_node_num; i++) corrupt_any_nodes();
    if (unexpected_shutdown_flag) unexpected_shutdown();

    if (index_op) alter_file("index", index_op);
    if (block_op) alter_file("block", block_op);
    if (shake_filename) res=res || shake_file(shake_filename);
    if (funny_files_flag) funny_files();
    ddfs_close();
    return res;

}
