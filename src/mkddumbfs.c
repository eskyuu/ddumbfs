/**
 * @mkddumbfs.c
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
 * mkddumbfs format a ddumbfs filesystem
 *
 */

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ftw.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#include "ddfslib.h"

static int verbose_flag=0;
static int force_flag=0;
static int reuse_asap_flag=0;

static struct option long_options[] =
{
       {"help",         no_argument,       0, 'h'},
       {"force",        no_argument,       0, 'f'},
       {"asap",         no_argument,       0, 'a'},

       {"index",        required_argument, 0, 'i'},
       {"block",        required_argument, 0, 'b'},
       {"hash",         required_argument, 0, 'H'},
       {"size",         required_argument, 0, 's'},
       {"block-size",   required_argument, 0, 'B'},
       {"overflow",     required_argument, 0, 'o'},
       {0, 0, 0, 0}
};

char *index_filename=INDEX_FILENAME;
char *block_filename=BLOCK_FILENAME;
char *hash_name="TIGER";
long long int partition_size=0;
int block_size=131072;
float overflow=1.3;
off_t ALLOCATIONGRANULARITY=65536; // max(linux.mmap.ALLOCATIONGRANULARITY, windows.mmap.ALLOCATIONGRANULARITY)

off_t boundary_align(off_t addr, off_t granularity)
{
    if (addr%granularity!=0) addr=(addr/granularity+1)*granularity;
    return addr;
}

int tree_delete(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
//    fprintf(stderr, "path: %s, %d\n", fpath, typeflag);
    int res;
    if (typeflag==FTW_DP)
    {
        res=rmdir(fpath);
        if (res==-1)
        {
            fprintf(stderr, "cannot remove directory: %s\n", fpath);
            return 1;
        }
    }
    else
    {
        res=unlink(fpath);
        if (res==-1)
        {
            fprintf(stderr, "cannot remove file: %s\n", fpath);
            return 1;
        }
    }
    return 0;
}

int init(char *parent_dir, char* blockfile, char *indexfile, long long int partition_size, int block_size, double overflow, const char *hash, int reuse_asap)
{
    int res, len, i;

    char path[FILENAME_MAX];
    char *buf;

    buf=malloc(block_size);
    if (!buf)
    {
        fprintf(stderr, "Cannot allocate %d bytes\n", block_size);
        return 1;
    }

    ddfs->c_reuse_asap=reuse_asap;
    ddfs->c_auto_buffer_flush=60;
    ddfs->c_auto_sync=120;
    ddfs->c_index_block_size=INDEX_BLOCK_SIZE;
    ddfs->c_file_header_size=FILE_HEADER_SIZE;

    printf("Initialize ddumb filesystem in directory: %s\n", parent_dir);

    ddfs->c_block_size=block_size;
    ddfs->c_node_overflow=overflow;

    if (0==strcmp(hash, "SHA1") || 0==strcmp(hash, "TIGER160")) ddfs->c_hash_size=20;
    else if (0==strcmp(hash, "TIGER")) ddfs->c_hash_size=24;
    else if (0==strcmp(hash, "TIGER128")) ddfs->c_hash_size=16;
    else
    {
        fprintf(stderr, "Hash unknown: %s\n", hash);
        return 1;
    }
    ddfs->c_hash=strdup(hash);

    // parent directory
    ddfs->pdir=strdup(parent_dir);
    res=mkdir(ddfs->pdir, 0700);
    if (!isdir(ddfs->pdir))
    {
        fprintf(stderr, "Cannot create parent directory: %s\n", ddfs->pdir);
        return 1;
    }

    // root directory
    ddfs->c_root_directory=ROOT_DIR;
    sprintf(path, "%s/%s", ddfs->pdir, ddfs->c_root_directory);
    ddfs->rdir=strdup(path);

    sprintf(path, "%s/%s", ddfs->pdir, ".autofsck");
    unlink(path);
    if (pathexists(path))
    {
        res=unlink(path);
        if (res==-1)
        {
            fprintf(stderr, "Cannot delete: %s (%s)\n", path, strerror(errno));
            return 1;
        }
    }

    sprintf(path, "%s/%s", ddfs->pdir, ".rebuildfsck");
    unlink(path);
    if (pathexists(path))
    {
        res=unlink(path);
        if (res==-1)
        {
            fprintf(stderr, "Cannot delete: %s (%s)\n", path, strerror(errno));
            return 1;
        }
    }

    if (isdir(ddfs->rdir) && nftw(ddfs->rdir, tree_delete, 10, FTW_DEPTH | FTW_MOUNT | FTW_PHYS))
    {
        fprintf(stderr, "Root directory not empty: %s\n", ddfs->rdir);
        return 1;
    }

    res=mkdir(ddfs->rdir, 0700);
    if (!isdir(ddfs->rdir))
    {
        fprintf(stderr, "Cannot create root directory: %s\n", ddfs->rdir);
        return 1;
    }

    //
    // system directory
    //
    sprintf(path, "%s/%s", ddfs->rdir, SPECIAL_DIR);
    char *special_dir=strdup(path);

    res=mkdir(special_dir, 0700);
    if (!isdir(special_dir))
    {
        fprintf(stderr, "Cannot create system directory: %s\n", special_dir);
        return 1;
    }

    char **p;
    for (p=special_filenames; *p; p++)
    {
        sprintf(path, "%s/%s", special_dir, *p);
        int fd=open(path, O_RDWR|O_CREAT|O_TRUNC, 0666);
        if (fd==-1)
        {
            fprintf(stderr, "Cannot create system file: %s\n", path);
            return 1;
        }
        close(fd);
    }

    int ifile, bfile;
    long long int index_file_size;

    //
    // block file
    //
    if (blockfile && *blockfile=='/')
    {
        strcpy(path, blockfile);
    }
    else
    {
        if (!blockfile || !*blockfile) blockfile=BLOCK_FILENAME;
        sprintf(path, "%s/%s", ddfs->pdir, blockfile);
    }

    ddfs->c_block_filename=strdup(blockfile);
    blockfile=strdup(path);

    if (0==strncmp(blockfile, "/dev/", 5))
    {
        // calculate device file size
        struct stat sbuf;
        res=stat(blockfile, &sbuf);
        if (res==-1)
        {
            fprintf(stderr, "block file error: %s\n", blockfile);
            return 1;
        }
        if (!S_ISBLK(sbuf.st_mode))
        {
            fprintf(stderr, "not a block file: %s\n", blockfile);
            return 1;
        }
        bfile=open(blockfile, O_RDONLY);
        if (bfile==-1)
        {
            perror(blockfile);
            return 1;
        }
        len=read(bfile, buf, DDFS_MAGIC_BLOCK_LEN);
        if (len!=DDFS_MAGIC_BLOCK_LEN)
        {
            fprintf(stderr, "cannot read: %s\n", blockfile);
            return 1;
        }

        long long int size=lseek64(bfile, 0, SEEK_END);
        if (size==-1)
        {
            perror(blockfile);
            return 1;
        }
        close(bfile);

        if (0!=strncmp(buf, DDFS_MAGIC_BLOCK, DDFS_MAGIC_BLOCK_LEN) && !force_flag)
        {
            printf("Do your really want to initialize this block device: %s (y/N)?", blockfile);
            *buf='N';
            if (fgets(buf, sizeof(buf), stdin)==NULL || *buf!='y') return 2;
        }


        if (partition_size>0 && partition_size<=size) ddfs->c_partition_size=partition_size;
        else ddfs->c_partition_size=size;
    }
    else
    {
        if (partition_size<=0)
        {
            fprintf(stderr, "You must specify a BlockFile size.\n");
            return 1;
        }
        ddfs->c_partition_size=partition_size;
    }

    //
    // index file
    //
    if (indexfile && *indexfile=='/')
    {
        strcpy(path, indexfile);
    }
    else
    {
        if (!indexfile || !*indexfile) indexfile=INDEX_FILENAME;
        sprintf(path, "%s/%s", ddfs->pdir, indexfile);
    }

    ddfs->c_index_filename=strdup(indexfile);
    indexfile=strdup(path);

    if (0==strncmp(indexfile, "/dev/", 5))
    {
        // calculate device file size
        struct stat sbuf;
        res=stat(indexfile, &sbuf);
        if (res==-1)
        {
            fprintf(stderr, "index file error: %s\n", indexfile);
            return 1;
        }
        if (!S_ISBLK(sbuf.st_mode))
        {
            fprintf(stderr, "not a block file: %s\n", indexfile);
            return 1;
        }
        ifile=open(indexfile, O_RDONLY);
        if (ifile==-1)
        {
            perror(indexfile);
            return 1;
        }
        len=read(ifile, buf, DDFS_MAGIC_INDEX_LEN);
        if (len!=DDFS_MAGIC_INDEX_LEN)
        {
            fprintf(stderr, "cannot read: %s\n", indexfile);
            return 1;
        }

        index_file_size=lseek64(ifile, 0, SEEK_END);
        if (index_file_size==-1)
        {
            perror(indexfile);
            return 1;
        }
        close(ifile);

        if (0!=strncmp(buf, DDFS_MAGIC_INDEX, DDFS_MAGIC_INDEX_LEN) && !force_flag)
        {
            printf("Do your really want to initialize this block device: %s (y/N)?", indexfile);
            *buf='N';
            if (fgets(buf, sizeof(buf), stdin)==NULL || *buf!='y') return 2;
        }
    }
    else
    {
        index_file_size=0;
    }

    //
    // some calculations
    //

    ddfs->c_block_count=ddfs->c_partition_size/ddfs->c_block_size;
    ddfs->c_addr_size=1;
    while ((1LL<<(ddfs->c_addr_size*8))<ddfs->c_block_count) ddfs->c_addr_size++;

    ddfs->c_node_size=ddfs->c_addr_size+ddfs->c_hash_size;
    // calculate c_node_count including overflow and _lot_ of free space at end to avoid a DEADLY overflow
    ddfs->c_node_count=(long long int)(ddfs->c_block_count*ddfs->c_node_overflow+512*ddfs->c_node_overflow);
    ddfs->c_node_block_count=(ddfs->c_node_count*ddfs->c_node_size+ddfs->c_index_block_size-1)/ddfs->c_index_block_size;
    ddfs->c_node_count=ddfs->c_node_block_count*ddfs->c_index_block_size/ddfs->c_node_size;

    ddfs->c_freeblock_offset=ddfs->c_index_block_size; // first block is reserved
    ddfs->c_freeblock_offset=boundary_align(ddfs->c_freeblock_offset, ALLOCATIONGRANULARITY);
    ddfs->c_freeblock_size=(ddfs->c_block_count+7)/8;
    ddfs->c_node_offset=ddfs->c_freeblock_offset+(ddfs->c_freeblock_size+ddfs->c_index_block_size-1)/ddfs->c_index_block_size*ddfs->c_index_block_size;
    ddfs->c_node_offset=boundary_align(ddfs->c_node_offset, ALLOCATIONGRANULARITY);
    ddfs->c_index_size=ddfs->c_node_offset+ddfs->c_node_block_count*ddfs->c_index_block_size;
    assert(ddfs->c_index_size%ddfs->c_index_block_size==0);
    ddfs->c_index_block_count=ddfs->c_index_size/ddfs->c_index_block_size;

    if (index_file_size!=0 && index_file_size<ddfs->c_index_size)
    {
        fprintf(stderr, "The block device allocated for the IndexFile is too small.\n");
        return 1;
    }

    //
    //initialize block file
    //
    bfile=open(blockfile, O_RDWR|O_CREAT|O_TRUNC, 0600);
    if (bfile==-1)
    {
        perror(blockfile);
        return 1;
    }
    // first block is header, second is unused
    memset(buf, '\0', ddfs->c_block_size);
    memcpy(buf, DDFS_MAGIC_BLOCK, DDFS_MAGIC_BLOCK_LEN);
    len=write(bfile, buf, ddfs->c_block_size);
    if (len!=ddfs->c_block_size)
    {
        if (len==-1) perror(blockfile);
        fprintf(stderr, "Cannot write header in BlockFile: %s\n", blockfile);
        return 1;
    }
    memset(buf, '\0', ddfs->c_block_size);
    len=write(bfile, buf, ddfs->c_block_size);
    if (len!=ddfs->c_block_size)
    {
        if (len==-1) perror(blockfile);
        fprintf(stderr, "Cannot write second reserved block in BlockFile: %s\n", blockfile);
        return 1;
    }

    printf("BlockFile initialized: %s\n", blockfile);

    //
    //initialize index file
    //
    ifile=open(indexfile, O_RDWR|O_CREAT|O_TRUNC, 0600);
    if (ifile==-1)
    {
        perror(indexfile);
        return 1;
    }
    // first block is header
    memset(buf, '\0', ddfs->c_index_block_size);
    memcpy(buf, DDFS_MAGIC_INDEX, DDFS_MAGIC_INDEX_LEN);
    len=write(ifile, buf, ddfs->c_index_block_size);
    if (len!=ddfs->c_index_block_size)
    {
        if (len==-1) perror(indexfile);
        fprintf(stderr, "Cannot write header in IndexFile: %s\n", indexfile);
        return 1;
    }

    // used block list (bit field)
    // first and second block are reserved => '\x80'

    if (lseek64(ifile, ddfs->c_freeblock_offset, SEEK_SET)==-1)
    {
        perror(indexfile);
        return 1;
    }
    memset(buf, '\0', ddfs->c_index_block_size);
    buf[0]=0xC0;
    len=write(ifile, buf, ddfs->c_index_block_size);
    buf[0]=0;
    for (i=1; i<(ddfs->c_node_offset-ddfs->c_freeblock_offset)/ddfs->c_index_block_size; i++)
    {
        len=write(ifile, buf, ddfs->c_index_block_size);
        if (len!=ddfs->c_index_block_size)
        {
            if (len==-1) perror(ddfs->indexfile);
            fprintf(stderr, "Error writing to IndexFile: %s\n", ddfs->indexfile);
            return 1;
        }

    }

    // nodes
    if (lseek64(ifile, ddfs->c_node_offset, SEEK_SET)==-1)
    {
        perror(indexfile);
        return 1;
    }
    for (i=0; i<ddfs->c_node_block_count; i++)
    {
        len=write(ifile, buf, ddfs->c_index_block_size);
        if (len!=ddfs->c_index_block_size)
        {
            if (len==-1) perror(ddfs->indexfile);
            fprintf(stderr, "Error writing to IndexFile: %s\n", ddfs->indexfile);
            return 1;
        }
    }

    // printf("seek=%lld size=%lld\n", lseek64(ifile, 0, SEEK_CUR), ddfs->c_index_size);
    assert(lseek64(ifile, 0, SEEK_CUR)==ddfs->c_index_size);
    close(ifile);

    printf("IndexFile initialized: %s\n", indexfile);


    sprintf(path, "%s/%s", ddfs->pdir, CFG_FILENAME);

    FILE *cfgfile=fopen(path, "w+");
    struct cfgfile *c;
    for (c=cfg; c->name!=NULL; c++)
    {
        void *p=((char*)ddfs)+c->offset;
        switch (c->type)
        {
            case 'I':
                fprintf(cfgfile,"%s: %d\n", c->name, *(int*)p);
                break;
            case 'L':
                fprintf(cfgfile,"%s: %lld\n", c->name, *(long long int*)p);
                break;
            case 'D':
                fprintf(cfgfile,"%s: %.2f\n", c->name, *(double*)p);
                break;
            case 'S':
                fprintf(cfgfile,"%s: %s\n", c->name, *(char **)p);
                break;
            default:
                fprintf(stderr, "Cannot write variable c_%s, unknown format '%c'.\n", c->name, c->type);
                return 1;
        }
    }

    fclose(cfgfile);
    printf("ddfs.cfg file initialized: %s\n", path);

    printf("ddumbfs initialized in %s\n", ddfs->pdir);

    int fd=open(path, O_RDONLY);
    len=read(fd, buf, ddfs->c_index_block_size);
    close(fd);
    len=fwrite(buf, len, 1, stdout);

    free(buf);
    return 0;
}

void usage()
{
    printf("Usage: mkddumbfs [options] target_dir\n"
            "\n    initialize a ddumbfs filesystem\n\nOptions:\n"
            "  -h, --help            show this help message and exit\n"
            "  -f, --force           don't ask question\n"
            "  -a, --asap            reuse reclaimed blocks ASAP(useless when using device)\n"
            "  -i INDEXFILE, --index=INDEXFILE\n"
            "                        specify the index filename or device\n"
            "  -b BLOCKFILE, --block=BLOCKFILE\n"
            "                        specify the block filename or device\n"
            "  -H HASH, --hash=HASH  specify the hash, in SHA1, TIGER128, TIGER160, TIGER\n"
            "                        default is TIGER (aka TIGER192) and SHA1 for Pentium IV\n"
            "  -s SIZE, --size=SIZE  the size of block file\n"
            "                        if -b is a block device, 0 means use the size of\n"
            "                        the device\n"
            "  -B BLOCK_SIZE, --block-size=BLOCK_SIZE\n"
            "                        the block size (default is 128k)\n"
            "  -o OVERFLOW, --overflow=OVERFLOW\n"
            "                        the overflow factor (default is 1.3)\n"
            "\nSamples:\n"
            "  mkddumbfs -s 20G -a /data/ddumbfs\n"
            "  mkddumbfs -s 20G -a -B 64k /data/ddumbfs\n"
            "  mkddumbfs -b /dev/sdb3 /data/ddumbfs\n"
    );
}

struct ddfs_ctx ddfsctx;

int main(int argc, char *argv[])
{
    int c;
    char *unit;

    ddfs=&ddfsctx;
    while (1)
    {
        // getopt_long stores the option index here.
        int option_index = 0;

        c=getopt_long(argc, argv, "hvfai:b:H:s:B:o:", long_options, &option_index);

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

            case 'a':
                reuse_asap_flag=1;
                break;

            case 'i':
                index_filename=strdup(optarg);
                break;

            case 'b':
                block_filename=strdup(optarg);
                break;

            case 'H':
                hash_name=strdup(optarg);
                break;

            case 's':
                partition_size=strtol(optarg, &unit, 10);
                partition_size*=unitvalue(unit);
                break;

            case 'B':
                block_size=strtol(optarg, &unit, 10);
                block_size*=unitvalue(unit);
                break;

            case 'o':
                overflow=strtod(optarg, NULL);
                break;

            case '?':
                // getopt_long already printed an error message.
                break;

            default:
                abort();
        }
    }

    // check block size
    if (block_size<512 || 2*1024*1024<block_size)
    {
        fprintf(stderr, "block size must be between 4k and 128k (between 512 and 2Mo for test only): %d\n", block_size);
        return 1;
    }
    int bs=block_size;
    while (bs>2)
    {
        if (bs%2)
        {
            fprintf(stderr, "block size must be a power of 2: %d\n", block_size);
            return 1;
        }
        bs/=2;
    }

    // check partition size
    if (partition_size>0 && partition_size<2*block_size)
    {
        fprintf(stderr, "partition size must be a least 2 blocks wide: %lld\n", partition_size);
        return 1;
    }

    // check overflow
    if (overflow<1.1 || 10.0<overflow)
    {
        fprintf(stderr, "Overflow must be between 1.0 and 10.0: %.2f\n", overflow);
        return 1;
    }

    // check HASH
    if (0!=strcmp("SHA1", hash_name) &&
        0!=strcmp("TIGER", hash_name) &&
        0!=strcmp("TIGER128", hash_name) &&
        0!=strcmp("TIGER160", hash_name))
    {
        fprintf(stderr, "Unknown HASH: %s\n", hash_name);
        return 1;
    }

    if (optind>=argc)
    {
        fprintf(stderr, "Target directory is missing !\n");
        return 1;
    }

    char *pdir=argv[optind];
    if (!isdir(pdir))
    {
        fprintf(stderr, "Target must be an existing directory: %s\n", pdir);
        return 1;
    }

    if (is_mounted(pdir))
    {
        fprintf(stderr, "filesystem already mounted: %s\n", pdir);
        return 1;
    }

    return init(pdir, block_filename, index_filename, partition_size, block_size, overflow, hash_name, reuse_asap_flag);
}
