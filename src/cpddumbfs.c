/**
 * @cpddumbfs.c
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
 * cpddumbfs copy from and to a offline ddumbfs filesystem
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

static struct option long_options[] =
{
       /* These options set a flag. */
       {"help",          no_argument,       0, 'h'},
       {"lock_index",    no_argument,       0, 'l'},
       {"check",         no_argument,       0, 'c'},
       {"verbose",       no_argument,       0, 'v'},
       {"force",         no_argument,       0, 'f'},
       {0, 0, 0, 0}
};

void usage()
{
    printf("Usage: cpddumbfs [options] source target\n"
            "\n    copy from and to an offline ddumbfs filesystem\n\nOptions\n"
            "  -h, --help            show this help message and exit\n"
            "  -v, --verbose         display addresses and hashes from file (download only)\n"
            "  -l, --lock_index      lock index into memory (increase speed for large file)\n"
            "  -c, --check           check file integrity in exit code (download only)\n"
            "  -f, --force           download a file even with a corrupted MAGIC in the header\n"
            "\n  One and only one of the source or target must be inside the\n"
            "  ddfsroot directory. Use - to redirect from/to stdin/stdout.\n"
            "\nSamples:\n"
            "  cpddumbfs -l /data/ddumbfs/ddfsroot/data /tmp\n"
            "  cpddumbfs /tmp/data /data/ddumbfs/ddfsroot\n"
    );
}

int download(char *source, char *destination)
{
    char dstfilename[FILENAME_MAX];
    unsigned char node[NODE_SIZE];
    uint64_t size;
    int len;

    int integrity=1;

    if (verbose_flag) fprintf(stderr, "download %s %s\n", source, destination);
    int fsrc=open(source, O_RDONLY);
    if (fsrc==-1)
    {
        perror(source);
        return 1;
    }

    int fdst=-1;
    if (0==strcmp("-", destination))
    {
        fdst=1; // stdout
    }
    else
    {
        if (isdir(destination))
        {
            char *p=strrchr(source, '/');
            if (p==NULL) p=source;
            else p=p+1;
            snprintf(dstfilename, FILENAME_MAX, "%s/%s", destination, p);
        }
        else strncpy(dstfilename, destination, FILENAME_MAX);

        fdst=open(dstfilename, O_RDWR | O_CREAT | O_TRUNC, 0600);
        if (fdst==-1)
        {
            perror(dstfilename);
            goto ERROR1;
        }
    }

    // read file size
    len=file_header_set_conv(fsrc, &size);

    if (len==-1)
    {
        perror(source);
        goto ERROR1;
    }
    else if (len==0)
    {
        fprintf(stderr, "magic header missing, not a valid ddumbfs file: %s\n", source);
        if (!force_flag) goto ERROR1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        fprintf(stderr, "cannot read file header size: %s\n", source);
        if (!force_flag) goto ERROR1;
    }

    long long int file_size=size;
    if (len!=ddfs->c_file_header_size)
    {
        fprintf(stderr, "unknown file size, continue: %s\n", source);
        file_size=-1;
        integrity=0;
    }

    long long int i=0;
    long long int write_size=0;

    int nlen=ddfs->c_node_size;
    while (1 || (force_flag && file_size==-1)) // || write_size<file_size)
    {
        int fake_node=0;
        int fake_block=0;
        long long int fileoff=ddfs->c_file_header_size+i*ddfs->c_node_size;
        nlen=pread(fsrc, node, ddfs->c_node_size, fileoff);
        if (nlen==0) break; // EOF
        if (nlen!=ddfs->c_node_size)
        {
            if (nlen==-1) fprintf(stderr, "error reading offset %lld (%s)\n", fileoff, strerror(errno));
            else fprintf(stderr, "error reading offset %lld short read %d/%d\n", fileoff, nlen, ddfs->c_node_size);
            if (!force_flag) goto ERROR1;
            memset(node, '\0', ddfs->c_node_size);
            ddfs_convert_addr(1, node);
            integrity=0;
            fake_node=1;
        }

        blockaddr addr=ddfs_get_node_addr(node);
        len=ddfs_read_full_block(addr, ddfs->aux_buffer);
        if (len!=ddfs->c_block_size)
        {
            if (len==-1) fprintf(stderr, "error reading block file offset %lld (%s)\n", addr*ddfs->c_block_size, strerror(errno));
            else fprintf(stderr, "error reading block file offset %lld short read %d/%d\n", addr*ddfs->c_block_size, len, ddfs->c_block_size);
            integrity=0;
            if (!force_flag) goto ERROR1;
            ddfs_forced_read_full_block(addr, ddfs->aux_buffer, 1024);
            len=ddfs->c_block_size;
            fake_block=1;
        }

        char *status="";
        int badblock=0;
        if (addr==1)
        {
            badblock=1;
            status="corrupted";
        }

        if (check_integrity && !fake_node)
        {
            status="ok";
            if (addr==0)
            {   // check hash match zero block
                if (0!=memcmp(ddfs->null_block_hash, node+ddfs->c_addr_size, ddfs->c_hash_size) && 0!=memcmp(ddfs->zero_block_hash, node+ddfs->c_addr_size, ddfs->c_hash_size))
                {
                    badblock=1;
                    status="bad"; // bad node
                }
            }
            else
            {
                unsigned char hash[HASH_SIZE];
                ddfs_hash(ddfs->aux_buffer, hash);
                if (memcmp(node+ddfs->c_addr_size, hash, ddfs->c_hash_size)!=0)
                {
                    badblock=1;
                    status="err";
                    // fprintf(stderr, "err block %6lld %6lld %016llx<>%016llx\n", i, addr, *(long long int*)(node+ddfs->c_addr_size), *(long long int*)hash);
                    // search the Index, maybe I can found the good hash ?
                    blockaddr baddr;
                    nodeidx node_idx=ddfs_search_hash(node+ddfs->c_addr_size, &baddr);
                    //fprintf(stderr, "err node_idx=%lld baddr=%lld\n", node_idx, baddr);
                    while (badblock && 0<=node_idx && node_idx<ddfs->c_node_count && 0==memcmp(node+ddfs->c_addr_size, ddfs->nodes+(ddfs->c_node_size*node_idx+ddfs->c_addr_size), ddfs->c_hash_size))
                    {
                        baddr=ddfs_get_node_addr(ddfs->nodes+(ddfs->c_node_size*node_idx));
                        // fprintf(stderr, "test node node_idx=%lld baddr=%lld\n", node_idx, baddr);
                        if (baddr!=addr && baddr!=1)
                        {
                            len=ddfs_read_full_block(baddr, ddfs->aux_buffer);
                            if (len!=ddfs->c_block_size)
                            {
                                if (len==-1) fprintf(stderr, "error reading block file offset %lld (%s)\n", baddr*ddfs->c_block_size, strerror(errno));
                                else fprintf(stderr, "error reading block file offset %lld short read %d/%d\n", baddr*ddfs->c_block_size, len, ddfs->c_block_size);
                                if (!force_flag) goto ERROR1;
                                ddfs_forced_read_full_block(baddr, ddfs->aux_buffer, 1024);
                                integrity=0;
                                len=ddfs->c_block_size;
                                fake_block=1;
                            }
                            ddfs_hash(ddfs->aux_buffer, hash);
                            if (0==memcmp(node+ddfs->c_addr_size, hash, ddfs->c_hash_size))
                            {
                                badblock=0;
                                status="corrected";
                                addr=baddr;
                            }
                        }
                        node_idx++;
                    }
                }

            }
        }

        if (badblock) integrity=0;

        long long int sz=ddfs->c_block_size;
        if (file_size!=-1)
        {
            sz=file_size-write_size;
        }
        if (sz>ddfs->c_block_size) sz=ddfs->c_block_size;

        len=write(fdst, ddfs->aux_buffer, sz);
        if (len!=sz)
        {
            if (len==-1) fprintf(stderr, "error writing file offset %lld (%s)\n", write_size, strerror(errno));
            else fprintf(stderr, "error writing file, short write offset %lld %d/%lld\n", write_size, len, sz);
            goto ERROR1; // no force_flag here
        }
        if (verbose_flag || 0!=strcmp(status, "ok"))
        {
            fprintf(stderr, "%6lld %6lld %016llx %s\n", i, addr, (long long int)ddfs_hton64(*(uint64_t*)(node+ddfs->c_addr_size)), status);
        }

        write_size+=sz;
        i++;
    }

    if (verbose_flag)
    {
        if (file_size>0) fprintf(stderr, "        size: %lld\n", file_size);
        else fprintf(stderr, "        size: NA\n");
        fprintf(stderr, "written size: %lld\n", write_size);
    }

    close(fsrc);
    if (fdst!=1) close(fdst);

    return !integrity;

    ERROR1:
    if (fdst!=1 && fdst!=-1) close(fdst);
    close(fsrc);
    return 1;
}


int upload(char *source, char *destination)
{
    char dstfilename[FILENAME_MAX];
    uint64_t size;
    int len, res;

    if (ddfs->auto_fsck || ddfs->rebuild_fsck)
    {
        fprintf(stderr, "The filesystem must be checked.\n");
        return 1;
    }

    if (verbose_flag) fprintf(stderr, "upload %s %s\n", source, destination);
    int fsrc;
    if (0==strcmp("-", source))
    {
        fsrc=0; // stdin
    }
    else
    {
        fsrc=open(source, O_RDONLY);
        if (fsrc==-1)
        {
            perror(source);
            return 1;
        }
    }

    int fdst;
    if (isdir(destination))
    {
        if (fsrc==1)
        {
            fprintf(stderr, "destination must have a name when source is stdin\n");
            return 1;
        }
        char *p=strrchr(source, '/');
        if (p==NULL) p=source;
        else p=p+1;
        snprintf(dstfilename, FILENAME_MAX, "%s/%s", destination, p);
    }
    else strncpy(dstfilename, destination, FILENAME_MAX);

    fdst=open(dstfilename, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fdst==-1)
    {
        perror(dstfilename);
        close(fsrc);
        return 1;
    }

    res=lseek(fdst, ddfs->c_file_header_size, SEEK_SET);
    if (res==-1)
    {
        perror(source);
        return 1;
    }

    size=0;
    len=ddfs->c_block_size;
    long long int i=0;
    while (len==ddfs->c_block_size)
    {
        unsigned char node[NODE_SIZE];
        long long int addr;

        len=read(fsrc, ddfs->aux_buffer, ddfs->c_block_size);
        if (len==-1)
        {
            perror(source);
            return 1;
        }
        size+=len;
        if (len<ddfs->c_block_size) memset(ddfs->aux_buffer+len, '\0', ddfs->c_block_size-len);
        addr=ddfs_write_block(ddfs->aux_buffer, node+ddfs->c_addr_size);
        ddfs_convert_addr(addr, node);

        if (verbose_flag)
        {
            fprintf(stderr, "%6lld %6lld %016llx...\n", i, addr, (long long int)ddfs_hton64(*(uint64_t*)(node+ddfs->c_addr_size)));
        }

        res=write(fdst, node, ddfs->c_node_size);
        if (res!=ddfs->c_node_size)
        {
            fprintf(stderr, "error writing file\n");
            return 1;
        }
        i++;
    }

    // write size
    len=file_header_get_conv(fdst, size);
    if (len==-1)
    {
        perror(dstfilename);
        return 1;
    }
    else if (len!=ddfs->c_file_header_size)
    {
        fprintf(stderr, "cannot write file header\n");
        return 1;
    }

    if (fsrc!=0) close(fsrc);
    close(fdst);

    return 0;

}

struct ddfs_ctx ddfsctx;

int main(int argc, char *argv[])
{
    int res=0;
    int c;

    ddfs=&ddfsctx;
    while (1)
    {
        // getopt_long stores the option index here.
        int option_index = 0;

        c=getopt_long(argc, argv, "hvlcf", long_options, &option_index);

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

            case 'c':
                check_integrity=1;
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
        fprintf(stderr, "missing source or target directory %d %d!\n", optind, argc );
        return 1;
    }

    char pdir[FILENAME_MAX];
    int up=0;
    int down=0;

    if (ddfs_find_parent(argv[optind], pdir)==0) down=1;
    if (ddfs_find_parent(argv[optind+1], pdir)==0) up=1;

    if (down && up)
    {
        fprintf(stderr, "Only one of the source or destination can be inside the offline ddumbfs filesystem.\n");
        return 1;
    }

    if (up && is_mounted(pdir))
    {   // yes you can download from a mounted filesystem :-)
        fprintf(stderr, "filesystem already mounted: %s\n", pdir);
        return 1;
    }

    if (ddfs_loadcfg(pdir, NULL))
    {
        return 1;
    }

    if (ddfs_init(DDFS_NOFORCE, DDFS_NOREBUILD, DDFS_DIRECTIOAUTO, lock_index_flag, NULL))
    {
        return 1;
    }

    if (down)
    {
        res=download(argv[optind], argv[optind+1]);
        if (check_integrity)
        {
            if (res) fprintf(stderr, "ERR\n");
            else fprintf(stderr, "OK\n");
        }
    }
    else if (up)
    {
        res=upload(argv[optind], argv[optind+1]);
    }

    ddfs_close();
    return res;

}
