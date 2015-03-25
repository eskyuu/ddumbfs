/*
  xlog.c
  Copyright (C) 2011  Alain Spineux <alain.spineux@gmail.com>

  handle logging to multiple handler a la python

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

*/


#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>

#include "xlog.h"

char *level_long_str[]={
        "EMERGENCY",
        "ALERT",
        "CRITICAL",
        "ERROR",
        "WARNING",
        "NOTICE",
        "INFO",
        "DEBUG",
};

static pthread_mutex_t xlog_mutex=PTHREAD_MUTEX_INITIALIZER;

int xlog_rollout(char *filename, int backup_count)
{
    struct stat stats;
    char filename1[FILENAME_MAX];
    char filename2[FILENAME_MAX];
    int ret=0;
    int res;
    int count;

    if (stat(filename, &stats)!=0)
    {   // file don't exist, don't role anything
        return ret;
    }
//    printf("rollout %s %d\n", filename, backup_count);
    for (count=backup_count; count>=0; count--)
    {
        if (count==0) strcpy(filename1, filename);
        else snprintf(filename1, sizeof(filename1), "%s.%d", filename, count);

//        printf("filename1 %s\n", filename1);

        if (stat(filename1, &stats)==0 && S_ISREG(stats.st_mode))
        {
            if (count==backup_count)
            {
                res=unlink(filename1);
//                printf("unlink %s\n", filename1);
            }
            else
            {
                snprintf(filename2, sizeof(filename2), "%s.%d", filename, count+1);
                res=rename(filename1, filename2);
//                printf("rename %s -> %s\n", filename1, filename2);
            }
            if (res==-1)
            {
                perror(filename1);
                ret=1;
            }
        }

    }
    return ret;
}

int xlog(struct xlogger* logger, int level, const char *format, ...)
{
    struct xloghandler* l;
    int ret=0;
    int res;

    if (level<0 || LOG_DEBUG<level || level>logger->level) return ret;

    pthread_mutex_lock(&xlog_mutex);
    for (l=logger->handlers; l->enable>=0; l++)
    {
        va_list args;               // moved inside the loop because of x64
        va_start(args, format);     // was fine outside with i386
        if (l->level<0 || l->level<level) continue;
        if (l->filename)
        {
            if (l->file!=NULL)
            {
                if (l->max_bytes>0)
                {
                    struct stat stats;
                    res=fstat(fileno(l->file), &stats);
                    if (res==-1)
                    {
                        ret=1;
                        perror(l->filename);
                        continue;
                    }
                    if (stats.st_size >= l->max_bytes)
                    {
//                        printf("close %lld>%d\n", stats.st_size, l->max_bytes);
                        res=fclose(l->file);
                        if (res==-1)
                        {
                            ret=1;
                            perror(l->filename);
                            continue;
                        }
                        res=xlog_rollout(l->filename, l->backup_count);
                        if (res) ret=1;
                        l->file=NULL;
                    }
                }
            }

            if (l->file==NULL)
            {
                if (0!=strcmp(l->mode, "a") && l->backup_count>0)
                {
                    res=xlog_rollout(l->filename, l->backup_count);
                    if (res) ret=1;
                }
//                printf("open %s %s\n", l->filename, l->mode);
                l->file=fopen(l->filename, l->mode);
                if (l->file==NULL)
                {
                    ret=1;
                    perror(l->filename);
                    continue;
                }
                setlinebuf(l->file);
            }
        }
        if (format && l->time_fmt)
        {
            char timestr[1024];
            time_t t;
            struct tm *tmp;

            t=time(NULL);
            tmp=localtime(&t);

            strftime(timestr, sizeof(timestr), l->time_fmt, tmp);
            char *p=strstr(timestr, "%{LEVEL}");
            if (p)
            {
                memmove(p, p+5, strlen(p)-4); // include '\0'
                memcpy(p, level_long_str[level], 3);
            }

            res=fputs(timestr, l->file);
            if (res==EOF)
            {
                ret=1;
            }

        }
        if (format)
        {
            res=vfprintf(l->file, format, args);
            if (res<0) ret=1;
        }
        va_end(args);
    }
    pthread_mutex_unlock(&xlog_mutex);
    return ret;
}

struct xloghandler *xloghandler_init(struct xloghandler *xlh, int enable, int level, char *time_fmt, char *filename, char *alias, char *mode, int max_bytes, int backup_count, FILE *file)
{
    xlh->enable=enable;
    xlh->level=level;
    xlh->time_fmt=time_fmt;
    xlh->filename=filename;
    xlh->alias=alias;
    xlh->mode=mode;
    xlh->max_bytes=max_bytes;
    xlh->backup_count=backup_count;
    xlh->file=file;
    return xlh;
}

struct xloghandler xlh[3];

struct xlogger logger;

int xlog_main(int argc, char *argv)
{   // This is for testing
    int i;
    xloghandler_init(xlh+0,  1, LOG_ERR, "%F %T %%{LEVEL} ", "/tmp/xlog.log", "xlog.log", "a", 500, 3, NULL);
    xloghandler_init(xlh+1,  1, LOG_DEBUG, "%T %%{LEVEL} ", NULL, "stderr", NULL, 0, 0, stderr);
    xloghandler_init(xlh+2, -1, 0, NULL, NULL, NULL, NULL, 0, 0, NULL);

    if (xlog(&logger, LOG_EMERG, NULL)) fprintf(stderr, "error initializing logging\n");

    logger.level=LOG_DEBUG;
    logger.handlers=xlh;

    for (i=0; i<20; i++) {
        xlog(&logger, LOG_DEBUG, "%4d DEBUG Hello World ! %s\n", i, "go go");
        xlog(&logger, LOG_INFO, "%4d INFO  Hello World ! %s\n", i, "go go");
        xlog(&logger, LOG_ERR, "%4d ERROR Hello World ! %s\n", i, "go go");
    }

    return 0;
}
