/*
 * xlog.h
 *
 *  Created on: Jun 3, 2011
 *      Author: alain.spineux@gmail.com
 */

#ifndef XLOG_H_
#define XLOG_H_

#include <stdio.h>

#ifdef HAVE_SYSLOG_H
    #include<syslog.h>
#else
    #define LOG_EMERG   0   /* system is unusable */
    #define LOG_ALERT   1   /* action must be taken immediately */
    #define LOG_CRIT    2   /* critical conditions */
    #define LOG_ERR     3   /* error conditions */
    #define LOG_WARNING 4   /* warning conditions */
    #define LOG_NOTICE  5   /* normal but significant condition */
    #define LOG_INFO    6   /* informational */
    #define LOG_DEBUG   7   /* debug-level messages */
#endif

struct xloghandler
{
    int enable;
    int level;
    char *time_fmt;
    char *filename;
    char *alias;
    char *mode;
    int  max_bytes;
    int  backup_count;

    FILE *file;
    int  is_last;
};

struct xlogger
{
    int level;
    struct xloghandler *handlers;
};

int xlog_rollout(char *filename, int backup_count);
int xlog(struct xlogger* logger, int level, const char *format, ...)
    __attribute__ ((format (printf, 3, 4)));

struct xloghandler *xloghandler_init(struct xloghandler *xlh, int enable, int level, char *time_fmt, char *filename, char *alias, char *mode, int max_bytes, int backup_count, FILE *file);

#endif /* XLOG_H_ */
