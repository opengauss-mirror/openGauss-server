/* -------------------------------------------------------------------------
 *
 * syslogger.h
 *	  Exports from postmaster/syslogger.c.
 *
 * Copyright (c) 2004-2012, PostgreSQL Global Development Group
 *
 * src/include/postmaster/syslogger.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _SYSLOGGER_H
#define _SYSLOGGER_H

#include <limits.h> /* for PIPE_BUF */

/*
 * Primitive protocol structure for writing to syslogger pipe(s).  The idea
 * here is to divide long messages into chunks that are not more than
 * PIPE_BUF bytes long, which according to POSIX spec must be written into
 * the pipe atomically.  The pipe reader then uses the protocol headers to
 * reassemble the parts of a message into a single string.	The reader can
 * also cope with non-protocol data coming down the pipe, though we cannot
 * guarantee long strings won't get split apart.
 *
 * We use non-nul bytes in is_last to make the protocol a tiny bit
 * more robust against finding a false double nul byte prologue. But
 * we still might find it in the len and/or pid bytes unless we're careful.
 */

#ifdef PIPE_BUF
/* Are there any systems with PIPE_BUF > 64K?  Unlikely, but ... */
#if PIPE_BUF > 65536
#define LOGPIPE_CHUNK_SIZE 65536
#else
#define LOGPIPE_CHUNK_SIZE ((int)PIPE_BUF)
#endif
#else /* not defined */
/* POSIX says the value of PIPE_BUF must be at least 512, so use that */
#define LOGPIPE_CHUNK_SIZE 512
#endif

#define PROFILE_LOG_TAG "gs_profile"
#define PROFILE_LOG_SUFFIX ".prf"
#define SLOWQUERY_LOG_TAG "sql_monitor"
#define ASP_LOG_TAG "asp_data"
#define PERF_JOB_TAG "pg_perf"

enum LogType {
    /* error log */
    LOG_TYPE_ELOG = 0,

    /* profiling log */
    LOG_TYPE_PLOG,

    /* slow query monitor log */
    LOG_TYPE_PLAN_LOG,
    /* active session profile log */
    LOG_TYPE_ASP_LOG,

    /* a solider flag, LOG_TYPE_MAXVALID should be <= LOG_TYPE_UPLIMIT */
    LOG_TYPE_MAXVALID,

    /*
     * max value of log type.
     * it's stored by CHAR datatype. see LogPipeProtoHeader::logtype
     */
    LOG_TYPE_UPLIMIT = 127
};

typedef struct LogControlData {
    bool inited;
    uint16 ver;

    /* rotation request */
    volatile sig_atomic_t rotation_requested;

    /* to flush buffer request */
    volatile sig_atomic_t flush_requested;

    /* log directory */
    char* log_dir;

    /* pattern of log file name */
    char* filename_pattern;
    char* file_suffix;

    /* current log file name and its fd */
    char* now_file_name;
    FILE* now_file_fd;

    /* log chunk buffer */
    char* log_buf;
    int cur_len;
    int max_len;
} LogControlData;

/*
 * max length of this node name. this is a arbitrary value.
 * just keep it the same with struct NameData.
 */
#define LOG_MAX_NODENAME_LEN 64

#define LOG_MAGICNUM 0x2017091810170000
#define PROTO_HEADER_MAGICNUM 0x123456789ABCDEF0
/*
 * current log version about profile log
 * advance it when log struct is modified each time.
 */
#define PROFILE_LOG_VERSION 1

/* header data in each binary log file */
typedef struct {
    /* must be the first */
    unsigned long fst_magic;

    uint16 version;
    uint8 hostname_len;
    uint8 nodename_len;
    uint16 timezone_len;

    /*
     * part1: hostname <- hostname_len
     * part2: nodename <- nodename_len
     * part3: timezone <- timezone_len
     */

    /* must be the last */
    unsigned long lst_magic;
} LogFileHeader;

typedef struct {
    char nuls[2]; /* always \0\0 */
    uint16 len;   /* size of this chunk (counts data only) */
    char logtype; /* which log type, see LogType */
    char is_last; /* last chunk of message? 't' or 'f' ('T' or
                   * 'F' for CSV case) */
    /* writer's pid. be placed the last, and make data 8 bytes alligned */
    ThreadId pid;
    uint64 magic; /* magic number to check the proto header */
    char data[FLEXIBLE_ARRAY_MEMBER]; /* data payload starts here */
} LogPipeProtoHeader;

typedef union {
    LogPipeProtoHeader proto;
    char filler[LOGPIPE_CHUNK_SIZE];
} LogPipeProtoChunk;

#define LOGPIPE_HEADER_SIZE offsetof(LogPipeProtoHeader, data)
#define LOGPIPE_MAX_PAYLOAD ((int)(LOGPIPE_CHUNK_SIZE - LOGPIPE_HEADER_SIZE))

extern THR_LOCAL bool am_syslogger;

#ifndef WIN32

#else
extern THR_LOCAL HANDLE syslogPipe[2];
#endif

extern ThreadId SysLogger_Start(void);

extern void SysLoggerClose(void);

extern void write_syslogger_file(char* buffer, int count, int dest);

#ifdef EXEC_BACKEND
extern void SysLoggerMain(int fd);
#endif

extern void LogCtlLastFlushBeforePMExit(void);
extern void set_flag_to_flush_buffer(void);
extern void* SQMOpenLogFile(bool *doOpen);
extern void SQMCloseLogFile();

extern void* ASPOpenLogFile(bool *doOpen);
extern void ASPCloseLogFile();
extern void init_instr_log_directory(bool include_nodename, const char* logid);

#endif /* _SYSLOGGER_H */
