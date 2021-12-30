/* -------------------------------------------------------------------------
 *
 * pg_rewind.h
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_REWIND_H
#define PG_REWIND_H

#include "c.h"

#include "datapagemap.h"

#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "bin/elog.h"

#define TAG_START "build_completed.start"
#define TAG_DONE "build_completed.done"
#define TAG_BACKUP_START "backup_completed.start"
#define TAG_BACKUP_DONE "backup_completed.done"
#define BACKUP_DIR "pg_rewind_bak"
#define RESTORE_REWIND_STATUS "pg_rewind.restore"
#define BUILD_PID "gs_build.pid"

#define RT_WITH_DUMMY_STANDBY 0
#define RT_WITH_MULTI_STANDBY 1
#define MAX_ERR_MSG_LENTH 1024
#define XLOG_FILE_NAME_LENGTH 25
#define MAX_CONFIG_FILE_SIZE 0xFFFFF /* max file size for configurations = 1M */
typedef enum { BUILD_SUCCESS = 0, BUILD_ERROR, BUILD_FATAL } BuildErrorCode;

/* Configuration options */
extern char* datadir_target;
extern char* datadir_source;
extern char* connstr_source;
extern bool debug;
extern bool showprogress;
extern bool dry_run;
extern int replication_type;
extern bool backup;
extern pid_t process_id;
extern const char* progname;
extern BuildErrorCode increment_return_code;
extern char divergeXlogFileName[MAXFNAMELEN];

/* in parsexlog.c */
extern void extractPageMap(const char* datadir, XLogRecPtr startpoint, TimeLineID tli);
extern XLogRecPtr readOneRecord(const char* datadir, XLogRecPtr ptr, TimeLineID tli);
extern XLogRecPtr FindMaxLSN(char* workingpath, char* returnmsg, pg_crc32 *maxLsnCrc, uint32 *maxLsnLen = NULL, 
    TimeLineID *returnTli = NULL);
BuildErrorCode findCommonCheckpoint(const char* datadir, TimeLineID tli, XLogRecPtr startrec, XLogRecPtr* lastchkptrec,
    TimeLineID* lastchkpttli, XLogRecPtr *lastchkptredo, uint32 term);
extern int find_gucoption(const char** optlines, const char* opt_name, int* name_offset, int* name_len, 
    int* value_offset, int* value_len);

extern bool TransLsn2XlogFileName(XLogRecPtr lsn, TimeLineID lastcommontli, char* xlogName);
extern XLogRecPtr getValidCommonLSN(XLogRecPtr checkLsn, XLogRecPtr maxLsn);
int gs_increment_build(const char* pgdata, char* sysidentifier, uint32 timeline, uint32 term);
extern BuildErrorCode targetFileStatThread(void);
extern BuildErrorCode waitEndTargetFileStatThread(void);
extern BuildErrorCode targetFilemapProcess(void);
void recordReadTest(const char* datadir, XLogRecPtr ptr, TimeLineID tli);
void openDebugLog(void);

#define PG_CHECKBUILD_AND_RETURN()                  \
    do {                                            \
        if (increment_return_code != BUILD_SUCCESS) \
            return increment_return_code;           \
    } while (0)

#define PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res)                  \
        do {                                            \
            if (increment_return_code != BUILD_SUCCESS) { \
                PQclear(res);                             \
                res = NULL;                                \
                return increment_return_code;           \
                                                          \
            }                                             \
        } while (0)

#define PG_CHECKRETURN_AND_RETURN(rv) \
    do {                              \
        if (rv != BUILD_SUCCESS)      \
            return rv;                \
    } while (0)

#define PG_CHECKRETURN_AND_FREE_PGRESULT_RETURN(rv, res) \
    do {                              \
        if (rv != BUILD_SUCCESS) {      \
            PQclear(res);              \
            res = NULL;                \
            return rv;                 \
        }                              \
    } while (0)

#endif /* PG_REWIND_H */

