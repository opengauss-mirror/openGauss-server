#ifndef SRC_BIN_PG_BASEBACKUP_STREAMUTIL_H
#define SRC_BIN_PG_BASEBACKUP_STREAMUTIL_H
#include "libpq/libpq-fe.h"
#include "utils/timestamp.h"

extern const char* progname;
extern char* dbhost;
extern char* dbuser;
extern char* dbport;
extern char* dbname;
extern int   rwtimeout;
extern int dbgetpassword;
extern char* replication_slot;
extern int standby_message_timeout;

extern char** tblspaceDirectory;
extern int tblspaceCount;
extern int tblspaceIndex;

/* Connection kept global so we can disconnect easily */
extern PGconn* conn;

extern void removeCreatedTblspace(void);

#define disconnect_and_exit(code) \
do {                              \
    if (conn != NULL)             \
        PQfinish(conn);           \
    removeCreatedTblspace();      \
    exit(code);                   \
} while(0)

#define GS_FREE(ptr)      \
    if (NULL != ptr) {    \
        free((char*)ptr); \
        ptr = NULL;       \
    }

#define pg_log(type, fmt, ...)                                                    \
do {                                                                              \
    time_t t = time(0);                                                           \
    char ch[64];                                                                  \
    tm tmStruct;                                                                  \
    strftime(ch, sizeof(ch), "%Y-%m-%d %H:%M:%S", localtime_r(&t, &tmStruct));    \
    fprintf(stderr, "[%s]:", ch);                                                 \
    fprintf(stderr, fmt, ##__VA_ARGS__);                                          \
} while(0)

#define pg_fatal(fmt, ...)                   \
do {                                         \
    fprintf(stderr, fmt, ##__VA_ARGS__);     \
    fflush(stdout);                          \
    abort();                                 \
} while(0)

char* xstrdup(const char* s);
void* xmalloc0(int size);

char* inc_dbport(const char* dbport);
PGconn* GetConnection(void);
void ClearAndFreePasswd(void);

extern TimestampTz feGetCurrentTimestamp(void);
extern void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time, long* secs, long* microsecs);
extern bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time, int msec);

#endif  // SRC_BIN_PG_BASEBACKUP_STREAMUTIL_H
