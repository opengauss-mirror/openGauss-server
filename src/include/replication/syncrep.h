/* -------------------------------------------------------------------------
 *
 * syncrep.h
 *	  Exports from replication/syncrep.c.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/replication/syncrep.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _SYNCREP_H
#define _SYNCREP_H

#include "access/xlogdefs.h"
#include "utils/guc.h"
#include "replication/replicainternal.h"

#define SyncRepRequested() \
    (g_instance.attr.attr_storage.max_wal_senders > 0 && \
    u_sess->attr.attr_storage.guc_synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)

/* SyncRepWaitMode */
#define SYNC_REP_NO_WAIT -1
#define SYNC_REP_WAIT_RECEIVE 0
#define SYNC_REP_WAIT_WRITE 1
#define SYNC_REP_WAIT_FLUSH 2
#define SYNC_REP_WAIT_APPLY 3

#define NUM_SYNC_REP_WAIT_MODE 4

/* syncRepState */
#define SYNC_REP_NOT_WAITING 0
#define SYNC_REP_WAITING 1
#define SYNC_REP_WAIT_COMPLETE 2

/* syncrep_method of SyncRepConfigData */
#define SYNC_REP_PRIORITY 0
#define SYNC_REP_QUORUM 1

#define SYNC_REP_MAX_GROUPS 256

extern volatile bool most_available_sync;

#define SyncStandbysDefined() \
    (u_sess->attr.attr_storage.SyncRepStandbyNames != NULL && u_sess->attr.attr_storage.SyncRepStandbyNames[0] != '\0')

#define GetWalsndSyncRepConfig(walsnder)  \
    (t_thrd.syncrep_cxt.SyncRepConfig[(walsnder)->sync_standby_group])

/*
 * Struct for the configuration of synchronous replication.
 *
 * Note: this must be a flat representation that can be held in a single
 * chunk of malloc'd memory, so that it can be stored as the "extra" data
 * for the synchronous_standby_names GUC.
 */
typedef struct SyncRepConfigData {
    int config_size;      /* total size of this struct, in bytes */
    int num_sync;         /* number of sync standbys that we need to wait for */
    uint8 syncrep_method; /* method to choose sync standbys */
    int nmembers;         /* number of members in the following list */
    /* member_names contains nmembers consecutive nul-terminated C strings */
    char member_names[FLEXIBLE_ARRAY_MEMBER];
} SyncRepConfigData;

/* called by user backend */
extern void SyncRepWaitForLSN(XLogRecPtr XactCommitLSN, bool enableHandleCancel = true);
extern bool SyncPaxosWaitForLSN(XLogRecPtr PaxosConsensusLSN);

/* called at backend exit */
extern void SyncRepCleanupAtProcExit(void);

/* called by wal sender */
extern void SyncRepInitConfig(void);
extern void SyncRepReleaseWaiters(void);
extern int SyncRepWakeQueue(bool all, int mode);
extern void SyncPaxosReleaseWaiters(XLogRecPtr PaxosConsensusLSN);

/* called by wal writer */
extern void SyncRepUpdateSyncStandbysDefined(void);

/* called by wal sender, check if any synchronous standby is alive */
extern void SyncRepCheckSyncStandbyAlive(void);

/* called by wal sender and user backend */
extern List* SyncRepGetSyncStandbys(bool* am_sync, List** catchup_standbys = NULL);

extern bool check_synchronous_standby_names(char** newval, void** extra, GucSource source);
extern void assign_synchronous_standby_names(const char* newval, void* extra);
extern void assign_synchronous_commit(int newval, void* extra);

/*
 * Internal functions for parsing synchronous_standby_names grammar,
 * in syncrep_gram.y and syncrep_scanner.l
 */
#define YYLTYPE int
typedef void* syncrep_scanner_yyscan_t;

typedef union syncrep_scanner_YYSTYPE {
    char* str;
    List* list;
    SyncRepConfigData* config;
} syncrep_scanner_YYSTYPE;

extern int syncrep_scanner_yylex(syncrep_scanner_YYSTYPE* lvalp, YYLTYPE* llocp, syncrep_scanner_yyscan_t yyscanner);
extern void syncrep_scanner_yyerror(const char* message, syncrep_scanner_yyscan_t yyscanner);
extern bool SyncRepGetSyncRecPtr(XLogRecPtr* receivePtr, XLogRecPtr* writePtr, XLogRecPtr* flushPtr, XLogRecPtr* replayPtr, bool* am_sync, bool check_am_sync = true);

#ifndef ENABLE_MULTIPLE_NODES
/*
 * Configuration file synchronization strategy
 * ALL_NODE : All standby nodes are allowed to send synchronous requests, and
 *            the host are allowed  to actively send configuration files to all standby nodes.
 * ONLY_SYNC_NODE : The standby nodes are allowed to send synchronization requests, and
 *                  the host only actively sends configuration files to the standby
 * NONE_NODE : No standby requests are allowed,
 *             and the host is not allowed to actively send configuration files to the standby.
*/
typedef enum {
    ALL_NODE,
    ONLY_SYNC_NODE,
    NONE_NODE
} Sync_Config_Strategy;
#endif

#endif /* _SYNCREP_H */
