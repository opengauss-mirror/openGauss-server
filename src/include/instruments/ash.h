/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * ash.h
 *
 * IDENTIFICATION
 *        src/include/instruments/ash.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ASH_H
#define ASH_H
#include "gs_thread.h"
#include "pgstat.h"

#define ENABLE_ASP u_sess->attr.attr_common.enable_asp
typedef struct SessionHistEntry {
    uint64 changCount;
    uint64 sample_id;
    /* time stamp of every case */
    TimestampTz sample_time;
    /* Whether the sample has been flushed to wdr */
    bool need_flush_sample;

    uint64  session_id;
    TimestampTz start_time;
    bool is_flushed_sample;
    uint64 psessionid;
    /* Database OID, owning user's OID, connection client address */
    Oid databaseid;
    Oid userid;
    SockAddr clientaddr;
    char* clienthostname; /* MUST be null-terminated */

    /* application name; MUST be null-terminated */
    char* st_appname;

    uint64 queryid;  /* debug query id of current query */
    UniqueSQLKey unique_sql_key; /* get unique sql key */
    ThreadId procpid; /* The entry is valid iff st_procpid > 0, unused if st_procpid == 0 */
    pid_t tid;
    int thread_level;   /* thread level, mark with plan node id of Stream node */
    uint32 smpid;          /* smp worker id, used for parallel execution */
    WaitState waitstatus;            /* backend waiting states */
    uint32 waitevent;      /* backend's wait event */
    uint64 xid;                      /* for transaction id, fit for 64-bit */
    int waitnode_count;              /* count of waiting nodes */
    int nodeid;                      /* maybe for nodeoid/nodeidx */
    int plannodeid;                  /* indentify which consumer is receiving data for SCTP */
    char* relname;                   /* relation name, for analyze, vacuum, .etc.*/
    Oid libpq_wait_nodeid;           /* for libpq, point to libpq_wait_node*/
    int libpq_wait_nodecount;        /* for libpq, point to libpq_wait_nodecount*/
    WaitStatePhase waitstatus_phase; /* detailed phase for wait status, now only for 'wait node' status */
    int numnodes;                    /* nodes number when reporting waitstatus in case it changed */
    LOCALLOCKTAG locallocktag;       /* locked object */
    uint64 st_block_sessionid;       /* block session */
    GlobalSessionId globalSessionId;
} SessionHistEntry;

typedef struct ActiveSessHistArrary {
    uint32 curr_index;      /* the current index of active session history arrary */
    uint32 max_size;        /* the max size of the active_sess_hist_arrary */
    SessionHistEntry *active_sess_hist_info;
} ActiveSessHistArrary;

void InitAsp();
extern ThreadId ash_start(void);
extern void ActiveSessionCollectMain();
extern bool IsJobAspProcess(void);
#endif

