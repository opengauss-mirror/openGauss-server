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
 * pagerepair.h
 *        Data struct to store pagerepair thread variables.
 * 
 * 
 * IDENTIFICATION
 *        src/include/postmaster/pagerepair.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _PAGEREPAIR_H
#define _PAGEREPAIR_H

#include "access/xlog.h"
#include "access/xlog_basic.h"
#include "gs_thread.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "storage/smgr/relfilenode.h"

typedef uint64 XLogRecPtr;

const int SEGLEN = 20;
const int MAX_REPAIR_PAGE_NUM = 1000;

typedef enum {
    CRC_CHECK_FAIL = 0,
    LSN_CHECK_FAIL
} PageErrorType;

typedef enum {
    WAIT_REMOTE_READ = 0,      /* the page need read from primary */
    WAIT_LSN_CHECK,         /* the page read finish, wait do lsn check */
    WAIT_REPAIR
} RepairPageState;

typedef enum {
    WAIT_FILE_CHECK_REPAIR = 0,   /* Waiting to determine if the file repair is required */
    WAIT_FILE_REMOTE_READ,        /* the file need read from primary */
    WAIT_FILE_REPAIR,           /* wait the standby replay lsn greater than file lsn from primary */
    WAIT_FILE_REPAIR_SEGMENT,
    WAIT_RENAME,
    WAIT_FOREGT_INVALID_PAGE,   /* only segment file need startup thread handle forget invalid page again */
    WAIT_CLEAN                 /* primary this file also not exits, wait clean by some option (drop table etc.) */
} RepairFileState;

typedef enum {
    NEED_CONTINUE_CHECK = 0,   /* need check next record */
    CHECK_SUCCESS,
    CHECK_FAIL
} BlockLsnState;

typedef struct RepairBlockKey {
    RelFileNode relfilenode;
    ForkNumber forknum;
    BlockNumber blocknum;
} RepairBlockKey;

typedef struct RepairBlockEntry {
    RepairBlockKey key;
    ThreadId recovery_tid;  /* recovery thread id, when the page can recovery, need tell the recovery thread */
    PageErrorType error_type;    /* crc error or lsn check error */
    RepairPageState page_state;  /* page current state */
    XLogRecPtr page_old_lsn;     /* lsn check error, current page lsn */
    XLogRecPtr page_new_lsn;     /* page lsn of new page */
    XLogPhyBlock pblk;            /* physical location for segment-page storage */
    char page_content[BLCKSZ];    /* new page from primary */
} RepairBlockEntry;

typedef struct RepairFileKey {
    RelFileNode relfilenode;
    ForkNumber forknum;
    BlockNumber segno;
} RepairFileKey;

typedef struct RemoteReadFileKey {
    RelFileNode relfilenode;
    ForkNumber forknum;
    BlockNumber blockstart;
} RemoteReadFileKey;

typedef struct RepairFileEntry {
    RepairFileKey key;
    XLogRecPtr min_recovery_point;   /* min_recovery_point of found the file not exists */
    RepairFileState file_state;     /* file current state */
    XLogRecPtr primary_file_lsn;   /* LSN of from the primary DN read file finish */
} RepairFileEntry;

extern void PageRepairMain(void);
extern void PageRepairHashTblInit(void);
extern void FileRepairHashTblInit(void);
extern void ClearPageRepairTheadMem(void);
extern bool CheckRepairPage(RepairBlockKey key, XLogRecPtr min_lsn, XLogRecPtr max_lsn, char *page);
extern bool BlockNodeMatch(RepairBlockKey key, XLogPhyBlock pblk, RelFileNode node, ForkNumber forknum,
    BlockNumber minblkno, bool segment_shrink);
extern bool dbNodeandSpcNodeMatch(RelFileNode *rnode, Oid spcNode, Oid dbNode);
extern void BatchClearPageRepairHashTbl(Oid spcNode, Oid dbNode);
extern void ClearPageRepairHashTbl(const RelFileNode &node, ForkNumber forknum, BlockNumber minblkno,
    bool segment_shrink);
extern void ClearSpecificsPageRepairHashTbl(RepairBlockKey key);
extern bool PushBadPageToRemoteHashTbl(RepairBlockKey key, PageErrorType error_type, XLogRecPtr old_lsn,
    XLogPhyBlock pblk, ThreadId tid);
extern void BatchClearBadFileHashTbl(Oid spcNode, Oid dbNode);
extern void ClearBadFileHashTbl(const RelFileNode &node, ForkNumber forknum, uint32 segno);
extern void CheckNeedRenameFile();
extern void CheckIsStopRecovery(void);
extern int CreateRepairFile(char *path);
extern int WriteRepairFile(int fd, char* path, char *buf, uint32 offset, uint32 size);
extern void CheckNeedRecordBadFile(RepairFileKey key, uint32 nblock, uint32 blocknum,
    const XLogPhyBlock *pblk);
extern bool CheckFileRepairHashTbl(RelFileNode rnode, ForkNumber forknum, uint32 segno);
extern void df_clear_and_close_all_file(RepairFileKey key, int32 max_sliceno);
extern void df_open_all_file(RepairFileKey key, int32 max_sliceno);

inline bool IsPrimaryClusterStandbyDN()
{
    load_server_mode();
    /* Standby DN or RecoveryInProgress DN of the primary cluster or a single cluster */
    if (g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_PRIMARY &&
        g_instance.attr.attr_common.stream_cluster_run_mode == RUN_MODE_PRIMARY &&
        (!t_thrd.xlog_cxt.is_hadr_main_standby && !t_thrd.xlog_cxt.is_cascade_standby) &&
        (t_thrd.xlog_cxt.server_mode == STANDBY_MODE || t_thrd.xlog_cxt.server_mode == PENDING_MODE)) {
        return true;
    }

    return false;
}

#define CheckVerionSupportRepair() (t_thrd.proc->workingVersionNum >= SUPPORT_DATA_REPAIR)

#endif /* _PAGEREPAIR_H */