/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * imcs_ctlg.h
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/htap/imcs_ctlg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IMCS_CTLG_H
#define IMCS_CTLG_H
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/htup.h"
#include "access/ustore/knl_upage.h"
#include "pgxc/pgxcnode.h"
#include "postmaster/bgworker.h"

#define MAX_IMCS_PAGES_ONE_CU 1024
#define MAX_IMCS_COL_LENGTH 8192
#define MAX_PARALLEL_WORK_NUMS (u_sess->attr.attr_common.imcs_parallel_populate_workers)
#define VIRTUAL_IMCS_CTID (-1)
#define TYPE_IMCSTORED 1
#define TYPE_UNIMCSTORED 2
#define TYPE_PARTITION_IMCSTORED 3
#define TYPE_PARTITION_UNIMCSTORED 4
#define TYPE_SS_IMCSTORED 5
#define TYPE_SS_IMCSTORE_SYNC_CU 6
#define CHECK_WALRCV_FREQ 1024
#define WALRCV_STATUS_UP 0
#define WALRCV_STATUS_DOWN 1
#define IMCSTORE_CACHE_UP 0
#define IMCSTORE_CACHE_DOWN 1
#define CHECK_IMCSTORE_CACHE_DOWN \
    (pg_atomic_read_u32(&g_instance.imcstore_cxt.is_imcstore_cache_down) == IMCSTORE_CACHE_DOWN)

#define MAX_IMCS_ROWS_ONE_CU(rel) \
    (((RelationIsAstoreFormat(rel)) ? MaxHeapTuplesPerPage : MaxUHeapTuplesPerPage(rel)) * MAX_IMCS_PAGES_ONE_CU)
#define IMCS_IS_PRIMARY_MODE (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE)
#define IMCS_IS_STANDBY_MODE (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE)
#define IMCS_IS_SS_MODE (ENABLE_DSS)

#define imcs_free_uheap_tuple(tup)                                             \
    do {                                                                       \
        if ((tup) != NULL && ((UHeapTuple)tup)->tupTableType == UHEAP_TUPLE) { \
            UHeapFreeTuple(tup);                                               \
        }                                                                      \
    } while (0)

typedef struct SendStandbyPopulateParams {
    Oid relOid;
    Oid partOid;
    int imcsNatts{0};
    int2 *attsNums;
    int msglen;
    int imcstoreType;
    int shmChunksNum{0};
} SendPopulateParams;

typedef struct ParseStandbyPopulateParams {
    int2vector* imcsAttsNum;
    int imcsNatts;
    XLogRecPtr currentLsn;
    int shmChunksNum{0};
} ParsePopulateParams;

typedef struct ImcstoreCtid {
    ItemPointerData ctid;
    uint16 reservedSpace;
} ImcstoreCtid;

/* for imcs parallel populate */
typedef struct IMCSPopulateSharedContext {
    Relation rel;
    int2vector* imcsAttsNum;
    int imcsNatts;
    TupleDesc imcsTupleDesc;
    uint32* curTotalScanBlks;
    pg_atomic_uint32 cuThreadId;
} PopulateSharedContext;

extern void CheckImcstoreCacheReady();

extern void CheckForSSMode(Relation rel, bool isShareMemory = false);

extern void CheckAndSetDBName();

extern void ResetDBNameIfNeed();

extern void CheckForEnableImcs(Relation rel, List* colList, int2vector* &imcsAttsNum, int* imcsNatts,
    Oid specifyPartOid = InvalidOid);

extern bool RelHasImcs(Oid relOid);

extern void CheckImcsSupportForRelType(Relation relation);

extern bool CheckIsInTrans();

extern void AbortIfSinglePrimary();

extern void CheckWalRcvIsRunning(uint32 nScan);

extern void CheckImcsSupportForDataTypes(Relation rel, List* colList, int2vector* &imcsAttsNum, int* imcsNatts);

extern void CheckForDataType(Oid typeOid, int32 typeMod);

extern void CreateImcsDescForPrimaryNode(Relation rel, int2vector* imcsAttsNum, int imcsNatts);

extern void AlterTableEnableImcstore(Relation rel, int2vector* imcsAttsNum, int imcsNatts, bool useShareMemroy = false);

extern void EnableImcstoreForRelation(Relation rel, int2vector* imcsAttsNum, int imcsNatts);

extern void PopulateImcs(Relation rel, int2vector* imcsAttsNum, int imcsNatts);

extern TupleDesc FormImcsTupleDesc(TupleDesc relTupleDesc, int2vector* imcsAttsNum, int imcsNatts);

extern void ParallelPopulateImcs(Relation rel, int2vector* imcsAttsNum, int imcsNatts);

extern void ParallelPopulateImcsMain(const BgWorkerContext *bwc);

extern void InitImcsParallelScan(
    PopulateSharedContext *shared, TableScanDesc scan, BlockNumber *start, BlockNumber *end);

extern PopulateSharedContext *ImcsInitShared(Relation rel, int2vector* imcsAttsNum, int imcsNatts, int* nworks);

extern void ImcsPopulateEndParallel();

extern void AlterTableDisableImcstore(Relation rel);

extern void UnPopulateImcs(Relation rel);

extern void PopulateImcsOnStandby(Oid relOid, StringInfo inputMsg);

extern void PopulateImcsForPartitionOnStandby(Oid relOid, Oid partOid, StringInfo inputMsg);

extern void UnPopulateImcsOnStandby(Oid relOid);

extern void UnPopulateImcsForPartitionOnStandby(Oid relOid, Oid partOid);

extern void ParsePopulateImcsParam(Oid relOid, StringInfo inputMsg, ParsePopulateParams& populateParams,
    bool shareMemory = false);

extern void SendImcstoredRequest(PGXCNodeHandle** connections, int connCount, const SendPopulateParams& populateParams);

extern void CopyTupleInfo(Tuple tuple, Datum* val, uint32 *blkno);

extern uint32 ImcsCeil(uint32 x, uint32 y);

extern Relation ParallelImcsOpenRelation(Relation rel);

extern void ParallelImcsCloseRelation(Relation rel);

extern Oid ImcsPartNameGetPartOid(Oid relOid, const char* partName);

extern void DropImcsForPartitionedRelIfNeed(Relation partitionedRel);

extern void WaitXLogRedoToCurrentLsn(XLogRecPtr currentLsn);

extern void PopulateImcsOnSSReadNode(Oid relOid, StringInfo inputMsg);

extern void SyncCUForSSImcstore(PGXCNodeHandle **connection, int connCount, Oid relOid);

extern void SendSyncCURequestsForSS(
    PGXCNodeHandle **connections, int connCount, CUDesc *cuDesc, CU *cuPtr, int imcsColId, Oid relOid);

extern void SSImcstoreCacheRemoteCU(Oid relOid, StringInfo inputMsg);

extern void ParseRemoteCU(int &imcsColId, CU *cuPtr, CUDesc *cuDescPtr, StringInfo inputMsg);

extern void PackStandbyPopulateParams(
    SendPopulateParams &populateParams, Oid relOid, Oid partOid, int2* attsNums, int imcsNatts, int type);

void SqlExecImcstored(Relation rel, List* colList);

void SqlExecImcstoredWithShm(Relation rel, List* colList);

void SqlExecUnImcstored(Relation rel);

void SqlExecModifyPartitionImcstored(Relation rel, const char* partName, List* colList);

void SqlExecModifyPartitionUnImcstored(Relation rel, const char* partName);

#endif /* IMCS_CTLG_H */