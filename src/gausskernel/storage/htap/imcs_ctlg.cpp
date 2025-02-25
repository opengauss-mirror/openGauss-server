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
 * imcs_ctlg.cpp
 *        routines to support IMColStore
 *
 *
 * IDENTIFICATION
 *        src/include/storage/htap/imcs_ctlg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "commands/dbcommands.h"
#include "access/htap/imcucache_mgr.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "libpq/libpq-int.h"
#include "utils/guc_storage.h"
#include "pgxc/execRemote.h"
#include "libpq/libpq.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "access/htap/imcstore_insert.h"
#include "access/htap/imcs_ctlg.h"

void CheckImcstoreCacheReady()
{
    if (CHECK_IMCSTORE_CACHE_DOWN) {
        ereport(ERROR, (errmsg("Imcstore Cache is recovering, please wait or restart database.")));
    }
}

bool CheckDBName(const char* dbname)
{
    if (strcmp(dbname, get_database_name(u_sess->proc_cxt.MyDatabaseId)) != 0) {
        return false;
    } else {
        pg_atomic_add_fetch_u32(&g_instance.imcstore_cxt.dbname_reference_count, 1);
        return true;
    }
}

void CheckAndSetDBName()
{
    bool checkSuccess = true;
    uint32 dbnameRefCount = 0;
    char* dbname = nullptr;

    pthread_rwlock_rdlock(&g_instance.imcstore_cxt.context_mutex);
    dbnameRefCount = pg_atomic_read_u32(&g_instance.imcstore_cxt.dbname_reference_count);
    /* means that imcstore tables exist */
    if (dbnameRefCount > 0) {
        dbname = g_instance.imcstore_cxt.dbname;
        checkSuccess = CheckDBName(dbname);
    }
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);

    if (!checkSuccess) {
        ereport(ERROR, (errmsg("try populate a table locate in a different database,"
            "please populate in the database: %s.", dbname)));
    }
    if (dbnameRefCount > 0) {
        return;
    }

    /* dbnameRefCount == 0, means the first imcstore table, need to set dbname */
    pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
    dbnameRefCount = pg_atomic_read_u32(&g_instance.imcstore_cxt.dbname_reference_count);
    if (dbnameRefCount == 0) {
        pg_atomic_add_fetch_u32(&g_instance.imcstore_cxt.dbname_reference_count, 1);
        g_instance.imcstore_cxt.dboid = u_sess->proc_cxt.MyDatabaseId;
        g_instance.imcstore_cxt.dbname = pg_strdup(get_database_name(u_sess->proc_cxt.MyDatabaseId));
        if (IMCS_IS_PRIMARY_MODE) {
            ereport(LOG, (errmsg("HTAP: Set DB name: %s.", g_instance.imcstore_cxt.dbname)));
        } else {
            SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        }
    } else {
        dbname = g_instance.imcstore_cxt.dbname;
        checkSuccess = CheckDBName(dbname);
    }
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);

    if (!checkSuccess) {
        ereport(ERROR, (errmsg("try populate a table locate in a different database,"
            "please populate in the database: %s.", dbname)));
    }
}

void ResetDBNameIfNeed()
{
    pthread_rwlock_wrlock(&g_instance.imcstore_cxt.context_mutex);
    if (pg_atomic_sub_fetch_u32(&g_instance.imcstore_cxt.dbname_reference_count, 1) == 0) {
        if (!IMCS_IS_PRIMARY_MODE) {
            g_instance.imcstore_cxt.should_clean = true;
            SetLatch(&g_instance.imcstore_cxt.vacuum_latch);
        }
        ereport(LOG, (errmsg("No imcstore tables left, cur DB name: %s, Reset it.", g_instance.imcstore_cxt.dbname)));
        g_instance.imcstore_cxt.dboid = InvalidOid;
        g_instance.imcstore_cxt.dbname = nullptr;
    }
    pthread_rwlock_unlock(&g_instance.imcstore_cxt.context_mutex);
}

void CheckForEnableImcs(Relation rel, List* colList, int2vector* &imcsAttsNum, int* imcsNatts, Oid specifyPartOid)
{
    Oid relOid = RelationGetRelid(rel);
    if (OidIsValid(specifyPartOid)) {
        if (RelHasImcs(specifyPartOid)) {
            ereport(ERROR, (errmsg("partition %d of rel %d has been populated, please unpopulate first.",
                specifyPartOid, relOid)));
        }
    } else if (RelHasImcs(relOid)) {
        ereport(ERROR, (errmsg("rel %d has been populated, please unpopulate first.", relOid)));
    }

    if (CheckIsInTrans()) {
        ereport(ERROR, (errmsg("can not populate in transation block for HTAP.")));
    }

    CheckImcsSupportForRelType(rel);

    CheckImcsSupportForDataTypes(rel, colList, imcsAttsNum, imcsNatts);
}

bool RelHasImcs(Oid relOid)
{
    if (IMCU_CACHE->GetImcsDesc(relOid) != NULL) {
        return true;
    }
    return false;
}

void CheckImcsSupportForRelType(Relation relation)
{
    if (IsSystemRelation(relation) || IsCatalogRelation(relation) || IsToastRelation(relation) ||
        RelationIsToast(relation) || isAnyTempNamespace(RelationGetNamespace(relation)) || RELATION_IS_TEMP(relation) ||
        RelationGetRelPersistence(relation) == RELPERSISTENCE_UNLOGGED || relation->rd_isblockchain) {
        ereport(ERROR, (errmsg("Table type not support for HTAP.")));
    }

    if (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE && relation->storage_type == SEGMENT_PAGE) {
        ereport(ERROR, (errmsg("Segment table standby read is not yet supported.")));
    }
}

bool CheckIsInTrans()
{
    if (u_sess->SPI_cxt._current != NULL || IsTransactionInProgressState()) {
        return true;
    }
    return false;
}

void AbortIfSinglePrimary()
{
    SyncRepStandbyData *syncStandbys;
    int numStandbys = SyncRepGetSyncStandbys(&syncStandbys);
    if (syncStandbys != NULL) {
        pfree_ext(syncStandbys);
    }
    if (numStandbys == 0) {
        ereport(ERROR, (errmsg("Single primary can not populate or unpopulate.")));
    }
}

void CheckWalRcvIsRunning(uint32 nScan)
{
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        if (pg_atomic_read_u32(&g_instance.imcstore_cxt.is_walrcv_down) == WALRCV_STATUS_DOWN) {
            ereport(ERROR, (errmsg("HTAP parallel populate error, walreceiver is not running, is_walrcv_down: %d",
                WALRCV_STATUS_DOWN)));
        }
        if (nScan % CHECK_WALRCV_FREQ == 0 && !WalRcvIsRunning()) {
            pg_atomic_write_u32(&g_instance.imcstore_cxt.is_walrcv_down, WALRCV_STATUS_DOWN);
            ereport(ERROR, (errmsg("HTAP parallel populate error, walreceiver is not running, is_walrcv_down: %d",
                WALRCV_STATUS_DOWN)));
        }
    }
}

static FORCE_INLINE int CompareAttrNumberFunc(const void *left, const void *right)
{
    return (*(const int2 *)left) - (*(const int2 *)right);
}

static FORCE_INLINE void DeDuplicateAttrNumber(int2* sortedAttsNums, int *colNum)
{
    Assert(sortedAttsNums && colNum && *colNum > 0);
    if (*colNum == 1) {
        return;
    }

    int curr = 0;
    for (int i = 0; i < *colNum; i++) {
        if (sortedAttsNums[curr] == sortedAttsNums[i]) {
            continue;
        }
        ++curr;
        sortedAttsNums[curr] = sortedAttsNums[i];
    }
    *colNum = curr + 1;
}


int32 TypeMaximumSize(Oid type_oid, int32 typemod)
{
    if (typemod < 0)
        return -1;

    switch (type_oid) {
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
            /* typemod includes varlena header */

            /* typemod is in characters not bytes */
            return (typemod - VARHDRSZ) * pg_encoding_max_length(GetDatabaseEncoding()) + VARHDRSZ;

        case NUMERICOID:
            return numeric_maximum_size(typemod);

        case VARBITOID:
        case BITOID:
            /* typemod is the (max) number of bits */
            return (typemod + (BITS_PER_BYTE - 1)) / BITS_PER_BYTE + 2 * sizeof(int32);
        default:
            break;
    }

    /* Unknown type, or unlimited-width type such as 'text' */
    return -1;
}

void CheckForAttrLen(Oid relOid, FormData_pg_attribute* att)
{
    if (att->attlen > 0) {
        /* Fixed-length types are never over maxlen */
        return;
    }

    int32 maxlen = TypeMaximumSize(att->atttypid, att->atttypmod);
    if (maxlen < 0) {
        ereport(ERROR, (errmsg("Max attr length of Rel [%d]: col [%s] is unknow, not supported by imcstore.",
            relOid, att->attname.data)));
    }
    if (maxlen > MAX_IMCS_COL_LENGTH) {
        ereport(ERROR, (errmsg("Max attr length [%d] of Rel [%d]: col [%s] exceeded imcs col max length: %d.",
            maxlen, relOid, att->attname.data, MAX_IMCS_COL_LENGTH)));
    }
}

void CheckImcsSupportForDataTypes(Relation rel, List* colList, int2vector* &imcsAttsNum, int* imcsNatts)
{
    /* check if specify cols, yes when colCnt != 0 */
    int imcsColCnt = 0;
    int i = 0;
    int2* attsNums = NULL;
    ListCell* cell = NULL;
    Oid relOid = RelationGetRelid(rel);
    foreach(cell, colList) {
        imcsColCnt++;
    }

    FormData_pg_attribute *relAtts = rel->rd_att->attrs;
    /* only populate specified cols */
    if (imcsColCnt != 0) {
        attsNums = (int2*)palloc(sizeof(int2*) * imcsColCnt);
        foreach(cell, colList) {
            char* colName = strVal(lfirst(cell));
            AttrNumber attnumber = get_attnum(relOid, colName);
            if (!AttributeNumberIsValid(attnumber)) {
                ereport(ERROR, (errmsg("Col %s not exist in rel %d.", colName, relOid)));
            }
            CheckForDataType(relAtts[attnumber - 1].atttypid, relAtts[attnumber - 1].atttypmod);
            CheckForAttrLen(relOid, &relAtts[attnumber - 1]);
            *(attsNums + i) = attnumber;
            i++;
        }
        qsort(attsNums, (size_t)imcsColCnt, sizeof(int2), CompareAttrNumberFunc);
        DeDuplicateAttrNumber(attsNums, &imcsColCnt);
    } else {
        /* populate all cols */
        int natts = rel->rd_att->natts;
        for (i = 0; i < natts; i++) {
            if (relAtts[i].attisdropped) {
                continue;
            }
            CheckForDataType(relAtts[i].atttypid, relAtts[i].atttypmod);
            CheckForAttrLen(relOid, &relAtts[i]);
            imcsColCnt++;
        }

        if (imcsColCnt == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("all cols of rel %d  are dropped, not supported for HTAP", relOid)));
        }

        int j = 0;
        attsNums = (int2*)palloc(sizeof(int2*) * imcsColCnt);
        for (i = 0; i < natts; i++) {
            *(attsNums + j) = relAtts[i].attnum;
            j++;
        }
    }

    imcsAttsNum = buildint2vector(attsNums, imcsColCnt);
    *imcsNatts = imcsColCnt;
    pfree(attsNums);
}

void CheckForDataType(Oid typeOid, int32 typeMod)
{
    if (!IsTypeSupportedByCStore(typeOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("type \"%s\" is not supported for HTAP",
                    format_type_with_typemod(typeOid, typeMod))));
    }
}

void CreateImcsDescForPrimaryNode(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    /* create imcsdesc for rel */
    IMCU_CACHE->CreateImcsDesc(rel, imcsAttsNum, imcsNatts);
    /* rel is partitioned table, all patitions alse need to create imcsdesc */
    if (RelationIsPartitioned(rel)) {
        Relation partRel = NULL;
        Partition partition = NULL;
        ListCell* cell = NULL;
        List* partitions = relationGetPartitionList(rel, NoLock);
        foreach (cell, partitions) {
            partition = (Partition)lfirst(cell);
            partRel = partitionGetRelation(rel, partition);
            /* create imcsdesc for partiton */
            CreateImcsDescForPrimaryNode(partRel, imcsAttsNum, imcsNatts);
            releaseDummyRelation(&partRel);
        }
        releasePartitionList(rel, &partitions, NoLock);
    }
}

void AlterTableEnableImcstore(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    /* 1. create imcsdesc for rel */
    IMCU_CACHE->CreateImcsDesc(rel, imcsAttsNum, imcsNatts);
    PG_TRY();
    {
        /* 2. partitioned rel, need to populate all partitions */
        if (RelationIsPartitioned(rel)) {
            Relation partRel = NULL;
            Partition partition = NULL;
            ListCell* cell = NULL;
            List* partitions = relationGetPartitionList(rel, NoLock);
            foreach (cell, partitions) {
                partition = (Partition)lfirst(cell);
                partRel = partitionGetRelation(rel, partition);
				/* start to populate partition */
                AlterTableEnableImcstore(partRel, imcsAttsNum, imcsNatts);
                releaseDummyRelation(&partRel);
            }
            releasePartitionList(rel, &partitions, NoLock);
        } else {
            /* 3. not partition rel, start populate rel */
            EnableImcstoreForRelation(rel, imcsAttsNum, imcsNatts);
        }
        /* 4. update status */
        IMCU_CACHE->UpdateImcsStatus(RelationGetRelid(rel), IMCS_POPULATE_COMPLETE);
    }
    PG_CATCH();
    {
        if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
            pg_atomic_read_u32(&g_instance.imcstore_cxt.is_walrcv_down) == WALRCV_STATUS_DOWN) {
            pg_atomic_write_u32(&g_instance.imcstore_cxt.is_walrcv_down, WALRCV_STATUS_UP);
            IMCUDataCacheMgr::ResetInstance();
            PG_RE_THROW();
        }
        IMCU_CACHE->UpdateImcsStatus(RelationGetRelid(rel), IMCS_POPULATE_ERROR);
        IMCU_CACHE->ClearImcsMem(RelationGetRelid(rel), &(rel->rd_node));
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void EnableImcstoreForRelation(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    if (u_sess->attr.attr_common.enable_parallel_populate) {
        ParallelPopulateImcs(rel, imcsAttsNum, imcsNatts);
    } else {
        PopulateImcs(rel, imcsAttsNum, imcsNatts);
    }
}

void PopulateImcs(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    /* no data, no need to populate */
    if (RelationGetNumberOfBlocks(rel) == 0) {
        return;
    }
    Tuple tuple = NULL;
    uint32 blkno = 0;
    uint32 cuid = 0;

    /* for scan row data */
    TupleDesc relTupleDesc = rel->rd_att;
    TableScanDesc scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    Datum* val = (Datum*)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool* null = (bool*)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    /* form TupleDesc for imcstore */
    TupleDesc imcsTupleDesc = FormImcsTupleDesc(relTupleDesc, imcsAttsNum, imcsNatts);

    /* init imcstoreInsert */
    IMCStoreInsert imcstoreInsert(rel, imcsTupleDesc, imcsAttsNum);

    PG_TRY();
    {
        if (g_instance.attr.attr_memory.enable_borrow_memory) {
            IMCSDesc *imcsDesc = IMCU_CACHE->GetImcsDesc(RelationGetRelid(rel));
            u_sess->imcstore_ctx.pinnedBorrowMemPool = imcsDesc->borrowMemPool;
        }
        while ((tuple = tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
            null[relTupleDesc->natts] = false;
            CopyTupleInfo(tuple, &val[relTupleDesc->natts], &blkno);

            uint32 currCu = blkno / MAX_IMCS_PAGES_ONE_CU;
            Assert(currCu >= cuid);
            if (currCu > cuid) {
                imcstoreInsert.BatchInsertCommon(cuid);
                cuid = currCu;
                imcstoreInsert.ResetBatchRows(true);
            }
            imcstoreInsert.AppendOneTuple(val, null);
            imcs_free_uheap_tuple(tuple);
        }

        /* make sure that last batch data is inserted */
        imcstoreInsert.BatchInsertCommon(cuid);
        imcstoreInsert.ResetBatchRows(true);
    }
    PG_CATCH();
    {
        tableam_scan_end(scan);
        pfree(val);
        pfree(null);
        imcstoreInsert.Destroy();
        PG_RE_THROW();
    }
    PG_END_TRY();

    tableam_scan_end(scan);
    pfree(val);
    pfree(null);
    imcstoreInsert.Destroy();
}

TupleDesc FormImcsTupleDesc(TupleDesc relTupleDesc, int2vector* imcsAttsNum, int imcsNatts)
{
    Assert(imcsAttsNum != NULL && imcsNatts > 0);
    errno_t rc = EOK;
    TupleDesc imcsTupleDesc = CreateTemplateTupleDesc(imcsNatts + 1, false);
    TupleDescInitEntry(imcsTupleDesc, imcsNatts + 1, "ctid", TIDOID, -1, 0);
    imcsTupleDesc->tdtypeid = relTupleDesc->tdtypeid;
    imcsTupleDesc->tdtypmod = relTupleDesc->tdtypmod;
    imcsTupleDesc->tdisredistable = relTupleDesc->tdisredistable;

    for (int i = 0; i < imcsNatts; i++) {
        AttrNumber attNum = imcsAttsNum->values[i];
        rc = memcpy_s(&imcsTupleDesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE,
            &relTupleDesc->attrs[attNum - 1], ATTRIBUTE_FIXED_PART_SIZE);
        securec_check_c(rc, "\0", "\0");
        imcsTupleDesc->attrs[i].attnotnull = false;
        imcsTupleDesc->attrs[i].atthasdef = false;
    }

    imcsTupleDesc->attrs[imcsNatts].attnotnull = false;
    imcsTupleDesc->attrs[imcsNatts].atthasdef = false;
    imcsTupleDesc->attrs[imcsNatts].attnum = VIRTUAL_IMCS_CTID;
    imcsTupleDesc->attrs[imcsNatts].attlen = sizeof(ImcstoreCtid);
    return imcsTupleDesc;
}

void ParallelPopulateImcs(Relation rel, int2vector* imcsAttsNum, int imcsNatts)
{
    if (RelationGetNumberOfBlocks(rel) == 0) {
        /* no data, no need to populate */
        return;
    }
    int nworkers;
    IMCSPopulateSharedContext* shared = ImcsInitShared(rel, imcsAttsNum, imcsNatts, &nworkers);

    int successWorkers = LaunchBackgroundWorkers(nworkers, shared, ParallelPopulateImcsMain, NULL);
    if (successWorkers == 0) {
        pfree_ext(shared);
        ereport(ERROR, (errmsg("Parallel populate error, all workers failed.")));
    }

    PG_TRY();
    {
        BgworkerListWaitFinish(&successWorkers);
    }
    PG_CATCH();
    {
        ImcsPopulateEndParallel();
        PG_RE_THROW();
    }
    PG_END_TRY();

    ImcsPopulateEndParallel();
}

void ParallelPopulateImcsMain(const BgWorkerContext *bwc)
{
    Tuple tuple = NULL;
    uint32 blkno = 0;
    uint32 cuid;
    uint32 curStartBlock;
    uint32 curEndBlock;
    uint32 preBlkno;
    uint32 nScan = 0;

    PopulateSharedContext *shared = (PopulateSharedContext *)bwc->bgshared;
    Relation rel = ParallelImcsOpenRelation(shared->rel);
    TableScanDesc scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    InitImcsParallelScan(shared, scan, &curStartBlock, &curEndBlock);
    /* init cuid in cur thread */
    cuid = curStartBlock / MAX_IMCS_PAGES_ONE_CU;
    preBlkno = curStartBlock;

    /* for scan row data */
    TupleDesc relTupleDesc = rel->rd_att;
    Datum* val = (Datum*)palloc(sizeof(Datum) * (relTupleDesc->natts + 1));
    bool* null = (bool*)palloc(sizeof(bool) * (relTupleDesc->natts + 1));

    /* init imcstoreInsert */
    IMCStoreInsert imcstoreInsert(rel, shared->imcsTupleDesc, shared->imcsAttsNum);
    PG_TRY();
    {
        if (g_instance.attr.attr_memory.enable_borrow_memory) {
            IMCSDesc *imcsDesc = IMCU_CACHE->GetImcsDesc(RelationGetRelid(rel));
            u_sess->imcstore_ctx.pinnedBorrowMemPool = imcsDesc->borrowMemPool;
        }
        while ((tuple = tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            tableam_tops_deform_tuple(tuple, relTupleDesc, val, null);
            null[relTupleDesc->natts] = false;
            CopyTupleInfo(tuple, &val[relTupleDesc->natts], &blkno);

            /* check if current thread read all blocks */
            if (blkno < preBlkno || blkno >= curEndBlock) {
                break;
            } else {
                preBlkno = blkno;
            }

            uint32 currCu = blkno / MAX_IMCS_PAGES_ONE_CU;
            Assert(currCu >= cuid);
            if (currCu > cuid) {
                imcstoreInsert.BatchInsertCommon(cuid);
                cuid = currCu;
                imcstoreInsert.ResetBatchRows(true);
            }
            imcstoreInsert.AppendOneTuple(val, null);
            imcs_free_uheap_tuple(tuple);

            CheckWalRcvIsRunning(nScan++);
        }

        /* make sure that last batch data is inserted */
        imcstoreInsert.BatchInsertCommon(cuid);
        imcstoreInsert.ResetBatchRows(true);
    }
    PG_CATCH();
    {
        tableam_scan_end(scan);
        ParallelImcsCloseRelation(rel);
        pfree(val);
        pfree(null);
        imcstoreInsert.Destroy();
        PG_RE_THROW();
    }
    PG_END_TRY();

    tableam_scan_end(scan);
    ParallelImcsCloseRelation(rel);
    pfree(val);
    pfree(null);
    imcstoreInsert.Destroy();
}

void InitImcsParallelScan(PopulateSharedContext *shared, TableScanDesc scan, BlockNumber *start, BlockNumber *end)
{
    uint32 curThreadId = pg_atomic_add_fetch_u32(&shared->cuThreadId, 1);
    Assert(curThreadId > 0);
    BlockNumber curStartBlock = *(shared->curTotalScanBlks + curThreadId - 1);
    *start = curStartBlock;
    *end = *(shared->curTotalScanBlks + curThreadId);

    scan->rs_startblock = curStartBlock;
    scan->rs_flags &= ~SO_ALLOW_SYNC;
    scan->rs_syncscan = false;
}

PopulateSharedContext *ImcsInitShared(Relation rel, int2vector* imcsAttsNum, int imcsNatts, int* nworkers)
{
    uint32 totalBlks = RelationGetNumberOfBlocks(rel);
    Assert(totalBlks > 0);

    PopulateSharedContext *shared = (PopulateSharedContext *)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(PopulateSharedContext));

    shared->rel = rel;
    shared->imcsAttsNum = imcsAttsNum;
    shared->imcsNatts = imcsNatts;
    shared->imcsTupleDesc = FormImcsTupleDesc(rel->rd_att, imcsAttsNum, imcsNatts);
    pg_atomic_init_u32(&shared->cuThreadId, 0);
    shared->curTotalScanBlks = (uint32*)palloc0(sizeof(uint32) * (MAX_PARALLEL_WORK_NUMS + 1));

    /* calculate the blocks sum of threads */
    uint32 rowGroupNums = ImcsCeil(totalBlks, MAX_IMCS_PAGES_ONE_CU);
    uint32 baseRGsPerWorker = rowGroupNums / MAX_PARALLEL_WORK_NUMS;
    int restRGs = rowGroupNums % MAX_PARALLEL_WORK_NUMS;
    int workers = rowGroupNums > MAX_PARALLEL_WORK_NUMS ? MAX_PARALLEL_WORK_NUMS : rowGroupNums;

    for (int i = 1; i <= workers; i++) {
        int restRG = restRGs > 0 ? 1 : 0;
        uint32 preScanBlocks = *(shared->curTotalScanBlks + i - 1);
        uint32 curScanBlks = (baseRGsPerWorker + restRG) * MAX_IMCS_PAGES_ONE_CU;
        /* blocks sum of last thread must be total block of relation  */
        *(shared->curTotalScanBlks + i) = preScanBlocks + curScanBlks > totalBlks
                                          ? totalBlks
                                          : preScanBlocks + curScanBlks;
        restRGs--;
    }
    *nworkers = workers;
    return shared;
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
void ImcsPopulateEndParallel()
{
    BgworkerListSyncQuit();
}

void AlterTableDisableImcstore(Relation rel)
{
    /* rel is partitioned table, unpopulate all partitions */
    if (RelationIsPartitioned(rel)) {
        Relation partRel = NULL;
        Partition partition = NULL;
        ListCell* partcell = NULL;
        List* partitions = relationGetPartitionList(rel, NoLock);

        foreach (partcell, partitions) {
            partition = (Partition)lfirst(partcell);
            partRel = partitionGetRelation(rel, partition);
            /* partRel is subpartitioned table, unpopulate all subpartitions */
            if (RelationIsPartitioned(partRel)) {
                Relation subpartRel = NULL;
                Partition subpartition = NULL;
                ListCell* subpartcell = NULL;
                List* subpartitions = relationGetPartitionList(partRel, NoLock);

                foreach (subpartcell, subpartitions) {
                    subpartition = (Partition)lfirst(subpartcell);
                    subpartRel = partitionGetRelation(partRel, subpartition);
                    /* unpopulate subpart rel */
                    UnPopulateImcs(subpartRel);
                    releaseDummyRelation(&subpartRel);
                }
                releasePartitionList(partRel, &subpartitions, NoLock);
            }
            /* unpopulate part rel */
            UnPopulateImcs(partRel);
            releaseDummyRelation(&partRel);
        }
        releasePartitionList(rel, &partitions, NoLock);
    }
    /* unpopulate rel */
    UnPopulateImcs(rel);
}

void UnPopulateImcs(Relation rel)
{
    RelFileNode* relNode = t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE
                          ? NULL
                          : &(rel->rd_node);
    PG_TRY();
    {
        IMCU_CACHE->DeleteImcsDesc(RelationGetRelid(rel), relNode);
    }
    PG_CATCH();
    {
        IMCU_CACHE->UpdateImcsStatus(RelationGetRelid(rel), IMCS_POPULATE_ERROR);
    }
    PG_END_TRY();
}

void PopulateImcsOnStandby(Oid relOid, StringInfo inputMsg)
{
    int imcsNatts = 0;
    int2vector* imcsAtts = NULL;
    XLogRecPtr currentLsn = InvalidXLogRecPtr;
    ParsePopulateImcsParam(relOid, inputMsg, imcsAtts, &imcsNatts, &currentLsn);
    WaitXLogRedoToCurrentLsn(currentLsn);
    /* Make sure we are in a transaction command */
    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot(false));
    Relation rel = heap_open(relOid, NoLock);
    PG_TRY();
    {
        CheckImcstoreCacheReady();
        AlterTableEnableImcstore(rel, imcsAtts, imcsNatts);
    }
    PG_CATCH();
    {
        pfree_ext(imcsAtts);
        heap_close(rel, NoLock);
        pq_putemptymessage('E'); /* PlanIdComplete */
        pq_flush();
        PG_RE_THROW();
    }
    PG_END_TRY();
    heap_close(rel, NoLock);
    pq_putemptymessage('O'); /* PlanIdComplete */
    pq_flush();
    PopActiveSnapshot();
    finish_xact_command();
}

void PopulateImcsForPartitionOnStandby(Oid relOid, Oid partOid, StringInfo inputMsg)
{
    int imcsNatts = 0;
    int2vector* imcsAtts = NULL;
    XLogRecPtr currentLsn = InvalidXLogRecPtr;
    ParsePopulateImcsParam(relOid, inputMsg, imcsAtts, &imcsNatts, &currentLsn);
    WaitXLogRedoToCurrentLsn(currentLsn);
    /* Make sure we are in a transaction command */
    start_xact_command();
    PushActiveSnapshot(GetTransactionSnapshot(false));
    Relation rel = heap_open(relOid, NoLock);

    Partition part = partitionOpen(rel, partOid, NoLock);
    Relation partRel = partitionGetRelation(rel, part);

    PG_TRY();
    {
        CheckImcstoreCacheReady();
        /* create imcsdesc for rel if not populated  */
        if (IMCU_CACHE->GetImcsDesc(relOid) == NULL) {
            // first partitition to populate, create imcsdesc for partitioned table
            // In case of specify partition, different table may have different imcs cols,
            // do not remember imcs cols for partitioned table */
            IMCU_CACHE->CreateImcsDesc(rel, NULL, 0);
        }

        /* start populate partition */
        AlterTableEnableImcstore(partRel, imcsAtts, imcsNatts);
        IMCU_CACHE->UpdateImcsStatus(relOid, IMCS_POPULATE_COMPLETE);
    }
    PG_CATCH();
    {
        IMCU_CACHE->UpdateImcsStatus(relOid, IMCS_POPULATE_ERROR);
        releaseDummyRelation(&partRel);
        partitionClose(rel, part, NoLock);
        pfree_ext(imcsAtts);
        heap_close(rel, NoLock);
        pq_putemptymessage('E'); /* PlanIdComplete */
        pq_flush();
        PG_RE_THROW();
    }
    PG_END_TRY();
    releaseDummyRelation(&partRel);
    partitionClose(rel, part, NoLock);
    heap_close(rel, NoLock);
    pq_putemptymessage('O'); /* PlanIdComplete */
    pq_flush();
    PopActiveSnapshot();
    finish_xact_command();
}

void UnPopulateImcsOnStandby(Oid relOid)
{
    if (CHECK_IMCSTORE_CACHE_DOWN) {
        pq_putemptymessage('E');
        pq_flush();
        ereport(ERROR, (errmsg("Imcstore Cache is recovering, please wait a moment or restart database.")));
    }
    if (RelHasImcs(relOid)) {
        /* Make sure we are in a transaction command */
        start_xact_command();
        Relation rel = heap_open(relOid, NoLock);
        PG_TRY();
        {
            AlterTableDisableImcstore(rel);
        }
        PG_CATCH();
        {
            pq_putemptymessage('E'); /* PlanIdComplete */
            pq_flush();
            heap_close(rel, NoLock);
            PG_RE_THROW();
        }
        PG_END_TRY();
        heap_close(rel, NoLock);
        finish_xact_command();
    }
    pq_putemptymessage('O'); /* PlanIdComplete */
    pq_flush();
}

void UnPopulateImcsForPartitionOnStandby(Oid relOid, Oid partOid)
{
    if (CHECK_IMCSTORE_CACHE_DOWN) {
        pq_putemptymessage('E'); /* PlanIdComplete */
        pq_flush();
        ereport(ERROR, (errmsg("Imcstore Cache is recovering, please wait a moment or restart database.")));
    }
    if (RelHasImcs(partOid)) {
        /* Make sure we are in a transaction command */
        start_xact_command();
        Relation rel = heap_open(relOid, NoLock);
        Partition part = partitionOpen(rel, partOid, NoLock);
        Relation partRel = partitionGetRelation(rel, part);
        PG_TRY();
        {
            /* unpopulate partition */
            AlterTableDisableImcstore(partRel);
            /* drop imcs for rel if all partition unpopulated */
            DropImcsForPartitionedRelIfNeed(rel);
        }
        PG_CATCH();
        {
            releaseDummyRelation(&partRel);
            partitionClose(rel, part, NoLock);
            pq_putemptymessage('E'); /* PlanIdComplete */
            pq_flush();
            heap_close(rel, NoLock);
            PG_RE_THROW();
        }
        PG_END_TRY();
        releaseDummyRelation(&partRel);
        partitionClose(rel, part, NoLock);
        heap_close(rel, NoLock);
        finish_xact_command();
    }
    pq_putemptymessage('O'); /* PlanIdComplete */
    pq_flush();
}

void ParsePopulateImcsParam(
    Oid relOid, StringInfo inputMsg, int2vector* &imcsAttsNum, int* imcsNatts, XLogRecPtr* currentLsn)
{
    errno_t rc = EOK;
    /* get column_name_list */
    int count = 0;
    rc = memcpy_s(&count, sizeof(int), pq_getmsgbytes(inputMsg, sizeof(int)), sizeof(int));
    securec_check(rc, "\0", "\0");

    int2 *attsNums = (int2*)palloc(sizeof(int2) * count);
    rc = memcpy_s(attsNums, sizeof(int2) * count, pq_getmsgbytes(inputMsg, sizeof(int2) * count), sizeof(int2) * count);
    securec_check(rc, "\0", "\0");

    /* get current lsn from primary */
    rc = memcpy_s(currentLsn, sizeof(XLogRecPtr), pq_getmsgbytes(inputMsg, sizeof(XLogRecPtr)),
        sizeof(XLogRecPtr));
    securec_check(rc, "\0", "\0");
    ereport(DEBUG1, (errmsg("Received lsn for HTAP population.")));
    pq_getmsgend(inputMsg);

    imcsAttsNum = buildint2vector(attsNums, count);
    *imcsNatts = count;
    pfree(attsNums);
    return;
}

// copy void InitMultinodeExecutor(bool is_force)
PGXCNodeHandle *InitMultiNodeExecutor(Oid nodeoid)
{
    PGXCNodeHandle *result = (PGXCNodeHandle *)palloc0(sizeof(PGXCNodeHandle));
    result->sock = NO_SOCKET;
    init_pgxc_handle(result);
    result->nodeoid = nodeoid;
    result->remote_node_type = VDATANODE;
    return result;
}

PGXCNodeHandle **GetStandbyConnections(int *connCount)
{
    int dnConnCount = MAX_REPLNODE_NUM;
    PGXCNodeHandle **connections = (PGXCNodeHandle **)palloc(dnConnCount * sizeof(PGXCNodeHandle *));
    Oid *dnNode = (Oid *)palloc0(sizeof(Oid) * dnConnCount);
    char **connectionStrs = (char **)palloc0(sizeof(char *) * dnConnCount);
    PGconn **nodeCons = (PGconn **)palloc0(sizeof(PGconn *) * dnConnCount);
    errno_t rc;
    int replArrLength;
    auto releaseConnect = [&](char *errMsg, int connIdx) {
        if (errMsg != NULL) {
            connections[connIdx]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(WARNING, (errmsg("PQconnectdbParallel error: %s", errMsg)));
            return;
        }
        for (int i = 0; i < dnConnCount; i++) {
            pfree_ext(connectionStrs[i]);
        }
        pfree_ext(dnNode);
        pfree_ext(connectionStrs);
        return;
    };

    for (int i = 1; i < MAX_REPLNODE_NUM; i++) {
        char *replconninfo = NULL;
        replconninfo = u_sess->attr.attr_storage.ReplConnInfoArr[i];
        ReplConnInfo *repl_conn_info = GetReplConnInfo(replconninfo, &replArrLength);

        if (repl_conn_info == NULL)
            continue;

        connectionStrs[i - 1] = (char *)palloc0(INITIAL_EXPBUFFER_SIZE * 4);
        NodeDefinition *node = (NodeDefinition *)palloc0(sizeof(NodeDefinition));
        rc = strncpy_s(node->nodehost.data, NAMEDATALEN, repl_conn_info->remotehost, NAMEDATALEN);
        securec_check_c(rc, "\0", "\0");
        rc = strncpy_s(node->nodehost1.data, NAMEDATALEN, repl_conn_info->remotehost, NAMEDATALEN);
        securec_check_c(rc, "\0", "\0");
        node->nodeport = repl_conn_info->remoteport;
        rc = sprintf_s(connectionStrs[i - 1], INITIAL_EXPBUFFER_SIZE * 4,
            "host=%s port=%d dbname=%s user=%s application_name=coordinator1 connect_timeout=600 rw_timeout=600 \
        options='-c remotetype=coordinator  -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres \
        -c lc_monetary=en_US.UTF-8 -c lc_numeric=en_US.UTF-8	-c lc_time=en_US.UTF-8 -c omit_encoding_error=off' \
        prototype=1 keepalives_idle=600 keepalives_interval=30 keepalives_count=20 \
        backend_version=%u enable_ce=1",
            node->nodehost.data, node->nodeport, u_sess->proc_cxt.MyProcPort->database_name,
            u_sess->proc_cxt.MyProcPort->user_name, GRAND_VERSION_NUM);
        securec_check_ss_c(rc, "\0", "\0");
        dnNode[i - 1] = node->nodeoid;
        connections[i - 1] = InitMultiNodeExecutor(node->nodeoid);
        connections[i - 1]->nodeIdx = i;
        *connCount = *connCount + 1;
    }

    PQconnectdbParallel(connectionStrs, dnConnCount, nodeCons, dnNode);

    for (int i = 0; i < *connCount; i++) {
        if (nodeCons[i] && (CONNECTION_OK == nodeCons[i]->status)) {
            pgxc_node_init(connections[i], nodeCons[i]->sock);
        } else {
            char firstError[INITIAL_EXPBUFFER_SIZE] = {0};
            errno_t ss_rc = EOK;
            if (nodeCons[i] == NULL) {
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, "out of memory");
            } else if (nodeCons[i]->errorMessage.data != NULL) {
                if (strlen(nodeCons[i]->errorMessage.data) >= INITIAL_EXPBUFFER_SIZE) {
                    nodeCons[i]->errorMessage.data[INITIAL_EXPBUFFER_SIZE - 1] = '\0';
                }
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, nodeCons[i]->errorMessage.data);
            } else {
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, "unknown error");
            }
            releaseConnect(firstError, i);
        }
    }
    releaseConnect(NULL, 0);
    return connections;
}

static int HandleImcsResponse(PGXCNodeHandle *conn, RemoteQueryState *combiner)
{
    char *msg = NULL;
    int msgLen;
    char msgType;
    bool errorFlag = false;

    for (;;) {
        Assert(conn->state != DN_CONNECTION_STATE_IDLE);

        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         *
         * If not in GPC mode, should receive datanode messages but not interrupt immediately in loop while.
         */
        if (t_thrd.proc_cxt.proc_exit_inprogress && ENABLE_CN_GPC) {
            conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(DEBUG2, (errmsg(
                "DN_CONNECTION_STATE_ERROR_FATAL0 is set for connection to node %s[%u] when proc_exit_inprogress",
                conn->remoteNodeName, conn->nodeoid)));
        }

        /* don't read from from the connection if there is a fatal error */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            ereport(DEBUG2,
                (errmsg("handle_response0 returned with DN_CONNECTION_STATE_ERROR_FATAL for connection to node %s[%u] ",
                conn->remoteNodeName, conn->nodeoid)));
            return RESPONSE_COMPLETE;
        }

        /* No data available, read one more time or exit */
        if (!HAS_MESSAGE_BUFFERED(conn)) {
            /*
             * For FATAL error, no need to read once more, because openGauss thread(DN) will exit
             * immediately after sending error message without sending 'Z'(ready for query).
             */
            if (combiner != NULL && combiner->is_fatal_error) {
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                conn->combiner = NULL;

                return RESPONSE_COMPLETE;
            }

            if (errorFlag) {
                /* incomplete message, if last message type is ERROR,read once more */
                if (pgxc_node_receive(1, &conn, NULL))
                    ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to receive message from %s[%u]", conn->remoteNodeName, conn->nodeoid)));
                errorFlag = false;
                continue;
            } else {
                return RESPONSE_EOF;
            }
        }
        /* no need to check conn's combiner when abort transaction */
        Assert(t_thrd.xact_cxt.bInAbortTransaction || conn->combiner == combiner || conn->combiner == NULL);

        msgType = get_message(conn, &msgLen, &msg);

        switch (msgType) {
            case '\0': /* Not enough data in the buffer */
                return RESPONSE_EOF;
            case 'O': /* Complete */
                conn->state = DN_CONNECTION_STATE_IDLE;
                return RESPONSE_PLANID_OK;
            case 'E': /* Populate error */
                return RESPONSE_EOF;
            case 'I': /* EmptyQuery */
            default:
                /* sync lost? */
                elog(WARNING, "Received unsupported message type: %c", msgType);
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                /* stop reading */
                return RESPONSE_COMPLETE;
        }
    }
    /* never happen, but keep compiler quiet */
    return RESPONSE_EOF;
}

static bool HandlePgxcReceive(int connCount, PGXCNodeHandle **tempConnections)
{
    bool hasError = false;
    int originConnCount = connCount;
    RemoteQueryState *combiner = NULL;
    combiner = CreateResponseCombiner(connCount, COMBINE_TYPE_NONE);
    while (connCount > 0) {
        if (pgxc_node_receive(connCount, tempConnections, NULL)) {
            int errorCode;
            char *errorMsg = getSocketError(&errorCode);
            hasError = true;
            ereport(WARNING, (errcode(errorCode),
                errmsg("Failed to read response from Datanodes while sending rel_id for HTAP populate. Detail: %s",
                errorMsg)));
            break;
        }
        int i = 0;
        while (i < connCount) {
            int res = HandleImcsResponse(tempConnections[i], combiner);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_PLANID_OK) {
                if (--connCount > i)
                    tempConnections[i] = tempConnections[connCount];
            } else {
                hasError = true;
                tempConnections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
                ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Unexpected response from %s while sending rel_id for HTAP populate",
                    tempConnections[i]->remoteNodeName),
                    errdetail("%s", (combiner->errorMessage == NULL) ? "none" : combiner->errorMessage)));
                if (--connCount > i)
                    tempConnections[i] = tempConnections[connCount];
            }
        }
        /* report error if any */
        pgxc_node_report_error(combiner);
    }
    ValidateAndCloseCombiner(combiner);
    for (int i = 0; i < originConnCount; ++i) {
        pgxc_node_free(tempConnections[i]);
    }
    pfree_ext(tempConnections);
    return hasError;
}

static void PackBasicImcstoredRequest(
    PGXCNodeHandle *temp_connection, SendPopulateParams* populateParams, int imcsNatts)
{
    errno_t ss_rc = EOK;
    int msglen = populateParams->msglen;
    /* msgType + msgLen */
    ensure_out_buffer_capacity(1 + msglen, temp_connection);
    Assert(temp_connection->outBuffer != NULL);
    temp_connection->outBuffer[temp_connection->outEnd++] = 'x';
    msglen = htonl(msglen);
    ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
        temp_connection->outSize - temp_connection->outEnd - 1, &msglen, sizeof(int));
    securec_check(ss_rc, "\0", "\0");
    temp_connection->outEnd += sizeof(int);

    ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
        temp_connection->outSize - temp_connection->outEnd - 1, &populateParams->imcstoreType, sizeof(int));
    securec_check(ss_rc, "\0", "\0");
    temp_connection->outEnd += sizeof(int);

    ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
        temp_connection->outSize - temp_connection->outEnd, &populateParams->relOid, sizeof(Oid));
    securec_check(ss_rc, "\0", "\0");
    temp_connection->outEnd += sizeof(Oid);

    ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
        temp_connection->outSize - temp_connection->outEnd, &populateParams->partOid, sizeof(Oid));
    securec_check(ss_rc, "\0", "\0");
    temp_connection->outEnd += sizeof(Oid);

    if (imcsNatts > 0) {
        ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
            temp_connection->outSize - temp_connection->outEnd, &imcsNatts, sizeof(int));
        securec_check(ss_rc, "\0", "\0");
        temp_connection->outEnd += sizeof(int);
        ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
            temp_connection->outSize - temp_connection->outEnd, populateParams->attsNums, sizeof(int2) * imcsNatts);
        securec_check(ss_rc, "\0", "\0");
        temp_connection->outEnd += sizeof(int2) * imcsNatts;
        XLogRecPtr lsn = t_thrd.shemem_ptr_cxt.XLogCtl->LogwrtRqst.Write;
        ss_rc = memcpy_s(temp_connection->outBuffer + temp_connection->outEnd,
            temp_connection->outSize - temp_connection->outEnd, &lsn, sizeof(XLogRecPtr));
        securec_check(ss_rc, "\0", "\0");
        temp_connection->outEnd += sizeof(XLogRecPtr);
    }
}

void SendImcstoredRequest(Oid relOid, Oid specifyPartOid, int2* attsNums, int imcsNatts, int type)
{
    int connCount = 0;
    PGXCNodeHandle** connections = NULL;
    SendPopulateParams populateParams;

    /* init send populate params */
    populateParams.relOid = relOid;
    populateParams.partOid = specifyPartOid;
    populateParams.attsNums = attsNums;
    populateParams.imcstoreType = type;
    populateParams.msglen =
        sizeof(int) + sizeof(int) + sizeof(Oid) + sizeof(Oid) + sizeof(int) +
        imcsNatts * sizeof(int2) + sizeof(XLogRecPtr);

    connections = GetStandbyConnections(&connCount);
    PGXCNodeHandle **temp_connections = NULL;
    /* use temp connections instead */
    int i = 0;
    int connCountTemp = 0;
    temp_connections = (PGXCNodeHandle **)palloc(connCount * sizeof(PGXCNodeHandle *));
    for (i = 0; i < connCount; i++) {
        if (connections[i]->state != DN_CONNECTION_STATE_ERROR_FATAL) {
            temp_connections[connCountTemp++] = connections[i];
        }
    }
    connCount = connCountTemp;

    for (i = 0; i < connCount; i++) {
        if (temp_connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(temp_connections[i]);

        if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
            LIBCOMM_DEBUG_LOG("Populate failed, send rel_id to node:%s[nid:%hu,sid:%hu] with abnormal state:%d",
                temp_connections[i]->remoteNodeName, temp_connections[i]->gsock.idx, temp_connections[i]->gsock.sid,
                temp_connections[i]->state);

        PackBasicImcstoredRequest(temp_connections[i], &populateParams, imcsNatts);

        if (pgxc_node_flush(temp_connections[i]) != 0) {
            temp_connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Failed to send populate request to %s", temp_connections[i]->remoteNodeName)));
        }

        temp_connections[i]->state = DN_CONNECTION_STATE_QUERY;
    }

    if (HandlePgxcReceive(connCount, temp_connections)) {
        IMCU_CACHE->UpdatePrimaryImcsStatus(relOid, IMCS_POPULATE_ERROR);
        ereport(ERROR, (errmsg("HTAP populate failed, some standby occurs error.")));
    }
}

void SendUnImcstoredRequest(Oid relOid, Oid specifyPartOid, int type)
{
    int connCount = 0;
    PGXCNodeHandle** connections = NULL;
    SendPopulateParams populateParams;

    /* init send populate params */
    populateParams.relOid = relOid;
    populateParams.partOid = specifyPartOid;
    populateParams.attsNums = NULL;
    populateParams.imcstoreType = type;
    populateParams.msglen =
        sizeof(int) + sizeof(int) + sizeof(Oid) + sizeof(Oid);
    connections = GetStandbyConnections(&connCount);
    PGXCNodeHandle **temp_connections = NULL;
    /* use temp connections instead */
    int i = 0;
    int connCountTemp = 0;
    temp_connections = (PGXCNodeHandle **)palloc(connCount * sizeof(PGXCNodeHandle *));
    for (i = 0; i < connCount; i++) {
        if (connections[i]->state != DN_CONNECTION_STATE_ERROR_FATAL) {
            temp_connections[connCountTemp++] = connections[i];
        }
    }
    connCount = connCountTemp;

    for (i = 0; i < connCount; i++) {
        if (temp_connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(temp_connections[i]);
        if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
            LIBCOMM_DEBUG_LOG("Unpopulate failed, send rel_id to node:%s[nid:%hu,sid:%hu] with abnormal state:%d",
                temp_connections[i]->remoteNodeName, temp_connections[i]->gsock.idx, temp_connections[i]->gsock.sid,
                temp_connections[i]->state);

        PackBasicImcstoredRequest(temp_connections[i], &populateParams, 0);
        if (pgxc_node_flush(temp_connections[i]) != 0) {
            temp_connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Failed to send unpopulate request to %s", temp_connections[i]->remoteNodeName)));
        }

        temp_connections[i]->state = DN_CONNECTION_STATE_QUERY;
    }

    if (HandlePgxcReceive(connCount, temp_connections)) {
        ereport(ERROR, (errmsg("HTAP unpopulate failed, some standby occurs error.")));
    }
}

void CopyTupleInfo(Tuple tuple, Datum* curVal, uint32 *blkno)
{
    Assert(tuple != NULL && curVal != NULL && blkno != NULL);
    errno_t rc = EOK;
    ImcstoreCtid imcsCtid;
    imcsCtid.reservedSpace = 0;
    if (TUPLE_IS_HEAP_TUPLE(tuple)) {
        HeapTuple heapTuple = (HeapTuple) tuple;
        *blkno = BlockIdGetBlockNumber(&heapTuple->t_self.ip_blkid);
        rc = memcpy_s(&imcsCtid.ctid, sizeof(ImcstoreCtid), &heapTuple->t_self, sizeof(ItemPointerData));
    } else {
        UHeapTuple uHeapTup = (UHeapTuple) tuple;
        *blkno = BlockIdGetBlockNumber(&uHeapTup->ctid.ip_blkid);
        rc = memcpy_s(&imcsCtid.ctid, sizeof(ImcstoreCtid), &uHeapTup->ctid, sizeof(ItemPointerData));
    }
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(curVal, sizeof(Datum), &imcsCtid, sizeof(ImcstoreCtid));
    securec_check_c(rc, "\0", "\0");
}

uint32 ImcsCeil(uint32 x, uint32 y)
{
    Assert(y > 0);
    return (x % y) == 0 ? (x / y) : (x / y + 1);
}

Relation ParallelImcsOpenRelation(Relation rel)
{
    Assert(RelationIsValid(rel));
    bool isPartition = OidIsValid(rel->parentId);
    if (isPartition) {
        Oid rootRelOid = OidIsValid(rel->grandparentId) ? rel->grandparentId : rel->parentId;
        Relation rootRel = heap_open(rootRelOid, NoLock);
        Partition part = partitionOpen(rootRel, RelationGetRelid(rel), NoLock);
        Relation partRel = partitionGetRelation(rootRel, part);
        partitionClose(rootRel, part, NoLock);
        heap_close(rootRel, NoLock);
        return partRel;
    } else {
        return heap_open(RelationGetRelid(rel), NoLock);
    }
}

void ParallelImcsCloseRelation(Relation rel)
{
    Assert(RelationIsValid(rel));
    bool isPartition = OidIsValid(rel->parentId);
    if (isPartition) {
        releaseDummyRelation(&rel);
    } else {
        heap_close(rel, NoLock);
    }
}

Oid ImcsPartNameGetPartOid(Oid relOid, const char* partName)
{
    Oid partOid = InvalidOid;
    partOid = PartitionNameGetPartitionOid(
        relOid, partName, PART_OBJ_TYPE_TABLE_PARTITION, AccessExclusiveLock, true, false, NULL, NULL, NoLock);
    if (!OidIsValid(partOid)) {
        ereport(ERROR, (errmsg("rel %d has no partition %s.", relOid, partName)));
    }
    return partOid;
}


void DropImcsForPartitionedRelIfNeed(Relation partitionedRel)
{
    if (!RelationIsPartitioned(partitionedRel)) {
        return;
    }

    bool needDrop = true;
    List* partOids = NIL;
    ListCell* cell = NULL;
    Oid partOid = InvalidOid;
    partOids = relationGetPartitionOidList(partitionedRel);
    foreach (cell, partOids) {
        partOid = lfirst_oid(cell);
        if (RelHasImcs(partOid)) {
            needDrop = false;
            break;
        }
    }
    releasePartitionOidList(&partOids);
    /* all partition unpopulate, drop partitioned rel imcs */
    if (needDrop) {
        UnPopulateImcs(partitionedRel);
    }
}

/* Check whether the standby node has been rolled back to the current LSN. */
void WaitXLogRedoToCurrentLsn(XLogRecPtr currentLsn)
{
    int waitTimeMs = 0;
    XLogRecPtr latestXLogLsn = InvalidXLogRecPtr;
    do {
        latestXLogLsn = pg_atomic_read_u64(&IMCU_CACHE->m_xlog_latest_lsn);
        ereport(DEBUG1, (errmsg("Wait lsn for HTAP population, current lsn: %lu, xlog redo lsn: %lu.",
            currentLsn, latestXLogLsn)));
        Assert(XLogRecPtrIsValid(latestXLogLsn));

        if (currentLsn <= latestXLogLsn) {
            break;
        }

        if (waitTimeMs >= WAIT_XLOG_REDO_TIMEOUT_MS) {
            ereport(ERROR, (errmsg("Wait lsn for HTAP population time out after %fs, current lsn: %lu,"
                "xlog redo lsn: %lu.", ((double)waitTimeMs / 1000), currentLsn, latestXLogLsn)));
        }

        pg_usleep(100000); /* sleep 100ms */
        waitTimeMs = waitTimeMs + 100;
    } while (true);
}
