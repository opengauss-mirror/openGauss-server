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
 * cstore_delta.cpp
 *      routines to support ColStore delta
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_delta.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/cstore_delta.h"
#include "access/cstore_insert.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_partition_fn.h"
#include "nodes/makefuncs.h"
#include "parser/parse_utilcmd.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

static Oid GetCUIdxFromDeltaIdx(Relation deltaIdx, bool isPartition);

/*
 * @Description: Move datas on delta to its corresponding CU.
 * @IN rel: the CU owns detal table.
 * @IN parentRel: if rel is a partition of a cstore, parentRel is
 *     its parent relation. Otherwise parentRel is NULL.
 */
void MoveDeltaDataToCU(Relation rel, Relation parentRel)
{
    if (RELATION_IS_PARTITIONED(rel)) {
        /* For partitioned table. */
        List* partitionList = relationGetPartitionOidList(rel);
        ListCell* cell = NULL;
        Oid partitionOid = InvalidOid;
        Partition partition = NULL;
        Relation partRelation = NULL;
        foreach (cell, partitionList) {
            partitionOid = lfirst_oid(cell);
            /* Data movement consists of delete and insert, so we apply RowExclusiveLock. */
            partition = partitionOpen(rel, partitionOid, RowExclusiveLock);
            partRelation = partitionGetRelation(rel, partition);

            MoveDeltaDataToCU(partRelation, rel);

            partitionClose(rel, partition, NoLock);
            releaseDummyRelation(&partRelation);
        }
        list_free_ext(partitionList);
        return;
    }

    Relation deltaRel = heap_open(RelationGetDeltaRelId(rel), RowExclusiveLock);
    TableScanDesc deltaScanDesc = tableam_scan_begin(deltaRel, GetActiveSnapshot(), 0, NULL);
    InsertArg args;
    HeapTuple deltaTup = NULL;
    ResultRelInfo *resultRelInfo = NULL;
    if (rel->rd_rel->relhasindex) {
        resultRelInfo = makeNode(ResultRelInfo);
        if (parentRel != NULL) {
            InitResultRelInfo(resultRelInfo, parentRel, 1, 0);
        } else {
            InitResultRelInfo(resultRelInfo, rel, 1, 0);
        }
        ExecOpenIndices(resultRelInfo, false);
    }
    CStoreInsert::InitInsertArg(rel, resultRelInfo, true, args);
    CStoreInsert cstoreInsert(rel, args, false, NULL, NULL);
    TupleDesc tupDesc = rel->rd_att;
    Datum* val = (Datum*)palloc(sizeof(Datum) * tupDesc->natts);
    bool* null = (bool*)palloc(sizeof(bool) * tupDesc->natts);
    bulkload_rows batchRow(tupDesc, RelationGetMaxBatchRows(rel), true);

    while ((deltaTup = (HeapTuple) tableam_scan_getnexttuple(deltaScanDesc, ForwardScanDirection)) != NULL) {
        tableam_tops_deform_tuple(deltaTup, tupDesc, val, null);

        /* ignore returned value because only one tuple is appended into */
        (void)batchRow.append_one_tuple(val, null, tupDesc);

        /* delete the current tuple from delta table */
        simple_heap_delete(deltaRel, &deltaTup->t_self);

        if (batchRow.full_rownum()) {
            /* insert into main table */
            cstoreInsert.BatchInsert(&batchRow, 0);
            batchRow.reset(true);
        }
    }

    if (batchRow.m_rows_curnum > 0) {
        cstoreInsert.BatchInsert(&batchRow, 0);
    }
    cstoreInsert.SetEndFlag();
    tableam_scan_end(deltaScanDesc);

    /* clean cstore insert */
    pfree(val);
    pfree(null);
    CStoreInsert::DeInitInsertArg(args);
    batchRow.Destroy();
    cstoreInsert.Destroy();
    if (resultRelInfo != NULL) {
        ExecCloseIndices(resultRelInfo);
        pfree(resultRelInfo);
    }

    heap_close(deltaRel, RowExclusiveLock);
}

/*
 * @Description: Define unique index on delta table.
 * @IN relationId: the CU owns delta table.
 * @IN stmt: IndexStmt describing the properties of the new index.
 * @IN indexRelationId: the index on CU.
 * @IN parentRel: the parent relation of the CU if relationId is a partition of cstore.
 */
void DefineDeltaUniqueIndex(Oid relationId, IndexStmt* stmt, Oid indexRelationId, Relation parentRel)
{
    if (!stmt->unique) {
        return;
    }

    Relation rel = NULL;
    Relation deltaRelation = NULL;
    Partition partition = NULL;
    int numberOfKeyAttributes = 0;
    List* indexColNames =  NIL;
    char deltaIndexRelName[NAMEDATALEN] = {'\0'};
    error_t ret = 0;

    if (parentRel) {
        /* For partiontioned table. relationId is a partition id. */
        /* We will build index on partioned delta table, so we just use AccessShareLock on partition. */
        partition = partitionOpen(parentRel, relationId, AccessShareLock);
        rel = partitionGetRelation(parentRel, partition);
        partitionClose(parentRel, partition, NoLock);
    } else {
        /* For non-partioned table. */
        rel = relation_open(relationId, AccessShareLock);
    }

    if (RELATION_IS_PARTITIONED(rel)) {
        /* For partitioned table. */
        List* partitionList = relationGetPartitionOidList(rel);
        ListCell* cell = NULL;
        Oid partitionOid = InvalidOid;
        Oid partIdxOid = InvalidOid;

        /*
         * Global partition index does not support column store,
         * so each partition has an index. We build index on each
         * partition delta.
         */
        foreach (cell, partitionList) {
            partitionOid = lfirst_oid(cell);
            partIdxOid = getPartitionIndexOid(indexRelationId, partitionOid);
            DefineDeltaUniqueIndex(partitionOid, stmt, partIdxOid, rel);
        }

        list_free_ext(partitionList);
        relation_close(rel, NoLock);
        return;
    }

    deltaRelation = heap_open(RelationGetDeltaRelId(rel), ShareLock);

    numberOfKeyAttributes = list_length(stmt->indexParams);
    indexColNames = ChooseIndexColumnNames(stmt->indexParams);

    if (!parentRel) {
        ret = snprintf_s(deltaIndexRelName, sizeof(deltaIndexRelName),
            sizeof(deltaIndexRelName) - 1, "pg_delta_index_%u", indexRelationId);
    } else {
        ret = snprintf_s(deltaIndexRelName, sizeof(deltaIndexRelName),
            sizeof(deltaIndexRelName) - 1, "pg_delta_part_index_%u", indexRelationId);
    }
    securec_check_ss_c(ret, "\0", "\0");

    IndexInfo* indexInfo = NULL;
    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = numberOfKeyAttributes;
    indexInfo->ii_NumIndexKeyAttrs = numberOfKeyAttributes;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_PgClassAttrId = 0;

    (void)CreateDeltaUniqueIndex(deltaRelation, deltaIndexRelName, indexInfo, stmt->indexParams,
        indexColNames, stmt->primary);

    heap_close(deltaRelation, NoLock);

    if (parentRel) {
        releaseDummyRelation(&rel);
    } else {
        relation_close(rel, NoLock);
    }
}

/*
 * @Description: Create unique index on delta table.
 * @IN deltaRel: the delta table.
 * @IN deltaIndexName: the name of index on delta table
 * @IN indexInfo: indexInfo of CU index
 * @IN indexElemList: the list contains each index column attribute
 * @IN indexColNames: the list contains name of each index column
 * @IN isPrimary: is primary index?
 */
Oid CreateDeltaUniqueIndex(Relation deltaRel, const char* deltaIndexName, IndexInfo* indexInfo,
    List* indexElemList, List* indexColNames, bool isPrimary)
{
    int numIndexCols = indexInfo->ii_NumIndexAttrs;
    Oid* typeObjectId = (Oid*)palloc(numIndexCols * sizeof(Oid));
    Oid* collationObjectId = (Oid*)palloc(numIndexCols * sizeof(Oid));
    Oid* classObjectId = (Oid*)palloc(numIndexCols * sizeof(Oid));
    int16* coloptions = (int16*)palloc(numIndexCols * sizeof(int16));

    ComputeIndexAttrs(indexInfo, typeObjectId, collationObjectId, classObjectId, coloptions,
        indexElemList, NULL, RelationGetRelid(deltaRel), "btree", BTREE_AM_OID, true, false);

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    Oid deltaIndexOid = index_create(deltaRel,
        deltaIndexName, InvalidOid, InvalidOid, indexInfo,
        indexColNames, BTREE_AM_OID, deltaRel->rd_rel->reltablespace,
        collationObjectId, classObjectId, coloptions, (Datum)0,
        isPrimary, false, false, false, true, false,
        false, &extra);

    pfree(typeObjectId);
    pfree(collationObjectId);
    pfree(classObjectId);
    pfree(coloptions);

    return deltaIndexOid;
}

/*
 * @Description: Delete the index of old delta table and build index on new delta table.
 * @IN oldRelOid: the old cstore after relation files swap.
 * @IN newRelOid: the new cstore after relation files swap.
 * @IN parentOid: parent oid for partitioned table, otherwise InvalidOid.
 */
void BuildIndexOnNewDeltaTable(Oid oldRelOid, Oid newRelOid, Oid parentOid)
{
    Relation oldRel = NULL;
    Relation newRel = NULL;
    Relation parentRel = NULL;
    Partition partition = NULL;
    Oid oldDeltaOid = InvalidOid;
    Oid newDeltaOid = InvalidOid;

    oldRel = heap_open(oldRelOid, AccessShareLock);
    oldDeltaOid = oldRel->rd_rel->reldeltarelid;

    if (OidIsValid(parentOid)) {
        /* For partitioned table. */
        parentRel = heap_open(parentOid, AccessShareLock);
        partition = partitionOpen(parentRel, newRelOid, AccessShareLock);
        newRel = partitionGetRelation(parentRel, partition);
    } else {
        /* For non partitioned table. */
        newRel = heap_open(newRelOid, AccessShareLock);
    }

    newDeltaOid = newRel->rd_rel->reldeltarelid;

    /* Apply AccssShareLock because we only get information from old delta. */
    Relation oldDelta = heap_open(oldDeltaOid, AccessShareLock);

    TupleDesc tupleDesc = RelationGetDescr(oldRel);
    int attmapLength = tupleDesc->natts;
    AttrNumber* attmap = (AttrNumber*)palloc0(sizeof(AttrNumber) * attmapLength);
    for (int i = 0; i < attmapLength; ++i) {
        attmap[i] = i + 1;
    }

    List* indexIds = RelationGetIndexList(oldDelta);

    heap_close(oldDelta, NoLock);

    /* Apply ShareLock because we will build index on new delta. */
    Relation newDelta = heap_open(newDeltaOid, ShareLock);

    ListCell* cell = NULL;
    ObjectAddress indexObject;
    foreach (cell, indexIds) {
        Oid idxOid = lfirst_oid(cell);
        bool isPartition = OidIsValid(parentOid);
        Relation idxRel = index_open(idxOid, AccessShareLock);

        if (idxRel->rd_index != NULL && idxRel->rd_index->indisunique) {
            Oid CUIdxOid = GetCUIdxFromDeltaIdx(idxRel, isPartition);

            CreateStmtContext cxt;
            cxt.relation = makeRangeVar(
                get_namespace_name(RelationGetNamespace(oldRel)), RelationGetRelationName(oldRel), -1);
            IndexStmt* indexStmt = NULL;
            indexStmt = generateClonedIndexStmt(&cxt, idxRel, attmap, attmapLength, NULL, TRANSFORM_INVALID);

            /*
             * Must delete the index on old delta table, or conflicts will happen between
             * new index name and old index name.
             */
            indexObject.classId = RelationRelationId;
            indexObject.objectId = idxOid;
            indexObject.objectSubId = 0;
            index_close(idxRel, NoLock);
            performDeletion(&indexObject, DROP_RESTRICT, 0);
            DefineDeltaUniqueIndex(newRelOid, indexStmt, CUIdxOid, parentRel);
        } else {
            index_close(idxRel, NoLock);
        }
    }

    heap_close(newDelta, NoLock);
    heap_close(oldRel, NoLock);

    if (OidIsValid(parentOid)) {
        releaseDummyRelation(&newRel);
        partitionClose(parentRel, partition, NoLock);
        heap_close(parentRel, NoLock);
    } else {
        heap_close(newRel, NoLock);
    }
    
    pfree_ext(attmap);
    list_free_ext(indexIds);
}

/*
 * @Description: Reindex index on delta.
 * @IN indexId: the index on CU
 * @IN indexPartId: the index on partition CU.
 *      For partitioned cstore, indexPartId=InvaidOid means reindex all
        partition delta index. Otherwise reindex single partition delta index
 *      For non-partioned cstore, indexPartId=InvaldOid.
 */
void ReindexDeltaIndex(Oid indexId, Oid indexPartId)
{
    Oid heapId = IndexGetRelation(indexId, false);

    /* We get AccessShareLock on CU table because we will build index on delta table. */
    Relation heapRelation = heap_open(heapId, AccessShareLock);
    

    Oid deltaIdxOid = InvalidOid;

    if (!RELATION_IS_PARTITIONED(heapRelation)) {
        /* For non partioned cstore table. */
        deltaIdxOid = GetDeltaIdxFromCUIdx(indexId, false);
        reindex_index(deltaIdxOid, InvalidOid, false, NULL, false);
    } else {
        /* For partitioned cstore table. */
        if (OidIsValid(indexPartId)) {
            deltaIdxOid = GetDeltaIdxFromCUIdx(indexPartId, true);
            reindex_index(deltaIdxOid, InvalidOid, false, NULL, false);
        } else {
            /* Reindex all indexes on part delta table. */
            List* indexPartOidList = NIL;
            ListCell* partCell = NULL;
            Relation iRel = index_open(indexId, AccessShareLock);

            indexPartOidList = indexGetPartitionOidList(iRel);
            foreach (partCell, indexPartOidList) {
                Oid indexPartOid = lfirst_oid(partCell);
                deltaIdxOid = GetDeltaIdxFromCUIdx(indexPartOid, true);
                reindex_index(deltaIdxOid, InvalidOid, false, NULL, false);
            }

            releasePartitionOidList(&indexPartOidList);
            index_close(iRel, NoLock);
        }
    }
    /* Close CU table, but keep locks. */
    heap_close(heapRelation, NoLock);
}

/*
 * @Description: Reindex on partition delta table.
 * @IN indexOid: parent index on CU
 * @IN partOid: partition CU oid
 */
void ReindexPartDeltaIndex(Oid indexOid, Oid partOid)
{
    Relation pg_partition = NULL;
    ScanKeyData scanKey;
    SysScanDesc partScan;
    HeapTuple partTuple = NULL;
    Form_pg_partition partForm = NULL;
    Oid indexPartOid = InvalidOid;

    pg_partition = heap_open(PartitionRelationId, AccessShareLock);
    ScanKeyInit(&scanKey, Anum_pg_partition_indextblid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(partOid));
    partScan = systable_beginscan(pg_partition, PartitionIndexTableIdIndexId, true, SnapshotNow, 1, &scanKey);
    while ((partTuple = systable_getnext(partScan)) != NULL) {
        partForm = (Form_pg_partition)GETSTRUCT(partTuple);
        if (partForm->parentid == indexOid) {
            indexPartOid = HeapTupleGetOid(partTuple);
            break;
        }
    }
    systable_endscan(partScan);
    heap_close(pg_partition, AccessShareLock);

    if (!OidIsValid(indexPartOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for partitioned index %u", indexOid)));
    }

    ReindexDeltaIndex(indexOid, indexPartOid);
}

/*
 * @Description: Get CU index oid from its corresponding delta index.
 * @IN deltaIdx: delta index
 * @IN isPartition: true means delta index name is like pg_delta_part_index_xxx,
 *      false means delta index name is like pg_delta_index_xxx.
 * @Reture: the CU index oid
 */
static Oid GetCUIdxFromDeltaIdx(Relation deltaIdx, bool isPartition)
{
    Oid CUIdxOid = 0;
    int curIdx = 0;
    const char* deltaIdxName = RelationGetRelationName(deltaIdx);

    if (isPartition) {
        /* For partitioned delta table index. */
        curIdx = strlen("pg_delta_part_index_");
    } else {
        /* For non partitioned delta table index. */
        curIdx = strlen("pg_delta_index_");
    }

    while (deltaIdxName[curIdx] != '\0') {
        CUIdxOid = CUIdxOid * 10 + (deltaIdxName[curIdx] - '0');
        curIdx++;
    }
    return CUIdxOid;
}

/*
 * @Description: Get delta index oid from its corresponding CU index.
 * @IN CUIndexOid: CU index
 * @IN isPartitioned: indicates whether CU index is a partitioned index.
 * @IN suppressMiss: return InvalidOid if true when delta index is not found
 * @Reture: the delta index oid
 */
Oid GetDeltaIdxFromCUIdx(Oid CUIndexOid, bool isPartitioned, bool suppressMiss)
{
    char deltaIdxName[NAMEDATALEN] = {'\0'};
    Relation pgclass = NULL;
    ScanKeyData scanKey[1];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    Oid deltaIdxOid = InvalidOid;
    error_t ret = 0;

    if (!isPartitioned) {
        ret = snprintf_s(deltaIdxName, sizeof(deltaIdxName),
                         sizeof(deltaIdxName) - 1, "pg_delta_index_%u", CUIndexOid);
    } else {
        ret = snprintf_s(deltaIdxName, sizeof(deltaIdxName),
                         sizeof(deltaIdxName) - 1, "pg_delta_part_index_%u", CUIndexOid);
    }
    securec_check_ss_c(ret, "\0", "\0");

    ScanKeyInit(&scanKey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(deltaIdxName));
    pgclass = heap_open(RelationRelationId, AccessShareLock);
    scan = systable_beginscan(pgclass, ClassNameNspIndexId, true, SnapshotNow, 1, scanKey);
    tup = systable_getnext(scan);
    if (HeapTupleIsValid(tup)) {
        deltaIdxOid = HeapTupleGetOid(tup);
    } else {
        if (suppressMiss) {
            systable_endscan(scan);
            heap_close(pgclass, AccessShareLock);
            return InvalidOid;
        }
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation \"%s\" does not exist", deltaIdxName)));
    }
    systable_endscan(scan);
    heap_close(pgclass, AccessShareLock);

    return deltaIdxOid;
}

/*
 * @Description: Get CU index name from delta index.
 * @IN deltaIdx: delta index which name stores CU index oid
 * @Return: CU index name
 */
char* GetCUIdxNameFromDeltaIdx(Relation deltaIdx)
{
    const char* deltaIdxName = RelationGetRelationName(deltaIdx);
    bool partitioned;
    if (pg_strncasecmp(deltaIdxName, "pg_delta_part_index_", strlen("pg_delta_part_index_")) == 0) {
        partitioned = true;
    } else {
        partitioned = false;
    }

    Oid CUIdxOid = GetCUIdxFromDeltaIdx(deltaIdx, partitioned);

    char* CUIdxName = NULL;
    if (!partitioned) {
        CUIdxName = get_rel_name(CUIdxOid);
    } else {
        HeapTuple tuple = SearchSysCacheCopy1(PARTRELID, ObjectIdGetDatum(CUIdxOid));
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("could not find tuple for partition index %u", CUIdxOid)));
        }
        Form_pg_partition partForm = (Form_pg_partition)GETSTRUCT(tuple);
        Oid parentIdxOid = partForm->parentid;

        CUIdxName = get_rel_name(parentIdxOid);

        heap_freetuple(tuple);
    }

    return CUIdxName;
}
