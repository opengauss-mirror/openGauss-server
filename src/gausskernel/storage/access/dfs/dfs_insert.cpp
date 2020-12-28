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
 * ------------------------------------------------------------------------------
 *
 *  dfs_insert.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_insert.cpp
 * ------------------------------------------------------------------------------
 */

#include "access/dfs/dfs_insert.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "dfsdesc.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/catalog.h"
#include "storage/lmgr.h"
#include "pgxc/pgxc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"

HTAB *g_dfsSpaceCache = NULL;
#define MAX_FILE_ID ((1 << 24) - 1)
typedef struct DFSSpaceKV {
    uint64 offset;
} DFSSpaceKV;

DfsInsertInter *CreateDfsInsert(Relation rel, bool is_update, Relation dataDestRel, Plan *plan,
                                MemInfoArg *m_dfsInsertMemInfo)
{
    DfsInsertInter *dfsinsert = NULL;

    if (RelationIsValuePartitioned(rel)) {
        dfsinsert = New(CurrentMemoryContext) DfsPartitionInsert(rel, is_update, plan, m_dfsInsertMemInfo);
    } else {
        dfsinsert = New(CurrentMemoryContext) DfsInsert(rel, is_update, NULL, plan, m_dfsInsertMemInfo);
    }

    /* data redistribution for DFS table. */
    dfsinsert->setDataDestRel(dataDestRel);

    return dfsinsert;
}

/*
 * ScalarValue => Datum, Datum should point to data without length header
 * for varing length data type.
 */
Datum convertScalarToDatum(Oid typid, ScalarValue val)
{
    Datum datum = 0;
    switch (typid) {
        case TIMETZOID:
        case TINTERVALOID:
        case INTERVALOID:
        case NAMEOID:
        case MACADDROID: {
            datum = convertScalarToDatumT<TIMETZOID>(val);
            break;
        }
        case CSTRINGOID:
        case UNKNOWNOID: {
            datum = convertScalarToDatumT<UNKNOWNOID>(val);
            break;
        }
        default: {
            datum = (Datum)val;
            break;
        }
    }
    return datum;
}

/*
 * @Description: Construct the struct of index insert information.
 * @IN rel: the base relation on which the index is created.
 * @IN resultRelInfo: includes index defination informations.
 * @Return: the index insert information object
 * @See also:
 */
IndexInsertInfo *BuildIndexInsertInfo(Relation rel, ResultRelInfo *resultRelInfo)
{
    if (resultRelInfo != NULL && resultRelInfo->ri_NumIndices > 0) {
        IndexInsertInfo *indexInfo = (IndexInsertInfo *)palloc0(sizeof(IndexInsertInfo));
        int attNo = rel->rd_att->natts;
        Form_pg_attribute *attrs = rel->rd_att->attrs;
        indexInfo->maxKeyNum = 0;
        indexInfo->indexNum = resultRelInfo->ri_NumIndices;

        /* allocate memory for basic member variables. */
        indexInfo->idxRelation = (Relation *)palloc(sizeof(Relation) * resultRelInfo->ri_NumIndices);
        indexInfo->idxInsertArgs = (InsertArg *)palloc0(sizeof(InsertArg) * resultRelInfo->ri_NumIndices);
        indexInfo->idxInsert = (CStoreInsert **)palloc0(sizeof(CStoreInsert *) * resultRelInfo->ri_NumIndices);
        indexInfo->idxKeyNum = (int *)palloc(sizeof(int) * resultRelInfo->ri_NumIndices);
        indexInfo->idxKeyAttr = (int **)palloc(sizeof(int *) * resultRelInfo->ri_NumIndices);
        RelationPtr idxRelArray = resultRelInfo->ri_IndexRelationDescs;

        /* Loop all index and get some information. */
        for (int i = 0; i < indexInfo->indexNum; ++i) {
            Oid idxOid;
            Relation rawIndexRel = idxRelArray[i];

            if (rawIndexRel->rd_rel->relam == PSORT_AM_OID) {
                /*
                 * PSort index oid is stored in index_rel->relcudescrelid
                 * We reuse relcudescrelid as real index talbe oid.
                 */
                idxOid = idxRelArray[i]->rd_rel->relcudescrelid;
            } else {
                /* for cbtree index */
                idxOid = RelationGetRelid(rawIndexRel);
            }
            indexInfo->idxRelation[i] = relation_open(idxOid, RowExclusiveLock);

            /* Initialize index insert object. */
            indexInfo->idxKeyNum[i] = resultRelInfo->ri_IndexRelationInfo[i]->ii_NumIndexAttrs;
            if (indexInfo->idxKeyNum[i] > indexInfo->maxKeyNum) {
                indexInfo->maxKeyNum = indexInfo->idxKeyNum[i];
            }
            indexInfo->idxKeyAttr[i] = (int *)palloc(sizeof(int) * indexInfo->idxKeyNum[i]);
            AttrNumber *keyPtr = resultRelInfo->ri_IndexRelationInfo[i]->ii_KeyAttrNumbers;

            /* Initialize index key logical column id. */
            for (int j = 0; j < indexInfo->idxKeyNum[i]; ++j) {
                for (int col = 0; col < attNo; ++col) {
                    if (attrs[col]->attnum == keyPtr[j]) {
                        indexInfo->idxKeyAttr[i][j] = col;
                        break;
                    }
                }
            }

            if (rawIndexRel->rd_rel->relam == PSORT_AM_OID) {
                /* Initialize the cstore insert object. */
                CStoreInsert::InitIndexInsertArg(rel, indexInfo->idxKeyAttr[i], indexInfo->idxKeyNum[i],
                                                 indexInfo->idxInsertArgs[i]);
                indexInfo->idxInsert[i] = New(CurrentMemoryContext) CStoreInsert(indexInfo->idxRelation[i],
                                                                                 indexInfo->idxInsertArgs[i], false,
                                                                                 NULL, NULL);
            }
        }
        return indexInfo;
    } else {
        return NULL;
    }
}

DfsInsert::DfsInsert(Relation relation, bool is_update, const char *parsigs, Plan *plan, MemInfoArg *ArgmemInfo)
    : m_relation(relation),
      m_writer(NULL),
      m_sorter(NULL),
      m_iterContext(NULL),
      m_nextFile(true),
      m_delta(NULL),
      m_values(NULL),
      m_nulls(NULL)
{
    m_dataDestRelation = NULL;
    m_colMap = NULL;
    m_insert_fn = NULL;
    m_dfsInsertMemInfo = NULL;
    m_filePathPrefix = makeStringInfo();
    m_desc = RelationGetDescr(relation);
    m_isUpdate = is_update;
    if (parsigs) {
        m_parsigs = makeStringInfo();
        appendStringInfo(m_parsigs, "%s", parsigs);
    } else
        m_parsigs = NULL;

    /*
     * Set the column map. When insert data into desc table, the colMap
     * will be written into the desc table also.
     */
    setColMap();
    m_end = false;
    m_indexInsertInfo = NULL;
    m_indexInfoOuterDestroy = false;

    InitInsertMemArg(plan, ArgmemInfo);
}

/*
 * @Description: init insert memory info for dfs insert.
 * @IN plan: If insert operator is directly used, the plan mem_info is used.
 * @IN ArgmemInfo: ArgmemInfo is used to passing parameters, such as update.
 * @Return: void
 * @See also: InitUpdateMemArg
 */
void DfsInsert::InitInsertMemArg(Plan *plan, MemInfoArg *ArgmemInfo)
{
    bool hasPck = false;
    int partialClusterRows = 0;

    /* get cluster key */
    partialClusterRows = RelationGetPartialClusterRows(m_relation);
    if (m_relation->rd_rel->relhasclusterkey) {
        hasPck = true;
    }

    m_dfsInsertMemInfo = (MemInfoArg *)palloc0(sizeof(struct MemInfoArg));
    /*
     * plan : Plan is for insert operator to caculate the memory.
     * ArgmemInfo: ArgmemInfo is for update operator to Pass the memory parameters.
     * others: not Memory management will be in.
     */
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        if (!hasPck) {
            m_dfsInsertMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                        : plan->operatorMemKB[0];
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsInsert(init plan) mem is not engough for the basic use: workmem is : %dKB, can spread maxMem "
                    "is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }

            m_dfsInsertMemInfo->MemSort = 0;
        } else {
            /* if has pck, we should first promise the insert memory,
             * Insert memory : must > 128mb to promise the basic use on DFS insert.
             * Sort memory : must >10mb to promise the basic use on sort.
             */
            m_dfsInsertMemInfo->MemInsert = plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE
                                                                                        : plan->operatorMemKB[0];
            if (plan->operatorMemKB[0] < DFS_MIN_MEM_SIZE) {
                MEMCTL_LOG(
                    LOG,
                    "DfsInsert(init plan pck) mem is not engough for the basic use: workmem is : %dKB, can spread "
                    "maxMem is %dKB.",
                    plan->operatorMemKB[0], plan->operatorMaxMem);
            }
            m_dfsInsertMemInfo->MemSort = plan->operatorMemKB[0] - m_dfsInsertMemInfo->MemInsert;
            if (m_dfsInsertMemInfo->MemSort < SORT_MIM_MEM) {
                m_dfsInsertMemInfo->MemSort = SORT_MIM_MEM;
                if (m_dfsInsertMemInfo->MemInsert >= DFS_MIN_MEM_SIZE)
                    m_dfsInsertMemInfo->MemInsert = m_dfsInsertMemInfo->MemInsert - SORT_MIM_MEM;
            }
        }
        m_dfsInsertMemInfo->canSpreadmaxMem = plan->operatorMaxMem;
        m_dfsInsertMemInfo->spreadNum = 0;
        m_dfsInsertMemInfo->partitionNum = 1;
        MEMCTL_LOG(DEBUG2, "DfsInsert(init plan): workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                   m_dfsInsertMemInfo->MemInsert, m_dfsInsertMemInfo->MemSort, m_dfsInsertMemInfo->canSpreadmaxMem);
    } else if (ArgmemInfo != NULL) {
        Assert(ArgmemInfo->partitionNum > 0);
        m_dfsInsertMemInfo->partitionNum = ArgmemInfo->partitionNum;
        m_dfsInsertMemInfo->canSpreadmaxMem = ArgmemInfo->canSpreadmaxMem;
        m_dfsInsertMemInfo->MemInsert = ArgmemInfo->MemInsert;
        m_dfsInsertMemInfo->MemSort = ArgmemInfo->MemSort / ArgmemInfo->partitionNum;
        m_dfsInsertMemInfo->spreadNum = ArgmemInfo->spreadNum;
        MEMCTL_LOG(DEBUG2,
                   "DfsInsert(init ArgmemInfo): workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                   m_dfsInsertMemInfo->MemInsert, m_dfsInsertMemInfo->MemSort, m_dfsInsertMemInfo->canSpreadmaxMem);
    } else {
        m_dfsInsertMemInfo->canSpreadmaxMem = 0;
        m_dfsInsertMemInfo->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        m_dfsInsertMemInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_dfsInsertMemInfo->spreadNum = 0;
        m_dfsInsertMemInfo->partitionNum = 1;
    }
}

void DfsInsert::BeginBatchInsert(int type, ResultRelInfo *resultRelInfo)
{
    /* Build index insert infomations if need */
    if (m_indexInsertInfo == NULL) {
        m_indexInsertInfo = BuildIndexInsertInfo(m_relation, resultRelInfo);
    }

    /* build the writer according to the store format. */
    if (RelationIsPAXFormat(m_relation)) {
        m_writer = dfs::writer::createOrcWriter(CurrentMemoryContext, m_relation, getDataDestRel(), m_indexInsertInfo,
                                                RelationIsValuePartitioned(m_relation) ? m_parsigs->data : NULL);
    }

    /* If relation has cluster keys, then initialize env. */
    m_sorter = NULL;
    TupleConstr *constr = m_relation->rd_att->constr;
    if (constr != NULL && constr->clusterKeyNum > 0) {
        int sortKeyNum = constr->clusterKeyNum;
        AttrNumber *sortKeys = constr->clusterKeys;
        m_sorter = New(CurrentMemoryContext) CStorePSort(m_relation, sortKeys, sortKeyNum, type, m_dfsInsertMemInfo);
    }

    /* Initialize the prefix of file path. */
    initPathPrefix();

    /* Initialize the global file space cache. */
    initSpaceAllocCache(m_relation);

    /* Initialize the memory context which will be cleaned for each insert iteration. */
    m_iterContext = AllocSetContextCreate(CurrentMemoryContext, "interate insert memory context",
                                          ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
                                          STANDARD_CONTEXT, m_dfsInsertMemInfo->MemInsert * 1024L);

    /* Initialize the buffer of tuple during VectorBatch => tuple on insert ... select ... */
    if (TO_DELTA == u_sess->attr.attr_storage.cstore_insert_mode) {
        m_delta = heap_open(m_relation->rd_rel->reldeltarelid, RowExclusiveLock);
        m_values = (Datum *)palloc(sizeof(Datum) * BatchMaxSize);
        m_nulls = (bool *)palloc(sizeof(bool) * BatchMaxSize);
    }
}

void DfsInsert::Destroy()
{
    if (RelationIsPAXFormat(m_relation)) {
        if (m_writer != NULL) {
            RemoveDfsWriteHandler(m_writer);
            DELETE_EX(m_writer);
        }
    }

    if (m_sorter) {
        DELETE_EX(m_sorter);
    }

    if (m_parsigs) {
        pfree_ext(m_parsigs->data);
        pfree_ext(m_parsigs);
    }

    if (m_filePathPrefix) {
        pfree_ext(m_filePathPrefix->data);
        pfree_ext(m_filePathPrefix);
    }

    if (CurrentMemoryContext != m_iterContext) {
        MemoryContextDelete(m_iterContext);
    }

    if (TO_DELTA == u_sess->attr.attr_storage.cstore_insert_mode && m_delta) {
        heap_close(m_delta, NoLock);
    }

    if (m_values) {
        pfree_ext(m_values);
    }

    if (m_nulls) {
        pfree_ext(m_nulls);
    }

    if (m_colMap) {
        pfree_ext(m_colMap);
    }

    if (m_dfsInsertMemInfo) {
        pfree_ext(m_dfsInsertMemInfo);
    }
}

void DfsInsert::BatchInsert(VectorBatch *batch, int option)
{
    if (TO_DELTA == u_sess->attr.attr_storage.cstore_insert_mode) {
        if (isEnd() || BatchIsNull(batch))
            return;

        /* batch => tuple */
        for (int row = 0; row < batch->m_rows; row++) {
            for (int col = 0; col < batch->m_cols; col++) {
                ScalarVector *sv = &(batch->m_arr[col]);
                Oid typid = sv->m_desc.typeId;

                m_nulls[col] = IS_NULL(sv->m_flag[row]);
                if (!m_nulls[col]) {
                    m_values[col] = convertScalarToDatum(typid, sv->m_vals[row]);
                }
            }

            TupleInsert(m_values, m_nulls, option);
        }

        return;
    }

    if (NeedPartialSort()) {
        /* Put into Sorter */
        if (!BatchIsNull(batch) && !isEnd()) {
            m_sorter->PutVecBatch(m_relation, batch);
        }

        /* Do check whether full */
        if (m_sorter->IsFull() || isEnd()) {
            VectorBatch *tbatch = NULL;
            m_sorter->RunSort();
            tbatch = m_sorter->GetVectorBatch();
            while (!BatchIsNull(tbatch)) {
                batchInsertInternal(tbatch, option, false);
                tbatch = m_sorter->GetVectorBatch();
            }

            m_sorter->Reset(isEnd());
            if (isEnd()) {
                batchInsertInternal(NULL, option, true);
            }
        }
    } else {
        batchInsertInternal(batch, option, isEnd());
    }
}

void DfsInsert::TupleInsert(Datum *values, bool *nulls, int option)
{
    if (TO_DELTA == u_sess->attr.attr_storage.cstore_insert_mode) {
        if (isEnd() || values == NULL || nulls == NULL)
            return;

        /* insert tuple into delta table */
        MemoryContextReset(m_iterContext);
        AutoContextSwitch newContext(m_iterContext);
        HeapTuple tuple = (HeapTuple)tableam_tops_form_tuple(m_delta->rd_att, values, nulls, HEAP_TUPLE);
        (void)heap_insert(m_delta, tuple, GetCurrentCommandId(true), option, NULL);

        return;
    }

    if (NeedPartialSort()) {
        /* Put into Sorter */
        if (values != NULL && nulls != NULL && !isEnd()) {
            m_sorter->PutSingleTuple(values, nulls);
        }

        /* Do check whether full */
        if (m_sorter->IsFull() || isEnd()) {
            TupleTableSlot *slot = NULL;
            m_sorter->RunSort();
            slot = m_sorter->GetTuple();
            while (!TupIsNull(slot)) {
                heap_deform_tuple((HeapTuple)slot->tts_tuple, m_desc, slot->tts_values, slot->tts_isnull);
                tupleInsertInternal(slot->tts_values, slot->tts_isnull, option, false);
                slot = m_sorter->GetTuple();
            }

            m_sorter->Reset(isEnd());
            if (isEnd()) {
                tupleInsertInternal(NULL, NULL, option, true);
            }
        }
    } else {
        tupleInsertInternal(values, nulls, option, isEnd());
    }
}

void DfsInsert::batchInsertInternal(VectorBatch *batch, int option, bool isEnd)
{
    MemoryContextReset(m_iterContext);
    AutoContextSwitch newContext(m_iterContext);

    /* Batch insert ends. */
    if (isEnd) {
        handleTailData(option);
        return;
    }

    /* If the the batch is empty, just return. But it may not mean the ending. */
    if (BatchIsNull(batch)) {
        return;
    }

    /* If we can not append more rows into the writer, then spill the buffer data to dfs file. */
    while (!m_writer->canAppend(batch->m_rows)) {
        spillOut(option);
    }

    /* Append the whole batch into buffer. */
    m_writer->appendBatch(batch);
}

void DfsInsert::tupleInsertInternal(Datum *values, bool *nulls, int option, bool isEnd)
{
    MemoryContextReset(m_iterContext);
    AutoContextSwitch newContext(m_iterContext);

    /* Batch insert ends. */
    if (isEnd) {
        handleTailData(option);
        return;
    }

    if (values == NULL || nulls == NULL) {
        return;
    }

    /* If we can not append more rows into the writer, then spill the buffer data to dfs file. */
    while (!m_writer->canAppend(1)) {
        spillOut(option);
    }

    /* Append the whole batch into buffer. */
    m_writer->appendTuple(values, nulls);
}

void DfsInsert::spillOut(int option)
{
    uint64 fileID;
    int row_in_buffer = m_writer->getBufferRows();
    int left_row_in_buffer;
    /* Check if we need to open a new file which means the last spill generate a full file. */
    if (m_nextFile) {
        /* Accquire the file path of the new file. */
        fileID = allocNextSpace(RelationGetRelFileNode(m_relation));
        m_writer->setFileID(fileID);
        char *filePath = getFilePath(fileID);
        left_row_in_buffer = m_writer->spillToNewFile(filePath);
        m_nextFile = left_row_in_buffer == 0 ? false : true;
    } else {
        left_row_in_buffer = m_writer->spillToCurFile();
        m_nextFile = left_row_in_buffer == 0 ? false : true;
    }

    /*
     * only count num spill out to dfs file, count
     * num to delta table when we do deltainsert.
     */
    if (m_isUpdate)
        pgstat_count_dfs_update(m_relation, row_in_buffer - left_row_in_buffer);
    else
        pgstat_count_dfs_insert(m_relation, row_in_buffer - left_row_in_buffer);

    /*
     * When we need a new file in the next time after the spill, insert one tuple about
     * the current file into desc table.
     */
    if (m_nextFile && m_writer->getSerializedRows() > 0) {
        descInsert(option);
    }
}

void DfsInsert::handleTailData(int option)
{
    if (TO_MAIN == u_sess->attr.attr_storage.cstore_insert_mode) {
        while (m_writer->getBufferRows() > 0) {
            spillOut(option);
        }
    } else if (m_writer->getBufferRows() > 0) {
        // If the last file is not full, put the tail data into HDFS instead of into delta table.
        Assert(TO_AUTO == u_sess->attr.attr_storage.cstore_insert_mode);

        /*
         * If the last spilling fills the file to the full, then m_nextFile is
         * true here and we jsut put the tail data into delta.
         */
        if (m_nextFile) {
            m_writer->deltaInsert(option);
        } else {
            /*
             * If the last spilling does not fill the file to the full, then m_nextFile
             * is false, so we try to put the tail data into hdfs. But there still may be some
             * data left after the spilling if the data in the buffer is too much for
             * the remaining space. For the tail data of the tail, we put them into delta in
             * the end.
             */
            spillOut(option);
        }
    }

    if (m_writer->getSerializedRows() > 0) {
        descInsert(option);
    }

    if (m_writer->getBufferRows() > 0) {
        Assert(TO_AUTO == u_sess->attr.attr_storage.cstore_insert_mode);
        m_writer->deltaInsert(option);
    }

    m_writer->handleTail();
    clearIndexInsert();
}

void DfsInsert::descInsert(int option)
{
    uint32 fileID = (uint32)m_writer->getFileID();
    int64 fileSize = m_writer->closeCurWriter(getFilePath(fileID));
    uint64 fileRows = m_writer->getTotalRows();
    if (fileRows > 0) {
        char *fileName = getFileName(fileID);
        int relCols = m_relation->rd_att->natts;
        const char *colMap = getColMap();
        DFSDesc desc(NON_PARTITION_TALBE_PART_NUM, (uint32)fileID, relCols);
        desc.SetMagic(GetCurrentTransactionId());
        desc.SetFileName(fileName);
        desc.SetRowCnt(m_writer->getTotalRows());
        desc.SetFileSize(fileSize);
        desc.setColMap(colMap, GET_COLMAP_LENGTH((unsigned int)relCols));

        Oid ownerid = m_relation->rd_rel->relowner;
        Assert(fileSize > 0);
        uint64 size = (uint64)fileSize;
        if (m_insert_fn) {
            (*m_insert_fn)(fileName, ownerid, size);
        }

        /* Store min/max information in desc table. */
        for (int i = 0; i < relCols; i++) {
            char *minstr = NULL;
            char *maxStr = NULL;
            bool hasMin = false;
            bool hasMax = false;

            if (m_writer->getMinMax(i, minstr, maxStr, hasMin, hasMax)) {
                if (hasMin) {
                    (void)desc.SetMinVal(i, minstr, strlen(minstr));
                }
                if (hasMax) {
                    (void)desc.SetMaxVal(i, maxStr, strlen(maxStr));
                }
            }
        }

        DFSDescHandler handler(1, relCols, m_relation);
        handler.Add(&desc, 1, GetCurrentCommandId(true), option);
        desc.Destroy();
    }
}

/* The following is private methods. */
void DfsInsert::initPathPrefix()
{
    StringInfo tmpPath;
    /* data redistribution for DFS table. */
    if (NULL == getDataDestRel()) {
        tmpPath = getDfsStorePath(m_relation);
    } else {
        tmpPath = getDfsStorePath(getDataDestRel());
    }

    if (RelationIsValuePartitioned(m_relation)) {
        appendStringInfo(m_filePathPrefix, "%s/%s%s_%s.%u", tmpPath->data, m_parsigs->data,
                         TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName,
                         RelationGetRelFileNode(m_relation));
    } else {
        appendStringInfo(m_filePathPrefix, "%s/%s_%s.%u", tmpPath->data, TABLESPACE_VERSION_DIRECTORY,
                         g_instance.attr.attr_common.PGXCNodeName, RelationGetRelFileNode(m_relation));
    }

    pfree_ext(tmpPath->data);
    pfree_ext(tmpPath);
}

char *DfsInsert::getFilePath(uint64 offset) const
{
    StringInfo filePath = makeStringInfo();
    appendStringInfo(filePath, "%s.%lu", m_filePathPrefix->data, offset);
    char *path = filePath->data;
    return path;
}

char *DfsInsert::getFileName(uint64 offset) const
{
    StringInfo filePath = makeStringInfo();

    if (RelationIsValuePartitioned(m_relation)) {
        appendStringInfo(filePath, "%s%s_%s.%u.%lu", m_parsigs->data, TABLESPACE_VERSION_DIRECTORY,
                         g_instance.attr.attr_common.PGXCNodeName, RelationGetRelFileNode(m_relation), offset);
    } else {
        appendStringInfo(filePath, "%s_%s.%u.%lu", TABLESPACE_VERSION_DIRECTORY,
                         g_instance.attr.attr_common.PGXCNodeName, RelationGetRelFileNode(m_relation), offset);
    }

    char *path = filePath->data;
    return path;
}

bool DfsInsert::dfsSpaceCacheExist(Oid fileOid) const
{
    bool found = false;
    LWLockAcquire(DfsSpaceCacheLock, LW_SHARED);
    hash_search(g_dfsSpaceCache, (void *)&fileOid, HASH_FIND, &found);
    LWLockRelease(DfsSpaceCacheLock);
    return found;
}

void DfsInsert::initSpaceAllocCache(Relation rel)
{
    if (!dfsSpaceCacheExist(RelationGetRelFileNode(rel))) {
        LockRelationForExtension(rel, ExclusiveLock);
        uint64 maxFileID = getMaxFileID(rel);

        if (m_indexInsertInfo != NULL) {
            uint32 maxIdxFileID = GetMaxIndexFileID(rel);
            if (maxIdxFileID > maxFileID) {
                maxFileID = maxIdxFileID;
            }
        }
        buildSpaceAllocCache(RelationGetRelFileNode(rel), maxFileID);
        UnlockRelationForExtension(rel, ExclusiveLock);
    }
}

void DfsInsert::InvalidSpaceAllocCache(Oid fileOid)
{
    LWLockAcquire(DfsSpaceCacheLock, LW_EXCLUSIVE);
    hash_search(g_dfsSpaceCache, (void *)&fileOid, HASH_REMOVE, NULL);
    LWLockRelease(DfsSpaceCacheLock);
}

uint32 DfsInsert::GetMaxIndexFileID(Relation heapRel)
{
    List *btreeIndex = NIL;
    ListCell *lc = NULL;
    uint32 maxFileID = 0;
    uint32 tmpFileID = 0;

    /* when we get the max file ID, wo need to check the TIDS in all the btree index */
    for (int i = 0; i < m_indexInsertInfo->indexNum; ++i) {
        if (m_indexInsertInfo->idxRelation[i]->rd_rel->relam == CBTREE_AM_OID) {
            btreeIndex = lappend(btreeIndex, m_indexInsertInfo->idxRelation[i]);
        }
    }

    foreach (lc, btreeIndex) {
        ItemPointer tid;
        Relation idxRel = (Relation)lfirst(lc);
        IndexScanDesc indexScan = (IndexScanDesc)index_beginscan(heapRel, idxRel, GetActiveSnapshot(), 0, 0);
        index_rescan(indexScan, NULL, 0, NULL, 0);
        while ((tid = index_getnext_tid(indexScan, ForwardScanDirection)) != NULL) {
            tmpFileID = DfsItemPointerGetFileId(tid);
            if (tmpFileID > maxFileID) {
                maxFileID = tmpFileID;
            }
        }

        index_endscan(indexScan);
    }

    if (btreeIndex != NIL) {
        list_free(btreeIndex);
        btreeIndex = NIL;
    }
    return maxFileID;
}

uint64 DfsInsert::getMaxFileID(Relation rel) const
{
    /* data redistribution for DFS table. */
    Oid descOid = rel->rd_rel->relcudescrelid;
    Relation descHeapRel = heap_open(descOid, AccessShareLock);
    TupleDesc descTupDesc = RelationGetDescr(descHeapRel);
    Relation descIndexRel = index_open(descHeapRel->rd_rel->relcudescidx, AccessShareLock);

    SysScanDesc descScan = systable_beginscan_ordered(descHeapRel, descIndexRel, SnapshotAny, 0, NULL);
    uint64 maxdescID = 0;
    HeapTuple tup = NULL;
    while ((tup = systable_getnext_ordered(descScan, BackwardScanDirection)) != NULL) {
        bool isnull = true;
        uint32 descID = DatumGetInt32(fastgetattr(tup, (uint32)Anum_pg_dfsdesc_duid, descTupDesc, &isnull));
        if (descID > maxdescID) {
            maxdescID = descID;
        }
    }

    systable_endscan_ordered(descScan);
    descScan = NULL;
    index_close(descIndexRel, AccessShareLock);
    descIndexRel = NULL;
    heap_close(descHeapRel, AccessShareLock);
    descHeapRel = NULL;

    return maxdescID;
}

void DfsInsert::buildSpaceAllocCache(Oid fileOid, uint64 maxFileID) const
{
    bool found = false;
    DFSSpaceKV *entry = NULL;

    LWLockAcquire(DfsSpaceCacheLock, LW_EXCLUSIVE);

    // We should check if all cache entries of the relation are valid.
    // If not, update them.
    // If yes, skip.
    // 
    entry = (DFSSpaceKV *)hash_search(g_dfsSpaceCache, (void *)&fileOid, HASH_ENTER, &found);
    if (entry == NULL) {
        ereport(PANIC, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_DFS),
                        errmsg("build global dfs space cache hash table failed")));
    }

    if (!found) {
        entry->offset = maxFileID;
    }

    LWLockRelease(DfsSpaceCacheLock);
}

uint64 DfsInsert::allocNextSpace(Oid fileOid) const
{
    bool found = false;
    uint64 fileId = 0;
    DFSSpaceKV *entry = NULL;
    LWLockAcquire(DfsSpaceCacheLock, LW_EXCLUSIVE);

    entry = (DFSSpaceKV *)hash_search(g_dfsSpaceCache, (void *)&fileOid, HASH_FIND, &found);
    Assert(found);

    /* For each new file, increment the file id by 1. */
    entry->offset = entry->offset + 1;
    if (entry->offset > MAX_FILE_ID) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_DFS),
                        errmsg("The number of files exceeds the limit %d.", MAX_FILE_ID)));
    }
    fileId = entry->offset;
    LWLockRelease(DfsSpaceCacheLock);
    return fileId;
}

void DfsInsert::setColMap()
{
    m_colMap = make_column_map(RelationGetDescr(m_relation));
}

static int matchOid(const void *key1, const void *key2, Size keySize)
{
    return (*(Oid *)key1 - *(Oid *)key2);
}

void DfsInsert::InitDfsSpaceCache()
{
    errno_t rc = EOK;
    HASHCTL ctl;

    if (g_dfsSpaceCache == NULL) {
        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(DFSSpaceKV);
        ctl.match = (HashCompareFunc)matchOid;
        ctl.hash = (HashValueFunc)oid_hash;
        // init size 40k, max size 80k
        g_dfsSpaceCache = HeapMemInitHash("DFS space cache", 40960, 81920, &ctl,
                                          HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
        if (g_dfsSpaceCache == NULL) {
            ereport(PANIC, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_DFS),
                            errmsg("could not initialize DFS space hash table")));
        }
    }
}

void DfsInsert::ResetDfsSpaceCache(void)
{
    if (g_dfsSpaceCache != NULL) {
        HeapMemResetHash(g_dfsSpaceCache, "DFS space cache");
    }
}
