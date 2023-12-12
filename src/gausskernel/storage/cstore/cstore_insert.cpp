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
 * cstore_insert.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_insert.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/genam.h"
#include "access/cstore_delta.h"
#include "access/cstore_rewrite.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "utils/aiomem.h"
#include "utils/datum.h"
#include "utils/gs_bitmap.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "utils/relcache.h"
#include "catalog/pg_type.h"
#include "access/cstore_am.h"
#include "storage/custorage.h"
#include "utils/builtins.h"
#include "executor/executor.h"
#include "replication/dataqueue.h"
#include "storage/lmgr.h"
#include "storage/cucache_mgr.h"
#include "access/cstore_insert.h"
#include "pgxc/pgxc.h"
#include "access/heapam.h"
#include "utils/memutils.h"
#include "utils/date.h"
#include "storage/cstore/cstorealloc.h"
#include "storage/ipc.h"
#include "catalog/pg_partition_fn.h"
#include "libpq/pqformat.h"
#include "workload/workload.h"
#include "commands/tablespace.h"
#include "optimizer/var.h"
#include "catalog/index.h"
#include "storage/time_series_compress.h"

#define MAX_CU_WRITE_REQSIZ 64

#define cstore_backwrite_quantity 8192

#define ENABLE_DELTA(batch)                                                              \
    ((batch) != NULL && ((g_instance.attr.attr_storage.enable_delta_store && IsEnd()) && \
                         ((batch)->m_rows_curnum < m_delta_rows_threshold)))

// total memory cache size by adio, used for memory control with  cstore_backwrite_max_threshold
int64 adio_write_cache_size = 0;

extern void CUListWriteAbort(int code, Datum arg);

static inline void cu_append_null_value(int which, void* cuPtr)
{
    ((CU*)cuPtr)->AppendNullValue(which);
}

static inline int str_to_uint64(const char* str, uint64* val)
{
    char* end = NULL;
    uint64 uint64_value = 0;

    Assert(str != NULL && val != NULL);
    uint32 str_len = strlen(str);
    if (str_len == 0) {
        return -1;
    }

    /* clear errno before convert */
    errno = 0;
#ifdef WIN32
    uint64_value = _strtoui64(str, &end, 10);
#else
    uint64_value = strtoul(str, &end, 10);
#endif
    if ((errno != 0) || (end != (str + str_len))) {
        return -1;
    }

    *val = uint64_value;
    return 0;
}

/* index of m_formCUFuncArray[] calls */
#define FORMCU_IDX_NONE_NULL 0
#define FORMCU_IDX_HAVE_NULL 1

/*
 * @Description: compute the max number of keys within all index relation.
 * @IN rel: result relation info
 * @Return: the max number of index keys
 * @See also:
 */
static inline int get_max_num_of_index_keys(ResultRelInfo* rel)
{
    IndexInfo** indexes = rel->ri_IndexRelationInfo;
    int max_num = indexes[0]->ii_NumIndexAttrs;

    for (int i = 1; i < rel->ri_NumIndices; ++i) {
        if (max_num < indexes[i]->ii_NumIndexAttrs) {
            /* update the max number */
            max_num = indexes[i]->ii_NumIndexAttrs;
        }
    }
    return max_num;
}

/*
 * @Description: check whether result relation has any index.
 * @IN rel: result relation info
 * @Return: true if have any index; otherwise false.
 * @See also:
 */
static inline bool relation_has_indexes(ResultRelInfo* rel)
{
    return (rel && rel->ri_NumIndices > 0);
}

CStoreInsert::CStoreInsert(_in_ Relation relation, _in_ const InsertArg& args, _in_ bool is_update_cu, _in_ Plan* plan,
                           _in_ MemInfoArg* ArgmemInfo)
    : m_fullCUSize(RelationGetMaxBatchRows(relation)), m_delta_rows_threshold(RelationGetDeltaRowsThreshold(relation))
{
    m_insert_end_flag = false;
    m_relation = relation;
    m_resultRelInfo = args.es_result_relations;
    m_bufferedBatchRows = NULL;
    m_tmpBatchRows = args.tmpBatchRows;
    m_idxBatchRow = args.idxBatchRow;
    m_cstorInsertMem = NULL;

    /* set the cstore Insert mem info. */
    InitInsertMemArg(plan, ArgmemInfo);

    m_tmpMemCnxt = AllocSetContextCreate(CurrentMemoryContext, "INSERT TEMP MEM CNXT",
                                         ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    m_batchInsertCnxt = AllocSetContextCreate(CurrentMemoryContext, "Batch Insert Mem CNXT", ALLOCSET_DEFAULT_MINSIZE, 
                                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
                                              STANDARD_CONTEXT, m_cstorInsertMem->MemInsert * 1024L);

    ADIO_RUN()
    {
        m_aio_memcnxt = AllocSetContextCreate(CurrentMemoryContext, "ADIO CU CACHE CNXT", ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        /* the other ADIO vars will be initialized later */
    }
    ADIO_ELSE()
    {
        m_aio_memcnxt = NULL;
        m_aio_cu_PPtr = NULL;
        m_aio_dispath_cudesc = NULL;
        m_aio_dispath_idx = NULL;
        m_aio_cache_write_threshold = NULL;
        m_vfdList = NULL;
    }
    ADIO_END();

    AutoContextSwitch autoMemContext(m_batchInsertCnxt);

    int attNo = m_relation->rd_att->natts;

    m_cuStorage = (CUStorage**)palloc(sizeof(CUStorage*) * attNo);
    m_cuCmprsOptions = (compression_options*)palloc(sizeof(compression_options) * attNo);

    for (int i = 0; i < attNo; ++i) {
        if (m_relation->rd_att->attrs[i].attisdropped) {
            m_cuStorage[i] = NULL;
        } else {
            // Here we must use physical column id
            CFileNode cFileNode(m_relation->rd_node, m_relation->rd_att->attrs[i].attnum, MAIN_FORKNUM);
            m_cuStorage[i] = New(CurrentMemoryContext) CUStorage(cFileNode);
        }
        /* init compression filter */
        m_cuCmprsOptions[i].reset();
    }

    /// set the compression level and extra modes.
    m_compress_modes = 0;
    heaprel_set_compressing_modes(m_relation, &m_compress_modes);

    /* set update flag */
    m_isUpdate = is_update_cu;

    BeginBatchInsert(args);
}

CStoreInsert::~CStoreInsert()
{
    m_relation = NULL;
    m_idxBatchRow = NULL;
    m_idxRelation = NULL;
    m_setMinMaxFuncs = NULL;
    m_fake_isnull = NULL;
    m_idxInsertArgs = NULL;
    m_aio_memcnxt = NULL;
    m_fake_values = NULL;
    m_delta_relation = NULL;
    m_cuCmprsOptions = NULL;
    m_estate = NULL;
    m_cuDescPPtr = NULL;
    m_delta_desc = NULL;
    m_sorter = NULL;
    m_aio_dispath_idx = NULL;
    m_tmpBatchRows = NULL;
    m_bufferedBatchRows = NULL;
    m_batchInsertCnxt = NULL;
    m_cuStorage = NULL;
    m_idxKeyAttr = NULL;
    m_resultRelInfo = NULL;
    m_tmpMemCnxt = NULL;
    m_aio_dispath_cudesc = NULL;
    m_vfdList = NULL;
    m_cuPPtr = NULL;
    m_idxKeyNum = NULL;
    m_aio_cache_write_threshold = NULL;
    m_formCUFuncArray = NULL;
    m_econtext = NULL;
    m_aio_cu_PPtr = NULL;
    m_cstorInsertMem = NULL;
    m_idxInsert = NULL;
}

void CStoreInsert::FreeMemAllocateByAdio()
{
    int attNo = m_relation->rd_att->natts;

    if (m_aio_cu_PPtr) {
        for (int i = 0; i < attNo; i++) {
            if (m_aio_cu_PPtr[i]) {
                pfree(m_aio_cu_PPtr[i]);
                m_aio_cu_PPtr[i] = NULL;
            }
        }
        pfree(m_aio_cu_PPtr);
        m_aio_cu_PPtr = NULL;
    }
    if (m_aio_dispath_cudesc) {
        for (int i = 0; i < attNo; i++) {
            if (m_aio_dispath_cudesc[i]) {
                pfree(m_aio_dispath_cudesc[i]);
                m_aio_dispath_cudesc[i] = NULL;
            }
        }
        pfree(m_aio_dispath_cudesc);
        m_aio_dispath_cudesc = NULL;
    }
    if (m_aio_dispath_idx) {
        pfree(m_aio_dispath_idx);
        m_aio_dispath_idx = NULL;
    }
    if (m_aio_cache_write_threshold) {
        pfree(m_aio_cache_write_threshold);
        m_aio_cache_write_threshold = NULL;
    }

    if (m_vfdList) {
        for (int i = 0; i < attNo; i++) {
            if (m_vfdList[i]) {
                pfree(m_vfdList[i]);
                m_vfdList[i] = NULL;
            }
        }
        pfree(m_vfdList);
        m_vfdList = NULL;
    }

    return;
}

/*
 * @Description: set m_formCUFuncArray[] function point.
 * @See also:
 */
inline void CStoreInsert::SetFormCUFuncArray(Form_pg_attribute attr, int col)
{
    if (ATT_IS_NUMERIC_TYPE(attr->atttypid) && attr->atttypmod != -1) {
        /* Numeric data type. */
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = &CStoreInsert::FormCUTNumeric<false>;
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = &CStoreInsert::FormCUTNumeric<true>;
    } else if (ATT_IS_CHAR_TYPE(attr->atttypid)) {
        /* String data type. */
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = &CStoreInsert::FormCUTNumString<false>;
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = &CStoreInsert::FormCUTNumString<true>;
    } else {
        /* copy data directly for the other data types */
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = &CStoreInsert::FormCUT<false>;
        m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = &CStoreInsert::FormCUT<true>;
    }
}

void CStoreInsert::Destroy()
{
    /* Step 1: End of batch insert */
    EndBatchInsert();

    /* Step 2: Destroy all the new objects. */
    if (m_sorter) {
        DELETE_EX(m_sorter);
    }

    int attNo = m_relation->rd_att->natts;
    for (int col = 0; col < attNo; ++col) {
        if (m_cuDescPPtr[col]) {
            delete m_cuDescPPtr[col];
            m_cuDescPPtr[col] = NULL;
        }
        if (m_cuStorage[col])
            DELETE_EX(m_cuStorage[col]);
    }

    relation_close(m_delta_relation, NoLock);

    if (relation_has_indexes(m_resultRelInfo)) {
        for (int i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
            /*
             * For partition table with cbtree/cgin index, release the fake dummy relation,
             * otherwise just close the index relation .
             */
            if ((m_idxRelation[i]->rd_rel->relam == CBTREE_AM_OID || m_idxRelation[i]->rd_rel->relam == CGIN_AM_OID) &&
                RelationIsPartition(m_relation)) {
                releaseDummyRelation(&m_idxRelation[i]);
            } else {
                relation_close(m_idxRelation[i], RowExclusiveLock);
            }

            if (m_deltaIdxRelation[i]) {
                relation_close(m_deltaIdxRelation[i], RowExclusiveLock);
            }

            /* destroy index inserter */
            if (m_idxInsert[i] != NULL) {
                DELETE_EX(m_idxInsert[i]);
                /* destroy index inserter' arguments */
                DeInitInsertArg(m_idxInsertArgs[i]);
            }
        }

        FreeExecutorState(m_estate);
        m_estate = NULL;
        m_econtext = NULL;
        pfree(m_fake_values);
        pfree(m_fake_isnull);
    }

#ifdef USE_ASSERT_CHECKING
    if (m_cuPPtr) {
        BFIO_RUN()
        {
            for (int i = 0; i < attNo; i++) {
                Assert(m_cuPPtr[i] == NULL);
            }
        }
        BFIO_END();
    }
#endif

    ADIO_RUN()
    {
        FreeMemAllocateByAdio();
        MemoryContextDelete(m_aio_memcnxt);
    }
    ADIO_END();

    /* Step 3: Destroy memory context */
    MemoryContextDelete(m_tmpMemCnxt);
    MemoryContextDelete(m_batchInsertCnxt);

    /* Step 4: Reset all the pointers */
    m_formCUFuncArray = NULL;
    m_setMinMaxFuncs = NULL;
    m_cuStorage = NULL;
    m_cuDescPPtr = NULL;
    m_cuPPtr = NULL;
    m_idxKeyAttr = NULL;
    m_idxKeyNum = NULL;
    m_idxRelation = NULL;
    m_cuCmprsOptions = NULL;
    m_fake_values = NULL;
    m_fake_isnull = NULL;

    if (m_cstorInsertMem) {
        pfree_ext(m_cstorInsertMem);
    }
}

/*
 * @Description: init insert memory info for cstore insert. There are three branches, plan is the optimizer
 *    estimation parameter passed to the storage layer for execution; ArgmemInfo is to execute the operator
 *	 from the upper layer to pass the parameter to the insert; others is uncontrolled memory.
 * @IN plan: If insert operator is directly used, the plan mem_info is used.
 * @IN ArgmemInfo: ArgmemInfo is used to passing parameters, such as update.
 * @Return: void
 * @See also: InitInsertPartMemArg
 */
void CStoreInsert::InitInsertMemArg(Plan* plan, MemInfoArg* ArgmemInfo)
{
    bool hasPck = false;
    int partialClusterRows = 0;

    /* get cluster key */
    partialClusterRows = RelationGetPartialClusterRows(m_relation);
    if (m_relation->rd_rel->relhasclusterkey) {
        hasPck = true;
    }

    m_cstorInsertMem = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        if (!hasPck) {
            m_cstorInsertMem->MemInsert = plan->operatorMemKB[0];
            m_cstorInsertMem->MemSort = 0;
        } else {
            m_cstorInsertMem->MemInsert = (int)((double)plan->operatorMemKB[0] * (double)(m_fullCUSize * 3) /
                                                (double)(m_fullCUSize * 3 + partialClusterRows));
            m_cstorInsertMem->MemSort = plan->operatorMemKB[0] - m_cstorInsertMem->MemInsert;
        }
        m_cstorInsertMem->canSpreadmaxMem = plan->operatorMaxMem;
        m_cstorInsertMem->spreadNum = 0;
        m_cstorInsertMem->partitionNum = 1;
        MEMCTL_LOG(DEBUG2, "CStoreInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,can spread "
            "mem is %dKB.",m_cstorInsertMem->MemInsert, m_cstorInsertMem->MemSort, m_cstorInsertMem->canSpreadmaxMem);
    } else if (ArgmemInfo != NULL) {
        Assert(ArgmemInfo->partitionNum > 0);
        m_cstorInsertMem->canSpreadmaxMem = ArgmemInfo->canSpreadmaxMem;
        m_cstorInsertMem->MemInsert = ArgmemInfo->MemInsert;
        m_cstorInsertMem->MemSort = ArgmemInfo->MemSort / ArgmemInfo->partitionNum;
        if (m_cstorInsertMem->MemSort < SORT_MIM_MEM)
            m_cstorInsertMem->MemSort = SORT_MIM_MEM;
        m_cstorInsertMem->spreadNum = ArgmemInfo->spreadNum;
        m_cstorInsertMem->partitionNum = ArgmemInfo->partitionNum;
        MEMCTL_LOG(DEBUG2, "CStoreInsert(init ArgmemInfo):Insert workmem is : %dKB, one paritition sort workmem: %dKB,"
                   "partition totalnum is %d, can spread mem is %dKB.", m_cstorInsertMem->MemInsert,
                   m_cstorInsertMem->MemSort, m_cstorInsertMem->partitionNum, m_cstorInsertMem->canSpreadmaxMem);
    } else {
        m_cstorInsertMem->canSpreadmaxMem = 0;
        m_cstorInsertMem->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        m_cstorInsertMem->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_cstorInsertMem->spreadNum = 0;
        m_cstorInsertMem->partitionNum = 1;
    }
}
void CStoreInsert::InitFuncPtr()
{
    int attNo = m_relation->rd_att->natts;
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;

    m_setMinMaxFuncs = (FuncSetMinMax*)palloc(attNo * sizeof(FuncSetMinMax));
    m_formCUFuncArray = (FormCUFuncArray*)palloc(sizeof(FormCUFuncArray) * attNo);
    m_cuDescPPtr = (CUDesc**)palloc(attNo * sizeof(CUDesc*));

    /*
     * Initilize Min/Max set function for all columns
     * Initilize FormCU function
     */
    for (int col = 0; col < attNo; ++col) {
        if (!attrs[col].attisdropped) {
            m_setMinMaxFuncs[col] = GetMinMaxFunc(attrs[col].atttypid);
            SetFormCUFuncArray(&attrs[col], col);
            m_cuDescPPtr[col] = New(CurrentMemoryContext) CUDesc;
        } else {
            m_setMinMaxFuncs[col] = NULL;
            m_cuDescPPtr[col] = NULL;
            m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = NULL;
            m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = NULL;
        }
    }
}

void CStoreInsert::InitColSpaceAlloc()
{
    int attNo = m_relation->rd_att->natts;
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;

    AttrNumber* attrIds = (AttrNumber*)palloc(sizeof(AttrNumber) * attNo);
    for (int i = 0; i < attNo; ++i) {
        attrIds[i] = attrs[i].attnum;

        /* Set all column use APPEND_ONLY */
        if (!attrs[i].attisdropped)
            m_cuStorage[i]->SetAllocateStrategy(APPEND_ONLY);
    }

    /* when we get the max cu ID, wo need to check the TIDS in all the btree index */
    List *indexRel = NIL;
    if (m_resultRelInfo != NULL) {
        for (int i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
            Oid amOid = m_idxRelation[i]->rd_rel->relam;
            if (amOid == CBTREE_AM_OID || amOid == CGIN_AM_OID) {
                indexRel = lappend(indexRel, m_idxRelation[i]);
            }
        }
    }

    /* Whether we need build column space cache for relation */
    CStoreAllocator::BuildColSpaceCacheForRel(m_relation, attrIds, attNo, indexRel);

    pfree_ext(attrIds);
    if (indexRel != NIL) {
        list_free(indexRel);
    }
}

void CStoreInsert::BeginBatchInsert(const InsertArg& args)
{
    GetCurrentTransactionId();

    /* Step 1: Init function pointer for FormCU and get min/max */
    InitFuncPtr();

    /*
     * Step 2: Init partial cluster key
     * If relation has cluster keys, then initialize env.
     */
    m_sorter = NULL;
    TupleConstr* constr = m_relation->rd_att->constr;
    if (tupledesc_have_pck(constr)) {
        int sortKeyNum = constr->clusterKeyNum;
        AttrNumber* sortKeys = constr->clusterKeys;
        m_sorter =
            New(CurrentMemoryContext) CStorePSort(m_relation, sortKeys, sortKeyNum, args.sortType, m_cstorInsertMem);
        Assert(NeedPartialSort());
    } else if (args.using_vectorbatch) {
        m_bufferedBatchRows =
            New(CurrentMemoryContext) bulkload_rows(m_relation->rd_att, RelationGetMaxBatchRows(m_relation), true);
    }

    /* Step3: Initilize delta information */
    InitDeltaInfo();

    /* Step 4: Initilize index information if needed */
    InitIndexInfo();

    /* Step 5: Initialize column space cache allocation */
    InitColSpaceAlloc();

    /* Step 6: Initilize CU objects. */
    m_cuPPtr = (CU**)palloc0(sizeof(CU*) * m_relation->rd_att->natts);

    /*
     * Step 7: Lock relfilenode.
     * When one column data insert a cu block, catchup read it immediately,
     * catchup maybe get zero block. This case is tested at xfs file system.
     * So insert acquire relfilenode lock to block catchup.
     */
    LockRelFileNode(m_relation->rd_node, RowExclusiveLock);

    ADIO_RUN()
    {
        m_aio_dispath_idx = (int*)palloc0(sizeof(int) * m_relation->rd_att->natts);
        m_aio_dispath_cudesc =
            (AioDispatchCUDesc_t***)palloc(sizeof(AioDispatchCUDesc_t**) * m_relation->rd_att->natts);
        m_aio_cu_PPtr = (CU***)palloc(sizeof(CU**) * m_relation->rd_att->natts);
        m_aio_cache_write_threshold = (int32*)palloc0(sizeof(int32) * m_relation->rd_att->natts);

        m_vfdList = (File**)palloc(sizeof(File*) * m_relation->rd_att->natts);

        for (int col = 0; col < m_relation->rd_att->natts; ++col) {
            m_aio_cu_PPtr[col] = (CU**)palloc(sizeof(CU*) * MAX_CU_WRITE_REQSIZ);
            m_aio_dispath_cudesc[col] =
                (AioDispatchCUDesc_t**)palloc(sizeof(AioDispatchCUDesc_t*) * MAX_CU_WRITE_REQSIZ);
            m_vfdList[col] = (File*)palloc(sizeof(File) * MAX_CU_WRITE_REQSIZ);
            errno_t rc = memset_s((char*)m_vfdList[col], sizeof(File) * MAX_CU_WRITE_REQSIZ, 0xFF, sizeof(File) * MAX_CU_WRITE_REQSIZ);
            securec_check(rc, "\0", "\0");
        }
    }
    ADIO_END();
}

void CStoreInsert::InitDeltaInfo()
{
    Oid deltaOid = m_relation->rd_rel->reldeltarelid;
    m_delta_relation = relation_open(deltaOid, RowExclusiveLock);
    m_delta_desc = m_delta_relation->rd_att;
}

/*
 * @Description: init index key logical column id.
 * @See also:
 */
void CStoreInsert::InitIndexColId(int which_index)
{
    int attrs_num = m_relation->rd_att->natts;
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    AttrNumber* keyPtr = NULL;
    List* exprList = NULL;

    keyPtr = m_resultRelInfo->ri_IndexRelationInfo[which_index]->ii_KeyAttrNumbers;
    m_idxKeyNum[which_index] = m_resultRelInfo->ri_IndexRelationInfo[which_index]->ii_NumIndexAttrs;
    m_idxKeyAttr[which_index] = (int*)palloc(sizeof(int) * m_idxKeyNum[which_index]);
    exprList = m_resultRelInfo->ri_IndexRelationInfo[which_index]->ii_Expressions;

    if (exprList == NIL) {
        for (int which_idxkey = 0; which_idxkey < m_idxKeyNum[which_index]; ++which_idxkey) {
            for (int attr_cnt = 0; attr_cnt < attrs_num; ++attr_cnt) {
                if (attrs[attr_cnt].attnum == keyPtr[which_idxkey]) {
                    m_idxKeyAttr[which_index][which_idxkey] = attr_cnt;
                    break;
                }
            }
        }
    } else {
        List* vars = pull_var_clause((Node*)exprList, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
        ListCell* lc = list_head(vars);
        for (int which_idxkey = 0; which_idxkey < m_idxKeyNum[which_index]; ++which_idxkey) {
            int keycol = keyPtr[which_idxkey];
            if (keycol == 0) {
                Var* var = (Var*)lfirst(lc);
                keycol = (int)var->varattno;
                lc = lnext(lc);
            }

            for (int attr_cnt = 0; attr_cnt < attrs_num; ++attr_cnt) {
                if (attrs[attr_cnt].attnum == keycol) {
                    m_idxKeyAttr[which_index][which_idxkey] = attr_cnt;
                    break;
                }
            }
        }
        list_free(vars);
    }
}

/*
 * @Description: init index insert info.
 * @See also:
 */
void CStoreInsert::InitIndexInfo(void)
{
    m_idxInsert = NULL;
    m_idxInsertArgs = NULL;

    if (relation_has_indexes(m_resultRelInfo)) {
        /* allocate memory for basic member variables */
        m_idxInsert = (CStoreInsert**)palloc0(sizeof(CStoreInsert*) * m_resultRelInfo->ri_NumIndices);
        m_idxInsertArgs = (InsertArg*)palloc0(sizeof(InsertArg) * m_resultRelInfo->ri_NumIndices);
        m_idxKeyAttr = (int**)palloc(sizeof(int*) * m_resultRelInfo->ri_NumIndices);
        m_idxKeyNum = (int*)palloc(sizeof(int) * m_resultRelInfo->ri_NumIndices);
        m_fake_values = (Datum*)palloc(sizeof(Datum) * m_relation->rd_att->natts);
        m_fake_isnull = (bool*)palloc(sizeof(bool) * m_relation->rd_att->natts);
        m_estate = CreateExecutorState();
        m_econtext = GetPerTupleExprContext(m_estate);
        RelationPtr idxRelArray = m_resultRelInfo->ri_IndexRelationDescs;
        m_idxRelation = (Relation*)palloc(sizeof(Relation) * m_resultRelInfo->ri_NumIndices);
        m_deltaIdxRelation = (Relation*)palloc0(sizeof(Relation) * m_resultRelInfo->ri_NumIndices);

        bool isPartition = RelationIsPartition(m_relation);

        /* Loop all index and get some information */
        for (int which_index = 0; which_index < m_resultRelInfo->ri_NumIndices; ++which_index) {
            Oid idxOid;
            Relation rawIndexRel = idxRelArray[which_index];

            /* PSort index oid is stored in index_rel->relcudescrelid.
             * We reuse relcudescrelid as real index talbe oid.
             */
            if (isPartition) {
                Oid partidxOid = getPartitionIndexOid(RelationGetRelid(rawIndexRel), RelationGetRelid(m_relation));
                Partition partIndex = partitionOpen(rawIndexRel, partidxOid, RowExclusiveLock);

                if (rawIndexRel->rd_rel->relam == PSORT_AM_OID)
                    idxOid = partIndex->pd_part->relcudescrelid;
                else {
                    m_idxRelation[which_index] = partitionGetRelation(rawIndexRel, partIndex);
#ifndef ENABLE_MULTI_NODES
                    if (m_idxRelation[which_index]->rd_index != NULL &&
                        m_idxRelation[which_index]->rd_index->indisunique) {
                        Oid deltaIdxOid = GetDeltaIdxFromCUIdx(RelationGetRelid(m_idxRelation[which_index]), true);
                        m_deltaIdxRelation[which_index] = relation_open(deltaIdxOid, RowExclusiveLock);
                    }
#endif
                    idxOid = InvalidOid;
                }
                partitionClose(rawIndexRel, partIndex, NoLock);
            } else {
                if (rawIndexRel->rd_rel->relam == PSORT_AM_OID)
                    idxOid = rawIndexRel->rd_rel->relcudescrelid;
                else
                    idxOid = RelationGetRelid(rawIndexRel);
            }

            /* open this index relation */
            if (idxOid != InvalidOid) {
                m_idxRelation[which_index] = relation_open(idxOid, RowExclusiveLock);
#ifndef ENABLE_MULTI_NODES
                Form_pg_index indexInfo = m_idxRelation[which_index]->rd_index;
                if (indexInfo != NULL && indexInfo->indisunique) {
                    Oid deltaIdxOid = GetDeltaIdxFromCUIdx(RelationGetRelid(m_idxRelation[which_index]), false);
                    m_deltaIdxRelation[which_index] = relation_open(deltaIdxOid, RowExclusiveLock);
                }
            }
#endif
            CStoreInsert::InitIndexColId(which_index);

            if (rawIndexRel->rd_rel->relam == PSORT_AM_OID) {
                /* Initilize index relation' batch buffer */
                CStoreInsert::InitIndexInsertArg(
                    m_relation, m_idxKeyAttr[which_index], m_idxKeyNum[which_index], m_idxInsertArgs[which_index]);
                /* Initilize index inserter */
                m_idxInsert[which_index] = New(CurrentMemoryContext)
                                            CStoreInsert(m_idxRelation[which_index], m_idxInsertArgs[which_index], false, NULL, NULL);
            }
        }
    }
}

void CStoreInsert::EndBatchInsert()
{
    int attNo = m_relation->rd_att->natts;

    ADIO_RUN()
    {
        CUListFlushAll(attNo);
    }
    ADIO_ELSE()
    {
        for (int i = 0; i < attNo && m_cuStorage[i] != NULL; ++i)
            m_cuStorage[i]->FlushDataFile();
    }
    ADIO_END();
}

void CStoreInsert::DoBatchInsert(int options)
{
    /* reset and fetch next batch of values */
    m_tmpBatchRows->reset(true);
    m_sorter->GetBatchValue(m_tmpBatchRows);
    while (m_tmpBatchRows->m_rows_curnum > 0) {
        /* do batch inserting */
        if (ENABLE_DELTA(m_tmpBatchRows))
            InsertDeltaTable(m_tmpBatchRows, options);
        else
            BatchInsertCommon(m_tmpBatchRows, options);

        /* reset and fetch next batch of values */
        m_tmpBatchRows->reset(true);
        m_sorter->GetBatchValue(m_tmpBatchRows);
    }
}

/*
 * Batch insert interface for copy
 */
void CStoreInsert::BatchInsert(bulkload_rows* batchRowPtr, int options)
{
    /* keep memory space from leaking during bulk-insert */
    MemoryContext oldCnxt = MemoryContextSwitchTo(m_tmpMemCnxt);

    if (NeedPartialSort()) {
        /* Put into Sorter */
        if (batchRowPtr)
            m_sorter->PutBatchValues(batchRowPtr);

        /* Do check whether full */
        if (m_sorter->IsFull() || IsEnd()) {
            m_sorter->RunSort();

            /* reset and fetch next batch of values */
            DoBatchInsert(options);

            m_sorter->Reset(IsEnd());

            /* reset and free all memory blocks */
            m_tmpBatchRows->reset(false);
        }
    } else {
        if (ENABLE_DELTA(batchRowPtr)) {
            InsertDeltaTable(batchRowPtr, options);
        } else {
            BatchInsertCommon(batchRowPtr, options);
        }
    }

    /* handle index data */
    FlushIndexDataIfNeed();

    MemoryContextReset(m_tmpMemCnxt);
    (void)MemoryContextSwitchTo(oldCnxt);
}

void CStoreInsert::BatchInsertCommon(bulkload_rows* batchRowPtr, int options)
{
    if (unlikely(batchRowPtr == NULL || batchRowPtr->m_rows_curnum == 0))
        return;

    int attno = m_relation->rd_rel->relnatts;
    int col = 0;
    Assert(attno == batchRowPtr->m_attr_num);

    CHECK_FOR_INTERRUPTS();
    /* step 1: form CU and CUDesc; */
    for (col = 0; col < attno; ++col) {
        if (!m_relation->rd_att->attrs[col].attisdropped) {
            m_cuPPtr[col] = FormCU(col, batchRowPtr, m_cuDescPPtr[col]);
            m_cuCmprsOptions[col].m_sampling_finished = true;
        }
    }
    if (m_isUpdate)
        pgstat_count_cu_update(m_relation, batchRowPtr->m_rows_curnum);
    else
        pgstat_count_cu_insert(m_relation, batchRowPtr->m_rows_curnum);

    /*
     * step 2: a) Allocate CUID and CUPointer
     *		   b) Write CU and CUDesc
     */
    SaveAll(options);

    /* step 3: batch insert index table */
    if (m_relation->rd_att->attrs[0].attisdropped) {
        int fstColIdx = CStoreGetfstColIdx(m_relation);
        InsertIdxTableIfNeed(batchRowPtr, m_cuDescPPtr[fstColIdx]->cu_id);
    } else
        InsertIdxTableIfNeed(batchRowPtr, m_cuDescPPtr[0]->cu_id);
}

void CStoreInsert::InsertDeltaTable(bulkload_rows* batchRowPtr, int options)
{
    HeapTuple tuple = NULL;

    if (batchRowPtr->m_attr_num != m_delta_desc->natts) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("The delta table's definition is not the same"
                        " with main relation, please use pg_sync_cstore_delta to adjust it.")));
    }
    Datum* values = (Datum*)palloc(sizeof(Datum) * batchRowPtr->m_attr_num);
    bool* nulls = (bool*)palloc(sizeof(bool) * batchRowPtr->m_attr_num);

    Datum idxValues[INDEX_MAX_KEYS];
    bool idxIsNull[INDEX_MAX_KEYS];
    List* idxNums = NIL;
    List* allIndexInfos = NIL;
    int i = 0;
    if (relation_has_indexes(m_resultRelInfo)) {
        for (i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
            if (m_deltaIdxRelation[i] != NULL) {
                IndexInfo* indexInfo = BuildIndexInfo(m_deltaIdxRelation[i]);
                idxNums = lappend_int(idxNums, i);
                allIndexInfos = lappend(allIndexInfos, indexInfo);
            }
        }
    }

    bool needInsertIdx = list_length(idxNums) > 0 ? true : false;
    TupleTableSlot* slot = NULL;
    if (needInsertIdx) {
        slot = MakeSingleTupleTableSlot(RelationGetDescr(m_delta_relation));
    }

    bulkload_rows_iter iter;
    iter.begin(batchRowPtr);
    while (iter.not_end()) {
        iter.next(values, nulls);
        tuple = (HeapTuple)tableam_tops_form_tuple(m_delta_desc, values, nulls);

        /* We always generate xlog for delta tuple */
        uint32 tmpVal = (uint32)options;
        tmpVal = tmpVal & (~TABLE_INSERT_SKIP_WAL);

        (void)tableam_tuple_insert(m_delta_relation, tuple, GetCurrentCommandId(true), (int)tmpVal, NULL);

        if (needInsertIdx) {
            (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);

            for (i = 0; i < list_length(allIndexInfos); i++) {
                int idxNum = list_nth_int(idxNums, i);
                IndexInfo* indexInfo = (IndexInfo*)list_nth(allIndexInfos, i);
                FormIndexDatum(indexInfo, slot, NULL, idxValues, idxIsNull);

                /* Firstly check unique constraint on the CU index. */
                CheckUniqueOnOtherIdx(m_idxRelation[idxNum], m_relation, idxValues, idxIsNull);

                /*
                 * If pass the check above, we actually insert new index item to delta index.
                 * It will check unique constraint in delta index insertion implicitly.
                 */
                (void)index_insert(m_deltaIdxRelation[idxNum],
                    idxValues,
                    idxIsNull,
                    &(tuple->t_self),
                    m_delta_relation,
                    UNIQUE_CHECK_YES);
                CheckUniqueOnOtherIdx(m_idxRelation[idxNum], m_relation, idxValues, idxIsNull);
            }
        }
    }

    pfree(values);
    pfree(nulls);

    list_free_ext(idxNums);
    list_free_ext(allIndexInfos);

    if (needInsertIdx) {
        ExecDropSingleTupleTableSlot(slot);
    }
}

void CStoreInsert::InsertNotPsortIdx(int indice)
{
    Relation indexRel = m_idxRelation[indice];
    Datum* values = (Datum*)palloc(sizeof(Datum) * m_idxBatchRow->m_attr_num);
    bool* isnull = (bool*)palloc(sizeof(bool) * m_idxBatchRow->m_attr_num);
    TupleDesc tupDesc = m_resultRelInfo->ri_RelationDesc->rd_att;
    IndexInfo** indexInfoArray = m_resultRelInfo->ri_IndexRelationInfo;
    IndexInfo* indexInfo = indexInfoArray[indice];
    bool hasIndexExpr = (indexInfo->ii_Expressions != NIL);

    if (hasIndexExpr) {
        for (int i = 0; i < tupDesc->natts; i++) {
            m_fake_values[i] = (Datum)0;
            m_fake_isnull[i] = true;
        }
    }

    bulkload_rows_iter iter;
    iter.begin(m_idxBatchRow);
    while (iter.not_end()) {
        iter.next(values, isnull);
        ItemPointer tupleid = (ItemPointer) & values[m_idxBatchRow->m_attr_num - 1];

        if (!hasIndexExpr) {
            if (indexRel->rd_index->indisunique) {
                Relation deltaIdxRel = m_deltaIdxRelation[indice];

                /* Firstly check unique constraint on the delta index. */
                CheckUniqueOnOtherIdx(deltaIdxRel, m_delta_relation, values, isnull);

                /*
                 * If pass the check above, insert the index item to CU index.
                 * It will check unique constraint in CU index insertion implicitly.
                 */
                (void)index_insert(indexRel, values, isnull, tupleid, m_relation, UNIQUE_CHECK_YES);
                CheckUniqueOnOtherIdx(deltaIdxRel, m_delta_relation, values, isnull);
            } else {
                (void)index_insert(indexRel, values, isnull, tupleid, m_relation, UNIQUE_CHECK_NO);
            }
        } else {
            for (int i = 0; i < m_idxKeyNum[indice]; i++) {
                if (!isnull[i]) {
                    m_fake_values[m_idxKeyAttr[indice][i]] = values[i];
                    m_fake_isnull[m_idxKeyAttr[indice][i]] = false;
                }
            }

            MemoryContext oldCxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(m_estate));
            HeapTuple fakeTuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, m_fake_values, m_fake_isnull);
            TupleTableSlot* fakeSlot = MakeSingleTupleTableSlot(tupDesc);

            (void)ExecStoreTuple(fakeTuple, fakeSlot, InvalidBuffer, false);
            m_econtext->ecxt_scantuple = fakeSlot;
            FormIndexDatum(indexInfo, fakeSlot, m_estate, values, isnull);

            (void)index_insert(indexRel, values, isnull, tupleid, m_relation, UNIQUE_CHECK_NO); 

            for (int i = 0; i < m_idxKeyNum[indice]; i++) {
                if (!isnull[i]) {
                    m_fake_values[m_idxKeyAttr[indice][i]] = (Datum)0;
                    m_fake_isnull[m_idxKeyAttr[indice][i]] = true;
                }
            }

            heap_freetuple(fakeTuple);
            fakeTuple = NULL;
            ExecDropSingleTupleTableSlot(fakeSlot);
            /*
             * This has been pointing to somewhere locate in the per-query memory context "m_estate" which will be free soon
             * therefore, set it to NULL.
             */
            indexInfo->ii_ExpressionsState = NIL;
            fakeSlot = NULL;
            (void)MemoryContextSwitchTo(oldCxt);
            ResetExprContext(m_econtext);
        }
    }

    pfree(values);
    pfree(isnull);
}

/*
 * InsertIdxTableIfNeed
 * When BatchInsert data, we also need update index if need
 * We need construct index key and ctid data
 */
void CStoreInsert::InsertIdxTableIfNeed(bulkload_rows* batchRowPtr, uint32 cuId)
{
    Assert(batchRowPtr);

    if (relation_has_indexes(m_resultRelInfo)) {
        /* form all tids */
        bulkload_indexbatch_set_tids(m_idxBatchRow, cuId, batchRowPtr->m_rows_curnum);

        for (int indice = 0; indice < m_resultRelInfo->ri_NumIndices; ++indice) {
            /* form index-keys data for index relation */
            for (int key = 0; key < m_idxKeyNum[indice]; ++key) {
                bulkload_indexbatch_copy(m_idxBatchRow, key, batchRowPtr, m_idxKeyAttr[indice][key]);
            }

            /* form tid-keys data for index relation */
            bulkload_indexbatch_copy_tids(m_idxBatchRow, m_idxKeyNum[indice]);

            /* update the actual number of used attributes */
            m_idxBatchRow->m_attr_num = m_idxKeyNum[indice] + 1;

            if (m_idxInsert[indice] != NULL) {
                /* insert index data into psort index relation */
                m_idxInsert[indice]->BatchInsert(m_idxBatchRow, 0);
            } else {
                /* insert index data into cbtree/cgin index relation */
                CStoreInsert::InsertNotPsortIdx(indice);
            }
        }
    }
}

void CStoreInsert::FlushIndexDataIfNeed()
{
    if (IsEnd() && m_resultRelInfo && m_idxInsert) {
        for (int i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
            if (m_idxInsert[i] != NULL) {
                m_idxInsert[i]->SetEndFlag();
                m_idxInsert[i]->BatchInsert((bulkload_rows*)NULL, 0);
            }
        }
    }
}

/*
 * @Description: all columns flush cus which cached
 * @Param[IN] attno: relation column count
 * @See also:
 */
void CStoreInsert::CUListFlushAll(int attno)
{
#ifndef ENABLE_LITE_MODE
    /* flush cu */
    for (int col = 0; col < attno; ++col) {
        int count = m_aio_dispath_idx[col];
        AioDispatchCUDesc_t** dList = m_aio_dispath_cudesc[col];
        /* submint io */
        if (count > 0) {
            FileAsyncCUWrite(dList, count);
        }
    }

    /* check flush state */
    for (int col = 0; col < attno; ++col) {
        int count = m_aio_dispath_idx[col];
        if (count > 0) {
            CUListWriteCompeleteIO(col, count);
            count = 0;
        }
        m_aio_dispath_idx[col] = count;
    }
#endif
}

/*
 * @Description: async write one cu
 * @Param[IN] attno: relation column count
 * @Param[IN] col:column idx
 * @See also:
 */
void CStoreInsert::CUWrite(int attno, int col)
{
#ifndef ENABLE_LITE_MODE
    CU* cu = m_cuPPtr[col];
    CUDesc* cuDesc = m_cuDescPPtr[col];
    CUStorage* cuStorage = m_cuStorage[col];
    AioDispatchCUDesc_t** dList = m_aio_dispath_cudesc[col];
    int count = m_aio_dispath_idx[col];
    AioDispatchCUDesc_t* aioDescp = NULL;

    Assert(cuDesc->cu_size > 0);

    /* if the cu store in two files or more, need flush the cus cached and sync write the cu */
    if (!cuStorage->IsCUStoreInOneFile(cuDesc->cu_pointer, cuDesc->cu_size)) {
        /* submint io */
        if (count > 0) {
            FileAsyncCUWrite(dList, count);
            CUListWriteCompeleteIO(col, count);
            count = 0;
        }
        m_aio_dispath_idx[col] = count;

        cuStorage->SaveCU(cu->m_compressedBuf, cuDesc->cu_pointer, cuDesc->cu_size, true);
        CStoreCUReplication(
            m_relation, cuStorage->m_cnode.m_attid, cu->m_compressedBuf, cuDesc->cu_size, cuDesc->cu_pointer);
        cu->FreeMem<false>();
        ereport(LOG, (errmodule(MOD_ADIO),
                 errmsg("CUListWrite: sync write cloumn(%d), cuid(%u), offset(%lu), size(%d)  ",
                        col, cuDesc->cu_id, cuDesc->cu_pointer, cuDesc->cu_size)));
        return;
    }

    /* when cache cu exceed upper limit, write all */
    bool need_flush_cache = false;
    CUCache->LockPrivateCache();
    if (adio_write_cache_size + cuDesc->cu_size >= u_sess->attr.attr_storage.cstore_backwrite_max_threshold * 1024LL) {
        need_flush_cache = true;
    }
    CUCache->UnLockPrivateCache();
    if (need_flush_cache) {
        ereport(LOG, (errmodule(MOD_ADIO), errmsg("CUListWrite: exceed total cache, relid(%u), size(%ld)  ",
                        m_relation->rd_node.relNode, adio_write_cache_size)));
        CUListFlushAll(attno);
    }

    /* must get count again becaue CUListFlushAll change  m_aio_dispath_idx[col] */
    count = m_aio_dispath_idx[col];

    /* when col cache cu exceed upper limit, write cache cu of this col */
    if (m_aio_cache_write_threshold[col] + cuDesc->cu_size >= cstore_backwrite_quantity * 1024LL) {
        ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CUListWrite: exceed write threshold, column(%d), count(%d), size(%d)  ",
                        col, count, m_aio_cache_write_threshold[col])));
        /* submint io */
        if (count > 0) {
            FileAsyncCUWrite(dList, count);
            CUListWriteCompeleteIO(col, count);
            count = 0;
        }
    }
    m_aio_cache_write_threshold[col] += cuDesc->cu_size;

    aioDescp = (AioDispatchCUDesc_t*)adio_share_alloc(sizeof(AioDispatchCUDesc_t));

    m_aio_cu_PPtr[col][count] = cu;

    /* iocb filled in later */
    aioDescp->aiocb.data = 0;
    aioDescp->aiocb.aio_fildes = 0;
    aioDescp->aiocb.aio_lio_opcode = 0;
    aioDescp->aiocb.u.c.buf = 0;
    aioDescp->aiocb.u.c.nbytes = 0;
    aioDescp->aiocb.u.c.offset = 0;

    aioDescp->cuDesc.buf = cu->m_compressedBuf;
    aioDescp->cuDesc.offset = cuStorage->GetCUOffsetInFile(cuDesc->cu_pointer);
    aioDescp->cuDesc.size = cuDesc->cu_size;
    aioDescp->cuDesc.fd = cuStorage->GetCUFileFd(cuDesc->cu_pointer);
    m_vfdList[col][count] = aioDescp->cuDesc.fd;
    aioDescp->cuDesc.io_error = &cu->m_adio_error;
    aioDescp->cuDesc.slotId = cuDesc->cu_id;
    aioDescp->cuDesc.cu_pointer = cuDesc->cu_pointer;
    aioDescp->cuDesc.io_finish = false;
    aioDescp->cuDesc.reqType = CUListWriteType;
    aioDescp->aiocb.aio_reqprio = CompltrPriority(aioDescp->cuDesc.reqType);

    dList[count] = aioDescp;
    io_prep_pwrite((struct iocb*)dList[count], aioDescp->cuDesc.fd, aioDescp->cuDesc.buf, aioDescp->cuDesc.size,
                   aioDescp->cuDesc.offset);
    count++;

    /* record size */
    CUCache->LockPrivateCache();
    adio_write_cache_size += cuDesc->cu_size;
    CUCache->UnLockPrivateCache();
    ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CUListWrite: increase cache size, column(%d), cu_size(%d),total cache size(%ld)",
                    col, cuDesc->cu_size, adio_write_cache_size)));

    /* submint io */
    if (count >= MAX_CU_WRITE_REQSIZ) {
        ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CUListWrite: exceed queue size, cloumn(%d), count(%d), size(%d) ",
                        col, count, m_aio_cache_write_threshold[col])));
        FileAsyncCUWrite(dList, count);
        CUListWriteCompeleteIO(col, count);
        count = 0;
    }
    m_aio_dispath_idx[col] = count;
    ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CUListWrite: add cloumn(%d), cuid(%u), offset(%lu), size(%d) ",
                    col, cuDesc->cu_id, cuDesc->cu_pointer, cuDesc->cu_size)));

    return;
#endif
}

/*
 * @Description:  write all column cu
 * @See also:
 */
void CStoreInsert::CUListWrite()
{
    int attno = m_relation->rd_rel->relnatts;
    int col = 0;

    PG_ENSURE_ERROR_CLEANUP(CUListWriteAbort, (Datum)this);
    {
        for (col = 0; col < attno; ++col) {
            if (!m_relation->rd_att->attrs[col].attisdropped) {
                if (m_cuDescPPtr[col]->cu_size > 0) {
                    CUWrite(attno, col);
                } else {
                    /* same cu and null cu need free memory early */
                    DELETE_EX(m_cuPPtr[col]);
                }
            }
        }
    }
    PG_END_ENSURE_ERROR_CLEANUP(CUListWriteAbort, (Datum)this);
    return;
}

/*
 * @Description: aio cu list write clean up, write use local resource, do abort here better
 * @Param[IN] arg: no use
 * @Param[IN] code: no use
 * @See also:
 */
void CUListWriteAbort(int code, Datum arg)
{
    int col = 0;
    CStoreInsert* insert = (CStoreInsert*)arg;
    int attno = insert->m_relation->rd_rel->relnatts;

    /* if error occur, need do some resource clean */
    for (col = 0; col < attno; ++col) {
        AioDispatchCUDesc_t** dList = insert->m_aio_dispath_cudesc[col];
        int count = insert->m_aio_dispath_idx[col];
        for (int idx = 0; idx < count; idx++) {
            if (dList[idx] == NULL) {
                continue;
            }
            AioCUDesc_t* cuDesc = &(dList[idx]->cuDesc);
            CU* cu = insert->m_aio_cu_PPtr[col][idx];
            if (cuDesc->size > 0) {
                if (cu != NULL) {
                    cu->FreeMem<false>();
                }
                CUCache->LockPrivateCache();
                adio_write_cache_size -= cuDesc->size;
                Assert(adio_write_cache_size >= 0);
                CUCache->UnLockPrivateCache();
                ereport(LOG, (errmodule(MOD_ADIO),
                         errmsg("aio cu write abort: relation(%s), colid(%d), cuid(%u), cu_size(%d), total cache "
                                "size(%ld),", RelationGetRelationName(insert->m_relation), col,
                                (uint32)cuDesc->slotId, cuDesc->size, adio_write_cache_size)));
            }
            adio_share_free((void*)dList[idx]);
            dList[idx] = NULL;
        }
    }
    return;
}

/*
 * @Description: wait the async write cu finish and check write state, then send to stanby and free resource
 * @Param[IN] col: column id
 * @Param[IN] count: column cache cu count
 * @See also:
 */
void CStoreInsert::CUListWriteCompeleteIO(int col, int count)
{
#ifndef ENABLE_LITE_MODE
    int idx = 0;
    AioDispatchCUDesc_t** dList = m_aio_dispath_cudesc[col];
    CUStorage* cuStorage = m_cuStorage[col];

    for (idx = 0; idx < count; ++idx) {
        CU* cu = m_aio_cu_PPtr[col][idx];
        AioCUDesc_t* cuDesc = &(dList[idx]->cuDesc);

        while (!cuDesc->io_finish) {
            /* see jack email, low efficient, think more */
            pg_usleep(1);
        }

        if (cu->m_adio_error) {
            ereport(ERROR, (errcode_for_file_access(),
                    errmsg("write cu failed, colid(%d) cuid(%u), offset(%lu), size(%d) : %m",
                    col, (uint32)cuDesc->slotId, cuDesc->cu_pointer, cuDesc->size), errhint("Check free disk space.")));
        }
    }

    for (idx = 0; idx < count; ++idx) {
        CU* cu = m_aio_cu_PPtr[col][idx];
        AioCUDesc_t* cuDesc = &(dList[idx]->cuDesc);

        if (cuDesc->size > 0) {
            CStoreCUReplication(
                m_relation, cuStorage->m_cnode.m_attid, cu->m_compressedBuf, cuDesc->size, cuDesc->cu_pointer);
            cu->FreeMem<false>();

            CUCache->LockPrivateCache();
            adio_write_cache_size -= cuDesc->size;
            Assert(adio_write_cache_size >= 0);
            CUCache->UnLockPrivateCache();
            ereport(DEBUG1, (errmodule(MOD_ADIO), errmsg("CUListWriteCompeleteIO: decrease cache size, column(%d)," 
                "idx(%d), total cache size(%ld)", col, idx, adio_write_cache_size)));
            m_aio_cache_write_threshold[col] -= cuDesc->size;
            Assert(m_aio_cache_write_threshold[col] >= 0);
        }
        DELETE_EX(cu);
        adio_share_free((void*)dList[idx]);
        dList[idx] = NULL;
    }

    FileAsyncCUClose(m_vfdList[col], count);
#endif
}

/* Write CU data and CUDesc */
void CStoreInsert::SaveAll(int options, _in_ const char* delBitmap)
{
    int attno = m_relation->rd_rel->relnatts, col = 0;
    uint64 totalSize = 0;
    int firstColIdx = 0;

    /*
     * IO Collector and IO Scheduler
     * for AllocSpace, attno numbers of IO requests are to send to OS at most.
     * IO is controlled before acquring lock
     */
    if (ENABLE_WORKLOAD_CONTROL)
        IOSchedulerAndUpdate(IO_TYPE_WRITE, attno, IO_TYPE_COLUMN);

    STORAGE_SPACE_OPERATION(m_relation, 0);

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(m_relation);

    /* step 1: Lock relation for extension */
    LockRelationForExtension(m_relation, ExclusiveLock);
    uint32 curCUID = CStoreAllocator::GetNextCUID(m_relation);
    for (col = 0; col < attno; ++col) {
        if (m_relation->rd_att->attrs[col].attisdropped)
            continue;
        /* step 2: Allocate space, update m_cuDescPPtr[col]->cu_pointer */
        CUDesc* cuDesc = m_cuDescPPtr[col];
        CUStorage* cuStorage = m_cuStorage[col];
        firstColIdx = col;

        cuDesc->cu_id = curCUID;
        if (likely(cuDesc->cu_size > 0)) {
            cuDesc->cu_pointer = cuStorage->AllocSpace(cuDesc->cu_size);

            totalSize += cuDesc->cu_size;
        }

        /* step 3: Save CUDesc */
        CStore::SaveCUDesc(m_relation, cuDesc, col, options);
    }

    /* storage space processing before copying column data. */
    perm_space_increase(
        m_relation->rd_rel->relowner, totalSize, RelationUsesSpaceType(m_relation->rd_rel->relpersistence));

    /* step 4: unlock extension locker */
    UnlockRelationForExtension(m_relation, ExclusiveLock);

    /* Step 5: Write CU into storage */
    ADIO_RUN()
    {
        CUListWrite();
    }
    ADIO_ELSE()
    {
        for (col = 0; col < attno; ++col) {
            if (m_relation->rd_att->attrs[col].attisdropped) {
                if (m_cuPPtr[col])
                    DELETE_EX(m_cuPPtr[col]);
                continue;
            }
            CU* cu = m_cuPPtr[col];
            CUDesc* cuDesc = m_cuDescPPtr[col];
            CUStorage* cuStorage = m_cuStorage[col];

            if (cuDesc->cu_size > 0) {
                /* IO collector and IO scheduler */
                if (ENABLE_WORKLOAD_CONTROL)
                    IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_COLUMN);

                cuStorage->SaveCU(cu->m_compressedBuf, cuDesc->cu_pointer, cuDesc->cu_size, false);
                const int CUALIGNSIZE = cuStorage->Is2ByteAlign() ? ALIGNOF_TIMESERIES_CUSIZE : ALIGNOF_CUSIZE;
                if (u_sess->attr.attr_storage.HaModuleDebug)
                    ereport(LOG, (errmsg("HA-SaveAll: rnode %u/%u/%u, col %d, blockno %lu, cuUnitCount %d",
                        m_relation->rd_node.spcNode, m_relation->rd_node.dbNode, m_relation->rd_node.relNode, 
                        cuStorage->m_cnode.m_attid, cuDesc->cu_pointer / CUALIGNSIZE, cuDesc->cu_size / CUALIGNSIZE)));

                CStoreCUReplication(
                    m_relation, cuStorage->m_cnode.m_attid, cu->m_compressedBuf, cuDesc->cu_size, cuDesc->cu_pointer);

                cu->FreeMem<false>();
            }

            /* free CU object immediately */
            if (m_cuPPtr[col])
                DELETE_EX(m_cuPPtr[col]);
        }
    }
    ADIO_END();

    /* step 6: Insert CUDesc of virtual column */
    CStore::SaveVCCUDesc(m_relation->rd_rel->relcudescrelid, m_cuDescPPtr[firstColIdx]->cu_id,
                         m_cuDescPPtr[firstColIdx]->row_count, m_cuDescPPtr[firstColIdx]->magic, options, delBitmap);

    /* Workaround for those SQL (insert) unsupported by optimizer */
    if (unlikely(!IS_PGXC_DATANODE)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("This query is not supported by optimizer in CStore."))));
    }
}

/*
 * @Description: form CU data
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuDescPtr: CU Descriptor object
 * @Return: CU object
 * @See also:
 */
CU* CStoreInsert::FormCU(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr)
{
    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    int attlen = attrs[col].attlen;
    CU* cuPtr = NULL;

    ADIO_RUN()
    {
        /* cuPtr need keep untill async write finish */
        cuPtr = New(m_aio_memcnxt) CU(attlen, attrs[col].atttypmod, attrs[col].atttypid);
    }
    ADIO_ELSE()
    {
        cuPtr = New(CurrentMemoryContext) CU(attlen, attrs[col].atttypmod, attrs[col].atttypid);
    }
    ADIO_END();

    cuDescPtr->Reset();

    int funIdx = batchRowPtr->m_vectors[col].m_values_nulls.m_has_null ? FORMCU_IDX_HAVE_NULL : FORMCU_IDX_NONE_NULL;
    (this->*(m_formCUFuncArray[col].colFormCU[funIdx]))(col, batchRowPtr, cuDescPtr, cuPtr);

    // We should not compress in two case.
    // case1) IsNullCU
    // case2) Min is the same to max in CU. In this case, we don't
    // 		  need CUStorage
    cuDescPtr->magic = GetCurrentTransactionIdIfAny();
    if (!(cuDescPtr->IsNullCU()) && !(cuDescPtr->IsSameValCU())) {
        // a little tricky to reduce the recomputation of min/max value.
        // some data type is equal to int8/int16/int32/int32. for them it
        // is not necessary to recompute the min/max value.
        m_cuTempInfo.m_valid_minmax = !NeedToRecomputeMinMax(attrs[col].atttypid);
        if (m_cuTempInfo.m_valid_minmax) {
            m_cuTempInfo.m_min_value = ConvertToInt64Data(cuDescPtr->cu_min, attlen);
            m_cuTempInfo.m_max_value = ConvertToInt64Data(cuDescPtr->cu_max, attlen);
        }
        m_cuTempInfo.m_options = (m_cuCmprsOptions + col);
        cuPtr->m_tmpinfo = &m_cuTempInfo;

        // Magic number is for checking CU data
        cuPtr->SetMagic(cuDescPtr->magic);
        cuPtr->Compress(batchRowPtr->m_rows_curnum, m_compress_modes, ALIGNOF_CUSIZE);
        cuDescPtr->cu_size = cuPtr->GetCUSize();
    }
    cuDescPtr->row_count = batchRowPtr->m_rows_curnum;

    return cuPtr;
}

/*
 * @Description: encode numeric values
 * @IN batchRowPtr: batch values about numeric
 * @IN col: which column
 * @IN/OUT cuDescPtr: CU Description
 * @IN/OUT cuPtr: CU object
 * @IN hasNull: null flag
 * @Return: whether encode numeric values successfully
 * @See also:
 */
bool CStoreInsert::TryEncodeNumeric(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr, bool hasNull)
{
    char* ptr = NULL;
    int rows = batchRowPtr->m_rows_curnum;
    int not_exact_size = 0;
    int exact_size = 0;
    int rc = 0;

    if (COMPRESS_NO == heaprel_get_compression_from_modes(m_compress_modes)) {
        // nothing to do if compression level is NO.
        return false;
    }

    /* description: future plan-memory opt. */
    numerics_statistics_out phase1_out;
    rc = memset_s(&phase1_out, sizeof(numerics_statistics_out), 0, sizeof(numerics_statistics_out));
    securec_check(rc, "\0", "\0");
    phase1_out.success = (bool*)palloc(sizeof(bool) * rows);
    phase1_out.ascale_codes = (char*)palloc(sizeof(char) * rows);
    // In order to avoid over-boundary read in the function readData, more 8 byte memory is allocated.
    phase1_out.int64_values = (int64*)palloc(sizeof(int64) * rows + 8);
    phase1_out.int32_values = (int32*)palloc(sizeof(int32) * rows + 8);

    numerics_cmprs_out phase2_out;
    rc = memset_s(&phase2_out, sizeof(numerics_cmprs_out), 0, sizeof(numerics_cmprs_out));
    securec_check(rc, "\0", "\0");

    /* set compression filter which is from bulkload inserter */
    phase2_out.filter = m_cuCmprsOptions + col;

    BatchNumeric batch = {batchRowPtr->m_vectors[col].m_values_nulls.m_vals_points,
                          batchRowPtr->m_vectors[col].m_values_nulls.m_null_bitmap,
                          batchRowPtr->m_rows_curnum, hasNull, cu_append_null_value, (void*)cuPtr
                        };
    if (NumericCompressBatchValues(&batch, &phase1_out, &phase2_out, &not_exact_size, ALIGNOF_CUSIZE)) {
        // expand the memory if needed
        Assert(cuPtr->m_srcDataSize == 0);
        if (unlikely(cuPtr->m_srcData + not_exact_size > cuPtr->m_srcBuf + cuPtr->m_srcBufSize)) {
            Assert(not_exact_size >= 0);
            cuPtr->ReallocMem((Size)cuPtr->m_srcBufSize + (uint32)not_exact_size);
        }

        ptr = NumericCopyCompressedBatchValues(cuPtr->m_srcData, &phase1_out, &phase2_out);

        if (!m_cuCmprsOptions[col].m_sampling_finished) {
            /* get adopted compression methods from the first CU sample */
            m_cuCmprsOptions[col].set_numeric_flags(*(uint16*)cuPtr->m_srcData);
        }

        /// correct the import information with CU object.
        exact_size = (ptr - cuPtr->m_srcData);

        /// Important:
        /// now for numeric compression, *m_srcDataSize* is the size of compressed
        /// integer values, but not the total size of raw numeric values which is
        /// remembered in *phase1_out.original_size*. so consider this during the
        /// decompression.
        cuPtr->m_srcDataSize = exact_size;

        (void)CStore::SetCudescModeForMinMaxVal(false, false, hasNull, 0, -1, cuDescPtr);

        /// release all the memory unused.
        NumericCompressReleaseResource(&phase1_out, &phase2_out);

        // use d-scale int64 for numeric CU store.
        // and add CU_DSCALE_NUMERIC flags to cuPtr->m_infoMode
        cuPtr->m_infoMode |= (CU_IntLikeCompressed | CU_DSCALE_NUMERIC);

        return true;
    }

    /// release all the memory unused.
    NumericCompressReleaseResource(&phase1_out, &phase2_out);
    return false;
}

/*
 * this function change the simple string values to uint64 values, it requires first char from '1' to '9',
 * and others '0'~'9', and for fix length char type, ' ' in the behind is welcome.
 * '+' '_' ',' are not deal with this time.
 * strtoul() and snprintf() are low effcient for performance, but now,is ok.
 * this function is too long and use template for performance, so i did not split it into short function
 */
template <bool bpcharType, bool hasNull, bool has_MinMax_func>
bool CStoreInsert::FormNumberStringCU(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr)
{
    int attlen = this->m_relation->rd_att->attrs[col].attlen;
    int rows = batchRowPtr->m_rows_curnum;
    bulkload_datums* batch_values = &(batchRowPtr->m_vectors[col].m_values_nulls);
    uint64 data = 0;
    char* data_buf = (char*)palloc(sizeof(char) * (MAX_LEN_CHAR_TO_BIGINT_BUF) * (rows));
    uint32 data_count = 0;
    uint32 idx = 0;
    uint32 nextDataPos = 0;
    uint32 maxStrLen = 0;
    uint32 bpcharDataLen = 0;
    // the total length excludes the var-header size of all the values
    uint32 total_len = 0;
    FuncSetMinMax* minMaxFunc = this->m_setMinMaxFuncs + col;
    bool firstFlag = true;
    bool hasMinMaxFunc = false;

    for (int i = 0; i < rows; ++i) {
        if (hasNull && batch_values->is_null(i)) {
            cuPtr->AppendNullValue(i);
            continue;
        }

        Datum v = batch_values->get_datum(i);
        char* p = DatumGetPointer(v);
        Assert(p != NULL);
        uint32 len = VARSIZE_ANY_EXHDR(p);
        // this means that it's an empty string.
        // because we cannot map an empty string to any integer
        // so don't handle this case and return false.
        if (unlikely(len == 0)) {
            pfree(data_buf);
            return false;
        }
        Assert(len > 0);

        total_len += len;
        /* fix length char type, need know origin data len */
        if (bpcharType) {
            bpcharDataLen = len;
        }
        /* dymic length char type, check length first */
        if (!bpcharType && len > MAX_LEN_CHAR_TO_BIGINT) {
            pfree(data_buf);
            return false;
        }
        if (maxStrLen < len + VARHDRSZ) {
            maxStrLen = len + VARHDRSZ;
        }
        char* ptr = VARDATA_ANY(p);
        // first byte must be 1~9
        if (*ptr > '9' || *ptr < '1') {
            pfree(data_buf);
            return false;
        }

        // this index used to write cannot be overflow.
        Assert(idx < MAX_LEN_CHAR_TO_BIGINT_BUF * (uint32)rows);

        /* data_buf store valid data num */
        data_buf[idx] = *ptr;
        --len;
        ++ptr;
        ++idx;
        while (len > 0) {
            // next byte must be 0~9 or ' '
            if (*ptr > '9' || *ptr < '0') {
                if (bpcharType) {
                    // fix length char, need check ' ' after num
                    break;
                } else {
                    // dymic length char can return not ok
                    pfree(data_buf);
                    return false;
                }
            }

            // fix length char, need check length and ' ' which all the buf left.
            // we will write extra one byte next step, so we have to check
            // the current length of digits. because not all 20 digits can be
            // converted to uint64 value, we have to finish if this case happens.
            if (bpcharType && bpcharDataLen - len >= MAX_LEN_CHAR_TO_BIGINT) {
                pfree(data_buf);
                return false;
            }

            data_buf[idx] = *ptr;
            --len;
            ++ptr;
            ++idx;
        }

        // this index used to write cannot be overflow.
        Assert(idx < MAX_LEN_CHAR_TO_BIGINT_BUF * (uint32)rows);
        if (bpcharType) {
            // each integer digits cannot be greater than 19
            Assert(bpcharDataLen - len <= MAX_LEN_CHAR_TO_BIGINT);
            MEMCTL_LOG(DEBUG2, "Assert each integer digits cannot be greater than 19 when bpcharType is true");
        }

        // set complete flag for function strtoul need it
        data_buf[idx] = '\0';
        if (bpcharType) {
            while (len > 0) {
                if (*ptr != ' ') {
                    pfree(data_buf);
                    return false;
                }
                --len;
                ++ptr;
            }
        }

        // make right idx for store next data value
        ++data_count;
        nextDataPos += MAX_LEN_CHAR_TO_BIGINT_BUF;
        idx = nextDataPos;

        if (has_MinMax_func) {
            (*(minMaxFunc))(v, cuDescPtr, &firstFlag);
            hasMinMaxFunc = true;
        }
        firstFlag = false;
    }

    /* for deform, store count */
    cuPtr->AppendValue(data_count, sizeof(uint64));

    /* do convert, must sucess */
    char* tmpDataBuf = data_buf;
    for (uint32 i = 0; i < data_count; ++i) {
        int ret = str_to_uint64(tmpDataBuf, &data);
        if (ret != 0) {
            pfree(data_buf);
            return false;
        }
        // append each integer value
        cuPtr->AppendValue(data, sizeof(uint64));
        tmpDataBuf += MAX_LEN_CHAR_TO_BIGINT_BUF;
    }

    // for deform, store src data size but excludes
    // their var-header size.
    cuPtr->AppendValue(total_len, sizeof(uint64));

    (void)CStore::SetCudescModeForMinMaxVal(firstFlag, hasMinMaxFunc, hasNull, maxStrLen, attlen, cuDescPtr);

    pfree(data_buf);
    cuPtr->m_infoMode |= CU_IntLikeCompressed;
    return true;
}

template <bool hasNull>
bool CStoreInsert::TryFormNumberStringCU(
    int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr, uint32 atttypid)
{
    bool ret = false;
    FuncSetMinMax func = *(this->m_setMinMaxFuncs + col);

    if (COMPRESS_NO == heaprel_get_compression_from_modes(this->m_compress_modes)) {
        return ret;
    }

    if (atttypid == BPCHAROID) {
        /* for type define bpchar, we do not know the length, so we don't do the change */
        if (this->m_relation->rd_att->attrs[col].atttypmod == -1) {
            return ret;
        }
        if (func == NULL) {
            ret = this->FormNumberStringCU<true, hasNull, false>(col, batchRowPtr, cuDescPtr, cuPtr);
        } else {
            ret = this->FormNumberStringCU<true, hasNull, true>(col, batchRowPtr, cuDescPtr, cuPtr);
        }
    } else {
        if (func == NULL) {
            ret = this->FormNumberStringCU<false, hasNull, false>(col, batchRowPtr, cuDescPtr, cuPtr);
        } else {
            ret = this->FormNumberStringCU<false, hasNull, true>(col, batchRowPtr, cuDescPtr, cuPtr);
        }
    }

    return ret;
}

/*
 * @Description: init CU memory for copying
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuPtr: CU object
 * @IN hasNull: has-null flag
 * @Return: total data size
 * @See also:
 */
Size CStoreInsert::FormCUTInitMem(CU* cuPtr, bulkload_rows* batchRowPtr, int col, bool hasNull)
{
    bulkload_vector* vector = batchRowPtr->m_vectors + col;

    /* compute the total size */
    Size dtSize = vector->data_size();
    Size initialSize = cuPtr->GetCUHeaderSize() + sizeof(Datum) + dtSize;
    initialSize = MAXALIGN(initialSize);
    cuPtr->InitMem((uint32)initialSize, batchRowPtr->m_rows_curnum, hasNull);
    return dtSize;
}

/*
 * @Description: copy and form CU data for common datatype
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuDescPtr: CU Descriptor object
 * @IN/OUT cuPtr: CU object
 * @IN dtSize: total data size
 * @IN hasNull: has-null flag
 * @Return:
 * @See also:
 */
void CStoreInsert::FormCUTCopyMem(
    CU* cuPtr, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, Size dtSize, int col, bool hasNull)
{
    bulkload_vector* vector = batchRowPtr->m_vectors + col;
    Form_pg_attribute attr = &this->m_relation->rd_att->attrs[col];

    /* copy null-bitmap */
    if (hasNull) {
        /* we have computed the total memory size including null bitmap */
        Size bpsize = bitmap_size(batchRowPtr->m_rows_curnum);
        errno_t rc = memcpy_s(cuPtr->m_nulls, bpsize, vector->m_values_nulls.m_null_bitmap, bpsize);
        securec_check(rc, "\0", "\0");
    }

    /* copy all the data */
    vector->data_copy(cuPtr->m_srcData);
    cuPtr->m_srcDataSize = dtSize;

    /* set min/max for CU Desc */
    bool hasMinMaxFunc = !IsCompareDatumDummyFunc(vector->m_minmax.m_compare);
    int maxStrLen = vector->m_minmax.m_varstr_maxlen;
    vector->m_minmax.m_finish_compare(vector->m_minmax.m_min_buf, vector->m_minmax.m_max_buf, cuDescPtr);

    /* Check whether CU is filled with the same value
     * Check whether CU is filled with all NULL value
     */
    (void)CStore::SetCudescModeForMinMaxVal(
        vector->m_values_nulls.m_all_null, hasMinMaxFunc, hasNull, maxStrLen, attr->attlen, cuDescPtr);
}

/*
 * @Description: form CU data for common datatype
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuDescPtr: CU Descriptor object
 * @IN/OUT cuPtr: CU object
 * @Return:
 * @See also:
 */
template <bool hasNull>
void CStoreInsert::FormCUT(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr)
{
    Size dtSize = this->FormCUTInitMem(cuPtr, batchRowPtr, col, hasNull);
    this->FormCUTCopyMem(cuPtr, batchRowPtr, cuDescPtr, dtSize, col, hasNull);
}

/*
 * @Description: form CU data for numeric datatype
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuDescPtr: CU Descriptor object
 * @IN/OUT cuPtr: CU object
 * @Return:
 * @See also:
 */
template <bool hasNull>
void CStoreInsert::FormCUTNumeric(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr)
{
    Size dtSize = this->FormCUTInitMem(cuPtr, batchRowPtr, col, hasNull);

    if (this->TryEncodeNumeric(col, batchRowPtr, cuDescPtr, cuPtr, hasNull)) {
        return;
    }

    /* clean-up work here.
     * ignore the nulls memory even though it will be set later again.
     */
    cuPtr->m_srcDataSize = 0;

    this->FormCUTCopyMem(cuPtr, batchRowPtr, cuDescPtr, dtSize, col, hasNull);

    /* change m_formCUFuncArray[col] to FormCUTCommon()
     * if the first time of encoding-numeric fails
     */
    if (!this->m_cuCmprsOptions[col].m_sampling_finished) {
        this->m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = &CStoreInsert::FormCUT<false>;
        this->m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = &CStoreInsert::FormCUT<true>;
    }
}

/*
 * @Description: form CU data for number string
 * @IN batchRowPtr: batchrows
 * @IN col: which column to handle
 * @IN/OUT cuDescPtr: CU Descriptor object
 * @IN/OUT cuPtr: CU object
 * @IN hasNull: has-null flag
 * @Return:
 * @See also:
 */
template <bool hasNull>
void CStoreInsert::FormCUTNumString(int col, bulkload_rows* batchRowPtr, CUDesc* cuDescPtr, CU* cuPtr)
{
    Form_pg_attribute attr = &this->m_relation->rd_att->attrs[col];

    Size dataSize = this->FormCUTInitMem(cuPtr, batchRowPtr, col, hasNull);

    if (this->TryFormNumberStringCU<hasNull>(col, batchRowPtr, cuDescPtr, cuPtr, attr->atttypid)) {
        return;
    }

    /* clean-up work here. Ignore the nulls memory even though it will be set later again. */
    cuPtr->m_srcDataSize = 0;

    this->FormCUTCopyMem(cuPtr, batchRowPtr, cuDescPtr, dataSize, col, hasNull);
    /* change m_formCUFuncArray[col] to FormCUTCommon()	if the first time of encoding-number-string fails */
    if (!this->m_cuCmprsOptions[col].m_sampling_finished) {
        this->m_formCUFuncArray[col].colFormCU[FORMCU_IDX_NONE_NULL] = &CStoreInsert::FormCUT<false>;
        this->m_formCUFuncArray[col].colFormCU[FORMCU_IDX_HAVE_NULL] = &CStoreInsert::FormCUT<true>;
    }
}

/*
 * @Description: Interface for insertion with undecompressed CUData. Modeled after
 *			    BatchInsertCommon. Used only ATExecCStoreMergePartition so far.
 *			    Further application will require modification
 * @IN CUData: BatchCUData that holds CUDescs, CUptrs, and Delbitmap
 * @IN options: insert option
 * @See also: BatchInsertCommon
 */
void CStoreInsert::CUInsert(_in_ BatchCUData* CUData, _in_ int options)
{
    if (unlikely(CUData == NULL))
        return;

    int attno = m_relation->rd_rel->relnatts;
    int col = 0;
    Assert(attno == CUData->colNum);

    // step 1: pass CUDesc and CU to CStoreInsert
    for (col = 0; col < attno; ++col) {
        if (m_relation->rd_att->attrs[col].attisdropped)
            continue;
        *m_cuDescPPtr[col] = *CUData->CUDescData[col];
        m_cuPPtr[col] = CUData->CUptrData[col];
    }

    /*
     * step 2: a) Allocate CUID and CUPointer
     *         b) Write CU and CUDesc
     */
    SaveAll(options, CUData->bitmap);
}

void CStoreInsert::SortAndInsert(int options)
{
    /* run sort */
    m_sorter->RunSort();

    /* reset and fetch next batch of values */
    DoBatchInsert(options);

    m_sorter->Reset(IsEnd());

    /* reset and free all memory blocks */
    m_tmpBatchRows->reset(false);
}

// BatchInsert
// Vertor interface for Insert into CStore table
void CStoreInsert::BatchInsert(_in_ VectorBatch* pBatch, _in_ int options)
{
    Assert(pBatch || IsEnd());

    /* keep memory space from leaking during bulk-insert */
    MemoryContext oldCnxt = MemoryContextSwitchTo(m_tmpMemCnxt);

    // Step 1: relation has partial cluster key
    // We need put data into sorter contatiner, and then do
    // batchinsert data
    if (NeedPartialSort()) {
        Assert(m_tmpBatchRows);

        if (pBatch) {
            Assert(pBatch->m_cols == m_relation->rd_att->natts);
            m_sorter->PutVecBatch(m_relation, pBatch);
        }

        if (m_sorter->IsFull() || IsEnd()) {
            SortAndInsert(options);
        }
    }

    // Step 2: relation doesn't have partial cluster key
    // We need cache data until batchrows is full
    else {
        Assert(m_bufferedBatchRows);

        // If batch row is full, we can do batchinsert now
        if (IsEnd()) {
            if (ENABLE_DELTA(m_bufferedBatchRows)) {
                InsertDeltaTable(m_bufferedBatchRows, options);
            } else {
                BatchInsertCommon(m_bufferedBatchRows, options);
            }
            m_bufferedBatchRows->reset(true);
        }

        // we need cache data until batchrows is full
        if (pBatch) {
            Assert(pBatch->m_rows <= BatchMaxSize);
            Assert(pBatch->m_cols && m_relation->rd_att->natts);
            Assert(m_bufferedBatchRows->m_rows_maxnum > 0);
            Assert(m_bufferedBatchRows->m_rows_maxnum % BatchMaxSize == 0);

            int startIdx = 0;
            while (m_bufferedBatchRows->append_one_vector(
                       RelationGetDescr(m_relation), pBatch, &startIdx, m_cstorInsertMem)) {
                BatchInsertCommon(m_bufferedBatchRows, options);
                m_bufferedBatchRows->reset(true);
            }
            Assert(startIdx == pBatch->m_rows);
        }
    }

    // Step 3: We must update index data for this batch data
    // if end of batchInsert
    FlushIndexDataIfNeed();

    MemoryContextReset(m_tmpMemCnxt);
    (void)MemoryContextSwitchTo(oldCnxt);
}

inline bool CStoreInsert::NeedPartialSort() const
{
    return m_sorter != NULL;
}

void CStoreInsert::SetEndFlag()
{
    m_insert_end_flag = true;
}

/*
 * @Description: init insert arguments for psort index
 * @OUT args:  insert argunets of batch buffer.
 * @IN heap_rel: heap relation
 * @IN key_map:  index keys map
 * @IN nkeys:    number of index keys
 * @See also:
 */
void CStoreInsert::InitIndexInsertArg(Relation heap_rel, const int* key_map, int nkeys, InsertArg& args)
{
    /* plus TID system attribute */
    int nkeys_plus_tid = nkeys + 1;
    errno_t rc;

    struct tupleDesc *index_tupdesc = CreateTemplateTupleDesc(nkeys_plus_tid, false);

    index_tupdesc->natts = nkeys_plus_tid;
    /* the following are not important to us, just init them */
    index_tupdesc->constr = NULL;
    index_tupdesc->initdefvals = NULL;
    index_tupdesc->tdhasoid = false;
    index_tupdesc->tdrefcount = 1;
    index_tupdesc->tdtypeid = InvalidOid;
    index_tupdesc->tdtypmod = -1;

    /* set attribute point exlcuding TID field */
    for (int i = 0; i < nkeys; ++i) {
        rc = memcpy_s(&index_tupdesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, &heap_rel->rd_att->attrs[key_map[i]],
                      ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");
    }

    /* set TID attribute */
    FormData_pg_attribute tid_attr;
    init_tid_attinfo(&tid_attr);
    rc = memcpy_s(&index_tupdesc->attrs[nkeys], ATTRIBUTE_FIXED_PART_SIZE, &tid_attr, ATTRIBUTE_FIXED_PART_SIZE);
    securec_check(rc, "\0", "\0");

    args.es_result_relations = NULL;
    /* psort index will use tuple sort */
    args.sortType = TUPLE_SORT;
    /* psort index shouldn't have any index, so make it NULL */
    args.idxBatchRow = NULL;
    /* init temp batch buffer for psort index */
    args.tmpBatchRows =
        New(CurrentMemoryContext) bulkload_rows(index_tupdesc, RelationGetMaxBatchRows(heap_rel), true);
    args.using_vectorbatch = false;

    /* release temp memory */
    pfree(index_tupdesc);
}

/*
 * @Description: init all kinds of batch buffers during bulk-load.
 * @OUT args: all kinds of batch buffers.
 * @IN using_vectorbatch: see InsertArg::using_vectorbatch comments.
 * @IN rel: relation to query
 * @IN resultRelInfo: resulting relation info
 * @See also:
 */
void CStoreInsert::InitInsertArg(Relation rel, ResultRelInfo* resultRelInfo, bool using_vectorbatch, InsertArg& args)
{
    TupleConstr* constr = rel->rd_att->constr;
    int maxValuesCount = RelationGetMaxBatchRows(rel);

    args.es_result_relations = resultRelInfo;
    args.using_vectorbatch = using_vectorbatch;

    /* create temp batchrows if relation has PCK */
    if (tupledesc_have_pck(constr)) {
        args.tmpBatchRows = New(CurrentMemoryContext) bulkload_rows(rel->rd_att, maxValuesCount, true);
    }

    /* create BatchRows for index, if relation has index. */
    if (relation_has_indexes(resultRelInfo)) {
        /* find max number of index keys,
         * and we will reuse these batch buffers.
         */
        int max_key_num = get_max_num_of_index_keys(resultRelInfo);
        args.idxBatchRow = bulkload_indexbatch_init(max_key_num, maxValuesCount);
    }
}

/*
 * @Description: destroy insert arguments
 * @IN args: insert arguments
 * @See also:
 */
void CStoreInsert::DeInitInsertArg(InsertArg& args)
{
    if (args.tmpBatchRows) {
        DELETE_EX(args.tmpBatchRows);
    }
    if (args.idxBatchRow) {
        bulkload_indexbatch_deinit(args.idxBatchRow);
    }
}

/*
 * @Description: Flash all cache data in CStoreInsert such as Partial Cluster Key, PSort Index and m_bufferedBatchRows.
 *                         This function is different with macro FLUSH_DATA.
 *                         Should called by somewhere need reduce CStoreInsert memory during the  INSERT INTO column
 * table .
 * @IN options:  cstore insert options
 */
void CStoreInsert::FlashData(int options)
{
    /* keep memory space from leaking during bulk-insert */
    MemoryContext oldCnxt = MemoryContextSwitchTo(m_tmpMemCnxt);

    /* flush partial cluster key */
    if (NeedPartialSort()) {
        Assert(m_tmpBatchRows);

        /* check sorter whether have data which not insert */
        if (m_sorter->GetRowNum() > 0) {
            SortAndInsert(options);
        }
    } else {
        /* flash buffered batch rows */
        if (m_bufferedBatchRows && m_bufferedBatchRows->m_rows_curnum > 0) {
            if (ENABLE_DELTA(m_bufferedBatchRows))
                InsertDeltaTable(m_bufferedBatchRows, options);
            else
                BatchInsertCommon(m_bufferedBatchRows, options);
            m_bufferedBatchRows->reset(true);
        }
    }

    /* flush index data */
    if (m_resultRelInfo && m_idxInsert) {
        for (int i = 0; i < m_resultRelInfo->ri_NumIndices; ++i) {
            if (m_idxInsert[i] != NULL)
                m_idxInsert[i]->FlashData(options);
        }
    }

    /* reset memory context */
    MemoryContextReset(m_tmpMemCnxt);
    (void)MemoryContextSwitchTo(oldCnxt);
}

CStorePartitionInsert::CStorePartitionInsert(_in_ Relation relation, _in_ ResultRelInfo* es_result_relations,
                                             _in_ int type, _in_ bool is_update_cu, _in_ Plan* plan, _in_ MemInfoArg* ArgmemInfo)
    : CStore()
{
    m_memInfo = NULL;

    /* reset total used memory to 0 */
    t_thrd.cstore_cxt.bulkload_memsize_used = 0;
    m_relation = relation;

    /* get max number of one CU values */
    m_fullCUSize = RelationGetMaxBatchRows(m_relation);

    /* increase partition map refcount */
    incre_partmap_refcount(m_relation->partMap);

    /* get partition number  */
    m_partitionNum = getNumberOfRangePartitions(m_relation);

    InitInsertPartMemArg(plan, ArgmemInfo);

    m_cstorePartMemContext = AllocSetContextCreate(CurrentMemoryContext, "CStore PARAENT PARTITION INSERT",
                                ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                ALLOCSET_DEFAULT_MAXSIZE, STANDARD_CONTEXT, m_memInfo->MemInsert * 1024L);

    m_tmpMemCnxt = AllocSetContextCreate(CurrentMemoryContext, "CStore PARTITIONED TEMP INSERT",
                                         ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    AutoContextSwitch contextSwitcher(m_cstorePartMemContext);

    CStoreInsert::InitInsertArg(m_relation, es_result_relations, false, m_insertArgs);

    int colNums = RelationGetDescr(m_relation)->natts;
    m_val = (Datum*)palloc(sizeof(Datum) * colNums);
    m_null = (bool*)palloc(sizeof(bool) * colNums);

    m_tmp_val = (Datum*)palloc(sizeof(Datum) * colNums);
    m_tmp_null = (bool*)palloc(sizeof(bool) * colNums);

    /* partiton cache strategy, default is cache each partition data as possible */
    m_cache_strategy = CACHE_EACH_PARTITION_AS_POSSIBLE;

    /* set last insert partition invaild */
    m_last_insert_partition = -1;

    /* lazy-create bulk-inserter for each partition */
    m_insert = (CStoreInsert**)palloc0(m_partitionNum * sizeof(CStoreInsert*));

    /* form relation for each partition */
    m_storePartFakeRelation = (Relation*)palloc0(m_partitionNum * sizeof(Relation));

    InitValueCache();
    /* set update flag */
    m_isUpdate = is_update_cu;
}

CStorePartitionInsert::~CStorePartitionInsert()
{
    m_batchFreeList = NULL;
    m_partUseCahce = NULL;
    m_cstorePartMemContext = NULL;
    m_diskFileSize = NULL;
    m_memInfo = NULL;
    m_storePartFakeRelation = NULL;
    m_partTmpBatch = NULL;
    m_partValCache = NULL;
    m_tmp_null = NULL;
    m_null = NULL;
    m_insert = NULL;
    m_tmp_val = NULL;
    m_partRelBatchRows = NULL;
    m_tmpMemCnxt = NULL;
    m_val = NULL;
}

void CStorePartitionInsert::Destroy()
{
    // Step 1: Destroy all NEW objects
    for (int i = 0; i < m_partitionNum; i++) {
        if (m_insert[i]) {
            DELETE_EX(m_insert[i]);
        }

        if (m_storePartFakeRelation[i]) {
            releaseDummyRelation(m_storePartFakeRelation + i);
            m_storePartFakeRelation[i] = NULL;
        }
    }

    CStoreInsert::DeInitInsertArg(m_insertArgs);
    DeInitValueCache();

    decre_partmap_refcount(m_relation->partMap);

    // Step 2: Destroy all the memory contexts
    MemoryContextDelete(m_cstorePartMemContext);
    MemoryContextDelete(m_tmpMemCnxt);

    // Step 3: Reset pointers
    m_val = NULL;
    m_null = NULL;
    m_tmp_val = NULL;
    m_tmp_null = NULL;
    m_insert = NULL;
    m_storePartFakeRelation = NULL;
    m_partRelBatchRows = NULL;
    m_partUseCahce = NULL;
    m_partValCache = NULL;
    m_diskFileSize = NULL;

    if (m_memInfo) {
        pfree_ext(m_memInfo);
    }
}

/*
 * @Description: init insert memory info for cstore part insert. There are three branches, plan is the optimizer
 *    estimation parameter passed to the storage layer for execution; ArgmemInfo is to execute the operator
 *	 from the upper layer to pass the parameter to the insert; others is uncontrolled memory.
 * @IN plan: If insert operator is directly used, the plan mem_info is given to execute.
 * @IN ArgmemInfo: ArgmemInfo is used to passing parameters of mem_info to execute, such as update.
 * @Return: void
 * @See also: InitInsertMemArg
 */
void CStorePartitionInsert::InitInsertPartMemArg(Plan* plan, MemInfoArg* ArgmemInfo)
{
    bool hasPck = false;

    if (m_relation->rd_rel->relhasclusterkey) {
        hasPck = true;
    }
    /* init the memory info of memory adjustment */
    m_memInfo = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));
    if (plan != NULL && plan->operatorMemKB[0] > 0) {
        /* if has pck, meminfo = insert + sort. */
        if (!hasPck) {
            m_memInfo->MemInsert = plan->operatorMemKB[0];
            m_memInfo->MemSort = 0;
        } else {
            m_memInfo->MemInsert = plan->operatorMemKB[0] * 1 / 3;
            m_memInfo->MemSort = plan->operatorMemKB[0] - m_memInfo->MemInsert;
        }
        m_memInfo->canSpreadmaxMem = plan->operatorMaxMem;
        m_memInfo->spreadNum = 0;
        m_memInfo->partitionNum = m_partitionNum;
        MEMCTL_LOG(DEBUG2, "CStorePartInsert(init plan):Insert workmem is : %dKB, sort workmem: %dKB,"
                   "parititions totalnum is(%d)can spread maxMem is %dKB.", m_memInfo->MemInsert,
                   m_memInfo->MemSort, m_memInfo->partitionNum, m_memInfo->canSpreadmaxMem);
    } else if (ArgmemInfo != NULL) {
        m_memInfo->MemInsert = ArgmemInfo->MemInsert;
        m_memInfo->MemSort = ArgmemInfo->MemSort;
        m_memInfo->canSpreadmaxMem = ArgmemInfo->canSpreadmaxMem;
        m_memInfo->spreadNum = ArgmemInfo->spreadNum;
        m_memInfo->partitionNum = ArgmemInfo->partitionNum;
        MEMCTL_LOG(DEBUG2, "CStorePartInsert(init ArgmemInfo):Insert workmem is : %dKB, sort workmem: %dKB,"
                   "parititions totalnum is(%d)can spread maxMem is %dKB.", m_memInfo->MemInsert,
                   m_memInfo->MemSort, m_memInfo->partitionNum, m_memInfo->canSpreadmaxMem);
    } else {
        /*
         * For static load, a single partition of sort Mem is 512MB, and there is no need to subdivide sort Mem.
         * So, set the partitionNum is 1 for all partition table.
         */
        m_memInfo->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        m_memInfo->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        m_memInfo->canSpreadmaxMem = 0;
        m_memInfo->spreadNum = 0;
        m_memInfo->partitionNum = 1;
    }
}

void CStorePartitionInsert::InitValueCache()
{
    m_partRelBatchRows = (bulkload_rows**)palloc0(m_partitionNum * sizeof(bulkload_rows*));
    m_partUseCahce = (bool*)palloc0(sizeof(bool) * m_partitionNum);
    m_partValCache = (PartitionValueCache**)palloc0(m_partitionNum * sizeof(PartitionValueCache*));
    m_batchFreeList = NIL;

    m_partTmpBatch = New(CurrentMemoryContext) bulkload_rows(m_relation->rd_att, m_fullCUSize, true);

    m_diskFileSize = (Size*)palloc0(sizeof(Size) * m_partitionNum);

    for (int i = 0; i < m_partitionNum; i++) {
        /* we lazy-alloc batchrows' buffers for bulkloading */
        m_batchFreeList =
            lappend(m_batchFreeList, New(CurrentMemoryContext) bulkload_rows(m_relation->rd_att, m_fullCUSize, false));
    }
}

void CStorePartitionInsert::DeInitValueCache()
{
    // because it maybe holds fd resources except memory
    // m_partValCache[i] must be destroyed
    for (int i = 0; i < m_partitionNum; i++) {
        if (m_partValCache[i])
            DELETE_EX(m_partValCache[i]);
    }
}

bool CStorePartitionInsert::CacheValues(Datum* values, const bool* nulls, int partitionidx)
{
    bulkload_rows* partitionBatchRows = GetBatchRow(partitionidx);

    if (partitionBatchRows == NULL) {
        /* the size of one tuple is not larger than 1G. */
        bulkload_rows batchRow(RelationGetDescr(m_relation), RelationGetMaxBatchRows(m_relation), false);
        Size tuple_size = batchRow.calculate_tuple_size(RelationGetDescr(m_relation), values, nulls);
        if ((BULKLOAD_MAX_MEMSIZE - m_diskFileSize[partitionidx]) < tuple_size) {
            /* need to flush the partition on disk. */
            return false;
        }
        /* if failed to get a BatchRows, we should write out the values to cache file.
         * otherwise, cache the values to BatchRows.
         */
        m_diskFileSize[partitionidx] += m_partValCache[partitionidx]->WriteRow(values, nulls);
        m_partValCache[partitionidx]->m_rows++;
        Assert(m_fullCUSize >= m_partValCache[partitionidx]->m_rows);
    } else {
        /* the size of one tuple is not larger than 1G. */
        Size tuple_size = partitionBatchRows->calculate_tuple_size(RelationGetDescr(m_relation), values, nulls);
        if ((BULKLOAD_MAX_MEMSIZE - partitionBatchRows->m_using_blocks_total_rawsize) < tuple_size) {
            if (unlikely(partitionBatchRows->m_rows_curnum == 0)) {
                ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("the size of one tuple reaches the limit (1GB).")));
            }
            /* need to flush the partition on cache. */
            return false;
        }
        (void)partitionBatchRows->append_one_tuple(values, nulls, RelationGetDescr(m_relation));
        Assert(m_fullCUSize >= partitionBatchRows->m_rows_curnum);
    }
    return true;
}

void CStorePartitionInsert::SaveCacheValues(int partitionidx, bool doFlush, int options)
{
    bulkload_rows* batch = m_partRelBatchRows[partitionidx];

    if (unlikely(m_insert[partitionidx] == NULL)) {
        AutoContextSwitch contextSwitcher(m_cstorePartMemContext);
        m_insert[partitionidx] = New(m_cstorePartMemContext)
                                CStoreInsert(GetPartFakeRelation(partitionidx), m_insertArgs, m_isUpdate, NULL, m_memInfo);
    }

    // Check which partition batchrows/valuecache is full,
    // we should do real batchinsert
    if (batch != NULL) {
        Assert(!m_partUseCahce[partitionidx]);

        if (batch->full_rownum() || doFlush) {
            m_insert[partitionidx]->BatchInsert(batch, options);

            /* destory and release batchrow of this partition */
            ReleaseBatchRow(partitionidx);
            batch->Destroy();
        }
    } else if (m_partUseCahce[partitionidx]) {
        PartitionValueCache* partitionValCache = m_partValCache[partitionidx];

        Assert(partitionValCache != NULL);
        if (partitionValCache->m_rows == m_fullCUSize || doFlush ||
            m_diskFileSize[partitionidx] >= BULKLOAD_MAX_MEMSIZE) {
            MemoryContext old_memcxt = MemoryContextSwitchTo(m_tmpMemCnxt);
            if (!m_partTmpBatch->m_inited) {
                m_partTmpBatch->init(RelationGetDescr(m_relation), m_fullCUSize);
            }
            partitionValCache->EndWrite();
            /* m_val may alloc new memory space, so use m_tmpMemCnxt to control that */
            partitionValCache->ReadBatchRow(m_partTmpBatch, m_tmp_val, m_tmp_null);
            partitionValCache->Reset();

            m_insert[partitionidx]->BatchInsert(m_partTmpBatch, options);
            m_partTmpBatch->Destroy();
            m_partUseCahce[partitionidx] = false;
            m_diskFileSize[partitionidx] = 0;

            MemoryContextReset(m_tmpMemCnxt);
            (void)MemoryContextSwitchTo(old_memcxt);
        }
    } else if (doFlush) {
        m_insert[partitionidx]->BatchInsert((bulkload_rows*)NULL, options);
    }
}

void CStorePartitionInsert::MoveBatchRowToPartitionValueCache(int partitionidx)
{
    // move memory cache (m_partRelBatchRows) to disk cache (m_partValCache)
    // before move, the memory cached data rows should < m_fullCUSize, and disk cache should empty
    Assert(partitionidx < m_partitionNum);

    if (m_partValCache[partitionidx] == NULL) {
        AutoContextSwitch contextSwitcher(m_cstorePartMemContext);
        m_partValCache[partitionidx] = New(CurrentMemoryContext) PartitionValueCache(GetPartFakeRelation(partitionidx));
    }

    bulkload_rows* batch = m_partRelBatchRows[partitionidx];
    PartitionValueCache* partitionValCache = m_partValCache[partitionidx];

    Assert(batch && batch->m_rows_curnum < m_fullCUSize);
    Assert(!m_partUseCahce[partitionidx]);

    bulkload_rows_iter iter;
    iter.begin(batch);
    while (iter.not_end()) {
        /* m_val[] just point to existing memory,
         * not allocing new space and copying data again.
         */
        iter.next(m_val, m_null);
        m_diskFileSize[partitionidx] += partitionValCache->WriteRow(m_val, m_null);
    }
    iter.end();

    /* update current rows' number */
    partitionValCache->m_rows = batch->m_rows_curnum;

    /* set flag to use PartitionValueCache */
    m_partUseCahce[partitionidx] = true;

    /* release and destroy batchrow */
    ReleaseBatchRow(partitionidx);
    batch->Destroy();
}

void CStorePartitionInsert::BatchInsert(_in_ Datum* values, _in_ const bool* nulls, _in_ int options)
{
    Relation partitionedRel = m_relation;
    Const consts[MAX_RANGE_PARTKEY_NUMS];
    Const* partKeyValues[MAX_RANGE_PARTKEY_NUMS] = {};
    PartitionIdentifier matchPartition;

    CHECK_FOR_INTERRUPTS();
    // Step 1: We need know this batchrow should be which partition and then
    // store into m_batchrows for each partition
    int2vector* partKeyColumn = ((RangePartitionMap*)(partitionedRel)->partMap)->base.partitionKey;
    int partkeyColNum = partKeyColumn->dim1;

    Assert(partkeyColNum <= MAX_RANGE_PARTKEY_NUMS);
    for (int i = 0; i < partkeyColNum; i++) {
        int col_location = partKeyColumn->values[i];
        partKeyValues[i] = transformDatum2Const(
                               (partitionedRel)->rd_att, col_location, values[col_location - 1], nulls[col_location - 1], &consts[i]);
    }
    partitionRoutingForValue((partitionedRel), partKeyValues, partkeyColNum, true, false, (&matchPartition));

    if (!OidIsValid(matchPartition.partitionId)) {
        if (options & HEAP_INSERT_SKIP_ERROR) {
            char *relname = get_rel_name((Oid) (partitionedRel)->rd_id);
            ereport(LOG, (errmsg("inserted partition key does not map to any table partition in %s", relname)));
            return;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("inserted partition key does not map to any table partition")));
        }
    }    

    // Step 2: Caches values to BatchRows or PartitionCacheValues.
    // flash partition insert when swith partition
    if (m_cache_strategy == FLASH_WHEN_SWICH_PARTITION && IsSwitchPartition(matchPartition.partSeq)) {
        // flush cachevlues to cstoreinsert
        SaveCacheValues(m_last_insert_partition, true, options);

        Assert(m_insert[m_last_insert_partition]);

        // flush cstoreinsert data
        m_insert[m_last_insert_partition]->FlashData(options);
    }

    while (!CacheValues(values, nulls, matchPartition.partSeq)) {
        /* If we find the free space for the tuple is exhausted, we need to flush this partition regardless of
         * whether this partition is in cache or disk temp file.
         */
        SaveCacheValues(matchPartition.partSeq, true, options);
    }

    // Step 3:Save cached values if number of values == m_fullCUSize
    SaveCacheValues(matchPartition.partSeq, false, options);

    m_last_insert_partition = matchPartition.partSeq;

    // Step 4: If total memcontext size exceeds partition_max_cache_size, then find the biggest partition memcontext,
    // and move it to disk cache.
    // judge if has enough memory to insert partition.
    bool moveCacheToDisk = false;
    int moveBatchCount = 0;
    if (!hasEnoughMem(matchPartition.partSeq, t_thrd.cstore_cxt.bulkload_memsize_used)) {
        moveCacheToDisk = true;
    }
    while ((m_memInfo->MemInsert > 0 &&
            (int64)t_thrd.cstore_cxt.bulkload_memsize_used >= ((int64)(m_memInfo->MemInsert)) * 1024) ||
           moveCacheToDisk) {
        int FlushIdx = -1;

        Size old_memsize = t_thrd.cstore_cxt.bulkload_memsize_used;

        FlushIdx = findBiggestPartition();

        /* move memory cache to disk cache */
        if (FlushIdx >= 0) {
            MoveBatchRowToPartitionValueCache(FlushIdx);
            moveBatchCount++;
            ereport(LOG, (errmsg("The insert memory reaches the upper limit, it needs to flush some partition "
                "to the disk. The used memory is %lu, the controlled memory limit is %dKB, the partition relation " 
                "is %s, the flush partition id is %d, the flush count is %d.",t_thrd.cstore_cxt.bulkload_memsize_used,
                m_memInfo->MemInsert, RelationGetRelationName(m_relation), FlushIdx, moveBatchCount)));
        }

        if (t_thrd.cstore_cxt.bulkload_memsize_used == old_memsize) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("The max insert memory is too small, it may occur "
            "endless loop. The used memory is %lu, the controlled memory limit is %dKB, the partition relation is "
            "%s, the flush partition id is %d.",  t_thrd.cstore_cxt.bulkload_memsize_used,
            m_memInfo->MemInsert, RelationGetRelationName(m_relation), FlushIdx)));
        }

        moveCacheToDisk = false;
    }
}

void CStorePartitionInsert::BatchInsert(VectorBatch* batch, int hi_options)
{
    if (batch == NULL)
        return;

    int ncols = batch->m_cols;
    int rows = batch->m_rows;

    FormData_pg_attribute* attrs = m_relation->rd_att->attrs;
    ScalarVector* pVec = NULL;
    ScalarValue* pVals = NULL;

    AutoContextSwitch contextSwitcher(m_tmpMemCnxt);

    /* loop, decode and insert all attributes within one tuple */
    for (int rowCnt = 0; rowCnt < rows; ++rowCnt) {
        for (int col = 0; col < ncols; ++col) {
            pVec = batch->m_arr + col;
            if (pVec->IsNull(rowCnt)) {
                m_null[col] = true;
                m_val[col] = (Datum)0;
                continue;
            }

            m_null[col] = false;
            pVals = pVec->m_vals;
            if (pVec->m_desc.encoded == false)
                m_val[col] = pVals[rowCnt];
            else {
                Assert(attrs[col].attlen < 0 || attrs[col].attlen > 8);
                Datum v = ScalarVector::Decode(pVals[rowCnt]);
                /* m_val[] just point to existing memory, not allocing new space. */
                m_val[col] = (attrs[col].attlen < 0) ? v : PointerGetDatum((char*)v + VARHDRSZ_SHORT);
            }
        }

        /* insert all attributes within one tuple every time */
        BatchInsert(m_val, m_null, hi_options);
    }

    /* reset temp memory context after each bulk-insert. */
    MemoryContextReset(m_tmpMemCnxt);
}

void CStorePartitionInsert::EndBatchInsert()
{
    for (int i = 0; i < m_partitionNum; i++) {
        if (m_insert[i]) {
            m_insert[i]->SetEndFlag();
            SaveCacheValues(i, true, 0);
            m_insert[i]->EndBatchInsert();
        }
    }
}

bulkload_rows* CStorePartitionInsert::GetBatchRow(int partitionIdx)
{
    Assert(partitionIdx < m_partitionNum);

    /* Using disk cache */
    if (m_partUseCahce[partitionIdx]) {
        Assert(m_partRelBatchRows[partitionIdx] == NULL);
        return NULL;
    }

    /* Check if the partition has held a batchrow */
    bulkload_rows* batch = m_partRelBatchRows[partitionIdx];
    if (batch == NULL) {
        Assert(list_length(m_batchFreeList) > 0);

        AutoContextSwitch contextSwitcher(m_cstorePartMemContext);

        /* get a new batchrow from free list */
        batch = (bulkload_rows*)lfirst(list_head(m_batchFreeList));
        m_batchFreeList = list_delete_first(m_batchFreeList);
        m_partRelBatchRows[partitionIdx] = batch;

        /* re-init those batch buffers if needed.
         * make sure that re-init action is performed under m_cstorePartMemContext.
         */
        if (!batch->m_inited) {
            batch->init(RelationGetDescr(m_relation), m_fullCUSize);
        }
    }
    return batch;
}

void CStorePartitionInsert::ReleaseBatchRow(int partitionIdx)
{
    Assert(partitionIdx < m_partitionNum);

    bulkload_rows* batch = m_partRelBatchRows[partitionIdx];
    if (batch != NULL) {
        /* reset batchrows for partition */
        m_partRelBatchRows[partitionIdx] = NULL;

        Assert(!list_member(m_batchFreeList, batch));

        /* put this batchrows into free list */
        MemoryContext oldcontext = MemoryContextSwitchTo(m_cstorePartMemContext);
        m_batchFreeList = lappend(m_batchFreeList, batch);
        MemoryContextSwitchTo(oldcontext);
    }
}

/*
 * @Description: set cstore partition insert cache strategy
 * @IN partition_cache_strategy: cache strategy
 */
void CStorePartitionInsert::SetPartitionCacheStrategy(int partition_cache_strategy)
{
    m_cache_strategy = partition_cache_strategy;
}

/*
 * @Description: judge whether CStorePartitionInsert  has enoughMem. If sysbusy and spread failed,
 *				it means we has not enough memory.
 * @IN partitionidx: the partition id
 * @IN memsize_used: the memory is used now, always means bulkload_memsize_used
 */
bool CStorePartitionInsert::hasEnoughMem(int partitionidx, int64 memsize_used)
{
    bool sysBusy = gs_sysmemory_busy((int64)memsize_used, true);
    if (memsize_used > (int64)(m_memInfo->MemInsert * 1024L) || sysBusy) {
        if (sysBusy) {
            MEMCTL_LOG(LOG, "CStorePartitionInsert(%d) early spilled, workmem: %dKB, usedmem: %ldKB",
                       partitionidx, m_memInfo->MemInsert, memsize_used / 1024L);
            pgstat_add_warning_early_spill();
        } else if (m_memInfo->canSpreadmaxMem > m_memInfo->MemInsert) {
            int64 spreadMem = Min(Min(dywlm_client_get_memory(), m_memInfo->MemInsert),
                                  m_memInfo->canSpreadmaxMem - m_memInfo->MemInsert - m_memInfo->MemSort);
            if (spreadMem > m_memInfo->MemInsert * 0.1) {
                m_memInfo->MemInsert += spreadMem;
                m_memInfo->spreadNum++;
                AllocSet context = (AllocSet)m_cstorePartMemContext;
                context->maxSpaceSize += spreadMem * 1024L;

                MEMCTL_LOG(DEBUG2, "CStorePartitionInsert(%d) auto mem spread %ldKB succeed, and work mem is %dKB, spreadNum is %d.",
                           partitionidx, spreadMem, m_memInfo->MemInsert, m_memInfo->spreadNum);
                return true;
            } else {
                MEMCTL_LOG(LOG, "CStorePartitionInsert(%d) auto mem spread %ldKB failed, and work mem is %dKB.",
                           partitionidx, spreadMem, m_memInfo->MemInsert);
            }

            if (m_memInfo->spreadNum > 0) {
                pgstat_add_warning_spill_on_memory_spread();
            }

            if (!sysBusy) {
                MEMCTL_LOG(LOG, "CStorePartitionInsert(%d) Disk Spilled: totalSpace: %dKB, used is: %ldKB",
                           partitionidx, m_memInfo->MemInsert, memsize_used / 1024L);
            }
        }
        return false;
    }

    return true;
}

/*
 * @Description: find the biggest partition and memcontext and  move partition cache to disk cache
 * @IN partition_cache_strategy: cache strategy
 */
int CStorePartitionInsert::findBiggestPartition() const
{
    Size maxSize = 0ULL;
    int needFlushIdx = -1;

    /* find the biggest partition memcontext */
    for (int i = 0; i < m_partitionNum; ++i) {
        if (!m_partUseCahce[i] && m_partRelBatchRows[i] && m_partRelBatchRows[i]->total_memory_size() > (Size)maxSize) {
            maxSize = m_partRelBatchRows[i]->total_memory_size();
            needFlushIdx = i;
        }
    }
    return needFlushIdx;
}

PartitionValueCache::PartitionValueCache(Relation rel)
{
    // create temp file
    m_fd = OpenTemporaryFile(false);
    m_rel = rel;

    ADIO_RUN()
    {
        m_buffer = (char*)CStoreMemAlloc::Palloc(PartitionValueCache::MAX_BUFFER_SIZE);
    }
    ADIO_ELSE()
    {
        m_buffer = (char*)palloc(PartitionValueCache::MAX_BUFFER_SIZE);
    }
    ADIO_END();

    Reset();
    Assert(m_fd > 0);
}

PartitionValueCache::~PartitionValueCache()
{
    m_rel = NULL;
    m_buffer = NULL;
}

void PartitionValueCache::Destroy()
{
    if (m_fd > 0)
        FileClose(m_fd);

    ADIO_RUN()
    {
        CStoreMemAlloc::Pfree(m_buffer);
        m_buffer = NULL;
    }
    ADIO_ELSE()
    {
        pfree(m_buffer);
        m_buffer = NULL;
    }
    ADIO_END();
}

Size PartitionValueCache::WriteRow(Datum* values, const bool* nulls)
{
    TupleDesc tupleDesc = RelationGetDescr(m_rel);
    int natts = tupleDesc->natts;
    FormData_pg_attribute* attrs = tupleDesc->attrs;
    Size row_size = 0;

    for (int i = 0; i < natts; i++) {
        Datum val = values[i];
        int att_len = attrs[i].attlen;

        if (nulls[i]) {
            InternalWriteInt(-1);
            continue;
        }

        if (att_len > 0 && att_len <= 8) {
            InternalWriteInt(att_len);
            InternalWrite((char*)&val, att_len);
            row_size += att_len;
        } else if (att_len > 8) {
            InternalWriteInt(att_len);
            InternalWrite(DatumGetPointer(val), att_len);
            row_size += att_len;
        } else if (att_len == -1) {
            InternalWriteInt(VARSIZE_ANY_EXHDR(DatumGetPointer(val)));
            InternalWrite(VARDATA_ANY(DatumGetPointer(val)), VARSIZE_ANY_EXHDR(DatumGetPointer(val)));
            row_size += VARSIZE_ANY(DatumGetPointer(val));
        } else {
            int len = strlen(DatumGetPointer(val)) + 1;
            InternalWriteInt(len);
            InternalWrite(DatumGetPointer(val), len);
            row_size += len;
        }
    }
    return row_size;
}

int PartitionValueCache::ReadRow(_out_ Datum* values, _out_ bool* nulls)
{
    TupleDesc tupleDesc = RelationGetDescr(m_rel);
    FormData_pg_attribute* attrs = tupleDesc->attrs;
    int natts = tupleDesc->natts;
    int i;
    int nread = 0;

    for (i = 0; i < natts; ++i) {
        int len = 0;
        Datum val = (Datum)0;

        // Get length of value
        int retval = InternalRead((char*)&len, sizeof(len));

        if (retval < 0)
            goto end;

        nread += sizeof(int);

        // if value is 'NULL', length == -1
        if (len == -1) {
            nulls[i] = true;
            values[i] = (Datum)0;
            continue;
        }

        if (attrs[i].attlen > 0 && attrs[i].attlen <= 8) {
            Assert(len == tupleDesc->attrs[i].attlen);
            retval = InternalRead((char*)&(val), len);
        } else if (attrs[i].attlen > 8 || attrs[i].attlen == -2) {
            val = (Datum)palloc(len);
            retval = InternalRead(DatumGetPointer(val), len);
        } else if (attrs[i].attlen == -1) {
            val = (Datum)palloc(VARHDRSZ + len);
            SET_VARSIZE(val, VARHDRSZ + len);
            retval = InternalRead(VARDATA(DatumGetPointer(val)), len);
        }

        if (retval < 0)
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("Invalid partition value cache record."))));

        nulls[i] = false;
        values[i] = val;
        nread += len;
    }

    return nread;

end:
    // we expect to fetch an complete record, or a NULL record.
    // if i==0, we think there is no record in cache and read is over.
    // otherwise, read fails and some error happens.
    if (i > 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("read incomplete record from partition value cache."))));

    return EOF;
}

int PartitionValueCache::ReadBatchRow(bulkload_rows* batch, Datum* values, bool* nulls)
{
    TupleDesc tupleDesc = RelationGetDescr(m_rel);
    int64 nread = 0;

    m_bufCursor = 0;
    m_dataLen = 0;
    m_readOffset = 0;

    while (nread < (int64)m_writeOffset) {
        int ret = ReadRow(values, nulls);
        if (unlikely(ret < 0))
            break;

        (void)batch->append_one_tuple(values, nulls, tupleDesc);
        nread += ret;
    }

    Assert(nread == (int64)m_writeOffset);
    return batch->m_rows_curnum;
}

void PartitionValueCache::Reset()
{
    m_rows = 0;
    m_writeOffset = 0;
    m_readOffset = 0;
    m_bufCursor = 0;
    m_dataLen = 0;
}

int PartitionValueCache::InternalRead(char* buf, int len)
{
    bool eof = false;
    int inlen = 0;
    int copiedlen = 0;
    errno_t rc = EOK;

    while (len > 0) {
        if (m_dataLen == m_bufCursor)
            eof = !FillBuffer();

        if (eof && m_dataLen == m_bufCursor)
            return -1;

        if (m_dataLen - m_bufCursor <= len)
            inlen = m_dataLen - m_bufCursor;
        else
            inlen = len;

        rc = memcpy_s(buf + copiedlen, inlen, m_buffer + m_bufCursor, inlen);
        securec_check(rc, "", "");
        m_bufCursor += inlen;
        copiedlen += inlen;
        len -= inlen;
    }

    return 0;
}

int PartitionValueCache::InternalWrite(const char* buf, int len)
{
    int outlen = 0;
    int copiedlen = 0;
    errno_t rc = EOK;

    while (len > 0) {
        if (m_bufCursor + len >= PartitionValueCache::MAX_BUFFER_SIZE)
            outlen = PartitionValueCache::MAX_BUFFER_SIZE - m_bufCursor;
        else
            outlen = len;

        len -= outlen;
        rc = memcpy_s(m_buffer + m_bufCursor, outlen, buf + copiedlen, outlen);
        securec_check(rc, "", "");
        copiedlen += outlen;
        m_bufCursor += outlen;

        if (m_bufCursor == PartitionValueCache::MAX_BUFFER_SIZE) {
            FlushData();
            m_bufCursor = 0;
        }
    }
    return 0;
}

void PartitionValueCache::FlushData()
{
    if (likely(m_bufCursor > 0)) {
        int retval;
        if (ENABLE_DSS) {
            char *buffer_ori = (char*)palloc(BLCKSZ + m_bufCursor);
            char *buffer_ali = (char*)BUFFERALIGN(buffer_ori);
            errno_t rc = memcpy_s(buffer_ali, m_bufCursor, m_buffer, m_bufCursor);
            securec_check(rc, "", "");
            retval = FilePWrite(m_fd, buffer_ali, m_bufCursor, (off_t)m_writeOffset);
            pfree(buffer_ori);
            buffer_ali = NULL;
        } else {
            retval = FilePWrite(m_fd, m_buffer, m_bufCursor, m_writeOffset);
        }
        if (retval < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write cache file \"%s\": %m", FilePathName(m_fd))));
        m_writeOffset += retval;
    }
}

int PartitionValueCache::FillBuffer()
{
    int retval = FilePRead(m_fd, m_buffer, PartitionValueCache::MAX_BUFFER_SIZE, m_readOffset);
    if (retval < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read cache file \"%s\": %m", FilePathName(m_fd))));
    m_bufCursor = 0;
    m_dataLen = retval;
    m_readOffset += retval;
    return retval;
}
