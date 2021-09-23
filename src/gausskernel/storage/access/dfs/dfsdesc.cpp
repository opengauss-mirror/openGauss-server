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
 * dfsdesc.cpp
 *    routines to support DFSStore
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfsdesc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <sys/file.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_am.h"
#include "access/cstore_roughcheck_func.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "dfsdesc.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/snapshot.h"
#include "access/heapam.h"

#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#define MIN_COMPACT_FILE_SIZE (5 * 1024 * 1024)

/* **************** DFSDesc implement************** */
DFSDesc::DFSDesc()
{
    m_DescId = ~0;
    m_PartId = ~0;
    m_RowCnt = 0;
    m_FileSize = 0;
    m_ColCnt = 0;
    m_MinMaxBaseSize = MIN_MAX_SIZE;
    m_DelMapBaseSize = 0;
    m_Magic = 0;
    m_Xmin = 0;
    m_MinVal = NULL;
    m_MaxVal = NULL;
    m_DelMap = NULL;
    m_colMap = NULL;
    m_colMapLen = 0;
    m_FileName = NULL;
    m_name_len = 0;
    ItemPointerSetInvalid(&tid);
}

DFSDesc::DFSDesc(uint32 PartId, uint32 DescId, uint32 ColCnt)
{
    m_DescId = DescId;
    m_PartId = PartId;
    m_RowCnt = 0;
    m_FileSize = 0;
    m_ColCnt = ColCnt;
    m_MinMaxBaseSize = MIN_MAX_SIZE;
    m_DelMapBaseSize = 0;
    m_Magic = 0;
    m_Xmin = 0;
    m_MinVal = NULL;
    m_MaxVal = NULL;
    m_DelMap = NULL;
    m_colMap = NULL;
    m_colMapLen = 0;
    m_FileName = NULL;
    m_name_len = 0;
    ItemPointerSetInvalid(&tid);
}

/*
 * @Description: set the min value
 * @IN Col: column id
 * @IN pMinVal: the string of min value
 * @IN Size: the size of string
 * @Return: 0 if success, 1 if fail
 * @See also:
 */
int DFSDesc::SetMinVal(uint32 Col, const char *pMinVal, uint32 Size)
{
    if ((Col > m_ColCnt) || (pMinVal == NULL)) {
        return 1;
    }

    /*
     * not allocate mem for min&max values
     */
    if (m_MinVal == NULL) {
        /*
         * allocate fail
         */
        if (AllocMinMax() != 0) {
            return 1;
        }
    }

    if (m_MinVal[Col] == NULL) {
        m_MinVal[Col] = (stMinMax *)palloc0(sizeof(stMinMax));
    }

    /* only 31 bytes are meaningful, the last one is always '\0' */
    uint32 CpyLen = Size < MIN_MAX_SIZE - 1 ? Size : (MIN_MAX_SIZE - 1);
    m_MinVal[Col]->Len = CpyLen;
    errno_t Rc = memcpy_s(m_MinVal[Col]->szContent, MIN_MAX_SIZE - 1, pMinVal, CpyLen);
    securec_check(Rc, "", "");
    m_MinVal[Col]->szContent[CpyLen] = '\0';
    return 0;
}

/*
 * @Description: set the max value
 * @IN Col: column id
 * @IN pMinVal: the string of max value
 * @IN Size: the size of string
 * @Return: 0 if success, 1 if fail
 * @See also:
 */
int DFSDesc::SetMaxVal(uint32 Col, const char *pMaxVal, uint32 Size)
{
    if ((Col > m_ColCnt) || (pMaxVal == NULL)) {
        return 1;
    }

    /*
     * not allocate mem for min&max values
     */
    if (m_MaxVal == NULL) {
        if (AllocMinMax() != 0) {
            /* allocate fail */
            return 1;
        }
    }

    if (m_MaxVal[Col] == NULL) {
        m_MaxVal[Col] = (stMinMax *)palloc0(sizeof(stMinMax));
    }

    /* only 31 bytes are meaningful, the last one is always '\0' */
    uint32 CpyLen = Size < MIN_MAX_SIZE - 1 ? Size : MIN_MAX_SIZE - 1;
    m_MaxVal[Col]->Len = CpyLen;
    errno_t Rc = memcpy_s(m_MaxVal[Col]->szContent, MIN_MAX_SIZE - 1, pMaxVal, CpyLen);
    securec_check(Rc, "", "");

    /* If the size exceeds 31, enlarge the max scope by +1 on the 31st byte. */
    if (Size > MIN_MAX_SIZE - 1)
        m_MaxVal[Col]->szContent[CpyLen - 1] = m_MaxVal[Col]->szContent[CpyLen - 1] + 1;
    m_MaxVal[Col]->szContent[CpyLen] = '\0';

    return 0;
}

int DFSDesc::MaskDelMap(const char *pDelMap, uint32 Size, bool ChkDelHasDone)
{
    errno_t Rc = EOK;

    if (pDelMap == NULL) {
        return 1;
    }

    /* not allocate mem for del map */
    if (m_DelMap == NULL) {
        if (AllocDelMap() != 0) { /* allocate fail */
            return 1;
        }
    }

    uint32 CpyLen = Size < m_DelMapBaseSize ? Size : m_DelMapBaseSize;
    if (ChkDelHasDone) {
        for (uint32 Loop = 0; Loop < CpyLen; Loop++) {
            if ((((unsigned char)pDelMap[Loop]) & ((unsigned char)m_DelMap[Loop])) != (unsigned char)0) {
                ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmodule(MOD_DFS),
                                errmsg("These rows have been deleted or updated")));
            } else {
                m_DelMap[Loop] = (((unsigned char)pDelMap[Loop]) | ((unsigned char)m_DelMap[Loop]));
            }
        }
    } else {
        Rc = memcpy_s(m_DelMap, m_DelMapBaseSize, pDelMap, CpyLen);
        securec_check(Rc, "", "");
    }

    return 0;
}

/*
 * @Description: allocate the memory of min and max values
 * @Return: true if row number or total memory reaches its limitation.
 * @See also:
 */
int DFSDesc::AllocMinMax()
{
    if (m_ColCnt == 0) {
        return 1;
    }

    /*
     * allocate for min
     */
    if (m_MinVal == NULL) {
        m_MinVal = (stMinMax **)palloc0(sizeof(stMinMax *) * m_ColCnt);
    }

    /*
     * allocate for max
     */
    if (m_MaxVal == NULL) {
        m_MaxVal = (stMinMax **)palloc0(sizeof(stMinMax *) * m_ColCnt);
    }

    return 0;
}

/*
 * @Description: free the memory of min and max values
 * @See also:
 */
void DFSDesc::DeallocMinMax()
{
    uint32 Loop = 0;

    if (m_MinVal != NULL) {
        for (Loop = 0; Loop < m_ColCnt; Loop++) {
            if (m_MinVal[Loop] != NULL) {
                pfree(m_MinVal[Loop]);
                m_MinVal[Loop] = NULL;
            }
        }
        pfree(m_MinVal);
        m_MinVal = NULL;
    }

    if (m_MaxVal != NULL) {
        /*
         *  refer to AllocMinMax
         */
        for (Loop = 0; Loop < m_ColCnt; Loop++) {
            if (m_MaxVal[Loop] != NULL) {
                pfree(m_MaxVal[Loop]);
                m_MaxVal[Loop] = NULL;
            }
        }
        pfree(m_MaxVal);
        m_MaxVal = NULL;
    }

    return;
}

int DFSDesc::AllocDelMap()
{
    if (m_DelMap == NULL) {
        m_DelMapBaseSize = (m_RowCnt + (BITS_IN_BYTE - 1)) / BITS_IN_BYTE;
        m_DelMap = (char *)palloc0(m_DelMapBaseSize);
    }

    return 0;
}

void DFSDesc::DeallocDelMap()
{
    if (m_DelMap != NULL) {
        pfree(m_DelMap);
        m_DelMap = NULL;
    }

    return;
}

void DFSDesc::Reset()
{
    /*
     * reset members
     */
    m_DescId = ~0;
    m_PartId = ~0;
    m_RowCnt = 0;
    m_FileSize = 0;
    m_ColCnt = 0;
    m_DelMapBaseSize = 0;
    m_Magic = 0;
    m_Xmin = 0;
    m_MinVal = NULL;
    m_MaxVal = NULL;
    m_DelMap = NULL;
    m_colMap = NULL;
    m_colMapLen = 0;
    m_FileName = NULL;
    m_name_len = 0;

    /*
     *  free the allocation
     */
    DeallocMinMax();
    DeallocDelMap();

    if (m_colMap) {
        pfree_ext(m_colMap);
    }
}

/**
 * @Description: Cleanup the resource.
 * @return None.
 */
void DFSDesc::Destroy()
{
    DeallocMinMax();
    DeallocDelMap();
    if (m_colMap) {
        pfree_ext(m_colMap);
    }

    if (m_FileName) {
        pfree_ext(m_FileName);
    }
}

DFSDesc::~DFSDesc()
{
    /*
     * Do nothing here. release memory in Destroy function.
     */
    this->Destroy();
}

/* ***************DFSDescHandler implement ******************* */
DFSDescHandler::DFSDescHandler(uint32 MaxBatCnt, int SrcRelCols, Relation mainRel)
{
    m_TableOid = mainRel->rd_rel->relcudescrelid;
    m_IndexOid = getDfsDescIndexOid(RelationGetRelid(mainRel));
    m_MaxBatCnt = MaxBatCnt;
    m_SrcRelCols = SrcRelCols;
    m_DelMapFileId = -1;
    m_pDelMap = NULL;
    m_mainRel = mainRel;
    m_DelMapSize = (((DfsMaxOffset >> MEGA_SHIFT) + (BITS_IN_BYTE - 1)) / BITS_IN_BYTE) << MEGA_SHIFT;
}

DFSDescHandler::~DFSDescHandler()
{
    pfree_ext(m_pDelMap);
    m_mainRel = NULL;
}

int DFSDescHandler::Load(DFSDesc *pDFSDescArray, uint32 Size, uint32 PartId, uint32 StartDescId, Snapshot SnapShot,
                         uint32 *pLoadCnt, uint32 *pLastId)
{
    ScanKeyData Key[1];
    SysScanDesc DfsDescScan = NULL;
    HeapTuple Tup = NULL;
    uint32 Loop = 0;

    bool IsNull = false;
    *pLoadCnt = 0;

    /*
     * open table & index
     */
    Relation DfsDescRel = heap_open(m_TableOid, AccessShareLock);
    TupleDesc DfsDescTupDesc = DfsDescRel->rd_att;
    Relation IdxRel = index_open(m_IndexOid, AccessShareLock);

    /*
     * Setup scan to fetch start from StartDescId & PartId.
     */
    ScanKeyInit(&Key[0], (AttrNumber)Anum_pg_dfsdesc_duid, BTGreaterEqualStrategyNumber, F_OIDGE,
                Int32GetDatum(StartDescId));

    SnapShot = (SnapShot == NULL) ? GetActiveSnapshot() : SnapShot;
    DfsDescScan = systable_beginscan_ordered(DfsDescRel, IdxRel, SnapShot, 1, Key);
    while ((Loop < Size) && (Loop < m_MaxBatCnt) &&
           ((Tup = systable_getnext_ordered(DfsDescScan, ForwardScanDirection)) != NULL)) {
        TupleToDfsDesc(Tup, DfsDescTupDesc, &pDFSDescArray[Loop]);
        *pLastId = DatumGetUInt32(fastgetattr(Tup, Anum_pg_dfsdesc_duid, DfsDescTupDesc, &IsNull));
        Loop += 1;
    }

    systable_endscan_ordered(DfsDescScan);
    *pLoadCnt = Loop;

    /*
     * close table & index
     */
    heap_close(DfsDescRel, AccessShareLock);
    index_close(IdxRel, AccessShareLock);

    return *pLoadCnt;
}

uint64 DFSDescHandler::GetLivedRowNumbers(int64 *deadrows)
{
    DFSDesc *pDFSDescArray = New(CurrentMemoryContext) DFSDesc[MAX_LOADED_DFSDESC];
    uint32 PartId = NON_PARTITION_TALBE_PART_NUM;
    uint32 StartId = 0;
    uint32 LoadCnt = 0;
    uint32 LastId = 0;
    uint64 RowNum = 0;

    *deadrows = 0;
    while (Load(pDFSDescArray, MAX_LOADED_DFSDESC, PartId, StartId, NULL, &LoadCnt, &LastId) > 0) {
        for (uint32 Loop = 0; Loop < LoadCnt; Loop++) {
            uint32 RowCnt = pDFSDescArray[Loop].GetRowCnt();
            for (uint32 Row = 0; Row < RowCnt; Row++) {
                if (pDFSDescArray[Loop].IsDeleted(Row))
                    (*deadrows)++;
                else
                    RowNum++;
            }
        }
        StartId = LastId + 1;
    }

    for (uint32 Loop = 0; Loop < MAX_LOADED_DFSDESC; Loop++) {
        pDFSDescArray[Loop].Destroy();
    }
    delete[] pDFSDescArray;

    return RowNum;
}

int DFSDescHandler::Add(DFSDesc *pDFSDescArray, uint32 Cnt, CommandId CmdId, int Options)
{
    Datum Values[DFSDESC_MAX_COL] = {0};
    bool Nulls[DFSDESC_MAX_COL] = {0};
    HeapTuple Tup = NULL;
    uint32 Loop = 0;

    /* open table & index */
    Relation DfsDescRel = heap_open(m_TableOid, RowExclusiveLock);
    TupleDesc DfsDescTupDesc = DfsDescRel->rd_att;
    Relation IdxRel = index_open(m_IndexOid, RowExclusiveLock);

    for (Loop = 0; Loop < Cnt; Loop++) {
        Tup = DfsDescToTuple(&pDFSDescArray[Loop], Values, Nulls, DfsDescTupDesc);

        /* We always generate xlog for cudesc tuple */
        Options = (unsigned int)Options & (~TABLE_INSERT_SKIP_WAL);

        (void)heap_insert(DfsDescRel, Tup, CmdId, Options, NULL);
        index_insert(IdxRel, Values, Nulls, &(Tup->t_self), DfsDescRel,
                     IdxRel->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);

        heap_freetuple(Tup);
        Tup = NULL;
        if (!Nulls[Anum_pg_dfsdesc_min - 1]) {
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_min - 1]));
        }
        if (!Nulls[Anum_pg_dfsdesc_max - 1]) {
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_max - 1]));
        }
        if (!Nulls[Anum_pg_dfsdesc_deletemap - 1]) {
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_deletemap - 1]));
        }
    }

    /*
     * close table & index
     */
    heap_close(DfsDescRel, RowExclusiveLock);
    index_close(IdxRel, RowExclusiveLock);

    return 0;
}

HeapTuple DFSDescHandler::DfsDescToTuple(DFSDesc *pDFSDesc, Datum *pTupVals, bool *pTupNulls, TupleDesc DfsDescTupDesc)
{
    errno_t Rc = EOK;
    Rc = memset_s(pTupNulls, DFSDESC_MAX_COL * sizeof(bool), false, DFSDESC_MAX_COL * sizeof(bool));
    securec_check(Rc, "", "");

    /*
     * partition id & desc id & row count & magic
     */
    pTupVals[Anum_pg_dfsdesc_partid - 1] = Int32GetDatum(pDFSDesc->GetPartId());
    pTupVals[Anum_pg_dfsdesc_duid - 1] = UInt32GetDatum(pDFSDesc->GetDescId());
    pTupVals[Anum_pg_dfsdesc_rowcount - 1] = Int32GetDatum(pDFSDesc->GetRowCnt());
    pTupVals[Anum_pg_dfsdesc_filesize - 1] = Int64GetDatum(pDFSDesc->GetFileSize());
    pTupVals[Anum_pg_dfsdesc_magic - 1] = UInt32GetDatum(pDFSDesc->GetMagic());
    pTupNulls[Anum_pg_dfsdesc_min - 1] = true;
    pTupNulls[Anum_pg_dfsdesc_max - 1] = true;

    int MinDataLen = 0;
    int MaxDataLen = 0;

    /*
     * min&max values
     * min&max format is whatever fixed or variable length
     * 1byte - length  remain bytes - content
     * if NULL value, just set length-byte to 1
     */
    int ColNum = pDFSDesc->GetColCnt();
    char *pTempBuf = (char *)palloc0((MIN_MAX_SIZE * ColNum * 2) + (ColNum * 2));
    int MaxRemainLen = MIN_MAX_SIZE * ColNum + ColNum;
    int MinRemainLen = MIN_MAX_SIZE * ColNum + ColNum;
    char *pMin = pTempBuf;
    char *pMax = pTempBuf + (MIN_MAX_SIZE * ColNum + ColNum);
    stMinMax *pstMinMaxTmp = NULL;

    for (int Loop = 0; Loop < ColNum; Loop++) {
        pstMinMaxTmp = pDFSDesc->GetMinVal(Loop);
        if (pstMinMaxTmp == NULL) {
            pMin[MinDataLen] = NULL_MAGIC;
            MinDataLen += 1;
            MinRemainLen -= 1;
        } else {
            /* store length */
            pMin[MinDataLen] = pstMinMaxTmp->Len;
            MinDataLen += 1;
            MinRemainLen -= 1;

            /* store content */
            Rc = memcpy_s(pMin + MinDataLen, MinRemainLen, pstMinMaxTmp->szContent, pstMinMaxTmp->Len);
            securec_check(Rc, "", "");
            MinDataLen += pstMinMaxTmp->Len;
            MinRemainLen -= pstMinMaxTmp->Len;
        }

        pstMinMaxTmp = pDFSDesc->GetMaxVal(Loop);
        if (pstMinMaxTmp == NULL) {
            pMax[MaxDataLen] = NULL_MAGIC;
            MaxDataLen += 1;
            MaxRemainLen -= 1;
        } else {
            /* store length */
            pMax[MaxDataLen] = pstMinMaxTmp->Len;
            MaxDataLen += 1;
            MaxRemainLen -= 1;

            /* store content	 */
            Rc = memcpy_s(pMax + MaxDataLen, MaxRemainLen, pstMinMaxTmp->szContent, pstMinMaxTmp->Len);
            securec_check(Rc, "", "");
            MaxDataLen += pstMinMaxTmp->Len;
            MaxRemainLen -= pstMinMaxTmp->Len;
        }
    }

    if (MinDataLen > ColNum) {
        /* exist min values */
        pTupVals[Anum_pg_dfsdesc_min - 1] = PointerGetDatum(cstring_to_text_with_len(pMin, MinDataLen));
        pTupNulls[Anum_pg_dfsdesc_min - 1] = false;
    } else {
        pTupNulls[Anum_pg_dfsdesc_min - 1] = true;
    }

    if (MaxDataLen > ColNum) {
        /* exist max values */
        pTupVals[Anum_pg_dfsdesc_max - 1] = PointerGetDatum(cstring_to_text_with_len(pMax, MaxDataLen));
        pTupNulls[Anum_pg_dfsdesc_max - 1] = false;
    } else {
        pTupNulls[Anum_pg_dfsdesc_max - 1] = true;
    }
    pfree_ext(pTempBuf);

    /* delete map */
    int RowCount = pDFSDesc->GetRowCnt();
    int DelMapSize = (RowCount + (BITS_IN_BYTE - 1)) / (BITS_IN_BYTE);
    const char *DelMap = pDFSDesc->GetDelMap();

    if (DelMap == NULL) { /* not any row be deleted */
        pTupNulls[Anum_pg_dfsdesc_deletemap - 1] = true;
    } else {
        pTupVals[Anum_pg_dfsdesc_deletemap - 1] = PointerGetDatum(cstring_to_text_with_len(DelMap, DelMapSize));
    }

    /*
     * column map.
     */
    const char *colMap = pDFSDesc->getColMap();
    if (colMap == NULL) {
        pTupNulls[Anum_pg_dfsdesc_columnmap - 1] = true;
    } else {
        int colMapSize = pDFSDesc->getColMapLength() + 1;
        pTupVals[Anum_pg_dfsdesc_columnmap - 1] = PointerGetDatum(cstring_to_text_with_len(colMap, colMapSize));
    }

    /*
     * filename
     */
    const char *pFileName = pDFSDesc->GetFileName();
    if (pFileName == NULL) {
        pTupNulls[Anum_pg_dfsdesc_relativelyfilename - 1] = true;
    } else {
        pTupVals[Anum_pg_dfsdesc_relativelyfilename - 1] = CStringGetTextDatum(pFileName);
    }

    /*
     * add attribute extra and set null flag.
     */
    pTupNulls[Anum_pg_dfsdesc_extra - 1] = true;

    return heap_form_tuple(DfsDescTupDesc, pTupVals, pTupNulls);
}

void DFSDescHandler::TupleToDfsDesc(HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc *pDFSDesc)
{
    bool IsNull = false;
    uint32 Tmp = 0;
    int64 lTmp = 0;
    char *pValPtr = NULL;
    Assert(pCudescTup != nullptr);
    int attNum = HeapTupleHeaderGetNatts(pCudescTup->t_data, DfsDescTupDesc);

    Assert(pDFSDesc != nullptr);

    /* avoid reused then clean it */
    pDFSDesc->Reset();

    /* set src relation column count */
    pDFSDesc->SetColCnt(m_SrcRelCols);
    /* desc id & partition id & row count & magic */
    lTmp = DatumGetInt64(fastgetattr(pCudescTup, Anum_pg_dfsdesc_duid, DfsDescTupDesc, &IsNull));
    pDFSDesc->SetDescId(lTmp);
    Assert(!IsNull);
    Tmp = DatumGetUInt32(fastgetattr(pCudescTup, Anum_pg_dfsdesc_partid, DfsDescTupDesc, &IsNull));
    pDFSDesc->SetPartId(Tmp);
    Assert(!IsNull);
    Tmp = DatumGetUInt32(fastgetattr(pCudescTup, Anum_pg_dfsdesc_magic, DfsDescTupDesc, &IsNull));
    pDFSDesc->SetMagic(Tmp);
    Assert(!IsNull);
    Tmp = DatumGetUInt32(fastgetattr(pCudescTup, Anum_pg_dfsdesc_rowcount, DfsDescTupDesc, &IsNull));
    pDFSDesc->SetRowCnt(Tmp);
    Assert(!IsNull);
    lTmp = DatumGetInt64(fastgetattr(pCudescTup, Anum_pg_dfsdesc_filesize, DfsDescTupDesc, &IsNull));
    if (!IsNull) {
        pDFSDesc->SetFileSize(lTmp);
    }

    /* filename */
    pValPtr = TextDatumGetCString(fastgetattr(pCudescTup, Anum_pg_dfsdesc_relativelyfilename, DfsDescTupDesc, &IsNull));
    Assert(!IsNull);
    pDFSDesc->SetFileName(pValPtr);

    /* delete map */
    setMapColumnByHeapTuple(pCudescTup, DfsDescTupDesc, pDFSDesc, DELETE_MAP_ATTR);

    /* Get column map. */
    if (attNum < DfsDescTupDesc->natts) {
        /*
         * If the column counts of physical tuple dose not match the column counts in catalog,
         * it indicates the DFS table is added columns.
         * In the upgrade scenario, the new column of desc table has no data in physical file, so
         * must build it. The "Add column" feature need to add colmap column on DFS table, do not
         * read column map in physical file. we have to build column map here.
         */
        TupleDesc desc = RelationGetDescr(m_mainRel);
        pValPtr = make_column_map(desc);
        pDFSDesc->setColMap(pValPtr, GET_COLMAP_LENGTH(desc->natts));
        pfree_ext(pValPtr);
    } else {
        setMapColumnByHeapTuple(pCudescTup, DfsDescTupDesc, pDFSDesc, COLUMN_MAP_ATTR);
    }

    /* Min & Max values */
    setMinMaxByHeapTuple(pCudescTup, DfsDescTupDesc, pDFSDesc, MIN_COLUMN_ATTR);
    setMinMaxByHeapTuple(pCudescTup, DfsDescTupDesc, pDFSDesc, MAX_COLUMN_ATTR);

    pDFSDesc->tid = pCudescTup->t_self;

    return;
}

void DFSDescHandler::setMinMaxByHeapTuple(HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc *pDFSDesc,
                                          ColumnEnum columnAttr)
{
    char *pValPtr = NULL;
    uint32 MinMaxLen = 0;
    bool IsNull = false;

    switch (columnAttr) {
        case MIN_COLUMN_ATTR: {
            pValPtr = DatumGetPointer(fastgetattr(pCudescTup, Anum_pg_dfsdesc_min, DfsDescTupDesc, &IsNull));
            break;
        }
        case MAX_COLUMN_ATTR: {
            pValPtr = DatumGetPointer(fastgetattr(pCudescTup, Anum_pg_dfsdesc_max, DfsDescTupDesc, &IsNull));
            break;
        }
        default: {
            /* Never occur here. */
            ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS),
                            errmsg("Invalid column attribute:%d", columnAttr)));
            break;
        }
    }

    if (!IsNull) {
        /* check toaster */
        char *DetoastPtr = (char *)PG_DETOAST_DATUM(pValPtr);
        char *pMovPtr = (DetoastPtr != pValPtr) ? DetoastPtr : pValPtr;
        pMovPtr = VARDATA_ANY(pMovPtr);

        for (uint32 Loop = 0; Loop < m_SrcRelCols; Loop++) {
            if (pDFSDesc->isDefaultColumn(Loop + 1)) {
                pMovPtr = pMovPtr + 1;
                continue;
            }
            if ((unsigned char)pMovPtr[0] != (unsigned char)NULL_MAGIC) {
                MinMaxLen = (uint32)((unsigned char)pMovPtr[0]);
                pMovPtr = pMovPtr + 1;
                if (columnAttr == MIN_COLUMN_ATTR) {
                    pDFSDesc->SetMinVal(Loop, pMovPtr, MinMaxLen);
                } else {
                    pDFSDesc->SetMaxVal(Loop, pMovPtr, MinMaxLen);
                }
                pMovPtr += MinMaxLen;
            } else {
                pMovPtr = pMovPtr + 1;
            }
        }

        /* clean it if need */
        if (DetoastPtr != pValPtr) {
            pfree_ext(DetoastPtr);
        }
    }
}

void DFSDescHandler::setMapColumnByHeapTuple(HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc *pDFSDesc,
                                             ColumnEnum columnAttr)
{
    char *pValPtr = NULL;
    char *pMovPtr = NULL;
    uint32 VarLen = 0;
    char *DetoastPtr = NULL;
    bool IsNull = false;

    switch (columnAttr) {
        case DELETE_MAP_ATTR: {
            pValPtr = DatumGetPointer(fastgetattr(pCudescTup, Anum_pg_dfsdesc_deletemap, DfsDescTupDesc, &IsNull));
            break;
        }
        case COLUMN_MAP_ATTR: {
            pValPtr = DatumGetPointer(fastgetattr(pCudescTup, Anum_pg_dfsdesc_columnmap, DfsDescTupDesc, &IsNull));
            break;
        }
        default: {
            /* Never occur here. */
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmodule(MOD_DFS), errmsg("setMinMaxByHeapTuple"),
                            errdetail("run into default code")));
            break;
        }
    }

    if (!IsNull) {
        /* check toaster */
        DetoastPtr = (char *)PG_DETOAST_DATUM(pValPtr);
        pMovPtr = (DetoastPtr != pValPtr) ? DetoastPtr : pValPtr;
        VarLen = VARSIZE_ANY_EXHDR(pMovPtr);
        pMovPtr = VARDATA_ANY(pMovPtr);

        if (columnAttr == DELETE_MAP_ATTR) {
            pDFSDesc->MaskDelMap(pMovPtr, VarLen, false);
        } else {
            pDFSDesc->setColMap(pMovPtr, VarLen);
        }

        if (DetoastPtr != pValPtr) {
            /*
             * if *detoastPtr* is a new space, we will free it
             * the first time when it's useless.
             */
            pfree_ext(DetoastPtr);
        }
    }
}

uint32 DFSDescHandler::SetDelMap(ItemPointer Tid, bool reportError)
{
    uint32 FileId = DfsItemPointerGetFileId(Tid);
    uint32 Row = DfsItemPointerGetOffset(Tid) - 1;
    errno_t Rc = EOK;

    if (m_pDelMap == NULL) {
        m_pDelMap = (char *)palloc0(m_DelMapSize);
    }

    if (FileId != (uint32)m_DelMapFileId) {
        if (m_DelMapFileId != -1) {
            FlushDelMap();
            Rc = memset_s(m_pDelMap, m_DelMapSize, 0, m_DelMapSize);
            securec_check(Rc, "", "");
        }

        m_DelMapFileId = FileId;
    }

    if ((unsigned char)m_pDelMap[Row >> 3] & (1 << (Row % 8))) {
        if (reportError) {
            ereport(ERROR,
                    (errcode(ERRCODE_CARDINALITY_VIOLATION), errmodule(MOD_DFS), errmsg("Non-deterministic UPDATE"),
                     errdetail("multiple updates to a row by a single query for column store table.")));
        } else
            return 0;
    }

    m_pDelMap[Row >> 3] = (unsigned char)m_pDelMap[Row >> 3] | (1 << (Row % 8));

    return 1;
}

void DFSDescHandler::FlushDelMap()
{
    if (-1 == m_DelMapFileId)
        return;

    ScanKeyData Key[1];
    HeapTuple TmpTup = NULL;
    HeapTuple OldTup = NULL;
    DFSDesc DFSDescTmp;
    bool Found = false;

    Snapshot SnapshotUse = SnapshotNow;

    /* open table & index */
    Relation DfsDescRel = heap_open(m_TableOid, RowExclusiveLock);
    TupleDesc DfsDescTupDesc = DfsDescRel->rd_att;
    Relation IdxRel = index_open(m_IndexOid, RowExclusiveLock);

    /* initlzie scan keys */
    ScanKeyInit(&Key[0], (AttrNumber)Anum_pg_dfsdesc_duid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(m_DelMapFileId));

    SysScanDesc DfsDescScan = systable_beginscan_ordered(DfsDescRel, IdxRel, SnapshotUse, 1, Key);

    /*
     * Step 1: Get delMask and modify it
     * Because of snapshotNow, you might see different versions of tuple when scan for long.
     * just need get one to deal with
     * If get the old one, it might show 'delete or update row conflict'
     * If get the new one, it will continue to deal with the new one
     */
    ItemPointerData OldTupCtid;
    if ((TmpTup = systable_getnext_ordered(DfsDescScan, ForwardScanDirection)) != NULL) {
        TupleToDfsDesc(TmpTup, DfsDescTupDesc, &DFSDescTmp);
        OldTup = TmpTup;

        DFSDescTmp.MaskDelMap(m_pDelMap, m_DelMapSize, true);
        DFSDescTmp.SetMagic(GetCurrentTransactionIdIfAny());

        OldTupCtid = OldTup->t_self;
        Found = true;
    }

    if (!Found) {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmodule(MOD_DFS),
                        errmsg("delete or update failed due to concurrent conflict")));
    }

    systable_endscan_ordered(DfsDescScan);
    index_close(IdxRel, RowExclusiveLock);

    /* create tup */
    Datum Values[DFSDESC_MAX_COL] = {0};
    bool Nulls[DFSDESC_MAX_COL] = {false};

    HeapTuple NewTup = DfsDescToTuple(&DFSDescTmp, Values, Nulls, DfsDescTupDesc);

    /* Step 2: update del_bitmap */
    TM_Result Result = TM_Invisible;
    TM_FailureData tmfd;

    Assert(NewTup);
    if (NewTup) {
        Result = tableam_tuple_update(DfsDescRel, NULL, &OldTupCtid, NewTup, GetCurrentCommandId(true),
                                      InvalidSnapshot, InvalidSnapshot, true, NULL, &tmfd, NULL, NULL, false);

        switch (Result) {
            case TM_SelfModified: {
                /* description: this transaction delete different rows in the same CU
                 * Now It is HeapTupleSelfUpdated */
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("delete or update failed because lock conflict")));
                break;
            }
            case TM_Ok: {
                /* description: Is OK? update the index */
                CatalogUpdateIndexes(DfsDescRel, NewTup);
                break;
            }

            case TM_Updated:
            case TM_Deleted: {
                /* description: how to fix this case that two transaction
                 * delete different rows in the same DfsDesc */
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("delete or update row conflict")));
                break;
            }

            default: {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("DfsStore: unrecognized heap_update status: %u", Result)));
                break;
            }
        }

        heap_freetuple(NewTup);
        NewTup = NULL;
        if (!Nulls[Anum_pg_dfsdesc_min - 1])
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_min - 1]));

        if (!Nulls[Anum_pg_dfsdesc_max - 1])
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_max - 1]));

        if (!Nulls[Anum_pg_dfsdesc_deletemap - 1])
            pfree(DatumGetPointer(Values[Anum_pg_dfsdesc_deletemap - 1]));
    }

    DFSDescTmp.Destroy();
    /* close table & index */
    heap_close(DfsDescRel, NoLock);
    CommandCounterIncrement();

    return;
}

List *DFSDescHandler::GetAllDescs(Snapshot _snapshot)
{
    return this->GetDescTuples(_snapshot, false);
}

List *DFSDescHandler::GetDescsToBeMerged(Snapshot _snapshot)
{
    return this->GetDescTuples(_snapshot, true);
}

List *DFSDescHandler::GetDescTuples(Snapshot _snapshot, bool only_tuples_with_invalid_data)
{
    List *rs = NIL;
    Snapshot snapshot;
    TableScanDesc scandesc;

    Relation descrel;
    HeapTuple tuple;
    TupleDesc tupdesc;

    /* max row = 2^23 = 1Mbytes, see itemptr.h for more details */
    const int max_bitmap_bytes = 1024 * 1024;
    char *delete_bitmap = (char *)palloc0(max_bitmap_bytes);

    descrel = heap_open(m_TableOid, AccessShareLock);
    tupdesc = descrel->rd_att;

    snapshot = _snapshot ? _snapshot : GetActiveSnapshot();
    scandesc = tableam_scan_begin(descrel, snapshot, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection)) != NULL) {
        /* Deconstruct the tuple ... faster than repeated heap_getattr */
        DFSDesc *desc = (DFSDesc *)New(CurrentMemoryContext) DFSDesc();
        TupleToDfsDesc(tuple, tupdesc, desc);

        if (only_tuples_with_invalid_data) {
            if (ContainDeleteRow((const char *)delete_bitmap, max_bitmap_bytes * 8, desc->GetDelMap(),
                                 desc->GetRowCnt()) ||
                desc->GetFileSize() < MIN_COMPACT_FILE_SIZE) {
                rs = lappend(rs, desc);
            } else {
                DELETE_EX(desc);
            }
            continue;
        }

        rs = lappend(rs, desc);
    }

    pfree_ext(delete_bitmap);

    tableam_scan_end(scandesc);
    heap_close(descrel, NoLock);

    return rs;
}

bool DFSDescHandler::ContainDeleteRow(const char *all_valid_bitmap, int bitmap_length, const char *delmap,
                                      int rows) const
{
    Assert(all_valid_bitmap != NULL);

    if (delmap == NULL)
        return false;

    /* the bit number of bitmap_length of the DU must be great than row number of the DU */
    Assert(bitmap_length >= rows);

    int len = rows / 8;
    if (memcmp(all_valid_bitmap, delmap, len))
        return true;

    unsigned char tmp = (unsigned char)*(delmap + len);
    for (int i = 0; i < (rows % 8); i++) {
        if (tmp & 0x01)
            return true;
        tmp = tmp >> 1;
    }

    return false;
}

void DFSDescHandler::DeleteDesc(DFSDesc *desc) const
{
    ScanKeyData Key[1];
    SysScanDesc scandesc = NULL;
    HeapTuple tup = NULL;

    /* open table & index */
    Relation rel = heap_open(m_TableOid, AccessShareLock);
    Relation index = index_open(m_IndexOid, AccessShareLock);

    Snapshot SnapShot = SnapshotNow;

    ScanKeyInit(&Key[0], (AttrNumber)Anum_pg_dfsdesc_duid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(desc->GetDescId()));

    scandesc = systable_beginscan_ordered(rel, index, SnapShot, 1, Key);
    tup = systable_getnext_ordered(scandesc, ForwardScanDirection);
    if (tup == NULL) {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmodule(MOD_DFS),
                        errmsg("getting next tuple in an ordered catalog scan failed")));
    }
    simple_heap_delete(rel, &(tup->t_self));

    systable_endscan_ordered(scandesc);

    heap_close(rel, AccessShareLock);
    index_close(index, AccessShareLock);
}
