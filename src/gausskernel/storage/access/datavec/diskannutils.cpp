/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * diskannutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/datavec/diskann.h"

/*
 * Get type info
 */
const DiskAnnTypeInfo* DiskAnnGetTypeInfo(Relation index)
{
    FmgrInfo* procinfo = DiskAnnOptionalProcInfo(index, DISKANN_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const DiskAnnTypeInfo typeInfo = {.maxDimensions = DISKANN_MAX_DIM,
                                                 .supportPQ = true,
                                                 .itemSize = VectorItemSize,
                                                 .normalize = l2_normalize,
                                                 .checkValue = NULL};
        return (&typeInfo);
    } else {
        return (const DiskAnnTypeInfo*)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

FmgrInfo* DiskAnnOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

    return index_getprocinfo(index, 1, procnum);
}

Datum DiskAnnNormValue(const DiskAnnTypeInfo* typeInfo, Oid collation, Datum value)
{
    return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

bool DiskAnnCheckNorm(FmgrInfo* procinfo, Oid collation, Datum value)
{
    return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

void DiskAnnUpdateMetaPage(Relation index, BlockNumber blkno, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);

    DiskAnnMetaPage metaPage = DiskAnnPageGetMeta(page);
    metaPage->insertPage = blkno;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

void DiskAnnInitPage(Page page, Size pagesize)
{
    PageInit(page, pagesize, sizeof(DiskAnnPageOpaqueData));
    DiskAnnPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    DiskAnnPageGetOpaque(page)->pageId = DISKANN_PAGE_ID;
}

Page DiskAnnInitRegisterPage(Relation index, Buffer buf)
{
    Page page = BufferGetPage(buf);
    DiskAnnInitPage(page, BufferGetPageSize(buf));
    return page;
}

void DiskANNGetMetaPageInfo(Relation index, DiskAnnMetaPage meta)
{
    Buffer buf;
    Page page;
    DiskAnnMetaPage metapage;

    buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    Size itemsz = sizeof(DiskAnnMetaPageData);
    metapage = (DiskAnnMetaPage)DiskAnnPageGetMeta(page);
    errno_t rc = memcpy_s(meta, itemsz, metapage, itemsz);
    securec_check(rc, "\0", "\0");
    UnlockReleaseBuffer(buf);
    return;
}

DiskAnnGraphStore::DiskAnnGraphStore(Relation relation)
{
    m_rel = relation;
    DiskAnnMetaPageData metapage;
    DiskANNGetMetaPageInfo(relation, &metapage);
    m_nodeSize = metapage.nodeSize;
    m_edgeSize = metapage.edgeSize;
    m_itemSize = metapage.itemSize;
    m_dimension = metapage.dimensions;
}

DiskAnnGraphStore::~DiskAnnGraphStore()
{
    m_rel = NULL;
}

void DiskAnnGraphStore::GetVector(BlockNumber blkno, float* vec, double* sqrSum, ItemPointerData* hctid) const
{
    Buffer buf;
    Page page;
    buf = ReadBuffer(m_rel, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    Assert(PageIsValid(page));

    /* get index value from page */
    IndexTuple ctup = (IndexTuple)PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
    *hctid = ctup->t_tid;
    TupleDesc tupdesc = RelationGetDescr(m_rel);
    bool isnull;
    Datum src = index_getattr(ctup, 1, tupdesc, &isnull);
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(src));

    /* get vector data from index tup */
    Vector* vector = (Vector*)DatumGetPointer(dst);
    Assert(m_dimension == vector->dim);
    errno_t rc = memcpy_s(vec, (Size)(vector->dim * sizeof(float)), vector->x, (Size)(vector->dim * sizeof(float)));
    securec_check_c(rc, "\0", "\0");

    /* get sqrSum from node tup */
    DiskAnnNodePage node_tup = DiskAnnPageGetNode(ctup);
    *sqrSum = node_tup->sqrSum;
    if (DatumGetPointer(dst) != DatumGetPointer(src)) {
        pfree(DatumGetPointer(dst));
    }
    UnlockReleaseBuffer(buf);
}
