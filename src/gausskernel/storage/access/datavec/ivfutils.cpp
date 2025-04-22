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
 * -------------------------------------------------------------------------
 *
 * ivfutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/ivfutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/datavec/bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "access/datavec/halfutils.h"
#include "access/datavec/halfvec.h"
#include "access/datavec/ivfflat.h"
#include "storage/buf/bufmgr.h"

/*
 * Allocate a vector array
 */
VectorArray VectorArrayInit(int maxlen, int dimensions, Size itemsize)
{
    VectorArray res = (VectorArray)palloc(sizeof(VectorArrayData));

    /* Ensure items are aligned to prevent UB */
    itemsize = MAXALIGN(itemsize);

    res->length = 0;
    res->maxlen = maxlen;
    res->dim = dimensions;
    res->itemsize = itemsize;
    res->items = (char *)palloc_extended(maxlen * itemsize, MCXT_ALLOC_ZERO | MCXT_ALLOC_HUGE);
    return res;
}

/*
 * Free a vector array
 */
void VectorArrayFree(VectorArray arr)
{
    pfree(arr->items);
    pfree(arr);
}

/*
 * Get the number of lists in the index
 */
int IvfflatGetLists(Relation index)
{
    IvfflatOptions *opts = (IvfflatOptions *)index->rd_options;

    if (opts)
        return opts->lists;

    return IVFFLAT_DEFAULT_LISTS;
}

/*
 * Get proc
 */
FmgrInfo *IvfflatOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

    return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum IvfflatNormValue(const IvfflatTypeInfo *typeInfo, Oid collation, Datum value)
{
    return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Check if non-zero norm
 */
bool IvfflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
    return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

/*
 * New buffer
 */
Buffer IvfflatNewBuffer(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    return buf;
}

/*
 * Init page
 */
void IvfflatInitPage(Buffer buf, Page page)
{
    PageInit(page, BufferGetPageSize(buf), sizeof(IvfflatPageOpaqueData));
    IvfflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    IvfflatPageGetOpaque(page)->page_id = IVFFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void IvfflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
    IvfflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void IvfflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void IvfflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
    /* Get new buffer */
    Buffer newbuf = IvfflatNewBuffer(index, forkNum);
    Page newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

    /* Update the previous buffer */
    IvfflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

    /* Init new page */
    IvfflatInitPage(newbuf, newpage);

    /* Commit */
    GenericXLogFinish(*state);

    /* Unlock */
    UnlockReleaseBuffer(*buf);

    *state = GenericXLogStart(index);
    *page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
    *buf = newbuf;
}

/*
 * Get the metapage info
 */
void IvfflatGetMetaPageInfo(Relation index, int *lists, int *dimensions)
{
    Buffer buf;
    Page page;
    IvfflatMetaPage metap;

    buf = ReadBuffer(index, IVFFLAT_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = IvfflatPageGetMeta(page);
    if (unlikely(metap->magicNumber != IVFFLAT_MAGIC_NUMBER))
        elog(ERROR, "ivfflat index is not valid");

    if (lists != NULL)
        *lists = metap->lists;

    if (dimensions != NULL)
        *dimensions = metap->dimensions;

    UnlockReleaseBuffer(buf);
}

/*
 * Update the start or insert page of a list
 */
void IvfflatUpdateList(Relation index, ListInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage,
                       BlockNumber startPage, ForkNumber forkNum)
{
    Buffer buf;
    Page page;
    GenericXLogState *state;
    IvfflatList list;
    bool changed = false;

    buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    list = (IvfflatList)PageGetItem(page, PageGetItemId(page, listInfo.offno));
    if (BlockNumberIsValid(insertPage) && insertPage != list->insertPage) {
        /* Skip update if insert page is lower than original insert page  */
        /* This is needed to prevent insert from overwriting vacuum */
        if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage) {
            list->insertPage = insertPage;
            changed = true;
        }
    }

    if (BlockNumberIsValid(startPage) && startPage != list->startPage) {
        list->startPage = startPage;
        changed = true;
    }

    /* Only commit if changed */
    if (changed) {
        IvfflatCommitBuffer(buf, state);
    } else {
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
    }
}

static Size VectorItemSize(int dimensions)
{
    return VECTOR_SIZE(dimensions);
}

static Size HalfvecItemSize(int dimensions)
{
    return HALFVEC_SIZE(dimensions);
}

static Size BitItemSize(int dimensions)
{
    return VARBITTOTALLEN(dimensions);
}

static void VectorUpdateCenter(Pointer v, int dimensions, float *x)
{
    Vector *vec = (Vector *)v;

    SET_VARSIZE(vec, VECTOR_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++)
        vec->x[k] = x[k];
}

static void HalfvecUpdateCenter(Pointer v, int dimensions, float *x)
{
    HalfVector *vec = (HalfVector *)v;

    SET_VARSIZE(vec, HALFVEC_SIZE(dimensions));
    vec->dim = dimensions;

    for (int k = 0; k < dimensions; k++)
        vec->x[k] = Float4ToHalfUnchecked(x[k]);
}

static void BitUpdateCenter(Pointer v, int dimensions, float *x)
{
    VarBit *vec = (VarBit *)v;
    unsigned char *nx = VARBITS(vec);

    SET_VARSIZE(vec, VARBITTOTALLEN(dimensions));
    VARBITLEN(vec) = dimensions;

    for (uint32 k = 0; k < VARBITBYTES(vec); k++) {
        nx[k] = 0;
    }

    for (int k = 0; k < dimensions; k++) {
        nx[k / 8] |= (x[k] > 0.5 ? 1 : 0) << (7 - (k % 8));
    }
}

static void VectorSumCenter(Pointer v, float *x)
{
    Vector *vec = (Vector *)v;

    for (int k = 0; k < vec->dim; k++)
        x[k] += vec->x[k];
}

static void HalfvecSumCenter(Pointer v, float *x)
{
    HalfVector *vec = (HalfVector *)v;

    for (int k = 0; k < vec->dim; k++)
        x[k] += HalfToFloat4(vec->x[k]);
}

static void BitSumCenter(Pointer v, float *x)
{
    VarBit *vec = (VarBit *)v;

    for (int k = 0; k < VARBITLEN(vec); k++)
        x[k] += (float)(((VARBITS(vec)[k / 8]) >> (7 - (k % 8))) & 0x01);
}

/*
 * Get type info
 */
const IvfflatTypeInfo *IvfflatGetTypeInfo(Relation index)
{
    FmgrInfo *procinfo = IvfflatOptionalProcInfo(index, IVFFLAT_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM,
                                                 .normalize = l2_normalize,
                                                 .itemSize = VectorItemSize,
                                                 .updateCenter = VectorUpdateCenter,
                                                 .sumCenter = VectorSumCenter};

        return (&typeInfo);
    } else {
        return (const IvfflatTypeInfo *)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_halfvec_support);
Datum ivfflat_halfvec_support(PG_FUNCTION_ARGS)
{
    static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM * 2,
                                             .normalize = halfvec_l2_normalize,
                                             .itemSize = HalfvecItemSize,
                                             .updateCenter = HalfvecUpdateCenter,
                                             .sumCenter = HalfvecSumCenter};

    PG_RETURN_POINTER(&typeInfo);
};

PGDLLEXPORT PG_FUNCTION_INFO_V1(ivfflat_bit_support);
Datum ivfflat_bit_support(PG_FUNCTION_ARGS)
{
    static const IvfflatTypeInfo typeInfo = {.maxDimensions = IVFFLAT_MAX_DIM * 32,
                                             .normalize = NULL,
                                             .itemSize = BitItemSize,
                                             .updateCenter = BitUpdateCenter,
                                             .sumCenter = BitSumCenter};

    PG_RETURN_POINTER(&typeInfo);
};
