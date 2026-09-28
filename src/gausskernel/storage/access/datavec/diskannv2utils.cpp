/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
 * diskannv2utils.cpp
 *
 *        Shared helpers for the DiskANN RaBitQ format (version 2): page init,
 *        meta page IO, region geometry, byte-stream region IO, node
 *        addressing, slot readers, transform application and RaBitQ encoding.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannv2utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cmath>

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/datavec/utils.h"
#include "access/datavec/vector.h"
#include "access/datavec/diskannv2.h"

/* ------------------------------------------------------------ heap access */

/*
 * The one L2 normalization every vector source shares, bitwise-equal to the
 * opclass norm proc (l2_normalize), so the duplicate check may compare
 * preprocessed vectors byte by byte. False for a zero vector (no direction).
 */
bool DiskAnnV2NormalizeVector(const float* src, int dim, float* out)
{
    double sq = 0;
    for (int i = 0; i < dim; i++) {
        sq += (double)src[i] * src[i];
    }
    if (sq <= 0) {
        return false;
    }
    double norm = sqrt(sq);
    if (norm == 0.0) {
        return false;
    }
    for (int i = 0; i < dim; i++) {
        out[i] = (float)(src[i] / norm);
    }
    return true;
}

static bool DiskAnnV2FillVector(const Vector* v, bool normalize, int dim, float* out)
{
    if (v->dim != dim) {
        return false;
    }
    if (normalize) {
        return DiskAnnV2NormalizeVector(v->x, dim, out);
    }
    errno_t rc = memcpy_s(out, sizeof(float) * (Size)dim, v->x, sizeof(float) * (Size)dim);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    return true;
}

/*
 * Fetch the indexed vector of the heap row at `tid` into `out` (dim floats),
 * normalizing for the cosine opclass. Follow the HOT chain using the caller's
 * snapshot before detoasting: an invisible old version may reference TOAST
 * chunks that have already been vacuumed. Scans use their query snapshot,
 * INSERT uses SnapshotSelf, and the locked build uses SnapshotAny.
 * Returns false if no matching tuple exists or the value is unusable.
 */
bool DiskAnnV2HeapVector(const DiskAnnV2HeapVecArgs* args, Snapshot snapshot)
{
    HeapTupleData tup;
    union {
        char data[BLCKSZ];
        double forceAlignDouble;
        int64 forceAlignInt64;
    } uncompressed;
    errno_t rc = memset_s(&tup, sizeof(tup), 0, sizeof(tup));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    ItemPointerData hotTid = *args->tid;
    Buffer buf = ReadBuffer(args->heap, ItemPointerGetBlockNumber(&hotTid));
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    bool found = heap_hot_search_buffer(&hotTid, args->heap, buf, snapshot, &tup, (HeapTupleHeader)uncompressed.data,
                                        NULL, true);
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    if (!found) {
        ReleaseBuffer(buf);
        return false;
    }
    tup.tupTableType = HEAP_TUPLE;

    bool isnull = false;
    Datum d = heap_getattr(&tup, args->attno, RelationGetDescr(args->heap), &isnull);
    bool ok = false;
    if (!isnull) {
        Vector* v = (Vector*)PG_DETOAST_DATUM(d);
        ok = DiskAnnV2FillVector(v, args->normalize, args->dim, args->out);
        if ((Pointer)v != DatumGetPointer(d)) {
            pfree(v);
        }
    }
    ReleaseBuffer(buf);
    return ok;
}

/* ------------------------------------------------------------ page + meta */

void DiskAnnV2InitPage(Page page, Size pageSize, uint8 pageType)
{
    PageInit(page, pageSize, sizeof(DiskAnnPageOpaqueData));
    DiskAnnPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    DiskAnnPageGetOpaque(page)->pageType = pageType;
    DiskAnnPageGetOpaque(page)->unused = 0;
    DiskAnnPageGetOpaque(page)->pageId = DISKANN_PAGE_ID;
}

uint16 DiskAnnV2CodeSlotSizeFor(int dimOut, int bits)
{
    Size slotSize = DiskAnnV2CodeSlotBytes(dimOut, bits);
    if (slotSize > DiskAnnV2PageUsable() || slotSize > PG_UINT16_MAX) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("diskann: dimension %d with rabitq_bits=%d needs a %lu-byte code slot, exceeding page "
                               "capacity",
                               dimOut, bits, (unsigned long)slotSize)));
    }
    return (uint16)slotSize;
}

/* slot geometry is a pure function of (dimOut, rabitqBits) */
static void DiskAnnV2CheckLayout(void)
{
    StaticAssertStmt((DISKANN_V2_CHUNK_NODES & (DISKANN_V2_CHUNK_NODES - 1)) == 0,
                     "diskann v2 chunk node count must be a power of two");
    StaticAssertStmt(DISKANN_V2_XFORM_PAGE_BYTES <= DISKANN_V2_PAGE_USABLE,
                     "transform page payload must fit into a block");
    StaticAssertStmt(offsetof(DiskAnnV2MetaPageData, magicNumber) == offsetof(DiskAnnMetaPageData, magicNumber),
                     "diskann v1/v2 meta pages must share magicNumber offset");
    StaticAssertStmt(offsetof(DiskAnnV2MetaPageData, version) == offsetof(DiskAnnMetaPageData, version),
                     "diskann v1/v2 meta pages must share version offset");
    StaticAssertStmt(sizeof(FactorDataBits) == DISKANN_V2_CODE_FACTOR_BYTES,
                     "diskann v2 code slot head must be 16 bytes");
    StaticAssertStmt(offsetof(RabitqVectorBits, data) == DISKANN_V2_CODE_FACTOR_BYTES,
                     "diskann v2 code slot data follows the factor head");
    StaticAssertStmt(sizeof(DiskAnnV2GraphSlot) == DISKANN_V2_GRAPH_SLOT,
                     "diskann v2 graph slot must be exactly 320 bytes");
    StaticAssertStmt(DISKANN_V2_PAGE_USABLE / DISKANN_V2_GRAPH_SLOT == DISKANN_V2_GRAPH_SLOTS_PER_PAGE,
                     "diskann v2 graph page must hold 25 slots");
}

void DiskAnnV2ComputeGeometry(DiskAnnV2Meta* meta)
{
    DiskAnnV2CheckLayout();
    if (meta->dimIn < 1 || meta->dimIn > DISKANN_V2_MAX_DIM) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: input dimension %u is outside the supported range [1, %d]", meta->dimIn,
                               DISKANN_V2_MAX_DIM)));
    }
    if (meta->dimOut < 1 || meta->dimOut > meta->dimIn) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: code dimension %u is outside the supported range [1, %d]", meta->dimOut,
                               (int)meta->dimIn)));
    }
    if (meta->rabitqBits != 1 && meta->rabitqBits != RBQ_TWO_BIT) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: rabitq_bits=%u on disk is not supported (use 1 or 2)", meta->rabitqBits)));
    }
    if (meta->graphDegree != DISKANN_V2_DEGREE) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: graph degree %u on disk does not match this build (%d)", meta->graphDegree,
                               DISKANN_V2_DEGREE)));
    }
    meta->codeSlotSize = DiskAnnV2CodeSlotSizeFor(meta->dimOut, meta->rabitqBits);
    meta->codeSlotsPerPage = (uint16)DiskAnnV2CodeSlotsPerPage(meta->codeSlotSize);
    if (meta->codeSlotsPerPage == 0) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("diskann: code slot size %u leaves no slots on a page", meta->codeSlotSize)));
    }
    meta->graphSlotSize = DISKANN_V2_GRAPH_SLOT;
    meta->graphSlotsPerPage = DISKANN_V2_GRAPH_SLOTS_PER_PAGE;
}

void DiskAnnV2MetaFromPage(const DiskAnnV2MetaPageData* src, DiskAnnV2Meta* out)
{
    errno_t rc = memset_s(out, sizeof(DiskAnnV2Meta), 0, sizeof(DiskAnnV2Meta));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    out->dimIn = src->dimIn;
    out->dimOut = src->dimOut;
    out->distType = src->distType;
    out->rabitqBits = src->rabitqBits;
    out->graphDegree = src->graphDegree;
    out->indexSize = src->indexSize;
    out->nextNodeId = src->nextNodeId;
    out->frozenNodeId = src->frozenNodeId;
    out->tailNodeStart = src->tailNodeStart;
    out->tailStart = src->tailStart;
    out->tailChunkCount = src->tailChunkCount;
    out->xform = src->xform;
    out->code = src->code;
    out->graph = src->graph;
    DiskAnnV2ComputeGeometry(out);
}

void DiskAnnV2GetMetaSnapshot(Relation index, DiskAnnV2Meta* out)
{
    Buffer buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    DiskAnnV2MetaPage metap = DiskAnnV2PageGetMeta(page);
    if (unlikely(metap->magicNumber != DISKANN_MAGIC_NUMBER)) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("\"%s\" is not a diskann index", RelationGetRelationName(index))));
    }
    if (unlikely(metap->version != DISKANN_VERSION_V2)) {
        uint32 v = metap->version;
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann index \"%s\" has unsupported format version %u, REINDEX it",
                               RelationGetRelationName(index), v)));
    }
    DiskAnnV2MetaPageData snap;
    errno_t rc = memcpy_s(&snap, sizeof(snap), metap, sizeof(snap));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    UnlockReleaseBuffer(buf);
    DiskAnnV2MetaFromPage(&snap, out);
}

/* extension lock page: pretend-full so FSM never hands it out (mirrors v1) */
static BlockNumber DiskAnnV2CreateLockPage(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf);
    DiskAnnV2InitPage(page, BufferGetPageSize(buf), DISKANN_V2_PAGE_LOCK);
    ((PageHeader)page)->pd_lower = ((PageHeader)page)->pd_upper;
    BlockNumber blk = BufferGetBlockNumber(buf);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    return blk;
}

/* blocks 0 and 1: meta skeleton (regions unset, no nodes) + extension lock page */
void DiskAnnV2CreateMetaPage(Relation index, ForkNumber forkNum, const DiskAnnV2CreateMetaArgs* args)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (BufferGetBlockNumber(buf) != DISKANN_METAPAGE_BLKNO) {
        ereport(ERROR, (errmsg("diskann: meta page landed on block %u", BufferGetBlockNumber(buf))));
    }

    Page page = BufferGetPage(buf);
    DiskAnnV2InitPage(page, BufferGetPageSize(buf), DISKANN_V2_PAGE_META);

    DiskAnnV2MetaPage metap = DiskAnnV2PageGetMeta(page);
    errno_t rc = memset_s(metap, sizeof(DiskAnnV2MetaPageData), 0, sizeof(DiskAnnV2MetaPageData));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }

    metap->magicNumber = DISKANN_MAGIC_NUMBER;
    metap->version = DISKANN_VERSION_V2;
    metap->dimIn = (uint16)args->dimIn;
    metap->dimOut = (uint16)args->dimOut;
    metap->distType = args->distType;
    metap->rabitqBits = args->bits;
    metap->graphDegree = DISKANN_V2_DEGREE;
    metap->indexSize = args->indexSize;
    metap->nextNodeId = 0;
    metap->frozenNodeId = DISKANN_V2_INVALID_NODE;
    metap->tailNodeStart = 0;
    metap->tailStart = InvalidBlockNumber;
    metap->tailChunkCount = 0;
    metap->xform.startBlk = InvalidBlockNumber;
    metap->code.startBlk = InvalidBlockNumber;
    metap->graph.startBlk = InvalidBlockNumber;

    /* validate the geometry once so a bad dimOut fails here, not at first use */
    DiskAnnV2Meta check;
    DiskAnnV2MetaFromPage(metap, &check);

    ((PageHeader)page)->pd_lower = ((char*)metap + sizeof(DiskAnnV2MetaPageData)) - (char*)page;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);

    BlockNumber lockBlk = DiskAnnV2CreateLockPage(index, forkNum);
    if (lockBlk != DISKANN_EXTENTION_LOCK_BLKNO) {
        ereport(ERROR, (errmsg("diskann: extension lock page landed on block %u", lockBlk)));
    }
}

/*
 * Append one page during build (exclusive relation lock held, so P_NEW is
 * strictly sequential). The caller keeps the buffer exclusively locked and
 * must MarkBufferDirty + UnlockReleaseBuffer.
 */
Buffer DiskAnnV2AppendPage(Relation index, ForkNumber forkNum, uint8 pageType, BlockNumber expected)
{
    Buffer buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    if (expected != InvalidBlockNumber && BufferGetBlockNumber(buf) != expected) {
        ereport(ERROR, (errmsg("diskann: non-sequential page extension (got %u, expected %u)",
                               BufferGetBlockNumber(buf), expected)));
    }
    DiskAnnV2InitPage(BufferGetPage(buf), BufferGetPageSize(buf), pageType);
    return buf;
}

/*
 * Write a raw byte stream into freshly appended pages, pageBytes of payload
 * per page starting at DISKANN_V2_PAGE_DATA_OFFSET. No header is written:
 * the reader derives the stream length from the meta page.
 */
void DiskAnnV2WriteStream(Relation index, ForkNumber forkNum, const DiskAnnV2WriteStreamArgs* args)
{
    Size pageBytes = args->pageBytes;
    Size total = args->total;
    const char* data = args->data;
    if (pageBytes == 0 || pageBytes > DiskAnnV2PageUsable()) {
        ereport(ERROR, (errmsg("diskann: invalid stream page payload %lu", (unsigned long)pageBytes)));
    }
    uint32 npages = (uint32)((total + pageBytes - 1) / pageBytes);
    BlockNumber start = InvalidBlockNumber;

    Size done = 0;
    for (uint32 i = 0; i < npages; i++) {
        Buffer buf = DiskAnnV2AppendPage(index, forkNum, args->pageType, InvalidBlockNumber);
        if (i == 0) {
            start = BufferGetBlockNumber(buf);
        }
        Page page = BufferGetPage(buf);
        Size chunk = Min(pageBytes, total - done);
        errno_t rc = memcpy_s((char*)page + DISKANN_V2_PAGE_DATA_OFFSET, pageBytes, data + done, chunk);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        ((PageHeader)page)->pd_lower = (uint16)(DISKANN_V2_PAGE_DATA_OFFSET + chunk);
        done += chunk;
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    args->extOut->startBlk = (npages > 0) ? start : InvalidBlockNumber;
    args->extOut->nblocks = npages;
}

void DiskAnnV2LoadStream(Relation index, const DiskAnnV2Extent* ext, Size pageBytes, char* out, Size total)
{
    Size done = 0;
    for (uint32 pg = 0; pg < ext->nblocks && done < total; pg++) {
        Buffer buf = ReadBuffer(index, ext->startBlk + pg);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        Size chunk = Min(pageBytes, total - done);
        errno_t rc = memcpy_s(out + done, total - done, (char*)page + DISKANN_V2_PAGE_DATA_OFFSET, chunk);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
        done += chunk;
        UnlockReleaseBuffer(buf);
    }
    if (done < total) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: transform region of \"%s\" truncated (%lu of %lu bytes)",
                               RelationGetRelationName(index), (unsigned long)done, (unsigned long)total)));
    }
}

/* transform region = PcaSerialize byte stream (mean[dimIn] then M[dimOut][dimIn]), 8000B per page */
void DiskAnnV2WriteTransform(Relation index, ForkNumber forkNum, const VectorTransform* vt, DiskAnnV2Extent* extOut)
{
    Size total = PcaSerializeSize(vt->dimIn, vt->dimOut);
    char* buf = (char*)palloc(total);
    PcaSerialize(vt, buf, total);
    DiskAnnV2WriteStreamArgs stream;
    stream.pageType = DISKANN_V2_PAGE_XFORM;
    stream.pageBytes = DISKANN_V2_XFORM_PAGE_BYTES;
    stream.data = buf;
    stream.total = total;
    stream.extOut = extOut;
    DiskAnnV2WriteStream(index, forkNum, &stream);
    pfree(buf);
}

/* the returned transform is one allocation in CurrentMemoryContext (PcaDeserialize) */
VectorTransform* DiskAnnV2LoadTransform(Relation index, const DiskAnnV2Meta* meta)
{
    Size total = PcaSerializeSize(meta->dimIn, meta->dimOut);
    char* buf = (char*)palloc(total);
    DiskAnnV2LoadStream(index, &meta->xform, DISKANN_V2_XFORM_PAGE_BYTES, buf, total);
    VectorTransform* vt = PcaDeserialize(meta->dimIn, meta->dimOut, buf);
    pfree(buf);
    return vt;
}

/* ------------------------------------------------------- node addressing
 *
 * Initial nodes: page = regionStart + id / slotsPerPage. Tail nodes:
 * chunkNo = (id - tailNodeStart) >> 10, chunkBase = tailStart + chunkNo *
 * chunkPages, [code pages][graph pages] inside the chunk. Only chunks below
 * tailChunkCount are published.
 */
static void DiskAnnV2ResolveSlot(const DiskAnnV2Meta* meta, bool codeRegion, uint32 nodeId, BlockNumber* blkno,
                                 uint16* slot)
{
    uint32 slotsPerPage = codeRegion ? meta->codeSlotsPerPage : meta->graphSlotsPerPage;
    if (slotsPerPage == 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: %s slotsPerPage is 0", codeRegion ? "code" : "graph")));
    }

    if (nodeId < meta->tailNodeStart) {
        const DiskAnnV2Extent* ext = codeRegion ? &meta->code : &meta->graph;
        uint32 virtualPage = nodeId / slotsPerPage;
        if (ext->startBlk == InvalidBlockNumber || virtualPage >= ext->nblocks) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("diskann: node %u is beyond the initial %s region", nodeId,
                                   codeRegion ? "code" : "graph")));
        }
        *blkno = ext->startBlk + virtualPage;
        *slot = (uint16)(nodeId % slotsPerPage);
        return;
    }

    uint32 tailNode = nodeId - meta->tailNodeStart;
    uint32 chunkNo = tailNode / DISKANN_V2_CHUNK_NODES;
    uint32 localId = tailNode & (DISKANN_V2_CHUNK_NODES - 1);
    if (chunkNo >= meta->tailChunkCount || meta->tailStart == InvalidBlockNumber) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: tail node %u is beyond the published chunk capacity", nodeId)));
    }

    uint32 regionOffset = codeRegion ? 0 : DiskAnnV2ChunkCodePages(meta);
    uint64 blk = (uint64)meta->tailStart + (uint64)chunkNo * DiskAnnV2ChunkPages(meta) + regionOffset +
                 localId / slotsPerPage;
    if (blk >= InvalidBlockNumber) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("diskann: block number overflow for node %u", nodeId)));
    }
    *blkno = (BlockNumber)blk;
    *slot = (uint16)(localId % slotsPerPage);
}

void DiskAnnV2ResolveCodeSlot(const DiskAnnV2Meta* meta, uint32 nodeId, BlockNumber* blkno, uint16* slot)
{
    DiskAnnV2ResolveSlot(meta, true, nodeId, blkno, slot);
}

void DiskAnnV2ResolveGraphSlot(const DiskAnnV2Meta* meta, uint32 nodeId, BlockNumber* blkno, uint16* slot)
{
    DiskAnnV2ResolveSlot(meta, false, nodeId, blkno, slot);
}

void DiskAnnV2ReadCodeSlot(Relation index, const DiskAnnV2Meta* meta, uint32 nodeId, DiskAnnV2CodeSlot* out)
{
    BlockNumber blkno;
    uint16 slot;
    DiskAnnV2ResolveCodeSlot(meta, nodeId, &blkno, &slot);
    Buffer buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    errno_t rc = memcpy_s(out, meta->codeSlotSize, (char*)page + DiskAnnV2SlotOffset(meta->codeSlotSize, slot),
                          meta->codeSlotSize);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    UnlockReleaseBuffer(buf);
}

void DiskAnnV2ReadGraphSlot(Relation index, const DiskAnnV2Meta* meta, uint32 nodeId, DiskAnnV2GraphSlot* out)
{
    BlockNumber blkno;
    uint16 slot;
    DiskAnnV2ResolveGraphSlot(meta, nodeId, &blkno, &slot);
    Buffer buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    Page page = BufferGetPage(buf);
    errno_t rc = memcpy_s(out, sizeof(DiskAnnV2GraphSlot), (char*)page + DiskAnnV2SlotOffset(meta->graphSlotSize, slot),
                          sizeof(DiskAnnV2GraphSlot));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
    UnlockReleaseBuffer(buf);
    if (out->count > DISKANN_V2_DEGREE) {
        out->count = DISKANN_V2_DEGREE;
    }
    if (out->tidCount > DISKANN_HEAPTIDS) {
        out->tidCount = DISKANN_HEAPTIDS;
    }
}

/*
 * Graph slots from `nodeId` (inclusive) to the end of its page: bounded by the
 * page and by the end of the initial region or of the node's tail chunk.
 */
uint32 DiskAnnV2GraphSlotsOnPage(const DiskAnnV2Meta* meta, uint32 nodeId)
{
    uint32 regionEnd;
    uint32 slotNo;
    if (nodeId < meta->tailNodeStart) {
        regionEnd = meta->tailNodeStart;
        slotNo = nodeId % meta->graphSlotsPerPage;
    } else {
        uint32 localId = (nodeId - meta->tailNodeStart) & (DISKANN_V2_CHUNK_NODES - 1);
        regionEnd = nodeId - localId + DISKANN_V2_CHUNK_NODES;
        slotNo = localId % meta->graphSlotsPerPage;
    }
    return Min(meta->graphSlotsPerPage - slotNo, regionEnd - nodeId);
}

/* ------------------------------------------------- runtime node allocation
 *
 * INSERT grows the index by fixed 1024-node tail chunks. Lock order is
 * extension lock (block 1) -> meta page; readers only ever see chunks whose
 * every page has been initialized and WAL-logged.
 */

/*
 * Allocate a node id under the meta page lock (WAL-logged) and report the
 * published tail capacity as of that moment. The traversal entry is published
 * separately, after both of the first node's slots exist.
 */
uint32 DiskAnnV2AllocateNodeId(Relation index, uint32* tailChunkCount)
{
    Buffer buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    DiskAnnV2MetaPage metap = DiskAnnV2PageGetMeta(page);
    uint32 id = metap->nextNodeId;
    if (id == DISKANN_V2_INVALID_NODE) {
        GenericXLogAbort(state);
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("diskann: node id space of index \"%s\" is exhausted", RelationGetRelationName(index))));
    }
    metap->nextNodeId = id + 1;
    *tailChunkCount = metap->tailChunkCount;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
    return id;
}

/*
 * Publish the first fully initialized node as the traversal entry. Concurrent
 * first inserters all observe the same winner, which is returned.
 */
uint32 DiskAnnV2PublishFirstNode(Relation index, uint32 nodeId)
{
    Buffer buf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(index);
    Page page = GenericXLogRegisterBuffer(state, buf, 0);
    DiskAnnV2MetaPage metap = DiskAnnV2PageGetMeta(page);
    uint32 entry = metap->frozenNodeId;
    if (entry == DISKANN_V2_INVALID_NODE) {
        metap->frozenNodeId = nodeId;
        entry = nodeId;
        GenericXLogFinish(state);
    } else {
        GenericXLogAbort(state);
    }
    UnlockReleaseBuffer(buf);
    return entry;
}

/*
 * Initialize (or, after a crash before the chunk was published, re-initialize)
 * one tail page at its deterministic block number. Full-page WAL image.
 */
static void DiskAnnV2PrepareTailPage(Relation index, BlockNumber expected, uint8 pageType)
{
    BlockNumber nblocks = RelationGetNumberOfBlocksInFork(index, MAIN_FORKNUM);
    Buffer buf;
    if (expected < nblocks) {
        /* a crash can leave a prefix of an unpublished chunk at EOF: reuse it in place */
        buf = ReadBuffer(index, expected);
    } else if (expected == nblocks) {
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        BlockNumber got = BufferGetBlockNumber(buf);
        if (got != expected) {
            ReleaseBuffer(buf);
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("diskann: tail page landed on block %u, expected %u", got, expected)));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: index \"%s\" has a hole before block %u", RelationGetRelationName(index),
                               expected)));
        return;
    }

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(index);
    Page page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    DiskAnnV2InitPage(page, BufferGetPageSize(buf), pageType);
    ((PageHeader)page)->pd_lower = (uint16)(DISKANN_V2_PAGE_DATA_OFFSET + DiskAnnV2PageUsable());
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

/*
 * Initialize unpublished tail chunks and publish tailChunkCount only after
 * every new page is in WAL. lockBuf is the extension lock; overflow paths
 * release it before ereport.
 */
static void DiskAnnV2GrowTailChunks(Relation index, Buffer lockBuf, const DiskAnnV2Meta* snap, uint32 targetChunks)
{
    uint32 codePages = DiskAnnV2ChunkCodePages(snap);
    uint32 graphPages = DiskAnnV2ChunkGraphPages(snap);
    uint32 chunkPages = codePages + graphPages;

    for (uint32 chunk = snap->tailChunkCount; chunk < targetChunks; chunk++) {
        uint64 start64 = (uint64)snap->tailStart + (uint64)chunk * chunkPages;
        if (start64 + chunkPages >= InvalidBlockNumber) {
            UnlockReleaseBuffer(lockBuf);
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("diskann: tail chunk block number overflow")));
        }
        BlockNumber start = (BlockNumber)start64;
        for (uint32 pg = 0; pg < codePages; pg++) {
            DiskAnnV2PrepareTailPage(index, start + pg, DISKANN_V2_PAGE_CODE);
        }
        for (uint32 pg = 0; pg < graphPages; pg++) {
            DiskAnnV2PrepareTailPage(index, start + codePages + pg, DISKANN_V2_PAGE_GRAPH);
        }
        CHECK_FOR_INTERRUPTS();
    }

    Buffer metaBuf = ReadBuffer(index, DISKANN_METAPAGE_BLKNO);
    LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);
    GenericXLogState* state = GenericXLogStart(index);
    Page metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
    DiskAnnV2PageGetMeta(metaPage)->tailChunkCount = targetChunks;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(metaBuf);
}

/*
 * Make node `requiredSlots - 1` addressable by appending 1024-node chunks.
 * The extension lock serializes creators; the meta page is re-read under it
 * because another session may have grown the index meanwhile. Every code and
 * graph page of a chunk is initialized and WAL-logged before tailChunkCount
 * is published, so readers never see a partial chunk. A post-crash retry
 * finds the pages at the same block numbers and overwrites them in place.
 */
void DiskAnnV2EnsureNodeCapacity(Relation index, uint64 requiredSlots)
{
    DiskAnnV2Meta snap;
    Buffer lockBuf;
    uint64 tailSlots;
    uint64 target64;

    DiskAnnV2GetMetaSnapshot(index, &snap);
    if (DiskAnnV2NodeCapacity(&snap) >= requiredSlots) {
        return;
    }

    lockBuf = ReadBuffer(index, DISKANN_EXTENTION_LOCK_BLKNO);
    LockBuffer(lockBuf, BUFFER_LOCK_EXCLUSIVE);
    DiskAnnV2GetMetaSnapshot(index, &snap);
    if (DiskAnnV2NodeCapacity(&snap) >= requiredSlots) {
        UnlockReleaseBuffer(lockBuf);
        return;
    }
    if (snap.tailStart == InvalidBlockNumber || requiredSlots <= snap.tailNodeStart) {
        UnlockReleaseBuffer(lockBuf);
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("diskann: index \"%s\" has invalid tail chunk metadata",
                               RelationGetRelationName(index))));
    }

    tailSlots = requiredSlots - snap.tailNodeStart;
    target64 = (tailSlots + DISKANN_V2_CHUNK_NODES - 1) / DISKANN_V2_CHUNK_NODES;
    if (target64 > PG_UINT32_MAX) {
        UnlockReleaseBuffer(lockBuf);
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("diskann: tail chunk count overflow")));
    }
    DiskAnnV2GrowTailChunks(index, lockBuf, &snap, (uint32)target64);
    UnlockReleaseBuffer(lockBuf);
}
