/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
 * vector_storage.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/vector_storage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/datavec/diskann.h"
#include "access/datavec/hnsw.h"
#include "access/datavec/hnsw_vector_storage.h"
#include "access/datavec/ivfflat.h"
#include "access/datavec/vector_buffer.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "securec.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufpage.h"
#include "storage/item/itemid.h"
#include "storage/item/itemptr.h"
#include "storage/off.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/datum.h"
#include "access/datavec/vector_storage.h"

static bool VecPayloadDiskRefReservedIsZero(const VecPayloadDiskRef *diskRef)
{
    for (size_t i = 0; i < lengthof(diskRef->reserved); i++) {
        if (diskRef->reserved[i] != 0) {
            return false;
        }
    }
    return true;
}

/*
 * Payload pages reuse HNSW special-area layout (page_id + role). IVFFlat /
 * DiskANN payload blocks store the same bytes; changing this is a format bump.
 */
static inline bool VecPayloadPageIsPayload(Page page)
{
    if (page == NULL || PageIsNew(page) || PageGetPageSize(page) != BLCKSZ ||
        PageGetSpecialSize(page) != MAXALIGN(sizeof(HnswPageOpaqueData))) {
        return false;
    }
    return HnswPageGetOpaque(page)->page_id == HNSW_PAGE_ID &&
        HnswPageGetRole(page) == HNSW_PAGE_ROLE_PAYLOAD;
}

static const char *VecPayloadRelationName(Relation index)
{
    return index != NULL ? RelationGetRelationName(index) : "<unknown>";
}

static TupleDesc VecPayloadRefTupdesc(Relation index)
{
    TupleDesc tupdesc = CreateTemplateTupleDesc(1, false);

    TupleDescInitEntry(tupdesc, (AttrNumber)1, "payload_ref", BYTEAOID, -1, 0);
    tupdesc->attrs[0].attrelid = RelationGetDescr(index)->attrs[0].attrelid;
    tupdesc->attrs[0].attstorage = 'p';
    return tupdesc;
}

IndexTuple VecPayloadFormIndexTupleFromRef(
    Relation index, const VecPayloadDiskRef *rawRef, ItemPointer heapTid)
{
    Size refSize = VARHDRSZ + sizeof(VecPayloadDiskRef);
    bytea *refBytes = (bytea *)palloc0(refSize);
    Datum refDatum;
    bool isnull = false;
    TupleDesc tupdesc;
    IndexTuple itup;
    errno_t rc;

    SET_VARSIZE(refBytes, refSize);
    rc = memcpy_s(VARDATA(refBytes), sizeof(*rawRef), rawRef, sizeof(*rawRef));
    if (rc != EOK) {
        pfree(refBytes);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to copy vector payload reference: memcpy_s returned %d", rc)));
    }
    refDatum = PointerGetDatum(refBytes);
    tupdesc = VecPayloadRefTupdesc(index);
    itup = index_form_tuple(tupdesc, &refDatum, &isnull);
    FreeTupleDesc(tupdesc);
    itup->t_tid = *heapTid;
    pfree(refBytes);
    return itup;
}

bool VecPayloadIndexTupleGetRef(IndexTuple itup, Relation index, VecPayloadDiskRef *outRef)
{
    TupleDesc tupdesc;
    bool isnull = false;
    Datum datum;
    bytea *bytes;
    errno_t rc;

    if (itup == NULL || outRef == NULL) {
        return false;
    }
    tupdesc = VecPayloadRefTupdesc(index);
    datum = index_getattr(itup, 1, tupdesc, &isnull);
    FreeTupleDesc(tupdesc);
    if (isnull || datum == (Datum)0) {
        return false;
    }
    bytes = DatumGetByteaP(datum);
    if (VARSIZE_ANY_EXHDR(bytes) != sizeof(VecPayloadDiskRef)) {
        return false;
    }
    rc = memcpy_s(outRef, sizeof(*outRef), VARDATA_ANY(bytes), sizeof(*outRef));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to read vector payload reference: memcpy_s returned %d", rc)));
    }
    return true;
}

IndexTuple VecPayloadInsertIndexTuple(Relation index, const VecPayloadInput *payload,
    const VecPayloadInsertIndexTupleRequest *request)
{
    VecPayloadDiskRef rawRef;
    VecPayloadInsertRequest insertRequest;

    Assert(payload != NULL);
    Assert(payload->kind == VEC_PAYLOAD_RAW_VECTOR);
    Assert(request != NULL);
    insertRequest = {request->forkNum, request->startBlkno, &rawRef, request->insertBlknoOut,
        request->buildExtensionLock};
    VecPayloadInsert(index, payload, &insertRequest);
    return VecPayloadFormIndexTupleFromRef(index, &rawRef, request->heapTid);
}

static Buffer VecPayloadNewBuffer(Relation index, ForkNumber forkNum, LWLock *buildExtensionLock = NULL)
{
    Buffer volatile buf = InvalidBuffer;
    bool volatile lockHeld = false;

    PG_TRY();
    {
        if (buildExtensionLock != NULL) {
            LWLockAcquire(buildExtensionLock, LW_EXCLUSIVE);
            lockHeld = true;
        }
        buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        if (lockHeld) {
            LWLockRelease(buildExtensionLock);
            lockHeld = false;
        }
    }
    PG_CATCH();
    {
        if (lockHeld) {
            LWLockRelease(buildExtensionLock);
        }
        if (BufferIsValid(buf)) {
            ReleaseBuffer(buf);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    return buf;
}

static void VecPayloadInitPage(Buffer buf, Page page)
{
    HnswInitPage(buf, page);
    HnswPageSetRole(page, HNSW_PAGE_ROLE_PAYLOAD);
}


typedef struct VecPayloadInvalidContext {
    bool raiseError;
    Relation index;
    uint32 payloadLen;
} VecPayloadInvalidContext;

static void VecPayloadRaiseInvalid(const VecPayloadInvalidContext *context, BlockNumber blkno,
    OffsetNumber offno, const char *reason)
{
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("invalid vector payload tuple in index \"%s\" at block %u offset %u for kind %u length %u: %s",
            VecPayloadRelationName(context->index), (uint32)blkno, (uint32)offno,
            (uint32)VEC_PAYLOAD_RAW_VECTOR, context->payloadLen, reason)));
}

static bool VecPayloadInvalid(const VecPayloadInvalidContext *context, BlockNumber blkno, OffsetNumber offno,
    const char *reason)
{
    if (context->raiseError) {
        VecPayloadRaiseInvalid(context, blkno, offno, reason);
    }
    return false;
}

static void VecPayloadClearLoadGuard(VectorBufferLoadGuard *guard)
{
    if (guard == NULL) {
        return;
    }
    guard->data = NULL;
    guard->opaque[0] = 0;
    guard->opaque[1] = 0;
    guard->active = false;
}

static inline bool VecPayloadTidIsValid(const ItemPointerData *payloadTid, BlockNumber *blkno, OffsetNumber *offno)
{
    *blkno = InvalidBlockNumber;
    *offno = InvalidOffsetNumber;

    if (payloadTid == NULL) {
        return false;
    }

    *blkno = ItemPointerGetBlockNumberNoCheck(payloadTid);
    *offno = ItemPointerGetOffsetNumberNoCheck(payloadTid);
    return BlockNumberIsValid(*blkno) && OffsetNumberIsValid(*offno);
}


static bool VecPayloadTupleSizeFits(uint32 payloadLen, Size *tupleSize)
{
    Size rawSize;
    Size alignedSize;

    if (payloadLen > HNSW_MAX_SIZE) {
        return false;
    }

    rawSize = sizeof(VecPayloadTupleHeaderData) + (Size)payloadLen;
    alignedSize = MAXALIGN(rawSize);
    if (alignedSize < rawSize) {
        return false;
    }

    if (tupleSize != NULL) {
        *tupleSize = alignedSize;
    }
    return alignedSize <= HNSW_MAX_SIZE;
}

static void VecPayloadCheckPayloadLen(Relation index, VecPayloadKind kind, uint32 payloadLen, Size *tupleSize)
{
    if (!VecPayloadTupleSizeFits(payloadLen, tupleSize)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("vector payload tuple too large for index \"%s\" for kind %u length %u",
                VecPayloadRelationName(index), (uint32)kind, payloadLen)));
    }
}

static VecPayloadTupleHeaderData *VecPayloadFormTuple(const VecPayloadInput *payload, Size tupleSize)
{
    VecPayloadTupleHeaderData *tuple = (VecPayloadTupleHeaderData *)palloc0(tupleSize);
    errno_t rc = EOK;

    tuple->status = VEC_PAYLOAD_STATUS_LIVE;
    tuple->kind = (uint8)payload->kind;
    tuple->payloadLen = payload->len;
    if (payload->len > 0) {
        rc = memcpy_s(VecPayloadTupleGetData(tuple), payload->len, payload->data, payload->len);
        securec_check(rc, "\0", "\0");
    }
    return tuple;
}

static OffsetNumber VecPayloadPageAddTuple(Page page, const VecPayloadInput *payload, Size tupleSize)
{
    VecPayloadTupleHeaderData *tuple = VecPayloadFormTuple(payload, tupleSize);
    OffsetNumber offno = PageAddItem(page, (Item)tuple, tupleSize, InvalidOffsetNumber, false, false);

    pfree(tuple);
    return offno;
}

static void VecPayloadRaiseAddFailed(Relation index, VecPayloadKind kind, uint32 payloadLen)
{
    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
        errmsg("failed to add vector payload tuple to \"%s\" for kind %u length %u",
            VecPayloadRelationName(index), (uint32)kind, payloadLen)));
}

/*
 * Guard before writing to a page reached through the payload chain: refuse anything that
 * is not a payload page, so a bad hint cannot corrupt graph or metapage blocks.
 */
static void VecPayloadCheckWritablePage(Relation index, Page page, BlockNumber blkno)
{
    if (!VecPayloadPageIsPayload(page)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("block %u of index \"%s\" is not a vector payload page",
                (uint32)blkno, VecPayloadRelationName(index))));
    }
}

static VecPayloadTupleHeaderData *VecPayloadGetTuple(Page page, OffsetNumber offno, uint32 payloadLen)
{
    ItemId itemId;
    VecPayloadTupleHeaderData *header;
    Size expectedTupleSize;

    if (!VecPayloadTupleSizeFits(payloadLen, &expectedTupleSize) ||
        !OffsetNumberIsValid(offno) || offno > PageGetMaxOffsetNumber(page)) {
        return NULL;
    }
    itemId = PageGetItemId(page, offno);
    if (!ItemIdIsUsed(itemId) || !ItemIdIsNormal(itemId) ||
        ItemIdGetLength(itemId) != expectedTupleSize) {
        return NULL;
    }
    header = (VecPayloadTupleHeaderData *)PageGetItem(page, itemId);
    if (header->payloadLen != payloadLen) {
        return NULL;
    }
    return header;
}

static void VecPayloadStoreFreeNext(VecPayloadTupleHeaderData *header, const ItemPointerData *next)
{
    errno_t rc;

    Assert(header->payloadLen >= sizeof(ItemPointerData));
    rc = memcpy_s(VecPayloadTupleGetData(header), sizeof(ItemPointerData), next, sizeof(*next));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to store vector payload free-list link: memcpy_s returned %d", rc)));
    }
}

static void VecPayloadLoadFreeNext(const VecPayloadTupleHeaderData *header, ItemPointerData *next)
{
    errno_t rc = memcpy_s(next, sizeof(*next), VecPayloadTupleGetConstData(header), sizeof(ItemPointerData));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to load vector payload free-list link: memcpy_s returned %d", rc)));
    }
}

static void VecPayloadFillLive(VecPayloadTupleHeaderData *header, const VecPayloadInput *payload)
{
    errno_t rc;

    header->status = VEC_PAYLOAD_STATUS_LIVE;
    header->kind = (uint8)payload->kind;
    header->flags = 0;
    header->payloadLen = payload->len;
    if (payload->len > 0) {
        rc = memcpy_s(VecPayloadTupleGetData(header), payload->len, payload->data, payload->len);
        securec_check(rc, "\0", "\0");
    }
}

static Buffer VecPayloadLockMeta(Relation index, ForkNumber forkNum)
{
    Buffer buf = ReadBufferExtended(index, forkNum, VEC_PAYLOAD_METAPAGE_BLKNO, RBM_NORMAL, NULL);

    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    return buf;
}

typedef struct VecPayloadMetaFields {
    BlockNumber *insertBlkno;
    ItemPointerData *freeHead;
    uint64 *livePayloads;
    uint64 *deadPayloads;
} VecPayloadMetaFields;

static void VecPayloadBindMetaFields(Relation index, Page page, VecPayloadMetaFields *out)
{
    Oid am;

    if (out == NULL || page == NULL || !RelationIsValid(index) || index->rd_rel == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("vector payload metapage bind for \"%s\" needs a valid index page",
                VecPayloadRelationName(index))));
    }
    am = index->rd_rel->relam;
    if (am == HNSW_AM_OID) {
        HnswMetaPage metap = HnswPageGetMeta(page);

        out->insertBlkno = &metap->payloadInsertBlkno;
        out->freeHead = &metap->payloadFreeHead;
        out->livePayloads = &metap->livePayloads;
        out->deadPayloads = &metap->deadPayloads;
        return;
    }
    if (am == IVFFLAT_AM_OID) {
        IvfflatMetaPage metap = IvfflatPageGetMeta(page);

        out->insertBlkno = &metap->payloadInsertBlkno;
        out->freeHead = &metap->payloadFreeHead;
        out->livePayloads = &metap->livePayloads;
        out->deadPayloads = &metap->deadPayloads;
        return;
    }
    if (am == DISKANN_AM_OID) {
        DiskAnnMetaPage metap = DiskAnnPageGetMeta(page);

        out->insertBlkno = &metap->payloadInsertBlkno;
        out->freeHead = &metap->payloadFreeHead;
        out->livePayloads = &metap->livePayloads;
        out->deadPayloads = &metap->deadPayloads;
        return;
    }
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("vector payload metapage is not supported for access method %u", am)));
}

static void VecPayloadInitBuildPage(VecPayloadBuildState *state)
{
    state->page = BufferGetPage(state->buf);
    VecPayloadInitPage(state->buf, state->page);
    state->insertBlkno = BufferGetBlockNumber(state->buf);
}

static void VecPayloadAppendBuildPage(VecPayloadBuildState *state)
{
    Buffer newbuf = VecPayloadNewBuffer(state->index, state->forkNum);

    HnswPageGetOpaque(state->page)->nextblkno = BufferGetBlockNumber(newbuf);
    MarkBufferDirty(state->buf);
    UnlockReleaseBuffer(state->buf);
    state->buf = InvalidBuffer;
    state->page = NULL;

    LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    PG_TRY();
    {
        CHECK_FOR_INTERRUPTS();
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
    }
    PG_CATCH();
    {
        ReleaseBuffer(newbuf);
        PG_RE_THROW();
    }
    PG_END_TRY();

    state->buf = newbuf;
    VecPayloadInitBuildPage(state);
}

static bool VecPayloadValidatePage(const VecPayloadInvalidContext *context, Page page,
    BlockNumber blkno, OffsetNumber offno)
{
    if (page == NULL) {
        return VecPayloadInvalid(context, blkno, offno, "missing payload page");
    }

    PageHeader header = (PageHeader)page;
    if (!PageHeaderIsValid(header)) {
        return VecPayloadInvalid(context, blkno, offno, "payload page header is invalid");
    }
    if (PageGetPageSize(page) != BLCKSZ) {
        return VecPayloadInvalid(context, blkno, offno, "payload page size is invalid");
    }
    if (PageGetSpecialSize(page) != MAXALIGN(sizeof(HnswPageOpaqueData))) {
        return VecPayloadInvalid(context, blkno, offno, "payload page special size is invalid");
    }
    if (header->pd_lower < SizeOfPageHeaderData ||
        (header->pd_lower - SizeOfPageHeaderData) % sizeof(ItemIdData) != 0 ||
        header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special ||
        header->pd_special > BLCKSZ || header->pd_special < SizeOfPageHeaderData ||
        header->pd_special != MAXALIGN(header->pd_special)) {
        return VecPayloadInvalid(context, blkno, offno, "payload page boundaries are invalid");
    }
    if (HnswPageGetRole(page) != HNSW_PAGE_ROLE_PAYLOAD) {
        return VecPayloadInvalid(context, blkno, offno, "page role is not payload");
    }
    return true;
}

typedef struct VecPayloadTupleValidation {
    const VecPayloadInvalidContext *invalid;
    Page page;
    BlockNumber blkno;
    OffsetNumber offno;
    Size expectedTupleSize;
} VecPayloadTupleValidation;

static bool VecPayloadValidateTuple(const VecPayloadTupleValidation *context, const char **data)
{
    Page page = context->page;
    BlockNumber blkno = context->blkno;
    OffsetNumber offno = context->offno;
    PageHeader pageHeader = (PageHeader)page;
    if (!OffsetNumberIsValid(offno) || offno > PageGetMaxOffsetNumber(page)) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload offset is out of range");
    }

    ItemId itemId = PageGetItemId(page, offno);
    if (!ItemIdIsUsed(itemId) || !ItemIdIsNormal(itemId)) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload line pointer is not normal");
    }
    Size itemLen = ItemIdGetLength(itemId);
    LocationIndex itemOffset = ItemIdGetOffset(itemId);
    if (itemOffset < pageHeader->pd_upper || itemOffset >= pageHeader->pd_special) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload line pointer offset is out of range");
    }
    if (itemLen > (Size)(pageHeader->pd_special - itemOffset)) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload tuple extends past page data");
    }
    if (itemLen < sizeof(VecPayloadTupleHeaderData)) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload tuple header is truncated");
    }
    if (itemLen != context->expectedTupleSize) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload tuple physical length mismatch");
    }

    const VecPayloadTupleHeaderData *header =
        (const VecPayloadTupleHeaderData *)PageGetItem(page, itemId);
    if (header->status != VEC_PAYLOAD_STATUS_LIVE) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload tuple is not live");
    }
    if (header->kind != (uint8)VEC_PAYLOAD_RAW_VECTOR) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload kind mismatch");
    }
    if (header->flags != 0) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload flags are not zero");
    }
    if (header->payloadLen != context->invalid->payloadLen) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "payload length mismatch");
    }
    *data = VecPayloadTupleGetConstData(header);
    if ((uint32)VARSIZE_ANY(*data) != context->invalid->payloadLen) {
        return VecPayloadInvalid(context->invalid, blkno, offno, "raw varlena length mismatch");
    }
    return true;
}

typedef struct VecPayloadLocateRequest {
    Relation index;
    Page page;
    const ItemPointerData *payloadTid;
    uint32 payloadLen;
    bool raiseError;
} VecPayloadLocateRequest;

static bool VecPayloadLocatePageInternal(const VecPayloadLocateRequest *request, const char **data)
{
    Relation index = request->index;
    Page page = request->page;
    const ItemPointerData *payloadTid = request->payloadTid;
    uint32 payloadLen = request->payloadLen;
    const VecPayloadInvalidContext context = {request->raiseError, index, payloadLen};
    BlockNumber blkno = InvalidBlockNumber;
    OffsetNumber offno = InvalidOffsetNumber;
    Size expectedTupleSize;

    if (data == NULL) {
        return VecPayloadInvalid(&context, blkno, offno, "missing payload output");
    }
    *data = NULL;
    if (!VecPayloadTidIsValid(payloadTid, &blkno, &offno)) {
        return VecPayloadInvalid(&context, blkno, offno, "invalid payload tid");
    }
    if (payloadLen < VARHDRSZ || !VecPayloadTupleSizeFits(payloadLen, &expectedTupleSize)) {
        return VecPayloadInvalid(&context, blkno, offno, "payload length exceeds page limit");
    }
    if (!VecPayloadValidatePage(&context, page, blkno, offno)) {
        return false;
    }
    const VecPayloadTupleValidation validation = {&context, page, blkno, offno, expectedTupleSize};
    return VecPayloadValidateTuple(&validation, data);
}

void VecPayloadBeginBuild(VecPayloadBuildState *state, Relation index, ForkNumber forkNum)
{
    errno_t rc = EOK;

    Assert(state != NULL);
    Assert(index != NULL);

    rc = memset_s(state, sizeof(*state), 0, sizeof(*state));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to initialize vector payload build state: memset_s returned %d", rc)));
    }
    state->index = index;
    state->forkNum = forkNum;
    state->buf = VecPayloadNewBuffer(index, forkNum);
    VecPayloadInitBuildPage(state);
}

void VecPayloadFlushBuild(VecPayloadBuildState *state)
{
    Assert(state != NULL);

    if (!BufferIsValid(state->buf)) {
        return;
    }

    state->insertBlkno = BufferGetBlockNumber(state->buf);
    MarkBufferDirty(state->buf);
    UnlockReleaseBuffer(state->buf);
    state->buf = InvalidBuffer;
    state->page = NULL;
}

static void VecPayloadReacquireBuildBuffer(VecPayloadBuildState *state)
{
    Assert(state != NULL);
    Assert(state->index != NULL);

    if (BufferIsValid(state->buf)) {
        Assert(state->page != NULL);
        return;
    }

    if (!BlockNumberIsValid(state->insertBlkno)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("vector payload build lost insert page for index \"%s\"",
                VecPayloadRelationName(state->index))));
    }

    state->buf = ReadBufferExtended(state->index, state->forkNum, state->insertBlkno, RBM_NORMAL, NULL);
    LockBuffer(state->buf, BUFFER_LOCK_EXCLUSIVE);
    state->page = BufferGetPage(state->buf);
}

void VecPayloadPutBuild(VecPayloadBuildState *state, const VecPayloadInput *payload, VecPayloadDiskRef *outRef)
{
    Size tupleSize;
    OffsetNumber offno;
    errno_t rc = EOK;

    Assert(state != NULL);
    Assert(state->index != NULL);
    Assert(outRef != NULL);
    Assert(payload != NULL);
    Assert(payload->len == 0 || payload->data != NULL);

    VecPayloadReacquireBuildBuffer(state);

    VecPayloadCheckPayloadLen(state->index, payload->kind, payload->len, &tupleSize);

    if (PageGetFreeSpace(state->page) < tupleSize) {
        VecPayloadAppendBuildPage(state);
    }

    offno = VecPayloadPageAddTuple(state->page, payload, tupleSize);
    if (offno == InvalidOffsetNumber) {
        VecPayloadRaiseAddFailed(state->index, payload->kind, payload->len);
    }

    rc = memset_s(outRef, sizeof(*outRef), 0, sizeof(*outRef));
    if (rc != EOK) {
        UnlockReleaseBuffer(state->buf);
        state->buf = InvalidBuffer;
        state->page = NULL;
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to initialize vector payload reference: memset_s returned %d", rc)));
    }
    ItemPointerSet(&outRef->tid, BufferGetBlockNumber(state->buf), offno);
    outRef->payloadLen = payload->len;
    outRef->kind = (uint8)payload->kind;
}

void VecPayloadEndBuild(VecPayloadBuildState *state, BlockNumber *payloadInsertBlkno)
{
    Assert(state != NULL);

    VecPayloadFlushBuild(state);

    if (payloadInsertBlkno != NULL) {
        *payloadInsertBlkno = state->insertBlkno;
    }
}

/*
 * Append a payload page to the chain and put the tuple on it. The caller still holds
 * tailBuf exclusively, which is what serializes two backends racing off the same tail:
 * the loser wakes up seeing a valid nextblkno and follows it.
 */
static void VecPayloadMetaNoteInsert(Relation index, Page metaPage, BlockNumber landedBlkno, bool incrementLive)
{
    VecPayloadMetaFields metaFields;

    VecPayloadBindMetaFields(index, metaPage, &metaFields);
    if (BlockNumberIsValid(landedBlkno)) {
        *metaFields.insertBlkno = landedBlkno;
    }
    if (incrementLive) {
        (*metaFields.livePayloads)++;
    }
}

typedef enum VecPayloadReuseResult {
    VEC_PAYLOAD_REUSE_DONE = 0,
    VEC_PAYLOAD_REUSE_SKIPPED,
    VEC_PAYLOAD_REUSE_STOP
} VecPayloadReuseResult;

#define VEC_PAYLOAD_FREELIST_WALK_LIMIT 1000000
#define VEC_PAYLOAD_REDO_CHANGED_CAP 256


typedef struct VecPayloadFreelistTraversalContext {
    Relation index;
    ForkNumber forkNum;
    uint32 payloadLen;
    Buffer heldBuf;
    BlockNumber heldBlkno;
} VecPayloadFreelistTraversalContext;

static bool VecPayloadFreelistReadNext(const VecPayloadFreelistTraversalContext *context,
    const ItemPointerData *tid, ItemPointerData *nextOut)
{
    BlockNumber blkno;
    OffsetNumber offno;
    VecPayloadTupleHeaderData *header;
    Buffer buf;
    bool usedHeld;

    if (tid == NULL || nextOut == NULL || !ItemPointerIsValid(tid)) {
        return false;
    }
    blkno = ItemPointerGetBlockNumberNoCheck(tid);
    offno = ItemPointerGetOffsetNumberNoCheck(tid);
    usedHeld = BufferIsValid(context->heldBuf) && blkno == context->heldBlkno;
    if (usedHeld) {
        header = VecPayloadGetTuple(BufferGetPage(context->heldBuf), offno, context->payloadLen);
        if (header == NULL || header->status != VEC_PAYLOAD_STATUS_REUSABLE) {
            return false;
        }
        VecPayloadLoadFreeNext(header, nextOut);
        return true;
    }

    buf = ReadBufferExtended(context->index, context->forkNum, blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    header = VecPayloadGetTuple(BufferGetPage(buf), offno, context->payloadLen);
    if (header == NULL || header->status != VEC_PAYLOAD_STATUS_REUSABLE) {
        UnlockReleaseBuffer(buf);
        return false;
    }
    VecPayloadLoadFreeNext(header, nextOut);
    UnlockReleaseBuffer(buf);
    return true;
}

static bool VecPayloadFreelistWalkToTail(const VecPayloadFreelistTraversalContext *context,
    const ItemPointerData *startTid, const ItemPointerData *stopIfSeen, ItemPointerData *tailTid)
{
    ItemPointerData cur;
    uint32 i;

    if (startTid == NULL || tailTid == NULL || !ItemPointerIsValid(startTid)) {
        return false;
    }
    cur = *startTid;
    for (i = 0; i < VEC_PAYLOAD_FREELIST_WALK_LIMIT; i++) {
        ItemPointerData nxt;

        if (!VecPayloadFreelistReadNext(context, &cur, &nxt)) {
            return false;
        }
        if (!ItemPointerIsValid(&nxt)) {
            *tailTid = cur;
            return true;
        }
        if (stopIfSeen != NULL && ItemPointerIsValid(stopIfSeen)) {
            ItemPointerData stopCopy = *stopIfSeen;

            if (ItemPointerEquals(&nxt, &stopCopy)) {
                *tailTid = cur;
                return true;
            }
        }
        cur = nxt;
    }
    return false;
}

typedef struct VecPayloadFreelistRotation {
    Relation index;
    Buffer metaBuf;
    Buffer headBuf;
    const ItemPointerData *headTid;
    uint32 payloadLen;
    const ItemPointerData *newHead;
} VecPayloadFreelistRotation;

static VecPayloadReuseResult VecPayloadRotateFreelistPages(const VecPayloadFreelistRotation *rotation,
    Buffer tailBuf, const ItemPointerData *tailTid)
{
    GenericXLogState *state = GenericXLogStart(rotation->index);
    Page headPage = GenericXLogRegisterBuffer(state, rotation->headBuf, 0);
    Page tailPage = tailBuf == rotation->headBuf ? headPage : GenericXLogRegisterBuffer(state, tailBuf, 0);
    Page metaPage = GenericXLogRegisterBuffer(state, rotation->metaBuf, 0);
    VecPayloadTupleHeaderData *head = VecPayloadGetTuple(
        headPage, ItemPointerGetOffsetNumberNoCheck(rotation->headTid), rotation->payloadLen);
    VecPayloadTupleHeaderData *tail = VecPayloadGetTuple(
        tailPage, ItemPointerGetOffsetNumberNoCheck(tailTid), rotation->payloadLen);
    ItemPointerData invalid;

    if (head == NULL || tail == NULL) {
        GenericXLogAbort(state);
        return VEC_PAYLOAD_REUSE_STOP;
    }
    ItemPointerSetInvalid(&invalid);
    VecPayloadStoreFreeNext(head, &invalid);
    VecPayloadStoreFreeNext(tail, rotation->headTid);
    VecPayloadMetaFields metaFields;
    VecPayloadBindMetaFields(rotation->index, metaPage, &metaFields);
    *metaFields.freeHead = *rotation->newHead;
    GenericXLogFinish(state);
    return VEC_PAYLOAD_REUSE_SKIPPED;
}

static VecPayloadReuseResult VecPayloadFreelistRotateDeferred(const VecPayloadFreelistRotation *rotation,
    ForkNumber forkNum)
{
    Relation index = rotation->index;
    Buffer headBuf = rotation->headBuf;
    const ItemPointerData *headTid = rotation->headTid;
    const ItemPointerData *newHead = rotation->newHead;
    BlockNumber headBlkno = ItemPointerGetBlockNumberNoCheck(headTid);
    const VecPayloadFreelistTraversalContext context = {
        index, forkNum, rotation->payloadLen, headBuf, headBlkno};
    ItemPointerData tailTid;

    if (!VecPayloadFreelistWalkToTail(&context, newHead, headTid, &tailTid)) {
        return VEC_PAYLOAD_REUSE_STOP;
    }
    if (ItemPointerGetBlockNumberNoCheck(&tailTid) == headBlkno) {
        return VecPayloadRotateFreelistPages(rotation, headBuf, &tailTid);
    }

    BlockNumber tailBlkno = ItemPointerGetBlockNumberNoCheck(&tailTid);
    Buffer tailBuf = ReadBufferExtended(index, forkNum, tailBlkno, RBM_NORMAL, NULL);
    LockBuffer(tailBuf, BUFFER_LOCK_EXCLUSIVE);
    VecPayloadReuseResult result = VecPayloadRotateFreelistPages(rotation, tailBuf, &tailTid);
    UnlockReleaseBuffer(tailBuf);
    return result;
}

static void VecPayloadFreelistPrepend(Relation index, Page metaPage, Page payloadPage, const ItemPointerData *tid,
    uint32 payloadLen)
{
    VecPayloadMetaFields metaFields;
    OffsetNumber offno = ItemPointerGetOffsetNumberNoCheck(tid);
    VecPayloadTupleHeaderData *header = VecPayloadGetTuple(payloadPage, offno, payloadLen);
    ItemPointerData oldHead;

    VecPayloadBindMetaFields(index, metaPage, &metaFields);
    oldHead = *metaFields.freeHead;

    if (header == NULL || payloadLen < sizeof(ItemPointerData)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("vector payload free slot at block %u offset %u is unusable",
                (uint32)ItemPointerGetBlockNumberNoCheck(tid), (uint32)offno)));
    }
    if (!ItemPointerIsValid(&oldHead)) {
        ItemPointerSetInvalid(&oldHead);
    }
    VecPayloadStoreFreeNext(header, &oldHead);
    header->status = VEC_PAYLOAD_STATUS_REUSABLE;
    header->flags = 0;
    *metaFields.freeHead = *tid;
}

typedef struct VecPayloadAppendContext {
    Relation index;
    ForkNumber forkNum;
    Buffer metaBuf;
    Buffer tailBuf;
    LWLock *buildExtensionLock;
} VecPayloadAppendContext;

typedef struct VecPayloadAppendRequest {
    Size tupleSize;
    BlockNumber *outBlkno;
    OffsetNumber *outOffno;
} VecPayloadAppendRequest;

static void VecPayloadAppendPageAndAdd(const VecPayloadAppendContext *context, const VecPayloadInput *payload,
    const VecPayloadAppendRequest *request)
{
    GenericXLogState *volatile state = NULL;
    Buffer volatile nbuf = InvalidBuffer;
    Page npage;
    Page tailPage;
    OffsetNumber offno;
    bool volatile extensionLocked = false;

    PG_TRY();
    {
        LockRelationForExtension(context->index, ExclusiveLock);
        extensionLocked = true;
        nbuf = VecPayloadNewBuffer(context->index, context->forkNum, context->buildExtensionLock);

        state = GenericXLogStart(context->index);
        npage = GenericXLogRegisterBuffer(state, nbuf, GENERIC_XLOG_FULL_IMAGE);
        VecPayloadInitPage(nbuf, npage);

        offno = VecPayloadPageAddTuple(npage, payload, request->tupleSize);
        if (offno == InvalidOffsetNumber) {
            VecPayloadRaiseAddFailed(context->index, payload->kind, payload->len);
        }

        tailPage = GenericXLogRegisterBuffer(state, context->tailBuf, 0);
        HnswPageGetOpaque(tailPage)->nextblkno = BufferGetBlockNumber(nbuf);
        if (BufferIsValid(context->metaBuf)) {
            Page metaPage = GenericXLogRegisterBuffer(state, context->metaBuf, 0);

            VecPayloadMetaNoteInsert(context->index, metaPage, BufferGetBlockNumber(nbuf), true);
        }

        GenericXLogFinish(state);
        state = NULL;
        UnlockRelationForExtension(context->index, ExclusiveLock);
        extensionLocked = false;
    }
    PG_CATCH();
    {
        if (state != NULL) {
            GenericXLogAbort(state);
        }
        if (BufferIsValid(nbuf)) {
            UnlockReleaseBuffer(nbuf);
        }
        if (extensionLocked) {
            UnlockRelationForExtension(context->index, ExclusiveLock);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();

    *request->outBlkno = BufferGetBlockNumber(nbuf);
    *request->outOffno = offno;
    UnlockReleaseBuffer(nbuf);
}

typedef struct VecPayloadReuseContext {
    Relation index;
    ForkNumber forkNum;
    Buffer metaBuf;
    const VecPayloadInput *payload;
} VecPayloadReuseContext;

static void VecPayloadCommitReuse(const VecPayloadReuseContext *context, Buffer payloadBuf,
    OffsetNumber offno, const ItemPointerData *next)
{
    GenericXLogState *state = GenericXLogStart(context->index);
    Page payloadPage = GenericXLogRegisterBuffer(state, payloadBuf, 0);
    Page metaPage = GenericXLogRegisterBuffer(state, context->metaBuf, 0);
    VecPayloadTupleHeaderData *header = VecPayloadGetTuple(payloadPage, offno, context->payload->len);
    if (header == NULL || header->status != VEC_PAYLOAD_STATUS_REUSABLE) {
        GenericXLogAbort(state);
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("vector payload free slot changed before reuse")));
    }
    VecPayloadFillLive(header, context->payload);
    VecPayloadMetaFields metaFields;
    VecPayloadBindMetaFields(context->index, metaPage, &metaFields);
    *metaFields.freeHead = *next;
    if (*metaFields.deadPayloads > 0) {
        (*metaFields.deadPayloads)--;
    }
    VecPayloadMetaNoteInsert(context->index, metaPage, InvalidBlockNumber, true);
    GenericXLogFinish(state);
}

static VecPayloadReuseResult VecPayloadTryReuseSlot(const VecPayloadReuseContext *context,
    ItemPointerData *outTid)
{
    Relation index = context->index;
    ForkNumber forkNum = context->forkNum;
    Buffer metaBuf = context->metaBuf;
    const VecPayloadInput *payload = context->payload;
    VecPayloadMetaFields metaFields;
    VecPayloadBindMetaFields(index, BufferGetPage(metaBuf), &metaFields);
    ItemPointerData tid = *metaFields.freeHead;
    if (!ItemPointerIsValid(&tid)) {
        return VEC_PAYLOAD_REUSE_STOP;
    }

    BlockNumber blkno = ItemPointerGetBlockNumberNoCheck(&tid);
    OffsetNumber offno = ItemPointerGetOffsetNumberNoCheck(&tid);
    Buffer payloadBuf = ReadBufferExtended(index, forkNum, blkno, RBM_NORMAL, NULL);
    LockBuffer(payloadBuf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(payloadBuf);
    VecPayloadCheckWritablePage(index, page, blkno);
    VecPayloadTupleHeaderData *header = VecPayloadGetTuple(page, offno, payload->len);
    if (header == NULL || header->status != VEC_PAYLOAD_STATUS_REUSABLE) {
        UnlockReleaseBuffer(payloadBuf);
        return VEC_PAYLOAD_REUSE_STOP;
    }

    ItemPointerData next;
    VecPayloadLoadFreeNext(header, &next);
    if (VectorBufferInvalidatePayload(&index->rd_node, &tid) == VECTOR_BUFFER_INVALIDATE_DEFERRED) {
        if (!ItemPointerIsValid(&next)) {
            UnlockReleaseBuffer(payloadBuf);
            return VEC_PAYLOAD_REUSE_STOP;
        }
        const VecPayloadFreelistRotation rotation = {index, metaBuf, payloadBuf, &tid, payload->len, &next};
        VecPayloadReuseResult result = VecPayloadFreelistRotateDeferred(&rotation, forkNum);
        UnlockReleaseBuffer(payloadBuf);
        return result;
    }

    VecPayloadCommitReuse(context, payloadBuf, offno, &next);
    UnlockReleaseBuffer(payloadBuf);
    *outTid = tid;
    return VEC_PAYLOAD_REUSE_DONE;
}

static bool VecPayloadTryReuse(const VecPayloadReuseContext *context, ItemPointerData *reused)
{
    Relation index = context->index;
    Buffer metaBuf = context->metaBuf;
    ItemPointerData firstSkipped;

    ItemPointerSetInvalid(&firstSkipped);
    for (;;) {
        VecPayloadMetaFields metaFields;
        ItemPointerData headBefore;
        CHECK_FOR_INTERRUPTS();
        VecPayloadBindMetaFields(index, BufferGetPage(metaBuf), &metaFields);
        headBefore = *metaFields.freeHead;
        VecPayloadReuseResult result = VecPayloadTryReuseSlot(context, reused);
        if (result == VEC_PAYLOAD_REUSE_DONE) {
            return true;
        }
        if (result != VEC_PAYLOAD_REUSE_SKIPPED) {
            return false;
        }
        if (!ItemPointerIsValid(&firstSkipped)) {
            firstSkipped = headBefore;
        }
        VecPayloadBindMetaFields(index, BufferGetPage(metaBuf), &metaFields);
        if (ItemPointerEquals(metaFields.freeHead, &firstSkipped)) {
            return false;
        }
    }
}

typedef struct VecPayloadInsertAppendContext {
    Relation index;
    Buffer metaBuf;
    const VecPayloadInput *payload;
    const VecPayloadInsertRequest *request;
    Size tupleSize;
    BlockNumber *landedBlkno;
    OffsetNumber *landedOffno;
} VecPayloadInsertAppendContext;

static void VecPayloadAppend(const VecPayloadInsertAppendContext *insertContext, BlockNumber blkno)
{
    Relation index = insertContext->index;
    Buffer metaBuf = insertContext->metaBuf;
    const VecPayloadInput *payload = insertContext->payload;
    const VecPayloadInsertRequest *request = insertContext->request;
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        Buffer buf = ReadBufferExtended(index, request->forkNum, blkno, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf);
        VecPayloadCheckWritablePage(index, page, blkno);
        if (PageGetFreeSpace(page) >= insertContext->tupleSize) {
            GenericXLogState *state = GenericXLogStart(index);
            Page wpage = GenericXLogRegisterBuffer(state, buf, 0);
            Page metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
            OffsetNumber offno = VecPayloadPageAddTuple(wpage, payload, insertContext->tupleSize);
            if (offno == InvalidOffsetNumber) {
                GenericXLogAbort(state);
                UnlockReleaseBuffer(buf);
                UnlockReleaseBuffer(metaBuf);
                VecPayloadRaiseAddFailed(index, payload->kind, payload->len);
            }
            VecPayloadMetaNoteInsert(index, metaPage, blkno, true);
            GenericXLogFinish(state);
            *insertContext->landedBlkno = blkno;
            *insertContext->landedOffno = offno;
            UnlockReleaseBuffer(buf);
            return;
        }
        BlockNumber nextblkno = HnswPageGetOpaque(page)->nextblkno;
        if (BlockNumberIsValid(nextblkno)) {
            UnlockReleaseBuffer(buf);
            blkno = nextblkno;
            continue;
        }
        VecPayloadAppendContext context = {index, request->forkNum, metaBuf, buf, request->buildExtensionLock};
        VecPayloadAppendRequest appendRequest = {
            insertContext->tupleSize, insertContext->landedBlkno, insertContext->landedOffno};
        VecPayloadAppendPageAndAdd(&context, payload, &appendRequest);
        UnlockReleaseBuffer(buf);
        return;
    }
}

/*
 * Reuse a free-list slot when one exists, otherwise append on the payload
 * chain. Metapage hint and livePayloads are updated in this WAL record.
 */
void VecPayloadInsert(Relation index, const VecPayloadInput *payload, const VecPayloadInsertRequest *request)
{
    Assert(payload != NULL);
    Assert(request != NULL);
    Assert(request->outRef != NULL);
    Assert(payload->len == 0 || payload->data != NULL);
    if (!RelationIsValid(index)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("vector payload insert into \"%s\" needs a valid index", VecPayloadRelationName(index))));
    }

    Size tupleSize;
    VecPayloadCheckPayloadLen(index, payload->kind, payload->len, &tupleSize);
    errno_t rc = memset_s(request->outRef, sizeof(*request->outRef), 0, sizeof(*request->outRef));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to initialize vector payload reference: memset_s returned %d", rc)));
    }

    ItemPointerData reused;
    ItemPointerSetInvalid(&reused);
    Buffer metaBuf = VecPayloadLockMeta(index, request->forkNum);
    BlockNumber landedBlkno = InvalidBlockNumber;
    OffsetNumber landedOffno = InvalidOffsetNumber;
    const VecPayloadReuseContext reuseContext = {index, request->forkNum, metaBuf, payload};
    if (VecPayloadTryReuse(&reuseContext, &reused)) {
        landedBlkno = ItemPointerGetBlockNumber(&reused);
        landedOffno = ItemPointerGetOffsetNumber(&reused);
    } else {
        BlockNumber blkno = request->startBlkno;
        if (!BlockNumberIsValid(blkno)) {
            VecPayloadMetaFields fields;
            VecPayloadBindMetaFields(index, BufferGetPage(metaBuf), &fields);
            blkno = *fields.insertBlkno;
        }
        if (!BlockNumberIsValid(blkno)) {
            UnlockReleaseBuffer(metaBuf);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("vector payload insert into \"%s\" needs a valid payload page hint",
                    VecPayloadRelationName(index))));
        }
        const VecPayloadInsertAppendContext appendContext = {
            index, metaBuf, payload, request, tupleSize, &landedBlkno, &landedOffno};
        VecPayloadAppend(&appendContext, blkno);
    }

    UnlockReleaseBuffer(metaBuf);
    ItemPointerSet(&request->outRef->tid, landedBlkno, landedOffno);
    request->outRef->payloadLen = payload->len;
    request->outRef->kind = (uint8)payload->kind;
    if (request->insertBlknoOut != NULL) {
        *request->insertBlknoOut = landedBlkno;
    }
}

/* Return the on-page bytes, not a detoasted copy: WAL must clear the owner. */
static char *VecPayloadRetiredRef(Relation index, Page page, OffsetNumber offno)
{
    if (!OffsetNumberIsValid(offno) || offno > PageGetMaxOffsetNumber(page)) {
        return NULL;
    }
    ItemId item = PageGetItemId(page, offno);
    if (!ItemIdIsUsed(item) || !ItemIdHasStorage(item)) {
        return NULL;
    }
    if (index->rd_rel->relam == HNSW_AM_OID) {
        HnswElementTuple etup = (HnswElementTuple)PageGetItem(page, item);
        if (ItemIdGetLength(item) < HNSW_ELEMENT_TUPLE_V2_SIZE ||
            !HnswIsElementTuple(etup) || !etup->deleted || !HnswElementTupleHasVectorStorage(etup)) {
            return NULL;
        }
        return (char *)&HnswElementTupleGetPayloads(etup)->raw;
    }
    if ((index->rd_rel->relam != DISKANN_AM_OID && index->rd_rel->relam != IVFFLAT_AM_OID) ||
        !ItemIdIsDead(item) || ItemIdGetLength(item) < sizeof(IndexTupleData)) {
        return NULL;
    }
    IndexTuple itup = (IndexTuple)PageGetItem(page, item);
    Size dataOffset = IndexInfoFindDataOffset(itup->t_info);
    if (IndexTupleHasNulls(itup) || IndexTupleSize(itup) > ItemIdGetLength(item) ||
        dataOffset + VARHDRSZ > IndexTupleSize(itup)) {
        return NULL;
    }
    char *bytes = (char *)itup + dataOffset;
    if (VARATT_IS_EXTERNAL(bytes) || VARATT_IS_COMPRESSED(bytes) ||
        VARSIZE_ANY_EXHDR(bytes) != sizeof(VecPayloadDiskRef) ||
        dataOffset + VARSIZE_ANY(bytes) > IndexTupleSize(itup)) {
        return NULL;
    }
    return VARDATA_ANY(bytes);
}

/*
 * A retired owner retains its reference until reference removal, freelist
 * publication and counters are committed together. No bare payload TID may
 * be retried after reuse. HNSW also excludes such owners from graph reuse.
 *
 * Scans can hold graph then payload locks, while writers also lock metapages.
 * Acquire the latter locks conditionally: wait only after dropping the owner
 * and retry from its current reference, so no reverse-order wait is possible.
 */
void VecPayloadRecycle(Relation index, ForkNumber forkNum, const ItemPointerData *indexTid)
{
    BlockNumber ownerBlk;
    OffsetNumber ownerOff;
    if (!VecPayloadTidIsValid(indexTid, &ownerBlk, &ownerOff) || ownerBlk == VEC_PAYLOAD_METAPAGE_BLKNO) {
        return;
    }
    for (;;) {
        CHECK_FOR_INTERRUPTS();
        Buffer ownerBuf = ReadBufferExtended(index, forkNum, ownerBlk, RBM_NORMAL, NULL);
        LockBuffer(ownerBuf, BUFFER_LOCK_EXCLUSIVE);
        Page ownerPage = BufferGetPage(ownerBuf);
        char *refBytes = VecPayloadRetiredRef(index, ownerPage, ownerOff);
        VecPayloadDiskRef ref;
        if (refBytes == NULL) {
            UnlockReleaseBuffer(ownerBuf);
            return;
        }
        errno_t rc = memcpy_s(&ref, sizeof(ref), refBytes, sizeof(ref));
        securec_check(rc, "\0", "\0");
        if (!ItemPointerIsValid(&ref.tid)) {
            UnlockReleaseBuffer(ownerBuf);
            return;
        }
        BlockNumber payloadBlk = ItemPointerGetBlockNumber(&ref.tid);
        if (payloadBlk == ownerBlk || payloadBlk == VEC_PAYLOAD_METAPAGE_BLKNO) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("invalid retired vector payload reference")));
        }
        Size refOffset = refBytes - (char *)ownerPage;
        Buffer metaBuf = ReadBufferExtended(index, forkNum, VEC_PAYLOAD_METAPAGE_BLKNO, RBM_NORMAL, NULL);
        if (!ConditionalLockBuffer(metaBuf)) {
            UnlockReleaseBuffer(ownerBuf);
            LockBuffer(metaBuf, BUFFER_LOCK_EXCLUSIVE);
            UnlockReleaseBuffer(metaBuf);
            continue;
        }
        Buffer payloadBuf = ReadBufferExtended(index, forkNum, payloadBlk, RBM_NORMAL, NULL);
        if (!ConditionalLockBuffer(payloadBuf)) {
            UnlockReleaseBuffer(metaBuf);
            UnlockReleaseBuffer(ownerBuf);
            LockBuffer(payloadBuf, BUFFER_LOCK_EXCLUSIVE);
            UnlockReleaseBuffer(payloadBuf);
            continue;
        }
        const char *data = NULL;
        const VecPayloadLocateRequest request = {index, BufferGetPage(payloadBuf), &ref.tid, ref.payloadLen, true};
        (void)VecPayloadLocatePageInternal(&request, &data);
        (void)VectorBufferInvalidatePayload(&index->rd_node, &ref.tid);

        GenericXLogState *state = GenericXLogStart(index);
        Page payloadPage = GenericXLogRegisterBuffer(state, payloadBuf, 0);
        Page metaPage = GenericXLogRegisterBuffer(state, metaBuf, 0);
        ownerPage = GenericXLogRegisterBuffer(state, ownerBuf, 0);
        VecPayloadFreelistPrepend(index, metaPage, payloadPage, &ref.tid, ref.payloadLen);
        VecPayloadMetaFields metaFields;
        VecPayloadBindMetaFields(index, metaPage, &metaFields);
        if (*metaFields.livePayloads > 0) {
            (*metaFields.livePayloads)--;
        }
        (*metaFields.deadPayloads)++;
        ItemPointerSetInvalid(&ref.tid);
        rc = memcpy_s((char *)ownerPage + refOffset, sizeof(ref), &ref, sizeof(ref));
        securec_check(rc, "\0", "\0");
        GenericXLogFinish(state);
        UnlockReleaseBuffer(payloadBuf);
        UnlockReleaseBuffer(metaBuf);
        UnlockReleaseBuffer(ownerBuf);
        return;
    }
}

static bool VecPayloadSlotGetBytes(Page page, OffsetNumber offno, const char **data, uint16 *len)
{
    ItemId itemId;
    uint16 lpOff;
    uint16 lpLen;
    OffsetNumber maxoff;

    if (page == NULL || data == NULL || len == NULL || !OffsetNumberIsValid(offno)) {
        return false;
    }
    if (((PageHeader)page)->pd_lower > BLCKSZ) {
        return false;
    }
    maxoff = PageGetMaxOffsetNumber(page);
    if (offno > maxoff) {
        return false;
    }
    itemId = PageGetItemId(page, offno);
    if (!ItemIdIsUsed(itemId) || !ItemIdIsNormal(itemId) || !ItemIdHasStorage(itemId)) {
        return false;
    }
    lpLen = ItemIdGetLength(itemId);
    lpOff = ItemIdGetOffset(itemId);
    if (lpLen == 0 || lpOff < SizeOfPageHeaderData || ((Size)lpOff + lpLen) > BLCKSZ) {
        return false;
    }
    *data = ((const char *)page) + lpOff;
    *len = lpLen;
    return true;
}

static void VecPayloadInvalidateOneTid(const RelFileNode *rnode, BlockNumber blkno, OffsetNumber offno)
{
    ItemPointerData tid;
    if (rnode == NULL || !BlockNumberIsValid(blkno) || !OffsetNumberIsValid(offno)) {
        return;
    }
    ItemPointerSet(&tid, blkno, offno);
    (void)VectorBufferInvalidatePayload(rnode, &tid);
}

static void VecPayloadInvalidateAllSlotsOnPage(const RelFileNode *rnode, BlockNumber blkno, Page page)
{
    OffsetNumber offno;
    OffsetNumber maxoff;

    if (rnode == NULL || !VecPayloadPageIsPayload(page)) {
        return;
    }
    if (((PageHeader)page)->pd_lower > BLCKSZ) {
        return;
    }
    maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff > MaxOffsetNumber) {
        return;
    }
    for (offno = FirstOffsetNumber; offno <= maxoff; offno++) {
        ItemId itemId = PageGetItemId(page, offno);
        if (!ItemIdIsUsed(itemId)) {
            continue;
        }
        VecPayloadInvalidateOneTid(rnode, blkno, offno);
    }
}

/*
 * Compare old/new payload pages and fill *out with changed offset numbers.
 * Returns the count, or -1 if the pages cannot be compared safely.
 */
static int VecPayloadCollectChangedOffnos(Page oldPage, Page newPage, OffsetNumber *out, int outCap)
{
    OffsetNumber oldMax;
    OffsetNumber newMax;
    OffsetNumber maxoff;
    OffsetNumber offno;
    int nchanged = 0;

    if (!VecPayloadPageIsPayload(newPage) || !VecPayloadPageIsPayload(oldPage)) {
        return -1;
    }
    if (((PageHeader)oldPage)->pd_lower > BLCKSZ || ((PageHeader)newPage)->pd_lower > BLCKSZ) {
        return -1;
    }
    oldMax = PageGetMaxOffsetNumber(oldPage);
    newMax = PageGetMaxOffsetNumber(newPage);
    if (oldMax > MaxOffsetNumber || newMax > MaxOffsetNumber) {
        return -1;
    }
    maxoff = (oldMax > newMax) ? oldMax : newMax;
    for (offno = FirstOffsetNumber; offno <= maxoff; offno++) {
        const char *oldData = NULL;
        const char *newData = NULL;
        uint16 oldLen = 0;
        uint16 newLen = 0;
        bool oldUsed;
        bool newUsed;
        bool changed;

        oldUsed = VecPayloadSlotGetBytes(oldPage, offno, &oldData, &oldLen);
        newUsed = VecPayloadSlotGetBytes(newPage, offno, &newData, &newLen);
        if (!oldUsed && !newUsed) {
            continue;
        }
        changed = (oldUsed != newUsed) || (oldLen != newLen) ||
            (oldUsed && newUsed && memcmp(oldData, newData, oldLen) != 0);
        if (!changed) {
            continue;
        }
        if (out != NULL && nchanged < outCap) {
            out[nchanged] = offno;
        }
        nchanged++;
    }
    return nchanged;
}

/*
 * Redo-path gate. Callers snapshot the pre-redo page image only when this
 * returns true, so blocks that cannot hold cached payload slots never pay for
 * the copy. Skipping the snapshot stays correct: passing oldPage == NULL makes
 * VecPayloadRedoInvalidateChangedSlots() invalidate the whole page instead of
 * diffing it.
 */
bool VecPayloadRedoNeedsOldImage(Page page)
{
    return VectorBufferIsActive() && VecPayloadPageIsPayload(page);
}

void VecPayloadRedoInvalidateIfNeeded(const RelFileNode *rnode, BlockNumber blkno, Page newPage)
{
    if (!VectorBufferIsActive()) {
        return;
    }
    VecPayloadInvalidateAllSlotsOnPage(rnode, blkno, newPage);
}

void VecPayloadRedoInvalidateChangedSlots(const RelFileNode *rnode, BlockNumber blkno, Page oldPage, Page newPage)
{
    OffsetNumber changed[VEC_PAYLOAD_REDO_CHANGED_CAP];
    int nchanged;
    int i;

    if (rnode == NULL || !VectorBufferIsActive() || !VecPayloadPageIsPayload(newPage)) {
        return;
    }
    if (oldPage == NULL) {
        VecPayloadInvalidateAllSlotsOnPage(rnode, blkno, newPage);
        return;
    }
    nchanged = VecPayloadCollectChangedOffnos(oldPage, newPage, changed, VEC_PAYLOAD_REDO_CHANGED_CAP);
    if (nchanged < 0 || nchanged > VEC_PAYLOAD_REDO_CHANGED_CAP) {
        VecPayloadInvalidateAllSlotsOnPage(rnode, blkno, newPage);
        return;
    }
    for (i = 0; i < nchanged; i++) {
        VecPayloadInvalidateOneTid(rnode, blkno, changed[i]);
    }
}


static bool VecPayloadLoadBegin(const ItemPointerData *payloadTid, uint32 payloadLen,
    void *loaderCtx, VectorBufferLoadGuard *guard)
{
    Relation index = static_cast<Relation>(loaderCtx);
    volatile Buffer buf = InvalidBuffer;
    BlockNumber blkno = InvalidBlockNumber;
    OffsetNumber offno = InvalidOffsetNumber;
    volatile bool locked = false;
    const char *data = NULL;
    Size tupleSize;

    VecPayloadClearLoadGuard(guard);
    if (guard == NULL || !RelationIsValid(index) || payloadLen < VARHDRSZ ||
        !VecPayloadTidIsValid(payloadTid, &blkno, &offno) ||
        !VecPayloadTupleSizeFits(payloadLen, &tupleSize)) {
        return false;
    }

    PG_TRY();
    {
        buf = ReadBuffer(index, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        locked = true;
        const VecPayloadLocateRequest request = {index, BufferGetPage(buf), payloadTid, payloadLen, true};
        (void)VecPayloadLocatePageInternal(&request, &data);

        guard->data = data;
        guard->opaque[0] = (uintptr_t)(intptr_t)buf;
        guard->opaque[1] = 0;
        pg_write_barrier();
        guard->active = true;
    }
    PG_CATCH();
    {
        if (locked) {
            UnlockReleaseBuffer(buf);
        } else if (BufferIsValid(buf)) {
            ReleaseBuffer(buf);
        }
        VecPayloadClearLoadGuard(guard);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return true;
}

static void VecPayloadLoadEnd(VectorBufferLoadGuard *guard, bool isCommit)
{
    Buffer buf;

    if (guard == NULL || !guard->active) {
        return;
    }

    buf = (Buffer)(intptr_t)guard->opaque[0];
    VecPayloadClearLoadGuard(guard);
    if (BufferIsValid(buf)) {
        /* A nested function's subtransaction can release our lock before
         * the executor calls EndAccess, even on the normal cleanup path. */
        if (isCommit && !BufferIsLocal(buf) &&
            LWLockHeldByMe(GetBufferDescriptor(buf - 1)->content_lock)) {
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }
        ReleaseBuffer(buf);
    }
}

static const VectorBufferLoadOps VecPayloadLoadOps = {VecPayloadLoadBegin, VecPayloadLoadEnd};

static inline bool VecPayloadPinRequestIsValid(Relation index, const VecPayloadDiskRef *diskRef,
    const VecPayloadPinRequest *request)
{
    return RelationIsValid(index) && diskRef != NULL && request != NULL &&
        request->kind == VEC_PAYLOAD_RAW_VECTOR && diskRef->kind == (uint8)VEC_PAYLOAD_RAW_VECTOR &&
        request->expectedPayloadLen >= VARHDRSZ && diskRef->payloadLen == request->expectedPayloadLen &&
        VEC_PAYLOAD_TUPLE_SIZE(request->expectedPayloadLen) <= BLCKSZ &&
        ItemPointerIsValid(&diskRef->tid) && VecPayloadDiskRefReservedIsZero(diskRef);
}

bool VecPayloadPinGet(Relation index, const VecPayloadDiskRef *diskRef,
    const VecPayloadPinRequest *request, VecPayloadPin *pin)
{
    errno_t rc = EOK;
    volatile bool pinned = false;

    if (pin == NULL) {
        return false;
    }
    rc = memset_s(pin, sizeof(*pin), 0, sizeof(*pin));
    if (rc != EOK) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("failed to initialize vector payload pin: memset_s returned %d", rc)));
    }
    if (!VecPayloadPinRequestIsValid(index, diskRef, request)) {
        return false;
    }

    if (request->access != NULL) {
        if (!VectorBufferPinFast(request->access, &diskRef->tid, &VecPayloadLoadOps,
            index, &pin->handle)) {
            return false;
        }
        pin->datum = PointerGetDatum(pin->handle.data);
        return true;
    }

    PG_TRY();
    {
        if (VectorBufferBeginAccess(&index->rd_node, request->expectedPayloadLen, &pin->ownedAccess) &&
            pin->ownedAccess != NULL) {
            pinned = VectorBufferPinFast(pin->ownedAccess, &diskRef->tid, &VecPayloadLoadOps,
                index, &pin->handle);
        }
    }
    PG_CATCH();
    {
        VectorBufferRelease(&pin->handle);
        if (pin->ownedAccess != NULL) {
            VectorBufferEndAccess(&pin->ownedAccess);
        }
        pin->datum = (Datum)0;
        PG_RE_THROW();
    }
    PG_END_TRY();

    if (!pinned) {
        if (pin->ownedAccess != NULL) {
            VectorBufferEndAccess(&pin->ownedAccess);
        }
        return false;
    }
    pin->datum = PointerGetDatum(pin->handle.data);
    return true;
}

void VecPayloadUnpin(VecPayloadPin *pin)
{
    if (pin == NULL) {
        return;
    }

    VectorBufferRelease(&pin->handle);
    if (pin->ownedAccess != NULL) {
        VectorBufferEndAccess(&pin->ownedAccess);
    }
    pin->datum = (Datum)0;
}
