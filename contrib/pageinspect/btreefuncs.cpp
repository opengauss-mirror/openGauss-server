/*
 * contrib/pageinspect/btreefuncs.c
 *
 *
 * btreefuncs.c
 *
 * Copyright (c) 2006 Satoshi Nagayasu <nagayasus@nttdata.co.jp>
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

extern "C" Datum bt_metap(PG_FUNCTION_ARGS);
extern "C" Datum bt_page_items(PG_FUNCTION_ARGS);
extern "C" Datum bt_page_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(bt_metap);
PG_FUNCTION_INFO_V1(bt_page_items);
PG_FUNCTION_INFO_V1(bt_page_stats);

#define IS_INDEX(r) ((r)->rd_rel->relkind == RELKIND_INDEX)
#define IS_BTREE(r) ((r)->rd_rel->relam == BTREE_AM_OID || \
    (r)->rd_rel->relam == CBTREE_AM_OID || \
    (r)->rd_rel->relam == UBTREE_AM_OID)

/* note: BlockNumber is unsigned, hence can't be negative */
#define CHECK_RELATION_BLOCK_RANGE(rel, blkno)                      \
    {                                                               \
        if (RelationGetNumberOfBlocks(rel) <= (BlockNumber)(blkno)) \
            elog(ERROR, "block number out of range");               \
    }

/* ------------------------------------------------
 * structure for single btree page statistics
 * ------------------------------------------------
 */
typedef struct BTPageStat {
    uint32 blkno;
    uint32 live_items;
    uint32 dead_items;
    uint32 page_size;
    uint32 max_avail;
    uint32 free_size;
    uint32 avg_item_size;
    char type;

    /* opaque data */
    BlockNumber btpo_prev;
    BlockNumber btpo_next;
    union {
        uint32 level;
        TransactionId xact;
    } btpo;
    uint16 btpo_flags;
    BTCycleId btpo_cycleid;
} BTPageStat;

/* -------------------------------------------------
 * GetBTPageStatistics()
 *
 * Collect statistics of single b-tree page
 * -------------------------------------------------
 */
static void GetBTPageStatistics(BlockNumber blkno, Buffer buffer, BTPageStat* stat)
{
    Page page = BufferGetPage(buffer);
    PageHeader phdr = (PageHeader)page;
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    int item_size = 0;
    int off;

    stat->blkno = blkno;

    stat->max_avail = BLCKSZ - (BLCKSZ - phdr->pd_special + SizeOfPageHeaderData);

    stat->dead_items = stat->live_items = 0;

    stat->page_size = PageGetPageSize(page);

    /* page type (flags) */
    if (P_ISDELETED(opaque)) {
        stat->type = 'd';
        stat->btpo.xact = ((BTPageOpaque)opaque)->xact;
        return;
    } else if (P_IGNORE(opaque))
        stat->type = 'e';
    else if (P_ISLEAF(opaque))
        stat->type = 'l';
    else if (P_ISROOT(opaque))
        stat->type = 'r';
    else
        stat->type = 'i';

    /* btpage opaque data */
    stat->btpo_prev = opaque->btpo_prev;
    stat->btpo_next = opaque->btpo_next;
    stat->btpo.level = opaque->btpo.level;
    stat->btpo_flags = opaque->btpo_flags;
    stat->btpo_cycleid = opaque->btpo_cycleid;

    /* count live and dead tuples, and free space */
    for (off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++) {
        IndexTuple itup;

        ItemId id = PageGetItemId(page, off);

        itup = (IndexTuple)PageGetItem(page, id);

        item_size += IndexTupleSize(itup);

        if (!ItemIdIsDead(id))
            stat->live_items++;
        else
            stat->dead_items++;
    }
    stat->free_size = PageGetFreeSpace(page);

    if ((stat->live_items + stat->dead_items) > 0)
        stat->avg_item_size = item_size / (stat->live_items + stat->dead_items);
    else
        stat->avg_item_size = 0;
}

/* -----------------------------------------------
 * bt_page_stats()
 *
 * Usage: SELECT * FROM bt_page_stats('t1_pkey', 1);
 * -----------------------------------------------
 */
Datum bt_page_stats(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    Buffer buffer;
    Relation rel;
    RangeVar* relrv = NULL;
    Datum result;
    HeapTuple tuple;
    TupleDesc tupleDesc;
    int j;
    char* values[11];
    BTPageStat stat = {0};

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use pageinspect functions"))));

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);

    if (!IS_INDEX(rel) || !IS_BTREE(rel))
        elog(ERROR, "relation \"%s\" is not a btree index", RelationGetRelationName(rel));

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));

    if (blkno == 0)
        elog(ERROR, "block 0 is a meta page");

    CHECK_RELATION_BLOCK_RANGE(rel, blkno);

    buffer = ReadBuffer(rel, blkno);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    /* keep compiler quiet */
    stat.btpo_prev = stat.btpo_next = InvalidBlockNumber;
    stat.btpo_flags = stat.free_size = stat.avg_item_size = 0;

    GetBTPageStatistics(blkno, buffer, &stat);

    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    j = 0;
    errno_t ret = 0;
    const int charLen = 32;
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.blkno);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%c", stat.type);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.live_items);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.dead_items);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.avg_item_size);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.page_size);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.free_size);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.btpo_prev);
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.btpo_next);
    securec_check_ss(ret, "", "");
    if (stat.type == 'd') {
        values[j] = (char*)palloc(charLen * 2);
        ret = snprintf_s(values[j++], charLen * 2, charLen * 2 - 1, XID_FMT, stat.btpo.xact);
    } else {
        values[j] = (char*)palloc(charLen);
        ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.btpo.level);
    }
    securec_check_ss(ret, "", "");
    values[j] = (char*)palloc(charLen);
    ret = snprintf_s(values[j++], charLen, charLen - 1, "%d", stat.btpo_flags);
    securec_check_ss(ret, "", "");

    tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);

    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

/*-------------------------------------------------------
 * bt_page_items()
 *
 * Get IndexTupleData set in a btree page
 *
 * Usage: SELECT * FROM bt_page_items('t1_pkey', 1);
 *-------------------------------------------------------
 */

/*
 * cross-call data structure for SRF
 */
struct user_args {
    Page page;
    OffsetNumber offset;
};

Datum bt_page_items(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    Datum result;
    char* values[6];
    HeapTuple tuple;
    FuncCallContext* fctx = NULL;
    MemoryContext mctx;
    struct user_args* uargs;
    errno_t rc;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use pageinspect functions"))));

    if (SRF_IS_FIRSTCALL()) {
        RangeVar* relrv = NULL;
        Relation rel;
        Buffer buffer;
        BTPageOpaqueInternal opaque;
        TupleDesc tupleDesc;

        fctx = SRF_FIRSTCALL_INIT();

        relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
        rel = relation_openrv(relrv, AccessShareLock);

        if (!IS_INDEX(rel) || !IS_BTREE(rel))
            elog(ERROR, "relation \"%s\" is not a btree index", RelationGetRelationName(rel));

        /*
         * Reject attempts to read non-local temporary relations; we would be
         * likely to get wrong data since we have no visibility into the
         * owning session's local buffers.
         */
        if (RELATION_IS_OTHER_TEMP(rel))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));

        if (blkno == 0)
            elog(ERROR, "block 0 is a meta page");

        CHECK_RELATION_BLOCK_RANGE(rel, blkno);

        buffer = ReadBuffer(rel, blkno);
        LockBuffer(buffer, BUFFER_LOCK_SHARE);

        /*
         * We copy the page into local storage to avoid holding pin on the
         * buffer longer than we must, and possibly failing to release it at
         * all if the calling query doesn't fetch all rows.
         */
        mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

        uargs = (user_args*)palloc(sizeof(struct user_args));

        uargs->page = (char*)palloc(BLCKSZ);
        rc = memcpy_s(uargs->page, BLCKSZ, BufferGetPage(buffer), BLCKSZ);

        UnlockReleaseBuffer(buffer);
        relation_close(rel, AccessShareLock);

        uargs->offset = FirstOffsetNumber;

        opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(uargs->page);

        if (P_ISDELETED(opaque))
            elog(NOTICE, "page is deleted");

        fctx->max_calls = PageGetMaxOffsetNumber(uargs->page);

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
            elog(ERROR, "return type must be a row type");

        fctx->attinmeta = TupleDescGetAttInMetadata(tupleDesc);

        fctx->user_fctx = uargs;

        MemoryContextSwitchTo(mctx);
    }

    fctx = SRF_PERCALL_SETUP();
    uargs = (user_args*)(fctx->user_fctx);

    if (fctx->call_cntr < fctx->max_calls) {
        ItemId id;
        IndexTuple itup;
        int j;
        int off;
        int dlen;
        char* dump = NULL;
        char* ptr = NULL;

        id = PageGetItemId(uargs->page, uargs->offset);

        if (!ItemIdIsValid(id))
            elog(ERROR, "invalid ItemId");

        itup = (IndexTuple)PageGetItem(uargs->page, id);

        j = 0;
        values[j] = (char*)palloc(32);
        rc = snprintf_s(values[j++], 32, 31, "%d", uargs->offset);
        securec_check_ss(rc, "", "");
        values[j] = (char*)palloc(32);
        rc = snprintf_s(values[j++], 32, 31, "(%u,%u)",
            BlockIdGetBlockNumber(&(itup->t_tid.ip_blkid)), itup->t_tid.ip_posid);
        securec_check_ss(rc, "", "");
        values[j] = (char*)palloc(32);
        rc = snprintf_s(values[j++], 32, 31, "%d", (int)IndexTupleSize(itup));
        securec_check_ss(rc, "", "");
        values[j] = (char*)palloc(32);
        rc = snprintf_s(values[j++], 32, 31, "%c", IndexTupleHasNulls(itup) ? 't' : 'f');
        securec_check_ss(rc, "", "");
        values[j] = (char*)palloc(32);
        rc = snprintf_s(values[j++], 32, 31, "%c", IndexTupleHasVarwidths(itup) ? 't' : 'f');
        securec_check_ss(rc, "", "");

        ptr = (char*)itup + IndexInfoFindDataOffset(itup->t_info);
        dlen = IndexTupleSize(itup) - IndexInfoFindDataOffset(itup->t_info);
        dump = (char*)palloc0(dlen * 3 + 1);
        int length = dlen * 3 + 1;
        values[j] = dump;
        for (off = 0; off < dlen; off++) {
            if (off > 0) {
                *dump++ = ' ';
                length--;
            }
            rc = sprintf_s(dump, length, "%02x", *(ptr + off) & 0xff);
            securec_check_ss(rc, "", "");
            dump += 2;
            length -= 2;
        }

        tuple = BuildTupleFromCStrings(fctx->attinmeta, values);
        result = HeapTupleGetDatum(tuple);

        uargs->offset = uargs->offset + 1;

        SRF_RETURN_NEXT(fctx, result);
    } else {
        pfree(uargs->page);
        pfree(uargs);
        SRF_RETURN_DONE(fctx);
    }
}

/* ------------------------------------------------
 * bt_metap()
 *
 * Get a btree's meta-page information
 *
 * Usage: SELECT * FROM bt_metap('t1_pkey')
 * ------------------------------------------------
 */
Datum bt_metap(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    Datum result;
    Relation rel;
    RangeVar* relrv = NULL;
    BTMetaPageData* metad = NULL;
    TupleDesc tupleDesc;
    int j;
    char* values[6];
    Buffer buffer;
    Page page;
    HeapTuple tuple;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use pageinspect functions"))));

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);

    if (!IS_INDEX(rel) || !IS_BTREE(rel))
        elog(ERROR, "relation \"%s\" is not a btree index", RelationGetRelationName(rel));

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));

    buffer = ReadBuffer(rel, 0);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    page = BufferGetPage(buffer);
    metad = BTPageGetMeta(page);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    j = 0;
    errno_t rc;
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_magic);
    securec_check_ss(rc, "", "");
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_version);
    securec_check_ss(rc, "", "");
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_root);
    securec_check_ss(rc, "", "");
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_level);
    securec_check_ss(rc, "", "");
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_fastroot);
    securec_check_ss(rc, "", "");
    values[j] = (char*)palloc(32);
    rc = snprintf_s(values[j++], 32, 31, "%d", metad->btm_fastlevel);
    securec_check_ss(rc, "", "");

    tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(tupleDesc), values);

    result = HeapTupleGetDatum(tuple);

    UnlockReleaseBuffer(buffer);
    relation_close(rel, AccessShareLock);

    PG_RETURN_DATUM(result);
}
