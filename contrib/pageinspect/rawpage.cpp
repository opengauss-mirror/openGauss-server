/*-------------------------------------------------------------------------
 *
 * rawpage.c
 *	  Functions to extract a raw page as bytea and inspect it
 *
 * Access-method specific inspection functions are in separate files.
 *
 * Copyright (c) 2007-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/rawpage.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/checksum.h"
#include "storage/pagecompress.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

PG_MODULE_MAGIC;

extern "C" Datum get_raw_page(PG_FUNCTION_ARGS);
extern "C" Datum get_raw_page_fork(PG_FUNCTION_ARGS);
extern "C" Datum page_header(PG_FUNCTION_ARGS);
extern "C" Datum page_compress_meta(PG_FUNCTION_ARGS);
extern "C" Datum page_compress_meta_usage(PG_FUNCTION_ARGS);

static bytea* get_raw_page_internal(text* relname, ForkNumber forknum, BlockNumber blkno);

/*
 * get_raw_page
 *
 * Returns a copy of a page from shared buffers as a bytea
 */
PG_FUNCTION_INFO_V1(get_raw_page);

Datum get_raw_page(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    bytea* raw_page = NULL;

    /*
     * We don't normally bother to check the number of arguments to a C
     * function, but here it's needed for safety because early 8.4 beta
     * releases mistakenly redefined get_raw_page() as taking three arguments.
     */
    if (PG_NARGS() != 2)
        ereport(ERROR,
            (errmsg("wrong number of arguments to get_raw_page()"),
                errhint("Run the updated pageinspect.sql script.")));

    raw_page = get_raw_page_internal(relname, MAIN_FORKNUM, blkno);

    PG_RETURN_BYTEA_P(raw_page);
}

/*
 * get_raw_page_fork
 *
 * Same, for any fork
 */
PG_FUNCTION_INFO_V1(get_raw_page_fork);

Datum get_raw_page_fork(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    text* forkname = PG_GETARG_TEXT_P(1);
    uint32 blkno = PG_GETARG_UINT32(2);
    bytea* raw_page = NULL;
    ForkNumber forknum;

    forknum = forkname_to_number(text_to_cstring(forkname));

    raw_page = get_raw_page_internal(relname, forknum, blkno);

    PG_RETURN_BYTEA_P(raw_page);
}

/*
 * workhorse
 */
static bytea* get_raw_page_internal(text* relname, ForkNumber forknum, BlockNumber blkno)
{
    bytea* raw_page = NULL;
    RangeVar* relrv = NULL;
    Relation rel;
    char* raw_page_data = NULL;
    Buffer buf;

    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use raw functions"))));

    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);

    /* Check that this relation has storage */
    if (rel->rd_rel->relkind == RELKIND_VIEW)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from view \"%s\"", RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_CONTQUERY)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from contview for streaming engine \"%s\"", 
                       RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from composite type \"%s\"", RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from foreign table \"%s\"", RelationGetRelationName(rel))));

    if (rel->rd_rel->relkind == RELKIND_STREAM)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from stream for streaming engine \"%s\"", 
                       RelationGetRelationName(rel))));

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));

    if (blkno >= RelationGetNumberOfBlocks(rel))
        elog(ERROR, "block number %u is out of range for relation \"%s\"", blkno, RelationGetRelationName(rel));

    /* Initialize buffer to copy to */
    raw_page = (bytea*)palloc(BLCKSZ + VARHDRSZ);
    SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
    raw_page_data = VARDATA(raw_page);

    /* Take a verbatim copy of the page */

    buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_SHARE);

    errno_t rc = memcpy_s(raw_page_data, BLCKSZ, BufferGetPage(buf), BLCKSZ);
    securec_check_c(rc, "\0", "\0");

    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buf);

    relation_close(rel, AccessShareLock);

    return raw_page;
}

/*
 * page_header
 *
 * Allows inspection of page header fields of a raw page
 */

PG_FUNCTION_INFO_V1(page_header);

Datum page_header(PG_FUNCTION_ARGS)
{
    bytea* raw_page = PG_GETARG_BYTEA_P(0);
    int raw_page_size;

    TupleDesc tupdesc;

    Datum result;
    HeapTuple tuple;
    Datum values[11];
    bool nulls[11];

    PageHeader page;
    XLogRecPtr lsn;
    char lsnchar[64];
    errno_t rc = EOK;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use raw page functions"))));

    raw_page_size = VARSIZE(raw_page) - VARHDRSZ;

    /*
     * Check that enough data was supplied, so that we don't try to access
     * fields outside the supplied buffer.
     */
    if (raw_page_size < GetPageHeaderSize(page))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("input page too small (%d bytes)", raw_page_size)));

    page = (PageHeader)VARDATA(raw_page);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    /* Extract information from the page header */

    lsn = PageGetLSN(page);
    rc = snprintf_s(lsnchar, sizeof(lsnchar), sizeof(lsnchar) - 1, "%X/%X", (uint32)(lsn >> 32), (uint32)lsn);
    securec_check_ss(rc, "\0", "\0");

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    values[0] = CStringGetTextDatum(lsnchar);
    values[1] = UInt16GetDatum(page->pd_checksum);
    values[2] = UInt16GetDatum(page->pd_flags);
    values[3] = UInt16GetDatum(page->pd_lower);
    values[4] = UInt16GetDatum(page->pd_upper);
    values[5] = UInt16GetDatum(page->pd_special);
    values[6] = UInt16GetDatum(PageGetPageSize(page));
    values[7] = UInt16GetDatum(PageGetPageLayoutVersion(page));
    if (PageIs8BXidHeapVersion(page)) {
        values[8] = TransactionIdGetDatum(page->pd_prune_xid + ((HeapPageHeader)page)->pd_xid_base);
        values[9] = TransactionIdGetDatum(((HeapPageHeader)page)->pd_xid_base);
        values[10] = TransactionIdGetDatum(((HeapPageHeader)page)->pd_multi_base);
        nulls[8] = false;
        nulls[9] = false;
        nulls[10] = false;
    } else {
        values[8] = ShortTransactionIdGetDatum(page->pd_prune_xid);
        nulls[9] = true;
        nulls[10] = true;
    }

    /* Build and return the tuple. */

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

// compress metadata extension plug
//
static void super_user(void);
static void check(Relation rel);
static char* read_raw_page(Relation rel, ForkNumber forknum, BlockNumber blkno);
static void parse_compress_meta(StringInfo outputBuf, char* page_content, Relation rel);

// arg1: relation name
// arg2: start blockno
// arg3: number of parsing block, default is 1
//
PG_FUNCTION_INFO_V1(page_compress_meta);

Datum page_compress_meta(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    uint32 blkno = PG_GETARG_UINT32(1);
    uint32 blknum = PG_GETARG_UINT32(2);
    uint32 real_blknum;
    RangeVar* relrv = NULL;
    Relation rel;
    StringInfo output = makeStringInfo();

    super_user();
    relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    rel = relation_openrv(relrv, AccessShareLock);
    check(rel);

    // get block number, then check start blkno and input blknum
    //
    real_blknum = RelationGetNumberOfBlocks(rel);
    if (blkno >= real_blknum) {
        appendStringInfo(output, "start blkno %u >= real block number %u \n", blkno, real_blknum);
    } else {
        if (blkno + blknum > real_blknum) {
            blknum = real_blknum - blkno;
        }

        appendStringInfo(output,
            "relfinenode (space=%u, db=%u, rel=%u) \n",
            rel->rd_node.spcNode,
            rel->rd_node.dbNode,
            rel->rd_node.relNode);

        // parse and output the compression metadata
        //
        for (int i = 0; i < blknum; ++i) {
            char* raw_page = read_raw_page(rel, MAIN_FORKNUM, blkno + i);
            appendStringInfo(output, "Block #%d \n", blkno + i);
            parse_compress_meta(output, raw_page, rel);
            appendStringInfo(output, "\n");
            pfree(raw_page);
        }
    }

    relation_close(rel, AccessShareLock);

    bytea* dumpVal = (bytea*)palloc(VARHDRSZ + output->len);
    SET_VARSIZE(dumpVal, VARHDRSZ + output->len);
    errno_t rc = memcpy_s(VARDATA(dumpVal), output->len, output->data, output->len);
    securec_check_c(rc, "\0", "\0");
    pfree(output->data);
    pfree(output);

    PG_RETURN_TEXT_P(dumpVal);
}

PG_FUNCTION_INFO_V1(page_compress_meta_usage);

Datum page_compress_meta_usage(PG_FUNCTION_ARGS)
{
    char* help = "usage: page_compress_meta name blkno blknum \n\n"
                 "\tname, relation/table name, only for heap \n"
                 "\tblkno, the start blockno \n"
                 "\tblknum, how many blocks to parse \n";
    int help_size = strlen(help) + 1;

    bytea* dumpVal = (bytea*)palloc(VARHDRSZ + help_size);
    SET_VARSIZE(dumpVal, VARHDRSZ + help_size);
    errno_t rc = memcpy_s(VARDATA(dumpVal), help_size, help, help_size);
    securec_check_c(rc, "\0", "\0");

    PG_RETURN_TEXT_P(dumpVal);
}

static void super_user(void)
{
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to use raw functions"))));
    return;
}

static void check(Relation rel)
{
    /* Check that this relation has storage */
    if (rel->rd_rel->relkind == RELKIND_VIEW)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from view \"%s\"", RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_CONTQUERY)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from contview \"%s\"", RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from composite type \"%s\"", RelationGetRelationName(rel))));
    if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from foreign table \"%s\"", RelationGetRelationName(rel))));

    if (rel->rd_rel->relkind == RELKIND_STREAM)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot get raw page from stream \"%s\"", RelationGetRelationName(rel))));

    /*
     * Reject attempts to read non-local temporary relations; we would be
     * likely to get wrong data since we have no visibility into the owning
     * session's local buffers.
     */
    if (RELATION_IS_OTHER_TEMP(rel))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot access temporary tables of other sessions")));
    return;
}

static const char HexCharMaps[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

#define Byte2HexChar(_buff, _byte)              \
    do {                                        \
        (_buff)[0] = HexCharMaps[(_byte) >> 4]; \
        (_buff)[1] = HexCharMaps[(_byte)&0x0F]; \
    } while (0)

static void formatBytes(StringInfo buf, char* start, int len)
{
    int cnt;
    char byteBuf[4] = {0, 0, ' ', '\0'};
    unsigned char ch;

    for (cnt = 0; cnt < len; cnt++) {
        ch = (unsigned char)*start;
        start++;

        Byte2HexChar(byteBuf, ch);
        appendStringInfoString(buf, byteBuf);
    }
}

static char* read_raw_page(Relation rel, ForkNumber forknum, BlockNumber blkno)
{
    char* raw_page = NULL;
    Buffer buf;

    if (blkno >= RelationGetNumberOfBlocks(rel)) {
        elog(ERROR, "block number %u is out of range for relation \"%s\"", blkno, RelationGetRelationName(rel));
        return NULL;
    }

    /* Initialize buffer to copy to */
    raw_page = (char*)palloc(BLCKSZ);
    buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    errno_t rc = memcpy_s(raw_page, BLCKSZ, BufferGetPage(buf), BLCKSZ);
    securec_check_c(rc, "\0", "\0");
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buf);

    return raw_page;
}

static void parse_compress_meta(StringInfo outputBuf, char* page_content, Relation rel)
{
    PageHeader page_header = (PageHeader)page_content;

    if (!PageIsCompressed(page_header)) {
        appendStringInfo(outputBuf, "\t This page is not compressed \n");
        return;
    }

    char* start = page_content + page_header->pd_special;
    char* current = start;
    int size = PageGetSpecialSize(page_header);

    TupleDesc desc = RelationGetDescr(rel);
    Form_pg_attribute* att = desc->attrs;
    int attrno;
    int attrnum = desc->natts;

    int cmprsOff = 0;
    void* metaInfo = NULL;
    char mode = 0;

    for (attrno = 0; attrno < attrnum && cmprsOff < size; ++attrno) {
        Form_pg_attribute thisatt = att[attrno];
        int metaSize = 0;

        metaInfo = PageCompress::FetchAttrCmprMeta(start + cmprsOff, thisatt->attlen, &metaSize, &mode);
        switch (mode) {
            case CMPR_DELTA: {
                DeltaCmprMeta* deltaInfo = (DeltaCmprMeta*)metaInfo;
                appendStringInfo(outputBuf, "\t Col #%d: Delta, attr-len %d, min-val ", attrno, deltaInfo->bytes);

                int min_val_start = cmprsOff + sizeof(mode) + sizeof(unsigned char);
                formatBytes(outputBuf, (start + min_val_start), thisatt->attlen);
                appendStringInfo(outputBuf, "\n");
                break;
            }

            case CMPR_DICT: {
                DictCmprMeta* dictMeta = (DictCmprMeta*)metaInfo;
                appendStringInfo(outputBuf, "\t Col #%d: dictionary, items %d \n", attrno, dictMeta->dictItemNum);

                for (int i = 0; i < dictMeta->dictItemNum; ++i) {
                    DictItemData* item = dictMeta->dictItems + i;
                    appendStringInfo(outputBuf, "\t\t Item #%d, len %d, data ", i, item->itemSize);
                    formatBytes(outputBuf, item->itemData, item->itemSize);
                    appendStringInfo(outputBuf, "\n");
                }

                break;
            }

            case CMPR_PREFIX: {
                PrefixCmprMeta* prefixMeta = (PrefixCmprMeta*)metaInfo;
                appendStringInfo(outputBuf, "\t Col #%d: prefix, len %d, data ", attrno, prefixMeta->len);
                formatBytes(outputBuf, prefixMeta->prefixStr, prefixMeta->len);
                appendStringInfo(outputBuf, "\n");

                break;
            }

            case CMPR_NUMSTR: {
                appendStringInfo(outputBuf, "\t Col #%d: number string compression \n", attrno);
                break;
            }

            case CMPR_NONE: {
                appendStringInfo(outputBuf, "\t Col #%d: none compression \n", attrno);
                break;
            }
        }
        cmprsOff += metaSize;
    }
}

/*
 * page_checksum
 *
 * Compute checksum of a raw page
 */

PG_FUNCTION_INFO_V1(page_checksum);

Datum page_checksum(PG_FUNCTION_ARGS)
{
    bytea* raw_page = PG_GETARG_BYTEA_P(0);
    uint32 blkno = PG_GETARG_INT32(1);
    int raw_page_size;
    PageHeader page;

    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be superuser to use raw page functions"))));

    raw_page_size = VARSIZE(raw_page) - VARHDRSZ;

    /*
     * Check that the supplied page is of the right size.
     */
    if (raw_page_size != BLCKSZ)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("incorrect size of input page (%d bytes)", raw_page_size)));

    page = (PageHeader)VARDATA(raw_page);

    PG_RETURN_INT16(pg_checksum_page((char*)page, blkno));
}
