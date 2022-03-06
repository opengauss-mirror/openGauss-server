/* -------------------------------------------------------------------------
 *
 * copy.cpp
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/copy.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <arpa/inet.h>
#include <fnmatch.h>
#include <libgen.h> 
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "access/hbucket_am.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/tupdesc.h"
#include "access/xlog.h"
#include "bulkload/dist_fdw.h"
#include "catalog/heap.h"
#include "catalog/pgxc_class.h"
#include "bulkload/dist_fdw.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#ifdef PGXC
#include "catalog/pg_trigger.h"
#endif
#include "catalog/storage_gtt.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "commands/copypartition.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/spi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "parser/parse_relation.h"
#ifdef PGXC
#include "optimizer/pgxcship.h"
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/locator.h"
#include "pgxc/remotecopy.h"
#include "nodes/nodes.h"
#include "foreign/fdwapi.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#endif
#include "replication/dataqueue.h"
#include "rewrite/rewriteHandler.h"
#include "storage/smgr/fd.h"
#include "storage/pagecompress.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "catalog/pg_partition_fn.h"
#include "pgaudit.h"
#include "auditfuncs.h"
#include "bulkload/utils.h"
#include "commands/copypartition.h"
#include "access/cstore_insert.h"
#include "access/dfs/dfs_insert.h"
#include "commands/copy.h"
#include "parser/parser.h"
#include "catalog/pg_attrdef.h"
#include "commands/formatter.h"
#include "bulkload/roach_adpter.h"
#include "storage/lmgr.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/plog.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/sec_rls_utils.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "parser/parsetree.h"
#include "utils/partitionmap_gs.h"
#include "access/tableam.h"
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"
#include "gs_ledger/blockchain.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/storage/ts_store_insert.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "access/ustore/knl_uscan.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_whitebox_test.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')
#define MAX_INT32 2147483600
#define DELIM_MAX_LEN 10
#define EOL_MAX_LEN 10
#define INVALID_DELIM_CHAR "\\.abcdefghijklmnopqrstuvwxyz0123456789"
#define INVALID_EOL_CHAR ".abcdefghijklmnopqrstuvwxyz0123456789"
#define MAX_HEADER_SIZE (1024 * 1024)
#define MAX_WRITE_BLKSIZE (1024 * 1024)
#define COPY_ERROR_TABLE_SCHEMA "public"
#define COPY_ERROR_TABLE "pgxc_copy_error_log"

#define COPY_ERROR_TABLE_NUM_COL 6

static const Oid copy_error_table_col_typid[COPY_ERROR_TABLE_NUM_COL] = {
    VARCHAROID, TIMESTAMPTZOID, VARCHAROID, INT8OID, TEXTOID, TEXTOID};

const char* COPY_SUMMARY_TABLE_SCHEMA = "public";
const char* COPY_SUMMARY_TABLE = "gs_copy_summary";
static const int COPY_SUMMARY_TABLE_NUM_COL = 12;
static Oid copy_summary_table_col_typid[COPY_SUMMARY_TABLE_NUM_COL] = {
    VARCHAROID, TIMESTAMPTZOID, TIMESTAMPTZOID, INT8OID, INT8OID, INT8OID,
    INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, TEXTOID};

/*
 * Note that similar macros also exist in executor/execMain.c.  There does not
 * appear to be any good header to put them into, given the structures that
 * they use, so we let them be duplicated.  Be sure to update all if one needs
 * to be changed, however.
 */
#define GetInsertedColumns(relinfo, estate) \
    (rt_fetch((relinfo)->ri_RangeTableIndex, (estate)->es_range_table)->insertedCols)
#define GetUpdatedColumns(relinfo, estate) \
    (rt_fetch((relinfo)->ri_RangeTableIndex, (estate)->es_range_table)->updatedCols)

/*
 * lengthof
 *		Number of elements in an array.
 */
#define lengthof(array) (sizeof(array) / sizeof((array)[0]))

extern void EnableDoingCommandRead();
extern void DisableDoingCommandRead();
extern void getOBSOptions(ObsCopyOptions* obs_copy_options, List* options);
extern int32 get_relation_data_width(Oid relid, Oid partitionid, int32* attr_widths, bool vectorized = false);

/* DestReceiver for COPY (SELECT) TO */
typedef struct {
    DestReceiver pub; /* publicly-known function pointers */
    CopyState cstate; /* CopyStateData for the command */
    uint64 processed; /* # of tuples processed */
} DR_copy;

/*
 * These macros centralize code used to process line_buf and raw_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.	We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen)               \
    if (1) {                                                        \
        if (raw_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) { \
            raw_buf_ptr = prev_raw_ptr; /* undo fetch */            \
            need_data = true;                                       \
            continue;                                               \
        }                                                           \
    } else                                                          \
        ((void)0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen)                                  \
    if (1) {                                                                    \
        if (raw_buf_ptr + (extralen) >= copy_buf_len && hit_eof) {              \
            if (extralen)                                                       \
                raw_buf_ptr = copy_buf_len; /* consume the partial character */ \
            /* backslash just before EOF, treat as data char */                 \
            result = true;                                                      \
            break;                                                              \
        }                                                                       \
    } else                                                                      \
        ((void)0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in raw_buf.
 */
#define REFILL_LINEBUF                                                                                            \
    if (1) {                                                                                                      \
        if (raw_buf_ptr > cstate->raw_buf_index) {                                                                \
            appendBinaryStringInfo(                                                                               \
                &cstate->line_buf, cstate->raw_buf + cstate->raw_buf_index, raw_buf_ptr - cstate->raw_buf_index); \
            cstate->raw_buf_index = raw_buf_ptr;                                                                  \
        }                                                                                                         \
    } else                                                                                                        \
        ((void)0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO             \
    if (1) {                            \
        raw_buf_ptr = prev_raw_ptr + 1; \
        goto not_end_of_copy;           \
    } else                              \
        ((void)0)

static const char BinarySignature[15] = "PGCOPY\n\377\r\n\0";

/* non-export function prototypes */
static CopyState BeginCopy(bool is_from, Relation rel, Node* raw_query, const char* queryString, List* attnamelist,
    List* options, bool is_copy = true);
static void EndCopy(CopyState cstate);
CopyState BeginCopyTo(
    Relation rel, Node* query, const char* queryString, const char* filename, List* attnamelist, List* options);
void EndCopyTo(CopyState cstate);
uint64 DoCopyTo(CopyState cstate);
static uint64 CopyTo(CopyState cstate, bool isFirst, bool isLast);
static uint64 CopyToCompatiblePartions(CopyState cstate);
void CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum* values, const bool* nulls);
static uint64 CopyFrom(CopyState cstate);
static void EstCopyMemInfo(Relation rel, UtilityDesc* desc);

static int CopyFromCompressAndInsertBatch(PageCompress* pcState, EState* estate, CommandId mycid, int hi_options,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    HeapTuple* bufferedTuples, bool isLast, Relation heapRelation, Partition partition, int2 bucketId);

CopyFromManager initCopyFromManager(MemoryContext parent, Relation heapRel, bool isInsertSelect = false);
void deinitCopyFromManager(CopyFromManager mgr);
static void newMemCxt(CopyFromManager mgr, int num);
static CopyFromMemCxt findMemCxt(CopyFromManager mgr, bool* found);
CopyFromBulk findBulk(CopyFromManager mgr, Oid partOid, int2 bucketId, bool* toFlush);

static void exportWriteHeaderLine(CopyState cstate);
static void exportInitOutBuffer(CopyState cstate);
template <bool skipEOL>
static void exportAppendOutBuffer(CopyState cstate);
static void exportDeinitOutBuffer(CopyState cstate);
static void RemoteExportWriteOut(CopyState cstate);
static void RemoteExportFlushData(CopyState cstate);

static bool CopyReadLine(CopyState cstate);
static bool CopyReadLineText(CopyState cstate);
static void bulkload_set_readattrs_func(CopyState cstate);
static void bulkload_init_time_format(CopyState cstate);
static Datum CopyReadBinaryAttribute(
    CopyState cstate, int column_no, FmgrInfo* flinfo, Oid typioparam, int32 typmod, bool* isnull);
static void CopyAttributeOutText(CopyState cstate, char* string);
static void CopyAttributeOutCSV(CopyState cstate, char* string, bool use_quote, bool single_attr);
List* CopyGetAttnums(TupleDesc tupDesc, Relation rel, List* attnamelist);
List* CopyGetAllAttnums(TupleDesc tupDesc, Relation rel);

/* Low-level communications functions */
static void SendCopyBegin(CopyState cstate);
static void ReceiveCopyBegin(CopyState cstate);
static void SendCopyEnd(CopyState cstate);
static void CopySendData(CopyState cstate, const void* databuf, int datasize);
template <bool skipEol>
void CopySendEndOfRow(CopyState cstate);
static int CopyGetData(CopyState cstate, void* databuf, int minread, int maxread);
static int CopyGetDataDefault(CopyState cstate, void* databuf, int minread, int maxread);
static void CopySendInt32(CopyState cstate, int32 val);
static bool CopyGetInt32(CopyState cstate, int32* val);
static void CopySendInt16(CopyState cstate, int16 val);
static bool CopyGetInt16(CopyState cstate, int16* val);
static void InitCopyMemArg(CopyState cstate, MemInfoArg* CopyMem);
static bool CheckCopyFileInBlackList(const char* filename);
static char* FindFileName(const char* path);
static void TransformColExpr(CopyState cstate);
static void SetColInFunction(CopyState cstate, int attrno, const TypeName* type);
static ExprState* ExecInitCopyColExpr(CopyState cstate, int attrno, Oid attroid, int attrmod, Node *expr);

#ifdef PGXC
static RemoteCopyOptions* GetRemoteCopyOptions(CopyState cstate);
static void pgxc_datanode_copybegin(RemoteCopyData* remoteCopyState, AdaptMem* memInfo);
static void append_defvals(Datum* values, CopyState cstate);
#endif

static void serialize_begin(CopyState cstate, StringInfo buf, const char* filename, int ref);
static void ExecTransColExpr(CopyState cstate, ExprContext* econtext, int numPhysAttrs, Datum* values, bool* nulls);

List* getDataNodeTask(List* totalTask, const char* dnName);
List* getPrivateModeDataNodeTask(List* urllist, const char* dnName);

bool getNextCopySegment(CopyState cstate);
template <bool import>
bool getNextGDS(CopyState cstate);
void bulkloadFuncFactory(CopyState cstate);
static bool CopyGetNextLineFromGDS(CopyState cstate);
int HandleGDSCmd(CopyState cstate, StringInfo buf);
void SetFixedAlignment(TupleDesc tupDesc, Relation rel, FixFormatter* formatter, const char* char_alignment);
extern int ReadAttributesFixedWith(CopyState cstate);
extern void FixedRowOut(CopyState cstate, Datum* values, const bool* nulls);
extern char* ExecBuildSlotValueDescription(
    Oid reloid, TupleTableSlot* slot, TupleDesc tupdesc, Bitmapset* modifiedCols, int maxfieldlen);
extern void VerifyFixedFormatter(TupleDesc tupDesc, FixFormatter* formatter);
extern void PrintFixedHeader(CopyState cstate);
void CheckIfGDSReallyEnds(CopyState cstate, GDSStream* stream);
void ProcessCopyErrorLogOptions(CopyState cstate, bool isRejectLimitSpecified);
void Log_copy_error_spi(CopyState cstate);
static void LogCopyErrorLogBulk(CopyState cstate);
static void Log_copy_summary_spi(CopyState cstate);
extern bool TrySaveImportError(CopyState cstate);
static void FormAndSaveImportWhenLog(CopyState cstate, Relation errRel, Datum begintime, CopyErrorLogger *elogger);
static void CheckCopyWhenList(List *when_list);
static bool IfCopyLineMatchWhenListPosition(CopyState cstate);
static bool IfCopyLineMatchWhenListField(CopyState cstate);
static void CopyGetWhenListAttFieldno(CopyState cstate, List *attnamelist);
static int CopyGetColumnListIndex(CopyState cstate, List *attnamelist, const char* colname);

const char *gds_protocol_version_gaussdb = "1.0";
/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void SendCopyBegin(CopyState cstate)
{
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
        /* new way */
        StringInfoData buf;
        int natts = list_length(cstate->attnumlist);
        int16 format = (IS_BINARY(cstate) ? 1 : 0);

        pq_beginmessage(&buf, 'H');
        pq_sendbyte(&buf, format); /* overall format */
        pq_sendint16(&buf, natts);

        TupleDesc	tupDesc;
        Form_pg_attribute *attr = NULL;
        ListCell *cur = NULL;
        if (cstate->rel)
            tupDesc = RelationGetDescr(cstate->rel);
        else
            tupDesc = cstate->queryDesc->tupDesc;
        attr = tupDesc->attrs;
        foreach(cur, cstate->attnumlist)
        {
            int16 fmt = format;
            int attnum = lfirst_int(cur);
            if (attr[attnum - 1]->atttypid == BYTEAWITHOUTORDERWITHEQUALCOLOID ||
                attr[attnum - 1]->atttypid == BYTEAWITHOUTORDERCOLOID)
                fmt = (int16)attr[attnum - 1]->atttypmod;
            pq_sendint16(&buf, fmt); /* per-column formats */
        }
        pq_endmessage(&buf);
        cstate->copy_dest = COPY_NEW_FE;
    } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2) {
        /* old way */
        if (IS_BINARY(cstate))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("COPY BINARY is not supported to stdout or from stdin")));
        pq_putemptymessage('H');
        /* grottiness needed for old COPY OUT protocol */
        pq_startcopyout();
        cstate->copy_dest = COPY_OLD_FE;
    } else {
        /* very old way */
        if (IS_BINARY(cstate))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("COPY BINARY is not supported to stdout or from stdin")));
        pq_putemptymessage('B');
        /* grottiness needed for old COPY OUT protocol */
        pq_startcopyout();
        cstate->copy_dest = COPY_OLD_FE;
    }
}

static void ReceiveCopyBegin(CopyState cstate)
{
    if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
        /* new way */
        StringInfoData buf;
        int natts = list_length(cstate->attnumlist);
        int16 format = (IS_BINARY(cstate) ? 1 : 0);
        int i;

        pq_beginmessage(&buf, 'G');
        pq_sendbyte(&buf, format); /* overall format */
        pq_sendint16(&buf, natts);
        for (i = 0; i < natts; i++)
            pq_sendint16(&buf, format); /* per-column formats */
        pq_endmessage(&buf);
        cstate->copy_dest = COPY_NEW_FE;
        cstate->fe_msgbuf = makeStringInfo();
    } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2) {
        /* old way */
        if (IS_BINARY(cstate))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("COPY BINARY is not supported to stdout or from stdin")));
        pq_putemptymessage('G');
        cstate->copy_dest = COPY_OLD_FE;
    } else {
        /* very old way */
        if (IS_BINARY(cstate))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("COPY BINARY is not supported to stdout or from stdin")));
        pq_putemptymessage('D');
        cstate->copy_dest = COPY_OLD_FE;
    }
    /* We *must* flush here to ensure FE knows it can send. */
    pq_flush();
}

static void SendCopyEnd(CopyState cstate)
{
    if (cstate->copy_dest == COPY_NEW_FE) {
        /* Shouldn't have any unsent data */
        Assert(cstate->fe_msgbuf->len == 0);
        /* Send Copy Done message */
        pq_putemptymessage('c');
    } else {
        CopySendData(cstate, "\\.", 2);
        /* Need to flush out the trailer (this also appends a newline) */
        CopySendEndOfRow<false>(cstate);
        pq_endcopyout(false);
    }
}

/* ----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 * ----------
 */
static void CopySendData(CopyState cstate, const void* databuf, int datasize)
{
    appendBinaryStringInfo(cstate->fe_msgbuf, (const char*)databuf, datasize);
}

void CopySendString(CopyState cstate, const char* str)
{
    appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

void CopySendChar(CopyState cstate, char c)
{
    appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

static void SetEndOfLineType(CopyState cstate)
{
    Assert(cstate != NULL);

    if (cstate->eol_type == EOL_NL)
        CopySendChar(cstate, '\n');
    else if (cstate->eol_type == EOL_CR)
        CopySendChar(cstate, '\r');
    else if (cstate->eol_type == EOL_CRNL)
        CopySendString(cstate, "\r\n");
    else if (cstate->eol_type == EOL_UD)
        CopySendString(cstate, cstate->eol);
    else if (cstate->eol_type == EOL_UNKNOWN)
        CopySendChar(cstate, '\n');
}

template <bool skipEol>
void CopySendEndOfRow(CopyState cstate)
{
    StringInfo fe_msgbuf = cstate->fe_msgbuf;
    int ret = 0;

    switch (cstate->copy_dest) {
        case COPY_FILE:
            if (!IS_BINARY(cstate) && !skipEol) {
                /* Default line termination depends on platform */
                SetEndOfLineType(cstate);
            }

            {
                PROFILING_MDIO_START();
                PGSTAT_INIT_TIME_RECORD();
                PGSTAT_START_TIME_RECORD();
                ret = fwrite(fe_msgbuf->data, fe_msgbuf->len, 1, cstate->copy_file);
                PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
                PROFILING_MDIO_END_WRITE(fe_msgbuf->len, (fe_msgbuf->len * ret));

                if ((ret != 1) || ferror(cstate->copy_file))
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to COPY file: %m")));
            }
            break;
        case COPY_OLD_FE:
            /* The FE/BE protocol uses \n as newline for all platforms */
            if (!IS_BINARY(cstate))
                CopySendChar(cstate, '\n');

            if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len)) {
                /* no hope of recovering connection sync, so FATAL */
                ereport(FATAL, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("connection lost during COPY to stdout")));
            }
            break;
        case COPY_NEW_FE:
            if (!IS_BINARY(cstate) && !skipEol) {
                /* Default line termination depends on platform */
                SetEndOfLineType(cstate);
            }

            /* Dump the accumulated row as one CopyData message */
            (void)pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
            break;
#ifdef PGXC
        case COPY_BUFFER:
            /* Do not send yet anywhere, just return */
            return;
        case COPY_FILE_SEGMENT:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("COPY_FILE_SEGMENT does not implement in CopySendEndOfRow")));
            return;
        case COPY_GDS:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY_GDS does not implement in CopySendEndOfRow")));
            return;
        case COPY_OBS:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY_OBS does not implement in CopySendEndOfRow")));
            return;
        case COPY_ROACH:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY_ROACH does not implement in CopySendEndOfRow")));
            return;
#endif
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unimplemented CopyDest mode")));
            return;
    }

    resetStringInfo(fe_msgbuf);
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.	The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 */
static int CopyGetData(CopyState cstate, void* databuf, int minread, int maxread)
{
    Assert(cstate->copyGetDataFunc);
    /*
     * Now DoingCommandRead is always false during copyFrom, we need enable it.
     * Allow asynchronous signals to be executed immediately if they
     * come in while we are waiting for client input. (This must be
     * conditional since we don't want, say, reads on behalf of COPY FROM
     * STDIN doing the same thing.)
     */
    int ret = 0;
    EnableDoingCommandRead();
    ret = cstate->copyGetDataFunc(cstate, databuf, minread, maxread);
    DisableDoingCommandRead();

    return ret;
}

/* the default implement of CopyGetData
 * the cstate->copyGetDataFunc is initialized in the  BeginCopy by CopyGetDataDefault
 */
static int CopyGetDataDefault(CopyState cstate, void* databuf, int minread, int maxread)
{
    int bytesread = 0;

    if (minread > maxread) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("could not get data from COPY source")));
    }

    switch (cstate->copy_dest) {
        case COPY_FILE: {
            PROFILING_MDIO_START();
            bytesread = fread(databuf, 1, maxread, cstate->copy_file);
            PROFILING_MDIO_END_READ(maxread, bytesread);
            if (ferror(cstate->copy_file))
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from COPY file: %m")));
            break;
        }
        case COPY_OLD_FE:

            /*
             * We cannot read more than minread bytes (which in practice is 1)
             * because old protocol doesn't have any clear way of separating
             * the COPY stream from following data.  This is slow, but not any
             * slower than the code path was originally, and we don't care
             * much anymore about the performance of old protocol.
             */
            if (pq_getbytes((char*)databuf, minread)) {
                /* Only a \. terminator is legal EOF in old protocol */
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                        errmsg("unexpected EOF on client connection with an open transaction")));
            }
            bytesread = minread;
            break;
        case COPY_NEW_FE:
            while (maxread > 0 && bytesread < minread && !cstate->fe_eof) {
                int avail;

                while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len) {
                    /* Try to receive another message */
                    int mtype;

                readmessage:
                    mtype = pq_getbyte();
                    if (mtype == EOF)
                        ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                                errmsg("unexpected EOF on client connection with an open transaction")));

                    /*
                     * Check the validity for message flag.
                     */
                    switch (mtype) {
                        case 'd': /* CopyData */
                        case 'c': /* CopyDone */
                        case 'f': /* CopyFail */
                        case 'H': /* Flush */
                        case 'S': /* Sync */
                            break;
                        case 'X': /* close connection */
                            /* receive close conn type */
                            ereport(ERROR,
                                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("copy from stdin failed because receive close conn message type 0x%02X",
                                        mtype)));
                            break;
                        default:
                            ereport(ERROR,
                                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("unexpected message type 0x%02X during COPY from stdin", mtype)));
                            break;
                    }

                    if (pq_getmessage(cstate->fe_msgbuf, 0))
                        ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_FAILURE),
                                errmsg("unexpected EOF on client connection with an open transaction")));

                    switch (mtype) {
                        case 'd': /* CopyData */
                            break;
                        case 'c': /* CopyDone */
                            /* COPY IN correctly terminated by frontend */
                            cstate->fe_eof = true;
                            return bytesread;
                        case 'f': /* CopyFail */
                            ereport(ERROR,
                                (errcode(ERRCODE_QUERY_CANCELED),
                                    errmsg("COPY from stdin failed: %s", pq_getmsgstring(cstate->fe_msgbuf))));
                            break;
                        case 'H': /* Flush */
                        case 'S': /* Sync */

                            /*
                             * Ignore Flush/Sync for the convenience of client
                             * libraries (such as libpq) that may send those
                             * without noticing that the command they just
                             * sent was COPY.
                             */
                            goto readmessage;
                        case 'X': /* close connection */
                                  /* receive close conn type */
                            ereport(ERROR,
                                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("copy from stdin failed because receive close conn message type 0x%02X",
                                        mtype)));
                            break;
                        default:
                            ereport(ERROR,
                                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("unexpected message type 0x%02X during COPY from stdin", mtype)));
                            break;
                    }
                }
                avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
                if (avail > maxread)
                    avail = maxread;
                pq_copymsgbytes(cstate->fe_msgbuf, (char*)databuf, avail);
                databuf = (void*)((char*)databuf + avail);
                maxread -= avail;
                bytesread += avail;
            }
            break;
#ifdef PGXC
        case COPY_BUFFER:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY_BUFFER not allowed in this context")));
            break;
#endif
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unimplemented CopyDest mode")));
            break;
    }

    return bytesread;
}

/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static void CopySendInt32(CopyState cstate, int32 val)
{
    uint32 buf;

    buf = htonl((uint32)val);
    CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static bool CopyGetInt32(CopyState cstate, int32* val)
{
    uint32 buf;

    if (CopyGetData(cstate, &buf, sizeof(buf), sizeof(buf)) != sizeof(buf)) {
        *val = 0; /* suppress compiler warning */
        return false;
    }
    *val = (int32)ntohl(buf);
    return true;
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static void CopySendInt16(CopyState cstate, int16 val)
{
    uint16 buf;

    buf = htons((uint16)val);
    CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static bool CopyGetInt16(CopyState cstate, int16* val)
{
    uint16 buf;

    if (CopyGetData(cstate, &buf, sizeof(buf), sizeof(buf)) != sizeof(buf)) {
        *val = 0; /* suppress compiler warning */
        return false;
    }
    *val = (int16)ntohs(buf);
    return true;
}

/*
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns TRUE if able to obtain at least one more byte, else FALSE.
 *
 * If raw_buf_index < raw_buf_len, the unprocessed bytes are transferred
 * down to the start of the buffer and then we load more data after that.
 * This case is used only when a frontend multibyte character crosses a
 * bufferload boundary.
 */
bool CopyLoadRawBuf(CopyState cstate)
{
    int nbytes;
    int inbytes;
    errno_t ret = EOK;

    if (cstate->raw_buf_index < cstate->raw_buf_len) {
        /* Copy down the unprocessed data */
        nbytes = cstate->raw_buf_len - cstate->raw_buf_index;
        ret = memmove_s(cstate->raw_buf, RAW_BUF_SIZE, cstate->raw_buf + cstate->raw_buf_index, nbytes);
        securec_check(ret, "", "");
    } else
        nbytes = 0; /* no data need be saved */

    inbytes = CopyGetData(cstate, cstate->raw_buf + nbytes, 1, RAW_BUF_SIZE - nbytes);
    nbytes += inbytes;
    cstate->raw_buf[nbytes] = '\0';
    cstate->raw_buf_index = 0;
    cstate->raw_buf_len = nbytes;
    return (inbytes > 0);
}

static void CopyToFileSecurityChecks(const char* filename)
{
    char* pathToCheck = pstrdup(filename);
    char* token = pathToCheck;

    while (*token != '\0' && *(token + 1) != '\0') {
        if (*token == '/' && *(token + 1) == '.')
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("Cannot use OS-reserved file as COPY destination.")));
        token++;
    }

    if (pathToCheck != NULL) {
        pfree_ext(pathToCheck);
        pathToCheck = NULL;
    }
}

/**
 * @Description: synchronize bulkload states with the current copy state.
 * @in cstate: the current bulkload CopyState
 * @return: void
 */
void SyncBulkloadStates(CopyState cstate)
{
#ifdef PGXC

    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("the bulkload compatible illegal chars flag is already set before bulkload starts")));
    } else {
        u_sess->cmd_cxt.bulkload_compatible_illegal_chars = cstate->compatible_illegal_chars;
    }

#endif
}

/**
 * @Description: when the bulkload ends(fail or succeed) the bulkload states need be cleaned.
 * @return: void
 */
void CleanBulkloadStates()
{
#ifdef PGXC

    /*
     * the following bulkload states just need reset,
     * and no allocated memory is to be freed.
     */
    u_sess->cmd_cxt.bulkload_compatible_illegal_chars = false;

#endif
}

/*
 * Check permission for copy between tables and files.
 */
static void CheckCopyFilePermission()
{
    if (u_sess->attr.attr_storage.enable_copy_server_files) {
        /* Only allow file copy to users with sysadmin privilege or the member of gs_role_copy_files role. */
        if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_COPY_FILES)) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin or a member of the gs_role_copy_files role to COPY to or from a file"),
                    errhint("Anyone can COPY to stdout or from stdin. "
                        "gsql's \\copy command also works for anyone.")));
        }
    } else {
        /*
         * Disallow file copy when enable_copy_server_files is set to false.
         * NOTICE : system admin has copy permissions, which might cause security risks
         * when privileges separation is used, e.g., config/audit file might be modified.
         * Recommend setting enable_copy_server_files to false in such case.
         */
        if (!initialuser()) {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("COPY to or from a file is prohibited for security concerns"),
                    errhint("Anyone can COPY to stdout or from stdin. "
                        "gsql's \\copy command also works for anyone.")));
        }
    }
}

/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = TRUE means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.	Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a openGauss user without sysadmin privilege or the member of gs_role_copy_files role
 * to read from or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
uint64 DoCopy(CopyStmt* stmt, const char* queryString)
{
    CopyState cstate;
    bool is_from = stmt->is_from;
    bool pipe = (stmt->filename == NULL);
    Relation rel;
    uint64 processed = 0;
    RangeTblEntry* rte = NULL;
    Node* query = NULL;
    stmt->hashstate.has_histhash = false;
    /*
     * we can't retry gsql \copy from command and JDBC 'CopyManager.copyIn' operation,
     * since these kinds of copy from fetch data from client, so we disallow CN Retry here.
     */
    if (is_from && pipe) {
        StmtRetrySetQuerytUnsupportedFlag();
    }

    WHITEBOX_TEST_STUB("DoCopy_Begin", WhiteboxDefaultErrorEmit);

    /* Disallow copy to/from files except to users with appropriate permission. */
    if (!pipe) {
        CheckCopyFilePermission();
    }

    if (stmt->relation) {
        TupleDesc tupDesc;
        AclMode required_access = (is_from ? ACL_INSERT : ACL_SELECT);
        List* attnums = NIL;
        ListCell* cur = NULL;

        Assert(!stmt->query);

        /* Open and lock the relation, using the appropriate lock type. */
        rel = heap_openrv(stmt->relation, (is_from ? RowExclusiveLock : AccessShareLock));

        TrForbidAccessRbObject(RelationRelationId, RelationGetRelid(rel), stmt->relation->relname);

        if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
        } else if (rel != NULL && rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !validateTempNamespace(rel->rd_rel->relnamespace)) {
            heap_close(rel, (is_from ? RowExclusiveLock : AccessShareLock));
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                    errmsg("Temp table's data is invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.", 
                       g_instance.attr.attr_common.PGXCNodeName)));
        }

        /* Table of COPY FROM can not be blockchain related table */
        if (is_from && rel != NULL) {
            Oid relid = RelationGetRelid(rel);
            Oid nspid = RelationGetNamespace(rel);
            if (nspid == PG_BLOCKCHAIN_NAMESPACE || relid == GsGlobalChainRelationId) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Table %s of COPY FROM can not be blockchain related table.", rel->rd_rel->relname.data)));
            }
        }

        /* @Online expansion: check if the table is in redistribution read only mode */
        if (rel != NULL && is_from && RelationInClusterResizingWriteErrorMode(rel))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("%s is redistributing, please retry later.", rel->rd_rel->relname.data)));

        rte = makeNode(RangeTblEntry);
        rte->rtekind = RTE_RELATION;
        rte->relid = RelationGetRelid(rel);
        rte->relkind = rel->rd_rel->relkind;
        rte->requiredPerms = required_access;

#ifdef PGXC
        /* In case COPY is used on a temporary table, never use 2PC for implicit commits */
        if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
            ExecSetTempObjectIncluded();
#endif

        tupDesc = RelationGetDescr(rel);
        attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist);
        foreach (cur, attnums) {
            int attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

            if (is_from)
                rte->insertedCols = bms_add_member(rte->insertedCols, attno);
            else
                rte->selectedCols = bms_add_member(rte->selectedCols, attno);
        }
        (void)ExecCheckRTPerms(list_make1(rte), true);

        /* Check this is COPY TO and table is influenced by RLS */
        if ((is_from == false) && (CheckEnableRlsPolicies(rel, GetUserId()) == RLS_ENABLED)) {
            query = (Node*)MakeRlsSelectStmtForCopyTo(rel, stmt);
            /*
             * Keep the lock on it to prevent changes between now and when
             * start the query-based COPY. Relation will be reopened later
             * as part of the query-based COPY.
             */
            relation_close(rel, NoLock);
            rel = NULL;
        }
    } else {
        Assert(stmt->query);
        query = stmt->query;
        rel = NULL;
    }

    if (is_from) {
        Assert(rel);

        /* check read-only transaction */
        if (u_sess->attr.attr_common.XactReadOnly && !RELATION_IS_TEMP(rel))
            PreventCommandIfReadOnly("COPY FROM");

        /* set write for backend status for the thread, we will use it to check default transaction readOnly */
        pgstat_set_stmt_tag(STMTTAG_WRITE);

        cstate = BeginCopyFrom(rel, stmt->filename, stmt->attlist, stmt->options, &stmt->memUsage, queryString);
        cstate->range_table = list_make1(rte);

        /* Assign mem usage in datanode */
        if (stmt->memUsage.work_mem > 0) {
            cstate->memUsage.work_mem = stmt->memUsage.work_mem;
            cstate->memUsage.max_mem = stmt->memUsage.max_mem;
        }

        /*
         * If some errors occurs CleanBulkloadStates() need be called before errors re-thrown.
         */
        PG_TRY();
        {
            SyncBulkloadStates(cstate);

            processed = CopyFrom(cstate); /* copy from file to database */

            /* Record copy from to gchain. */
            if (cstate->hashstate.has_histhash) {
                const char *query_string = t_thrd.postgres_cxt.debug_query_string;
                ledger_gchain_append(RelationGetRelid(rel), query_string, cstate->hashstate.histhash);
            }
            stmt->hashstate.has_histhash = cstate->hashstate.has_histhash;
            stmt->hashstate.histhash = cstate->hashstate.histhash;
        }
        PG_CATCH();
        {
            CleanBulkloadStates();
            PG_RE_THROW();
        }
        PG_END_TRY();

        CleanBulkloadStates();
        EndCopyFrom(cstate);
    } else {
        pgstat_set_stmt_tag(STMTTAG_READ);
        cstate = BeginCopyTo(rel, query, queryString, stmt->filename, stmt->attlist, stmt->options);
        cstate->range_table = list_make1(rte);
        processed = DoCopyTo(cstate); /* copy from database to file */
        EndCopyTo(cstate);
    }

    /*
     * Close the relation. If reading, we can release the AccessShareLock we
     * got; if writing, we should hold the lock until end of transaction to
     * ensure that updates will be committed before lock is released.
     */
    if (rel != NULL)
        heap_close(rel, (is_from ? NoLock : AccessShareLock));

    if (AUDIT_COPY_ENABLED) {
        if (is_from && stmt->relation)
            audit_report(AUDIT_COPY_FROM, AUDIT_OK, stmt->relation->relname, queryString);
        else
            audit_report(AUDIT_COPY_TO, AUDIT_OK, stmt->filename ? stmt->filename : "stdout", queryString);
    }

    return processed;
}

static void TransformFormatter(CopyState cstate, List* formatter)
{
    FixFormatter* form = (FixFormatter*)palloc0(sizeof(FixFormatter));
    ListCell* lc = NULL;
    ListCell* pre = NULL;
    List* ordered = NIL;
    int colid = 0;
    int nfield;

    Assert(formatter);

    form->format = FORMAT_FIXED;
    form->nfield = list_length(formatter);
    nfield = form->nfield;
    form->fieldDesc = (nfield > 0 ? (FieldDesc*)palloc0(sizeof(FieldDesc) * nfield) : NULL);

    // order the fields by the begin position
    //
    foreach (lc, formatter) {
        Position* col = (Position*)lfirst(lc);
        ListCell* cell = NULL;

        if (col->position < 0)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("position of field \"%s\" can not be less then 0", col->colname)));
        if (col->fixedlen < 0)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("length of field \"%s\" can not be less then 0", col->colname)));

        if ((uint64)(col->fixedlen) + (uint64)(col->position) > MaxAllocSize)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("max length of data row cannot greater than 1GB")));
        foreach (cell, ordered) {
            Position* col2 = (Position*)lfirst(cell);
            if (col2->position > col->position)
                break;
            pre = cell;
        }

        if (pre != NULL)
            lappend_cell(ordered, pre, col);
        else if (cell != NULL)
            ordered = lcons(col, ordered);
        else
            ordered = lappend(ordered, col);
    }

    // Check if the max length of data row < 1GB
    //
    if (ordered != NIL) {
        Position* col = (Position*)lfirst(list_tail(ordered));
        form->lineSize = (col->position + col->fixedlen);
        if ((Size)(form->lineSize) >= MaxAllocSize)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("max length of data row cannot greater than 1GB")));
    }

    pre = NULL;
    foreach (lc, ordered) {
        errno_t rc = 0;
        Position* col = (Position*)lfirst(lc);
        form->fieldDesc[colid].fieldname = pstrdup(col->colname);
        form->fieldDesc[colid].fieldPos = col->position;
        form->fieldDesc[colid].fieldSize = col->fixedlen;
        form->fieldDesc[colid].nullString = (char*)palloc0(col->fixedlen + 1);
        rc = memset_s(form->fieldDesc[colid].nullString, col->fixedlen + 1, ' ', col->fixedlen);
        securec_check(rc, "", "");
        colid++;
        if (pre != NULL) {
            Position* precol = (Position*)lfirst(pre);
            if (precol->position + precol->fixedlen > col->position && cstate->is_load_copy == false)
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("pre-field \"%s\" can not be covered by field \"%s\"", precol->colname, col->colname)));
        }
        pre = lc;
    }

    cstate->formatter = form;
}

void VerifyEncoding(int encoding)
{
    Oid proc;

    if (encoding == GetDatabaseEncoding() || encoding == PG_SQL_ASCII || GetDatabaseEncoding() == PG_SQL_ASCII)
        return;

    proc = FindDefaultConversionProc(encoding, GetDatabaseEncoding());
    if (!OidIsValid(proc))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("default conversion function for encoding \"%s\" to \"%s\" does not exist",
                    pg_encoding_to_char(encoding),
                    pg_encoding_to_char(GetDatabaseEncoding()))));
}

static void PrintDelimHeader(CopyState cstate)
{
    TupleDesc tupDesc;
    Form_pg_attribute* attr = NULL;
    ListCell* cur = NULL;
    bool hdr_delim = false;
    char* colname = NULL;

    if (cstate->rel)
        tupDesc = RelationGetDescr(cstate->rel);
    else
        tupDesc = cstate->queryDesc->tupDesc;
    attr = tupDesc->attrs;

    foreach (cur, cstate->attnumlist) {
        int attnum = lfirst_int(cur);

        if (hdr_delim)
            CopySendString(cstate, cstate->delim);
        hdr_delim = true;
        colname = NameStr(attr[attnum - 1]->attname);
        CopyAttributeOutCSV(cstate, colname, false, list_length(cstate->attnumlist) == 1);
    }
}

void ProcessFileHeader(CopyState cstate)
{
    cstate->headerString = makeStringInfo();
    if (cstate->headerFilename) {
        GDSUri uri;
        int nread = 0;
        char* data = NULL;
        int eolPos = 0;

        uri.Parse(cstate->headerFilename);
        if (access(uri.m_uri, F_OK) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("invalid user-define header file \"%s\"", uri.m_uri)));
        File fd = PathNameOpenFile(cstate->headerFilename, O_RDONLY | PG_BINARY, 0400);
        if (fd < 0)
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open header file \"%s\": %m", cstate->headerFilename)));
        enlargeStringInfo(cstate->headerString, MAX_HEADER_SIZE + 1);
        data = cstate->headerString->data;
        nread = FilePRead(fd, data, MAX_HEADER_SIZE, 0);

        if (nread <= 0)
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("no data in user-define header file \"%s\"", cstate->headerFilename)));
        // find \n
        //
        for (eolPos = 0; eolPos < nread; eolPos++) {
            if (data[eolPos] == '\n') {
                break;
            }
        }

        if (eolPos >= MAX_HEADER_SIZE)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("user-define header should not longger than 1MB")));
        cstate->headerString->data[eolPos + 1] = '\0';
        cstate->headerString->len = eolPos + 1;
        FileClose(fd);
    } else if (cstate->header_line) {
        if (IS_FIXED(cstate))
            PrintFixedHeader(cstate);
        else
            PrintDelimHeader(cstate);
        SetEndOfLineType(cstate);
        appendBinaryStringInfo(cstate->headerString, cstate->fe_msgbuf->data, cstate->fe_msgbuf->len);
        resetStringInfo(cstate->fe_msgbuf);
    }
}

/**
 * @Description: check bulkload incompatible options, which is extracted from ProcessCopyOptions
 * to decrease call complexity.
 * @in cstate: the current CopyState
 * @return:void
 */
static void CheckCopyIncompatibleOptions(CopyState cstate, int force_fix_width)
{
    /*
     * Check for incompatible options (must do these two before inserting
     * defaults)
     */
    if ((IS_BINARY(cstate) || IS_FIXED(cstate)) && cstate->delim)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot specify DELIMITER in BINARY/FIXED mode")));

    if ((IS_BINARY(cstate) || IS_FIXED(cstate)) && cstate->null_print)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot specify NULL in BINARY/FIXED mode")));

    if (!IS_FIXED(cstate) && cstate->formatter)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("FORMATTER only can be specified in FIXED mode")));

    if (IS_FIXED(cstate) && !cstate->formatter)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("FORMATTER must be specified in FIXED mode")));

    if (!IS_FIXED(cstate) && force_fix_width != 0)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("FIX only can be specified in FIXED mode")));

    /*
     * can't specify bulkload compatibility options in binary mode.
     */
    if (IS_BINARY(cstate) &&
        ((cstate->compatible_illegal_chars) || (cstate->date_format.str) || (cstate->time_format.str) ||
            (cstate->timestamp_format.str) || (cstate->smalldatetime_format.str))) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot specify bulkload compatibility options in BINARY mode")));
    }

    return;
}

void GetTransSourceStr(CopyState cstate, int beginPos, int endPos)
{
    if (beginPos == -1 || endPos == -1) {
        cstate->transform_query_string = NULL;
        return;
    }

    /* valid length is endPos - beginPos + 1, extra 1 for \0 */
    int transStrLen = endPos - beginPos + 2;
    char *transString = (char *)palloc0(transStrLen * sizeof(char));
    
    errno_t rc = strncpy_s(transString, transStrLen, 
                           cstate->source_query_string + beginPos, transStrLen - 1);
    securec_check(rc, "\0", "\0");
    
    cstate->transform_query_string = transString;
}


/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void ProcessCopyOptions(CopyState cstate, bool is_from, List* options)
{
    #define NULL_VAL_STRING_LEN 100
    bool format_specified = false;
    ListCell* option = NULL;
    bool noescapingSpecified = false;
    int force_fix_width = 0;
    bool ignore_extra_data_specified = false;
    bool compatible_illegal_chars_specified = false;
    bool rejectLimitSpecified = false;

    /* OBS copy options */
    bool obs_chunksize = false;
    bool obs_encrypt = false;

    /* Support external use for option sanity checking */
    if (cstate == NULL)
        cstate = (CopyStateData*)palloc0(sizeof(CopyStateData));

    cstate->file_encoding = -1;
    cstate->fileformat = FORMAT_TEXT;
    cstate->is_from = is_from;

    /* Extract options from the statement node tree */
    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "format") == 0) {
            char* fmt = defGetString(defel);

            if (format_specified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            format_specified = true;
            if (strcasecmp(fmt, "text") == 0)
                cstate->fileformat = FORMAT_TEXT;
            else if (strcasecmp(fmt, "csv") == 0)
                cstate->fileformat = FORMAT_CSV;
            else if (strcasecmp(fmt, "binary") == 0)
                cstate->fileformat = FORMAT_BINARY;
            else if (strcasecmp(fmt, "fixed") == 0)
                cstate->fileformat = FORMAT_FIXED;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("COPY format \"%s\" not recognized", fmt)));
        } else if (strcmp(defel->defname, "oids") == 0) {
            if (cstate->oids)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->oids = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "freeze") == 0) {
            if (cstate->freeze)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->freeze = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "delimiter") == 0) {
            if (cstate->delim)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->delim = defGetString(defel);
        } else if (strcmp(defel->defname, "null") == 0) {
            if (cstate->null_print)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            char* null_print = defGetString(defel);
            if (strlen(null_print) > NULL_VAL_STRING_LEN) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("null value string is too long")));
            }
            cstate->null_print = null_print;
        } else if (strcmp(defel->defname, "header") == 0) {
            if (cstate->header_line)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->header_line = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "quote") == 0) {
            if (cstate->quote)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->quote = defGetString(defel);
        } else if (strcmp(defel->defname, "escape") == 0) {
            if (cstate->escape)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->escape = defGetString(defel);
        } else if (strcmp(defel->defname, "force_quote") == 0) {
            if (cstate->force_quote || cstate->force_quote_all)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            if (defel->arg && IsA(defel->arg, A_Star))
                cstate->force_quote_all = true;
            else if (defel->arg && IsA(defel->arg, List))
                cstate->force_quote = (List*)defel->arg;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("argument to option \"%s\" must be a list of column names", defel->defname)));
        } else if (strcmp(defel->defname, "force_not_null") == 0) {
            if (cstate->force_notnull)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            if (defel->arg && IsA(defel->arg, List))
                cstate->force_notnull = (List*)defel->arg;
            else
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("argument to option \"%s\" must be a list of column names", defel->defname)));
        } else if (strcmp(defel->defname, "encoding") == 0) {
            if (cstate->file_encoding >= 0)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->file_encoding = pg_char_to_encoding(defGetString(defel));
            if (cstate->file_encoding < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("argument to option \"%s\" must be a valid encoding name", defel->defname)));
        } else if (strcmp(defel->defname, "fill_missing_fields") == 0) {
            if (cstate->fill_missing_fields)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            else
                cstate->fill_missing_fields = defGetMixdInt(defel);
        } else if (strcmp(defel->defname, "noescaping") == 0) {
            if (noescapingSpecified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            else
                cstate->without_escaping = defGetBoolean(defel);
            noescapingSpecified = true;
        } else if (strcmp(defel->defname, "formatter") == 0) {
            TransformFormatter(cstate, (List*)(defel->arg));
        } else if (strcmp(defel->defname, "fileheader") == 0) {
            if (cstate->headerFilename)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            else
                cstate->headerFilename = defGetString(defel);
        } else if (strcmp(defel->defname, "out_filename_prefix") == 0) {
            if (cstate->out_filename_prefix)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            else
                cstate->out_filename_prefix = defGetString(defel);
        } else if (strcmp(defel->defname, "out_fix_alignment") == 0) {
            if (cstate->out_fix_alignment)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            else
                cstate->out_fix_alignment = defGetString(defel);
        } else if (strcmp(defel->defname, "eol") == 0) {
            if (cstate->eol_type == EOL_UD)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));

            cstate->eol_type = EOL_UD;
            if (strcasecmp(defGetString(defel), "0x0A") == 0 || strcasecmp(defGetString(defel), "\\n") == 0) {
                cstate->eol = defGetString(defel);
                cstate->eol[0] = '\n';
                cstate->eol[1] = '\0';
            } else if (strcasecmp(defGetString(defel), "0x0D0A") == 0 ||
                       strcasecmp(defGetString(defel), "\\r\\n") == 0) {
                cstate->eol = defGetString(defel);
                cstate->eol[0] = '\r';
                cstate->eol[1] = '\n';
                cstate->eol[2] = '\0';
            } else if (strcasecmp(defGetString(defel), "0x0D") == 0 || strcasecmp(defGetString(defel), "\\r") == 0) {
                cstate->eol = defGetString(defel);
                cstate->eol[0] = '\r';
                cstate->eol[1] = '\0';
            } else {
                cstate->eol = defGetString(defel);
                if (strlen(defGetString(defel)) == 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("\"%s\" is not a valid EOL string, EOL string must not be empty",
                                defGetString(defel))));
                else if (strlen(defGetString(defel)) > EOL_MAX_LEN)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("\"%s\" is not a valid EOL string, EOL string must not exceed the maximum length "
                                   "(10 bytes)",
                                defGetString(defel))));
            }
        } else if (strcmp(defel->defname, "fix") == 0) {
            char* end = NULL;
            if (force_fix_width > 0)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            force_fix_width = strtol(defGetString(defel), &end, 10);
            if (*end != '\0')
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value of FIX")));
        } else if (strcmp(defel->defname, "ignore_extra_data") == 0) {

            if (ignore_extra_data_specified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));

            ignore_extra_data_specified = true;
            cstate->ignore_extra_data = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "date_format") == 0) {
            if (cstate->date_format.str)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->date_format.str = defGetString(defel);
            /*
             * Here no need to do a extra date format check, because as the following:
             * (1)for foreign table date format checking occurs in ProcessDistImportOptions();
             * (2)for copy date format checking occurs when date format is firstly used in date_in().
             */
        } else if (strcmp(defel->defname, "time_format") == 0) {
            if (cstate->time_format.str)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->time_format.str = defGetString(defel);
            /*
             * Here no need to do a extra time format check, because as the following:
             * (1)for foreign table time format checking occurs in ProcessDistImportOptions();
             * (2)for copy time format checking occurs when date format is firstly used in time_in().
             */
        } else if (strcmp(defel->defname, "timestamp_format") == 0) {
            if (cstate->timestamp_format.str)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->timestamp_format.str = defGetString(defel);
            /*
             * Here no need to do a extra timestamp format check, because as the following:
             * (1)for foreign table timestamp format checking occurs in ProcessDistImportOptions();
             * (2)for copy timestamp format checking occurs when timestamp format is firstly used in timestamp_in().
             */
        } else if (strcmp(defel->defname, "smalldatetime_format") == 0) {
            if (cstate->smalldatetime_format.str)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->smalldatetime_format.str = defGetString(defel);
            /*
             * Here no need to do a extra smalldatetime format check, because as the following:
             * (1)for foreign table smalldatetime format checking occurs in ProcessDistImportOptions();
             * (2)for copy smalldatetime format checking occurs when smalldatetime format is firstly used in
             * smalldatetime_in().
             */
        } else if (strcmp(defel->defname, "compatible_illegal_chars") == 0) {
            if (compatible_illegal_chars_specified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));

            compatible_illegal_chars_specified = true;
            cstate->compatible_illegal_chars = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "log_errors") == 0) {
            if (cstate->log_errors) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            } else {
                cstate->log_errors = defGetBoolean(defel);
            }
        } else if (strcmp(defel->defname, "log_errors_data") == 0) {
            if (cstate->logErrorsData) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            } else {
                cstate->logErrorsData = defGetBoolean(defel);
            }
        } else if (strcmp((defel->defname), "reject_limit") == 0) {
            if (rejectLimitSpecified) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            if (pg_strcasecmp(defGetString(defel), "unlimited") == 0) {
                cstate->reject_limit = REJECT_UNLIMITED;
            } else {
                /*
                 * The default behavior is reject_limit = 0, which is no
                 * fault tolerance at all.
                 */
                char* value = defGetString(defel);
                int limit = pg_strtoint32(value);
                cstate->reject_limit = (limit > 0 ? limit : 0);
            }

            rejectLimitSpecified = true;
        } else if (pg_strcasecmp((defel->defname), "transform") == 0) {
            cstate->trans_expr_list = (List*)(defel->arg);
            GetTransSourceStr(cstate, defel->begin_location, defel->end_location);
        } else if (pg_strcasecmp((defel->defname), "when_expr") == 0) {
            cstate->when = (List *)defel->arg;
            CheckCopyWhenList(cstate->when);
        } else if (pg_strcasecmp((defel->defname), "skip") == 0) {
            cstate->skip = defGetInt64(defel);
            if (cstate->skip < 0) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("SKIP %ld should be >= 0", cstate->skip)));
            }
        } else if (pg_strcasecmp((defel->defname), "sequence") == 0) {
            cstate->sequence_col_list = (List*)(defel->arg);
        } else if (pg_strcasecmp((defel->defname), "filler") == 0) {
            cstate->filler_col_list = (List*)(defel->arg);
        } else if (pg_strcasecmp((defel->defname), "constant") == 0) {
            cstate->constant_col_list = (List*)(defel->arg);
        } else if (strcmp(defel->defname, "loader") == 0) {
            if (cstate->is_load_copy)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cstate->is_load_copy = defGetBoolean(defel);
        } else if (pg_strcasecmp((defel->defname), "useeof") == 0) {
            cstate->is_useeof = defGetBoolean(defel);
        } else if (pg_strcasecmp(defel->defname, optChunkSize) == 0) {
            if (obs_chunksize) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            obs_chunksize = true;
        } else if (pg_strcasecmp(defel->defname, optEncrypt) == 0) {
            if (obs_encrypt) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            obs_encrypt = true;
        } else if (pg_strcasecmp(defel->defname, optAccessKey) == 0) {
        } else if (pg_strcasecmp(defel->defname, optSecretAccessKey) == 0) {
        } else if (pg_strcasecmp(defel->defname, optLocation) == 0) {
        } else if (pg_strcasecmp(defel->defname, OPTION_NAME_REGION) == 0) {

        } else
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    /*
     * check bulkload incompatible options.
     */
    CheckCopyIncompatibleOptions(cstate, force_fix_width);

    ProcessCopyErrorLogOptions(cstate, rejectLimitSpecified);

    /* Set defaults for omitted options */
    if (!cstate->delim)
        cstate->delim = (char*)(IS_CSV(cstate) ? "," : "\t");

    if (!cstate->null_print)
        cstate->null_print = (char*)(IS_CSV(cstate) ? "" : "\\N");
    cstate->null_print_len = strlen(cstate->null_print);

    if (IS_CSV(cstate)) {
        if (!cstate->quote)
            cstate->quote = "\"";
        if (!cstate->escape)
            cstate->escape = cstate->quote;
    }

    if (cstate->mode == MODE_INVALID)
        cstate->mode = MODE_NORMAL;

    if (cstate->eol_type != EOL_UD && !is_from)
        cstate->eol_type = EOL_NL;

    if (IS_FIXED(cstate) && force_fix_width != 0) {
        FixFormatter* formatter = (FixFormatter*)cstate->formatter;
        if (force_fix_width < 0 || force_fix_width < formatter->lineSize || force_fix_width > (int)MaxAllocSize)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value of FIX")));
        formatter->lineSize = force_fix_width;
        formatter->forceLineSize = true;
    }

    /* delimiter strings should be no more than 10 bytes. */
    if ((cstate->delim_len = strlen(cstate->delim)) > DELIM_MAX_LEN)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("COPY delimiter must be less than %d bytes", DELIM_MAX_LEN)));

    /* Disallow end-of-line characters */
    if (strchr(cstate->delim, '\r') != NULL || strchr(cstate->delim, '\n') != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("COPY delimiter cannot be newline or carriage return")));

    if (strchr(cstate->null_print, '\r') != NULL || strchr(cstate->null_print, '\n') != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("COPY null representation cannot use newline or carriage return")));

    if (cstate->eol_type == EOL_UD) {
        if (strstr(cstate->delim, cstate->eol) != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("COPY delimiter cannot contain user-define EOL string")));
        if (strstr(cstate->null_print, cstate->eol) != NULL)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("COPY null representation cannot contain user-define EOL string")));
    }

    /*
     * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
     * backslash because it would be ambiguous.  We can't allow the other
     * cases because data characters matching the delimiter must be
     * backslashed, and certain backslash combinations are interpreted
     * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
     * more than strictly necessary, but seems best for consistency and
     * future-proofing.  Likewise we disallow all digits though only octal
     * digits are actually dangerous.
     */
    if (!IS_CSV(cstate) && strpbrk(cstate->delim, INVALID_DELIM_CHAR) != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("delimiter \"%s\" cannot contain any characters in\"%s\"", cstate->delim, INVALID_DELIM_CHAR)));

    /* Disallow unsafe eol characters in non-CSV mode. */
    if (!IS_CSV(cstate) && cstate->eol_type == EOL_UD && strpbrk(cstate->eol, INVALID_EOL_CHAR) != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("EOL string \"%s\" cannot contain any characters in\"%s\"", cstate->eol, INVALID_EOL_CHAR)));

    /* Check header */
    if (!IS_CSV(cstate) && !IS_FIXED(cstate) && cstate->header_line)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY HEADER available only in CSV mode")));

    /* Check quote */
    if (!IS_CSV(cstate) && cstate->quote != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY quote available only in CSV mode")));

    if (IS_CSV(cstate) && strlen(cstate->quote) != 1)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY quote must be a single one-byte character")));

    if (IS_CSV(cstate) && strchr(cstate->delim, cstate->quote[0]) != NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("delimiter cannot contain quote character")));

    /* Check escape */
    if (!IS_CSV(cstate) && cstate->escape != NULL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY escape available only in CSV mode")));

    if (IS_CSV(cstate) && strlen(cstate->escape) != 1)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY escape must be a single one-byte character")));

    /* Check force_quote */
    if (!IS_CSV(cstate) && (cstate->force_quote || cstate->force_quote_all))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY force quote available only in CSV mode")));
    if ((cstate->force_quote || cstate->force_quote_all) && is_from)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY force quote only available using COPY TO")));

    /* Check force_notnull */
    if (!IS_CSV(cstate) && cstate->force_notnull != NIL)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY force not null available only in CSV mode")));
    if (cstate->force_notnull != NIL && !is_from)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY force not null only available using COPY FROM")));

    /* Don't allow the delimiter to appear in the null string. */
    if ((strlen(cstate->null_print) >= 1 && strlen(cstate->delim) >= 1) &&
        (strstr(cstate->null_print, cstate->delim) != NULL || strstr(cstate->delim, cstate->null_print) != NULL))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("COPY delimiter must not appear in the NULL specification")));

    /*
     * Enhance the following check:
     * noescaping is just allowed for text mode.
     */
    if ((!IS_TEXT(cstate)) && noescapingSpecified)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("without escaping available only in TEXT mode")));

    /* Don't allow the CSV quote char to appear in the null string. */
    if (IS_CSV(cstate) && strchr(cstate->null_print, cstate->quote[0]) != NULL)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("CSV quote character must not appear in the NULL specification")));
    if (IS_CSV(cstate) && cstate->mode == MODE_SHARED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SHARED mode can not be used with CSV format")));
    if (cstate->headerFilename && is_from)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("HEADER FILE only available using COPY TO or WRITE ONLY foreign table")));

    if (cstate->eol_type == EOL_UD && is_from && !IS_TEXT(cstate))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EOL specification can not be used with non-text format using COPY FROM or READ ONLY foreign "
                       "table")));
    if (cstate->eol_type == EOL_UD && strcmp(cstate->eol, "\r\n") != 0 && strcmp(cstate->eol, "\n") != 0 && !is_from &&
        !IS_TEXT(cstate))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("EOL specification can not be used with non-text format using COPY TO or WRITE ONLY foreign "
                       "table except 0x0D0A and 0x0A")));
    if (cstate->eol_type == EOL_UD && IS_BINARY(cstate))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify EOL in BINARY mode")));

    if (!is_from && force_fix_width != 0)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("FIX specification only available using COPY FROM or READ ONLY foreign table")));
    if (!cstate->header_line && cstate->headerFilename)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("FILEHEADER specification only available using HEAD")));
    if (!is_from && ignore_extra_data_specified)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("IGNORE_EXTRA_DATA specification only available using COPY FROM or READ ONLY foreign table")));
    if (!is_from && compatible_illegal_chars_specified)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("COMPATIBLE_ILLEGAL_CHARS specification only available using COPY FROM or READ ONLY foreign "
                       "table")));
    if (!is_from && cstate->date_format.str)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("DATE_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
    if (!is_from && cstate->time_format.str)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("TIME_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
    if (!is_from && cstate->timestamp_format.str)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("TIMESTAMP_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
    if (!is_from && cstate->smalldatetime_format.str)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg(
                    "SMALLDATETIME_FORMAT specification only available using COPY FROM or READ ONLY foreign table")));
    /*
     * There are some confusion between the following flags with compatible_illegal_chars_specified flag.
     *
     * check null_print
     */
    if (cstate->null_print && (cstate->null_print_len == 1) &&
        ((cstate->null_print[0] == ' ') || (cstate->null_print[0] == '?')) && (cstate->compatible_illegal_chars)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("illegal chars conversion may confuse COPY null 0x%x", cstate->null_print[0])));
    }

    /*
     * check delimiter
     */
    if (cstate->delim && (cstate->delim_len == 1) && ((cstate->delim[0] == ' ') || (cstate->delim[0] == '?')) &&
        (cstate->compatible_illegal_chars)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("illegal chars conversion may confuse COPY delimiter 0x%x", cstate->delim[0])));
    }

    /*
     * check quote
     */
    if (cstate->quote && ((cstate->quote[0] == ' ') || (cstate->quote[0] == '?')) &&
        (cstate->compatible_illegal_chars)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("illegal chars conversion may confuse COPY quote 0x%x", cstate->quote[0])));
    }

    /*
     * check escape
     */
    if (cstate->escape && ((cstate->escape[0] == ' ') || (cstate->escape[0] == '?')) &&
        (cstate->compatible_illegal_chars)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("illegal chars conversion may confuse COPY escape 0x%x", cstate->escape[0])));
    }
}

/* transform the raw expr list into ExprState node Array. */
static void TransformColExpr(CopyState cstate)
{
    ListCell* lc = NULL;
    int colcounter = 0;
    List* exprlist = cstate->trans_expr_list;
    int colnum = list_length(exprlist);
    
    if (colnum == 0) {
        return;
    }

#ifdef ENABLE_MULTIPLE_NODES
    ListCell* cell = NULL;
    RelationLocInfo* rel_loc_info = GetRelationLocInfo(cstate->rel->rd_id);
    List* distributeCol = GetRelationDistribColumn(rel_loc_info);
#endif

    foreach (lc, exprlist) {
        int attrno;
        Oid attroid;
        int attrmod;
        bool execexpr = false;
        Node* expr = NULL;
        CopyColExpr *col = NULL;

        col = (CopyColExpr *)lfirst(lc);
        attrno = attnameAttNum(cstate->rel, col->colname, false);

        if (attrno == InvalidAttrNumber)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" of relation \"%s\" does not exist", col->colname,
                           RelationGetRelationName(cstate->rel))));

#ifdef ENABLE_MULTIPLE_NODES
        foreach (cell, distributeCol) {
            if (strcmp(col->colname, strVal(lfirst(cell))) == 0) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                     errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                     errmsg("Distributed key column can't be transformed"),
                     errdetail("Column %s is distributed key which can't be transformed", col->colname),
                     errcause("There is a risk of violating uniqueness when transforming distribution columns."),
                     erraction("Change transform column.")));
            }
        }
#endif

        attroid = attnumTypeId(cstate->rel, attrno);
        attrmod = cstate->rel->rd_att->attrs[attrno - 1]->atttypmod;

        if (col->typname != NULL) {
            SetColInFunction(cstate, attrno, col->typname);

            /* Build a ColumnRef in order to transform datatype */
            if (col->colexpr == NULL &&
                (attroid != cstate->trans_tupledesc->attrs[attrno - 1]->atttypid
                || attrmod != cstate->as_typemods[attrno - 1].typemod)) {
                ColumnRef *cref = makeNode(ColumnRef);

                /* Build an unqualified ColumnRef with the given name */
                cref->fields = list_make1(makeString(col->colname));
                cref->location = -1;

                expr = (Node *)cref;
                execexpr = true;
            }
        }

        if (col->colexpr != NULL) {
            /* We don't need ColumnRef expr if any has colexpr */
            expr = col->colexpr;
            execexpr = true;
        }

        if (execexpr) {
            cstate->transexprs[attrno - 1]  = ExecInitCopyColExpr(cstate, attrno, attroid, attrmod, expr);
        }
        colcounter++;
    }

#ifdef ENABLE_MULTIPLE_NODES
    list_free_ext(distributeCol);
#endif

    Assert(colnum == colcounter);
}

/* obtain the type message and in_function by target column. */
static void SetColInFunction(CopyState cstate, int attrno, const TypeName* type)
{
    Oid attroid;
    int attrmod;
    Oid infunc;
    Type tup;

    /* Make new typeid & typemod of Typename. */
    tup = typenameType(NULL, type, &attrmod);
    attroid = HeapTupleGetOid(tup);
    
    cstate->as_typemods[attrno - 1].assign = true;
    cstate->as_typemods[attrno - 1].typemod = attrmod;

    if (cstate->trans_tupledesc->attrs[attrno - 1]->atttypid != attroid) {
        cstate->trans_tupledesc->attrs[attrno - 1]->atttypid =  attroid;
        cstate->trans_tupledesc->attrs[attrno - 1]->atttypmod = attrmod;
        cstate->trans_tupledesc->attrs[attrno - 1]->attalign = ((Form_pg_type)GETSTRUCT(tup))->typalign;
        cstate->trans_tupledesc->attrs[attrno - 1]->attlen = ((Form_pg_type)GETSTRUCT(tup))->typlen;
        cstate->trans_tupledesc->attrs[attrno - 1]->attbyval = ((Form_pg_type)GETSTRUCT(tup))->typbyval;
        cstate->trans_tupledesc->attrs[attrno - 1]->attstorage = ((Form_pg_type)GETSTRUCT(tup))->typstorage;
    }
    ReleaseSysCache(tup);
    

    /* Fetch the input function and typioparam info */
    if (IS_BINARY(cstate))
        getTypeBinaryInputInfo(attroid, &infunc,
                               &(cstate->typioparams[attrno - 1]));
    else
        getTypeInputInfo(attroid, &infunc,
                         &(cstate->typioparams[attrno - 1]));

    fmgr_info(infunc, &(cstate->in_functions[attrno - 1]));
}

/* prepare an column expression tree for execution */
static ExprState* ExecInitCopyColExpr(CopyState cstate, int attrno, Oid attroid, int attrmod, Node *expr)
{
    int attrnum = cstate->rel->rd_att->natts;
    ParseState* pstate = NULL;
    RangeTblEntry* rte = NULL;
    List* colLists = NIL;
    List* colnames = NIL;
    List* collations = NIL;
    Form_pg_attribute attr;
    Oid exprtype;
    int i;

    pstate = make_parsestate(NULL);

    /* Make value: each attribute as same as target relation, except attrno-1 column. */
    for (i = 0; i < attrnum; i++) {
        Var* value;

        if (i == attrno - 1)
            attr = cstate->trans_tupledesc->attrs[i];
        else
            attr = cstate->rel->rd_att->attrs[i];

        value = makeVar(0, i + 1, attr->atttypid, attr->atttypmod, attr->attcollation, 0);

        colLists = lappend(colLists, value);
        colnames = lappend(colnames, makeString(pstrdup(NameStr(attr->attname))));
        collations = lappend_oid(collations, attr->attcollation);
    }

    rte = addRangeTableEntryForValues(pstate, list_make1(colLists), collations, NULL, false);
    rte->eref->colnames = colnames;
    addRTEtoQuery(pstate, rte, true, true, true);

    expr = transformExpr(pstate, expr);
    exprtype = exprType(expr);
    expr = coerce_to_target_type(pstate, expr, exprtype, attroid, attrmod,
                                 COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1);
    if (expr == NULL) {
        ereport(ERROR,
                (errmodule(MOD_OPT),
                 errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cannot convert %s to %s",
                        format_type_be(exprtype),
                        format_type_be(attroid)),
                 errdetail("column \"%s\" is of type %s, but expression is of type %s",
                           NameStr(*attnumAttName(cstate->rel, attrno)),
                           format_type_be(attroid),
                           format_type_be(exprtype)),
                 errcause("There is no conversion path in pg_cast."),
                 erraction("Rewrite or cast the expression.")));
    }
    return ExecInitExpr((Expr *)expr, NULL);
}

static void ProcessCopyNotAllowedOptions(CopyState cstate)
{
    if (cstate->out_filename_prefix) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("out_filename_prefix is only allowed in write-only foreign tables")));
    }

    if (cstate->out_fix_alignment) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("out_fix_alignment is only allowed in write-only foreign tables")));
    }

#ifdef ENABLE_MULTIPLE_NODES
    /*
     *  We could not make CopyGetDataDefault to respond to cancel signal. As a result
     *  subtransactions that include COPY statement would be able to rollback or trigger exception
     *  handling correctly. Therefore we raise error first before COPY in subxact even begin.
     */
    if (IsInLiveSubtransaction()) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("COPY does not support subtransactions or exceptions.")));
    }
#endif
}
/*
 * ProcessCopyErrorLogSetUps is used to set up necessary structures used for copy from error logging.
 */
static bool CheckCopyErrorTableDef(int nattrs, Form_pg_attribute* attr)
{
    if (nattrs != COPY_ERROR_TABLE_NUM_COL) {
        return false;
    }

    for (int attnum = 0; attnum < nattrs; attnum++) {
        if (attr[attnum]->atttypid != copy_error_table_col_typid[attnum])
            return false;
    }
    return true;
}

static void ProcessCopyErrorLogSetUps(CopyState cstate)
{
    Oid sys_namespaceid = InvalidOid;
    Oid err_table_reloid = InvalidOid;
    int attnum;

    if (!cstate->log_errors && !cstate->logErrorsData) {
        return;
    }
    if (cstate->logErrorsData && !superuser() && cstate->is_load_copy == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to execute \"COPY  from log errors data\" ")));
    }

    cstate->copy_beginTime = TimestampTzGetDatum(GetCurrentTimestamp());

    /* One thing to know is that copy_error_log is in public, at least temperarily. */
    sys_namespaceid = get_namespace_oid(COPY_ERROR_TABLE_SCHEMA, false);
    err_table_reloid = get_relname_relid(COPY_ERROR_TABLE, sys_namespaceid);
    if (!OidIsValid(err_table_reloid)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Unable to open %s.%s table for COPY FROM error logging.",
                    COPY_ERROR_TABLE_SCHEMA,
                    COPY_ERROR_TABLE),
                errhint("You may want to use copy_error_log_create() to create it first.")));
    }
    /* RowExclusiveLock is used for insertion during spi. */
    cstate->err_table = relation_open(err_table_reloid, RowExclusiveLock);

    /*
     *	This logging is used for both saving error info into the cache file
     *	and reading error info for spi insertion.
     *	The offset is reset in between.
     */
    CopyErrorLogger* logger = New(CurrentMemoryContext) CopyErrorLogger;
    logger->Initialize(cstate);
    cstate->logger = logger;

    /* err_out_functions used when reading error info from cache files. */
    int natts = RelationGetNumberOfAttributes(cstate->err_table);
    TupleDesc tupDesc = RelationGetDescr(cstate->err_table);
    Form_pg_attribute* attr = tupDesc->attrs;
    cstate->err_out_functions = (FmgrInfo*)palloc(natts * sizeof(FmgrInfo));

    if (!CheckCopyErrorTableDef(natts, attr)) {
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("The column definition of %s.%s table is not as intended.",
                    COPY_ERROR_TABLE_SCHEMA,
                    COPY_ERROR_TABLE),
                errhint("You may want to use copy_error_log_create() to create it instead in order to use COPY FROM "
                        "error logging.")));
    }

    for (attnum = 0; attnum < natts; attnum++) {
        Oid out_func_oid;
        bool isvarlena = false;

        getTypeOutputInfo(attr[attnum]->atttypid, &out_func_oid, &isvarlena);
        fmgr_info(out_func_oid, &cstate->err_out_functions[attnum]);
    }
}

/*
 * ProcessCopySummaryLogSetUps is used to set up necessary structures used for copy from summary logging.
 */
static bool CheckCopySummaryTableDef(int nattrs, Form_pg_attribute *attr)
{
    if (nattrs != COPY_SUMMARY_TABLE_NUM_COL) {
        return false;
    }

    for (int attnum = 0; attnum < nattrs; attnum++) {
        if (attr[attnum]->atttypid != copy_summary_table_col_typid[attnum])
            return false;
    }
    return true;
}

static void ProcessCopySummaryLogSetUps(CopyState cstate)
{
    Oid sys_namespaceid = InvalidOid;
    Oid summary_table_reloid = InvalidOid;

    cstate->copy_beginTime = TimestampTzGetDatum(GetCurrentTimestamp());
    cstate->skiprows = 0;
    cstate->loadrows = 0;
    cstate->errorrows = 0;
    cstate->whenrows = 0;
    cstate->allnullrows = 0;
    cstate->summary_table = NULL;

    if (!cstate->is_from) {
        return;
    }
    /* One thing to know is that gs_copy_summary is in public, */
    sys_namespaceid = get_namespace_oid(COPY_SUMMARY_TABLE_SCHEMA, false);
    summary_table_reloid = get_relname_relid(COPY_SUMMARY_TABLE, sys_namespaceid);
    if (!OidIsValid(summary_table_reloid)) {
        return;
    }
    /* RowExclusiveLock is used for insertion during spi. */
    cstate->summary_table = relation_open(summary_table_reloid, RowExclusiveLock);
    int natts = RelationGetNumberOfAttributes(cstate->summary_table);
    TupleDesc tupDesc = RelationGetDescr(cstate->summary_table);
    Form_pg_attribute *attr = tupDesc->attrs;

    if (!CheckCopySummaryTableDef(natts, attr)) {
        ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                 errmsg("The column definition of %s.%s table is not as intended.", COPY_SUMMARY_TABLE_SCHEMA,
                        COPY_SUMMARY_TABLE),
                 errhint("You may want to use copy_summary_create() to create it instead in order to use COPY FROM "
                         "summary logging.")));
    }
}

/*
 * Common setup routines used by BeginCopyFrom and BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
static CopyState BeginCopy(bool is_from, Relation rel, Node* raw_query, const char* queryString, List* attnamelist,
    List* options, bool is_copy)
{
    CopyState cstate;
    TupleDesc tupDesc;
    int num_phys_attrs;
    MemoryContext oldcontext;

    /* Allocate workspace and zero all fields */
    cstate = (CopyStateData*)palloc0(sizeof(CopyStateData));

    /*
     * We allocate everything used by a cstate in a new memory context. This
     * avoids memory leaks during repeated use of COPY in a query.
     */
    cstate->copycontext = AllocSetContextCreate(
        CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    oldcontext = MemoryContextSwitchTo(cstate->copycontext);

    cstate->source_query_string = queryString;

    /* Extract options from the statement node tree */
    ProcessCopyOptions(cstate, is_from, options);
    if (is_copy)
        ProcessCopyNotAllowedOptions(cstate);
    /* Process the source/target relation or query */
    if (rel) {
        Assert(!raw_query);

        cstate->rel = rel;

        tupDesc = RelationGetDescr(cstate->rel);

        /* Don't allow COPY w/ OIDs to or from a table without them */
        if (cstate->oids && !cstate->rel->rd_rel->relhasoids)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("table \"%s\" does not have OIDs", RelationGetRelationName(cstate->rel))));
#ifdef PGXC
        /* Get copy statement and execution node information */
        if (IS_PGXC_COORDINATOR) {
            RemoteCopyData* remoteCopyState = (RemoteCopyData*)palloc0(sizeof(RemoteCopyData));
            List* attnums = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);
            List* allAttnums = CopyGetAllAttnums(tupDesc, cstate->rel);

            /* Setup correct COPY FROM/TO flag */
            remoteCopyState->is_from = is_from;

            /* Get execution node list */
            RemoteCopy_GetRelationLoc(remoteCopyState, cstate->rel, allAttnums);
            /* Build remote query */
            RemoteCopy_BuildStatement(remoteCopyState, cstate->rel, GetRemoteCopyOptions(cstate), attnamelist, attnums);

            /* Then assign built structure */
            cstate->remoteCopyState = remoteCopyState;
        }
#endif
    } else {
        List* rewritten = NIL;
        Query* query = NULL;
        PlannedStmt* plan = NULL;
        DestReceiver* dest = NULL;

        Assert(!is_from);
        cstate->rel = NULL;

        /* Don't allow COPY w/ OIDs from a select */
        if (cstate->oids)
            ereport(
                ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY (SELECT) WITH OIDS is not supported")));

        /*
         * Run parse analysis and rewrite.	Note this also acquires sufficient
         * locks on the source table(s).
         *
         * Because the parser and planner tend to scribble on their input, we
         * make a preliminary copy of the source querytree.  This prevents
         * problems in the case that the COPY is in a portal or plpgsql
         * function and is executed repeatedly.  (See also the same hack in
         * DECLARE CURSOR and PREPARE.)
         */
        rewritten = pg_analyze_and_rewrite((Node*)copyObject(raw_query), queryString, NULL, 0);

        /* We don't expect more or less than one result query */
        if (list_length(rewritten) != 1)
            ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("unexpected rewrite result")));

        query = (Query*)linitial(rewritten);

#ifdef PGXC
        /*
         * The grammar allows SELECT INTO, but we don't support that.
         * openGauss uses an INSERT SELECT command in this case
         */
        if ((query->utilityStmt != NULL && IsA(query->utilityStmt, CreateTableAsStmt)) ||
            query->commandType == CMD_INSERT)
#else
        /* The grammar allows SELECT INTO, but we don't support that */
        if (query->utilityStmt != NULL && IsA(query->utilityStmt, CreateTableAsStmt))
#endif
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COPY (SELECT INTO) is not supported")));

        Assert(query->commandType == CMD_SELECT);
        Assert(query->utilityStmt == NULL);

        /* plan the query */
        plan = planner(query, 0, NULL);

        /*
         * Use a snapshot with an updated command ID to ensure this query sees
         * results of any previously executed queries.
         */
        PushCopiedSnapshot(GetActiveSnapshot());
        UpdateActiveSnapshotCommandId();

        /* Create dest receiver for COPY OUT */
        dest = CreateDestReceiver(DestCopyOut);
        ((DR_copy*)dest)->cstate = cstate;

        /* Create a QueryDesc requesting no output */
        cstate->queryDesc = CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, NULL, 0);

        /*
         * Call ExecutorStart to prepare the plan for execution.
         *
         * ExecutorStart computes a result tupdesc for us
         */
        ExecutorStart(cstate->queryDesc, 0);

        tupDesc = cstate->queryDesc->tupDesc;
    }

    if (is_copy) {
        ProcessCopySummaryLogSetUps(cstate);
        ProcessCopyErrorLogSetUps(cstate);
    }

    if (IS_FIXED(cstate)) {
        SetFixedAlignment(tupDesc, rel, (FixFormatter*)(cstate->formatter), cstate->out_fix_alignment);
        if (!is_from)
            VerifyFixedFormatter(tupDesc, (FixFormatter*)(cstate->formatter));
    }

    /* Generate or convert list of attributes to process */
    cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

    num_phys_attrs = tupDesc->natts;

    /* Convert FORCE QUOTE name list to per-column flags, check validity */
    cstate->force_quote_flags = (bool*)palloc0(num_phys_attrs * sizeof(bool));
    if (cstate->force_quote_all) {
        int i;

        for (i = 0; i < num_phys_attrs; i++)
            cstate->force_quote_flags[i] = true;
    } else if (cstate->force_quote) {
        List* attnums = NIL;
        ListCell* cur = NULL;

        attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

        foreach (cur, attnums) {
            int attnum = lfirst_int(cur);

            if (!list_member_int(cstate->attnumlist, attnum))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                        errmsg("FORCE QUOTE column \"%s\" not referenced by COPY",
                            NameStr(tupDesc->attrs[attnum - 1]->attname))));
            cstate->force_quote_flags[attnum - 1] = true;
        }
    }

    /* Convert FORCE NOT NULL name list to per-column flags, check validity */
    cstate->force_notnull_flags = (bool*)palloc0(num_phys_attrs * sizeof(bool));
    if (cstate->force_notnull) {
        List* attnums = NIL;
        ListCell* cur = NULL;

        attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

        foreach (cur, attnums) {
            int attnum = lfirst_int(cur);

            if (!list_member_int(cstate->attnumlist, attnum))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                        errmsg("FORCE NOT NULL column \"%s\" not referenced by COPY",
                            NameStr(tupDesc->attrs[attnum - 1]->attname))));
            cstate->force_notnull_flags[attnum - 1] = true;
        }
    }

    /*
     * Check fill_missing_fields conflict
     * */
    if (cstate->fill_missing_fields == 1) {
        /* find last valid column */
        int i = num_phys_attrs - 1;
        for (; i >= 0; i--) {
            if (!tupDesc->attrs[i]->attisdropped)
                break;
        }

        if (cstate->force_notnull_flags[i])
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("fill_missing_fields can't be set while \"%s\" is NOT NULL",
                        NameStr(tupDesc->attrs[i]->attname))));
    }

    /* Use client encoding when ENCODING option is not specified. */
    if (cstate->file_encoding < 0)
        cstate->file_encoding = pg_get_client_encoding();
    /*
     * Set up encoding conversion info.  Even if the file and server encodings
     * are the same, we must apply pg_any_to_server() to validate data in
     * multibyte encodings.
     */
    cstate->need_transcoding =
        (cstate->file_encoding != GetDatabaseEncoding() || pg_database_encoding_max_length() > 1);

    VerifyEncoding(cstate->file_encoding);

    /* See Multibyte encoding comment above */
    cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
    cstate->copy_dest = COPY_FILE; /* default */
    /* init default CopyGetData function */
    cstate->copyGetDataFunc = CopyGetDataDefault;
    cstate->readlineFunc = CopyReadLineText;
    /* set attributes reading functions */
    bulkload_set_readattrs_func(cstate);
    /* parse time format specified by user */
    bulkload_init_time_format(cstate);

    cstate->hashstate.has_histhash = false;
    cstate->hashstate.histhash = 0;
    cstate->trans_tupledesc = CreateTupleDescCopy(tupDesc);

    (void)MemoryContextSwitchTo(oldcontext);

    return cstate;
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void EndCopy(CopyState cstate)
{
    if (cstate->filename != NULL && FreeFile(cstate->copy_file))
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", cstate->filename)));

    if (cstate->is_from && (cstate->log_errors || cstate->logErrorsData)) {
        relation_close(cstate->err_table, RowExclusiveLock);
        cstate->logger->Destroy();
    }

    /* This memcontext delete operation is responsible for no memory leaking in COPY.
     *
     *	Might not be a good way to deal with it, tbh, especially
     *	from a programming perspective.
     */

    MemoryContextDelete(cstate->copycontext);
    pfree_ext(cstate);
}
/* Check "copy to" and "copy from" file name blacklist */
static bool CheckCopyFileInBlackList(const char* path)
{
    char* blacklist[] = { /* The following files are configuration files, key files and certificate files */
        FindFileName(g_instance.attr.attr_common.ConfigFileName),
        FindFileName(g_instance.attr.attr_common.HbaFileName),
        FindFileName(g_instance.attr.attr_common.IdentFileName),
        FindFileName(g_instance.attr.attr_common.external_pid_file),
        FindFileName(g_instance.attr.attr_security.ssl_cert_file),
        FindFileName(g_instance.attr.attr_security.ssl_key_file),
        FindFileName(g_instance.attr.attr_security.ssl_ca_file),
        FindFileName(g_instance.attr.attr_security.ssl_crl_file),
        FindFileName(u_sess->attr.attr_security.pg_krb_server_keyfile),
        ".key.cipher",
        ".key.rand",
        "openssl.cnf",
        /* Audit filename is always be like "*_adt" */
        "_adt",
        /* Sensitive files related to OM */
        "config.py",
        "om_agent.conf"};

    char* copyFileName = FindFileName(path);
    if (copyFileName == NULL) {
        return false;
    }

    for (unsigned i = 0; i < lengthof(blacklist); i++) {
        if (blacklist[i] == NULL) {
            continue;
        }

        if (strstr(copyFileName, blacklist[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* Find file name from file path */
static char* FindFileName(const char* path)
{
    char* name_start = NULL;
    int sep = '/';

    if (path == NULL) {
        return NULL;
    }
    if (strlen(path) == 0) {
        return NULL;
    }

    name_start = strrchr((char*)path, sep);
    return (name_start == NULL) ? (char*)path : (name_start + 1);
}

static void CopyToCheck(Relation rel)
{
    if (rel->rd_rel->relkind == RELKIND_VIEW)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from view \"%s\"", RelationGetRelationName(rel)),
                errhint("Try the COPY (SELECT ...) TO variant.")));
    else if (rel->rd_rel->relkind == RELKIND_CONTQUERY)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from contview \"%s\"", RelationGetRelationName(rel)),
                errhint("Try the COPY (SELECT ...) TO variant.")));
    else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from materialized view \"%s\"", RelationGetRelationName(rel)),
                errhint("Try the COPY (SELECT ...) TO variant.")));
    else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy from foreign table \"%s\"", RelationGetRelationName(rel)),
                    errhint("Try the COPY (SELECT ...) TO variant.")));
    } else if (rel->rd_rel->relkind == RELKIND_STREAM) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from stream \"%s\"", RelationGetRelationName(rel)),
                errhint("Try the COPY (SELECT ...) TO variant.")));
    } else if (RELKIND_IS_SEQUENCE(rel->rd_rel->relkind))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from (large) sequence \"%s\"", RelationGetRelationName(rel))));
    else
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot copy from non-table relation \"%s\"", RelationGetRelationName(rel))));

}
/*
 * Setup CopyState to read tuples from a table or a query for COPY TO.
 */
CopyState BeginCopyTo(
    Relation rel, Node* query, const char* queryString, const char* filename, List* attnamelist, List* options)
{
    CopyState cstate;
    bool pipe = (filename == NULL);
    MemoryContext oldcontext;
    bool flag = rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION;

    if (flag) {
        CopyToCheck(rel);
    }

    cstate = BeginCopy(false, rel, query, queryString, attnamelist, options);
    oldcontext = MemoryContextSwitchTo(cstate->copycontext);

    if (pipe) {
        if (t_thrd.postgres_cxt.whereToSendOutput != DestRemote)
            cstate->copy_file = stdout;
    } else {
        struct stat st;
        bool dirIsExist = false;
        struct stat checkdir;
        mode_t oumask; /* Pre-existing umask value */

        /*
         * Prevent write to relative path ... too easy to shoot oneself in the
         * foot by overwriting a database file ...
         */
        if (!is_absolute_path(filename))
            ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("relative path not allowed for COPY to file")));

        cstate->filename = pstrdup(filename);
        if (!u_sess->sec_cxt.last_roleid_is_super && CheckCopyFileInBlackList(cstate->filename)) {
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("do not support system admin to COPY to %s", filename)));
        }
        if (stat(cstate->filename, &checkdir) == 0)
            dirIsExist = true;

        CopyToFileSecurityChecks(filename);
        oumask = umask(S_IWGRP | S_IWOTH);
        PG_TRY();
        {
            cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
        }
        PG_CATCH();
        {
            (void)umask(oumask);
            PG_RE_THROW();
        }
        PG_END_TRY();
        (void)umask(oumask);

        if (cstate->copy_file == NULL)
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open file \"%s\" for writing: %m", cstate->filename)));

        fstat(fileno(cstate->copy_file), &st);
        if (S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a directory", cstate->filename)));
        if (!dirIsExist) {
            if (fchmod(fileno(cstate->copy_file), S_IRUSR | S_IWUSR) < 0)
                ereport(
                    ERROR, (errcode_for_file_access(), errmsg("could not chmod file \"%s\" : %m", cstate->filename)));
        }
    }

    cstate->writelineFunc = CopySendEndOfRow<false>;
    (void)MemoryContextSwitchTo(oldcontext);

    return cstate;
}

/*
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
uint64 DoCopyTo(CopyState cstate)
{
    bool pipe = (cstate->filename == NULL);
    bool fe_copy = (pipe && t_thrd.postgres_cxt.whereToSendOutput == DestRemote);
    uint64 processed = 0;

    PG_TRY();
    {
        if (fe_copy)
            SendCopyBegin(cstate);

        processed = CopyToCompatiblePartions(cstate);

        if (fe_copy)
            SendCopyEnd(cstate);
    }
    PG_CATCH();
    {
        /*
         * Make sure we turn off old-style COPY OUT mode upon error. It is
         * okay to do this in all cases, since it does nothing if the mode is
         * not on.
         */
        pq_endcopyout(true);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return processed;
}

/*
 * Clean up storage and release resources for COPY TO.
 */
void EndCopyTo(CopyState cstate)
{
    if (cstate->queryDesc != NULL) {
        /* Close down the query and free resources. */
        ExecutorFinish(cstate->queryDesc);
        ExecutorEnd(cstate->queryDesc);
        FreeQueryDesc(cstate->queryDesc);
        PopActiveSnapshot();
    }

    /* Clean up storage */
    EndCopy(cstate);
    cstate = NULL;
}

static void DeformCopyTuple(
    MemoryContext perBatchMcxt, CopyState cstate, VectorBatch* batch, TupleDesc tupDesc, Datum* values, bool* nulls)
{
    errno_t rc = EOK;
    Form_pg_attribute* attrs = tupDesc->attrs;

    // deform values from vectorbatch.
    for (int nrow = 0; nrow < batch->m_rows; nrow++) {
        rc = memset_s(nulls, sizeof(bool) * tupDesc->natts, 0, sizeof(bool) * tupDesc->natts);
        securec_check(rc, "\0", "\0");

        for (int ncol = 0; ncol < batch->m_cols; ncol++) {
            /* If the column is dropped, skip deform values from vectorbatch */
            if (attrs[ncol]->attisdropped) {
                continue;
            }
            ScalarVector* pVec = &(batch->m_arr[ncol]);
            ScalarValue* pVal = pVec->m_vals;

            if (pVec->IsNull(nrow)) {
                nulls[ncol] = true;
                continue;
            }

            if (pVec->m_desc.encoded) {
                Datum val = ScalarVector::Decode(pVal[nrow]);
                MemoryContext oldmcxt = MemoryContextSwitchTo(perBatchMcxt);

                if (attrs[ncol]->attlen > 8) {
                    char* result = NULL;
                    result = (char*)val + VARHDRSZ_SHORT;
                    values[ncol] = datumCopy(PointerGetDatum(result), attrs[ncol]->attbyval, attrs[ncol]->attlen);
                } else
                    values[ncol] = datumCopy(val, attrs[ncol]->attbyval, attrs[ncol]->attlen);

                MemoryContextSwitchTo(oldmcxt);
            } else
                values[ncol] = pVal[nrow];
        }

        // Check interrupts
        CHECK_FOR_INTERRUPTS();

        /* Format and send the data */
        CopyOneRowTo(cstate, InvalidOid, values, nulls);
    }

    MemoryContextReset(perBatchMcxt);
}

static uint64 DFSCopyTo(CopyState cstate, TupleDesc tupDesc, Datum* values, bool* nulls)
{
    TableScanDesc scandesc = NULL;
    HeapTuple tuple = NULL;
    Relation delta = NULL;
    DfsScanState* scanState = NULL;
    VectorBatch* batch = NULL;

    /* Copy the data on dfs to local file. */
    MemoryContext perBatchMcxt = AllocSetContextCreate(CurrentMemoryContext,
        "COPY TO PER BATCH FOR DFS TABLE",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    uint64 processed = 0;
    scanState = dfs::reader::DFSBeginScan(cstate->curPartionRel, NIL, 0, NULL);

    do {
        CHECK_FOR_INTERRUPTS();
        batch = dfs::reader::DFSGetNextBatch(scanState);
        if (BatchIsNull(batch))
            break;
        DeformCopyTuple(perBatchMcxt, cstate, batch, tupDesc, values, nulls);
        processed += batch->m_rows;
    } while (true);

    dfs::reader::DFSEndScan(scanState);
    MemoryContextDelete(perBatchMcxt);
    pfree_ext(scanState);

    delta = heap_open(cstate->curPartionRel->rd_rel->reldeltarelid, AccessShareLock);
    scandesc = tableam_scan_begin(delta, GetActiveSnapshot(), 0, NULL);

    /* Copy the data on delta table to local file. */
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        /* Deconstruct the tuple ... faster than repeated heap_getattr */
        heap_deform_tuple2(tuple, tupDesc, values, nulls, scandesc->rs_cbuf);

        /* Format and send the data */
        CopyOneRowTo(cstate, HeapTupleGetOid(tuple), values, nulls);
        processed++;
    }

    tableam_scan_end(scandesc);
    heap_close(delta, AccessShareLock);

    return processed;
}

/*
 * Brief        : CopyTo function for CStore
 * Input        : cstate: copystate descriptor of current copy-to command
 *              : tupleDesc: TupleDesc for target relation
 *				: values/nulls: value and null array pass to Tuple level copy
 *                              function, have to be pre-allocated
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static uint64 CStoreCopyTo(CopyState cstate, TupleDesc tupDesc, Datum* values, bool* nulls)
{
    CStoreScanDesc scandesc;
    VectorBatch* batch = NULL;
    int16* colIdx = (int16*)palloc0(sizeof(int16) * tupDesc->natts);
    Form_pg_attribute* attrs = tupDesc->attrs;
    MemoryContext perBatchMcxt = AllocSetContextCreate(CurrentMemoryContext,
        "COPY TO PER BATCH",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    uint64 processed = 0;

    for (int i = 0; i < tupDesc->natts; i++)
        colIdx[i] = attrs[i]->attnum;

    scandesc = CStoreBeginScan(cstate->curPartionRel, tupDesc->natts, colIdx, GetActiveSnapshot(), true);

    do {
        batch = CStoreGetNextBatch(scandesc);
        DeformCopyTuple(perBatchMcxt, cstate, batch, tupDesc, values, nulls);
        processed += batch->m_rows;
    } while (!CStoreIsEndScan(scandesc));

    /* End cstore scan and destroy MemroyContext. */
    CStoreEndScan(scandesc);
    MemoryContextDelete(perBatchMcxt);

    return processed;
}

/*
 * Copy from relation or query TO file.
 */
static uint64 CopyTo(CopyState cstate, bool isFirst, bool isLast)
{
    TupleDesc tupDesc;
    int num_phys_attrs;
    Form_pg_attribute* attr = NULL;
    ListCell* cur = NULL;
    uint64 processed;

#ifdef PGXC
    /* Send COPY command to datanode */
    if (IS_PGXC_COORDINATOR && cstate->remoteCopyState && cstate->remoteCopyState->rel_loc)
        pgxc_datanode_copybegin(cstate->remoteCopyState, NULL);
#endif

    if (cstate->curPartionRel)
        tupDesc = RelationGetDescr(cstate->curPartionRel);
    else
        tupDesc = cstate->queryDesc->tupDesc;
    attr = tupDesc->attrs;
    num_phys_attrs = tupDesc->natts;
    cstate->null_print_client = cstate->null_print; /* default */

    /* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
    if (cstate->fe_msgbuf == NULL) {
        cstate->fe_msgbuf = makeStringInfo();
        if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)
            ProcessFileHeader(cstate);
    }
    /* Get info about the columns we need to process. */
    cstate->out_functions = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
    foreach (cur, cstate->attnumlist) {
        int attnum = lfirst_int(cur);
        Oid out_func_oid;
        bool isvarlena = false;

        if (IS_BINARY(cstate))
            getTypeBinaryOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
        else
            getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
        fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
    }

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway.	(We don't need a whole econtext as CopyFrom does.)
     */
    cstate->rowcontext = AllocSetContextCreate(
        CurrentMemoryContext, "COPY TO", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    if (IS_BINARY(cstate)) {
#ifdef PGXC
        if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && isFirst) {
#endif
            /* Generate header for a binary copy */
            int32 tmp;

            /* Signature */
            CopySendData(cstate, BinarySignature, 11);
            /* Flags field */
            tmp = 0;
            if (cstate->oids)
                tmp |= (1 << 16);
            CopySendInt32(cstate, tmp);
            /* No header extension */
            tmp = 0;
            CopySendInt32(cstate, tmp);

#ifdef PGXC
            /* Need to flush out the trailer */
            CopySendEndOfRow<false>(cstate);
        }
#endif
    } else {
        /*
         * For non-binary copy, we need to convert null_print to file
         * encoding, because it will be sent directly with CopySendString.
         */
        if (cstate->need_transcoding)
            cstate->null_print_client =
                pg_server_to_any(cstate->null_print, cstate->null_print_len, cstate->file_encoding);

        /* if a header has been requested send the line */
        if (cstate->header_line && isFirst && cstate->headerString != NULL) {
            appendBinaryStringInfo(cstate->fe_msgbuf, cstate->headerString->data, cstate->headerString->len);
            CopySendEndOfRow<true>(cstate);
        }
    }

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && cstate->remoteCopyState && cstate->remoteCopyState->rel_loc) {
        RemoteCopyData* remoteCopyState = cstate->remoteCopyState;
        RemoteCopyType remoteCopyType;
        ExecNodes* en = NULL;

        /* Set up remote COPY to correct operation */
        if (cstate->copy_dest == COPY_FILE)
            remoteCopyType = REMOTE_COPY_FILE;
        else
            remoteCopyType = REMOTE_COPY_STDOUT;

        en = GetRelationNodes(remoteCopyState->rel_loc, NULL, NULL, NULL, NULL, RELATION_ACCESS_READ, false);

        /*
         * In case of a read from a replicated table GetRelationNodes
         * returns all nodes and expects that the planner can choose
         * one depending on the rest of the join tree
         * Here we should choose the preferred node in the list and
         * that should suffice.
         * If we do not do so system crashes on
         * COPY replicated_table (a, b) TO stdout;
         * and this makes pg_dump fail for any database
         * containing such a table.
         */
        if (IsLocatorReplicated(remoteCopyState->rel_loc->locatorType))
            en->nodeList = list_copy(remoteCopyState->exec_nodes->nodeList);

        /*
         * We don't know the value of the distribution column value, so need to
         * read from all nodes. Hence indicate that the value is NULL.
         */
        u_sess->cmd_cxt.dest_encoding_for_copytofile = cstate->file_encoding;
        u_sess->cmd_cxt.need_transcoding_for_copytofile = cstate->need_transcoding;
        processed = DataNodeCopyOut(en, remoteCopyState->connections, NULL, cstate->copy_file, NULL, remoteCopyType);
    } else {
#endif

        if (cstate->curPartionRel) {
            Datum* values = NULL;
            bool* nulls = NULL;

            values = (Datum*)palloc(num_phys_attrs * sizeof(Datum));
            nulls = (bool*)palloc(num_phys_attrs * sizeof(bool));
            processed = 0;

            if (RelationIsColStore(cstate->rel)) {
                if (RelationIsPAXFormat(cstate->rel)) {
                    processed = DFSCopyTo(cstate, tupDesc, values, nulls);
                } else {
                    processed = CStoreCopyTo(cstate, tupDesc, values, nulls);
                }
            // XXXTAM: WE need to refactor this once we know what to do 
            // with heap_deform_tuple2 in the heap case
            } else {
                Tuple tuple;
                TableScanDesc scandesc;
                scandesc = scan_handler_tbl_beginscan(cstate->curPartionRel, GetActiveSnapshot(), 0, NULL);
                while ((tuple = scan_handler_tbl_getnext(scandesc, ForwardScanDirection, cstate->curPartionRel)) != NULL) {
                    CHECK_FOR_INTERRUPTS();
                
                    /* Deconstruct the tuple ... faster than repeated heap_getattr */
                    tableam_tops_deform_tuple2(tuple, tupDesc, values, nulls, GetTableScanDesc(scandesc, cstate->curPartionRel)->rs_cbuf);
                    if (((HeapTuple)tuple)->tupTableType == HEAP_TUPLE) {
                        CopyOneRowTo(cstate, HeapTupleGetOid((HeapTuple)tuple), values, nulls);
                    } else {
                        CopyOneRowTo(cstate, InvalidOid, values, nulls);
                    }
                    processed++;
                }

                scan_handler_tbl_endscan(scandesc);
            }
            pfree_ext(values);
            pfree_ext(nulls);
        } else {
            /* run the plan --- the dest receiver will send tuples */
            ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L);
            processed = ((DR_copy*)cstate->queryDesc->dest)->processed;
        }

#ifdef PGXC
    }
#endif

#ifdef PGXC
    /*
     * In PGXC, it is not necessary for a Datanode to generate
     * the trailer as Coordinator is in charge of it
     */
    if (IS_BINARY(cstate) && IS_PGXC_COORDINATOR && isLast) {
#else
    if (IS_BINARY(cstate) && isLast) {
#endif
        /* Generate trailer for a binary copy */
        CopySendInt16(cstate, -1);
        /* Need to flush out the trailer */
        CopySendEndOfRow<false>(cstate);
    }

    MemoryContextDelete(cstate->rowcontext);

    return processed;
}

/*
 * Just like CopyTo ,Copy from relation (including partion relation ) or query TO file.
 */
static uint64 CopyToCompatiblePartions(CopyState cstate)
{
    uint64 processed = 0;

    /* Partion relaton */
#ifdef PGXC
    /*
     * In PGXC, it is not necessary for a Datanode to generate
     * the trailer as Coordinator is in charge of it
     */
    if (cstate->rel != NULL && RELATION_IS_PARTITIONED(cstate->rel) && IS_PGXC_DATANODE) {
#else
    if (cstate->rel != NULL && RELATION_IS_PARTITIONED(cstate->rel)) {
#endif
        Partition partition = NULL;
        List* partitionList = NULL;
        ListCell* cell = NULL;
        Relation partionRel = NULL;
        bool isLast = false;
        bool isFirst = true;

        partitionList = relationGetPartitionList(cstate->rel, AccessShareLock);

        foreach (cell, partitionList) {

            partition = (Partition)lfirst(cell);

            partionRel = partitionGetRelation(cstate->rel, partition);

            if (RelationIsSubPartitioned(cstate->rel)) {
                ListCell* lc = NULL;
                List* subPartitionList = relationGetPartitionList(partionRel, AccessShareLock);
                foreach (lc, subPartitionList) {
                    Partition subPartition = (Partition)lfirst(lc);
                    Relation subPartionRel = partitionGetRelation(partionRel, subPartition);
                    cstate->curPartionRel = subPartionRel;
                    if (NULL == lnext(cell) && NULL == lnext(lc)) {
                        isLast = true;
                    }
                    processed += CopyTo(cstate, isFirst, isLast);
                    isFirst = false;
                    releaseDummyRelation(&subPartionRel);
                }
                releasePartitionList(partionRel, &subPartitionList, AccessShareLock);
                releaseDummyRelation(&partionRel);
            } else {
                cstate->curPartionRel = partionRel;
                if (NULL == lnext(cell)) {
                    isLast = true;
                }
                processed += CopyTo(cstate, isFirst, isLast);
                isFirst = false;
                releaseDummyRelation(&partionRel);
            }
        }

        if (partitionList != NIL) {
            releasePartitionList(cstate->rel, &partitionList, AccessShareLock);
        }
    } else {
        cstate->curPartionRel = cstate->rel;
        processed = CopyTo(cstate, true, true);
    }

    return processed;
}

/*
 * Emit one row during CopyTo().
 */
void CopyOneRowTo(CopyState cstate, Oid tupleOid, Datum* values, const bool* nulls)
{
    bool need_delim = false;
    FmgrInfo* out_functions = cstate->out_functions;
    MemoryContext oldcontext;
    ListCell* cur = NULL;
    char* string = NULL;

    MemoryContextReset(cstate->rowcontext);
    oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

    if (IS_BINARY(cstate)) {
        /* Binary per-tuple header */
        CopySendInt16(cstate, list_length(cstate->attnumlist));
        /* Send OID if wanted --- note attnumlist doesn't include it */
        if (cstate->oids) {
            /* Hack --- assume Oid is same size as int32 */
            CopySendInt32(cstate, sizeof(int32));
            CopySendInt32(cstate, tupleOid);
        }
    } else if (cstate->oids){
        /* Text format has no per-tuple header, but send OID if wanted */
        /* Assume digits don't need any quoting or encoding conversion */
        string = DatumGetCString(DirectFunctionCall1(oidout, ObjectIdGetDatum(tupleOid)));
        CopySendString(cstate, string);
        need_delim = true;
    }

    if (IS_FIXED(cstate))
        FixedRowOut(cstate, values, nulls);
    else {
        foreach (cur, cstate->attnumlist) {
            int attnum = lfirst_int(cur);
            Datum value = values[attnum - 1];
            bool isnull = nulls[attnum - 1];

            if (cstate->fileformat == FORMAT_CSV || cstate->fileformat == FORMAT_TEXT) {
                if (need_delim)
                    CopySendString(cstate, cstate->delim);
                need_delim = true;
            }

            if (isnull) {
                switch (cstate->fileformat) {
                    case FORMAT_CSV:
                    case FORMAT_TEXT:
                        CopySendString(cstate, cstate->null_print_client);
                        break;
                    case FORMAT_BINARY:
                        CopySendInt32(cstate, -1);
                        break;
                    default:
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Invalid file format")));
                }
            } else {
                if (!IS_BINARY(cstate)) {
                    string = OutputFunctionCall(&out_functions[attnum - 1], value);
                    switch (cstate->fileformat) {
                        case FORMAT_CSV:
                            CopyAttributeOutCSV(cstate,
                                string,
                                cstate->force_quote_flags[attnum - 1],
                                list_length(cstate->attnumlist) == 1);
                            break;
                        case FORMAT_TEXT:
                            CopyAttributeOutText(cstate, string);
                            break;
                        default:
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Invalid file format")));
                    }
                } else {
                    bytea* outputbytes = NULL;

                    outputbytes = SendFunctionCall(&out_functions[attnum - 1], value);
                    CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
                    CopySendData(cstate, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
                }
            }
        }
    }

    cstate->writelineFunc(cstate);

    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyState.
 */
void CopyFromErrorCallback(void* arg)
{
    CopyState cstate = (CopyState)arg;

    if (IS_BINARY(cstate)) {
        /* can't usefully display the data */
        if (cstate->cur_attname)
            (void)errcontext(
                "COPY %s, line %u, column %s", cstate->cur_relname, cstate->cur_lineno, cstate->cur_attname);
        else
            (void)errcontext("COPY %s, line %u", cstate->cur_relname, cstate->cur_lineno);
    } else {
        if (cstate->cur_attname && cstate->cur_attval) {
            /* error is relevant to a particular column */
            char* attval = NULL;

            attval = limit_printout_length(cstate->cur_attval);
            (void)errcontext("COPY %s, line %u, column %s: \"%s\"",
                cstate->cur_relname,
                cstate->cur_lineno,
                cstate->cur_attname,
                attval);
            pfree_ext(attval);
        } else if (cstate->cur_attname) {
            /* error is relevant to a particular column, value is NULL */
            (void)errcontext("COPY %s, line %u, column %s: null input",
                cstate->cur_relname,
                cstate->cur_lineno,
                cstate->cur_attname);
        } else {
            /* error is relevant to a particular line */
            if (cstate->line_buf_converted || !cstate->need_transcoding) {
                char* lineval = NULL;

                lineval = limit_printout_length(cstate->line_buf.data);
                (void)errcontext("COPY %s, line %u: \"%s\"", cstate->cur_relname, cstate->cur_lineno, lineval);
                pfree_ext(lineval);
            } else {
                /*
                 * Here, the line buffer is still in a foreign encoding, and
                 * indeed it's quite likely that the error is precisely a
                 * failure to do encoding conversion (ie, bad data).  We dare
                 * not try to convert it, and at present there's no way to
                 * regurgitate it without conversion.  So we have to punt and
                 * just report the line number.
                 */
                (void)errcontext("COPY %s, line %u", cstate->cur_relname, cstate->cur_lineno);
            }
        }
    }
}

void BulkloadErrorCallback(void* arg)
{
    CopyState cstate = (CopyState)arg;

    if (IS_BINARY(cstate)) {
        /* can't usefully display the data */
        if (cstate->cur_attname)
            (void)errcontext(
                "LOAD %s, line %u, column %s", cstate->cur_relname, cstate->cur_lineno, cstate->cur_attname);
        else
            (void)errcontext("LOAD %s, line %u", cstate->cur_relname, cstate->cur_lineno);
    } else {
        if (cstate->cur_attname && cstate->cur_attval) {
            /* error is relevant to a particular column */
            char* attval = NULL;
            char* lineval = NULL;

            attval = limit_printout_length(cstate->cur_attval);
            lineval = limit_printout_length(cstate->line_buf.data);
            (void)errcontext("LOAD %s, line %u, column %s: \"%s\", line:\"%s\"",
                cstate->cur_relname,
                cstate->cur_lineno,
                cstate->cur_attname,
                attval,
                lineval);
            pfree_ext(attval);
            pfree_ext(lineval);
        } else if (cstate->cur_attname) {
            char* lineval = NULL;

            lineval = limit_printout_length(cstate->line_buf.data);
            /* error is relevant to a particular column, value is NULL */
            (void)errcontext("LOAD %s, line %u, column %s: null input, line:\"%s\"",
                cstate->cur_relname,
                cstate->cur_lineno,
                cstate->cur_attname,
                lineval);
        } else {
            /* error is relevant to a particular line */
            if (cstate->line_buf_converted || !cstate->need_transcoding) {
                char* lineval = NULL;

                lineval = limit_printout_length(cstate->line_buf.data);
                (void)errcontext("LOAD %s, line %u: \"%s\"", cstate->cur_relname, cstate->cur_lineno, lineval);
                pfree_ext(lineval);
            } else {
                /*
                 * Here, the line buffer is still in a foreign encoding, and
                 * indeed it's quite likely that the error is precisely a
                 * failure to do encoding conversion (ie, bad data).  We dare
                 * not try to convert it, and at present there's no way to
                 * regurgitate it without conversion.  So we have to punt and
                 * just report the line number.
                 */
                (void)errcontext("LOAD %s, line %u", cstate->cur_relname, cstate->cur_lineno);
            }
        }
    }
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * It would seem a lot easier to just use the sprintf "precision" limit to
 * truncate the string.  However, some versions of glibc have a bug/misfeature
 * that vsnprintf will always fail (return -1) if it is asked to truncate
 * a string that contains invalid byte sequences for the current encoding.
 * So, do our own truncation.  We return a pstrdup'd copy of the input.
 */
char* limit_printout_length(const char* str)
{
#define MAX_COPY_DATA_DISPLAY 1024

    int slen = strlen(str);
    int len;
    int msg_len;
    char* res = NULL;
    const char* msg = "...";
    errno_t rc = EOK;

    /* Fast path if definitely okay */
    if (slen <= MAX_COPY_DATA_DISPLAY)
        return pstrdup(str);

    /* Apply encoding-dependent truncation */
    len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

    /*
     * Truncate, and add "..." to show we truncated the input.
     */
    msg_len = 4;
    res = (char*)palloc(len + msg_len);
    rc = memcpy_s(res, len + msg_len, str, len);
    securec_check(rc, "\0", "\0");
    rc = strcpy_s(res + len, msg_len, msg);
    securec_check(rc, "\0", "\0");

    return res;
}

static void newMemCxt(CopyFromManager mgr, int num)
{
    int cnt;
    MemoryContext oldCxt = NULL;
    CopyFromMemCxt copyFromMemCxt = NULL;

    Assert((mgr != NULL) && (mgr->parent != NULL));
    Assert((mgr->numMemCxt <= mgr->maxMemCxt) && (mgr->maxMemCxt <= MAX_MEMCXT_NUM));
    Assert(num > 0);
    num = Min(num, (MAX_MEMCXT_NUM - mgr->numMemCxt));

    oldCxt = MemoryContextSwitchTo(mgr->parent);

    if (num > mgr->maxMemCxt - mgr->numMemCxt) {
        mgr->memCxt = (CopyFromMemCxt*)repalloc(mgr->memCxt, sizeof(CopyFromMemCxt) * mgr->maxMemCxt * 2);
        mgr->maxMemCxt *= 2;
    }

    for (cnt = 0; cnt < num; cnt++) {
        mgr->memCxt[mgr->numMemCxt] = copyFromMemCxt = (CopyFromMemCxt)palloc(sizeof(CopyFromMemCxtData));
        copyFromMemCxt->memCxtCandidate = AllocSetContextCreate(
            mgr->parent, "BulkMemory", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
        copyFromMemCxt->memCxtSize = 0;
        copyFromMemCxt->nextBulk = 0;
        copyFromMemCxt->id = mgr->numMemCxt;

        mgr->freeMemCxt[mgr->numFreeMemCxt++] = copyFromMemCxt->id;
        mgr->numMemCxt++;
    }

    (void)MemoryContextSwitchTo(oldCxt);
}

CopyFromManager initCopyFromManager(MemoryContext parent, Relation heapRel, bool isInsertSelect)
{
    MemoryContext oldCxt = NULL;
    CopyFromManager mgr = NULL;
    HASHCTL hashCtrl;
    int flags;
    errno_t rc = 0;

    Assert(parent != NULL);
    oldCxt = MemoryContextSwitchTo(parent);

    mgr = (CopyFromManager)palloc0(sizeof(CopyFromManagerData));
    mgr->parent = parent;
    mgr->isPartRel = RELATION_IS_PARTITIONED(heapRel) || RELATION_OWN_BUCKET(heapRel);
    if (!mgr->isPartRel) {
        CopyFromBulk bulk;
        
        mgr->bulk = bulk =
            (CopyFromBulk)palloc(sizeof(CopyFromBulkData) + sizeof(Tuple) * MAX_BUFFERED_TUPLES);

        mgr->LastFlush = false;

        bulk->maxTuples = MAX_BUFFERED_TUPLES;
        if (isInsertSelect) {
            CopyFromMemCxt copyFromMemCxt = NULL;
            mgr->numMemCxt = 0;
            mgr->numFreeMemCxt = 0;
            mgr->maxMemCxt = 0;
            mgr->memCxt = NULL;
            mgr->switchKey.partOid = InvalidOid;
            mgr->switchKey.bucketId = InvalidBktId;
            bulk->memCxt = copyFromMemCxt = (CopyFromMemCxt)palloc(sizeof(CopyFromMemCxtData));
            copyFromMemCxt->memCxtCandidate = AllocSetContextCreate(mgr->parent,
                "BulkMemory",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE);
            copyFromMemCxt->memCxtSize = 0;
            copyFromMemCxt->nextBulk = 0;
            copyFromMemCxt->id = 0;
        } else {
            bulk->memCxt = NULL;
        }

        bulk->tuples = (Tuple *)((char *)bulk + sizeof(CopyFromBulkData));

        bulk->numTuples = 0;
        bulk->partOid = InvalidOid;
        bulk->bucketId = InvalidBktId;
        bulk->sizeTuples = 0;
    } else {
        mgr->numMemCxt = 0;
        mgr->numFreeMemCxt = 0;
        mgr->nextMemCxt = 0;
        mgr->maxMemCxt = MIN_MEMCXT_NUM * 2;
        mgr->memCxt = (CopyFromMemCxt*)palloc(sizeof(CopyFromMemCxt) * mgr->maxMemCxt);
        newMemCxt(mgr, MIN_MEMCXT_NUM);
        Assert(mgr->numMemCxt == MIN_MEMCXT_NUM);

        rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
        securec_check(rc, "", "");
        hashCtrl.hcxt = (MemoryContext)parent;
        hashCtrl.hash = tag_hash;
        hashCtrl.keysize = sizeof(CopyFromBulkKey);
        hashCtrl.entrysize = sizeof(CopyFromBulkData);
        flags = (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM);
        mgr->hash = hash_create("CopyFromHashTable", mgr->maxMemCxt, &hashCtrl, flags);

        mgr->switchKey.partOid = InvalidOid;
        mgr->switchKey.bucketId = InvalidBktId;
    }

    (void)MemoryContextSwitchTo(oldCxt);
    return mgr;
}

CopyFromMemCxt findMemCxt(CopyFromManager mgr, bool* found)
{
    int idx;
    int cnt;
    int fullest;
    bool hasFreeMemCxt = false;
    bool needNewMemCxt = false;
    uint32 preSize;
    CopyFromMemCxt copyFromMemCxt = NULL;

    Assert(mgr != NULL);
    *found = true;
    hasFreeMemCxt = mgr->numFreeMemCxt > 0;
    needNewMemCxt = !hasFreeMemCxt && (mgr->numMemCxt < MAX_MEMCXT_NUM);

    if (hasFreeMemCxt || needNewMemCxt) {
        if (needNewMemCxt) {
            newMemCxt(mgr, 1);
        }

        /* get a empty MemCxt */
        mgr->numFreeMemCxt--;
        idx = mgr->freeMemCxt[mgr->numFreeMemCxt];
        Assert(idx < mgr->numMemCxt);
        Assert(mgr->memCxt[idx]->nextBulk < PART_NUM_PER_MEMCXT);
        return mgr->memCxt[idx];
    }

    /* get next avaliable MemCxt */
    fullest = idx = mgr->nextMemCxt;
    preSize = mgr->memCxt[fullest]->memCxtSize;
    for (cnt = 0; cnt < MAX_MEMCXT_NUM; cnt++) {
        copyFromMemCxt = mgr->memCxt[idx];
        if (copyFromMemCxt->nextBulk < PART_NUM_PER_MEMCXT &&
            MAX_SIZE_PER_MEMCXT - copyFromMemCxt->memCxtSize > LMT_SIZE_PER_PART) {
            mgr->nextMemCxt = idx;
            return copyFromMemCxt;
        }

        if (copyFromMemCxt->memCxtSize > preSize) {
            fullest = idx;
            preSize = copyFromMemCxt->memCxtSize;
        }

        idx = (idx + 1) % MAX_MEMCXT_NUM;
    }

    /* case4: no MemCxt is usable, flush it and recycle */
    *found = false;
    Assert(fullest >= 0 && fullest < MAX_MEMCXT_NUM);
    copyFromMemCxt = mgr->memCxt[fullest];
    return copyFromMemCxt;
}

CopyFromBulk findBulk(CopyFromManager mgr, Oid partOid, int2 bucketId, bool* toFlush)
{
    CopyFromBulk bulk = NULL;
    CopyFromBulkKey key = {partOid, bucketId};
    MemoryContext oldCxt = NULL;
    bool found = false;

    Assert(mgr != NULL);
    *toFlush = false;

    /* case1: without partition tables */
    if (!mgr->isPartRel && bucketId == InvalidBktId) {
        return mgr->bulk;
    }

    /* case2: a existing bulk is found */
    bulk = (CopyFromBulk)hash_search(mgr->hash, &key, HASH_ENTER, &found);
    if (found) {
        Assert(bulk->memCxt != NULL);
        Assert(partOid == bulk->partOid);
        Assert(bucketId == bulk->bucketId);
        return bulk;
    }

    /* case3: a new bulk is allocated */
    bulk->partOid = InvalidOid;
    bulk->bucketId = InvalidBktId;
    bulk->sizeTuples = 0;
    bulk->numTuples = 0;
    bulk->maxTuples = -1;

    bulk->memCxt = findMemCxt(mgr, &found);
    if (found) {
        CopyFromMemCxt copyFromMemCxt = bulk->memCxt;
        Assert(copyFromMemCxt->nextBulk < PART_NUM_PER_MEMCXT);
        copyFromMemCxt->chunk[copyFromMemCxt->nextBulk++] = bulk;

        bulk->partOid = partOid;
        bulk->bucketId = bucketId;
        bulk->maxTuples = DEF_BUFFERED_TUPLES;
        oldCxt = MemoryContextSwitchTo(copyFromMemCxt->memCxtCandidate);

        bulk->tuples = (Tuple*)palloc(sizeof(Tuple) * bulk->maxTuples);
        
        (void)MemoryContextSwitchTo(oldCxt);

        return bulk;
    }

    /* case4: have to flush and recycle the fullest bulk */
    *toFlush = true;
    mgr->switchKey.partOid = partOid;
    mgr->switchKey.bucketId = bucketId;
    return bulk;
}

bool isBulkFull(CopyFromBulk bulk)
{
    CopyFromMemCxt copyFromMemCxt = bulk->memCxt;

    if (copyFromMemCxt == NULL) {
        Assert(InvalidOid == bulk->partOid);
        return ((bulk->numTuples >= MAX_BUFFERED_TUPLES) || (bulk->sizeTuples >= MAX_TUPLES_SIZE));
    }

    if (copyFromMemCxt->memCxtSize > MAX_SIZE_PER_MEMCXT) {
        return true;
    }

    return false;
}

void CopyFromBulkInsert(EState* estate, CopyFromBulk bulk, PageCompress* pcState, CommandId mycid, int hi_options,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, bool forceToFlush, BulkInsertState bistate)
{
    Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
    Relation heaprel = NULL;
    Relation insertRel = resultRelationDesc;
    Partition partition = NULL;
    int nInsertedTuples = 0;

    bool isCompressed = RowRelationIsCompressed(resultRelationDesc);
    bool isPartitional = RELATION_IS_PARTITIONED(resultRelationDesc);

    /* step 1: open PARTITION relation */
    if (isPartitional) {
        searchFakeReationForPartitionOid(estate->esfRelations,
            estate->es_query_cxt,
            resultRelationDesc,
            bulk->partOid,
            heaprel,
            partition,
            RowExclusiveLock);
        estate->esCurrentPartition = heaprel;
        insertRel = heaprel;
    }

    /* step 2: bulk insert */
    Assert(isPartitional == OidIsValid(insertRel->parentId));
    if (isCompressed) {
        nInsertedTuples = CopyFromCompressAndInsertBatch(pcState,
            estate,
            mycid,
            hi_options,
            resultRelInfo,
            myslot,
            bistate,
            bulk->numTuples,
            (HeapTuple*)bulk->tuples,
            forceToFlush,
            insertRel,
            partition,
            bulk->bucketId);
        Assert(!forceToFlush || (nInsertedTuples == bulk->numTuples));

        /* Note: There are two cases:
         * case 1: all input tuples are inserted, so that
         *		1) *nBufferedTuples = 0;
         *		2) *bufferedTuplesSize = 0;
         * case 2: most of input tuples are inserted but the others are moved in the front of bulk. In fact
         *		1) *nBufferedTuples != 0;
         *		2) *bufferedTuplesSize != 0;
         *		But we use a tricky here. move the rest in the front of bulk, and record the number. Instead,
         *		reset *bufferedTuplesSize to 0, which results in use of some more memory, without failure
         *		of inseting or absence of any tuple.
         */
        bulk->numTuples -= nInsertedTuples;
        bulk->sizeTuples = 0;
    } else {
        tableam_tops_copy_from_insert_batch(insertRel,
            estate,
            mycid,
            hi_options,
            resultRelInfo,
            myslot,
            bistate,
            bulk->numTuples,
            (Tuple*)bulk->tuples,
            partition,
            bulk->bucketId);
        bulk->numTuples = 0;
        bulk->sizeTuples = 0;
    }
}

void deinitCopyFromManager(CopyFromManager mgr)
{
    if (mgr == NULL) {
        return;
    }

    if (!mgr->isPartRel) {
        CopyFromBulk bulk = mgr->bulk;
        pfree_ext(bulk);
    } else {
        pfree_ext(mgr->memCxt);
        hash_destroy(mgr->hash);
    }

    pfree_ext(mgr);
}

static void CStoreCopyConstraintsCheck(ResultRelInfo* resultRelInfo, Datum* values, bool* nulls, EState* estate)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupDesc = RelationGetDescr(rel);
    TupleConstr* constr = rel->rd_att->constr;

    Assert(constr);
    if (constr->has_not_null) {
        int natts = rel->rd_att->natts;
        int attrChk;

        for (attrChk = 1; attrChk <= natts; attrChk++) {
            if (rel->rd_att->attrs[attrChk - 1]->attnotnull && nulls[attrChk - 1]) {
                HeapTuple tuple = heap_form_tuple(tupDesc, values, nulls);
                TupleTableSlot* slot = ExecInitExtraTupleSlot(estate);
                Bitmapset* modifiedCols = NULL;
                Bitmapset* insertedCols = NULL;
                Bitmapset* updatedCols = NULL;
                char* val_desc = NULL;

                ExecSetSlotDescriptor(slot, tupDesc);
                (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);

                insertedCols = GetInsertedColumns(resultRelInfo, estate);
                updatedCols = GetUpdatedColumns(resultRelInfo, estate);
                modifiedCols = bms_union(insertedCols, updatedCols);
                val_desc = ExecBuildSlotValueDescription(RelationGetRelid(rel), slot, tupDesc, modifiedCols, 64);
                ereport(ERROR,
                    (errcode(ERRCODE_NOT_NULL_VIOLATION),
                        errmsg("null value in column \"%s\" violates not-null constraint",
                            NameStr(tupDesc->attrs[attrChk - 1]->attname)),
                        val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
            }
        }
    }

    if (constr->num_check > 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Column Store unsupport CHECK constraint")));
}


template<bool isInsertSelect>
void AddToBulk(CopyFromBulk bulk, HeapTuple tup, bool needCopy)
{
    MemoryContext oldCxt = NULL;
    CopyFromMemCxt copyFromMemCxt = NULL;
    HeapTuple newTuple = NULL;

    Assert(bulk != NULL && tup != NULL);

    Assert(TUPLE_IS_HEAP_TUPLE(tup)); 

    if (needCopy) {
#ifdef USE_ASSERT_CHECKING
        int idx;
        bool found = false;
#endif
        copyFromMemCxt = bulk->memCxt;
        Assert(copyFromMemCxt != NULL);

#ifdef USE_ASSERT_CHECKING
        if (!isInsertSelect) {
            for (idx = 0; idx < copyFromMemCxt->nextBulk; idx++) {
                found = found || (copyFromMemCxt->chunk[idx] == bulk);
            }
            Assert(true == found);
        }
#endif

        oldCxt = MemoryContextSwitchTo(copyFromMemCxt->memCxtCandidate);

        newTuple = heap_copytuple(tup);
        copyFromMemCxt->memCxtSize += newTuple->t_len;
        if (bulk->numTuples == bulk->maxTuples) {
            bulk->maxTuples *= 2;
            bulk->tuples = (Tuple*)repalloc(bulk->tuples, sizeof(Tuple) * bulk->maxTuples);
        }

        (void)MemoryContextSwitchTo(oldCxt);
    } else {
        Assert(InvalidOid == bulk->partOid);
        Assert(NULL == bulk->memCxt);
        Assert(bulk->numTuples < MAX_BUFFERED_TUPLES);
        Assert(bulk->sizeTuples < MAX_TUPLES_SIZE);
        newTuple = tup;
    }

    Assert(bulk->numTuples < bulk->maxTuples);
    bulk->tuples[bulk->numTuples] = (Tuple)newTuple;
    bulk->sizeTuples += newTuple->t_len;
    bulk->numTuples++;
}

template<bool isInsertSelect>
void AddToUHeapBulk(CopyFromBulk bulk, UHeapTuple tup, bool needCopy)
{
    MemoryContext oldCxt = NULL;
    CopyFromMemCxt copyFromMemCxt = NULL;
    UHeapTuple newTuple = NULL;

    Assert(bulk != NULL && tup != NULL);
    Assert(tup->tupTableType == UHEAP_TUPLE);

    if (needCopy) {
#ifdef USE_ASSERT_CHECKING
        int idx;
        bool found = false;
#endif
        copyFromMemCxt = bulk->memCxt;
        Assert(copyFromMemCxt != NULL);

#ifdef USE_ASSERT_CHECKING
        if (!isInsertSelect) {
            for (idx = 0; idx < copyFromMemCxt->nextBulk; idx++) {
                found = found || (copyFromMemCxt->chunk[idx] == bulk);
            }
            Assert(found == true);
        }
#endif

        oldCxt = MemoryContextSwitchTo(copyFromMemCxt->memCxtCandidate);

        newTuple = UHeapCopyTuple(tup);
        copyFromMemCxt->memCxtSize += newTuple->disk_tuple_size;
        if (bulk->numTuples == bulk->maxTuples) {
            bulk->maxTuples *= 2;
            bulk->tuples = (Tuple*)repalloc(bulk->tuples, sizeof(Tuple) * bulk->maxTuples);
        }

        (void)MemoryContextSwitchTo(oldCxt);
    } else {
        Assert(InvalidOid == bulk->partOid);
        Assert(NULL == bulk->memCxt);
        Assert(bulk->numTuples < MAX_BUFFERED_TUPLES);
        Assert(bulk->sizeTuples < MAX_TUPLES_SIZE);
        newTuple = tup;
    }

    Assert(bulk->numTuples < bulk->maxTuples);
    bulk->tuples[bulk->numTuples] = (Tuple)newTuple;
    bulk->sizeTuples += newTuple->disk_tuple_size;
    bulk->numTuples++;
}

void UHeapAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    UHeapTuple utuple = (UHeapTuple)tup;
    AddToUHeapBulk<true>(bulk, utuple, needCopy);
}

void HeapAddToBulkInsertSelect(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    HeapTuple tuple = (HeapTuple)tup;
    AddToBulk<true>(bulk, tuple, needCopy);
}

void UHeapAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    UHeapTuple utuple = (UHeapTuple)tup;
    AddToUHeapBulk<false>(bulk, utuple, needCopy);
}

void HeapAddToBulk(CopyFromBulk bulk, Tuple tup, bool needCopy)
{
    HeapTuple tuple = (HeapTuple)tup;
    AddToBulk<false>(bulk, tuple, needCopy);
}

/*
 * Copy FROM file to relation.
 */
static uint64 CopyFrom(CopyState cstate)
{
    Tuple tuple;
    TupleDesc tupDesc;
    Datum* values = NULL;
    bool* nulls = NULL;
    ResultRelInfo* resultRelInfo = NULL;
    EState* estate = CreateExecutorState(); /* for ExecConstraints() */
    ExprContext* econtext = NULL;
    TupleTableSlot* myslot = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;

    ErrorContextCallback errcontext;
    CommandId mycid = GetCurrentCommandId(true);
    int hi_options = 0; /* start with default heap_insert options */
    BulkInsertState bistate;
    uint64 processed = 0;
    
    int2 bucketid = InvalidBktId;
    bool useHeapMultiInsert = false;
    bool isPartitionRel = false;
    bool resetPerTupCxt = false;
    Relation resultRelationDesc = NULL;
    CopyFromManager mgr = NULL;
    Size PerTupCxtSize = 0;

    List *partitionList = NIL;
    bool hasPartition = false;
    bool hasBucket = false;
    MemInfoArg* CopyMem = NULL;
    bool needflush = false;
    bool isForeignTbl = false; /* Whether this foreign table support COPY */
    bool rel_isblockchain = false;
    int hash_colno = -1;
    Assert(cstate->rel);

    if (cstate->rel->rd_rel->relkind != RELKIND_RELATION) {
        if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy to view \"%s\"", RelationGetRelationName(cstate->rel))));
        else if (cstate->rel->rd_rel->relkind == RELKIND_CONTQUERY)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy to contview \"%s\"",
                    RelationGetRelationName(cstate->rel))));
        else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy to materialized view \"%s\"",
                    RelationGetRelationName(cstate->rel))));
        else if (cstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            if (!CheckSupportedFDWType(RelationGetRelid(cstate->rel)))
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("cannot copy to foreign table \"%s\"", RelationGetRelationName(cstate->rel))));
            isForeignTbl = true;
        } else if (cstate->rel->rd_rel->relkind == RELKIND_STREAM) {
            if (!CheckSupportedFDWType(RelationGetRelid(cstate->rel)))
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("cannot copy to stream \"%s\"", RelationGetRelationName(cstate->rel))));
            isForeignTbl = true;
        } else if (RELKIND_IS_SEQUENCE(cstate->rel->rd_rel->relkind))
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy to (large) sequence \"%s\"", RelationGetRelationName(cstate->rel))));
        else
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("cannot copy to non-table relation \"%s\"", RelationGetRelationName(cstate->rel))));
    }

    tupDesc = RelationGetDescr(cstate->rel);
    rel_isblockchain = cstate->rel->rd_isblockchain;
    hash_colno = user_hash_attrno(tupDesc);

    /* ----------
     * Check to see if we can avoid writing WAL
     *
     * If archive logging/streaming is not enabled *and* either
     *	- table was created in same transaction as this COPY
     *	- data is being written to relfilenode created in this transaction
     * then we can skip writing WAL.  It's safe because if the transaction
     * doesn't commit, we'll discard the table (or the new relfilenode file).
     * If it does commit, we'll have done the heap_sync at the bottom of this
     * routine first.
     *
     * As mentioned in comments in utils/rel.h, the in-same-transaction test
     * is not completely reliable, since in rare cases rd_createSubid or
     * rd_newRelfilenodeSubid can be cleared before the end of the transaction.
     * However this is OK since at worst we will fail to make the optimization.
     *
     * Also, if the target file is new-in-transaction, we assume that checking
     * FSM for free space is a waste of time, even if we must use WAL because
     * of archiving.  This could possibly be wrong, but it's unlikely.
     *
     * The comments for heap_insert and RelationGetBufferForTuple specify that
     * skipping WAL logging is only safe if we ensure that our tuples do not
     * go into pages containing tuples from any other transactions --- but this
     * must be the case if we have a new table or new relfilenode, so we need
     * no additional work to enforce that.
     * ----------
     */
    if (cstate->rel->rd_createSubid != InvalidSubTransactionId ||
        cstate->rel->rd_newRelfilenodeSubid != InvalidSubTransactionId) {
        hi_options |= TABLE_INSERT_SKIP_FSM;
        if (!XLogIsNeeded())
            hi_options |= TABLE_INSERT_SKIP_WAL;
    }
    /*
     * Optimize if new relfilenode was created in this subxact or
     * one of its committed children and we won't see those rows later
     * as part of an earlier scan or command. This ensures that if this
     * subtransaction aborts then the frozen rows won't be visible
     * after xact cleanup. Note that the stronger test of exactly
     * which subtransaction created it is crucial for correctness
     * of this optimisation.
     */
    if (cstate->freeze && ThereAreNoPriorRegisteredSnapshots() && ThereAreNoReadyPortals() &&
        (cstate->rel->rd_newRelfilenodeSubid == GetCurrentSubTransactionId() ||
            cstate->rel->rd_createSubid == GetCurrentSubTransactionId())) {
        hi_options |= TABLE_INSERT_FROZEN;
    }

    /*
     * We need a ResultRelInfo so we can use the regular executor's
     * index-entry-making machinery.  (There used to be a huge amount of code
     * here that basically duplicated execUtils.c ...)
     */
    resultRelInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(resultRelInfo,
        cstate->rel,
        1, /* dummy rangetable index */
        0);

    ExecOpenIndices(resultRelInfo, false);
    init_gtt_storage(CMD_INSERT, resultRelInfo);

    resultRelationDesc = resultRelInfo->ri_RelationDesc;
    isPartitionRel = RELATION_IS_PARTITIONED(resultRelationDesc);
    hasBucket = RELATION_OWN_BUCKET(resultRelationDesc);
    hasPartition = isPartitionRel || hasBucket;
    needflush = ((hi_options & TABLE_INSERT_SKIP_WAL) || enable_heap_bcm_data_replication()) 
                && !RelationIsForeignTable(cstate->rel) && !RelationIsStream(cstate->rel)
                && !RelationIsSegmentTable(cstate->rel);

    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;
    estate->es_range_table = cstate->range_table;

    /* Set up a tuple slot too */
    myslot = ExecInitExtraTupleSlot(estate, cstate->rel->rd_tam_type);
    ExecSetSlotDescriptor(myslot, tupDesc);
    /* Triggers might need a slot as well */
    estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);

    /*
     * It's more efficient to prepare a bunch of tuples for insertion, and
     * insert them in one heap_multi_insert() call, than call heap_insert()
     * separately for every tuple. However, we can't do that if there are
     * BEFORE/INSTEAD OF triggers, or we need to evaluate volatile default
     * expressions. Such triggers or expressions might query the table we're
     * inserting to, and act differently if the tuples that have already been
     * processed and prepared for insertion are not there.
     */
    if ((resultRelInfo->ri_TrigDesc != NULL &&
        (resultRelInfo->ri_TrigDesc->trig_insert_before_row || resultRelInfo->ri_TrigDesc->trig_insert_instead_row)) ||
        cstate->volatile_defexprs) {
        useHeapMultiInsert = false;
    } else {
        useHeapMultiInsert = true;
        mgr = initCopyFromManager(cstate->copycontext, resultRelationDesc);
        cstate->pcState = New(cstate->copycontext) PageCompress(resultRelationDesc, cstate->copycontext);
    }

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    /*
     * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
     * should do this for COPY, since it's not really an "INSERT" statement as
     * such. However, executing these triggers maintains consistency with the
     * EACH ROW triggers that we already fire on COPY.
     */
    ExecBSInsertTriggers(estate, resultRelInfo);

#ifdef PGXC
    /* Send COPY command to datanode */
    if (IS_PGXC_COORDINATOR && cstate->remoteCopyState && cstate->remoteCopyState->rel_loc) {
        RemoteCopyData* remoteCopyState = cstate->remoteCopyState;

        /* Send COPY command to datanode */
        pgxc_datanode_copybegin(remoteCopyState, &cstate->memUsage);

        /* In case of binary COPY FROM, send the header */
        if (IS_BINARY(cstate)) {
            RemoteCopyData* remoteCopyState = cstate->remoteCopyState;
            int32 tmp;

            /* Empty buffer info and send header to all the backends involved in COPY */
            resetStringInfo(&cstate->line_buf);

            enlargeStringInfo(&cstate->line_buf, 19);
            appendBinaryStringInfo(&cstate->line_buf, BinarySignature, 11);
            tmp = 0;

            if (cstate->oids)
                tmp |= (1 << 16);
            tmp = htonl(tmp);

            appendBinaryStringInfo(&cstate->line_buf, (char*)&tmp, 4);
            tmp = 0;
            tmp = htonl(tmp);
            appendBinaryStringInfo(&cstate->line_buf, (char*)&tmp, 4);

            if (DataNodeCopyInBinaryForAll(cstate->line_buf.data, 19, remoteCopyState->connections))
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid COPY file header (COPY SEND)")));
        }
    }
#endif

    values = (Datum*)palloc(tupDesc->natts * sizeof(Datum));
    nulls = (bool*)palloc(tupDesc->natts * sizeof(bool));

    bistate = GetBulkInsertState();
    econtext = GetPerTupleExprContext(estate);

    /* Set up callback to identify error line number */
    errcontext.callback = CopyFromErrorCallback;
    errcontext.arg = (void*)cstate;
    errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcontext;

    /*
     * Push the relfilenode to the hash tab, when the transaction abort, we should heap_sync
     * the relation
     */
    if (enable_heap_bcm_data_replication() && !RelationIsSegmentTable(cstate->rel)) {
        if (!RelationIsForeignTable(cstate->rel) && !RelationIsStream(cstate->rel))
            HeapSyncHashSearch(cstate->rel->rd_id, HASH_ENTER);
        LockRelFileNode(cstate->rel->rd_node, RowExclusiveLock);
    }

    // Copy support ColStore
    //
    CStorePartitionInsert* cstorePartitionInsert = NULL;
    CStoreInsert* cstoreInsert = NULL;

    DfsInsertInter* dfsInsert = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    Tsdb::TsStoreInsert *tsstoreInsert = NULL;
#endif   /* ENABLE_MULTIPLE_NODES */
    // because every partition relation share the same option values with their parent relation, so
    // we can init once and read many times for both normal relation and partition relation.
    //
    const int maxValuesCount = RelationGetMaxBatchRows(cstate->rel);
    bulkload_rows* batchRowsPtr = NULL;
    InsertArg args;
    CopyMem = (MemInfoArg*)palloc0(sizeof(struct MemInfoArg));

    /* Assign mem usage here from cstate->memUsage */
    if (RelationIsColStore(cstate->rel) && IS_PGXC_DATANODE) {
        oldcontext = MemoryContextSwitchTo(cstate->copycontext);
        /* Init copy mem, it will pass the allocated memory to copy datanode to use. */
        InitCopyMemArg(cstate, CopyMem);
        if (!isPartitionRel) {
            if (RelationIsCUFormat(cstate->rel)) {
                CStoreInsert::InitInsertArg(cstate->rel, estate->es_result_relations, false, args);
                cstoreInsert = New(CurrentMemoryContext) CStoreInsert(cstate->rel, args, false, NULL, CopyMem);
                batchRowsPtr = New(CurrentMemoryContext) bulkload_rows(cstate->rel->rd_att, maxValuesCount);
            } else if (RelationIsPAXFormat(cstate->rel)) {
                dfsInsert = CreateDfsInsert(cstate->rel, false, NULL, NULL, CopyMem);
                dfsInsert->BeginBatchInsert(TUPLE_SORT, estate->es_result_relations);
            }
        } else {
            cstorePartitionInsert = New(CurrentMemoryContext)
                CStorePartitionInsert(cstate->rel, estate->es_result_relations, TUPLE_SORT, false, NULL, CopyMem);
        }
        MemoryContextSwitchTo(oldcontext);
    }
#ifdef ENABLE_MULTIPLE_NODES
    else if (RelationIsTsStore(cstate->rel) && IS_PGXC_DATANODE) {
        if (!g_instance.attr.attr_common.enable_tsdb) {
            ereport(ERROR, 
                (errcode(ERRCODE_LOG),
                errmsg("Can't copy data when 'enable_tsdb' is off"),
                errdetail("When the guc is off, it is forbidden to insert to timeseries table"),
                errcause("Unexpected error"),
                erraction("Turn on the 'enable_tsdb'."),
                errmodule(MOD_TIMESERIES)));
        }
        oldcontext = MemoryContextSwitchTo(cstate->copycontext);
        /*Init copy mem, it will pass the allocated memory to copy datanode to use.*/
        InitCopyMemArg(cstate, CopyMem);
        Assert (isPartitionRel);
        tsstoreInsert = New (CurrentMemoryContext)Tsdb::TsStoreInsert(cstate->rel);

        MemoryContextSwitchTo(oldcontext);
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    for (;;) {
        TupleTableSlot* slot = NULL;
        bool skip_tuple = false;
        Oid loaded_oid = InvalidOid;

        Oid partitionid = InvalidOid;
        Partition partition = NULL;
        Relation heaprel = NULL;

        bool is_EOF = false;
        bool has_hash = false;
        uint64 res_hash = 0;


    retry_copy:

        CHECK_FOR_INTERRUPTS();

        if (resetPerTupCxt) {
            /*
             * Reset the per-tuple exprcontext. We can only do this if the
             * tuple buffer is empty (calling the context the per-tuple memory
             * context is a bit of a misnomer now
             */
            ResetPerTupleExprContext(estate);
            resetPerTupCxt = false;
            PerTupCxtSize = 0;
        }

        /* Switch into its memory context */
        (void)MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

        if (IS_PGXC_COORDINATOR) {
#ifdef ENABLE_MULTIPLE_NODES
            PG_TRY();
            {
                is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
            }

            PG_CATCH();
            {
                if (TrySaveImportError(cstate)) {
                    resetPerTupCxt = true;
                    goto retry_copy;
                } else {
                    ereport(LOG,
                        (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("An error in Copy From cannot be catched.")));
                    PG_RE_THROW();
                }
            }

            PG_END_TRY();
#endif
            if (unlikely(is_EOF))
                break;
        } else {
            if (RelationIsPAXFormat(cstate->rel)) {
                for (int i = 0; i < maxValuesCount; ++i) {
                    PG_TRY();
                    {
                        is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
                    }

                    PG_CATCH();
                    {
                        if (TrySaveImportError(cstate)) {
                            resetPerTupCxt = true;
                            goto retry_copy;
                        } else {
                            ereport(LOG,
                                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("An error in Copy From cannot be catched.")));
                            PG_RE_THROW();
                        }
                    }

                    PG_END_TRY();

                    if (!is_EOF) {
                        if (cstate->rel->rd_att->constr)
                            CStoreCopyConstraintsCheck(resultRelInfo, values, nulls, estate);

                        ++processed;
                        if (dfsInsert) {
                            dfsInsert->TupleInsert(values, nulls, hi_options);
                        }
                    } else {
                        if (dfsInsert) {
                            dfsInsert->SetEndFlag();
                            dfsInsert->TupleInsert(NULL, NULL, hi_options);
                        }
                        break;
                    }
                }

                // we will reset and free all the used memory after inserting,
                // so make resetPerTupCxt true.
                resetPerTupCxt = true;

                if (dfsInsert->isEnd())
                    break;

                continue;
            } else if (RelationIsCUFormat(cstate->rel)) {
                if (!isPartitionRel) {
                    batchRowsPtr->reset(true);

                    /*
                     * we limit the batch by two factors:
                     * 1. tuple numbers ( <= maxValuesCount );
                     * 2. memroy batchRowsPtr is using;
                     */
                    for (int i = 0; i < maxValuesCount; ++i) {
                        PG_TRY();
                        {
                            is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
                        }

                        PG_CATCH();
                        {
                            if (TrySaveImportError(cstate)) {
                                resetPerTupCxt = true;
                                goto ctore_non_partition_retry_copy;
                            } else {
                                ereport(LOG,
                                    (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                        errmsg("An error in Copy From cannot be catched.")));
                                PG_RE_THROW();
                            }
                        }

                        PG_END_TRY();

                        if (!is_EOF) {
                            if (cstate->rel->rd_att->constr)
                                CStoreCopyConstraintsCheck(resultRelInfo, values, nulls, estate);

                            Size tuple_size = batchRowsPtr->calculate_tuple_size(tupDesc, values, nulls);
                            if ((BULKLOAD_MAX_MEMSIZE - batchRowsPtr->m_using_blocks_total_rawsize) < tuple_size) {
                                cstoreInsert->BatchInsert(batchRowsPtr, hi_options);
                                batchRowsPtr->reset(true);
                                i = 0;
                            }

                            ++processed;
                            if (batchRowsPtr->append_one_tuple(values, nulls, tupDesc))
                                break;
                        } else {
                            cstoreInsert->SetEndFlag();
                            break;
                        }
                    }

                    // we will reset and free all the used memory after inserting,
                    // so make resetPerTupCxt true.
                    // reset batchRowsPtr at the start of new loop.
                    //
                    ctore_non_partition_retry_copy:

                    cstoreInsert->BatchInsert(batchRowsPtr, hi_options);
                    resetPerTupCxt = true;
                    if (cstoreInsert->IsEnd())
                        break;

                    continue;
                } else {
                    bool endFlag = false;
                    for (int i = 0; i < maxValuesCount; ++i) {
                        PG_TRY();
                        {
                            is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
                        }

                        PG_CATCH();
                        {
                            if (TrySaveImportError(cstate)) {
                                resetPerTupCxt = true;
                                goto retry_copy;
                            } else {
                                ereport(LOG,
                                    (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                        errmsg("An error in Copy From cannot be catched.")));
                                PG_RE_THROW();
                            }
                        }

                        PG_END_TRY();

                        if (!is_EOF) {
                            if (cstate->rel->rd_att->constr)
                                CStoreCopyConstraintsCheck(resultRelInfo, values, nulls, estate);
                            cstorePartitionInsert->BatchInsert(values, nulls, hi_options);
                            ++processed;
                        } else {
                            endFlag = true;
                            cstorePartitionInsert->EndBatchInsert();
                            break;
                        }
                    }

                    resetPerTupCxt = true;
                    if (endFlag) {
                        break;
                    }

                    continue;
                }
            }
#ifdef ENABLE_MULTIPLE_NODES
            else if (RelationIsTsStore(cstate->rel)) {
                bool endFlag = false;
                while (true) {
                    PG_TRY();
                    {
                        is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
                    }

                    PG_CATCH();
                    {
                        if(TrySaveImportError(cstate)) {
                            resetPerTupCxt = true;
                            goto retry_copy;
                        } else {
                            ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                    errmsg("An error in Copy From cannot be catched.")));
                            PG_RE_THROW();
                        }
                    }

                    PG_END_TRY();
                    if (!is_EOF) {
                        tsstoreInsert->batch_insert(values, nulls, hi_options, false);
                    } else {
                        endFlag = true;
                        tsstoreInsert->end_batch_insert();
                        break;
                    }
                }
                resetPerTupCxt = true;
                if (true == endFlag) {
                    break;
                }
                continue;
            }
#endif   /* ENABLE_MULTIPLE_NODES */
            else {
                PG_TRY();
                {
                    is_EOF = !NextCopyFrom(cstate, econtext, values, nulls, &loaded_oid);
                }

                PG_CATCH();
                {
                    if (TrySaveImportError(cstate)) {
                        if (useHeapMultiInsert) {
                            if (hasPartition) {
                                int cnt = 0;
                                Assert(mgr->isPartRel);
                                for (cnt = 0; cnt < mgr->numMemCxt; cnt++) {
                                    CopyFromMemCxt copyFromMemCxt = mgr->memCxt[cnt];
                                    if (copyFromMemCxt->memCxtSize > 0) {
                                        Assert(copyFromMemCxt->nextBulk > 0);
                                        (void)CopyFromChunkInsert<false>(cstate,
                                            estate,
                                            copyFromMemCxt->chunk[0],
                                            mgr,
                                            cstate->pcState,
                                            mycid,
                                            hi_options,
                                            resultRelInfo,
                                            myslot,
                                            bistate);
                                    }
                                }
                            } else if (mgr->bulk->numTuples > 0) {
                                Assert(!mgr->isPartRel);
                                mgr->LastFlush = true;
                                (void)CopyFromChunkInsert<false>(cstate,
                                    estate,
                                    mgr->bulk,
                                    mgr,
                                    cstate->pcState,
                                    mycid,
                                    hi_options,
                                    resultRelInfo,
                                    myslot,
                                    bistate);
                            }
                        }
                        resetPerTupCxt = true;
                        goto retry_copy;
                    } else {
                        ereport(LOG,
                            (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                                errmsg("A error in Copy From cannot be catched.")));
                        PG_RE_THROW();
                    }
                }

                PG_END_TRY();

                if (is_EOF) {
                    break;
                }
            }
        }

#ifdef PGXC
        /*
         * Send the data row as-is to the Datanodes. If default values
         * are to be inserted, append them onto the data row.
         */
        if (IS_PGXC_COORDINATOR && cstate->remoteCopyState && cstate->remoteCopyState->rel_loc) {
            Form_pg_attribute* attr = tupDesc->attrs;
            Oid* att_type = NULL;
            RemoteCopyData* remoteCopyState = cstate->remoteCopyState;
            ExecNodes* exec_nodes = NULL;
            List* taglist = NIL;
            int dcolNum = -1;
            bool pseudoTsDistcol = false;

            if (remoteCopyState->idx_dist_by_col != NULL) {
                att_type = (Oid*)palloc(tupDesc->natts * sizeof(Oid));
                dcolNum = lfirst_int(list_head(remoteCopyState->idx_dist_by_col));

                /* If a timeseries table use hidden column as distribution column,
                 * then fetch all the tag columns in the table to build a list,
                 * i.e. taglist. Use pseudoTsDistcol to identify this situation.
                 */
                if (RelationIsTsStore(cstate->rel) && dcolNum > 0 &&
                    list_length(remoteCopyState->idx_dist_by_col) == 1 &&
                    TsRelWithImplDistColumn(attr, dcolNum)) {
                    pseudoTsDistcol = true;
                    for (int i = 0; i < tupDesc->natts; i++) {
                        att_type[i] = attr[i]->atttypid;
                        /* collect all the tag columns info to taglist */
                        if (attr[i]->attkvtype == ATT_KV_TAG) {
                            taglist = lappend_int(taglist, i);
                        }
                    }
                    Assert(taglist != NULL);
                } else {
                    for (int i = 0; i < tupDesc->natts; i++) {
                        att_type[i] = attr[i]->atttypid;
                    }
                }
            }

            /* For implicit distribution ts table, use targlist instead of idx_dist_by_col */
            if (pseudoTsDistcol) {
                exec_nodes = GetRelationNodes(remoteCopyState->rel_loc,
                    values,
                    nulls,
                    att_type,
                    taglist,
                    RELATION_ACCESS_INSERT,
                    false);
                list_free_ext(taglist);
            } else {
                exec_nodes = GetRelationNodes(remoteCopyState->rel_loc,
                    values,
                    nulls,
                    att_type,
                    remoteCopyState->idx_dist_by_col,
                    RELATION_ACCESS_INSERT,
                    false);
            }
            if (att_type != NULL) {
                pfree_ext(att_type);
            }

            if (DataNodeCopyIn(cstate->line_buf.data,
                    cstate->line_buf.len,
                    cstate->eol,
                    exec_nodes,
                    remoteCopyState->connections,
                    IS_BINARY(cstate)))
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Copy failed on a Datanode")));
            processed++;
            if (rel_isblockchain) {
                uint64 hash;
                if (get_copyfrom_line_relhash(cstate->line_buf.data, cstate->line_buf.len, hash_colno, '\t', &hash)) {
                    cstate->hashstate.histhash += hash;
                    cstate->hashstate.has_histhash = true;
                }
            }

            resetPerTupCxt = true;
        } else {
#endif

            /* And now we can form the input tuple. */
            tuple = (Tuple)tableam_tops_form_tuple(tupDesc, values, nulls, tableam_tops_get_tuple_type(cstate->rel));

            if (loaded_oid != InvalidOid)
                HeapTupleSetOid((HeapTuple)tuple, loaded_oid);

            /* Triggers and stuff need to be invoked in query context. */
            (void)MemoryContextSwitchTo(oldcontext);

            /* Place tuple in tuple slot --- but slot shouldn't free it */
            slot = myslot;
            // ExecStoreTuple already handles the tupe casting inside itself. Only need to pass in Tuple tuple
            (void)ExecStoreTuple(tuple, slot, InvalidBuffer, false);

            skip_tuple = false;

            /* BEFORE ROW INSERT Triggers */
            if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row) {
                slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);

                if (slot == NULL) /* "do nothing" */
                    skip_tuple = true;
                else /* trigger might have changed tuple */
                    tuple = tableam_tslot_get_tuple_from_slot(cstate->rel, slot);
            }

            if (!skip_tuple && isForeignTbl) {
                resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate, resultRelInfo, slot, NULL);
                processed++;
            } else if (!skip_tuple) {
                /*
                 * Compute stored generated columns
                 */
                if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
                    resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored) {
                    ExecComputeStoredGenerated(resultRelInfo, estate, slot, tuple, CMD_INSERT);
                    tuple = slot->tts_tuple;
                }
                if (rel_isblockchain) {
                    tuple = set_user_tuple_hash((HeapTuple)tuple, resultRelationDesc, true);
                }

                /* Check the constraints of the tuple */
                if (cstate->rel->rd_att->constr)
                    ExecConstraints(resultRelInfo, slot, estate);

                if (hasBucket) {
                    bucketid = computeTupleBucketId(resultRelationDesc, (HeapTuple)tuple);
                }

                if (useHeapMultiInsert) {
                    bool toFlush = false;
                    CopyFromBulk bulk = NULL;
                    Oid targetOid = InvalidOid;
                    /* step 1: query and get the caching buffer */
                    if (isPartitionRel) {
                        if (RelationIsSubPartitioned(resultRelationDesc)) {
                            targetOid = heapTupleGetSubPartitionId(resultRelationDesc, tuple);
                        } else {
                            targetOid = heapTupleGetPartitionId(resultRelationDesc, tuple);
                        }
                    } else {
                        targetOid = RelationGetRelid(resultRelationDesc);
                    }
                    bulk = findBulk(mgr, targetOid, bucketid, &toFlush);
                    /* step 2: check bulk-insert */
                    Assert(hasPartition || (toFlush == false));
                    Assert(!toFlush || InvalidOid != mgr->switchKey.partOid);
                    if (toFlush) {
                        (void)CopyFromChunkInsert<false>(cstate,
                            estate,
                            bulk,
                            mgr,
                            cstate->pcState,
                            mycid,
                            hi_options,
                            resultRelInfo,
                            myslot,
                            bistate);
                    }

                    if (rel_isblockchain) {
                        has_hash = hist_table_record_insert(resultRelationDesc, (HeapTuple)tuple, &res_hash);
                    }

                    /* step 3: cache tuples as many as possible */
                    Assert(hasPartition == (InvalidOid != bulk->partOid));
                    tableam_tops_add_to_bulk(cstate->rel, bulk, (Tuple) tuple, hasPartition);
                    PerTupCxtSize += ((HeapTuple)tuple)->t_len;
                    /* step 4: check and flush the full bulk */
                    if (isBulkFull(bulk)) {
                        resetPerTupCxt = CopyFromChunkInsert<false>(cstate,
                            estate,
                            bulk,
                            mgr,
                            cstate->pcState,
                            mycid,
                            hi_options,
                            resultRelInfo,
                            myslot,
                            bistate);
                    }

                    if (hasPartition) {
                        resetPerTupCxt = resetPerTupCxt || (PerTupCxtSize >= MAX_TUPLES_SIZE);
                    }
                    
                    if (isPartitionRel && needflush) {
                        Oid targetPartOid = InvalidOid;
                        if (RelationIsSubPartitioned(resultRelationDesc)) {
                            targetPartOid = heapTupleGetSubPartitionId(resultRelationDesc, tuple);
                        } else {
                            targetPartOid = heapTupleGetPartitionId(resultRelationDesc, tuple);
                        }
                        partitionList = list_append_unique_oid(partitionList, targetPartOid);
                    }
                } else {
                    List* recheckIndexes = NIL;
                    Relation targetRel;
                    ItemPointer pTSelf = NULL;
                    Relation subPartRel = NULL;
                    Partition subPart = NULL;
                    if (isPartitionRel) {
                        /* get partititon oid to insert the record */
                        partitionid = heapTupleGetPartitionId(resultRelationDesc, tuple);
                        searchFakeReationForPartitionOid(estate->esfRelations,
                            estate->es_query_cxt,
                            resultRelationDesc,
                            partitionid,
                            heaprel,
                            partition,
                            RowExclusiveLock);

                        if (RelationIsSubPartitioned(resultRelationDesc)) {
                            partitionid = heapTupleGetPartitionId(heaprel, tuple);
                            searchFakeReationForPartitionOid(estate->esfRelations, estate->es_query_cxt, heaprel,
                                                             partitionid, subPartRel, subPart, RowExclusiveLock);
                            heaprel = subPartRel;
                            partition = subPart;
                        }

                        targetRel = heaprel;

                        if (bucketid != InvalidBktId) {
                            searchHBucketFakeRelation(
                                estate->esfRelations, estate->es_query_cxt, heaprel, bucketid, targetRel);
                        }

                        (void)tableam_tuple_insert(targetRel, tuple, mycid, 0, NULL);

                        if (needflush) {
                            partitionList = list_append_unique_oid(partitionList, partitionid);
                        }
                    } else if (hasBucket) {
                        Assert(bucketid != InvalidBktId);
                        searchHBucketFakeRelation(
                            estate->esfRelations, estate->es_query_cxt, cstate->rel, bucketid, targetRel);
                        (void)tableam_tuple_insert(targetRel, tuple, mycid, 0, NULL);
                    } else {
                        targetRel = cstate->rel;
                        (void)tableam_tuple_insert(targetRel, tuple, mycid, 0, NULL);
                    }

                    if (rel_isblockchain) {
                        has_hash = hist_table_record_insert(targetRel, (HeapTuple)tuple, &res_hash);
                    }

                    pTSelf = tableam_tops_get_t_self(cstate->rel, tuple);

                    /*
                     * Global Partition Index stores the partition's tableOid with the index
                     * tuple which is extracted from the tuple of the slot. Make sure it is set.
                     */
                    if (slot->tts_tupslotTableAm != TAM_USTORE) {
                        ((HeapTuple)slot->tts_tuple)->t_tableOid = RelationGetRelid(targetRel);
                    } else {
                        ((UHeapTuple)slot->tts_tuple)->table_oid = RelationGetRelid(targetRel);
                    }

                    /* OK, store the tuple and create index entries for it */
                    if (resultRelInfo->ri_NumIndices > 0 && !RelationIsColStore(cstate->rel))
                        recheckIndexes = ExecInsertIndexTuples(slot,
                            pTSelf,
                            estate,
                            isPartitionRel ? heaprel : NULL,
                            isPartitionRel ? partition : NULL,
                            bucketid, NULL, NULL);

                    /* AFTER ROW INSERT Triggers */
                    ExecARInsertTriggers(estate, resultRelInfo, partitionid, bucketid, (HeapTuple)tuple,
                        recheckIndexes);

                    list_free(recheckIndexes);

                    resetPerTupCxt = true;
                }
                if (has_hash) {
                    cstate->hashstate.has_histhash = true;
                    cstate->hashstate.histhash += res_hash;
                }

                /*
                 * We count only tuples not suppressed by a BEFORE INSERT trigger;
                 * this is the same definition used by execMain.c for counting
                 * tuples inserted by an INSERT command.
                 */
                processed++;
            }
#ifdef PGXC
        }
#endif
    }

    if (cstoreInsert != NULL)
        DELETE_EX(cstoreInsert);
    if (dfsInsert != NULL)
        DELETE_EX(dfsInsert);

    if (batchRowsPtr != NULL)
        DELETE_EX(batchRowsPtr);

    if (cstorePartitionInsert != NULL)
        DELETE_EX(cstorePartitionInsert);

#ifdef ENABLE_MULTIPLE_NODES
    if (tsstoreInsert != NULL) {
        delete tsstoreInsert;
        tsstoreInsert = NULL;
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    if (CopyMem != NULL) {
        pfree_ext(CopyMem);
    }

    CStoreInsert::DeInitInsertArg(args);

    /* Flush any remaining buffered tuples */
    if (useHeapMultiInsert) {
        if (hasPartition) {
            int cnt = 0;
            Assert(mgr->isPartRel);
            for (cnt = 0; cnt < mgr->numMemCxt; cnt++) {
                CopyFromMemCxt copyFromMemCxt = mgr->memCxt[cnt];
                if (copyFromMemCxt->memCxtSize > 0) {
                    Assert(copyFromMemCxt->nextBulk > 0);
                    (void)CopyFromChunkInsert<false>(cstate,
                        estate,
                        copyFromMemCxt->chunk[0],
                        mgr,
                        cstate->pcState,
                        mycid,
                        hi_options,
                        resultRelInfo,
                        myslot,
                        bistate);
                }

                /* the following code should be placed into function deinitCopyFromManager()
                 * according to SRP rule. But it's here in order to reduce the FOR calling. */
                Assert(copyFromMemCxt->nextBulk == 0);
                Assert(copyFromMemCxt->memCxtSize == 0);
                MemoryContextDelete(copyFromMemCxt->memCxtCandidate);
                pfree_ext(copyFromMemCxt);
            }
        } else if (mgr->bulk->numTuples > 0) {
            Assert(!mgr->isPartRel);
            mgr->LastFlush = true;
            (void)CopyFromChunkInsert<false>(
                cstate, estate, mgr->bulk, mgr, cstate->pcState, mycid, hi_options, resultRelInfo, myslot, bistate);
        }
    }

    /* Done, clean up */
    t_thrd.log_cxt.error_context_stack = errcontext.previous;

    FreeBulkInsertState(bistate);

    deinitCopyFromManager(mgr);
    mgr = NULL;
    (void)MemoryContextSwitchTo(oldcontext);

    delete cstate->pcState;
    cstate->pcState = NULL;

#ifdef PGXC

    /* Send COPY DONE to datanodes */
    if (IS_PGXC_COORDINATOR && cstate->remoteCopyState != NULL && 
        cstate->remoteCopyState->rel_loc != NULL) {
        RemoteCopyData* remoteCopyState = cstate->remoteCopyState;
        bool replicated = (remoteCopyState->rel_loc->locatorType == LOCATOR_TYPE_REPLICATED);
        DataNodeCopyFinish(remoteCopyState->connections,
            u_sess->pgxc_cxt.NumDataNodes,
            replicated ? PGXCNodeGetNodeId(u_sess->pgxc_cxt.primary_data_node, PGXC_NODE_DATANODE) : -1,
            replicated ? COMBINE_TYPE_SAME : COMBINE_TYPE_SUM,
            cstate->rel);
    }
#endif

    /* Send cached error records to datanodes using SPI */
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && (cstate->log_errors || cstate->logErrorsData)) {
        Log_copy_error_spi(cstate);
    }

    /* Execute AFTER STATEMENT insertion triggers */
    ExecASInsertTriggers(estate, resultRelInfo);

    /* Handle queued AFTER triggers */
    AfterTriggerEndQuery(estate);

    /* To free the statement allocated in ExecForeignInsert, MOT doesn't need this */
    if (isForeignTbl && !isMOTFromTblOid(RelationGetRelid(cstate->rel))) {
        resultRelInfo->ri_FdwRoutine->EndForeignModify(estate, resultRelInfo);
    }

    pfree_ext(values);
    pfree_ext(nulls);

    ExecResetTupleTable(estate->es_tupleTable, false);

    ExecCloseIndices(resultRelInfo);

    if (estate->esfRelations != NULL) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }
    FreeExecutorState(estate);

    /*
     * If we skipped writing WAL, then we need to sync the heap (but not
     * indexes since those use WAL anyway)
     */
    if (needflush) {
        if (isPartitionRel) {
            ListCell *cell = NULL;
            foreach(cell, partitionList) {
                Oid partitionOid = lfirst_oid(cell);
                partition_sync(resultRelationDesc, partitionOid, AccessShareLock);
            }
        } else {
            heap_sync(cstate->rel);
        }
    }

    if (enable_heap_bcm_data_replication() && !RelationIsForeignTable(cstate->rel)
        && !RelationIsStream(cstate->rel) && !RelationIsSegmentTable(cstate->rel))
        HeapSyncHashSearch(cstate->rel->rd_id, HASH_REMOVE);

    list_free_ext(partitionList);

    cstate->loadrows = processed;
    Log_copy_summary_spi(cstate);

    return processed;
}

/*
 * @Description: estimate memory info for cstore copy. There are three branches, 1)for cstore table. 2) for
 * dfs table. 3)for cstore partition table. The method for estimation memory is same to vecmodifytable's insert.
 * the tuples is used MAX_BATCH_ROWS and PARTIAL_CLUSTER_ROWS to estimate and not the actual tuples.
 * the main reason is copy can not estimate the actual tuples for files. For superuser, we can use fstat() to estimate
 * the file size(<2g), but the memory manager is not to manage the superuser's memory. For ordinary user, '\COPY FROM'
 * is pipe method, we don't have the file information to estimate.
 * @IN rel: copy' relation.
 * @IN and OUT desc: desc is used to estimating the memory to stmt. SetSortMemInfo() is mainly used to
 * estimate memory. it is same to vecmodifytable's insert.
 * @Return: void
 */
static void EstCopyMemInfo(Relation rel, UtilityDesc* desc)
{
    int width = 0;
    int maxBatchRow = MAX_BATCH_ROWS;
    int partialClusterRows = PARTIAL_CLUSTER_ROWS;
    int tuples = 0;

    maxBatchRow = RelationGetMaxBatchRows(rel);
    partialClusterRows = RelationGetPartialClusterRows(rel);

    width = get_relation_data_width(rel->rd_id, InvalidOid, NULL, rel->rd_rel->relhasclusterkey);

    if (rel->rd_rel->relhasclusterkey) {
        tuples = partialClusterRows;
    } else {
        tuples = maxBatchRow;
    }

    SetSortMemInfo(desc,
        tuples,
        width,
        rel->rd_rel->relhasclusterkey,
        false,
        u_sess->attr.attr_storage.partition_max_cache_size,
        rel);
}

/*
 * @Description: init insert memory info for cstore copy. There are three branches, 1)for cstore table. 2) for
 * dfs table. 3)for cstore partition table. The method for estimation memory is same to vecmodifytable's insert.
 * @IN cstate: copy' information for estimating the memory.
 * @IN CopyMem: CopyMem is used to passing parameters of mem_info to execute on copy.
 * @Return: void
 */
static int getPartitionNumInInitCopy(Relation relation)
{
    int partitionNum = 1;
    if (relation->partMap->type == PART_TYPE_LIST) {
        partitionNum = getNumberOfListPartitions(relation);
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        partitionNum = getNumberOfHashPartitions(relation);
    } else {
        partitionNum = getNumberOfRangePartitions(relation);
    }
    return partitionNum; 
}

static void InitCopyMemArg(CopyState cstate, MemInfoArg* CopyMem)
{
    bool isPartitionRel = false;
    bool hasPck = false;
    int partialClusterRows = PARTIAL_CLUSTER_ROWS;
    int maxBatchSize = MAX_BATCH_ROWS;
    Relation relation = NULL;
    int partitionNum = 1;

    Assert(cstate != NULL);
    relation = cstate->rel;

    Assert(relation != NULL);
    isPartitionRel = RELATION_IS_PARTITIONED(relation);

    /* get cluster key and get max batch size */
    partialClusterRows = RelationGetPartialClusterRows(relation);
    maxBatchSize = RelationGetMaxBatchRows(relation);
    if (relation->rd_rel->relhasclusterkey) {
        hasPck = true;
    }

    if (cstate->memUsage.work_mem > 0) {
        if (!isPartitionRel) {
            /* Init memory for cstore ordinary table.*/
            if (RelationIsCUFormat(cstate->rel)) {
                if (!hasPck) {
                    CopyMem->MemInsert = cstate->memUsage.work_mem;
                    CopyMem->MemSort = 0;
                } else {
                    CopyMem->MemInsert = (int)((double)cstate->memUsage.work_mem * (double)(maxBatchSize * 3) /
                                               (double)(maxBatchSize * 3 + partialClusterRows));
                    CopyMem->MemSort = cstate->memUsage.work_mem - CopyMem->MemInsert;
                }
                CopyMem->canSpreadmaxMem = cstate->memUsage.max_mem;
                CopyMem->spreadNum = 0;
                CopyMem->partitionNum = 1;
                MEMCTL_LOG(DEBUG2,
                    "CStoreInsert(init FOR COPY):Insert workmem is : %dKB, sort workmem: %dKB,can spread mem is %dKB.",
                    CopyMem->MemInsert,
                    CopyMem->MemSort,
                    CopyMem->canSpreadmaxMem);
            } else if (RelationIsPAXFormat(cstate->rel)) {
                /* Init memory for cstore ordinary table.*/
                if (!hasPck) {
                    CopyMem->MemInsert =
                        cstate->memUsage.work_mem < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE : cstate->memUsage.work_mem;
                    if (cstate->memUsage.work_mem < DFS_MIN_MEM_SIZE) {
                        MEMCTL_LOG(LOG,
                            "DfsInsert(init copy) mem is not engough for the basic use: workmem is : %dKB, can spread "
                            "maxMem is %dKB.",
                            cstate->memUsage.work_mem,
                            cstate->memUsage.max_mem);
                    }
                    CopyMem->MemSort = 0;
                } else {
                    /* if has pck, we should first promise the insert memory,
                     * Insert memory : must > 128mb to promise the basic use on DFS insert.
                     * Sort memory : must >10mb to promise the basic use on sort.
                     */
                    CopyMem->MemInsert =
                        cstate->memUsage.work_mem < DFS_MIN_MEM_SIZE ? DFS_MIN_MEM_SIZE : cstate->memUsage.work_mem;
                    if (cstate->memUsage.work_mem < DFS_MIN_MEM_SIZE) {
                        MEMCTL_LOG(LOG,
                            "DfsInsert(init copy pck) mem is not engough for the basic use: workmem is : %dKB, can "
                            "spread maxMem is %dKB.",
                            cstate->memUsage.work_mem,
                            cstate->memUsage.max_mem);
                    }
                    CopyMem->MemSort = cstate->memUsage.work_mem - CopyMem->MemInsert;
                    if (CopyMem->MemSort < SORT_MIM_MEM) {
                        CopyMem->MemSort = SORT_MIM_MEM;
                        if (CopyMem->MemInsert >= DFS_MIN_MEM_SIZE)
                            CopyMem->MemInsert = CopyMem->MemInsert - SORT_MIM_MEM;
                    }
                }
                CopyMem->canSpreadmaxMem = cstate->memUsage.max_mem;
                CopyMem->spreadNum = 0;
                CopyMem->partitionNum = 1;
                MEMCTL_LOG(DEBUG2,
                    "DfsInsert(init copy): workmem is : %dKB, sort workmem: %dKB,can spread maxMem is %dKB.",
                    CopyMem->MemInsert,
                    CopyMem->MemSort,
                    CopyMem->canSpreadmaxMem);
            }
        } else {
            /* Init memory for cstore partition table. */
            partitionNum = getPartitionNumInInitCopy(relation);
            /* if has pck, meminfo = insert + sort. */
            if (!hasPck) {
                CopyMem->MemInsert = cstate->memUsage.work_mem;
                CopyMem->MemSort = 0;
            } else {
                CopyMem->MemInsert = cstate->memUsage.work_mem * 1 / 3;
                CopyMem->MemSort = cstate->memUsage.work_mem - CopyMem->MemInsert;
            }
            CopyMem->canSpreadmaxMem = cstate->memUsage.max_mem;
            CopyMem->spreadNum = 0;
            CopyMem->partitionNum = partitionNum;
            MEMCTL_LOG(DEBUG2,
                "CStorePartInsert(init copy):Insert workmem is : %dKB, sort workmem: %dKB,"
                "parititions totalnum is(%d)can spread maxMem is %dKB.",
                CopyMem->MemInsert,
                CopyMem->MemSort,
                CopyMem->partitionNum,
                CopyMem->canSpreadmaxMem);
        }
    } else {
        /* If the guc use_workload_manager is off, it means we don't use workload manager. The estimated
         * parameters cannot take effect and can only be assigned according to the original logic.
         */
        CopyMem->canSpreadmaxMem = 0;
        CopyMem->MemInsert = u_sess->attr.attr_storage.partition_max_cache_size;
        CopyMem->MemSort = u_sess->attr.attr_storage.psort_work_mem;
        CopyMem->spreadNum = 0;
        CopyMem->partitionNum = 1;
    }
}

static void CopyFromUpdateIndexAndRunAfterRowTrigger(EState* estate, ResultRelInfo* resultRelInfo,
    TupleTableSlot* myslot, HeapTuple* bufferedTuples, int nBufferedTuples, Oid partitionOid, Partition partition,
    int2 bucketid)
{
    int i;
    Relation actualHeap = resultRelInfo->ri_RelationDesc;
    bool ispartitionedtable = false;

    if (RELATION_IS_PARTITIONED(actualHeap)) {
        actualHeap = estate->esCurrentPartition;
        Assert(PointerIsValid(actualHeap));
        ispartitionedtable = true;
    }

    /*
     * If there are any indexes, update them for all the inserted tuples, and
     * run AFTER ROW INSERT triggers.
     */
    if (resultRelInfo->ri_NumIndices > 0) {
        for (i = 0; i < nBufferedTuples; i++) {
            List* recheckIndexes = NIL;

            Assert(ItemPointerIsValid(&(bufferedTuples[i]->t_self)));
            (void)ExecStoreTuple(bufferedTuples[i], myslot, InvalidBuffer, false);
            recheckIndexes = ExecInsertIndexTuples(myslot,
                &(bufferedTuples[i]->t_self),
                estate,
                ispartitionedtable ? actualHeap : NULL,
                ispartitionedtable ? partition : NULL,
                bucketid, NULL, NULL);
            ExecARInsertTriggers(estate, resultRelInfo, partitionOid, bucketid, bufferedTuples[i], recheckIndexes);
            list_free(recheckIndexes);
        }
    } else if (resultRelInfo->ri_TrigDesc != NULL && resultRelInfo->ri_TrigDesc->trig_insert_after_row) {
        /*
         * Although There's no indexes, see if we need to run AFTER ROW INSERT
         * triggers anyway.
         */
        for (i = 0; i < nBufferedTuples; i++) {
            Assert(ItemPointerIsValid(&(bufferedTuples[i]->t_self)));
            ExecARInsertTriggers(estate, resultRelInfo, partitionOid, bucketid, bufferedTuples[i], NIL);
        }
    }
}

/*
 * A subroutine of CopyFrom, to write the current batch of buffered heap
 * tuples to the heap. Also updates indexes and runs AFTER ROW INSERT
 * triggers.
 */
void CopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hi_options,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    HeapTuple* bufferedTuples, Partition partition, int2 bucketId)
{
    MemoryContext oldcontext;
    int i;
    Relation actualHeap = resultRelInfo->ri_RelationDesc;
    bool ispartitionedtable = false;
    Oid partitionOid = InvalidOid;

    if (bufferedTuples == NULL)
        return;

    partitionOid = OidIsValid(rel->parentId) ? RelationGetRelid(rel) : InvalidOid;
    if (RELATION_IS_PARTITIONED(actualHeap)) {
        actualHeap = estate->esCurrentPartition;
        Assert(PointerIsValid(actualHeap));
        ispartitionedtable = true;
    }

    if (bucketId != InvalidBktId) {
        searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, rel, bucketId, rel);
    }
    /*
     * heap_multi_insert leaks memory, so switch to short-lived memory context
     * before calling it.
     */
    oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    /* 1. not to use page compression
     * 2. not forbid page replication
     */
    HeapMultiInsertExtraArgs args = {NULL, 0, false};
    (void)tableam_tuple_multi_insert(rel, resultRelInfo->ri_RelationDesc,
        (Tuple*)bufferedTuples, nBufferedTuples, mycid, hi_options, bistate, &args);
    MemoryContextSwitchTo(oldcontext);

    /*
     * If there are any indexes, update them for all the inserted tuples, and
     * run AFTER ROW INSERT triggers.
     */
    if (resultRelInfo->ri_NumIndices > 0) {
        for (i = 0; i < nBufferedTuples; i++) {
            List* recheckIndexes = NIL;

            (void)ExecStoreTuple(bufferedTuples[i], myslot, InvalidBuffer, false);
            recheckIndexes = ExecInsertIndexTuples(myslot,
                &(((HeapTuple)bufferedTuples[i])->t_self),
                estate,
                ispartitionedtable ? actualHeap : NULL,
                ispartitionedtable ? partition : NULL,
                bucketId, NULL, NULL);
            ExecARInsertTriggers(estate, resultRelInfo, partitionOid, bucketId, (HeapTuple)bufferedTuples[i],
                recheckIndexes);
            list_free(recheckIndexes);
        }
    } else if (resultRelInfo->ri_TrigDesc != NULL && resultRelInfo->ri_TrigDesc->trig_insert_after_row) {
        /*
         * There's no indexes, but see if we need to run AFTER ROW INSERT triggers
         * anyway.
         */
        for (i = 0; i < nBufferedTuples; i++)
            ExecARInsertTriggers(estate, resultRelInfo, partitionOid, bucketId, (HeapTuple)bufferedTuples[i], NIL);
    }
}

/*
 * A subroutine of CopyFrom, to write the current batch of buffered heap
 * tuples to the heap. Also updates indexes and runs AFTER ROW INSERT
 * triggers.
 */
void UHeapCopyFromInsertBatch(Relation rel, EState* estate, CommandId mycid, int hiOptions,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    UHeapTuple* bufferedTuples, Partition partition, int2 bucketId)
{
    MemoryContext oldcontext;
    int i;
    Relation actualHeap = resultRelInfo->ri_RelationDesc;
    bool ispartitionedtable = false;
    Oid partitionOid = InvalidOid;

    if (bufferedTuples == NULL)
        return;

    partitionOid = OidIsValid(rel->parentId) ? RelationGetRelid(rel) : InvalidOid;
    if (RELATION_IS_PARTITIONED(actualHeap)) {
        actualHeap = estate->esCurrentPartition;
        Assert(PointerIsValid(actualHeap));
        ispartitionedtable = true;
    }

    if (bucketId != InvalidBktId) {
        searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, rel, bucketId, rel);
    }
    /*
     * heap_multi_insert leaks memory, so switch to short-lived memory context
     * before calling it.
     */
    oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    /* 1. not to use page compression
     * 2. not forbid page replication
     */
    tableam_tuple_multi_insert(rel,
            NULL,
            (Tuple*)bufferedTuples,
            nBufferedTuples, 
            mycid, 
            hiOptions, 
            bistate,
            NULL);
    MemoryContextSwitchTo(oldcontext);

    /*
     * If there are any indexes, update them for all the inserted tuples, and
     * run AFTER ROW INSERT triggers.
     */
    if (resultRelInfo->ri_NumIndices > 0) {
        for (i = 0; i < nBufferedTuples; i++) {
            List* recheckIndexes = NIL;

            (void)ExecStoreTuple(bufferedTuples[i], myslot, InvalidBuffer, false);

            /*
             * Global Partition Index stores the partition's tableOid with the index
             * tuple which is extracted from the tuple of the slot. Make sure it is set.
             */
            if (myslot->tts_tupslotTableAm != TAM_USTORE) {
                ((HeapTuple)myslot->tts_tuple)->t_tableOid = RelationGetRelid(rel);
            } else {
                ((UHeapTuple)myslot->tts_tuple)->table_oid = RelationGetRelid(rel);
            }

            recheckIndexes = ExecInsertIndexTuples(myslot,
                &(((UHeapTuple)bufferedTuples[i])->ctid),
                estate,
                ispartitionedtable ? actualHeap : NULL,
                ispartitionedtable ? partition : NULL,
                bucketId, NULL, NULL);
            list_free(recheckIndexes);
        }
    } else if (resultRelInfo->ri_TrigDesc != NULL && resultRelInfo->ri_TrigDesc->trig_insert_after_row) {
        /*
         * There's no indexes, but see if we need to run AFTER ROW INSERT triggers
         * anyway.
         */
        for (i = 0; i < nBufferedTuples; i++) {
            ExecARInsertTriggers(estate, resultRelInfo, partitionOid, bucketId, (HeapTuple)bufferedTuples[i], NIL);
        }
    }
}

void FlushInsertSelectBulk(DistInsertSelectState* node, EState* estate, bool canSetTag, int hi_options,
                           List** partitionList)
{
    ResultRelInfo* resultRelInfo = NULL;
    Relation resultRelationDesc;
    TupleTableSlot* slot = NULL;
    bool needflush = false;
    
    /* get information on the (current) result relation
     */
    resultRelInfo = estate->es_result_relation_info;
    resultRelationDesc = resultRelInfo->ri_RelationDesc;

    slot = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(slot, RelationGetDescr(resultRelationDesc));
    
    if (enable_heap_bcm_data_replication() && !RelationIsForeignTable(resultRelationDesc)
        && !RelationIsStream(resultRelationDesc) && !RelationIsSegmentTable(resultRelationDesc)) {
        needflush = true;
    }
    
    if (node->mgr->isPartRel) {
        int cnt = 0;
        Assert(RELATION_IS_PARTITIONED(resultRelationDesc) || RELATION_CREATE_BUCKET(resultRelationDesc));
        for (cnt = 0; cnt < node->mgr->numMemCxt; cnt++) {
            CopyFromMemCxt copyFromMemCxt = node->mgr->memCxt[cnt];
            if (copyFromMemCxt->memCxtSize > 0) {
                Assert(copyFromMemCxt->nextBulk > 0);

                /* partition oid for sync */
                if (needflush) {
                    for (int16 i = 0; i < copyFromMemCxt->nextBulk; i++) {
                        *partitionList = list_append_unique_oid(*partitionList, copyFromMemCxt->chunk[i]->partOid);
                    }
                }
                
                (void)CopyFromChunkInsert<true>(NULL,
                    estate,
                    copyFromMemCxt->chunk[0],
                    node->mgr,
                    node->pcState,
                    estate->es_output_cid,
                    hi_options,
                    resultRelInfo,
                    slot,
                    node->bistate);
            }

            /* the following code should be placed into function deinitCopyFromManager()
             * according to SRP rule. But it's here in order to reduce the FOR calling. */
            Assert(copyFromMemCxt->nextBulk == 0);
            Assert(copyFromMemCxt->memCxtSize == 0);
            MemoryContextDelete(copyFromMemCxt->memCxtCandidate);
            pfree_ext(copyFromMemCxt);
        }
    } else if (node->mgr->bulk->numTuples > 0) {
        Assert(!node->mgr->isPartRel);
        node->mgr->LastFlush = true;
        (void)CopyFromChunkInsert<true>(NULL,
            estate,
            node->mgr->bulk,
            node->mgr,
            node->pcState,
            estate->es_output_cid,
            hi_options,
            resultRelInfo,
            slot,
            node->bistate);
    }

    if (!needflush) {
        return;
    }
    
    if (resultRelationDesc->rd_rel->parttype ==  PARTTYPE_PARTITIONED_RELATION) {
        ListCell *cell = NULL;
        foreach(cell, *partitionList) {
            Oid partitionOid = lfirst_oid(cell);
            partition_sync(resultRelationDesc, partitionOid, RowExclusiveLock);
        }
    } else {
        heap_sync(resultRelationDesc);
    }
    HeapSyncHashSearch(resultRelationDesc->rd_id, HASH_REMOVE);
}

/* the same to CopyFromInsertBatch, but compress first. */
static int CopyFromCompressAndInsertBatch(PageCompress* pcState, EState* estate, CommandId mycid, int hi_options,
    ResultRelInfo* resultRelInfo, TupleTableSlot* myslot, BulkInsertState bistate, int nBufferedTuples,
    HeapTuple* bufferedTuples, bool isLast, Relation heapRelation, Partition partition, int2 bucketId)
{
    MemoryContext oldcontext;
    MemoryContext selfcontext;
    int nInsertedTuples = 0;
    int total = 0;
    Oid partitionId;

    Assert(heapRelation != NULL);
    Assert((bufferedTuples != NULL) && (nBufferedTuples > 0));
    Assert((pcState != NULL) && (estate != NULL) && (myslot != NULL));
    Assert(resultRelInfo != NULL);

    if (bucketId != InvalidBktId) {
        searchHBucketFakeRelation(estate->esfRelations, estate->es_query_cxt, heapRelation, bucketId, heapRelation);
    }
    pcState->SetBatchTuples(bufferedTuples, nBufferedTuples, isLast);

    /* heap_multi_insert() will alloc new memory, so switch to private memcontext for one page. */
    selfcontext = pcState->SelfMemCnxt();
    oldcontext = MemoryContextSwitchTo(selfcontext);

    while (pcState->CompressOnePage()) {
        /* 1. use page compression
         * 2. not forbid page replication
         */
        HeapMultiInsertExtraArgs args = {pcState->GetCmprHeaderData(), pcState->GetCmprHeaderSize(), false};

        nInsertedTuples = tableam_tuple_multi_insert(heapRelation,
            resultRelInfo->ri_RelationDesc,
            (Tuple*)pcState->GetOutputTups(),
            pcState->GetOutputCount(),
            mycid,
            hi_options,
            bistate,
            &args);
        if (pcState->GetOutputCount() != nInsertedTuples) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("Missing data when batch insert compressed data !")));
        }

        Assert(nInsertedTuples > 0);
        total += nInsertedTuples;

        /* when tuples are compressed, rewrite all the transaction info. */
        pcState->BackWrite();
    }

    (void)MemoryContextSwitchTo(oldcontext);

    /* according to openGauss, After this buffer is handled, update index and run trigger one time */
    partitionId = OidIsValid(heapRelation->parentId) ? RelationGetRelid(heapRelation) : InvalidOid;
    CopyFromUpdateIndexAndRunAfterRowTrigger(
        estate, resultRelInfo, myslot, bufferedTuples, total, partitionId, partition, bucketId);

    /* move the remains within this buffer ahead. */
    int remain = pcState->Remains();
    if (remain > 0 && remain != nBufferedTuples) {
        int i;
        for (i = 0; i < total; i++) {
            pfree_ext(bufferedTuples[i]);
        }

        for (i = 0; i < remain; i++) {
            bufferedTuples[i] = (HeapTuple)bufferedTuples[total + i];
        }
    }
    Assert((remain > 0) || (total == nBufferedTuples));

    return total;
}

// Check if the type can accept empty string
//
bool IsTypeAcceptEmptyStr(Oid typeOid)
{
    switch (typeOid) {
        case VARCHAROID:
        case NVARCHAR2OID:
        case TEXTOID:
        case TSVECTOROID:
        case BPCHAROID:
        case RAWOID:
        case OIDVECTOROID:
        case INT2VECTOROID:
        case BYTEAOID:
        case CLOBOID:
        case BLOBOID:
        case NAMEOID:
        case CHAROID:
            return true;
        default:
            return false;
    }
}

static void CopyInitSpecColInfo(CopyState cstate, List* attnamelist, AttrNumber num_phys_attrs)
{
    ListCell* lc = NULL;
    int attrno;

    if (cstate->sequence_col_list != NULL) {
        cstate->sequence = (SqlLoadSequInfo **)palloc0(num_phys_attrs * sizeof(SqlLoadSequInfo*));
        List* sequelist = cstate->sequence_col_list;
        SqlLoadSequInfo *sequeInfo = NULL;

        foreach (lc, sequelist) {
            sequeInfo = (SqlLoadSequInfo *)lfirst(lc);
            attrno = CopyGetColumnListIndex(cstate, attnamelist, sequeInfo->colname);
            cstate->sequence[attrno - 1] = sequeInfo;
        }
    }

    if (cstate->filler_col_list != NULL) {
        int palloc_size = num_phys_attrs + list_length(cstate->filler_col_list);
        cstate->filler = (SqlLoadFillerInfo **)palloc0(palloc_size * sizeof(SqlLoadFillerInfo*));
        List* fillerlist = cstate->filler_col_list;
        SqlLoadFillerInfo *fillerInfo = NULL;

        foreach (lc, fillerlist) {
            fillerInfo = (SqlLoadFillerInfo *)lfirst(lc);
            attrno = fillerInfo->index;
            cstate->filler[attrno - 1] = fillerInfo;
        }
    }

    if (cstate->constant_col_list != NULL) {
        cstate->constant = (SqlLoadConsInfo **)palloc0(num_phys_attrs * sizeof(SqlLoadConsInfo*));
        List* conslist = cstate->constant_col_list;
        SqlLoadConsInfo *consInfo = NULL;

        foreach (lc, conslist) {
            consInfo = (SqlLoadConsInfo *)lfirst(lc);
            attrno = CopyGetColumnListIndex(cstate, attnamelist, consInfo->colname);
            cstate->constant[attrno - 1] = consInfo;
        }
    }
}

static void CopyInitCstateVar(CopyState cstate)
{
    /* Initialize state variables */
    cstate->fe_eof = false;
    if (cstate->eol_type != EOL_UD)
        cstate->eol_type = EOL_UNKNOWN;
    cstate->cur_relname = RelationGetRelationName(cstate->rel);
    cstate->cur_lineno = 0;
    cstate->cur_attname = NULL;
    cstate->cur_attval = NULL;
    cstate->illegal_chars_error = NIL;

    /* Set up variables to avoid per-attribute overhead. */
    initStringInfo(&cstate->attribute_buf);
    initStringInfo(&cstate->sequence_buf);
    initStringInfo(&cstate->line_buf);
    cstate->line_buf_converted = false;
    cstate->raw_buf = (char*)palloc(RAW_BUF_SIZE + 1);
    cstate->raw_buf_index = cstate->raw_buf_len = 0;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
CopyState BeginCopyFrom(Relation rel, const char* filename, List* attnamelist, 
                             List* options, void* mem_info, const char* queryString)
{
    CopyState cstate;
    bool pipe = (filename == NULL);
    TupleDesc tupDesc;
    Form_pg_attribute* attr = NULL;
    AttrNumber num_phys_attrs, num_defaults;
    FmgrInfo* in_functions = NULL;
    Oid* typioparams = NULL;
    bool* accept_empty_str = NULL;
    int attnum;
    Oid in_func_oid;
    int* defmap = NULL;
    ExprState** defexprs = NULL;
    MemoryContext oldcontext;
    AdaptMem* memUsage = (AdaptMem*)mem_info;
    bool volatile_defexprs = false;

    cstate = BeginCopy(true, rel, NULL, queryString, attnamelist, options);
    oldcontext = MemoryContextSwitchTo(cstate->copycontext);

    /* Initialize state variables */
    CopyInitCstateVar(cstate);

    tupDesc = RelationGetDescr(cstate->rel);
    attr = tupDesc->attrs;
    num_phys_attrs = tupDesc->natts;
    num_defaults = 0;
    volatile_defexprs = false;

    if (cstate->is_from && (cstate->log_errors || cstate->logErrorsData)) {
        cstate->logger->m_namespace = get_namespace_name(RelationGetNamespace(cstate->rel));
    }

    /*
     * Pick up the required catalog information for each attribute in the
     * relation, including the input function, the element type (to pass to
     * the input function), and info about defaults and constraints. (Which
     * input function we use depends on text/binary format choice.)
     */
    in_functions = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
    typioparams = (Oid*)palloc(num_phys_attrs * sizeof(Oid));
    accept_empty_str = (bool*)palloc(num_phys_attrs * sizeof(bool));
    defmap = (int*)palloc(num_phys_attrs * sizeof(int));
    defexprs = (ExprState**)palloc(num_phys_attrs * sizeof(ExprState*));
    if (cstate->trans_expr_list != NULL) {
        cstate->as_typemods = (AssignTypmod *)palloc0(num_phys_attrs * sizeof(AssignTypmod));
        cstate->transexprs = (ExprState **)palloc0(num_phys_attrs * sizeof(ExprState*));
    }
    CopyInitSpecColInfo(cstate, attnamelist, num_phys_attrs);

#ifdef ENABLE_MULTIPLE_NODES
    /*
     * Don't support COPY FROM table with INSERT trigger as whether or not fire
     * INSERT trigger in this condition will both cause confusion.
     */
    if (cstate->rel->trigdesc != NULL && pgxc_has_trigger_for_event(TRIGGER_TYPE_INSERT, cstate->rel->trigdesc))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Don't suppport COPY FROM table with INSERT triggers"),
                errhint("You may need drop the INSERT trigger first.")));
#endif

    /* Output functions are required to convert default values to output form */
    cstate->out_functions = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));

    for (attnum = 1; attnum <= num_phys_attrs; attnum++) {
        /* We don't need info for dropped attributes */
        if (attr[attnum - 1]->attisdropped)
            continue;

        accept_empty_str[attnum - 1] = IsTypeAcceptEmptyStr(attr[attnum - 1]->atttypid);
        /* Fetch the input function and typioparam info */
        if (IS_BINARY(cstate))
            getTypeBinaryInputInfo(attr[attnum - 1]->atttypid, &in_func_oid, &typioparams[attnum - 1]);
        else
            getTypeInputInfo(attr[attnum - 1]->atttypid, &in_func_oid, &typioparams[attnum - 1]);
        fmgr_info(in_func_oid, &in_functions[attnum - 1]);

        /* Get default info if needed */
        if (!list_member_int(cstate->attnumlist, attnum) && !ISGENERATEDCOL(tupDesc, attnum - 1)) {
            /* attribute is NOT to be copied from input */
            /* use default value if one exists */
            Expr *defexpr = (Expr *)build_column_default(cstate->rel, attnum);

            if (defexpr != NULL) {
#ifdef PGXC
                if (IS_PGXC_COORDINATOR) {
                    /*
                     * If default expr is shippable to Datanode, don't include
                     * default values in the data row sent to the Datanode; let
                     * the Datanode insert the default values.
                     */
                    Expr* planned_defexpr = expression_planner((Expr*)defexpr);
                    if ((!pgxc_is_expr_shippable(planned_defexpr, NULL)) ||
                        (list_member_int(cstate->remoteCopyState->idx_dist_by_col, attnum - 1))) {
                        Oid out_func_oid;
                        bool isvarlena = false;
                        /* Initialize expressions in copycontext. */
                        defexprs[num_defaults] = ExecInitExpr(planned_defexpr, NULL);
                        defmap[num_defaults] = attnum - 1;
                        num_defaults++;

                        /*
                         * Initialize output functions needed to convert default
                         * values into output form before appending to data row.
                         */
                        if (IS_BINARY(cstate))
                            getTypeBinaryOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
                        else
                            getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
                        fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
                    }
                } else {
#endif /* PGXC */
                    /* Run the expression through planner */
                    defexpr = expression_planner(defexpr);

                    /* Initialize executable expression in copycontext */
                    defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
                    defmap[num_defaults] = attnum - 1;
                    num_defaults++;

                    /* Check to see if we have any volatile expressions */
                    if (!volatile_defexprs)
                        volatile_defexprs = contain_volatile_functions((Node*)defexpr);
#ifdef PGXC
                }
#endif
            }
        }
    }

    /* We keep those variables in cstate. */
    cstate->in_functions = in_functions;
    cstate->typioparams = typioparams;
    cstate->accept_empty_str = accept_empty_str;
    cstate->defmap = defmap;
    cstate->defexprs = defexprs;
    cstate->volatile_defexprs = volatile_defexprs;
    cstate->num_defaults = num_defaults;

    if (pipe) {
        if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote)
            ReceiveCopyBegin(cstate);
        else
            cstate->copy_file = stdin;
    } else {
        struct stat st;

        cstate->filename = pstrdup(filename);
        if (!u_sess->sec_cxt.last_roleid_is_super && CheckCopyFileInBlackList(cstate->filename)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME), errmsg("do not support system admin to COPY from %s", filename)));
        }

        cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);

        if (cstate->copy_file == NULL)
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not open file \"%s\" for reading: %m", cstate->filename)));

        fstat(fileno(cstate->copy_file), &st);
        if (S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a directory", cstate->filename)));
    }

    if (!IS_BINARY(cstate)) {
        /* must rely on user to tell us... */
        cstate->file_has_oids = cstate->oids;
    } else {
        /* Read and verify binary header */
        char readSig[11];
        int32 tmp;

        /* Signature */
        if (CopyGetData(cstate, readSig, 11, 11) != 11 || memcmp(readSig, BinarySignature, 11) != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("COPY file signature not recognized")));
        /* Flags field */
        if (!CopyGetInt32(cstate, &tmp))
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid COPY file header (missing flags)")));
        cstate->file_has_oids = (tmp & (1 << 16)) != 0;
        tmp &= ~(1 << 16);
        if ((tmp >> 16) != 0)
            ereport(ERROR,
                (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unrecognized critical flags in COPY file header")));
        /* Header extension length */
        if (!CopyGetInt32(cstate, &tmp) || tmp < 0)
            ereport(
                ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid COPY file header (missing length)")));
        /* Skip extension header, if present */
        while (tmp-- > 0) {
            if (CopyGetData(cstate, readSig, 1, 1) != 1)
                ereport(
                    ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid COPY file header (wrong length)")));
        }
    }

    if (cstate->file_has_oids && IS_BINARY(cstate)) {
        getTypeBinaryInputInfo(OIDOID, &in_func_oid, &cstate->oid_typioparam);
        fmgr_info(in_func_oid, &cstate->oid_in_function);
    }

    /* create workspace for CopyReadAttributes results */
    if (!IS_BINARY(cstate)) {
        cstate_fields_buffer_init(cstate);
    }

    if (cstate->trans_expr_list != NULL)
        TransformColExpr(cstate);

    if (cstate->when != NULL) {
        CopyGetWhenListAttFieldno(cstate, attnamelist);
    }

    (void)MemoryContextSwitchTo(oldcontext);

    /* workload client manager */
    /* Estimate how many memory is needed in dn */
    if (IS_PGXC_COORDINATOR && ENABLE_WORKLOAD_CONTROL && g_instance.wlm_cxt->dynamic_workload_inited &&
        RelationIsColStore(rel)) {
        UtilityDesc desc;
        errno_t rc = memset_s(&desc, sizeof(UtilityDesc), 0, sizeof(UtilityDesc));
        securec_check(rc, "\0", "\0");

        EstCopyMemInfo(rel, &desc);
        WLMInitQueryPlan((QueryDesc*)&desc, false);
        dywlm_client_manager((QueryDesc*)&desc, false);
        AdjustIdxMemInfo(memUsage, &desc);
    }

    return cstate;
}

static bool CopyReadLineMatchWhenPosition(CopyState cstate)
{
    bool done = false;
    while (1) { /* while for when position retry */
    
        cstate->cur_lineno++;
    
        /* Actually read the line into memory here */
        done = CopyReadLine(cstate);
        if (done) {
            break;
        }
    
        if (IfCopyLineMatchWhenListPosition(cstate) == true) {
            break;
        } else {
            cstate->whenrows++;
            if (cstate->log_errors || cstate->logErrorsData) {
                FormAndSaveImportWhenLog(cstate, cstate->err_table, cstate->copy_beginTime, cstate->logger);
            }
        }
    }

    return done;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool NextCopyFromRawFields(CopyState cstate, char*** fields, int* nfields)
{
    int fldct = 0;
    bool done = false;

    /* only available for text or csv input */
    Assert(!IS_BINARY(cstate));

    /* on input just throw the header line away */
    if (cstate->cur_lineno == 0 && cstate->header_line) {
        cstate->cur_lineno++;
        if (CopyReadLine(cstate))
            return false; /* done */
    }

    while (cstate->skiprows < cstate->skip) {
        cstate->cur_lineno++;
        if (CopyReadLine(cstate))
            return false; /* done */
        cstate->skiprows++;
    }

    while (1) { /* while for when field retry */

        done = CopyReadLineMatchWhenPosition(cstate);

        /*
        * EOF at start of line means we're done.  If we see EOF after some
        * characters, we act as though it was newline followed by EOF, ie,
        * process the line and then exit loop on next iteration.
        */
        if (done && cstate->line_buf.len == 0)
            return false;

        /* Parse the line into de-escaped field values */
        fldct = cstate->readAttrsFunc(cstate);

        if (IfCopyLineMatchWhenListField(cstate) == true) {
            break;
        }

        cstate->whenrows++;
        if (cstate->log_errors || cstate->logErrorsData) {
            FormAndSaveImportWhenLog(cstate, cstate->err_table, cstate->copy_beginTime, cstate->logger);
        }
    }

    *fields = cstate->raw_fields;
    *nfields = fldct;
    return true;
}

/**
 * @Description: check whether the oid type inputed is char type or not.
 * @in attr_type: a oid type inputed for a attribute
 * @return: true for char type; false for non-char type
 */
bool IsCharType(Oid attr_type)
{
    return ((attr_type == BPCHAROID) || (attr_type == VARCHAROID) || (attr_type == NVARCHAR2OID) ||
            (attr_type == CLOBOID) || (attr_type == TEXTOID));
}

static void ReportIllegalCharError(IllegalCharErrInfo* err_info)
{
    if (err_info->err_code == ERRCODE_UNTRANSLATABLE_CHARACTER) {
        ereport(ERROR,
            (errcode(err_info->err_code),
                errmsg("bulkload illegal chars conversion is just allowed for char type attribute, the original "
                       "character with byte sequence %s in encoding \"%s\" has no equivalent in encoding \"%s\"",
                    err_info->err_info,
                    pg_enc2name_tbl[err_info->src_encoding].name,
                    pg_enc2name_tbl[err_info->dest_encoding].name)));
    }
    if (err_info->err_code == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE) {
        ereport(ERROR,
            (errcode(err_info->err_code),
                errmsg("bulkload illegal chars conversion is just allowed for char type attribute, the original "
                       "invalid byte sequence for encoding \"%s\": %s",
                    pg_enc2name_tbl[err_info->dest_encoding].name,
                    err_info->err_info)));
    }
}

/**
 * @Description: bulkload compatible illegal chars conversion just works for char type attribute,
 * for non-char type attribute original invalid char error still need to be rethrown.
 * @in cstate: the current bulkload CopyState
 * @in attr: all tuple attributes
 * @in field_strings: each parsed field string
 * @return: void
 */
static void BulkloadIllegalCharsErrorCheck(CopyState cstate, Form_pg_attribute* attr, char** field_strings)
{
    ListCell* cur = NULL;
    ListCell* attr_cur = NULL;
    int fieldno = 0;

#ifdef PGXC
    /*
     * loop to check each illegal chars conversion.
     */
    if (cstate->illegal_chars_error) {
        IllegalCharErrInfo* err_info = NULL;

        foreach (cur, cstate->illegal_chars_error) {
            err_info = (IllegalCharErrInfo*)lfirst(cur);
            fieldno = 0;

            int attnum = -1;
            int m = -1;

            foreach (attr_cur, cstate->attnumlist) {
                attnum = lfirst_int(attr_cur);
                m = attnum - 1;

                if (err_info->err_field_no == fieldno) {
                    /*
                     * for non-char type attribute illegal chars error still be thrown.
                     */
                    if (!IsCharType(attr[m]->atttypid)) {
                        /*
                         * reflush illegal chars error list for the next parsed line.
                         * no need extra pfree and depends on memory context reset.
                         */
                        ReportIllegalCharError(err_info);
                    }

                    /*
                     * go to next illegal chars conversion check.
                     */
                    break;
                }

                fieldno++;
            }
        }
    }
#endif
}

extern bool SPI_push_conditional(void);
extern void SPI_pop_conditional(bool pushed);

/**
 * @Description: Call a previously-looked-up datatype input function for bulkload compatibility, which prototype
 * is InputFunctionCall. The new one tries to input bulkload compatiblity-specific parameters to built-in datatype
 * input function.
 * @in cstate: the current CopyState
 * @in flinfo: inputed FmgrInfo
 * @in str: datatype value in string-format
 * @in typioparam: some datatype oid
 * @in typmod:  some datatype mode
 * @return: the datatype value outputed from built-in datatype input function
 */
Datum InputFunctionCallForBulkload(CopyState cstate, FmgrInfo* flinfo, char* str, Oid typioparam, int32 typmod)
{
    FunctionCallInfoData fcinfo;
    Datum result;
    bool pushed = false;
    short nargs = 3;
    char* date_time_fmt = NULL;

    if (str == NULL && flinfo->fn_strict)
        return (Datum)0; /* just return null result */

    SPI_STACK_LOG("push cond", NULL, NULL);
    pushed = SPI_push_conditional();

    switch (typioparam) {
        case DATEOID: {
            if (cstate->date_format.fmt) {
                nargs = 4;
                date_time_fmt = (char*)cstate->date_format.fmt;
            }
        } break;
        case TIMEOID: {
            if (cstate->time_format.fmt) {
                nargs = 4;
                date_time_fmt = (char*)cstate->time_format.fmt;
            }
        } break;
        case TIMESTAMPOID: {
            if (cstate->timestamp_format.fmt) {
                nargs = 4;
                date_time_fmt = (char*)cstate->timestamp_format.fmt;
            }
        } break;
        case SMALLDATETIMEOID: {
            if (cstate->smalldatetime_format.fmt) {
                nargs = 4;
                date_time_fmt = (char*)cstate->smalldatetime_format.fmt;
            }
        } break;
        default: {
            /*
             * do nothing.
             */
        } break;
    }

    InitFunctionCallInfoData(fcinfo, flinfo, nargs, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = CStringGetDatum(str);
    fcinfo.arg[1] = ObjectIdGetDatum(typioparam);
    fcinfo.arg[2] = Int32GetDatum(typmod);

    fcinfo.argnull[0] = (str == NULL);
    fcinfo.argnull[1] = false;
    fcinfo.argnull[2] = false;

    /*
     * input specified datetime format.
     */
    if (nargs == 4) {
        fcinfo.arg[3] = CStringGetDatum(date_time_fmt);
        fcinfo.argnull[3] = false;
    }

    result = FunctionCallInvoke(&fcinfo);

    /* Should get null result if and only if str is NULL */
    if (str == NULL) {
        if (!fcinfo.isnull)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("input function %u returned non-NULL", fcinfo.flinfo->fn_oid)));
    } else {
        if (fcinfo.isnull)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("input function %u returned NULL", fcinfo.flinfo->fn_oid)));
    }

    SPI_STACK_LOG("pop cond", NULL, NULL);
    SPI_pop_conditional(pushed);

    return result;
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 * Oid of the tuple is returned with 'tupleOid' separately.
 */
bool NextCopyFrom(CopyState cstate, ExprContext* econtext, Datum* values, bool* nulls, Oid* tupleOid)
{
    TupleDesc tupDesc;
    Form_pg_attribute* attr = NULL;
    AttrNumber num_phys_attrs;
    AttrNumber attr_count;
    AttrNumber num_defaults = cstate->num_defaults;
    FmgrInfo* in_functions = cstate->in_functions;
    Oid* typioparams = cstate->typioparams;
    bool* accept_empty_str = cstate->accept_empty_str;
    int i;
    int nfields;
    bool isnull = false;
    bool file_has_oids = cstate->file_has_oids;
    int* defmap = cstate->defmap;
    ExprState** defexprs = cstate->defexprs;
    errno_t rc = 0;
    bool pseudoTsDistcol = false;
    int tgt_col_num = -1;
    AssignTypmod *asTypemods = cstate->as_typemods;
    int32 atttypmod;

    tupDesc = RelationGetDescr(cstate->rel);
    attr = tupDesc->attrs;
    num_phys_attrs = tupDesc->natts;
    attr_count = list_length(cstate->attnumlist);
    nfields = file_has_oids ? (attr_count + 1) : attr_count;

    /* Initialize all values for row to NULL */
    rc = memset_s(values, num_phys_attrs * sizeof(Datum), 0, num_phys_attrs * sizeof(Datum));
    securec_check(rc, "", "");
    rc = memset_s(nulls, num_phys_attrs * sizeof(bool), true, num_phys_attrs * sizeof(bool));
    securec_check(rc, "", "");

    /* For both CN & DN,
     * check whether the operation is performed on the timeseries table with
     * an implicit distribution column.
     * If so, find out the hidden column index and set pseudoTsDistcol.
     */
    if (RelationIsTsStore(cstate->rel)) {
        List* dist_col = NIL;
        if (IS_PGXC_COORDINATOR) {
            dist_col = cstate->rel->rd_locator_info->partAttrNum;
            if (list_length(dist_col)==1) {
                tgt_col_num = lfirst_int(list_head(dist_col)) - 1;
                if (TsRelWithImplDistColumn(attr, tgt_col_num)) {
                    pseudoTsDistcol = true;
                }
            }
        } else {
            for (i = 0; i < num_phys_attrs; i++) {
                if (TsRelWithImplDistColumn(attr, i)) {
                    tgt_col_num = i;
                    pseudoTsDistcol = true;
                    break;
                }
            }
        }
    }

    if (!IS_BINARY(cstate)) {
        char** field_strings;
        ListCell* cur = NULL;
        int fldct;      // total num of field in file
        int fieldno;    // current field count
        char* string = NULL;
        bool all_null_flag = true;

        while (all_null_flag) {
            /* read raw fields in the next line */
            if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
                return false;
            /* trailing nullcols */
            if (cstate->fill_missing_fields != -1)
                break;
            for (i = 0; i < fldct; i++) {
                /* treat empty C string as NULL */
                if (field_strings[i] != NULL && field_strings[i][0] != '\0') {
                    all_null_flag = false;
                    break;
                }
            }
            if (all_null_flag)
                cstate->allnullrows++;
        }

        /* check for overflowing fields. if cstate->ignore_extra_data is true,  ignore overflowing fields */
        if (nfields > 0 && fldct > nfields && !cstate->ignore_extra_data)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));

        /*
         * check bulkload illegal chars conversion works just for string type,
         * the precondition of which is the above overflowing fields check.
         */
        if (u_sess->cmd_cxt.bulkload_copy_state && (cstate != u_sess->cmd_cxt.bulkload_copy_state)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("the bulkload state isn't accordant")));
        }

        BulkloadIllegalCharsErrorCheck(cstate, attr, field_strings);

        fieldno = 0;

        /* Read the OID field if present */
        if (file_has_oids) {
            if (fieldno >= fldct)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("missing data for OID column")));
            string = field_strings[fieldno++];

            if (string == NULL)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("null OID in COPY data")));
            else if (cstate->oids && tupleOid != NULL) {
                cstate->cur_attname = "oid";
                cstate->cur_attval = string;
                *tupleOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(string)));
                if (*tupleOid == InvalidOid)
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid OID in COPY data")));
                cstate->cur_attname = NULL;
                cstate->cur_attval = NULL;
            }
        }

        /* Loop to read the user attributes on the line. */
        foreach (cur, cstate->attnumlist) {
            int attnum = lfirst_int(cur);
            int m = attnum - 1;

            /* Skip the hidden distribute column */
            if (pseudoTsDistcol && m == tgt_col_num) {
                nfields--;
                continue;
            }

            /* 0 off;1 Compatible with the original copy; -1 trailing nullcols */
            if (cstate->fill_missing_fields != -1){
                if (fieldno > fldct || (!cstate->fill_missing_fields && fieldno == fldct))
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                                    errmsg("missing data for column \"%s\"", NameStr(attr[m]->attname))));
            }

            if (fieldno < fldct) {
                string = field_strings[fieldno++];

                /* treat empty C string as NULL if either one of the followings does:
                 * 1. A db SQL compatibility requires; or
                 * 2. This column donesn't accept any empty string.
                 */
                if ((u_sess->attr.attr_sql.sql_compatibility == A_FORMAT || !accept_empty_str[m]) &&
                    (string != NULL && string[0] == '\0')) {
                    /* for any type, '' = null */
                    string = NULL;
                }
            } else {
                string = NULL;
                fieldno++;
            }

            if ((IS_CSV(cstate) && string == NULL && cstate->force_notnull_flags[m])) {
                /* Go ahead and read the NULL string */
                string = cstate->null_print;
            }

            cstate->cur_attname = NameStr(attr[m]->attname);
            cstate->cur_attval = string;
            atttypmod = (asTypemods != NULL && asTypemods[m].assign) ? asTypemods[m].typemod : attr[m]->atttypmod;
            values[m] =
                InputFunctionCallForBulkload(cstate, &in_functions[m], string, typioparams[m], atttypmod);
            if (string != NULL)
                nulls[m] = false;
            cstate->cur_attname = NULL;
            cstate->cur_attval = NULL;
        }

        Assert(fieldno == nfields);
    } else {
        /* binary */
        int16 fld_count;
        ListCell* cur = NULL;

        cstate->cur_lineno++;

        if (!CopyGetInt16(cstate, &fld_count)) {
#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                /* Empty buffer */
                resetStringInfo(&cstate->line_buf);

                enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
                /* Receive field count directly from Datanodes */
                fld_count = htons(fld_count);
                appendBinaryStringInfo(&cstate->line_buf, (char*)&fld_count, sizeof(uint16));
            }
#endif

            /* EOF detected (end of file, or protocol-level EOF) */
            return false;
        }

        if (fld_count == -1) {
            /*
             * Received EOF marker.  In a V3-protocol copy, wait for the
             * protocol-level EOF, and complain if it doesn't come
             * immediately.  This ensures that we correctly handle CopyFail,
             * if client chooses to send that now.
             *
             * Note that we MUST NOT try to read more data in an old-protocol
             * copy, since there is no protocol-level EOF marker then.	We
             * could go either way for copy from file, but choose to throw
             * error if there's data after the EOF marker, for consistency
             * with the new-protocol case.
             */
            char dummy;

#ifdef PGXC
            if (IS_PGXC_COORDINATOR) {
                /* Empty buffer */
                resetStringInfo(&cstate->line_buf);

                enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
                /* Receive field count directly from Datanodes */
                fld_count = htons(fld_count);
                appendBinaryStringInfo(&cstate->line_buf, (char*)&fld_count, sizeof(uint16));
            }
#endif

            if (cstate->copy_dest != COPY_OLD_FE && CopyGetData(cstate, &dummy, 1, 1) > 0)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("received copy data after EOF marker")));
            return false;
        }
        if (fld_count != attr_count && !pseudoTsDistcol)
            ereport(ERROR,
                (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                    errmsg("row field count is %d, expected %d", (int)fld_count, attr_count)));

#ifdef PGXC
        if (IS_PGXC_COORDINATOR) {
            /*
             * Include the default value count also, because we are going to
             * append default values to the user-supplied attributes.
             */
            int16 total_fld_count = fld_count + num_defaults;
            /* Empty buffer */
            resetStringInfo(&cstate->line_buf);

            enlargeStringInfo(&cstate->line_buf, sizeof(uint16));
            total_fld_count = htons(total_fld_count);
            appendBinaryStringInfo(&cstate->line_buf, (char*)&total_fld_count, sizeof(uint16));
        }
#endif

        if (file_has_oids) {
            Oid loaded_oid;

            cstate->cur_attname = "oid";
            loaded_oid = DatumGetObjectId(
                CopyReadBinaryAttribute(cstate, 0, &cstate->oid_in_function, cstate->oid_typioparam, -1, &isnull));
            if (isnull || loaded_oid == InvalidOid)
                ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid OID in COPY data")));
            cstate->cur_attname = NULL;
            if (cstate->oids && tupleOid != NULL)
                *tupleOid = loaded_oid;
        }

        i = 0;
        foreach (cur, cstate->attnumlist) {
            int attnum = lfirst_int(cur);
            int m = attnum - 1;

            /* Skip the hidden distribute column */
            if (pseudoTsDistcol && m == tgt_col_num) continue;

            cstate->cur_attname = NameStr(attr[m]->attname);
            i++;
            atttypmod = (asTypemods != NULL && asTypemods[m].assign) ? asTypemods[m].typemod : attr[m]->atttypmod;
            values[m] =
                CopyReadBinaryAttribute(cstate, i, &in_functions[m], typioparams[m], atttypmod, &nulls[m]);
            cstate->cur_attname = NULL;
        }
    }

    /*
     * Now compute and insert any defaults available for the columns not
     * provided by the input data.	Anything not processed here or above will
     * remain NULL.
     */
    for (i = 0; i < num_defaults; i++) {
        /*
         * The caller must supply econtext and have switched into the
         * per-tuple memory context in it.
         */
        Assert(econtext != NULL);
        Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

        values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext, &nulls[defmap[i]], NULL);
    }

    if (cstate->transexprs != NULL)
        ExecTransColExpr(cstate, econtext, num_phys_attrs, values, nulls);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR) {
        /* Append default values to the data-row in output format. */
        append_defvals(values, cstate);
    }
#endif

    return true;
}

/* execute transform expr on columns, only for copy from */
static void ExecTransColExpr(CopyState cstate, ExprContext* econtext, int numPhysAttrs, Datum* values, bool* nulls)
{
    bool needTransform = false;
    int i;
    
    for (i = 0; i < numPhysAttrs; i++) {
        if (cstate->transexprs[i]) {
            needTransform = true;
            break;
        }
    }

    if (needTransform) {
        TupleTableSlot *slot = MakeSingleTupleTableSlot(cstate->trans_tupledesc);
        HeapTuple tuple = heap_form_tuple(cstate->trans_tupledesc, values, nulls);
        
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);
        econtext->ecxt_scantuple = slot;
        
        for (i = 0; i < numPhysAttrs; i++) {
            if (!cstate->transexprs[i])
                continue;
            
            values[i] = ExecEvalExpr(cstate->transexprs[i], econtext,
                                    &nulls[i], NULL);
        }
        
        ExecDropSingleTupleTableSlot(slot);
    }
}

#ifdef PGXC
/*
 * append_defvals:
 * Append default values in output form onto the data-row.
 * 1. scans the default values with the help of defmap,
 * 2. converts each default value into its output form,
 * 3. then appends it into cstate->defval_buf buffer.
 * This buffer would later be appended into the final data row that is sent to
 * the Datanodes.
 * So for e.g., for a table :
 * tab (id1 int, v varchar, id2 default nextval('tab_id2_seq'::regclass), id3 )
 * with the user-supplied data  : "2 | abcd",
 * and the COPY command such as:
 * copy tab (id1, v) FROM '/tmp/a.txt' (delimiter '|');
 * Here, cstate->defval_buf will be populated with something like : "| 1"
 * and the final data row will be : "2 | abcd | 1"
 */
static void append_defvals(Datum* values, CopyState cstate)
{
    /* fastpath to return if there is no any default value to append */
    if (cstate->num_defaults <= 0) {
        return;
    }
    /* In binary format, the first two bytes indicate the number of columns */
    int binaryColBytesSize = 2;

    CopyStateData new_cstate = *cstate;
    int i;

    new_cstate.fe_msgbuf = makeStringInfo();
    if(IS_BINARY(cstate)) {
        appendBinaryStringInfo(new_cstate.fe_msgbuf, cstate->line_buf.data, binaryColBytesSize);
     }

    for (i = 0; i < cstate->num_defaults; i++) {
        int attindex = cstate->defmap[i];
        Datum defvalue = values[attindex];

        /*
         * For using the values in their output form, it is not sufficient
         * to just call its output function. The format should match
         * that of COPY because after all we are going to send this value as
         * an input data row to the Datanode using COPY FROM syntax. So we call
         * exactly those functions that are used to output the values in case
         * of COPY TO. For instace, CopyAttributeOutText() takes care of
         * escaping, CopySendInt32 take care of byte ordering, etc. All these
         * functions use cstate->fe_msgbuf to copy the data. But this field
         * already has the input data row. So, we need to use a separate
         * temporary cstate for this purpose. All the COPY options remain the
         * same, so new cstate will have all the fields copied from the original
         * cstate, except fe_msgbuf.
         */
        if (IS_BINARY(cstate)) {
            bytea* outputbytes = NULL;

            outputbytes = SendFunctionCall(&cstate->out_functions[attindex], defvalue);
            CopySendInt32(&new_cstate, VARSIZE(outputbytes) - VARHDRSZ);
            CopySendData(&new_cstate, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
        } else {
            char* string = NULL;

            string = OutputFunctionCall(&cstate->out_functions[attindex], defvalue);
            if (IS_CSV(cstate))
                CopyAttributeOutCSV(&new_cstate,
                    string,
                    false /* don't force quote */,
                    false /* there's at least one user-supplied attribute */);
            else
                CopyAttributeOutText(&new_cstate, string);
            CopySendString(&new_cstate, new_cstate.delim);
        }
    }

    if(IS_BINARY(cstate)) {
        appendBinaryStringInfo(new_cstate.fe_msgbuf, cstate->line_buf.data + binaryColBytesSize, new_cstate.line_buf.len - binaryColBytesSize);
    } else {
        appendBinaryStringInfo(new_cstate.fe_msgbuf, cstate->line_buf.data, new_cstate.line_buf.len);
    }
     /* reset */
     resetStringInfo(&cstate->line_buf);
     /* append all to line_buf */
     appendBinaryStringInfo(&cstate->line_buf, new_cstate.fe_msgbuf->data, new_cstate.fe_msgbuf->len);
}
#endif

/*
 * Clean up storage and release resources for COPY FROM.
 */
void EndCopyFrom(CopyState cstate)
{
#ifdef PGXC
    /* For PGXC related COPY, free remote COPY state */
    if (IS_PGXC_COORDINATOR && cstate->remoteCopyState)
        FreeRemoteCopyData(cstate->remoteCopyState);
#endif

    /* No COPY FROM related resources except memory. */
    EndCopy(cstate);
    cstate = NULL;
}

/*
 * Read the next input line and stash it in line_buf, with conversion to
 * server encoding.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.	The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool CopyReadLine(CopyState cstate)
{
    bool result = false;

retry:
    resetStringInfo(&cstate->line_buf);

    /* Mark that encoding conversion hasn't occurred yet */
    cstate->line_buf_converted = false;

    /* Parse data and transfer into line_buf */
    result = cstate->readlineFunc(cstate);

    if (result) {
        /*
         * Reached EOF.  In protocol version 3, we should ignore anything
         * after \. up to the protocol end of copy data.  (XXX maybe better
         * not to treat \. as special?)
         */
        if (cstate->copy_dest == COPY_NEW_FE) {
            do {
                cstate->raw_buf_index = cstate->raw_buf_len;
            } while (CopyLoadRawBuf(cstate));
        }

        if (cstate->mode == MODE_NORMAL) {
            if (cstate->filename && is_obs_protocol(cstate->filename)) {
#ifndef ENABLE_LITE_MODE
                if (getNextOBS(cstate)) {
                    cstate->eol_type = EOL_UNKNOWN;
                    goto retry;
                }
#else
                FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
            } else {
                if (getNextGDS<true>(cstate)) {
                    if (cstate->eol_type != EOL_UD)
                        cstate->eol_type = EOL_UNKNOWN;
                    goto retry;
                }
            }
        }

        if (cstate->mode == MODE_PRIVATE || cstate->mode == MODE_SHARED) {
            if (cstate->line_buf.len != 0) {
                /* reached EOF but not have EOL, process the line_buf before move next file */
                result = false;
            } else if (cstate->getNextCopyFunc(cstate)) {
                if (cstate->eol_type != EOL_UD)
                    cstate->eol_type = EOL_UNKNOWN;
                cstate->cur_lineno++;
                if (cstate->header_line) {
                    /* skip header, this should only in private mode with csv file*/
                    resetStringInfo(&cstate->line_buf);
                    cstate->line_buf_converted = false;
                    cstate->cur_lineno++;
                    (void)CopyReadLineText(cstate);
                }
                goto retry;
            }
        }
    } else {
        /*
         * If we didn't hit EOF, then we must have transferred the EOL marker
         * to line_buf along with the data.  Get rid of it.
         */
        switch (cstate->eol_type) {
            case EOL_NL:
                Assert(cstate->line_buf.len >= 1);
                cstate->line_buf.len--;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_CR:
                Assert(cstate->line_buf.len >= 1);
                Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
                cstate->line_buf.len--;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_CRNL:
                Assert(cstate->line_buf.len >= 2);
                Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
                Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
                cstate->line_buf.len -= 2;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_UD:
                Assert(cstate->line_buf.len >= (int)strlen(cstate->eol));
                Assert(strstr(cstate->line_buf.data, cstate->eol) != NULL);
                cstate->line_buf.len -= strlen(cstate->eol);
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_UNKNOWN:
                /* shouldn't get here */
                Assert(false);
                break;
            default:
                /* shouldn't get here */
                Assert(false);
                break;
        }
    }

    /* Done reading the line.  Convert it to server encoding. */
    if (cstate->need_transcoding) {
        char* cvt = NULL;

        cvt = pg_any_to_server(cstate->line_buf.data, cstate->line_buf.len, cstate->file_encoding);
        if (cvt != cstate->line_buf.data) {
            /* transfer converted data back to line_buf */
            resetStringInfo(&cstate->line_buf);
            appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
            pfree_ext(cvt);
        }
    }

    /* Now it's safe to use the buffer in error messages */
    cstate->line_buf_converted = true;

    return result;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
template <bool csv_mode>
static bool CopyReadLineTextTemplate(CopyState cstate)
{
    char* copy_raw_buf = NULL;
    int raw_buf_ptr;
    int copy_buf_len;
    bool need_data = false;
    bool hit_eof = false;
    bool result = false;
    char mblen_str[2];

    /* CSV variables */
    bool first_char_in_line = true;
    bool in_quote = false;
    bool last_was_esc = false;
    char quotec = '\0';
    char escapec = '\0';

    if (csv_mode) {
        quotec = cstate->quote[0];
        escapec = cstate->escape[0];
        /* ignore special escape processing if it's the same as quotec */
        if (quotec == escapec) {
            escapec = '\0';
        }
    }

    mblen_str[1] = '\0';

    /*
     * The objective of this loop is to transfer the entire next input line
     * into line_buf.  Hence, we only care for detecting newlines (\r and/or
     * \n) and the end-of-copy marker (\.).
     *
     * In CSV mode, \r and \n inside a quoted field are just part of the data
     * value and are put in line_buf.  We keep just enough state to know if we
     * are currently in a quoted field or not.
     *
     * These four characters, and the CSV escape and quote characters, are
     * assumed the same in frontend and backend encodings.
     *
     * For speed, we try to move data from raw_buf to line_buf in chunks
     * rather than one character at a time.  raw_buf_ptr points to the next
     * character to examine; any characters from raw_buf_index to raw_buf_ptr
     * have been determined to be part of the line, but not yet transferred to
     * line_buf.
     *
     * For a little extra speed within the loop, we copy raw_buf and
     * raw_buf_len into local variables.
     */
    copy_raw_buf = cstate->raw_buf;
    raw_buf_ptr = cstate->raw_buf_index;
    copy_buf_len = cstate->raw_buf_len;

    for (;;) {
        int prev_raw_ptr;
        char c;

        /*
         * Load more data if needed.  Ideally we would just force four bytes
         * of read-ahead and avoid the many calls to
         * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but the COPY_OLD_FE protocol
         * does not allow us to read too far ahead or we might read into the
         * next data, so we read-ahead only as far we know we can.	One
         * optimization would be to read-ahead four byte here if
         * cstate->copy_dest != COPY_OLD_FE, but it hardly seems worth it,
         * considering the size of the buffer.
         */
        if (raw_buf_ptr >= copy_buf_len || need_data) {
            REFILL_LINEBUF;

            /*
             * Try to read some more data.	This will certainly reset
             * raw_buf_index to zero, and raw_buf_ptr must go with it.
             */
            if (!CopyLoadRawBuf(cstate))
                hit_eof = true;
            raw_buf_ptr = 0;
            copy_buf_len = cstate->raw_buf_len;

            /*
             * If we are completely out of data, break out of the loop,
             * reporting EOF.
             */
            if (copy_buf_len <= 0) {
                result = true;
                break;
            }
            need_data = false;
        }

        /* OK to fetch a character */
        prev_raw_ptr = raw_buf_ptr;
        c = copy_raw_buf[raw_buf_ptr++];

        if (csv_mode) {
            /*
             * If character is '\\' or '\r', we may need to look ahead below.
             * Force fetch of the next character if we don't already have it.
             * We need to do this before changing CSV state, in case one of
             * these characters is also the quote or escape character.
             *
             * Note: old-protocol does not like forced prefetch, but it's OK
             * here since we cannot validly be at EOF.
             */
            if (c == '\\' || c == '\r') {
                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
            }

            /*
             * Dealing with quotes and escapes here is mildly tricky. If the
             * quote char is also the escape char, there's no problem - we
             * just use the char as a toggle. If they are different, we need
             * to ensure that we only take account of an escape inside a
             * quoted field and immediately preceding a quote char, and not
             * the second in a escape-escape sequence.
             */
            if (in_quote && c == escapec) {
                last_was_esc = !last_was_esc;
            }
            if (c == quotec && !last_was_esc) {
                in_quote = !in_quote;
            }
            if (c != escapec) {
                last_was_esc = false;
            }

            /*
             * Updating the line count for embedded CR and/or LF chars is
             * necessarily a little fragile - this test is probably about the
             * best we can do.	(XXX it's arguable whether we should do this
             * at all --- is cur_lineno a physical or logical count?)
             */
            if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
                cstate->cur_lineno++;
        }

        /* Process \r */
        if (cstate->eol_type != EOL_UD && c == '\r' && (!csv_mode || !in_quote)) {
            /* Check for \r\n on first line, _and_ handle \r\n. */
            if (cstate->eol_type == EOL_UNKNOWN || cstate->eol_type == EOL_CRNL) {
                /*
                 * If need more data, go back to loop top to load it.
                 *
                 * Note that if we are at EOF, c will wind up as '\0' because
                 * of the guaranteed pad of raw_buf.
                 */
                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

                /* get next char */
                c = copy_raw_buf[raw_buf_ptr];

                if (c == '\n') {
                    raw_buf_ptr++;               /* eat newline */
                    cstate->eol_type = EOL_CRNL; /* in case not set yet */
                } else {
                    /* found \r, but no \n */
                    if (cstate->eol_type == EOL_CRNL) {
                        if (cstate->log_errors)
                            cstate->log_errors = false;

                        if (cstate->logErrorsData) {
                            cstate->logErrorsData = false;
                        }
                        ereport(ERROR,
                            (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                                !IS_CSV(cstate) ? errmsg("literal carriage return found in data")
                                                : errmsg("unquoted carriage return found in data"),
                                !IS_CSV(cstate) ? errhint("Use \"\\r\" to represent carriage return.")
                                                : errhint("Use quoted CSV field to represent carriage return.")));
                    }

                    /*
                     * if we got here, it is the first line and we didn't find
                     * \n, so don't consume the peeked character
                     */
                    cstate->eol_type = EOL_CR;
                }
            } else if (cstate->eol_type == EOL_NL) {
                if (cstate->log_errors)
                    cstate->log_errors = false;

                if (cstate->logErrorsData) {
                    cstate->logErrorsData = false;
                }
                ereport(ERROR,
                    (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                        !IS_CSV(cstate) ? errmsg("literal carriage return found in data")
                                        : errmsg("unquoted carriage return found in data"),
                        !IS_CSV(cstate) ? errhint("Use \"\\r\" to represent carriage return.")
                                        : errhint("Use quoted CSV field to represent carriage return.")));
            }
            /* If reach here, we have found the line terminator */
            break;
        }

        /* Process \n */
        if (cstate->eol_type != EOL_UD && c == '\n' && (!csv_mode || !in_quote)) {
            if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL) {
                if (cstate->log_errors)
                    cstate->log_errors = false;

                if (cstate->logErrorsData) {
                    cstate->logErrorsData = false;
                }
                ereport(ERROR,
                    (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                        !IS_CSV(cstate) ? errmsg("literal newline found in data")
                                        : errmsg("unquoted newline found in data"),
                        !IS_CSV(cstate) ? errhint("Use \"\\n\" to represent newline.")
                                        : errhint("Use quoted CSV field to represent newline.")));
            }
            cstate->eol_type = EOL_NL; /* in case not set yet */
            /* If reach here, we have found the line terminator */
            break;
        }

        /* Process user-define EOL string */
        if (cstate->eol_type == EOL_UD && c == cstate->eol[0] && (!csv_mode || !in_quote)) {
            int remainLen = strlen(cstate->eol) - 1;
            int pos = 0;

            /*
             *  If the length of EOL string is above one,
             *  we need to look ahead several characters and check
             *  if these characters equal remaining EOL string.
             */
            if (remainLen > 0) {
                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(remainLen - 1);

                for (; pos < remainLen; pos++) {
                    if (copy_raw_buf[raw_buf_ptr + pos] != cstate->eol[pos + 1])
                        break;
                }
            }

            /* If reach here, we have found the line terminator */
            if (pos == remainLen) {
                raw_buf_ptr += pos;
                break;
            }
        }

        /*
         * In CSV mode, we only recognize \. alone on a line.  This is because
         * \. is a valid CSV data value.
         */
        /*
         *  Only allow checking for "end of copy" ("\.") from frontend on
         *  coodinators to prevent skipping special characters ("\r", "\n",etc.), which
         *  would lead to format disorientation and cause import error when
         *  importing from file.
         */
        if (!cstate->is_useeof && (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && cstate->copy_dest != COPY_FILE) {
            if (c == '\\' && (!csv_mode || first_char_in_line)) {
                char c2;

                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                IF_NEED_REFILL_AND_EOF_BREAK(0);

                /* -----
                 * get next character
                 * Note: we do not change c so if it isn't \., we can fall
                 * through and continue processing for file encoding.
                 * -----
                 */
                c2 = copy_raw_buf[raw_buf_ptr];

                if (c2 == '.') {
                    raw_buf_ptr++; /* consume the '.' */

                    /*
                     * Note: if we loop back for more data here, it does not
                     * matter that the CSV state change checks are re-executed; we
                     * will come back here with no important state changed.
                     */
                    if (cstate->eol_type == EOL_CRNL) {
                        /* Get the next character */
                        IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                        /* if hit_eof, c2 will become '\0' */
                        c2 = copy_raw_buf[raw_buf_ptr++];

                        if (c2 == '\n') {
                            if (!csv_mode) {
                                if (cstate->log_errors)
                                    cstate->log_errors = false;

                                if (cstate->logErrorsData) {
                                    cstate->logErrorsData = false;
                                }
                                ereport(ERROR,
                                    (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                                        errmsg("end-of-copy marker does not match previous newline style")));
                            } else
                                NO_END_OF_COPY_GOTO;
                        } else if (c2 != '\r') {
                            if (!csv_mode) {
                                if (cstate->log_errors)
                                    cstate->log_errors = false;

                                if (cstate->logErrorsData) {
                                    cstate->logErrorsData = false;
                                }
                                ereport(ERROR,
                                    (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("end-of-copy marker corrupt")));
                            } else
                                NO_END_OF_COPY_GOTO;
                        }
                    }

                    /* Get the next character */
                    IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                    /* if hit_eof, c2 will become '\0' */
                    c2 = copy_raw_buf[raw_buf_ptr++];

                    if (c2 != '\r' && c2 != '\n') {
                        if (!csv_mode) {
                            if (cstate->log_errors)
                                cstate->log_errors = false;

                            if (cstate->logErrorsData) {
                                cstate->logErrorsData = false;
                            }
                            ereport(
                                ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("end-of-copy marker corrupt")));
                        } else
                            NO_END_OF_COPY_GOTO;
                    }

                    if ((cstate->eol_type == EOL_NL && c2 != '\n') || (cstate->eol_type == EOL_CRNL && c2 != '\n') ||
                        (cstate->eol_type == EOL_CR && c2 != '\r')) {
                        if (cstate->log_errors)
                            cstate->log_errors = false;

                        if (cstate->logErrorsData) {
                            cstate->logErrorsData = false;
                        }
                        ereport(ERROR,
                            (errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
                                errmsg("end-of-copy marker does not match previous newline style")));
                    }

                    /*
                     * Transfer only the data before the \. into line_buf, then
                     * discard the data and the \. sequence.
                     */
                    if (prev_raw_ptr > cstate->raw_buf_index)
                        appendBinaryStringInfo(&cstate->line_buf,
                            cstate->raw_buf + cstate->raw_buf_index,
                            prev_raw_ptr - cstate->raw_buf_index);
                    cstate->raw_buf_index = raw_buf_ptr;
                    result = true; /* report EOF */
                    break;
                } else if (!csv_mode) {

                    /*
                     * If we are here, it means we found a backslash followed by
                     * something other than a period.  In non-CSV mode, anything
                     * after a backslash is special, so we skip over that second
                     * character too.  If we didn't do that \\. would be
                     * considered an eof-of copy, while in non-CSV mode it is a
                     * literal backslash followed by a period.	In CSV mode,
                     * backslashes are not special, so we want to process the
                     * character after the backslash just like a normal character,
                     * so we don't increment in those cases.
                     */
                    raw_buf_ptr++;
                }
            }
        }
        /*
         * This label is for CSV cases where \. appears at the start of a
         * line, but there is more text after it, meaning it was a data value.
         * We are more strict for \. in CSV mode because \. could be a data
         * value, while in non-CSV mode, \. cannot be a data value.
         */
    not_end_of_copy:

        /*
         * Process all bytes of a multi-byte character as a group.
         *
         * We only support multi-byte sequences where the first byte has the
         * high-bit set, so as an optimization we can avoid this block
         * entirely if it is not set.
         */
        if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c)) {
            int mblen;

            mblen_str[0] = c;
            /* All our encodings only read the first byte to get the length */
            mblen = pg_encoding_mblen(cstate->file_encoding, mblen_str);
            IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(mblen - 1);
            IF_NEED_REFILL_AND_EOF_BREAK(mblen - 1);
            raw_buf_ptr += mblen - 1;
        }
        first_char_in_line = false;
    } /* end of outer loop */

    /*
     * Transfer any still-uncopied data to line_buf.
     */
    REFILL_LINEBUF;

    return result;
}

static bool CopyReadLineText(CopyState cstate)
{
    switch (cstate->fileformat) {
        case FORMAT_CSV:
            return CopyReadLineTextTemplate<true>(cstate);
        case FORMAT_TEXT:
        case FORMAT_FIXED:
            return CopyReadLineTextTemplate<false>(cstate);
        default:
            Assert(false);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid file format")));
    }
    return false;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
int GetDecimalFromHex(char hex)
{
    if (isdigit((unsigned char)hex))
        return hex - '0';
    else
        return tolower((unsigned char)hex) - 'a' + 10;
}

/*
 * @Description: compute the max number of raw fields, and allocate memory for them.
 *    if the first field in file is about OID, add 1 to the max number.
 * @IN/OUT cstate: CopyState object, max_fields and raw_fields will be assigned.
 * @See also:
 */
void cstate_fields_buffer_init(CopyState cstate)
{
    int nfields = list_length(cstate->attnumlist) + (cstate->file_has_oids ? 1 : 0);
    cstate->max_fields = nfields;
    cstate->raw_fields = (char**)palloc(nfields * sizeof(char*));
}

/*
 * @Description: Parse the current line into separate attributes (fields),
 *   performing de-escaping as needed.
 *
 *   The input is in line_buf.  We use attribute_buf to hold the result
 *   strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 *   string, or NULL when the input matches the null marker string.
 *   This array is expanded as necessary.
 *
 *   (Note that the caller cannot check for nulls since the returned
 *   string would be the post-de-escaping equivalent, which may look
 *   the same as some valid data string.)
 *
 *   delim is the column delimiter string (must be just one byte for now).
 *   null_print is the null marker string.  Note that this is compared to
 *   the pre-de-escaped input string.
 *
 * @IN/OUT cstate: CopyState object holding line buffer and attribute buffer.
 * @Return:  The return value is the number of fields actually read.
 * @See also:
 */
template <bool multbyteDelim>
static int CopyReadAttributesTextT(CopyState cstate)
{
    char delimc = cstate->delim[0];
    char* delimiter = cstate->delim;
    int fieldno;
    char* output_ptr = NULL;
    char* cur_ptr = NULL;
    char* line_end_ptr = NULL;
    char* line_begin_ptr = NULL;
    IllegalCharErrInfo* err_info = NULL;
    ListCell* cur = NULL;
    StringInfo sequence_buf_ptr = &cstate->sequence_buf;
    int numattrs = list_length(cstate->attnumlist);
    int proc_col_num = 0;
    int max_filler_index = numattrs + list_length(cstate->filler_col_list);

    /*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
    if (cstate->max_fields <= 0) {
        if (cstate->line_buf.len != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));
        return 0;
    }

    resetStringInfo(&cstate->attribute_buf);
    resetStringInfo(sequence_buf_ptr);
    memset_s(cstate->raw_fields, sizeof(char*) * cstate->max_fields, 0, sizeof(char*) * cstate->max_fields);

    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.	We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
        enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
    output_ptr = cstate->attribute_buf.data;

    /* set pointer variables for loop */
    cur_ptr = cstate->line_buf.data;
    /*
     * reserve the line_buf ptr;
     */
    line_begin_ptr = cstate->line_buf.data;
    line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

    /* Outer loop iterates over fields */
    fieldno = 0;
    for (;;) {
        bool found_delim = false;
        char* start_ptr = NULL;
        char* end_ptr = NULL;
        int input_len;
        bool saw_non_ascii = false;
        proc_col_num++;

        /* Make sure there is enough space for the next value */
        if (fieldno >= cstate->max_fields) {
            cstate->max_fields *= 2;
            cstate->raw_fields = (char**)repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char*));
        }

        if (cstate->filler == NULL || proc_col_num > max_filler_index || cstate->filler[proc_col_num - 1] == NULL)
        {
            if (cstate->sequence != NULL && fieldno < numattrs && cstate->sequence[fieldno] != NULL) {
                cstate->raw_fields[fieldno] = &sequence_buf_ptr->data[sequence_buf_ptr->len];
                appendStringInfo(sequence_buf_ptr, "%ld", cstate->sequence[fieldno]->start);
                cstate->sequence[fieldno]->start += cstate->sequence[fieldno]->step;
                cstate->sequence_buf.len++;
                fieldno++;
                continue;
            } else if (cstate->constant != NULL && fieldno < numattrs && cstate->constant[fieldno] != NULL) {
                cstate->raw_fields[fieldno] = cstate->constant[fieldno]->consVal;
                fieldno++;
                continue;
            }
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;
        cstate->raw_fields[fieldno] = output_ptr;

        /*
         * Scan data for field.
         *
         * Note that in this loop, we are scanning to locate the end of field
         * and also speculatively performing de-escaping.  Once we find the
         * end-of-field, we can match the raw field contents against the null
         * marker string.  Only after that comparison fails do we know that
         * de-escaping is actually the right thing to do; therefore we *must
         * not* throw any syntax errors before we've done the null-marker
         * check.
         */
        for (;;) {
            char c;

            end_ptr = cur_ptr;
            if (cur_ptr >= line_end_ptr) {
                break;
            }
            c = *cur_ptr++;
            if (c == delimc && (!multbyteDelim || strncmp(delimiter, cur_ptr - 1, cstate->delim_len) == 0)) {
                if (multbyteDelim)
                    cur_ptr += (cstate->delim_len - 1);

                found_delim = true;
                break;
            }

            if (c == '\\' && !cstate->without_escaping) {
                if (cur_ptr >= line_end_ptr) {
                    break;
                }
                c = *cur_ptr++;
                switch (c) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7': {
                        /* handle \013 */
                        int val;

                        val = OCTVALUE(c);
                        if (cur_ptr < line_end_ptr) {
                            c = *cur_ptr;
                            if (ISOCTAL(c)) {
                                cur_ptr++;
                                val = (val << 3) + OCTVALUE(c);
                                if (cur_ptr < line_end_ptr) {
                                    c = *cur_ptr;
                                    if (ISOCTAL(c)) {
                                        cur_ptr++;
                                        val = (val << 3) + OCTVALUE(c);
                                    }
                                }
                            }
                        }
                        c = val & 0377;
                        if (c == '\0' || IS_HIGHBIT_SET(c))
                            saw_non_ascii = true;
                    } break;
                    case 'x':
                        /* Handle \x3F */
                        if (cur_ptr < line_end_ptr) {
                            char hexchar = *cur_ptr;

                            if (isxdigit((unsigned char)hexchar)) {
                                int val = GetDecimalFromHex(hexchar);

                                cur_ptr++;
                                if (cur_ptr < line_end_ptr) {
                                    hexchar = *cur_ptr;
                                    if (isxdigit((unsigned char)hexchar)) {
                                        cur_ptr++;
                                        val = (val << 4) + GetDecimalFromHex(hexchar);
                                    }
                                }
                                c = val & 0xff;
                                if (c == '\0' || IS_HIGHBIT_SET(c))
                                    saw_non_ascii = true;
                            }
                        }
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'v':
                        c = '\v';
                        break;

                        /*
                         * in all other cases, take the char after '\'
                         * literally
                         */
                    default:
                        break;
                }
            }

            /*
             * Important!!!
             * When we changed GBK as a database encoding, we haven't really thought through every
             * aspect of the code we need to change. Therefore we could end up with all kinds of
             * problems with GBK, either as a result of GBK not being a valid encoding or missing
             * compatibility updates. This issue needs further attention.
             *
             * Here we have one that does not correctly identifies delimiter because of the nature
             * of GBK encoding. It is fixed by skipping the second char when we encounter a GBK 2-byte.
             */
            if ((PG_GBK == GetDatabaseEncoding()) && IS_HIGHBIT_SET(c)) {
                /*
                 * We don't do encoding validation check here because we already went through
                 * the test in pg_any_to_server
                 */
                char sec;
                if (cur_ptr >= line_end_ptr) {
                    /*
                     * Unlikely, yet we deal with it anyway.
                     */
                    *output_ptr++ = c;
                    break;
                }
                sec = *cur_ptr++;
                *output_ptr++ = c;
                *output_ptr++ = sec;
                /*
                 * We don't mark saw_non_ascii because output here is not from de-escaping.
                 */
            } else {
                /* Add c to output string */
                *output_ptr++ = c;
            }
        }

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (input_len == cstate->null_print_len && strncmp(start_ptr, cstate->null_print, input_len) == 0)
            cstate->raw_fields[fieldno] = NULL;
        else {
            /*
             * At this point we know the field is supposed to contain data.
             *
             * If we de-escaped any non-7-bit-ASCII chars, make sure the
             * resulting string is valid data for the db encoding.
             */
            if (saw_non_ascii) {
                char* fld = cstate->raw_fields[fieldno];

                pg_verifymbstr(fld, output_ptr - fld, false);
            }
        }

        /* Terminate attribute value in output area */
        *output_ptr++ = '\0';

        /*
         * match each bulkload illegal error to each field.
         */
        if (cstate->illegal_chars_error) {
            foreach (cur, cstate->illegal_chars_error) {
                err_info = (IllegalCharErrInfo*)lfirst(cur);

                if (err_info->err_offset > 0) {
                    /*
                     * bulkload illegal error is matched to this field.
                     * for text format the each file has the value range[start_ptr - line_begin_ptr, end_ptr -
                     * line_begin_ptr]
                     */
                    if (err_info->err_offset <= (end_ptr - line_begin_ptr)) {
                        /*
                         * indicate the matched field;
                         */
                        err_info->err_field_no = fieldno;
                        /*
                         * if has been matched set err_offset to -1 to avoid duplicated check.
                         */
                        err_info->err_offset = -1;
                    }
                }
            }
        }

        if (cstate->filler == NULL || proc_col_num > max_filler_index || cstate->filler[proc_col_num - 1] == NULL) {
            fieldno++;
        }

        /* Done if we hit EOL instead of a delim */
        if (!found_delim) {
            break;
        }
    }

    for (int i = fieldno; i < numattrs; i++) {
        if (cstate->sequence != NULL && cstate->sequence[i] != NULL) {
            cstate->raw_fields[i] = &sequence_buf_ptr->data[sequence_buf_ptr->len];
            appendStringInfo(sequence_buf_ptr, "%ld", cstate->sequence[i]->start);
            cstate->sequence[i]->start += cstate->sequence[i]->step;
            cstate->sequence_buf.len++;
            fieldno = i + 1;
        } else if (cstate->constant != NULL && cstate->constant[i] != NULL) {
            cstate->raw_fields[i] = cstate->constant[i]->consVal;
            fieldno = i + 1;
        }
    }

    /* Clean up state of attribute_buf */
    output_ptr--;
    Assert(*output_ptr == '\0');
    cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

    return fieldno;
}

/*
 * @Description: Parse the current line into separate attributes (fields),
 *    performing de-escaping as needed.  This has exactly the same API as
 *    CopyReadAttributesTextT, except we parse the fields according to
 *    "standard" (i.e. common) CSV usage.
 * @IN/OUT cstate: CopyState object holding line buffer and attribute buffer.
 * @Return: the number of fields actually read.
 * @See also:
 */
template <bool multbyteDelim>
static int CopyReadAttributesCSVT(CopyState cstate)
{
    char delimc = cstate->delim[0];
    char* delimiter = cstate->delim;
    char quotec = cstate->quote[0];
    char escapec = cstate->escape[0];
    int fieldno;
    char* output_ptr = NULL;
    char* cur_ptr = NULL;
    char* line_end_ptr = NULL;
    char* line_begin_ptr = NULL;
    IllegalCharErrInfo* err_info = NULL;
    ListCell* cur = NULL;
    StringInfo sequence_buf_ptr = &cstate->sequence_buf;
    int numattrs = list_length(cstate->attnumlist);
    int proc_col_num = 0;
    int max_filler_index = numattrs + list_length(cstate->filler_col_list);

    /*
     * We need a special case for zero-column tables: check that the input
     * line is empty, and return.
     */
    if (cstate->max_fields <= 0) {
        if (cstate->line_buf.len != 0)
            ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("extra data after last expected column")));
        return 0;
    }

    resetStringInfo(&cstate->attribute_buf);
    resetStringInfo(sequence_buf_ptr);
    memset_s(cstate->raw_fields, sizeof(char*) * cstate->max_fields, 0, sizeof(char*) * cstate->max_fields);

    /*
     * The de-escaped attributes will certainly not be longer than the input
     * data line, so we can just force attribute_buf to be large enough and
     * then transfer data without any checks for enough space.	We need to do
     * it this way because enlarging attribute_buf mid-stream would invalidate
     * pointers already stored into cstate->raw_fields[].
     */
    if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
        enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
    output_ptr = cstate->attribute_buf.data;

    /* set pointer variables for loop */
    cur_ptr = cstate->line_buf.data;
    /*
     * reserve the line_buf ptr;
     */
    line_begin_ptr = cstate->line_buf.data;
    line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

    /* Outer loop iterates over fields */
    fieldno = 0;
    for (;;) {
        bool found_delim = false;
        bool saw_quote = false;
        char* start_ptr = NULL;
        char* end_ptr = NULL;
        int input_len;
        proc_col_num++;

        /* Make sure there is enough space for the next value */
        if (fieldno >= cstate->max_fields) {
            int max_field = 2 * cstate->max_fields;
            if ((max_field / 2) != cstate->max_fields) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("conn cursor overflow")));
            }
            cstate->max_fields *= 2;
            cstate->raw_fields = (char**)repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char*));
        }

        if (cstate->filler == NULL || proc_col_num > max_filler_index || cstate->filler[proc_col_num - 1] == NULL)
        {
            if (cstate->sequence != NULL && fieldno < numattrs && cstate->sequence[fieldno] != NULL) {
                cstate->raw_fields[fieldno] = &sequence_buf_ptr->data[sequence_buf_ptr->len];
                appendStringInfo(sequence_buf_ptr, "%ld", cstate->sequence[fieldno]->start);
                cstate->sequence[fieldno]->start += cstate->sequence[fieldno]->step;
                cstate->sequence_buf.len++;
                fieldno++;
                continue;
            } else if (cstate->constant != NULL && fieldno < numattrs && cstate->constant[fieldno] != NULL) {
                cstate->raw_fields[fieldno] = cstate->constant[fieldno]->consVal;
                fieldno++;
                continue;
            }
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;
        cstate->raw_fields[fieldno] = output_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        for (;;) {
            char c;

            /* Not in quote */
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) {
                    goto endfield;
                }
                c = *cur_ptr++;
                /* unquoted field delimiter */
                if (c == delimc && (!multbyteDelim || strncmp(cur_ptr - 1, delimiter, cstate->delim_len) == 0)) {
                    if (multbyteDelim)
                        cur_ptr += (cstate->delim_len - 1);

                    found_delim = true;
                    goto endfield;
                }
                /* ""ab"",""b"" error for gs_load */
                if (cstate->is_load_copy && saw_quote) {
                    ereport(ERROR,
                            (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("no terminator found after quoted field")));
                }
                /* start of quoted field (or part of field) */
                if (c == quotec) {
                    saw_quote = true;
                    break;
                }
                /* Add c to output string */
                *output_ptr++ = c;
            }

            /* In quote */
            for (;;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr)
                    ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unterminated CSV quoted field")));

                c = *cur_ptr++;

                /* escape within a quoted field */
                if (c == escapec) {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if (cur_ptr < line_end_ptr) {
                        char nextc = *cur_ptr;

                        if (nextc == escapec || nextc == quotec) {
                            *output_ptr++ = nextc;
                            cur_ptr++;
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if (c == quotec) {
                    break;
                }

                /* Add c to output string */
                *output_ptr++ = c;
            }
        }
    endfield:

        /* Terminate attribute value in output area */
        *output_ptr++ = '\0';

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (!saw_quote && input_len == cstate->null_print_len && strncmp(start_ptr, cstate->null_print, input_len) == 0)
            cstate->raw_fields[fieldno] = NULL;
        /*
         * match each bulkload illegal error to each field.
         */
        if (cstate->illegal_chars_error) {
            foreach (cur, cstate->illegal_chars_error) {
                err_info = (IllegalCharErrInfo*)lfirst(cur);

                if (err_info->err_offset > 0) {
                    /*
                     * bulkload illegal error is matched to this field.
                     * for csv format each field has the value range[start_ptr - line_begin_ptr, end_ptr -
                     * line_begin_ptr]
                     */
                    if (err_info->err_offset <= (end_ptr - line_begin_ptr)) {
                        /*
                         * indicate the matched field;
                         */
                        err_info->err_field_no = fieldno;
                        /*
                         * if has been matched set err_offset to -1 to avoid duplicated check.
                         */
                        err_info->err_offset = -1;
                    }
                }
            }
        }

        if (cstate->filler == NULL || proc_col_num > max_filler_index || cstate->filler[proc_col_num - 1] == NULL) {
            fieldno++;
        }

        /* Done if we hit EOL instead of a delim */
        if (!found_delim) {
            break;
        }
    }

    for (int i = fieldno; i < numattrs; i++) {
        if (cstate->sequence != NULL && cstate->sequence[i] != NULL) {
            cstate->raw_fields[i] = &sequence_buf_ptr->data[sequence_buf_ptr->len];
            appendStringInfo(sequence_buf_ptr, "%ld", cstate->sequence[i]->start);
            cstate->sequence[i]->start += cstate->sequence[i]->step;
            cstate->sequence_buf.len++;
            fieldno = i + 1;
        } else if (cstate->constant != NULL && cstate->constant[i] != NULL) {
            cstate->raw_fields[i] = cstate->constant[i]->consVal;
            fieldno = i + 1;
        }
    }

    /* Clean up state of attribute_buf */
    output_ptr--;
    Assert(*output_ptr == '\0');
    cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

    return fieldno;
}

/*
 * Read a binary attribute
 */
static Datum CopyReadBinaryAttribute(
    CopyState cstate, int column_no, FmgrInfo* flinfo, Oid typioparam, int32 typmod, bool* isnull)
{
    int32 fld_size;
    int32 nSize;
    Datum result;

    if (!CopyGetInt32(cstate, &fld_size))
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unexpected EOF in COPY data")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR) {
        /* Add field size to the data row, unless it is invalid. */
        if (fld_size >= -1) {
            /* -1 is valid; it means NULL value */
            nSize = htonl(fld_size);
            appendBinaryStringInfo(&cstate->line_buf, (char*)&nSize, sizeof(int32));
        }
    }
#endif

    if (fld_size == -1) {
        *isnull = true;
        return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
    }
    if (fld_size < 0)
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("invalid field size")));

    /* reset attribute_buf to empty, and load raw data in it */
    resetStringInfo(&cstate->attribute_buf);

    enlargeStringInfo(&cstate->attribute_buf, fld_size);
    if (CopyGetData(cstate, cstate->attribute_buf.data, fld_size, fld_size) != fld_size)
        ereport(ERROR, (errcode(ERRCODE_BAD_COPY_FILE_FORMAT), errmsg("unexpected EOF in COPY data")));

    cstate->attribute_buf.len = fld_size;
    cstate->attribute_buf.data[fld_size] = '\0';
#ifdef PGXC
    if (IS_PGXC_COORDINATOR) {
        /* add the binary attribute value to the data row */
        appendBinaryStringInfo(&cstate->line_buf, cstate->attribute_buf.data, fld_size);
    }
#endif

    /* Call the column type's binary input converter */
    result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf, typioparam, typmod);

    /* Trouble if it didn't eat the whole buffer */
    if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
        ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("incorrect binary data format")));

    *isnull = false;
    return result;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR()                                   \
    do {                                              \
        if (ptr > start)                              \
            CopySendData(cstate, start, ptr - start); \
    } while (0)

template <bool multbyteDelim>
static void CopyAttributeOutTextT(CopyState cstate, char* string)
{
    char* ptr = NULL;
    char* start = NULL;
    char c;
    char delimc = cstate->delim[0];
    char* delimiter = cstate->delim;
    int delimLen = cstate->delim_len;

    if (cstate->need_transcoding)
        ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
    else
        ptr = string;

    /*
     * We have to grovel through the string searching for control characters
     * and instances of the delimiter character.  In most cases, though, these
     * are infrequent.	To avoid overhead from calling CopySendData once per
     * character, we dump out all characters between escaped characters in a
     * single call.  The loop invariant is that the data from "start" to "ptr"
     * can be sent literally, but hasn't yet been.
     *
     * We can skip pg_encoding_mblen() overhead when encoding is safe, because
     * in valid backend encodings, extra bytes of a multibyte character never
     * look like ASCII.  This loop is sufficiently performance-critical that
     * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
     * of the normal safe-encoding path.
     */
    if (cstate->encoding_embeds_ascii) {
        start = ptr;
        while ((c = *ptr) != '\0') {
            if ((unsigned char)c < (unsigned char)0x20 && !cstate->without_escaping) {
                /*
                 * \r and \n must be escaped, the others are traditional. We
                 * prefer to dump these using the C-like notation, rather than
                 * a backslash and the literal character, because it makes the
                 * dump file a bit more proof against Microsoftish data
                 * mangling.
                 */
                switch (c) {
                    case '\b':
                        c = 'b';
                        break;
                    case '\f':
                        c = 'f';
                        break;
                    case '\n':
                        c = 'n';
                        break;
                    case '\r':
                        c = 'r';
                        break;
                    case '\t':
                        c = 't';
                        break;
                    case '\v':
                        c = 'v';
                        break;
                    default:
                        /* If it's the delimiter, must backslash it */
                        if (c == delimc && (!multbyteDelim || strncmp(ptr, delimiter, delimLen) == 0))
                            break;
                        /* All ASCII control chars are length 1 */
                        ptr++;
                        continue; /* fall to end of loop */
                }
                /* if we get here, we need to convert the control char */
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                CopySendChar(cstate, c);
                start = ++ptr; /* do not include char in next run */
            } else if (c == '\\') {
                DUMPSOFAR();
                /*
                 * for exporting if no escaping flag is true no extra '\' added
                 */
                if (!cstate->without_escaping) {
                    CopySendChar(cstate, '\\');
                }
                start = ptr++; /* we include char in next run */
            } else if (c == delimc && (!multbyteDelim || strncmp(ptr, delimiter, delimLen) == 0)) {
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                start = ptr++; /* we include char in next run */
            } else if (IS_HIGHBIT_SET(c))
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            else
                ptr++;
        }
    } else {
        start = ptr;
        while ((c = *ptr) != '\0') {
            if ((unsigned char)c < (unsigned char)0x20 && !cstate->without_escaping) {
                /*
                 * \r and \n must be escaped, the others are traditional. We
                 * prefer to dump these using the C-like notation, rather than
                 * a backslash and the literal character, because it makes the
                 * dump file a bit more proof against Microsoftish data
                 * mangling.
                 */
                switch (c) {
                    case '\b':
                        c = 'b';
                        break;
                    case '\f':
                        c = 'f';
                        break;
                    case '\n':
                        c = 'n';
                        break;
                    case '\r':
                        c = 'r';
                        break;
                    case '\t':
                        c = 't';
                        break;
                    case '\v':
                        c = 'v';
                        break;
                    default:
                        /* If it's the delimiter, must backslash it */
                        if (c == delimc && (!multbyteDelim || strncmp(ptr, delimiter, delimLen) == 0))
                            break;
                        /* All ASCII control chars are length 1 */
                        ptr++;
                        continue; /* fall to end of loop */
                }
                /* if we get here, we need to convert the control char */
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                CopySendChar(cstate, c);
                start = ++ptr; /* do not include char in next run */
            } else if (c == '\\') {
                DUMPSOFAR();
                /*
                 * for exporting if no escaping flag is true no extra '\' added
                 */
                if (!cstate->without_escaping) {
                    CopySendChar(cstate, '\\');
                }
                start = ptr++; /* we include char in next run */
            } else if (c == delimc && (!multbyteDelim || strncmp(ptr, delimiter, delimLen) == 0)) {
                DUMPSOFAR();
                CopySendChar(cstate, '\\');
                start = ptr++; /* we include char in next run */
            } else if (IS_HIGHBIT_SET(c) && (PG_GBK == cstate->file_encoding))
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            else
                ptr++;
        }
    }

    DUMPSOFAR();
}

static void CopyAttributeOutText(CopyState cstate, char* string)
{
    switch (cstate->delim_len) {
        case 1:
            CopyAttributeOutTextT<false>(cstate, string);
            break;
        default:
            CopyAttributeOutTextT<true>(cstate, string);
    }
}

template <bool multbyteDelim>
static void CopyAttributeOutCSVT(CopyState cstate, char* string, bool use_quote, bool single_attr)
{
    char* ptr = NULL;
    char* start = NULL;
    char c;
    char delimc = cstate->delim[0];
    char* delimiter = cstate->delim;
    char quotec = cstate->quote[0];
    char escapec = cstate->escape[0];

    /* force quoting if it matches null_print (before conversion!) */
    if (!use_quote && strcmp(string, cstate->null_print) == 0)
        use_quote = true;

    if (cstate->need_transcoding)
        ptr = pg_server_to_any(string, strlen(string), cstate->file_encoding);
    else
        ptr = string;

    /*
     * Make a preliminary pass to discover if it needs quoting
     */
    if (!use_quote) {
        /*
         * Because '\.' can be a data value, quote it if it appears alone on a
         * line so it is not interpreted as the end-of-data marker.
         */
        if (single_attr && strcmp(ptr, "\\.") == 0)
            use_quote = true;
        else {
            char* tptr = ptr;

            while ((c = *tptr) != '\0') {
                if ((c == delimc && (!multbyteDelim || strncmp(tptr, delimiter, cstate->delim_len) == 0)) ||
                    c == quotec || c == '\n' || c == '\r') {
                    use_quote = true;
                    break;
                }
                if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
                    tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
                else
                    tptr++;
            }
        }
    }

    if (use_quote) {
        CopySendChar(cstate, quotec);

        /*
         * We adopt the same optimization strategy as in CopyAttributeOutText
         */
        start = ptr;
        while ((c = *ptr) != '\0') {
            if (c == quotec || c == escapec) {
                DUMPSOFAR();
                CopySendChar(cstate, escapec);
                start = ptr; /* we include char in next run */
            }
            if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            else
                ptr++;
        }
        DUMPSOFAR();

        CopySendChar(cstate, quotec);
    } else {
        /* If it doesn't need quoting, we can just dump it as-is */
        CopySendString(cstate, ptr);
    }
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void CopyAttributeOutCSV(CopyState cstate, char* string, bool use_quote, bool single_attr)
{
    switch (cstate->delim_len) {
        case 1:
            CopyAttributeOutCSVT<false>(cstate, string, use_quote, single_attr);
            break;
        default:
            CopyAttributeOutCSVT<true>(cstate, string, use_quote, single_attr);
    }
}

/*
 * CopyGetAllAttnums - build an integer list of attnums in a relationi
 *
 * Similar to CopyGetAllattnums. The purpose of this function is to get
 * all column defs to be evaluated for RemoteCopy_GetRelationLoc so that
 * we don't miss any default columns.
 */
List* CopyGetAllAttnums(TupleDesc tupDesc, Relation rel)
{
    List* attnums = NIL;
    Form_pg_attribute* attr = tupDesc->attrs;
    int attr_count = tupDesc->natts;
    int i;

    for (i = 0; i < attr_count; i++) {
        if (attr[i]->attisdropped)
            continue;
        attnums = lappend_int(attnums, i + 1);
    }

    return attnums;
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
List* CopyGetAttnums(TupleDesc tupDesc, Relation rel, List* attnamelist)
{
    List* attnums = NIL;

    if (attnamelist == NIL) {
        /* Generate default column list */
        Form_pg_attribute *attr = tupDesc->attrs;
        int attr_count = tupDesc->natts;
        int i;

        for (i = 0; i < attr_count; i++) {
            if (attr[i]->attisdropped)
                continue;
            if (ISGENERATEDCOL(tupDesc, i))
                continue;
            attnums = lappend_int(attnums, i + 1);
        }
    } else {
        /* Validate the user-supplied list and extract attnums */
        ListCell* l = NULL;

        foreach (l, attnamelist) {
            char* name = strVal(lfirst(l));
            int attnum;
            int i;

            /* Lookup column name */
            attnum = InvalidAttrNumber;
            for (i = 0; i < tupDesc->natts; i++) {
                if (tupDesc->attrs[i]->attisdropped)
                    continue;
                if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0) {
                    if (ISGENERATEDCOL(tupDesc, i)) {
                        ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            errmsg("column \"%s\" is a generated column", name),
                            errdetail("Generated columns cannot be used in COPY.")));
                    }
                    attnum = tupDesc->attrs[i]->attnum;
                    break;
                }
            }
            if (attnum == InvalidAttrNumber) {
                if (rel != NULL)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_COLUMN),
                            errmsg("column \"%s\" of relation \"%s\" does not exist",
                                name,
                                RelationGetRelationName(rel))));
                else
                    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", name)));
            }
            /* Check for duplicates */
            if (list_member_int(attnums, attnum))
                ereport(
                    ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN), errmsg("column \"%s\" specified more than once", name)));
            attnums = lappend_int(attnums, attnum);
        }
    }

    return attnums;
}

void SetFixedAlignment(TupleDesc tupDesc, Relation rel, FixFormatter* formatter, const char* char_alignment)
{
    for (int i = 0; i < formatter->nfield; i++) {
        Form_pg_attribute attr = NULL;
        char* name = formatter->fieldDesc[i].fieldname;

        // Validate the user-supplied list
        //
        for (int j = 0; j < tupDesc->natts; j++) {
            if (tupDesc->attrs[j]->attisdropped)
                continue;
            if (namestrcmp(&(tupDesc->attrs[j]->attname), name) == 0) {
                attr = tupDesc->attrs[j];
                break;
            }
        }

        if (!attr) {
            if (rel != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist", name, RelationGetRelationName(rel))));
            else
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("column \"%s\" does not exist", name)));
        }

        Assert(formatter != NULL);
        formatter->fieldDesc[i].attnum = attr->attnum;

        switch (attr->atttypid) {
            case BYTEAOID:
            case CHAROID:
            case NAMEOID:
            case TEXTOID:
            case BPCHAROID:
            case VARCHAROID:
            case NVARCHAR2OID:
            case CSTRINGOID: {
                if ((char_alignment == NULL) || (strcmp(char_alignment, "align_right") == 0))
                    formatter->fieldDesc[i].align = ALIGN_RIGHT;
                else if (strcmp(char_alignment, "align_left") == 0)
                    formatter->fieldDesc[i].align = ALIGN_LEFT;
                else
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg(
                                "Only \"align_left\" and \"align_right\" is allowed in out_filename_prefix option")));

                break;
            }
            default:
                formatter->fieldDesc[i].align = ALIGN_LEFT;
                break;
        }
    }
}

/*
 * copy_dest_startup --- executor startup
 */
static void copy_dest_startup(DestReceiver* self, int operation, TupleDesc typeinfo)
{
    /* no-op */
}

/*
 * copy_dest_receive --- receive one tuple
 */
static void copy_dest_receive(TupleTableSlot* slot, DestReceiver* self)
{
    DR_copy* myState = (DR_copy*)self;
    CopyState cstate = myState->cstate;

    /* Make sure the tuple is fully deconstructed */
    tableam_tslot_getallattrs(slot);

    /* And send the data */
    CopyOneRowTo(cstate, InvalidOid, slot->tts_values, slot->tts_isnull);
    myState->processed++;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void copy_dest_shutdown(DestReceiver* self)
{
    /* no-op */
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void copy_dest_destroy(DestReceiver* self)
{
    pfree_ext(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver* CreateCopyDestReceiver(void)
{
    DR_copy* self = (DR_copy*)palloc0(sizeof(DR_copy));

    self->pub.receiveSlot = copy_dest_receive;
    self->pub.rStartup = copy_dest_startup;
    self->pub.rShutdown = copy_dest_shutdown;
    self->pub.rDestroy = copy_dest_destroy;
    self->pub.sendBatch = NULL;
    self->pub.finalizeLocalStream = NULL;
    self->pub.mydest = DestCopyOut;

    self->cstate = NULL; /* will be set later */
    self->processed = 0;

    return (DestReceiver*)self;
}

#ifdef PGXC
static RemoteCopyOptions* GetRemoteCopyOptions(CopyState cstate)
{
    RemoteCopyOptions* res = makeRemoteCopyOptions();
    Assert(cstate);

    /* Then fill in structure */
    res->rco_oids = cstate->oids;
    res->rco_freeze = cstate->freeze;
    res->rco_ignore_extra_data = cstate->ignore_extra_data;
    res->rco_formatter = cstate->formatter;
    res->rco_format = cstate->fileformat;
    res->rco_without_escaping = cstate->without_escaping;
    res->rco_eol_type = cstate->eol_type;
    res->rco_compatible_illegal_chars = cstate->compatible_illegal_chars;
    res->rco_fill_missing_fields = cstate->fill_missing_fields;
    res->transform_query_string = NULL;

    if (cstate->delim)
        res->rco_delim = pstrdup(cstate->delim);
    if (cstate->null_print)
        res->rco_null_print = pstrdup(cstate->null_print);
    if (cstate->quote)
        res->rco_quote = pstrdup(cstate->quote);
    if (cstate->escape)
        res->rco_escape = pstrdup(cstate->escape);
    if (cstate->eol)
        res->rco_eol = pstrdup(cstate->eol);
    if (cstate->force_quote)
        res->rco_force_quote = list_copy(cstate->force_quote);
    if (cstate->force_notnull)
        res->rco_force_notnull = list_copy(cstate->force_notnull);
    if (cstate->date_format.str)
        res->rco_date_format = pstrdup(cstate->date_format.str);
    if (cstate->time_format.str)
        res->rco_time_format = pstrdup(cstate->time_format.str);
    if (cstate->timestamp_format.str)
        res->rco_timestamp_format = pstrdup(cstate->timestamp_format.str);
    if (cstate->smalldatetime_format.str)
        res->rco_smalldatetime_format = pstrdup(cstate->smalldatetime_format.str);
    if (cstate->transform_query_string)
        res->transform_query_string = cstate->transform_query_string;

    return res;
}

/* Convenience wrapper around DataNodeCopyBegin() */
static void pgxc_datanode_copybegin(RemoteCopyData* remoteCopyState, AdaptMem* memInfo)
{
    char* queryString = remoteCopyState->query_buf.data;
    if (memInfo != NULL) {
        queryString = ConstructMesageWithMemInfo(queryString, *memInfo);
    }
    remoteCopyState->connections =
        DataNodeCopyBegin(queryString, remoteCopyState->exec_nodes->nodeList, GetActiveSnapshot());

    if (!remoteCopyState->connections) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to initialize Datanodes for COPY")));
    }
}

#endif

bool is_valid_location(const char* location)
{
    const char* l = location;

    if (location == NULL)
        return false;

    while (isspace(*l))
        l++;

    // check URL prefix
    if (strncmp(l, GSFS_PREFIX, GSFS_PREFIX_LEN) != 0 && strncmp(l, GSFSS_PREFIX, GSFSS_PREFIX_LEN) != 0  &&
        strncmp(l, GSOBS_PREFIX, GSOBS_PREFIX_LEN) != 0 && strncmp(l, LOCAL_PREFIX, LOCAL_PREFIX_LEN) != 0 &&
        strncmp(l, ROACH_PREFIX, ROACH_PREFIX_LEN) != 0 && strncmp(l, OBS_PREFIX, OBS_PREfIX_LEN) != 0 && 
        l[0] != '/' && l[0] != '.')
        return false;

    if (strncmp(l, "//", strlen("//")) == 0)
        return false;

    if (strstr(l, GSFS_PREFIX) != NULL)
        l += GSFS_PREFIX_LEN;
    else if (strstr(l, GSFSS_PREFIX) != NULL)
        l += GSFSS_PREFIX_LEN;
    else if (strstr(l, GSOBS_PREFIX) != NULL)
        l += GSOBS_PREFIX_LEN;
    else if (strstr(l, OBS_PREFIX) != NULL)
        l += OBS_PREfIX_LEN;
    else if (strstr(l, LOCAL_PREFIX) != NULL)
        l += LOCAL_PREFIX_LEN;
    else if (strstr(l, ROACH_PREFIX) != NULL)
        l += ROACH_PREFIX_LEN;

    if (strlen(l) == 0)
        return false;

    return true;
}

bool is_local_location(const char* location)
{
    return strncmp(location, LOCAL_PREFIX, LOCAL_PREFIX_LEN) == 0 || location[0] == '/' || location[0] == '.';
}

bool is_roach_location(const char* location)
{
    return strncmp(location, ROACH_PREFIX, ROACH_PREFIX_LEN) == 0;
}

char* scan_dir(DIR* dir, const char* dirpath, const char* pattern, long* filesize)
{
    struct dirent* dent = NULL;
    char* filename = NULL;
    char tmppath[PATH_MAX + 1];
    int len;
    char* p = NULL;
    char* q = NULL;
    bool isslash = false;
    errno_t rc = EOK;

    Assert(dirpath != NULL && pattern != NULL);

    while ((dent = ReadDir(dir, dirpath)) != NULL) {
        /* regular file, symbolic link, type unknown */
        if (dent->d_type != DT_REG && dent->d_type != DT_LNK && dent->d_type != DT_UNKNOWN) {
            continue;
        }

        rc = snprintf_s(tmppath, sizeof(tmppath), PATH_MAX, "%s/%s", dirpath, dent->d_name);
        securec_check_ss(rc, "\0", "\0");
        tmppath[PATH_MAX] = '\0';

        /* get file status */
        struct stat st;
        if (lstat(tmppath, &st) < 0) {
            continue;
        }

        /* regular file, symbolic link, file size>0 */
        if ((!S_ISREG(st.st_mode) && !S_ISLNK(st.st_mode)) || st.st_size == 0) {
            continue;
        }

        /* match file name */
        if (fnmatch(pattern, dent->d_name, 0) == FNM_NOMATCH) {
            continue;
        }

        /* symbolic link file */
        if (S_ISLNK(st.st_mode)) {
            /* get target file */
            char linkpath[PATH_MAX + 1];
            int rllen = readlink(tmppath, linkpath, sizeof(linkpath));
            if (rllen < 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read symbolic link \"%s\": %m", tmppath)));
            }
            if (rllen >= (int)sizeof(linkpath)) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("symbolic link \"%s\" target is too long", tmppath)));
            }
            linkpath[rllen] = '\0';

            /* test target file */
            struct stat linkst;
            if (lstat(linkpath, &linkst) < 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("target of symbolic link \"%s\" doesn't exist.", tmppath)));
                continue;
            }

            /* do not support symbolic link ->  symbolic link */
            if (!S_ISREG(linkst.st_mode) || linkst.st_size == 0) {
                continue;
            }

            filename = pstrdup(linkpath);
            *filesize = linkst.st_size;

        } else {
            filename = pstrdup(tmppath);
            *filesize = st.st_size;
        }

        // remove "//" in file path
        rc = memset_s(tmppath, PATH_MAX + 1, 0, PATH_MAX + 1);
        securec_check(rc, "\0", "\0");
        len = strlen(filename);
        rc = strncpy_s(tmppath, PATH_MAX + 1, filename, len);
        securec_check(rc, "\0", "\0");
        p = tmppath;
        q = filename;

        while (*p != '\0') {
            if (*p == '/') {
                if (isslash) {
                    isslash = false;
                    p++;
                    continue;
                } else
                    isslash = true;
            } else
                isslash = false;
            *q = *p;
            q++;
            p++;
        }
        *q = '\0';
        return filename;
    }

    return NULL;
}

bool StrToInt32(const char* s, int *val)
{
    /* set val to zero */
    *val = 0;
    int base = 10;
    const char* ptr = s;
    /* process digits */
    while (*ptr != '\0') {
        if (isdigit((unsigned char)*ptr) == 0)
            return false;
        int8 digit = (*ptr++ - '0');
        *val = *val * base + digit;
        if (*val > PG_INT32_MAX || *val < PG_INT32_MIN) {
            return false;
        }
    }
    return true;
}

char* TrimStr(const char* str)
{
    if (str == NULL) {
        return NULL;
    }

    int len;
    char* end = NULL;
    char* cur = NULL;
    char* cpyStr = pstrdup(str);
    char* begin = cpyStr;
    errno_t rc;

    while (isspace((int)*begin)) {
        begin++;
    }

    for (end = cur = begin; *cur != '\0'; cur++) {
        if (!isspace((int)*cur))
            end = cur;
    }

    if (*begin == '\0') {
        pfree_ext(cpyStr);
        return NULL;
    }

    len = end - begin + 1;
    rc = memmove_s(cpyStr, strlen(cpyStr), begin, len);
    securec_check(rc, "\0", "\0");
    cpyStr[len] = '\0';
    return cpyStr;
}

/* Deserialize the LOCATION options into locations list.
 * the multi-locations should be separated by '|'
 */
List* DeserializeLocations(const char* location)
{
    List* locations = NIL;
    int len = location ? strlen(location) : 0;
    char* str = NULL;
    char* token = NULL;
    char* saved = NULL;
    char* cpyLocation = NULL;

    if (!len)
        return locations;

    cpyLocation = pstrdup(location);
    token = strtok_r(cpyLocation, "|", &saved);
    while (token != NULL) {
        str = TrimStr(token);
        if (str == NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid URL \"%s\" in LOCATION", token)));
        locations = lappend(locations, makeString(str));
        token = strtok_r(NULL, "|", &saved);
    }
    return locations;
}

/*
 * Parse url to get path and pattern
 */
void getPathAndPattern(char* url, char** path, char** pattern)
{
    if (!is_valid_location(url)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized URL \"%s\"", url)));
    }

    /* delete space char at the header. */
    while (isspace((int)*url))
        url++;

    /* jump "gsfs://" or "file://" */
    if (*url != '/' && *url != '.') {
        char* pos = strstr(url, "://");
        if (pos != NULL) {
            url = pos + 3;
        }
    }

    char* p = strrchr(url, '/');
    if (p == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("wrong URL format \"%s\"", url)));
    }

    p++;
    *path = pnstrdup(url, p - url);
    *pattern = pstrdup(p);

    // delete space char at the tail.
    p = *pattern + strlen(*pattern) - 1;
    while (p != *pattern && isspace((int)*p))
        *(p--) = '\0';
}

/*
 * Get data node 's task form total task list
 */
List* getDataNodeTask(List* totalTask, const char* dnName)
{
    List* task = NIL;
    if (totalTask == NIL) {
        return task;
    }
    ListCell* lc = NULL;
    foreach (lc, totalTask) {
        DistFdwDataNodeTask* dnTask = (DistFdwDataNodeTask*)lfirst(lc);
        Assert(dnTask);

        if (0 == pg_strcasecmp(dnTask->dnName, dnName)) {
            task = dnTask->task;
            break;
        }
    }

    return task;
}

/*
 * Get next copy file segment
 * return false if end of task
 */
bool getNextCopySegment(CopyState cstate)
{
    List* taskList = cstate->taskList;
    ListCell* prevTaskPtr = cstate->curTaskPtr;
    ListCell* curTaskPtr = (prevTaskPtr == NULL ? list_head(taskList) : lnext(prevTaskPtr));

    if (curTaskPtr == NULL) {
        /* all tasks have been done or task list maybe empty */
        /* cstate->copy_file close in end_dist_copy */
        return false;
    }

    DistFdwFileSegment* segment = (DistFdwFileSegment*)lfirst(curTaskPtr);
    Assert(segment);

    if (prevTaskPtr != NULL) {
        /* not the first node of the task list */
        DistFdwFileSegment* prevSegment = (DistFdwFileSegment*)lfirst(prevTaskPtr);

        if (0 != pg_strcasecmp(segment->filename, prevSegment->filename)) {
            /* different from the last opened file */
            /* close the last opened file */
            Assert(cstate->copy_file);
            (void)FreeFile(cstate->copy_file);

            /* open the file */
            cstate->copy_file = AllocateFile(segment->filename, "r");
        }
    } else {
        /* open the file */
        cstate->copy_file = AllocateFile(segment->filename, "r");
    }

    if (cstate->filename != NULL) {
        MemoryContext oldCmxt = MemoryContextSwitchTo(cstate->copycontext);
        pfree(cstate->filename);
        cstate->filename = NULL;
        cstate->filename = pstrdup(segment->filename);
        cstate->cur_lineno = 0;
        MemoryContextSwitchTo(oldCmxt);
    }
    if (!cstate->copy_file) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("unable to open file \"%s\"", segment->filename)));
    }

    /* set the file read start postion */
    if (fseek(cstate->copy_file, segment->begin, SEEK_SET)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("unable to fseek file \"%s\"", segment->filename)));
    }
    cstate->curReadOffset = segment->begin;
    cstate->curTaskPtr = curTaskPtr;
    return true;
}

template <bool import>
bool getNextGDS(CopyState cstate)
{
    List* taskList = cstate->taskList;
    ListCell* prevTaskPtr = cstate->curTaskPtr;
    ListCell* curTaskPtr = (prevTaskPtr == NULL ? list_head(taskList) : lnext(prevTaskPtr));

    if (curTaskPtr == NULL) {
        /* all tasks have been done or task list maybe empty */
        /* cstate->io_stream close in end_dist_copy */
        return false;
    }

    DistFdwFileSegment* segment = (DistFdwFileSegment*)lfirst(curTaskPtr);
    Assert(segment);

    if (prevTaskPtr != NULL && cstate->io_stream != NULL) {
        cstate->io_stream->Close();
        delete (GDSStream*)cstate->io_stream;
        cstate->io_stream = NULL;
    }

    /* some string alloc in GDSStream use default memory context (CurrentMemoryContext) */
    MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);
    StringInfo buf = makeStringInfo();
    GDSStream* stream = New(CurrentMemoryContext) GDSStream;
    cstate->io_stream = stream;

    /* open socket */
    stream->Initialize(segment->filename);

    /* send command to gds */
    serialize_begin(cstate, buf, segment->filename, 0);
    stream->Write(buf->data, buf->len);
    resetStringInfo(buf);

    /* read response */
    stream->ReadMessage(*buf);
    (void)HandleGDSCmd(cstate, buf);

    if (import) {
        // Start request data from GDS server.
        //
        CmdBase cmd;
        cmd.m_type = CMD_TYPE_REQ;
        SerializeCmd(&cmd, stream->m_outBuf);
        stream->Flush();
    }
    (void)MemoryContextSwitchTo(oldcontext);

    cstate->curTaskPtr = curTaskPtr;

    return true;
}

// Serialize the commmand as the json string.
//
static void serialize_begin(CopyState cstate, StringInfo buf, const char* filename, int ref)
{
    /* Adding a couple of Assert to help RCA a mcxt problem. Should be removed later on. */
    CmdBegin cmd;

    cmd.m_type = CMD_TYPE_BEGIN;
    cmd.m_id = cstate->distSessionKey;
    cmd.m_url = pstrdup(filename);
    cmd.m_nodeType = IS_PGXC_COORDINATOR ? 'C' : 'D';
    cmd.m_nodeName = pstrdup(g_instance.attr.attr_common.PGXCNodeName);
    cmd.m_format = cstate->remoteExport ? FORMAT_REMOTEWRITE : cstate->fileformat;
    cmd.m_nodeNum = ref;

    Assert(CurrentMemoryContext);
    if (IS_CSV(cstate)) {
        cmd.m_escape = cstate->escape[0];
        cmd.m_quote = cstate->quote[0];
        cmd.m_header = cstate->header_line;
    } else if (IS_FIXED(cstate)) {
        cmd.m_header = cstate->header_line;
        if (((FixFormatter*)cstate->formatter)->forceLineSize)
            cmd.m_fixSize = ((FixFormatter*)cstate->formatter)->lineSize;
        else
            cmd.m_fixSize = 0;
    } else if (IS_TEXT(cstate)) {
        if (cstate->eol_type == EOL_UD)
            cmd.m_eol = pstrdup(cstate->eol);
    }

    Assert(CurrentMemoryContext);
    cmd.m_prefix = (cstate->out_filename_prefix) ? pstrdup(cstate->out_filename_prefix)
                                                 : pstrdup(RelationGetRelationName(cstate->rel));
    if (cstate->headerFilename) {
        StringInfo hUrl = makeStringInfo();

        appendStringInfo(hUrl, "%s/%s", cmd.m_url, cstate->headerFilename);
        cmd.m_fileheader = pstrdup(hUrl->data);
        pfree_ext(hUrl->data);
    }
    Assert(CurrentMemoryContext);
    SerializeCmd(&cmd, buf);

    ereport(LOG, (errmodule(MOD_GDS), (errmsg("Migration session %u initiated.", cstate->distSessionKey))));
    Assert(CurrentMemoryContext);
}

List* addNullTask(List* taskList)
{
    DistFdwFileSegment* segment = makeNode(DistFdwFileSegment);
    segment->filename = "/dev/null";
    segment->begin = 0;
    segment->end = -1;
    taskList = lappend(taskList, segment);
    return taskList;
}

/*
 * In private mode, datanode generates its own task
 */
List* getPrivateModeDataNodeTask(List* urllist, const char* dnName)
{
    char* path = NULL;
    char* pattern = NULL;
    char* url = NULL;
    char* filename = NULL;
    DIR* dir = NULL;
    ListCell* lc = NULL;
    List* task = NIL;
    long filesize = 0;
    char privatePath[PATH_MAX + 1];
    errno_t rc = EOK;

    foreach (lc, urllist) {
        url = strVal(lfirst(lc));
        /* get path and pattern */
        getPathAndPattern(url, &path, &pattern);

        /* datanode private path = url + nodename */
        rc = snprintf_s(privatePath, sizeof(privatePath), PATH_MAX, "%s\%s", path, dnName);
        securec_check_ss(rc, "\0", "\0");

        privatePath[PATH_MAX] = '\0';

        dir = AllocateDir(privatePath);
        if (dir == NULL) {
            /* not have private path */
            continue;
        }

        /* for each file */
        while ((filename = scan_dir(dir, privatePath, pattern, &filesize)) != NULL) {
            /* add to task list */
            DistFdwFileSegment* segment = makeNode(DistFdwFileSegment);
            segment->filename = pstrdup(filename);
            segment->begin = 0;
            segment->end = filesize;
            task = lappend(task, segment);
        }

        FreeDir(dir);
    }

    return task;
}

static int getUrlReference(const char* url, List* totalTask)
{
    ListCell* lc = NULL;
    ListCell* llc = NULL;
    int ref = 0;

    foreach (lc, totalTask) {
        DistFdwDataNodeTask* tsk = (DistFdwDataNodeTask*)lfirst(lc);
        foreach (llc, tsk->task) {
            DistFdwFileSegment* fs = (DistFdwFileSegment*)lfirst(llc);
            if (strcmp(fs->filename, url) == 0)
                ref++;
        }
    }
    Assert(ref > 0);
    return ref;
}

template <bool import>
void initNormalModeState(CopyState cstate, const char* filename, List* totalTask)
{

#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-supported feature"),
            errdetail("Gauss Data Service(GDS) are not supported in single node mode.")));
#endif

    cstate->copy_dest = COPY_GDS;
    if (IS_PGXC_COORDINATOR) {
        List* locations = DeserializeLocations(filename);
        ListCell* lc = NULL;
        StringInfo buf = makeStringInfo();
        uint32 totalFiles = 0;

        foreach (lc, locations) {
            GDSStream* stream = New(CurrentMemoryContext) GDSStream;

            filename = strVal(lfirst(lc));
            cstate->io_stream = stream;
            stream->Initialize(filename);
            resetStringInfo(buf);
            serialize_begin(cstate, buf, filename, getUrlReference(filename, totalTask));
            stream->Write(buf->data, buf->len);
            resetStringInfo(buf);
            stream->ReadMessage(*buf);
            totalFiles += HandleGDSCmd(cstate, buf);
            cstate->file_list = lappend(cstate->file_list, stream);
        }

        if (!totalFiles && u_sess->attr.attr_storage.raise_errors_if_no_files)
            ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("no files found to import")));
    } else if (IS_PGXC_DATANODE && totalTask != NIL) {
        /* in normal mode, head line skipped by gds.
        cstate->header_line must be false in data node */
        cstate->header_line = false;
        cstate->inBuffer.reset();

        /*
         * If the current DN isn't involved in GDS foreign table distribution info
         * the assigned taskList is NULL.
         */
        List* taskList = getDataNodeTask(totalTask, g_instance.attr.attr_common.PGXCNodeName);

        cstate->taskList = taskList;
        cstate->curTaskPtr = NULL;

        if (taskList != NIL) {
            (void)getNextGDS<import>(cstate);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmsg("Failed to get import task for dn:%s", g_instance.attr.attr_common.PGXCNodeName)));
        }
    }
}

void initSharedModeState(CopyState cstate, const char* filename, List* totalTask)
{
    cstate->copy_dest = COPY_FILE_SEGMENT;
    if (IS_PGXC_DATANODE) {
        /* get this data node 's task list */
        List* taskList = getDataNodeTask(totalTask, g_instance.attr.attr_common.PGXCNodeName);
        if (list_length(taskList) == 0) {
            taskList = addNullTask(taskList);
        }
        cstate->taskList = taskList;
        cstate->curTaskPtr = NULL;
        cstate->curReadOffset = 0;
        (void)getNextCopySegment(cstate);
    }
}

void initPrivateModeState(CopyState cstate, const char* filename, List* totalTask)
{
    if (IS_PGXC_DATANODE) {
        List* urllist = DeserializeLocations(filename);
        List* taskList = getPrivateModeDataNodeTask(urllist, g_instance.attr.attr_common.PGXCNodeName);
        if (list_length(taskList) == 0) {
            taskList = addNullTask(taskList);
        }
        cstate->taskList = taskList;
        cstate->curTaskPtr = NULL;
        cstate->curReadOffset = 0;
        (void)getNextCopySegment(cstate);
    }
}

void endNormalModeBulkLoad(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_GDS);
    if (IS_PGXC_COORDINATOR) {
        ListCell* lc = NULL;
        foreach (lc, cstate->file_list)
            delete (GDSStream*)lfirst(lc);
    } else if (cstate->io_stream) {
        /*
         * In bulkload tasks ended not as a result of exceptions, we want to make sure that the
         * error remotelog buffer has been flushed to the file; if not, this bulkload is not considered
         * to be successful
         * If we encounter an uncatchable error during foreign scan execution (isExceptionShutdown)
         * or early return/stop in planner (currently constant qualification check in planner,
         * u_sess->exec_cxt.exec_result_checkqual_fail), we don't need to check GDS status at all and close regardless.
         */
        if (!cstate->isExceptionShutdown && !u_sess->exec_cxt.exec_result_checkqual_fail) {
            /* send CMD_TYPE_REQ to make sure remote error log is write down. */
            CmdBase cmd;
            cmd.m_type = CMD_TYPE_REQ;
            SerializeCmd(&cmd, ((GDSStream*)cstate->io_stream)->m_outBuf);
            cstate->io_stream->Flush();

            /* check GDS return CMD_TYPE_END */
            CheckIfGDSReallyEnds(cstate, (GDSStream*)cstate->io_stream);
        }

        cstate->io_stream->Close();
        delete (GDSStream*)cstate->io_stream;
        cstate->io_stream = NULL;
    }
    ereport(LOG, (errmodule(MOD_GDS), (errmsg("Migration session %u terminated.", cstate->distSessionKey))));
}

void endSharedModeBulkLoad(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_FILE_SEGMENT);
    if (cstate->copy_file && FreeFile(cstate->copy_file)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", cstate->filename)));
    }
}

void endPrivateModeBulkLoad(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_FILE);
    if (cstate->copy_file && FreeFile(cstate->copy_file)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", cstate->filename)));
    }
}

/*
 * CopyGetData function for COPY_FILE
 */
static int CopyGetDataFile(CopyState cstate, void* databuf, int minread, int maxread)
{
    PROFILING_MDIO_START();
    int bytesread = fread(databuf, 1, maxread, cstate->copy_file);
    PROFILING_MDIO_END_READ(maxread, bytesread);
    if (ferror(cstate->copy_file)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from COPY file: %m")));
    }
    return bytesread;
}

/*
 * CopyGetData function for COPY_FILE_SEGMENT
 */
static int CopyGetDataFileSegment(CopyState cstate, void* databuf, int minread, int maxread)
{
    Assert(cstate->curTaskPtr);
    int bytesread = 0;
    /* get segment end postion */
    long segmentEnd = ((DistFdwFileSegment*)lfirst(cstate->curTaskPtr))->end;
    /* calculate residual offset */
    long residues = segmentEnd != -1 ? segmentEnd - cstate->curReadOffset : maxread;
    if (residues > 0) {
        maxread = Min(maxread, residues);
        PROFILING_MDIO_START();
        bytesread = fread(databuf, 1, maxread, cstate->copy_file);
        PROFILING_MDIO_END_READ(maxread, bytesread);
        if (ferror(cstate->copy_file)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from COPY file: %m")));
        }
        cstate->curReadOffset += bytesread;
    }

    return bytesread;
}

// Handle the GDS command.
//
int HandleGDSCmd(CopyState cstate, StringInfo buf)
{
    int retval = 0;
    CmdBase* cmd = DeserializeCmd(buf);
    GDSStream* stream = dynamic_cast<GDSStream*>(cstate->io_stream);
    if (stream == NULL) {
        ereport(ERROR,
            (errmodule(MOD_GDS),
                errcode(ERRORCODE_ASSERT_FAILED),
                errmsg("GDS stream is null pointer, the dynamic cast failed")));
    }

    switch (cmd->m_type) {
        case CMD_TYPE_DATA:
        case CMD_TYPE_DATA_SEG: {
            /* it's not necessary to copy message data, because message buffer will not be reused
             * if the current package is not fetched and handled.
             */
            cstate->inBuffer.set(((CmdData*)cmd)->m_data, ((CmdData*)cmd)->m_len);

            /* solve the problem that local variable name conflicts with the other stack variable. */
            CmdBase tmp_cmd;
            tmp_cmd.m_type = CMD_TYPE_REQ;
            SerializeCmd(&tmp_cmd, stream->m_outBuf);
            stream->Flush();
        } break;
        case CMD_TYPE_ERROR:
            switch (((CmdError*)cmd)->m_level) {
                case LEVEL_ERROR:
                    ereport(
                        ERROR, (errmodule(MOD_GDS), errcode(ERRCODE_LOG), errmsg("%s", ((CmdError*)cmd)->m_detail)));
                    break;
                case LEVEL_WARNING:
                    ereport(WARNING, (errmodule(MOD_GDS), (errmsg("%s", ((CmdError*)cmd)->m_detail))));
                    break;
                default:
                    // ignore it.
                    retval = 1;
                    break;
            }
            break;
        case CMD_TYPE_FILE_SWITCH: {
            MemoryContext oldctx;
            oldctx = MemoryContextSwitchTo(cstate->copycontext);
            cstate->filename = pstrdup(((CmdFileSwitch*)cmd)->m_fileName);
            MemoryContextSwitchTo(oldctx);
        } break;
        case CMD_TYPE_RESPONSE:
            retval = ((CmdResponse*)cmd)->m_result;
            if (retval > 0)
                ereport(
                    ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("%s", ((CmdResponse*)cmd)->m_reason)));
            break;
        case CMD_TYPE_QUERY_RESULT_V1: {
            retval = ((CmdQueryResult*)cmd)->m_result;
            const char* gds_version = ((CmdQueryResult*)cmd)->m_version_num;
            if (gds_version == NULL || (strcmp(gds_version, gds_protocol_version_gaussdb)) != 0) {
                ereport(ERROR,
                    (errmodule(MOD_GDS),
                        errcode(ERRCODE_LOG),
                        errmsg("GDS version does not match Gaussdb version."),
                        errdetail("Please use GDS from the same distribution package. GDS might need to be upgrade or "
                                  "downgrade to match the version")));
            }
            if (u_sess->attr.attr_storage.gds_debug_mod) {
                ereport(LOG,
                    (errmodule(MOD_GDS),
                        errmsg("Handshake with GDS: Success. Session : \"%s\"",
                            ((GDSStream*)cstate->io_stream)->m_uri->m_uri)));
            }
            break;
        }
        case CMD_TYPE_END:
        default:
            break;
    }

    if (CMD_TYPE_QUERY_RESULT_V1 != cmd->m_type)
        retval = cmd->m_type;
    pfree_ext(cmd);
    return retval;
}

#define HAS_DATA_BUFFERED(buf)         \
    ((buf)->cursor + 4 < (buf)->len && \
        (buf)->cursor + 4 + ntohl(*(uint32*)((buf)->data + (buf)->cursor + 1)) < (size_t)(buf)->len)

/*
 * @Description: get next line from remote server GDS. this function make
 *               sure that one complete line will be returned if any data exists.
 * @IN/OUT cstate: it will read from GDS and store data block in itself,
 *                 and extract line from data block.
 * @Return: return true if see EOF, otherwise return false.
 * @See also:
 */
static bool CopyGetNextLineFromGDS(CopyState cstate)
{
    GDSStream* streamfd = dynamic_cast<GDSStream*>(cstate->io_stream);
    Assert(streamfd != NULL);
    if (unlikely(streamfd == NULL)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid streamfd value.")));
    }

    StringInfo buffer = streamfd->m_inBuf;
    stringinfo_ptr inBuffer = &cstate->inBuffer;
    int length = 0;
    int retval = 0;

    /* goto-output will ensure the line ends with '\n', so its type is EOL_NL. */
    if (cstate->eol_type != EOL_UD)
        cstate->eol_type = EOL_NL;
    resetStringInfo(&(cstate->line_buf));

retry1:
    if (inBuffer->cursor < inBuffer->len) {
        /* Length info: 4B */
        length = ntohl(*(uint32*)(inBuffer->data + inBuffer->cursor));
        /* Check data integraty and make sure we have a row header per line */
        if (length <= 4 || length > inBuffer->len)
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED),
                    errmsg("Abnormal data package received, package length is %d, input buffer length is %d",
                        length,
                        inBuffer->len),
                    errdetail("Illegal data exists in data files. Please cleanse data files before migration.")));
        length -= 4;

        enlargeStringInfo(&(cstate->line_buf), length);

        /* Line Number: 4B */
        inBuffer->cursor += 4;
        cstate->cur_lineno = ntohl(*(uint32*)(inBuffer->data + inBuffer->cursor));

        /* Line Data */
        inBuffer->cursor += 4;
        appendBinaryStringInfo(&(cstate->line_buf), inBuffer->data + inBuffer->cursor, length);
        inBuffer->cursor += length;

        if (CMD_TYPE_DATA_SEG == retval)
            goto retry1;

    output:
        if (cstate->eol_type == EOL_UD) {
            if (strstr(cstate->line_buf.data, cstate->eol) == NULL &&
                cstate->line_buf.data[cstate->line_buf.len] == '\0') {
                /* append \n if this line doesn't end with EOL */
                appendBinaryStringInfo(&(cstate->line_buf), cstate->eol, strlen(cstate->eol));
            }
        } else if (cstate->line_buf.data[cstate->line_buf.len - 2] == '\r') {
            /* process \r\n and replace them with \n */
            cstate->line_buf.data[cstate->line_buf.len - 2] = '\n';
            cstate->line_buf.data[cstate->line_buf.len - 1] = '\0';
            cstate->line_buf.len--;
        } else if (cstate->line_buf.data[cstate->line_buf.len - 1] == '\r') {
            /* process \r and replace it with \n */
            cstate->line_buf.data[cstate->line_buf.len - 1] = '\n';
        } else if (cstate->line_buf.data[cstate->line_buf.len - 1] != '\n' &&
                   cstate->line_buf.data[cstate->line_buf.len] == '\0') {
            /* append \n if this line doesn't end with EOL */
            appendBinaryStringInfo(&(cstate->line_buf), "\n", 1);
        }
        return false;
    }

retry2:

    /* read more data into msg_buf from GDS.
     * HAS_DATA_BUFFERED() ensure that at least one complete package exists.
     */
    while (!HAS_DATA_BUFFERED(buffer) && (retval = streamfd->Read()) != EOF)
        ;

    if (retval != EOF) {
        retval = HandleGDSCmd(cstate, buffer);
        switch (retval) {
            case CMD_TYPE_DATA:
                /* fall down and read one completed line */
            case CMD_TYPE_DATA_SEG:
                /*
                 * read more data segments until the whole line is completed.
                 * messages like CMD_TYPE_DATA_SEG1,
                 *				 CMD_TYPE_DATA_SEG2,
                 *				 CMD_TYPE_DATA_SEG3,
                 *				 ...,
                 *				 CMD_TYPE_DATA	<--- the last message type
                 */
                goto retry1;
            case CMD_TYPE_END:
                /*
                 * If there are datas left with EOF message DN need receive a more EOF message to do
                 * double check. For new un-completed line operation the EOF message from GDS may
                 * be already prefetched so we need send a extra CMD_TYPE_REQ message to prevent DN
                 * falls into hang just waiting for a CMD_TYPE_END message from GDS.
                 */
                if (cstate->line_buf.len > 0) {
                    CmdBase cmd;
                    cmd.m_type = CMD_TYPE_REQ;
                    SerializeCmd(&cmd, streamfd->m_outBuf);
                    streamfd->Flush();

                    /*
                     * flush the the current line in cstate->line_buf.
                     */
                    goto output;
                } else
                    return true;
            default:
                /* read nore data and then parse the next line */
                goto retry2;
        }
    } else if (buffer->len > 0)
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Incomplete Message from GDS .")));

    return true;
}

void bulkloadFuncFactory(CopyState cstate)
{
    ImportMode mode = cstate->mode;
    BulkLoadFunc& func = cstate->bulkLoadFunc;
    CopyGetDataFunc& copyGetDataFunc = cstate->copyGetDataFunc;
    CopyReadlineFunc& readlineFunc = cstate->readlineFunc;
    GetNextCopyFunc& getNextCopyFunc = cstate->getNextCopyFunc;

    /* set attributes reading functions */
    bulkload_set_readattrs_func(cstate);
    /* parse time format specified by user */
    bulkload_init_time_format(cstate);

    switch (mode) {
        case MODE_NORMAL: /* for GDS oriented dist import */
            if (is_obs_protocol(cstate->filename)) {
#ifndef ENABLE_LITE_MODE
                /* Attache working house routines for OBS oriented dist import */
                func.initBulkLoad = initOBSModeState;
                func.endBulkLoad = endOBSModeBulkLoad;
                copyGetDataFunc = NULL;
                readlineFunc = CopyGetNextLineFromOBS;
                getNextCopyFunc = getNextOBS;
#else
                FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
            } else {
                /* Attache working house routines for GDS oriented dist import */
                func.initBulkLoad = initNormalModeState<true>;
                func.endBulkLoad = endNormalModeBulkLoad;
                copyGetDataFunc = NULL;
                readlineFunc = CopyGetNextLineFromGDS;
                getNextCopyFunc = getNextGDS<true>;
            }

            break;
        case MODE_SHARED:
            func.initBulkLoad = initSharedModeState;
            func.endBulkLoad = endSharedModeBulkLoad;
            copyGetDataFunc = CopyGetDataFileSegment;
            readlineFunc = CopyReadLineText;
            getNextCopyFunc = getNextCopySegment;
            break;
        case MODE_PRIVATE:
            if (NULL == strstr(cstate->filename, ROACH_PREFIX)) {
                func.initBulkLoad = initPrivateModeState;
                func.endBulkLoad = endPrivateModeBulkLoad;
                copyGetDataFunc = CopyGetDataFile;
                readlineFunc = CopyReadLineText;
                getNextCopyFunc = getNextCopySegment;
            } else {
                func.initBulkLoad = initRoachState<true>;
                func.endBulkLoad = endRoachBulkLoad;
                copyGetDataFunc = copyGetRoachData;
                readlineFunc = CopyReadLineText;
                getNextCopyFunc = getNextRoach<true>;
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED), errmsg("unimplemented bulkload mode")));
            break;
    }
}

CopyState beginExport(
    Relation rel, const char* filename, List* options, bool isRemote, uint32 sessionKey, List* tasklist)
{
    CopyState cstate = BeginCopy(false, rel, NULL, NULL, NIL, options, false);
    MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

    cstate->distSessionKey = sessionKey;
    if (IS_PGXC_DATANODE && !isRemote) {
        exportAllocNewFile(cstate, filename);
    }

    Assert(cstate->rel);
    TupleDesc tupDesc = RelationGetDescr(cstate->rel);
    Form_pg_attribute* attr = tupDesc->attrs;
    int num_phys_attrs = tupDesc->natts;

    cstate->curPartionRel = cstate->rel;
    cstate->null_print_client = cstate->null_print;
    cstate->fe_msgbuf = makeStringInfo();

    if (isRemote) {
        cstate->remoteExport = true;

        if (!is_roach_location(filename)) {
            cstate->writelineFunc = RemoteExportWriteOut;

            if (is_obs_protocol(filename)) {
#ifndef ENABLE_LITE_MODE
                /* Fetch OBS write only table related attribtues */
                getOBSOptions(&cstate->obs_copy_options, options);

                const char* object_path = filename;

                /* fix object path end with '/' */
                int filename_len = strlen(filename);
                if (filename[filename_len - 1] != '/') {
                    char* filename_fix = (char*)palloc(filename_len + 2);
                    int rc = snprintf_s(filename_fix, (filename_len + 2), (filename_len + 1), "%s/", filename);
                    securec_check_ss(rc, "", "");

                    object_path = filename_fix;
                }

                initOBSModeState(cstate, object_path, tasklist);
#else
                FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
            } else {
                initNormalModeState<false>(cstate, filename, tasklist);
            }

            cstate->outBuffer = makeStringInfo();
        } else {
            cstate->writelineFunc = exportRoach;
            initRoachState<false>(cstate, filename, tasklist);
            exportInitOutBuffer(cstate);
        }
    } else {
        cstate->writelineFunc = exportAppendOutBuffer<false>;
        exportInitOutBuffer(cstate);
    }

    if (IS_FIXED(cstate)) {
        SetFixedAlignment(tupDesc, rel, (FixFormatter*)(cstate->formatter), cstate->out_fix_alignment);
        VerifyFixedFormatter(tupDesc, (FixFormatter*)(cstate->formatter));
    }
    /* Get info about the columns we need to process. */
    cstate->out_functions = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
    ListCell* cur = NULL;
    foreach (cur, cstate->attnumlist) {
        int attnum = lfirst_int(cur);
        Oid out_func_oid;
        bool isvarlena = false;

        if (IS_BINARY(cstate))
            getTypeBinaryOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
        else
            getTypeOutputInfo(attr[attnum - 1]->atttypid, &out_func_oid, &isvarlena);
        fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
    }

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway.	(We don't need a whole econtext as CopyFrom does.)
     */
    cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
        "DIST FOREIGN TABLE",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * For non-binary copy, we need to convert null_print to file
     * encoding, because it will be sent directly with CopySendString.
     */
    if (cstate->need_transcoding)
        cstate->null_print_client = pg_server_to_any(cstate->null_print, cstate->null_print_len, cstate->file_encoding);

    (void)MemoryContextSwitchTo(oldcontext);
    return cstate;
}

void execExport(CopyState cpstate, TupleTableSlot* slot)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(cpstate->copycontext);

    // write the header-line first for each new segment file. so that for
    // read-only foreign table, all files can be correctly handled.
    //
    if (cpstate->distCopyToTotalSize == 0 && !cpstate->remoteExport)
        exportWriteHeaderLine(cpstate);

    Oid tupOid = InvalidOid;
    if (slot->tts_tuple && RelationGetDescr(cpstate->rel)->tdhasoid)
        tupOid = HeapTupleGetOid((HeapTuple) slot->tts_tuple);

    tableam_tslot_getallattrs(slot);
    CopyOneRowTo(cpstate, tupOid, slot->tts_values, slot->tts_isnull);

    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * Check if GDS actually get the end message from CN/DN and flushed data to files.So that we check
 * if there is an ack (which is a CMD_TYPE_END) of our CMD_TYPE_END message. If not, we throw
 * an error to abort current export transaction cause sth might go wrong with GDS during this ending
 * process; otherwise, this transaction could end normally.
 *
 * @param cstate the current CopyState
 * @param stream stream used by this transaction within this communication
 */
void CheckIfGDSReallyEnds(CopyState cstate, GDSStream* stream)
{
    StringInfo buf = makeStringInfo();

    stream->ReadMessage(*buf);
    HandleGDSCmd(cstate, buf);
    /*
     *	The reason this function exists is to make sure that GDS successfully flushed
     *	its write buffer.
     *	1. Sending the CMD_TYPE_END will do for export since GDS flushes upon receiving a CMD_TYPE_END.
     *	2. Sending the extra CMD_TYPE_REQ will do for import. In the normal situation, the extra
     *		REQ will trigger the HandleWrite to send us another END, upon which GDS will also
     *		flush its buffer.
     *	In abnormal circumstances where the process does not end correctly (uncatchable error
     *	or Executor early stop), checking the return message does not make any sense either.
     *
     *	Therefore, we only send what we need, check and see we got something back, and yet
     *	don't need to check what that acutally is.
     */
}

void endExport(CopyState cstate)
{
    if (cstate->remoteExport && cstate->copy_dest == COPY_GDS) {
        if (IS_PGXC_COORDINATOR) {
            ListCell* lc = NULL;

            foreach (lc, cstate->file_list) {
                GDSStream* stream = (GDSStream*)lfirst(lc);
                CmdBase cmd;
                if (cstate->outBuffer->len > 0)
                    RemoteExportFlushData(cstate);

                cmd.m_type = CMD_TYPE_END;
                SerializeCmd(&cmd, stream->m_outBuf);
                stream->Flush();
                stream->Close();
                delete stream;
            }
        } else {
            GDSStream* stream = dynamic_cast<GDSStream*>(cstate->io_stream);
            Assert(stream != NULL);
            if (unlikely(stream == NULL)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid stream value.")));
            }
            CmdBase cmd;
            if (cstate->outBuffer->len > 0)
                RemoteExportFlushData(cstate);

            cmd.m_type = CMD_TYPE_END;
            SerializeCmd(&cmd, stream->m_outBuf);
            stream->Flush();

            CheckIfGDSReallyEnds(cstate, stream);
            stream->Close();
            delete stream;
            cstate->io_stream = NULL;
        }
        ereport(LOG, (errmodule(MOD_GDS), (errmsg("Migration session %u terminated.", cstate->distSessionKey))));
    } else if (cstate->copy_dest == COPY_ROACH) {
        if (IS_PGXC_DATANODE) {
            if (cstate->raw_buf_len > 0)
                exportRoachFlushOut(cstate, true);

            endRoachBulkLoad(cstate);
        }
    } else if (cstate->copy_dest == COPY_OBS) {
#ifndef ENABLE_LITE_MODE
        if (IS_PGXC_DATANODE) {
            if (cstate->outBuffer->len > 0)
                RemoteExportFlushData(cstate);

            cstate->io_stream->Flush();
            endOBSModeBulkLoad(cstate);
        }
#else
        FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    } else
        exportDeinitOutBuffer(cstate);
    MemoryContextDelete(cstate->rowcontext);
    EndCopyTo(cstate);
}

uint64 exportGetTotalSize(CopyState cstate)
{
    return cstate->distCopyToTotalSize;
}

void exportResetTotalSize(CopyState cstate)
{
    cstate->distCopyToTotalSize = 0;
}

static void exportWriteHeaderLine(CopyState cstate)
{
    Assert(!IS_BINARY(cstate));
    if (cstate->header_line == false)
        return;

    // treat header line data as one normal record data, so row
    // memory context will be used, and is reset before the next
    // record is parsed and stored.
    MemoryContext oldcontext = MemoryContextSwitchTo(cstate->rowcontext);
    appendBinaryStringInfo(cstate->fe_msgbuf, cstate->headerString->data, cstate->headerString->len);
    exportAppendOutBuffer<true>(cstate);

    (void)MemoryContextSwitchTo(oldcontext);
}

void exportAllocNewFile(CopyState cstate, const char* newfile)
{
    /*
     * Prevent write to relative path ... too easy to shoot oneself in the
     * foot by overwriting a database file ...
     */
    if (!is_absolute_path(newfile))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_NAME), errmsg("relative path not allowed for writable foreign table file")));

    // first, close the existing file and free existing filename space
    if (cstate->copy_file)
        (void)FreeFile(cstate->copy_file);
    if (cstate->filename)
        pfree_ext(cstate->filename);

    struct stat st;
    bool dirIsExist = false;
    struct stat checkdir;

    cstate->filename = pstrdup(newfile);
    if (stat(cstate->filename, &checkdir) == 0)
        dirIsExist = true;

    // then create new file
    cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
    if (cstate->copy_file == NULL)
        ereport(
            ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\" for writing: %m", cstate->filename)));

    fstat(fileno(cstate->copy_file), &st);
    if (S_ISDIR(st.st_mode))
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a directory", cstate->filename)));

    if (!dirIsExist) {
        if (fchmod(fileno(cstate->copy_file), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not chmod file \"%s\" : %m", cstate->filename)));
    }
}

static inline void exportInitOutBuffer(CopyState cstate)
{
    // fixed buffer is used to reduce disk io.
    cstate->raw_buf = (char*)palloc(RAW_BUF_SIZE);
    cstate->raw_buf_len = 0;
}

// append one record to output buffer, and newline will be added.
// if the output buffer is full, call fwrite() once.
//
template <bool skipEOL>
static void exportAppendOutBuffer(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_FILE);
    Assert(cstate->fileformat != FORMAT_BINARY);
    errno_t rc = 0;

    // first append the newline to the new record data.
    if (!skipEOL) {
        SetEndOfLineType(cstate);
    }
    StringInfo in = cstate->fe_msgbuf;
    const char* pdata = in->data;
    int dsize = in->len;

    if (cstate->raw_buf_len == RAW_BUF_SIZE) {
        // Flush out and set cstate->raw_buf_len = 0
        exportFlushOutBuffer(cstate);
    }

    do {
        int avail = (RAW_BUF_SIZE - cstate->raw_buf_len);
        Assert(avail > 0);

        if (dsize <= avail) {
            rc = memcpy_s(cstate->raw_buf + cstate->raw_buf_len, avail, pdata, dsize);
            securec_check(rc, "\0", "\0");
            cstate->raw_buf_len += dsize;
            break;
        }

        rc = memcpy_s(cstate->raw_buf + cstate->raw_buf_len, avail, pdata, avail);
        securec_check(rc, "\0", "\0");
        Assert((cstate->raw_buf_len + avail) == RAW_BUF_SIZE);
        cstate->raw_buf_len = RAW_BUF_SIZE;
        // Flush out and set cstate->raw_buf_len = 0
        exportFlushOutBuffer(cstate);

        pdata += avail;
        dsize -= avail;
        Assert(dsize > 0);
    } while (true);

    cstate->distCopyToTotalSize += in->len;
    resetStringInfo(in);
}

inline static void RemoteExportFlushData(CopyState cstate)
{
    int retval = 0;
    GDSStream* stream = dynamic_cast<GDSStream*>(cstate->io_stream);

    if (cstate->copy_dest == COPY_OBS) {
        cstate->io_stream->Write(cstate->outBuffer->data, cstate->outBuffer->len);
        resetStringInfo(cstate->outBuffer);
        return;
    }

    Assert(stream != NULL);
    if (unlikely(stream == NULL)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid stream value.")));
    }

    PackData(stream->m_outBuf, cstate->outBuffer);
    stream->Flush();
    resetStringInfo(cstate->outBuffer);

    ereport(LOG,
        (errcode(ERRCODE_LOG), errmsg("Sending data to GDS at: %s:%d", stream->m_uri->m_uri, stream->m_uri->m_port)));

    while (!HAS_DATA_BUFFERED(stream->m_inBuf) && (retval = stream->Read()) != EOF)
        ;

    if (retval != EOF) {
        retval = HandleGDSCmd(cstate, stream->m_inBuf);
        if (retval != CMD_TYPE_RESPONSE) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Receive wrong messge %d from GDS.", retval)));
        }
    } else if (stream->m_inBuf->len > 0)
        ereport(ERROR, (errcode(ERRCODE_OPERATE_RESULT_NOT_EXPECTED), errmsg("Receive incomplete message from GDS.")));
}

static void RemoteExportWriteOut(CopyState cstate)
{
    Assert(cstate->copy_dest == COPY_GDS || cstate->copy_dest == COPY_OBS);
    Assert(cstate->fileformat != FORMAT_BINARY);

    // first append the newline to the new record data.
    SetEndOfLineType(cstate);

    StringInfo in = cstate->fe_msgbuf;

    /* For OBS case, we only flush data to OBS writer's buffer */
    if (cstate->outBuffer->len >= MAX_WRITE_BLKSIZE)
        RemoteExportFlushData(cstate);

    appendBinaryStringInfo(cstate->outBuffer, in->data, in->len);

    resetStringInfo(in);
}

static void exportDeinitOutBuffer(CopyState cstate)
{
    if (cstate->raw_buf_len > 0)
        exportFlushOutBuffer(cstate);

    if (cstate->raw_buf) {
        /* For OBS case we don't use cstate->raw_buf */
        Assert(cstate->copy_dest != COPY_OBS);
        pfree_ext(cstate->raw_buf);
    }
    cstate->raw_buf = NULL;
}

void exportFlushOutBuffer(CopyState cstate)
{
    Assert(((cstate->filename == NULL) && IS_STREAM_PLAN) || (!(cstate->filename == NULL) && !IS_STREAM_PLAN));

    if (cstate->filename != NULL) {
        PROFILING_MDIO_START();
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        int ret = fwrite(cstate->raw_buf, cstate->raw_buf_len, 1, cstate->copy_file);
        PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
        PROFILING_MDIO_END_WRITE(cstate->raw_buf_len, (ret * cstate->raw_buf_len));
        if ((ret != 1) || ferror(cstate->copy_file)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to COPY file: %m")));
        }
    }

    cstate->raw_buf_len = 0;
}

void FormAndSaveImportError(CopyState cstate, Relation errRel, Datum begintime, ImportErrorLogger* elogger)
{
    ImportError edata;
    AutoContextSwitch autosiwitch(elogger->m_memCxt);

    MemoryContextReset(elogger->m_memCxt);
    elogger->FormError(cstate, begintime, &edata);
    elogger->SaveError(&edata);
}

void FormAndSaveImportError(CopyState cstate, Relation errRel, Datum begintime, CopyErrorLogger* elogger)
{
    CopyError edata;
    AutoContextSwitch autosiwitch(elogger->m_memCxt);

    MemoryContextReset(elogger->m_memCxt);
    elogger->FormError(cstate, begintime, &edata);
    elogger->SaveError(&edata);
}

static void FormAndSaveImportWhenLog(CopyState cstate, Relation errRel, Datum begintime, CopyErrorLogger* elogger)
{
    CopyError edata;
    AutoContextSwitch autosiwitch(elogger->m_memCxt);

    MemoryContextReset(elogger->m_memCxt);
    elogger->FormWhenLog(cstate, begintime, &edata);
    elogger->SaveError(&edata);
}

void FlushErrorInfo(Relation rel, EState* estate, ErrorCacheEntry* cache)
{
    ImportError err;

    Assert(RelationGetNumberOfAttributes(rel) == ImportError::MaxNumOfValue);
    err.m_desc = RelationGetDescr(rel);

    /* handle one logger in each loop */
    for (int i = 0; i < cache->logger_num; ++i) {
        /* read from error cache file and insert data into error table */
        while (cache->loggers[i]->FetchError(&err) != EOF) {
            HeapTuple tup = NULL;

            CHECK_FOR_INTERRUPTS();

            tup = (HeapTuple)heap_form_tuple(err.m_desc, err.m_values, err.m_isNull);
            tableam_tuple_insert(rel, tup, estate->es_output_cid, 0, NULL);
            tableam_tops_free_tuple(tup);
        }
    }
}

/*
 * @Description: set attributes reading function for non-binary bulkload.
 *   here only TEXT/CSV/FIXED formats are taken into account.
 * @IN/OUT cstate: CopyState object, readAttrsFunc will be assigned.
 * @See also:
 */
static void bulkload_set_readattrs_func(CopyState cstate)
{
    bool multi_byte_delim = (cstate->delim_len > 1);

    switch (cstate->fileformat) {
        case FORMAT_TEXT:
            if (multi_byte_delim)
                cstate->readAttrsFunc = CopyReadAttributesTextT<true>;
            else
                cstate->readAttrsFunc = CopyReadAttributesTextT<false>;
            break;
        case FORMAT_CSV:
            if (multi_byte_delim)
                cstate->readAttrsFunc = CopyReadAttributesCSVT<true>;
            else
                cstate->readAttrsFunc = CopyReadAttributesCSVT<false>;
            break;
        case FORMAT_FIXED:
            cstate->readAttrsFunc = ReadAttributesFixedWith;
            break;
        default:
            /*
             * 1. FORMAT_BINARY format will not use readAttrsFunc pointer.
             * 2. FORMAT_REMOTEWRITE format doesn't bulkload data into tables.
             * So just set readAttrsFunc be NULL.
             */
            cstate->readAttrsFunc = NULL;
            break;
    }
}

/*
 * @Description: if customer has specified date/time/timestamp/small datatime format,
 *    parse this format string and get its format node.
 * @IN/OUT cstate: CopyState object holding date/time/timestamp/smalldatetime format.
 * @See also: TimeFormatInfo struct in formatting.cpp
 */
static void bulkload_init_time_format(CopyState cstate)
{
    Assert(cstate != NULL);
    if (cstate->date_format.str)
        cstate->date_format.fmt = get_time_format(cstate->date_format.str);

    if (cstate->time_format.str)
        cstate->time_format.fmt = get_time_format(cstate->time_format.str);

    if (cstate->timestamp_format.str)
        cstate->timestamp_format.fmt = get_time_format(cstate->timestamp_format.str);

    if (cstate->smalldatetime_format.str)
        cstate->smalldatetime_format.fmt = get_time_format(cstate->smalldatetime_format.str);
}

/*
 *	ProcessCopyErrorLogOptions is used to check the specific requirements needed for copy fault
 *	tolerance.
 */
void ProcessCopyErrorLogOptions(CopyState cstate, bool isRejectLimitSpecified)
{
    if ((!cstate->log_errors || !cstate->logErrorsData) && !isRejectLimitSpecified) {
        return;
    }

    if (isRejectLimitSpecified && (cstate->reject_limit == 0 || cstate->reject_limit < REJECT_UNLIMITED))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PER NODE REJECT LIMIT must be greater than 0")));

    if ((cstate->log_errors || cstate->logErrorsData || isRejectLimitSpecified) && !cstate->is_from) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Reject Limit , LOG ERRORS or LOG ERRORS DATA can only be used in COPY From Statements.")));
    }
    if (!cstate->log_errors && !cstate->logErrorsData && isRejectLimitSpecified) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Reject Limit must be used with LOG ERRORS or LOG ERRORS DATA.")));
    }

    if ((cstate->log_errors || cstate->logErrorsData) && !isRejectLimitSpecified) {
        cstate->reject_limit = 0;
        ereport(NOTICE,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Reject Limit was not set for LOG ERRORS or LOG ERRORS DATA. '0' is used as default.")));
    }
}

/*
 *	Log_copy_error_spi is used to insert the error record into copy_error_log table with SPI
 *	APIs. Xids, execution, and rollback are dealt and guaranteed by SPI state machine.
 */
void Log_copy_error_spi(CopyState cstate)
{
    CopyError err;
    Oid argtypes[] = {VARCHAROID, TIMESTAMPTZOID, VARCHAROID, INT8OID, TEXTOID, TEXTOID};
    /* for copy from load. */
    if (cstate->is_from && cstate->is_load_copy) {
        return LogCopyErrorLogBulk(cstate);
    }

    Assert (RelationGetNumberOfAttributes(cstate->err_table) == CopyError::MaxNumOfValue);
    err.m_desc = RelationGetDescr(cstate->err_table);

    /* Reset the offset of the logger. Read from 0. */
    cstate->logger->Reset();

    int ret;
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((ret = SPI_connect()) < 0) {
        /* internal error */
        ereport(ERROR,
            (errcode(ERRCODE_SPI_CONNECTION_FAILURE),
            errmsg("SPI connect failure - returned %d", ret)));
    }

    /* Read each line from the cache file and assemble the query string for spi to execute */
    while (cstate->logger->FetchError(&err) != EOF) {
        /* note* values params must start from $1 not $0 */
        if (err.m_values[err.RawDataIdx] == 0) {
            err.m_values[err.RawDataIdx] = CStringGetTextDatum("");
        }
        char *querystr = "insert into public.pgxc_copy_error_log values($1,$2,$3,$4,$5,$6);";
        ret = SPI_execute_with_args(querystr, err.MaxNumOfValue, argtypes, err.m_values, NULL,
                                    false, 0, NULL);
        if (ret != SPI_OK_INSERT) {
            ereport(ERROR,
                (errcode(ERRCODE_SPI_EXECUTE_FAILURE),
                    errmsg("Error encountered while logging error into %s.%s.",
                        COPY_ERROR_TABLE_SCHEMA,
                        COPY_ERROR_TABLE)));
        }
    }

    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();
}

/*
 *	Log_copy_summary_spi is used to insert the summary record into copy_summary table with SPI
 *	APIs. Xids, execution, and rollback are dealt and guaranteed by SPI state machine.
 */
static void Log_copy_summary_spi(CopyState cstate)
{
    const int MAXNAMELEN = (NAMEDATALEN * 4 + 2);
    const int MAXDETAIL = 512;
    Datum values[COPY_SUMMARY_TABLE_NUM_COL];
    char relname[MAXNAMELEN] = {'\0'};
    char detail[MAXDETAIL] = {'\0'};
    errno_t rc;
    int ret;
    if (cstate->summary_table == NULL) {
        return;
    }

    if ((ret = SPI_connect()) < 0) {
        /* internal error */
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI connect failure - returned %d", ret)));
    }

    rc = snprintf_s(relname, MAXNAMELEN, MAXNAMELEN - 1, "%s.%s", get_namespace_name(RelationGetNamespace(cstate->rel)),
                    RelationGetRelationName(cstate->rel));
    securec_check_ss(rc, "\0", "\0");

    values[0] = PointerGetDatum(cstring_to_text_with_len(relname, strlen(relname)));

    /* save the begintime/endtime/rows  */
    values[1] = cstate->copy_beginTime;
    values[2] = TimestampTzGetDatum(GetCurrentTimestamp());
    values[3] = Int64GetDatum(GetTopTransactionId());
    values[4] = Int64GetDatum(t_thrd.proc_cxt.MyProcPid);
    /* processed rows */
    values[5] = Int64GetDatum(cstate->cur_lineno-1);
    values[6] = Int64GetDatum(cstate->skiprows);
    values[7] = Int64GetDatum(cstate->loadrows);
    values[8] = Int64GetDatum(cstate->errorrows);
    values[9] = Int64GetDatum(cstate->whenrows);
    values[10] = Int64GetDatum(cstate->allnullrows);
    /* save the detail here  */
    snprintf_s(detail, MAXDETAIL, MAXDETAIL - 1,
               "%lld Rows successfully loaded.\n"
               "%lld Rows not loaded due to data errors.\n"
               "%lld Rows not loaded because all WHEN clauses were failed.\n"
               "%lld Rows not loaded because all fields were null.\n",
               cstate->loadrows, cstate->errorrows, cstate->whenrows, cstate->allnullrows);

    values[11] = PointerGetDatum(cstring_to_text_with_len(detail, strlen(detail)));

    char *querystr = "insert into public.gs_copy_summary values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);";
    ret = SPI_execute_with_args(querystr, COPY_SUMMARY_TABLE_NUM_COL, copy_summary_table_col_typid, values, NULL, false,
                                0, NULL);
    if (ret != SPI_OK_INSERT) {
        ereport(ERROR,
                (errcode(ERRCODE_SPI_EXECUTE_FAILURE), errmsg("Error encountered while logging summary into %s.%s.",
                                                              COPY_SUMMARY_TABLE_SCHEMA, COPY_SUMMARY_TABLE)));
    }

    SPI_finish();

    relation_close(cstate->summary_table, RowExclusiveLock);
    cstate->summary_table = NULL;
}

void CheckCopyWhenExprOptions(LoadWhenExpr *when)
{
    if (when == NULL) {
        return;
    }
    if (when->whentype == 0){
        if (when->start <= 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN start position %d should be > 0", when->start)));
        }
        if (when->end <= 0) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN end position %d should be > 0", when->end)));
        }
        if (when->end < when->start) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("WHEN start position %d should be <= end position %d", when->start, when->end)));
        }
    } else {
        if (when->attname == NULL) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field name is null")));
        }
        if (when->oper == NULL) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field oper name is null")));
        }
    }
    return;
}

static void CheckCopyWhenList(List *when_list)
{
    ListCell* lc = NULL;

    foreach (lc, when_list) {
        LoadWhenExpr *when = (LoadWhenExpr *)lfirst(lc);
        CheckCopyWhenExprOptions(when);
    }
}

static int whenpostion_strcmp(const char *arg1, int len1, const char *arg2, int len2)
{
    int result;
    result = memcmp(arg1, arg2, Min(len1, len2));
    if ((result == 0) && (len1 != len2)) {
        result = (len1 < len2) ? -1 : 1;
    }
    return result;
}

static bool IfCopyLineMatchWhenPositionExpr(CopyState cstate, LoadWhenExpr *when)
{
    StringInfo buf =  &cstate->line_buf;

    if (when == NULL || when->whentype == 1) {
        return true;
    }
    if (buf->len < when->start || buf->len < when->end) {
        return false;
    }
    if (when->val == NULL)
        return false;

    char *rawfield = buf->data + when->start - 1;
    int rawfieldlen = when->end + 1 - when->start;

    if (strlen(when->oper) == 1) {
        if (strncmp(when->oper, "=", 1) == 0)
            return (whenpostion_strcmp(rawfield, rawfieldlen, when->val, strlen(when->val)) == 0);
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN oper error")));
        }
    } else if (strlen(when->oper) == 2) {
        if (strncmp(when->oper, "<>", 2) == 0)
            return (whenpostion_strcmp(rawfield, rawfieldlen, when->val, strlen(when->val)) != 0);
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN oper error")));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN oper error")));
    }
    return false;
}

static void CopyGetWhenExprAttFieldno(CopyState cstate, LoadWhenExpr *when, List *attnamelist)
{
    TupleDesc tupDesc;
    if (when == NULL || when->whentype != 1) {
        return;
    }
    if (when->attname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("WHEN no field name")));
    }

    if (attnamelist == NULL) {
        if (cstate->rel == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("WHEN no relation")));
        }
        tupDesc = RelationGetDescr(cstate->rel);
        for (int i = 0; i < tupDesc->natts; i++) {
            if (tupDesc->attrs[i]->attisdropped)
                continue;
            if (namestrcmp(&(tupDesc->attrs[i]->attname), when->attname) == 0) {
                when->attnum = tupDesc->attrs[i]->attnum; /* based 1 */
                return;
            }
        }
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("WHEN field name not find")));
    } else {
        ListCell *l = NULL;
        int attnum = 0;
        foreach (l, attnamelist) {
            attnum++;
            char *name = strVal(lfirst(l));
            if (strcmp(name, when->attname) == 0) {
                when->attnum = attnum; /* based 1 */
                return;
            }
        }
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("WHEN field name not find")));
    }
}

static bool IfCopyLineMatchWhenFieldExpr(CopyState cstate, LoadWhenExpr *when)
{
    if (when == NULL || when->whentype != 1) {
        return true;
    }
    if (when->attnum == 0) {  // assert
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field name not find")));
    }
    int attnum = when->attnum - 1;  // fields 0 based
    if (attnum >= cstate->max_fields) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field > max fields")));
    }
    if (when->val == NULL)
        return false;
    if (cstate->raw_fields[attnum] == NULL) {
        return false;
    }

    if (strlen(when->oper) == 1) {
        if (strncmp(when->oper, "=", 1) == 0)
            return (strcmp(cstate->raw_fields[attnum], when->val) == 0);
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field oper error")));
        }
    } else if (strlen(when->oper) == 2){
        if (strncmp(when->oper, "<>", 2) == 0)
            return (strcmp(cstate->raw_fields[attnum], when->val) != 0);
        else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field oper error")));
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("WHEN field oper error")));
    }
    return false;
}

static bool IfCopyLineMatchWhenListPosition(CopyState cstate)
{
    ListCell* lc = NULL;
    List *when_list = cstate->when;

    foreach (lc, when_list) {
        LoadWhenExpr *when = (LoadWhenExpr *)lfirst(lc);
        if (IfCopyLineMatchWhenPositionExpr(cstate, when) == false) {
            return false;
        }
    }
    return true;
}

static void CopyGetWhenListAttFieldno(CopyState cstate, List *attnamelist)
{
    ListCell* lc = NULL;
    List *when_list = cstate->when;

    foreach (lc, when_list) {
        LoadWhenExpr *when = (LoadWhenExpr *)lfirst(lc);
        CopyGetWhenExprAttFieldno(cstate, when, attnamelist);
    }
    return;
}

static bool IfCopyLineMatchWhenListField(CopyState cstate)
{
    ListCell* lc = NULL;
    List *when_list = cstate->when;

    foreach (lc, when_list) {
        LoadWhenExpr *when = (LoadWhenExpr *)lfirst(lc);
        if (IfCopyLineMatchWhenFieldExpr(cstate, when) == false) {
            return false;
        }
    }
    return true;
}

static int CopyGetColumnListIndex(CopyState cstate, List *attnamelist, const char* colname)
{
    TupleDesc tupDesc = NULL;
    ListCell *col = NULL;
    int index = 0;
    if (colname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("Column name is NULL")));
    }

    if (attnamelist == NULL) {
        if (cstate->rel == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("Column list no relation")));
        }
        tupDesc = RelationGetDescr(cstate->rel);
        for (int i = 0; i < tupDesc->natts; i++) {
            if (tupDesc->attrs[i]->attisdropped)
                continue;
            if (namestrcmp(&(tupDesc->attrs[i]->attname), colname) == 0) {
                return tupDesc->attrs[i]->attnum; /* based 1 */
            }
        }
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("Column name %s not find", colname)));
    } else {
        foreach (col, attnamelist) {
            index++;
            char *name = strVal(lfirst(col));
            if (strcmp(name, colname) == 0) {
                return index;
            }
        }
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE), errmsg("Column name %s not find", colname)));
    }

    /* on failure */
    return InvalidAttrNumber;
}

static void BatchInsertCopyLog(LogInsertState copyLogInfo, int nBufferedTuples, HeapTuple *bufferedTuples)
{
    MemoryContext oldcontext;
    HeapMultiInsertExtraArgs args = {NULL, 0, false};
    TupleTableSlot *myslot = copyLogInfo->myslot;

    /*
     * heap_multi_insert leaks memory, so switch memory context
     */
    oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(copyLogInfo->estate));
    (void)tableam_tuple_multi_insert(copyLogInfo->rel, copyLogInfo->resultRelInfo->ri_RelationDesc,
                                    (Tuple *)bufferedTuples, nBufferedTuples, copyLogInfo->mycid,
                                    copyLogInfo->insertOpt, copyLogInfo->bistate, &args);
    MemoryContextSwitchTo(oldcontext);

    if (copyLogInfo->resultRelInfo->ri_NumIndices > 0) {
        int i;
        for (i = 0; i < nBufferedTuples; i++) {
            List *recheckIndexes = NULL;

            (void)ExecStoreTuple(bufferedTuples[i], myslot, InvalidBuffer, false);

            recheckIndexes = ExecInsertIndexTuples(myslot, &(bufferedTuples[i]->t_self), copyLogInfo->estate,
                                                    NULL, NULL, InvalidBktId, NULL, NULL);

            list_free(recheckIndexes);
        }
    }

    return;
}

static LogInsertState InitInsertCopyLogInfo(CopyState cstate)
{
    LogInsertState copyLogInfo = NULL;
    ResultRelInfo *resultRelInfo = NULL;
    EState *estate = NULL;

    copyLogInfo = (LogInsertState)palloc(sizeof(InsertCopyLogInfoData));
    copyLogInfo->rel = cstate->err_table;
    copyLogInfo->insertOpt= TABLE_INSERT_FROZEN;
    copyLogInfo->mycid = GetCurrentCommandId(true);

    resultRelInfo = makeNode(ResultRelInfo);
    resultRelInfo->ri_RangeTableIndex = 1;
    resultRelInfo->ri_RelationDesc = copyLogInfo->rel;
    ExecOpenIndices(resultRelInfo, false);
    copyLogInfo->resultRelInfo = resultRelInfo;

    estate= CreateExecutorState();
    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;
    copyLogInfo->estate = estate;

    copyLogInfo->myslot = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(copyLogInfo->myslot, RelationGetDescr(copyLogInfo->rel));

    copyLogInfo->bistate = GetBulkInsertState();

    return copyLogInfo;
}

static void FreeInsertCopyLogInfo(LogInsertState copyLogInfo)
{
    if (copyLogInfo == NULL) {
        return;
    }

    FreeBulkInsertState(copyLogInfo->bistate);
    ExecResetTupleTable(copyLogInfo->estate->es_tupleTable, false);
    ExecCloseIndices(copyLogInfo->resultRelInfo);
    FreeExecutorState(copyLogInfo->estate);
    pfree(copyLogInfo->resultRelInfo);
    pfree(copyLogInfo);
}

static void LogCopyErrorLogBulk(CopyState cstate)
{
    const int MAX_TUPLES = 10000;
    const int MAX_TUPLES_BYTES = 1024 * 1024;
    HeapTuple tuple;
    MemoryContext oldcontext = CurrentMemoryContext;
    int nBufferedTuples = 0;
    HeapTuple *bufferedTuples = NULL;
    Size bufferedTuplesSize = 0;
    LogInsertState copyLogInfo = NULL;
    CopyError err;

    if (cstate->err_table == NULL)
        return;
    Assert(RelationGetNumberOfAttributes(cstate->err_table) == CopyError::MaxNumOfValue);
    err.m_desc = RelationGetDescr(cstate->err_table);

    /* Reset the offset of the logger. Read from 0. */
    cstate->logger->Reset();

    copyLogInfo = InitInsertCopyLogInfo(cstate);
    bufferedTuples = (HeapTuple *)palloc0(MAX_TUPLES * sizeof(HeapTuple));

    for (;;) {
        CHECK_FOR_INTERRUPTS();
        if (nBufferedTuples == 0) {
            ResetPerTupleExprContext(copyLogInfo->estate);
        }

        MemoryContextSwitchTo(GetPerTupleMemoryContext(copyLogInfo->estate));

        if (cstate->logger->FetchError(&err) == EOF) {
            break;
        }

        tuple = (HeapTuple)heap_form_tuple(err.m_desc, err.m_values, err.m_isNull);
        ExecStoreTuple(tuple, copyLogInfo->myslot, InvalidBuffer, false);

        bufferedTuples[nBufferedTuples++] = tuple;
        bufferedTuplesSize += tuple->t_len;

        if (nBufferedTuples == MAX_TUPLES || bufferedTuplesSize > MAX_TUPLES_BYTES) {
            BatchInsertCopyLog(copyLogInfo, nBufferedTuples, bufferedTuples);
            nBufferedTuples = 0;
            bufferedTuplesSize = 0;
        }
    }

    if (nBufferedTuples > 0) {
        BatchInsertCopyLog(copyLogInfo, nBufferedTuples, bufferedTuples);
    }

    MemoryContextSwitchTo(oldcontext);
    pfree(bufferedTuples);
    FreeInsertCopyLogInfo(copyLogInfo);
    return;
}
