/* ---------------------------------------------------------------------------------------
 * *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * gs_xlogdump.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/storage/page/gs_xlogdump.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include "c.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "access/xlog_basic.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "access/xlog_internal.h"
#include "catalog/catalog.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "postgres.h"
#include "storage/smgr/relfilenode.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "pageparse.h"

typedef struct XLogPrivate {
    const char *datadir;
    TimeLineID tli;
} XLogPrivate;

typedef struct XLogFilter {
    TransactionId by_xid;
    bool by_xid_enabled;
    bool by_tablepath_enabled;
    bool by_block;
    RelFileNode by_relfilenode;
    BlockNumber blocknum;
} XLogFilter;

static void GenerateOutputFileName(char *outputFilename, char *start_lsn_str, char *end_lsn_str)
{
    List *elemlist = NIL;
    SplitIdentifierString(start_lsn_str, '/', &elemlist);
    char *start_lsn_str_p2 = (char *)lsecond(elemlist);
    list_free_ext(elemlist);
    SplitIdentifierString(end_lsn_str, '/', &elemlist);
    char *end_lsn_str_p2 = (char *)lsecond(elemlist);
    int rc = snprintf_s(outputFilename + (int)strlen(outputFilename), MAXFILENAME, MAXFILENAME - 1, "%s/%s_%s.xlog",
        t_thrd.proc_cxt.DataDir, start_lsn_str_p2, end_lsn_str_p2);
    securec_check_ss(rc, "\0", "\0");
}

static void ValidateLSN(char *lsn_str, XLogRecPtr *lsn_ptr)
{
    uint32 hi = 0;
    uint32 lo = 0;
    validate_xlog_location(lsn_str);

    if (sscanf_s(lsn_str, "%X/%X", &hi, &lo) != TWO)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not parse xlog location \"%s\"", lsn_str)));
    *lsn_ptr = (((uint64)hi) << XIDTHIRTYTWO) | lo;
}

static void ValidateStartEndLSN(char *start_lsn_str, char *end_lsn_str, XLogRecPtr *start_lsn, XLogRecPtr *end_lsn)
{
    ValidateLSN(start_lsn_str, start_lsn);
    ValidateLSN(end_lsn_str, end_lsn);

    if (XLByteLT(*end_lsn, *start_lsn))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("start xlog location %X/%X should be smaller than or equal to end xlog location %X/%X",
            (uint32)(*start_lsn >> XIDTHIRTYTWO), (uint32)(*start_lsn), (uint32)(*end_lsn >> XIDTHIRTYTWO),
            (uint32)(*end_lsn))));
}

static XLogRecPtr GetMinLSN()
{
    XLogSegNo lastRemovedSegNo = XLogGetLastRemovedSegno();
    XLogRecPtr current_recptr = (lastRemovedSegNo + 1) * XLogSegSize;
    return current_recptr;
}

static XLogRecPtr GetMaxLSN()
{
    if (RecoveryInProgress())
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
            errhint("Can't get local max LSN during recovery.")));
    XLogRecPtr current_recptr = GetXLogWriteRecPtr();
    return current_recptr;
}

void XLogDumpDisplayRecord(XLogReaderState *record, char *strOutput)
{
    errno_t rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "start_lsn: %X/%X \nend_lsn: %X/%X \nxid: " XID_FMT " \nterm: %u \ntotal length: %u \ndesc: %s - ",
        (uint32)(record->ReadRecPtr >> XIDTHIRTYTWO), (uint32)record->ReadRecPtr,
        (uint32)(record->EndRecPtr >> XIDTHIRTYTWO), (uint32)record->EndRecPtr, XLogRecGetXid(record),
        XLogRecGetTerm(record), XLogRecGetTotalLen(record), RmgrTable[XLogRecGetRmid(record)].rm_name);
    securec_check_ss(rc, "\0", "\0");
    StringInfoData buf;
    initStringInfo(&buf);
    RmgrTable[XLogRecGetRmid(record)].rm_desc(&buf, record);
    rc = strcat_s(strOutput, MAXOUTPUTLEN, buf.data);
    securec_check(rc, "\0", "\0");
    pfree_ext(buf.data);
    if (!XLogRecHasAnyBlockRefs(record)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\n");
        securec_check(rc, "\0", "\0");
        return;
    }

    if (record->max_block_id >= 0) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "\nothers: ");
        securec_check(rc, "\0", "\0");
    }

    for (int block_id = 0; block_id <= record->max_block_id; block_id++) {
        if (!XLogRecHasBlockRef(record, block_id))
            continue;
        /* Print format: others: rel %u/%u/%u/%d fork %s blk %u */
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blk;
        if (!XLogRecGetBlockTag(record, block_id, &rnode, &forknum, &blk))
            continue;
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\trel %u/%u/%u",
            rnode.spcNode, rnode.dbNode, rnode.relNode);
        securec_check_ss(rc, "\0", "\0");
        if (IsBucketFileNode(rnode)) { /* check between InvalidBktId and SegmentBktId */
            rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                "/%d", rnode.bucketNode);
            securec_check_ss(rc, "\0", "\0");
        }
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, ", fork %s",
            forkNames[forknum]);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, ", blk %u", blk);
        securec_check_ss(rc, "\0", "\0");
        /* others: lastlsn %X/%X" */
        XLogRecPtr lsn;
        XLogRecGetBlockLastLsn(record, block_id, &lsn);
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, ", lastlsn %X/%X",
            (uint32)(lsn >> XIDTHIRTYTWO), (uint32)lsn);
        securec_check_ss(rc, "\0", "\0");
    }
    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\n");
    securec_check(rc, "\0", "\0");
}

void CheckOpenFile(FILE *outputfile, char *outputFilename)
{
    if (outputfile == NULL)
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED), (errmsg("Cannot read %s", outputFilename))));
}

void CheckWriteFile(FILE *outputfile, char *outputFilename, char *strOutput)
{
    uint result = fwrite(strOutput, 1, strlen(strOutput), outputfile);
    if (result != strlen(strOutput)) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR, (errcode(ERRCODE_FILE_WRITE_FAILED), (errmsg("Cannot write %s", outputFilename))));
    }
}

void CheckCloseFile(FILE *outputfile, char *outputFilename, bool is_error)
{
    if (0 != fclose(outputfile)) {
        if (is_error)
            ereport(ERROR, (errcode(ERRCODE_IO_ERROR), (errmsg("Cannot close %s", outputFilename))));
        else
            ereport(WARNING, (errcode(ERRCODE_IO_ERROR), (errmsg("Cannot close %s", outputFilename))));
    }
}

static bool CheckValidRecord(XLogReaderState *xlogreader_state, XLogFilter *filter)
{
    bool found = false;
    for (int i = 0; i <= xlogreader_state->max_block_id; i++) {
        RelFileNode rnode;
        ForkNumber forknum;
        BlockNumber blk;
        if (!XLogRecGetBlockTag(xlogreader_state, i, &rnode, &forknum, &blk))
            continue;
        if (RelFileNodeEquals(rnode, filter->by_relfilenode))
            /* if equal to specific block or check all blocks, found = ture; */
            found = (!filter->by_block) || (blk == filter->blocknum);
        if (found) {
            return found;
        }
    }
    return found;
}

XLogRecPtr UpdateNextLSN(XLogRecPtr cur_lsn, XLogRecPtr end_lsn, XLogReaderState *xlogreader_state, bool *found)
{
    XLogRecPtr next_record = InvalidXLogRecPtr;
    for (int tryTimes = 0; tryTimes < FIVE; tryTimes++) {
        XLogRecPtr start_lsn = Max(cur_lsn, (g_instance.comm_cxt.predo_cxt.redoPf.oldest_segment) * XLogSegSize);
        next_record = XLogFindNextRecord(xlogreader_state, start_lsn);
        if (!XLByteEQ(next_record, InvalidXLogRecPtr) && XLByteLT(next_record, end_lsn)) {
            *found = true;
            return next_record;
        }
    }
    return next_record;
}

static void XLogDump(XLogRecPtr start_lsn, XLogRecPtr end_lsn, XLogFilter *filter, char *outputFilename)
{
    /* start reading */
    errno_t rc = EOK;
    XLogPrivate readprivate;
    rc = memset_s(&readprivate, sizeof(XLogPrivate), 0, sizeof(XLogPrivate));
    securec_check_c(rc, "\0", "\0");
    readprivate.datadir = t_thrd.proc_cxt.DataDir;
    readprivate.tli = 1;
    XLogReaderState *xlogreader_state = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (!xlogreader_state)
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
            (errmsg("memory is temporarily unavailable while allocate xlog reader"))));

    /* get the first valid xlog record location */
    XLogRecPtr first_record = XLogFindNextRecord(xlogreader_state, start_lsn);
    /* if we are recycling or removing log files concurrently, we can't find the next record right after.
     * Hence, we need to update the min_lsn */
    if (XLByteEQ(first_record, InvalidXLogRecPtr)) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("XLogFindNextRecord: could not find a valid record after %X/%X. Retry.",
                (uint32)(start_lsn >> XIDTHIRTYTWO), (uint32)start_lsn))));
        bool found = false;
        first_record = UpdateNextLSN(start_lsn, end_lsn, xlogreader_state, &found);
        if (!found)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    (errmsg("XLogFindNextRecord: could not find a valid record between %X/%X and %X/%X.",
                        (uint32)(start_lsn >> XIDTHIRTYTWO), (uint32)start_lsn,
                        (uint32)(end_lsn >> XIDTHIRTYTWO), (uint32)end_lsn))));
    }

    XLogRecPtr valid_start_lsn = first_record;
    XLogRecPtr valid_end_lsn = valid_start_lsn;

    FILE *outputfile = fopen(outputFilename, "w");
    CheckOpenFile(outputfile, outputFilename);
    char *strOutput = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
    rc = memset_s(strOutput, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
    securec_check(rc, "\0", "\0");
    /* valid first record is not the given one */
    if (!XLByteEQ(first_record, start_lsn) && (start_lsn % XLogSegSize) != 0) {
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
            "first record is after %X/%X, at %X/%X, skipping over %lu bytes\n", (uint32)(start_lsn >> XIDTHIRTYTWO),
            (uint32)start_lsn, (uint32)(first_record >> XIDTHIRTYTWO), (uint32)first_record,
            XLByteDifference(first_record, start_lsn));
        securec_check_ss(rc, "\0", "\0");
    }
    CheckWriteFile(outputfile, outputFilename, strOutput);
    pfree_ext(strOutput);

    int count = 0;
    char *errormsg = NULL;
    XLogRecord *record = NULL;
    while (XLByteLT(xlogreader_state->EndRecPtr, end_lsn)) {
        CHECK_FOR_INTERRUPTS(); /* Allow cancel/die interrupts */
        record = XLogReadRecord(xlogreader_state, first_record, &errormsg);
        valid_end_lsn = xlogreader_state->EndRecPtr;
        if (!record && XLByteLT(valid_end_lsn, end_lsn)) {
            /* if we are recycling or removing log files concurrently, and we can't find the next record right after.
             * In this case, we try to read from the current oldest xlog file. */
            bool found = false;
            XLogRecPtr temp_start_lsn = Max(xlogreader_state->EndRecPtr, start_lsn);
            first_record = UpdateNextLSN(temp_start_lsn, end_lsn, xlogreader_state, &found);
            if (found) {
                ereport(WARNING, (errcode(ERRCODE_WARNING),
                (errmsg("We cannot read %X/%X. After retried, we jump to read the next available %X/%X. " \
                    "The missing part might be recycled or removed.",
                    (uint32)(temp_start_lsn >> XIDTHIRTYTWO), (uint32)temp_start_lsn,
                    (uint32)(first_record >> XIDTHIRTYTWO), (uint32)first_record))));
                continue;
            }

            if (errormsg != NULL)
                ereport(LOG,
                    (errcode(ERRCODE_LOG),
                        (errmsg("could not read WAL record at %X/%X: %s",
                            (uint32)(xlogreader_state->ReadRecPtr >> XIDTHIRTYTWO),
                            (uint32)xlogreader_state->ReadRecPtr,
                            errormsg))));
            else
                ereport(LOG,
                    (errcode(ERRCODE_LOG),
                        (errmsg("could not read WAL record at %X/%X",
                            (uint32)(xlogreader_state->ReadRecPtr >> XIDTHIRTYTWO),
                            (uint32)xlogreader_state->ReadRecPtr))));
            break;
        }
        first_record = InvalidXLogRecPtr; /* No explicit start point; read the record after the one we just read */
        if (filter->by_xid_enabled && filter->by_xid != record->xl_xid) {
            continue;
        }

        if (filter->by_tablepath_enabled) { /* filter by table path */
            if (!XLogRecHasAnyBlockRefs(xlogreader_state)) {
                continue;
            } else if (!CheckValidRecord(xlogreader_state, filter)) {
                /* at least have one block ref, but not match filter */
                continue;
            }
        }
        strOutput = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
        rc = memset_s(strOutput, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
        securec_check(rc, "\0", "\0");

        XLogDumpDisplayRecord(xlogreader_state, strOutput);
        count++;

        CheckWriteFile(outputfile, outputFilename, strOutput);
        pfree_ext(strOutput);
    }

    XLogReaderFree(xlogreader_state);
    strOutput = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
    rc = memset_s(strOutput, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
    securec_check(rc, "\0", "\0");

    /* Summary(xx total): valid start_lsn: xxx, valid end_lsn: xxx */
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\nSummary (%d total): valid start_lsn: %X/%X, valid end_lsn: %X/%X\n", count,
        (uint32)(valid_start_lsn >> XIDTHIRTYTWO), (uint32)(valid_start_lsn), (uint32)(valid_end_lsn >> XIDTHIRTYTWO),
        (uint32)(valid_end_lsn));
    securec_check_ss(rc, "\0", "\0");
    /* generate output file */
    CheckWriteFile(outputfile, outputFilename, strOutput);
    CheckCloseFile(outputfile, outputFilename, true);
    pfree_ext(strOutput);
    CloseXlogFile();
}

/* There are only two parameters in PG_FUNCTION_ARGS: start_lsn and end_lsn */
Datum gs_xlogdump_lsn(PG_FUNCTION_ARGS)
{
    if (ENABLE_DSS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported gs_xlogdump_lsn when enable dss.")));
        PG_RETURN_VOID();
    }

    errno_t rc = EOK;
    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_xlogdump_lsn";
    CheckUser(fName);

    /* read in parameters */
    char *start_lsn_str = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *end_lsn_str = text_to_cstring(PG_GETARG_TEXT_P(1));

    /* validate lsn */
    XLogRecPtr start_lsn, end_lsn;
    ValidateStartEndLSN(start_lsn_str, end_lsn_str, &start_lsn, &end_lsn);

    char *outputFilename = (char *)palloc(MAXFILENAME * sizeof(char));
    rc = memset_s(outputFilename, MAXFILENAME, 0, MAXFILENAME);
    securec_check(rc, "\0", "\0");
    GenerateOutputFileName(outputFilename, start_lsn_str, end_lsn_str);

    /* update start_lsn and end_lsn based on cur min_lsn and max_lsn */
    XLogRecPtr min_lsn = GetMinLSN();
    XLogRecPtr max_lsn = GetMaxLSN();

    if (XLByteLT(start_lsn, min_lsn)) {
        start_lsn = min_lsn;
    }
    if (XLByteLT(max_lsn, end_lsn)) {
        end_lsn = max_lsn;
    }

    XLogFilter filter;
    rc = memset_s(&filter, sizeof(XLogFilter), 0, sizeof(XLogFilter));
    securec_check_ss(rc, "\0", "\0");
    XLogDump(start_lsn, end_lsn, &filter, outputFilename);
    PG_RETURN_TEXT_P(cstring_to_text(outputFilename));
}

/* There are only one parameter in PG_FUNCTION_ARGS: c_xid */
Datum gs_xlogdump_xid(PG_FUNCTION_ARGS)
{
    if (ENABLE_DSS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported gs_xlogdump_xid when enable dss.")));
        PG_RETURN_VOID();
    }

    errno_t rc = EOK;
    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_xlogdump_xid";
    CheckUser(fName);

    /* read in parameters */
    TransactionId c_xid = PG_GETARG_TRANSACTIONID(0);
    /* check parameters */
    TransactionId topXid = GetTopTransactionId();
    if (TransactionIdPrecedes(topXid, c_xid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("Xid should less than or equal to %lu.", topXid))));

    /* generate output file name */
    char *outputFilename = (char *)palloc(MAXFILENAME * sizeof(char));
    rc = memset_s(outputFilename, MAXFILENAME, 0, MAXFILENAME);
    securec_check(rc, "\0", "\0");
    rc = snprintf_s(outputFilename + (int)strlen(outputFilename), MAXFILENAME, MAXFILENAME - 1, "%s/%lu.xlog",
        t_thrd.proc_cxt.DataDir, c_xid);
    securec_check_ss(rc, "\0", "\0");
    /* update start_lsn and end_lsn based on cur min_lsn and max_lsn */
    XLogRecPtr min_lsn = GetMinLSN();
    XLogRecPtr max_lsn = GetMaxLSN();
    XLogFilter filter;
    rc = memset_s(&filter, sizeof(XLogFilter), 0, sizeof(XLogFilter));
    securec_check(rc, "\0", "\0");
    filter.by_xid_enabled = true;
    filter.by_xid = c_xid;
    XLogDump(min_lsn, max_lsn, &filter, outputFilename);
    PG_RETURN_TEXT_P(cstring_to_text(outputFilename));
}

/* There are only three parameters in PG_FUNCTION_ARGS: path, blocknum, relation_type */
Datum gs_xlogdump_tablepath(PG_FUNCTION_ARGS)
{
    if (ENABLE_DSS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported gs_xlogdump_tablepath when enable dss.")));
        PG_RETURN_VOID();
    }

    errno_t rc = EOK;
    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_xlogdump_tablepath";
    CheckUser(fName);

    /* read in parameters */
    char *path = text_to_cstring(PG_GETARG_TEXT_P(0));
    int64 blocknum = PG_GETARG_INT64(1);
    char *relation_type = text_to_cstring(PG_GETARG_TEXT_P(2));
    /* check parameters */
    if (blocknum > MaxBlockNumber || blocknum < -1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Blocknum should be between -1 and %u.", MaxBlockNumber))));

    /* update start_lsn and end_lsn based on cur min_lsn and max_lsn */
    XLogRecPtr min_lsn = GetMinLSN();
    XLogRecPtr max_lsn = GetMaxLSN();
    XLogFilter filter;
    rc = memset_s(&filter, sizeof(XLogFilter), 0, sizeof(XLogFilter));
    securec_check_c(rc, "\0", "\0");
    filter.by_tablepath_enabled = true;

    if (blocknum == -1) { /* care all blocks */
        filter.by_block = false;
    } else { /* only one block */
        filter.by_block = true;
        filter.blocknum = blocknum;
    }

    /* generate output file name */
    char *outputFilename = (char *)palloc(MAXFILENAME * sizeof(char));
    rc = memset_s(outputFilename, MAXFILENAME, 0, MAXFILENAME);
    securec_check(rc, "\0", "\0");
    PrepForRead(path, blocknum, relation_type, outputFilename, &(filter.by_relfilenode), false);
    ValidateParameterPath(filter.by_relfilenode, path);
    XLogDump(min_lsn, max_lsn, &filter, outputFilename);

    PG_RETURN_TEXT_P(cstring_to_text(outputFilename));
}

Datum gs_xlogdump_parsepage_tablepath(PG_FUNCTION_ARGS)
{
    if (ENABLE_DSS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("unsupported gs_xlogdump_parsepage_tablepath when enable dss.")));
        PG_RETURN_VOID();
    }

    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_xlogdump_parsepage_tablepath";
    CheckUser(fName);

    /* read in parameters */
    char *path = text_to_cstring(PG_GETARG_TEXT_P(0));
    int64 blocknum = PG_GETARG_INT64(1);
    char *relation_type = text_to_cstring(PG_GETARG_TEXT_P(2));
    bool read_memory = PG_GETARG_BOOL(3);
    int rc = -1;

    /* page part, copy path since for segment we will edit path */
    char *path_cpy = (char *)palloc(MAXFILENAME + 1);
    rc = strcpy_s(path_cpy, MAXFILENAME + 1, path);
    securec_check(rc, "\0", "\0");

    /* In order to avoid querying the shared buffer and applying LW locks, blocking the business. */
    /* In the case of finding all pages, force to check disk  */
    if (blocknum == -1) {
        read_memory = false;
    }
    char *outputFilenamePage = ParsePage(path_cpy, blocknum, relation_type, read_memory);
    pfree_ext(path_cpy);
    /* xlog part */
    /* update start_lsn and end_lsn based on cur min_lsn and max_lsn */
    XLogFilter filter;
    rc = memset_s(&filter, sizeof(XLogFilter), 0, sizeof(XLogFilter));
    securec_check_c(rc, "\0", "\0");

    if (blocknum == -1) { /* care all blocks */
        filter.by_block = false;
    } else { /* only one block */
        filter.by_block = true;
        filter.blocknum = blocknum;
    }
    filter.by_tablepath_enabled = true;
    /* generate output file name */
    char *outputFilename = (char *)palloc(MAXFILENAME * sizeof(char));
    rc = memset_s(outputFilename, MAXFILENAME, 0, MAXFILENAME);
    securec_check(rc, "\0", "\0");
    PrepForRead(path, blocknum, relation_type, outputFilename, &(filter.by_relfilenode), false);
    XLogRecPtr min_lsn = GetMinLSN();
    XLogRecPtr max_lsn = GetMaxLSN();
    XLogDump(min_lsn, max_lsn, &filter, outputFilename);

    int outputLen = strlen(outputFilename) + strlen(outputFilenamePage) + 100;
    if (outputLen <= 0)
        ereport(ERROR, (errcode(ERRCODE_IO_ERROR), (errmsg("Cannot generate right output file name."))));
    char *result = (char *)palloc(outputLen * sizeof(char));
    rc = memset_s(result, outputLen, 0, outputLen);
    securec_check(rc, "\0", "\0");

    rc = snprintf_s(result + (int)strlen(result), outputLen, outputLen - 1,
        "Output file for parsing xlog: %s\nOutput file for parsing data page: %s", outputFilename, outputFilenamePage);
    securec_check_ss(rc, "\0", "\0");
    pfree_ext(outputFilename);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}
