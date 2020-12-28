/* -------------------------------------------------------------------------
 *
 * xlogreader.cpp
 *		Generic XLog reading facility
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		src/gausskernel/storage/access/transam/xlogreader.cpp
 *
 * NOTES
 * 		See xlogreader.h for more notes on this facility.
 *
 *		This file is compiled as both front-end and backend code, so it
 *		may not use ereport, server-defined static variables, etc.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#ifdef FRONTEND
#include "common/fe_memutils.h"
#endif
#include "access/transam.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "securec_check.h"
#include "replication/logical.h"
#include "access/parallel_recovery/redo_item.h"
#include "utils/memutils.h"

typedef struct XLogPageReadPrivate {
    const char *datadir;
    TimeLineID tli;
} XLogPageReadPrivate;

#ifdef FRONTEND
static int xlogreadfd = -1;
static XLogSegNo xlogreadsegno = 0;
#else
static THR_LOCAL int xlogreadfd = -1;
static THR_LOCAL XLogSegNo xlogreadsegno = 0;
#endif

bool ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr, XLogPageHeader hdr, bool readoldversion);
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, bool readoldversion);
void ResetDecoder(XLogReaderState *state);

static inline void prepare_invalid_report(XLogReaderState *state, char *fname, const size_t fname_len,
                                          const XLogSegNo segno)
{
    errno_t errorno = memset_s(fname, fname_len, 0, fname_len);
    securec_check(errorno, "", "");
    errorno = snprintf_s(fname, fname_len, fname_len - 1, "%08X%08X%08X", state->readPageTLI,
                         (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
#ifndef FRONTEND
    securec_check_ss(errorno, "", "");
#else
    securec_check_ss_c(errorno, "", "");
#endif
}

/*
 * Construct a string in state->errormsg_buf explaining what's wrong with
 * the current record being read.
 */
void report_invalid_record(XLogReaderState *state, const char *fmt, ...)
{
    va_list args;
    int rc = 0;

    fmt = _(fmt);

    va_start(args, fmt);
    rc = vsnprintf_s(state->errormsg_buf, MAX_ERRORMSG_LEN, MAX_ERRORMSG_LEN - 1, fmt, args);
    va_end(args);

#ifndef FRONTEND
    securec_check_ss(rc, "", "");
#else
    securec_check_ss_c(rc, "", "");
#endif
}

/*
 * Allocate and initialize a new XLogReader.
 *
 * Returns NULL if the xlogreader couldn't be allocated.
 */
XLogReaderState *XLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data)
{
    XLogReaderState *state = NULL;
    errno_t errorno = EOK;

    state = (XLogReaderState *)palloc_extended(sizeof(XLogReaderState), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
    if (state == NULL)
        return NULL;
    errorno = memset_s(state, sizeof(XLogReaderState), 0, sizeof(XLogReaderState));
    securec_check(errorno, "", "");

    state->max_block_id = -1;
    state->isPRProcess = false;

    /*
     * Permanently allocate readBuf.  We do it this way, rather than just
     * making a static array, for two reasons: (1) no need to waste the
     * storage in most instantiations of the backend; (2) a static char array
     * isn't guaranteed to have any particular alignment, whereas malloc()
     * will provide MAXALIGN'd storage.
     */
    state->readBuf = (char *)palloc_extended(XLOG_BLCKSZ, MCXT_ALLOC_NO_OOM);
    if (state->readBuf == NULL) {
        pfree(state);
        state = NULL;
        return NULL;
    }

    state->read_page = pagereadfunc;
    /* system_identifier initialized to zeroes above */
    state->private_data = private_data;
    /* ReadRecPtr and EndRecPtr initialized to zeroes above */
    /* readSegNo, readOff, readLen, readPageTLI initialized to zeroes above */
    state->errormsg_buf = (char *)palloc_extended(MAX_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (state->errormsg_buf == NULL) {
        pfree(state->readBuf);
        state->readBuf = NULL;
        pfree(state);
        state = NULL;
        return NULL;
    }
    state->errormsg_buf[0] = '\0';

    /*
     * Allocate an initial readRecordBuf of minimal size, which can later be
     * enlarged if necessary.
     */
    if (!allocate_recordbuf(state, 0)) {
        pfree(state->errormsg_buf);
        state->errormsg_buf = NULL;
        pfree(state->readBuf);
        state->readBuf = NULL;
        pfree(state);
        state = NULL;
        return NULL;
    }

    return state;
}

/*
 * Free the xlog reader memory.
 */
void XLogReaderFree(XLogReaderState *state)
{
    int block_id;

    for (block_id = 0; block_id <= XLR_MAX_BLOCK_ID; block_id++) {
        if (state->blocks[block_id].data) {
            pfree(state->blocks[block_id].data);
            state->blocks[block_id].data = NULL;
        }
    }
    if (state->main_data) {
        pfree(state->main_data);
        state->main_data = NULL;
    }

    pfree(state->errormsg_buf);
    state->errormsg_buf = NULL;
    if (state->readRecordBuf) {
        pfree(state->readRecordBuf);
        state->readRecordBuf = NULL;
    }
    pfree(state->readBuf);
    state->readBuf = NULL;

    /* state need to be reset NULL by caller */
    pfree(state);
}

/*
 * Allocate readRecordBuf to fit a record of at least the given length.
 * Returns true if successful, false if out of memory.
 *
 * readRecordBufSize is set to the new buffer size.
 *
 * To avoid useless small increases, round its size to a multiple of
 * XLOG_BLCKSZ, and make sure it's at least 5*Max(BLCKSZ, XLOG_BLCKSZ) to start
 * with.  (That is enough for all "normal" records, but very large commit or
 * abort records might need more space.)
 */
bool allocate_recordbuf(XLogReaderState *state, uint32 reclength)
{
    uint32 newSize = reclength;

    newSize += XLOG_BLCKSZ - (newSize % XLOG_BLCKSZ);
    newSize = state->isPRProcess ? Max(newSize, 512) : Max(newSize, 5 * Max(BLCKSZ, XLOG_BLCKSZ));

    if (state->readRecordBuf != NULL) {
        pfree(state->readRecordBuf);
        state->readRecordBuf = NULL;
    }
    state->readRecordBuf = (char *)palloc_extended(newSize, MCXT_ALLOC_NO_OOM);
    if (state->readRecordBuf == NULL) {
        state->readRecordBufSize = 0;
        return false;
    }

    state->readRecordBufSize = newSize;
    return true;
}

/*
 * Attempt to read an XLOG record.
 *
 * If RecPtr is not NULL, try to read a record at that position.  Otherwise
 * try to read a record just after the last one previously read.
 *
 * If the page read callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to XLogReadRecord.
 */
XLogRecord *XLogReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg, bool readoldversion,
                           bool doDecode)
{
    XLogRecord *record = NULL;
    XLogRecPtr targetPagePtr;
    /*
     * randAccess indicates whether to verify the previous-record pointer of
     * the record we're reading.  We only do this if we're reading
     * sequentially, which is what we initially assume.
     */
    bool randAccess = false;
    uint32 len, total_len;
    uint32 targetRecOff;
    uint32 pageHeaderSize;
    bool gotheader = false;
    int readOff;
    errno_t errorno = EOK;

    /* reset error state */
    *errormsg = NULL;
    state->errormsg_buf[0] = '\0';

    ResetDecoder(state);

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if (XLByteEQ(RecPtr, InvalidXLogRecPtr)) {
        /* No explicit start point; read the record after the one we just read */
        RecPtr = state->EndRecPtr;

        if (readoldversion && (XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ) < SizeOfXLogRecordOld) {
            NextLogPage(RecPtr);
        }

        if (XLByteEQ(state->ReadRecPtr, InvalidXLogRecPtr))
            randAccess = true;

        /*
         * If at page start, we must skip over the page header using xrecoff check.
         */
        if (0 == RecPtr % XLogSegSize) {
            XLByteAdvance(RecPtr, SizeOfXLogLongPHD);
        } else if (0 == RecPtr % XLOG_BLCKSZ) {
            XLByteAdvance(RecPtr, SizeOfXLogShortPHD);
        }
    } else {
        /*
         * Caller supplied a position to start at.
         *
         * In this case, the passed-in record pointer should already be
         * pointing to a valid record starting position.
         */
        Assert(XRecOffIsValid(RecPtr));
        randAccess = true;
    }

    state->currRecPtr = RecPtr;

    targetPagePtr = RecPtr - RecPtr % XLOG_BLCKSZ;
    targetRecOff = RecPtr % XLOG_BLCKSZ;

    /*
     * Read the page containing the record into state->readBuf. Request
     * enough byte to cover the whole record header, or at least the part of
     * it that fits on the same page.
     */
    readOff = ReadPageInternal(state, targetPagePtr, Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ), readoldversion);
    if (readOff < 0) {
        report_invalid_record(state, "read xlog page failed at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /* The page has been read. Check the XLOG version again. */
    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    /*
     * ReadPageInternal always returns at least the page header, so we can
     * examine it now.
     */
    pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
    if (targetRecOff == 0) {
        /*
         * At page start, so skip over page header.
         */
        RecPtr += pageHeaderSize;
        targetRecOff = pageHeaderSize;
    } else if (targetRecOff < pageHeaderSize) {
        report_invalid_record(state, "invalid record offset at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    if ((((XLogPageHeader)state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) && targetRecOff == pageHeaderSize) {
        report_invalid_record(state, "contrecord is requested by %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /* ReadPageInternal has verified the page header */
    Assert((int)pageHeaderSize <= readOff);

    /*
     * Read the record length.
     *
     * NB: Even though we use an XLogRecord pointer here, the whole record
     * header might not fit on this page. xl_tot_len is the first field of the
     * struct, so it must be on this page (the records are MAXALIGNed), but we
     * cannot access any other fields until we've verified that we got the
     * whole header.
     */
    record = (XLogRecord *)(state->readBuf + RecPtr % XLOG_BLCKSZ);
    total_len = record->xl_tot_len;

    /*
     * If the whole record header is on this page, validate it immediately.
     * Otherwise do just a basic sanity check on xl_tot_len, and validate the
     * rest of the header after reading it from the next page.  The xl_tot_len
     * check is necessary here to ensure that we enter the "Need to reassemble
     * record" code path below; otherwise we might fail to apply
     * static ValidXLogRecordHeader at all.
     */
    if (targetRecOff <= XLOG_BLCKSZ - (readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord)) {
        if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
            goto err;
        gotheader = true;
    } else {
        /* more validation should be done here */
        if (total_len < (readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord) || total_len >= XLogRecordMaxSize) {
            report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                                  (uint32)RecPtr, (uint32)(readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord),
                                  total_len);
            goto err;
        }
        gotheader = false;
    }

    /*
     * Enlarge readRecordBuf as needed.
     */
    if (total_len > state->readRecordBufSize && !allocate_recordbuf(state, total_len)) {
        /* We treat this as a "bogus data" condition */
        report_invalid_record(state, "record length %u at %X/%X too long", total_len, (uint32)(RecPtr >> 32),
                              (uint32)RecPtr);
        goto err;
    }

    len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
    if (total_len > len) {
        /* Need to reassemble record */
        char *contdata = NULL;
        XLogPageHeader pageHeader;
        char *buffer = NULL;
        uint32 gotlen;
        errno_t errorno = EOK;

        /* Copy the first fragment of the record from the first page. */
        errorno = memcpy_s(state->readRecordBuf, len, state->readBuf + RecPtr % XLOG_BLCKSZ, len);
        securec_check_c(errorno, "\0", "\0");
        buffer = state->readRecordBuf + len;
        gotlen = len;

        do {
            /* Calculate pointer to beginning of next page */
            XLByteAdvance(targetPagePtr, XLOG_BLCKSZ);

            /* Wait for the next page to become available */
            readOff = ReadPageInternal(state, targetPagePtr, Min(total_len - gotlen + SizeOfXLogShortPHD, XLOG_BLCKSZ),
                                       readoldversion);

            if (readOff < 0)
                goto err;

            Assert((int)SizeOfXLogShortPHD <= readOff);

            /* Check that the continuation on next page looks valid */
            pageHeader = (XLogPageHeader)state->readBuf;
            if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD)) {
                report_invalid_record(state, "there is no contrecord flag at %X/%X", (uint32)(RecPtr >> 32),
                                      (uint32)RecPtr);
                goto err;
            }

            /*
             * Cross-check that xlp_rem_len agrees with how much of the record
             * we expect there to be left.
             */
            if (pageHeader->xlp_rem_len == 0 || total_len != (pageHeader->xlp_rem_len + gotlen)) {
                report_invalid_record(state, "invalid contrecord length %u at %X/%X", pageHeader->xlp_rem_len,
                                      (uint32)(RecPtr >> 32), (uint32)RecPtr);
                goto err;
            }

            /* Append the continuation from this page to the buffer */
            pageHeaderSize = XLogPageHeaderSize(pageHeader);

            if (readOff < (int)pageHeaderSize)
                readOff = ReadPageInternal(state, targetPagePtr, pageHeaderSize, readoldversion);

            Assert((int)pageHeaderSize <= readOff);

            contdata = (char *)state->readBuf + pageHeaderSize;
            len = XLOG_BLCKSZ - pageHeaderSize;
            if (pageHeader->xlp_rem_len < len)
                len = pageHeader->xlp_rem_len;

            if (readOff < (int)(pageHeaderSize + len))
                readOff = ReadPageInternal(state, targetPagePtr, pageHeaderSize + len, readoldversion);

            errorno = memcpy_s(buffer, total_len - gotlen, (char *)contdata, len);
            securec_check_c(errorno, "", "");
            buffer += len;
            gotlen += len;

            /* If we just reassembled the record header, validate it. */
            if (!gotheader) {
                record = (XLogRecord *)state->readRecordBuf;
                if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
                    goto err;
                gotheader = true;
            }
        } while (gotlen < total_len);

        Assert(gotheader);

        record = (XLogRecord *)state->readRecordBuf;
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
        state->ReadRecPtr = RecPtr;
        state->EndRecPtr = targetPagePtr;
        XLByteAdvance(state->EndRecPtr, (pageHeaderSize + MAXALIGN(pageHeader->xlp_rem_len)));
    } else {
        /* Wait for the record data to become available */
        readOff = ReadPageInternal(state, targetPagePtr, Min(targetRecOff + total_len, XLOG_BLCKSZ), readoldversion);
        if (readOff < 0) {
            goto err;
        }

        /* Record does not cross a page boundary */
        if (!ValidXLogRecord(state, record, RecPtr)) {
            goto err;
        }

        state->EndRecPtr = RecPtr;
        XLByteAdvance(state->EndRecPtr, MAXALIGN(total_len));

        state->ReadRecPtr = RecPtr;
        errorno = memcpy_s(state->readRecordBuf, total_len, record, total_len);
        securec_check_c(errorno, "\0", "\0");
        record = (XLogRecord *)state->readRecordBuf;
    }

    /*
     * Special processing if it's an XLOG SWITCH record
     */

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if ((readoldversion ? ((XLogRecordOld *)record)->xl_rmid : ((XLogRecord *)record)->xl_rmid) == RM_XLOG_ID &&
        (readoldversion ? ((XLogRecordOld *)record)->xl_info : ((XLogRecord *)record)->xl_info) == XLOG_SWITCH) {
        /* Pretend it extends to end of segment */
        state->EndRecPtr += XLogSegSize - 1;
        state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
    }

    if (doDecode) {
        if (DecodeXLogRecord(state, record, errormsg, readoldversion)) {
            return record;
        } else {
            return NULL;
        }
    } else {
        return record;
    }

err:

    /*
     * Invalidate the read state. We might read from a different source after
     * failure.
     */
    XLogReaderInvalReadState(state);

    if (state->errormsg_buf[0] != '\0')
        *errormsg = state->errormsg_buf;

    return NULL;
}

/*
 * Avoid the record provided for XlogReadRecord is not a valid record,
 * maybe necessary to skip the LogLongPHD in startpoint of segment
 * or startpoint of page
 */
void AlignXlogPtrToNextPageIfNeeded(XLogRecPtr *recPtr)
{
    if (((*recPtr) % XLogSegSize) < SizeOfXLogLongPHD) {
        *recPtr -= ((*recPtr) % XLogSegSize);
        XLByteAdvance((*recPtr), SizeOfXLogLongPHD);
    } else if (((*recPtr) % XLOG_BLCKSZ) < SizeOfXLogShortPHD) {
        *recPtr -= ((*recPtr) % XLOG_BLCKSZ);
        XLByteAdvance((*recPtr), SizeOfXLogShortPHD);
    }
}

/*
 * Ref XLogFindNextRecord() to find the first record with at an lsn >= RecPtr.
 *
 * Useful for checking wether RecPtr is a valid xlog address for reading and to
 * find the first valid address after some address when dumping records for
 * debugging purposes.
 */
bool ValidateNextXLogRecordPtr(XLogReaderState *state, XLogRecPtr &cur_ptr, char **err_msg)
{
    if (state == NULL) {
        Assert(false);
        return false;
    }

    XLogPageHeader header = NULL;
    bool has_err = false;
    XLogReaderState saved_state = *state;
    XLogRecPtr RecPtr = cur_ptr;
    XLogRecPtr targetPagePtr = InvalidXLogRecPtr;
    XLogRecPtr err_ptr = InvalidXLogRecPtr;
    int targetRecOff = 0;
    uint32 pageHeaderSize = 0;
    int read_len = 0;
    int err_req_len = 0;
    bool isOldFormat = false;
    char *errormsg = NULL;

    /* reset error state */
    *err_msg = NULL;
    state->errormsg_buf[0] = '\0';

    /*
     * Here MUST  be adapted for 64 bit xid:
     * Read the xlog page including cur_ptr and check the page header
     * to ensure whether this xlog is old format or new format supporting
     * the 64 bit xlog or not.
     */
    targetRecOff = cur_ptr % XLOG_BLCKSZ;

    if (targetRecOff > 0) {
        /* scroll back to page boundary */
        targetPagePtr = cur_ptr - targetRecOff;

        /* Read the page containing the record */
        read_len = ReadPageInternal(state, targetPagePtr, targetRecOff, true);
        if (read_len < 0) {
            err_ptr = targetPagePtr;
            err_req_len = targetRecOff;
            has_err = true;
            goto err;
        }

        header = (XLogPageHeader)state->readBuf;

        pageHeaderSize = XLogPageHeaderSize(header);

        /* make sure we have enough data for the page header */
        read_len = ReadPageInternal(state, targetPagePtr, pageHeaderSize, true);
        if (read_len < 0) {
            err_ptr = targetPagePtr;
            err_req_len = (int)pageHeaderSize;
            has_err = true;
            goto err;
        }

        isOldFormat = (header->xlp_magic == XLOG_PAGE_MAGIC_OLD);
    }

    /* Ref findLastCheckpoint() and XLogReadRecord() to find the first valid xlog record following the start_ptr pos. */
    if (isOldFormat && (XLOG_BLCKSZ - cur_ptr % XLOG_BLCKSZ) < SizeOfXLogRecordOld)
        NextLogPage(cur_ptr);

    /*
     * If at page start, we must skip over the page header using xrecoff check.
     */
    if (cur_ptr % XLogSegSize == 0)
        XLByteAdvance(cur_ptr, SizeOfXLogLongPHD);
    else if (cur_ptr % XLOG_BLCKSZ == 0)
        XLByteAdvance(cur_ptr, SizeOfXLogShortPHD);

    /*
     * skip over potential continuation data, keeping in mind that it may span
     * multiple pages
     */
    while (true) {
        /*
         * Compute targetRecOff. It should typically be equal or greater than
         * short page-header since a valid record can't start anywhere before
         * that, except when caller has explicitly specified the offset that
         * falls somewhere there or when we are skipping multi-page
         * continuation record. It doesn't matter though because
         * ReadPageInternal() is prepared to handle that and will read at least
         * short page-header worth of data
         */
        targetRecOff = cur_ptr % XLOG_BLCKSZ;

        /* scroll back to page boundary */
        targetPagePtr = cur_ptr - targetRecOff;

        /* Read the page containing the record */
        read_len = ReadPageInternal(state, targetPagePtr, targetRecOff, true);
        if (read_len < 0) {
            err_ptr = targetPagePtr;
            err_req_len = targetRecOff;
            has_err = true;
            goto err;
        }

        header = (XLogPageHeader)state->readBuf;

        pageHeaderSize = XLogPageHeaderSize(header);

        /* make sure we have enough data for the page header */
        read_len = ReadPageInternal(state, targetPagePtr, pageHeaderSize, true);
        if (read_len < 0) {
            err_ptr = targetPagePtr;
            err_req_len = (int)pageHeaderSize;
            has_err = true;
            goto err;
        }

        /* skip over potential continuation data */
        if (header->xlp_info & XLP_FIRST_IS_CONTRECORD) {
            /*
             * If the length of the remaining continuation data is more than
             * what can fit in this page, the continuation record crosses over
             * this page. Read the next page and try again. xlp_rem_len in the
             * next page header will contain the remaining length of the
             * continuation data
             *
             * Note that record headers are MAXALIGN'ed
             */
            if (MAXALIGN(header->xlp_rem_len) >= (XLOG_BLCKSZ - pageHeaderSize)) {
                cur_ptr = targetPagePtr;
                XLByteAdvance(cur_ptr, XLOG_BLCKSZ);
            } else {
                /*
                 * The previous continuation record ends in this page. Set
                 * tmpRecPtr to point to the first valid record
                 */
                cur_ptr = targetPagePtr;
                XLByteAdvance(cur_ptr, pageHeaderSize + MAXALIGN(header->xlp_rem_len));
                break;
            }
        } else {
            cur_ptr = targetPagePtr;
            XLByteAdvance(cur_ptr, pageHeaderSize);
            break;
        }
    }
    /*
     * we know now that cur_ptr is an address pointing to a valid XLogRecord
     * because either we're at the first record after the beginning of a page
     * or we just jumped over the remaining data of a continuation.
     */
    while (XLogReadRecord(state, cur_ptr, &errormsg, true) != NULL) {
        /* continue after the record */
        cur_ptr = InvalidXLogRecPtr;

        /* past the record we've found, break out */
        if (XLByteLE(RecPtr, state->ReadRecPtr)) {
            cur_ptr = state->ReadRecPtr;
            goto out;
        }
    }
err:
    if (has_err) {
        report_invalid_record(state, "Failed to read the xlog record info %d bytes at %X/%X.", err_req_len,
                              (uint32)(err_ptr >> 32), (uint32)err_ptr);

        *err_msg = state->errormsg_buf;

        return false;
    }
out:
    /* Reset state to what we had before finding the record */
    state->ReadRecPtr = saved_state.ReadRecPtr;
    state->EndRecPtr = saved_state.EndRecPtr;
    XLogReaderInvalReadState(state);

    return (cur_ptr != InvalidXLogRecPtr);
}

/*
 * Read a single xlog page including at least [pageptr, reqLen] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason; errormsg_buf
 * is set in that case (unless the error occurs in the read_page callback).
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, bool readoldversion)
{
    int readLen;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;
    XLogPageHeader hdr;

    Assert((pageptr % XLOG_BLCKSZ) == 0);

    XLByteToSeg(pageptr, targetSegNo);
    targetPageOff = (pageptr % XLogSegSize);

    /* check whether we have all the requested data already */
    if (targetSegNo == state->readSegNo && targetPageOff == state->readOff && reqLen < (int)state->readLen) {
        return state->readLen;
    }

    /*
     * First, read the requested data length, but at least a short page header
     * so that we can validate it.
     */
    readLen = state->read_page(state, pageptr, Max(reqLen, (int)SizeOfXLogShortPHD), state->currRecPtr, state->readBuf,
                               &state->readPageTLI);
    if (readLen < 0) {
        goto err;
    }

    Assert(readLen <= XLOG_BLCKSZ);

    /* Do we have enough data to check the header length? */
    if (readLen <= (int)SizeOfXLogShortPHD) {
        goto err;
    }

    Assert(readLen >= reqLen);

    hdr = (XLogPageHeader)state->readBuf;

    if (hdr->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    /* still not enough */
    if (readLen < (int)XLogPageHeaderSize(hdr)) {
        readLen = state->read_page(state, pageptr, XLogPageHeaderSize(hdr), state->currRecPtr, state->readBuf,
                                   &state->readPageTLI);
        if (readLen < 0) {
            goto err;
        }
    }

    /*
     * Now that we know we have the full header, validate it.
     */
    if (!ValidXLogPageHeader(state, pageptr, hdr, readoldversion)) {
        goto err;
    }

    /* update read state information */
    state->readSegNo = targetSegNo;
    state->readOff = targetPageOff;
    state->readLen = readLen;

    return readLen;

err:
    XLogReaderInvalReadState(state);
    return -1;
}

/*
 * Invalidate the xlogreader's read state to force a re-read.
 */
void XLogReaderInvalReadState(XLogReaderState *state)
{
    state->readSegNo = 0;
    state->readOff = 0;
    state->readLen = 0;
}

/*
 * Validate an XLOG record header.
 *
 * This is just a convenience subroutine to avoid duplicated code in
 * XLogReadRecord.  It's not intended for use from anywhere else.
 */
bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr, XLogRecPtr PrevRecPtr, XLogRecord *record,
                           bool randAccess)
{
    bool readoldversion = true;
    XLogRecPtr xl_prev;

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if ((readoldversion ? ((XLogRecordOld *)record)->xl_tot_len : ((XLogRecord *)record)->xl_tot_len) <
            (readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord) ||
        (readoldversion ? ((XLogRecordOld *)record)->xl_tot_len : ((XLogRecord *)record)->xl_tot_len) >=
            XLogRecordMaxSize) {
        report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                              (uint32)RecPtr, (uint32)(readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord),
                              (readoldversion ? ((XLogRecordOld *)record)->xl_tot_len
                                                : ((XLogRecord *)record)->xl_tot_len));
        return false;
    }
    if ((readoldversion ? ((XLogRecordOld *)record)->xl_rmid : ((XLogRecord *)record)->xl_rmid) > RM_MAX_ID) {
        report_invalid_record(state, "invalid resource manager ID %u at %X/%X",
                              (readoldversion ? ((XLogRecordOld *)record)->xl_rmid : ((XLogRecord *)record)->xl_rmid),
                              (uint32)(RecPtr >> 32), (uint32)RecPtr);
        return false;
    }

    if (readoldversion) {
        xl_prev = XLogRecPtrSwap(((XLogRecordOld *)record)->xl_prev);
    } else {
        xl_prev = record->xl_prev;
    }
    if (randAccess) {
        /*
         * We can't exactly verify the prev-link, but surely it should be less
         * than the record's own address.
         */
        if (XLByteLE(RecPtr, xl_prev)) {
            report_invalid_record(state, "record with incorrect prev-link %X/%X at %X/%X", (uint32)(xl_prev >> 32),
                                  (uint32)xl_prev, (uint32)(RecPtr >> 32), (uint32)RecPtr);
            return false;
        }
    } else {
        /*
         * Record's prev-link should exactly match our previous location. This
         * check guards against torn WAL pages where a stale but valid-looking
         * WAL record starts on a sector boundary.
         */
        if (!(XLByteEQ(xl_prev, PrevRecPtr))) {
            report_invalid_record(state, "record with incorrect prev-link %X/%X at %X/%X", (uint32)(xl_prev >> 32),
                                  (uint32)xl_prev, (uint32)(RecPtr >> 32), (uint32)RecPtr);
            return false;
        }
    }

    return true;
}

/*
 * CRC-check an XLOG record.  We do not believe the contents of an XLOG
 * record (other than to the minimal extent of computing the amount of
 * data to read in) until we've checked the CRCs.
 *
 * We assume all of the record (that is, xl_tot_len bytes) has been read
 * into memory at *record.  Also, ValidXLogRecordHeader() has accepted the
 * record's header, which means in particular that xl_tot_len is at least
 * SizeOfXlogRecord.
 */
bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr)
{
    bool readoldversion = true;
    pg_crc32c crc; /* pg_crc32c is same as pg_crc32 */

    if (((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC) {
        readoldversion = false;
    }

    if (readoldversion && ((XLogPageHeader)state->readBuf)->xlp_magic == XLOG_PAGE_MAGIC_OLD) {
        /* using PG's CRC32 before V1R8C10 */
        INIT_CRC32(crc);
        COMP_CRC32(crc, ((char *)record) + SizeOfXLogRecordOld,
                   ((XLogRecordOld *)record)->xl_tot_len - SizeOfXLogRecordOld);

        /* include the record header last */
        COMP_CRC32(crc, (char *)record, offsetof(XLogRecordOld, xl_crc));
        FIN_CRC32(crc);

        if (!EQ_CRC32(((XLogRecordOld *)record)->xl_crc, crc)) {
            report_invalid_record(state, "incorrect resource manager data checksum in old version record at %X/%X",
                                  (uint32)(recptr >> 32), (uint32)recptr);
            return false;
        }
        return true;
    } else {
        /* using CRC32C since V1R8C10 */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, ((char *)record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
        /* include the record header last */
        COMP_CRC32C(crc, (char *)record, offsetof(XLogRecord, xl_crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(record->xl_crc, crc)) {
#ifdef FRONTEND
            report_invalid_record(state, "incorrect resource manager data checksum in record at %X/%X",
                                  (uint32)(recptr >> 32), (uint32)recptr);
#else
            report_invalid_record(state,
                                  "incorrect resource manager data checksum in record at %X/%X, "
                                  "record info: xl_info=%d, xl_prev=%X/%X, xl_rmid=%d, xl_tot_len=%u, xl_xid= %lu",
                                  (uint32)(recptr >> 32), (uint32)recptr, record->xl_info,
                                  (uint32)(record->xl_prev >> 32), (uint32)(record->xl_prev), record->xl_rmid,
                                  record->xl_tot_len, record->xl_xid);
#endif
            return false;
        }

        return true;
    }
}

/*
 * Validate a page header
 */
bool ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr, XLogPageHeader hdr, bool readoldversion)
{
    XLogRecPtr recaddr;
    XLogSegNo segno;
    uint32 offset;
    int ss_c = 0;
    XLogRecPtr xlp_pageaddr;
    char fname[MAXFNAMELEN];

    Assert((recptr % XLOG_BLCKSZ) == 0);

    XLByteToSeg(recptr, segno);
    offset = recptr % XLogSegSize;
    recaddr = recptr;

    if (hdr->xlp_magic != XLOG_PAGE_MAGIC) {
        prepare_invalid_report(state, fname, MAXFNAMELEN, segno);

        if (hdr->xlp_magic == XLOG_PAGE_MAGIC_OLD && !readoldversion) {
            report_invalid_record(state, "read old version XLog, magic number %04X in log segment %s, offset %u",
                                  hdr->xlp_magic, fname, offset);
            return false;
        }
        if (hdr->xlp_magic != XLOG_PAGE_MAGIC_OLD) {
            report_invalid_record(state, "invalid magic number %04X in log segment %s, offset %u", hdr->xlp_magic,
                                  fname, offset);
            return false;
        }
    }

    if ((hdr->xlp_info & ~XLP_ALL_FLAGS) != 0) {
        prepare_invalid_report(state, fname, MAXFNAMELEN, segno);

        report_invalid_record(state, "invalid info bits %04X in log segment %s, offset %u", hdr->xlp_info, fname,
                              offset);
        return false;
    }

    if (hdr->xlp_info & XLP_LONG_HEADER) {
        XLogLongPageHeader longhdr = (XLogLongPageHeader)hdr;

        if (state->system_identifier && longhdr->xlp_sysid != state->system_identifier) {
            char fhdrident_str[32];
            char sysident_str[32];

            /*
             * Format sysids separately to keep platform-dependent format code
             * out of the translatable message string.
             */
            ss_c = snprintf_s(fhdrident_str, sizeof(fhdrident_str), sizeof(fhdrident_str) - 1, UINT64_FORMAT,
                              longhdr->xlp_sysid);
#ifndef FRONTEND
            securec_check_ss(ss_c, "", "");
#else
            securec_check_ss_c(ss_c, "", "");
#endif

            ss_c = snprintf_s(sysident_str, sizeof(sysident_str), sizeof(sysident_str) - 1, UINT64_FORMAT,
                              state->system_identifier);
#ifndef FRONTEND
            securec_check_ss(ss_c, "", "");
#else
            securec_check_ss_c(ss_c, "", "");
#endif

            report_invalid_record(
                state,
                "WAL file is from different database system: WAL file database system identifier is %s, pg_control "
                "database system identifier is %s.",
                fhdrident_str, sysident_str);
            return false;
        } else if (longhdr->xlp_seg_size != XLogSegSize) {
            report_invalid_record(
                state, "WAL file is from different database system: Incorrect XLOG_SEG_SIZE in page header.");
            return false;
        } else if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ) {
            report_invalid_record(state,
                                  "WAL file is from different database system: Incorrect XLOG_BLCKSZ in page header.");
            return false;
        }
    } else if (offset == 0) {
        prepare_invalid_report(state, fname, MAXFNAMELEN, segno);

        /* hmm, first page of file doesn't have a long header? */
        report_invalid_record(state, "invalid info bits %04X in log segment %s, offset %u", hdr->xlp_info, fname,
                              offset);
        return false;
    }

    if (readoldversion) {
        xlp_pageaddr = (((hdr->xlp_pageaddr) >> 32) | ((hdr->xlp_pageaddr) << 32));
    } else {
        xlp_pageaddr = hdr->xlp_pageaddr;
    }

    if (!XLByteEQ(xlp_pageaddr, recaddr)) {
        prepare_invalid_report(state, fname, MAXFNAMELEN, segno);

        report_invalid_record(state, "unexpected pageaddr %X/%X in log segment %s, offset %u",
                              (uint32)(xlp_pageaddr >> 32), (uint32)xlp_pageaddr, fname, offset);
        return false;
    }

    /*
     * Since child timelines are always assigned a TLI greater than their
     * immediate parent's TLI, we should never see TLI go backwards across
     * successive pages of a consistent WAL sequence.
     *
     * Sometimes we re-read a segment that's already been (partially) read. So
     * we only verify TLIs for pages that are later than the last remembered
     * LSN.
     */
    if (XLByteLT(state->latestPagePtr, recptr)) {
        if (hdr->xlp_tli < state->latestPageTLI) {
            prepare_invalid_report(state, fname, MAXFNAMELEN, segno);

            report_invalid_record(state, "out-of-sequence timeline ID %u (after %u) in log segment %s, offset %u",
                                  hdr->xlp_tli, state->latestPageTLI, fname, offset);
            return false;
        }
    }
    state->latestPagePtr = recptr;
    state->latestPageTLI = hdr->xlp_tli;

    return true;
}

/*
 * Functions that are currently not needed in the backend, but are better
 * implemented inside xlogreader.c because of the internal facilities available
 * here.
 *
 * Find the first record with at an lsn >= RecPtr.
 *
 * Useful for checking wether RecPtr is a valid xlog address for reading and to
 * find the first valid address after some address when dumping records for
 * debugging purposes.
 */
XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr)
{
    XLogReaderState saved_state = *state;
    XLogRecPtr tmpRecPtr;
    XLogRecPtr found = InvalidXLogRecPtr;
    XLogPageHeader header;
    char *errormsg = NULL;

    Assert(!XLogRecPtrIsInvalid(RecPtr));

    /*
     * skip over potential continuation data, keeping in mind that it may span
     * multiple pages
     */
    tmpRecPtr = RecPtr;
    while (true) {
        XLogRecPtr targetPagePtr;
        int targetRecOff;
        uint32 pageHeaderSize;
        int readLen;

        /*
         * Compute targetRecOff. It should typically be equal or greater than
         * short page-header since a valid record can't start anywhere before
         * that, except when caller has explicitly specified the offset that
         * falls somewhere there or when we are skipping multi-page
         * continuation record. It doesn't matter though because
         * ReadPageInternal() is prepared to handle that and will read at least
         * short page-header worth of data
         */
        targetRecOff = tmpRecPtr % XLOG_BLCKSZ;

        /* scroll back to page boundary */
        targetPagePtr = tmpRecPtr - targetRecOff;

        /* Read the page containing the record */
        readLen = ReadPageInternal(state, targetPagePtr, targetRecOff, true);
        if (readLen < 0) {
            goto out;
        }

        header = (XLogPageHeader)state->readBuf;

        pageHeaderSize = XLogPageHeaderSize(header);

        /* make sure we have enough data for the page header */
        readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize, true);
        if (readLen < 0) {
            goto out;
        }

        /* skip over potential continuation data */
        if (header->xlp_info & XLP_FIRST_IS_CONTRECORD) {
            /*
             * If the length of the remaining continuation data is more than
             * what can fit in this page, the continuation record crosses over
             * this page. Read the next page and try again. xlp_rem_len in the
             * next page header will contain the remaining length of the
             * continuation data
             *
             * Note that record headers are MAXALIGN'ed
             */
            if (MAXALIGN(header->xlp_rem_len) >= (XLOG_BLCKSZ - pageHeaderSize)) {
                tmpRecPtr = targetPagePtr;
                XLByteAdvance(tmpRecPtr, XLOG_BLCKSZ);
            } else {
                /*
                 * The previous continuation record ends in this page. Set
                 * tmpRecPtr to point to the first valid record
                 */
                tmpRecPtr = targetPagePtr;
                XLByteAdvance(tmpRecPtr, pageHeaderSize + MAXALIGN(header->xlp_rem_len));
                break;
            }
        } else {
            tmpRecPtr = targetPagePtr;
            XLByteAdvance(tmpRecPtr, pageHeaderSize);
            break;
        }
    }
    /*
     * we know now that tmpRecPtr is an address pointing to a valid XLogRecord
     * because either we're at the first record after the beginning of a page
     * or we just jumped over the remaining data of a continuation.
     */
    while (XLogReadRecord(state, tmpRecPtr, &errormsg, true) != NULL) {
        /* continue after the record */
        tmpRecPtr = InvalidXLogRecPtr;

        /* past the record we've found, break out */
        if (XLByteLE(RecPtr, state->ReadRecPtr)) {
            found = state->ReadRecPtr;
            goto out;
        }
    }

out:
    /* Reset state to what we had before finding the record */
    state->ReadRecPtr = saved_state.ReadRecPtr;
    state->EndRecPtr = saved_state.EndRecPtr;
    XLogReaderInvalReadState(state);

    return found;
}

/* ----------------------------------------
 * Functions for decoding the data and block references in a record.
 * ----------------------------------------
 */
/* private function to reset the state between records */
void ResetDecoder(XLogReaderState *state)
{
    int block_id;

    state->decoded_record = NULL;

    state->main_data_len = 0;

    for (block_id = 0; block_id <= state->max_block_id; block_id++) {
        state->blocks[block_id].in_use = false;
        state->blocks[block_id].has_image = false;
        state->blocks[block_id].has_data = false;
        state->blocks[block_id].data_len = 0;
#ifdef USE_ASSERT_CHECKING
        state->blocks[block_id].replayed = 0;
#endif
    }
    state->max_block_id = -1;
}

/*
 * Decode the previously read record.
 *
 * On error, a human-readable error message is returned in *errormsg, and
 * the return value is false.
 */
bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record, char **errormsg, bool readoldversion)
{
    char *ptr = NULL;
    uint32 remaining;
    uint32 datatotal;
    DecodedBkpBlock *lastBlock = NULL;
    uint8 block_id;
    errno_t rc = EOK;

    ResetDecoder(state);

    state->decoded_record = record;
    state->record_origin = InvalidRepOriginId;

    ptr = (char *)record;
    ptr += readoldversion ? SizeOfXLogRecordOld : SizeOfXLogRecord;
    remaining = readoldversion ? ((XLogRecordOld *)record)->xl_tot_len - SizeOfXLogRecordOld
                                : record->xl_tot_len - SizeOfXLogRecord;

    /* Decode the headers */
    datatotal = 0;
    while (remaining > datatotal) {
        if (remaining < sizeof(uint8))
            goto shortdata_err;

        block_id = *(uint8 *)ptr;
        ptr += sizeof(uint8);
        remaining -= sizeof(uint8);

        if (block_id == XLR_BLOCK_ID_DATA_SHORT) {
            /* XLogRecordDataHeaderShort */
            if (remaining < sizeof(uint8))
                goto shortdata_err;

            state->main_data_len = *(uint8 *)ptr;
            ptr += sizeof(uint8);
            remaining -= sizeof(uint8);
            datatotal += state->main_data_len;
            break; /* by convention, the main data fragment is
                    * always last */
        } else if (block_id == XLR_BLOCK_ID_DATA_LONG) {
            /* XLogRecordDataHeaderLong */
            if (remaining < sizeof(uint32))
                goto shortdata_err;
            state->main_data_len = *(uint32 *)ptr;
            ptr += sizeof(uint32);
            remaining -= sizeof(uint32);
            datatotal += state->main_data_len;
            break; /* by convention, the main data fragment is
                    * always last */
        } else if (block_id == XLR_BLOCK_ID_ORIGIN) {
            if (remaining < sizeof(RepOriginId))
                goto shortdata_err;
            state->record_origin = *(RepOriginId *)ptr;
            ptr += sizeof(RepOriginId);
            remaining -= sizeof(RepOriginId);

        } else if (BKID_GET_BKID(block_id) <= XLR_MAX_BLOCK_ID) {
            /* XLogRecordBlockHeader */
            DecodedBkpBlock *blk = NULL;
            uint8 fork_flags;
            bool hasbucket = (block_id & BKID_HAS_BUCKET) != 0;
            block_id = BKID_GET_BKID(block_id);

            if (block_id <= state->max_block_id) {
                report_invalid_record(state, "out-of-order block_id %u at %X/%X", block_id,
                                      (uint32)(state->ReadRecPtr >> 32), (uint32)state->ReadRecPtr);
                goto err;
            }
            state->max_block_id = block_id;

            blk = &state->blocks[block_id];
            blk->in_use = true;

            if (remaining < sizeof(uint8))
                goto shortdata_err;

            fork_flags = *(uint8 *)ptr;
            ptr += sizeof(uint8);
            remaining -= sizeof(uint8);

            blk->forknum = fork_flags & BKPBLOCK_FORK_MASK;
            blk->flags = fork_flags;
            blk->has_image = ((fork_flags & BKPBLOCK_HAS_IMAGE) != 0);
            blk->has_data = ((fork_flags & BKPBLOCK_HAS_DATA) != 0);

            if (remaining < sizeof(uint16))
                goto shortdata_err;
            blk->data_len = *(uint16 *)ptr;
            securec_check(rc, "\0", "\0");
            ptr += sizeof(uint16);
            remaining -= sizeof(uint16);

            /* cross-check that the HAS_DATA flag is set iff data_length > 0 */
            if (blk->has_data && blk->data_len == 0) {
                report_invalid_record(state, "BKPBLOCK_HAS_DATA set, but no data included at %X/%X",
                                      (uint32)(state->ReadRecPtr >> 32), (uint32)state->ReadRecPtr);
                goto err;
            }
            if (!blk->has_data && blk->data_len != 0) {
                report_invalid_record(state, "BKPBLOCK_HAS_DATA not set, but data length is %u at %X/%X",
                                      (unsigned int)blk->data_len, (uint32)(state->ReadRecPtr >> 32),
                                      (uint32)state->ReadRecPtr);
                goto err;
            }
            datatotal += blk->data_len;

            if (blk->has_image) {
                if (remaining < sizeof(uint16))
                    goto shortdata_err;
                blk->hole_offset = *(uint16 *)ptr;
                ptr += sizeof(uint16);
                remaining -= sizeof(uint16);

                if (remaining < sizeof(uint16))
                    goto shortdata_err;

                blk->hole_length = *(uint16 *)ptr;
                ptr += sizeof(uint16);
                remaining -= sizeof(uint16);

                datatotal += BLCKSZ - blk->hole_length;
            }
            if (!(fork_flags & BKPBLOCK_SAME_REL)) {
                uint32 filenodelen = (hasbucket ? sizeof(RelFileNode) : sizeof(RelFileNodeOld));
                if (remaining < filenodelen)
                    goto shortdata_err;
                blk->rnode.bucketNode = InvalidBktId;
                errno_t rc = memcpy_s(&blk->rnode, filenodelen, ptr, filenodelen);
                securec_check(rc, "\0", "\0");
                ptr += filenodelen;
                remaining -= filenodelen;

                if (remaining < sizeof(uint16))
                    goto shortdata_err;

                blk->extra_flag = *(uint16 *)ptr;
                ptr += sizeof(uint16);
                remaining -= sizeof(uint16);

                lastBlock = blk;
            } else {
                if (lastBlock == NULL) {
                    report_invalid_record(state, "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                                          (uint32)(state->ReadRecPtr >> 32), (uint32)state->ReadRecPtr);
                    goto err;
                }

                blk->rnode = lastBlock->rnode;
                blk->extra_flag = lastBlock->extra_flag;
            }

            if (remaining < sizeof(BlockNumber))
                goto shortdata_err;
            blk->blkno = *(BlockNumber *)ptr;
            ptr += sizeof(BlockNumber);
            remaining -= sizeof(BlockNumber);

            if (remaining < sizeof(XLogRecPtr))
                goto shortdata_err;
            blk->last_lsn = *(XLogRecPtr *)ptr;
            ptr += sizeof(XLogRecPtr);
            remaining -= sizeof(XLogRecPtr);
        } else {
            report_invalid_record(state, "invalid block_id %u at %X/%X", block_id, (uint32)(state->ReadRecPtr >> 32),
                                  (uint32)state->ReadRecPtr);
            goto err;
        }
    }

    if (remaining != datatotal)
        goto shortdata_err;

    /*
     * Ok, we've parsed the fragment headers, and verified that the total
     * length of the payload in the fragments is equal to the amount of data
     * left. Copy the data of each fragment to a separate buffer.
     *
     * We could just set up pointers into readRecordBuf, but we want to align
     * the data for the convenience of the callers. Backup images are not
     * copied, however; they don't need alignment.
     */
    /* block data first */
    for (block_id = 0; block_id <= state->max_block_id; block_id++) {
        DecodedBkpBlock *blk = &state->blocks[block_id];

        if (!blk->in_use)
            continue;
        if (blk->has_image) {
            blk->bkp_image = ptr;
            ptr += BLCKSZ - blk->hole_length;
        }
        if (blk->has_data) {
            if (!blk->data || blk->data_len > blk->data_bufsz) {
                if (blk->data) {
                    pfree(blk->data);
                    blk->data = NULL;
                }
                blk->data_bufsz = blk->data_len;
                blk->data = (char *)palloc(blk->data_bufsz);
                Assert(blk->data != NULL);
            }
            rc = memcpy_s(blk->data, blk->data_len, ptr, blk->data_len);
            securec_check(rc, "\0", "\0");
            ptr += blk->data_len;
        }
    }

    /* and finally, the main data */
    if (state->main_data_len > 0) {
        if (!state->main_data || state->main_data_len > state->main_data_bufsz) {
            if (state->main_data) {
                pfree(state->main_data);
                state->main_data = NULL;
            }
            state->main_data_bufsz = state->main_data_len;
            state->main_data = (char *)palloc(state->main_data_bufsz);
            Assert(state->main_data != NULL);
        }
        rc = memcpy_s(state->main_data, state->main_data_len, ptr, state->main_data_len);
        securec_check(rc, "\0", "\0");
        ptr += state->main_data_len;
    }

    state->isDecode = true;
    return true;

shortdata_err:
    report_invalid_record(state, "record with invalid length at %X/%X", (uint32)(state->ReadRecPtr >> 32),
                          (uint32)state->ReadRecPtr);
err:
    *errormsg = state->errormsg_buf;

    return false;
}

/*
 * Returns information about the block that a block reference refers to.
 *
 * If the WAL record contains a block reference with the given ID, *rnode,
 * *forknum, and *blknum are filled in (if not NULL), and returns TRUE.
 * Otherwise returns FALSE.
 */

bool XLogRecGetBlockLastLsn(XLogReaderState *record, uint8 block_id, XLogRecPtr *lsn)
{
    DecodedBkpBlock *bkpb = NULL;

    if (!record->blocks[block_id].in_use)
        return false;

    bkpb = &record->blocks[block_id];
    if (lsn != NULL)
        *lsn = bkpb->last_lsn;
    return true;
}

char *XLogRecGetBlockImage(XLogReaderState *record, uint8 block_id, uint16 *hole_offset, uint16 *hole_length)
{
    DecodedBkpBlock *bkpb = NULL;

    if (!record->blocks[block_id].in_use)
        return NULL;
    if (!record->blocks[block_id].has_image)
        return NULL;

    bkpb = &record->blocks[block_id];

    if (hole_offset != NULL)
        *hole_offset = bkpb->hole_offset;
    if (hole_length != NULL)
        *hole_length = bkpb->hole_length;
    return bkpb->bkp_image;
}

/*
 * Restore a full-page image from a backup block attached to an XLOG record.
 *
 * Returns the buffer number containing the page.
 */
bool RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
    DecodedBkpBlock *bkpb = NULL;
    errno_t rc = EOK;

    if (!record->blocks[block_id].in_use)
        return false;
    if (!record->blocks[block_id].has_image)
        return false;

    bkpb = &record->blocks[block_id];

    if (bkpb->hole_length == 0) {
        rc = memcpy_s(page, BLCKSZ, bkpb->bkp_image, BLCKSZ);
        securec_check(rc, "", "");
    } else {
        rc = memcpy_s(page, BLCKSZ, bkpb->bkp_image, bkpb->hole_offset);
        securec_check(rc, "", "");
        /* must zero-fill the hole */
        rc = memset_s(page + bkpb->hole_offset, BLCKSZ - bkpb->hole_offset, 0, bkpb->hole_length);
        securec_check(rc, "", "");

        Assert(bkpb->hole_offset + bkpb->hole_length <= BLCKSZ);
        if (bkpb->hole_offset + bkpb->hole_length == BLCKSZ)
            return true;

        rc = memcpy_s(page + (bkpb->hole_offset + bkpb->hole_length), BLCKSZ - (bkpb->hole_offset + bkpb->hole_length),
                      bkpb->bkp_image + bkpb->hole_offset, BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
        securec_check(rc, "", "");
    }

    return true;
}

/* XLogreader callback function, to read a WAL page */
int SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                       char *readBuf, TimeLineID *pageTLI)
{
    XLogPageReadPrivate *readprivate = (XLogPageReadPrivate *)xlogreader->private_data;
    uint32 targetPageOff;
    int ss_c = 0;
    const uint32 readMaxRetry = 10;
    uint32 retryCnt = 0;
    char xlogfpath[MAXPGPATH];
    char xlogfname[MAXFNAMELEN];

    targetPageOff = targetPagePtr % XLogSegSize;

tryAgain:
    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if (xlogreadfd >= 0 && !XLByteInSeg(targetPagePtr, xlogreadsegno)) {
        close(xlogreadfd);
        xlogreadfd = -1;
    }

    XLByteToSeg(targetPagePtr, xlogreadsegno);

    if (xlogreadfd < 0) {
        ss_c = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", readprivate->tli,
                          (uint32)((xlogreadsegno) / XLogSegmentsPerXLogId),
                          (uint32)((xlogreadsegno) % XLogSegmentsPerXLogId));
#ifndef FRONTEND
        securec_check_ss(ss_c, "", "");
#else
        securec_check_ss_c(ss_c, "", "");
#endif

        ss_c = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/" XLOGDIR "/%s", readprivate->datadir, xlogfname);
#ifndef FRONTEND
        securec_check_ss(ss_c, "", "");
#else
        securec_check_ss_c(ss_c, "", "");
#endif

        xlogreadfd = open(xlogfpath, O_RDONLY | PG_BINARY, 0);
        if (xlogreadfd < 0) {
            if (retryCnt++ < readMaxRetry) {
                goto tryAgain;
            }
            report_invalid_record(xlogreader, "SimpleXLogPageRead could not open file \"%s\": %s, retryCnt(%u)\n",
                                  xlogfpath, strerror(errno), retryCnt);
            return -1;
        }
    }

    /*
     * At this point, we have the right segment open.
     */
    Assert(xlogreadfd != -1);

    /* Read the requested page */
    if (lseek(xlogreadfd, (off_t)targetPageOff, SEEK_SET) < 0) {
        if (retryCnt++ < readMaxRetry) {
            goto tryAgain;
        }
        report_invalid_record(xlogreader, "SimpleXLogPageRead could not seek in file \"%s\": %s, retryCnt(%u)\n",
                              xlogfpath, strerror(errno), retryCnt);
        return -1;
    }
    if (read(xlogreadfd, readBuf, XLOG_BLCKSZ) != XLOG_BLCKSZ) {
        if (retryCnt++ < readMaxRetry) {
            goto tryAgain;
        }
        report_invalid_record(xlogreader, "SimpleXLogPageRead could not read from file \"%s\": %s, retryCnt(%u)\n",
                              xlogfpath, strerror(errno), retryCnt);
        return -1;
    }
    retryCnt = 0;
    *pageTLI = readprivate->tli;
    return XLOG_BLCKSZ;
}

XLogRecPtr FindMaxLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *maxLsnCrc)
{
    DIR *xlogDir = NULL;
    struct dirent *dirEnt = NULL;
    XLogReaderState *xlogReader = NULL;
    XLogPageReadPrivate readPrivate = {
        .datadir = NULL,
        .tli = 0
    };
    XLogRecord *record = NULL;
    TimeLineID tli = 0;
    XLogRecPtr maxLsn = InvalidXLogRecPtr;
    XLogRecPtr startLsn = InvalidXLogRecPtr;
    XLogRecPtr curLsn = InvalidXLogRecPtr;
    char xlogDirStr[MAXPGPATH];
    char maxXLogFileName[MAXPGPATH] = {0};
    char *errorMsg = NULL;
    bool findValidXLogFile = false;
    uint32 xlogReadLogid = -1;
    uint32 xlogReadLogSeg = -1;
    errno_t rc = EOK;

    rc = snprintf_s(xlogDirStr, MAXPGPATH, MAXPGPATH - 1, "%s/%s", workingPath, XLOGDIR);
#ifndef FRONTEND
    securec_check_ss(rc, "", "");
#else
    securec_check_ss_c(rc, "", "");
#endif

    if (msgLen > XLOG_READER_MAX_MSGLENTH) {
        msgLen = XLOG_READER_MAX_MSGLENTH;
    }

    xlogDir = opendir(xlogDirStr);
    if (!xlogDir) {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1,
                        "open xlog dir %s failed when find max lsn \n", xlogDirStr);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        return InvalidXLogRecPtr;
    }

    /* Ranking xlog from large to small */
    while ((dirEnt = readdir(xlogDir)) != NULL) {
        if (strlen(dirEnt->d_name) == 24 && strspn(dirEnt->d_name, "0123456789ABCDEF") == 24) {
            if (strlen(maxXLogFileName) == 0 || strcmp(maxXLogFileName, dirEnt->d_name) < 0) {
                rc = strncpy_s(maxXLogFileName, MAXPGPATH, dirEnt->d_name, strlen(dirEnt->d_name) + 1);
#ifndef FRONTEND
                securec_check(rc, "", "");
#else
                securec_check_c(rc, "", "");
#endif
                maxXLogFileName[strlen(dirEnt->d_name)] = '\0';
            }
        }
    }

    (void)closedir(xlogDir);

    if (sscanf_s(maxXLogFileName, "%08X%08X%08X", &tli, &xlogReadLogid, &xlogReadLogSeg) != 3) {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1,
                        "failed to translate name to xlog: %s\n", maxXLogFileName);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        return InvalidXLogRecPtr;
    }

    /* Initializing the ReaderState */
    rc = memset_s(&readPrivate, sizeof(XLogPageReadPrivate), 0, sizeof(XLogPageReadPrivate));
    securec_check_c(rc, "\0", "\0");
    readPrivate.tli = tli;
    readPrivate.datadir = workingPath;
    xlogReader = XLogReaderAllocate(&SimpleXLogPageRead, &readPrivate);
    if (xlogReader == NULL) {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1, "reader allocate failed.\n");
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        return InvalidXLogRecPtr;
    }

    /* Start to find the max lsn from a valid xlogfile */
    startLsn = (xlogReadLogSeg * XLOG_SEG_SIZE) + ((XLogRecPtr)xlogReadLogid * XLogSegmentsPerXLogId * XLogSegSize);
    while (!XLogRecPtrIsInvalid(startLsn)) {
        /* find the first valid record from the bigger xlogrecord. then break */
        curLsn = XLogFindNextRecord(xlogReader, startLsn);
        if (XLogRecPtrIsInvalid(curLsn)) {
            if (xlogreadfd > 0) {
                close(xlogreadfd);
                xlogreadfd = -1;
            }
            startLsn = startLsn - XLOG_SEG_SIZE;
            continue;
        } else {
            findValidXLogFile = true;
            break;
        }
    }

    if (findValidXLogFile == false) {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1,
                        "no valid record in pg_xlog.\n");
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        /* Free all opened resources */
        if (xlogReader != NULL) {
            XLogReaderFree(xlogReader);
            xlogReader = NULL;
        }
        if (xlogreadfd > 0) {
            close(xlogreadfd);
            xlogreadfd = -1;
        }
        return InvalidXLogRecPtr;
    }

    /* find the max lsn. */
    do {
        record = XLogReadRecord(xlogReader, curLsn, &errorMsg);
        if (record == NULL) {
            break;
        }
        curLsn = InvalidXLogRecPtr;
        *maxLsnCrc = record->xl_crc;
    } while (true);

    maxLsn = xlogReader->ReadRecPtr;
    if (XLogRecPtrIsInvalid(maxLsn)) {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1, "%s", errorMsg);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
    } else {
        rc = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1,
                        "find max lsn rec (%X/%X) success.\n", (uint32)(maxLsn >> 32), (uint32)maxLsn);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
    }

    /* Free all opened resources */
    if (xlogReader != NULL) {
        XLogReaderFree(xlogReader);
        xlogReader = NULL;
    }
    if (xlogreadfd > 0) {
        close(xlogreadfd);
        xlogreadfd = -1;
    }
    return maxLsn;
}

/* Judging the file of input lsn is existed. */
bool XlogFileIsExisted(const char *workingPath, XLogRecPtr inputLsn, TimeLineID timeLine)
{
    errno_t rc = EOK;
    char xlogfname[MAXFNAMELEN];
    char xlogfpath[MAXPGPATH];
    XLogSegNo xlogSegno = 0;
    struct stat st;

    XLByteToSeg(inputLsn, xlogSegno);
    rc = snprintf_s(xlogfname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", timeLine,
                    (uint32)((xlogSegno) / XLogSegmentsPerXLogId), (uint32)((xlogSegno) % XLogSegmentsPerXLogId));
    securec_check_ss_c(rc, "", "");
    rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/" XLOGDIR "/%s", workingPath, xlogfname);
    securec_check_ss_c(rc, "", "");
    if (stat(xlogfpath, &st) == 0) {
        return true;
    } else {
        return false;
    }
}

void CloseXlogFile(void)
{
    if (xlogreadfd != -1) {
        close(xlogreadfd);
        xlogreadfd = -1;
    }
    return;
}
