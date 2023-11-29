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
#include "access/xlog_basic.h"
#include "catalog/pg_control.h"
#include "securec_check.h"
#include "replication/logical.h"
#include "access/parallel_recovery/redo_item.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "ddes/dms/ss_dms_recovery.h"
#include "storage/file/fio_device.h"

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

#define CLOSE_FD(fd)            \
    do {                        \
        if (fd > 0) {           \
            close(fd);          \
            fd = -1;            \
        }                       \
    } while (0)

bool ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr, XLogPageHeader hdr);
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, char* xlog_path);
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
XLogReaderState *XLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize)
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
    state->preReadBuf = NULL;

    /*
     * Permanently allocate readBuf.  We do it this way, rather than just
     * making a static array, for two reasons: (1) no need to waste the
     * storage in most instantiations of the backend; (2) a static char array
     * isn't guaranteed to have any particular alignment, whereas malloc()
     * will provide MAXALIGN'd storage.
     */
    state->readBufOrigin = (char *)palloc_extended(XLOG_BLCKSZ + alignedSize, MCXT_ALLOC_NO_OOM);
    if (state->readBufOrigin == NULL) {
        pfree(state);
        state = NULL;
        return NULL;
    }
    if (alignedSize == 0) {
        state->readBuf = state->readBufOrigin;
    } else {
        state->readBuf = (char *)TYPEALIGN(alignedSize, state->readBufOrigin);
    }

    state->read_page = pagereadfunc;
    /* system_identifier initialized to zeroes above */
    state->private_data = private_data;
    /* ReadRecPtr and EndRecPtr initialized to zeroes above */
    /* readSegNo, readOff, readLen, readPageTLI initialized to zeroes above */
    state->errormsg_buf = (char *)palloc_extended(MAX_ERRORMSG_LEN + 1, MCXT_ALLOC_NO_OOM);
    if (state->errormsg_buf == NULL) {
        pfree(state->readBufOrigin);
        state->readBufOrigin = NULL;
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
        pfree(state->readBufOrigin);
        state->readBufOrigin = NULL;
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
    pfree(state->errormsg_buf);
    state->errormsg_buf = NULL;
    if (state->readRecordBuf) {
        pfree(state->readRecordBuf);
        state->readRecordBuf = NULL;
    }
    pfree(state->readBufOrigin);
    state->readBufOrigin = NULL;
    state->readBuf = NULL;
    if (state->preReadBufOrigin) {
        pfree(state->preReadBufOrigin);
        state->preReadBufOrigin = NULL;
        state->preReadBuf = NULL;
    }

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
XLogRecord *XLogReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg,
            bool doDecode, char* xlog_path)
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

    if (XLByteEQ(RecPtr, InvalidXLogRecPtr)) {
        /* No explicit start point; read the record after the one we just read */
        RecPtr = state->EndRecPtr;

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
    readOff = ReadPageInternal(state, targetPagePtr, Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ), xlog_path);
    if (readOff < 0) {
        report_invalid_record(state, "read xlog page failed at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
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
    if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord) {
        if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
            goto err;
        gotheader = true;
    } else {
        /* more validation should be done here */
        if (total_len < SizeOfXLogRecord || total_len >= XLogRecordMaxSize) {
            report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                                  (uint32)RecPtr, (uint32)(SizeOfXLogRecord),
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
            readOff = ReadPageInternal(state, targetPagePtr, Min(total_len - gotlen + SizeOfXLogShortPHD, XLOG_BLCKSZ), xlog_path);
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
                readOff = ReadPageInternal(state, targetPagePtr, pageHeaderSize, xlog_path);

            Assert((int)pageHeaderSize <= readOff);

            contdata = (char *)state->readBuf + pageHeaderSize;
            len = XLOG_BLCKSZ - pageHeaderSize;
            if (pageHeader->xlp_rem_len < len)
                len = pageHeader->xlp_rem_len;

            if (readOff < (int)(pageHeaderSize + len))
                readOff = ReadPageInternal(state, targetPagePtr, pageHeaderSize + len, xlog_path);

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
        readOff = ReadPageInternal(state, targetPagePtr, Min(targetRecOff + total_len, XLOG_BLCKSZ), xlog_path);
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
    if (((XLogRecord *)record)->xl_rmid == RM_XLOG_ID && ((XLogRecord *)record)->xl_info == XLOG_SWITCH) {
        /* Pretend it extends to end of segment */
        state->EndRecPtr += XLogSegSize - 1;
        state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
    }

    if (doDecode) {
        if (DecodeXLogRecord(state, record, errormsg)) {
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
 * Read a single xlog page including at least [pageptr, reqLen] of valid data
 * via the read_page() callback.
 *
 * Returns -1 if the required page cannot be read for some reason; errormsg_buf
 * is set in that case (unless the error occurs in the read_page callback).
 *
 * We fetch the page from a reader-local cache if we know we have the required
 * data and if there hasn't been any error since caching the data.
 */
static int ReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, char* xlog_path)
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
                               &state->readPageTLI, xlog_path);
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

    /* still not enough */
    if (readLen < (int)XLogPageHeaderSize(hdr)) {
        readLen = state->read_page(state, pageptr, XLogPageHeaderSize(hdr), state->currRecPtr, state->readBuf,
                                   &state->readPageTLI, xlog_path);
        if (readLen < 0) {
            goto err;
        }
    }

    /*
     * Now that we know we have the full header, validate it.
     */
    if (!ValidXLogPageHeader(state, pageptr, hdr)) {
        goto err;
    }

    /* update read state information */
    state->readSegNo = targetSegNo;
    state->readOff = targetPageOff;
    state->readLen = readLen;
    state->isTde = false;

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
    XLogRecPtr xl_prev;

    if (record->xl_tot_len < SizeOfXLogRecord || record->xl_tot_len >= XLogRecordMaxSize) {
        report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                              (uint32)RecPtr, (uint32)SizeOfXLogRecord, record->xl_tot_len);
        return false;
    }
    if (record->xl_rmid > RM_MAX_ID) {
        report_invalid_record(state, "invalid resource manager ID %u at %X/%X", record->xl_rmid,
                              (uint32)(RecPtr >> 32), (uint32)RecPtr);
        return false;
    }

    xl_prev = record->xl_prev;
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
    pg_crc32c crc; /* pg_crc32c is same as pg_crc32 */

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

/*
 * Validate a page header
 */
bool ValidXLogPageHeader(XLogReaderState *state, XLogRecPtr recptr, XLogPageHeader hdr)
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
        report_invalid_record(state, "invalid magic number %04X in log segment %s, offset %u", hdr->xlp_magic,
                              fname, offset);
        return false;
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


    xlp_pageaddr = hdr->xlp_pageaddr;

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
XLogRecPtr XLogFindNextRecord(XLogReaderState *state, XLogRecPtr RecPtr, XLogRecPtr *endPtr, char *xlog_path)
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
        readLen = ReadPageInternal(state, targetPagePtr, Max(targetRecOff, (int)SizeOfXLogLongPHD), xlog_path);
        if (readLen < 0) {
            goto out;
        }

        header = (XLogPageHeader)state->readBuf;

        pageHeaderSize = XLogPageHeaderSize(header);

        /* make sure we have enough data for the page header */
        readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize, xlog_path);
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
    while (XLogReadRecord(state, tmpRecPtr, &errormsg, true, xlog_path) != NULL) {
        /* continue after the record */
        tmpRecPtr = InvalidXLogRecPtr;

        /* past the record we've found, break out */
        if (XLByteLE(RecPtr, state->ReadRecPtr)) {
            found = state->ReadRecPtr;
            if (endPtr != NULL) {
                *endPtr = state->EndRecPtr;
            }
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
        state->blocks[block_id].tdeinfo = NULL;
#ifdef USE_ASSERT_CHECKING
        state->blocks[block_id].replayed = 0;
#endif
    }
    state->max_block_id = -1;
    state->readblocks = 0;
}

/*
 * This macro can be only used in DecodeXLogRecord to decode one variable from each time
 */
#define DECODE_XLOG_ONE_ITEM(variable, type) \
    do { \
        if (remaining < sizeof(type)) \
            goto shortdata_err; \
        variable = *(type *)ptr; \
        ptr += sizeof(type); \
        remaining -= sizeof(type); \
    } while (0)

/**
 * happens during the upgrade, copy the RelFileNodeV2 to RelFileNode
 * support little-endian system
 * @param relfileNode relfileNode
 */
static void CompressTableRecord(RelFileNode* relfileNode)
{
    if (relfileNode->bucketNode <= -1 && relfileNode->opt == 0xFFFF) {
        relfileNode->opt = 0;
    }
}

/*  
 * Decode the previously read record.
 *
 * On error, a human-readable error message is returned in *errormsg, and
 * the return value is false.
 */
bool DecodeXLogRecord(XLogReaderState *state, XLogRecord *record, char **errormsg)
{
    char *ptr = NULL;
    uint32 remaining;
    uint32 datatotal;
    DecodedBkpBlock *lastBlock = NULL;
    uint8 block_id;

    ResetDecoder(state);

    state->decoded_record = record;
    state->record_origin = InvalidRepOriginId;

    ptr = (char *)record;
    ptr += SizeOfXLogRecord;
    remaining = record->xl_tot_len - SizeOfXLogRecord;

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
            bool hasbucket_segpage = (block_id & BKID_HAS_BUCKET_OR_SEGPAGE) != 0;
            state->isTde = (block_id & BKID_HAS_TDE_PAGE) != 0;
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

            DECODE_XLOG_ONE_ITEM(blk->data_len, uint16);

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
                DECODE_XLOG_ONE_ITEM(blk->hole_offset, uint16);
                DECODE_XLOG_ONE_ITEM(blk->hole_length, uint16);
                datatotal += BLCKSZ - blk->hole_length;
            }
            if (!(fork_flags & BKPBLOCK_SAME_REL)) {
                uint32 filenodelen = (hasbucket_segpage ? sizeof(RelFileNode) : sizeof(RelFileNodeOld));
                if (remaining < filenodelen)
                    goto shortdata_err;
                blk->rnode.bucketNode = InvalidBktId;
                blk->rnode.opt = 0;
                errno_t rc = memcpy_s(&blk->rnode, filenodelen, ptr, filenodelen);
                securec_check(rc, "\0", "\0");
                /* support decode old version of relfileNode */
                CompressTableRecord(&blk->rnode);
                ptr += filenodelen;
                remaining -= filenodelen;

                if (state->isTde) {
                    blk->tdeinfo = (TdeInfo*)ptr;
                    ptr += sizeof(TdeInfo);
                    remaining -= sizeof(TdeInfo);
                }

                DECODE_XLOG_ONE_ITEM(blk->extra_flag, uint16);

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

            DECODE_XLOG_ONE_ITEM(blk->blkno, BlockNumber);
            if (XLOG_NEED_PHYSICAL_LOCATION(blk->rnode)) {
                DECODE_XLOG_ONE_ITEM(blk->seg_fileno, uint8);
                DECODE_XLOG_ONE_ITEM(blk->seg_blockno, BlockNumber);

                if (blk->seg_fileno & BKPBLOCK_HAS_VM_LOC) {
                    blk->has_vm_loc = true;
                    blk->seg_fileno = BKPBLOCK_GET_SEGFILENO(blk->seg_fileno);
                    
                    DECODE_XLOG_ONE_ITEM(blk->vm_seg_fileno, uint8);
                    DECODE_XLOG_ONE_ITEM(blk->vm_seg_blockno, BlockNumber);
                } else {
                    blk->has_vm_loc = false;
                    blk->vm_seg_fileno = InvalidOid;
                    blk->vm_seg_blockno = InvalidBlockNumber;
                }
            } else {
                blk->seg_fileno = InvalidOid;
                blk->seg_blockno = InvalidBlockNumber;
                blk->vm_seg_fileno = InvalidOid;
                blk->vm_seg_blockno = InvalidBlockNumber;
                blk->has_vm_loc = false;
            }
            
            DECODE_XLOG_ONE_ITEM(blk->last_lsn, XLogRecPtr);
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
            blk->data = ptr;
            ptr += blk->data_len;
        }
    }

    /* and finally, the main data */
    if (state->main_data_len > 0) {
        state->main_data_bufsz = state->main_data_len;
        state->main_data = ptr;
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

/* XLogreader callback function, to read a WAL page */
int SimpleXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                       char *readBuf, TimeLineID *pageTLI, char* xlog_path)
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

        if (xlog_path != NULL) {
            ss_c = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", xlog_path, xlogfname);
        } else {
            ss_c = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/" XLOGDIR "/%s", readprivate->datadir, xlogfname);
        }
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

XLogRecord* XLogReadRecordFromAllDir(char** xlogDirs, int xlogDirNum, XLogReaderState *xlogReader, XLogRecPtr curLsn, char** errorMsg)
{
    XLogRecord* record = NULL;
    for (int i = 0; i < xlogDirNum; i++) {
        record = XLogReadRecord(xlogReader, curLsn, errorMsg, true, xlogDirs[i]);
        if (record != NULL) {
            break;
        } else {
            CLOSE_FD(xlogreadfd);
        }
    }
    return record;
}

void SSFindMaxXlogFileName(char* maxXLogFileName, char** xlogDirs, int xlogDirNum)
{
    errno_t rc = EOK;
    for (int i = 0; i < xlogDirNum; i++) {
        DIR* subDir = opendir(xlogDirs[i]);
        struct dirent* subDirEntry = NULL;
        while (subDir != NULL && (subDirEntry = readdir(subDir)) != NULL) {
            if (strlen(subDirEntry->d_name) == 24 && strspn(subDirEntry->d_name, "0123456789ABCDEF") == 24 && 
                (strlen(maxXLogFileName) == 0 || strcmp(maxXLogFileName, subDirEntry->d_name) < 0)) {
                rc = strncpy_s(maxXLogFileName, MAXPGPATH, subDirEntry->d_name, strlen(subDirEntry->d_name) + 1);
                securec_check(rc, "", "");
                maxXLogFileName[strlen(subDirEntry->d_name)] = '\0';
            }
        }
        (void)closedir(subDir);
    }
}

XLogRecPtr SSFindMaxLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *maxLsnCrc, char** xlogDirs, int xlogDirNum)
{
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
    char maxXLogFileName[MAXPGPATH] = {0};
    char *errorMsg = NULL;
    bool findValidXLogFile = false;
    uint32 xlogReadLogid = -1;
    uint32 xlogReadLogSeg = -1;
    errno_t rc = EOK;

    /* Ranking xlog from large to small */
    SSFindMaxXlogFileName(maxXLogFileName, xlogDirs, xlogDirNum);

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
    startLsn = (xlogReadLogSeg * XLogSegSize) + ((XLogRecPtr)xlogReadLogid * XLogSegmentsPerXLogId * XLogSegSize);
    while (!XLogRecPtrIsInvalid(startLsn) && !findValidXLogFile) {
        /* find the first valid record from the bigger xlogrecord. then break */
        for (int i = 0; i < xlogDirNum; i++) {
            curLsn = XLogFindNextRecord(xlogReader, startLsn, NULL, xlogDirs[i]);
            if (XLogRecPtrIsInvalid(curLsn)) {
                CLOSE_FD(xlogreadfd);
            } else {
                findValidXLogFile = true;
                break;
            }
        }
        startLsn = startLsn - XLogSegSize;
    }

    CLOSE_FD(xlogreadfd);
    if (!findValidXLogFile) {
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
        return InvalidXLogRecPtr;
    }

    /* find the max lsn. */
    while(true) {
        record = XLogReadRecordFromAllDir(xlogDirs, xlogDirNum, xlogReader, curLsn, &errorMsg);
        if (record == NULL) {
            break;
        }
        curLsn = InvalidXLogRecPtr;
        *maxLsnCrc = record->xl_crc;
    }

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

    CLOSE_FD(xlogreadfd);
    return maxLsn;
}

XLogRecPtr FindMaxLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *maxLsnCrc,
    uint32 *maxLsnLen, TimeLineID *returnTli, char* xlog_path)
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

    if (xlog_path != NULL) {
        rc = snprintf_s(xlogDirStr, MAXPGPATH, MAXPGPATH - 1, "%s", xlog_path);
    } else {
        rc = snprintf_s(xlogDirStr, MAXPGPATH, MAXPGPATH - 1, "%s/%s", workingPath, XLOGDIR);
    }

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

    if (returnTli != NULL) {
        *returnTli = tli;
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
    startLsn = (xlogReadLogSeg * XLogSegSize) + ((XLogRecPtr)xlogReadLogid * XLogSegmentsPerXLogId * XLogSegSize);
    while (!XLogRecPtrIsInvalid(startLsn)) {
        /* find the first valid record from the bigger xlogrecord. then break */
        curLsn = XLogFindNextRecord(xlogReader, startLsn, NULL, xlogDirStr);
        if (XLogRecPtrIsInvalid(curLsn)) {
            if (xlogreadfd > 0) {
                close(xlogreadfd);
                xlogreadfd = -1;
            }
            startLsn = startLsn - XLogSegSize;
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
        record = XLogReadRecord(xlogReader, curLsn, &errorMsg, true, xlogDirStr);
        if (record == NULL) {
            break;
        }
        curLsn = InvalidXLogRecPtr;
        *maxLsnCrc = record->xl_crc;
        if (maxLsnLen != NULL) {
            *maxLsnLen = (uint32)(xlogReader->EndRecPtr - xlogReader->ReadRecPtr);
        }
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

XLogRecPtr FindMinLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *minLsnCrc)
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
    XLogRecPtr minLsn = InvalidXLogRecPtr;
    XLogRecPtr startLsn = InvalidXLogRecPtr;
    XLogRecPtr curLsn = InvalidXLogRecPtr;
    char xlogDirStr[MAXPGPATH];
    char minXLogFileName[MAXPGPATH] = {0};
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

    xlogDir = opendir(xlogDirStr);
    if (!xlogDir) {
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1,
                        "open xlog dir %s failed when find min lsn \n", xlogDirStr);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        return InvalidXLogRecPtr;
    }

    /* Find smallest xlog file */
    while ((dirEnt = readdir(xlogDir)) != NULL) {
        if (strlen(dirEnt->d_name) == 24 && strspn(dirEnt->d_name, "0123456789ABCDEF") == 24) {
            if (strlen(minXLogFileName) == 0 || strcmp(minXLogFileName, dirEnt->d_name) > 0) {
                rc = strncpy_s(minXLogFileName, MAXPGPATH, dirEnt->d_name, strlen(dirEnt->d_name) + 1);
#ifndef FRONTEND
                securec_check(rc, "", "");
#else
                securec_check_c(rc, "", "");
#endif
                minXLogFileName[strlen(dirEnt->d_name)] = '\0';
            }
        }
    }

    (void)closedir(xlogDir);

    if (sscanf_s(minXLogFileName, "%08X%08X%08X", &tli, &xlogReadLogid, &xlogReadLogSeg) != 3) {
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1,
                        "failed to translate name to xlog: %s\n", minXLogFileName);
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
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1, "reader allocate failed.\n");
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
        return InvalidXLogRecPtr;
    }

    /* Start to find the min lsn from a valid xlogfile */
    startLsn = (xlogReadLogSeg * XLogSegSize) + ((XLogRecPtr)xlogReadLogid * XLogSegmentsPerXLogId * XLogSegSize);
    while (!XLogRecPtrIsInvalid(startLsn)) {
        curLsn = XLogFindNextRecord(xlogReader, startLsn);
        if (XLogRecPtrIsInvalid(curLsn)) {
            if (xlogreadfd > 0) {
                close(xlogreadfd);
                xlogreadfd = -1;
            }
            startLsn = startLsn + XLogSegSize;
            continue;
        } else {
            findValidXLogFile = true;
            break;
        }
    }
    if (findValidXLogFile == false) {
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1,
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

    minLsn = curLsn;
    if (XLogRecPtrIsInvalid(minLsn)) {
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1, "%s", errorMsg);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
    } else {
        rc = snprintf_s(returnMsg, msgLen, msgLen - 1,
            "find min lsn rec (%X/%X) success.\n", (uint32)(minLsn >> 32), (uint32)minLsn);
#ifndef FRONTEND
        securec_check_ss(rc, "", "");
#else
        securec_check_ss_c(rc, "", "");
#endif
    }
    record = XLogReadRecord(xlogReader, curLsn, &errorMsg);
    *minLsnCrc = record->xl_crc;

    /* Free all opened resources */
    if (xlogReader != NULL) {
        XLogReaderFree(xlogReader);
        xlogReader = NULL;
    }
    if (xlogreadfd > 0) {
        close(xlogreadfd);
        xlogreadfd = -1;
    }

    return minLsn;
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

/*
 * @Description: Read library file length.
 * @in bufptr: Library ptr head.
 * @in nlibrary: Library number.
 * @return: Library length.
 */
int read_library(char *bufptr, int nlibrary)
{
    int nlib = nlibrary;
    int over_length = 0;
    char *ptr = bufptr;

    while (nlib > 0) {
        int libraryLen = 0;
        errno_t rc = memcpy_s(&libraryLen, sizeof(int), ptr, sizeof(int));
        securec_check_c(rc, "", "");

        over_length += (sizeof(int) + libraryLen);
        ptr += (sizeof(int) + libraryLen);
        nlib--;
    }

    return over_length;
}

char *GetRepOriginPtr(char *xnodes, uint64 xinfo, int nsubxacts, int nmsgs, int nrels, int nlibrary, bool compress)
{
    if (!(xinfo & XACT_HAS_ORIGIN)) {
        return NULL;
    }
#ifndef ENABLE_MULTIPLE_NODES
    /* One more recent_xmin for single node */
    nsubxacts++;
#endif
    char *libPtr = xnodes + (nrels * SIZE_OF_COLFILENODE(compress)) +
                   (nsubxacts * sizeof(TransactionId)) + (nmsgs * sizeof(SharedInvalidationMessage));
    int libLen = read_library(libPtr, nlibrary);
    return (libPtr + libLen);
}
