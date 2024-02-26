/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * ---------------------------------------------------------------------------------------
 * 
 * xlog_basic.h
 *        Some basic definations of xlog operation .
 * 
 * 
 * IDENTIFICATION
 *        src/include/access/xlog_basic.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef XLOG_BASIC_H
#define XLOG_BASIC_H

#include "c.h"
#include "access/xlogdefs.h"
#include "storage/smgr/relfilenode.h"
#include "access/rmgr.h"
#include "port/pg_crc32c.h"
#include "utils/pg_crc.h"
#include "tde_key_management/data_common.h"
#include "storage/file/fio_device_com.h"
/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN 64


/* size of the buffer allocated for error message. */
#define MAX_ERRORMSG_LEN 1000

/*
 * Each page of XLOG file has a header like this:
 */
#define XLOG_PAGE_MAGIC 0xD074     /* can be used as WAL version indicator */

/*
 * The XLOG is split into WAL segments (physical files) of the size indicated
 * by XLOG_SEG_SIZE.
 */
#define XLogSegSize XLogSegmentSize
#define XLogSegmentsPerXLogId (UINT64CONST(0x100000000) / XLogSegmentSize)
#define XLogRecordMaxSize ((uint32)0x3fffe000) /* 1 gigabyte - 8 kbyte */
#define XLogBaseSize (16ULL * 1024 * 1024)
#define XLogSegmentsNum(val) (((val) * XLogBaseSize + XLogSegSize - 1) / XLogSegSize)


#define XLogPreReadSize 4194304 // 4MB

/* Compute XLogRecPtr with segment number and offset. */
#define XLogSegNoOffsetToRecPtr(segno, offset, dest) \
    (dest) = (segno) * XLogSegmentSize + (offset)

/*
 * Compute ID and segment from an XLogRecPtr.
 *
 * For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
 * a boundary byte is taken to be in the previous segment.	This is suitable
 * for deciding which segment to write given a pointer to a record end,
 * for example.
 */
#define XLByteToSeg(xlrp, logSegNo) logSegNo = (xlrp) / XLogSegSize

#define XLByteToPrevSeg(xlrp, logSegNo) logSegNo = ((xlrp)-1) / XLogSegSize

/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 */
#define XLByteInSeg(xlrp, logSegNo) (((xlrp) / XLogSegSize) == (logSegNo))

#define XLByteInPrevSeg(xlrp, logSegNo) ((((xlrp)-1) / XLogSegSize) == (logSegNo))

#define XLByteInPreReadBuf(xlrp, preReadStartPtr) \
    (((xlrp) >= (preReadStartPtr)) && ((xlrp) < (preReadStartPtr + XLogPreReadSize)))

/* Check if an XLogRecPtr value is in a plausible range */
#define XRecOffIsValid(xlrp) ((xlrp) % XLOG_BLCKSZ >= SizeOfXLogShortPHD)

/*
 * The XLog directory and control file (relative to $PGDATA)
 */
#define SS_XLOGDIR (g_instance.datadir_cxt.xlogDir)
#define SS_XLOGRECOVERYDIR (g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir)
#define XLOGDIR "pg_xlog"
#define ARCHIVEDIR "pg_xlog/archive_status"
#define XLOG_CONTROL_FILE (g_instance.datadir_cxt.controlPath)
#define XLOG_CONTROL_FILE_BAK (g_instance.datadir_cxt.controlBakPath)
#define MAX_PAGE_FLUSH_LSN_FILE           "global/max_page_flush_lsn"

#define PG_LSN_XLOG_FLUSH_CHK_FILE "global/pg_lsnxlogflushchk"
#define REDO_STATS_FILE             "redo.state"
#define REDO_STATS_FILE_TMP         "redo.state.tmp"

#define SS_DORADO_CTRL_FILE (g_instance.datadir_cxt.controlInfoPath)


#define InvalidRepOriginId 0
#define InvalidXlogPreReadStartPtr 0xFFFFFFFFFFFFFFFF

/*
 * Block IDs used to distinguish different kinds of record fragments. Block
 * references are numbered from 0 to XLR_MAX_BLOCK_ID. A rmgr is free to use
 * any ID number in that range (although you should stick to small numbers,
 * because the WAL machinery is optimized for that case). A couple of ID
 * numbers are reserved to denote the "main" data portion of the record.
 *
 * The maximum is currently set at 32, quite arbitrarily. Most records only
 * need a handful of block references, but there are a few exceptions that
 * need more.
 */
#define XLR_MAX_BLOCK_ID 32

#define XLR_BLOCK_ID_DATA_SHORT 255
#define XLR_BLOCK_ID_DATA_LONG 254
#define XLR_BLOCK_ID_ORIGIN 253

/*
 * The fork number fits in the lower 4 bits in the fork_flags field. The upper
 * bits are used for flags.
 */
#define BKPBLOCK_FORK_MASK 0x0F
#define BKPBLOCK_FLAG_MASK 0xF0
#define BKPBLOCK_HAS_IMAGE 0x10 /* block data is an XLogRecordBlockImage */
#define BKPBLOCK_HAS_DATA 0x20
#define BKPBLOCK_WILL_INIT 0x40 /* redo will re-init the page */
#define BKPBLOCK_SAME_REL 0x80  /* RelFileNode omitted, same as previous */

/*
 * The first bit in seg_fileno field denates whether the xlog record contains VM 
 * physical block location
 */
#define BKPBLOCK_HAS_VM_LOC 0x80
#define BKPBLOCK_GET_SEGFILENO(fileno) ((fileno) & 0x3F)

typedef struct XLogReaderState XLogReaderState;

/* Function type definition for the read_page callback */
typedef int (*XLogPageReadCB)(XLogReaderState* xlogreader, XLogRecPtr targetPagePtr, int reqLen,
    XLogRecPtr targetRecPtr, char* readBuf, TimeLineID* pageTLI, char* xlog_path);

typedef struct {
    /* Is this block ref in use? */
    bool in_use;

    /* Identify the block this refers to */
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blkno;

    /* Segment block number */
    uint8 seg_fileno;
    BlockNumber seg_blockno;
    bool has_vm_loc;
    uint8 vm_seg_fileno;
    BlockNumber vm_seg_blockno;

    /* copy of the fork_flags field from the XLogRecordBlockHeader */
    uint8 flags;

    /* Information on full-page image, if any */
    bool has_image;
    char* bkp_image;
    uint16 hole_offset;
    uint16 hole_length;

    /* Buffer holding the rmgr-specific data associated with this block */
    bool has_data;
    char* data;
    uint16 data_len;
    uint16 data_bufsz;
    XLogRecPtr last_lsn;
    uint16 extra_flag;
#ifdef USE_ASSERT_CHECKING
    uint8 replayed;
#endif
    TdeInfo* tdeinfo;
} DecodedBkpBlock;

/*
 * The overall layout of an XLOG record is:
 *		Fixed-size header (XLogRecord struct)
 *		XLogRecordBlockHeader struct
 *		XLogRecordBlockHeader struct
 *		...
 *		XLogRecordDataHeader[Short|Long] struct
 *		block data
 *		block data
 *		...
 *		main data
 *
 * There can be zero or more XLogRecordBlockHeaders, and 0 or more bytes of
 * rmgr-specific data not associated with a block.  XLogRecord structs
 * always start on MAXALIGN boundaries in the WAL files, but the rest of
 * the fields are not aligned.
 *
 * The XLogRecordBlockHeader, XLogRecordDataHeaderShort and
 * XLogRecordDataHeaderLong structs all begin with a single 'id' byte. It's
 * used to distinguish between block references, and the main data structs.
 */
typedef struct XLogRecord {
    uint32 xl_tot_len; /* total len of entire record */
    uint32 xl_term;
    TransactionId xl_xid; /* xact id */
    XLogRecPtr xl_prev;   /* ptr to previous record in log */
    uint8 xl_info;        /* flag bits, see below */
    RmgrId xl_rmid;       /* resource manager for this record */
    uint2  xl_bucket_id;    /* stores bucket id */
    pg_crc32c xl_crc; /* CRC for this record */

    /* XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding */
} XLogRecord;

struct XLogReaderState {
    /*
     * 
     * Public parameters
     * 
     */

    /*
     * Data input callback (mandatory).
     *
     * This callback shall read at least reqLen valid bytes of the xlog page
     * starting at targetPagePtr, and store them in readBuf.  The callback
     * shall return the number of bytes read (never more than XLOG_BLCKSZ), or
     * -1 on failure.  The callback shall sleep, if necessary, to wait for the
     * requested bytes to become available.  The callback will not be invoked
     * again for the same page unless more than the returned number of bytes
     * are needed.
     *
     * targetRecPtr is the position of the WAL record we're reading.  Usually
     * it is equal to targetPagePtr + reqLen, but sometimes xlogreader needs
     * to read and verify the page or segment header, before it reads the
     * actual WAL record it's interested in.  In that case, targetRecPtr can
     * be used to determine which timeline to read the page from.
     *
     * The callback shall set *pageTLI to the TLI of the file the page was
     * read from.	It is currently used only for error reporting purposes, to
     * reconstruct the name of the WAL file where an error occurred.
     */
    XLogPageReadCB read_page;

    /*
     * System identifier of the xlog files we're about to read.  Set to zero
     * (the default value) if unknown or unimportant.
     */
    uint64 system_identifier;

    /*
     * Opaque data for callbacks to use.  Not used by XLogReader.
     */
    void* private_data;
        /* Buffer to hold error message */
    char* errormsg_buf;
    /*
     * Start and end point of last record read.  EndRecPtr is also used as the
     * position to read next, if XLogReadRecord receives an invalid recptr.
     */
    XLogRecPtr ReadRecPtr; /* start of last record read */
    XLogRecPtr EndRecPtr;  /* end+1 of last record read */

    RepOriginId record_origin;

    /* ----------------------------------------
     * Decoded representation of current record
     *
     * Use XLogRecGet* functions to investigate the record; these fields
     * should not be accessed directly.
     * ----------------------------------------
     */
    XLogRecord* decoded_record; /* currently decoded record */

    char* main_data;        /* record's main data portion */
    uint32 main_data_len;   /* main data portion's length */
    uint32 main_data_bufsz; /* allocated size of the buffer */

    /* 
     * Information about blocks referenced by the record.
     */
    DecodedBkpBlock blocks[XLR_MAX_BLOCK_ID + 1];

    int max_block_id; /* highest block_id in use (-1 if none) */

    /* ----------------------------------------
     * private/internal state
     * ----------------------------------------
     */

    /*
     * Buffer for currently read page (XLOG_BLCKSZ bytes, valid up to at least
     * readLen bytes)
     */
    char* readBuf;
    uint32 readLen;
    char* readBufOrigin;

    /* per-reading for dss */
    XLogRecPtr preReadStartPtr;
    char* preReadBuf;
    char* preReadBufOrigin;
    
    /* streaming replication during pre-reading for dss */
    XLogRecPtr xlogFlushPtrForPerRead;

    /* last read segment, segment offset, TLI for data currently in readBuf */
    XLogSegNo readSegNo;
    uint32 readOff;
    TimeLineID readPageTLI;

    /* the current read segment, segment offset */
    XLogSegNo curReadSegNo;
    uint32 curReadOff;

    /*
     * Beginning of prior page read, and its TLI.  Doesn't necessarily
     * correspond to what's in readBuf; used for timeline sanity checks.
     */
    XLogRecPtr latestPagePtr;
    TimeLineID latestPageTLI;

    /* beginning of the WAL record being read. */
    XLogRecPtr currRecPtr;

    /* Buffer for current ReadRecord result (expandable) */
    char* readRecordBuf;
    uint32 readRecordBufSize;

    // For parallel recovery
    bool isPRProcess;
    bool isDecode;
    bool isFullSync;
    /* add for batch redo */
    uint32 refcount;
    bool isTde;
    uint32 readblocks;
};

#define SizeOfXLogRecord (offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c))

typedef struct XLogPageHeaderData {
    uint16 xlp_magic;        /* magic value for correctness checks */
    uint16 xlp_info;         /* flag bits, see below */
    TimeLineID xlp_tli;      /* TimeLineID of first record on page */
    XLogRecPtr xlp_pageaddr; /* XLOG address of this page */

    /*
     * When there is not enough space on current page for whole record, we
     * continue on the next page.  xlp_rem_len is the number of bytes
     * remaining from a previous page.
     *
     * Note that xl_rem_len includes backup-block data; that is, it tracks
     * xl_tot_len not xl_len in the initial header.  Also note that the
     * continuation data isn't necessarily aligned.
     */
    uint32 xlp_rem_len; /* total len of remaining data for record */
    uint32 xlp_total_len;
} XLogPageHeaderData;

#define SizeOfXLogShortPHD MAXALIGN(sizeof(XLogPageHeaderData))

typedef XLogPageHeaderData* XLogPageHeader;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)	The additional fields serve to identify the file accurately.
 */
typedef struct XLogLongPageHeaderData {
    XLogPageHeaderData std; /* standard header fields */
    uint64 xlp_sysid;       /* system identifier from pg_control */
    uint32 xlp_seg_size;    /* just as a cross-check */
    uint32 xlp_xlog_blcksz; /* just as a cross-check */
} XLogLongPageHeaderData;
typedef struct PageLsnInfo
{
	XLogRecPtr	max_page_flush_lsn;
	pg_crc32c	crc;
} PageLsnInfo;

#define SizeOfXLogLongPHD MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData* XLogLongPageHeader;

/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD 0x0001
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER 0x0002
/* This flag indicates backup blocks starting in this page are optional */
#define XLP_BKP_REMOVABLE 0x0004
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define XLP_ALL_FLAGS 0x0007

/*
 * Macros for manipulating XLOG pointers
 */
#define XLogPageHeaderSize(hdr) (((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)

/* Align a record pointer to next page */
#define NextLogPage(recptr)                                                \
    do {                                                                   \
        if ((recptr) % XLOG_BLCKSZ != 0)                                   \
            XLByteAdvance(recptr, (XLOG_BLCKSZ - (recptr) % XLOG_BLCKSZ)); \
    } while (0)

/* transform old version LSN into new version. */
#define XLogRecPtrSwap(x) (((uint64)((x).xlogid)) << 32 | (x).xrecoff)

typedef struct XLogPhyBlock {
    Oid         relNode;
    BlockNumber block;
    XLogRecPtr  lsn;
} XLogPhyBlock;

const uint16 PAD_SIZE = 500;
typedef struct ShareStorageXLogCtl_ {
    uint16 magic;
    uint16 length;
    uint32 version;
    uint64 systemIdentifier;
    XLogRecPtr insertHead;
    XLogRecPtr insertTail;
    uint64 checkNumber;   // new member add after checkNumber
    uint64 xlogFileSize;
    // reserved fields
    uint32 term;
    uint32 pad1;
    uint64 pad2;
    uint64 pad3;
    uint32 pad4;
    pg_crc32c crc;
    uint8 pad[PAD_SIZE];  // just make sizeof(ShareStorageXLogCtl) >= 512
} ShareStorageXLogCtl;
#define SizeOfShareStorageXLogCtl (offsetof(ShareStorageXLogCtl, pad))
typedef struct ShareStorageXLogCtlAlloc_ {
    ShareStorageXLogCtl ctlInfo;
    void *originPointer;
}ShareStorageXLogCtlAllocBlock;
static const int ShareStorageBufSize = (1 << 20);  // 1MB
static const int ShareStorageAlnSize = (1 << 26);  // 64MB
const uint16 SHARE_STORAGE_CTL_MAGIC = 0x6C69;
const uint32 CURRENT_SHARE_STORAGE_CTL_VERSION = 0;
const uint64 SHARE_STORAGE_CTL_CHCK_NUMBER = 0x444F5241444F0630;
typedef struct ShareStorageOpereate_ {
    void (*ReadCtlInfo)(ShareStorageXLogCtl*);
    void (*WriteCtlInfo)(const ShareStorageXLogCtl*);
    int (*readXlog)(XLogRecPtr startLsn, char* buf, int readLen);
    int (*WriteXlog)(XLogRecPtr startLsn, char* buf, int writeLen);
    void (*fsync)();
} ShareStorageOperateIf;
typedef struct ShareStorageOperateCtl_ {
    bool isInit;
    int fd;
    const char* xlogFilePath;
    uint64 xlogFileSize;
    uint32 blkSize;
    const ShareStorageOperateIf *opereateIf;
} ShareStorageOperateCtl;

extern List *readTimeLineHistory(TimeLineID targetTLI);

#endif /* XLOG_BASIC_H */

