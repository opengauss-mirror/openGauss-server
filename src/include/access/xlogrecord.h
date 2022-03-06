/* ---------------------------------------------------------------------------------------
 * 
 * xlogrecord.h
 *        Definitions for the WAL record format.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/access/xlogrecord.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef XLOGRECORD_H
#define XLOGRECORD_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "utils/pg_crc.h"
#include "port/pg_crc32c.h"
/*
 * The high 4 bits in xl_info may be used freely by rmgr. The
 * XLR_SPECIAL_REL_UPDATE bit can be passed by XLogInsert caller. The rest
 * are set internally by XLogInsert.
 */
#define XLR_INFO_MASK 0x0F
#define XLR_RMGR_INFO_MASK 0xF0

/*
 * If a WAL record modifies any relation files, in ways not covered by the
 * usual block references, this flag is set. This is not used for anything
 * by openGauss itself, but it allows external tools that read WAL and keep
 * track of modified blocks to recognize such special record types.
 */
#define XLR_SPECIAL_REL_UPDATE 0x01
#define XLR_BTREE_UPGRADE_FLAG 0x02
/* If xlog record is the compress table creation */
#define XLR_REL_COMPRESS       0X04
#define XLR_IS_TOAST           0X08
/* If xlog record is from toast page */

/*
 * Enforces consistency checks of replayed WAL at recovery. If enabled,
 * each record will log a full-page write for each block modified by the
 * record and will reuse it afterwards for consistency checks. The caller
 * of XLogInsert can use this value if necessary, but if
 * wal_consistency_checking is enabled for a rmgr this is set unconditionally.
 */
#define XLR_CHECK_CONSISTENCY   0x03

/*
 * Header info for block data appended to an XLOG record.
 *
 * Note that we don't attempt to align the XLogRecordBlockHeader struct!
 * So, the struct must be copied to aligned local storage before use.
 * 'data_length' is the length of the payload data associated with this,
 * and includes the possible full-page image, and rmgr-specific data. It
 * does not include the XLogRecordBlockHeader struct itself.
 */
typedef struct XLogRecordBlockHeader {
    uint8 id;           /* block reference ID */
    uint8 fork_flags;   /* fork within the relation, and flags */
    uint16 data_length; /* number of payload bytes (not including page
                         * image) */

    /* If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows */
    /* If !BKPBLOCK_SAME_REL is not set, a RelFileNode follows */
    /* BlockNumber follows */
} XLogRecordBlockHeader;

#define SizeOfXLogRecordBlockHeader (offsetof(XLogRecordBlockHeader, data_length) + sizeof(uint16))

/*
 * We use the highest bit of XLogRecordBlockHeader->id for hash bucket table.
 * The RelFileNode is for hash bucket tables, and RelFileNodeOld is for regular tables.
 */
#define BKID_HAS_BUCKET_OR_SEGPAGE (0x80)
/*
 * The second digit from the left of the ID is used as the identifier of the TDE table.
 * Considering that the value of XLR_MAX_BLOCK_ID lower than 32, the original function is not affected.
 */
#define BKID_HAS_TDE_PAGE (0x40)
#define BKID_GET_BKID(id) (id & 0x3F)

/*
 * In segment-page storage, RelFileNode and block number are logic for XLog. Thus, we need record
 * physical location in xlog. This macro is used to check whether in such situation.
 */
#define XLOG_NEED_PHYSICAL_LOCATION(relfilenode) \
    (IsSegmentFileNode(relfilenode) && !IsSegmentPhysicalRelNode(relfilenode))

/*
 * Additional header information when a full-page image is included
 * (i.e. when BKPBLOCK_HAS_IMAGE is set).
 *
 * As a trivial form of data compression, the XLOG code is aware that
 * PG data pages usually contain an unused "hole" in the middle, which
 * contains only zero bytes.  If hole_length > 0 then we have removed
 * such a "hole" from the stored data (and it's not counted in the
 * XLOG record's CRC, either).  Hence, the amount of block data actually
 * present is BLCKSZ - hole_length bytes.
 */
typedef struct XLogRecordBlockImageHeader {
    uint16 hole_offset; /* number of bytes before "hole" */
    uint16 hole_length; /* number of bytes in "hole" */
} XLogRecordBlockImageHeader;

#define SizeOfXLogRecordBlockImageHeader sizeof(XLogRecordBlockImageHeader)

/*
 * Maximum size of the header for a block reference. This is used to size a
 * temporary buffer for constructing the header.
 */
#define MaxSizeOfXLogRecordBlockHeader \
    (SizeOfXLogRecordBlockHeader + SizeOfXLogRecordBlockImageHeader + sizeof(RelFileNode) + sizeof(BlockNumber) \
    + sizeof(BlockNumber) + sizeof(uint8))

/*
 * XLogRecordDataHeaderShort/Long are used for the "main data" portion of
 * the record. If the length of the data is less than 256 bytes, the short
 * form is used, with a single byte to hold the length. Otherwise the long
 * form is used.
 *
 * (These structs are currently not used in the code, they are here just for
 * documentation purposes).
 */
typedef struct XLogRecordDataHeaderShort {
    uint8 id;          /* XLR_BLOCK_ID_DATA_SHORT */
    uint8 data_length; /* number of payload bytes */
} XLogRecordDataHeaderShort;

#define SizeOfXLogRecordDataHeaderShort (sizeof(uint8) * 2)

typedef struct XLogRecordDataHeaderLong {
    uint8 id; /* XLR_BLOCK_ID_DATA_LONG */
              /* followed by uint32 data_length, unaligned */
} XLogRecordDataHeaderLong;

#define SizeOfXLogRecordDataHeaderLong (sizeof(uint8) + sizeof(uint32))
#endif /* XLOGRECORD_H */
