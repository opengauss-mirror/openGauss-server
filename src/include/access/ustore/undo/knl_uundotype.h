/* -------------------------------------------------------------------------
 *
 * knl_uundotype.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundotype.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOTYPE_H__
#define __KNL_UUNDOTYPE_H__

#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace.h"
#include "storage/buf/bufpage.h"

/* The type used to identify an undo log and position within it. */
typedef uint64 UndoRecPtr;

/* The type used for undo record lengths. */
typedef uint16 UndoRecordSize;

/* Type for offsets within undo logs */
typedef uint64 UndoLogOffset;

/* Type for offsets within undo transaction slot */
typedef uint64 UndoSlotOffset;

typedef uint64 UndoSlotPtr;

const uint32 USTORE_VERSION = 92350;

const int32 UNDO_ZONE_COUNT = 1024*1024;

/* Parameter for calculating the number of locks used in undo zone. */
#define UNDO_ZONE_LOCK 4

const int PAGES_READ_NUM = 1024 * 16;

#define UNDO_LOG_MAX_SIZE ((UndoLogOffset) 1L << 44)

#define INVALID_ZONE_ID -1

#define MAX_UNDO_RECORD_SIZE (1024 << 1)

/* Number of blocks of BLCKSZ in an undo log segment file.  128 = 1MB. */
#define UNDOSEG_SIZE 128

/* Size of an undo log segment file in bytes. */
#define UNDO_LOG_SEGMENT_SIZE ((size_t)BLCKSZ * UNDOSEG_SIZE)

/* Number of blocks of BLCKSZ in an undo meta segment file. */
#define UNDO_META_SEG_SIZE 4

/* Size of an undo meta segment file in bytes. */
#define UNDO_META_SEGMENT_SIZE ((size_t)BLCKSZ * UNDO_META_SEG_SIZE)

/* Special value for undo record pointer which indicates that it is invalid. */
#define INVALID_UNDO_REC_PTR ((UndoRecPtr)0)

/* Special value for undo record pointer which indicates that it is invalid. */
#define INVALID_UNDO_SLOT_PTR ((UndoSlotPtr)0)

#define IS_VALID_UNDO_REC_PTR(urecptr) ((bool)((UndoRecPtr)(urecptr) != INVALID_UNDO_REC_PTR))

#define UNDO_REC_PTR_FORMAT "%016lX"

/* The width of an undo log number in bits.  20 allows for 1048576 logs. */
#define UNDO_LOG_NUMBER_BITS 20

/* The width of an undo log offset in bits.  44 allows for 16TB per log. */
#define UNDO_LOG_OFFSET_BITS (64 - UNDO_LOG_NUMBER_BITS)

/* Extract the undo log number from an Undo Ptr. */
#define UNDO_PTR_GET_ZONE_ID(urp) ((urp) >> UNDO_LOG_OFFSET_BITS)

/* Make an UndoRecPtr from an log number and offset. */
#define MAKE_UNDO_PTR(zid, offset) (((uint64)(zid) << UNDO_LOG_OFFSET_BITS) | (offset))

#define MAKE_UNDO_PTR_ZONE(urp) UNDO_PTR_GET_ZONE_ID(urp) << UNDO_LOG_OFFSET_BITS

#define MAKE_UNDO_PTR_ALIGN(urp) ((MAKE_UNDO_PTR_ZONE(urp)) | (UNDO_PTR_GET_BLOCK_NUM(urp) * BLCKSZ))

/* Extract the offset from an UndoRecPtr. */
#define UNDO_PTR_GET_OFFSET(urp) ((urp) & ((UINT64CONST(1) << UNDO_LOG_OFFSET_BITS) - 1))

/* Compute the offset of a given UndoRecPtr in the page that holds it. */
#define UNDO_PTR_GET_PAGE_OFFSET(urp) (UNDO_PTR_GET_OFFSET(urp) % BLCKSZ)

/* The number of unusable bytes in the header of each block. */
#define UNDO_LOG_BLOCK_HEADER_SIZE SizeOfPageHeaderData

/* The number of usable bytes we can store per block. */
#define UNDO_LOG_USABLE_BYTES_PER_PAGE (BLCKSZ - UNDO_LOG_BLOCK_HEADER_SIZE)

/* How many non-header bytes are there before a given offset? */
#define UNDO_LOG_OFFSET_TO_USABLE_BYTE_NO(offset) \
    (((offset) % BLCKSZ - UNDO_LOG_BLOCK_HEADER_SIZE) + ((offset) / BLCKSZ) * UNDO_LOG_USABLE_BYTES_PER_PAGE)

/* What is the offset of the i'th non-header byte? */
#define UNDO_LOG_OFFSET_FROM_USABLE_BYTE_NO(i)                                      \
    (((i) / UNDO_LOG_USABLE_BYTES_PER_PAGE) * BLCKSZ + UNDO_LOG_BLOCK_HEADER_SIZE + \
        ((i) % UNDO_LOG_USABLE_BYTES_PER_PAGE))

/* Add 'n' usable bytes to offset stepping over headers to find new offset. */
#define UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(offset, n) \
    UNDO_LOG_OFFSET_FROM_USABLE_BYTE_NO(UNDO_LOG_OFFSET_TO_USABLE_BYTE_NO(offset) + (n))

/* Compute the block number that holds a given UndoRecPtr. */
#define UNDO_PTR_GET_BLOCK_NUM(urp) (UNDO_PTR_GET_OFFSET(urp) / BLCKSZ)

/* Extract the relnode for an undo log. */
#define UNDO_PTR_GET_REL_NODE(urp) UNDO_PTR_GET_ZONE_ID(urp)

/* The only valid fork number for undo log buffers. */
#define UNDO_FORKNUM MAIN_FORKNUM

typedef enum {
    UNDO_PERMANENT = 0,
    UNDO_UNLOGGED = 1,
    UNDO_TEMP = 2,
    UNDO_PERSISTENT_BUTT
} UndoPersistence;

typedef enum {
    UNDO_LOG_SPACE = 0,
    UNDO_SLOT_SPACE = 1,
    UNDO_SPACE_BUTT
} UndoSpaceType;

typedef enum {
    UNDO_TRAVERSAL_DEFAULT = 0,
    UNDO_TRAVERSAL_COMPLETE,
    UNDO_TRAVERSAL_STOP,
    UNDO_TRAVERSAL_ABORT,
    UNDO_TRAVERSAL_END
} UndoTraversalState;

typedef enum {
    UNDO_RECORD_NORMAL = 0,
    UNDO_RECORD_DISCARD,
    UNDO_RECORD_FORCE_DISCARD,
    UNDO_RECORD_NOT_INSERT,
    UNDO_RECORD_INVALID
} UndoRecordState;

#define UNDO_PERSISTENCE_LEVELS 3

#define REL_PERSISTENCE(upersistence)   \
    ((upersistence) == UNDO_PERMANENT ? \
        RELPERSISTENCE_PERMANENT :      \
        (upersistence) == UNDO_UNLOGGED ? RELPERSISTENCE_UNLOGGED : RELPERSISTENCE_TEMP)

/*
 * Convert from relpersistence ('p', 'u', 't') to an UndoPersistence
 * enumerator.
 */
#define UndoPersistenceForRelPersistence(rp) \
    ((rp) == RELPERSISTENCE_PERMANENT ? UNDO_PERMANENT : (rp) == RELPERSISTENCE_UNLOGGED ? UNDO_UNLOGGED : UNDO_TEMP)

/*
 * Get the appropriate UndoPersistence value from a Relation.
 */
#define UndoPersistenceForRelation(rel) (UndoPersistenceForRelPersistence((rel)->rd_rel->relpersistence))


/* Populate a RelFileNode from an UndoRecPtr. */
#define UNDO_PTR_ASSIGN_REL_FILE_NODE(rfn, urp, dbId) \
    do {                                              \
        (rfn).spcNode = DEFAULTTABLESPACE_OID;        \
        (rfn).dbNode = dbId;                          \
        (rfn).relNode = UNDO_PTR_GET_REL_NODE(urp);   \
        (rfn).bucketNode = InvalidBktId;              \
        (rfn).opt = 0;                                \
    } while (false);

#define DECLARE_NODE_COUNT()                                                                      \
    int nodeCount;                                                                                \
    if (g_instance.attr.attr_common.enable_thread_pool && g_instance.shmem_cxt.numaNodeNum > 1) { \
        nodeCount = g_instance.shmem_cxt.numaNodeNum;                                             \
    } else {                                                                                      \
        nodeCount = 1;                                                                            \
    }

#define DECLARE_NODE_NO()                                                                         \
    int nodeNo;                                                                                   \
    if (g_instance.attr.attr_common.enable_thread_pool && g_instance.shmem_cxt.numaNodeNum > 1) { \
        nodeNo = t_thrd.proc->nodeno;                                                             \
    } else {                                                                                      \
        nodeNo = 0;                                                                               \
    }

#define PERSIST_ZONE_COUNT (UNDO_ZONE_COUNT / UNDO_PERSISTENCE_LEVELS)
#define ZONE_COUNT_PER_LEVEL_NODE(nodeCount) (UNDO_ZONE_COUNT / UNDO_PERSISTENCE_LEVELS / nodeCount)
#define ZONE_COUNT_PER_LEVELS(nodeCount) ((ZONE_COUNT_PER_LEVEL_NODE(nodeCount)) * nodeCount)

#define IS_PERSIST_LEVEL(zoneId, nodeCount) (zoneId < ZONE_COUNT_PER_LEVELS(nodeCount))

#define GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount)           \
    UndoPersistence upersistence;                               \
    if (zoneId < (int)ZONE_COUNT_PER_LEVELS(nodeCount)) {            \
        upersistence = UNDO_PERMANENT;                          \
    } else if (zoneId < 2 * (int)ZONE_COUNT_PER_LEVELS(nodeCount)) { \
        upersistence = UNDO_UNLOGGED;                           \
    } else {                                                    \
        upersistence = UNDO_TEMP;                               \
    }

#define UNDO_RET_FAIL -1
#define UNDO_RET_SUCC 0

#define UNDO_UNKNOWN 0x00
#define UNDO_INSERT 0x01
#define UNDO_MULTI_INSERT 0x02
#define UNDO_DELETE 0x03
#define UNDO_INPLACE_UPDATE 0x04
#define UNDO_UPDATE 0x05
#define UNDO_ITEMID_UNUSED 0x09

#define UNDO_UREC_INFO_UNKNOWN 0x00
#define UNDO_UREC_INFO_PAYLOAD 0x01
#define UNDO_UREC_INFO_TRANSAC 0x02
#define UNDO_UREC_INFO_BLOCK 0x04
#define UNDO_UREC_INFO_OLDTD 0x08
#define UNDO_UREC_INFO_CONTAINS_SUBXACT 0x10
#define UNDO_UREC_INFO_HAS_PARTOID 0x20
#define UNDO_UREC_INFO_HAS_TABLESPACEOID 0x40

#define UREC_INPLACE_UPDATE_XOR_PREFIX 0x01
#define UREC_INPLACE_UPDATE_XOR_SUFFIX 0x02

#define UNDODEBUGINFO , __FUNCTION__, __LINE__
#define UNDODEBUGSTR "[%s:%d]"
#define UNDOFORMAT(f) UNDODEBUGSTR f UNDODEBUGINFO

extern const int UNDO_FILE_MAXSIZE;
extern const int UNDO_FILE_BLOCKS;

extern const int UNDO_DIR_LEN;
extern const int UNDO_FILE_PATH_LEN;
extern const int UNDO_FILE_DIR_LEN;

extern const char *UNDO_FILE_DIR_PREFIX;
extern const char *UNDO_PERMANENT_DIR;
extern const char *UNDO_UNLOGGED_DIR;
extern const char *UNDO_TEMP_DIR;

extern int UNDOZONE_META_PAGE_COUNT;
extern int UNDOSPACE_META_PAGE_COUNT;
#endif // __KNL_UUNDOTYPE_H__
