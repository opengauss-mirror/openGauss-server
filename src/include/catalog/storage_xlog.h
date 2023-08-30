/* ---------------------------------------------------------------------------------------
 * 
 * storage_xlog.h
 *        prototypes for functions in backend/catalog/storage.cpp
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/catalog/storage_xlog.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STORAGE_XLOG_H
#define STORAGE_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "lib/ilist.h"
#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/segment.h"

/*
 * Declarations for smgr-related XLOG records
 *
 * Note: we log file creation and truncation here, but logging of deletion
 * actions is handled by xact.c, because it is part of transaction commit.
 */

/* XLOG gives us high 4 bits */
#define XLOG_SMGR_CREATE    0x10
#define XLOG_SMGR_TRUNCATE  0x20

typedef struct xl_smgr_create {
	RelFileNodeOld rnode;
	ForkNumber	forkNum;
} xl_smgr_create;

typedef struct xl_smgr_create_compress {
    xl_smgr_create xlrec;
    uint2 pageCompressOpts;
} xl_smgr_create_compress;

typedef struct xl_smgr_truncate {
	BlockNumber blkno;
	RelFileNodeOld rnode;
} xl_smgr_truncate;

typedef struct xl_smgr_truncate_compress {
    xl_smgr_truncate xlrec;
    uint2 pageCompressOpts;
    TransactionId latest_removed_xid;
} xl_smgr_truncate_compress;

#define TRUNCATE_CONTAIN_XID_SIZE (offsetof(xl_smgr_truncate_compress, latest_removed_xid) + sizeof(TransactionId))

extern void log_smgrcreate(RelFileNode *rnode, ForkNumber forkNum);

extern void smgr_redo(XLogReaderState *record);
extern void smgr_desc(StringInfo buf, XLogReaderState *record);
extern const char* smgr_type_name(uint8 subtype);

extern void smgr_redo_create(RelFileNode rnode, ForkNumber forkNum, char *data);
extern void xlog_block_smgr_redo_truncate(RelFileNode rnode, BlockNumber blkno, XLogRecPtr lsn,
    TransactionId latest_removed_xid);

/* An xlog combined by multiply sub-xlog, it will be decoded again */
#define XLOG_SEG_ATOMIC_OPERATION 0x00
/* Segment head's nblocks++, the corresponding block needs to be set as all-zero */
#define XLOG_SEG_SEGMENT_EXTEND 0x10
/* Create segment space */
#define XLOG_SEG_CREATE_EXTENT_GROUP 0x20
/* Init MapPage */
#define XLOG_SEG_INIT_MAPPAGE 0x30
/* Init Inverse Pointer Page */
#define XLOG_SEG_INIT_INVRSPTR_PAGE 0x40
/* Add new map group */
#define XLOG_SEG_ADD_NEW_GROUP 0x50
/* Truncate Segment  */
#define XLOG_SEG_TRUNCATE 0x60
/* Update space high water mark */
#define XLOG_SEG_SPACE_SHRINK 0x70
/* Drop tablespace */
#define XLOG_SEG_SPACE_DROP 0x80
/* Copy data during shrink */
#define XLOG_SEG_NEW_PAGE 0x90

typedef struct tagxl_new_map_group_info {
    BlockNumber first_map_pageno;
    int extent_size;
    uint16 group_count;
    uint8 group_size;
    uint8 reserved;
} xl_new_map_group_info_t;

typedef struct tagxl_seg_bktentry_tag {
    uint32       bktentry_id;
    BlockNumber  bktentry_header;
} xl_seg_bktentry_tag_t;

extern void log_move_segment_buckets(xl_seg_bktentry_tag_t *mapentry, uint32 nentry, Buffer buffer);
extern void log_move_segment_redisinfo(SegRedisInfo *dredis, SegRedisInfo *sredis, Buffer dbuffer, Buffer sbuffer);
extern void segpage_smgr_redo(XLogReaderState *record);
extern void segpage_smgr_desc(StringInfo buf, XLogReaderState *record);
extern const char* segpage_smgr_type_name(uint8 subtype);

typedef struct XLogDataSpaceAllocateExtent {
    uint32 hwm;
    uint32 allocated_extents;
    uint32 free_group;
    uint32 free_page;
} XLogDataSpaceAllocateExtent;

typedef struct XLogDataUpdateSegmentHead {
    int level0_slot;
    BlockNumber level0_value;
    int level1_slot;
    BlockNumber level1_value;
    uint32 nblocks;
    uint32 total_blocks;
    uint32 nextents;
    BlockNumber old_extent;   // replaced extent when shrink files
} XLogDataUpdateSegmentHead;

// level0 page add a new extent
typedef struct XLogDataSetLevel0Page {
    uint32 slot;
    BlockNumber extent;
} XLogDataSetLevel0Page;

// Add segment head's nblocks
typedef struct XLogDataSegmentExtend {
    BlockNumber old_nblocks;
    BlockNumber new_nblocks;
    BlockNumber ext_size;
    BlockNumber blocknum;

    /* used for parallel-redo and extreme-rto, spcNode and dbNode are the same */
    BlockNumber main_fork_head;
    uint32 reset_zero;
    int forknum;
} XLogDataSegmentExtend;

// Space shrink update hwm
typedef struct XLogDataUpdateSpaceHWM {
    BlockNumber old_hwm;
    BlockNumber new_hwm;
    BlockNumber old_groupcnt;
    BlockNumber new_groupcnt;
} XLogDataUpdateSpaceHWM;

typedef struct XLogDataSpaceShrink {
    RelFileNode rnode;
    int forknum;
    BlockNumber target_size;
} XLogDataSpaceShrink;

typedef struct XLogMoveExtent {
    RelFileNode logic_rnode;
    int forknum;
    uint32 extent_id;
    uint32 nblocks;
    BlockNumber new_extent;
    BlockNumber old_extent;
} XLogMoveExtent;

struct HTAB* redo_create_remain_segs_htbl();
extern void move_extent_flush_buffer(XLogMoveExtent *xlog_data);

/*==========================================CFS redo design====================================*/

struct CfsShrink_t {
    RelFileNode node;
    ForkNumber forknum;
    char parttype;
};

#define XLOG_CFS_SHRINK_OPERATION 0x00

extern void CfsShrinkRedo(XLogReaderState *record);
extern void CfsShrinkDesc(StringInfo buf, XLogReaderState *record);
extern const char* CfsShrinkTypeName(uint8 subtype);

static inline uint2 GetCreateXlogFileNodeOpt(const XLogReaderState *record)
{
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    return compress ? ((xl_smgr_create_compress*)(void *)XLogRecGetData(record))->pageCompressOpts : 0;
}

static inline uint2 GetTruncateXlogFileNodeOpt(const XLogReaderState *record)
{
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    return compress ? ((xl_smgr_truncate_compress*)(void *)XLogRecGetData(record))->pageCompressOpts : 0;
}

#endif   /* STORAGE_XLOG_H */

