/*
 * xlogutils.h
 *
 * Utilities for replaying WAL records.
 *
 * openGauss transaction log manager utility routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogutils.h
 */
#ifndef XLOG_UTILS_H
#define XLOG_UTILS_H

#include "access/xlogreader.h"
#include "storage/buf/bufmgr.h"
#include "postmaster/pagerepair.h"

/* Result codes for XLogReadBufferForRedo[Extended] */
typedef enum {
    BLK_NEEDS_REDO, /* changes from WAL record need to be applied */
    NO_BLK,         /* ddl no need get block */
    BLK_DONE,       /* block is already up-to-date */
    BLK_RESTORED,   /* block was restored from a full-page image */
    BLK_NOTFOUND    /* block was not found (and hence does not need to be
                     * replayed) */
} XLogRedoAction;

typedef enum {
    NOT_PRESENT,
    NOT_INITIALIZED,
    LSN_CHECK_ERROR,
    CRC_CHECK_ERROR,
    SEGPAGE_LSN_CHECK_ERROR,
}InvalidPageType;

extern bool XLogHaveInvalidPages(void);
extern void* XLogGetInvalidPages();

extern void XLogCheckInvalidPages(void);

extern void XLogDropRelation(const RelFileNode& rnode, ForkNumber forknum);
extern void XlogDropRowReation(RelFileNode rnode);
extern void XLogDropDatabase(Oid dbid);
extern void XLogTruncateRelation(RelFileNode rnode, ForkNumber forkNum, BlockNumber nblocks);
extern void XLogTruncateSegmentSpace(RelFileNode rnode, ForkNumber forkNum, BlockNumber nblocks);
extern void XLogDropSegmentSpace(Oid spcNode, Oid dbNode);

extern Buffer XLogReadBufferExtended(const RelFileNode& rnode, ForkNumber forknum, BlockNumber blkno, 
    ReadBufferMode mode, const XLogPhyBlock *pblk, bool tde = false);

extern Buffer XLogReadBufferExtendedForHeapDisk(const RelFileNode &rnode, ForkNumber forknum, BlockNumber blkno,
    ReadBufferMode mode, const XLogPhyBlock *pblk, bool tde = false);
extern Buffer XLogReadBufferExtendedForSegpage(const RelFileNode& rnode, ForkNumber forknum, BlockNumber blkno, ReadBufferMode mode);

extern XLogRedoAction XLogReadBufferForRedo(XLogReaderState* record, uint8 buffer_id, Buffer* buf);
extern Relation CreateFakeRelcacheEntry(const RelFileNode& rnode);
extern Relation CreateCUReplicationRelation(const RelFileNode& rnode, int BackendId, char relpersistence, const char* relname);
extern void FreeFakeRelcacheEntry(Relation fakerel);
extern void log_invalid_page(const RelFileNode &node, ForkNumber forkno, BlockNumber blkno, InvalidPageType type,
                             const XLogPhyBlock *pblk);
extern int read_local_xlog_page(XLogReaderState* state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
    char* cur_page, TimeLineID* pageTLI, char* xlog_path = NULL);
extern void closeXLogRead();
extern bool IsDataBaseDrop(XLogReaderState* record);
extern bool IsTableSpaceDrop(XLogReaderState* record);
extern bool IsTableSpaceCreate(XLogReaderState* record);
extern bool IsBarrierRelated(XLogReaderState *record);
extern bool IsDataBaseCreate(XLogReaderState* record);
extern bool IsSegPageShrink(XLogReaderState *record);
extern bool IsSegPageDropSpace(XLogReaderState *record);

extern Buffer XLogReadBufferExtendedWithoutBuffer(
    RelFileNode rnode, ForkNumber forknum, BlockNumber blkno, ReadBufferMode mode);

extern Buffer XLogReadBufferExtendedWithLocalBuffer(
    RelFileNode rnode, ForkNumber forknum, BlockNumber blkno, ReadBufferMode mode);

extern void XlogUpdateFullPageWriteLsn(Page page, XLogRecPtr lsn);
void XLogSynAllBuffer();
bool ParseStateUseShareBuf();
bool ParseStateUseLocalBuf();
bool ParseStateWithoutCache();

extern void forget_specified_invalid_pages(RepairBlockKey key);
extern void forget_range_invalid_pages(void *pageinfo);
#endif
