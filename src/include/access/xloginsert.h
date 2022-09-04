/* ---------------------------------------------------------------------------------------
 * 
 * xloginsert.h
 *        Functions for generating WAL records
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/access/xloginsert.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef XLOGINSERT_H
#define XLOGINSERT_H

#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufpage.h"
#include "storage/page_compression.h"
#include "storage/smgr/relfilenode.h"

struct XLogPhyBlock;
/*
 * The minimum size of the WAL construction working area. If you need to
 * register more than XLR_NORMAL_MAX_BLOCK_ID block references or have more
 * than XLR_NORMAL_RDATAS data chunks in a single WAL record, you must call
 * XLogEnsureRecordSpace() first to allocate more working memory.
 */
#define XLR_NORMAL_MAX_BLOCK_ID 4
#define XLR_NORMAL_RDATAS 20

/* flags for XLogRegisterBuffer */
#define REGBUF_FORCE_IMAGE 0x01 /* force a full-page image */
#define REGBUF_NO_IMAGE 0x02    /* don't take a full-page image */
#define REGBUF_WILL_INIT                            \
    (0x04 | 0x02) /* page will be re-initialized at \
                   * replay (implies NO_IMAGE) */
#define REGBUF_STANDARD                          \
    0x08 /* page follows "standard" page layout, \
          * (data between pd_lower and pd_upper  \
          * will be skipped) */
#define REGBUF_KEEP_DATA                           \
    0x10 /* include data even if a full-page image \
          * is taken */

/* The time unit is microsecond. */
static const int MAX_RPO_SLEEP_TIME = 500000;
typedef enum XLOG_FPI_FOR_HINT_TYPE {
    XLOG_FPI_FOR_HINT_HEAP = 0,
    XLOG_FPI_FOR_HINT_UHEAP
} XLOG_FPI_FOR_HINT_TYPE;

/* prototypes for public functions in xloginsert.c: */
extern void XLogBeginInsert(void);
extern XLogRecPtr XLogInsert(RmgrId rmid, uint8 info, int bucket_id = InvalidBktId, bool istoast = false);
extern void XLogEnsureRecordSpace(int nbuffers, int ndatas);
extern void XLogRegisterData(char* data, int len);
extern void XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags, TdeInfo* tdeinfo = NULL);
extern void XLogRegisterBlock(uint8 block_id, RelFileNode *rnode, ForkNumber forknum, BlockNumber blknum, char *page,
    uint8 flags, const XLogPhyBlock *pblk, TdeInfo* tdeinfo = NULL);
extern void XLogRegisterBufData(uint8 block_id, char* data, int len);
extern void XLogResetInsertion(void);
extern bool XLogCheckBufferNeedsBackup(Buffer buffer);
extern XLogRecPtr XLogSaveBufferForHint(Buffer buffer, bool buffer_std);
extern void InitXLogInsert(void);
extern void XLogIncludeOrigin(void);
extern void LogCSN(CommitSeqNo *curCSN);
#endif /* XLOGINSERT_H */
