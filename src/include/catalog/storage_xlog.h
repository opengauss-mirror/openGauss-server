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
#include "storage/buf/block.h"
#include "storage/relfilenode.h"

/*
 * Declarations for smgr-related XLOG records
 *
 * Note: we log file creation and truncation here, but logging of deletion
 * actions is handled by xact.c, because it is part of transaction commit.
 */

/* XLOG gives us high 4 bits */
#define XLOG_SMGR_CREATE	0x10
#define XLOG_SMGR_TRUNCATE	0x20

typedef struct xl_smgr_create {
	RelFileNodeOld rnode;
	ForkNumber	forkNum;
} xl_smgr_create;

typedef struct xl_smgr_truncate {
	BlockNumber blkno;
	RelFileNodeOld rnode;
} xl_smgr_truncate;

extern void log_smgrcreate(RelFileNode *rnode, ForkNumber forkNum, const oidvector* bucketlist = NULL);

extern void smgr_redo(XLogReaderState *record);
extern void smgr_desc(StringInfo buf, XLogReaderState *record);
extern void smgr_redo_create(RelFileNode rnode, ForkNumber forkNum, char *data);
extern void xlog_block_smgr_redo_truncate(RelFileNode rnode, BlockNumber blkno, XLogRecPtr lsn);

#endif   /* STORAGE_XLOG_H */

