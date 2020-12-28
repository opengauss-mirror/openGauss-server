/* -------------------------------------------------------------------------
 *
 * basebackup.h
 *	  Exports from replication/basebackup.c.
 *
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * src/include/replication/basebackup.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _BASEBACKUP_H
#define _BASEBACKUP_H

#include "nodes/replnodes.h"

/*
 * Maximum file size for a tar member: The limit inherent in the
 * format is 2^33-1 bytes (nearly 8 GB).  But we don't want to exceed
 * what we can represent in pgoff_t.
 */
#define MAX_TAR_MEMBER_FILELEN (((int64)1 << Min(33, sizeof(pgoff_t) * 8 - 1)) - 1)

#define MAX_FILE_SIZE_LIMIT  ((0x80000000))

typedef struct {
    char* oid;
    char* path;
    char* relativePath;
    int64 size;
} tablespaceinfo;

extern XLogRecPtr XlogCopyStartPtr;

extern void SendBaseBackup(BaseBackupCmd* cmd);
extern int64 sendTablespace(const char* path, bool sizeonly);
extern bool is_row_data_file(const char* filePath, int* segNo);
#ifdef ENABLE_MOT
extern void PerformMotCheckpointFetch();
#endif

/* ut test */
extern void ut_save_xlogloc(const char* xloglocation);
#endif /* _BASEBACKUP_H */
