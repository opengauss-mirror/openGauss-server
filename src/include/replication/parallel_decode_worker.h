/* ---------------------------------------------------------------------------------------
 *
 * parallel_decode_worker.h
 *        openGauss parallel decoding create worker threads.
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/include/replication/parallel_decode_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PARALLEL_DECODEWORKER_H
#define PARALLEL_DECODEWORKER_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"

#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/snapbuild.h" /* just for SnapBuildSnapDecRefcount */
#include "access/xlog_internal.h"

#include "storage/smgr/fd.h"
#include "storage/sinval.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "lib/binaryheap.h"

#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "knl/knl_thread.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "storage/ipc.h"


extern ParallelDecodeWorker *CreateLogicalDecodeWorker(uint32 id, char* dbUser, char* dbName, char* slotname, uint32 slotId);
extern void SendSignalToDecodeWorker(int signal, int slotId);
extern void parallel_tuple_to_stringinfo(StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool skip_nulls);
extern void LogicalReadRecordMain(ParallelDecodeReaderWorker *worker);
extern int logical_read_xlog_page(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                                  char *cur_page, TimeLineID *pageTLI, char* xlog_path);
#endif
