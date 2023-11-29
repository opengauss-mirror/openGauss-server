/*-------------------------------------------------------------------------
 *
 * nodeShareInputScan.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012-2021 VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/node/nodeShareInputScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESHAREINPUTSCAN_H
#define NODESHAREINPUTSCAN_H
 
#ifdef USE_SPQ
#include "nodes/execnodes.h"
#include "storage/sharedfileset.h"
 
extern ShareInputScanState *ExecInitShareInputScan(ShareInputScan *node, EState *estate, int eflags);
extern void ExecEndShareInputScan(ShareInputScanState *node);
extern void ExecReScanShareInputScan(ShareInputScanState *node);
extern TupleTableSlot *ExecShareInputScan(PlanState *pstate);
 
extern Size ShareInputShmemSize(void);
extern void ShareInputShmemInit(void);
 
extern SharedFileSet *get_shareinput_fileset(void);
 
extern void tuplestore_make_shared(Tuplestorestate *state, SharedFileSet *fileset, const char *filename);
extern void tuplestore_freeze(Tuplestorestate *state);
extern Tuplestorestate *tuplestore_open_shared(SharedFileSet *fileset, const char *filename);
#endif /* USE_SPQ */
 
#endif   /* NODESHAREINPUTSCAN_H */
