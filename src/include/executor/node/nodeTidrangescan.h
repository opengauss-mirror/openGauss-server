/* -------------------------------------------------------------------------
 *
 * nodeTidscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeTidscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODETIDRANGESCAN_H
#define NODETIDRANGESCAN_H

#include "nodes/execnodes.h"

extern TidScanState* ExecInitTidRangeScan(TidRangeScan* node, EState* estate, int eflags);
extern void ExecEndTidRangeScan(TidRangeScanState* node);
extern void ExecReScanTidRangeScan(TidRangeScanState *node);

#endif /* NODETIDRANGESCAN_H */
