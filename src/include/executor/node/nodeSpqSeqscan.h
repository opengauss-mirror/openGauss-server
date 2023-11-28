/* -------------------------------------------------------------------------
 *
 * nodeSpqSeqscan.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * src/include/executor/node/nodeSpqSeqscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#ifndef NODESPQSEQSCAN_H
#define NODESPQSEQSCAN_H
 
#include "nodes/execnodes.h"
 
typedef SpqSeqScanState* (*init_spqscan_hook_type)(SpqSeqScan* node, EState* estate, int eflags);
typedef TupleTableSlot* (*exec_spqscan_hook_type)(PlanState* node);
typedef void (*end_spqscan_hook_type)(SpqSeqScanState* node);
typedef void (*spqscan_rescan_hook_type)(SpqSeqScanState* node);
 
extern THR_LOCAL init_spqscan_hook_type init_spqscan_hook;
extern THR_LOCAL exec_spqscan_hook_type exec_spqscan_hook;
extern THR_LOCAL end_spqscan_hook_type end_spqscan_hook;
extern THR_LOCAL spqscan_rescan_hook_type spqscan_rescan_hook;
 
// unchanged function compare with seqscan
extern void ExecSpqSeqMarkPos(SpqSeqScanState* node);
extern void ExecSpqSeqRestrPos(SpqSeqScanState* node);
 
#endif  // NODESPQSEQSCAN_H
#endif
