/* -------------------------------------------------------------------------
*
* nodeSpqSeqscan.cpp
*	  Support routines for sequential scans of relations.
*
* Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
*
*
* IDENTIFICATION
*	  src/gausskernel/runtime/executor/nodeSpqSeqscan.cpp
*
* -------------------------------------------------------------------------
*
* INTERFACE ROUTINES
*		ExecSeqScan			sequentially scans a relation.
*		ExecSeqNext			retrieve next tuple in sequential order.
*		ExecInitSeqScan			creates and initializes a seqscan node.
*		ExecEndSeqScan			releases any storage allocated.
*		ExecReScanSeqScan		rescans the relation
*		ExecSeqMarkPos			marks scan position
*		ExecSeqRestrPos			restores scan position
 */
#ifdef USE_SPQ
#include "executor/node/nodeSeqscan.h"
#include "executor/node/nodeSpqSeqscan.h"
 
THR_LOCAL init_spqscan_hook_type init_spqscan_hook = nullptr;
THR_LOCAL exec_spqscan_hook_type exec_spqscan_hook = nullptr;
THR_LOCAL end_spqscan_hook_type end_spqscan_hook = nullptr;
THR_LOCAL spqscan_rescan_hook_type spqscan_rescan_hook = nullptr;
 
/* ----------------------------------------------------------------
 *		ExecSeqMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void ExecSpqSeqMarkPos(SpqSeqScanState* node)
{
    ExecSeqMarkPos((SeqScanState*)node);
}
 
/* ----------------------------------------------------------------
 *		ExecSeqRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void ExecSpqSeqRestrPos(SpqSeqScanState* node)
{
    ExecSeqRestrPos((SeqScanState*)node);
}
 
#endif
