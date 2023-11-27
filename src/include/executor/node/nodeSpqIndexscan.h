/* -------------------------------------------------------------------------
 *
 * nodeSpqIndexscan.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * src/include/executor/node/nodeSpqIndexscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#ifndef NODESPQINDEXSCAN_H
#define NODESPQINDEXSCAN_H

#include "nodes/execnodes.h"

typedef IndexScanState* (*init_spqindexscan_hook_type)(SpqIndexScan* node, EState* estate, int eflags);
typedef TupleTableSlot* (*exec_spqindexscan_hook_type)(PlanState* node);

extern THR_LOCAL init_spqindexscan_hook_type init_indexscan_hook;
extern THR_LOCAL exec_spqindexscan_hook_type exec_indexscan_hook;

#endif  // NODESPQINDEXSCAN_H
#endif
