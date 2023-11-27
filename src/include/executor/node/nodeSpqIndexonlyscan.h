/* -------------------------------------------------------------------------
*
* nodeSpqIndexonlyscan.h
*
* Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
*
* src/include/executor/node/nodeSpqIndexonlyscan.h
*
* -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#ifndef NODESPQINDEXONLYSCAN_H
#define NODESPQINDEXONLYSCAN_H

#include "nodes/execnodes.h"

typedef IndexOnlyScanState* (*init_spqindexonlyscan_hook_type)(SpqIndexOnlyScan* node, EState* estate, int eflags);
typedef TupleTableSlot* (*exec_spqindexonlyscan_hook_type)(PlanState* node);

extern THR_LOCAL init_spqindexonlyscan_hook_type init_indexonlyscan_hook;
extern THR_LOCAL exec_spqindexonlyscan_hook_type exec_indexonlyscan_hook;

#endif  // NODESPQINDEXONLYSCAN_H
#endif