/* -------------------------------------------------------------------------
 *
 * nodeSpqBitmapHeapscan.h
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 *
 * src/include/executor/node/nodeSpqBitmapHeapscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#ifndef NODESPQBITMAPHEAPSCAN_H
#define NODESPQBITMAPHEAPSCAN_H

#include "nodes/execnodes.h"

typedef BitmapHeapScanState* (*init_spqbitmapheapscan_hook_type)(SpqBitmapHeapScan* node, EState* estate, int eflags);
typedef TupleTableSlot* (*exec_spqbitmapheapscan_hook_type)(PlanState* node);

extern THR_LOCAL init_spqbitmapheapscan_hook_type init_bitmapheapscan_hook;
extern THR_LOCAL exec_spqbitmapheapscan_hook_type exec_bitmapheapscan_hook;

#endif  // NODESPQBITMAPHEAPSCAN_H
#endif
