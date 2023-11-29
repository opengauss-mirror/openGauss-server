/* -------------------------------------------------------------------------
*
* nodeSpqBitmapHeapscan.cpp
*	  Support routines for sequential scans of relations.
*
* Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
*
*
* IDENTIFICATION
*	  src/gausskernel/runtime/executor/nodeSpqBitmapHeapscan.cpp
*
* -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "executor/node/nodeSpqBitmapHeapscan.h"

THR_LOCAL init_spqbitmapheapscan_hook_type init_bitmapheapscan_hook = nullptr;
THR_LOCAL exec_spqbitmapheapscan_hook_type exec_bitmapheapscan_hook = nullptr;
#endif
