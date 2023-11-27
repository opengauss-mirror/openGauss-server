/* -------------------------------------------------------------------------
*
* nodeSpqIndexscan.cpp
*	  Support routines for sequential scans of relations.
*
* Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
*
*
* IDENTIFICATION
*	  src/gausskernel/runtime/executor/nodeSpqIndexscan.cpp
*
* -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "executor/node/nodeSpqIndexscan.h"

THR_LOCAL init_spqindexscan_hook_type init_indexscan_hook = nullptr;
THR_LOCAL exec_spqindexscan_hook_type exec_indexscan_hook = nullptr;
#endif