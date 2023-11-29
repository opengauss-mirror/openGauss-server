/* -------------------------------------------------------------------------
*
* nodeSpqIndexonlyscan.cpp
*	  Support routines for sequential scans of relations.
*
* Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
*
*
* IDENTIFICATION
*	  src/gausskernel/runtime/executor/nodeSpqIndexonlyscan.cpp
*
* -------------------------------------------------------------------------
 */
#ifdef USE_SPQ
#include "executor/node/nodeSpqIndexonlyscan.h"

THR_LOCAL init_spqindexonlyscan_hook_type init_indexonlyscan_hook = nullptr;
THR_LOCAL exec_spqindexonlyscan_hook_type exec_indexonlyscan_hook = nullptr;
#endif
