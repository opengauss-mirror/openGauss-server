/* -------------------------------------------------------------------------
*
* nodeSpqIndexonlyscan.cpp
*	  Support routines for sequential scans of relations.
*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
