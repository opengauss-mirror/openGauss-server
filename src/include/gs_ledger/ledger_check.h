/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ledger_check.h
 *    Declaration of functions for check and repair history table.
 *
 * IDENTIFICATION
 *    src/include/gs_ledger/ledger_check.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_LEDGER_CHECK_H
#define GS_LEDGER_CHECK_H

#include "postgres.h"
#include "fmgr/fmgr_comp.h"

/* ledger check function */
extern Datum get_dn_hist_relhash(PG_FUNCTION_ARGS);
extern Datum ledger_hist_check(PG_FUNCTION_ARGS);
extern Datum ledger_hist_repair(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_check(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_repair(PG_FUNCTION_ARGS);

#ifndef ENABLE_MULTIPLE_NODES
Datum check_gchain_relhash_consistent(PG_FUNCTION_ARGS);
#endif

#endif