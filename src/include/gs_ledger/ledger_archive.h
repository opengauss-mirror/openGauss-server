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
 * userchain.h
 *    Declaration of functions for archive history table.
 *
 * IDENTIFICATION
 *    src/include/gs_ledger/userchain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_LEDGER_ARCHIVE_H
#define GS_LEDGER_ARCHIVE_H

/* ledger check function */
extern Datum ledger_hist_archive(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_archive(PG_FUNCTION_ARGS);

#endif