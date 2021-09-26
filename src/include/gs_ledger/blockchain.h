/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * blockchain.h
 *     Declaration of functions for gchain records.
 *
 * IDENTIFICATION
 *    src/include/gs_ledger/blockchain.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_LEDGER_H
#define GS_LEDGER_H

#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "pgxc/groupmgr.h"
#include "opfusion/opfusion_util.h"
#include "utils/uuid.h"

bool gen_global_hash(hash32_t *hash_buffer, const char *info_string, bool exist, const hash32_t *prev_hash);
void ledger_hook_init(void);
void ledger_hook_fini(void);
void light_ledger_ExecutorEnd(Query *query, uint64 relhash);
void opfusion_ledger_ExecutorEnd(FusionType fusiontype, Oid relid, const char *query, uint64 rel_hash);

void ledger_gchain_append(Oid relid, const char *query_string, uint64 cn_hash);
char *set_gchain_comb_string(const char *db_name, const char *user_name,
    const char *nsp_name, const char *rel_name, const char *cmdtext, uint64 rel_hash);
#endif