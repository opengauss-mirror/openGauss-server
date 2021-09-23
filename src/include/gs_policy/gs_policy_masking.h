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
 * gs_policy_masking.h
 *
 * IDENTIFICATION
 *    src/include/gs_policy/gs_policy_masking.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _PG_POLICY_MASKING_H
#define _PG_POLICY_MASKING_H

#include "gs_policy/gs_string.h"
#include "postgres.h"
#include "nodes/parsenodes.h"

extern void create_masking_policy(CreateMaskingPolicyStmt *stmt);

extern void alter_masking_policy(AlterMaskingPolicyStmt *stmt);

extern void drop_masking_policy(DropMaskingPolicyStmt *stmt);
extern void parse_function_name(const char* func_name, char** parsed_funcname, Oid& schemaid);
extern bool IsMaskingFunctionOid(Oid funcid);
extern bool IsUDF(const char *funcname, Oid nspid);
extern bool is_masked_relation(Oid relid, const char *column = NULL);
extern bool is_masked_relation_enabled(Oid relid);
#endif

