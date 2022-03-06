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
 * masking.h
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/masking.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MASKING_H_
#define MASKING_H_
#include <string>
#include "parser/parse_node.h"
#include "nodes/primnodes.h"
#include "gs_mask_policy.h"

bool parser_target_entry(ParseState *pstate, TargetEntry*& old_tle, const policy_set *policy_ids,
                         masking_result *result, List* rtable, bool can_mask = true);
void reset_node_location();

/* col_type for integer should be int8, int4, int2, int1 */
Node* create_integer_node(ParseState *pstate, int value, int location, int col_type = INT4OID, bool make_cast = true);

void free_masked_cursor_stmts();
void free_masked_prepared_stmts();
void close_cursor_stmt_as_masked(const char* name);
void unprepare_stmt_as_masked(const char* name);
void set_prepare_stmt_as_masked(const char* name, const masking_result *result);
void set_cursor_stmt_as_masked(const char* name, const masking_result *result);
void flush_cursor_stmt_masking_result(const char* name);
void flush_prepare_stmt_masking_result(const char* name);
bool process_union_masking(Node *union_node,
    ParseState *pstate, const Query *query, const policy_set *policy_ids, bool audit_exist);
void process_masking(ParseState *pstate, Query *query, const policy_set *policy_ids, bool audit_exist);
void select_PostParseAnalyze(ParseState *pstate, Query *&query, const policy_set *policy_ids, bool audit_exist);
bool handle_masking(List* targetList, ParseState *pstate,
    const policy_set *policy_ids, List* rtable, Node* utilityNode);

#endif /* MASKING_H_ */
