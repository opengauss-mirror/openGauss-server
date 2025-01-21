/*
 * Copyright (c) 2020-2025 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * auto_parameterization.h
 *
 * IDENTIFICATION
 * /src/include/commands/auto_parameterization.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef AUTO_PARAMETERIZATION_H
#define AUTO_PARAMETERIZATION_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"

#define MAX_PARAM_QUERY_LEN 512
#define PARAM_QUERIES_BUCKET 32

typedef struct ParamCachedKey {
    char parameterized_query[MAX_PARAM_QUERY_LEN];
    int query_len;
    Oid* param_types;
    int num_param;
} ParamCachedKey;

typedef struct ParameterizationInfo {
    const char* query_string;
    Node* parent_node;
    List* param_locs;
    List* param_types;
    List* params;
    bool is_skip;
    int param_count;
} ParameterizationInfo;

typedef struct ParamLocationLen {
    int location;
    int length;
} ParamLocationLen;

typedef struct ParamState {
    ParamLocationLen* clocations;
    int clocations_count;
} ParamState;

bool execQueryParameterization(Node* parsetree, const char* query_string, CommandDest cmdDest, char* completionTag);
bool isQualifiedIuds(Node* parsetree);

#endif  /* OPENGAUSS_SERVER_AUTO_PARAMETERIZATION_H */