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
#include "utils/plancache.h"

#define MAX_PARAM_QUERY_LEN 512
#define PARAM_QUERIES_BUCKET 32
#define MAX_PARAM_NODES 64
#define MAX_PARAMETERIZED_QUERY_STORED 512
#define IUD_COMMAND_LEN 7
#define FIXED_QUERY_TYPE_LEN 4
#define QUERY_TYPE_UNKNOWN 0
#define QUERY_TYPE_INSERT 1
#define QUERY_TYPE_UPDATE 2
#define QUERY_TYPE_DELETE 3

typedef struct ParamCachedKey {
    char parameterized_query[MAX_PARAM_QUERY_LEN];
    int query_len;
    Oid param_types[MAX_PARAM_NODES];
    int num_param;
    Oid relOid;
} ParamCachedKey;

typedef struct ParamLocationLen {
    int location;
    Node* node;
    Node** node_addr;
    int length;
    ListCell* lc;
} ParamLocationLen;

typedef struct ParamState {
    ParamLocationLen clocations[MAX_PARAM_NODES];
    Node* params[MAX_PARAM_NODES];
    int clocations_count;
} ParamState;

typedef struct ParameterizationInfo {
    bool is_skip;
    const char* query_string;
    Node* parent_node;
    int param_count;
    ParamState param_state;
    List* params;
    Oid* param_types;
} ParameterizationInfo;

typedef struct ParamCachedPlan {
    ParamCachedKey paramCachedKey;
    CachedPlanSource* psrc;
} ParamCachedPlan;

extern char* query_type_text[FIXED_QUERY_TYPE_LEN];

bool execQueryParameterization(Node* parsetree, const char* query_string, CommandDest cmdDest, char* completionTag,
                               Oid relOid);
bool isQualifiedIuds(Node* parsetree, const char* queryString, Oid* relOid);
void dropAllParameterizedQueries(void);
extern uint32 cachedPlanKeyHashFunc(const void* key, Size keysize);
extern int cachedPlanKeyHashMatch(const void* key1, const void* key2, Size keysize);

#endif /* OPENGAUSS_SERVER_AUTO_PARAMETERIZATION_H */