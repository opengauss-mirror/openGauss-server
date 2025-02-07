/* -------------------------------------------------------------------------
*
* auto_parameterization.h
*	  POSTGRES C Backend Interface
*
* Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
* Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
* Portions Copyright (c) 2010-2012 Postgres-XC Development Group
* Portions Copyright (c) 2021, openGauss Contributors
*
* IDENTIFICATION
*	  src/include/commands/auto_parameterization.h
*
* NOTES
*	  A module that turns a "simple query" into a parameterized query
*
* -------------------------------------------------------------------------
*/

#ifndef AUTO_PARAMETERIZATION_H
#define AUTO_PARAMETERIZATION_H

#include "utils/plancache.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

#define MAX_PARAM_QUERY_LEN 512

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

typedef struct ParamCachedPlan {
    char parameterized_query[MAX_PARAM_QUERY_LEN];
    CachedPlanSource* psrc;
} ParamCachedPlan;

bool execQueryParameterization(Node* parsetree, const char* query_string, CommandDest cmdDest, char* completionTag);
bool isQualifiedIuds(Node* parsetree);
#endif  /* OPENGAUSS_SERVER_AUTO_PARAMETERIZATION_H */