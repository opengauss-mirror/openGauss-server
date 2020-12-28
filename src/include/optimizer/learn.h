/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * learn.h
 *
 * IDENTIFICATION
 * src/include/optimizer/learn.h
 *
 * DESCRIPTION
 * Public resources of Code/src/backend/utils/learn/
 *
 * -------------------------------------------------------------------------
 */

#ifndef LEARN_H
#define LEARN_H

#include "postgres.h"
#include "catalog/gs_opt_model.h"
#include "stdlib.h"

#define RLSTM_TEMPLATE_NAME                     "rlstm"

#define MAX_LEN_ONE_HOT                         1024
#define MAX_LEN_ROW                             4096
#define MAX_LEN_TEXT                            100
#define DOP_MAX_LEN                             10

#define CONDITION_LEN                           8

/* information of table gs_wlm_plan_operator_info */
#define PLAN_OPT_TABLE_COL_NUM                  16
#define PLAN_OPT_TABLE_NAME                     "gs_wlm_plan_operator_info"

/* The encoding map for operator name, this should be kept in the alphabatical order */
#define LEN_ENCODE_OPTNAME                      20
#define TEXT_OPTNAME_ADAPTOR                    "ADAPTOR"
#define TEXT_OPTNAME_AGG                        "AGG"
#define TEXT_OPTNAME_APPEND                     "APPEND"
#define TEXT_OPTNAME_BITMAP                     "BITMAP"
#define TEXT_OPTNAME_EXTENSIBLE                 "EXTENSIBLE"
#define TEXT_OPTNAME_GROUP                      "GROUP"
#define TEXT_OPTNAME_HASH                       "HASH"
#define TEXT_OPTNAME_JOIN                       "JOIN"
#define TEXT_OPTNAME_LIMIT                      "LIMIT"
#define TEXT_OPTNAME_LOCKROWS                   "LOCKROWS"
#define TEXT_OPTNAME_MATERIALIZE                "MATERIALIZE"
#define TEXT_OPTNAME_MODIFY_TABLE               "MODIFY_TABLE"
#define TEXT_OPTNAME_PART_ITER                  "PART_ITER"
#define TEXT_OPTNAME_RECURSIVE_UNION            "RECURSIVE_UNION"
#define TEXT_OPTNAME_RESULT                     "RESULT"
#define TEXT_OPTNAME_SCAN                       "SCAN"
#define TEXT_OPTNAME_SET_OP                     "SET_OP"
#define TEXT_OPTNAME_SORT                       "SORT"
#define TEXT_OPTNAME_STREAM                     "STREAM"
#define TEXT_OPTNAME_UNIQUE                     "UNIQUE"

#define LEN_ENCODE_ORIENTATION                  2
#define TEXT_ORIENTATION_ROW                    "ROW"
#define TEXT_ORIENTATION_COL                    "COL"

#define LEN_ENCODE_STRATEGY                     13
#define LEN_ENCODE_STRATEGY_ADAPTOR             0
#define LEN_ENCODE_STRATEGY_AGG                 4
#define LEN_ENCODE_STRATEGY_APPEND              2
#define LEN_ENCODE_STRATEGY_BITMAP              2
#define LEN_ENCODE_STRATEGY_EXTENSIBLE          0
#define LEN_ENCODE_STRATEGY_GROUP               0
#define LEN_ENCODE_STRATEGY_HASH                0
#define LEN_ENCODE_STRATEGY_JOIN                3
#define LEN_ENCODE_STRATEGY_LIMIT               0
#define LEN_ENCODE_STRATEGY_LOCKROWS            0
#define LEN_ENCODE_STRATEGY_MATERIALIZE         0
#define LEN_ENCODE_STRATEGY_MODIFY_TABLE        4
#define LEN_ENCODE_STRATEGY_PART_ITER           0
#define LEN_ENCODE_STRATEGY_RECURSIVE_UNION     0
#define LEN_ENCODE_STRATEGY_RESULT              0
#define LEN_ENCODE_STRATEGY_SCAN                13
#define LEN_ENCODE_STRATEGY_SET_OP              2
#define LEN_ENCODE_STRATEGY_SORT                0
#define LEN_ENCODE_STRATEGY_STREAM              7
#define LEN_ENCODE_STRATEGY_UNIQUE              0
#define TEXT_STRATEGY_AGG_PLAIN                 "PLAIN"
#define TEXT_STRATEGY_AGG_SORTED                "SORTED"
#define TEXT_STRATEGY_AGG_HASHED                "HASHED"
#define TEXT_STRATEGY_AGG_WINDOW                "WINDOW"
#define TEXT_STRATEGY_APPEND_PLAIN              "PLAIN"
#define TEXT_STRATEGY_APPEND_MERGE              "MERGE"
#define TEXT_STRATEGY_BITMAP_AND                "AND"
#define TEXT_STRATEGY_BITMAP_OR                 "OR"
#define TEXT_STRATEGY_JOIN_NESTED_LOOP          "NESTED_LOOP"
#define TEXT_STRATEGY_JOIN_MERGE                "MERGE"
#define TEXT_STRATEGY_JOIN_HASH                 "HASH"
#define TEXT_STRATEGY_MODIFY_TABLE_INSERT       "INSERT"
#define TEXT_STRATEGY_MODIFY_TABLE_UPDATE       "UPDATE"
#define TEXT_STRATEGY_MODIFY_TABLE_DELETE       "DELETE"
#define TEXT_STRATEGY_MODIFY_TABLE_MERGE        "MERGE"
#define TEXT_STRATEGY_SCAN_SEQ                  "SEQ"
#define TEXT_STRATEGY_SCAN_INDEX                "INDEX"
#define TEXT_STRATEGY_SCAN_INDEX_ONLY           "INDEX_ONLY"
#define TEXT_STRATEGY_SCAN_BITMAP_INDEX         "BITMAP_INDEX"
#define TEXT_STRATEGY_SCAN_BITMAP_HEAP          "BITMAP_HEAP"
#define TEXT_STRATEGY_SCAN_TID                  "TID"
#define TEXT_STRATEGY_SCAN_SUBQUERY             "SUBQUERY"
#define TEXT_STRATEGY_SCAN_FOREIGN              "FOREIGN"
#define TEXT_STRATEGY_SCAN_DATA_NODE            "DATA_NODE"
#define TEXT_STRATEGY_SCAN_FUNCTION             "FUNCTION"
#define TEXT_STRATEGY_SCAN_VALUES               "VALUES"
#define TEXT_STRATEGY_SCAN_CTE                  "CTE"
#define TEXT_STRATEGY_SCAN_WORK_TABLE           "WORK_TABLE"
#define TEXT_STRATEGY_SET_OP_SORTED             "SORTED"
#define TEXT_STRATEGY_SET_OP_HASHED             "HASHED"
#define TEXT_STRATEGY_STREAM_BROADCAST          "BROADCAST"
#define TEXT_STRATEGY_STREAM_REDISTRIBUTE       "REDISTRIBUTE"
#define TEXT_STRATEGY_STREAM_GATHER             "GATHER"
#define TEXT_STRATEGY_STREAM_ROUND_ROBIN        "ROUND_ROBIN"
#define TEXT_STRATEGY_STREAM_SCAN_GATHER        "SCAN_GATHER"
#define TEXT_STRATEGY_STREAM_PLAN_ROUTER        "PLAN_ROUTER"
#define TEXT_STRATEGY_STREAM_HYBRID             "HYBRID"


#define LEN_ENCODE_OPTION                       7
#define LEN_ENCODE_OPTION_ADAPTOR               0
#define LEN_ENCODE_OPTION_AGG                   0
#define LEN_ENCODE_OPTION_APPEND                0
#define LEN_ENCODE_OPTION_BITMAP                0
#define LEN_ENCODE_OPTION_EXTENSIBLE            0
#define LEN_ENCODE_OPTION_GROUP                 0
#define LEN_ENCODE_OPTION_HASH                  0
#define LEN_ENCODE_OPTION_JOIN                  7
#define LEN_ENCODE_OPTION_LIMIT                 0
#define LEN_ENCODE_OPTION_LOCKROWS              0
#define LEN_ENCODE_OPTION_MATERIALIZE           0
#define LEN_ENCODE_OPTION_MODIFY_TABLE          0
#define LEN_ENCODE_OPTION_PART_ITER             0
#define LEN_ENCODE_OPTION_RECURSIVE_UNION       0
#define LEN_ENCODE_OPTION_RESULT                0
#define LEN_ENCODE_OPTION_SCAN                  3
#define LEN_ENCODE_OPTION_SET_OP                0
#define LEN_ENCODE_OPTION_SORT                  0
#define LEN_ENCODE_OPTION_STREAM                2
#define LEN_ENCODE_OPTION_UNIQUE                0
#define TEXT_OPTION_JOIN_INNER                  "INNER"
#define TEXT_OPTION_JOIN_LEFT                   "LEFT"
#define TEXT_OPTION_JOIN_FULL                   "FULL"
#define TEXT_OPTION_JOIN_RIGHT                  "RIGHT"
#define TEXT_OPTION_JOIN_SEMI                   "SEMI"
#define TEXT_OPTION_JOIN_ANTI                   "ANTI"
#define TEXT_OPTION_JOIN_UNIQUE                 "UNIQUE"
#define TEXT_OPTION_SCAN_PARTITIONED            "PARTITIONED"
#define TEXT_OPTION_SCAN_DFS                    "DFS"
#define TEXT_OPTION_SCAN_SAMPLE                 "SAMPLE"
#define TEXT_OPTION_STREAM_LOCAL                "LOCAL"
#define TEXT_OPTION_STREAM_SPLIT                "SPLIT"

#define LEN_ENCODE_CONDITION                    12
#define LEN_ENCODE_PROJECTION                   12

#define TEXT_UNKNOWN                            "???"

/* For python APIs */
#define PORT_LEN                                6
#define PYTHON_SERVER_ROUTE_PRETRAIN            "/configure"
#define PYTHON_SERVER_ROUTE_TRAIN               "/train"
#define PYTHON_SERVER_ROUTE_PREPREDICT          "/model_setup"
#define PYTHON_SERVER_ROUTE_PREDICT             "/predict"
#define PYTHON_SERVER_ROUTE_POSTPREDICT         "/model_release"
#define PYTHON_SERVER_ROUTE_TRACK               "/track_process"
#define PYTHON_SERVER_HEADER_JSON               "Content-Type: application/json"
#define TRAIN_DATASET_FILEPATH                  "/tmp/gs_encoding.csv"
#define PRED_DATASET_FILEPATH                   "/tmp/gs_pred.csv"
#define MAX_LEN_JSON                            4096

typedef struct {
    int64       query_id;
    int32       plan_node_id;
    int32       parent_node_id;
    int64       startup_time;
    int64       total_time;
    int64       rows;
    int32       peak_memory;
    text        encode[1];
} TreeEncData;

typedef TreeEncData *TreeEncPtr;

typedef struct {
    char optname[MAX_LEN_TEXT];
    int len_strategy;
    char strategy[LEN_ENCODE_STRATEGY][MAX_LEN_TEXT];
    int len_option;
    char options[LEN_ENCODE_OPTION][MAX_LEN_TEXT];
} OptText;

typedef struct {
    int         length;
    int64      *startup_time;
    int64      *total_time;
    int64      *rows;
    int64      *peak_memory;
} ModelPredictInfo;

typedef struct {
    int         feature_size;
    bool        available;
    int4        max[gs_opt_model_label_length];
    float4      acc[gs_opt_model_label_length];
} ModelTrainInfo;

typedef struct {
    double       startup_time_accuracy;
    double       total_time_accuracy;
    double       rows_accuracy;
    double       peak_memory_accuracy;
} ModelAccuracy;

#endif  /* LEARN_H */