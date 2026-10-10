/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * graph_options.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/graph_options.h
 *
 * --------------------------------------------------------------------------------------
 */

#ifndef GV_GRAPH_OPTIONS_H
#define GV_GRAPH_OPTIONS_H

#include "fmgr.h"
#include "postgres.h"
#include "access/reloptions.h"
#include "utils/rel.h"
#include "lite/subindex/dist_metric/vector_distance_type.h"

#define GRAPH_INDEX_DISTANCE_PROC 1

struct GraphScanCtidTuple {
    ItemPointerData tid;
    BlockNumber block_no;
    bool need_recheck;
    IndexTuple itup;
};

struct GraphScanOpaqueData {
    int next_scan_index;
    bool first;
    int total_num_tuple;
    GraphScanCtidTuple *ctid_tuples;

    Oid collation;
};

typedef GraphScanOpaqueData *GraphScanOpaque;

struct GraphOptions {
    StdRdOptions std_options;
    char *storage_type;                     /* storage_type: auto/ustore/astore */
    int subgraph_count;                     /* subgraph count, must be > 0 */
    int num_parallels;                      /* max threads for build */
    int graph_degree;                       /* graph degree */
    char *quantization_type;                /* quantization type: lvq/pq */
    char *pruneAlg;                         /* prune algorithm: vamana/npu_knn */
    bool enable_neighbor_embedded;          /* embedded neighbor vectors (experimental legacy, auto-disabled) */
    bool enable_vector_copy;                /* copy vectors during build */
    bool build_with_quantized_vector;       /* use quantized vectors during build */
};

/*
 * GRAPH_RELOPTIONS - X-macro: 定义所有 reloption 描述，展开一次生成注册代码，
 * 展开两次生成 parse 表。
 * 新增选项时需要做两件事：
 *   1. 在此宏追加一行（FIELD 参数填GraphOptions 中的成员名）
 *   2. 在上方 GraphOptions 结构体中添加同名成员（类型与 TYPE 对应：STRING->char*, INT->int, BOOL->bool）
 * 参数：NAME, DESC, TYPE, DEFAULT, MIN, MAX, EXTRA, FIELD
 *   TYPE=STRING:   DEFAULT=默认字符串，MIN/MAX=0, EXTRA=validator 或 NULL
 *   TYPE=INT:      DEFAULT=默认值, MIN=最小值, MAX=最大值, EXTRA=0
 *   TYPE=BOOL:     DEFAULT=默认值, MIN/MAX/EXTRA=0
 *   FIELD: GraphOptions 结构体中的成员名（必须与结构体字段名一致，否则编译报错）
 */
#define GRAPH_RELOPTIONS(XX) \
    XX("storage_type", "storage_type: auto/ustore/astore", STRING, "auto", 0, 0, ValidateStorageTypeOption, storage_type) \
    XX("quantization_type", "quantization type: lvq/pq", STRING, "lvq", 0, 0, NULL, quantization_type) \
    XX("prune_alg", "prune algorithm: vamana/npu_knn", STRING, "vamana", 0, 0, ValidatePruneAlg, pruneAlg) \
    XX("subgraph_count", "number of subgraphs", INT, 1, 1, 64, 0, subgraph_count) \
    XX("num_parallels", "max threads for build", INT, 32, 1, 64, 0, num_parallels) \
    XX("graph_degree", "graph degree", INT, 96, 8, 256, 0, graph_degree) \
    XX("enable_neighbor_embedded", "embed neighbor vectors", BOOL, false, 0, 0, 0, enable_neighbor_embedded) \
    XX("enable_vector_copy", "copy vectors during build", BOOL, false, 0, 0, 0, enable_vector_copy) \
    XX("build_with_quantized_vector", "use quantized vectors", BOOL, false, 0, 0, 0, build_with_quantized_vector) \

/* 展开为 parse 表（graph_relopt_tab[]）。由 GRAPH_RELOPTIONS 一次展开生成。 */
#define GRAPH_RELOPT_TAB_ENTRY(name, desc, type, def, minv, maxv, extra, field) \
    {name, RELOPT_TYPE_##type, offsetof(GraphOptions, field)},
static const relopt_parse_elt graph_relopt_tab[] = {
    GRAPH_RELOPTIONS(GRAPH_RELOPT_TAB_ENTRY)
};
#undef GRAPH_RELOPT_TAB_ENTRY

#endif /* GV_GRAPH_OPTIONS_H */
