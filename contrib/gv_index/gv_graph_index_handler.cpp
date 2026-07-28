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
 * gv_graph_index_handler.cpp
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/gv_graph_index_handler.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include <filesystem>
#include "postgres.h"
#include "fmgr.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "nodes/nodes.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/bytea.h"
#include "utils/memutils.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "catalog/pg_class.h"
#include "access/htup.h"
#include "access/amapi.h"
#include "access/reloptions.h"
#include "lite/subindex/details/pair.h"

#include "lite/index/vector_metapage.h"
#include "lite/index/graph_factory.h"
#include "lite/index/index_options.h"
#include "lite/subindex/vector_format/vector_quantization_config.h"
#include "lite/subindex/graph/graph_options.h"
#include "lite/subindex/graph/builder/graph_build_algorithm.h"
#include "lite/subindex/graph/searcher/search_type.h"
#include "graph_options.h"
#include "graph_values.h"
#include "pg_init.h"

#include "utils.h"
#include "env/gv_light_env_impl.h"
#include "storage/smgr/smgr.h"
#include "filter.h"

/* 前向声明 */
static bytea* gv_graph_amoptions(Datum reloptions, bool validate);

/* 校验并调整图索引参数。 在 ambuild 中调用，确保在构建开始前完成所有校验和自动调整 */
static void validate_and_adjust_graph_options(Relation index, GraphOptions *opts)
{
    /* 1. 读取向量维度并校验 */
    int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
    if (dim <= 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("column does not have dimensions")));
    }
    static constexpr int MAX_DIMENSION = 4096;
    if (dim > MAX_DIMENSION) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("vector_dimension %d exceeds maxmium allowed %d", dim, MAX_DIMENSION)));
    }

    /* 2. subgraph_count 由 reloption 约束 [1, 64], SQL 层已保证非零，无需重复检查 */

    /* 3. enable_neighbor_embedded 是实验性legacy功能，强制关闭并警告 */
    if (opts->enable_neighbor_embedded) {
        opts->enable_neighbor_embedded = false;
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("enable_neighbor_embedded is an experimental legacy feature"
                                    "and is not supported, treating it as false")));
    }

    /* 4. subgraph_count > 1时，build_with_quantized_vector和enable_vector_copy必须有一个为true
    *     否则强制开启build_with_quantized_vector并警告 */
    if (opts->subgraph_count > 1 && !opts->build_with_quantized_vector && !opts->enable_vector_copy) {
        opts -> build_with_quantized_vector = true;
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("When subgraph_count > 1 and enable_vector_copy = false, "
                                    "build_with_quantized_vector will be treated as true")));
    }
}

/* 根据 quantization_type 字符串选择量化配置函数 */
static annlite::IndexOptions make_quant_opts(const char *quant_type, uint32_t dim)
{
    static constexpr size_t default_lvq_clusters = 128;
    static constexpr size_t pq_max_use_all_subs = 128;
    /* fp32:不量化 */
    if (strcmp(quant_type, "fp32") == 0) {
        FAST_NOTICE("fp32 is not supported now, defaulting to lvq");
        return annlite::VectorQuantizationConfig::make_quantized_lvq(dim, default_lvq_clusters);
    }
    /* pq: Product Quantization (默认 16 centroids，即pq4bit) */
    if (strcmp(quant_type, "pq") == 0) {
        size_t segment_count = ((dim > pq_max_use_all_subs) && (dim % 2 == 0)) ? dim : dim / 2;
        return annlite::VectorQuantizationConfig::make_quantized_pq4bit(dim, segment_count);
    }
    /* lvq: Locally Adaptive Vector Quantization (默认) */
    if (strcmp(quant_type, "lvq") == 0) {
        return annlite::VectorQuantizationConfig::make_quantized_lvq(dim, default_lvq_clusters);
    }
    /* 未知类型，默认lvq */
    FAST_NOTICE("unknown quantization_type '%s', defaulting to lvq", quant_type);
    return annlite::VectorQuantizationConfig::make_quantized_lvq(dim, default_lvq_clusters);
}

/* 填充 build_options, 使用 GraphOptions 中用户指定的参数，缺失则用默认值 */
static annlite::IndexOptions build_options_from_graph_options(Relation index, GraphOptions *opts,
    annlite::VectorDistanceType dist_type)
{
    /* 从rd_att读取向量维度（validate_and_adjust_graph_options 已确保dim > 0） */
    int dim = TupleDescAttr(index->rd_att, 0)->atttypmod;

    /* 使用options中的值（validate_and_adjust_graph_options 已确保subgraph_count > 0） */
    int subgraph_count = opts->subgraph_count;
    int num_parallels = opts->num_parallels;
    int graph_degree = opts->graph_degree;
    bool enable_neighbor_embedded = opts->enable_neighbor_embedded;
    bool enable_vector_copy = opts->enable_vector_copy;
    bool build_with_quantized_vec = opts->build_with_quantized_vector;

    const char *quant_type = (char *)opts + *(int *)&(opts->quantization_type);
    const char *storage_type = (char *)opts + *(int *)&(opts->storage_type);

    /* 从 storage_type 派生 is_ustore 标志 */
    bool is_ustore = (storage_type != NULL && strcmp(storage_type, "ustore") == 0);
    if (!is_ustore) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("graph_index doesn't support astore")));
    }

    FAST_NOTICE("graph_index.distance_type = %d (%s)", (int)dist_type,
        annlite::VectorDistanceTypeNameParser::name(dist_type));
    FAST_NOTICE("graph_index.subgraph_count = %d", subgraph_count);
    FAST_NOTICE("graph_index.num_parallels = %d", num_parallels);
    FAST_NOTICE("graph_index.graph_degree = %d", graph_degree);
    FAST_NOTICE("graph_index.dim = %d", dim);
    FAST_NOTICE("graph_index.quantization_type = %s", quant_type);
    FAST_NOTICE("graph_index.is_ustore = %s", is_ustore ? "true" : "false");
    FAST_NOTICE("graph_index.enable_neighbor_embedded = %s", enable_neighbor_embedded ? "true" : "false");
    FAST_NOTICE("graph_index.enable_vector_copy = %s", enable_vector_copy ? "true" : "false");
    FAST_NOTICE("graph_index.build_with_quantized_vector = %s", build_with_quantized_vec ? "true" : "false");

    annlite::IndexOptions bopt = {
        {"distance_type", (int)dist_type},
        {"max_threads", (int)num_parallels},
        {"builder_type", (int)annlite::GraphBuilderType::gbmVamanaDiskGraphBuilder},
        {"edge_builder_type", (int)annlite::EdgeBuilderType::ebtCpuDefaultBuilder},
        {"prune_algorithm", (int)annlite::patVAMANA},
        {"work_mem", 16_GB},
        {"shared_mem", 16_GB},
        {"instruction_set", (int)annlite::IS_NEON},
        {"vector_transform_options.transform_type", (int)annlite::tsOriginal},
        {"vector_transform_options.dimension", (int)dim},
        {"vector_transform_options.transform_dim", (int)dim},
        {"build_with_quantized_vector", (bool)build_with_quantized_vec},
        {"enable_hnsw_navigator", (bool)false},
        {"enable_bptree", (bool)true},
        {"enable_vector_copy", (bool)enable_vector_copy},
        {"enable_lsg", (bool)false},
        {"neighbor_format", (int)annlite::NeighborOptions::nftNeighborOnly},
        {"neighbor_options.enable_neighbor_embedded", (bool)enable_neighbor_embedded},
        {"subgraph_options.subgraph_count", (int)subgraph_count},
        {"subgraph_options.subgraph_max_relative_size", (int)1},
        {"subgraph_options.subgraph_vertex_duplication", (int)2},
        {"graph_degree", (int)graph_degree},
        {"graph_degree_redundancy", (double)0.3},
        {"insert_beam_search_limit", (int)100},
        {"insert_candidates_limit", (int)400},
        {"enable_pq_min_rows", (int)256},
        {"is_ustore", (bool)is_ustore}
    };
    /* Append quantization config - fp32 则不追加任何量化参数 */
    annlite::IndexOptions quant_opts = make_quant_opts(quant_type, (uint32_t)dim);
    bopt.append_all(quant_opts);
    return bopt;
}

static void graph_build_indexfile(Relation heap, Relation index, IndexInfo *index_info, GraphOptions *opts,
    annlite::VectorDistanceType dist_type)
{
    using KeyBase = float;
    using LightEnvImpl = gs_vector::GVLightEnvImpl;
    using IndexType = annlite::Index<KeyBase, annlite::Value64, uint32_t, annlite::PageBasedToolKitEnv>;

    LightEnvImpl light_env_impl(index);
    annlite::IndexOptions build_options = build_options_from_graph_options(index, opts, dist_type);

    // 数据源 - 从堆表读取向量数据，返回annlite::Value64(二进制与GraphValueTypeV3兼容)
    using DataSource = gs_vector::GVDataSourceImpl<KeyBase, annlite::Value64>;
    DataSource data_source(heap, index, index_info, true);

    IndexType* graph_index = CreateGraphIndex(&light_env_impl);
    // create_metapage 内部会先调用DocFactory::init初始化存储，再创建page0的metapage
    graph_index->create_metapage(build_options);
    graph_index->train(&data_source, build_options);
    graph_index->close();
    DestroyGraphIndex(graph_index);
}

/*
 * SQL functions
 */
extern "C" Datum gv_graph_index_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(gv_graph_index_handler);

/* 1. ambuild */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_ambuild);
static IndexBuildResult* gv_graph_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result = NULL;
    result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));

    GraphOptions *opts = (GraphOptions *)index->rd_options;
    if (opts == NULL) {
        /* rd_options 未被server填充，从pg_class catalog读取原始datum并解析 */
        Datum reloptions_datum = get_index_reloptions_datum(RelationGetRelid(index));
        if (reloptions_datum != (Datum)0) {
            index->rd_options = gv_graph_amoptions(reloptions_datum, true);
        } else {
            index->rd_options = gv_graph_amoptions((Datum)0, true);
        }
        opts = (GraphOptions *)index->rd_options;
    }
    /* 从 opfamily 名称推断距离类型（L2 或 cosine） */
    annlite::VectorDistanceType dist_type = get_distance_type_from_index(index);
    FAST_NOTICE("graphbuild called (index: %s, rd_options = %p, dist_type = %d)",
        RelationGetRelationName(index), (void*)index->rd_options, (int)dist_type);
    /* 校验并调整参数（在构建开始前完成） */
    validate_and_adjust_graph_options(index, opts);
    graph_build_indexfile(heap, index, indexInfo, opts, dist_type);

    /* 刷所有索引页到WAL日志并提交 */
    RelationOpenSmgr(index);
    gv_graph_xlog_write_page(index, smgrnblocks(index->rd_smgr, MAIN_FORKNUM),
                             RM_GRAPH_ID, XLOG_GRAPH_WRITE_FULL_PAGES);

    result->heap_tuples = 0;
    result->index_tuples = 0;

    return result;
}

/* 2. ambuildempty */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_ambuildempty);
static void gv_graph_ambuildempty(Relation index)
{
    FAST_NOTICE("ambuildempty called (index: %s)", RelationGetRelationName(index));
}

/* 3. aminsert */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_aminsert);
static bool gv_graph_aminsert(Relation index, Datum *values, const bool *isnull,
                        ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique)
{
    FAST_NOTICE("aminsert called (index: %s)", RelationGetRelationName(index));

    Datum src = values[0];
    Datum dst = PointerGetDatum(PG_DETOAST_DATUM(src));
    annlite::NewVector& vector = *(annlite::NewVector*) dst;
    const float* data = &vector.x[0];
    uint16_t dim = static_cast<uint16_t>(vector.dim);

    using KeyBase = float;
    using LightEnvImpl = gs_vector::GVLightEnvImpl;
    using IndexType = annlite::Index<KeyBase, annlite::Value64, uint32_t, annlite::PageBasedToolKitEnv>;
    LightEnvImpl light_env_impl(index);
    {
        bool fake_isnull[1] = {true};   // we don't store index tuple with value(NewVector).
        TupleDesc tupDesc = RelationGetDescr(index);
        TupleDesc curDesc = CreateTemplateTupleDesc(tupDesc->natts, tupDesc->tdhasoid, tupDesc->td_tam_ops);
        IndexTuple itup = index_form_tuple(curDesc, values, &fake_isnull[0]);
        pfree(curDesc);
        itup->t_tid = *heap_tid;
        VectorIndexXidData vxid;
        vector_set_xmin_xmax(&vxid, VECTOR_DML_INSERT, 0);
        GraphValueTypeV3 value(itup, vxid);

        annlite::IndexOptions insert_options = {
            {"enable_bptree", (bool)true}
        };
        IndexType* graph_index = CreateGraphIndex(&light_env_impl);
        graph_index->open(true);
        graph_index->add(data, dim, *(annlite::Value64*)&value, insert_options);
        graph_index->close();
        DestroyGraphIndex(graph_index);

        pfree_ext(itup);
    }
    return true;
}

static void graph_vacuum(Relation index, IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback, const void *callback_state)
{
    using KeyBase = float;
    using LightEnvImpl = gs_vector::GVLightEnvImpl;
    using IndexType = annlite::Index<KeyBase, annlite::Value64, uint32_t, annlite::PageBasedToolKitEnv>;
    LightEnvImpl light_env_impl(index);

    annlite::IndexOptions vaccum_options = {{"needs_wal", (bool)true}, {"enable_bptree", (bool)true}};
    IndexType* graph_index = CreateGraphIndex(&light_env_impl);
    graph_index->open(true);
    bool is_ustore = false;
    annlite::VectorMetaPage meta;
    if (graph_index->read_metapage(meta)) {
        is_ustore = meta.is_ustore;
    }
    VacuumFilter<annlite::Value64> filter(index, is_ustore, callback, callback_state);
    graph_index->bulk_delete(filter, vaccum_options);
    graph_index->close();
    DestroyGraphIndex(graph_index);
}

/* 4. ambulkdelete */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_ambulkdelete);
static IndexBulkDeleteResult *gv_graph_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback, const void *callback_state)
{
    Relation index = info->index;
    FAST_NOTICE("ambulkdelete called (index: %s)", RelationGetRelationName(info->index));
    if (!IndexIsUsable(index->rd_index)) {
        return stats;
    }

    if (!IS_PGXC_DATANODE) {
        return stats;
    }

    /* After the penultimate phase of reindexing concurrently commited, no need to update the old index. */
    if (!IndexIsLive(index->rd_index)) {
        return stats;
    }
    graph_vacuum(index, stats, callback, callback_state);
    return stats;
}

/* 5. amvacuumcleanup */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amvacuumcleanup);
static IndexBulkDeleteResult *gv_graph_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    FAST_NOTICE("amvacuumcleanup called (index: %s)", RelationGetRelationName(info->index));
    /* No-op in ANALYZE ONLY mode */
    if (info->analyze_only) {
        return stats;
    }
    IndexBulkDeleteResult *bulk_delete_res = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
    stats = bulk_delete_res;
    graph_vacuum(info->index, stats, NULL, NULL);
    return stats;
}

/* 6. amcostestimate */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amcostestimate);
static void gv_graph_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                               Cost *indexStartupCost, Cost *indexTotalCost,
                               Selectivity *indexSelectivity, double *indexCorrelation)
{
    Relation indexRel = NULL;
    indexRel = index_open(path->indexinfo->indexoid, AccessShareLock);
    FAST_NOTICE("amcostestimate called (index: %s)", RelationGetRelationName(indexRel));
    index_close(indexRel, AccessShareLock);
    static constexpr double total_cost = 10.0;
    static constexpr double selectivity = 0.5;
    *indexStartupCost = 1.0;
    *indexTotalCost = total_cost;
    *indexSelectivity = selectivity;
    *indexCorrelation = 0.0;
}

/* 7. amoptions */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amoptions);
static bytea *gv_graph_amoptions(Datum reloptions, bool validate)
{
    /* 如果 gv_graph_kind_id为0（_PG_init尚未运行），先注册选项 */
    if (gv_graph_kind_id == 0) {
        gv_graph_kind_id = add_reloption_kind();
        register_graph_reloptions(gv_graph_kind_id);
    }
    FAST_NOTICE("amoptions called");
    relopt_value *options = NULL;
    int numoptions;
    GraphOptions *rdopts = NULL;
    /* Track total size including string data appended after the struct, so we can
     * set the varlena header (VARSIZE) for relcache. fill_rel_options does this
     * internally but we set it explicitly here as a safeguard. */
    Size total_string_size = 0;
    options = parseRelOptions(reloptions, false, gv_graph_kind_id, &numoptions);

    /* if none set, still return a default-initialized struct (ambuild expects non-null rd_options) */
    if (numoptions == 0) {
        rdopts = (GraphOptions *)palloc0(sizeof(GraphOptions));
        SET_VARSIZE(rdopts, sizeof(GraphOptions));
        pfree_ext(options);
        return (bytea *)rdopts;
    }
    /* compute total size for varlena header:basesize + all string data */
    for (int i = 0; i < numoptions; i++) {
        if (options[i].gen->type == RELOPT_TYPE_STRING) {
            total_string_size += GET_STRING_RELOPTION_LEN(options[i]) + 1;
        }
    }
    Size total_size = sizeof(GraphOptions) + total_string_size;
    rdopts = (GraphOptions *)allocateReloptStruct(sizeof(GraphOptions), options, numoptions);
    fillRelOptions((void *)rdopts, sizeof(GraphOptions), options, numoptions, false,
        graph_relopt_tab, lengthof(graph_relopt_tab));
    /* free string value copies allocated by parse_rel_options */
    for (int i = 0; i < numoptions; i++) {
        if (options[i].gen->type == RELOPT_TYPE_STRING && options[i].isset) {
            pfree_ext(options[i].values.string_val);
        }
    }
    pfree_ext(options);
    /* Ensure varlena header is set so relcache can use VARSIZE() to get the size. */
    SET_VARSIZE(rdopts, total_size);
    return (bytea *)rdopts;
}

/* 8. amvalidate */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amvalidate);
static bool gv_graph_amvalidate(Oid opclassoid)
{
    FAST_NOTICE("amvalidate called (opclass: %u)", opclassoid);
    return true;
}

/* 9. ambeginscan */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_ambeginscan);
static IndexScanDesc gv_graph_ambeginscan(Relation index, int nkeys, int norderbys)
{
    FAST_NOTICE("ambeginscan called (index: %s)", RelationGetRelationName(index));

    IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);

    GraphScanOpaque so = (GraphScanOpaque)palloc0(sizeof(GraphScanOpaqueData));
    so->first = true;
    so->next_scan_index = 0;
    so->ctid_tuples = NULL;

    so->collation = index->rd_indcollation[0];

    scan->opaque = so;
    scan->isUpsert = false;
    scan->xs_itupdesc = RelationGetDescr(index);
    return scan;
}

/* 10. amrescan */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amrescan);
static void gv_graph_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    FAST_NOTICE("amrescan called");
    GraphScanOpaque so = (GraphScanOpaque)scan->opaque;
    errno_t rc = EOK;

    so->first = true;
    so->next_scan_index = 0;
    if (so->ctid_tuples != nullptr) {
        pfree(so->ctid_tuples);
    }
    if (keys && scan->numberOfKeys >0) {
        rc = memmove_s(scan->keyData, (size_t)(scan->numberOfKeys * sizeof(ScanKeyData)), keys,
                        (size_t)(scan->numberOfKeys * sizeof(ScanKeyData)));
        securec_check(rc, "", "");
    }
    if (orderbys && scan->numberOfOrderBys >0) {
        rc = memmove_s(scan->orderByData, (size_t)(scan->numberOfOrderBys * sizeof(ScanKeyData)), orderbys,
                        (size_t)(scan->numberOfOrderBys * sizeof(ScanKeyData)));
        securec_check(rc, "", "");
    }
}

void graph_index_first(IndexScanDesc scan, GraphScanOpaque so)
{
    annlite::NewVector *vector = DatumGetVector(scan->orderByData->sk_argument);
    size_t dim = vector->dim;

    size_t rerank_ncandidates = (size_t)u_sess->attr.attr_common.gv_graph_nprobes;

    using KeyBase = float;
    using LightEnvImpl = gs_vector::GVLightEnvImpl;
    using IndexType = annlite::Index<KeyBase, annlite::Value64, uint32_t, annlite::PageBasedToolKitEnv>;

    LightEnvImpl light_env_impl(scan->indexRelation);
    using ValuePair = annlite::Pair<uint32_t, annlite::Value64>;
    using DataSourceIter = gs_vector::GVDataSourceImpl<KeyBase, annlite::Value64>::ReadIterator;

    using ResultSet = typename annlite::PageBasedToolKitEnv::Memory::template SmallArray<ValuePair>;
    using AllocatorWrapper = annlite::toolkit::AllocatorWrapper;
    AllocatorWrapper allocator(&light_env_impl);
    ResultSet result_set(allocator, rerank_ncandidates);

    IndexType* graph_index = CreateGraphIndex(&light_env_impl);
    graph_index->open(false);
    // 从 metapage读取dist_type，决定是否归一化
    annlite::VectorMetaPage meta;
    bool need_normalize = true;
    bool is_ustore = false;
    if (graph_index->read_metapage(meta)) {
        need_normalize = (meta.dist_type == annlite::COSINE_DIST_FUNC);
        is_ustore = meta.is_ustore;
    }
    DataSourceIter iter(scan->heapRelation, scan->indexRelation, true);
    SelectFilter<annlite::Value64> filter(scan, is_ustore);
    annlite::IndexOptions search_options = {
        {"search_type", (int)annlite::NORMAL_ANN},
        {"candidates", (int)rerank_ncandidates},
        {"enable_brute_force_threshold", (int)0},
        {"need_normalized", (bool)need_normalize}};
    FAST_NOTICE("GRAPH_search need_normalize=%d dist_type=%d", (int)need_normalize, (int)meta.dist_type);
    size_t count = graph_index->search(&result_set[0], rerank_ncandidates, &vector->x[0], dim, filter, search_options,
                                        iter, meta.dist_type);
    graph_index->close();
    DestroyGraphIndex(graph_index);
    iter.close();
    so->total_num_tuple = count;
    FAST_NOTICE("GRAPH_tuples_returned: %zu", count);
    // no rerank now
    so->ctid_tuples = (GraphScanCtidTuple *)palloc0(sizeof(GraphScanCtidTuple) * so->total_num_tuple);
    for (size_t i = 0; i < (size_t)so->total_num_tuple; ++i) {
        // Value64(binary-compatible with GraphValueTypeV3):
        const auto* entry = reinterpret_cast<const GraphValueTypeV3*>(&result_set[i].value());
        so->ctid_tuples[i].tid = entry->index_tuple.t_tid;
    }
    result_set.destructor();
}

/* 11. amgettuple */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amgettuple);
static bool gv_graph_amgettuple(IndexScanDesc scan, ScanDirection direction)
{
    FAST_NOTICE("amgettuple called");
    GraphScanOpaque so = (GraphScanOpaque)scan->opaque;

    scan->xs_recheck = false;
    if (so->first) {
        graph_index_first(scan, so);
        so->first = false;
    }

    if (so->next_scan_index < so->total_num_tuple) {
        GraphScanCtidTuple *ctup = &so->ctid_tuples[so->next_scan_index];
        scan->xs_ctup.t_self = ctup->tid;
        if (scan->xs_want_itup) {
            scan->xs_itup = (IndexTuple)ctup->itup;
        }
        ++so->next_scan_index;
        return true;
    }
    return false; // 无实际元组
}

/* 12. amendscan */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amendscan);
static void gv_graph_amendscan(IndexScanDesc scan)
{
    FAST_NOTICE("amendscan called");
    GraphScanOpaque so = (GraphScanOpaque)scan->opaque;

    pfree(so->ctid_tuples);
    pfree(so);
    scan->opaque = NULL;
}

/* 13. amdelete */
PGDLLEXPORT PG_FUNCTION_INFO_V1(gv_graph_amdelete);
static bool gv_graph_amdelete(Relation index, Datum *values, const bool *isnull,
                                ItemPointer heap_t_ctid, bool isRollbackIndex)
{
    FAST_NOTICE("amdelete called (index:%s, is_rollback:%s)",
        RelationGetRelationName(index), isRollbackIndex ? "true" : "false");
    using KeyBase = float;
    using LightEnvImpl = gs_vector::GVLightEnvImpl;
    using IndexType = annlite::Index<KeyBase, annlite::Value64, uint32_t, annlite::PageBasedToolKitEnv>;
    LightEnvImpl light_env_impl(index);
    {
        bool fake_isnull[1] = {true};   // we don't store index tuple with value(NewVector).
        TupleDesc tupDesc = RelationGetDescr(index);
        TupleDesc curDesc = CreateTemplateTupleDesc(tupDesc->natts, tupDesc->tdhasoid, tupDesc->td_tam_ops);
        IndexTuple itup = index_form_tuple(curDesc, values, &fake_isnull[0]);
        pfree(curDesc);
        itup->t_tid = *heap_t_ctid;
        VectorIndexXidData vxid;
        DeleteFilter<annlite::Value64> filter(index, itup, FrozenTransactionId);
        GraphValueTypeV3 value(itup, vxid);

        annlite::IndexOptions delete_options = {
            {"enable_bptree", (bool)true}
        };
        IndexType* graph_index = CreateGraphIndex(&light_env_impl);
        graph_index->open(true);
        graph_index->mark_delete(*(annlite::Value64*)&value, filter, delete_options);
        graph_index->close();
        DestroyGraphIndex(graph_index);

        pfree_ext(itup);
    }
    return true;
}

Datum gv_graph_index_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    /* Basic Metainfo */
    amroutine->amstrategies = 0;
    amroutine->amsupport = 5;
    amroutine->amoptsprocnum = 0;
    amroutine->amcanorder = true;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false;
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false;
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = true;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = true;
    
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amparallelvacuumoptions = 0;
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    amroutine->ambuild = gv_graph_ambuild;
    amroutine->ambuildempty = gv_graph_ambuildempty;
    amroutine->aminsert = gv_graph_aminsert;
    amroutine->ambulkdelete = gv_graph_ambulkdelete;
    amroutine->amvacuumcleanup = gv_graph_amvacuumcleanup;
    amroutine->amcostestimate = gv_graph_amcostestimate;
    amroutine->amoptions = gv_graph_amoptions;
    amroutine->amvalidate = gv_graph_amvalidate;
    amroutine->ammerge = NULL;
    amroutine->amdelete = gv_graph_amdelete;
    amroutine->ambeginscan = gv_graph_ambeginscan;
    amroutine->amrescan = gv_graph_amrescan;
    amroutine->amgettuple = gv_graph_amgettuple;
    amroutine->amendscan = gv_graph_amendscan;
    amroutine->amgetbitmap = NULL;
    amroutine->ammarkpos = NULL;

    PG_RETURN_POINTER(amroutine);
}