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
 * ---------------------------------------------------------------------------------------
 *
 *  bayes_network.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/algorithm/bayes/bayes_network.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <boost/math/distributions/normal.hpp>
#include "db4ai/bayes.h"
#include "access/hash.h"

#define MAX_NODE_ID_SIZE 3


typedef struct DiscreteProbParameters {
    tupleMatrix num_feature_tuple;
    int *offset;
    int *parentids;
    int node_id;
    double **coefficients;
} DiscreteProbParameters;

typedef struct SerializedBayesNet {
    int num_nodes;
} SerializedBayesNet;

typedef struct BayesNetTrainingVars {
    bayesDatumList **featuresmatrix;
    bayesDatumList *bins;
    bayesDatumList *mcvs;
    HTAB **value_stats;
    int *data_preprocess;
    double **coefficients;

    bayesContinNetList **cdep;
    tupleMatrix *num_feature_tuple;

    /* bin_widths
     * for MCV: number of distinct values not in MCVs,
     * for BINs: number of values in each range bin (width)
     */
    int *bin_widths;
    int **parentids;
    int *num_parents;
    Oid *attrtype;
    int *offsets;
} BayesNetTrainingVars;

typedef struct TupleData {
    Datum *values;
    bool *isnull;
    Oid *types;
    int ncolumns;
} TupleData;

typedef struct AuxilaryData {
    int *selected_ids;
    int target_colid;
    int *data_process;
    double **singlecol_probs;
    ProbVector **parent_prob;
} AuxilaryData;

uint32 value_hash(const void* key, Size keysize)
{
    const ValueInTuple *k = (const ValueInTuple *)key;
    return k->hashval;
}

int value_compare(const void *a, const void *b, Size keysize) {
    return cmp_value_bayesnet<ValueInTuple>(a, b);
}

HyperparameterDefinition bayes_hyperparameter_definitions[] = {
    HYPERPARAMETER_STRING("edges_of_network", "", NULL, 0, HyperparametersBayes, edges_of_network, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_edges", 0, 0, true, INT32_MAX, true, HyperparametersBayes, num_edges, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_nodes", 0, 0, true, MAX_NODES_NUM, true, HyperparametersBayes, num_nodes, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_bins", 0, 0, true, MAX_BINS_NUM, true, HyperparametersBayes, num_bins, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_mcvs", 0, 0, true, MAX_MCVS_NUM, true, HyperparametersBayes, num_mcvs, HP_NO_AUTOML()),
    HYPERPARAMETER_INT8("num_sample_rows", 1, 1, true, INT64_MAX, true, HyperparametersBayes,
        num_sample_rows, HP_NO_AUTOML()),
    HYPERPARAMETER_INT8("num_total_rows", 1, 1, true, INT64_MAX, true, HyperparametersBayes,
        num_total_rows, HP_NO_AUTOML())
};

// --------------------------------------------------------------------------------------------
static HyperparameterDefinition const *bayes_net_get_hyperparameters(AlgorithmAPI *self, int32_t *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(bayes_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return bayes_hyperparameter_definitions;
}

static ModelHyperparameters *bayes_net_make_hyperparameters(AlgorithmAPI *self)
{
    auto bayes_net_hyperp = reinterpret_cast<HyperparametersBayes *>(palloc0(sizeof(HyperparametersBayes)));
    return &bayes_net_hyperp->mhp;
}

static inline bool IsNumber(Oid type)
{
    return type == FLOAT4OID or type == FLOAT8OID or type == INT16OID or type == INT8OID or type == INT4OID;
}


static HTAB *initialize_hashmap(MemoryContext mcxt)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(mcxt);
    HASHCTL hash_ctl;
    int rc;
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc,"\0","\0");
    hash_ctl.keysize = sizeof(ValueInTuple);
    hash_ctl.entrysize = sizeof(ValueMapEntry);
    hash_ctl.hash = value_hash;
    hash_ctl.match = (HashCompareFunc)value_compare;
    hash_ctl.hcxt = mcxt;
    HTAB *hashmap = hash_create("bayes_network_distinct_map", 100, &hash_ctl,
                                HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    MemoryContextSwitchTo(oldcxt);
    return hashmap;
}

// ---------------------------------------------------------------------------------------

static bool validateParamEdges(int pos, int total_size, const char *str, int str_len)
{
    if (pos >= total_size) {
        return false;
    }
    for (int i = 0; i < str_len; i++) {
        if (str[i] < '0' || str[i] > '9') {
            return false;
        }
    }
    return true;
}

static bool validateParamNodes(const int *all_edges, int size, int num_nodes)
{
    for (int i = 0; i < size; i++) {
        if (all_edges[i] >= num_nodes) {
            return false;
        }
    }
    return true;
}

static bool validateNoCycle(int root, bayesState *bns, uint128 *visited)
{
    if ((((uint128)1 << root) & (*visited)) > 0) {
        return false;
    }
    (*visited) += ((uint128)1 << root);
    bool res;
    for (int i = 0; i < bns->num_nodes; i++) {
        if (i != root && bns->parents[root * bns->num_nodes + i]) {
            res = validateNoCycle(i, bns, visited);
            if (!res) {
                return false;
            }
        }
    }
    return true;
}

void extract_graph(const char *edges_of_network, int num_of_edge, int num_nodes, bayesState *bns)
{
    int str_len = strlen(edges_of_network);
    int start = 0;
    int node_pos = 0;
    errno_t rc = EOK;
    int edge_size = num_of_edge + num_of_edge;
    int *all_edges = (int *)palloc0(sizeof(int) * edge_size);
    for (int end = 0; end < str_len; end++) {
        if (edges_of_network[end] == ',') {
            int substr_len = end - start;
            char substr[substr_len + 1];
            rc = strncpy_s(substr, substr_len + 1, edges_of_network + start, substr_len);
            securec_check(rc, "\0", "\0");
            substr[substr_len] = '\0';
            if (substr_len > MAX_NODE_ID_SIZE) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Incorrect Hyperparameters (too large node id), parse failed.")));
            }
            if (!validateParamEdges(node_pos, edge_size, substr, substr_len)) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Incorrect Hyperparameters (edges_of_network unmatch with num_edges), "
                                "parse failed.")));
            }
            all_edges[node_pos++] = atoi(substr);
            start = end + 1;
        }
    }
    int substr_len = str_len - start;
    if (substr_len > MAX_NODE_ID_SIZE) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Incorrect Hyperparameters (too large node id), parse failed.")));
    }
    char substr[substr_len + 1];
    rc = strncpy_s(substr, substr_len + 1, edges_of_network + start, substr_len);
    securec_check(rc, "\0", "\0");
    substr[substr_len] = '\0';
    if (!validateParamEdges(node_pos, edge_size, substr, substr_len) || node_pos != edge_size - 1) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Incorrect Hyperparameters (edges_of_network should match with num_edges), "
                                "parse failed.")));
    }
    all_edges[node_pos] = atoi(substr);
    if (!validateParamNodes(all_edges, edge_size, num_nodes)) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Incorrect Hyperparameters (maximal node id should less than num_nodes), parse failed.")));
    }
    bns->num_nodes = num_nodes;
    bns->parents = (bool *)palloc0(sizeof(bool) * bns->num_nodes * bns->num_nodes);
    for (int i = 0; i < num_of_edge; i++) {
        int l = all_edges[i * 2 + 1];
        int r = all_edges[i * 2];
        bns->parents[l * bns->num_nodes + r] = true;
    }
    uint128 visited = 0;
    for (int root = 0; root < bns->num_nodes; root++) {
        if (!validateNoCycle(root, bns, &visited)) {
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Incorrect Hyperparameters (Bayes graph should be acyclic), parse failed.")));
        }
        visited = 0;
    }
    bns->bayes_tmss = (bayesNodeState **)palloc0fast(sizeof(bayesNodeState *) * bns->num_nodes);
    for (int i = 0; i < bns->num_nodes; i++) {
        bns->bayes_tmss[i] = reinterpret_cast<bayesNodeState *>(makeNodeWithSize(TrainModelState, sizeof(bayesNodeState)));
    }
}

static TrainModelState *bayes_net_create(AlgorithmAPI *self, const TrainModel *pnode)
{
    auto bayes_net_state = reinterpret_cast<bayesState *>(makeNodeWithSize(TrainModelState, sizeof(bayesState)));
    HyperparametersBayes *hyper = (HyperparametersBayes *)pnode->hyperparameters[0];
    extract_graph(hyper->edges_of_network, hyper->num_edges, hyper->num_nodes, bayes_net_state);
    bayes_net_state->num_bins = hyper->num_bins;
    bayes_net_state->num_mcvs = hyper->num_mcvs;
    bayes_net_state->num_sample_rows = hyper->num_sample_rows;
    bayes_net_state->num_total_rows = hyper->num_total_rows;
    return &bayes_net_state->tms;
}

static void bayesSerialize(bayesNodeState *bayes_state, Model *model, struct varlena **categories_array,
                           uint32_t *categories_sizes)
{
    uint32_t nattrs = bayes_state->chunk.features_cols;
    int chunk_size;
    model->return_type = bayes_state->returntype;
    model->processed_tuples = bayes_state->processedtuples;
    model->discarded_tuples = bayes_state->discardtuples;
    model->exec_time_secs = bayes_state->executiontime;

    // serialize
    model->data.size = bayes_state->chunk_size;
    ModelBayesNodeV01 *mdata = (ModelBayesNodeV01 *)palloc(model->data.size);
    model->data.raw_data = mdata;
    model->data.version = DB4AI_MODEL_V01;

    mdata->features_cols = nattrs;
    mdata->categories_size = bayes_state->chunk.categories_size;
    mdata->prob_offset = bayes_state->chunk.prob_offset;
    int8_t *ptr = (int8_t *)(mdata + 1);
    int avail = model->data.size - sizeof(ModelBayesNodeV01);

    chunk_size = sizeof(uint32_t) * nattrs;
    errno_t rc = memcpy_s((uint32_t *)ptr, avail, bayes_state->category, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(Oid) * nattrs;
    rc = memcpy_s((Oid *)ptr, avail, bayes_state->attrtype, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(double) * mdata->prob_offset;
    rc = memcpy_s((double *)ptr, avail, bayes_state->prob, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(uint32_t) * nattrs;
    rc = memcpy_s((uint32_t *)ptr, avail, categories_sizes, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    for (uint32_t i = 0; i < nattrs; i++) {
        chunk_size = categories_sizes[i];
        rc = memcpy_s(ptr, avail, categories_array[i], chunk_size);
        securec_check_ss(rc, "\0", "\0");
        avail -= chunk_size;
        ptr += chunk_size;
    }
}

// --------------------------------------------------------------------------------------

static void bayes_handle_tuple(bayesNodeState *bayes_state, ModelTuple *outer_tuple_slot, bayesDatumList featuresmatrix[],
                               bayesContinNetList cdep[], tupleMatrix num_feature_tuple, int *data_preprocess,
                               int *parentsids, int nodeid)
{
    bayes_state->attrtype = outer_tuple_slot->typid;
    int tg_cat = bayes_state->category[0];

    // get target column's categlory index  -- tpos
    ValueInTuple val = create_value(outer_tuple_slot->values[0], outer_tuple_slot->typid[0], outer_tuple_slot->isnull[0]);
    int tpos = findDiscreteIndex(val, &featuresmatrix[0], data_preprocess[nodeid]);
    if (tpos < 0)
        tpos = featuresmatrix[0].size();

    int position = 0;
    num_feature_tuple[0][tpos]++;
    int nattrs = bayes_state->tms.tuple.ncolumns;
    for (int i = 1; i < nattrs; ++i) {
        if (IsContinuous(bayes_state->attrtype[i])) {
            if (outer_tuple_slot->isnull[i]) {
                continue;
            }
            cdep[i][tpos].setCard(DatumGetFloat8(outer_tuple_slot->values[i]));
            cdep[i][tpos].setVariance(DatumGetFloat8(outer_tuple_slot->values[i]));
        } else {
            ValueInTuple val = create_value(outer_tuple_slot->values[i], outer_tuple_slot->typid[i], outer_tuple_slot->isnull[i]);
            position = findDiscreteIndex(val, &featuresmatrix[i], data_preprocess[parentsids[i - 1]]);
            // we use one-dimensional list to express two-dimensional matrices.
            if (position < 0)
                position = featuresmatrix[i].size();
            num_feature_tuple[i][position * (tg_cat + 1) + tpos]++;
        }
    }
    ++bayes_state->processedtuples;
}

static double compute_expected_distinctnums(double rows, double distincts)
{
    /* estimate ndistincts given nrows and maximal alternative ndistincts.
    */
    if (rows == 0 || distincts == 0) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Either num_rows or num_distincts equals to zero.")));
    }
    if (rows > distincts) {
        return distincts * (1 - pow((distincts - 1) / distincts, rows));
    } else {
        return rows * (1 - pow((rows - 1) / rows, distincts));
    }
}


static void bayes_calculate_discrete(bayesState *bayes_net_state, DiscreteProbParameters *params, int tg_cat,
                                     int col_id, int target_index)
{
    bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[params->node_id];
    for (uint32_t j = 0; j < bayes_state->category[col_id] + 1; j++) {
        if (params->num_feature_tuple[col_id][j * (tg_cat + 1) + target_index] > 0 and
            params->num_feature_tuple[0][target_index] > 0 and
            params->coefficients[params->parentids[col_id - 1]][j] > 0 and
            params->coefficients[params->node_id][target_index] > 0) {
            double actual_rows = (double)params->num_feature_tuple[col_id][j * (tg_cat + 1) + target_index];
            double actual_rows_condition = (double)params->num_feature_tuple[0][target_index];
            double left_possible_distinct = 1.0 / params->coefficients[params->parentids[col_id - 1]][j];
            double right_possible_distinct = 1.0 / params->coefficients[params->node_id][target_index];
            double possible_distinct_for_two_col = Max(1.0, left_possible_distinct *
                                                            right_possible_distinct);
            double sample_ratio = (double)bayes_net_state->num_total_rows /
                                  bayes_net_state->num_sample_rows;
            double coeff_jk = 1.0 /
                compute_expected_distinctnums(actual_rows * sample_ratio, possible_distinct_for_two_col);
            bayes_state->prob[(*params->offset)++] =
                (actual_rows * coeff_jk) /
                (actual_rows_condition * params->coefficients[params->node_id][target_index]);
        } else {
            bayes_state->prob[(*params->offset)++] = 0;
        }
    }
}

static void bayes_calculate(bayesState *bayes_net_state, bayesContinNetList cdep[], DiscreteProbParameters *dpp)
{
    bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[dpp->node_id];
    *dpp->offset = 0;
    int nattrs = bayes_state->tms.tuple.ncolumns;
    int tg_cat = bayes_state->category[0];
    for (int i = 0; i < nattrs; i++) {
        for (int k = 0; k < tg_cat + 1; k++) {
            if (i == 0) {
                bayes_state->prob[(*dpp->offset)++] =
                    (double)dpp->num_feature_tuple[i][k] /
                    bayes_state->processedtuples * dpp->coefficients[dpp->node_id][k];
            } else if (IsContinuous(bayes_state->attrtype[i])) {
                bayes_state->prob[(*dpp->offset)++] = cdep[i][k].getMean();
                bayes_state->prob[(*dpp->offset)++] = cdep[i][k].getStandardDeviation();
                bayes_state->prob[(*dpp->offset)++] = (double)cdep[i][k].getNumnulls();
                bayes_state->prob[(*dpp->offset)++] = (double)cdep[i][k].getNumrows();
                bayes_state->prob[(*dpp->offset)++] = (double)cdep[i][k].getDelta();
            } else {
                bayes_calculate_discrete(bayes_net_state, dpp, tg_cat, i, k);
            }
        }
    }
    bayes_state->done = true;
}

static void bayesPrepareAndSerielized(Model *model, bayesNodeState *bayes_state, bayesDatumList featuresmatrix[],
                                      int offset)
{
    /*
     * Data transfer
     * Transfer features to Array. And serialize the model data.
     */
    int nattrs = bayes_state->tms.tuple.ncolumns;
    uint32_t *categories_sizes = (uint32_t *)palloc0(sizeof(uint32_t) * nattrs);
    struct varlena **categories_array = (struct varlena **)palloc0(sizeof(struct varlena *) * nattrs);

    bayes_state->chunk_size =
        sizeof(ModelBayesNodeV01) + (sizeof(uint32_t) + sizeof(Oid)) * nattrs + sizeof(double) * offset;

    for (int i = 0; i < nattrs; i++) {
        ArrayBuildState *astate = nullptr;
        for (bayesDatumList::iterator it = featuresmatrix[i].begin(); it != featuresmatrix[i].end(); it++) {
            astate = accumArrayResult(astate, it->data, it->isnull, it->type, CurrentMemoryContext);
        }
        Datum dt = makeArrayResult(astate, CurrentMemoryContext);
        categories_array[i] = pg_detoast_datum((struct varlena *)DatumGetPointer(dt));
        categories_sizes[i] = VARSIZE(categories_array[i]);
        bayes_state->chunk_size += (sizeof(uint32_t) + categories_sizes[i]);
    }

    bayes_state->chunk.features_cols = nattrs;
    bayes_state->chunk.prob_offset = offset;

    // store the model
    MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);
    bayesSerialize(bayes_state, model, categories_array, categories_sizes);
    model->status = ERRCODE_SUCCESSFUL_COMPLETION;
    MemoryContextSwitchTo(oldcxt);
    for (int i = 0; i < nattrs; i++) {
        pfree(categories_array[i]);
    }
    pfree(categories_array);
    pfree(categories_sizes);
}

ModelTuple select_tuple(int num_parents, int *parentids, int targetid, ModelTuple *origin_tuple)
{
    ModelTuple tuple;
    tuple.values = (Datum *)palloc0(sizeof(Datum) * (num_parents + 1));
    tuple.isnull = (bool *)palloc0(sizeof(bool) * (num_parents + 1));
    tuple.typid = (Oid *)palloc0(sizeof(Oid) * (num_parents + 1));
    tuple.typbyval = (bool *)palloc0(sizeof(bool) * (num_parents + 1));
    tuple.typlen = (int16 *)palloc0(sizeof(int16) * (num_parents + 1));

    tuple.values[0] = origin_tuple->values[targetid];
    tuple.isnull[0] = origin_tuple->isnull[targetid];
    tuple.typid[0] = origin_tuple->typid[targetid];
    tuple.typbyval[0] = origin_tuple->typbyval[targetid];
    tuple.typlen[0] = origin_tuple->typlen[targetid];
    for (int id = 0; id < num_parents; id++) {
        tuple.values[id + 1] = origin_tuple->values[parentids[id]];
        tuple.isnull[id + 1] = origin_tuple->isnull[parentids[id]];
        tuple.typid[id + 1] = origin_tuple->typid[parentids[id]];
        tuple.typbyval[id + 1] = origin_tuple->typbyval[parentids[id]];
        tuple.typlen[id + 1] = origin_tuple->typlen[parentids[id]];
    }
    tuple.ncolumns = num_parents + 1;
    return tuple;
}

static int first_cmp(const void *lhs, const void *rhs)
{
    return cmp_value_bayesnet<ValueInTuple>((void *)(*(ValueInTuple **)lhs), (void *)(*(ValueInTuple **)rhs));
}

static int second_cmp(const void *lhs, const void *rhs)
{
    uint32_t l_usage = (*(ValueMapEntry **)lhs)->cnt;
    uint32_t r_usage = (*(ValueMapEntry **)rhs)->cnt;
    if (l_usage < r_usage) {
        return +1;
    } else if (l_usage == r_usage) {
        return 0;
    } else {
        return -1;
    }
}

static int set_cmp(const void *lhs, const void *rhs)
{
    ValueInTuple ll = *(*(bayesDatumList::iterator *)lhs);
    ValueInTuple rr = *(*(bayesDatumList::iterator *)rhs);
    return cmp_value_bayesnet<ValueInTuple>((void *)(&ll), (void *)(&rr));
}

static void copy_datum_list(bayesDatumList *mcv, bayesDatumList *bin, HTAB *value_stats,
                            bayesDatumList *features)
{
    // MCV and NOTHING should be sorted before copy into featurematrix
    int pos;
    if (mcv->size() > 0) {
        bayesDatumList::iterator *entries =
            (bayesDatumList::iterator *)palloc0(mcv->size() * sizeof(bayesDatumList::iterator));
        pos = 0;
        for (bayesDatumList::iterator it = mcv->begin(); it != mcv->end(); ++it) {
            entries[pos++] = it;
        }
        qsort(entries, mcv->size(), sizeof(bayesDatumList::iterator), set_cmp);
        for (uint32_t i = 0; i < mcv->size(); i++) {
            features->push_back(*entries[i]);
        }
        pfree(entries);
    } else if (bin->size() > 0) {
        for (bayesDatumList::iterator it = bin->begin(); it != bin->end(); ++it) {
            features->push_back(*it);
        }
    } else {
        uint32_t numval = hash_get_num_entries(value_stats);
        ValueMapEntry **entries =
            (ValueMapEntry **)palloc0(numval * sizeof(ValueMapEntry *));
        pos = 0;
        HASH_SEQ_STATUS seq;
        hash_seq_init(&seq, value_stats);
        ValueMapEntry *entry;
        while ((entry = (ValueMapEntry *)hash_seq_search(&seq)) != NULL) {
            entries[pos++] = entry;
        }
        qsort(entries, numval, sizeof(ValueMapEntry *), first_cmp);
        for (uint32_t i = 0; i < numval; i++) {
            features->push_back(entries[i]->value);
        }
        pfree(entries);
    }
    return;
}

/* ----------
 * Estimate the number of distinct values using the estimator
 * proposed by Haas and Stokes in IBM Research Report RJ 10025:
 *      n*d / (n - f1 + f1*n/N)
 * where f1 is the number of distinct values that occurred
 * exactly once in our sample of n rows (from a total of N),
 * and d is the total number of distinct values in the sample.
 * This is their Duj1 estimator; the other estimators they
 * recommend are considerably more complex, and are numerically
 * very unstable when n is much smaller than N.
 *
 * Overwidth values are assumed to have been distinct.
 * ----------
 */
static uint64_t estimate_ndistinct_from_sample(double n, double N, double d, double f1)
{
    /* if n < 1. It's an empty bucket, the ndistinct becomes usesless.
 
     */
    if (n < 1) { 
        return 1;
    }
    /* if n >= 1, d must be more than/equal to 1.
     */
    Assert(n >= 1 && d >= 1);
    /* if n >= N, we take it as they are equal because of the precision errors.
     */
    if (n >= N) { 
        return (uint64_t)Max(d, 1.0);
    }    
    return (uint64_t)(n * d / (n - f1 + f1 * n / N));
}


static void bayes_net_vars_initialize(TrainModelState *pstate, bayesState *bayes_net_state,
                                      BayesNetTrainingVars *bayes_net_training_vars, Model *model)
{
    int nattrs = pstate->tuple.ncolumns;
    bayes_net_training_vars->featuresmatrix = (bayesDatumList **)palloc0(sizeof(bayesDatumList *) *
                                                                         bayes_net_state->num_nodes);
    bayes_net_training_vars->cdep = (bayesContinNetList **)palloc0(sizeof(bayesContinNetList *) *
                                                                   bayes_net_state->num_nodes);
    for (int i = 0; i < bayes_net_state->num_nodes; i++) {
        bayes_net_training_vars->featuresmatrix[i] = (bayesDatumList *)palloc0(sizeof(bayesDatumList) * nattrs);
        bayes_net_training_vars->cdep[i] = (bayesContinNetList *)palloc0(sizeof(bayesContinNetList) * nattrs);
        for (int j = 0; j < nattrs; j++) {
            bayes_net_training_vars->featuresmatrix[i][j] = bayesDatumList();
            bayes_net_training_vars->cdep[i][j] = bayesContinNetList();
        }
    }
    bayes_net_training_vars->bins = (bayesDatumList *)palloc0(sizeof(bayesDatumList) * nattrs);
    bayes_net_training_vars->mcvs = (bayesDatumList *)palloc0(sizeof(bayesDatumList) * nattrs);
    for (int j = 0; j < nattrs; j++) {
        bayes_net_training_vars->bins[j] = bayesDatumList();
        bayes_net_training_vars->mcvs[j] = bayesDatumList();
    }
    bayes_net_training_vars->value_stats = (HTAB **)palloc0(sizeof(HTAB *) * nattrs);
    bayes_net_training_vars->bin_widths = (int *)palloc0(sizeof(int) * nattrs);
    bayes_net_training_vars->data_preprocess = (int *)palloc0(sizeof(int) * nattrs);
    bayes_net_training_vars->coefficients = (double **)palloc0(sizeof(double *) * nattrs);
    bayes_net_training_vars->num_feature_tuple = (tupleMatrix *)palloc0(sizeof(tupleMatrix) *
                                                                        bayes_net_state->num_nodes);
    bayes_net_training_vars->offsets = (int *)palloc0(sizeof(int) * bayes_net_state->num_nodes);

    for (int i = 0; i < nattrs; i++) {
        bayes_net_training_vars->value_stats[i] = initialize_hashmap(model->memory_context);
    }

    bayes_net_training_vars->parentids = (int **)palloc0(sizeof(int *) * bayes_net_state->num_nodes);
    bayes_net_training_vars->num_parents = (int *)palloc0(sizeof(int) * bayes_net_state->num_nodes);
    for (int i = 0; i < bayes_net_state->num_nodes; i++) {
        bayes_net_training_vars->parentids[i] = (int *)palloc0(sizeof(int) * bayes_net_state->num_nodes);
        bayes_net_training_vars->num_parents[i] = 0;
        for (int id = 0; id < bayes_net_state->num_nodes; id++) {
            if (bayes_net_state->parents[i * bayes_net_state->num_nodes + id]) {
                bayes_net_training_vars->parentids[i][bayes_net_training_vars->num_parents[i]++] = id;
            }
        }
        bayes_net_state->bayes_tmss[i]->category = (uint32_t *)palloc0(sizeof(uint32_t) *
                                                   (bayes_net_training_vars->num_parents[i] + 1));
    }
    bayes_net_training_vars->attrtype = NULL;
}

static void bayes_net_vars_delete(TrainModelState *pstate, bayesState *bayes_net_state,
                                  BayesNetTrainingVars *bayes_net_training_vars, Model *model)
{
    int nattrs = pstate->tuple.ncolumns;
    // free temp variables
    for (int i = 0; i < bayes_net_state->num_nodes; i++) {
        pfree_ext(bayes_net_training_vars->featuresmatrix[i]);
        pfree_ext(bayes_net_training_vars->cdep[i]);
        pfree_ext(bayes_net_training_vars->parentids[i]);
        pfree_ext(bayes_net_training_vars->coefficients[i]);
    }
    for (int i = 0; i < nattrs; i++) {
        hash_destroy(bayes_net_training_vars->value_stats[i]);
    }
    pfree_ext(bayes_net_training_vars->offsets);
    pfree_ext(bayes_net_training_vars->featuresmatrix);
    pfree_ext(bayes_net_training_vars->bins);
    pfree_ext(bayes_net_training_vars->mcvs);
    pfree_ext(bayes_net_training_vars->value_stats);
    pfree_ext(bayes_net_training_vars->data_preprocess);
    pfree_ext(bayes_net_training_vars->coefficients);

    pfree_ext(bayes_net_training_vars->cdep);
    pfree_ext(bayes_net_training_vars->num_feature_tuple);

    pfree_ext(bayes_net_training_vars->bin_widths);
    pfree_ext(bayes_net_training_vars->parentids);
    pfree_ext(bayes_net_training_vars->num_parents);
}

static void do_data_descretize(TrainModelState *pstate, bayesState *bayes_net_state,
                               BayesNetTrainingVars *bayes_net_training_vars,
                               Model *model, uint32_t total_cardinality)
{
    int nattrs = pstate->tuple.ncolumns;
    for (int i = 0; i < nattrs; i++) {
        int num_statistic_value = (int)hash_get_num_entries(bayes_net_training_vars->value_stats[i]);
        ValueMapEntry **entries =
            (ValueMapEntry **)palloc0(num_statistic_value * sizeof(ValueMapEntry *));
        uint32_t pos = 0;
        uint32_t num_null = 0;
        uint32_t max_freq = 0;
        HASH_SEQ_STATUS seq;
        hash_seq_init(&seq, bayes_net_training_vars->value_stats[i]);
        ValueMapEntry *entry;
        while ((entry = (ValueMapEntry *)hash_seq_search(&seq)) != NULL) {
            entries[pos++] = entry;
            if (entry->value.isnull) {
                num_null += entry->cnt;
            } else if (entry->cnt > max_freq) {
                max_freq = entry->cnt;
            }
        }
        // If frequency of mcv larger than a bin size, do not use bins

        if ((bayes_net_state->num_bins > 0) and IsNumber(bayes_net_training_vars->attrtype[i]) and
            (num_statistic_value > bayes_net_state->num_bins) and
            (max_freq < (uint32_t)((total_cardinality - num_null) / bayes_net_state->num_bins))) {
            // Get Bins Values
            // | lower1 | lower2 | lower3 | ... | lowern | highest [| null]|
            int size_bin = (int)ceil((double)(total_cardinality - num_null) / bayes_net_state->num_bins);
            bayes_net_training_vars->bin_widths[i] = size_bin;
            bayes_net_training_vars->data_preprocess[i] = BAYES_BIN;
            qsort(entries, num_statistic_value, sizeof(ValueMapEntry *), first_cmp);
            uint32_t current_size = 0;
            pos = 0;
            int value_idx = 0;
            const int max_possible_extra = 3;
            const int upper_bound_pos_with_null = -2;
            int nmultiple = 0;
            int ndistinct = 0;
            bayes_net_training_vars->coefficients[i] = (double *)palloc0(sizeof(double) *
                                                       (bayes_net_state->num_bins + max_possible_extra));
            for (int j = 0; j < num_statistic_value; j++) {
                if (entries[j]->value.isnull) {
                    break;
                }
                pos = current_size / size_bin;
                Assert(pos <= bayes_net_training_vars->bins[i].size());
                if (pos == bayes_net_training_vars->bins[i].size()) {
                    bayes_net_training_vars->bins[i].push_back(entries[j]->value);
                    if (j > value_idx) {
                        ndistinct = j - value_idx;
                        bayes_net_training_vars->
                            coefficients[i][bayes_net_training_vars->bins[i].size() + upper_bound_pos_with_null] = 1.0 /
                                estimate_ndistinct_from_sample(
                                    (double)bayes_net_state->num_sample_rows / bayes_net_state->num_bins,
                                    (double)bayes_net_state->num_total_rows / bayes_net_state->num_bins,
                                    ndistinct, ndistinct - nmultiple);
                        value_idx = j;
                    }
                    nmultiple = 0;
                }
                if (entries[j]->cnt > 1) {
                    nmultiple++;
                }
                current_size += entries[j]->cnt;
            }
            ndistinct = num_statistic_value - value_idx;
            bayes_net_training_vars->coefficients[i][bayes_net_training_vars->bins[i].size() - 1] = 1.0 /
                estimate_ndistinct_from_sample(
                    (double)bayes_net_state->num_sample_rows / bayes_net_state->num_bins,
                    (double)bayes_net_state->num_total_rows / bayes_net_state->num_bins,
                    ndistinct, ndistinct - nmultiple);
            const int upper_bound_pos_without_null = -1;
            if (num_null > 0) {
                // Save the maximal value only once
                if ((num_statistic_value > 1) and
                    cmp_value_bayesnet<ValueInTuple>(entries[num_statistic_value + upper_bound_pos_with_null],
                                                     &bayes_net_training_vars->bins[i][bayes_net_training_vars->bins[i].size() - 1]) > 0) {
                        bayes_net_training_vars->bins[i].push_back(entries[num_statistic_value + upper_bound_pos_with_null]->value);
                        bayes_net_training_vars->coefficients[i][bayes_net_training_vars->bins[i].size() - 1] = 1.0;
                }
                // Save the NULL
                bayes_net_training_vars->bins[i].push_back(entries[num_statistic_value - 1]->value);
                bayes_net_training_vars->coefficients[i][bayes_net_training_vars->bins[i].size() - 1] = 1.0;
            } else if ((num_statistic_value > 0) and
                    cmp_value_bayesnet<ValueInTuple>(entries[num_statistic_value + upper_bound_pos_without_null],
                                                     &bayes_net_training_vars->bins[i][bayes_net_training_vars->bins[i].size() - 1]) > 0) {
                        bayes_net_training_vars->bins[i].push_back(entries[num_statistic_value + upper_bound_pos_without_null]->value);
                        bayes_net_training_vars->coefficients[i][bayes_net_training_vars->bins[i].size() - 1] = 1.0;
            }
            bayes_net_training_vars->coefficients[i][bayes_net_training_vars->bins[i].size()] = 0.0;
        } else if (bayes_net_state->num_mcvs > 0 and num_statistic_value > bayes_net_state->num_mcvs) {
            // Get Most Common Values
            bayes_net_training_vars->data_preprocess[i] = BAYES_MCV;
            bayes_net_training_vars->bin_widths[i] = num_statistic_value - bayes_net_state->num_mcvs;
            qsort(entries, num_statistic_value, sizeof(ValueMapEntry *), second_cmp);
            pos = 0;
            bayes_net_training_vars->coefficients[i] = (double *)palloc0(sizeof(double) * (bayes_net_state->num_mcvs + 1));
            unsigned int used_cnt = 0;
            unsigned int nmultiple = 0;
            unsigned int ndistinct = 0;
            while (pos < (uint32_t)num_statistic_value) {
                if (pos < (uint32_t)bayes_net_state->num_mcvs) {
                    used_cnt += entries[pos]->cnt;
                    bayes_net_training_vars->mcvs[i].push_back(entries[pos]->value);
                    bayes_net_training_vars->coefficients[i][pos] = 1.0;
                } else if (entries[pos]->cnt > 1) {
                    nmultiple++;
                } else {
                    break;
                }
                pos++;
            }
            ndistinct = num_statistic_value - bayes_net_state->num_mcvs;
            double total_rows_discount =
                (double)(bayes_net_state->num_sample_rows - used_cnt) / (double)bayes_net_state->num_sample_rows;
            bayes_net_training_vars->coefficients[i][bayes_net_state->num_mcvs] =
                1.0 / estimate_ndistinct_from_sample(bayes_net_state->num_sample_rows - used_cnt,
                                                     ((double)bayes_net_state->num_total_rows * total_rows_discount),
                                                     ndistinct, ndistinct - nmultiple);
        } else {
            if (num_statistic_value > MAX_DISTINCT_NO_BIN_MCV) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                                errmsg("Too many distinct values (%lld), try to reduce sample size.",
                                       (long long)num_statistic_value)));
            }
            bayes_net_training_vars->coefficients[i] = (double *)palloc0(sizeof(double) * (num_statistic_value + 1));
            for (int j = 0; j < num_statistic_value + 1; j++) {
                bayes_net_training_vars->coefficients[i][j] = 1.0;
            }
        }
        pfree(entries);
    }
}

static void data_descretize(TrainModelState *pstate, bayesState *bayes_net_state,
                            BayesNetTrainingVars *bayes_net_training_vars, Model *model)
{
    int nattrs = pstate->tuple.ncolumns;
    // Scan for descretizing the datasets
    uint32_t total_cardinality = 0;
    ModelTuple const *outer_tuple_slot = nullptr;
    // Get data for discretizing
    while (true) {
        outer_tuple_slot = pstate->fetch(pstate->callback_data, &pstate->tuple) ? &pstate->tuple : nullptr;
        if (outer_tuple_slot == nullptr) {
            break;
        }
        bayes_net_training_vars->attrtype = pstate->tuple.typid;
        for (int i = 0; i < nattrs; i++) {
            uint32_t ndistinct = hash_get_num_entries(bayes_net_training_vars->value_stats[i]);
            if (ndistinct >= MAX_DISTINCT_SUPPORT) {
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                                errmsg("Too many distinct values (%lld), try to reduce sample size.",
                                       (long long)ndistinct)));
            }
            ValueInTuple val = create_value(pstate->tuple.values[i], pstate->tuple.typid[i], pstate->tuple.isnull[i]);
            MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);
            bool found = false;
            ValueMapEntry *entry = (ValueMapEntry *)hash_search(bayes_net_training_vars->value_stats[i],
                                                                &val, HASH_ENTER, &found);
            if (found) {
                entry->cnt++;
            } else {
                entry->value = val;
                entry->cnt = 1;
            }
            MemoryContextSwitchTo(oldcxt);
        }
        total_cardinality += 1;
    }
    do_data_descretize(pstate, bayes_net_state, bayes_net_training_vars, model, total_cardinality);
    // construct cdep, featuresmatrix
    for (int nodeid = 0; nodeid < bayes_net_state->num_nodes; nodeid++) {
        bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[nodeid];
        bayes_state->returntype = FLOAT8OID;
        copy_datum_list(&bayes_net_training_vars->mcvs[nodeid], &bayes_net_training_vars->bins[nodeid],
                        bayes_net_training_vars->value_stats[nodeid],
                        &bayes_net_training_vars->featuresmatrix[nodeid][0]);
        bayes_state->category[0] = bayes_net_training_vars->featuresmatrix[nodeid][0].size();
        for (int i = 0; i < bayes_net_training_vars->num_parents[nodeid]; i++) {
            int attr_id = bayes_net_training_vars->parentids[nodeid][i];
            copy_datum_list(&bayes_net_training_vars->mcvs[attr_id], &bayes_net_training_vars->bins[attr_id],
                            bayes_net_training_vars->value_stats[attr_id],
                            &bayes_net_training_vars->featuresmatrix[nodeid][i + 1]);
            if (IsContinuous(bayes_net_training_vars->attrtype[attr_id])) {
                for (uint32_t j = 0; j < bayes_state->category[0] + 1; j++) {
                    ContinDescriptionNet item;
                    bayes_net_training_vars->cdep[nodeid][i + 1].push_back(item);
                }
                bayes_state->category[i + 1] = bayes_net_training_vars->cdep[nodeid][i + 1][0].getCategory();
            } else {
                bayes_state->category[i + 1] = bayes_net_training_vars->featuresmatrix[nodeid][i + 1].size();
            }
        }
    }
    pstate->rescan(pstate->callback_data);
}

static void gaussian_fit(TrainModelState *pstate, bayesState *bayes_net_state,
                         BayesNetTrainingVars *bayes_net_training_vars)
{
    ModelTuple const *outer_tuple_slot = nullptr;
    while (true) {
        outer_tuple_slot = pstate->fetch(pstate->callback_data, &pstate->tuple) ? &pstate->tuple : nullptr;
        if (outer_tuple_slot == nullptr) {
            break;
        }
        for (int nodeid = 0; nodeid < bayes_net_state->num_nodes; nodeid++) {
            bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[nodeid];
            bayes_state->tms.tuple = select_tuple(bayes_net_training_vars->num_parents[nodeid],
                                                  bayes_net_training_vars->parentids[nodeid],
                                                  nodeid, &pstate->tuple);
            int nattrs = bayes_state->tms.tuple.ncolumns;

            bayes_state->attrtype = bayes_state->tms.tuple.typid;
            ValueInTuple val = create_value(bayes_state->tms.tuple.values[0], bayes_state->tms.tuple.typid[0],
                                            bayes_state->tms.tuple.isnull[0]);
            int loc = findDiscreteIndex(val, &bayes_net_training_vars->featuresmatrix[nodeid][0],
                                        bayes_net_training_vars->data_preprocess[nodeid]);
            if (loc < 0) {
                loc = bayes_net_training_vars->featuresmatrix[nodeid][0].size();
            }

            for (int i = 1; i < nattrs; i++) {
                if (IsContinuous(bayes_state->attrtype[i])) {
                    if (bayes_state->tms.tuple.isnull[i])
                        bayes_net_training_vars->cdep[nodeid][i][loc].addNumnulls();
                    else {
                        bayes_net_training_vars->cdep[nodeid][i][loc].setMeanMinMax(DatumGetFloat8(bayes_state->tms.tuple.values[i]));
                    }
                    bayes_net_training_vars->cdep[nodeid][i][loc].addNumrows();
                }
            }
        }
    }

    pstate->rescan(pstate->callback_data);
}


static void probability_fit(TrainModelState *pstate, bayesState *bayes_net_state,
                            BayesNetTrainingVars *bayes_net_training_vars)
{
    ModelTuple const *outer_tuple_slot = nullptr;
    for (int nodeid = 0; nodeid < bayes_net_state->num_nodes; nodeid++) {
        bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[nodeid];
        int sum_cat = 0;
        int tg_cat = bayes_state->category[0] + 1;
        int nattrs = bayes_net_training_vars->num_parents[nodeid] + 1;
        bayes_net_training_vars->num_feature_tuple[nodeid] = (int **)palloc(sizeof(int *) * nattrs);
        bayes_net_training_vars->num_feature_tuple[nodeid][0] = (int *)palloc0(sizeof(int) * tg_cat * tg_cat);
        sum_cat += tg_cat;
        for (int i = 1; i < nattrs; i++) {
            if (IsContinuous(bayes_state->attrtype[i])) {
                sum_cat += (bayes_state->category[i]);
            } else {
                bayes_net_training_vars->num_feature_tuple[nodeid][i] = (int *)palloc0(sizeof(int) *
                                                                        tg_cat *
                                                                        (bayes_state->category[i] + 1));
                sum_cat += (bayes_state->category[i] + 1);
            }
        }
        bayes_state->prob = (double *)palloc0(sizeof(double) * (sum_cat - tg_cat + 1) * tg_cat);
    }

    while (!bayes_net_state->done) {
        outer_tuple_slot = pstate->fetch(pstate->callback_data, &pstate->tuple) ? &pstate->tuple : nullptr;
        if (outer_tuple_slot == nullptr) {
            break;
        }
        for (int nodeid = 0; nodeid < bayes_net_state->num_nodes; nodeid++) {
            bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[nodeid];
            bayes_state->tms.tuple = select_tuple(bayes_net_training_vars->num_parents[nodeid],
                                                  bayes_net_training_vars->parentids[nodeid],
                                                  nodeid, &pstate->tuple);
            bayes_handle_tuple(bayes_state, &bayes_state->tms.tuple,
                               bayes_net_training_vars->featuresmatrix[nodeid],
                               bayes_net_training_vars->cdep[nodeid],
                               bayes_net_training_vars->num_feature_tuple[nodeid],
                               bayes_net_training_vars->data_preprocess,
                               bayes_net_training_vars->parentids[nodeid], nodeid);
        }
        ++bayes_net_state->processedtuples;
    }

    // compute numdistincts for each continuous value
    for (int nodeid = 0; nodeid < bayes_net_state->num_nodes; nodeid++) {
        bayesNodeState *bayes_state = bayes_net_state->bayes_tmss[nodeid];
        bayes_state->category[0] = bayes_net_training_vars->featuresmatrix[nodeid][0].size();
        for (int i = 0; i < bayes_net_training_vars->num_parents[nodeid]; i++) {
            int attr_id = bayes_net_training_vars->parentids[nodeid][i];
            if (IsContinuous(bayes_net_training_vars->attrtype[attr_id])) {
                for (uint32_t j = 0; j < bayes_state->category[0] + 1; j++) {
                    bayes_net_training_vars->cdep[nodeid][i + 1][j].estimate_numdistincts();
                }
            }
        }
    }

    for (int node_id = 0; node_id < bayes_net_state->num_nodes; node_id++) {
        DiscreteProbParameters dpp = {bayes_net_training_vars->num_feature_tuple[node_id],
                                      &bayes_net_training_vars->offsets[node_id],
                                      bayes_net_training_vars->parentids[node_id],
                                      node_id, bayes_net_training_vars->coefficients};
        bayes_calculate(bayes_net_state, bayes_net_training_vars->cdep[node_id], &dpp);
    }
}

static void bayes_net_serialize(TrainModelState *pstate, bayesState *bayes_net_state,
                                BayesNetTrainingVars *bayes_net_training_vars, Model *model)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);
    // Serialize BayesNet Model
    int *sizes = (int *)palloc0(sizeof(int) * bayes_net_state->num_nodes);
    Model *submodels = (Model *)palloc0((sizeof(Model) * bayes_net_state->num_nodes));
    int total_size = 0;

    for (int i = 0; i < bayes_net_state->num_nodes; i++) {
        submodels[i].memory_context = model->memory_context;
        bayesPrepareAndSerielized(&submodels[i], bayes_net_state->bayes_tmss[i],
                                  bayes_net_training_vars->featuresmatrix[i], bayes_net_training_vars->offsets[i]);
        sizes[i] = submodels[i].data.size;
        total_size += sizes[i];
    }

    model->return_type = bayes_net_state->returntype;
    model->processed_tuples = bayes_net_state->processedtuples;
    model->discarded_tuples = bayes_net_state->discardtuples;
    model->exec_time_secs = bayes_net_state->executiontime;
    
    const int num_int_array = 3;
    model->data.size = total_size + sizeof(SerializedBayesNet) +
                       sizeof(int) * bayes_net_state->num_nodes * num_int_array +
                       sizeof(bool) * bayes_net_state->num_nodes * bayes_net_state->num_nodes;
    SerializedBayesNet *sbn = (SerializedBayesNet *)palloc0(model->data.size);
    model->data.raw_data = sbn;
    model->data.version = DB4AI_MODEL_V01;
    /*
     * | num_nodes(n) | size1 ... sizen | raw1 ... rawn | data_preprocess[1 ... n] | bin_width[1 ... n] | parents[1 ...
     * nxn] |
     */
    sbn->num_nodes = bayes_net_state->num_nodes;

    int8_t *ptr = (int8_t *)(sbn + 1);
    int avail = model->data.size - sizeof(SerializedBayesNet);

    int chunk_size = sizeof(int) * sbn->num_nodes;
    errno_t rc = memcpy_s((int *)ptr, avail, sizes, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    for (int i = 0; i < sbn->num_nodes; i++) {
        chunk_size = sizes[i];
        rc = memcpy_s(ptr, avail, submodels[i].data.raw_data, chunk_size);
        securec_check_ss(rc, "\0", "\0");
        avail -= chunk_size;
        ptr += chunk_size;
    }

    chunk_size = sizeof(int) * sbn->num_nodes;
    rc = memcpy_s((int *)ptr, avail, bayes_net_training_vars->data_preprocess, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(int) * sbn->num_nodes;
    rc = memcpy_s((int *)ptr, avail, bayes_net_training_vars->bin_widths, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(bool) * sbn->num_nodes * sbn->num_nodes;
    rc = memcpy_s((bool *)ptr, avail, bayes_net_state->parents, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    model->status = ERRCODE_SUCCESSFUL_COMPLETION;

    pfree(sizes);
    pfree(submodels);

    model->memory_context = NULL;

    MemoryContextSwitchTo(oldcxt);
}

void bayes_run(AlgorithmAPI *self, TrainModelState *pstate, Model *model)
{
    bayesState *bayes_state = reinterpret_cast<bayesState *>(pstate);
    if (pstate->tuple.ncolumns != bayes_state->num_nodes) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Incorrect Hyperparameters (number of columns should match num_nodes)")));
    }
    BayesNetTrainingVars bayes_net_training_vars;
    bayes_net_vars_initialize(pstate, bayes_state, &bayes_net_training_vars, model);
    struct timespec exec_start_time, exec_end_time;
    clock_gettime(CLOCK_MONOTONIC, &exec_start_time);
  
    data_descretize(pstate, bayes_state, &bayes_net_training_vars, model);
    gaussian_fit(pstate, bayes_state, &bayes_net_training_vars);
    probability_fit(pstate, bayes_state, &bayes_net_training_vars);

    clock_gettime(CLOCK_MONOTONIC, &exec_end_time);
    bayes_state->done = true;

    bayes_state->executiontime = interval_to_sec(time_diff(&exec_end_time, &exec_start_time));
    ++pstate->finished;
    bayes_net_serialize(pstate, bayes_state, &bayes_net_training_vars, model);
    
    bayes_net_vars_delete(pstate, bayes_state, &bayes_net_training_vars, model);
}

static void bayes_net_run(AlgorithmAPI *self, TrainModelState *pstate, Model **models)
{
    Model *model = models[0];
    Assert(pstate->finished == 0);
    auto bayes_net_state = reinterpret_cast<bayesState *>(pstate);
    bayes_net_state->returntype = FLOAT8OID;
    bayes_run(self, pstate, model);
}

void bayes_net_end(AlgorithmAPI *self, TrainModelState *pstate)
{
    bayesState *bayes_net_state = reinterpret_cast<bayesState *>(pstate);

    pfree(bayes_net_state->parents);
    for (int i = 0; i < bayes_net_state->num_nodes; i++) {
        pfree(bayes_net_state->bayes_tmss[i]->category);
        pfree(bayes_net_state->bayes_tmss[i]->prob);
        bayes_net_state->bayes_tmss[i]->chunk_size = 0U;
        pfree(bayes_net_state->bayes_tmss[i]->tms.tuple.values);
        pfree(bayes_net_state->bayes_tmss[i]->tms.tuple.isnull);
        pfree(bayes_net_state->bayes_tmss[i]->tms.tuple.typid);
        pfree(bayes_net_state->bayes_tmss[i]->tms.tuple.typbyval);
        pfree(bayes_net_state->bayes_tmss[i]->tms.tuple.typlen);
        pfree(bayes_net_state->bayes_tmss[i]);
    }
    pfree(bayes_net_state->bayes_tmss);
}

// ---------------------------------------------------------------------------------------
double probabilityDensityRange(double x1, double x2, double mean, double stdDiv)
{
    double prob = 0.0;
    if (stdDiv == 0.0) {
        if (mean <= x2 and mean >= x1) {
            prob = 1.0;
        }
    } else {
        Assert(x2 > x1);
        boost::math::normal_distribution<double> norm(mean, stdDiv);
        prob = boost::math::cdf(norm, x2) - boost::math::cdf(norm, x1);
    }
    return prob;
}

static void bayesNodeDeserialize(Bayes *bayes, const SerializedModel *model, Oid returntype, SerializedModelBayesNode *bym)
{
    bym->algorithm = bayes;
    bym->return_type = returntype;
    if (model->version != DB4AI_MODEL_V01)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS), errmsg("Invalid Bayes model version")));

    ModelBayesNodeV01 *mdata = (ModelBayesNodeV01 *)model->raw_data;
    int avail = model->size - sizeof(ModelBayesNodeV01);
    bym->features_cols = mdata->features_cols;
    bym->categories_size = mdata->categories_size;
    bym->prob_offset = mdata->prob_offset;

    int nattrs = bym->features_cols;
    uint8_t *ptr = (uint8_t *)(mdata + 1);
    /* nattrs is within [1,64].
     */
    int chunk_size = sizeof(uint32_t) * nattrs;
    if (avail < chunk_size)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Model data corrupted reading description of attributes.")));

    bym->category_num = (uint32_t *)palloc(chunk_size);
    errno_t rc = memcpy_s(bym->category_num, chunk_size, (uint32_t *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(Oid) * nattrs;
    if (avail < chunk_size)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Model data corrupted reading attributes types")));
    bym->attrtype = (Oid *)palloc(chunk_size);
    rc = memcpy_s(bym->attrtype, chunk_size, (Oid *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(double) * bym->prob_offset;
    if (avail < chunk_size)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Model data corrupted reading probability")));
    bym->prob = (double *)palloc(chunk_size);
    rc = memcpy_s(bym->prob, chunk_size, (double *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    chunk_size = sizeof(uint32_t) * nattrs;
    if (avail < chunk_size)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Model data corrupted reading probability")));
    uint32_t *category_sizes = (uint32_t *)palloc(chunk_size);
    rc = memcpy_s(category_sizes, chunk_size, (uint32_t *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    avail -= chunk_size;
    ptr += chunk_size;

    // create featuresmatrix
    bym->featuresmatrix_fornet = (bayesDatumList **)palloc(nattrs * sizeof(bayesDatumList *));
    rc = memset_s(bym->featuresmatrix_fornet, nattrs * sizeof(void *), 0, nattrs * sizeof(void *));
    securec_check_ss(rc, "\0", "\0");
    for (int i = 0; i < nattrs; i++) {
        bym->featuresmatrix_fornet[i] = (bayesDatumList *)palloc(sizeof(bayesDatumList));
        bym->featuresmatrix_fornet[i]->initialize_vector();
    }

    for (int i = 0; i < nattrs; i++) {
        chunk_size = (int)category_sizes[i];
        ArrayType *arr = (ArrayType *)palloc(chunk_size);
        rc = memcpy_s(arr, chunk_size, (ArrayType *)ptr, chunk_size);
        securec_check_ss(rc, "\0", "\0");
        if (avail < chunk_size)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS),
                            errmsg("Model data corrupted reading categories")));
        Datum dt = 0;
        bool isnull = false;
        ArrayIterator it = array_create_iterator(arr, 0);
        while (array_iterate(it, &dt, &isnull)) {
            ValueInTuple val = create_value(dt, bym->attrtype[i], isnull);
            bym->featuresmatrix_fornet[i]->push_back(val);
        }
        array_free_iterator(it);
        avail -= chunk_size;
        ptr += chunk_size;
    }
    pfree(category_sizes);
}

static void bayesNetDeserialize(BayesNet *bayes_net, const SerializedModel *model, Oid returntype,
                                SerializedModelBayesNet *bynm)
{
    bynm->algorithm = bayes_net;
    bynm->return_type = returntype;
    if (model->version != DB4AI_MODEL_V01)
        ereport(ERROR,
                (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_STATUS), errmsg("Invalid Bayes Net model version")));
    SerializedBayesNet *sbn = (SerializedBayesNet *)model->raw_data;
    bynm->num_nodes = sbn->num_nodes;

    uint8_t *ptr = (uint8_t *)(sbn + 1);

    int chunk_size = sizeof(int) * bynm->num_nodes;
    bynm->sizes = (int *)palloc(chunk_size);
    errno_t rc = memcpy_s(bynm->sizes, chunk_size, (int *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    ptr += chunk_size;

    bynm->targets_singlecol_prob = (double **)palloc0(bynm->num_nodes * sizeof(double *));

    bynm->nodes = (SerializedModelBayesNode *)palloc(sizeof(SerializedModelBayesNode) * bynm->num_nodes);
    for (int i = 0; i < bynm->num_nodes; i++) {
        Bayes *bayes = (Bayes *)palloc(sizeof(Bayes));
        SerializedModel *sm = (SerializedModel *)palloc(sizeof(SerializedModel));
        sm->version = model->version;
        sm->size = bynm->sizes[i];
        sm->raw_data = (void *)ptr;
        bayesNodeDeserialize(bayes, sm, returntype, &bynm->nodes[i]);
        ptr += bynm->sizes[i];
        bynm->targets_singlecol_prob[i] = bynm->nodes[i].prob;
    }

    chunk_size = sizeof(int) * bynm->num_nodes;
    bynm->data_process = (int *)palloc(chunk_size);
    rc = memcpy_s(bynm->data_process, chunk_size, (int *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    ptr += chunk_size;

    chunk_size = sizeof(int) * bynm->num_nodes;
    bynm->bin_widths = (int *)palloc(chunk_size);
    rc = memcpy_s(bynm->bin_widths, chunk_size, (int *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    ptr += chunk_size;

    chunk_size = sizeof(bool) * bynm->num_nodes * bynm->num_nodes;
    bynm->parents = (bool *)palloc(chunk_size);
    rc = memcpy_s(bynm->parents, chunk_size, (bool *)ptr, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    ptr += chunk_size;

    bynm->has_children = (bool *)palloc0(sizeof(bool) * bynm->num_nodes);
    for (int i = 0; i < bynm->num_nodes; i++) {
        for (int j = 0; j < bynm->num_nodes; j++) {
            bynm->has_children[i] |= bynm->parents[j * bynm->num_nodes + i];
        }
    }
}

ModelPredictor bayes_net_predict_prepare(AlgorithmAPI *self, SerializedModel const *model, Oid returntype)
{
    if (unlikely(!model))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("bayes predict prepare: model cannot be null")));

    if (unlikely(model->version != DB4AI_MODEL_V01))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("bayes predict prepare: currently only model V01 is supported")));

    SerializedModelBayesNet *model_bayes_net = (SerializedModelBayesNet *)palloc0(sizeof(SerializedModelBayesNet));

    bayesNetDeserialize((BayesNet *)self, model, returntype, model_bayes_net);
    return reinterpret_cast<ModelPredictor>(model_bayes_net);
}

/* For target column with eq filters, we need produce the probability density.
 */
void bayes_predict_prob_eq(SerializedModelBayesNode *bayes_model, TupleData tup,
                           AuxilaryData aux)
{
    Datum *values = tup.values;
    bool *isnull = tup.isnull;
    Oid *types = tup.types;
    int ncolumns = tup.ncolumns;

    int *selected_ids = aux.selected_ids;
    int target_colid = aux.target_colid;
    int *data_process = aux.data_process;
    double **singlecol_probs = aux.singlecol_probs;
    ProbVector **parent_prob = aux.parent_prob;

    if ((uint32_t)ncolumns != bayes_model->features_cols)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Bayes predict: the number of input columns is wrong.")));

    int prefix = 0;
    int f_index[ncolumns];
    uint32_t t_cat = bayes_model->category_num[0];

    ValueInTuple value = create_value(values[0], types[0], isnull[0]);
    f_index[0] = findDiscreteIndex(value, bayes_model->featuresmatrix_fornet[0], data_process[target_colid]);
    if (f_index[0] < 0)
        f_index[0] = t_cat;

    for (int i = 1; i < ncolumns; i++) {
        if (parent_prob[selected_ids[i - 1]]->numProb == 0) {  // parent with eq cond
            ValueInTuple value = create_value(values[i], types[i], isnull[i]);
            f_index[i] =
                findDiscreteIndex(value, bayes_model->featuresmatrix_fornet[i], data_process[selected_ids[i - 1]]);
            if (f_index[i] < 0)
                f_index[i] = bayes_model->category_num[i];
        }
        // otherwise prob_x *= 1.0
    }

    /*
     * prob: |{T1, T2, T3} | {A1t1, A1t2, A1t3, A2t1, A2t2, A2t3} | {...} | {meanT1, varT1, meanT2, varT2, meanT3, ...
     *                     ^                                              ^
     *                  prefix0                                        prefix1
     */
    // calculate P(X|Y)P(Y) => P(Y|X)=P(X|Y)P(Y)/P(X)
    uint32_t target_id = f_index[0];
    double prob_y = bayes_model->prob[target_id];  // P(Y)
    double prob_x_y = 1.0;                         // P(X|Y)=P(x1|Y)P(x2|Y)...P(xn|Y)
    double prob_y_x = 1.0;                         // P(Y|X)=P(X|Y)P(Y)/P(X)
    double prob_y_x_z = prob_y;                    // P(X|Z)P(Y|X): Z is previous variables
    double prob_x = 1.0;                           // P(X)=P(x1)P(x2)...P(xn)
    prefix = t_cat + 1;
    for (uint32_t j = 1; j < (uint32_t)ncolumns; j++) {
        if (parent_prob[selected_ids[j - 1]]->numProb == 0) {  // parent with eq cond
            prob_x = singlecol_probs[selected_ids[j - 1]][f_index[j]];
            if (IsContinuous(types[j])) {
                double mean = bayes_model->prob[prefix + target_id * bayes_model->category_num[j]];
                double std = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 1];
                double num_null = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 2];
                double num_total = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 3];
                double delta = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 4];
                if (isnull[j]) {
                    prob_x_y = num_null / num_total;
                } else {
                    double val = DatumGetFloat8(values[j]);
                    prob_x_y = probabilityDensityRange(val, val + delta, mean, std);
                }
                prefix += (t_cat + 1) * bayes_model->category_num[j];
            } else {
                prob_x_y = bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + f_index[j]];
                prefix += (t_cat + 1) * (bayes_model->category_num[j] + 1);
            }
            if (prob_x == 0) {
                prob_y_x_z *= 0;
            } else {
                prob_y_x = prob_x_y / prob_x;
                prob_y_x_z *= prob_y_x * parent_prob[selected_ids[j - 1]]->prob;
            }
        } else {  // parent with no cond \sigma_X(P(X|Z)P(Y|X))
            bayesDatumList *featuresmatrix = bayes_model->featuresmatrix_fornet[j];
            double cum_prob_y_x_z;
            if (IsContinuous(types[j])) {
                cum_prob_y_x_z = 0.0;
                double mean = bayes_model->prob[prefix + target_id * bayes_model->category_num[j]];
                double std = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 1];
                double num_null = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 2];
                double num_total = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 3];
                uint32_t enumerate_num = 0;
                if (num_null > 0) {  // has null
                    cum_prob_y_x_z +=
                        num_null / num_total * parent_prob[selected_ids[j - 1]]->probs[featuresmatrix->size() - 1];
                    enumerate_num = featuresmatrix->size() - 2;
                } else {
                    enumerate_num = featuresmatrix->size() - 1;
                }
                for (uint32_t jj = 0; jj < enumerate_num; jj++) {
                    prob_x_y = probabilityDensityRange(DatumGetFloat8((*featuresmatrix)[jj].data),
                                                       DatumGetFloat8((*featuresmatrix)[jj + 1].data), mean, std);
                    prob_x = singlecol_probs[selected_ids[j - 1]][jj];
                    double prob_x_z = parent_prob[selected_ids[j - 1]]->probs[jj];
                    // both prob_x and prob_x_z should multiply the bin_width, and thus
                    // they can be eliminated.
                    if (prob_x > 0) {
                        cum_prob_y_x_z += prob_x_y / prob_x * prob_x_z;
                    }
                }
                prefix += (t_cat + 1) * bayes_model->category_num[j];
            } else {
                cum_prob_y_x_z = 0.0;
                int binWidth = parent_prob[selected_ids[j - 1]]->binWidth;
                if (data_process[selected_ids[j - 1]] == BAYES_BIN) {
                    for (uint32_t jj = 0; jj < bayes_model->category_num[j]; jj++) {
                        prob_x_y = bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + jj];
                        prob_x = singlecol_probs[selected_ids[j - 1]][jj];
                        double prob_x_z = parent_prob[selected_ids[j - 1]]->probs[jj];
                        // both prob_x and prob_x_z should multiply the bin_width, and thus
                        // they can be eliminated.
                        if (prob_x > 0) {
                            if (jj == bayes_model->category_num[j] - 1 || (*featuresmatrix)[jj + 1].isnull) {
                                cum_prob_y_x_z += prob_x_y / prob_x * prob_x_z;
                            } else {
                                cum_prob_y_x_z += prob_x_y * binWidth / prob_x * prob_x_z;
                            }
                        }
                    }
                } else if (data_process[selected_ids[j - 1]] == BAYES_MCV) {
                    for (uint32_t jj = 0; jj < bayes_model->category_num[j]; jj++) {
                        prob_x_y = bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + jj];
                        prob_x = singlecol_probs[selected_ids[j - 1]][jj];
                        double prob_x_z = parent_prob[selected_ids[j - 1]]->probs[jj];
                        if (prob_x > 0) {
                            cum_prob_y_x_z += prob_x_y / prob_x * prob_x_z;
                        }
                    }
                    uint32_t other_loc = bayes_model->category_num[j];
                    prob_x_y = bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + other_loc];
                    prob_x = singlecol_probs[selected_ids[j - 1]][other_loc];
                    double prob_x_z = parent_prob[selected_ids[j - 1]]->probs[other_loc];
                    if (prob_x > 0) {
                        cum_prob_y_x_z += prob_x_y / prob_x * prob_x_z * binWidth;
                    }
                } else {
                    for (uint32_t jj = 0; jj < bayes_model->category_num[j]; jj++) {
                        prob_x_y = bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + jj];
                        prob_x = singlecol_probs[selected_ids[j - 1]][jj];
                        double prob_x_z = parent_prob[selected_ids[j - 1]]->probs[jj];
                        if (prob_x > 0) {
                            cum_prob_y_x_z += prob_x_y / prob_x * prob_x_z;
                        }
                    }
                }
                prefix += (t_cat + 1) * (bayes_model->category_num[j] + 1);
            }
            prob_y_x_z *= cum_prob_y_x_z;
        }
    }
    parent_prob[target_colid]->prob = prob_y_x_z;
}

void conditional_bayes_net_predict_direct(SerializedModelBayesNet *bayes_net_model, int num_parents, int *selected_ids,
                                          Datum *values, bool *isnull, Oid *types, int ncolumns, int target_colid,
                                          ProbVector **parent_prob)
{
    Datum new_values[num_parents + 1];
    bool new_isnull[num_parents + 1];
    Oid new_types[num_parents + 1];
    new_values[0] = values[target_colid];
    new_isnull[0] = isnull[target_colid];
    new_types[0] = types[target_colid];
    for (int i = 0; i < num_parents; i++) {
        new_values[i + 1] = values[selected_ids[i]];
        new_isnull[i + 1] = isnull[selected_ids[i]];
        new_types[i + 1] = types[selected_ids[i]];
    }
    // Notice that this is for debugging by using sql.
    // Formally, we should use a parameter to decide
    // whether it's a equal or range on each column.
    TupleData tup = {new_values, new_isnull, new_types, num_parents + 1};
    AuxilaryData aux = {selected_ids, target_colid, bayes_net_model->data_process,
                        bayes_net_model->targets_singlecol_prob, parent_prob};
    bayes_predict_prob_eq(&bayes_net_model->nodes[target_colid], tup, aux);

    // one node need to be mulplied only once
    for (int i = 0; i < num_parents; i++) {
        int colid = selected_ids[i];
        if (parent_prob[colid]->numProb == 0) {
            parent_prob[colid]->prob = 1.0;
        } else {
            for (int j = 0; j < parent_prob[colid]->numProb; j++) {
                parent_prob[colid]->probs[j] = 1.0;
            }
        }
    }
}

void bayes_net_predict_dfs_direct(SerializedModelBayesNet *bayes_net_model, Datum *values, bool *isnull, Oid *types,
                                  int ncolumns, int targetid, uint128 *visited, ProbVector **parent_prob)
{
    uint128 target = (uint128)1 << targetid;
    if (((*visited) & target) == target) {
        return;
    }
    (*visited) += target;
    int num_parents = 0;
    int selected_ids[bayes_net_model->num_nodes];
    for (int i = 0; i < bayes_net_model->num_nodes; i++) {
        if (bayes_net_model->parents[targetid * bayes_net_model->num_nodes + i]) {
            selected_ids[num_parents++] = i;
            bayes_net_predict_dfs_direct(bayes_net_model, values, isnull, types, ncolumns, i, visited,
                                     parent_prob);
        }
    }
    conditional_bayes_net_predict_direct(bayes_net_model, num_parents, selected_ids, values, isnull, types, ncolumns,
                                         targetid, parent_prob);
}

Datum bayes_net_predict_direct(AlgorithmAPI *, ModelPredictor model, Datum *values, bool *isnull, Oid *types,
                               int ncolumns)
{
    SerializedModelBayesNet *bayes_net_model = reinterpret_cast<SerializedModelBayesNet *>(model);
    ProbVector **parent_prob = (ProbVector **)palloc0(sizeof(ProbVector *) * ncolumns);
    for (int i = 0; i < ncolumns; i++) {
        parent_prob[i] = new ProbVector;
    }
    int targetid = 0;
    uint128 visited = 0;
    double prob = 1.0;
    while (targetid < ncolumns) {
        bayes_net_predict_dfs_direct(bayes_net_model, values, isnull, types, ncolumns, targetid, &visited, parent_prob);
        if (!bayes_net_model->has_children[targetid]) {
            if (parent_prob[targetid]->numProb == 0) {
                prob *= parent_prob[targetid]->prob;
            } else {
                double cum_prob = 0.0;
                for (int i = 0; i < parent_prob[targetid]->numProb; i++) {
                    cum_prob += parent_prob[targetid]->probs[i];
                }
                prob *= cum_prob;
            }
        }
        while ((visited & ((uint128)1 << targetid)) > 0) {
            targetid += 1;
        }
    }
    for (int i = 0; i < ncolumns; i++) {
        if (parent_prob[i]->numProb > 0) {
            pfree(parent_prob[i]->probs);
        }
        delete parent_prob[i];
    }
    pfree(parent_prob);
    return Float8GetDatum(prob);
}

double bayes_predict_prob(SerializedModelBayesNode *bayes_model, TupleData tup,
                          AuxilaryData aux)
{
    Datum *values = tup.values;
    bool *isnull = tup.isnull;
    Oid *types = tup.types;
    int ncolumns = tup.ncolumns;

    int *selected_ids = aux.selected_ids;
    int target_colid = aux.target_colid;
    int *data_process = aux.data_process;
    double **singlecol_probs = aux.singlecol_probs;

    if ((uint32_t)ncolumns != bayes_model->features_cols)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Bayes predict: the number of input columns is wrong.")));

    if (!checkTypeConsistent(bayes_model->attrtype, types, ncolumns))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Bayes predict: the input type mismatch with training data types.")));

    int prefix = 0;
    int *f_index = (int *)palloc0(sizeof(int) * ncolumns);
    uint32_t t_cat = bayes_model->category_num[0];

    ValueInTuple value = create_value(values[0], types[0], isnull[0]);
    f_index[0] = findDiscreteIndex(value, bayes_model->featuresmatrix_fornet[0], data_process[target_colid]);
    if (f_index[0] < 0)
        f_index[0] = t_cat;

    double prob_x = 1.0;
    for (int i = 1; i < ncolumns; i++) {
        if (IsContinuous(types[i])) {
            continue;
        }
        ValueInTuple value = create_value(values[i], types[i], isnull[i]);
        f_index[i] = findDiscreteIndex(value, bayes_model->featuresmatrix_fornet[i], data_process[selected_ids[i - 1]]);

        if (f_index[i] < 0)
            f_index[i] = bayes_model->category_num[i];

        // calculate P(X)=P(x1)p(x2)...p(xn) => P(Y|X)=P(X|Y)P(Y)/P(X)
        prob_x *= singlecol_probs[selected_ids[i - 1]][f_index[i]];
    }

    /*
     * prob: |{T1, T2, T3} | {A1t1, A1t2, A1t3, A2t1, A2t2, A2t3} | {...} | {meanT1, varT1, meanT2, varT2, meanT3, ...
     *                     ^                                              ^
     *                  prefix0                                        prefix1
     */
    // calculate P(X|Y)P(Y) => P(Y|X)=P(X|Y)P(Y)/P(X)
    double current_prob = 1.0;
    uint32_t target_id = f_index[0];
    current_prob = bayes_model->prob[target_id];
    prefix = t_cat + 1;
    for (uint32_t j = 1; j < (uint32_t)ncolumns; j++) {
        if (IsContinuous(types[j])) {
            double mean = bayes_model->prob[prefix + target_id * bayes_model->category_num[j]];
            double std = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 1];
            double num_null = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 2];
            double num_total = bayes_model->prob[prefix + target_id * bayes_model->category_num[j] + 3];
            if (isnull[j])
                current_prob *= num_null / num_total;
            else
                current_prob *= probabilityDensity(DatumGetFloat8(values[j]), mean, std);
            prefix += (t_cat + 1) * bayes_model->category_num[j];
        } else {
            current_prob *= bayes_model->prob[prefix + target_id * (bayes_model->category_num[j] + 1) + f_index[j]];
            prefix += (t_cat + 1) * (bayes_model->category_num[j] + 1);
        }
    }
    return current_prob / prob_x;
}

double conditional_bayes_net_predict(SerializedModelBayesNet *bayes_net_model, int num_parents, int *selected_ids,
                                     TupleData tup, int target_colid)
{
    Datum *values = tup.values;
    bool *isnull = tup.isnull;
    Oid *types = tup.types;

    Datum *new_values = (Datum *)palloc0(sizeof(Datum) * (num_parents + 1));
    bool *new_isnull = (bool *)palloc0(sizeof(bool) * (num_parents + 1));
    Oid *new_types = (Oid *)palloc0(sizeof(Oid) * (num_parents + 1));
    new_values[0] = values[target_colid];
    new_isnull[0] = isnull[target_colid];
    new_types[0] = types[target_colid];
    for (int i = 0; i < num_parents; i++) {
        new_values[i + 1] = values[selected_ids[i]];
        new_isnull[i + 1] = isnull[selected_ids[i]];
        new_types[i + 1] = types[selected_ids[i]];
    }
    TupleData new_tup = {new_values, new_isnull, new_types, num_parents + 1};
    AuxilaryData aux = {selected_ids, target_colid, bayes_net_model->data_process,
                        bayes_net_model->targets_singlecol_prob, NULL};
    double res = bayes_predict_prob(&bayes_net_model->nodes[target_colid], new_tup, aux);
    pfree(new_values);
    pfree(new_isnull);
    pfree(new_types);
    return res;
}

double bayes_net_predict_dfs(SerializedModelBayesNet *bayes_net_model, Datum *values, bool *isnull, Oid *types,
                             int ncolumns, int targetid, uint128 *visited)
{
    uint128 target = ((uint128)1 << targetid);
    if (((*visited) & target) == target) {
        return 1.0;
    }
    (*visited) += target;
    int num_parents = 0;
    int *selected_ids = (int *)palloc0(sizeof(int) * bayes_net_model->num_nodes);
    for (int i = 0; i < bayes_net_model->num_nodes; i++) {
        if (bayes_net_model->parents[targetid * bayes_net_model->num_nodes + i]) {
            selected_ids[num_parents++] = i;
        }
    }
    TupleData tup = {values, isnull, types, ncolumns};
    double prob = conditional_bayes_net_predict(bayes_net_model, num_parents, selected_ids, tup, targetid);
    for (int i = 0; i < num_parents; i++) {
        int next_targetid = selected_ids[i];
        prob *= bayes_net_predict_dfs(bayes_net_model, values, isnull, types, ncolumns, next_targetid, visited);
    }
    pfree(selected_ids);
    return prob;
}

Datum bayes_net_predict(AlgorithmAPI *, ModelPredictor model, Datum *values, bool *isnull, Oid *types, int ncolumns)
{
    struct timespec exec_start_time, exec_end_time;
    clock_gettime(CLOCK_MONOTONIC, &exec_start_time);
    SerializedModelBayesNet *bayes_net_model = reinterpret_cast<SerializedModelBayesNet *>(model);
    int targetid = 0;
    uint128 visited = 0;
    double prob = 1.0;
    while (targetid < ncolumns) {
        prob *= bayes_net_predict_dfs(bayes_net_model, values, isnull, types, ncolumns, targetid, &visited);
        while ((visited & ((uint128)1 << targetid)) > 0) {
            targetid += 1;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &exec_end_time);
    double predicttime = interval_to_sec(time_diff(&exec_end_time, &exec_start_time));
    elog(INFO, "prediction time: %f", predicttime);
    return Float8GetDatum(prob);
}

// ---------------------------------------------------------------------------------------

BayesNet bayes_net_internal = {
    /* AlgorithmAPI */
    {BAYES_NET_INTERNAL, "bayes_net_internal", ALGORITHM_ML_TARGET_MULTICLASS, nullptr, bayes_net_get_hyperparameters,
     bayes_net_make_hyperparameters, nullptr, bayes_net_create, bayes_net_run, bayes_net_end, bayes_net_predict_prepare,
     bayes_net_predict_direct, nullptr},
};

