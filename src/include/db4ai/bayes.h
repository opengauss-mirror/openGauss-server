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
 *---------------------------------------------------------------------------------------
 *
 *  bayes.h
 *
 * IDENTIFICATION
 *        src/include/db4ai/bayes.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _BAYES_H_
#define _BAYES_H_

#include "db4ai/db4ai_common.h"
#include "db4ai/bayesnet.h"

#define MAX_NODES_NUM 64
#define MAX_BINS_NUM 4096
#define MAX_MCVS_NUM 4096

typedef struct ModelBayesNodeV01 {
    uint32_t features_cols;
    uint32_t categories_size;
    uint32_t prob_offset = 0U;
    /*
     * next in binary format:
     * uint32_t *category;
     * Oid *attrtype;
     * double *prob;
     * struct varlena *categories
     */
} ModelBayesNodeV01;

typedef struct bayesNodeState {
    TrainModelState tms;
    bool done = false;
    uint32_t chunk_size = 0U;
    uint32_t discardtuples = 0U;
    uint32_t processedtuples = 0U;
    double executiontime = 0;
    double loss = 0;
    Oid returntype;

    ModelBayesNodeV01 chunk;
    struct varlena *categories;
    uint32_t *category;
    Oid *attrtype;
    double *prob;
} bayesNodeState;

typedef struct bayesState {
    TrainModelState tms;

    bool done = false;
    uint32_t chunk_size = 0U;
    uint32_t discardtuples = 0U;
    uint32_t processedtuples = 0U;
    double executiontime = 0;
    Oid returntype;

    int num_nodes;
    bool *parents;

    int num_bins;
    int num_mcvs;
    uint64_t num_sample_rows;
    uint64_t num_total_rows;
    bayesNodeState **bayes_tmss;
} bayesState;

typedef struct HyperparametersBayesNode {
    ModelHyperparameters mhp;  // place-holder
} HyperparametersBayesNode;

typedef struct HyperparametersBayesNet {
    ModelHyperparameters mhp;  // place-holder
    const char *edges_of_network{nullptr};
    int64_t num_edges;
    int64_t num_nodes;
} HyperparametersBayesNet;

typedef struct HyperparametersBayes {
    ModelHyperparameters mhp;  // place-holder
    const char *edges_of_network{nullptr};
    int64_t num_edges;
    int64_t num_nodes;
    int64_t num_bins;
    int64_t num_mcvs;
    int64_t num_sample_rows;
    int64_t num_total_rows;
} HyperparametersBayes;

double probabilityDensity(double x, double mean, double stdDiv);
void bayes_run(AlgorithmAPI *self, TrainModelState *pstate, Model *models);
void extract_graph(const char *edges_of_network, int num_of_edge, int num_nodes, bayesState *bns);
Datum bayes_net_predict_direct(AlgorithmAPI *, ModelPredictor model, Datum *values, bool *isnull, Oid *types,
                               int ncolumns);
void bayes_net_end(AlgorithmAPI *self, TrainModelState *pstate);
bool checkTypeConsistent(Oid *tl, Oid *tr, int nattrs);
ValueInTuple create_value(Datum data, Oid type, bool isnull);
int findDiscreteIndex(ValueInTuple input, bayesDatumList *features, int data_preprocess);
#endif /* BAYES_H */
