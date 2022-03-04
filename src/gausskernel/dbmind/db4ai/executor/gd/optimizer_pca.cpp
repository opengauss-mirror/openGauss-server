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
 * optimizer_pca.cpp
 *        Optimizer used for Principal Component Analysis
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/optimizer_pca.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"
#include "db4ai/db4ai_cpu.h"

#include <cmath>
#include <random>

typedef struct OptimizerPCA {
    OptimizerGD opt;
    IncrementalStatistics *eigenvalues_stats = nullptr;
    int32_t number_eigenvectors = 0;
    Matrix dot_products;
} OptimizerPCA;

static void gd_pca_update_batch(OptimizerGD *optimizer, Matrix const *features, Matrix const *dep_var)
{
    auto opt = reinterpret_cast<OptimizerPCA *>(optimizer);
    GradientDescent *gd_algo = (GradientDescent *)opt->opt.gd_state->tms.algorithm;
    
    int32_t const batch_size = features->rows;
    int32_t const num_eigenvectors = opt->number_eigenvectors;
    
    /*
     * scaling by the size of the batch seems to be optimal in practice
     * (it has been reported elsewhere as well)
     */
    float8 const step = 1. / batch_size;
    
    // clear gradients of the batch
    matrix_zeroes(&optimizer->gradients);
    // clear the working matrix
    matrix_zeroes(&opt->dot_products);
    
    /*
     * we compute the gradient but don't move in that direction inside that function
     * the current proportion of each eigenvector is also computed therein
     */
    GradientsConfigPCA cfg_pca;
    cfg_pca.hdr.hyperp = optimizer->hyperp;
    cfg_pca.hdr.features = features;
    cfg_pca.hdr.weights = &optimizer->weights;
    cfg_pca.hdr.gradients = &optimizer->gradients;
    cfg_pca.dot_products = &opt->dot_products;
    cfg_pca.eigenvalues_stats = opt->eigenvalues_stats;
    
    /*
     * the actual computations
     */
    gd_algo->compute_gradients(&cfg_pca.hdr);
    
    /*
     * moving in the direction of the gradient is actually done in here
     * for pca we actually do gradient ascent (max problem) directly instead
     * of doing descent with multiplicative factor -1
     */
    matrix_mult_scalar(&optimizer->gradients, step);
    matrix_add(&optimizer->weights, &optimizer->gradients);
    
    /*
     * we ortho-normalize before the next iteration.
     * for the time being we perform Gram-Schmidt for a set of eigenvectors
     */
    matrix_gram_schmidt(&optimizer->weights, num_eigenvectors);
    
    // DB4AI_API gd_pca_update_batch: TrainModel is an read-only input structure from the query plan
    // hyperparameters should be also read-only, use the state instead
    HyperparametersGD *hyperp = (HyperparametersGD *)optimizer->hyperp;
    hyperp->lambda = cfg_pca.batch_error;
}

force_inline static void gd_pca_release(OptimizerGD *optimizer)
{
    auto pca_opt = reinterpret_cast<OptimizerPCA *>(optimizer);
    
    matrix_release(&pca_opt->dot_products);
    pfree(pca_opt->eigenvalues_stats);
    pfree(pca_opt);
}

OptimizerGD *gd_init_optimizer_pca(GradientDescentState const *gd_state, HyperparametersGD *hyperp)
{
    auto pca_opt = reinterpret_cast<OptimizerPCA *>(palloc0(sizeof(OptimizerPCA)));
    uint64_t external_seed = hyperp->seed;
    int32_t const dimension = gd_state->n_features;
    
    pca_opt->opt.hyperp = hyperp;
    pca_opt->opt.start_iteration = nullptr;
    pca_opt->opt.end_iteration = nullptr;
    pca_opt->opt.update_batch = gd_pca_update_batch;
    pca_opt->opt.release = gd_pca_release;
    pca_opt->opt.finalize = nullptr;
    pca_opt->opt.gd_state = gd_state;
    int32_t num_eigenvectors = pca_opt->number_eigenvectors = hyperp->number_dimensions;
    int32_t batch_size = hyperp->batch_size;
    
    /*
     * the number of principal components can be at most the full dimension of the data
     * (as seen from the number of features passed)
     */
    if (unlikely(num_eigenvectors > dimension))
        num_eigenvectors = pca_opt->number_eigenvectors = dimension;
    
    /*
     * to keep track of running statistics on the eigenvalues
     */
    check_hyper_bounds(sizeof(IncrementalStatistics), num_eigenvectors, "number_components");
    pca_opt->eigenvalues_stats =
            reinterpret_cast<IncrementalStatistics *>(palloc0(sizeof(IncrementalStatistics) * num_eigenvectors));
    
    /*
     * eigenvectors are stored in a matrix (weights) of size (d x k) where d is the dimension
     * of the k (column) eigenvectors
     *
     * observe that we allocate two columns more in which we will keep the proportion of each
     * eigenvalue as well as running statistics of each one of them
     *
     * observe that num_eigenvectors <= dimension, and thus using the last two columns
     * of weights to store statistics and proportions of eigenvalues is posible
     */
    matrix_init(&pca_opt->opt.weights, dimension, num_eigenvectors + 2);
    matrix_init(&pca_opt->opt.gradients, dimension, num_eigenvectors + 2);
    
    /*
     * this matrix (of dimensions n x k) represents in each column (component) a dot product
     * it is used internally when computing the gradients
     */
    matrix_init(&pca_opt->dot_products, batch_size, num_eigenvectors);
    
    /*
     * we produce high quality random numbers
     */
    uint64_t const internal_seed = 0x274066DB9441E851ULL;
    external_seed ^= internal_seed;
    std::mt19937_64 gen(external_seed);
    std::uniform_real_distribution<float8> sampler(0., 1.);
    
    Matrix eigenvector;
    eigenvector.transposed = false;
    eigenvector.rows = pca_opt->opt.weights.rows;
    eigenvector.columns = 1;
    eigenvector.allocated = pca_opt->opt.weights.rows;
    
    for (int32_t e = 0; e < num_eigenvectors; ++e) {
        eigenvector.data = pca_opt->opt.weights.data + (e * dimension);
        for (int32_t f = 0; f < dimension; ++f)
            eigenvector.data[f] = sampler(gen);
    }
    
    /*
     * once the initial set of eigenvectors have been produced we have to orthonormalize them using
     * (for the moment) modified Gram-Schmidt
     */
    matrix_gram_schmidt(&pca_opt->opt.weights, num_eigenvectors);
    
    return &pca_opt->opt;
}