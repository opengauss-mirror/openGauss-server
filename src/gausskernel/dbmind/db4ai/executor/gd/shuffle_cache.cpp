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
 *  shuffle_cache.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/dbmind/db4ai/executor/gd/shuffle_cache.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "db4ai/gd.h"

/*
 * Shuffle using a limited cache is performed by caching and training batches
 * in random order. For each new batch there are two random options:
 * - append (there are free slots into the cache)
 * - or train (and test) an existing batch and replace it by the new one
 * At the end of the iteration, the cache is emptied randomly. At each step,
 * one remaining batch is selected, trained and removed.
 * At the end of the two phases, all batches have been visited only once in a
 * random sequence, but the ability to shuffle batches depends on the cache size
 * (the available working memory) and the batch size (a matrix of N rows
 * by M features). The probability distribution is not uniform and initial
 * batches have a higher probability than the last batches, but the shuffling
 * is good enough and has a very small impact into the performance of GD.
 */

typedef struct ShuffleCache {
    ShuffleGD shf;
    int cache_size;
    int *cache_batch;
    Matrix **cache_features;
    Matrix **cache_dep_var;
    int cache_allocated;
    int max_cache_usage;
    int num_batches;
    int batch_size;
    int n_features;
    int iteration;
    int cached;
    int next;
    struct drand48_data rnd;
} ShuffleCache;

inline int32_t rnd_next(ShuffleCache *shf) {
    long int r;
    lrand48_r(&shf->rnd, &r);
    return r;
}

static void update_batch(ShuffleCache *cache)
{
    Assert(cache->next < cache->cached);

    ereport(DEBUG1, (errmodule(MOD_DB4AI), errmsg("GD shuffle cache iteration %d train batch %d of %d",
        cache->iteration, cache->cache_batch[cache->next] + 1, cache->num_batches)));

    Matrix *features = cache->cache_features[cache->next];
    Matrix *dep_var = cache->cache_dep_var[cache->next];

    cache->shf.optimizer->update_batch(cache->shf.optimizer, features, dep_var);

    if (features->rows < cache->batch_size) {
        matrix_resize(features, cache->batch_size, cache->n_features);
        matrix_resize(dep_var, cache->batch_size, 1);
    }
}

static void swap_last(ShuffleCache *cache)
{
    Matrix *features = cache->cache_features[cache->next];
    cache->cache_features[cache->next] = cache->cache_features[cache->cached];
    cache->cache_features[cache->cached] = features;

    Matrix *dep_var = cache->cache_dep_var[cache->next];
    cache->cache_dep_var[cache->next] = cache->cache_dep_var[cache->cached];
    cache->cache_dep_var[cache->cached] = dep_var;

    cache->cache_batch[cache->next] = cache->cache_batch[cache->cached];
}

static void cache_start_iteration(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    cache->next = -1;
    cache->cached = 0;
    cache->num_batches = 0;
}

static void cache_end_iteration(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    if (cache->iteration == 0) {
        cache->max_cache_usage = cache->cache_size;
        if (cache->max_cache_usage > cache->num_batches)
            cache->max_cache_usage = cache->num_batches;
    } else {
        // empty the cache
        while (cache->cached > 0) {
            cache->next = rnd_next(cache) % cache->cached;
            update_batch(cache);

            cache->cached--;
            if (cache->next != cache->cached)
                swap_last(cache);
        }
    }
    cache->iteration++;
}

static void cache_release(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    for (int c = 0; c < cache->cache_allocated; c++) {
        matrix_release(cache->cache_features[c]);
        pfree(cache->cache_features[c]);

        matrix_release(cache->cache_dep_var[c]);
        pfree(cache->cache_dep_var[c]);
    }
    pfree(cache->cache_features);
    pfree(cache->cache_dep_var);
    pfree(cache->cache_batch);
    pfree(cache);
}

static Matrix *cache_get(ShuffleGD *shuffle, Matrix **pdep_var)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    Assert(cache->next == -1);

    Matrix *features;
    Matrix *dep_var;
    if (cache->iteration == 0) {
        // special case, do not shuffle
        Assert(cache->cached == 0);
        cache->next = 0;
        cache->cached++;
        if (cache->cache_allocated == 0) {
            features = (Matrix *)palloc0(sizeof(Matrix));
            matrix_init(features, cache->batch_size, cache->n_features);

            dep_var = (Matrix *)palloc0(sizeof(Matrix));
            matrix_init(dep_var, cache->batch_size);

            cache->cache_features[0] = features;
            cache->cache_dep_var[0] = dep_var;
            cache->cache_allocated++;
        } else {
            // reuse the batch, it has been already
            features = cache->cache_features[0];
            dep_var = cache->cache_dep_var[0];
        }
    } else {
        // look for an empty slot, otherwise reuse one
        cache->next = rnd_next(cache) % cache->max_cache_usage;
        if (cache->next < cache->cached) {
            // reuse slot
            update_batch(cache);
            features = cache->cache_features[cache->next];
            dep_var = cache->cache_dep_var[cache->next];
        } else {
            // append
            cache->next = cache->cached++;
            if (cache->next == cache->cache_allocated) {
                features = (Matrix *)palloc0(sizeof(Matrix));
                matrix_init(features, cache->batch_size, cache->n_features);

                dep_var = (Matrix *)palloc0(sizeof(Matrix));
                matrix_init(dep_var, cache->batch_size);

                cache->cache_features[cache->next] = features;
                cache->cache_dep_var[cache->next] = dep_var;
                cache->cache_allocated++;
            } else {
                features = cache->cache_features[cache->next];
                dep_var = cache->cache_dep_var[cache->next];
            }
        }
    }
    cache->cache_batch[cache->next] = cache->num_batches;
    cache->num_batches++;

    *pdep_var = dep_var;
    return features;
}

static void cache_unget(ShuffleGD *shuffle, int tuples)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    Assert(cache->next != -1);
    Assert(cache->cached > 0);

    if (tuples == 0) {
        // ignore batch
        if (cache->iteration == 0) {
            cache->cached--;
        } else {
            // special case when the last batch is empty
            // there are two potential cases
            cache->cached--;
            if (cache->next < cache->cached) {
                // it is in the middle, swap it with the last
                swap_last(cache);
            }
        }
        cache->num_batches--;
    } else {
        if (tuples < cache->batch_size) {
            // resize batch temporarily
            Matrix *features = cache->cache_features[cache->next];
            Matrix *dep_var = cache->cache_dep_var[cache->next];
            matrix_resize(features, tuples, cache->n_features);
            matrix_resize(dep_var, tuples, 1);
        }

        if (cache->iteration == 0) {
            Assert(cache->next == 0);
            update_batch(cache);
            cache->cached--;
        }
    }

    cache->next = -1;
}

ShuffleGD *gd_init_shuffle_cache(const GradientDescentState *gd_state)
{
    int batch_size = gd_get_node(gd_state)->batch_size;

    // check if a batch fits into memory
    int64_t avail_mem = u_sess->attr.attr_memory.work_mem * 1024LL -
        matrix_expected_size(gd_state->n_features) * 2; // weights & gradients

    int batch_mem = matrix_expected_size(batch_size, gd_state->n_features) // features
        + matrix_expected_size(gd_state->n_features);                      // dep var
    if (batch_mem > avail_mem)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("batch size is too large for the available working memory")));

    // initialize
    ShuffleCache *cache = (ShuffleCache *)palloc0(sizeof(ShuffleCache));
    srand48_r(gd_get_node(gd_state)->seed, &cache->rnd);
    cache->shf.start_iteration = cache_start_iteration;
    cache->shf.end_iteration = cache_end_iteration;
    cache->shf.release = cache_release;
    cache->shf.get = cache_get;
    cache->shf.unget = cache_unget;

    // cache for shuffle
    cache->cache_size = avail_mem / (batch_mem + 2 * sizeof(Matrix *) + sizeof(int));
    if (cache->cache_size == 0)
        cache->cache_size = 1; // shuffle is not possible

    ereport(NOTICE, (errmodule(MOD_DB4AI), errmsg("GD shuffle cache size %d", cache->cache_size)));

    cache->batch_size = batch_size;
    cache->n_features = gd_state->n_features;
    cache->cache_batch = (int *)palloc(cache->cache_size * sizeof(int));
    cache->cache_features = (Matrix **)palloc(cache->cache_size * sizeof(Matrix *));
    cache->cache_dep_var = (Matrix **)palloc(cache->cache_size * sizeof(Matrix *));
    cache->cache_allocated = 0;
    cache->max_cache_usage = 0;
    cache->iteration = 0;

    return &cache->shf;
}
