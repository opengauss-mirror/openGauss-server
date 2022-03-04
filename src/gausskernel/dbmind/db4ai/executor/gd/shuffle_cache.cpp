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
 * in random order. During the first iteration the batches are processed
 * sequentially in the same order as provided by the source relation or query.
 * The cache tries to keep a snapshot of the whole data as long as it fits
 * into the available memory (working set), otherwise it discards the snapshot.
 *
 * For the following iterations there are two options: with snapshot, and the
 * relations or queries are not scanned again, or with a rescan. In the first
 * case, at each iteration the batches are shuffled again using a random
 * uniform distribution.
 *
 * Instead, if there is no snapshot then for each batch read again from the
 * source data there are two random options:
 *
 * - to cache the batch into a free slot
 * - to replace a cached batch by the new one
 *
 * Batches are trained only when they are discarded from the cache.
 *
 * At the end of the iteration, the remaining batches in the cache are emptied
 * randomly. The process guarantees that all batches have been trained only once
 * in a random sequence, but the ability to shuffle batches depends on the
 * cache size. In this case, the probability distribution is not uniform and
 * initial batches have a higher probability than the last batches, but the
 * shuffling is good enough and has a very small impact into the accuracy of GD.
 */

typedef struct ShuffleCache {
    ShuffleGD shf;
    int cache_size;
    int *cache_batch;
    Matrix **cache_features;
    Matrix **cache_dep_var;
    int cache_allocated;
    int max_cache_usage;
    int batch_size;
    int n_features;
    int iteration;
    int cached;
    int next;
    struct drand48_data rnd;
    List *snapshot;
    int snapshot_last_size;
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
        cache->iteration, cache->cache_batch[cache->next] + 1, cache->shf.num_batches)));

    Matrix *features = cache->cache_features[cache->next];
    Matrix *dep_var = nullptr;
    if (cache->shf.supervised)
        dep_var = cache->cache_dep_var[cache->next];

    cache->shf.optimizer->update_batch(cache->shf.optimizer, features, dep_var);

    if (features->rows < cache->batch_size) {
        matrix_resize(features, cache->batch_size, cache->n_features);
        if (dep_var != nullptr)
            matrix_resize(dep_var, cache->batch_size, 1);
    }
}

static void swap_last(ShuffleCache *cache)
{
    Matrix *features = cache->cache_features[cache->next];
    cache->cache_features[cache->next] = cache->cache_features[cache->cached];
    cache->cache_features[cache->cached] = features;

    if (cache->shf.supervised) {
        Matrix *dep_var = cache->cache_dep_var[cache->next];
        cache->cache_dep_var[cache->next] = cache->cache_dep_var[cache->cached];
        cache->cache_dep_var[cache->cached] = dep_var;
    }

    cache->cache_batch[cache->next] = cache->cache_batch[cache->cached];
}

static void cache_release_snapshot(ShuffleCache *cache)
{
    if (cache->snapshot != nullptr) {
        ListCell *lc;
        foreach (lc, cache->snapshot) {
            Matrix **matrices = lfirst_node(Matrix *, lc);
            matrix_release(matrices[0]);
            pfree(matrices[0]);
            if (cache->shf.supervised) {
                matrix_release(matrices[1]);
                pfree(matrices[1]);
            }
            pfree(matrices);
        }
        list_free(cache->snapshot);
        cache->snapshot = nullptr;
    }
}

static void cache_start_iteration(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    cache->next = -1;
    cache->cached = 0;
    cache->shf.num_batches = 0;
    if (cache->snapshot != nullptr) {
        // move the snapshot to the cache
        Assert(cache->cache_allocated == 0);
        ListCell *lc;
        Matrix **matrices = nullptr;
        foreach (lc, cache->snapshot) {
            Assert(cache->cached < cache->cache_size);
            matrices = lfirst_node(Matrix *, lc);
            cache->cache_features[cache->cached] = matrices[0];
            if (cache->shf.supervised)
                cache->cache_dep_var[cache->cached] = matrices[1];
            cache->cached++;
        }
        matrix_resize(matrices[0], cache->snapshot_last_size, cache->n_features);
        if (cache->shf.supervised)
            matrix_resize(matrices[1], cache->snapshot_last_size, 1);
    }
}

static void cache_end_iteration(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    if (cache->iteration == 0) {
        cache->max_cache_usage = cache->cache_size;
        if (cache->max_cache_usage > cache->shf.num_batches)
            cache->max_cache_usage = cache->shf.num_batches;
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

        if (cache->shf.supervised) {
            matrix_release(cache->cache_dep_var[c]);
            pfree(cache->cache_dep_var[c]);
        }
    }

    if (cache->shf.supervised)
        pfree(cache->cache_dep_var);

    pfree(cache->cache_features);
    pfree(cache->cache_batch);
    cache_release_snapshot(cache);
    pfree(cache);
}

static Matrix *cache_get(ShuffleGD *shuffle, Matrix **pdep_var)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    Assert(cache->next == -1);

    Matrix *features;
    Matrix *dep_var = nullptr;
    if (cache->iteration == 0) {
        // special case, do not shuffle
        Assert(cache->cached == 0);
        cache->next = 0;
        cache->cached++;
        if (cache->cache_allocated == 0) {
            features = (Matrix *)palloc0(sizeof(Matrix));
            matrix_init(features, cache->batch_size, cache->n_features);
            cache->cache_features[0] = features;

            if (cache->shf.supervised) {
                dep_var = (Matrix *)palloc0(sizeof(Matrix));
                matrix_init(dep_var, cache->batch_size);
                cache->cache_dep_var[0] = dep_var;
            }
            
            cache->cache_allocated++;
        } else {
            // reuse the batch, it has been already
            features = cache->cache_features[0];
            if (cache->shf.supervised)
                dep_var = cache->cache_dep_var[0];
        }
    } else {
        if (cache->snapshot != nullptr) {
            // check if there are more chunks to process
            if (cache->cached == 0)
                return nullptr;

            cache->next = rnd_next(cache) % cache->cached;
            features = cache->cache_features[cache->next];
            if (cache->shf.supervised)
                dep_var = cache->cache_dep_var[cache->next];
        } else {
            // look for an empty slot, otherwise reuse one
            cache->next = rnd_next(cache) % cache->max_cache_usage;
            if (cache->next < cache->cached) {
                // reuse slot
                update_batch(cache);
                features = cache->cache_features[cache->next];
                if (cache->shf.supervised)
                    dep_var = cache->cache_dep_var[cache->next];
            } else {
                // append
                cache->next = cache->cached++;
                if (cache->next == cache->cache_allocated) {
                    features = (Matrix *)palloc0(sizeof(Matrix));
                    matrix_init(features, cache->batch_size, cache->n_features);
                    cache->cache_features[cache->next] = features;

                    if (cache->shf.supervised) {
                        dep_var = (Matrix *)palloc0(sizeof(Matrix));
                        matrix_init(dep_var, cache->batch_size);
                        cache->cache_dep_var[cache->next] = dep_var;
                    }

                    cache->cache_allocated++;
                } else {
                    features = cache->cache_features[cache->next];
                    if (cache->shf.supervised)
                        dep_var = cache->cache_dep_var[cache->next];
                }
            }
        }
    }
    cache->cache_batch[cache->next] = cache->shf.num_batches;
    cache->shf.num_batches++;

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

        if (cache->snapshot != nullptr) {
            Assert(cache->cache_allocated == 1);

            // there is a valid snapshot, we must clean the unused arrays
            matrix_release(cache->cache_features[0]);
            pfree(cache->cache_features[0]);

            if (cache->shf.supervised) {
                matrix_release(cache->cache_dep_var[0]);
                pfree(cache->cache_dep_var[0]);
            }

            cache->cache_allocated--;
        }
    } else {
        if (tuples < cache->batch_size) {
            // resize batch temporarily
            Matrix *features = cache->cache_features[cache->next];
            matrix_resize(features, tuples, cache->n_features);

            if (cache->shf.supervised) {
                Matrix *dep_var = cache->cache_dep_var[cache->next];
                matrix_resize(dep_var, tuples, 1);
            }
        }

        if (cache->iteration == 0) {
            Assert(cache->next == 0);
            update_batch(cache);
            cache->cached--;

            if (cache->shf.num_batches < cache->cache_size) {
                if (cache->shf.num_batches == 1 || cache->snapshot != nullptr) {
                    // accumulate into snapshot
                    Assert(cache->cache_allocated == 1);
                    int nmatrices = cache->shf.supervised ? 2 : 1;
                    Matrix **matrices = (Matrix **) palloc(nmatrices * sizeof(Matrix *));
                    matrices[0] = cache->cache_features[0];
                    if (cache->shf.supervised)
                        matrices[1] = cache->cache_dep_var[0];
                    cache->cache_allocated--;
                    cache->snapshot = lappend(cache->snapshot, matrices);
                    cache->snapshot_last_size = tuples;
                }
            } else {
                if (cache->snapshot != nullptr) {
                    // cannot make a snapshot, there is not enough memory
                    cache_release_snapshot(cache);
                }
            }
        } else {
            if (cache->snapshot != nullptr) {
                update_batch(cache);
                cache->cached--;
                if (cache->next < cache->cached) {
                    // it is in the middle, swap it with the last
                    swap_last(cache);
                }
            }
        }
    }

    cache->next = -1;
}

static bool cache_has_snapshot(ShuffleGD *shuffle)
{
    ShuffleCache *cache = (ShuffleCache *)shuffle;
    return cache->snapshot != nullptr;
}

ShuffleGD *gd_init_shuffle_cache(const GradientDescentState *gd_state, HyperparametersGD *hyperp)
{
    GradientDescent *gd_algo = (GradientDescent*)gd_state->tms.algorithm;

    bool supervised = gd_is_supervised(gd_algo);
    int batch_size = hyperp->batch_size;

    // check if a batch fits into memory
    int64_t avail_mem = u_sess->attr.attr_memory.work_mem * 1024LL -
        matrix_expected_size(gd_state->n_features) * 2; // weights & gradients

    int batch_mem = matrix_expected_size(batch_size, gd_state->n_features); // features
    if (supervised)
        batch_mem += matrix_expected_size(gd_state->n_features); // dep var
    
    if (batch_mem > avail_mem)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("batch size is too large for the available working memory")));

    // initialize
    ShuffleCache *cache = (ShuffleCache *)palloc0(sizeof(ShuffleCache));
    cache->snapshot = nullptr;
    srand48_r(hyperp->seed, &cache->rnd);
    cache->shf.supervised = supervised;
    cache->shf.start_iteration = cache_start_iteration;
    cache->shf.end_iteration = cache_end_iteration;
    cache->shf.release = cache_release;
    cache->shf.get = cache_get;
    cache->shf.unget = cache_unget;
    cache->shf.has_snapshot = cache_has_snapshot;

    // cache for shuffle
    int batch_matrices = (supervised ? 2 : 1);
    cache->cache_size = avail_mem / (batch_mem + batch_matrices * sizeof(Matrix *) + sizeof(int));
    if (cache->cache_size == 0)
        cache->cache_size = 1; // shuffle is not possible

    ereport(DEBUG1, (errmodule(MOD_DB4AI), errmsg("GD shuffle cache size %d", cache->cache_size)));

    cache->batch_size = batch_size;
    cache->n_features = gd_state->n_features;
    cache->cache_batch = (int *)palloc(cache->cache_size * sizeof(int));
    cache->cache_features = (Matrix **)palloc(cache->cache_size * sizeof(Matrix *));
    if (supervised)
        cache->cache_dep_var = (Matrix **)palloc(cache->cache_size * sizeof(Matrix *));
    cache->cache_allocated = 0;
    cache->max_cache_usage = 0;
    cache->iteration = 0;

    return &cache->shf;
}
