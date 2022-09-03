/**
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

kmeans.cpp
        k-means operations


IDENTIFICATION
    src/gausskernel/dbmind/db4ai/executor/kmeans/kmeans.cpp

---------------------------------------------------------------------------------------
* */

#include <random>

#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "postgres_ext.h"

#include "db4ai/kmeans.h"
#include "db4ai/aifuncs.h"
#include "db4ai/fp_ops.h"
#include "db4ai/distance_functions.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/predict_by.h"
#include "db4ai/db4ai_cpu.h"
#include "db4ai/db4ai_common.h"


/*
 * parameters that affect k-means (hyper-parameters)
 */
typedef struct HyperparametersKMeans {
    ModelHyperparameters mhp;  // place-holder
    SeedingFunction seeding = KMEANS_RANDOM_SEED;
    DistanceFunction distance = KMEANS_L2_SQUARED;
    Verbosity verbosity = NO_OUTPUT;
    uint32_t num_centroids = 1U;
    uint32_t num_iterations = 10U;
    uint32_t n_features = 0U;
    uint32_t batch_size = 1000U;
    uint64_t external_seed = 0ULL;
    double tolerance = 0.00001;
} HyperparametersKMeans;

/*
 * optimized for prediction
 */
typedef struct PCentroid {
    double objective_function = DBL_MAX;
    double avg_distance = DBL_MAX;
    double min_distance = DBL_MAX;
    double max_distance = DBL_MAX;
    double std_dev_distance = DBL_MAX;
    uint64_t cluster_size = 0ULL;
    uint32_t id = 0U;
    double *coordinates = nullptr;
} PCentroid;

typedef struct ModelKMeansV01 {
    /*
     * the following fields are put here for convenience
     * (currently used for prediction, and explain)
     */
    uint32_t actual_num_centroids = 0U;
    uint32_t num_actual_iterations = 0U;
    uint32_t dimension = 0U;
    uint32_t distance_function_id = 0U;
    uint32_t coordinates_offset = 0U;
    uint32_t padding = 0U;
    PCentroid *centroids = nullptr;
} ModelKMeansV01;

/*
 * current state of the k-means algorithm
 */
typedef struct KMeansStateDescription {
    Centroid *centroids[2] = {nullptr};
    ArrayType *bbox_min = nullptr;
    ArrayType *bbox_max = nullptr;
    GSPoint *batch = nullptr;
    
    double (*distance)(double const *, double const *, uint32_t const dimension) = nullptr;
    
    double execution_time = 0.;
    double seeding_time = 0.;
    IncrementalStatistics solution_statistics[2];
    uint64_t num_good_points = 0UL;
    uint64_t num_dead_points = 0UL;
    uint32_t current_iteration = 0U;
    uint32_t current_centroid = 0U;
    uint32_t dimension = 0U;
    uint32_t num_centroids = 0U;
    uint32_t actual_num_iterations = 0U;
    uint32_t size_centroids_bytes = 0U;
    bool initialized = false;
    bool late_initialized = false;
} KMeansStateDescription;

typedef struct KMeansState {
    TrainModelState tms;
    KMeansStateDescription description;
    bool done = false;
} KMeansState;

/*
 * kmeans|| does at most 10 iterations
 */
uint32_t constexpr NUM_ITERATIONS_KMEANSBB = 10U;

/*
 * internally, the operator works in stages, this enum identifies each one of them
 */
typedef enum : uint32_t {
    KMEANS_INIT = 0,
    KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE,
    KMEANS_INITIAL_CENTROIDS_BB_SAMPLE,
    KMEANS_INITIAL_CENTROIDS_BB_COMPUTE_COST,
    KMEANS_LLOYD
} AlgoStage;

/*
 * this initializes all relevant fields once the dimension of the points (as specified
 * by the user or as obtained from the very first tuple read) is known
 */
force_inline static void initialize_fields(KMeansStateDescription *kmeans_state_description,
                                           uint32_t const num_centroids, uint32_t const dimension)
{
    auto datums_tmp = reinterpret_cast<Datum *>(palloc0(sizeof(Datum) * dimension));
    
    for (uint32_t c = 0; c < num_centroids; ++c) {
        kmeans_state_description->centroids[0][c].id = c + 1;
        kmeans_state_description->centroids[1][c].id = c + 1;
        /*
         * this is an internal array that will eventually hold the coordinates of a centroid
         * its representation as a PG Array is legacy. It will be better to represent it as
         * an array of double directly (in a manner that can be returned to the client directly)
         */
        kmeans_state_description->centroids[0][c].coordinates =
                construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
        kmeans_state_description->centroids[1][c].coordinates =
                construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    }
    
    /*
     * general running time information of the operator
     */
    kmeans_state_description->bbox_max =
            construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    kmeans_state_description->bbox_min =
            construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    kmeans_state_description->num_good_points = 0;
    kmeans_state_description->num_dead_points = 0;
    kmeans_state_description->current_iteration = 0;
    kmeans_state_description->current_centroid = 0;
    kmeans_state_description->dimension = dimension;
    kmeans_state_description->initialized = true;
    kmeans_state_description->size_centroids_bytes = sizeof(float8) * dimension;
    
    pfree(datums_tmp);
}

/*
 * to verify that the pg array we get complies with what we expect
 * we discard entries that do not pass the test because we cannot start
 * much (consistent) with them anyway
 */
static bool verify_pgarray(ArrayType const *pg_array, int32_t n)
{
    /*
     * We expect the input to be an n-element array of doubles; verify that. We
     * don't need to use deconstruct_array() since the array data is just
     * going to look like a C array of n double values.
     */
    if (unlikely((ARR_NDIM(pg_array) != 1) || (ARR_DIMS(pg_array)[0] != n) || ARR_HASNULL(pg_array) ||
                 (ARR_ELEMTYPE(pg_array) != FLOAT8OID)))
        return false;
    
    return true;
}

/*
 * this copies the coordinates found inside a slot onto an array we own
 */
static bool copy_slot_coordinates_to_array(GSPoint *coordinates, ModelTuple const *slot, uint32_t const dimension)
{
    if (unlikely((slot == nullptr) or (coordinates == nullptr)))
        return false;
    
    /*
     * we obtain the coordinates of the current point (function call incurs in detoasting
     * and thus memory is allocated and we have to free it once we don't need it
     */
    ArrayType *current_point_pgarray = DatumGetArrayTypePCopy(slot->values[0]);
    bool const valid_point = verify_pgarray(current_point_pgarray, dimension);
    bool release_point = PointerGetDatum(current_point_pgarray) != slot->values[0];
    
    /*
     * if the point is not valid and it was originally toasted, then we release the copy
     */
    if (unlikely(!valid_point && release_point)) {
        pfree(current_point_pgarray);
        release_point = false;
    }
    
    coordinates->pg_coordinates = current_point_pgarray;
    coordinates->should_free = release_point;
    
    return valid_point;
}

/*
 * given a set of centroids (as a PG list) and a point, this function compute the distance to the closest
 * centroid
 */
static bool closest_centroid(List const *centroids, GSPoint const *point, uint32_t const dimension, double *distance)
{
    ListCell const *current_centroid_cell = centroids ? centroids->head : nullptr;
    GSPoint *centroid = nullptr;
    GSPoint *closest_centroid_ptr = nullptr;
    bool result = false;
    bool min_distance_changed = false;
    double local_distance = 0.;
    auto min_distance = DBL_MAX;
    auto const *point_coordinates = reinterpret_cast<double const *>(ARR_DATA_PTR(point->pg_coordinates));
    
    for (; current_centroid_cell != nullptr; current_centroid_cell = lnext(current_centroid_cell)) {
        /*
         * low temporal locality for a prefetch for read
         */
        prefetch(lnext(current_centroid_cell), 0, 1);
        centroid = reinterpret_cast<GSPoint *>(lfirst(current_centroid_cell));
        local_distance = l2_squared(point_coordinates,
                                    reinterpret_cast<double const *>(ARR_DATA_PTR(centroid->pg_coordinates)),
                                    dimension);
        min_distance_changed = local_distance < min_distance;
        min_distance = min_distance_changed ? local_distance : min_distance;
        closest_centroid_ptr = min_distance_changed ? centroid : closest_centroid_ptr;
    }
    
    if (closest_centroid_ptr) {
        result = true;
        ++closest_centroid_ptr->weight;
    }
    
    if (likely(distance != nullptr))
        *distance = min_distance;
    
    return result;
}

static bool deal_sample(bool const sample, std::mt19937_64 *prng, GSPoint *batch, uint32_t current_slot,
                        ModelTuple const *scan_slot, uint32_t const dimension, bool first_candidate,
                        List *centroid_candidates, double local_sample_probability, AlgoStage stage)
{
    double coin = 0.;
    bool result = false;
    bool distance_computed = false;
    double distance_to_centroid = 0.;
    double op_error = 0.;
    std::uniform_real_distribution<double> unit_sampler(0., 1.);
    /*
     * toss the coin to see if we have to
     */
    if (sample) {
        if (unlikely(prng == nullptr))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: prng must be set (non-null)")));
        
        coin = unit_sampler(*prng);
        /*
         * random++ and kmeans|| sample with different probabilities
         * (the latter is much more complicated)
         *
         * when sampling we will pay the price of unpacking every single
         * tuple (which has to be done for kmeans|| anyway) for code
         * simplicity
         */
        result = copy_slot_coordinates_to_array(batch + current_slot, scan_slot, dimension);
        
        if (result && (stage == KMEANS_INITIAL_CENTROIDS_BB_SAMPLE) && !first_candidate) {
            distance_computed = closest_centroid(centroid_candidates, batch + current_slot,
                                                 dimension, &distance_to_centroid);
            twoMult(local_sample_probability, distance_computed ? distance_to_centroid : 1.,
                    &local_sample_probability, &op_error);
            local_sample_probability += op_error;
        }
        
        /*
         * if the data point is not valid or did not pass the test
         * we release the memory it occupies and ask for the next data point
         */
        if (!result || coin >= local_sample_probability) {
            if (likely(batch[current_slot].should_free)) {
                pfree(batch[current_slot].pg_coordinates);
                batch[current_slot].pg_coordinates = nullptr;
                batch[current_slot].should_free = false;
            }
            return true;
        }
    }
    return false;
}

/*
 * given a set of centroids (as a PG list) and a set of points, this function computes
 * the cost of the set of centroids as well as their weights (number of points assigned
 * to each centroid)
 * we assumed that all weights have been reset already
 */
static bool compute_cost_and_weights(List const *centroids, GSPoint const *points, uint32_t dimension,
                                     uint32_t const num_slots, double *cost)
{
    GSPoint const *point = nullptr;
    double local_distance = 0.;
    double cost_of_batch = 0.;
    double local_op_error = 0.;
    double total_op_error = 0.;
    uint32_t current_slot = 0U;
    bool const result = current_slot < num_slots;
    
    // for every point we compute the closest centroid and increase the weight of the corresponding centroid
    while (current_slot < num_slots) {
        point = points + current_slot;
        closest_centroid(centroids, point, dimension, &local_distance);
        twoSum(cost_of_batch, local_distance, &cost_of_batch, &local_op_error);
        total_op_error += local_op_error;
        ++current_slot;
    }
    cost_of_batch += total_op_error;
    *cost = cost_of_batch;
    return result;
}

/*
 * a batch is used only once and thus the data it points to is
 * released after it has been used
 */
force_inline static void release_batch(GSPoint *batch, uint32_t const num_slots)
{
    GSPoint *point = nullptr;
    for (uint32_t current_slot = 0; current_slot < num_slots; ++current_slot) {
        point = batch + current_slot;
        if (likely(point->should_free)) {
            pfree(point->pg_coordinates);
            point->pg_coordinates = nullptr;
            point->should_free = false;
        }
    }
}

/*
 * given the running mean of a centroid and a new point, this adds the new point to the aggregate
 * using a sum that provides higher precision (we could provide much higher precision at the cost
 * of allocating yet another array to keep correction terms for every dimension
 */
force_inline static void aggregate_point(double *centroid_aggregation, double const *new_point,
                                         uint32_t const dimension)
{
    double local_correction = 0;
    for (uint32_t d = 0; d < dimension; ++d) {
        twoSum(centroid_aggregation[d], new_point[d], centroid_aggregation + d, &local_correction);
        centroid_aggregation[d] += local_correction;
    }
}

/*
 * we assume that all slots in the batch are non-null (guaranteed by the upper call)
 * also, that the next set of centroids has been reset previous to the very first call
 */
static void update_centroids(KMeansStateDescription *description, GSPoint *slots, uint32_t const num_slots,
                             uint32_t const idx_current_centroids, uint32_t const idx_next_centroids)
{
    uint32_t const dimension = description->dimension;
    uint32_t current_slot = 0U;
    uint32_t current_centroid = 0U;
    uint32_t const num_centroids = description->num_centroids;
    uint32_t closest_centroid = 0U;
    GSPoint const *current_point = nullptr;
    double const *current_point_coordinates = nullptr;
    double *current_centroid_coordinates = nullptr;
    double *next_centroid_coordinates = nullptr;
    double dist = 0.;
    auto min_dist = DBL_MAX;
    Centroid *current_centroids = description->centroids[idx_current_centroids];
    Centroid *next_centroids = description->centroids[idx_next_centroids];
    Centroid *closest_centroid_ptr = nullptr;
    Centroid *closest_centroid_next_ptr = nullptr;
    bool min_dist_change = false;
    IncrementalStatistics local_statistics;
    
    /*
     * just in case, but this should not happen as we control the parent call
     */
    if (unlikely(num_slots == 0))
        return;
    
    do {
        current_centroid = 0U;
        min_dist = DBL_MAX;
        /*
         * we obtain the coordinates of the current point
         */
        current_point = slots + current_slot;
        
        /*
         * this loops obtains the distance of the current point to all centroids and keeps the closest one
         */
        while (likely(current_centroid < num_centroids)) {
            current_centroid_coordinates =
                    reinterpret_cast<double *>(ARR_DATA_PTR(current_centroids[current_centroid].coordinates));
            current_point_coordinates = reinterpret_cast<double *>(ARR_DATA_PTR(current_point->pg_coordinates));
            
            dist = description->distance(current_point_coordinates, current_centroid_coordinates, dimension);
            min_dist_change = dist < min_dist;
            closest_centroid = min_dist_change ? current_centroid : closest_centroid;
            min_dist = min_dist_change ? dist : min_dist;
            ++current_centroid;
        }
        
        /*
         * once the closest centroid has been detected we proceed with the aggregation and update
         * of statistics
         */
        local_statistics.setTotal(min_dist);
        closest_centroid_ptr = current_centroids + closest_centroid;
        closest_centroid_next_ptr = next_centroids + closest_centroid;
        closest_centroid_ptr->statistics += local_statistics;
        /*
         * for the next iteration (if there is any) we have to obtain a new centroid, which will be
         * the average of the points that we aggregate here
         */
        next_centroid_coordinates = reinterpret_cast<double *>(ARR_DATA_PTR(closest_centroid_next_ptr->coordinates));
        aggregate_point(next_centroid_coordinates, current_point_coordinates, dimension);
    } while (likely(++current_slot < num_slots));
}

/*
 * updates the minimum bounding box to contain the new given point
 */
force_inline static void update_bbox(double *const bbox_min, double *const bbox_max, double const *point,
                                     uint32_t const dimension)
{
    uint32_t current_dimension = 0;
    double min = 0.;
    double max = 0.;
    double p = 0.;
    
    while (current_dimension < dimension) {
        min = bbox_min[current_dimension];
        max = bbox_max[current_dimension];
        p = point[current_dimension];
        
        /*
         * we do cmovs instead of ifs (we could spare a comparison from time to time
         * by using ifs, but we increase branch misprediction as well. thus we settle
         * for the branchless option (more expensive than a hit, but cheaper than a miss)
         */
        bbox_min[current_dimension] = p < min ? p : min;
        bbox_max[current_dimension] = p > max ? p : max;
        
        ++current_dimension;
    }
}

static bool init_kmeans(KMeansStateDescription *description, double *bbox_min, double *bbox_max, GSPoint const *batch,
                        uint32_t const num_slots)
{
    uint32_t const dimension = description->dimension;
    uint32_t const size_centroid_bytes = description->size_centroids_bytes;
    bool const first_run = description->current_iteration == 0;
    uint32_t current_slot = 0;
    GSPoint const *current_point = nullptr;
    double const *current_point_coordinates = nullptr;
    
    if (unlikely(num_slots == 0))
        return false;
    
    /*
     * the very first slot of the batch is a bit special. observe that we have got here
     * after the point has passed validity checks and thus it is safe to access its information
     * directly
     */
    current_point = batch + current_slot;
    current_point_coordinates = reinterpret_cast<double const *>(ARR_DATA_PTR(current_point->pg_coordinates));
    
    /*
     * in the very first run we set the coordinates of the bounding box as the ones
     * of the very first point (we improve from there)
     */
    if (unlikely(first_run)) {
        /*
         * no need to memset to zero because in the very first run they are freshly allocated
         * with palloc0
         */
        errno_t rc = memcpy_s(bbox_min, size_centroid_bytes, current_point_coordinates, size_centroid_bytes);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(bbox_max, size_centroid_bytes, current_point_coordinates, size_centroid_bytes);
        securec_check(rc, "\0", "\0");
        description->current_iteration = 1;
    } else {
        update_bbox(bbox_min, bbox_max, current_point_coordinates, dimension);
    }
    
    ++description->num_good_points;
    
    /*
     * let's consider the rest of the batch
     */
    while (likely(++current_slot < num_slots)) {
        current_point = batch + current_slot;
        current_point_coordinates = reinterpret_cast<double const *>(ARR_DATA_PTR(current_point->pg_coordinates));
        
        update_bbox(bbox_min, bbox_max, current_point_coordinates, dimension);
        
        ++description->num_good_points;
    }
    
    /*
     * done with the batch
     */
    return true;
}

/*
 * this is the work horse of the whole algorithm. in here we do a lot of things depending on the stage
 * of the algorithm. this function does a complete (single) pass over the data. the upper layer
 * calls this function multiple times depending the stage of the algorithm
 */
static List *one_data_pass(TrainModelState *pstate, KMeansStateDescription *state_description,
                           uint32_t const batch_size, uint32_t const idx_current_centroids,
                           uint32_t const idx_next_centroids, bool const sample, double const sample_probability,
                           AlgoStage stage, List *centroid_candidates, double *cost_centroid_candidates,
                           std::mt19937_64 *prng)
{
    bool plan_exhausted = false;
    bool result = false;
    bool first_candidate = centroid_candidates == nullptr;
    bool initialized = state_description->initialized;
    uint32_t current_slot = 0;
    uint32_t dimension = state_description->dimension;
    uint32_t const num_centroids = state_description->num_centroids;
    uint32_t num_elements_round = 0;
    uint32_t slot_number = 0;
    uint32_t valid_row = 0;
    ModelTuple const *scan_slot = nullptr;
    double *bbox_min = nullptr;
    double *bbox_max = nullptr;
    double local_sample_probability = sample_probability;
    double cost_of_batch = 0.;
    GSPoint *centroid_candidate = nullptr;
    GSPoint *batch = state_description->batch;
    
    while (!plan_exhausted) {
        current_slot = 0;
        /* we produce a batch of slots to be passed to the algorithm */
        while (current_slot < batch_size) {
            scan_slot = pstate->fetch(pstate->callback_data, &pstate->tuple)
                            ? &pstate->tuple : nullptr;
            ++slot_number;
            // every slot will have its own chances
            local_sample_probability = sample_probability;
            /*
             * we get out of the whole thing if we have exhausted the relation or
             * we have found our k centroids.
             * if we were not able to sample the k centroids, the upper call
             * will perform runs until we have done so
             */
            if (unlikely(!scan_slot || state_description->current_centroid == num_centroids)) {
                plan_exhausted = true;
                break;
            }
            
            /*
             * we jump over rows with empty coordinates
             */
            if (unlikely(scan_slot->isnull[0])) {
                continue;
            }
            
            /*
             * if we have not initialized the node state because the user didn't provide the dimension, we set it
             * here and initialized with what we found. this is very optimistic because if the very first row
             * is not "correct", then all rows not matching this dimension will be ignored.
             *
             * observe that this branch will be executed at most once and after it all fields are allocated
             * and "properly" initialized
             */
            if (unlikely(stage == KMEANS_INIT && !initialized)) {
                ArrayType *first_poing_pgarray = DatumGetArrayTypeP(scan_slot->values[0]);
                dimension = ARR_DIMS(first_poing_pgarray)[0];
                initialize_fields(state_description, num_centroids, dimension);
                if (unlikely(PointerGetDatum(first_poing_pgarray) != scan_slot->values[0]))
                    pfree(first_poing_pgarray);
                batch = state_description->batch;
                initialized = true;
            }
            
            if (deal_sample(sample, prng, batch, current_slot, scan_slot, dimension, first_candidate,
                            centroid_candidates, local_sample_probability, stage))
                continue;
            
            if ((stage == KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE) || (stage == KMEANS_INITIAL_CENTROIDS_BB_SAMPLE)) {
                /*
                 * we only know the expected number of centroid candidates that we will produce
                 * but not the exact number. thus we allocate each one of them on demand
                 * (observe that we cannot use the batch structure because the number of
                 * candidates we generate can be much larger than the size of a batch)
                 */
                centroid_candidate = reinterpret_cast<GSPoint *>(palloc0(sizeof(GSPoint)));
                
                /*
                 * observe that the scan_slot was already copied above (when sampling) and thus
                 * we just move the memory reference from the batch slot to the newly allocated
                 * GSPoint
                 */
                *centroid_candidate = batch[current_slot];
                centroid_candidate->distance_to_closest_centroid = DBL_MAX;
                centroid_candidate->id = slot_number;
                batch[current_slot].id = 0;
                batch[current_slot].distance_to_closest_centroid = 0.;
                batch[current_slot].weight = 0.;
                batch[current_slot].pg_coordinates = nullptr;
                batch[current_slot].should_free = false;
                
                /*
                 * this stores the reference to the current selected candidate and thus we
                 * can forget about it
                 */
                centroid_candidates = lappend(centroid_candidates, centroid_candidate);
                
                /*
                 * memory should be allocated in the next iteration
                 */
                centroid_candidate = nullptr;
                
                ++num_elements_round;
                
                /*
                 * for kmeans|| we produce a single candidate the very first time.
                 * for random++ we produce a number of candidates in a single pass
                 * (thus we consume the whole relation since we do not update current_slot)
                 */
                if (unlikely(first_candidate && (stage == KMEANS_INITIAL_CENTROIDS_BB_SAMPLE))) {
                    plan_exhausted = true;
                    break;
                } else if (unlikely(first_candidate)) {
                    first_candidate = false;
                }
            } else {
                /*
                 * the element's coordinates are copied to be processed
                 */
                result = copy_slot_coordinates_to_array(batch + current_slot, scan_slot, dimension);
                batch[current_slot].id = slot_number;
                valid_row = result ? 1U : 0U;
                current_slot += valid_row;
                state_description->num_dead_points += 1 - valid_row;
            }
        }
        
        /* we process the batch
         * each stage happens in a batch and thus branch misprediction should not be a problem
         * also, except for KMEANS_LLOYD, the other two stages require exactly one data pass
         */
        switch (stage) {
            case KMEANS_INIT:
                /*
                 * this run is to obtain initial statistics about the data (like the number of valid tuples)
                 * and the coordinates of the bounding box
                 */
                bbox_min = reinterpret_cast<double *>(state_description->bbox_min);
                bbox_max = reinterpret_cast<double *>(state_description->bbox_max);
                init_kmeans(state_description, bbox_min, bbox_max, batch, current_slot);
                
                /*
                 * we are done with the batch and thus we release the allocated memory (corresponding
                 * to the points of the batch)
                 */
                release_batch(batch, current_slot);
                
                break;
            case KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE:
            case KMEANS_INITIAL_CENTROIDS_BB_SAMPLE:
                /*
                 * when sampling for random++ and kmeans|| we do no computations other than the sample
                 * the upper call will run kmeans++ after the candidates have been sampled
                 */
                break;
            case KMEANS_INITIAL_CENTROIDS_BB_COMPUTE_COST:
                /*
                 * when computing the cost of a solution, we do it in a batched manner as the other
                 * non-sampling cases
                 */
                compute_cost_and_weights(centroid_candidates, batch, dimension, current_slot, &cost_of_batch);
                
                if (unlikely(cost_centroid_candidates == nullptr))
                    ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("k-means exec: cost variable must be non-null")));
                
                *cost_centroid_candidates += cost_of_batch;
                
                /*
                 * we are done with the batch and thus we release the allocated memory (corresponding
                 * to the points of the batch)
                 */
                release_batch(batch, current_slot);
                
                break;
            case KMEANS_LLOYD:
                /*
                 * let's find out which centroid is the closest and aggregate the corresponding statistics
                 */
                update_centroids(state_description, batch, current_slot, idx_current_centroids, idx_next_centroids);
                
                /*
                 * we are done with the batch and thus we release the allocated memory (corresponding
                 * to the points of the batch)
                 */
                release_batch(batch, current_slot);
                
                break;
            default:
                ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("k-means exec: no known algorithm stage")));
        }
    }
    return centroid_candidates;
}

/*
 * this sets the weights of a set of candidates to 1 (every point is the centroid of itself)
 */
void reset_weights(List const *centroids)
{
    ListCell const *current_centroid_cell = centroids ? centroids->head : nullptr;
    GSPoint *centroid = nullptr;
    
    for (; current_centroid_cell != nullptr; current_centroid_cell = lnext(current_centroid_cell)) {
        centroid = reinterpret_cast<GSPoint *>(lfirst(current_centroid_cell));
        centroid->weight = 1U;
    }
}

/*
 * this runs kmeans++ on a super-set of centroids to obtain the k centroids we want as seeding
 */
List *kmeanspp(KMeansStateDescription *description, List *centroids_candidates, uint32_t const idx_current_centroids,
               std::mt19937_64 *prng)
{
    Centroid *centroids = description->centroids[idx_current_centroids];
    uint32_t const num_centroids_needed = description->num_centroids;
    uint32_t const size_centroid_bytes = description->size_centroids_bytes;
    uint32_t num_candidates = centroids_candidates ? centroids_candidates->length : 0;
    uint32_t const dimension = description->dimension;
    ListCell *current_candidate_cell = nullptr;
    ListCell *prev_candidate_cell = nullptr;
    ListCell *tmp_cell = nullptr;
    std::uniform_real_distribution<double> unit_sampler(0., 1.);
    // if there are less candidates than centroids needed, then all of them become centroids
    double sample_probability = num_candidates <= num_centroids_needed ? 1. : 1. / static_cast<double>(num_candidates);
    double sample_probability_correction = 0.;
    double candidate_probability = 0.;
    double distance = 0.;
    double sum_distances = 0.;
    double sum_distances_local_correction = 0.;
    double sum_distances_correction = 0.;
    ArrayType *current_candidate_pgarray = nullptr;
    ArrayType *current_centroid_pgarray = nullptr;
    GSPoint *current_candidate = nullptr;
    GSPoint *tmp_candidate = nullptr;
    uint32_t current_centroid_idx = description->current_centroid;
    uint32_t tries_until_next_centroid = 0;
    bool no_more_candidates = false;
    
    /*
     * we expect to produce all centroids in one go and to be able to produce them because
     * we have enough candidates
     */
    if ((current_centroid_idx > 0) || (num_centroids_needed == 0))
        ereport(ERROR,
                (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("k-means: not able to run k-means++")));
    /*
     * we produce the very first centroid uniformly at random (not weighted)
     * the rest of the centroids are found by sampling w.r.t. the (weighted) distance to their closest centroid
     * (elements that are far from their centroids are more likely to become centroids themselves,
     * but heavier points also have better chances)
     */
    while (likely((current_centroid_idx < num_centroids_needed) && !no_more_candidates)) {
        ++tries_until_next_centroid;
        /*
         * the probability of no choosing a centroid in a round is > 0
         * so we loop until we have found all our k centroids
         * in each round one centroid will be found at most
         */
        prev_candidate_cell = nullptr;
        no_more_candidates = true;
        current_candidate_cell = centroids_candidates ? centroids_candidates->head : nullptr;
        for (; current_candidate_cell != nullptr; current_candidate_cell = lnext(current_candidate_cell)) {
            current_candidate = reinterpret_cast<GSPoint *>(lfirst(current_candidate_cell));
            current_candidate_pgarray = current_candidate->pg_coordinates;
            
            no_more_candidates = false;
            candidate_probability = unit_sampler(*prng);
            
            /*
             * the very first candidate will be sampled uniformly, the rest will be sampled
             * w.r.t. the distance to their closest centroid (the farther the more probable)
             */
            if (likely(current_centroid_idx > 0)) {
                // this distance is already weighted and thus requires no weighting again
                distance = current_candidate->distance_to_closest_centroid;
                twoDiv(distance, sum_distances, &sample_probability, &sample_probability_correction);
                sample_probability += sample_probability_correction;
            }
            
            /*
             * this is weighted sampling (taking into consideration the weight of the point)
             */
            if (candidate_probability >= sample_probability) {
                prev_candidate_cell = current_candidate_cell;
                continue;
            }
            
            /*
             * this candidate becomes a centroid
             */
            current_centroid_pgarray = centroids[current_centroid_idx].coordinates;
            
            /*
             * we copy the coordinates of the centroid to the official set of centroids
             */
            if (unlikely((current_centroid_pgarray != nullptr) and (current_candidate_pgarray != nullptr))) {
                auto point_coordinates_to = reinterpret_cast<double *>(ARR_DATA_PTR(current_centroid_pgarray));
                auto point_coordinates_from = reinterpret_cast<double *>(ARR_DATA_PTR(current_candidate_pgarray));
                memset_s(point_coordinates_to, size_centroid_bytes, 0, size_centroid_bytes);
                errno_t rc = memcpy_s(point_coordinates_to,
                                      size_centroid_bytes, point_coordinates_from, size_centroid_bytes);
                securec_check(rc, "\0", "\0");
            }
            
            /*
             * we delete the element that just became centroid from the list of candidates (along all its information)
             */
            centroids_candidates = list_delete_cell(centroids_candidates, current_candidate_cell, prev_candidate_cell);
            pfree(current_candidate->pg_coordinates);
            current_candidate_cell =
                    prev_candidate_cell ? prev_candidate_cell : centroids_candidates ? centroids_candidates->head
                                                                                     : nullptr;
            
            /*
             * we can reset sum_distances because it will be overwritten below anyway
             */
            sum_distances = 0.;
            
            /*
             * we update the distance to the closest centroid depending on the presence of the new
             * centroid
             */
            tmp_cell = centroids_candidates ? centroids_candidates->head : nullptr;
            for (; tmp_cell != nullptr; tmp_cell = lnext(tmp_cell)) {
                tmp_candidate = reinterpret_cast<GSPoint *>(lfirst(tmp_cell));
                distance = l2_squared(reinterpret_cast<double *>(ARR_DATA_PTR(tmp_candidate->pg_coordinates)),
                                      reinterpret_cast<double *>(ARR_DATA_PTR(current_centroid_pgarray)), dimension);
                
                /*
                 * medium temporal locality for a prefetch for write
                 */
                prefetch(lnext(tmp_cell), 1, 2);
                
                /*
                 * since we are dealing with weighted points, the overall sum of distances has
                 * to consider the weight of each point
                 */
                twoMult(distance, tmp_candidate->weight, &distance, &sum_distances_correction);
                distance += sum_distances_correction;
                
                /*
                 * we store the weighted distance at the point to save the multiplication later on
                 * when sampling
                 * only if the new centroid becomes the closest one to a candidate we update
                 */
                if ((current_centroid_idx == 0) || (distance < tmp_candidate->distance_to_closest_centroid))
                    tmp_candidate->distance_to_closest_centroid = distance;
                
                /*
                 * every distance appears as many times as the weight of the point
                 */
                twoSum(sum_distances, tmp_candidate->distance_to_closest_centroid, &sum_distances,
                       &sum_distances_local_correction);
                sum_distances_correction += sum_distances_local_correction;
            }
            /*
             * high temporal locality for a prefetch for read
             */
            if (likely(current_candidate_cell != nullptr))
                prefetch(lnext(current_candidate_cell), 0, 3);
            
            sum_distances += sum_distances_correction;
            
            ++current_centroid_idx;
            tries_until_next_centroid = 0;
            break;
        }
    }
    
    /*
     * we get rid of the linked list of candidates in case some candidates are left
     */
    while (centroids_candidates) {
        current_candidate_cell = centroids_candidates->head;
        current_candidate = reinterpret_cast<GSPoint *>(lfirst(current_candidate_cell));
        /*
         * low temporal locality for a prefetch for read
         */
        prefetch(lnext(current_candidate_cell), 0, 1);
        /*
         * this frees the list cell
         */
        centroids_candidates = list_delete_cell(centroids_candidates, current_candidate_cell, nullptr);
        /*
         * this frees the PG array inside the GSPoint
         */
        pfree(current_candidate->pg_coordinates);
        /*
         * and finally, this free ths GSPoint
         */
        pfree(current_candidate);
    }
    
    description->current_centroid = current_centroid_idx;
    
    return centroids_candidates;
}

/*
 * this aggregates the value of the function over all centroids
 */
void compute_cost(KMeansStateDescription *description, uint32_t const idx_current_centroids)
{
    Centroid *centroids = description->centroids[idx_current_centroids];
    uint32_t const num_centroids = description->num_centroids;
    IncrementalStatistics *output_statistics = description->solution_statistics + idx_current_centroids;
    output_statistics->reset();
    
    for (uint32_t c = 0; c < num_centroids; ++c)
        *output_statistics += centroids[c].statistics;
}

/*
 * every iteration there is a set of centroids (the next one) that is reset
 */
void reset_centroids(KMeansStateDescription *description, uint32_t const idx_centroids)
{
    uint32_t const num_centroids = description->num_centroids;
    uint32_t const size_centroid_bytes = description->size_centroids_bytes;
    Centroid *centroids = description->centroids[idx_centroids];
    Centroid *centroid = nullptr;
    double *centroid_coordinates = nullptr;
    
    for (uint32_t c = 0; c < num_centroids; ++c) {
        centroid = centroids + c;
        centroid->statistics.reset();
        centroid_coordinates = reinterpret_cast<double *>(ARR_DATA_PTR(centroid->coordinates));
        memset_s(centroid_coordinates, size_centroid_bytes, 0, size_centroid_bytes);
    }
}

/*
 * this produces the centroid by dividing the aggregate by the amount of points it got assigned
 * we assumed that population > 0
 */
force_inline void finish_centroid(double *centroid_aggregation, uint32_t const dimension, double const population)
{
    double local_correction = 0.;
    for (uint32_t d = 0; d < dimension; ++d) {
        twoDiv(centroid_aggregation[d], population, centroid_aggregation + d, &local_correction);
        centroid_aggregation[d] += local_correction;
    }
}

void merge_centroids(KMeansStateDescription *description, uint32_t const idx_current_centroids,
                     uint32_t const idx_next_centroids)
{
    uint32_t const num_centroids = description->num_centroids;
    uint32_t const size_centroid_bytes = description->size_centroids_bytes;
    uint32_t const dimension = description->dimension;
    Centroid *const current_centroids = description->centroids[idx_current_centroids];
    Centroid *const next_centroids = description->centroids[idx_next_centroids];
    Centroid *current_centroid = nullptr;
    Centroid *next_centroid = nullptr;
    double *current_centroid_coordinates = nullptr;
    double *next_centroid_coordinates = nullptr;
    
    for (uint32_t c = 0; c < num_centroids; ++c) {
        next_centroid = next_centroids + c;
        current_centroid = current_centroids + c;
        next_centroid_coordinates = reinterpret_cast<double *>(ARR_DATA_PTR(next_centroid->coordinates));
        /*
         * if the cluster of a centroid is empty we copy it from the previous iteration verbatim
         * to not loose it
         */
        if (unlikely(current_centroid->statistics.getPopulation() == 0)) {
            current_centroid_coordinates = reinterpret_cast<double *>(ARR_DATA_PTR(current_centroid->coordinates));
            /*
             * observe that we do not have to memset to zero because current_centroid was reset before the run
             * and since no point was assigned to it, it has remained reset
             */
            error_t rc = memcpy_s(next_centroid_coordinates, size_centroid_bytes, current_centroid_coordinates,
                                  size_centroid_bytes);
            securec_check(rc, "\0", "\0");
        } else {
            finish_centroid(next_centroid_coordinates, dimension, current_centroid->statistics.getPopulation());
        }
    }
}

/*
 * new API follows
 * functions are defined in the order they are required during execution
 */
HyperparameterDefinition kmeans_hyperparameter_definitions[] = {
    HYPERPARAMETER_ENUM("seeding_function", "Random++", kmeans_seeding_str, kmeans_seeding_str_size,
                        kmeans_seeding_getter, kmeans_seeding_setter, HyperparametersKMeans, seeding, HP_NO_AUTOML()),
    HYPERPARAMETER_ENUM("distance_function", "L2_Squared", kmeans_distance_functions_str,
                        kmeans_distance_functions_str_size, kmeans_distance_function_getter,
                        kmeans_distance_function_setter, HyperparametersKMeans, distance, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("verbose", 0, 0, true, 2, true, HyperparametersKMeans, verbosity, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_centroids", 1, 1, true, 1000000, true, HyperparametersKMeans, num_centroids,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("max_iterations", 10, 1, true, ITER_MAX, true, HyperparametersKMeans, num_iterations,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("num_features", 0, 1, true, INT32_MAX, true, HyperparametersKMeans, n_features, HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("batch_size", 1000, 1, true, MAX_BATCH_SIZE, true, HyperparametersKMeans, batch_size,
                        HP_NO_AUTOML()),
    HYPERPARAMETER_INT4("seed", 0, 0, true, INT32_MAX, true, HyperparametersKMeans, external_seed,
                        HP_AUTOML_INT(1, INT32_MAX, 1, ProbabilityDistribution::UNIFORM_RANGE)),
    HYPERPARAMETER_FLOAT8("tolerance", 0.00001, 0.0, false, 1.0, true, HyperparametersKMeans, tolerance,
                          HP_NO_AUTOML()),
};

force_inline static ModelHyperparameters *kmeans_make_hyperparameters(AlgorithmAPI *self)
{
    auto kmeans_hyperp = reinterpret_cast<HyperparametersKMeans *>(palloc0(sizeof(HyperparametersKMeans)));
    
    return &kmeans_hyperp->mhp;
}

force_inline static HyperparameterDefinition const *kmeans_get_hyperparameters(AlgorithmAPI *self,
                                                                               int32_t *definitions_size)
{
    Assert(definitions_size != nullptr);
    *definitions_size = sizeof(kmeans_hyperparameter_definitions) / sizeof(HyperparameterDefinition);
    return kmeans_hyperparameter_definitions;
}

force_inline static void kmeans_update_hyperparameters(AlgorithmAPI *self, ModelHyperparameters *hyperp)
{
    auto kmeans_hyperp = reinterpret_cast<HyperparametersKMeans *>(hyperp);
    
    /* if the user-provided seed is 0 we take the current time but reset
    * the higher order bits to be able to return this seed to the user
    * as an int32_t so that the user can reproduce the run
    * (observe that epoch 2^31 is around year 2038 and the shifts are
    * until then useless)
    */
    if (kmeans_hyperp->external_seed == 0ULL)
        kmeans_hyperp->external_seed = (get_time_ms() << 33U) >> 33U;
}

/*
 * this function initializes the algorithm
 */
static TrainModelState *kmeans_create(AlgorithmAPI *self, const TrainModel *pnode)
{
    if (pnode->configurations != 1)
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("multiple hyper-parameter configurations for k-means are not yet supported")));
    
    auto kmeans_hyperp = reinterpret_cast<HyperparametersKMeans const *>(pnode->hyperparameters[0]);
    
    uint32_t const num_centroids = kmeans_hyperp->num_centroids;
    uint32_t const dimension = kmeans_hyperp->n_features;
    uint32_t const batch_size = kmeans_hyperp->batch_size;
    
    // create state structure
    auto kmeans_state = reinterpret_cast<KMeansState *>(makeNodeWithSize(TrainModelState,
                                                                         sizeof(KMeansState)));
    KMeansStateDescription *kmeans_state_description = &kmeans_state->description;
    
    kmeans_state->done = false;
    
    kmeans_state->description.centroids[0] =
            reinterpret_cast<Centroid *>(palloc0(sizeof(Centroid) * num_centroids));
    kmeans_state->description.centroids[1] =
            reinterpret_cast<Centroid *>(palloc0(sizeof(Centroid) * num_centroids));
    
    /*
     * if the dimension was provided by the user we use that one as a reference
     * to initialize all fields that depend on it
     * if later on the dimension of the tuple disagrees and error is reported
     * if the dimension is not provided by the user then it will be set on the very
     * first tuple we read (optimistically)
     */
    if (dimension > 0) {
        initialize_fields(kmeans_state_description, num_centroids, dimension);
        kmeans_state_description->late_initialized = false;
    }
    
    /*
     * the following fields do not depend on the dimension and thus they are initialized
     * here already
     */
    kmeans_state_description->num_centroids = num_centroids;
    
    switch (kmeans_hyperp->distance) {
        case KMEANS_L1:
            kmeans_state_description->distance = l1;
            break;
        case KMEANS_L2:
            kmeans_state_description->distance = l2;
            break;
        case KMEANS_LINF:
            kmeans_state_description->distance = linf;
            break;
        case KMEANS_L2_SQUARED:
            kmeans_state_description->distance = l2_squared;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means init: no known distance function: %u", kmeans_hyperp->distance)));
    }
    
    kmeans_state_description->batch = reinterpret_cast<GSPoint *>(palloc0(sizeof(GSPoint) * batch_size));
    
    return &kmeans_state->tms;
}

force_inline static void output_kmeans_state(Verbosity const verbosity, uint32_t const dimension,
                                             uint64_t const num_good_points, uint64_t const num_dead_points,
                                             double const local_elapsed_time)
{
    if (verbosity > NO_OUTPUT) {
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Initial statistics gathered:")));
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Dimension used for computations: %u", dimension)));
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Number of valid points: %lu", num_good_points)));
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Number of dead points:  %lu", num_dead_points)));
        if (verbosity == VERBOSE_OUTPUT)
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Duration (s):           %0.6lf", local_elapsed_time)));
    }
}

static void kmeans_deal_seeding_function(SeedingFunction seeding_function, uint32_t num_centroids, uint64_t num_points,
                                         double sample_factor, uint32_t num_centroids_orig, Verbosity verbosity,
                                         List **centroid_candidates_kmeans_bb, TrainModelState *pstate,
                                         KMeansStateDescription *state_description, uint32_t const batch_size,
                                         uint32_t idx_current_centroids,
                                         uint32_t idx_next_centroids, std::mt19937_64 *prng)
{
    double sample_probability = 0.;
    double oversampling = 0.;
    struct timespec start_kmeans_round, finish_kmeans_round;
    uint64_t num_candidates = 0ULL;
    uint64_t prev_num_candidates = 0ULL;
    double cost_kmeans_bb = 0.;
    double op_error = 0.;
    double local_elapsed_time = 0.;
    uint32_t current_iteration_kmeans_bb = 0U;
    
    switch (seeding_function) {
        case KMEANS_BB:
            sample_probability = num_centroids >= num_points ? 1.0 : sample_factor / num_points;
            
            /*
             * the number of iterations of kmeans|| depends on the value of the initial solution
             * if the solution is bad, we will iterate for longer time
             * (one iteration consists of two data passes (one to sample candidates, and another
             * to compute the cost of the (partial) solution
             */
            oversampling = sample_factor * static_cast<double>(num_centroids_orig);
            if (verbosity > NO_OUTPUT)
                ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("*** k-means|| oversampling factor: %lf, "
                               "expected number of candidates per round: %lu", sample_factor,
                               static_cast<uint64_t>(oversampling))));
            do {
                clock_gettime(CLOCK_MONOTONIC, &start_kmeans_round);
                /*
                 * this data pass will sample candidates
                 * (the probability of not choosing a single element is > 0 and thus we have to
                 * try until at least one candidate is found)
                 */
                
                do {
                    prev_num_candidates = num_candidates;
                    pstate->rescan(pstate->callback_data);
                    *centroid_candidates_kmeans_bb = one_data_pass(pstate, state_description, batch_size,
                                                                   idx_current_centroids,
                                                                   idx_next_centroids,
                                                                   true, sample_probability,
                                                                   KMEANS_INITIAL_CENTROIDS_BB_SAMPLE,
                                                                   *centroid_candidates_kmeans_bb, nullptr, prng);
                    num_candidates = *centroid_candidates_kmeans_bb ? (*centroid_candidates_kmeans_bb)->length : 0ULL;
                } while (num_candidates <= prev_num_candidates);
                
                /*
                 * this data pass will compute the cost of the (partial) solution
                 */
                reset_weights(*centroid_candidates_kmeans_bb);
                cost_kmeans_bb = 0.;
                pstate->rescan(pstate->callback_data);
                *centroid_candidates_kmeans_bb = one_data_pass(pstate, state_description, batch_size,
                                                               idx_current_centroids,
                                                               idx_next_centroids,
                                                               false, 0., KMEANS_INITIAL_CENTROIDS_BB_COMPUTE_COST,
                                                               *centroid_candidates_kmeans_bb, &cost_kmeans_bb,
                                                               nullptr);
                
                /*
                 * for the next iteration, sample probability changes according to the cost of the current
                 * solution
                 */
                twoDiv(oversampling, cost_kmeans_bb, &sample_probability, &op_error);
                sample_probability += op_error;
                
                clock_gettime(CLOCK_MONOTONIC, &finish_kmeans_round);
                local_elapsed_time = static_cast<double>(finish_kmeans_round.tv_sec - start_kmeans_round.tv_sec);
                local_elapsed_time +=
                        static_cast<double>(finish_kmeans_round.tv_nsec - start_kmeans_round.tv_nsec) /
                        1000000000.0;
                
                if (verbosity == VERBOSE_OUTPUT)
                    ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("*** k-means|| round %u stats: cost: %lf, total number of candidates: %u, "
                                   "duration (s): %0.06lf", current_iteration_kmeans_bb + 1,
                                   cost_kmeans_bb, (*centroid_candidates_kmeans_bb)->length, local_elapsed_time)));
            } while (likely((++current_iteration_kmeans_bb < NUM_ITERATIONS_KMEANSBB) &&
                            (num_candidates < num_points)));
            
            break;
        case KMEANS_RANDOM_SEED:
            /*
             * the expected number of points to sample
             */
            oversampling = sample_factor * static_cast<double>(num_centroids_orig);
            /*
             * if the number of centroids is larger than the number of data points (corner case)
             * each data point becomes a centroid. otherwise we over sample (all data points could
             * be sampled)
             */
            sample_probability = num_centroids >= num_points ? 1.0 : oversampling / num_points;
            
            if (verbosity > NO_OUTPUT)
                ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("*** random++ oversampling factor: %lf, expected number of candidates: %lu",
                               sample_factor, static_cast<uint64_t>(oversampling))));
            
            do {
                pstate->rescan(pstate->callback_data);
                *centroid_candidates_kmeans_bb = one_data_pass(pstate, state_description, batch_size,
                                                               idx_current_centroids, idx_next_centroids,
                                                               true, sample_probability,
                                                               KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE,
                                                               *centroid_candidates_kmeans_bb, nullptr, prng);
                num_candidates = *centroid_candidates_kmeans_bb ? (*centroid_candidates_kmeans_bb)->length : 0ULL;
            } while ((num_candidates < num_points) && (num_candidates < oversampling));
            reset_weights(*centroid_candidates_kmeans_bb);
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: not known seeding function: %u", seeding_function)));
    }
}

void kmeans_create_model(KMeansState *kmeans_state, Model *model)
{
    auto kmeans_hyperp =
            reinterpret_cast<HyperparametersKMeans const *>(kmeans_state->tms.config->hyperparameters[0]);
    uint32_t const num_centroids = kmeans_state->description.num_centroids;
    uint32_t const num_points = kmeans_state->description.num_good_points;
    uint32_t const actual_num_centroids = num_centroids > num_points ? num_points : num_centroids;
    uint32_t const dimension = kmeans_state->description.dimension;
    uint32_t idx_current_centroids = 1U - (kmeans_state->description.current_iteration & 1U);
    bool const late_initialization = kmeans_state->description.late_initialized;
    uint64_t cluster_size = 0ULL;
    auto *score = reinterpret_cast<TrainingScore *>(palloc0(sizeof(TrainingScore)));
    Centroid *current_centroid = nullptr;
    PCentroid *current_wh_centroid = nullptr;
    double *centroid_coordinates_data = nullptr;
    double objective_function = 0.;
    double local_error = 0.;
    double total_error = 0.;
    uint32_t const size_centroid_bytes = kmeans_state->description.size_centroids_bytes;
    errno_t errorno = EOK;
    
    /*
     * this is generic information about the model. specific information (about k-means) is set down below
     */
    model->return_type = INT4OID;
    model->pre_time_secs = kmeans_state->description.seeding_time;
    model->exec_time_secs = kmeans_state->description.execution_time;
    model->processed_tuples = kmeans_state->description.num_good_points;
    model->discarded_tuples = kmeans_state->description.num_dead_points;
    
    /*
     * in order to serialize the model, it has to be stored in a contiguous memory chunk,
     * thus we compute the required space as follows:
     * sizeof(ModelKMeans) + (sizeof(WHCentroid) * actual_num_centroids) +
     * (sizeof(double) * dimension * actual_num_centroids)
     */
    Size const coordinates_array_offset = sizeof(ModelKMeansV01) + (sizeof(PCentroid) * actual_num_centroids);
    Size const model_kmeans_size =
            coordinates_array_offset + (kmeans_state->description.size_centroids_bytes * actual_num_centroids);
    
    auto model_kmeans = reinterpret_cast<ModelKMeansV01 *>(palloc0(model_kmeans_size));
    
    /*
     * filling in the raw model data (to be serialized)
     */
    model->data.size = model_kmeans_size;
    model->data.raw_data = model_kmeans;
    model->data.version = DB4AI_MODEL_V01;
    
    /*
     * this is particular information about k-means (centroids are set down below)
     */
    model_kmeans->actual_num_centroids = actual_num_centroids;
    model_kmeans->dimension = dimension;
    model_kmeans->distance_function_id = kmeans_hyperp->distance;
    model_kmeans->coordinates_offset = coordinates_array_offset;
    model_kmeans->num_actual_iterations = kmeans_state->description.actual_num_iterations;
    
    /*
     * this fields are deprecated and will disappear in the future
     */
    model->train_info = nullptr;
    model->num_actual_iterations = 0U;
    model->weights = 0U;
    model->model_data = 0U;
    
    /*
     * on late initialization, the dimension was set at runtime and thus the corresponding
     * hyper-parameter has to be updated
     */
    if (late_initialization)
        update_model_hyperparameter(model->memory_context, model->hyperparameters, "num_features", INT4OID,
                                    Int32GetDatum(dimension));

    /*
     * we fill in the information of every centroid
     * the array of centroids start right after the last element of model_kmeans
     *
     * all coordinates are stored at the end of ModelKMeans (after the array of WHCentroid)
     */
    model_kmeans->centroids =
            reinterpret_cast<PCentroid *>(reinterpret_cast<char *>(model_kmeans) + sizeof(ModelKMeansV01));
    centroid_coordinates_data =
            reinterpret_cast<double *>(reinterpret_cast<char *>(model_kmeans) + coordinates_array_offset);
    uint32_t centroid_coordinates_offset = 0U;
    for (uint32_t current_centroid_idx = 0; current_centroid_idx < actual_num_centroids; ++current_centroid_idx) {
        current_centroid = kmeans_state->description.centroids[idx_current_centroids] + current_centroid_idx;
        cluster_size = current_centroid->statistics.getPopulation();

        current_wh_centroid = model_kmeans->centroids + current_centroid_idx;

        current_wh_centroid->id = current_centroid->id;
        current_wh_centroid->objective_function = current_centroid->statistics.getTotal();
        current_wh_centroid->avg_distance = current_centroid->statistics.getEmpiricalMean();
        current_wh_centroid->min_distance = cluster_size > 0 ? current_centroid->statistics.getMin() : 0.;
        current_wh_centroid->max_distance = current_centroid->statistics.getMax();
        current_wh_centroid->std_dev_distance = current_centroid->statistics.getEmpiricalStdDev();
        current_wh_centroid->cluster_size = cluster_size;
        current_wh_centroid->coordinates = centroid_coordinates_data + centroid_coordinates_offset;

        errorno = memcpy_s(current_wh_centroid->coordinates, size_centroid_bytes,
                           ARR_DATA_PTR(current_centroid->coordinates), size_centroid_bytes);
        securec_check(errorno, "\0", "\0");
        centroid_coordinates_offset += dimension;

        twoSum(objective_function, current_wh_centroid->objective_function, &objective_function, &local_error);
        total_error += local_error;
    }

    /*
     * finally, the loss function (objective function)
     * observe that hyper-parameters are set for all models the same
     */
    score->value = objective_function + total_error;
    score->name = kmeans_distance_to_string(kmeans_hyperp->distance);
    model->scores = lappend(model->scores, score);
}

/*
 * this executes the k-means "training" by executing multiple scans through the data
 * until convergence can be declared.
 * the operator works in stages:
 * 1) obtain general statistics about the input data (one data pass),
 * 2) execute a seeding method (random++ or kmeans||) (at least one data pass but not more than 10),
 * 3) run Lloyd's algorithm (at least one data pass)
 */
static void kmeans_run(AlgorithmAPI *self, TrainModelState *pstate, Model **models)
{
    /*
     * get information from the node
     */
    Assert(pstate->finished == 0);
    Assert(pstate->config->configurations == 1);
    
    auto kmeans_state = reinterpret_cast<KMeansState *>(pstate);
    auto kmeans_hyperp =
            const_cast<HyperparametersKMeans *>(
                    reinterpret_cast<HyperparametersKMeans const *>(pstate->config->hyperparameters[0]));
    KMeansStateDescription& state_description = kmeans_state->description;
    uint32_t const max_num_iterations = kmeans_hyperp->num_iterations;
    uint32_t const batch_size = kmeans_hyperp->batch_size;
    uint32_t idx_current_centroids = 0U;
    uint32_t idx_next_centroids = 0U;
    uint32_t num_centroids = state_description.num_centroids; // this field is always initialized
    uint32_t const num_centroids_orig = num_centroids;
    Verbosity const verbosity = kmeans_hyperp->verbosity;
    bool const late_initialization = !state_description.initialized;
    SeedingFunction const seeding_function = kmeans_hyperp->seeding;
    IncrementalStatistics *prev_solution_statistics = nullptr;
    IncrementalStatistics *current_solution_statistics = nullptr;
    double cost_fraction = 0.;
    double cost_fraction_correction = 0.;
    double local_elapsed_time = 0.;
    double total_time = 0.;
    double const sample_factor = num_centroids < 1000000 ? 4. : 2.;
    double const tolerance = kmeans_hyperp->tolerance;
    List *centroid_candidates_kmeans_bb = nullptr;
    struct timespec start_time, finish_time, start_kmeans_round, finish_kmeans_round;
    uint64_t num_points = 0ULL;
    uint64_t seed = kmeans_hyperp->external_seed;
    
    Assert(seed != 0);
    
    uint64_t const external_seed = seed;
    /*
     * internal seed obtained from random.org. do not change it because
     * it will (most probably) change the overall seed used and results
     * will not be reproducible
     */
    uint64_t const internal_seed = 0x274066DB9441E851ULL;
    seed ^= internal_seed;
    
    /*
     * high-quality prng (based Mersenne primes)
     * nothing of this sort is currently available in the system
     */
    std::mt19937_64 prng(seed);
    
    // check if training is already finished, if so we return the next centroid
    if (!kmeans_state->done) {
        /*
         * we have to see that the column we are passed on is of type array
         */
        Oid oidtype = pstate->tuple.typid[0];
        
        if (unlikely((oidtype != FLOAT8ARRAYOID)))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: data is not of type float8 (double precision) array")));
        
        if (pstate->tuple.ncolumns != 1)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: relation should contain only a single attribute "
                           "(point coordinates in a double precision array)")));
        
        /*
         * on late initialization we propagate the dimension properly
         */
        if (late_initialization) {
            kmeans_hyperp->n_features = state_description.dimension;
            kmeans_state->description.late_initialized = true;
        }
        
        /*
         * Before iterating we have to find an initial set of centroids (seeds)
         * either random seeds or using kmeans||, this is one table scan on which we
         * can also obtain the diameter of the bounding box (for normalization for example)
         * from this scan we can also obtain the coordinates of the bounding box
         */
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        one_data_pass(pstate, &state_description, batch_size,
                      0, 1, false, 0., KMEANS_INIT, nullptr, nullptr, nullptr);
        clock_gettime(CLOCK_MONOTONIC, &finish_time);
        
        local_elapsed_time = static_cast<double>(finish_time.tv_sec - start_time.tv_sec);
        local_elapsed_time += static_cast<double>(finish_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
        total_time += local_elapsed_time;
        
        output_kmeans_state(verbosity, state_description.dimension, state_description.num_good_points,
                            state_description.num_dead_points, local_elapsed_time);
        
        num_points = state_description.num_good_points;
        /*
         * if the number of centroids is larger than the number of data points we save useless computations
         * by keeping track of the actual number of centroids that can be realized, in the very end
         * we restore the original number of centroids
         */
        state_description.num_centroids = num_centroids_orig > num_points ? num_points : num_centroids_orig;
        num_centroids = state_description.num_centroids;
        
        if (unlikely(num_points == 0))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: no valid point found (no input array seems to be a one-dimensional array, "
                           "or no point seems to be fully dimensional (perhaps all points have a null dimension?))")));
        
        /*
         * it is time to produce an initial set of centroids to start with
         */
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        
        idx_current_centroids = state_description.current_iteration & 1U;
        idx_next_centroids = 1U - idx_current_centroids;
        
        /*
        * if the number of centers is larger than the number of data points we choose
        * all data points (we have no other option). this is a non-sense corner case.
        * for the time being we restrict to k's that fit in memory, later we can
        * lift that restriction by spooling centroids to a file
        *
        * for random production we sample with probability (sample_factor * k)/n
        *
        * for kmeans|| we sample the very first points with probability sample_factor / n and
        * later with probability (sample_factor * k * d(x))/sum(d(y))
        */
        kmeans_deal_seeding_function(seeding_function, num_centroids, num_points, sample_factor, num_centroids_orig,
                                     verbosity, &centroid_candidates_kmeans_bb, pstate, &state_description,
                                     batch_size, idx_current_centroids, idx_next_centroids, &prng);
        
        /*
         * once the set of candidates (> k) has been gathered, we produce k initial centroids using
         * (weighted) kmeans++
         * observe that the output of this function are the k centroids stored in their place
         * and the list of candidates is freed up inside the function, thus accessing the list
         * is illegal
         */
        if (verbosity > NO_OUTPUT)
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** k-means++ begin: consolidating %u candidates to %u centroid(s)",
                           centroid_candidates_kmeans_bb->length, num_centroids)));
        
        clock_gettime(CLOCK_MONOTONIC, &start_kmeans_round);
        
        centroid_candidates_kmeans_bb = kmeanspp(&state_description, centroid_candidates_kmeans_bb,
                                                 idx_current_centroids, &prng);
        
        clock_gettime(CLOCK_MONOTONIC, &finish_kmeans_round);
        local_elapsed_time = static_cast<double>(finish_kmeans_round.tv_sec - start_kmeans_round.tv_sec);
        local_elapsed_time +=
                static_cast<double>(finish_kmeans_round.tv_nsec - start_kmeans_round.tv_nsec) /
                1000000000.0;
        if (verbosity == VERBOSE_OUTPUT)
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** k-means++ ended: duration (s): %0.06lf", local_elapsed_time)));
        
        Assert(!centroid_candidates_kmeans_bb);
        
        clock_gettime(CLOCK_MONOTONIC, &finish_time);
        
        local_elapsed_time = static_cast<double>(finish_time.tv_sec - start_time.tv_sec);
        local_elapsed_time += static_cast<double>(finish_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
        total_time += local_elapsed_time;
        state_description.seeding_time = local_elapsed_time;
        
        if (verbosity == VERBOSE_OUTPUT)
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Seed centroids constructed (%lu, %u): duration (s): %0.6lf",
                           num_points, state_description.current_centroid,
                           state_description.seeding_time)));

        // we reset current_centroid so that later each centroid can be output
        state_description.current_centroid = 0;
        
        /*
         * Scan the sub-plan and feed all the tuples to kmeans (every single iteration).
         * for these scans we will do batching
         */
        do {
            clock_gettime(CLOCK_MONOTONIC, &start_time);
            idx_next_centroids = 1U - idx_current_centroids;
            prev_solution_statistics = state_description.solution_statistics + idx_next_centroids;
            
            /*
             * in this iteration we reset the set of next centroids start with a clean set for aggregation
             */
            reset_centroids(&state_description, idx_next_centroids);
            
            /*
             * every iteration we have to reset the sub-plan to be able to re-scan it
             */
            pstate->rescan(pstate->callback_data);
            one_data_pass(pstate, &state_description, batch_size, idx_current_centroids,
                          idx_next_centroids, false, 0., KMEANS_LLOYD, nullptr, nullptr, nullptr);
            
            /*
             * let's produce the new set of centroids for the next iteration
             * (this should be executed at the coordinator in a distributed environment
             */
            merge_centroids(&state_description, idx_current_centroids, idx_next_centroids);
            
            compute_cost(&state_description, idx_current_centroids);
            current_solution_statistics = state_description.solution_statistics + idx_current_centroids;
            
            if (unlikely(state_description.current_iteration == 1)) {
                cost_fraction = 1.;
            } else {
                twoDiv(prev_solution_statistics->getTotal(), current_solution_statistics->getTotal(),
                       &cost_fraction, &cost_fraction_correction);
                cost_fraction += cost_fraction_correction;
                twoDiff(cost_fraction, 1.0, &cost_fraction, &cost_fraction_correction);
                cost_fraction += cost_fraction_correction;
            }
            
            idx_current_centroids = idx_next_centroids;
            
            clock_gettime(CLOCK_MONOTONIC, &finish_time);
            
            local_elapsed_time = static_cast<double>(finish_time.tv_sec - start_time.tv_sec);
            local_elapsed_time += static_cast<double>(finish_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
            total_time += local_elapsed_time;
            
            if (verbosity == VERBOSE_OUTPUT)
                ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("*** iteration %u: duration (s): %0.6lf, "
                               "total relevant population %lu, "
                               "total cost %0.6lf, "
                               "average distance %0.6lf, "
                               "min distance %0.6lf, "
                               "max distance %0.6lf, "
                               "standard deviation of distances %0.6lf, "
                               "cost delta: %0.6lf", state_description.current_iteration, local_elapsed_time,
                               current_solution_statistics->getPopulation(),
                               current_solution_statistics->getTotal(), current_solution_statistics->getEmpiricalMean(),
                               current_solution_statistics->getMin(), current_solution_statistics->getMax(),
                               current_solution_statistics->getEmpiricalStdDev(), cost_fraction)));
        } while ((++state_description.current_iteration <= max_num_iterations)
                 && (cost_fraction >= tolerance) && (num_points > num_centroids));
        
        state_description.execution_time = total_time;
        state_description.actual_num_iterations = state_description.current_iteration - 1;
        
        if (verbosity > NO_OUTPUT) {
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Number of centroids constructed:    %u", num_centroids)));
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Value of global objective function: %0.6lf", current_solution_statistics->getTotal())));
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Seed:                               %lu", external_seed)));
            if (verbosity == VERBOSE_OUTPUT)
                ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("*** Total duration time (s):            %0.6lf", state_description.execution_time)));
        }
        
        /*
         * finally set the finished flag to true
         */
        kmeans_state->done = true;
        
        /*
         * we restore the original number of centroids
         */
        state_description.num_centroids = num_centroids_orig;
    }
    
    // number of configurations already run
    ++pstate->finished;
    
    Model *model = models[0];
    MemoryContext oldcxt = MemoryContextSwitchTo(model->memory_context);
    
    kmeans_create_model(kmeans_state, model);
    
    model->status = ERRCODE_SUCCESSFUL_COMPLETION;
    MemoryContextSwitchTo(oldcxt);
}

/*
 * this function frees up all the storage required by the algorithm.
 * at this point the upper layer has made persistent the model and
 * this information is not required anymore
 */
static void kmeans_end(AlgorithmAPI *self, TrainModelState *pstate)
{
    auto kmeans_state_node = reinterpret_cast<KMeansState *>(pstate);
    
    if (kmeans_state_node->description.initialized) {
        uint32_t const num_centroids = kmeans_state_node->description.num_centroids;
        
        for (uint32_t c = 0; c < num_centroids; ++c) {
            pfree(kmeans_state_node->description.centroids[0][c].coordinates);
            pfree(kmeans_state_node->description.centroids[1][c].coordinates);
        }
        
        pfree(kmeans_state_node->description.centroids[0]);
        pfree(kmeans_state_node->description.centroids[1]);
        pfree(kmeans_state_node->description.batch);
    }
}

force_inline static MetricML *kmeans_metrics_loss(AlgorithmAPI *self, int *num_metrics)
{
    elog(ERROR, "kmeans_metrics_loss");
    return nullptr;
}

void kmeans_set_references(ModelKMeansV01 *model_kmeans)
{
    uint32_t const actual_num_centroids = model_kmeans->actual_num_centroids;
    /*
     * we have to fix the internal pointers to point to the right locations (as memory has changed)
     * observe that this pointers are pretty much offsets into the same memory chunk
     *
     * the array of centroids start right after the last element of model_kmeans (we fix the address)
     */
    model_kmeans->centroids =
            reinterpret_cast<PCentroid *>(reinterpret_cast<char *>(model_kmeans) + sizeof(ModelKMeansV01));
    /*
     * the address of the coordinates of each centroid must be fixed as well
     */
    PCentroid *current_centroid = nullptr;
    double *centroid_coordinates_data =
            reinterpret_cast<double *>(reinterpret_cast<char *>(model_kmeans) + model_kmeans->coordinates_offset);
    uint32_t centroid_coordinates_offset = 0U;
    for (uint32_t c = 0; c < actual_num_centroids; ++c) {
        current_centroid = model_kmeans->centroids + c;
        current_centroid->coordinates = centroid_coordinates_data + centroid_coordinates_offset;
        centroid_coordinates_offset += model_kmeans->dimension;
    }
}

ModelPredictor kmeans_predict_prepare(AlgorithmAPI *self, SerializedModel const *model, Oid return_type)
{
    if (unlikely(!model))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict prepare: model cannot be null")));
    
    if (unlikely(model->version != DB4AI_MODEL_V01))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict prepare: currently only model V01 is supported")));
    
    auto model_kmeans = reinterpret_cast<ModelKMeansV01 *>(model->raw_data);
    
    kmeans_set_references(model_kmeans);
    
    return reinterpret_cast<ModelPredictor>(model_kmeans);
}

static Datum kmeans_predict(AlgorithmAPI *, ModelPredictor model, Datum *data, bool *nulls, Oid *types, int32_t nargs)
{
    auto kmeans_model = reinterpret_cast<ModelKMeansV01 *>(model);
    
    /*
     * sanity checks
     */
    if (unlikely(nargs != 1))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict: only a single attribute containing the coordinates is accepted")));
    
    if (unlikely(types[0] != FLOAT8ARRAYOID))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict: only double precision array of coordinates is accepted")));
    
    if (unlikely(nulls[0]))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict: array of coordinates cannot be null")));
    
    uint32_t const num_centroids = kmeans_model->actual_num_centroids;
    uint32_t const dimension = kmeans_model->dimension;
    double (*distance)(double const *, double const *, uint32_t const) = nullptr;
    
    if (unlikely(num_centroids == 0))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means predict: number of centroids must be positive")));
    
    switch (kmeans_model->distance_function_id) {
        case KMEANS_L1:
            distance = l1;
            break;
        case KMEANS_L2:
            distance = l2;
            break;
        case KMEANS_L2_SQUARED:
            distance = l2_squared;
            break;
        case KMEANS_LINF:
            distance = linf;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means predict: distance function id %u not recognized",
                           kmeans_model->distance_function_id)));
    }
    
    PCentroid *current_centroid = nullptr;
    PCentroid *closest_centroid = nullptr;
    auto input_point_pg_array = DatumGetArrayTypeP(data[0]);
    auto min_distance = DBL_MAX;
    double local_distance = 0.;
    double const *input_point_coordinates = nullptr;
    int32_t closest_centroid_id = -1;
    bool const valid_input = verify_pgarray(input_point_pg_array, dimension);
    bool min_distance_changed = false;
    
    if (unlikely(!valid_input))
        return Int32GetDatum(closest_centroid_id);
    
    input_point_coordinates = reinterpret_cast<double const *>(ARR_DATA_PTR(input_point_pg_array));
    
    for (uint32_t c = 0; c < num_centroids; ++c) {
        current_centroid = kmeans_model->centroids + c;
        local_distance = distance(input_point_coordinates, current_centroid->coordinates, dimension);
        min_distance_changed = local_distance < min_distance;
        min_distance = min_distance_changed ? local_distance : min_distance;
        closest_centroid = min_distance_changed ? current_centroid : closest_centroid;
    }
    
    closest_centroid_id = closest_centroid->id;
    
    /*
     * for the time being there is no other way to get to the computed distance other than by a log
     */
    return Int32GetDatum(closest_centroid_id);
}

/*
 * this is a faster version of construct_md_array in which we use knowledge we have
 * to speed up computations
 */
ArrayType *construct_empty_md_array(uint32_t const num_centroids, uint32_t const dimension)
{
    ArrayType *result = NULL;
    int32_t ndims = 0;
    uint32_t dims[2] = {0U};
    uint32_t const lbs[2] = {1U, 1U};
    
    if (num_centroids == 0) {
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("construct_empty_md_array: first dimension must be larger than 0")));
    } else if (num_centroids == 1) {
        ndims = 1;
        dims[0] = {dimension};
    } else {
        ndims = 2;
        dims[0] = num_centroids;
        dims[1] = dimension;
    }
    check_hyper_bounds(num_centroids, dimension, "num_centroids");
    check_hyper_bounds(num_centroids * dimension, sizeof(float8), "num_centroids");

    int32_t nbytes = num_centroids * dimension * sizeof(float8);
    nbytes += ARR_OVERHEAD_NONULLS(ndims);
    result = reinterpret_cast<ArrayType *>(palloc0(nbytes));
    SET_VARSIZE(result, nbytes);
    result->ndim = ndims;
    result->dataoffset = 0; /* marker for no null bitmap */
    result->elemtype = FLOAT8OID;
    errno_t errorno = EOK;
    errorno = memcpy_s(ARR_DIMS(result), ndims * sizeof(int32_t), dims, ndims * sizeof(int32_t));
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(ARR_LBOUND(result), ndims * sizeof(int32_t), lbs, ndims * sizeof(int32_t));
    securec_check(errorno, "\0", "\0");
    
    return result;
}

/*
 * used in EXPLAIN MODEL
 */
List *kmeans_explain(AlgorithmAPI *self, SerializedModel const *model, Oid return_type)
{
    if (unlikely(!model))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means explain: model cannot be null")));
    
    if (unlikely(model->version != DB4AI_MODEL_V01))
        ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("k-means explain: currently only model V01 is supported")));
    
    List *model_info = nullptr;
    
    auto model_kmeans = reinterpret_cast<ModelKMeansV01 *>(model->raw_data);
    kmeans_set_references(model_kmeans);
    
    uint32_t const actual_num_centroids = model_kmeans->actual_num_centroids;
    uint32_t const dimension = model_kmeans->dimension;
    
    TrainingInfo *actual_num_centroids_info = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
    TrainingInfo *num_iterations_info = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
    
    actual_num_centroids_info->name = "Actual number of centroids";
    num_iterations_info->name = "Number of iterations";
    
    actual_num_centroids_info->type = INT4OID;
    num_iterations_info->type = INT4OID;
    
    actual_num_centroids_info->value = Int32GetDatum(model_kmeans->actual_num_centroids);
    num_iterations_info->value = Int32GetDatum(model_kmeans->num_actual_iterations);
    
    model_info = lappend(model_info, actual_num_centroids_info);
    model_info = lappend(model_info, num_iterations_info);
    
    PCentroid *current_wh_centroid = nullptr;
    ArrayType *centroid_coordinates_array = nullptr;
    double *centroid_coordinates_data = nullptr;
    uint32_t const size_centroid_bytes = sizeof(double) * dimension;
    error_t errorno = EOK;
    for (uint32_t current_centroid_idx = 0; current_centroid_idx < actual_num_centroids; ++current_centroid_idx) {
        current_wh_centroid = model_kmeans->centroids + current_centroid_idx;
        /*
         * opening and closing the eigenvector group
         * if both open_group and close_group are set, they will be ignored)
         */
        TrainingInfo *centroid_open_group = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        centroid_open_group->open_group = true;
        TrainingInfo *centroid_close_group = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        centroid_close_group->close_group = true;
        
        // these define the properties of a centroid
        TrainingInfo *centroid_id = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_coordinates = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_loss_function = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_max_distance = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_min_distance = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_avg_distance = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_std_dev_distance = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
        TrainingInfo *centroid_cluster_size = (TrainingInfo *)palloc0(sizeof(TrainingInfo));
    
        // opening and closing a group must have exactly the same name (otherwise fire!)
        centroid_open_group->name = "Centroid";
        centroid_close_group->name = "Centroid";
        
        // the name of each property
        centroid_id->name = "ID";
        centroid_coordinates->name = "Coordinates";
        centroid_loss_function->name = "Objective function";
        centroid_max_distance->name = "Maximum cluster distance";
        centroid_min_distance->name = "Minimum cluster distance";
        centroid_avg_distance->name = "Average cluster distance";
        centroid_std_dev_distance->name = "Standard deviation of cluster distances";
        centroid_cluster_size->name = "Cluster size";
        
        // the type of each property
        centroid_id->type = INT4OID;
        centroid_coordinates->type = FLOAT8ARRAYOID;
        centroid_loss_function->type = FLOAT8OID;
        centroid_max_distance->type = FLOAT8OID;
        centroid_min_distance->type = FLOAT8OID;
        centroid_avg_distance->type = FLOAT8OID;
        centroid_std_dev_distance->type = FLOAT8OID;
        centroid_cluster_size->type = INT8OID;
        
        // the coordinates of a centroid
        centroid_coordinates_array = construct_empty_md_array(1, dimension);
        centroid_coordinates_data = reinterpret_cast<double *>(ARR_DATA_PTR(centroid_coordinates_array));
        
        // filling in the data
        centroid_id->value = Int32GetDatum(current_wh_centroid->id);
        centroid_coordinates->value = PointerGetDatum(centroid_coordinates_array);
        centroid_loss_function->value = Float8GetDatumFast(current_wh_centroid->objective_function);
        centroid_max_distance->value = Float8GetDatumFast(current_wh_centroid->max_distance);
        centroid_min_distance->value = Float8GetDatumFast(current_wh_centroid->min_distance);
        centroid_avg_distance->value = Float8GetDatumFast(current_wh_centroid->avg_distance);
        centroid_std_dev_distance->value = Float8GetDatumFast(current_wh_centroid->std_dev_distance);
        centroid_cluster_size->value = Int64GetDatumFast(current_wh_centroid->cluster_size);
        
        // filling in the coordinates
        errorno = memcpy_s(centroid_coordinates_data, size_centroid_bytes, current_wh_centroid->coordinates,
                           size_centroid_bytes);
        securec_check(errorno, "\0", "\0");
        
        /*
         * appending the properties to the list of properties
         * OBSERVE that open and close elements must be well positioned! (at the beginning and end of the information)
         */
        model_info = lappend(model_info, centroid_open_group);
        model_info = lappend(model_info, centroid_id);
        model_info = lappend(model_info, centroid_cluster_size);
        model_info = lappend(model_info, centroid_coordinates);
        model_info = lappend(model_info, centroid_loss_function);
        model_info = lappend(model_info, centroid_max_distance);
        model_info = lappend(model_info, centroid_min_distance);
        model_info = lappend(model_info, centroid_avg_distance);
        model_info = lappend(model_info, centroid_std_dev_distance);
        model_info = lappend(model_info, centroid_close_group);
    }
    
    return model_info;
}

KMeans kmeans = {
    // AlgorithmAPI
    {KMEANS, "kmeans", ALGORITHM_ML_UNSUPERVISED | ALGORITHM_ML_RESCANS_DATA, kmeans_metrics_loss,
     kmeans_get_hyperparameters, kmeans_make_hyperparameters, kmeans_update_hyperparameters, kmeans_create, kmeans_run,
     kmeans_end, kmeans_predict_prepare, kmeans_predict, kmeans_explain},
};