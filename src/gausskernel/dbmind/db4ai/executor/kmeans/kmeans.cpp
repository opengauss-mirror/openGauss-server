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

#include "db4ai/fp_ops.h"
#include "db4ai/distance_functions.h"
#include "db4ai/model_warehouse.h"
#include "db4ai/predict_by.h"
#include "db4ai/db4ai_cpu.h"

/*
 * to verify that the pg array we get complies with what we expect
 * we discard entries that do not pass the test because we cannot start
 * much (consistent) with them anyway
 */
bool verify_pgarray(ArrayType const * pg_array, int32_t n)
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
bool copy_slot_coordinates_to_array(GSPoint *coordinates, TupleTableSlot const * slot, uint32_t const dimension)
{
    if (unlikely((slot == nullptr) or (coordinates == nullptr)))
        return false;

    /*
     * we obtain the coordinates of the current point (function call incurs in detoasting
     * and thus memory is allocated and we have to free it once we don't need it
     */
    ArrayType *current_point_pgarray = DatumGetArrayTypePCopy(slot->tts_values[0]);
    bool const valid_point = verify_pgarray(current_point_pgarray, dimension);
    bool release_point = PointerGetDatum(current_point_pgarray) != slot->tts_values[0];

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
 * this sets the weights of a set of candidates to 1 (every point is the centroid of itself)
 */
void reset_weights(List const * centroids)
{
    ListCell const * current_centroid_cell = centroids ? centroids->head : nullptr;
    GSPoint *centroid = nullptr;

    for (; current_centroid_cell != nullptr; current_centroid_cell = lnext(current_centroid_cell)) {
        centroid = reinterpret_cast<GSPoint *>(lfirst(current_centroid_cell));
        centroid->weight = 1U;
    }
}

/*
 * given a set of centroids (as a PG list) and a point, this function compute the distance to the closest
 * centroid
 */
bool closest_centroid(List const * centroids, GSPoint const * point, uint32_t const dimension, double *distance)
{
    ListCell const * current_centroid_cell = centroids ? centroids->head : nullptr;
    GSPoint *centroid = nullptr;
    GSPoint *closest_centroid_ptr = nullptr;
    bool result = false;
    bool min_distance_changed = false;
    double local_distance = 0.;
    auto min_distance = DBL_MAX;
    auto const * point_coordinates = reinterpret_cast<double const *>(ARR_DATA_PTR(point->pg_coordinates));

    for (; current_centroid_cell != nullptr; current_centroid_cell = lnext(current_centroid_cell)) {
        /*
         * low temporal locality for a prefetch for read
         */
        prefetch(lnext(current_centroid_cell), 0, 1);
        centroid = reinterpret_cast<GSPoint *>(lfirst(current_centroid_cell));
        local_distance = l2_squared(point_coordinates,
            reinterpret_cast<double const *>(ARR_DATA_PTR(centroid->pg_coordinates)), dimension);
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

/*
 * given a set of centroids (as a PG list) and a set of points, this function computes
 * the cost of the set of centroids as well as their weights (number of points assigned
 * to each centroid)
 * we assumed that all weights have been reset already
 */
bool compute_cost_and_weights(List const * centroids, GSPoint const * points, uint32_t dimension,
    uint32_t const num_slots, double *cost)
{
    GSPoint const * point = nullptr;
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
 * this runs kmeans++ on a super-set of centroids to obtain the k centroids we want as seeding
 */
List *kmeanspp(KMeansStateDescription *description, List *centroids_candidates, uint32_t const idx_current_centroids,
    uint32_t const size_centroid_bytes, std::mt19937_64 *prng)
{
    Centroid *centroids = description->centroids[idx_current_centroids];
    uint32_t const num_centroids_needed = description->num_centroids;
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
                prev_candidate_cell ? prev_candidate_cell : centroids_candidates ? centroids_candidates->head : nullptr;

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
void reset_centroids(KMeansStateDescription *description, uint32_t const idx_centroids,
    uint32_t const size_centroid_bytes)
{
    uint32_t const num_centroids = description->num_centroids;
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
 * given the running mean of a centroid and a new point, this adds the new point to the aggregate
 * using a sum that provides higher precision (we could provide much higher precision at the cost
 * of allocating yet another array to keep correction terms for every dimension
 */
force_inline void aggregate_point(double *centroid_aggregation, double const * new_point, uint32_t const dimension)
{
    double local_correction = 0;
    for (uint32_t d = 0; d < dimension; ++d) {
        twoSum(centroid_aggregation[d], new_point[d], centroid_aggregation + d, &local_correction);
        centroid_aggregation[d] += local_correction;
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

/*
 * updates the minimum bounding box to contain the new given point
 */
force_inline void update_bbox(double * const bbox_min, double * const bbox_max, double const * point,
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

bool init_kmeans(KMeansStateDescription *description, double *bbox_min, double *bbox_max, GSPoint const * batch,
    uint32_t const num_slots, uint32_t const size_centroid_bytes)
{
    uint32_t const dimension = description->dimension;
    bool const first_run = description->current_iteration == 0;
    uint32_t current_slot = 0;
    GSPoint const * current_point = nullptr;
    double const * current_point_coordinates = nullptr;

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
 * we assume that all slots in the batch are non-null (guaranteed by the upper call)
 * also, that the next set of centroids has been reset previous to the very first call
 */
void update_centroids(KMeansStateDescription *description, GSPoint *slots, uint32_t const num_slots,
    uint32_t const idx_current_centroids, uint32_t const idx_next_centroids)
{
    uint32_t const dimension = description->dimension;
    uint32_t current_slot = 0U;
    uint32_t current_centroid = 0U;
    uint32_t const num_centroids = description->num_centroids;
    uint32_t closest_centroid = 0U;
    GSPoint const * current_point = nullptr;
    double const * current_point_coordinates = nullptr;
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
        local_statistics.setMin(min_dist);
        local_statistics.setMax(min_dist);
        local_statistics.setPopulation(1ULL);
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

void merge_centroids(KMeansStateDescription *description, uint32_t const idx_current_centroids,
    uint32_t const idx_next_centroids, uint32_t const size_centroid_bytes)
{
    uint32_t const num_centroids = description->num_centroids;
    uint32_t const dimension = description->dimension;
    Centroid * const current_centroids = description->centroids[idx_current_centroids];
    Centroid * const next_centroids = description->centroids[idx_next_centroids];
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

bool finish_kmeans()
{
    return true;
}

ModelPredictor kmeans_predict_prepare(Model const * model)
{
    return reinterpret_cast<ModelPredictor>(const_cast<Model *>(model));
}

Datum kmeans_predict(ModelPredictor model, Datum *data, bool *nulls, Oid *types, int32_t nargs)
{
    auto kmeans_model = reinterpret_cast<ModelKMeans *>(model);

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
                errmsg("k-means predict: distance function id %u not recognized", kmeans_model->distance_function_id)));
    }

    WHCentroid *current_centroid = nullptr;
    WHCentroid *closest_centroid = nullptr;
    auto input_point_pg_array = DatumGetArrayTypeP(data[0]);
    auto min_distance = DBL_MAX;
    double local_distance = 0.;
    double const * input_point_coordinates = nullptr;
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
