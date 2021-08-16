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

nodeKMeans.cpp
      Functions related to the k-means operator

IDENTIFICATION
    src/gausskernel/runtime/executor/nodeKMeans.cpp

---------------------------------------------------------------------------------------
**/

#include <random>

#include "executor/nodeKMeans.h"
#include "funcapi.h"
#include "utils/array.h"

#include "db4ai/fp_ops.h"
#include "db4ai/distance_functions.h"
#include "db4ai/db4ai_cpu.h"

/*
 * these functions are defined in kmeans.cpp and are not publicly exposed (under the public include directories)
 */
extern void compute_cost_and_weights(List const* centroids, GSPoint const* points, uint32_t dimension,
                                     uint32_t num_slots, double* cost);

extern List* kmeanspp(KMeansStateDescription* description, List* centroids_candidates,
                      uint32_t idx_current_centroids, uint32_t size_centroid_bytes, std::mt19937_64* prng);

extern bool init_kmeans(KMeansStateDescription* description, double* bbox_min, double* bbox_max,
                        GSPoint const* batch, uint32_t num_slots, uint32_t size_centroid_bytes);

extern bool copy_slot_coordinates_to_array(GSPoint* coordinates, TupleTableSlot const* slot,
                                           uint32_t dimension);

extern bool closest_centroid(List const* centroids, GSPoint const* point, uint32_t dimension, double* distance);

extern void update_centroids(KMeansStateDescription* description, GSPoint* slots, uint32_t num_slots,
                             uint32_t idx_current_centroids, uint32_t idx_next_centroids);

extern void reset_weights(List const* centroids);

extern void reset_centroids(KMeansStateDescription* description, uint32_t idx_centroids,
                            uint32_t size_centroid_bytes);

extern void merge_centroids(KMeansStateDescription* description, uint32_t idx_current_centroids,
                            uint32_t idx_next_centroids, uint32_t size_centroid_bytes);

extern void compute_cost(KMeansStateDescription* description, uint32_t idx_current_centroids);

uint32_t constexpr MAX_BATCH_SLOTS = 100000U;
uint32_t constexpr NUM_ITERATIONS_KMEANSBB = 10U;

/*
 * internally, the operator works in stages, this enum identifies each one of them
 */
enum AlgoStage : uint32_t {
    KMEANS_INIT = 0,
    KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE,
    KMEANS_INITIAL_CENTROIDS_BB_SAMPLE,
    KMEANS_INITIAL_CENTROIDS_BB_COMPUTE_COST,
    KMEANS_LLOYD
};

/*
 * this is a faster version of construct_md_array in which we use knowledge we have
 * on the centroids to speed up computations
 */
force_inline ArrayType* construct_empty_centroids_array(uint32_t const num_centroids, uint32_t const dimension) {
    ArrayType* result = NULL;
    int32_t const ndims = 2;
    uint32_t const dims[2] = {num_centroids, dimension};
    uint32_t const lbs[2] = {1U, 1U};
    int32_t nbytes = num_centroids * dimension * sizeof(float8);
    
    nbytes += ARR_OVERHEAD_NONULLS(ndims);
    result = reinterpret_cast<ArrayType*>(palloc0(nbytes));
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
 * a batch is used only once and thus the data it points to is
 * released after it has been used
 */
force_inline void release_batch(GSPoint* batch, uint32_t const num_slots) {
    GSPoint* point = nullptr;
    for (uint32_t current_slot = 0; current_slot < num_slots; ++current_slot) {
        point = batch + current_slot;
        if (likely(point->should_free)) {
            pfree(point->pg_coordinates);
            point->pg_coordinates = nullptr;
            point->should_free = false;
        }
    }
}

bool deal_sample(bool const sample, std::mt19937_64* prng, GSPoint* batch, uint32_t current_slot,
                 TupleTableSlot* scan_slot, uint32_t const dimension, bool first_candidate,
                 List* centroid_candidates, double local_sample_probability, AlgoStage stage)
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
 * this is the work horse of the whole algorithm. in here we do a lot of things depending on the stage
 * of the algorithm. this function does a complete (single) pass over the data. the upper layer
 * calls this function multiple times depending the stage of the algorithm
 */
List* one_data_pass(PlanState* outer_plan, KMeansStateDescription* state_description, uint32_t const batch_size,
                    GSPoint* batch, uint32_t const idx_current_centroids, uint32_t const idx_next_centroids,
                    uint32_t const size_centroid_bytes, bool const sample, double const sample_probability,
                    AlgoStage stage, List* centroid_candidates, double* cost_centroid_candidates,
                    std::mt19937_64* prng)
{
    bool plan_exhausted = false;
    bool result = false;
    bool first_candidate = centroid_candidates == nullptr;
    uint32_t current_slot = 0;
    uint32_t const dimension = state_description->dimension;
    uint32_t const num_centroids = state_description->num_centroids;
    uint32_t num_elements_round = 0;
    uint32_t slot_number = 0;
    uint32_t valid_row = 0;
    TupleTableSlot* scan_slot = nullptr;
    auto bbox_min = reinterpret_cast<double*>(state_description->bbox_min);
    auto bbox_max = reinterpret_cast<double*>(state_description->bbox_max);
    double local_sample_probability = sample_probability;
    double cost_of_batch = 0.;
    GSPoint* centroid_candidate = nullptr;
    
    while (!plan_exhausted) {
        current_slot = 0;
        /* we produce a batch of slots to be passed to the algorithm */
        while (current_slot < batch_size) {
            scan_slot = ExecProcNode(outer_plan);
            ++slot_number;
            // every slot will have its own chances
            local_sample_probability = sample_probability;
            /*
             * we get out of the whole thing if we have exhausted the relation or
             * we have found our k centroids.
             * if we were not able to sample the k centroids, the upper call
             * will perform runs until we have done so
             */
            if (unlikely(TupIsNull(scan_slot) || state_description->current_centroid == num_centroids)) {
                plan_exhausted = true;
                break;
            }
            
            /*
             * we jump over rows with empty coordinates
             */
            if (unlikely(scan_slot->tts_isnull[0]))
                continue;
            
            if (deal_sample(sample, prng, batch, current_slot, scan_slot, dimension, first_candidate,
                            centroid_candidates, local_sample_probability, stage) == true)
                continue;
            
            if ((stage == KMEANS_INITIAL_CENTROIDS_RANDOM_SAMPLE) || (stage == KMEANS_INITIAL_CENTROIDS_BB_SAMPLE)) {
                /*
                 * we only know the expected number of centroid candidates that we will produce
                 * but not the exact number. thus we allocate each one of them on demand
                 * (observe that we cannot use the batch structure because the number of
                 * candidates we generate can be much larger than the size of a batch)
                 */
                centroid_candidate = reinterpret_cast<GSPoint*>(palloc0(sizeof(GSPoint)));
                
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
                
                heap_slot_clear(scan_slot);
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
                init_kmeans(state_description, bbox_min, bbox_max, batch, current_slot, size_centroid_bytes);
                
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
 * this function encodes the internal representation of the model in a form that the upper layer
 * can easily process (virtual tuple)
 */
bool kmeans_get_tupleslot(KMeansState* kmeans_state, TupleTableSlot* slot, TupleDesc tuple_desc) {
    auto kmeans_node = reinterpret_cast<KMeans*>(kmeans_state->sst.ps.plan);
    uint32_t const num_centroids = kmeans_node->parameters.num_centroids;
    // this precision for num_points is enough for practical purposes
    uint32_t const num_points = kmeans_state->description.num_good_points;
    uint32_t const actual_num_centroids = num_centroids > num_points ? num_points : num_centroids;
    uint32_t current_position = 0;
    uint32_t const dimension = kmeans_state->description.dimension;
    uint32_t idx_current_centroids = 1U - (kmeans_state->description.current_iteration & 1U);
    uint32_t const size_centroid_bytes = sizeof(float8) * dimension;
    uint32_t centroid_coordinates_offset = 0U;
    Centroid* current_centroid = nullptr;
    /* these are the outer-facing arrays */
    ArrayType* centroid_ids = nullptr;
    ArrayType* centroid_coordinates = nullptr;
    ArrayType* objective_functions = nullptr;
    ArrayType* avg_distances = nullptr;
    ArrayType* min_distances = nullptr;
    ArrayType* max_distances = nullptr;
    ArrayType* std_dev_distances = nullptr;
    ArrayType* cluster_sizes = nullptr;
    /* these are the inner-facing arrays */
    int32_t* centroid_ids_data = nullptr;
    double* centroid_coordinates_data = nullptr;
    double* objective_functions_data = nullptr;
    double* avg_distances_data = nullptr;
    double* min_distances_data = nullptr;
    double* max_distances_data = nullptr;
    double* std_dev_distances_data = nullptr;
    int64_t* cluster_sizes_data = nullptr;
    uint64_t cluster_size = 0ULL;
    Datum* values = slot->tts_values;
    bool* nulls = slot->tts_isnull;
    errno_t errorno = EOK;
    
    /*
     * the descriptor is not reset
     */
    (void)ExecClearTuple(slot);
    
    Assert(tuple_desc->natts == NUM_ATTR_OUTPUT);
    
    /* there is nothing to output any more */
    if (kmeans_state->description.current_centroid >= actual_num_centroids)
        return false;
    
    auto datums_placeholder = reinterpret_cast<Datum*>(palloc0(sizeof(Datum) * actual_num_centroids));
    
    /*
     * we allocate all arrays in one shot and fill them as we process valid centroids
     */
    centroid_ids = construct_array(datums_placeholder, actual_num_centroids, INT4OID, sizeof(int4), true, 'i');
    objective_functions =
            construct_array(datums_placeholder, actual_num_centroids, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    avg_distances =
            construct_array(datums_placeholder, actual_num_centroids, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    min_distances =
            construct_array(datums_placeholder, actual_num_centroids, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    max_distances =
            construct_array(datums_placeholder, actual_num_centroids, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    std_dev_distances =
            construct_array(datums_placeholder, actual_num_centroids, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    cluster_sizes =
            construct_array(datums_placeholder, actual_num_centroids, INT8OID, sizeof(int64_t), FLOAT8PASSBYVAL, 'i');
    /*
     * this one is the 2-dimensional array that will hold the coordinates of the centroids
     * at this point the data of this array is zeroed
     */
    centroid_coordinates = construct_empty_centroids_array(actual_num_centroids, dimension);
    
    /*
     * we now obtain pointers to the actual raw arrays
     */
    centroid_ids_data = reinterpret_cast<int32_t*>(ARR_DATA_PTR(centroid_ids));
    centroid_coordinates_data = reinterpret_cast<double*>(ARR_DATA_PTR(centroid_coordinates));
    objective_functions_data = reinterpret_cast<double*>(ARR_DATA_PTR(objective_functions));
    avg_distances_data = reinterpret_cast<double*>(ARR_DATA_PTR(avg_distances));
    min_distances_data = reinterpret_cast<double*>(ARR_DATA_PTR(min_distances));
    max_distances_data = reinterpret_cast<double*>(ARR_DATA_PTR(max_distances));
    std_dev_distances_data = reinterpret_cast<double*>(ARR_DATA_PTR(std_dev_distances));
    cluster_sizes_data = reinterpret_cast<int64_t*>(ARR_DATA_PTR(cluster_sizes));
    
    /*
     * we go through the centroids until we have exhausted all (valid) centroids
     */
    current_position = 0;
    while (current_position < actual_num_centroids) {
        current_centroid =
                kmeans_state->description.centroids[idx_current_centroids] + current_position;
        
        cluster_size = current_centroid->statistics.getPopulation();
        
        centroid_ids_data[current_position] = current_centroid->id;
        objective_functions_data[current_position] = current_centroid->statistics.getTotal();
        avg_distances_data[current_position] = current_centroid->statistics.getEmpiricalMean();
        min_distances_data[current_position] =
                cluster_size > 0 ? current_centroid->statistics.getMin() : 0.;
        max_distances_data[current_position] = current_centroid->statistics.getMax();
        std_dev_distances_data[current_position] = current_centroid->statistics.getEmpiricalStdDev();
        cluster_sizes_data[current_position] = cluster_size;
        errorno = memcpy_s(centroid_coordinates_data + centroid_coordinates_offset, size_centroid_bytes,
                           ARR_DATA_PTR(current_centroid->coordinates), size_centroid_bytes);
        securec_check(errorno, "\0", "\0");
        centroid_coordinates_offset += dimension;
        ++current_position;
    }
    
    kmeans_state->description.current_centroid = actual_num_centroids;
    
    /*
     * Processing has finished and we can set the values accordingly
     */
    values[0] = PointerGetDatum(centroid_ids);
    values[1] = PointerGetDatum(centroid_coordinates);
    values[2] = PointerGetDatum(objective_functions);
    values[3] = PointerGetDatum(avg_distances);
    values[4] = PointerGetDatum(min_distances);
    values[5] = PointerGetDatum(max_distances);
    values[6] = PointerGetDatum(std_dev_distances);
    values[7] = PointerGetDatum(cluster_sizes);
    values[8] = UInt64GetDatum(kmeans_state->description.num_good_points);
    values[9] = UInt64GetDatum(kmeans_state->description.num_dead_points);
    values[10] = Float8GetDatumFast(kmeans_state->description.seeding_time);
    values[11] = Float8GetDatumFast(kmeans_state->description.execution_time);
    values[12] = UInt32GetDatum(kmeans_state->description.actual_num_iterations);
    values[13] = UInt32GetDatum(current_position);
    values[14] = UInt64GetDatum(kmeans_node->parameters.external_seed);
    /* no null attribute */
    memset_s(nulls, sizeof(bool) * NUM_ATTR_OUTPUT, 0, sizeof(bool) * NUM_ATTR_OUTPUT);
    
    /*
     * this saves one round of copying
     */
    ExecStoreVirtualTuple(slot);
    
    pfree(datums_placeholder);
    
    return true;
}

/*
 * this function initializes the operator
 */
KMeansState* ExecInitKMeans(KMeans* kmeans_node, EState* estate, int eflags) {
    KMeansState* kmeans_state = nullptr;
    Plan* outer_plan = outerPlan(kmeans_node);
    TupleDesc tup_desc_out;
    uint32_t const num_centroids = kmeans_node->parameters.num_centroids;
    uint32_t const dimension = kmeans_node->description.n_features;
    uint16_t current_attr = 0;
    
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    
    /*
     * create state structure
     */
    kmeans_state = makeNode(KMeansState);
    kmeans_state->sst.ps.plan = reinterpret_cast<Plan*>(kmeans_node);
    kmeans_state->sst.ps.state = estate;
    
    /*
     * initialize child nodes
     */
    outerPlanState(kmeans_state) = ExecInitNode(outer_plan, estate, eflags);
    
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &kmeans_state->sst.ps);
    ExecInitScanTupleSlot(estate, &kmeans_state->sst);
    
    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignScanTypeFromOuterPlan(&kmeans_state->sst);
    
    /*
     * we have to produce the record to be returned
     */
    tup_desc_out = CreateTemplateTupleDesc(NUM_ATTR_OUTPUT, false);
    
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "centroid_ids", INT4ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "centroid_coordinates", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "objective_functions", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "avg_distances", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "min_distances", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "max_distances", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "std_dev_distances", FLOAT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "cluster_sizes", INT8ARRAYOID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "good_points", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "bad_points", INT8OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "seeding_time", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "execution_time", FLOAT8OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "actual_number_iterations", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "actual_number_centroids", INT4OID, -1, 0);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)++current_attr, "seed", INT8OID, -1, 0);
    
    BlessTupleDesc(tup_desc_out);
    
    ExecAssignResultType(&kmeans_state->sst.ps, tup_desc_out);
    
    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignProjectionInfo(&kmeans_state->sst.ps, nullptr);
    kmeans_state->sst.ps.ps_TupFromTlist = false;
    kmeans_state->sst.ps.ps_ProjInfo = nullptr;
    
    kmeans_state->done = false;
    
    kmeans_state->description.centroids[0] =
            reinterpret_cast<Centroid*>(palloc0(sizeof(Centroid) * num_centroids));
    kmeans_state->description.centroids[1] =
            reinterpret_cast<Centroid*>(palloc0(sizeof(Centroid) * num_centroids));
    
    auto datums_tmp = reinterpret_cast<Datum*>(palloc0(sizeof(Datum) * dimension));
    
    for (uint32_t c = 0; c < num_centroids; ++c) {
        kmeans_state->description.centroids[0][c].id = c + 1;
        kmeans_state->description.centroids[1][c].id = c + 1;
        /*
         * this is an internal array that will eventually hold the coordinates of a centroid
         * its representation as a PG Array is legacy. It will be better to represent it as
         * an array of double directly (in a manner that can be returned to the client directly)
         */
        kmeans_state->description.centroids[0][c].coordinates =
                construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
        kmeans_state->description.centroids[1][c].coordinates =
                construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    }
    
    /*
     * general running time information of the operator
     */
    kmeans_state->description.bbox_max =
            construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    kmeans_state->description.bbox_min =
            construct_array(datums_tmp, dimension, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');
    kmeans_state->description.num_good_points = 0;
    kmeans_state->description.num_dead_points = 0;
    kmeans_state->description.current_iteration = 0;
    kmeans_state->description.current_centroid = 0;
    kmeans_state->description.dimension = dimension;
    kmeans_state->description.num_centroids = num_centroids;
    
    switch (kmeans_node->description.distance) {
        case KMEANS_L1:
            kmeans_state->description.distance = l1;
            break;
        case KMEANS_L2:
            kmeans_state->description.distance = l2;
            break;
        case KMEANS_LINF:
            kmeans_state->description.distance = linf;
            break;
        case KMEANS_L2_SQUARED:
            kmeans_state->description.distance = l2_squared;
            break;
        default:
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means init: no known distance function: %u", kmeans_node->description.distance)));
    }
    
    pfree(datums_tmp);
    
    return kmeans_state;
}

void kmeans_deal_seeding_function(SeedingFunction seeding_function, uint32_t num_centroids, uint64_t num_points,
                                  double sample_factor, uint32_t num_centroids_orig, Verbosity verbosity,
                                  List** centroid_candidates_kmeans_bb, PlanState* outer_plan,
                                  KMeansStateDescription* state_description, uint32_t const batch_size,
                                  GSPoint* batch, uint32_t idx_current_centroids,
                                  uint32_t idx_next_centroids, uint32_t const size_centroid_bytes,
                                  std::mt19937_64 *prng)
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
                    ExecReScan(outer_plan);
                    *centroid_candidates_kmeans_bb = one_data_pass(outer_plan, state_description, batch_size,
                                                                    batch, idx_current_centroids,
                                                                    idx_next_centroids, size_centroid_bytes,
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
                ExecReScan(outer_plan);
                *centroid_candidates_kmeans_bb = one_data_pass(outer_plan, state_description, batch_size,
                                                                batch, idx_current_centroids,
                                                                idx_next_centroids, size_centroid_bytes,
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
                ExecReScan(outer_plan);
                *centroid_candidates_kmeans_bb = one_data_pass(outer_plan, state_description, batch_size, batch,
                                                                idx_current_centroids, idx_next_centroids,
                                                                size_centroid_bytes, true, sample_probability,
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

static void output_kmeans_state(Verbosity const verbosity, uint64_t num_good_points, uint64_t num_dead_points,
                                double local_elapsed_time)
{
    if (verbosity > NO_OUTPUT) {
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Initial statistics gathered:")));
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Number of valid points: %lu", num_good_points)));
        ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("*** Number of dead points:  %lu", num_dead_points)));
        if (verbosity == VERBOSE_OUTPUT)
            ereport(NOTICE, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("*** Duration (s):           %0.6lf", local_elapsed_time)));
    }
}

/*
 * this executes the k-means "training" by executing multiple scans through the data
 * until convergence can be declared.
 * the operator works in stages:
 * 1) obtain general statistics about the input data (one data pass),
 * 2) execute a seeding method (random++ or kmeans||) (at least one data pass but not more than 10),
 * 3) run Lloyd's algorithm (at least one data pass)
 */
TupleTableSlot* ExecKMeans(KMeansState* kmeans_state_node)
{
    /*
     * get information from the node
     */
    auto kmeans_node = reinterpret_cast<KMeans*>(kmeans_state_node->sst.ps.plan);
    KMeansStateDescription& state_description = kmeans_state_node->description;
    EState* estate = kmeans_state_node->sst.ps.state;
    ScanDirection direction = estate->es_direction;
    PlanState* outer_plan = outerPlanState(kmeans_state_node);
    TupleTableSlot* kmeans_slot = nullptr;
    TupleDesc tup_desc_scan = nullptr;
    TupleDesc tup_desc_kmeans = ExecGetResultType(&kmeans_state_node->sst.ps);
    GSPoint* batch = nullptr;
    uint32_t const max_num_iterations = kmeans_node->parameters.num_iterations;
    uint32_t idx_current_centroids = 0U;
    uint32_t idx_next_centroids = 0U;
    uint32_t num_centroids = state_description.num_centroids;
    uint32_t const num_centroids_orig = num_centroids;
    uint32_t const dimension = state_description.dimension;
    uint32_t const size_centroid_bytes = sizeof(double) * dimension;
    uint32_t const one_gb = 1ULL << 30;
    uint32_t const max_batch_size = (one_gb - (sizeof(void*) * MAX_BATCH_SLOTS)) / size_centroid_bytes;
    uint32_t const batch_size = kmeans_node->description.batch_size < max_batch_size ?
                                kmeans_node->description.batch_size : max_batch_size;
    Verbosity const verbosity = kmeans_node->description.verbosity;
    SeedingFunction const seeding_function = kmeans_node->description.seeding;
    IncrementalStatistics* prev_solution_statistics = nullptr;
    IncrementalStatistics* current_solution_statistics = nullptr;
    double cost_fraction = 0.;
    double cost_fraction_correction = 0.;
    double local_elapsed_time = 0.;
    double total_time = 0.;
    double const sample_factor = num_centroids < 1000000 ? 4. : 2.;
    double const tolerance = kmeans_node->parameters.tolerance;
    List* centroid_candidates_kmeans_bb = nullptr;
    struct timespec start_time, finish_time, start_kmeans_round, finish_kmeans_round;
    uint64_t num_points = 0ULL;
    uint64_t seed = kmeans_node->parameters.external_seed;
    /* if the user-provided seed is 0 we take the current time but reset
     * the higher order bits to be able to return this seed to the user
     * as an int32_t so that the user can reproduce the run
     * (observe that epoch 2^31 is around year 2038 and the shifts are
     * mostly useless)
     */
    if (seed == 0ULL)
        kmeans_node->parameters.external_seed = seed = (get_time_ms() << 33U) >> 33U;
    
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
    if (!kmeans_state_node->done) {
        /*
         * Want to scan subplan in the forward direction while feeding rows to
         * the algorithm
         */
        estate->es_direction = ForwardScanDirection;
        
        tup_desc_scan = ExecGetResultType(outer_plan);
        
        /*
         * we have to see that the column we are passed on is of type array
         */
        Oid oidtype = tup_desc_scan->attrs[0]->atttypid;
        
        if (unlikely((oidtype != FLOAT8ARRAYOID)))
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: data is not of type float8 (double precision) array")));
        
        if (tup_desc_scan->natts != 1)
            ereport(ERROR, (errmodule(MOD_DB4AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("k-means exec: relation should contain only a single attribute "
                           "(point coordinates in a double precision array)")));
        /*
         * this is the array that will contain points in memory to be processed in a batch
         */
        batch = reinterpret_cast<GSPoint*>(palloc0(sizeof(GSPoint) * batch_size));
        
        /*
         * Before iterating we have to find an initial set of centroids (seeds)
         * either random seeds or using kmeans||, this is one table scan on which we
         * can also obtain the diameter of the bounding box (for normalization for example)
         * from this scan we can also obtain the coordinates of the bounding box
         */
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        one_data_pass(outer_plan, &state_description, batch_size, batch,
                      0, 1, size_centroid_bytes, false, 0., KMEANS_INIT, nullptr, nullptr, nullptr);
        clock_gettime(CLOCK_MONOTONIC, &finish_time);
        
        local_elapsed_time = static_cast<double>(finish_time.tv_sec - start_time.tv_sec);
        local_elapsed_time += static_cast<double>(finish_time.tv_nsec - start_time.tv_nsec) / 1000000000.0;
        total_time += local_elapsed_time;

        output_kmeans_state(verbosity, state_description.num_good_points, state_description.num_dead_points,
                            local_elapsed_time);

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
                                     verbosity, &centroid_candidates_kmeans_bb, outer_plan, &state_description,
                                     batch_size, batch, idx_current_centroids, idx_next_centroids,
                                     size_centroid_bytes, &prng);
        
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
                                                 idx_current_centroids, size_centroid_bytes, &prng);
        
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
            reset_centroids(&state_description, idx_next_centroids, size_centroid_bytes);
            
            /*
             * every iteration we have to reset the sub-plan to be able to re-scan it
             */
            ExecReScan(outer_plan);
            one_data_pass(outer_plan, &state_description, batch_size, batch, idx_current_centroids,
                          idx_next_centroids, size_centroid_bytes, false, 0., KMEANS_LLOYD, nullptr, nullptr, nullptr);
            
            /*
             * let's produce the new set of centroids for the next iteration
             * (this should be executed at the coordinator in a distributed environment
             */
            merge_centroids(&state_description, idx_current_centroids, idx_next_centroids, size_centroid_bytes);
            
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
        
        /* Finish scanning the subplan, it's safe to early free the memory of lefttree */
        ExecEarlyFree(outer_plan);
        
        /*
         * restore to user specified direction
         */
        estate->es_direction = direction;
        
        /*
         * finally set the finished flag to true
         */
        kmeans_state_node->done = true;
        
        /*
         * we restore the original number of centroids
         */
        state_description.num_centroids = num_centroids_orig;
        
        pfree(batch);
    }
    
    kmeans_slot = kmeans_state_node->sst.ps.ps_ResultTupleSlot;
    (void)kmeans_get_tupleslot(kmeans_state_node, kmeans_slot, tup_desc_kmeans);
    
    return kmeans_slot;
}

/*
 * this function frees up all the storage required by the algorithm
 * at thus point the upper layer has made persistent the model and
 * this information is not required anymore
 */
void ExecEndKMeans(KMeansState* kmeans_state_node) {
    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(kmeans_state_node->sst.ps.ps_ResultTupleSlot);
    
    /*
     * clean up subtrees
     */
    auto kmeans_node = reinterpret_cast<KMeans*>(kmeans_state_node->sst.ps.plan);
    
    ExecEndNode(outerPlanState(kmeans_state_node));
    for (uint32_t c = 0; c < kmeans_node->parameters.num_centroids; ++c) {
        pfree(kmeans_state_node->description.centroids[0][c].coordinates);
        pfree(kmeans_state_node->description.centroids[1][c].coordinates);
    }
    
    pfree(kmeans_state_node->description.centroids[0]);
    pfree(kmeans_state_node->description.centroids[1]);
    pfree(kmeans_state_node);
}
