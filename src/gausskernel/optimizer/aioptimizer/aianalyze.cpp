/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* aianalyze.cpp
*  ABO statistics generator.
*
*
* IDENTIFICATION
*        src/gausskernel/optimizer/aioptimizer/aianalyze.cpp
*
* ---------------------------------------------------------------------------------------
*/
#include "optimizer/aioptimizer.h"
#include "db4ai/db4ai_api.h"
#include "db4ai/bayes.h"
#include "utils/guc.h"
#include "utils/extended_statistics.h"
#include "access/tableam.h"
#include "catalog/gs_model.h"

#define DEFAULT_NUM_MCVS 500
#define DEFAULT_NUM_BINS 500
#define NUM_PARAM_BAYESNET 7
#define DEFAULT_HASH_CAPACITY 100

bool StatsModel::create_model(const char *model_name, HyperparameterValues *hyper, TableScannerSample2 *reader)
{
    int num_params = NUM_PARAM_BAYESNET;
    Hyperparameter hps[num_params];
    int slot = 0;

    hps[slot].name = "edges_of_network";
    hps[slot].type = CSTRINGOID;
    hps[slot].value = CStringGetDatum(hyper->edges);
    slot++;

    hps[slot].name = "num_edges";
    hps[slot].type = INT4OID;
    hps[slot].value = Int32GetDatum(hyper->num_edges);
    slot++;

    hps[slot].name = "num_nodes";
    hps[slot].type = INT4OID;
    hps[slot].value = Int32GetDatum(hyper->num_nodes);
    slot++;

    hps[slot].name = "num_bins";
    hps[slot].type = INT4OID;
    hps[slot].value = Int32GetDatum(hyper->num_bins);
    slot++;

    hps[slot].name = "num_mcvs";
    hps[slot].type = INT4OID;
    hps[slot].value = Int32GetDatum(hyper->num_mcvs);
    slot++;
    
    hps[slot].name = "num_sample_rows";
    hps[slot].type = INT8OID;
    hps[slot].value = Int64GetDatum(hyper->num_sample_rows);
    slot++;
    
    hps[slot].name = "num_total_rows";
    hps[slot].type = INT8OID;
    hps[slot].value = Int64GetDatum(hyper->num_total_rows);

    const Model *model = model_fit(model_name, BAYES_NET_INTERNAL, hps, num_params, reader->get_tuple()->typid,
                    reader->get_tuple()->typbyval, reader->get_tuple()->typlen, reader->get_tuple()->nattrs,
                    &StatsModel::fetch, &StatsModel::rescan, reader);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION) {
        ereport(ERROR,
            (errmsg("Model training failed, error %d\n", model->status)));
        return false;
    }
    model_store(model);

    LWLockAcquire(AboCacheLock, LW_EXCLUSIVE);
    bool found = false;
    AboModelCacheEntry *cache = (AboModelCacheEntry *)hash_search(g_instance.abo_cxt.models, (void *)model_name,
        HASH_REMOVE, &found);
    if (found) {
        pfree_ext(cache->model->bayesNetModel);
        MemoryContextDelete(cache->model->mcontext);
        pfree_ext(cache->model);
    }
    LWLockRelease(AboCacheLock);
    return true;
};

static Oid supported_types[] = {FLOAT4OID, FLOAT8OID, INT16OID, INT8OID, INT4OID, VARCHAROID, BPCHAROID, NUMERICOID, 0};
static const char *supported_typed_readable = "FLOAT8,Double Precision, FlOAT4, REAL, INT16, BIGINT, INTEGER, " \
                                              "VARCHAR, CHARACTER VARYING, CHAR, CHARACTER, NUMERIC";

static bool isSupport(Oid type)
{
    for (size_t i = 0; supported_types[i] > 0; i++) {
        if (type == supported_types[i]) {
            return true;
        }
    }
    return false;
}

/* BayesNet Structure Learning
 * Chow-Liu algo
 */
static void find_maximal_edge(unsigned int nattrs, uint128 visited, uint128 currentindicate, double *likelihood,
                              double *maximal, int *maximal_srcid, int *maximal_destid)
{
    for (unsigned int k = 0; k < nattrs; k++) {
        for (unsigned int j = 0; j < nattrs; j++) {
            double likeli = likelihood[k * nattrs + j];
            if (((visited & (currentindicate << k)) > 0) &&
                ((visited & (currentindicate << j)) == 0) &&
                (likeli > *maximal)) {
                *maximal = likeli;
                *maximal_srcid = k;
                *maximal_destid = j;
            }
        }
    }
}

typedef struct TwoValueInTuple {
    ValueInTuple x;
    ValueInTuple y;
} TwoValueInTuple;

typedef struct TwoValueMapEntry {
    TwoValueInTuple two_value;
    uint32_t cnt;
} TwoValueMapEntry;

uint32 two_value_hash(const void* key, Size keysize)
{
    const TwoValueInTuple *k = (const TwoValueInTuple *)key;
    return k->x.hashval + k->y.hashval;
}

static int two_value_compare(const void *a, const void *b, Size keysize) {
    const TwoValueInTuple *left = (const TwoValueInTuple *)a;
    const TwoValueInTuple *right = (const TwoValueInTuple *)b;
    int first_res = cmp_value_bayesnet<ValueInTuple>(&left->x, &right->x);
    if (first_res == 0) {
        return cmp_value_bayesnet<ValueInTuple>(&left->y, &right->y);
    } else {
        return first_res;
    }
}

static char *structure_learning(TableScannerSample2 *tss, double sample_size)
{
    unsigned int num_attrs = tss->get_tuple()->nattrs;
    double likelihood[num_attrs][num_attrs] = {0};
    double prob_x1x2 = 0.0;
    double prob_x1 = 0.0;
    double prob_x2 = 0.0;
    HASH_SEQ_STATUS seq;

    HTAB **single_col_freqs = (HTAB **)palloc0(sizeof(HTAB *) * num_attrs);
    for (unsigned int i = 0; i < num_attrs; i++) {
        HASHCTL hash_ctl;
        int rc;
        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");
        hash_ctl.keysize = sizeof(ValueInTuple);
        hash_ctl.entrysize = sizeof(ValueMapEntry);
        hash_ctl.hash = value_hash;
        hash_ctl.match = (HashCompareFunc)value_compare;
        hash_ctl.hcxt = CurrentMemoryContext;
        single_col_freqs[i] = hash_create("single_col_freqs", DEFAULT_HASH_CAPACITY, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
    }

    TupleScan *tup = nullptr;
    while ((tup = tss->fetch()) != nullptr) {
        for (unsigned int i = 0; i < num_attrs; i++) {
            bool found = false;
            ValueInTuple val = create_value(tup->values[i], tup->typid[i], tup->isnull[i]);
            ValueMapEntry *entry = (ValueMapEntry *)hash_search(single_col_freqs[i],
                                                                &val, HASH_ENTER, &found);
            if (found) {
                entry->cnt++;
            } else {
                entry->value = val;
                entry->cnt = 1;
            }
        }
    }
    tss->rescan();
    for (uint32 left = 0; left < num_attrs; left++) {
        for (uint32 right = left + 1; right < num_attrs; right++) {
            HASHCTL hash_ctl;
            int rc;
            rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
            securec_check(rc, "\0", "\0");
            hash_ctl.keysize = sizeof(TwoValueInTuple);
            hash_ctl.entrysize = sizeof(TwoValueMapEntry);
            hash_ctl.hash = two_value_hash;
            hash_ctl.match = (HashCompareFunc)two_value_compare;
            hash_ctl.hcxt = CurrentMemoryContext;
            HTAB *multi_col_freqs = hash_create("multi_col_freqs", DEFAULT_HASH_CAPACITY, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
            while ((tup = tss->fetch()) != nullptr) {
                ValueInTuple left_t = create_value(tup->values[left], tup->typid[left], tup->isnull[left]);
                ValueInTuple right_t = create_value(tup->values[right], tup->typid[right], tup->isnull[right]);
                TwoValueInTuple t = {left_t, right_t};
                bool found = false;
                TwoValueMapEntry *entry = (TwoValueMapEntry *)hash_search(multi_col_freqs, &t, HASH_ENTER, &found);
                if (found) {
                    entry->cnt++;
                } else {
                    entry->two_value = t;
                    entry->cnt = 1;
                }
            }
            tss->rescan();
            
            hash_seq_init(&seq, multi_col_freqs);
            TwoValueMapEntry *entry;
            while ((entry = (TwoValueMapEntry *)hash_seq_search(&seq)) != NULL) {
                prob_x1x2 = entry->cnt / sample_size;
                ValueMapEntry *ent;
                bool found = false;
                ent = (ValueMapEntry *)hash_search(single_col_freqs[left],
                                                   &entry->two_value.x, HASH_FIND, &found);
                prob_x1 = found ? ent->cnt / sample_size : 0.0;
                ent = (ValueMapEntry *)hash_search(single_col_freqs[right],
                                                   &entry->two_value.y, HASH_FIND, &found);
                prob_x2 = found ? ent->cnt / sample_size : 0.0;
                likelihood[left][right] += (prob_x1x2 * log(prob_x1x2 / (prob_x1 * prob_x2)));
                likelihood[right][left] += (prob_x1x2 * log(prob_x1x2 / (prob_x1 * prob_x2)));
            }
            hash_destroy(multi_col_freqs);
        }
    }
        
    for (uint32 i = 0; i < num_attrs; i++) {
        hash_destroy(single_col_freqs[i]);
    }
    pfree(single_col_freqs);

    StringInfoData str;
    initStringInfo(&str);
    uint128 visited = 0;
    uint128 currentindicate = 1;
    visited += currentindicate;
    for (unsigned int i = 0; i < num_attrs - 1; i++) {
        double maximal = -1;
        int maximal_destid = -1;
        int maximal_srcid = -1;
        find_maximal_edge(num_attrs, visited, currentindicate, (double *)likelihood,
                          &maximal, &maximal_srcid, &maximal_destid);
        Assert(maximal_srcid >= 0);
        Assert(maximal_destid >= 0);
        if (i == 0) {
            appendStringInfo(&str, "%d,%d", maximal_srcid, maximal_destid);
        } else {
            appendStringInfo(&str, ",%d,%d", maximal_srcid, maximal_destid);
        }
        visited += (currentindicate << maximal_destid);
    }
    return str.data;
}

typedef struct {
    MemoryContext memoryContext; /* current memory context for sample or save stats. */
    int num_cols;                /* how many columns in the result. */
    int64 num_rows;              /* how many rows in the result. */
    ArrayBuildState** output;    /* saved value of result array. */
    TupleDesc spi_tupDesc;       /* tuple desc of current mcv or histgram values. */
} SampleResultMultiColAsArraySpecInfo;

/*
 * Description: This routine creates an array from one of the result attributes after an SPI call.
 *          It allocates this array in the specified context. Note that, in general, the allocation
 *          context must not the SPI context because that is likely to get cleaned out soon.
 *
 * Parameters:
 *  @in attrno: attribute to flatten into an array (1 based)
 *  @in memory_context: memory context for statistic of column
 *
 * Returns: array of attribute type
 */
static ArrayBuildState* spi_get_result_array_sample(int attrno, MemoryContext memory_context)
{
    ArrayBuildState* result = NULL;
    Form_pg_attribute attribute = &SPI_tuptable->tupdesc->attrs[attrno];

    AssertEreport(attribute, MOD_OPT, "");

    for (uint32 i = 0; i < SPI_processed; i++) {
        Datum dValue = 0;
        bool isnull = false;
        dValue = tableam_tops_tuple_getattr(SPI_tuptable->vals[i], attrno + 1, SPI_tuptable->tupdesc, &isnull);

        /**
         * Add this value to the result array.
         */
        result = accumArrayResult(result, dValue, isnull, attribute->atttypid, memory_context);
    }

    return result;
}

/*
 * Description: A callback function for use with spiExecuteWithCallback.
 *          Reads each column of output into an array The number of arrays,
 *          and the output location are determined by treating *clientData.
 *
 * Parameters:
 *  @in clientData: as a EachResultColumnAsArraySpec
 *
 * Returns: void
 */
static void spi_callback_get_multicolarray_sample(void* clientData)
{
    SampleResultMultiColAsArraySpecInfo* spec = (SampleResultMultiColAsArraySpecInfo*)clientData;
    ArrayBuildState** out = spec->output;

    AssertEreport(SPI_tuptable != NULL, MOD_OPT, "");
    AssertEreport(SPI_tuptable->tupdesc, MOD_OPT, "");

    /*
     * Save the copy of tuple desc for check whether the attribute of received from datanode
     * is the same with local attribute for analyzed.
     */
    if (spec->num_rows == 0) {
        MemoryContext oldcontext = MemoryContextSwitchTo(spec->memoryContext);
        spec->spi_tupDesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (SPI_processed == 0)
        return;

    for (int i = 0; i < spec->num_cols; i++) {
        *out = spi_get_result_array_sample(i, spec->memoryContext);
        AssertEreport(*out, MOD_OPT, "");
        out++;
    }
    spec->num_rows += SPI_processed;
}

/*
 * Description: Computes the samples and their frequencies for relation.
 *
 * Parameters:
 *  @in tableName: temp table name
 *  @in spec: the sample info of special attribute for compute statistic
 *  @in sample: the sample data
 *
 * Returns: void
 */
static void prepare_samplerows(
    const char* tableName, AnalyzeSampleTableSpecInfo* spec, SampleDataSet* sample)
{
    ArrayBuildState** spiResult = (ArrayBuildState**)palloc0(sizeof(ArrayBuildState*) * (spec->stats->num_attrs + 1));
    StringInfoData str;
    SampleResultMultiColAsArraySpecInfo result;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    initStringInfo(&str);

    appendStringInfo(&str,
        "select %s, v_count from %s;",
        es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
        quote_identifier(tableName));

    result.num_cols = spec->stats->num_attrs + 1;
    result.output = spiResult;
    result.memoryContext = CurrentMemoryContext;
    result.num_rows = 0;
    result.spi_tupDesc = NULL;
    spi_exec_with_callback(DestSPI, str.data, false, 0, true,
        (void (*)(void*))spi_callback_get_multicolarray_sample, &result);
    pfree_ext(str.data);

    if (result.num_rows > MAX_SUPPORT_NUMROWS) {
        initStringInfo(&str);

        appendStringInfo(&str,
            "select %s, v_count from %s order by v_count desc limit %d;",
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
            quote_identifier(tableName),
            MAX_SUPPORT_NUMROWS);

        result.num_cols = spec->stats->num_attrs + 1;
        result.output = spiResult;
        result.memoryContext = CurrentMemoryContext;
        result.num_rows = 0;
        result.spi_tupDesc = NULL;
        spi_exec_with_callback(DestSPI, str.data, false, 0, true,
            (void (*)(void*))spi_callback_get_multicolarray_sample, &result);
        pfree_ext(str.data);
    }

    /*
     * Check whether the attribute of received from datanode
     * is the same with local attribute for analyzed or not.
     */
    if (spec->stats->num_attrs == 1 && result.spi_tupDesc &&
        (result.spi_tupDesc->attrs[0].atttypid != spec->stats->attrs[0]->atttypid ||
            result.spi_tupDesc->attrs[0].atttypmod != spec->stats->attrs[0]->atttypmod ||
            result.spi_tupDesc->attrs[0].attlen != spec->stats->attrs[0]->attlen)) {
        ereport(WARNING,
            (errmsg("The tupleDesc analyzed on %s is different from tupleDesc which received from datanode "
                    "when computing data counts.",
                    g_instance.attr.attr_common.PGXCNodeName),
             errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
                       NameStr(spec->stats->attrs[0]->attname),
                       format_type_be(spec->stats->attrs[0]->atttypid),
                       format_type_be(result.spi_tupDesc->attrs[0].atttypid))));
        FreeTupleDesc(result.spi_tupDesc);
        DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Load samples from table %s failed.", tableName);
        pfree_ext(spiResult);
        return;
    }

    sample->num_rows = result.num_rows;
    sample->tupdesc = result.spi_tupDesc;
    sample->num_attrs = spec->stats->num_attrs;
    if (result.num_rows > 0) {
        MemoryContext old_context;
        /* Must copy the target values into analyze_context */
        old_context = MemoryContextSwitchTo(spec->stats->anl_context);
        sample->values = (Datum*)palloc((result.num_rows * spec->stats->num_attrs) * sizeof(Datum));
        sample->isnull = (bool*)palloc((result.num_rows * spec->stats->num_attrs) * sizeof(bool));
        sample->freqs = (int64*)palloc(result.num_rows * sizeof(int64));
        for (int64 i = 0; i < result.num_rows; i++) {
            for (unsigned int j = 0; j < spec->stats->num_attrs; ++j) {
                int index = i + j * result.num_rows;
                sample->values[index] = datumCopy(
                    spiResult[j]->dvalues[i], spec->stats->attrtype[j]->typbyval, spec->stats->attrtype[j]->typlen);
                sample->isnull[index] = spiResult[j]->dnulls[i];
            }
            sample->freqs[i] = DatumGetInt64(spiResult[spec->stats->num_attrs]->dvalues[i]);
        }
        (void)MemoryContextSwitchTo(old_context);
    }

    pfree_ext(spiResult);

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC,
                         "Compute %d rows of sample for table %s success.",
                         spec->mcv_list.num_mcv, tableName);
    return;
}

bool analyze_compute_bayesnet(int *slot_idx, Relation onerel, AnalyzeMode analyzemode, bool inh,
    VacuumStmt* vacstmt, AnalyzeSampleTableSpecInfo* spec, const char* tmpTableName)
{
    SampleDataSet sample;
    prepare_samplerows(tmpTableName, spec, &sample);
    int64 numrows = sample.num_rows;
    TupleDesc tupdesc = sample.tupdesc;
    if (numrows == 0) {
        ereport(WARNING,
            (errmsg("[AI Stats] Empty data sample set.\n")));
        return false;
    }
    if (numrows > MAX_SUPPORT_NUMROWS) {
        ereport(WARNING,
            (errmsg("[AI Stats] Number of sample rows is %lld (> %d).\n", (long long)numrows, MAX_SUPPORT_NUMROWS)));
        return false;
    }
    if (spec->stats->num_attrs > MAX_SUPPORT_NUMCOLS) {
        ereport(WARNING,
            (errmsg("[AI Stats] Number of columns is %u (> %d).\n", spec->stats->num_attrs, MAX_SUPPORT_NUMCOLS)));
        return false;
    }
    for (uint32_t i = 0; i < spec->stats->num_attrs; i++) {
        if (!isSupport(tupdesc->attrs[i].atttypid)) {
            ereport(WARNING,
                (errmsg("[AI Stats] Unsupported attribute types %d.\n (Support only %s)",
                    tupdesc->attrs[i].atttypid, supported_typed_readable)));
            return false;
        }
    }
    Oid relid = RelationGetRelid(onerel);
    Bitmapset* bms_attnums = bms_make_singleton(spec->stats->attrs[0]->attnum);
    for (unsigned int i = 1; i < spec->stats->num_attrs; i++) {
        bms_attnums = bms_add_member(bms_attnums, spec->stats->attrs[i]->attnum);
    }

    const char* model_name_base = "BAYESNET_MODEL_STATS";
    StringInfoData model_name;
    initStringInfo(&model_name);
    appendStringInfo(&model_name, "%s_%d_%d", model_name_base, relid, bms_hash_value(bms_attnums));

    TableScannerSample2 tss(&sample);
    StatsModel bayesnet_model;
    
    char *edges_str = NULL;
    int num_edges = 0;
    if (spec->stats->num_attrs == 2) {
        edges_str = "0,1";
        num_edges = 1;
    } else if (spec->stats->num_attrs == 3) {
        edges_str = "0,1,1,2";
        num_edges = 2;
    } else {
        edges_str = structure_learning(&tss, (double)spec->totalrows * vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate);
        num_edges = spec->stats->num_attrs - 1;
    }

    /* Remove the old model.
     */
    remove_model_from_cache_and_disk(model_name.data);

    HyperparameterValues *hyper = (HyperparameterValues *)palloc0(sizeof(HyperparameterValues));
    hyper->num_nodes = spec->stats->num_attrs;
    hyper->num_edges = num_edges;
    hyper->num_mcvs = DEFAULT_NUM_MCVS;
    hyper->num_bins = DEFAULT_NUM_BINS;
    hyper->num_sample_rows = spec->samplerows;
    hyper->num_total_rows = (double)spec->totalrows;
    hyper->edges = edges_str;
    
    bayesnet_model.create_model(model_name.data, hyper, &tss);
    
    pfree_ext(hyper);
    
    spec->stats->stakind[*slot_idx] = STATISTIC_KIND_BAYESNET;
    spec->stats->staop[*slot_idx] = 0;
    (*slot_idx)++;
    pfree_ext(model_name.data);
    tss.delete_tuple();
    return true;
}

void remove_model_from_cache_and_disk(char *model_name) {
    /* Remove model from cache */
    char model_name_fix[1024];
    uint32_t avail = sizeof(char) * 1023;
    int chunk_size = Min(strlen(model_name), avail - 1);
    errno_t rc = memcpy_s(model_name_fix, avail, model_name, chunk_size);
    securec_check_ss(rc, "\0", "\0");
    model_name_fix[chunk_size] = '\0';

    LWLockAcquire(AboCacheLock, LW_EXCLUSIVE);
    bool found;
    AboModelCacheEntry *cache = (AboModelCacheEntry *)hash_search(g_instance.abo_cxt.models,
                                    model_name_fix, HASH_REMOVE, &found);
    if (found) {
        MemoryContextDelete(cache->model->mcontext);
    }
    LWLockRelease(AboCacheLock);
    /* Remove model from gs_model_warehouse */
    model_drop(model_name);
}

