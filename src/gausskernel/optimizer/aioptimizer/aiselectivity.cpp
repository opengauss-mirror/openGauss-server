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
 *  aiselectivity.cpp
 *   ABO selectivity estimation.
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/aioptimizer/aiselectivity.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "optimizer/aioptimizer.h"
#include "postgres.h"
#include "db4ai/bayesnet.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/extended_statistics.h"
#include "nodes/print.h"
#include "parser/parsetree.h"
#include "db4ai/db4ai_common.h"
#include "db4ai/db4ai_api.h"

struct DirectModelPredictor {
    AlgorithmAPI *palgo;
    ModelPredictor predictor;
};

static void destroy_model(Model *model)
{
    if (model) {
        pfree_ext(model->sql);
        list_free_ext(model->hyperparameters);
        list_free_ext(model->scores);
        list_free_ext(model->train_info);
        pfree_ext(model->data.raw_data);
    }
}

double StatsModel::predict_model(Datum *values, Oid *typids, bool *isnulls, int ncolumns, bool *iseliminate)
{
    if (bayesNetModel == nullptr) {
        ereport(INFO,
            (errmsg("[AI Stats] must do model train before predict\n")));
        return -1;
    }
    DirectModelPredictor *predictor = reinterpret_cast<DirectModelPredictor *>(bayesNetModel);
    SerializedModelBayesNet *bayes_net_model = reinterpret_cast<SerializedModelBayesNet *>(predictor->predictor);
    for (int i = 0; i < bayes_net_model->num_nodes; i++) {
        typids[i] = (*bayes_net_model->nodes[i].featuresmatrix_fornet[0])[0].type;
    }
    Datum dt = model_predict(bayesNetModel, values, isnulls, typids, ncolumns);
    return DatumGetFloat8(dt);
};

bool StatsModel::load_model(const char *model_name)
{
    Model *model = const_cast<Model *>(model_load(model_name));
    if (model == nullptr or model->status != ERRCODE_SUCCESSFUL_COMPLETION) {
        return false;
    }
    processedtuples = model->processed_tuples;
    bayesNetModel = model_prepare_predict(model);
    destroy_model(model);
    pfree_ext(model);
    return true;
};

StatsModel *search_model_from_cache(const char *model_name)
{
    double current_time = GetCurrentTimestamp();
    bool found = false;
    AboModelCacheEntry *cache = (AboModelCacheEntry *)hash_search(g_instance.abo_cxt.models, model_name, HASH_FIND, &found);
    if (found) {
        cache->recent_used_time = current_time;
        return cache->model;
    }
    return NULL;
}

bool need_do_memory_evicted(HTAB *cached_models)
{
    bool res = hash_get_num_entries(cached_models) >= NUM_CACHED_MODEL_LIMIT;
    return res;
}

void do_memory_evicted(HTAB *cached_models)
{
    HASH_SEQ_STATUS hseq;
    hash_seq_init(&hseq, cached_models);
    AboModelCacheEntry *cache;
    double min_timestamp = (double)PG_INT64_MAX;
    double max_timestamp = (double)PG_INT64_MIN;
    int num_evicted = int(NUM_CACHED_MODEL_LIMIT * CLEAN_RATIO);
    int pos_evicted = 0;
    while ((cache = (AboModelCacheEntry *)hash_seq_search(&hseq)) != NULL) {
        if (cache->recent_used_time > max_timestamp) {
            max_timestamp = cache->recent_used_time;
        }
        if (cache->recent_used_time < min_timestamp) {
            min_timestamp = cache->recent_used_time;
        }
    }
    double diff = max_timestamp - min_timestamp;
    hash_seq_init(&hseq, cached_models);
    while ((cache = (AboModelCacheEntry *)hash_seq_search(&hseq)) != NULL) {
        if ((double)gs_random() / (double)MAX_RANDOM_VALUE <
            1.0 - (cache->recent_used_time - min_timestamp) / diff) {
            if (pos_evicted < num_evicted) {
                pfree_ext(cache->model->bayesNetModel);
                MemoryContextDelete(cache->model->mcontext);
                pfree_ext(cache->model);
                hash_search(cached_models, cache->model_name, HASH_REMOVE, NULL);
                pos_evicted++;
            }
        }
    }
    return;
}


static StatsModel *load_model_from_disk(const char *model_name)
{
    // Load models in local mode
    StatsModel *model = (StatsModel *)palloc0(sizeof(StatsModel));
    model->mcontext = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
        "abo local model",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    MemoryContext oldContext = MemoryContextSwitchTo(model->mcontext);
    bool success = model->load_model(model_name);
    MemoryContextSwitchTo(oldContext);
    if (success) {
        return model;
    } else {
        MemoryContextDelete(model->mcontext);
        pfree_ext(model);
        return NULL;
    }
}

static void insert_model_into_cache(StatsModel *model, const char *model_name)
{
    if (need_do_memory_evicted(g_instance.abo_cxt.models)) {
        do_memory_evicted(g_instance.abo_cxt.models);
    }
    double current_time = GetCurrentTimestamp();
    bool found;
    AboModelCacheEntry *cache = (AboModelCacheEntry *)hash_search(g_instance.abo_cxt.models, model_name,
        HASH_ENTER, &found);
    if (found) { // Recheck existence of models in case of concurrent writing
        MemoryContextDelete(model->mcontext);
        pfree_ext(model);
        cache->recent_used_time = current_time;
    } else {
        uint32_t avail = sizeof(char) * 1023;
        int chunk_size = Min(strlen(model_name), avail - 1);
        errno_t rc = memcpy_s(cache->model_name, avail, model_name, chunk_size);
        securec_check_ss(rc, "\0", "\0");
        cache->model_name[chunk_size] = '\0';
        cache->model = model;
        cache->recent_used_time = current_time;
    }
    return;
}

void InitializeAboModelCache(void)
{
    g_instance.abo_cxt.abo_model_manager_mcxt = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
        "abo model cache memory context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    HASHCTL hash_ctl;
    int rc;
    MemoryContext oldContext;
    oldContext = MemoryContextSwitchTo(g_instance.abo_cxt.abo_model_manager_mcxt);
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc,"\0","\0");
    hash_ctl.keysize = 128;
    hash_ctl.entrysize = sizeof(AboModelCacheEntry);
    hash_ctl.hash = string_hash;
    hash_ctl.hcxt = g_instance.abo_cxt.abo_model_manager_mcxt;
    g_instance.abo_cxt.models = hash_create("Models <name, model>", 100, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
    MemoryContextSwitchTo(oldContext);
}

/*
 * @brief       Set up attnum order according to clause map, so that we can use this order
 *              to locate the corresponding clause when we go through the bitmap of attnums.
 *                We need this order because the clause is ordered by its position in clauselist and mcv
 *              in extended stats are ordered by attnum.
 *              left == true : set up left attnum order ; left == false : set up right attnum order .
 */
void ES_SELECTIVITY::set_up_attnum_order_reversed(es_candidate* es, int* attnum_order) const
{
    int i;
    int j = 0;
    ListCell* lc = NULL;
    foreach(lc, es->clause_map) {
        i = 0;
        int attnum = -1;
        es_clause_map* clause_map = (es_clause_map*)lfirst(lc);
        while ((attnum = bms_next_member(es->left_extended_stats->bms_attnum, attnum)) >= 0) {
            if (attnum == clause_map->left_attnum) {
                attnum_order[i] = j;
                break;
            }
            i++;
        }
        j++;
    }
    return;
}

/*
 * @brief       calculate selectivity for eqsel using AI model
 */
Selectivity ES_SELECTIVITY::cal_eqsel_ai(es_candidate *es)
{
    
    if (es->left_extended_stats == NULL) {
        return (Selectivity)-1;
    }
    if (es->left_extended_stats->bayesnet_model == NULL) {
        return (Selectivity)-1;
    }
    
    
    Selectivity result = 1.0;
    ListCell *lc = NULL;
    int column_count = 0;
    int stats_ncolumn = 0;

    column_count = es->clause_group->length;                               // ncolumns in where conditions
    stats_ncolumn = bms_num_members(es->left_extended_stats->bms_attnum);  // ncolumns in stats

    /* set up attnum order */
    int *attnum_order = (int *)palloc(stats_ncolumn * sizeof(int));  // columns in where conditions
    for (int i = 0; i < stats_ncolumn; i++) {
        attnum_order[i] = -1;
    }
    set_up_attnum_order_reversed(es, attnum_order);

    ListCell **clause_array = (ListCell **)palloc(column_count * sizeof(ListCell *));  // columns in where conditions
    int j = 0;
    foreach (lc, es->clause_group) {
        clause_array[j++] = lc;
    }
    
    Datum *values = (Datum *)palloc0(sizeof(Datum) * stats_ncolumn);
    Oid *typids = (Oid *)palloc0(sizeof(Oid) * stats_ncolumn);
    bool *isnulls = (bool *)palloc0(sizeof(bool) * stats_ncolumn);
    bool *iseliminate = (bool *)palloc0(sizeof(bool) * stats_ncolumn);

    /* process clause one by one */
    Datum const_value;
    bool fail_and_return = false;
    for (int i = 0; i < stats_ncolumn; i++) {
        if (attnum_order[i] >= 0) {
            lc = clause_array[attnum_order[i]];
            RestrictInfo *clause = (RestrictInfo *)lfirst(lc);
            if (IsA(clause->clause, OpExpr)) {
                OpExpr *opclause = (OpExpr *)clause->clause;
                /* set up const value */
                Node *left = (Node *)linitial(opclause->args);
                Node *right = (Node *)lsecond(opclause->args);
                if (IsA(left, Const)) {
                    const_value = ((Const *)left)->constvalue;
                } else if (IsA(right, Const)){
                    const_value = ((Const *)right)->constvalue;
                } else {
                    fail_and_return = true;
                    break;
                }
                values[i] = const_value;
                isnulls[i] = false;
                typids[i] = 0; // typids decision is made after
                iseliminate[i] = false;
            } else if (IsA(clause->clause, NullTest)){
                values[i] = 0;
                isnulls[i] = true;
                typids[i] = 0; // typids decision is made after
                iseliminate[i] = false;
            } else {
                fail_and_return = true;
                break;
            }
        } else {
            values[i] = 0;
            isnulls[i] = false;
            typids[i] = 0; // typids decision is made after
            iseliminate[i] = true;
        }
    }

    pfree_ext(attnum_order);
    pfree_ext(clause_array);
    if (fail_and_return) {
        pfree_ext(values);
        pfree_ext(typids);
        pfree_ext(isnulls);
        pfree_ext(iseliminate);
        return (Selectivity)-1;
    }

    LWLockAcquire(AboCacheLock, LW_SHARED);
    StatsModel *stats_model = search_model_from_cache(es->left_extended_stats->bayesnet_model);
    if (stats_model) {
        // Visiting the shared model should be atomic
        result = stats_model->predict_model(values, typids, isnulls, stats_ncolumn, iseliminate);
        LWLockRelease(AboCacheLock);
    } else {
        LWLockRelease(AboCacheLock);
        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.abo_cxt.abo_model_manager_mcxt);
        stats_model = load_model_from_disk(es->left_extended_stats->bayesnet_model);
        MemoryContextSwitchTo(oldContext);
        if (!stats_model) {
            pfree_ext(values);
            pfree_ext(typids);
            pfree_ext(isnulls);
            pfree_ext(iseliminate);
            return (Selectivity)-1;
        } else {
            result = stats_model->predict_model(values, typids, isnulls, stats_ncolumn, iseliminate);
            // Visit after insert is risky here
            LWLockAcquire(AboCacheLock, LW_EXCLUSIVE);
            insert_model_into_cache(stats_model, es->left_extended_stats->bayesnet_model);
            LWLockRelease(AboCacheLock);
        }
    }
    pfree_ext(values);
    pfree_ext(typids);
    pfree_ext(isnulls);
    pfree_ext(iseliminate);
    CLAMP_PROBABILITY(result);
    save_selectivity(es, result, 0.0);
    ereport(ES_DEBUG_LEVEL, (errmodule(MOD_OPT),
                             (errmsg("[AI Stats] extended statistic is used to calculate eqsel selectivity as %e", result))));
    return result;
}

bool get_attmultistatsslot_ai(HeapTuple statstuple, Bitmapset *bms_attnum, char **bayesnet_model)
{
    Form_pg_statistic_ext stats = (Form_pg_statistic_ext)GETSTRUCT(statstuple);
    int slot_index = 0;
    for (slot_index = 0; slot_index < STATISTIC_NUM_SLOTS; ++slot_index) {
        if ((&stats->stakind1)[slot_index] == STATISTIC_KIND_BAYESNET) {
            break;
        }
    }
    if (slot_index >= STATISTIC_NUM_SLOTS) {
        return false; /* not there */
    }
    Oid relid = stats->starelid;
    const char *model_name_base = "BAYESNET_MODEL_STATS";
    StringInfoData model_name;
    initStringInfo(&model_name);
    appendStringInfo(&model_name, "%s_%d_%d", model_name_base, relid, bms_hash_value(bms_attnum));
    *bayesnet_model = model_name.data;
    return true;
}
