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
 *  aioptimizer.h
 *
 * IDENTIFICATION
 *        src/include/optimizer/aioptimizer.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _AIOPTIMIZER_H_
#define _AIOPTIMIZER_H_

#define MAX_SUPPORT_NUMROWS 200000
#define MAX_SUPPORT_NUMCOLS 64

#include "postgres.h"
#include "catalog/namespace.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "nodes/execnodes.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "commands/vacuum.h"
#include "utils/snapmgr.h"
#include "gtm/utils/palloc.h"

typedef struct HyperparameterValues {
    int num_nodes;
    int num_edges;
    int num_mcvs;
    int num_bins;
    uint64_t num_sample_rows;
    uint64_t num_total_rows;
    const char *edges;
} HyperparameterValues;

typedef struct TupleScan {
    TupleScan() {
        this->cnt = 0;
    };
    Datum *values;
    Oid *typid;
    bool *isnull;
    bool *typbyval;  // attribute is passed by value or by reference
    int16 *typlen;   // the length of an attribute
    int nattrs;
    int64 cnt;
} TupleScan;

typedef struct TupleProb {
    TupleProb() {
        this->prob = 0.0;
    };
    Datum *values;
    Oid *typid;
    bool *isnull;
    int *datum_ids;
    int nattrs;
    double prob;
} TupleProb;

typedef struct SampleDataSet {
    SampleDataSet() {
        this->values = NULL;
        this->isnull = NULL;
        this->freqs = NULL;
        this->num_rows = 0;
        this->tupdesc = NULL;
        this->num_attrs = 0;
    }
    Datum *values;
    bool *isnull;
    int64 *freqs;
    int64 num_rows;
    TupleDesc tupdesc;
    unsigned int num_attrs;
} SampleDataSet;

class TableScannerSample2 {
private:
    Datum *values;
    bool *isnull;
    int64 *freqs;
    int64 numrows;
    TupleScan datumTuple;
    TupleDesc attDesc;
    int64 loc;

public:
    TableScannerSample2(SampleDataSet *sample)
    {
        attDesc = sample->tupdesc;
        numrows = sample->num_rows;
        datumTuple.values = (Datum *)palloc0(sizeof(Datum) * sample->num_attrs);
        datumTuple.typid = (Oid *)palloc0(sizeof(Oid) * sample->num_attrs);
        datumTuple.isnull = (bool *)palloc0(sizeof(bool) * sample->num_attrs);
        datumTuple.typbyval = (bool *)palloc0(sizeof(bool) * sample->num_attrs);
        datumTuple.typlen = (int16 *)palloc0(sizeof(int16) * sample->num_attrs);
        datumTuple.nattrs = sample->num_attrs;
        for (int i = 0; i < datumTuple.nattrs; i++) {
            datumTuple.typid[i] = attDesc->attrs[i].atttypid;
            datumTuple.typbyval[i] = attDesc->attrs[i].attbyval;
            datumTuple.typlen[i] = attDesc->attrs[i].attlen;
        }
        values = sample->values;
        freqs = sample->freqs;
        isnull = sample->isnull;
        loc = 0;
    };

    ~TableScannerSample2(){};

    void delete_tuple()
    {
        pfree(datumTuple.values);
        pfree(datumTuple.typid);
        pfree(datumTuple.isnull);
        pfree(datumTuple.typbyval);
        pfree(datumTuple.typlen);
    };

    TupleScan *get_tuple()
    {
        return &datumTuple;
    };

    TupleScan *fetch()
    {
        if (loc >= numrows) {
            return nullptr;
        }
        for (int i = 0; i < datumTuple.nattrs; i++) {
            int64 index = loc + i * this->numrows;
            datumTuple.values[i] = values[index];
            datumTuple.isnull[i] = isnull[index];
        }
        if ((++datumTuple.cnt) >= freqs[loc]) {
            datumTuple.cnt = 0;
            loc++;
        }
        return &datumTuple;
    };

    void rescan()
    {
        loc = 0;
        datumTuple.cnt = 0;
    };
};

class StatsModel {
public:
    void *bayesNetModel;
    uint64_t processedtuples;
    MemoryContext mcontext;

    bool create_model(const char *model_name, HyperparameterValues *hyper, TableScannerSample2 *reader);
    double predict_model(Datum *values, Oid *typids, bool *isnulls, int ncolumns, bool *iseliminate);
    bool load_model(const char *model_name);
    bool search_model_from_cache(const char *model_name);
    static bool fetch(void *callback_data, ModelTuple *tuple)
    {
        auto reader = reinterpret_cast<TableScannerSample2 *>(callback_data);
        TupleScan *rtuple = reader->fetch();
        if (rtuple == nullptr) {
            return false;
        }
        tuple->values = rtuple->values;
        tuple->isnull = rtuple->isnull;
        tuple->typid = rtuple->typid;
        tuple->typbyval = rtuple->typbyval;
        tuple->typlen = rtuple->typlen;
        tuple->ncolumns = rtuple->nattrs;
        return true;
    };

    static void rescan(void *callback_data)
    {
        auto reader = reinterpret_cast<TableScannerSample2 *>(callback_data);
        reader->rescan();
    };
};

#define NUM_CACHED_MODEL_LIMIT 30
#define CLEAN_RATIO 0.3

typedef struct AboModelCacheEntry {
    AboModelCacheEntry() {
        recent_used_time = 0;
    };
    char model_name[1024];
    double recent_used_time;
    StatsModel *model;
} AboModelCacheEntry;

extern bool analyze_compute_bayesnet(int *, Relation, AnalyzeMode, bool,
                                     VacuumStmt*, AnalyzeSampleTableSpecInfo*, const char*);
extern bool get_attmultistatsslot_ai(HeapTuple, Bitmapset *, char **);
                                     StatsModel *search_model_from_cache(const char *model_name);
void InitializeAboModelCache(void);
extern TupleProb *sample_from_model(StatsModel *model, uint32_t sample_size);
void remove_model_from_cache_and_disk(char *model_name);

#endif /* AIOPTIMIZER_H */

