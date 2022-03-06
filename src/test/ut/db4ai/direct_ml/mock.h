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
 *
 * mock.h
 *        Header file of easy to test
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/mock.h
 *
 * ---------------------------------------------------------------------------------------
**/

#ifndef DB4AI_DB4AI_MOCK_H
#define DB4AI_DB4AI_MOCK_H

#include <postgres.h>
#include <lib/stringinfo.h>
#include <commands/explain.h>
#include <db4ai/db4ai_api.h>
#include <utils/fmgroids.h>
#include "readers.h"

#define TEST_MAX_HYPERPARAMETERS    (30)
#define TEST_MAX_ATTRIBUTES         (1000)

void pg_mock_init(int log_level, int working_mem_mb);

uint64_t get_clock_usecs();

Datum OidFunctionCall2Coll(Oid functionId, Oid collation, Datum arg1, Datum arg2);

struct Hyperparameters {
    int count = 0;
    Hyperparameter hps[TEST_MAX_HYPERPARAMETERS];
    
    Hyperparameters() : count{0}
    {
        memset_s(hps, sizeof(hps), 0, sizeof(hps));
    }
    
    void add(const char *name, Oid type, Datum value)
    {
        Assert(count < TEST_MAX_HYPERPARAMETERS);
        hps[count].name = name;
        hps[count].type = type;
        hps[count].value = value;
        count++;
    }
    
    void add_bool(const char *name, bool value)
    {
        add(name, BOOLOID, BoolGetDatum(value));
    }
    
    void add_int(const char *name, int value)
    {
        add(name, INT4OID, Int32GetDatum(value));
    }
    
    void add_float(const char *name, double value)
    {
        add(name, FLOAT8OID, Float8GetDatum(value));
    }
    
    void add_enum(const char *name, const char *value)
    {
        add(name, ANYENUMOID, CStringGetDatum(value));
    }
    
    void reset()
    {
        memset_s(this, sizeof(Hyperparameters), 0, sizeof(Hyperparameters));
    }
};

struct Descriptor {
    int count = 0;
    Oid coltypes[TEST_MAX_ATTRIBUTES];
    bool colbyvals[TEST_MAX_ATTRIBUTES];
    int16 coltyplen[TEST_MAX_ATTRIBUTES];
    
    Descriptor() : count{0}
    {
        memset_s(coltypes, sizeof(coltypes), 0, sizeof(coltypes));
        memset_s(colbyvals, sizeof(colbyvals), 0, sizeof(colbyvals));
        memset_s(coltyplen, sizeof(coltyplen), 0, sizeof(coltyplen));
    }
    void reset()
    {
        memset_s(this, sizeof(Descriptor), 0, sizeof(Descriptor));
    }
};

/*
 * this class provides an interface to any DB4AI algorithm implemented using the
 * direct API. any method of this interface not implemented will hinder compilation
 */
template<class A>
class ModelCRTP {
protected:
    const Model *model;

    static bool fetch(void *callback_data, ModelTuple *tuple) {
        auto reader = reinterpret_cast<Reader *>(callback_data);
        const ModelTuple *rtuple = reader->fetch();
        if (rtuple == nullptr)
            return false;

        tuple->values = rtuple->values;
        tuple->isnull = rtuple->isnull;
        tuple->typid = rtuple->typid;
        tuple->typbyval = rtuple->typbyval;
        tuple->typlen = rtuple->typlen;
        tuple->ncolumns = rtuple->ncolumns;
        return true;
    }

    static void rescan(void *callback_data) {
        auto reader = reinterpret_cast<Reader *>(callback_data);
        reader->rescan();
    }

public:
    ModelCRTP() : model{nullptr}
    {}
    
    // no interest in these
    ModelCRTP(ModelCRTP<A> const&) = delete;
    
    ModelCRTP(ModelCRTP<A> const&&) = delete;
    
    ModelCRTP<A> operator=(ModelCRTP<A> const&) = delete;
    
    ModelCRTP<A> operator=(ModelCRTP<A> const&&) = delete;
    
    void set_model(const Model *model)
    {
        this->model = model;
    }
    
    Model const *get_model()
    {
        return model;
    }
    
    bool train(char const *name, Hyperparameters const *hps, Descriptor *td, Reader *reader)
    {
        return static_cast<A *>(this)->do_train(name, hps, td, reader);
    }
    
    bool predict(Reader *reader)
    {
        return static_cast<A *>(this)->do_predict(reader);
    }
    
};

typedef struct Execution_Times {
    double training_time_s = 0.;
    double prediction_time_s = 0.;
} Execution_Times;

/*
 * this class can be used in the main to run everything: train, explain, predict
 * see main.cpp for an example
 */
template<class A>
class Tester final {
    A algo;

public:
    Tester() = default;
    
    // no interest in these
    Tester(Tester<A> const&) = delete;
    
    Tester(Tester<A> const&&) = delete;
    
    Tester<A> operator=(Tester<A> const&) = delete;
    
    Tester<A> operator=(Tester<A> const&&) = delete;
    
    Execution_Times const run(Hyperparameters const *hps, Descriptor *td, char const *name,
                              ExplainFormat const format, Reader *rd_train, Reader *rd_test = nullptr)
    {
        Execution_Times times;
        printf("TRAIN\n");
        uint64_t start = get_clock_usecs();
        algo.train(name, hps, td, rd_train);
        uint64_t end = get_clock_usecs();
        times.training_time_s = static_cast<double>((end - start)) / 1000000.0;
        printf("\nTrain done in %.6f secs\n\n", times.training_time_s);
       
        // printf("DB4AI MODEL\n-----\n%s\n-------\n\n", algo.explain(format));

        Model const *model = algo.get_model();
        model_store(model);
    	printf("Model stored\n");
        
        model = model_load(model->model_name);

        algo.set_model(model);
        
        if (rd_test == nullptr)
            rd_test = rd_train;
        
        if (rd_test == rd_train)
            rd_test->rescan();
    
        printf("PREDICT\n");
        start = get_clock_usecs();
        algo.predict(rd_test);
        end = get_clock_usecs();
        times.prediction_time_s = static_cast<double>((end - start)) / 1000000.0;
        printf("\nPrediction done in %.6f secs\n", times.prediction_time_s);
        return times;
    }
};

#endif //DB4AI_DB4AI_MOCK_H
