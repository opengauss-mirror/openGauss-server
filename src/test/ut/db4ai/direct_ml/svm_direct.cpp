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
 * svm_direct.cpp
 *        Read data files
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/direct_ml/svm_direct.cpp
 *
 * ---------------------------------------------------------------------------------------
**/

#include <postgres.h>
#include "direct_algos.h"
#include "readers.h"

// #define USE_GAUSSIAN
// #define USE_POLYNOMIAL

struct TestGaussian {
    const char *name;
    int batch_size;
    double decay;
    double lambda;
    double learning_rate;
    double tolerance;
    int max_iterations;
    int seed;
    double accuracy;
} gaussian_tests[] = {
    { "separable",      4, 0.00000000, 451.43192744, 0.00128971, 0.00098990,    1,   200, 1.000      },
    { "almost_linear",  8, 0.84938336, 614.68837452, 0.17139901, 0.00000021,   87,    11, 0.985       },
    { "one",            1, 0.00000486, 355.01855320, 0.34238902, 0.00000246,    2,    71, 1.000      },
    { "clusters",       2, 0.01072456,  41.53315701, 0.51013572, 0.00000101,   14,     2, 0.630      },
    { "circle",         2, 0.04287848, 796.73487315, 0.00442136, 0.00000088,    7,   121, 0.675       },
    { "moons",          2, 0.01318587, 955.48766920, 0.01601325, 0.00010955,    4,    17, 0.890      },
    { "polynomial",     8, 0.41294937, 141.17150921, 0.10135892, 0.00120927,    7,    27, 0.920       },
    { "blob_1",         6, 0.03236468, 728.35294835, 0.89775039, 0.00571308,    5,    33, 0.930      },
    { "blob_2",         8, 0.84938336, 614.68837452, 0.13304777, 0.00009542,   52,    32, 0.900      },
    { "blob_3",         6, 0.21813823,  21.44762832, 0.49231980, 0.00000030,   21,    28, 0.895      },
    { "blob_4",         4, 0.04729034, 289.17150662, 0.03038806, 0.00000228,    6,   188, 0.878      },
};

#define NUM_TEST_GAUSSIAN   (sizeof(gaussian_tests) / sizeof(gaussian_tests[0]))

class TestKernels final : public ModelCRTP<TestKernels> {
private:
    friend ModelCRTP<TestKernels>;
    using ModelCRTP<TestKernels>::ModelCRTP;

    bool do_train(char const *model_name, Hyperparameters const *hp, Descriptor *td, Reader *reader) {
        model = model_fit(model_name, SVM_CLASSIFICATION, hp->hps, hp->count,
                        td->coltypes, td->colbyvals, td->coltyplen,
                        td->count, &fetch, &rescan, reader);
        if (model->status != ERRCODE_SUCCESSFUL_COMPLETION)
            elog(ERROR, "could not train model, error %d\n", model->status);
        
        return true;
    }

    bool do_predict(Reader *reader) {
        char filename[256];
        sprintf(filename, "/usr1/datasets/reg/reg_%s."
#ifdef USE_GAUSSIAN
                        "gaussian"
#elif defined(USE_POLYNOMIAL)
                        "polynomial"
#else
                        "linear"
#endif
                        ".csv", model->model_name);
        FILE *fp = fopen(filename, "w");
        int row = 0;
        int hit = 0;
        ModelPredictor pred = model_prepare_predict(model);
        const ModelTuple *rtuple;
        while ((rtuple = reader->fetch()) != nullptr) {
            bool isnull = rtuple->isnull[0];
            if (!isnull) {
                int target = DatumGetInt32(rtuple->values[0]);
                row++;

                Datum dt = model_predict(pred, &rtuple->values[1], &rtuple->isnull[1], &rtuple->typid[1], rtuple->ncolumns-1);
                fprintf(fp, "%d\n", DatumGetInt32(dt));
                if (false)
                    printf(" - %d %d %d\n", ++row, target, DatumGetInt32(dt));

                if (target == DatumGetInt32(dt))
                    hit++;
            }
        }
        fclose(fp);
        printf(" accuracy=%.3f (%d rows)\n", (double)hit / row, row);
        return true;
    }
};

// [0:1)
inline double rnd_next(struct drand48_data *rnd) {
    double r;
    drand48_r(rnd, &r);
    return r;
}

// [0:max)
inline int rnd_next(struct drand48_data *rnd, int max) {
    long int r;
    lrand48_r(rnd, &r);
    return r % max;
}

// [min:max)
inline int rnd_next(struct drand48_data *rnd, int min, int max) {
    return rnd_next(rnd, max - min) + min;
}

inline double rnd_next(struct drand48_data *rnd, double min, double max) {
    return rnd_next(rnd) * (max - min) + min;
}

inline double rnd_next_log(struct drand48_data *rnd, double min, double max) {
    static constexpr double EPSILON = 1E-12;
    double log_min_range = log(min + EPSILON);
    double log_max_range = log(max + EPSILON);
    double r = rnd_next(rnd, log_min_range, log_max_range);
    double v = exp(r);
    // printf("-- %.16g %.16g %.16g %.16g\n", min, max, r, v);
    return v;
}

struct HpoTask {
    int id;
    // hyperparameters
    int batch_size;
    double decay;
    double lambda;
    double learning_rate;
    double tolerance;
    double gamma;
    int degree;
    double coef0;
    int seed;
    // results
    Model const *model;
    double accuracy;
};

#ifdef USE_GAUSSIAN
#define SVM_HYPERP_HPO  6
#elif defined(USE_POLYNOMIAL)
#define SVM_HYPERP_HPO  7
#else
#define SVM_HYPERP_HPO  5
#endif
static void set_hyperp(HpoTask *task, int hyperp, struct drand48_data *rnd) {
    static int batch_sizes[] = { 1, 2, 4, 6, 8 };
    switch (hyperp) {
        case 0: // batch_size
            task->batch_size = batch_sizes[rnd_next(rnd, 5)];
            break;
        case 1: // decay
            task->decay = rnd_next_log(rnd, 0.0, 0.9);
            break;
        case 2: // lambda
            task->lambda = rnd_next_log(rnd, 1E-3, 1E3);
            break;
        case 3: // learning_rate
            task->learning_rate = rnd_next_log(rnd, 1E-3, 1.0);
            break;
        case 4: // tolerance
            task->tolerance = rnd_next_log(rnd, 1E-7, 1E-1);
            break;
#ifdef USE_GAUSSIAN
        case 5: // gamma
            task->gamma = rnd_next_log(rnd, 1E-1, 1);
            break;
#endif
#ifdef USE_POLYNOMIAL
        case 5: // degree
            task->degree = rnd_next(rnd, 3, 5);
            break;
        case 6: // coef0
            task->coef0 = rnd_next_log(rnd, 1E-2, 1E1);
            break;
#endif
        default:
            Assert(false);
    }
}

static void hpo_train(const char* name, HpoTask *tasks, int count, Reader *reader, Descriptor *td, double *gamma, int *degree, double *coef0) {
    Hyperparameters hps;
    for (int t=0 ; t<count ; t++, tasks++) {
        tasks->seed = t + 1;

        hps.reset();
        hps.add_int("batch_size", tasks->batch_size);
        hps.add_float("decay", tasks->decay);
        hps.add_float("lambda", tasks->lambda);
        hps.add_float("learning_rate", tasks->learning_rate);
        hps.add_float("tolerance", tasks->tolerance);
        // hps.add_float("gamma", tasks->gamma);
        hps.add_int("seed", tasks->seed);
        hps.add_int("max_iterations", 100);
        // hps.add_bool("verbose", true);

        *gamma = tasks->gamma;
        *degree = tasks->degree;
        *coef0 = tasks->coef0;
        reader->rescan();

        MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "hpo",
                                                ALLOCSET_DEFAULT_MINSIZE,
                                                ALLOCSET_DEFAULT_INITSIZE,
                                                ALLOCSET_DEFAULT_MAXSIZE);
        MemoryContext old_cxt = MemoryContextSwitchTo(cxt);

        TestKernels test;
        test.train(name, &hps, td, reader);
        // printf("%s\n",test.explain(EXPLAIN_FORMAT_YAML));

        tasks->model = test.get_model();
        TrainingScore *score = lfirst_node(TrainingScore, list_head(tasks->model->scores));
        tasks->accuracy = score->value;

        // printf("== %d: %d %.16g %.16g %.16g %.16g = %d %.16g\n", tasks->id,
        //         tasks->batch_size, tasks->decay, tasks->lambda, tasks->learning_rate, tasks->tolerance,
        //         tasks->model->num_actual_iterations, tasks->accuracy); fflush(stdout);

        MemoryContextSwitchTo(old_cxt);
    }
}

static int f_cmp_tasks(const void *lhs, const void *rhs) {
    const HpoTask *plhs = (const HpoTask *)lhs;
    const HpoTask *prhs = (const HpoTask *)rhs;
    // printf("** %d:%.16g %d:%.16g\n", plhs->id, plhs->accuracy, prhs->id, prhs->accuracy);
    if (plhs->accuracy > prhs->accuracy)
        return -1;

    if (plhs->accuracy < prhs->accuracy)
        return 1;

    if (plhs->model->num_actual_iterations < prhs->model->num_actual_iterations)
        return -1;

    if (plhs->model->num_actual_iterations > prhs->model->num_actual_iterations)
        return 1;

    return 0;
}

static void hpo_drop(HpoTask *tasks, int count) {
    while (count-- > 0) {
        MemoryContextDelete(tasks->model->memory_context);
        tasks++;
    }
}

static void hpo_mutate(const HpoTask *src, HpoTask *dst, int count, int id, struct drand48_data *rnd) {
    while (count-- > 0) {
        *dst = *src;
        set_hyperp(dst, rnd_next(rnd, SVM_HYPERP_HPO), rnd);
        src++;
        dst++;
    }
}

static const Model* hpo(TestGaussian *ptest, Reader *reader, int quota, double *gamma, int *degree, double *coef0) {
    // random grid
    int qpart = quota / 5;
    HpoTask tasks[qpart * 2];

    // fill the first partition
    struct drand48_data rnd;
    srand48_r(1000000, &rnd);
    for (int q=0 ; q<qpart ; q++) {
        HpoTask *task = &tasks[q];
        for (int h=0 ; h<SVM_HYPERP_HPO ; h++) {
            task->id = q;
            set_hyperp(task, h, &rnd);
        }
    }

    Descriptor td;
    char typalign;
    td.count = reader->get_columns();
    for (int i=0 ; i<td.count ; i++) {
        td.coltypes[i] = reader->get_column_type(i);
        get_typlenbyvalalign(td.coltypes[i], &td.coltyplen[i], &td.colbyvals[i], &typalign);
    }

    // train the first partition
    hpo_train(ptest->name, tasks, qpart, reader, &td, gamma, degree, coef0);
    qsort(tasks, qpart, sizeof(HpoTask), f_cmp_tasks);

    // do mutations
    for (int m=0 ; m<4 && tasks[0].accuracy < 1.0 ; m++) {
        // printf("{");for (int i=0; i<qpart ; i++) printf(" %d", tasks[i].id);printf(" }\n");fflush(stdout);
        hpo_mutate(tasks, tasks+qpart, qpart, qpart * (m+1), &rnd);
        hpo_train(ptest->name, tasks+qpart, qpart, reader, &td, gamma, degree, coef0);
        qsort(tasks, qpart * 2, sizeof(HpoTask), f_cmp_tasks);
        hpo_drop(tasks+qpart, qpart);
    }

    // drop all except first
    hpo_drop(tasks+1, qpart-1);

    printf("batch=%d %.8f, %.8f, %.8f, %.8f, %.8f, %2d, %.8f seed=%d acc=%.3f iter=%d\n",
            tasks[0].batch_size, tasks[0].decay, tasks[0].lambda, tasks[0].learning_rate, tasks[0].tolerance,
            tasks[0].gamma, tasks[0].degree, tasks[0].coef0,
            tasks[0].seed, tasks[0].accuracy, tasks[0].model->num_actual_iterations); fflush(stdout);

    // printf("** %d\n", tasks[0].id);
    TestKernels test;
    test.set_model(tasks[0].model);
    *gamma = tasks[0].gamma;
    *degree = tasks[0].degree;
    *coef0 = tasks[0].coef0;
    reader->rescan();
    test.predict(reader);

    return tasks[0].model;
}


///////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////

void test_kernels() {
    char filename[PATH_MAX];
    TestGaussian *ptest = gaussian_tests;
    for (int i=0 ; i<(int)NUM_TEST_GAUSSIAN ; i++, ptest++) {
        printf("RUNNING %s:\n", ptest->name);fflush(stdout);

        sprintf(filename, "/usr1/datasets/reg/reg_%s.csv", ptest->name);
        ReaderCSV reg(filename, false);
        Reader *reader = &reg;

        double gamma = 0.1;
        int degree = 3;
        double coef0 = 1.0;
#ifdef USE_GAUSSIAN
        ReaderGaussian gauss(reader, &gamma);
        reader = &gauss;
#elif defined(USE_POLYNOMIAL)
        ReaderPolynomial poly(reader, &degree, &coef0);
        reader = &poly;
#endif

        reader->open();
        const Model* model = hpo(ptest, reader, 5000, &gamma, &degree, &coef0);
        // TrainingScore *score = lfirst_node(TrainingScore, list_head(model->scores));
        MemoryContextDelete(model->memory_context);
        reader->close();

        // break;
    }

    printf("done.\n");fflush(stdout);
}

