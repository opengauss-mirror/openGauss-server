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
 * ut_direct_ml.cpp
 *       DirectML test master File
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/ut_direct_ml.cpp
 *
 * ---------------------------------------------------------------------------------------
**/
#include "ut_direct_ml.h"

#include <postgres.h>
#include <db4ai/fp_ops.h>
#include "direct_ml/direct_algos.h"
#include "direct_ml/readers.h"

GUNIT_TEST_REGISTRATION(UTDirectML, TestLogisticRegression);
GUNIT_TEST_REGISTRATION(UTDirectML, TestSvmKernel);
GUNIT_TEST_REGISTRATION(UTDirectML, TestSvmMulticlass);
GUNIT_TEST_REGISTRATION(UTDirectML, TestKMeans);
GUNIT_TEST_REGISTRATION(UTDirectML, TestRCA);
GUNIT_TEST_REGISTRATION(UTDirectML, TestXGBoost);

void UTDirectML::SetUp()
{
    // to be compatible with openGauss' default configuration
    pg_mock_init(NOTICE, 64);
}

void UTDirectML::TearDown() {}

void UTDirectML::TestLogisticRegression()
{
    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");
    Hyperparameters hps;
    Descriptor td;

    hps.add_int("batch_size", 20);
    hps.add_float("decay", 1.0);
    hps.add_float("learning_rate", 5.0);
    hps.add_int("max_iterations", 30);
    hps.add_float("tolerance", 1e-7);
    hps.add_int("seed", 1);
    hps.add_enum("optimizer", "ngd");

    td.coltypes[0] = BOOLOID;
    td.coltypes[1] = INT4OID;
    td.colbyvals[0] = td.colbyvals[1] = true;
    td.coltyplen[0] = td.coltyplen[1] = sizeof(int32_t);
    td.count = 2;

    strcpy(filetrain, datapath);
    strcat(filetrain, "patients.txt");
    strcpy(filepredict, filetrain);
    ReaderCSV patients(filetrain, false);

    Datum proj_patients_anxiety[] = {
        DP_PROJECT_COLUMN(1), DP_PROJECT_INT4(1), DP_PROJECT_CALL(int4eq), DP_PROJECT_RETURN,
        DP_PROJECT_COLUMN(3), DP_PROJECT_RETURN,  DP_PROJECT_RETURN,
    };
    ReaderProjection patients_anxiety(&patients, proj_patients_anxiety);

    Tester<LogisticRegressionDirectAPI> logisticRegressionModel;
    patients_anxiety.open();
    logisticRegressionModel.run(&hps, &td, "patients_logregr", EXPLAIN_FORMAT_JSON, &patients_anxiety);
    patients_anxiety.close();
}

void UTDirectML::TestSvmKernel()
{
    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");
    Hyperparameters hps;
    Descriptor td;

    td.coltypes[0] = INT4OID;
    td.colbyvals[0] = true;
    td.coltyplen[0] = 4;
    for (int c = 1; c < 2; c++) {
        td.coltypes[c] = FLOAT4OID;
        td.colbyvals[c] = true;
        td.coltyplen[c] = 4;
    }
    td.count = 3;

    strcpy(filetrain, datapath);
    strcat(filetrain, "moons.csv");

    // linear
    {
        ReaderCSV moons_rd(filetrain, false);
        hps.reset();
        hps.add_int("batch_size", 8);
        hps.add_float("decay", 1e-20);
        hps.add_float("lambda", 920.90725960);
        hps.add_float("learning_rate", 0.01215337);
        hps.add_float("tolerance", 0.06377824);
        hps.add_int("seed", 54);
        hps.add_int("max_iterations", 2);
        hps.add_enum("kernel", "linear");

        Tester<SvmcDirectAPI> svmLinear;
        moons_rd.open();
        svmLinear.run(&hps, &td, "moons_linear", EXPLAIN_FORMAT_JSON, &moons_rd);
        moons_rd.close();
    }

    // gaussian
    {
        ReaderCSV moons_rd(filetrain, false);

        hps.reset();
        hps.add_int("batch_size", 4);
        hps.add_float("decay", 0.80858937);
        hps.add_float("lambda", 274.28986109);
        hps.add_float("learning_rate", 0.16556385);
        hps.add_float("tolerance", 0.00714786);
        hps.add_float("gamma", 0.96736585);
        hps.add_int("seed", 1);
        hps.add_int("max_iterations", 33);
        hps.add_enum("kernel", "gaussian");

        Tester<SvmcDirectAPI> svmGaussian;
        moons_rd.open();
        svmGaussian.run(&hps, &td, "moons_gaussian", EXPLAIN_FORMAT_JSON, &moons_rd);
        moons_rd.close();
    }

    // polynomial
    {
        ReaderCSV moons_rd(filetrain, false);

        hps.reset();
        hps.add_int("batch_size", 2);
        hps.add_float("decay", 0.87908244);
        hps.add_float("lambda", 53.75794302);
        hps.add_float("learning_rate", 0.40456318);
        hps.add_float("tolerance", 0.00003070);
        hps.add_int("degree", 4);
        hps.add_float("coef0", 1.11311435);
        hps.add_int("seed", 1);
        hps.add_int("max_iterations", 100);
        hps.add_enum("kernel", "polynomial");

        Tester<SvmcDirectAPI> svmPolynomial;
        moons_rd.open();
        svmPolynomial.run(&hps, &td, "moons_polynomial", EXPLAIN_FORMAT_JSON, &moons_rd);
        moons_rd.close();
    }
}

void UTDirectML::TestSvmMulticlass()
{
    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");
    Hyperparameters hps;
    Descriptor td;

    hps.reset();
    hps.add_float("lambda", 50.0);
    hps.add_float("learning_rate", 2.0);
    hps.add_float("tolerance", 1e-7);
    hps.add_int("max_iterations", 1000);

    td.coltypes[0] = BOOLOID;
    td.colbyvals[0] = true;
    td.coltyplen[0] = 1;
    for (int c = 1; c < 8; c++) {
        td.coltypes[c] = FLOAT4OID;
        td.colbyvals[c] = true;
        td.coltyplen[c] = 4;
    }
    td.count = 8;

    strcpy(filetrain, datapath);
    strcat(filetrain, "ecoli.csv");
    ReaderCSV ecoli_rd(filetrain, false);

    Datum proj_ecoli[] = {
        DP_PROJECT_COLUMN(7), DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(0), DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(1),
        DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(2), DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(3), DP_PROJECT_RETURN,
        DP_PROJECT_COLUMN(4), DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(5), DP_PROJECT_RETURN,    DP_PROJECT_COLUMN(6),
        DP_PROJECT_RETURN,    DP_PROJECT_RETURN,
    };
    ReaderProjection ecoli(&ecoli_rd, proj_ecoli);

    Tester<MulticlassDirectAPI> svmMulticlassModel;
    ecoli.open();
    ecoli_rd.shuffle(time(NULL));
    svmMulticlassModel.run(&hps, &td, "ecoli_svm_multiclass", EXPLAIN_FORMAT_JSON, &ecoli);

    hps.reset();
    hps.add_float("learning_rate", 30.0);
    hps.add_float("tolerance", 1e-7);
    hps.add_int("max_iterations", 1000);
    hps.add_enum("classifier", "logistic_regression");

    Tester<MulticlassDirectAPI> logregMulticlassModel;
    ecoli.rescan();
    ecoli_rd.shuffle(time(NULL));
    logregMulticlassModel.run(&hps, &td, "ecoli_logreg_multiclass", EXPLAIN_FORMAT_JSON, &ecoli);
    ecoli.close();
}

void UTDirectML::TestKMeans()
{
    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");
    Hyperparameters hps;
    Descriptor td;

    hps.reset();
    hps.add_int("max_iterations", 50);
    hps.add_int("num_centroids", 10);
    hps.add_float("tolerance", 0.00001);
    hps.add_int("batch_size", 1000);
    hps.add_int("num_features", 7);
    hps.add_enum("distance_function", "L2_Squared");
    hps.add_enum("seeding_function", "Random++");
    hps.add_int("verbose", 2);
    hps.add_int("seed", 1255025990);

    td.reset();
    td.coltypes[0] = FLOAT8ARRAYOID;
    td.colbyvals[0] = FLOAT8PASSBYVAL;
    td.coltyplen[0] = -1;
    td.count = 1;

    strcpy(filetrain, datapath);
    strcpy(filepredict, datapath);
    strcat(filetrain, "kmeans_7_1000_10_train.txt");
    strcat(filepredict, "kmeans_7_10_predict.txt");

    ReaderCSV kmeans_training_coordinates_file(filetrain, false);
    ReaderCSV kmeans_prediction_coordinates_file(filepredict, false);
    ReaderVectorFloat kmeans_train_coordinates(&kmeans_training_coordinates_file);
    ReaderVectorFloat kmeans_prediction_coordinates(&kmeans_prediction_coordinates_file);
    kmeans_train_coordinates.open();
    kmeans_prediction_coordinates.open();

    Tester<KMeansDirectAPI> kMeansModel;
    kMeansModel.run(&hps, &td, "my_kmeans_pp_l2_sqr_fastcheck", EXPLAIN_FORMAT_JSON, &kmeans_train_coordinates,
                    &kmeans_prediction_coordinates);
    kmeans_train_coordinates.close();
    kmeans_prediction_coordinates.close();
}

void UTDirectML::TestRCA()
{
    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");
    Hyperparameters hps;
    Descriptor td;

    hps.reset();
    hps.add_int("number_components", 7);
    hps.add_float("tolerance", 0.0001);
    hps.add_int("batch_size", 1000);
    hps.add_int("max_iterations", 10);
    hps.add_int("seed", 96176977);

    td.reset();
    td.coltypes[0] = FLOAT8ARRAYOID;
    td.colbyvals[0] = FLOAT8PASSBYVAL;
    td.coltyplen[0] = -1;
    td.count = 1;

    strcpy(filetrain, datapath);
    strcpy(filepredict, datapath);
    strcat(filetrain, "pca_7_1000_train.txt");
    strcat(filepredict, "pca_7_1000_predict.txt");

    ReaderCSV pca_training_coordinates_file(filetrain, false);
    ReaderCSV pca_prediction_coordinates_file(filepredict, false);
    ReaderVectorFloat pca_train_coordinates(&pca_training_coordinates_file);
    ReaderVectorFloat pca_prediction_coordinates(&pca_prediction_coordinates_file);
    pca_train_coordinates.open();
    pca_prediction_coordinates.open();

    printf("configuration tested: %s\n", filetrain);

    Tester<PrincipalComponentsDirectAPI> pcaModel;
    pcaModel.run(&hps, &td, "pca_fit_7_7_1000_fastcheck", EXPLAIN_FORMAT_JSON, &pca_train_coordinates,
                 &pca_prediction_coordinates);
    pca_train_coordinates.close();
    pca_prediction_coordinates.close();
}

void UTDirectML::TestXGBoost()
{

    char filetrain[PATH_MAX];
    char filepredict[PATH_MAX];
    char datapath[PATH_MAX];
    Hyperparameters hps;
    Descriptor td;

    char *gauss_home = gs_getenv_r("GAUSSHOME");
    strcpy(datapath, gauss_home);
    strcat(datapath, "/ut_bin/data/");

    hps.reset();
    hps.add_int("n_iter", 10);
    hps.add_int("batch_size", 1000000);
    hps.add_int("nthread", 4);
    hps.add_int("seed", 3141);

    td.reset();
    td.coltypes[0] = INT4OID;
    td.coltypes[1] = INT4OID;
    td.colbyvals[0] = td.colbyvals[1] = INT4OID;
    td.coltyplen[0] = td.coltyplen[1] = sizeof(int32_t);
    td.count = 2;

    strcpy(filetrain, datapath);
    strcat(filetrain, "rain.txt");

    ReaderCSV rain_csv(filetrain, false);
    Datum proj_rain[] = {
        DP_PROJECT_COLUMN(16), DP_PROJECT_RETURN, DP_PROJECT_COLUMN(17), DP_PROJECT_RETURN, DP_PROJECT_RETURN,
    };
    ReaderProjection rain(&rain_csv, proj_rain);
    rain.open();

    Tester<XGBoostDirectAPI> xgModel;
    xgModel.run(&hps, &td, "xgboost_fastcheck", EXPLAIN_FORMAT_JSON, &rain);
    rain.close();
}