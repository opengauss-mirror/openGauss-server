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
 * ut_direct_ml.h
 *        Header file of directML test master File
 *
 *
 * IDENTIFICATION
 *        src/test/ut/db4ai/ut_direct_ml.h
 *
 * ---------------------------------------------------------------------------------------
**/
#ifndef UT_DIRECT_ML_H
#define UT_DIRECT_ML_H

#include "gunit_test.h"
#include "mockcpp/mockcpp.hpp"

#include "direct_ml/readers.h"
#include "direct_ml/mock.h"

#define PATH_MAX 4096
void test_kernels();
void test_readers();

class UTDirectML : public testing::Test {

    GUNIT_TEST_SUITE(UTDirectML);

public:
    virtual void SetUp();
    virtual void TearDown();

public:
    void TestLogisticRegression();
    void TestSvmKernel();
    void TestSvmMulticlass();
    void TestKMeans();
    void TestRCA();
    void TestXGBoost();
};

#endif
