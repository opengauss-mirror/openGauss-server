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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/test/ut/demo/ut_demo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UT_DEMO_H
#define UT_DEMO_H

#include "gunit_test.h"
#include "mockcpp/mockcpp.hpp"

class ut_demo : public testing::Test {
    GUNIT_TEST_SUITE(ut_demo);

   public:
    virtual void SetUp();

    virtual void TearDown();

   public:
    void TestCase01();
    void TestCase02();
};

int teststr(int abc) { return 20; }

int testmock(int mocka, int mockb) {
    int testa = 10;
    int testb = teststr(testa);
    if (testb == 20)
        return mocka + mockb;
    else
        return 0;
}

#endif
