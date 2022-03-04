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
 *        src/test/ut/demo/ut_demo.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "ut_demo.h"

#include <malloc.h>

#include <iostream>


using namespace std;

GUNIT_TEST_REGISTRATION(ut_demo, TestCase01)

void ut_demo::SetUp() {}

void ut_demo::TearDown() {}

/* TestCase01 */
extern int teststr(int abc);
extern int testmock(int mocka, int mockb);

int my_teststr(int abc) { return 0; }

void ut_demo::TestCase01() {
    ASSERT_EQ(3, testmock(1, 2));
    MOCKER(teststr).stubs().will(invoke(my_teststr));
    ASSERT_EQ(0, testmock(2, 3));
    GlobalMockObject::reset();
    ASSERT_EQ(10, testmock(5, 5));
}

