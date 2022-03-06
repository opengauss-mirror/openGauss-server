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
 *        src/test/ut/include/gunit_test.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GTEST_BACKEND_H
#define GTEST_BACKEND_H

#include "gtest/gtest.h"

#define GUNIT_TEST_SUITE(test_fixture) \
public:                                \
    test_fixture(){};                  \
    virtual ~test_fixture(){};

#define GUNIT_TEST_REGISTRATION(test_fixture, test_name) \
    TEST_F(test_fixture, test_name)                      \
    {                                                    \
        test_fixture##_##test_name##_Test().test_name(); \
    }

#endif
