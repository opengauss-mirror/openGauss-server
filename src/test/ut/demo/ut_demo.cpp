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
#include "utils/aset.h"
#include "utils/memutils.h"
#include "utils/palloc.h"



using namespace std;

GUNIT_TEST_REGISTRATION(ut_demo, TestCase01)
GUNIT_TEST_REGISTRATION(ut_demo, TestCase02)

void ut_demo::SetUp() {}

void ut_demo::TearDown() {}

/* TestCase01 */
extern int teststr(int abc);
extern int testmock(int mocka, int mockb);

int my_teststr(int abc) { return 0; }

static void CommProxyBaseInit() {
    MemoryContextInit();
    knl_thread_init(COMM_PROXYER);
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProgName = "CommProxy";
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    pg_timezone_initialize();
    CurrentMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "knn_mem_context", ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE * 10);

    return;
}

void ut_demo::TestCase01() {
    ASSERT_EQ(3, testmock(1, 2));
    MOCKER(teststr).stubs().will(invoke(my_teststr));
    ASSERT_EQ(0, testmock(2, 3));
    GlobalMockObject::reset();
    ASSERT_EQ(10, testmock(5, 5));
}

void ut_demo::TestCase02() {
    CommProxyBaseInit();
    const char* src = "QUOTATION_SEQ";
    int max_len = strlen("\"") + strlen(src) + strlen("\"") + 1;
    char *tmp_buffer = (char *)palloc0(max_len);
    int rt = sprintf_s(tmp_buffer, max_len, "\"%s\"", src);
    ASSERT_EQ(15, rt);
    securec_check_ss(rt, "\0", "\0");
}
