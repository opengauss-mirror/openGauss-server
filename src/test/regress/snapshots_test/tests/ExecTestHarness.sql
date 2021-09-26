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
 * -------------------------------------------------------------------------
 *
 * ExecTestHarness.sql
 *    DB4AI.Snapshot testing infrastructure.
 *
 *
 * -------------------------------------------------------------------------
 */

BEGIN;

SET ROLE _db4ai_test PASSWORD 'gauss@123';

SET db4ai.message_level = NOTICE;

\echo '\nIMPORTING TEST CASE:' :TEST_CASE
\i :TEST_CASE

:TEST_BREAK_POINT

\echo '\nINVOKING TEST CASE:' :TEST_CASE
SELECT _db4ai_test.test();

:TEST_BREAK_POINT

RESET ROLE;

ROlLBACK;
