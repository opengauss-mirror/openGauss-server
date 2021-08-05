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
 * deploy.sql
 *    Deploy DB4AI.Snapshot functionality.
 *
 * IDENTIFICATION
 *    src/gausskernel/dbmind/db4ai/snapshots/deploy.sql
 *
 * -------------------------------------------------------------------------
 */

-- db local objects
\set ON_ERROR_STOP 1

BEGIN;

\ir schema.sql
\ir create.sql
\ir prepare.sql
\ir sample.sql
\ir publish.sql
\ir purge.sql

COMMIT;
