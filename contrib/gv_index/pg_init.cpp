/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * --------------------------------------------------------------------------------------
 *
 * pg_init.cpp
 *
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/pg_init.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "pg_init.h"
#include "fmgr.h"
#include "postgres.h"
#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "nodes/nodes.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "utils.h"

relopt_kind gv_graph_kind_id;
extern void gv_graph_index_init(void);

/*
 * Initialize index options and variables.
 * Called when the extension library is first loaded.
 */

PG_MODULE_MAGIC;

PGDLLEXPORT void _PG_init(void);

void _PG_init(void)
{
    FAST_NOTICE("gv_graph: _PG_init start");
    gv_graph_kind_id = add_reloption_kind();
    gv_graph_index_init();
    FAST_NOTICE("gv_graph: _PG_init done: gv_graph_kind_id=%d", (int)gv_graph_kind_id);
}