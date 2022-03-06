/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
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
 * gs_global_config.cpp
 *
 * IDENTIFICATION
 *     src/common/backend/catalog/gs_global_config.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "access/heapam.h"
#include "nodes/parsenodes_common.h"
#include "access/tableam.h"
#include "access/htup.h"
#include "utils/elog.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "catalog/gs_global_config.h"

void CreateGlobalConfig(DefElem* defel)
{
    HeapTuple htup = NULL;
    Relation relation;
    bool nulls[Natts_gs_global_config];
    Datum values[Natts_gs_global_config];

    relation = heap_open(GsGlobalConfigRelationId, RowExclusiveLock);

    values[Anum_gs_global_config_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(defel->defname));
    nulls[Anum_gs_global_config_name - 1] = false;
    char *config_value = defGetString(defel);
    values[Anum_gs_global_config_value - 1] = DirectFunctionCall1(textin, CStringGetDatum(config_value));
    nulls[Anum_gs_global_config_value - 1] = false;

    htup = (HeapTuple)heap_form_tuple(relation->rd_att, values, nulls);
    (void)simple_heap_insert(relation, htup);
    heap_close(relation, NoLock);
}

void AlterGlobalConfig(AlterGlobalConfigStmt *stmt)
{
    ListCell *option = NULL;
    HeapTuple tup = NULL;
    HeapTuple htup = NULL;
    TableScanDesc scan;
    bool isNull = false;
    bool find;
    if (!initialuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to examine")));
    }
    foreach (option, stmt->options) {
        DefElem *defel = (DefElem *)lfirst(option);
        if (strcmp(defel->defname, "weak_password") == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Don't alter config named 'weak_password'")));
        }
        bool nulls[Natts_gs_global_config];
        Datum values[Natts_gs_global_config];
        bool repl[Natts_pg_resource_pool];
        find = false;

        Relation relation = heap_open(GsGlobalConfigRelationId, RowExclusiveLock);
        scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
        while ((tup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Datum datum = heap_getattr(tup, Anum_gs_global_config_name, RelationGetDescr(relation), &isNull);
            if (!isNull && strcmp(defel->defname, DatumGetCString(datum)) == 0) {
                find = true;
                char *config_value = defGetString(defel);
                values[Anum_gs_global_config_value - 1] = DirectFunctionCall1(textin, CStringGetDatum(config_value));
                nulls[Anum_gs_global_config_value - 1] = false;
                repl[Anum_gs_global_config_value - 1] = true;

                repl[Anum_gs_global_config_name - 1] = false;
                htup = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, repl);
                simple_heap_update(relation, &tup->t_self, htup);
                break;
            }
        }
        tableam_scan_end(scan);
        heap_close(relation, NoLock);
        if (!find) {
            CreateGlobalConfig(defel);
        }
    }
}

void DropGlobalConfig(DropGlobalConfigStmt *stmt)
{
    ListCell *item = NULL;
    HeapTuple tup = NULL;
    TableScanDesc scan;
    bool isNull = false;
    bool find;
    if (!initialuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to examine")));
    }
    foreach (item, stmt->options) {
        const char *global_name = strVal(lfirst(item));
        if (strcmp(global_name, "weak_password") == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Don't drop config named 'weak_password'")));
        }
        find = false;

        Relation relation = heap_open(GsGlobalConfigRelationId, RowExclusiveLock);
        scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
        while ((tup = (HeapTuple)tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Datum datum = heap_getattr(tup, Anum_gs_global_config_name, RelationGetDescr(relation), &isNull);
            if (!isNull && strcmp(global_name, DatumGetCString(datum)) == 0) {
                find = true;
                simple_heap_delete(relation, &tup->t_self);
                break;
            }
        }
        tableam_scan_end(scan);
        heap_close(relation, NoLock);
        if (!find) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME), errmsg("Parameter %s not exists, please check it.\n", global_name)));
        }
    }
}

