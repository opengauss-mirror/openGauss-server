/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * gms_stats.cpp
 *  gms_stats can effectively estimate statistical data.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_stats/gms_stats.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"

#include "access/skey.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "commands/sqladvisor.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "gms_stats.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gs_analyze_schema_tables);

static List* GetRelationsInSchema(char *namespc)
{
    Relation pg_class_rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    char* relname = NULL;
    List* tbl_relnames = NIL;
    int len;
    Oid nspid;

   nspid = get_namespace_oid(namespc, true);
    if (!OidIsValid(nspid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA), errmsg("schema \"%s\" does not exists", namespc)));

    ScanKeyInit(&skey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspid));
    pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
    sysscan = systable_beginscan(pg_class_rel, ClassNameNspIndexId, true, SnapshotNow, 1, skey);
    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
        if (reltup->relkind == RELKIND_RELATION || reltup->relkind == RELKIND_MATVIEW) {
            len = strlen(reltup->relname.data);
            relname = (char *) palloc(len + 1);
            errno_t rc = strcpy_s(relname, len + 1, reltup->relname.data);
            securec_check(rc, "\0", "\0");
            tbl_relnames = lappend(tbl_relnames, relname);
        }
    }
    systable_endscan(sysscan);
    heap_close(pg_class_rel, AccessShareLock);
    return tbl_relnames;
}

static void analyze_tables(char *namespc, List *relnames_list)
{
    StringInfo execute_sql;
    ListCell* lc;
    VacuumStmt* stmt;
    execute_sql = makeStringInfo();
    foreach(lc, relnames_list)
    {
        char* relnames = (char*)lfirst(lc);
        appendStringInfo(execute_sql, "ANALYZE  %s.%s;", quote_identifier(namespc), quote_identifier(relnames));

        List* parsetree_list = NULL;
        ListCell* parsetree_item = NULL;
        parsetree_list = raw_parser(execute_sql->data, NULL);
        foreach (parsetree_item, parsetree_list) {
            Node* parsetree = (Node*)lfirst(parsetree_item);
            stmt = (VacuumStmt*)parsetree;
        }
        vacuum(stmt, InvalidOid, true, NULL, true);
        pfree_ext(relnames);
        list_free(parsetree_list);
        resetStringInfo(execute_sql);
    }
    DestroyStringInfo(execute_sql);
}

Datum
gs_analyze_schema_tables(PG_FUNCTION_ARGS)
{
    char *schema_name = text_to_cstring(PG_GETARG_TEXT_P(0));
    List* relnames_list;

    relnames_list = GetRelationsInSchema(schema_name);
    analyze_tables(schema_name, relnames_list);

    pfree_ext(schema_name);
    list_free(relnames_list);

    PG_RETURN_VOID();
}

