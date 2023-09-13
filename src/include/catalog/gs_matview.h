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
 * gs_matview.h
 *     Definition about catalog of matviews.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_matview.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_MATVIEW_H
#define GS_MATVIEW_H

#include "catalog/genbki.h"
#include "nodes/parsenodes.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define MatviewRelationId   9982
#define MatviewRelationId_Rowtype_Id 9984

typedef enum {
    MATVIEW_NOT = 0,    /* Not a matview related table */
    MATVIEW_MAP,        /* Matviewmap table */
    MATVIEW_LOG         /* Mlog table */
} MvRelationType;

CATALOG(gs_matview,9982) BKI_SCHEMA_MACRO
{
    Oid         matviewid;
    Oid         mapid;
    bool        ivm;
    bool        needrefresh;
    timestamp   refreshtime;
} FormData_gs_matview;

typedef FormData_gs_matview *Form_gs_matview;

#define Natts_gs_matview                  5

#define Anum_gs_matview_matviewid         1
#define Anum_gs_matview_mapid             2
#define Anum_gs_matview_ivm               3
#define Anum_gs_matview_needrefresh       4
#define Anum_gs_matview_refreshtime       5

extern void create_matview_tuple(Oid matviewOid, Oid mapid, bool isIncremental);
extern void update_matview_tuple(Oid matviewOid, bool needrefresh, Datum curtime);
extern void delete_matview_tuple(Oid matviewOid);

extern void insert_matviewdep_tuple(Oid matviewOid, Oid relid, Oid mlogid);
extern void delete_matviewdep_tuple(Oid matviewOid);
extern void delete_matdep_table(Oid mlogid);

extern Datum get_matview_refreshtime(Oid matviewOid, bool *isNUll);
extern Datum get_matview_mapid(Oid matviewOid);
extern bool is_incremental_matview(Oid oid);

extern bool IsMatviewRelationbyOid(Oid relOid, MvRelationType *matviewRelationType);
extern Oid MatviewRelationGetBaseid(Oid relOid, MvRelationType matviewRelationType);
extern Query *get_matview_query(Relation matviewRel);
extern bool CheckPermissionForBasetable(const RangeTblEntry *rte);
extern void CheckRefreshMatview(Relation matviewRel, bool isIncremental);
extern void acquire_mativew_tables_lock(Query *query, bool incremental);
extern bool CheckMatviewQuals(Query *query);
extern Oid FindRoleid(Oid relid);

extern Oid get_matview_mlog_baserelid(Oid mlogOid);

#endif   /* GS_MATVIEW_H */
