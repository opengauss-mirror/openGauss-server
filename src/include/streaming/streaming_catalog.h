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
 * cont_query.h
 *        Head file for streaming engine cont_query.
 *
 *
 * IDENTIFICATION
 *        src/distribute/kernel/extension/streaming/include/cont_query.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STREAMING_INCLUDE_STREAMING_CATALOG_H_
#define STREAMING_INCLUDE_STREAMING_CATALOG_H_

#include "postgres.h"
#include "nodes/primnodes.h"
#include "nodes/parsenodes.h"

#define CVNAMELEN 80
const char TTL_OPTION[] = "ttl_interval";
const char WINDOW_OPTION[] = "sw_interval";
const char COLUMN_OPTION[] = "time_column";
const char ORIENT_OPTION[] = "orientation";
const char COMPRESS_OPTION[] = "compression";
const char GATHER_OPTION[] = "gather_interval";
const char PERIOD_OPTION[] = "partition_interval";
const char DEACTIVE_TIME_OPTION[] = "deactive_time";
const char STRING_OPTIMIZE_OPTION[] = "string_optimize";

const char ORIENT_COLUMN = 'c'; /* streaming_cont_query: type = 'c' */
const char ORIENT_ROW = 'r'; /* streaming_cont_query: type = 'r' */
const char ORIENT_COLUMN_PARTITION = 'p'; /* streaming_cont_query: type = 'p' */
const int16 TUMBLING_WINDOW = 0; /* streaming_cont_query: step_factor = 0 */
const int16 SLIDING_WINDOW = 1; /* streaming_cont_query: step_factor = 1 */
const char STRING_OPTIMIZE_ON[] = "on";
const char STRING_OPTIMIZE_OFF[] = "off";

const int MAX_VERSIONS = 10;
const char INIT_VERSION = '0';
const int MIN_TTL = 5;
const int MAX_CQS = 1024;
const int STREAMING_EXEC_CONTINUOUS = 0x100000;
const int DEFAULT_PARTITION_NUM = 10; /* default partition numbers when partition table is created */ 
const int DEFAULT_NAME_LEN = 64; /* default and auto partition name length */
const int PERIOD_OFF = -1;  /* represention for no partition_interval parameter */ 
const int PERIOD_MULT = 5; /* detection interval for  auto-partition */
const int GATHER_OFF = -1; /* represention for no gather_interval parameter */
const int PARTITION_INTERVAL_MAX = 1; /* partition_interval max value (day) */
const int PARTITION_INTERVAL_MIN = 30; /* partition_interval min value (minute) */
const int MSECS_PER_SECS = 1000; /* Milliseconds per second */

typedef struct ContQuery {
    Oid id;                 /*CV ID*/
    RangeVar *name;         /*CV name*/
    Oid oid;                /*tuple oid*/ 
    bool active;            /*is this CV active*/

    Oid relid;              /*overlay view oid*/
    Oid defrelid;           /*definiton view oid*/
    char *sql;              /*cont query's plain sql*/
    Query *cvdef;           /*def view's query tree*/
    Oid matrelid;           /*matrel oid*/
    Oid streamrelid;        /*stream oid*/
    Oid dictrelid;          /*stream oid*/

    RangeVar *matrel;       /*matrel name*/
    Oid lookupidxid;        /*group lookupidx oid*/
    AttrNumber ttl_attno;   /*ttl attrNumber*/
    int ttl;                /*ttl*/
    int period;             /*period*/
    int gather;             /* gather (second) */
    int gather_window_interval; /* gather_window_interval (min) */

    FuncExpr *hash;         /*func expr*/
    char type;              /*CV type*/
} ContQuery;

typedef struct StreamingObjectContext {
    List *streams;
} StreamingObjectContext;

Query *get_cont_query_def(Oid def_relid);
ContQuery *get_cont_query_for_id(Oid id);
Oid get_defrelid_by_tab_relid(Oid tab_relid);
Oid get_dictrelid_by_tab_relid(Oid tab_relid);
bool rangevar_is_cq_with_dict(RangeVar *rv);
bool is_streaming_thread();
Oid get_cqid_by_dict_relid(Oid relid);
Oid get_dictrelid_by_cqid(Oid cqid);

void streaming_context_set_is_ddl(void);
bool streaming_context_is_ddl(void);
void streaming_context_set_is_def_rel(bool);
void streaming_context_set_group_search(void);
bool streaming_context_is_group_search(void);
bool is_def_rel(void);

bool is_group_column_by_colname(const char *colname, Oid relid);
bool is_group_column_by_attrnum(int attrnum, Oid relid);
bool is_rte_cq(List *rtable);
bool is_streaming_string_optimze_on(Oid relid);

bool relid_is_stream(Oid relid);
bool range_var_is_stream(const RangeVar *rv, bool missing_ok);
bool range_var_is_cont_view(RangeVar *name);
bool relid_is_cv_tab(Oid relid);
bool view_stmt_has_stream(ViewStmt *view_stmt);
bool range_var_list_include_streaming_object(List *objects);
bool range_var_is_cv_tab(RangeVar *name);

char *cv_name_to_mrel_name(const char *cvname);
#endif
