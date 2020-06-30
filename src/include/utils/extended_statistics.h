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
 * extended_statistics.h
 *        Extended statistics and selectivity estimation functions.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/extended_statistics.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef EXTENDED_STATISTICS_H
#define EXTENDED_STATISTICS_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_statistic.h"
#include "commands/vacuum.h"
#include "nodes/parsenodes.h"
#include "utils/catcache.h"
#include "utils/relcache.h"

#define ES_LOGLEVEL DEBUG3
#define ES_COMMIT_VERSION 91112

#define ES_MULTI_COLUMN_STATS_ATTNUM -101
#define MAX_NUM_MULTI_COLUMN_STATS 1000
#define ES_MAX_FETCH_NUM_OF_INSTANCE 100

typedef enum ES_STATISTIC_KIND {
    ES_SEARCH = 0x00u,
    ES_NULLFRAC = 0x01u,
    ES_WIDTH = 0x02u,
    ES_DISTINCT = 0x04u,
    ES_DNDISTINCT = 0x08u,
    ES_MCV = 0x10u,
    ES_NULL_MCV = 0x20u,
    ES_ALL = 0x3fu
} ES_STATISTIC_KIND;

typedef enum ES_COLUMN_NAME_ALIAS { ES_COLUMN_NAME = 0x01u, ES_COLUMN_ALIAS = 0x02u } ES_COLUMN_NAME_ALIAS;

typedef struct ExtendedStats {
    Bitmapset* bms_attnum;
    float4 nullfrac;
    int4 width;
    float4 distinct;
    float4 dndistinct;
    Datum* mcv_values = NULL;
    bool* mcv_nulls = NULL;
    int mcv_nvalues;
    float4* mcv_numbers = NULL;
    int mcv_nnumbers;
    float4* other_mcv_numbers = NULL;
    int other_mcv_nnumbers;
} ExtendedStats;

extern void es_free_extendedstats(ExtendedStats* es);
extern void es_free_extendedstats_list(List* es_list);

extern char es_get_starelkind();
extern bool es_get_stainherit();

extern List* es_attnum_bmslist_add_unique_item(List* bmslist, Bitmapset* bms_attnum);

extern Form_pg_type es_get_attrtype(Oid typeoid);
extern void es_get_column_typid_typmod(Oid relid, int2 attnum, Oid* atttypid, int* atttypmod);
extern void es_get_columns_typid_typmod(
    Oid relid, int2vector* stakey, Oid** p_atttypid_array, int** p_atttypmod_array, unsigned int* p_num_column);
extern Oid es_get_typinput(Oid typeoid);

extern void es_get_attnum_of_statistic_items(
    Relation rel, VacuumStmt* vacstmt, Bitmapset** p_bms_single_column, List** p_list_multi_column);
extern void es_check_alter_table_statistics(Relation rel, AlterTableCmd* cmd);
extern Bitmapset* es_get_attnums_to_analyze(VacAttrStats** vacattrstats, int vacattrstats_size);

extern VacAttrStats* es_make_vacattrstats(unsigned int num_attrs);

extern bool es_is_valid_column_to_analyze(Form_pg_attribute attr);
extern bool es_is_type_supported_by_cstore(VacAttrStats* stats);
extern bool es_is_type_distributable(VacAttrStats* stats);

extern void es_check_availability_for_table(VacuumStmt* stmt, Relation rel, bool inh, bool* replicate_needs_extstats);

extern bool es_is_variable_width(VacAttrStats* stats);
extern bool es_is_not_null(AnalyzeSampleTableSpecInfo* spec);
extern bool es_is_distributekey_contained_in_multi_column(Oid relid, VacAttrStats* stats);

/*
 * Analyze query
 */
extern char* es_get_column_name_alias(AnalyzeSampleTableSpecInfo* spec, uint32 name_alias_flag,
    const char* separator = ", ", const char* prefix = "", const char* postfix = "");

/*
 * System table
 */
extern Bitmapset* es_get_multi_column_attnum(HeapTuple tuple, Relation relation);
extern List* es_explore_declared_stats(Oid relid, VacuumStmt* vacstmt, bool inh);
extern Datum es_mcv_slot_cstring_array_to_array_array(
    Datum cstring_array, unsigned int num_column, const Oid* atttypid_array, const int* atttypmod_array);
extern VacAttrStats** es_build_vacattrstats_array(
    Relation rel, List* bmslist_multicolumn, bool add_or_delete, int* array_length, bool inh);
extern ArrayType* es_construct_mcv_value_array(VacAttrStats* stats, int mcv_slot_index);
extern bool es_is_multicolumn_stats_exists(Oid relid, char relkind, bool inh, Bitmapset* bms_attnums);

/*
 * Interfaces for optimizer
 */
extern float4 es_get_multi_column_distinct(Oid relid, char relkind, bool inh, Bitmapset* bms_attnums);
extern ExtendedStats* es_get_multi_column_stats(
    Oid relid, char relkind, bool inh, Bitmapset* bms_attnums, bool has_null = false);
extern List* es_get_multi_column_stats(Oid relid, char relkind, bool inh, int* num_stats, bool has_null = false);
extern void es_split_multi_column_stats(List* va_list, List** va_cols, List** va_cols_multi);

#endif /* EXTENDED_STATISTICS_H */
