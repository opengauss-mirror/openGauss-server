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
 * extended_statistics.cpp
 *     Extended statistics and selectivity estimation functions.
 *
 * Portions Copyright (c) 2018, Huawei Tech. Co., Ltd.
 *
 * IDENTIFICATION
 *     src/common/backend/utils/adt/extended_statistics.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/extended_statistics.h"

#include "access/htup.h"
#include "access/relscan.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_statistic_ext.h"
#include "funcapi.h"
#include "nodes/print.h"
#include "nodes/value.h"
#include "optimizer/streamplan.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgxc/pgxc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/*
 * The max attribute number of multi-column statistic
 *
 * This value should less than 448,
 *     for the storage space limitation of index on pg_statistic
 */
#define ES_MAX_COLUMN_SIZE 32

static void es_error_column_not_exist(const char* relation_name, const char* column_name);
static bool es_is_supported_replicate(VacuumStmt* stmt, Oid relid);
static bool es_has_es_requirement(VacuumStmt* stmt, Oid relid, bool inh);
static int2vector* es_get_att_vector(Bitmapset* attnum);

/*========================================================*/
/* Static functions                                       */
/*========================================================*/

/*
 * es_error_column_not_exist
 *     report error when column is invalid
 *
 * @param (in) relation_name:
 *     name of the relation
 * @param (in) column_name:
 *     the invalid column's name
 */
static void es_error_column_not_exist(const char* relation_name, const char* column_name)
{
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_COLUMN),
            errmsg("column \"%s\" of relation \"%s\" does not exist", column_name, relation_name)));
}

/*========================================================*/
/* Normal functions                                       */
/*========================================================*/

/*
 * es_free_extendedstats
 *     free an ExtendedStats structure
 *
 * @param (in) es:
 *     the ExtendedStats structure to be freed
 */
void es_free_extendedstats(ExtendedStats* es)
{
    if (NULL == es) {
        return;
    }

    bms_free(es->bms_attnum);
    if (es->mcv_values) {
        pfree_ext(es->mcv_values);
    }
    if (es->mcv_numbers) {
        pfree_ext(es->mcv_numbers);
    }
    if (es->mcv_nulls) {
        pfree_ext(es->mcv_nulls);
    }
    if (es->other_mcv_numbers) {
        pfree_ext(es->other_mcv_numbers);
    }

    pfree_ext(es);
}

/*
 * es_free_extendedstats_list
 *     free a list of ExtendedStats structure
 *
 * @param (in) es_list:
 *     the ExtendedStats structure list
 */
void es_free_extendedstats_list(List* es_list)
{
    if (NULL == es_list) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, es_list) {
        ExtendedStats* es = (ExtendedStats*)lfirst(lc);
        es_free_extendedstats(es);
    }

    list_free(es_list);
}

/*
 * es_attnum_bmslist_add_unique_item
 *     add a Bitmapset to a Bitmapset list, keep the list is unique
 *
 * @param (in) bmslist:
 *     the Bitmapset list
 * @param (in) bms_attnum:
 *     the Bitmapset
 *
 * @return:
 *     the result Bitmapset list
 */
List* es_attnum_bmslist_add_unique_item(List* bmslist, Bitmapset* bms_attnum)
{
    ListCell* lc = NULL;
    foreach (lc, bmslist) {
        Bitmapset* bms_exists = (Bitmapset*)lfirst(lc);
        if (bms_equal(bms_exists, bms_attnum)) {
            return bmslist;
        }
    }

    List* ans = lappend(bmslist, bms_attnum);
    return ans;
}

/*
 * es_get_attrtype
 *     get pg_type tuple for a type oid
 *
 * @param (in) typeoid:
 *     the type oid
 *
 * @return:
 *     the pg_type tuple
 */
Form_pg_type es_get_attrtype(Oid typeoid)
{
    HeapTuple typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeoid));
    if (!HeapTupleIsValid(typtuple)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u", typeoid)));
    }
    Form_pg_type attrtype = (Form_pg_type)GETSTRUCT(typtuple);
    return attrtype;
}

/*
 * es_get_column_typid_typmod
 *     get 'typid' and 'typmod' value for a column in a relation
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) attnum:
 *     the column attnum
 * @param (out) p_atttypid:
 *     the 'typid' of the column
 * @param (out) p_atttypmod:
 *     the 'typmod' of the column
 */
void es_get_column_typid_typmod(Oid relid, int2 attnum, Oid* p_atttypid, int* p_atttypmod)
{
    HeapTuple attTup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int32GetDatum(attnum));
    if (!HeapTupleIsValid(attTup))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u, in node [%s]",
                    attnum,
                    relid,
                    g_instance.attr.attr_common.PGXCNodeName)));

    Form_pg_attribute attForm = (Form_pg_attribute)GETSTRUCT(attTup);

    /*
     * If a drop column operation happened between fetching sample rows and updating
     * pg_statistic, we don't have to further processing the dropped-column's stats
     * update, so just skip.
     */
    if (attForm->attisdropped) {
        elog(WARNING,
            "relation:%s's attnum:%d is droppred during ANALYZE, so skipped.",
            get_rel_name(attForm->attrelid),
            attForm->attnum);

        ReleaseSysCache(attTup);
        return;
    }

    *p_atttypid = attForm->atttypid;
    *p_atttypmod = attForm->atttypmod;

    ReleaseSysCache(attTup);
}

/*
 * es_get_columns_typid_typmod
 *     get 'typid' and 'typmod' value for a couple of columns in a relation
 *     especially for multi-column statistics
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) ext_info_str:
 *     the extended stats info string
 * @param (out) p_atttypid_array:
 *     the 'typid' of the columns, in an array
 * @param (out) p_atttypmod_array:
 *     the 'typmod' of the columns, in an array
 * @param (out) p_num_column:
 *     the size of the array
 */
void es_get_columns_typid_typmod(
    Oid relid, int2vector* stakey, Oid** p_atttypid_array, int** p_atttypmod_array, unsigned int* p_num_column)
{
    int num_column = stakey->dim1;
    Oid* atttypid_array = (Oid*)palloc0(sizeof(Oid) * num_column);
    int* atttypmod_array = (int*)palloc0(sizeof(int) * num_column);
    for (int i = 0; i < num_column; ++i) {
        int2 cur_attnum = stakey->values[i];
        es_get_column_typid_typmod(relid, cur_attnum, &(atttypid_array[i]), &(atttypmod_array[i]));
    }

    *p_num_column = num_column;
    *p_atttypid_array = atttypid_array;
    *p_atttypmod_array = atttypmod_array;
}

/*
 * es_get_typinput
 *     get 'typinput' for a type
 *
 * @param (in) typeoid:
 *     the target type oid
 *
 * @return:
 *     the 'typinput'
 */
Oid es_get_typinput(Oid typeoid)
{
    HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type oid %u", typeoid)));
    }

    Form_pg_type type_form = (Form_pg_type)GETSTRUCT(tuple);

    Oid funcoid = type_form->typinput;

    ReleaseSysCache(tuple);

    return funcoid;
}

/*
 * For newly defined extended statistics
 */

/*
 * es_get_attnum_of_statistic_items
 *     parse VacuumStmt into single column and multi-column information
 *
 * @param (in) rel:
 *     the relation
 * @param (in) vacstmt:
 *     the VacuumStmt structure
 * @param (out) p_bms_single_column:
 *     the single column information in Bitmapset
 * @param (out) p_list_multi_column:
 *     the multi-column information in Bitmapset list
 */
void es_get_attnum_of_statistic_items(
    Relation rel, VacuumStmt* vacstmt, Bitmapset** p_bms_single_column, List** p_list_multi_column)
{
    Assert(NULL != vacstmt->va_cols);

    Bitmapset* bms_single_column = NULL;
    List* list_multi_column = NIL;

    ListCell* lc = NULL;
    foreach (lc, vacstmt->va_cols) {
        Node* item = (Node*)lfirst(lc);
        /* Single column */
        if (IsA(item, String)) {
            char* column_name = strVal(item);
            int column_attnum = attnameAttNum(rel, column_name, false);
            if (InvalidAttrNumber != column_attnum) {
                bms_single_column = bms_add_member(bms_single_column, column_attnum);
            } else {
                es_error_column_not_exist(RelationGetRelationName(rel), column_name);
            }
        }
        /* Multi-column */
        else if (IsA(item, List)) {
            List* multi_column_name_list = (List*)item;
            Bitmapset* bms_multi_column = NULL;

            ListCell* lc_multi = NULL;
            foreach (lc_multi, multi_column_name_list) {
                Node* item_multi = (Node*)lfirst(lc_multi);
                Assert(IsA(item_multi, String));
                char* column_name_multi = strVal(item_multi);
                int column_attnum_multi = attnameAttNum(rel, column_name_multi, false);
                if (InvalidAttrNumber != column_attnum_multi) {
                    /*
                     * If we want to analyze single column used by multi-column,
                     * we also need to add column_attnum_multi to bms_single_column
                     * but in current design, we only add column_attnum_multi to bms_multi_column
                     */
                    bms_multi_column = bms_add_member(bms_multi_column, column_attnum_multi);
                } else {
                    es_error_column_not_exist(RelationGetRelationName(rel), column_name_multi);
                }
            }

            int column_size = bms_num_members(bms_multi_column);
            if (column_size < 2) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Multi-column statistic needs at least two columns.")));
            }

            if (column_size > ES_MAX_COLUMN_SIZE) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Multi-column statistic supports at most %d columns.", ES_MAX_COLUMN_SIZE)));
            }

            bool found = false;
            foreach (lc_multi, list_multi_column) {
                Bitmapset* bms_exists = (Bitmapset*)lfirst(lc_multi);
                if (bms_equal(bms_exists, bms_multi_column)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                list_multi_column = lappend(list_multi_column, bms_multi_column);
            }
        } else {
            Assert(false);
        }
    }

    *p_bms_single_column = bms_single_column;
    *p_list_multi_column = list_multi_column;

    return;
}

/*
 * es_check_alter_table_statistics
 *     used by alter table commands to add/delete statistics
 *     recognize columns name and convert to a Bitmapset
 *
 * @param (in) rel:
 *     the relation
 * @param (in & out) cmd:
 *     the AlterTableCmd
 */
void es_check_alter_table_statistics(Relation rel, AlterTableCmd* cmd)
{
    Assert(IsA(cmd->def, List));
    List* new_def = NIL;

    ListCell* lc1 = NULL;
    foreach (lc1, (List*)cmd->def) {
        Node* columns = (Node*)lfirst(lc1);
        Assert(IsA(columns, List));
        Bitmapset* bms_attnums = NULL;

        ListCell* lc2 = NULL;
        foreach (lc2, (List*)columns) {
            Node* col_name = (Node*)lfirst(lc2);
            Assert(IsA(col_name, String));
            char* col_name_str = strVal(col_name);
            int col_attnum = attnameAttNum(rel, col_name_str, false);
            if (InvalidAttrNumber != col_attnum) {
                bms_attnums = bms_add_member(bms_attnums, col_attnum);
                elog(ES_LOGLEVEL, "Define multi column stats, colname[%s] attnum[%d]", col_name_str, col_attnum);
            } else {
                es_error_column_not_exist(RelationGetRelationName(rel), col_name_str);
            }
        }

        int column_size = bms_num_members(bms_attnums);
        if (column_size < 2) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Multi-column statistic needs at least two columns.")));
        }

        if (column_size > ES_MAX_COLUMN_SIZE) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Multi-column statistic supports at most %d columns.", ES_MAX_COLUMN_SIZE)));
        }

        new_def = lappend(new_def, bms_attnums);
    }

    cmd->def = (Node*)new_def;
}

/*
 * es_get_attnums_to_analyze
 *     get all attnums to be analyzed, including single column stats and multi-column stats
 *
 * @param (in) vacattrstats:
 *     the VacAttrStats array
 * @param (in) vacattrstats_size:
 *     the size of the VacAttrStats array
 *
 * @return:
 *     a Bitmapset including all columns' attnum
 */
Bitmapset* es_get_attnums_to_analyze(VacAttrStats** vacattrstats, int vacattrstats_size)
{
    Bitmapset* bms_attnum = NULL;
    for (int i = 0; i < vacattrstats_size; ++i) {
        for (unsigned int j = 0; j < vacattrstats[i]->num_attrs; ++j) {
            int2 attnum = vacattrstats[i]->attrs[j]->attnum;
            bms_attnum = bms_add_member(bms_attnum, attnum);
        }
    }
    return bms_attnum;
}

/*
 * es_make_vacattrstats
 *     to make a VacAttrStats structure
 *
 * @param (in) num_attrs:
 *     number of columns used by this VacAttrStats
 *
 * @return:
 *     the VacAttrStats structure
 */
VacAttrStats* es_make_vacattrstats(unsigned int num_attrs)
{
    Assert(num_attrs >= 1);

    VacAttrStats* stats = (VacAttrStats*)palloc0(sizeof(VacAttrStats));

    stats->num_attrs = num_attrs;
    stats->attrs = (Form_pg_attribute*)palloc0(sizeof(Form_pg_attribute) * stats->num_attrs);
    stats->attrtypid = (Oid*)palloc0(sizeof(Oid) * stats->num_attrs);
    stats->attrtypmod = (int32*)palloc0(sizeof(int32) * stats->num_attrs);
    stats->attrtype = (Form_pg_type*)palloc0(sizeof(Form_pg_type) * stats->num_attrs);

    return stats;
}

/*
 * es_is_valid_column_to_analyze
 *     check wheather the column is valid to analyze
 *
 * @param (in) attr:
 *     the pg_attribute information
 *
 * @return:
 *     identify the validity
 */
bool es_is_valid_column_to_analyze(Form_pg_attribute attr)
{
    /* Never analyze dropped columns */
    if (attr->attisdropped) {
        return false;
    }

    /* Don't analyze column if user has specified not to */
    if (attr->attstattarget == 0) {
        return false;
    }

    return true;
}

/*
 * es_is_supported_replicate
 *     check wheather the table should collect extended statistics
 *     by using query from DN, conditions should be matched
 *     (1) not in cooridinator (in datanode or single node mode)
 *     (2) default_statistics_target set to be negative number
 *     (3) a replicate table
 *     (4) not a system table
 *
 * @param (in) stmt:
 *     the VacuumStmt
 * @param (in) relid:
 *     the relid
 *
 * @return:
 *     the validity
 */
static bool es_is_supported_replicate(VacuumStmt* stmt, Oid relid)
{
    if (IS_PGXC_COORDINATOR) {
        return false;
    }

    if (default_statistics_target > 0) {
        return false;
    }

    bool isReplication = (DISTTYPE_REPLICATION == stmt->disttype);
    if (!isReplication) {
        return false;
    }

    if (is_sys_table(relid)) {
        return false;
    }

    return true;
}

/*
 * es_has_es_requirement
 *     check wheather VacuumStmt has extended statistic requirement
 *     it has requirement when:
 *     (1) it has multi-column va_cols
 *     (2) if has multi-column declare when there is no multi-column va_cols
 *
 * @param (in) stmt:
 *     the VacuumStmt
 * @param (in) relid:
 *     it's rel oid
 * @param (in) inh:
 *     wheather it is inherit table
 *
 * @return:
 *     wheather it has extended statistic requirement
 */
static bool es_has_es_requirement(VacuumStmt* stmt, Oid relid, bool inh)
{
    if (stmt->va_cols) {
        ListCell* lc = NULL;
        foreach (lc, stmt->va_cols) {
            Node* item = (Node*)lfirst(lc);
            if (IsA(item, List)) {
                return true;
            }
        }
    }

    bool ans = false;
    List* bmslist_multi_attnum = NULL;
    if (NULL == stmt->va_cols) {
        bmslist_multi_attnum = es_explore_declared_stats(relid, stmt, inh);
    }
    if (NIL != bmslist_multi_attnum) {
        ans = true;
        list_free_deep(bmslist_multi_attnum);
    }

    return ans;
}

/*
 * es_is_type_supported_by_cstore
 *     to check wheather all columns are supported by cstore
 *
 * @param (in) stats:
 *     the VacAttrStats
 *
 * @return:
 *     true: all columns are supported
 */
bool es_is_type_supported_by_cstore(VacAttrStats* stats)
{
    if (IS_SINGLE_NODE)
        return false;

    for (unsigned int i = 0; i < stats->num_attrs; ++i) {
        if (!IsTypeSupportedByCStore(stats->attrtypid[i])) {
            return false;
        }
    }

    return true;
}

/*
 * es_is_type_distributable
 *     to check wheather all columns are distributeable
 *
 * @param (in) stats:
 *     the VacAttrStats
 *
 * @return:
 *     true: all columns are distributeable
 */
bool es_is_type_distributable(VacAttrStats* stats)
{
    for (unsigned int i = 0; i < stats->num_attrs; ++i) {
        if (!IsTypeDistributable(stats->attrtypid[i])) {
            return false;
        }
    }

    return true;
}

/*
 * es_check_availability_for_table
 *     in do_analyze_rel, check wheather the table is suitable for extended statistics
 *
 * @param (in) stmt:  the VacuumStmt
 * @param (in) rel:  the relation
 * @param (in) inh: wheather it is inherit table
 * @param (out) p_replicate_needs_extstats: mark wheather a replicate table need collect extended stats in DN
 */
void es_check_availability_for_table(VacuumStmt* stmt, Relation rel, bool inh, bool* p_replicate_needs_extstats)
{
    Oid relid = rel->rd_id;
    bool is_supported_replicate = es_is_supported_replicate(stmt, relid);
    bool has_es_requirement = es_has_es_requirement(stmt, relid, inh);
    bool replicate_needs_extstats = is_supported_replicate && has_es_requirement;

    *p_replicate_needs_extstats = replicate_needs_extstats;

    /* Report error for replicate foreign table */
    if (replicate_needs_extstats && (RELKIND_FOREIGN_TABLE == rel->rd_rel->relkind
        || RELKIND_STREAM == rel->rd_rel->relkind)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Replicate foreign table is not supported by extended statistic.")));
    }

    /* Report error for system table */
    if (has_es_requirement && is_sys_table(relid)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("System catalog is not supported by extended statistic.")));
    }
}

/*
 * es_is_variable_width
 *     check wheather it's variable width
 *
 * @param (in) stats:
 *     the VacAttrStats
 *
 * @return:
 *     wheather it's variable width
 */
bool es_is_variable_width(VacAttrStats* stats)
{
    bool is_var_width = false;

    for (unsigned int i = 0; i < stats->num_attrs; ++i) {
        bool item_is_var_width = (!stats->attrtype[i]->typbyval && stats->attrtype[i]->typlen < 0);
        is_var_width = is_var_width || item_is_var_width;
    }

    return is_var_width;
}

/*
 * es_is_not_null
 *     check wheather multi-column has not null constraint
 *
 * @param (in) spec:
 *     the AnalyzeSampleTableSpecInfo
 *
 * @return:
 *     true: all columns has not null constraint
 *     false: some columns do not have not null constraint, and may be null
 */
bool es_is_not_null(AnalyzeSampleTableSpecInfo* spec)
{
    for (unsigned int i = 0; i < spec->stats->num_attrs; ++i) {
        if (!(spec->stats->attrs[i]->attnotnull)) {
            return false;
        }
    }

    return true;
}

/*
 * es_get_column_name_alias_item
 *     to generate column name or column alias when construct statistics collection query
 *
 * @param (out) final_alias:
 *     the final string
 * @param (in) spec:
 *     the AnalyzeSampleTableSpecInfo to get column name or alias
 * @param (in) name_alias_flag:
 *     to mark we need column name or alias or both of them
 * @param (in) index:
 *     the column index
 * @param (in) separator:
 *     separator in multi-columns
 * @param (in) prefix:
 *     prefix of each column
 * @param (in) postfix:
 *     postfix of each column
 */
static void es_get_column_name_alias_item(StringInfoData* final_alias, AnalyzeSampleTableSpecInfo* spec,
    uint32 name_alias_flag, unsigned int index, const char* separator, const char* prefix, const char* postfix)
{
    /* For ", " */
    if (0 != index) {
        appendStringInfo(final_alias, "%s", separator);
    }

    /* For prefix */
    appendStringInfo(final_alias, "%s", prefix);

    /* For column name */
    if (ES_COLUMN_NAME & name_alias_flag) {
        char* name = NameStr(spec->stats->attrs[index]->attname);
        appendStringInfo(final_alias, "%s", quote_identifier(name));
    }

    if (ES_COLUMN_ALIAS & name_alias_flag) {
        /* For " AS " */
        if (ES_COLUMN_NAME & name_alias_flag) {
            appendStringInfo(final_alias, " as ");
        }

        /* For alias */
        const char* quote_alias = quote_identifier(spec->v_alias[index]);
        appendStringInfo(final_alias, "%s", quote_alias);
    }

    /* For postfix */
    appendStringInfo(final_alias, "%s", postfix);
}

/*
 * es_is_distributekey_contained_in_multi_column
 *     judge wheather the distribute key of the relation
 *     contained in the multi-column we are about to collect statistics.
 *
 * @param (in) relid:
 *     the oid of the relation
 * @param (in) stats:
 *     the VacAttrStats
 *
 * @return:
 *     true: distribute key contained in multi-column
 */
bool es_is_distributekey_contained_in_multi_column(Oid relid, VacAttrStats* stats)
{
    int2vector* vac_attnum = get_baserel_distributekey_no(relid);
    Bitmapset* bms_attnum = es_get_attnums_to_analyze(&stats, 1);
    int len = vac_attnum->dim1;

    for (int i = 0; i < len; i++) {
        int2 attnum = vac_attnum->values[i];
        if (!bms_is_member(attnum, bms_attnum)) {
            pfree_ext(vac_attnum);
            bms_free(bms_attnum);
            return false;
        }
    }

    pfree_ext(vac_attnum);
    bms_free(bms_attnum);
    return true;
}

/*
 * es_get_column_name_alias
 *     to generate column name or column alias when construct statistics collection query
 *
 * @param (in) spec:
 *     the AnalyzeSampleTableSpecInfo to get column name or alias
 * @param (in) name_alias_flag:
 *     to mark we need column name or alias or both of them
 * @param (in) separator:
 *     separator in multi-columns
 * @param (in) prefix:
 *     prefix of each column
 * @param (in) postfix:
 *     postfix of each column
 *
 * @return:
 *     the result string
 */
char* es_get_column_name_alias(AnalyzeSampleTableSpecInfo* spec, uint32 name_alias_flag, const char* separator,
    const char* prefix, const char* postfix)
{
    StringInfoData final_alias;
    initStringInfo(&final_alias);
    for (unsigned int i = 0; i < spec->stats->num_attrs; ++i) {
        es_get_column_name_alias_item(&final_alias, spec, name_alias_flag, i, separator, prefix, postfix);
    }

    return final_alias.data;
}

/*
 * es_get_starelkind
 *     get a general starelkind
 *
 * @return:
 *     the general starelkind
 */
char es_get_starelkind()
{
    return STARELKIND_CLASS;
}

/*
 * es_get_stainherit
 *     get a general stainherit
 *
 * @return:
 *     the general stainherit
 */
bool es_get_stainherit()
{
    return false;
}

/*
 * es_get_multi_column_attnum
 *     get bitmapset attnums from a tuple in pg_statistic
 *
 * @param (in) tuple:
 *     the pg_statistic tuple
 * @param (in) relation:
 *     the relation
 *
 * @return:
 *     the result Bitmapset attnum
 */
Bitmapset* es_get_multi_column_attnum(HeapTuple tuple, Relation relation)
{
    Bitmapset* stakeys_bms = NULL;
    bool isnull = false;
    Datum attnums = heap_getattr(tuple, Anum_pg_statistic_ext_stakey, RelationGetDescr(relation), &isnull);

    int2vector* keys = (int2vector*)DatumGetPointer(attnums);
    int key_nums = keys->dim1;
    for (int i = 0; i < key_nums; i++) {
        stakeys_bms = bms_add_member(stakeys_bms, keys->values[i]);
    }

    return stakeys_bms;
}

/*
 * es_explore_declared_stats
 *     search declared statistics in pg_statistic
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) vacstmt:
 *     the VacuumStmt
 * @param (in) inh:
 *     wheather it's inherit
 *
 * @return:
 *     a list of attnum bitmapset
 */
List* es_explore_declared_stats(Oid relid, VacuumStmt* vacstmt, bool inh)
{
    Assert(NIL == vacstmt->va_cols);

    char relkind = es_get_starelkind();
    ScanKeyData key[3];

    ScanKeyInit(&key[0], Anum_pg_statistic_ext_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_statistic_ext_starelkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[2], Anum_pg_statistic_ext_stainherit, BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(inh));

    Relation relation = heap_open(StatisticExtRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, StatisticExtRelidKindInhKeyIndexId, true, NULL, 3, key);

    List* es_list = NIL;
    for (HeapTuple tuple = NULL; NULL != (tuple = systable_getnext(scan));) {
        if (HeapTupleIsValid(tuple)) {
            Bitmapset* bms_attnum = es_get_multi_column_attnum(tuple, relation);
            if (NULL != bms_attnum) {
                es_list = lappend(es_list, bms_attnum);
            }
        }
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return es_list;
}

/*
 * es_mcv_slot_cstring_array_to_array_array
 *     construct ArrayType from C string from other node
 *
 * @param (in) cstring_array:
 *     the C string from other node
 * @param (in) num_column:
 *     number of columns
 * @param (in) atttypid_array:
 *     'typid' array
 * @param (in) atttypmod_array:
 *     'typmod' array
 *
 * @return:
 *     constructed ArrayType datum
 */
Datum es_mcv_slot_cstring_array_to_array_array(
    Datum cstring_array, unsigned int num_column, const Oid* atttypid_array, const int* atttypmod_array)
{
    Datum* elemsp = NULL;
    bool* nullsp = NULL;
    int nelemsp;

    /*
     * Step : deconstruct cstring-anyarray into cstring-datum-array
     */
    deconstruct_array(DatumGetArrayTypeP(cstring_array), CSTRINGOID, -2, false, 'c', &elemsp, &nullsp, &nelemsp);
    elog(ES_LOGLEVEL, ">>> number of elements is [%d]", nelemsp);

    if (num_column != (unsigned int)nelemsp) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("MCV column numbers are not matched.")));
    }

    /*
     * Step : convert each cstring into an anyarray
     */
    Datum* elems = (Datum*)palloc0(nelemsp * sizeof(Datum));
    for (int i = 0; i < nelemsp; ++i) {
        char* ch = DatumGetCString(elemsp[i]);
        elog(ES_LOGLEVEL, "[%s]", ch);

        Assert(false == nullsp[i]);
        elems[i] = OidFunctionCall3(ANYARRAYINFUNCOID, elemsp[i], atttypid_array[i], atttypmod_array[i]);
    }

    /*
     * Step : construct anyarray-anyarray with anyarray-datum-array
     */
    ArrayType* array = construct_array(elems, nelemsp, ANYARRAYOID, -1, false, 'd');

    return PointerGetDatum(array);
}

/*
 * es_build_vacattrstats_array
 *     construct VacAttrStats for statistic add or delete
 *
 * @param (in) rel:
 *     the relation
 * @param (in) bmslist_multicolumn:
 *     the multi-column attnums Bitmapset list
 * @param (in) add_or_delete:
 *     flag to mark add or delete statistic
 *     true: add
 *     false: delete
 * @param (out) array_length:
 *     the size of result
 * @param (in) :
 *     wheather it's inherit
 *
 * @return:
 *     an array of VacAttrStats
 */
VacAttrStats** es_build_vacattrstats_array(
    Relation rel, List* bmslist_multicolumn, bool add_or_delete, int* array_length, bool inh)
{
    Oid relid = rel->rd_id;
    char relkind = es_get_starelkind();

    /* A list of VacAttrStats */
    List* vacattrstats_list = NIL;

    bool flagExistsEvent = false;

    ListCell* lc = NULL;
    foreach (lc, bmslist_multicolumn) {
        Bitmapset* bms_multi_column = (Bitmapset*)lfirst(lc);

        /* Search this extended statistic, skip it if already defined */
        bool exists = es_is_multicolumn_stats_exists(relid, relkind, inh, bms_multi_column);
        if ((add_or_delete && exists) || ((!add_or_delete) && (!exists))) {
            flagExistsEvent = true;
            continue;
        }

        /* Build up structure for the new statistic */
        VacAttrStats* vacAttrStats = examine_attribute(rel, bms_multi_column, true);
        if (NULL == vacAttrStats) {
            continue;
        }
        vacAttrStats->anl_context = CurrentMemoryContext;
        vacAttrStats->stats_valid = true;

        /* Insert into stats list */
        vacattrstats_list = lappend(vacattrstats_list, vacAttrStats);
    }

    *array_length = 0;
    VacAttrStats** vacattrstats_array = NULL;
    if (NIL != vacattrstats_list) {
        *array_length = list_length(vacattrstats_list);
        vacattrstats_array = (VacAttrStats**)palloc0(list_length(vacattrstats_list) * sizeof(VacAttrStats*));
        int i = 0;
        ListCell* lc_stats = NULL;
        foreach (lc_stats, vacattrstats_list) {
            vacattrstats_array[i] = (VacAttrStats*)lfirst(lc_stats);
            ++i;
        }
    }

    if (list_length((List*)bmslist_multicolumn) != *array_length && flagExistsEvent) {
        if (add_or_delete) {
            elog(WARNING, "Some of extended statistics have already been defined.");
        } else {
            elog(WARNING, "Some of extended statistics have not been defined.");
        }
    }

    list_free(vacattrstats_list);

    return vacattrstats_array;
}

/*
 * es_search_extended_statistics
 *     search pg_statistic and find target statistic
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) relkind:
 *     the relkind
 * @param (in) inh:
 *     wheather it's inherit
 * @param (in) bms_attnums:
 *     the target attnum Bitmapset
 * @param (in) statistic_kind_flag:
 *     statistic flag to mark which kind of statistic we want
 *
 * @return:
 *     a list of ExtendedStats contains statistics
 */
static List* es_search_extended_statistics(
    Oid relid, char relkind, bool inh, Bitmapset* bms_attnums, uint32 statistic_kind_flag)
{
    List* es_list = NIL;
    int2vector* ext_att_num = NULL;

    int num_key = 3;
    ScanKeyData key[4];
    ScanKeyInit(&key[0], Anum_pg_statistic_ext_starelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_statistic_ext_starelkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(relkind));
    ScanKeyInit(&key[2], Anum_pg_statistic_ext_stainherit, BTEqualStrategyNumber, F_BOOLEQ, BoolGetDatum(inh));

    if (NULL != bms_attnums) {
        num_key = 4;
        ext_att_num = es_get_att_vector(bms_attnums);
        ScanKeyInit(
            &key[3], Anum_pg_statistic_ext_stakey, BTEqualStrategyNumber, F_INT2VECTOREQ, PointerGetDatum(ext_att_num));
    }

    Relation relation = heap_open(StatisticExtRelationId, AccessShareLock);
    SysScanDesc scan =
        systable_beginscan(relation, StatisticExtRelidKindInhKeyIndexId, true, NULL, num_key, key);

    int count = 0;
    for (HeapTuple tuple = NULL; NULL != (tuple = systable_getnext(scan));) {
        if (HeapTupleIsValid(tuple)) {
            Bitmapset* bms_attnum_tuple = es_get_multi_column_attnum(tuple, relation);
            Assert(NULL == bms_attnums || bms_equal(bms_attnums, bms_attnum_tuple));

            /* Skip single column stats */
            if (NULL == bms_attnum_tuple) {
                continue;
            }

            /*
             * Skip extended statistic declare in fetch statistic data conditions
             * A declare is 'nullfrac = 0' and 'distinct = 0'
             */
            bool isnull = false;
            Datum stadistinct_datum =
                heap_getattr(tuple, Anum_pg_statistic_ext_stadistinct, RelationGetDescr(relation), &isnull);
            float4 distinct = isnull ? 0.0 : DatumGetFloat4(stadistinct_datum);
            Datum stanullfrac_datum =
                heap_getattr(tuple, Anum_pg_statistic_ext_stanullfrac, RelationGetDescr(relation), &isnull);
            float4 nullfrac = isnull ? 0.0 : DatumGetFloat4(stanullfrac_datum);
            if (statistic_kind_flag > 0 && nullfrac < 0.0001 && -0.0001 < distinct && distinct < 0.0001) {
                continue;
            }

            /* We only care about ES_MAX_FETCH_NUM_OF_INSTANCE instance of extended statistics */
            ++count;
            if (statistic_kind_flag > 0 && count > (ES_MAX_FETCH_NUM_OF_INSTANCE + 1)) {
                break;
            }

            ExtendedStats* estats = (ExtendedStats*)palloc0(sizeof(ExtendedStats));
            estats->bms_attnum = bms_attnum_tuple;

            if (ES_NULLFRAC & statistic_kind_flag) {
                estats->nullfrac = nullfrac;
            }

            if (ES_WIDTH & statistic_kind_flag) {
                isnull = false;
                Datum stawidth_datum =
                    heap_getattr(tuple, Anum_pg_statistic_ext_stawidth, RelationGetDescr(relation), &isnull);
                estats->width = isnull ? 0 : DatumGetFloat4(stawidth_datum);
            }

            if (ES_DISTINCT & statistic_kind_flag) {
                estats->distinct = distinct;
            }

            if (ES_DNDISTINCT & statistic_kind_flag) {
                isnull = false;
                Datum stadndistinct_datum =
                    heap_getattr(tuple, Anum_pg_statistic_ext_stadndistinct, RelationGetDescr(relation), &isnull);
                estats->dndistinct = isnull ? 1.0 : DatumGetFloat4(stadndistinct_datum);
            }

            if (ES_MCV & statistic_kind_flag) {
                (void)get_attmultistatsslot(tuple,
                    InvalidOid,
                    0,
                    STATISTIC_KIND_MCV,
                    InvalidOid,
                    NULL,
                    &(estats->mcv_values),
                    &(estats->mcv_nvalues),
                    &(estats->mcv_numbers),
                    &(estats->mcv_nnumbers));
                (void)get_attmultistatsslot(tuple,
                    InvalidOid,
                    0,
                    STATISTIC_KIND_NULL_MCV,
                    InvalidOid,
                    NULL,
                    NULL,
                    NULL,
                    &(estats->other_mcv_numbers),
                    &(estats->other_mcv_nnumbers));
            } else if (ES_NULL_MCV & statistic_kind_flag) {
                (void)get_attmultistatsslot(tuple,
                    InvalidOid,
                    0,
                    STATISTIC_KIND_NULL_MCV,
                    InvalidOid,
                    NULL,
                    &(estats->mcv_values),
                    &(estats->mcv_nvalues),
                    &(estats->mcv_numbers),
                    &(estats->mcv_nnumbers),
                    &(estats->mcv_nulls));
                (void)get_attmultistatsslot(tuple,
                    InvalidOid,
                    0,
                    STATISTIC_KIND_MCV,
                    InvalidOid,
                    NULL,
                    NULL,
                    NULL,
                    &(estats->other_mcv_numbers),
                    &(estats->other_mcv_nnumbers));
            }

            es_list = lappend(es_list, estats);
        }
    }

    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    if (NULL != ext_att_num) {
        pfree_ext(ext_att_num);
    }

    return es_list;
}

/*
 * es_construct_mcv_value_array
 *     to construct MCV value for the target VacAttrStats
 *     when we write it to system catalog
 *
 * @param (in) stats:
 *     the VacAttrStats we have all information in it
 * @param (in) mcv_slot_index:
 *     the MCV slot index
 *
 * @return:
 *     the ArrayType of MCV to be wrote to system catalog
 */
ArrayType* es_construct_mcv_value_array(VacAttrStats* stats, int mcv_slot_index)
{
    /* Variables */
    int mcv_number = stats->numvalues[mcv_slot_index] / stats->num_attrs;

    /* Step : construct array for each column */
    ArrayType** array_columns = (ArrayType**)palloc0(sizeof(ArrayType*) * stats->num_attrs);
    for (unsigned int j = 0; j < stats->num_attrs; ++j) {
        int start_index = j * mcv_number;
        int dims[1];
        int lbs[1];

        dims[0] = mcv_number;
        lbs[0] = 1;

        array_columns[j] = construct_md_array(&(stats->stavalues[mcv_slot_index][start_index]),
            &(stats->stanulls[mcv_slot_index][start_index]),
            1,
            dims,
            lbs,
            stats->attrs[j]->atttypid,
            stats->attrs[j]->attlen,
            stats->attrs[j]->attbyval,
            stats->attrs[j]->attalign);
    }

    /* Step : compute required space */
    int32 nbytes = 0;
    for (unsigned int j = 0; j < stats->num_attrs; ++j) {
        Datum datum = PointerGetDatum(array_columns[j]);
        datum = PointerGetDatum(PG_DETOAST_DATUM(datum));
        nbytes = att_addlength_datum(nbytes, -1, datum);
        nbytes = att_align_nominal(nbytes, 'd');
    }
    nbytes += ARR_OVERHEAD_NONULLS(1);

    /* Step : allocate and initialize */
    ArrayType* array = (ArrayType*)palloc0(nbytes);
    SET_VARSIZE(array, nbytes);
    array->ndim = 1;
    array->dataoffset = 0;
    array->elemtype = ANYARRAYOID;
    *(ARR_DIMS(array)) = stats->num_attrs;
    *(ARR_LBOUND(array)) = 1;

    /* Step : copy elements */
    char* p = ARR_DATA_PTR(array);
    for (unsigned int j = 0; j < stats->num_attrs; ++j) {
        Datum datum = PointerGetDatum(array_columns[j]);
        datum = PointerGetDatum(PG_DETOAST_DATUM(datum));
        p += ArrayCastAndSet(datum, -1, false, 'd', p);
    }

    return array;
}

/*
 * es_is_multicolumn_stats_exists
 *     check wheather multi-column stats exists
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) relkind:
 *     the relkind
 * @param (in) inh:
 *     mark wheather it's inherit
 * @param (in) bms_attnums:
 *     the target attnum Bitmapset
 *
 * @return:
 *     true: it's exists
 */
bool es_is_multicolumn_stats_exists(Oid relid, char relkind, bool inh, Bitmapset* bms_attnums)
{
    List* es_list = es_search_extended_statistics(relid, relkind, inh, bms_attnums, ES_SEARCH);
    bool found = (NULL != es_list) ? true : false;
    es_free_extendedstats_list(es_list);
    return found;
}

/*
 * Interface for optimizer
 */

/*
 * es_get_multi_column_distinct
 *     to get multi-column distinct
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) relkind:
 *     the relkind
 * @param (in) inh:
 *     mark wheather it's inherit
 * @param (in) bms_attnums:
 *     the target attnum Bitmapset
 *
 * @return:
 *     the distinct, 0.0 if not found
 */
float4 es_get_multi_column_distinct(Oid relid, char relkind, bool inh, Bitmapset* bms_attnums)
{
    List* es_list = es_search_extended_statistics(relid, relkind, inh, bms_attnums, ES_DISTINCT);

    if (NULL == es_list) {
        return 0.0;
    } else {
        Assert(1 == list_length(es_list));
        ExtendedStats* es = (ExtendedStats*)linitial(es_list);
        float4 distinct = es->distinct;
        es_free_extendedstats_list(es_list);
        return distinct;
    }
}

/*
 * es_get_multi_column_stats
 *     get multi-column statistic
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) relkind:
 *     the relkind
 * @param (in) inh:
 *     mark wheather it's inherit
 * @param (in) bms_attnums:
 *     the target attnum Bitmapset
 *
 * @return:
 *     the multi-column statistic for this attnum Bitmapset
 */
ExtendedStats* es_get_multi_column_stats(Oid relid, char relkind, bool inh, Bitmapset* bms_attnums, bool has_null)
{
    uint32 mcv_flag = has_null ? ES_MCV : ES_NULL_MCV;
    List* es_list = es_search_extended_statistics(relid, relkind, inh, bms_attnums, ES_ALL & (~mcv_flag));

    if (NIL == es_list) {
        return NULL;
    } else {
        Assert(1 == list_length(es_list));
        ExtendedStats* es = (ExtendedStats*)linitial(es_list);
        list_free(es_list);
        return es;
    }
}

/*
 * es_get_multi_column_stats
 *     get multi-column statistic
 *
 * @param (in) relid:
 *     the relation oid
 * @param (in) relkind:
 *     the relkind
 * @param (in) inh:
 *     mark wheather it's inherit
 * @param (out) num_stats:
 *     number of statistics
 *
 * @return:
 *     a list of statistics
 */
List* es_get_multi_column_stats(Oid relid, char relkind, bool inh, int* num_stats, bool has_null)
{
    uint32 mcv_flag = has_null ? ES_MCV : ES_NULL_MCV;
    List* es_list = es_search_extended_statistics(relid, relkind, inh, NULL, ES_ALL & (~mcv_flag));

    if (NULL == es_list) {
        *num_stats = 0;
        return NIL;
    } else {
        *num_stats = list_length(es_list);
        if (*num_stats > ES_MAX_FETCH_NUM_OF_INSTANCE) {
            es_free_extendedstats_list(es_list);
            return NIL;
        } else {
            return es_list;
        }
    }
}

static int2vector* es_get_att_vector(Bitmapset* attnum)
{
    int num = bms_num_members(attnum), x = -1, i = 0;
    int2* attnum_array = (int2*)palloc0(sizeof(int2) * num);
    while ((x = bms_next_member(attnum, x)) >= 0)
        attnum_array[i++] = x;
    int2vector* result = buildint2vector(attnum_array, num);
    pfree_ext(attnum_array);
    return result;
}

void es_split_multi_column_stats(List* va_list, List** va_cols, List** va_cols_multi)
{
    ListCell* lc = NULL;

    foreach (lc, va_list) {
        Node* item = (Node*)lfirst(lc);
        if (IsA(item, String))
            *va_cols = list_append_unique(*va_cols, item);
        else if (IsA(item, List))
            *va_cols_multi = list_append_unique(*va_cols_multi, item);
        else
            Assert(false);
    }
}
