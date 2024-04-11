/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * index_advisor.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/dbmind/kernel/index_advisor.cpp
 *
 * DESCRIPTION
 * The functions in this file are used to provide the built-in index recommendation function of the database,
 * and users can obtain better performance by creating indexes on the corresponding tables and columns.
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "access/tupdesc.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_partition_fn.h"
#include "commands/sqladvisor.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "pg_config_manual.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/analyze.h"
#include "securec.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "distributelayer/streamMain.h"

#define MAX_SAMPLE_ROWS 10000    /* sampling range for executing a query */
#define CARDINALITY_THRESHOLD 30 /* the threshold of index selection */

#define RelAttrName(__tupdesc, __attridx) (NameStr((__tupdesc)->attrs[(__attridx)].attname))
#define IsSameRel(_schema1, _table1, _schema2, _table2) \
    ((!_schema1 || !_schema2 || strcasecmp(_schema1, _schema2) == 0) && strcasecmp(_table1, _table2) == 0)

typedef struct {
    char schema[NAMEDATALEN];
    char table[NAMEDATALEN];
    StringInfoData column;
    char index_type[NAMEDATALEN];
} SuggestedIndex;

typedef struct {
    char *schema_name;
    char *table_name;
    List *alias_name;
    List *index;
    List *join_cond;
    List *index_print;
    bool ispartition = false;
    bool issubpartition=false;
    List *partition_key_list;
    List *subpartition_key_list;
} TableCell;

typedef struct {
    char *alias_name;
    char *column_name;
} TargetCell;

typedef struct {
    uint4 cardinality;
    char *index_name;
    char *op;
    char *field_expr;
} IndexCell;

typedef struct {
    char *index_columns;
    char *index_type;
} IndexPrint;

struct StmtCell {
SelectStmt *stmt;
StmtCell *parent_stmt;
List *tables;
};

namespace index_advisor {
    typedef struct {
        char *field;
        TableCell *joined_table;
        char *joined_field;
    } JoinCell;
}

THR_LOCAL List *g_stmt_list = NIL;
THR_LOCAL List *g_tmp_table_list = NIL;
THR_LOCAL List *g_table_list = NIL;
THR_LOCAL List *g_drived_tables = NIL;
THR_LOCAL TableCell *g_driver_table = NULL;

static SuggestedIndex *suggest_index(const char *, _out_ int *);
static List *get_table_indexes(Oid oid);
static List *get_index_attname(Oid oid);
static char *search_table_attname(Oid attrelid, int2 attnum);
static DestReceiver *create_stmt_receiver();
static void receive(TupleTableSlot *slot, DestReceiver *self);
static void shutdown(DestReceiver *self);
static void startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void destroy(DestReceiver *self);
static void free_global_resource();
static void find_select_stmt(Node *, StmtCell *);
static void extract_stmt_from_clause(const List *, StmtCell *);
static void extract_stmt_where_clause(Node *, StmtCell *);
static void generate_stmt_tables(StmtCell *);
static void parse_where_clause(Node *);
static void field_value_trans(_out_ StringInfoData, A_Const *);
static void parse_field_expr(List *, List *, List *);
static inline uint4 tuple_to_uint(List *);
static uint4 get_table_count(char *, char *);
static uint4 calculate_field_cardinality(char *, char *, const char *);
static bool is_tmp_table(const char *);
static void find_table_by_column(char **, char **, char **);
static void split_field_list(List *, char **, char **, char **);
static void get_partition_key_name(Relation, TableCell *, bool is_subpartition = false);
static bool check_relation_type_valid(Oid);
static TableCell *find_or_create_tblcell(char *table_name, char *alias_name, char *schema_name = NULL,
    bool is_partition = false, bool is_subpartition = false);
static TargetCell *find_or_create_clmncell(char **, List *);
static void add_index_from_field(char *, char *, IndexCell *);
static bool parse_group_clause(List *, List *);
static bool parse_order_clause(List *, List *);
static void add_index_from_group_order(TableCell *, List *, List *, bool);
static void get_partition_index_type(IndexPrint *, TableCell *);
static IndexPrint *generat_index_print(TableCell *, char *);
static void generate_final_index();
static void parse_target_list(List *);
static void parse_from_clause(List *);
static void add_drived_tables(RangeVar *);
static void parse_join_tree(JoinExpr *);
static void add_join_cond(TableCell *, char *, TableCell *, char *);
static void parse_join_expr(Node *);
static void parse_join_expr(JoinExpr *);
static void determine_driver_table(int);
static uint4 get_join_table_result_set(const char *, const char *, const char *);
static void add_index_from_join(TableCell *, char *);
static void add_index_for_drived_tables();
static void extract_info_from_plan(Node* parse_tree, const char *query_string);
static void adv_get_info_from_plan_hook(Node* node, List* rtable);
static void get_join_condition_from_plan(Node* node, List* rtable);
static void get_order_condition_from_plan(Node* node);
static bool check_join_expr(A_Expr *);
static bool check_joined_tables();

Datum gs_index_advise(PG_FUNCTION_ARGS)
{
    FuncCallContext *func_ctx = NULL;
    SuggestedIndex *array = NULL;

    char *query = PG_GETARG_CSTRING(0);
    if (query == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("you must enter a query statement.")));
    }
#define COLUMN_NUM 4
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tup_desc;
        MemoryContext old_context;
        int max_calls = 0;

        /* create a function context for cross-call persistence */
        func_ctx = SRF_FIRSTCALL_INIT();
        /*
         * switch to memory context appropriate for multiple function calls
         */
        old_context = MemoryContextSwitchTo(func_ctx->multi_call_memory_ctx);
        array = suggest_index(query, &max_calls);

        /* build tup_desc for result tuples */
        tup_desc = CreateTemplateTupleDesc(COLUMN_NUM, false);
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "schema", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "table", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)3, "column", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)4, "indextype", TEXTOID, -1, 0);

        func_ctx->tuple_desc = BlessTupleDesc(tup_desc);
        func_ctx->max_calls = max_calls;
        func_ctx->user_fctx = (void *)array;

        (void)MemoryContextSwitchTo(old_context);
    }

    /* stuff done on every call of the function */
    func_ctx = SRF_PERCALL_SETUP();
    array = (SuggestedIndex *)func_ctx->user_fctx;

    /* for each row */
    if (func_ctx->call_cntr < func_ctx->max_calls) {
        Datum values[COLUMN_NUM];
        bool nulls[COLUMN_NUM] = {false};
        HeapTuple tuple = NULL;

        errno_t rc = EOK;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        int entry_index = func_ctx->call_cntr;
        SuggestedIndex *entry = array + entry_index;

        /* Locking is probably not really necessary */
        values[0] = CStringGetTextDatum(entry->schema);
        values[1] = CStringGetTextDatum(entry->table);
        values[2] = CStringGetTextDatum(entry->column.data);
        values[3] = CStringGetTextDatum(entry->index_type);

        tuple = heap_form_tuple(func_ctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(func_ctx, HeapTupleGetDatum(tuple));
    } else {
        pfree(array);
        SRF_RETURN_DONE(func_ctx);
    }
}

/*
 * suggest_index
 *     Parse the given query and return the suggested indexes. The suggested
 *     index consists of table names and column names.
 *
 * The main steps are summarized as follows:
 * 1. Get parse tree;
 * 2. Find and parse SelectStmt structures;
 * 3. Parse 'from' and 'where' clause, and add candidate indexes for tables;
 * 4. Determine the driver table;
 * 5. Parse 'group' and 'order' clause and add candidate indexes for tables;
 * 6. Add candidate indexes for drived tables according to the 'join' conditions.
 */
SuggestedIndex *suggest_index(const char *query_string, _out_ int *len)
{
    g_stmt_list = NIL;
    g_tmp_table_list = NIL;
    g_table_list = NIL;
    g_drived_tables = NIL;
    g_driver_table = NULL;

    List *parse_tree_list = raw_parser(query_string);
    if (parse_tree_list == NULL || list_length(parse_tree_list) <= 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("can not parse the query.")));
    }

    if (list_length(parse_tree_list) > 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("can not advise for multiple queries.")));
    }

    Node *parsetree = (Node *)lfirst(list_head(parse_tree_list));
    Node *parsetree_copy = (Node *)copyObject(parsetree);
    // Check for syntax errors
    parse_analyze(parsetree, query_string, NULL, 0);
    StmtCell *stmt_cell = NULL;
    find_select_stmt(parsetree_copy, stmt_cell);
    if (!g_stmt_list) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("can not advise for the query because not found a select statement.")));
    }

    // Parse the SelectStmt structure
    ListCell *item = NULL;

    foreach (item, g_stmt_list) {
        t_thrd.index_advisor_cxt.stmt_table_list = NIL;
        t_thrd.index_advisor_cxt.stmt_target_list = NIL;
        StmtCell *stmt_cell = (StmtCell *)lfirst(item);
        SelectStmt *stmt = stmt_cell->stmt;
        parse_from_clause(stmt->fromClause);
        parse_target_list(stmt->targetList);
        if (t_thrd.index_advisor_cxt.stmt_table_list) {
            int local_stmt_table_count = t_thrd.index_advisor_cxt.stmt_table_list->length;
            generate_stmt_tables(stmt_cell);
            t_thrd.index_advisor_cxt.stmt_table_list = stmt_cell->tables;
            parse_where_clause(stmt->whereClause);
            determine_driver_table(local_stmt_table_count);
            if (parse_group_clause(stmt->groupClause, stmt->targetList)) {
                add_index_from_group_order(g_driver_table, stmt->groupClause, stmt->targetList, true);
            } else if (parse_order_clause(stmt->sortClause, stmt->targetList)) {
                add_index_from_group_order(g_driver_table, stmt->sortClause, stmt->targetList, false);
            }
            if (check_joined_tables() && g_driver_table) {
                add_index_for_drived_tables();
            }
            // Generate the final index string for each table.
            generate_final_index();

            g_driver_table = NULL;
        }
        t_thrd.index_advisor_cxt.stmt_table_list = NIL;
        list_free_ext(t_thrd.index_advisor_cxt.stmt_target_list);
        list_free(g_drived_tables);
        g_drived_tables = NIL;
    }
    if (g_table_list == NIL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
            errmsg("can not advise for the query because can not recognize involved tables.")));
    }

    // Use SQL Advisor
    extract_info_from_plan(parsetree_copy, query_string);

    // Format the return result, e.g., 'table1, "(col1,col2),(col3)"'.
    // Get index quantity.
    Size index_len = 0;
    item = NULL;
    foreach (item, g_table_list) {
        TableCell *cur_table = (TableCell *)lfirst(item);
        if (cur_table->index_print) {
            index_len += cur_table->index_print->length;
        } else {
            index_len++;
        }
    }
    *len = index_len;
    SuggestedIndex *array = (SuggestedIndex *)palloc0(sizeof(SuggestedIndex) * index_len);
    errno_t rc = EOK;
    item = NULL;
    int i = 0;

    foreach (item, g_table_list) {
        TableCell *cur_table = (TableCell *)lfirst(item);
        List *index_list = cur_table->index_print;
        if (!index_list) {
            rc = strcpy_s((array + i)->schema, NAMEDATALEN, cur_table->schema_name);
            securec_check(rc, "\0", "\0");
            rc = strcpy_s((array + i)->table, NAMEDATALEN, cur_table->table_name);
            securec_check(rc, "\0", "\0");
            initStringInfo(&(array + i)->column);
            appendStringInfoString(&(array + i)->column, "");
            i++;
        } else {
            ListCell *cur_index = NULL;
            foreach (cur_index, index_list) {
                rc = strcpy_s((array + i)->schema, NAMEDATALEN, cur_table->schema_name);  
                securec_check(rc, "\0", "\0");
                rc = strcpy_s((array + i)->table, NAMEDATALEN, cur_table->table_name);
                securec_check(rc, "\0", "\0");
                initStringInfo(&(array + i)->column);
                appendStringInfoString(&(array + i)->column, (char *)((IndexPrint *)lfirst(cur_index))->index_columns);
                rc = strcpy_s((array + i)->index_type, NAMEDATALEN, ((IndexPrint *)lfirst(cur_index))->index_type);
                securec_check(rc, "\0", "\0");
                i++;
            }
        }
        
        list_free_deep(index_list);
    }

    // Release allocated resources.
    // Actually, even though we did not free the resources, 
    // the database kernel will automatically delete the memory context we allocated 
    // after the function is completed. 
    list_free_deep(parse_tree_list);
    free_global_resource();

    return array;
}

static void generate_stmt_tables(StmtCell *stmt_cell)
{
    StmtCell *parent_stmt = stmt_cell->parent_stmt;
    stmt_cell->tables = NIL;

    ListCell *table_item = NULL;
    foreach (table_item, t_thrd.index_advisor_cxt.stmt_table_list) {
        stmt_cell->tables = lappend(stmt_cell->tables, (TableCell *)lfirst(table_item));
    }
    if (parent_stmt && parent_stmt->tables) {
        ListCell *global_table = NULL;
        foreach (global_table, parent_stmt->tables) {
            stmt_cell->tables = lappend(stmt_cell->tables, (TableCell *)lfirst(global_table));
        }
    }
    list_free_ext(t_thrd.index_advisor_cxt.stmt_table_list);
}

void extract_info_from_plan(Node* parse_tree, const char *query_string)
{
    PlannedStmt* plan_tree = NULL;
    ListCell* lc = NULL;
    
    u_sess->adv_cxt.getPlanInfoFunc = (void*)adv_get_info_from_plan_hook;

    List* query_tree_list = pg_analyze_and_rewrite(parse_tree, query_string, NULL, 0);
    foreach (lc, query_tree_list) {
        t_thrd.index_advisor_cxt.stmt_table_list = NIL;
        Query* query = castNode(Query, lfirst(lc));

        if (query->commandType != CMD_UTILITY) {
            query->boundParamsQ = NULL;
            plan_tree = pg_plan_query(query, 0, NULL);
            extractNode((Plan*)plan_tree->planTree, NIL, plan_tree->rtable, plan_tree->subplans);
        }
        generate_final_index();
        list_free_ext(t_thrd.index_advisor_cxt.stmt_table_list);
    }

    u_sess->adv_cxt.getPlanInfoFunc = NULL;
}

void adv_get_info_from_plan_hook(Node* node, List* rtable)
{
    switch (nodeTag(node)) {
        case T_OpExpr: {
            /* parsing join clause */
            get_join_condition_from_plan(node, rtable);
        } break;
        case T_Sort: {
            /* parsing order by clause */
            get_order_condition_from_plan(node);
        } break;
        default:
            break;
    }
}

void get_join_condition_from_plan(Node* node, List* rtable)
{
    OpExpr *expr = (OpExpr *)node;
    if (list_length(expr->args) == 2) {
        Node *expr_arg1 = (Node *)linitial(expr->args);
        Node *expr_arg2 = (Node *)lsecond(expr->args);

        if (IsA(expr_arg1, Var) && IsA(expr_arg2, Var)) {
            Var *arg1 = (Var *)expr_arg1;
            Var *arg2 = (Var *)expr_arg2;
            RangeTblEntry *rte1 = rt_fetch(arg1->varno, rtable);
            RangeTblEntry *rte2 = rt_fetch(arg2->varno, rtable);

            if (rte1->relid != rte2->relid) {
                char *l_field_name = pstrdup(get_attname(rte1->relid, arg1->varattno));
                char *r_field_name = pstrdup(get_attname(rte2->relid, arg2->varattno));
                char *l_schema_name = get_namespace_name(get_rel_namespace(rte1->relid));
                char *r_schema_name = get_namespace_name(get_rel_namespace(rte2->relid));
                TableCell *ltable = find_or_create_tblcell(rte1->relname, NULL, l_schema_name);
                TableCell *rtable = find_or_create_tblcell(rte2->relname, NULL, r_schema_name);

                if (!ltable || !rtable) {
                    return;
                }

                // add join conditons
                add_join_cond(ltable, l_field_name, rtable, r_field_name);
                add_join_cond(rtable, r_field_name, ltable, l_field_name);

                // add possible drived tables
                if (!list_member(g_drived_tables, ltable)) {
                    g_drived_tables = lappend(g_drived_tables, ltable);
                }
                if (!list_member(g_drived_tables, rtable)) {
                    g_drived_tables = lappend(g_drived_tables, rtable);
                }
            }
        }
    }
}

void add_index(TableCell *table, char *index_name)
{
    IndexCell *index = (IndexCell *)palloc0(sizeof(*index));
    index->index_name = index_name;
    index->cardinality = 0;
    index->op = NULL;
    index->field_expr = NULL;

    if (table->index == NIL) {
        table->index = lappend(table->index, index);
    } else {
        ListCell *cur = NULL;
        ListCell *prev = NULL;
        foreach (cur, table->index) {
            IndexCell *table_index = (IndexCell *)lfirst(cur);

            if (index->index_name == NULL || strcasecmp(table_index->index_name, index->index_name) == 0) {
                pfree(index);
                break;
            }
            if (table_index->op && strcasecmp(table_index->op, "=") != 0) {
                if (prev == NULL) {
                    table->index = lcons(index, table->index);
                } else {
                    (void)lappend_cell(table->index, prev, index);
                }
                break;
            }
            if (cur == list_tail(table->index)) {
                (void)lappend_cell(table->index, cur, index);
                break;
            }
        }
    }
}

void get_order_condition_from_plan(Node* node)
{
    Sort *sortopt = (Sort *)node;
    List *targetlist = sortopt->plan.targetlist;
    if (!targetlist) {
        ereport(WARNING,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("index advisor: cannot get targetlist on Sort operator.")));
        return;
    }
    int numcols = sortopt->numCols;
    AttrNumber *sortkey = sortopt->sortColIdx;
    if (numcols <= 0 || !sortkey) {
        ereport(WARNING, (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("index advisor: incorrect numCols or sortColIdx of Sort operator.")));
        return;
    }
    for (int attridx = 0; attridx < numcols; attridx++) {
        TargetEntry *targetEntry =
            (TargetEntry *)list_nth(targetlist, *(sortkey + attridx) - 1);  // sortkey starts from 1 not 0.
        Oid origtbl = targetEntry->resorigtbl;
        if (origtbl == 0) {
            continue;
        }
        char *relname = get_rel_name(origtbl);
        char *schemaname = get_namespace_name(get_rel_namespace(origtbl));
        // get source column
        char *column_name = pstrdup(get_attname(origtbl, targetEntry->resorigcol));
        // not supported cte
        if (strcasecmp(column_name, targetEntry->resname) != 0) {
            return;
        }
        if (relname && schemaname) {
            TableCell *tblcell = find_or_create_tblcell(relname, NULL, schemaname);
            if (tblcell != nullptr) {
                add_index(tblcell, column_name);
            }
        } else {
            ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("index advisor: cannot get relname on Sort operator.")));
        }
    }
}

void free_global_resource()
{
    list_free(g_drived_tables);
    list_free(g_table_list);
    list_free(g_tmp_table_list);
    list_free(g_stmt_list);
    g_table_list = NIL;
    g_tmp_table_list = NIL;
    g_stmt_list = NIL;
    g_drived_tables = NIL;
    g_driver_table = NULL;
}

/* Search the oid of all indexes created on the table through the oid of the table, 
 * and return the index oid list. 
 */
List *get_table_indexes(Oid oid)
{
    Relation rel = heap_open(oid, NoLock);
    List *indexes = RelationGetIndexList(rel);
    heap_close(rel, NoLock);
    return indexes;
}

/* Search attribute name (column name) by the oid of the table 
 * and the serial number of the column. 
 */
char *search_table_attname(Oid attrelid, int2 attnum)
{
    ScanKeyData key[2];
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    char *name = NULL;
    bool isnull = true;

    Relation pg_attribute = heap_open(AttributeRelationId, NoLock);
    ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(attrelid));
    ScanKeyInit(&key[1], Anum_pg_attribute_attnum, BTEqualStrategyNumber, F_INT2EQ, Int16GetDatum(attnum));

    scan = systable_beginscan(pg_attribute, AttributeRelidNumIndexId, true, NULL, 2, key);
    tup = systable_getnext(scan);
    if (!HeapTupleIsValid(tup)) {
        systable_endscan(scan);
        heap_close(pg_attribute, NoLock);
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("attribute name lookup failed for table oid %u, attnum %d.", attrelid, attnum)));
    }

    TupleDesc desc = RelationGetDescr(pg_attribute);
    Datum datum = heap_getattr(tup, Anum_pg_attribute_attname, desc, &isnull);
    Assert(!isnull && datum);

    name = pstrdup(NameStr(*DatumGetName(datum)));

    systable_endscan(scan);
    heap_close(pg_attribute, NoLock);

    return name;
}

/* Return the names of all the columns involved in the index. */
List *get_index_attname(Oid index_oid)
{
    HeapTuple index_tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if (!HeapTupleIsValid(index_tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for index %u", index_oid)));

    Form_pg_index index_form = (Form_pg_index)GETSTRUCT(index_tup);
    int2vector *attnums = &(index_form->indkey);
    Oid indrelid = index_form->indrelid;

    // get attrnames from table oid.
    List *attnames = NIL;
    int2 attnum;
    for (int i = 0; i < attnums->dim1; i++) {
        attnum = attnums->values[i];
        if (attnum < 1) {
            break;
        }
        attnames = lappend(attnames, search_table_attname(indrelid, attnum));
    }

    ReleaseSysCache(index_tup);
    return attnames;
}

// Execute an SQL statement and return its result.
StmtResult *execute_stmt(const char *query_string, bool need_result)
{
    int16 format = 0;
    List *parsetree_list = NULL;
    ListCell *parsetree_item = NULL;
    bool snapshot_set = false;
    DestReceiver *receiver = NULL;
    parsetree_list = pg_parse_query(query_string, NULL);

    Assert(list_length(parsetree_list) == 1); // ought to be one query
    parsetree_item = list_head(parsetree_list);
#ifndef ENABLE_MULTIPLE_NODES
    AutoDopControl dopControl;
    dopControl.CloseSmp();
#endif
    PG_TRY();
    {
        Portal portal = NULL;

        List *querytree_list = NULL;
        List *plantree_list = NULL;
        Node *parsetree = (Node *)lfirst(parsetree_item);
        const char *commandTag = CreateCommandTag(parsetree);

        if (u_sess->utils_cxt.ActiveSnapshot == NULL && analyze_requires_snapshot(parsetree)) {
            PushActiveSnapshot(GetTransactionSnapshot());
            snapshot_set = true;
        }

        querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
        plantree_list = pg_plan_queries(querytree_list, 0, NULL);

        if (snapshot_set) {
            PopActiveSnapshot();
        }

        portal = CreatePortal(query_string, true, true);
        portal->visible = false;
        PortalDefineQuery(portal, NULL, query_string, commandTag, plantree_list, NULL);
        if (need_result)
            receiver = create_stmt_receiver();
        else
            receiver = CreateDestReceiver(DestNone);

        PortalStart(portal, NULL, 0, NULL);
        PortalSetResultFormat(portal, 1, &format);
        (void)PortalRun(portal, FETCH_ALL, true, receiver, receiver, NULL);
        PortalDrop(portal, false);
    }
    PG_CATCH();
    {
#ifndef ENABLE_MULTIPLE_NODES
        dopControl.ResetSmp();
#endif
        PG_RE_THROW();
    }
    PG_END_TRY();

    return (StmtResult *)receiver;
}

/* create_stmt_receiver
 *
 * The existing code cannot store the execution results of SQL statements to the list structure.
 * Therefore, this routine implements the preceding function through a customized data structure -- StmtResult. 
 * The following routines: receive, startup, shutdown and destroy are the methods of StmtResult.  
 *
 */
DestReceiver *create_stmt_receiver()
{
    StmtResult *self = (StmtResult *)palloc0(sizeof(StmtResult));
    self->pub.receiveSlot = receive;
    self->pub.rStartup = startup;
    self->pub.rShutdown = shutdown;
    self->pub.rDestroy = destroy;
    self->pub.mydest = DestTuplestore;

    self->tuples = NIL;
    self->isnulls = NULL;
    self->atttypids = NULL;
    self->mxct = CurrentMemoryContext;

    return (DestReceiver *)self;
}

void receive(TupleTableSlot *slot, DestReceiver *self)
{
    StmtResult *result = (StmtResult *)self;
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    int natts = typeinfo->natts;
    Datum origattr, attr;
    char *value = NULL;
    bool isnull = false;
    Oid typoutput;
    bool typisvarlena = false;
    List *values = NIL;

    if (result->isnulls == NULL) {
        result->isnulls = (bool *)pg_malloc(natts * sizeof(bool));
    }
    if (result->collids == NULL) {
        result->collids = (Oid *)pg_malloc(natts * sizeof(Oid));
    }
    if (result->atttypids == NULL) {
        result->atttypids = (Oid *)pg_malloc(natts * sizeof(Oid));
    }
    
    MemoryContext oldcontext = MemoryContextSwitchTo(result->mxct);

    for (int i = 0; i < natts; ++i) {
        origattr = tableam_tslot_getattr(slot, i + 1, &isnull);
        if (isnull) {
            result->isnulls[i] = true;
            result->collids[i] = typeinfo->attrs[i].attcollation;
            continue;
        }
        
        result->isnulls[i] = false;
        getTypeOutputInfo(typeinfo->attrs[i].atttypid, &typoutput, &typisvarlena);

        if (typisvarlena) {
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        } else {
            attr = origattr;
        }

        value = OidOutputFunctionCall(typoutput, attr);
        values = lappend(values, value);
        result->atttypids[i] = typeinfo->attrs[i].atttypid;
        result->collids[i] = typeinfo->attrs[i].attcollation;

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr)) {
            pfree(DatumGetPointer(attr));
        }
    }
    result->tuples = lappend(result->tuples, values);

    MemoryContextSwitchTo(oldcontext);
}

void startup(DestReceiver *self, int operation, TupleDesc typeinfo) {
    /* nothing */ 
}

void shutdown(DestReceiver *self) {
    /* nothing */ 
}

/* Release resources */
void destroy(DestReceiver *self)
{
    StmtResult *result = (StmtResult *)self;
    ListCell *item = NULL;
    List *tuples = result->tuples;

    foreach (item, tuples) {
        list_free_deep((List *)lfirst(item));
    }
    list_free(tuples);

    pfree_ext(result->isnulls);
    pfree_ext(result->collids);
    pfree_ext(result->atttypids);
    pfree_ext(self);
}

/*
 * find_select_stmt
 *     Recursively search for SelectStmt structures within a parse tree.
 *
 * The 'SelectStmt' structure may exist in set operations and the 'with',
 * 'from' and 'where' clauses, and it is added to the global variable
 * "g_stmt_list" only if it has no subquery structure.
 */
static void find_select_stmt(Node *parsetree, StmtCell *parent_stmt)
{
    if (parsetree == NULL || nodeTag(parsetree) != T_SelectStmt) {
        return;
    }
    SelectStmt *stmt = (SelectStmt *)parsetree;
    StmtCell *stmt_cell = (StmtCell *)palloc0(sizeof(StmtCell));
    stmt_cell->stmt = stmt;
    stmt_cell->parent_stmt = parent_stmt;
    g_stmt_list = lappend(g_stmt_list, stmt_cell);

    switch (stmt->op) {
        case SETOP_INTERSECT:
        case SETOP_EXCEPT:
        case SETOP_UNION: {
            // analyze the set operation: union
            find_select_stmt((Node *)stmt->larg, stmt_cell);
            find_select_stmt((Node *)stmt->rarg, stmt_cell);
            break;
        }
        case SETOP_NONE: {
            // analyze the 'with' clause
            if (stmt->withClause) {
                List *cte_list = stmt->withClause->ctes;
                ListCell *item = NULL;

                foreach (item, cte_list) {
                    CommonTableExpr *cte = (CommonTableExpr *)lfirst(item);
                    g_tmp_table_list = lappend(g_tmp_table_list, cte->ctename);
                    find_select_stmt(cte->ctequery, stmt_cell);
                }
                break;
            }

            // analyze the 'from' clause
            if (stmt->fromClause) {
                extract_stmt_from_clause(stmt->fromClause, stmt_cell);
            }

            // analyze the 'where' clause
            if (stmt->whereClause) {
                extract_stmt_where_clause(stmt->whereClause, stmt_cell);
            }
            break;
        }
        default: {
            break;
        }
    }
}

static void extract_stmt_from_clause(const List *from_list, StmtCell *stmt_cell)
{
    ListCell *item = NULL;

    foreach (item, from_list) {
        Node *from = (Node *)lfirst(item);
        if (from && IsA(from, RangeSubselect)) {
            find_select_stmt(((RangeSubselect *)from)->subquery, stmt_cell);
        }
        if (from && IsA(from, JoinExpr)) {
            Node *larg = ((JoinExpr *)from)->larg;
            Node *rarg = ((JoinExpr *)from)->rarg;
            if (larg && IsA(larg, RangeSubselect)) {
                find_select_stmt(((RangeSubselect *)larg)->subquery, stmt_cell);
            }
            if (rarg && IsA(rarg, RangeSubselect)) {
                find_select_stmt(((RangeSubselect *)rarg)->subquery, stmt_cell);
            }                                
        }
    }
}

static void extract_stmt_where_clause(Node *item_where, StmtCell *stmt_cell)
{
    if (IsA(item_where, SubLink)) {
        find_select_stmt(((SubLink *)item_where)->subselect, stmt_cell);
    }
    if (item_where && IsA(item_where, A_Expr)) {
        A_Expr *expr = (A_Expr *)item_where;
        Node *lexpr = expr->lexpr;
        Node *rexpr = expr->rexpr;
        if (lexpr) {
            extract_stmt_where_clause((Node *)lexpr, stmt_cell);
        }
        if (rexpr) {
            extract_stmt_where_clause((Node *)rexpr, stmt_cell);
        }
    }
}

void get_partition_index_type(IndexPrint *suggested_index, TableCell *table)
{
    StringInfoData partition_keys;
    initStringInfo(&partition_keys);
    int i = 0;
    ListCell *value = NULL;
    foreach (value, table->partition_key_list) {
        char *partition_key = (char *)lfirst(value);
        if (i == 0) {
            appendStringInfoString(&partition_keys, partition_key);
        } else {
            appendStringInfoString(&partition_keys, ",");
            appendStringInfoString(&partition_keys, partition_key);
        }
        i++;
    }
    if (table->subpartition_key_list != NIL) {
        StringInfoData subpartition_keys;
        initStringInfo(&subpartition_keys);
        int i = 0;
        foreach (value, table->subpartition_key_list) {
            char *subpartition_key = (char *)lfirst(value);
            if (i == 0) {
                appendStringInfoString(&subpartition_keys, subpartition_key);
            } else {
                appendStringInfoString(&subpartition_keys, ",");
                appendStringInfoString(&subpartition_keys, subpartition_key);
            }
            i++;
        }
        if (!strstr(suggested_index->index_columns, subpartition_keys.data)) {
            // if not specified subpartition and the suggested columns does not contain subpartition key,
            // then recommend global index
            suggested_index->index_type = "global";
            pfree_ext(partition_keys.data);
            pfree_ext(subpartition_keys.data);
            return;
        }
        if (table->ispartition || strstr(suggested_index->index_columns, partition_keys.data) != NULL) {
            // if the suggested columns contain subpartition key and specify partition or
            // the suggested columns contain subpartition key and partition key
            // then recommend local index
            suggested_index->index_type = "local";
        } else {
            suggested_index->index_type = "global";
        }
        pfree_ext(subpartition_keys.data);
    } else {
        if (table->ispartition || strstr(suggested_index->index_columns, partition_keys.data) != NULL) {
            // if the suggested columns contain partition key, then recommend local index
            suggested_index->index_type = "local";
        } else {
            suggested_index->index_type = "global";
        }
    }
    pfree_ext(partition_keys.data);
}

/*
 * generat_index_print
 *     Generate index type, normal table is '' by default
 *     partition table is divided into local and global.
 *
 * The table in each subquery is preferentially selected for matching.
 * The same table is repeated in different partition states in the whole query, 
.* resulting in an incorrect index type.
 */
IndexPrint *generat_index_print(TableCell *table, char *index_print)
{
    IndexPrint *suggested_index = (IndexPrint *)palloc0(sizeof(*suggested_index));
    suggested_index->index_columns = index_print;
    suggested_index->index_type = NULL;
    // recommend index type for partition index
    bool stmt_table_match = false;
    ListCell *stmt_table = NULL;
    foreach(stmt_table, t_thrd.index_advisor_cxt.stmt_table_list) {
        TableCell *cur_table = (TableCell *)lfirst(stmt_table);
        char *cur_schema_name = cur_table->schema_name;
        char *cur_table_name = cur_table->table_name;
        if (IsSameRel(cur_schema_name, cur_table_name, table->schema_name, table->table_name)) {
            stmt_table_match = true;
            if (cur_table->issubpartition) {
                // recommend the index type of the query with specified partition for local index
                suggested_index->index_type = "local";
            } else if (cur_table->partition_key_list != NIL) {
                get_partition_index_type(suggested_index, cur_table);
            } else {
                suggested_index->index_type = "";
            }
            break;
        }
    }
    if (!t_thrd.index_advisor_cxt.stmt_table_list or !stmt_table_match) {
        if (table->issubpartition) {
            suggested_index->index_type = "local";
        } else if (table->partition_key_list != NIL) {
            get_partition_index_type(suggested_index, table);
        } else {
            suggested_index->index_type = "";
        }
    }
    return suggested_index;
}

TableCell *find_table(TableCell *table)
{
    ListCell *item = NULL;
    foreach(item, g_table_list) {
        TableCell *cur_table = (TableCell *)lfirst(item);
        char *cur_schema_name = cur_table->schema_name;
        char *cur_table_name = cur_table->table_name;
        if (IsSameRel(cur_schema_name, cur_table_name, table->schema_name, table->table_name)) {
            return cur_table;
        }
    }
    return NULL;
}

/*
 * generate_final_index_print
 *     Concatenate the candidate indexes of the table into a string of
 *     composite index.
 *
 * The composite index is generated only when it is not the same as the
 * previously suggested indexes or existing indexes.
 */
void generate_final_index_print(TableCell *table, Oid table_oid)
{
    Size suggested_index_len = NAMEDATALEN * table->index->length;
    char *index_print = (char *)palloc0(suggested_index_len);
    ListCell *index = NULL;
    errno_t rc = EOK;
    int i = 0;

    // concatenate the candidate indexes into a string
    foreach (index, table->index) {
        char *index_name = ((IndexCell *)lfirst(index))->index_name;
        if ((strlen(index_name) + strlen(index_print) + 1) > suggested_index_len) {
            break;
        }
        if (i == 0) {
            rc = strcpy_s(index_print, suggested_index_len, index_name);
        } else {
            rc = strcat_s(index_print, suggested_index_len, ",");
            securec_check(rc, "\0", "\0");
            rc = strcat_s(index_print, suggested_index_len, index_name);
        }
        securec_check(rc, "\0", "\0");
        i++;
    }
    list_free_deep(table->index);
    table->index = NIL;

    TableCell *table_index = find_table(table);
    // check the existed indexes
    List *indexes = get_table_indexes(table_oid);
    index = NULL;

    foreach (index, indexes) {
        List *attnames = get_index_attname(lfirst_oid(index));
        if (attnames == NIL) {
            continue;
        }
        char *existed_index = (char *)palloc0(NAMEDATALEN * attnames->length);
        ListCell *item = NULL;
        i = 0;

        foreach (item, attnames) {
            if (i == 0) {
                rc = strcpy_s(existed_index, NAMEDATALEN * attnames->length, (char *)lfirst(item));
            } else {
                rc = strcat_s(existed_index, NAMEDATALEN * attnames->length, ",");
                securec_check(rc, "\0", "\0");
                rc = strcat_s(existed_index, NAMEDATALEN * attnames->length, (char *)lfirst(item));
            }
            securec_check(rc, "\0", "\0");
            i++;
        }
        list_free(attnames);
        // the suggested index has existed
        if (strcasecmp(existed_index, index_print) == 0) {
            if (table_index == NULL) {
                g_table_list = lappend(g_table_list, table);
            }
            pfree(index_print);
            pfree(existed_index);
            return;
        }
        pfree(existed_index);
    }
    if (table_index != NULL) {
        // check the previous suggested indexes
        ListCell *prev_index = NULL;
        IndexPrint *suggest_index = NULL;
        suggest_index = generat_index_print(table, index_print);
        foreach (prev_index, table_index->index_print) {
            if (strncasecmp(((IndexPrint *)lfirst(prev_index))->index_columns, index_print, strlen(index_print)) == 0) {
                if (((IndexPrint *)lfirst(prev_index))->index_type != suggest_index->index_type) {
                    ((IndexPrint *)lfirst(prev_index))->index_type = "global";
                }
                pfree(index_print);
                return;
            }
        }
        table_index->index_print = lappend(table_index->index_print, suggest_index);
    } else {
        table->index_print = lappend(table->index_print, generat_index_print(table, index_print));
        g_table_list = lappend(g_table_list, table);
    }

    return;
}
void generate_final_index()
{
    ListCell *table_item = NULL;
    foreach (table_item, t_thrd.index_advisor_cxt.stmt_table_list) {
        TableCell *table = (TableCell *)lfirst(table_item);
        if (table->index != NIL) {
            RangeVar *rtable = makeRangeVar(table->schema_name, table->table_name, -1);
            Oid table_oid = RangeVarGetRelid(rtable, NoLock, true);
            if (table_oid == InvalidOid) {
                continue;
            }
            generate_final_index_print(table, table_oid);
        } else if (find_table(table) == NULL) {
            g_table_list = lappend(g_table_list, table);
        }
    }
}
void parse_where_clause(Node *item_where)
{
    if (item_where == NULL) {
        return;
    }

    switch (nodeTag(item_where)) {
        case T_A_Expr: {
            A_Expr *expr = (A_Expr *)item_where;
            Node *lexpr = expr->lexpr;
            Node *rexpr = expr->rexpr;
            List *field_value = NIL;

            if (lexpr == NULL || rexpr == NULL) {
                return;
            }

            switch (expr->kind) {
                case AEXPR_OP: { // normal operator
                    if (IsA(lexpr, ColumnRef) && IsA(rexpr, ColumnRef)) {
                        // where t1.c1 = t2.c2
                        parse_join_expr(item_where);
                    } else if (IsA(lexpr, ColumnRef) && IsA(rexpr, A_Const)) {
                        // where t1.c1...
                        field_value = lappend(field_value, (A_Const *)rexpr);
                        parse_field_expr(((ColumnRef *)lexpr)->fields, expr->name, field_value);
                    }
                    break;
                }
                case AEXPR_AND: {
                    // where...and...
                    if (IsA(lexpr, A_Expr)) {
                        parse_where_clause(lexpr);
                    }
                    if (IsA(rexpr, A_Expr)) {
                        parse_where_clause(rexpr);
                    }
                    break;
                }
                case AEXPR_IN: { // where...in...
                    if (IsA(lexpr, ColumnRef) && IsA(rexpr, List)) {
                        field_value = (List *)rexpr;
                        parse_field_expr(((ColumnRef *)lexpr)->fields, expr->name, field_value);
                    }
                    break;
                }
                default: {
                    break;
                }
            }
        }
        case T_NullTest: {
            break;
        }
        default: {
            break;
        }
    }
}

void parse_from_clause(List *from_list)
{
    ListCell *from = NULL;
    foreach (from, from_list) {
        Node *item = (Node *)lfirst(from);
        if (nodeTag(item) == T_RangeVar) {
            // single table
            RangeVar *table = (RangeVar *)item;
            if (table->alias) {
                (void)find_or_create_tblcell(table->relname, table->alias->aliasname, table->schemaname,
                    table->ispartition, table->issubpartition);
            } else {
                (void)find_or_create_tblcell(table->relname, NULL, table->schemaname, table->ispartition,
                    table->issubpartition);
            }
        } else if (nodeTag(item) == T_JoinExpr) {
            // multi-table join
            parse_join_tree((JoinExpr *)item);
        } else if(nodeTag(item) == T_RangeSubselect) {
            SelectStmt *stmt = (SelectStmt *)(((RangeSubselect *)item)->subquery);
            if (stmt->fromClause) {
                parse_from_clause(stmt->fromClause);
            }
        } else {
        }
    }
}

void parse_target_list(List *target_list)
{
    ListCell *target_cell = NULL;
    foreach (target_cell, target_list) {
        Node *item = (Node *)lfirst(target_cell);
        if (nodeTag(item) != T_ResTarget) {
            continue;
        }
        ResTarget *target = (ResTarget *)item;
        if (nodeTag(target->val) != T_ColumnRef) {
            continue;
        }
        // only when the alias name exist, we need to save it.
        if (target->name) {
            (void)find_or_create_clmncell(&target->name, ((ColumnRef *)(target->val))->fields);
        }
    }
}

void add_drived_tables(RangeVar *join_node)
{
    TableCell *join_table = NULL;

    if (join_node->alias) {
        join_table = find_or_create_tblcell(join_node->relname, join_node->alias->aliasname, join_node->schemaname,
            join_node->ispartition, join_node->issubpartition);
    } else {
        join_table = find_or_create_tblcell(join_node->relname, NULL, join_node->schemaname, join_node->ispartition,
            join_node->issubpartition);
    }

    if (!join_table) {
        return;
    }
    if (!list_member(g_drived_tables, join_table)) {
        g_drived_tables = lappend(g_drived_tables, join_table);
    }
}

void parse_join_tree(JoinExpr *join_tree)
{
    Node *larg = join_tree->larg;
    Node *rarg = join_tree->rarg;

    if (nodeTag(larg) == T_JoinExpr) {
        parse_join_tree((JoinExpr *)larg);
    } else if (nodeTag(larg) == T_RangeVar) {
        add_drived_tables((RangeVar *)larg);
    } else if (nodeTag(larg) == T_RangeSubselect) {
        SelectStmt *stmt = (SelectStmt *)(((RangeSubselect *)larg)->subquery);
        if (stmt->fromClause) {
            parse_from_clause(stmt->fromClause);
        }
    }

    if (nodeTag(rarg) == T_JoinExpr) {
        parse_join_tree((JoinExpr *)rarg);
    } else if (nodeTag(rarg) == T_RangeVar) {
        add_drived_tables((RangeVar *)rarg);
    } else if (nodeTag(rarg) == T_RangeSubselect) {
        SelectStmt *stmt = (SelectStmt *)(((RangeSubselect *)rarg)->subquery);
        if (stmt->fromClause) {
            parse_from_clause(stmt->fromClause);
        }
    }

    if (join_tree->isNatural == true) {
        return;
    } 

    if (join_tree->usingClause) {
        parse_join_expr(join_tree);
    } else {
        parse_join_expr(join_tree->quals);
    }
}

void add_join_cond(TableCell *begin_table, char *begin_field, TableCell *end_table, char *end_field)
{
    index_advisor::JoinCell *join_cond = NULL;
    join_cond = (index_advisor::JoinCell *)palloc0(sizeof(*join_cond));
    join_cond->field = begin_field;
    join_cond->joined_table = end_table;
    join_cond->joined_field = end_field;
    begin_table->join_cond = lappend(begin_table->join_cond, join_cond);
}

bool check_join_expr(A_Expr *join_cond)
{
    return (join_cond->lexpr && join_cond->rexpr);
}
// parse 'join ... on'
void parse_join_expr(Node *item_join)
{
    if (!item_join || nodeTag(item_join) != T_A_Expr) {
        return;
    }
    A_Expr *join_cond = (A_Expr *)item_join;
    if (!check_join_expr(join_cond)) {
        return;
    }
    List *field_value = NIL;
    if (IsA(join_cond->lexpr, ColumnRef) && IsA(join_cond->rexpr, A_Const)){
        // on t1.c1...
        field_value = lappend(field_value, (A_Const *)join_cond->rexpr);
        parse_field_expr(((ColumnRef *)join_cond->lexpr)->fields, join_cond->name, field_value);
    } else if (IsA(join_cond->rexpr, ColumnRef) && IsA(join_cond->lexpr, A_Const)){
        // on ...t1.c1
        field_value = lappend(field_value, (A_Const *)join_cond->lexpr);
        parse_field_expr(((ColumnRef *)join_cond->rexpr)->fields, join_cond->name, field_value);
    } else if (IsA(join_cond->lexpr, ColumnRef) && IsA(join_cond->rexpr, ColumnRef)){
        // on t1.c1 = t2.c2
        List *l_join_fields = ((ColumnRef *)(join_cond->lexpr))->fields;
        List *r_join_fields = ((ColumnRef *)(join_cond->rexpr))->fields;

        char *l_schema_name = NULL, *r_schema_name = NULL;
        char *l_table_name = NULL, *r_table_name = NULL;
        char *l_field_name = NULL, *r_field_name = NULL;

        split_field_list(l_join_fields, &l_schema_name, &l_table_name, &l_field_name);
        split_field_list(r_join_fields, &r_schema_name, &r_table_name, &r_field_name);

        if (!l_schema_name || !r_schema_name || !l_table_name || !r_table_name || !l_field_name || !r_field_name) {
            return;
        }

        TableCell *ltable = find_or_create_tblcell(l_table_name, NULL, l_schema_name);
        TableCell *rtable = find_or_create_tblcell(r_table_name, NULL, r_schema_name);

        if (!ltable || !rtable) {
            return;
        }

        // add join conditons
        add_join_cond(ltable, l_field_name, rtable, r_field_name);
        add_join_cond(rtable, r_field_name, ltable, l_field_name);

        // add possible drived tables
        if (!list_member(g_drived_tables, ltable)) {
            g_drived_tables = lappend(g_drived_tables, ltable);
        }
        if (!list_member(g_drived_tables, rtable)) {
            g_drived_tables = lappend(g_drived_tables, rtable);
        }
    }
    list_free_ext(field_value);
}

// parse 'join ... using'
void parse_join_expr(JoinExpr *join_tree)
{
    if (!join_tree) {
        return;
    }

    /* The left and right nodes of the join condition may be a sub-query (T_RangeSubselect),
     * especially the sub-query involving aliases.
     * The parsing process will be very complicated, 
     * so skip it directly and analyze it by the method based on the execution plan later. 
     */
    if (nodeTag(join_tree->larg) != T_RangeVar ||
        nodeTag(join_tree->rarg) != T_RangeVar) {
        return;
    }

    List *join_fields = join_tree->usingClause;
    RangeVar *l_join_node = (RangeVar *)(join_tree->larg);
    RangeVar *r_join_node = (RangeVar *)(join_tree->rarg);
    TableCell *ltable, *rtable = NULL;

    if (l_join_node->alias) {
        ltable = find_or_create_tblcell(l_join_node->relname, l_join_node->alias->aliasname, l_join_node->schemaname,
            l_join_node->ispartition, l_join_node->issubpartition);
    } else {
        ltable = find_or_create_tblcell(l_join_node->relname, NULL, l_join_node->schemaname, l_join_node->ispartition,
            l_join_node->issubpartition);
    }
    if (r_join_node->alias) {
        rtable = find_or_create_tblcell(r_join_node->relname, r_join_node->alias->aliasname, r_join_node->schemaname,
            r_join_node->ispartition, l_join_node->issubpartition);
    } else {
        rtable = find_or_create_tblcell(r_join_node->relname, NULL, r_join_node->schemaname, r_join_node->ispartition,
            l_join_node->issubpartition);
    }

    if (!ltable || !rtable) {
        return;
    }

    // add join conditons
    ListCell *item = NULL;

    foreach (item, join_fields) {
        char *field_name = strVal(lfirst(item));
        add_join_cond(ltable, field_name, rtable, field_name);
        add_join_cond(rtable, field_name, ltable, field_name);
    }
}

// convert field value to string
void field_value_trans(_out_ StringInfoData target, A_Const *field_value)
{
    Value value = field_value->val;

    if (value.type == T_Integer) {
        pg_itoa(value.val.ival, target.data);
    } else if (value.type == T_String) {
        appendStringInfo(&target, "'%s'", value.val.str);
    }
}

/*
 * parse_field_expr
 *     Parse the field expression and add index.
 *
 * An index is added from the field only if the cardinality of the field
 * is greater than the threshold.
 */
void parse_field_expr(List *field, List *op, List *lfield_values)
{
    char *schema_name = NULL, *table_name = NULL, *col_name = NULL;
    split_field_list(field, &schema_name, &table_name, &col_name);
    
    if (!schema_name || !table_name || !col_name) {
        return;
    }

    char *op_type = strVal(linitial(op));
    StringInfoData field_expr;
    initStringInfo(&field_expr);
    StringInfoData field_value;
    initStringInfo(&field_value);
    ListCell *item = NULL;
    int i = 0;

    // get field values
    foreach (item, lfield_values) {
        StringInfoData str;
        initStringInfo(&str);
        if (!IsA(lfirst(item), A_Const)) {
            continue;
        }
        field_value_trans(str, (A_Const *)lfirst(item));
        if (strlen(str.data) == 0) {
            continue;
        }
        if (i == 0) {
            appendStringInfoString(&field_value, str.data);
        } else {
            appendStringInfo(&field_value, ",%s", str.data);
        }
        i++;
        pfree(str.data);
    }

    if (i == 0) {
        pfree(field_value.data);
        pfree(field_expr.data);
        return;
    }

    // get field expression, e.g., 'id = 100'
    if (strcasecmp(op_type, "~~") == 0) {
        // ... like ...
        if (field_value.data != NULL && field_value.data[1] == '%') {
            pfree_ext(field_value.data);
            pfree_ext(field_expr.data);
            return;
        } else {
            appendStringInfo(&field_expr, "%s like %s", col_name, field_value.data);
        }
    } else if (lfield_values->length > 1) {
        // ... (not) in ...
        if (strcasecmp(op_type, "=") == 0) {
            appendStringInfo(&field_expr, "%s in (%s)", col_name, field_value.data);
        } else {
            appendStringInfo(&field_expr, "%s not in (%s)", col_name, field_value.data);
        }
    } else {
        // ... >=< ...
        appendStringInfo(&field_expr, "%s %s %s", col_name, op_type, field_value.data);
    }
    pfree_ext(field_value.data);

    uint4 cardinality = calculate_field_cardinality(schema_name, table_name, field_expr.data);
    if (cardinality > CARDINALITY_THRESHOLD) {
        IndexCell *index = (IndexCell *)palloc0(sizeof(*index));
        index->index_name = col_name;
        index->cardinality = cardinality;
        index->op = op_type;
        index->field_expr = pstrdup(field_expr.data);
        add_index_from_field(schema_name, table_name, index);
    }
    pfree_ext(field_expr.data);
}

inline uint4 tuple_to_uint(List *tuples)
{
    return pg_atoi((char *)linitial((List *)linitial(tuples)), sizeof(uint4), 0);
}

uint4 get_table_count(char *schema_name, char *table_name)
{
    uint4 table_count = 0;
    HeapTuple tp;

    RangeVar* rtable = makeRangeVar(schema_name, table_name, -1);
    Oid relid = RangeVarGetRelid(rtable, NoLock, true);
    if (relid == InvalidOid) {
        return table_count;
    }

    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        table_count = static_cast<uint4>(reltup->reltuples);
        ReleaseSysCache(tp);
    } else {
        return table_count;
    }

    return table_count;
}

uint4 calculate_field_cardinality(char *schema_name, char *table_name, const char *field_expr)
{
    const int sample_factor = 2;

    uint4 cardinality;
    uint4 table_count = get_table_count(schema_name, table_name);
    uint4 sample_rows = (table_count / sample_factor) > MAX_SAMPLE_ROWS ? MAX_SAMPLE_ROWS : (table_count / sample_factor);
    StringInfoData query_string;
    initStringInfo(&query_string);
    appendStringInfo(&query_string, "select count(*) from ( select * from %s.%s limit %d) where %s",
        schema_name, table_name, sample_rows, field_expr);

    StmtResult *result = execute_stmt(query_string.data, true);
    pfree_ext(query_string.data);
    uint4 row = tuple_to_uint(result->tuples);
    (*result->pub.rDestroy)((DestReceiver *)result);

    cardinality = row == 0 ? sample_rows : (sample_rows / row);

    return cardinality;
}

void split_field_list(List *fields, char **schema_name_ptr, char **table_name_ptr, char **col_name_ptr)
{
    if (fields == NULL) {
        return;
    }

    ListCell *item = NULL;
    ListCell *sub_item = NULL;
    bool found_table = false;
    *schema_name_ptr = *table_name_ptr = *col_name_ptr = NULL;
    if (fields->length == 3) {
        *schema_name_ptr = strVal(linitial(fields));
        *table_name_ptr = strVal(lsecond(fields));
        *col_name_ptr = strVal(lthird(fields));
    } else if (fields->length == 2) {
        *table_name_ptr = strVal(linitial(fields));
        *col_name_ptr = strVal(lsecond(fields));
    } else if (fields->length == 1) {
        *col_name_ptr = strVal(linitial(fields));
    }
    // if fields have table name
    if (*table_name_ptr != NULL) {
        // check existed tables
        foreach (item, t_thrd.index_advisor_cxt.stmt_table_list) {
            TableCell *cur_table = (TableCell *)lfirst(item);
            if (*schema_name_ptr != NULL && strcasecmp(*schema_name_ptr, cur_table->schema_name) != 0) {
                continue;
            }
            if (strcasecmp(*table_name_ptr, cur_table->table_name) == 0) {
                found_table = true;
                *schema_name_ptr = cur_table->schema_name;
                break;
            }
            foreach (sub_item, cur_table->alias_name) {
                char *cur_alias_name = (char *)lfirst(sub_item);
                if (strcasecmp(*table_name_ptr, cur_alias_name) == 0) {
                    *schema_name_ptr = cur_table->schema_name;
                    *table_name_ptr = cur_table->table_name;
                    found_table = true;
                    break;
                }
            }
            if (found_table == true) {
                break;
            }
        }
        if (found_table == false) {
            *schema_name_ptr = *table_name_ptr = *col_name_ptr = NULL;
            return;
        }
    } else { // if fields only have field name
        find_table_by_column(schema_name_ptr, table_name_ptr, col_name_ptr);
    }
}

// check which table has the given column_name
void find_table_by_column(char **schema_name_ptr, char **table_name_ptr, char **col_name_ptr)
{
    ListCell *lc = NULL;
    int cnt = 0;
    RangeVar *rtable;
    Oid relid;
    Relation relation;
    TupleDesc tupdesc;
    TargetCell *target = NULL;
    target = find_or_create_clmncell(col_name_ptr, NULL);
    foreach (lc, t_thrd.index_advisor_cxt.stmt_table_list) {
        char *schema_name = ((TableCell *)lfirst(lc))->schema_name;
        char *table_name = ((TableCell *)lfirst(lc))->table_name;

        rtable = makeRangeVar(schema_name, table_name, -1);
        relid = RangeVarGetRelid(rtable, NoLock, true);

        relation = heap_open(relid, AccessShareLock);
        tupdesc = RelationGetDescr(relation);
        for (int attrIdx = tupdesc->natts - 1; attrIdx >= 0; --attrIdx) {
            if (strcasecmp(*col_name_ptr, RelAttrName(tupdesc, attrIdx)) == 0) {
                cnt += 1;
                *schema_name_ptr = schema_name;
                *table_name_ptr = table_name;
                break;
            }
        }
        if (cnt == 1) {
            heap_close(relation, AccessShareLock);
            break;
        }
        // handle alias name scene
        if (target == NULL) {
            heap_close(relation, AccessShareLock);
            continue;
        }
        for (int attrIdx = tupdesc->natts - 1; attrIdx >= 0; --attrIdx) {
            if (strcasecmp(target->column_name, RelAttrName(tupdesc, attrIdx)) == 0) {
                cnt += 1;
                *schema_name_ptr = schema_name;
                *table_name_ptr = table_name;
                *col_name_ptr = target->column_name;
                break;
            }
        }
        heap_close(relation, AccessShareLock);
        if (cnt == 1) {
            break;
        }
    }

    if (cnt != 1) {
        *schema_name_ptr = *table_name_ptr = *col_name_ptr = NULL;
    }
}

// Get the partition keys of the partition table or subpartition table
void get_partition_key_name(Relation rel, TableCell *table, bool is_subpartition)
{
    int partkey_column_n = 0;
    int2vector *partkey_column = NULL;
    partkey_column = GetPartitionKey(rel->partMap);
    partkey_column_n = partkey_column->dim1;
    for (int i = 0; i < partkey_column_n; i++) {
        table->partition_key_list =
            lappend(table->partition_key_list, get_attname(rel->rd_id, partkey_column->values[i]));
    }
    if (is_subpartition) {
        List *partOidList = relationGetPartitionOidList(rel);
        Assert(list_length(partOidList) != 0);
        Partition subPart = partitionOpen(rel, linitial_oid(partOidList), NoLock);
        Relation subPartRel = partitionGetRelation(rel, subPart);
        int subpartkey_column_n = 0;
        int2vector *subpartkey_column = NULL;
        subpartkey_column = GetPartitionKey(subPartRel->partMap);
        subpartkey_column_n = subpartkey_column->dim1;
        for (int i = 0; i < subpartkey_column_n; i++) {
            table->subpartition_key_list =
                lappend(table->subpartition_key_list, get_attname(rel->rd_id, subpartkey_column->values[i]));
        }
        releaseDummyRelation(&subPartRel);
        partitionClose(rel, subPart, NoLock);
    }
}

// Check whether the table type is supported.
bool check_relation_type_valid(Oid relid)
{
    Relation relation;
    bool result = false;

    relation = heap_open(relid, AccessShareLock);
    if (RelationIsValid(relation) == false) {
        heap_close(relation, AccessShareLock);
        return result;
    }
    if (RelationIsRelation(relation) && RelationGetStorageType(relation) == HEAP_DISK &&
        RelationGetRelPersistence(relation) == RELPERSISTENCE_PERMANENT &&
        !RelationIsSubPartitioned(relation)) {
        const char *format = ((relation->rd_options) && (((StdRdOptions *)(relation->rd_options))->orientation)) ?
            ((char *)(relation->rd_options) + *(int *)&(((StdRdOptions *)(relation->rd_options))->orientation)) :
            ORIENTATION_ROW;
        if (pg_strcasecmp(format, ORIENTATION_ROW) == 0) {
            result = true;
        }          
    }

    heap_close(relation, AccessShareLock);

    return result;
}

bool is_tmp_table(const char *table_name)
{
    ListCell *item = NULL;

    foreach (item, g_tmp_table_list) {
        char *tmp_table_name = (char *)lfirst(item);
        if (strcasecmp(tmp_table_name, table_name) == 0) {
            return true;
        }
    }

    return false;
}

/*
 * find_or_create_tblcell
 *     Find the TableCell that has given table_name(alias_name) among the global
 *     variable 't_thrd.index_advisor_cxt.stmt_table_list', and if not found, create a TableCell and return it.
 */
TableCell *find_or_create_tblcell(char *table_name, char *alias_name, char *schema_name, bool ispartition,
    bool issubpartition)
{
    if (!table_name) {
        return NULL;
    }
    if (is_tmp_table(table_name)) {
        ereport(WARNING, (errmsg("can not advise for table %s because it is a temporary table.", table_name)));
        return NULL;
    }

    // seach the table among existed tables
    ListCell *item = NULL;
    ListCell *sub_item = NULL;

    if (t_thrd.index_advisor_cxt.stmt_table_list != NIL) {
        foreach (item, t_thrd.index_advisor_cxt.stmt_table_list) {
            TableCell *cur_table = (TableCell *)lfirst(item);
            char *cur_schema_name = cur_table->schema_name;
            char *cur_table_name = cur_table->table_name;
            if (IsSameRel(cur_schema_name, cur_table_name, schema_name, table_name)) {
                if (alias_name) {
                    foreach (sub_item, cur_table->alias_name) {
                        char *cur_alias_name = (char *)lfirst(sub_item);
                        if (strcasecmp(cur_alias_name, alias_name) == 0) {
                            return cur_table;
                        }
                    }
                    cur_table->alias_name = lappend(cur_table->alias_name, alias_name);
                }
                return cur_table;
            }
            foreach (sub_item, cur_table->alias_name) {
                char *cur_alias_name = (char *)lfirst(sub_item);
                if (IsSameRel(cur_schema_name, cur_alias_name, schema_name, table_name)) {
                    return cur_table;
                }           
            }
        }
    }

    RangeVar* rtable = makeRangeVar(schema_name, table_name, -1);
    Oid table_oid = RangeVarGetRelid(rtable, NoLock, true);
    if (table_oid == InvalidOid || check_relation_type_valid(table_oid) == false) {
        ereport(WARNING, (errmsg("can not advise for table %s due to invalid oid or irregular table.", table_name))); 
        return NULL;                                                                
    }

    // create a new table
    TableCell *new_table = NULL;
    new_table = (TableCell *)palloc0(sizeof(*new_table));
    if (schema_name == NULL) {
        new_table->schema_name = get_namespace_name(get_rel_namespace(table_oid));
    } else {
        new_table->schema_name = schema_name;
    }
    new_table->table_name = table_name;
    new_table->alias_name = NIL;
    if (alias_name) {
        new_table->alias_name = lappend(new_table->alias_name, alias_name);
    }
    new_table->index = NIL;
    new_table->join_cond = NIL;
    new_table->index_print = NIL;
    new_table->partition_key_list = NIL;
    new_table->subpartition_key_list = NIL;
    new_table->ispartition = ispartition;
    new_table->issubpartition = issubpartition;
    // set the partition key of the partition table, including partition and subpartition
    Relation rel = heap_open(table_oid, AccessShareLock);
    if RelationIsPartitioned(rel) {
        if RelationIsSubPartitioned(rel) {
            get_partition_key_name(rel, new_table, true);
        } else {
            get_partition_key_name(rel, new_table);
        }
    }
    heap_close(rel, AccessShareLock);
    t_thrd.index_advisor_cxt.stmt_table_list = lappend(t_thrd.index_advisor_cxt.stmt_table_list, new_table);
    return new_table;
}

/*
 * find_or_create_clmncell
 *     Find the columnCell that has given column_name(alias_name) among the global
 *     variable 't_thrd.index_advisor_cxt.stmt_target_list', and if not found, create a columnCell and return it.
 * we consider column alias name is unique within a select query
 */
TargetCell *find_or_create_clmncell(char **alias_name, List *fields)
{
    if (fields == NIL) {
        // find the columnCell according to the alias_name
        ListCell *target_cell = NULL;
        foreach (target_cell, t_thrd.index_advisor_cxt.stmt_target_list) {
            TargetCell *target = (TargetCell *)lfirst(target_cell);
            if (strcasecmp(target->alias_name, *alias_name) == 0) {
                return target;
            }
        }
        return NULL;
    }
    // create a columnCell and return it
    TargetCell *new_target = NULL;
    new_target = (TargetCell *)palloc0(sizeof(TargetCell));
    new_target->alias_name = *alias_name;
    new_target->column_name = strVal(linitial(fields));
    t_thrd.index_advisor_cxt.stmt_target_list = lappend(t_thrd.index_advisor_cxt.stmt_target_list, new_target);

    return new_target;
}

/*
 * add_index_from_field
 *     Add index from field for the table.
 *
 * The index sorting rule is as follows: the index with operator'=' is ranked first,
 * and the index with higher cardinality is ranked first.
 */
void add_index_from_field(char *schema_name, char *table_name, IndexCell *index)
{
    TableCell *table = find_or_create_tblcell(table_name, NULL, schema_name);
    ListCell *table_index = NULL;
    if (table == NULL) {
        return;
    }

    if (table == NULL) {
        return;
    }

    if (table->index == NIL) {
        table->index = lappend(table->index, index);
    } else {
        ListCell *prev = NULL;

        foreach (table_index, table->index) {
            char *table_index_name = ((IndexCell *)lfirst(table_index))->index_name;
            char *table_index_op = ((IndexCell *)lfirst(table_index))->op;
            uint4 table_index_cardinality = ((IndexCell *)lfirst(table_index))->cardinality;

            if (strcasecmp(table_index_name, index->index_name) == 0) {
                break;
            }
            if (strcasecmp(table_index_op, "=") == 0) {
                if (strcasecmp(index->op, "=") == 0 && table_index_cardinality < index->cardinality) {
                    if (prev == NULL) {
                        table->index = lcons(index, table->index);
                    } else {
                        (void)lappend_cell(table->index, prev, index);
                    }
                    break;
                } else if (table_index->next == NULL) {
                    table->index = lappend(table->index, index);
                    break;
                }
            } else {
                if (strcasecmp(index->op, "=") == 0) {
                    if (prev == NULL) {
                        table->index = lcons(index, table->index);
                    } else {
                        (void)lappend_cell(table->index, prev, index);
                    }
                    break;
                } else if (table_index_cardinality < index->cardinality) {
                    if (prev == NULL) {
                        table->index = lcons(index, table->index);
                    } else {
                        (void)lappend_cell(table->index, prev, index);
                    }
                    break;
                }
            }
            prev = table_index;
        }
    }
}

// Check if there are joined tables
static bool check_joined_tables()
{
    return (g_drived_tables && g_drived_tables->length > 1);
}

// The table that has smallest result set is set as the driver table.
static void determine_driver_table(int local_stmt_table_count)
{
    if (local_stmt_table_count == 1 && (!check_joined_tables())) {
        g_driver_table = (TableCell *)linitial(t_thrd.index_advisor_cxt.stmt_table_list);
    } else {
        if (g_drived_tables != NIL) {
            uint4 small_result_set = UINT32_MAX;
            ListCell *item = NULL;

            foreach (item, g_drived_tables) {
                TableCell *table = (TableCell *)lfirst(item);
                uint4 result_set;
                if (table->index) {
                    result_set = get_join_table_result_set(table->schema_name, table->table_name,
                        ((IndexCell *)linitial(table->index))->field_expr);
                } else {
                    result_set = get_table_count(table->schema_name,table->table_name);
                }
                if (result_set < small_result_set) {
                    g_driver_table = table;
                    small_result_set = result_set;
                }
            }
        }
    }
}

uint4 get_join_table_result_set(const char *schema_name, const char *table_name, const char *condition)
{
    StringInfoData query_string;
    initStringInfo(&query_string);
    if (condition != NULL) {
        appendStringInfo(&query_string, "select * from %s.%s where %s;", schema_name, table_name, condition);
    }

    /* get query execution plan */
    List *parsetree_list = pg_parse_query(query_string.data, NULL);
    ListCell *parsetree_item = list_head(parsetree_list);
    Node *parsetree = (Node *)lfirst(parsetree_item);
    List *querytree_list = pg_analyze_and_rewrite(parsetree, query_string.data, NULL, 0);
    List *plantree_list = pg_plan_queries(querytree_list, 0, NULL);
    PlannedStmt *plan_stmt = (PlannedStmt *)lfirst(list_head(plantree_list));
    Scan *scan = (Scan *)(plan_stmt->planTree);
    uint4 rows = (uint4)scan->plan.plan_rows;
    list_free_ext(parsetree_list);
    list_free_ext(plantree_list);
    list_free_ext(querytree_list);
    return rows;
}

void add_index_from_join(TableCell *table, char *index_name)
{
    if (index_name == NULL) {
        return;
    }
    
    ListCell *item = NULL;

    foreach (item, table->index) {
        char *table_index_name = ((IndexCell *)lfirst(item))->index_name;
        if (strcasecmp(table_index_name, index_name) == 0) {
            return;
        }
    }

    IndexCell *index = (IndexCell *)palloc0(sizeof(*index));
    index->index_name = index_name;
    index->cardinality = 0;
    index->op = NULL;
    index->field_expr = NULL;
    table->index = lcons(index, table->index);
}

// Add index for the drived tables accordint to the join conditions.
void add_index_for_drived_tables()
{
    List *joined_tables = NIL;
    List *to_be_joined_tables = NIL;
    to_be_joined_tables = lappend(to_be_joined_tables, g_driver_table);
    TableCell *join_table;

    while (to_be_joined_tables != NIL) {
        join_table = (TableCell *)linitial(to_be_joined_tables);
        to_be_joined_tables = list_delete_cell2(to_be_joined_tables, list_head(to_be_joined_tables));
        if (list_member(joined_tables, join_table)) {
            continue;
        }
        List *join_list = join_table->join_cond;
        ListCell *item = NULL;

        foreach (item, join_list) {
            index_advisor::JoinCell *join_cond = (index_advisor::JoinCell *)lfirst(item);
            TableCell *to_be_joined_table = join_cond->joined_table;
            if (list_member(joined_tables, to_be_joined_table) || to_be_joined_table == NULL) {
                continue;
            }
            if (join_table != g_driver_table) {
                add_index_from_join(join_table, join_cond->field);
            }
            add_index_from_join(to_be_joined_table, join_cond->joined_field);

            to_be_joined_tables = lappend(to_be_joined_tables, to_be_joined_table);
        }
        joined_tables = lappend(joined_tables, join_table);

        list_free_deep(join_list);
        join_table->join_cond = NIL;
    }

    list_free(joined_tables);
    list_free(to_be_joined_tables);
}

Node *transform_group_order_node(Node *node, List *target_list)
{
    Value *val = &((A_Const *)node)->val;
    Assert(IsA(val, Integer));
    long target_pos = intVal(val);
    Assert(target_pos <= list_length(target_list));
    ResTarget *rt = (ResTarget *)list_nth(target_list, target_pos - 1);
    node = rt->val;

    return node;
}

bool parse_group_clause(List *group_clause, List *target_list)
{
    if (group_clause == NULL)
        return false;

    bool is_only_one_table = true;
    ListCell *group_item = NULL;
    char *pre_schema = NULL, *pre_table = NULL;
    char *cur_schema = NULL, *cur_table = NULL, *cur_col = NULL;

    foreach (group_item, group_clause) {
        Node *node = (Node *)lfirst(group_item);

        if (nodeTag(node) == T_A_Const) {
            node = transform_group_order_node(node, target_list);
        }
        if (nodeTag(node) != T_ColumnRef)
            break;
        List *fields = ((ColumnRef *)node)->fields;
        split_field_list(fields, &cur_schema, &cur_table, &cur_col);
        if (!cur_schema || !cur_table) {
            return false;
        }
        if (pre_schema && pre_table && !IsSameRel(pre_schema, pre_table, cur_schema, cur_table)) {
            is_only_one_table = false;
            break;
        }
        pre_schema = cur_schema;
        pre_table = cur_table;
    }

    if (is_only_one_table && pre_schema && pre_table) {
        if (g_driver_table &&
            IsSameRel(pre_schema, pre_table, g_driver_table->schema_name, g_driver_table->table_name)) {
            return true;
        }
    }

    return false;
}

bool has_star(List *targetList)
{
    ListCell* target_cell = NULL;
    foreach (target_cell, targetList) {
        ResTarget* restarget = (ResTarget *)lfirst(target_cell);
        if (nodeTag((Node *)restarget->val) == T_ColumnRef) {
            ColumnRef* cref = NULL;
            cref = (ColumnRef *)restarget->val;
            ListCell* field = NULL;
            foreach (field, cref->fields) {
                if (nodeTag((Node *)lfirst(field)) == T_A_Star) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool parse_order_clause(List *order_clause, List *target_list)
{
    if (order_clause == NULL)
        return false;

    bool is_only_one_table = true;
    ListCell *order_item = NULL;
    char *pre_schema = NULL, *pre_table = NULL;
    char *cur_schema = NULL, *cur_table = NULL, *cur_col = NULL;
    SortByDir pre_dir = SORTBY_DEFAULT;

    foreach (order_item, order_clause) {
        Node *node = ((SortBy *)lfirst(order_item))->node;
        SortByDir dir = ((SortBy *)(lfirst(order_item)))->sortby_dir;

        if (nodeTag(node) == T_A_Const) {
            if (has_star(target_list)) {
                return false;
            }
            node = transform_group_order_node(node, target_list);
        }
        if (nodeTag(node) != T_ColumnRef)
            break;
        List *fields = ((ColumnRef *)node)->fields;
        split_field_list(fields, &cur_schema, &cur_table, &cur_col);
        if (!cur_schema || !cur_table) {
            return false;
        }
        if (pre_schema && pre_table && (!IsSameRel(pre_schema, pre_table, cur_schema, cur_table) || dir != pre_dir)) {
            is_only_one_table = false;
            break;
        }
        pre_schema = cur_schema;
        pre_table = cur_table;
    }

    if (is_only_one_table && pre_schema && pre_table) {
        if (g_driver_table &&
            IsSameRel(pre_schema, pre_table, g_driver_table->schema_name, g_driver_table->table_name)) {
            return true;
        }
    }

    return false;
}

/*
 * add_index_from_group_order
 *     Add index from the group or order clause.
 *
 * The index from goup or order clause is added after the index with operator '='.
 */
void add_index_from_group_order(TableCell *table, List *clause, List *target_list, bool flag_group_order)
{
    ListCell *item = NULL;
    char *schema_name = NULL, *table_name = NULL, *index_name = NULL;

    foreach (item, clause) {
        Node *node = NULL;
        List *fields = NULL;

        if (flag_group_order) {
            node = (Node *)lfirst(item);
        } else {
            node = ((SortBy *)lfirst(item))->node;
        }
        if (nodeTag(node) == T_A_Const) {
            node = transform_group_order_node(node, target_list);
        }
        if (nodeTag(node) != T_ColumnRef)
            break;
        fields = ((ColumnRef *)node)->fields;
        split_field_list(fields, &schema_name, &table_name, &index_name);
        add_index(table, index_name);
    }
}
