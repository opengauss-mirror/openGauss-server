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
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "pg_config_manual.h"
#include "parser/parser.h"
#include "parser/analyze.h"
#include "securec.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#define MAX_SAMPLE_ROWS 10000    /* sampling range for executing a query */
#define CARDINALITY_THRESHOLD 30 /* the threshold of index selection */
#define MAX_QUERY_LEN 256

typedef struct {
    char table[NAMEDATALEN];
    char column[NAMEDATALEN];
} SuggestedIndex;

typedef struct {
    DestReceiver pub;
    MemoryContext mxct;
    List *tuples;
} StmtResult;

typedef struct {
    char *table_name;
    List *alias_name;
    List *index;
    List *join_cond;
    List *index_print;
} TableCell;

typedef struct {
    char *index_name;
    uint4 cardinality;
    char *op;
    char *field_expr;
} IndexCell;

typedef struct {
    char *field;
    char *table;
    char *table_field;
} JoinCell;

THR_LOCAL List *g_stmt_list = NIL;
THR_LOCAL List *g_tmp_table_list = NIL;
THR_LOCAL List *g_table_list = NIL;
THR_LOCAL List *g_drived_tables = NIL;
THR_LOCAL TableCell *g_driver_table = NULL;

static SuggestedIndex *suggest_index(const char *, _out_ int *);
static StmtResult *execute_stmt(const char *query_string, bool need_result = false);
static inline void analyze_tables(List *list);
static List *get_table_indexes(Oid oid);
static List *get_index_attname(Oid oid);
static char *search_table_attname(Oid attrelid, int2 attnum);
static DestReceiver *create_stmt_receiver();
static void receive(TupleTableSlot *slot, DestReceiver *self);
static void shutdown(DestReceiver *self);
static void startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void destroy(DestReceiver *self);
static void free_global_resource();
static void find_select_stmt(Node *);
static void extract_stmt_from_clause(List *);
static void extract_stmt_where_clause(Node *);
static void parse_where_clause(Node *);
static void field_value_trans(_out_ char *, A_Const *);
static void parse_field_expr(List *, List *, List *);
static inline uint4 tuple_to_uint(List *);
static uint4 get_table_count(const char *);
static uint4 calculate_field_cardinality(const char *, const char *);
static bool is_tmp_table(const char *);
static char *find_field_name(List *);
static char *find_table_name(List *);
static bool check_relation_type_valid(Oid);
static TableCell *find_or_create_tblcell(char *, char *);
static void add_index_from_field(char *, IndexCell *);
static char *parse_group_clause(List *, List *);
static char *parse_order_clause(List *, List *);
static void add_index_from_group_order(TableCell *, List *, List *, bool);
static void generate_final_index(TableCell *, Oid);
static void parse_from_clause(List *);
static void add_drived_tables(RangeVar *);
static void parse_join_tree(JoinExpr *);
static void add_join_cond(TableCell *, char *, TableCell *, char *);
static void parse_join_expr(Node *);
static void parse_join_expr(JoinExpr *);
static void determine_driver_table();
static uint4 get_join_table_result_set(const char *, const char *);
static void add_index_from_join(TableCell *, char *);
static void add_index_for_drived_tables();

Datum gs_index_advise(PG_FUNCTION_ARGS)
{
    FuncCallContext *func_ctx = NULL;
    SuggestedIndex *array = NULL;

    char *query = PG_GETARG_CSTRING(0);
    if (query == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("you must enter a query statement.")));
    }
#define COLUMN_NUM 2
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
        TupleDescInitEntry(tup_desc, (AttrNumber)1, "table", TEXTOID, -1, 0);
        TupleDescInitEntry(tup_desc, (AttrNumber)2, "column", TEXTOID, -1, 0);

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
        values[0] = CStringGetTextDatum(entry->table);
        values[1] = CStringGetTextDatum(entry->column);

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
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("can not parse query: %s.", query_string)));
    }

    if (list_length(parse_tree_list) > 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("can not advise for multiple queries.")));
    }

    Node *parsetree = (Node *)lfirst(list_head(parse_tree_list));
    Node* parsetree_copy = (Node*)copyObject(parsetree);
    (void)parse_analyze(parsetree_copy, query_string, NULL, 0);
    find_select_stmt(parsetree);

    if (!g_stmt_list) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("can not advise for query: %s.", query_string)));
    }

    // Parse the SelectStmt structures
    ListCell *item = NULL;

    foreach (item, g_stmt_list) {
        SelectStmt *stmt = (SelectStmt *)lfirst(item);
        parse_from_clause(stmt->fromClause);

        if (g_table_list) {
            parse_where_clause(stmt->whereClause);
            determine_driver_table();
            if (parse_group_clause(stmt->groupClause, stmt->targetList)) {
                add_index_from_group_order(g_driver_table, stmt->groupClause, stmt->targetList, true);
            } else if (parse_order_clause(stmt->sortClause, stmt->targetList)) {
                add_index_from_group_order(g_driver_table, stmt->sortClause, stmt->targetList, false);
            }
            if (g_table_list->length > 1 && g_driver_table) {
                add_index_for_drived_tables();
            }

            // Generate the final index string for each table.
            ListCell *table_item = NULL;

            foreach (table_item, g_table_list) {
                TableCell *table = (TableCell *)lfirst(table_item);
                if (table->index != NIL) {
                    RangeVar* rtable = makeRangeVar(NULL, table->table_name, -1);
                    Oid table_oid = RangeVarGetRelid(rtable, NoLock, true);
                    if (table_oid == InvalidOid) {
                        continue;
                    }
                    generate_final_index(table, table_oid);
                }
            }
            g_driver_table = NULL;
        }
    }
    if (g_table_list == NIL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("can not advise for query: %s.", query_string)));
    }

    // Format the returned result, e.g., 'table1, "(col1,col2),(col3)"'.
    int array_len = g_table_list->length;
    *len = array_len;
    SuggestedIndex *array = (SuggestedIndex *)palloc0(sizeof(SuggestedIndex) * array_len);
    errno_t rc = EOK;
    item = NULL;
    int i = 0;

    foreach (item, g_table_list) {
        TableCell *cur_table = (TableCell *)lfirst(item);
        List *index_list = cur_table->index_print;

        rc = strcpy_s((array + i)->table, NAMEDATALEN, cur_table->table_name);
        securec_check(rc, "\0", "\0");
        if (!index_list) {
            rc = strcpy_s((array + i)->column, NAMEDATALEN, "");
            securec_check(rc, "\0", "\0");
        } else {
            ListCell *cur_index = NULL;
            int j = 0;
            foreach (cur_index, index_list) {
                if (strlen((char *)lfirst(cur_index)) + strlen((array + i)->column) + 3 > NAMEDATALEN) {
                    continue;
                }
                if (j > 0) {
                    rc = strcat_s((array + i)->column, NAMEDATALEN, ",(");
                } else {
                    rc = strcpy_s((array + i)->column, NAMEDATALEN, "(");
                }
                securec_check(rc, "\0", "\0");
                rc = strcat_s((array + i)->column, NAMEDATALEN, (char *)lfirst(cur_index));
                securec_check(rc, "\0", "\0");
                rc = strcat_s((array + i)->column, NAMEDATALEN, ")");
                securec_check(rc, "\0", "\0");
                j++;
            }
        }
        i++;
        list_free_deep(index_list);
    }

    // free resources
    list_free_deep(parse_tree_list);
    free_global_resource();

    return array;
}

void free_global_resource()
{
    list_free_deep(g_drived_tables);
    list_free_deep(g_table_list);
    list_free_deep(g_tmp_table_list);
    list_free(g_stmt_list);
    g_table_list = NIL;
    g_tmp_table_list = NIL;
    g_stmt_list = NIL;
    g_drived_tables = NIL;
    g_driver_table = NULL;
}

/* Update table statistics obtained from query tree by executing the 'analyze table' statement. */
inline void analyze_tables(List *list)
{
    ListCell *item = NULL;
    foreach (item, list) {
        RangeTblEntry *entry = (RangeTblEntry *)lfirst(item);
        if (entry != NULL && entry->relname != NULL) {
            errno_t rc = EOK;

            char stmt[MAX_QUERY_LEN] = {0x00};
            rc = sprintf_s(stmt, MAX_QUERY_LEN, "analyze %s;", entry->relname);
            securec_check_ss_c(rc, "\0", "\0");
            (void)execute_stmt(stmt);
        }
    }
}

/* Search the oid of all indexes created on the table through the oid of the table, 
 * and return the index oid list. 
 *  */
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

    scan = systable_beginscan(pg_attribute, AttributeRelidNumIndexId, true, SnapshotNow, 2, key);
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
    int i;
    for (i = 0; i < attnums->dim1; i++) {
        attnum = attnums->values[i];
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
    parsetree_list = pg_parse_query(query_string, NULL);

    Assert(list_length(parsetree_list) == 1); // ought to be one query
    parsetree_item = list_head(parsetree_list);

    Portal portal = NULL;
    DestReceiver *receiver = NULL;
    List *querytree_list = NULL;
    List *plantree_list = NULL;
    Node *parsetree = (Node *)lfirst(parsetree_item);
    const char *commandTag = CreateCommandTag(parsetree);

    querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
    plantree_list = pg_plan_queries(querytree_list, 0, NULL);

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
    self->mxct = CurrentMemoryContext;

    return (DestReceiver *)self;
}

void receive(TupleTableSlot *slot, DestReceiver *self)
{
    StmtResult *result = (StmtResult *)self;
    TupleDesc typeinfo = slot->tts_tupleDescriptor;
    int natts = typeinfo->natts;
    int i;
    Datum origattr, attr;
    char *value = NULL;
    bool isnull = false;
    Oid typoutput;
    bool typisvarlena = false;
    List *values = NIL;

    MemoryContext oldcontext = MemoryContextSwitchTo(result->mxct);

    for (i = 0; i < natts; ++i) {
        origattr = tableam_tslot_getattr(slot, i + 1, &isnull);
        if (isnull) {
            continue;
        }
        getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &typoutput, &typisvarlena);

        if (typisvarlena) {
            attr = PointerGetDatum(PG_DETOAST_DATUM(origattr));
        } else {
            attr = origattr;
        }

        value = OidOutputFunctionCall(typoutput, attr);
        values = lappend(values, value);

        /* Clean up detoasted copy, if any */
        if (DatumGetPointer(attr) != DatumGetPointer(origattr)) {
            pfree(DatumGetPointer(attr));
        }
    }
    result->tuples = lappend(result->tuples, values);

    MemoryContextSwitchTo(oldcontext);
}

void startup(DestReceiver *self, int operation, TupleDesc typeinfo) {}

void shutdown(DestReceiver *self) {}

void destroy(DestReceiver *self)
{
    StmtResult *result = (StmtResult *)self;
    ListCell *item = NULL;
    List *tuples = result->tuples;

    foreach (item, tuples) {
        list_free_deep((List *)lfirst(item));
    }
    list_free(tuples);
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
void find_select_stmt(Node *parsetree)
{
    if (parsetree == NULL || nodeTag(parsetree) != T_SelectStmt) {
        return;
    }
    SelectStmt *stmt = (SelectStmt *)parsetree;
    g_stmt_list = lappend(g_stmt_list, stmt);

    switch (stmt->op) {
        case SETOP_UNION: {
            // analyze the set operation: union
            find_select_stmt((Node *)stmt->larg);
            find_select_stmt((Node *)stmt->rarg);
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
                    find_select_stmt(cte->ctequery);
                }
                break;
            }

            // analyze the 'from' clause
            if (stmt->fromClause) {
                extract_stmt_from_clause(stmt->fromClause);
            }

            // analyze the 'where' clause
            if (stmt->whereClause) {
                extract_stmt_where_clause(stmt->whereClause);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void extract_stmt_from_clause(List *from_list)
{
    ListCell *item = NULL;

    foreach (item, from_list) {
        Node *from = (Node *)lfirst(item);
        if (from && IsA(from, RangeSubselect)) {
            find_select_stmt(((RangeSubselect *)from)->subquery);
        }
        if (from && IsA(from, JoinExpr)) {
            Node *larg = ((JoinExpr *)from)->larg;
            Node *rarg = ((JoinExpr *)from)->rarg;
            if (larg && IsA(larg, RangeSubselect)) {
                find_select_stmt(((RangeSubselect *)larg)->subquery);
            }
            if (rarg && IsA(rarg, RangeSubselect)) {
                find_select_stmt(((RangeSubselect *)rarg)->subquery);
            }                                
        }
    }
}

void extract_stmt_where_clause(Node *item_where)
{
    if (IsA(item_where, SubLink)) {
        find_select_stmt(((SubLink *)item_where)->subselect);
    }
    while (item_where && IsA(item_where, A_Expr)) {
        A_Expr *expr = (A_Expr *)item_where;
        Node *lexpr = expr->lexpr;
        Node *rexpr = expr->rexpr;
        if (lexpr && IsA(lexpr, SubLink)) {
            find_select_stmt(((SubLink *)lexpr)->subselect);
        }
        if (rexpr && IsA(rexpr, SubLink)) {
            find_select_stmt(((SubLink *)rexpr)->subselect);
        }
        item_where = lexpr;
    }
}

/*
 * generate_final_index
 *     Concatenate the candidate indexes of the table into a string of
 *     composite index.
 *
 * The composite index is generated only when it is not the same as the
 * previously suggested indexes or existing indexes.
 */
void generate_final_index(TableCell *table, Oid table_oid)
{
    char *suggested_index = (char *)palloc0(NAMEDATALEN);
    ListCell *index = NULL;
    errno_t rc = EOK;
    int i = 0;

    // concatenate the candidate indexes into a string
    foreach (index, table->index) {
        char *index_name = ((IndexCell *)lfirst(index))->index_name;
        if (strlen(index_name) + strlen(suggested_index) + 1 > NAMEDATALEN) {
            break;
        }
        if (i == 0) {
            rc = strcpy_s(suggested_index, NAMEDATALEN, index_name);
        } else {
            rc = strcat_s(suggested_index, NAMEDATALEN, ",");
            securec_check(rc, "\0", "\0");
            rc = strcat_s(suggested_index, NAMEDATALEN, index_name);
        }
        securec_check(rc, "\0", "\0");
        i++;
    }
    list_free_deep(table->index);
    table->index = NIL;

    // check the previously suggested indexes
    ListCell *prev_index = NULL;
    foreach (prev_index, table->index_print) {
        if (strcasecmp((char *)lfirst(prev_index), suggested_index) == 0) {
            pfree(suggested_index);
            return;
        }
    }

    // check the existed indexes
    char *existed_index = (char *)palloc0(NAMEDATALEN);
    List *indexes = get_table_indexes(table_oid);
    index = NULL;

    foreach (index, indexes) {
        List *attnames = get_index_attname(lfirst_oid(index));
        ListCell *item = NULL;
        i = 0;

        foreach (item, attnames) {
            if (i == 0) {
                rc = strcpy_s(existed_index, NAMEDATALEN, (char *)lfirst(item));
            } else {
                rc = strcat_s(existed_index, NAMEDATALEN, ",");
                securec_check(rc, "\0", "\0");
                rc = strcat_s(existed_index, NAMEDATALEN, (char *)lfirst(item));
            }
            securec_check(rc, "\0", "\0");
            i++;
        }
        // the suggested index has existed
        if (strcasecmp(existed_index, suggested_index) == 0) {
            pfree(suggested_index);
            pfree(existed_index);
            return;
        }
    }
    table->index_print = lappend(table->index_print, suggested_index);
    pfree(existed_index);

    return;
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
                (void)find_or_create_tblcell(table->relname, table->alias->aliasname);
            } else {
                (void)find_or_create_tblcell(table->relname, NULL);
            }
        } else if (nodeTag(item) == T_JoinExpr) {
            // multi-table join
            parse_join_tree((JoinExpr *)item);
        } else {
        }
    }
}

void add_drived_tables(RangeVar *join_node)
{
    TableCell *join_table = NULL;

    if (join_node->alias) {
        join_table = find_or_create_tblcell(join_node->relname, join_node->alias->aliasname);
    } else {
        join_table = find_or_create_tblcell(join_node->relname, NULL);
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
    }

    if (nodeTag(rarg) == T_JoinExpr) {
        parse_join_tree((JoinExpr *)rarg);
    } else if (nodeTag(rarg) == T_RangeVar) {
        add_drived_tables((RangeVar *)rarg);
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
    JoinCell *join_cond = NULL;
    join_cond = (JoinCell *)palloc0(sizeof(*join_cond));
    join_cond->field = begin_field;
    join_cond->table = end_table->table_name;
    join_cond->table_field = end_field;
    begin_table->join_cond = lappend(begin_table->join_cond, join_cond);
}

// parse 'join on'
void parse_join_expr(Node *item_join)
{
    if (!item_join) {
        return;
    }

    A_Expr *join_cond = (A_Expr *)item_join;
    if (!join_cond->lexpr || !join_cond->rexpr) {
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
        char *l_table_name = find_table_name(l_join_fields);
        char *r_table_name = find_table_name(r_join_fields);
        char *l_field_name = find_field_name(l_join_fields);
        char *r_field_name = find_field_name(r_join_fields);

        if (!l_table_name || !r_table_name || !l_field_name || !r_field_name) {
            return;
        }

        TableCell *ltable = find_or_create_tblcell(l_table_name, NULL);
        TableCell *rtable = find_or_create_tblcell(r_table_name, NULL);

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

// parse 'join using'
void parse_join_expr(JoinExpr *join_tree)
{
    if (!join_tree) {
        return;
    }

    if (nodeTag(join_tree->larg) != T_RangeVar ||
        nodeTag(join_tree->rarg) != T_RangeVar) {
        return;
    }

    List *join_fields = join_tree->usingClause;
    char *l_table_name = ((RangeVar *)(join_tree->larg))->relname;
    char *r_table_name = ((RangeVar *)(join_tree->rarg))->relname;
    TableCell *ltable = find_or_create_tblcell(l_table_name, NULL);
    TableCell *rtable = find_or_create_tblcell(r_table_name, NULL);

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
void field_value_trans(_out_ char *target, A_Const *field_value)
{
    Value value = field_value->val;

    if (value.type == T_Integer) {
        pg_itoa(value.val.ival, target);
    } else if (value.type == T_String) {
        errno_t rc = sprintf_s(target, MAX_QUERY_LEN, "'%s'", value.val.str);
        securec_check_ss_c(rc, "\0", "\0");
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
    errno_t rc = EOK;
    char *table_name = find_table_name(field);
    char *index_name = find_field_name(field);
    if (!table_name || !index_name) {
        return;
    }

    char *op_type = strVal(linitial(op));
    char *field_expr = (char *)palloc0(MAX_QUERY_LEN);
    char *field_value = (char *)palloc0(MAX_QUERY_LEN);
    ListCell *item = NULL;
    int i = 0;

    // get field values
    foreach (item, lfield_values) {
        char *str = (char *)palloc0(MAX_QUERY_LEN);
        if (!IsA(lfirst(item), A_Const)) {
            continue;
        }
        field_value_trans(str, (A_Const *)lfirst(item));
        if (i == 0) {
            rc = strcpy_s(field_value, MAX_QUERY_LEN, str);
        } else {
            rc = strcat_s(field_value, MAX_QUERY_LEN, ",");
            securec_check(rc, "\0", "\0");
            rc = strcat_s(field_value, MAX_QUERY_LEN, str);
        }
        securec_check(rc, "\0", "\0");
        i++;
        pfree(str);
    }

    if (i == 0) {
        pfree(field_value);
        pfree(field_expr);
        return;
    }

    // get field expression, e.g., 'id = 100'
    if (strcasecmp(op_type, "~~") == 0) {
        // ...like...
        if (field_value != NULL && field_value[1] == '%') {
            pfree(field_value);
            pfree(field_expr);
            return;
        } else {
            rc = sprintf_s(field_expr, MAX_QUERY_LEN, "%s like %s", index_name, field_value);
        }
    } else if (lfield_values->length > 1) {
        // ...(not)in...
        if (strcasecmp(op_type, "=") == 0) {
            rc = sprintf_s(field_expr, MAX_QUERY_LEN, "%s in (%s)", index_name, field_value);
        } else {
            rc = sprintf_s(field_expr, MAX_QUERY_LEN, "%s not in (%s)", index_name, field_value);
        }
    } else {
        // ...>=<...
        rc = sprintf_s(field_expr, MAX_QUERY_LEN, "%s %s %s", index_name, op_type, field_value);
    }
    securec_check_ss_c(rc, "\0", "\0");
    pfree(field_value);

    uint4 cardinality = calculate_field_cardinality(table_name, field_expr);
    if (cardinality > CARDINALITY_THRESHOLD) {
        IndexCell *index = (IndexCell *)palloc0(sizeof(*index));
        index->index_name = index_name;
        index->cardinality = cardinality;
        index->op = op_type;
        index->field_expr = field_expr;
        add_index_from_field(table_name, index);
    }
}

inline uint4 tuple_to_uint(List *tuples)
{
    return pg_atoi((char *)linitial((List *)linitial(tuples)), sizeof(uint4), 0);
}

uint4 get_table_count(const char *table_name)
{
    uint4 table_count = 0;
    char query_string[MAX_QUERY_LEN] = {0x00};
    errno_t rc =
        sprintf_s(query_string, MAX_QUERY_LEN, "select reltuples from pg_class where relname='%s';", table_name);
    securec_check_ss_c(rc, "\0", "\0");

    StmtResult *result = execute_stmt(query_string, true);
    table_count = tuple_to_uint(result->tuples);
    (*result->pub.rDestroy)((DestReceiver *)result);

    return table_count;
}

uint4 calculate_field_cardinality(const char *table_name, const char *field_expr)
{
    const int sample_factor = 2;

    uint4 cardinality;
    uint4 table_count = get_table_count(table_name);
    uint4 sample_rows = (table_count / sample_factor) > MAX_SAMPLE_ROWS ? MAX_SAMPLE_ROWS : (table_count / sample_factor);
    char query_string[MAX_QUERY_LEN] = {0x00};
    errno_t rc = sprintf_s(query_string, MAX_QUERY_LEN, "select count(*) from ( select * from %s limit %d) where %s",
        table_name, sample_rows, field_expr);
    securec_check_ss_c(rc, "\0", "\0");

    StmtResult *result = execute_stmt(query_string, true);
    uint4 row = tuple_to_uint(result->tuples);
    (*result->pub.rDestroy)((DestReceiver *)result);

    cardinality = row == 0 ? sample_rows : (sample_rows / row);

    return cardinality;
}

char *find_field_name(List *fields)
{
    if (!fields) {
        return NULL;
    }

    char *field_name = NULL;
    if (fields->length > 1) {
        field_name = strVal(lsecond(fields));
    } else {
        field_name = strVal(linitial(fields));
    }

    return field_name;
}

char *find_table_name(List *fields)
{
    if (!fields) {
        return NULL;
    }

    char *table = NULL;
    ListCell *item = NULL;
    ListCell *sub_item = NULL;

    // if fields have table name
    if (fields->length > 1) {
        table = strVal(linitial(fields));
        // check existed tables
        foreach (item, g_table_list) {
            TableCell *cur_table = (TableCell *)lfirst(item);
            if (strcasecmp(table, cur_table->table_name) == 0) {
                return cur_table->table_name;
            }
            foreach (sub_item, cur_table->alias_name) {
                char *cur_alias_name = (char *)lfirst(sub_item);
                if (strcasecmp(table, cur_alias_name) == 0) {
                    return cur_table->table_name;
                }            
            }
        }
        return NULL;
    }

    // if fields only have field name
    int cnt = 0;
    item = NULL;
    char query_string[MAX_QUERY_LEN] = {0x00};
    char *column_name = strVal(linitial(fields));

    // check which table has the given column_name
    foreach (item, g_table_list) {
        char *table_name = ((TableCell *)lfirst(item))->table_name;
        errno_t rc = sprintf_s(query_string, MAX_QUERY_LEN,
            "select count(*) from information_schema.columns where table_name = '%s' and column_name = '%s'",
            table_name, column_name);
        securec_check_ss_c(rc, "\0", "\0");

        StmtResult *result = execute_stmt(query_string, true);
        if (result) {
            uint4 num = tuple_to_uint(result->tuples);
            if (num > 0) {
                cnt++;
                table = table_name;
            }
            (*result->pub.rDestroy)((DestReceiver *)result);
        }
    }
    if (cnt == 1) {
        return table;
    }

    return NULL;
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
    if (RelationIsRelation(relation) &&
        RelationGetPartType(relation) == PARTTYPE_NON_PARTITIONED_RELATION &&
        RelationGetRelPersistence(relation) == RELPERSISTENCE_PERMANENT) {
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
 *     variable 'g_table_list', and if not found, create a TableCell and return it.
 */
TableCell *find_or_create_tblcell(char *table_name, char *alias_name)
{
    if (!table_name) {
        return NULL;
    }
    if (is_tmp_table(table_name)) {
        ereport(WARNING, (errmsg("can not advise for: %s.", table_name)));
        return NULL;
    }

    // seach the table among existed tables
    ListCell *item = NULL;
    ListCell *sub_item = NULL;

    if (g_table_list != NIL) {
        foreach (item, g_table_list) {
            TableCell *cur_table = (TableCell *)lfirst(item);
            char *cur_table_name = cur_table->table_name;
            if (strcasecmp(cur_table_name, table_name) == 0) {
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
                if (strcasecmp(cur_alias_name, table_name) == 0) {
                    return cur_table;
                }            
            }
        }
    }

    RangeVar* rtable = makeRangeVar(NULL, table_name, -1);
    Oid table_oid = RangeVarGetRelid(rtable, NoLock, true);
    if (check_relation_type_valid(table_oid) == false) {
        ereport(WARNING, (errmsg("can not advise for: %s.", table_name)));
        return NULL;
    }

    // create a new table
    TableCell *new_table = NULL;
    new_table = (TableCell *)palloc0(sizeof(*new_table));
    new_table->table_name = table_name;
    new_table->alias_name = NIL;
    if (alias_name) {
        new_table->alias_name = lappend(new_table->alias_name, alias_name);
    }    
    new_table->index = NIL;
    new_table->join_cond = NIL;
    new_table->index_print = NIL;
    g_table_list = lappend(g_table_list, new_table);

    return new_table;
}

/*
 * add_index_from_field
 *     Add index from field for the table.
 *
 * The index sorting rule is as follows: the index with operator'=' is ranked first,
 * and the index with higher cardinality is ranked first.
 */
void add_index_from_field(char *table_name, IndexCell *index)
{
    TableCell *table = find_or_create_tblcell(table_name, NULL);
    ListCell *table_index = NULL;

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

// The table that has smallest result set is set as the driver table.
void determine_driver_table()
{
    if (g_table_list->length == 1) {
        g_driver_table = (TableCell *)linitial(g_table_list);
    } else {
        if (g_drived_tables != NIL) {
            uint4 small_result_set = UINT32_MAX;
            ListCell *item = NULL;

            foreach (item, g_drived_tables) {
                TableCell *table = (TableCell *)lfirst(item);
                uint4 result_set;
                if (table->index) {
                    result_set = get_join_table_result_set(table->table_name,
                        ((IndexCell *)linitial(table->index))->field_expr);
                } else {
                    result_set = get_join_table_result_set(table->table_name, NULL);
                }
                if (result_set < small_result_set) {
                    g_driver_table = table;
                    small_result_set = result_set;
                }
            }
        }
    }
    list_free(g_drived_tables);
    g_drived_tables = NIL;
}

uint4 get_join_table_result_set(const char *table_name, const char *condition)
{
    char query_string[MAX_QUERY_LEN] = {0x00};
    errno_t rc = EOK;
    if (condition == NULL) {
        rc = sprintf_s(query_string, MAX_QUERY_LEN, "select * from %s;", table_name);
    } else {
        rc = sprintf_s(query_string, MAX_QUERY_LEN, "select * from %s where %s;", table_name, condition);
    }
    securec_check_ss_c(rc, "\0", "\0");

    /* get query execution plan */
    List *parsetree_list = pg_parse_query(query_string, NULL);
    ListCell *parsetree_item = list_head(parsetree_list);
    Node *parsetree = (Node *)lfirst(parsetree_item);
    List *querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0);
    List *plantree_list = pg_plan_queries(querytree_list, 0, NULL);
    PlannedStmt *plan_stmt = (PlannedStmt *)lfirst(list_head(plantree_list));

    Scan *scan = (Scan *)(plan_stmt->planTree);
    return (uint4)scan->plan.plan_rows;
}

void add_index_from_join(TableCell *table, char *index_name)
{
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
            JoinCell *join_cond = (JoinCell *)lfirst(item);
            TableCell *to_be_joined_table = find_or_create_tblcell(join_cond->table, NULL);
            if (list_member(joined_tables, to_be_joined_table)) {
                continue;
            }
            if (join_table != g_driver_table) {
                add_index_from_join(join_table, join_cond->field);
            }
            add_index_from_join(to_be_joined_table, join_cond->table_field);

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

char *parse_group_clause(List *group_clause, List *target_list)
{
    if (group_clause == NULL)
        return NULL;

    bool is_only_one_table = true;
    ListCell *group_item = NULL;
    char *pre_table = NULL;

    foreach (group_item, group_clause) {
        Node *node = (Node *)lfirst(group_item);

        if (nodeTag(node) == T_A_Const) {
            node = transform_group_order_node(node, target_list);
        }
        if (nodeTag(node) != T_ColumnRef)
            break;
        List *fields = ((ColumnRef *)node)->fields;
        char *table_group = find_table_name(fields);
        if (!table_group) {
            return NULL;
        }
        if (pre_table != NULL && strcasecmp(table_group, pre_table) != 0) {
            is_only_one_table = false;
            break;
        }
        pre_table = table_group;
    }

    if (is_only_one_table && pre_table) {
        if (g_table_list->length == 1 || (g_driver_table && strcasecmp(pre_table, g_driver_table->table_name) == 0)) {
            return pre_table;
        }
    }

    return NULL;
}

char *parse_order_clause(List *order_clause, List *target_list)
{
    if (order_clause == NULL)
        return NULL;

    bool is_only_one_table = true;
    ListCell *order_item = NULL;
    char *pre_table = NULL;
    SortByDir pre_dir;

    foreach (order_item, order_clause) {
        Node *node = ((SortBy *)lfirst(order_item))->node;
        SortByDir dir = ((SortBy *)(lfirst(order_item)))->sortby_dir;

        if (nodeTag(node) == T_A_Const) {
            node = transform_group_order_node(node, target_list);
        }
        if (nodeTag(node) != T_ColumnRef)
            break;
        List *fields = ((ColumnRef *)node)->fields;
        char *table_order = find_table_name(fields);
        if (!table_order) {
            return NULL;
        }
        if (pre_table != NULL && (strcasecmp(table_order, pre_table) != 0 || dir != pre_dir)) {
            is_only_one_table = false;
            break;
        }
        pre_table = table_order;
        pre_dir = dir;
    }

    if (is_only_one_table && pre_table) {
        if (g_table_list->length == 1 || (g_driver_table && strcasecmp(pre_table, g_driver_table->table_name) == 0)) {
            return pre_table;
        }
    }

    return NULL;
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

    foreach (item, clause) {
        Node *node = NULL;
        List *fields = NULL;
        char *index_name = NULL;

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
        index_name = find_field_name(fields);
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
}
