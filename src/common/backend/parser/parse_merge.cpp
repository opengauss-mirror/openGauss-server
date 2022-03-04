/* -------------------------------------------------------------------------
 *
 * parse_merge.cpp
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_merge.cpp
 *
 * -------------------------------------------------------------------------
 */


#ifdef FRONTEND_PARSER
#include "nodes/parsenodes_common.h"
#include "nodes/feparser_memutils.h"
#else
#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"

#include "pgstat.h"

#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parsetree.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_cte.h"
#include "parser/parse_merge.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

static void transformMergeJoinClause(ParseState* pstate, Node* merge);
static List* expandSourceTL(ParseState* pstate, RangeTblEntry* rte, int rtindex);
static List* transformUpdateTargetList(ParseState* pstate, List* origTlist);
static void checkUpdateOnJoinKey(
    ParseState* pstate, MergeWhenClause* clause, List* join_var_list, bool is_insert_update);
static void checkUpdateOnDistributeKey(RangeTblEntry* rte, List* targetlist);
static void check_source_table_replicated(Node* source_relation);
static void check_target_table_columns(ParseState* pstate, bool is_insert_update);
static bool checkTargetTableReplicated(RangeTblEntry* rte);
static void check_system_column_varlist(List* varlist, bool is_insert_update);
static void check_system_column_node(Node* node, bool is_insert_update);
static void check_system_column_reference(List* joinVarList, List* mergeActionList, bool is_insert_update);
static void checkTargetTableSystemCatalog(Relation targetRel);
static void check_insert_action_targetlist(List* merge_action_list, List* source_targetlist);
static bool check_update_action_targetlist(List* update_action_list, List* source_targetlist);
bool check_unique_constraint(List*& index_list);
static bool find_valid_unique_constraint(Relation relation, List* colnames, List*& index_list);
static bool var_in_list(Var* var, List* list);
static Bitmapset* get_relation_attno_bitmap_by_names(Relation relation, List* colnames);
static Bitmapset* get_relation_default_attno_bitmap(Relation relation);
static MergeWhenClause* get_merge_when_clause(List* mergeWhenClauses, CmdType cmd_type, bool matched);
static Node* append_expr(Node* expr1, Node* expr2, A_Expr_Kind kind);
static int count_target_columns(Node* query);

extern char* nodeTagToString(NodeTag type);

/*
 *	Special handling for MERGE statement is required because we assemble
 *	the query manually. This is similar to setTargetTable() followed
 * 	by transformFromClause() but with a few less steps.
 *
 *	Process the FROM clause and add items to the query's range table,
 *	joinlist, and namespace.
 *
 *	A special targetlist comprising of the columns from the right-subtree of
 *	the join is populated and returned. Note that when the JoinExpr is
 *	setup by transformMergeStmt, the left subtree has the target result
 *	relation and the right subtree has the source relation.
 *
 *	Returns the rangetable index of the target relation.
 */
static void transformMergeJoinClause(ParseState* pstate, Node* merge)
{
    RangeTblEntry* rte = NULL;
    RangeTblEntry* rt_rte = NULL;
    List* relnamespace = NIL;
    int rtindex = 0;
    int rt_rtindex = 0;
    Node* n = NULL;
    Relids containedRels = NULL;
    Assert(IsA(merge, JoinExpr));

    n = transformFromClauseItem(
        pstate, merge, &rte, &rtindex, &rt_rte, &rt_rtindex, &relnamespace, true, false, true);
    bms_free(containedRels);

    pstate->p_joinlist = list_make1(n);

    /*
     * We created an internal join between the target and the source relation
     * to carry out the MERGE actions. Normally such an unaliased join hides
     * the joining relations, unless the column references are qualified.
     * Also, any unqualified column references are resolved to the Join RTE, if
     * there is a matching entry in the targetlist. But the way MERGE
     * execution is later setup, we expect all column references to resolve to
     * either the source or the target relation. Hence we must not add the
     * Join RTE to the namespace.
     *
     * The last entry must be for the top-level Join RTE. We don't want to
     * resolve any references to the Join RTE. So discard that.
     *
     * We also do not want to resolve any references from the leftside of the
     * Join since that corresponds to the target relation. References to the
     * columns of the target relation must be resolved from the result
     * relation and not the one that is used in the join. So the
     * mergeTarget_relation is marked invisible to both qualified as well as
     * unqualified references.
     */
    Assert(list_length(relnamespace) > 1);

    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    foreach (cell1, relnamespace) {
        ParseNamespaceItem* nitem1 = (ParseNamespaceItem*)lfirst(cell1);
        RangeTblEntry* entry1 = nitem1->p_rte;

        foreach (cell2, pstate->p_relnamespace) {
            ParseNamespaceItem* nitem2 = (ParseNamespaceItem*)lfirst(cell2);
            RangeTblEntry* entry2 = nitem2->p_rte;

            if (entry1->relid == entry2->relid) {
                continue;
            }
            pstate->p_relnamespace = lappend(pstate->p_relnamespace, lfirst(cell1));
        }
    }
}

/* @Description: find valid unique constraint of a relation
 * @in relation: target relation
 * @in colnames: insert target column names
 * @out index_list: valid unique constraint index list
 * @out: true if there is any valid unique constraint index.
 */
static bool find_valid_unique_constraint(Relation relation, List* colnames, List*& index_list)
{
    /* find the all the indexes of the target relation */
    index_list = RelationGetIndexInfoList(relation);
    /*
     * check if the relation has any primary or unique index,
     * if there are no unique constraint indexes, directly return.
     */
    if (!check_unique_constraint(index_list)) {
        return false;
    }
    /*
     * The relation do have primary or unique key constraints,
     * The index(es) is(are) considered valid
     * only when all the columns referenced by this index are in the INSERT's target columns,
     * or has default values.
     * Those index will be put in the join condition later.
     * The steps to find all the valid unique constraint index are listed below:
     * 1. assign a column search range,
     *    which includes all the columns appointed in the INSERT table(columns...) statement
     *    represented by the input parameter 'colnames',
     *    plus all the columns with default value constraints in the relation.
     * 2. find the columns referenced by each primary or unique index.
     * 3. remove the indexes which referenced columns are not within the column search range.
     * Then the remaining index list are the valid unique constraint index list.
     */
    Bitmapset* source_attrs = NULL; /* attr bitmap according to the colnames */
    Bitmapset* search_attrs = NULL; /* attr bitmap in search range */
    IndexInfo* index_info = NULL;
    int attrno = 0;
    int len = 0;
    int i = 0;
    int j = 0;

    /* find relation's column attribute number according to the colnames */
    source_attrs = get_relation_attno_bitmap_by_names(relation, colnames);

    /* add relation's columns which have default values into the search range */
    if (RelationGetNumberOfAttributes(relation) > list_length(colnames)) {
        search_attrs = get_relation_default_attno_bitmap(relation);
    }
    /* add all referenced columns into the search range */
    search_attrs = bms_union(source_attrs, search_attrs);

    /* delete those index whose referenced columns are not within the search range */
    len = list_length(index_list);
    for (i = len - 1; i >= 0; i--) {
        /* attr bitmap per index */
        Bitmapset* constraint_index_attrs = NULL;
        index_info = (IndexInfo*)list_nth(index_list, i);

        /* Collect simple attribute references */
        for (j = 0; j < index_info->ii_NumIndexAttrs; j++) {
            attrno = index_info->ii_KeyAttrNumbers[j];

            if (attrno != 0) {
                constraint_index_attrs = bms_add_member(constraint_index_attrs, attrno);
            }
        }

        /*
         * If the column search range dose not contain the primary or unique index columns,
         * delete the index from the index list, since the index should not be considered in join condition.
         */
        if (!bms_is_subset(constraint_index_attrs, search_attrs)) {
            index_list = RemoveListCell(index_list, i + 1);
        }
    }

    /*
     * Although the relation has primary or unique index,
     * the referenced columns of the primary or unique index are not included in the search range,
     * so there are no valid unique constraint index.
     */
    if (list_length(index_list) > 0) {
        return true;
    }
    return false;
}

static void check_merge_when_clause(MergeWhenClause* mergeWhenClause)
{
    if (unlikely(mergeWhenClause == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmodule(MOD_PARSER),
                errmsg("Cannot find expected NOT MATCHED WHEN clause with operator INSERT.")));
    }
}

static void add_column_values_to_subquery(RangeSubselect* range_subselect, Node* equal_rexpr)
{
    SelectStmt* select_stmt = (SelectStmt*)range_subselect->subquery;
    if (select_stmt->op == SETOP_NONE) {
        if (select_stmt->valuesLists != NIL) {
            for (int j = 0; j < list_length(select_stmt->valuesLists); j++) {
                lappend((List*)list_nth(select_stmt->valuesLists, j), equal_rexpr);
            }
        } else if (select_stmt->targetList != NIL) {
            ResTarget* rt = makeNode(ResTarget);
            rt->val = equal_rexpr;
            lappend(select_stmt->targetList, (Node*)rt);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_PARSER),
                    errmsg("Subquery should at least have a target list or values list.")));
        }
    } else {
        SelectStmt* set_stmt = makeNode(SelectStmt);
        set_stmt->op = SETOP_NONE;

        ResTarget* rt_star = makeNode(ResTarget);
        ColumnRef* cr_star = makeNode(ColumnRef);
        cr_star->fields = list_make1((Node*)makeNode(A_Star));
        rt_star->val = (Node*)cr_star;

        ResTarget* rt_expr = makeNode(ResTarget);
        rt_expr->val = equal_rexpr;

        set_stmt->targetList = list_make2((Node*)rt_star, (Node*)rt_expr);

        RangeSubselect* subselect = makeNode(RangeSubselect);
        subselect->subquery = (Node*)select_stmt;
        Alias* alias = makeAlias("__unnamed_subquery__", NIL);
        subselect->alias = alias;
        set_stmt->fromClause = list_make1((Node*)subselect);

        range_subselect->subquery = (Node*)set_stmt;
    }
}

static Node* build_equal_expr(
    ParseState* pstate, MergeStmt* stmt, IndexInfo* index_info, int index, Bitmapset** source_attrs)
{
    Node* equal_expr = NULL;
    Node* equal_lexpr = NULL;
    Node* equal_rexpr = NULL;
    Relation target_relation = pstate->p_target_relation;
    RangeSubselect* range_subselect = (RangeSubselect*)stmt->source_relation;
    char* target_aliasname = stmt->relation->alias->aliasname;
    int attrno = index_info->ii_KeyAttrNumbers[index];
    char* attname = pstrdup(NameStr(target_relation->rd_att->attrs[attrno - 1]->attname));
    char* source_aliasname = range_subselect->alias->aliasname;

    /* build the left expr of the equal expr, which comes from the target relation's index */
    equal_lexpr = (Node*)makeColumnRef(target_aliasname, attname, -1);

    /* build the right expr of the equal expr, which comes from the source relation */
    if (bms_is_member(attrno, *source_attrs)) {
        equal_rexpr = (Node*)makeColumnRef(source_aliasname, attname, -1);
    } else {
        equal_rexpr = build_column_default(target_relation, attrno, false);
        if (unlikely(equal_rexpr == NULL)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_PARSER),
                    errmsg("Default column should have default values.")));
        }
        if (contain_volatile_functions(equal_rexpr)) {
            /* If the default expr function is a volatile function, such as the scenario below:
             * considering a t1(a int, b int default 1, c intserial), which has a unique index on (a, c)
             * then do:
             *     INSERT INTO t1 VALUES(4), (4) ON DUPLICATE KEY UPDATE b = c;
             * then the rewrite should be:
             *     MERGE INTO t1 AS t1 USING
             *      (VALUES(4, nextval('t1_c_seq')), (4, nextval('t1_c_seq'))) AS __unnamed_subquery_source__(a, c)
             *     ON t1.a = __unnamed_subquery_source__.a AND t1.c = __unnamed_subquery_source__.c
             *     WHEN NOT MATCHED THEN
             *       INSERT (a, c) VALUES (__unnamed_subquery_source__.a, __unnamed_subquery_source__.c)
             *     WHEN MATCHED THEN
             *       UPDATE SET b = c;
             */
            /* 1. add the column alias referring this default column into the source relation  */
            range_subselect->alias->colnames = lappend(range_subselect->alias->colnames, (Node*)(makeString(attname)));

            /* 2. add the column value into the subquery's valueList or targetList  */
            add_column_values_to_subquery(range_subselect, equal_rexpr);

            /* 3. fix the WHEN NOT MATCHED THEN clause to insert the default column,
             * make sure the volatile expr should be only execute once
             * by using the column ref instead of expression as the values.
             */
            MergeWhenClause* mergeWhenClause = get_merge_when_clause(stmt->mergeWhenClauses, CMD_INSERT, false);
            check_merge_when_clause(mergeWhenClause);

            /* add the ResTarget referring to this default column into INSERT's target columns */
            ResTarget* rt = makeNode(ResTarget);
            rt->name = attname;
            mergeWhenClause->cols = lappend(mergeWhenClause->cols, (Node*)rt);

            /* add value referring to this default column into INSERT's values,
             * note that we should never use the default expression as the values
             * in order to avoid unexpected multi-execution of the volatile function
             */
            mergeWhenClause->values =
                lappend(mergeWhenClause->values, (Node*)makeColumnRef(source_aliasname, attname, -1));

            /*
             * 4. Fix the equal expr,
             * we should never use default expression as the join expr
             * in order to avoid unexpected multi-execution of the volatile function.
             */
            equal_rexpr = (Node*)makeColumnRef(source_aliasname, attname, -1);

            /*
             * 5. add the attrno number into the search range,
             * in case that the column is refered by any other constraint index later.
             */
            *source_attrs = bms_add_member(*source_attrs, attrno);
        }
    }

    /*
     * build equal expr with left and right expression,
     * such as t1.a = __unnamed_subquery_source__.a
     */
    equal_expr = (Node*)makeSimpleA_Expr(AEXPR_OP, "=", equal_lexpr, equal_rexpr, -1);
    return equal_expr;
}

/*
 * @Description: make up a join expression for INSERT UPDATE statement
 *  First find the valid unique constraint, if there's none, always do insert,
 *  otherwise, use AND and OR expr to make up a join expression.
 *  eg:
 *      CREATE UNIQUE INDEX t1_u1 ON t1(a, b);
 *      CREATE UNIQUE INDEX t1_u2 ON t1(a, c);
 *      INSERT INTO t1 VALUES(1, 2, 3) ON DUPLICATE KEY ...
 *  then:
 *      MERGE INTO t1 USING (VALUES(1, 2, 3)) AS __unnamed_subquery_source__(a, b, c)
 *          ON (t1.a = __unnamed_subquery_source__.a AND t1.b = __unnamed_subquery_source__.b)
 *          OR (t1.a = __unnamed_subquery_source__.a AND t1.c = __unnamed_subquery_source__.c)
 *     .....
 * The primary key or unique index is valid only when all the columns in this index are
 * within the INSERT target columns or with a default value.
 * @in pstate: parse state
 * @in stmt: INSERT UPDATE statement
 */
void fill_join_expr(ParseState* pstate, MergeStmt* stmt)
{
    List* constraint_index_list = NIL;
    Relation target_relation = pstate->p_target_relation;
    RangeSubselect* range_subselect = (RangeSubselect*)stmt->source_relation;

    if (!find_valid_unique_constraint(target_relation, range_subselect->alias->colnames, constraint_index_list)) {
        /*
         * The target relation does not have any index,
         * or the target relation does not have any primary or unique index,
         * or the columns referenced by any primary or unique index
         * is not within the INSERT's target column nor has a default value.
         * So the join condition should always be FALSE in order to do INSERT operation.
         */
        stmt->join_condition = makeBoolAConst(false, -1);
        return;
    }

    /* found valid primary or unique index,
     * use AND expr to connect each equation per column within one index,
     * use OR expr to connect each index
     */
    Node* or_expr = NULL;
    int i = 0;
    ListCell* cell = NULL;
    IndexInfo* index_info = NULL;
    Bitmapset* source_attrs = get_relation_attno_bitmap_by_names(target_relation, range_subselect->alias->colnames);

    foreach (cell, constraint_index_list) {
        Node* and_expr = NULL;
        index_info = (IndexInfo*)lfirst(cell);
        if (index_info->ii_Expressions != NIL) {
            /*
             * NOTE: the primary or unique index for distribution table cannot using expression,
             * but the replication tables are allowed.
             * However, the expression constraint is not supported for now.
             */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_PARSER),
                    errmsg("Expression whitin primary key or unique index are not supported yet.")));
        }
        for (i = 0; i < index_info->ii_NumIndexAttrs; i++) {
            Node* equal_expr = build_equal_expr(pstate, stmt, index_info, i, &source_attrs);
            /*
             * append the equal expr to the and expr,
             * which happens when there are more than one column referenced by one index
             * such as we have u_index_1(a, c):
             * ON (t1.a = __unnamed_subquery_source__.a AND t1.c = __unnamed_subquery_source__.c)
             */
            and_expr = append_expr(and_expr, equal_expr, AEXPR_AND);
        }

        /*
         * append the and expr to the or expr,
         * which happens when there are more than one valid index
         * such as we have u_index_1(a, c) and u_index_2(b)
         * ON ((t1.a equals to __unnamed_subquery_source__.a AND t1.c equals to __unnamed_subquery_source__.c)
         *        OR (t1.b equals to  __unnamed_subquery_source__.b)
         */
        or_expr = append_expr(or_expr, and_expr, AEXPR_OR);
    }

    stmt->join_condition = (Node*)copyObject(or_expr);
}

static void check_columns_count(int target_list_length, MergeWhenClause* mergeWhenClause, List* rel_valid_cols)
{
    if (target_list_length == 0 ||
        ((mergeWhenClause->cols == NIL) && (list_length(rel_valid_cols) < target_list_length)) ||
        ((mergeWhenClause->cols != NIL) && (list_length(mergeWhenClause->cols) != target_list_length))) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmodule(MOD_OPT_REWRITE),
                errmsg("INSERT has more expressions than target columns")));
    }
}

static void check_contain_default(
    MergeWhenClause* mergeWhenClause, ParseState* pstate, List* rel_valid_attrnos, RangeSubselect* range_subselect)
{
    if (range_subselect->subquery == NULL) {
        int attrno = 0;
        ListCell* cell = NULL;
        /*
         * Check if there is a subquery,
         * if none, it means we're dealing a "INSERT table_name DEFAULT VALUES ..." statement.
         * Rewrite the statement as "INSERT table_name VALUES (columns_default_value) ...."
         */
        if (unlikely(mergeWhenClause->cols != NIL)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmodule(MOD_PARSER),
                    errmsg("Should not specify INSERT target columns in DEFAULT VALUES.")));
        }

        Node* expr = NULL;
        SelectStmt* select_stmt = NULL;
        List* select_value_list = NIL;
        foreach (cell, rel_valid_attrnos) {
            attrno = lfirst_int(cell);
            expr = build_column_default(pstate->p_target_relation, attrno, false);
            if (expr == NULL) {
                expr = makeNullAConst(-1);
            }
            select_value_list = lappend(select_value_list, expr);
        }
        select_stmt = makeNode(SelectStmt);
        select_stmt->valuesLists = list_make1(select_value_list);
        select_stmt->op = SETOP_NONE;
        range_subselect->subquery = (Node*)select_stmt;
    }
}

/*
 * @Description: make up all the alias related component into MergeStmt for INSERT UPDATE
 * 1. make up a subquery when there's none.
 * 2. replace DEFAULT node in the valuesList with default value expression.
 * 3. give alias for source relation's column using the target's column names.
 * 4. make up NOT MATCHED THEN INSERT columns and values with the made alias
 * @in pstate: ParseState
 * @in stmt: the INSERT UPDATE statement
 */
void fix_relation_alias(ParseState* pstate, MergeStmt* stmt)
{
    int i = 0;
    int attrno = 0;
    ListCell* cell = NULL;
    List* attname_list = NIL;
    char* source_relname_alias = NULL;

    /* find the WHEN NOT MATCHED INSERT clause */
    MergeWhenClause* mergeWhenClause = get_merge_when_clause(stmt->mergeWhenClauses, CMD_INSERT, false);
    check_merge_when_clause(mergeWhenClause);

    /*
     * Find the INSERT's target columns' ref,
     * If columns are omitted in the INSERT statement,
     * find all the valid columns in the target relation (dropped column omitted) into rel_valid_cols.
     * otherwise, check and make sure the appointed INSERT target columns are existed and,
     * not specifed more than once.
     */
    List* rel_valid_attrnos = NIL;
    List* rel_valid_cols = checkInsertTargets(pstate, mergeWhenClause->cols, &rel_valid_attrnos);

    /*
     * Supplement the table and column's alias of the source relation.
     * If the WHEN NOT MATCHED INSERT clause has cols,
     * it means the insert columns are appointed in the "INSERT table_name(columns...) ..." statement,
     * so use the appointed column's name as the alias.
     * If not, it means insert columns are omitted in the INSERT statement,
     * then use the target relations' first n column's names as the alias.
     * n is decided by the length of source relation's target list or value list.
     */
    RangeSubselect* range_subselect = (RangeSubselect*)stmt->source_relation;
    source_relname_alias = range_subselect->alias->aliasname;

    /* Check if there is any DEFAULT in the statement */
    check_contain_default(mergeWhenClause, pstate, rel_valid_attrnos, range_subselect);
    Assert(range_subselect->subquery != NULL);
    int target_list_length = count_target_columns(range_subselect->subquery);

    /* check if the INSERT's columns count equal to the target columns count from the subquery
     * those case are not ALLOWED where t1 has three columns
     * INSERT INTO t1
     *   VALUES(...) or subquery with more than 3 (t1's columns) targets
     * INSERT INTO t1 (col1, col2)
     *   VALUES(...) or subquery with less or more than 2 (insert's target columns) targets.
     */
    check_columns_count(target_list_length, mergeWhenClause, rel_valid_cols);

    /* Now, it is save to truncate valid INSERT's target columns, in order to fix the following case
     * when INSERT's target columns are omitted,
     * form the correct columns' ref according to the number of columns in INSERT's values
     * 1. get the number of columns in the source relation, denote as n.
     * 2. get the first n columns' names of the target relation
     * as the INSERT target column and assign it to the WHEN NOT MATCHED THEN INSERT clause.
     * THIS IS IMPORTANT, because only if we've done this,
     *    it is able to fix following scenarios when we found any default column, which is
     *    a. not appointed in the INSERT's target column in the INSERT statement,
     *    b. and the column's default value is a volatile function
     *    c. and this column is appointed as a member in an primary or unique index.
     *    we have to append that default column ref into the INSERT's target columns and values.
     */
    if (mergeWhenClause->cols == NIL) {
        mergeWhenClause->cols = (List*)copyObject(list_truncate(rel_valid_cols, target_list_length));
    }
    /*
     * Check if there is INSERT table_name (colname, ...) VALUES (DEFAULT, ...) ... statement
     * yes, then rewrite the DEFAULT with its default expression.
     */
    List* select_value_lists = ((SelectStmt*)range_subselect->subquery)->valuesLists;
    if (select_value_lists != NULL) {
        List* select_value_list = NIL;
        Node* expr = NULL;
        foreach (cell, select_value_lists) {
            select_value_list = (List*)lfirst(cell);

            int len = list_length(select_value_list);
            for (i = 0; i < len; i++) {
                if (IsA(list_nth(select_value_list, i), SetToDefault)) {
                    attrno = list_nth_int(rel_valid_attrnos, i);
                    expr = build_column_default(pstate->p_target_relation, attrno, false);
                    if (expr == NULL) {
                        expr = makeNullAConst(-1);
                    }
                    lfirst(list_nth_cell(select_value_list, i)) = (void*)expr;
                }
            }
        }
    }

    /* use the names of the INSERT target columns as the alias of the source relation columns.
     * and meanwhile supplement the column reference as the WHEN NOT MATCHED INSERT clause's value,
     * which is the INSERT's values from the source relation.
     */
    List* value_list = NIL;
    ColumnRef* column_ref = NULL;
    ResTarget* rt = NULL;
    foreach (cell, mergeWhenClause->cols) {
        rt = (ResTarget*)lfirst(cell);
        attname_list = lappend(attname_list, makeString(rt->name));
        column_ref = makeColumnRef(source_relname_alias, rt->name, -1);
        value_list = lappend(value_list, (Node*)column_ref);
    }
    range_subselect->alias->colnames = (List*)copyObject(attname_list);
    mergeWhenClause->values = (List*)copyObject(value_list);
}

/* @Description: transform INSERT UPDATE into MERGE INTO statement.
 * In order to do that,
 * we should give table name and column alias,
 * fill ON expression and fill the WHEN MATCHED and WHEN NOT MATCHED clause.
 * considering a t1(a int, b int default 1, c intserial), which has a unique index on (a)
 * and do:
 *     INSERT INTO t1 VALUES(4), (4) ON DUPLICATE KEY UPDATE b = c;
 * then the rewrite should be as follows:
 *     MERGE INTO t1 AS t1 USING
 *       (VALUES(4) AS __unnamed_subquery_source__(a))
 *     ON t1.a = __unnamed_subquery_source__.a
 *     WHEN NOT MATCHED THEN
 *       INSERT VALUES (__unnamed_subquery_source__.a))
 *     WHEN MATCHED THEN
 *       UPDATE SET b = c;
 */
void fix_merge_stmt_for_insert_update(ParseState* pstate, MergeStmt* stmt)
{
    fix_relation_alias(pstate, stmt);
    fill_join_expr(pstate, stmt);
}

void setExtraUpdatedCols(RangeTblEntry* target_rte, TupleDesc tupdesc)
{
    /*
     * Record in extraUpdatedCols generated columns referencing updated base
     * columns.
     */
    if (tupdesc->constr &&
        tupdesc->constr->has_generated_stored)
    {
        for (int i = 0; i < tupdesc->constr->num_defval; i++)
        {
            AttrDefault defval = tupdesc->constr->defval[i];
            Node       *expr;
            Bitmapset  *attrs_used = NULL;

            /* skip if not generated column */
            if (!defval.generatedCol)
                continue;

            expr = (Node *)stringToNode(defval.adbin);
            pull_varattnos(expr, 1, &attrs_used);

            if (bms_overlap(target_rte->updatedCols, attrs_used))
                target_rte->extraUpdatedCols = bms_add_member(target_rte->extraUpdatedCols,
                                               defval.adnum - FirstLowInvalidHeapAttributeNumber);
        }
    }
}

/*
 * transformUpdateTargetList -
 *	handle SET clause in UPDATE clause
 */
static List* transformUpdateTargetList(ParseState* pstate, List* origTlist)
{
    List* tlist = NIL;
    RangeTblEntry* target_rte = NULL;
    ListCell* origTargetList = NULL;
    ListCell* tl = NULL;

    tlist = transformTargetList(pstate, origTlist);

    /* Prepare to assign non-conflicting resnos to resjunk attributes */
    if (pstate->p_next_resno <= RelationGetNumberOfAttributes(pstate->p_target_relation)) {
        pstate->p_next_resno = RelationGetNumberOfAttributes(pstate->p_target_relation) + 1;
    }
    /* Prepare non-junk columns for assignment to target table */
    target_rte = pstate->p_target_rangetblentry;
    origTargetList = list_head(origTlist);

    foreach (tl, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);
        ResTarget* origTarget = NULL;
        int attrno;

        if (tle->resjunk) {
            /*
             * Resjunk nodes need no additional processing, but be sure they
             * have resnos that do not match any target columns; else rewriter
             * or planner might get confused.  They don't need a resname
             * either.
             */
            tle->resno = (AttrNumber)pstate->p_next_resno++;
            tle->resname = NULL;
            continue;
        }
        if (origTargetList == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("UPDATE target count mismatch --- internal error")));
        }
        origTarget = (ResTarget*)lfirst(origTargetList);
        Assert(IsA(origTarget, ResTarget));

        attrno = attnameAttNum(pstate->p_target_relation, origTarget->name, true);
        if (attrno == InvalidAttrNumber) {
            if (!origTarget->indirection) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist",
                            origTarget->name,
                            RelationGetRelationName(pstate->p_target_relation)),
                        parser_errposition(pstate, origTarget->location)));
            } else {
                char* resname = pstrdup((char*)(((Value*)lfirst(list_head(origTarget->indirection)))->val.str));
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s.%s\" of relation \"%s\" does not exist",
                            origTarget->name,
                            resname,
                            RelationGetRelationName(pstate->p_target_relation)),
                        parser_errposition(pstate, origTarget->location)));
            }
        }
        updateTargetListEntry(pstate, tle, origTarget->name, attrno, origTarget->indirection, origTarget->location);

        /* Mark the target column as requiring update permissions */
        target_rte->updatedCols = bms_add_member(target_rte->updatedCols, attrno - FirstLowInvalidHeapAttributeNumber);

        origTargetList = lnext(origTargetList);
    }
    if (origTargetList != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("UPDATE target count mismatch --- internal error")));
    }

    setExtraUpdatedCols(pstate->p_target_rangetblentry, pstate->p_target_relation->rd_att);

    return tlist;
}

/*
 * buildJoinExpr -
 *  Create a JOIN between the target and the source relation.
 */
static JoinExpr* buildJoinExpr(MergeStmt* stmt, AclMode targetPerms)
{
    JoinExpr* joinexpr = makeNode(JoinExpr);
    joinexpr->isNatural = false;
    joinexpr->alias = NULL;
    joinexpr->usingClause = NIL;
    joinexpr->quals = stmt->join_condition;
    joinexpr->larg = (Node*)stmt->relation;
    joinexpr->rarg = (Node*)stmt->source_relation;

    /*
     * Simplify the MERGE query as much as possible
     *
     * These seem like things that could go into Optimizer, but they are
     * semantic simplifications rather than optimizations, per se.
     *
     * If there are no INSERT actions we won't be using the non-matching
     * candidate rows for anything, so no need for an outer join. We do still
     * need an inner join for UPDATE and DELETE actions.
     */
    if (targetPerms & ACL_INSERT)
        joinexpr->jointype = JOIN_RIGHT;
    else
        joinexpr->jointype = JOIN_INNER;
    return joinexpr;
}

static void checkUnsupportedCases(ParseState* pstate, MergeStmt* stmt)
{
    /**
     * replicate table is not yet supported for upsert.
     * merge into replicate table will be examined later in
     *      check_plan_mergeinto_replicate
     *      check_entry_mergeinto_replicate
     */
    if (checkTargetTableReplicated(pstate->p_target_rangetblentry)) {
        if (stmt->is_insert_update) {
            check_source_table_replicated(stmt->source_relation);
        }
        check_target_table_columns(pstate, stmt->is_insert_update);
    }

    if (unlikely(pstate->p_target_relation == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("pstate->p_target_relation should not be null")));
    }

    /* system catalog is not yet supported */
    checkTargetTableSystemCatalog(pstate->p_target_relation);

    /* internal relation is not yet supported */
    if (pstate->p_target_relation != NULL &&
        ((unsigned int)RelationGetInternalMask(pstate->p_target_relation) & INTERNAL_MASK_DUPDATE))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("internal relation doesn't allow %s",
                    stmt->is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO")));

    /* Check unsupported relation kind */
    if (!(RelationIsCUFormat(pstate->p_target_relation) || RelationIsRowFormat(pstate->p_target_relation)))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Target relation type is not supported for %s",
                    stmt->is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO")));
}

static Query* tryTransformMergeInsertStmt(ParseState* pstate, MergeStmt* stmt)
{
    Query* insert_query = NULL;
    ParseState* insert_pstate = make_parsestate(NULL);
    insert_pstate->p_resolve_unknowns = pstate->p_resolve_unknowns;
    insert_pstate->p_paramref_hook = pstate->p_paramref_hook;
    insert_pstate->p_ref_hook_state = pstate->p_ref_hook_state;
    insert_pstate->p_pre_columnref_hook = pstate->p_pre_columnref_hook;
    insert_pstate->p_post_columnref_hook = pstate->p_post_columnref_hook;
    insert_pstate->p_coerce_param_hook = pstate->p_coerce_param_hook;

    insert_query = transformStmt(insert_pstate, stmt->insert_stmt);

    /* free the parse state no matter we found correct insert query or not */
    free_parsestate(insert_pstate);
    return insert_query;
}
#ifdef ENABLE_MULTIPLE_NODES
static bool contain_subquery_walker(Node* node, void* context)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, SubLink)) {
        return true;
    }
    return expression_tree_walker(node, (bool (*)())contain_subquery_walker, (void*)context);
}

static bool contain_subquery(Node* clause)
{
    return contain_subquery_walker(clause, NULL);
}

/*
 * cannot have Subquery in action's qual and targetlist
 * report error if we found any.
 */
static void check_sublink_in_action(List* mergeActionList, bool is_insert_update)
{
    ListCell* lc = NULL;
    /* check action's qual and target list */
    foreach (lc, mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(lc);
        if (contain_subquery((Node*)action->qual)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Subquery in WHERE clauses are not yet supported for %s",
                        is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO")));
        }
        if (contain_subquery((Node*)action->targetList)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Subquery in INSERT/UPDATE clauses are not yet supported for %s",
                        is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO")));
        }
    }
}
#endif

/*
 * get current namespace for columns in the range of relation, subquery
 * param idx is the index in RTE collection only including relation, subquery
 */
List* get_varnamespace(ParseState* pstate, int idx = 0)
{
    List* result = NULL;
    ListCell* lc = NULL;
    int cur_idx = 1;
    foreach (lc, pstate->p_rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);
        if (rte->rtekind != RTE_RELATION && rte->rtekind != RTE_SUBQUERY) {
            continue;
        }

        if (idx <= 0) {  // if id is a non-positive integer, all RTEs are obtained.
            ParseNamespaceItem* tmp = makeNamespaceItem(rte, false, true);
            result = lappend(result, tmp);
            cur_idx += 1;
            continue;
        }

        if (cur_idx == idx) {  // if the value of id is valid, the specified RTE will be obtained
            ParseNamespaceItem* tmp = makeNamespaceItem(rte, false, true);
            result = lappend(result, tmp);
            break;
        }
        cur_idx += 1;
    }
    return result;
}

/*
 * transformMergeStmt -
 *	  transforms a MERGE statement
 */
Query* transformMergeStmt(ParseState* pstate, MergeStmt* stmt)
{
    Query* qry = makeNode(Query);
    ListCell* l = NULL;
    AclMode targetPerms = ACL_NO_RIGHTS;
    bool is_terminal[2];
    JoinExpr* joinexpr = NULL;
    List* mergeActionList = NIL;
    List* join_var_list = NIL;

    /* There can't be any outer WITH to worry about */
    Assert(pstate->p_ctenamespace == NIL);

    qry->commandType = CMD_MERGE;
    qry->hasRecursive = false;

    /* set io state for backend status for the thread, we will use it to check user space */
    pgstat_set_io_state(IOSTATE_WRITE);

    /* Check WHEN clauses for permissions and sanity */
    is_terminal[0] = false;
    is_terminal[1] = false;

    foreach (l, stmt->mergeWhenClauses) {
        MergeWhenClause* mergeWhenClause = (MergeWhenClause*)lfirst(l);
        int when_type = (mergeWhenClause->matched ? 0 : 1);

        /* Collect action types so we can check Target permissions */
        switch (mergeWhenClause->commandType) {
            case CMD_INSERT:
                targetPerms |= ACL_INSERT;
                break;
            case CMD_UPDATE:
                targetPerms |= ACL_UPDATE;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("unknown action in MERGE WHEN clause")));
        }

        /* Check for unreachable WHEN clauses */
        if (mergeWhenClause->condition == NULL) {
            is_terminal[when_type] = true;
        } else if (is_terminal[when_type]) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("unreachable WHEN clause specified after unconditional WHEN clause")));
        }
    }

    /*
     * Construct a query of the form
     * 	SELECT relation.ctid	--junk attribute
     *		  ,relation.tableoid	--junk attribute
     * 		  ,source_relation.<somecols>
     * 		  ,relation.<somecols>
     *  FROM relation RIGHT JOIN source_relation
     *  ON  join_condition; -- no WHERE clause - all conditions are applied in
     * executor
     *
     * stmt->relation is the target relation, given as a RangeVar
     * stmt->source_relation is a RangeVar or subquery
     *
     * We specify the join as a RIGHT JOIN as a simple way of forcing the
     * first (larg) RTE to refer to the target table.
     *
     * The MERGE query's join can be tuned in some cases, see below for these
     * special case tweaks.
     *
     * We set QSRC_PARSER to show query constructed in parse analysis
     *
     * Note that we have only one Query for a MERGE statement and the planner
     * is called only once. That query is executed once to produce our stream
     * of candidate change rows, so the query must contain all of the columns
     * required by each of the targetlist or conditions for each action.
     *
     * As top-level statements INSERT, UPDATE and DELETE have a Query, whereas
     * with MERGE the individual actions do not require separate planning,
     * only different handling in the executor. See nodeModifyTable handling
     * of commandType CMD_MERGE.
     *
     * A sub-query can include the Target, but otherwise the sub-query cannot
     * reference the outermost Target table at all.
     */
    qry->querySource = QSRC_PARSER;

    /*
     * Setup the target table. Unlike regular UPDATE/DELETE, we don't expand
     * inheritance for the target relation in case of MERGE.
     *
     * This special arrangement is required for handling partitioned tables
     * because we perform an JOIN between the target and the source relation to
     * identify the matching and not-matching rows. If we take the usual path
     * of expanding the target table's inheritance and create one subplan per
     * partition, then we we won't be able to correctly identify the matching
     * and not-matching rows since for a given source row, there may not be a
     * matching row in one partition, but it may exists in some other
     * partition. So we must first append all the qualifying rows from all the
     * partitions and then do the matching.
     *
     * Once a target row is returned by the underlying join, we find the
     * correct partition and setup required state to carry out UPDATE/DELETE.
     * All of this happens during execution.
     *
     * Note: Also, ExclusiveLock other than RowExclusiveLock is used for MERGE for now.
     * There are chances to use RowExclusiveLock to optimize the parallel performance
     * when the concurrent updates to be handled properly in the future.
     */
    qry->resultRelation = setTargetTable(pstate,
        stmt->relation,
        false, /* do not expand inheritance */
        true,
        targetPerms);

    checkUnsupportedCases(pstate, stmt);

    qry->mergeTarget_relation = qry->resultRelation;

    if (stmt->is_insert_update)
        fix_merge_stmt_for_insert_update(pstate, stmt);

    joinexpr = buildJoinExpr(stmt, targetPerms);

    /*
     * We use a special purpose transformation here because the normal
     * routines don't quite work right for the MERGE case.
     *
     * A special mergeSourceTargetList is setup by transformMergeJoinClause().
     * It refers to all the attributes provided by the source relation. This
     * is later used by set_plan_refs() to fix the UPDATE/INSERT target lists
     * to so that they can correctly fetch the attributes from the source
     * relation.
     *
     * The target relation when used in the underlying join, gets a new RTE
     * with rte->inh set to true. We remember this RTE (and later pass on to
     * the planner and executor) for two main reasons:
     *
     * 1. If we ever need to run EvalPlanQual while performing MERGE, we must
     * make the modified tuple available to the underlying join query, which is
     * using a different RTE from the resultRelation RTE.
     *
     * 2. rewriteTargetListMerge() requires the RTE of the underlying join in
     * order to add junk CTID and TABLEOID attributes.
     */
    transformMergeJoinClause(pstate, (Node*)joinexpr);

    /* get var that used in ON clause, then use it to check some restriction */
    join_var_list = pull_var_clause((Node*)pstate->p_joinlist, PVC_REJECT_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

    /*
     * The target table referenced in the MERGE is looked up twice; once while
     * setting it up as the result relation and again when it's used in the

     * these lookups return different results, for example, if a new relation
     * with the same name gets created in a schema which is ahead in the
     * search_path, in between the two lookups.
     *
     * It's a very narrow case, but nevertheless we guard against it by simply
     * checking if the OIDs returned by the two lookups is the same. If not, we
     * just throw an error.
     */
    Assert(qry->resultRelation > 0);

    /*
     * Expand the right relation and add its columns to the
     * mergeSourceTargetList. Note that the right relation can either be a
     * plain relation or a subquery or anything that can have a
     * RangeTableEntry.
     */
    qry->mergeSourceTargetList =
        expandSourceTL(pstate, rt_fetch(qry->resultRelation + 1, pstate->p_rtable), qry->resultRelation + 1);

    /*
     * This query should just provide the source relation columns. Later, in
     * preprocess_targetlist(), we shall also add "ctid" attribute of the
     * target relation to ensure that the target tuple can be fetched
     * correctly.
     */
    qry->targetList = qry->mergeSourceTargetList;

    /* qry has no WHERE clause so absent quals are shown as NULL */
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    qry->rtable = pstate->p_rtable;

    /* Check unsupported merge when clauses */
    if (list_length(stmt->mergeWhenClauses) > 2)
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("More than two MERGE WHEN clauses are specified")));

    if (list_length(stmt->mergeWhenClauses) == 2) {
        MergeWhenClause* mergeWhenClause1 = (MergeWhenClause*)linitial(stmt->mergeWhenClauses);
        MergeWhenClause* mergeWhenClause2 = (MergeWhenClause*)lsecond(stmt->mergeWhenClauses);

        if (mergeWhenClause1->matched == true && mergeWhenClause2->matched == true)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Two WHEN MATCHED clauses are not supported for MERGE INTO")));

        if (mergeWhenClause1->matched == false && mergeWhenClause2->matched == false)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Two WHEN NOT MATCHED clauses are not supported for MERGE INTO")));
    }

    /*
     * We now have a good query shape, so now look at the when conditions and
     * action targetlists.
     *
     * Overall, the MERGE Query's targetlist is NIL.
     *
     * Each individual action has its own targetlist that needs separate
     * transformation. These transforms don't do anything to the overall
     * targetlist, since that is only used for resjunk columns.
     *
     * We can reference any column in Target or Source, which is OK because
     * both of those already have RTEs. There is nothing like the EXCLUDED
     * pseudo-relation for INSERT ON CONFLICT.
     */
    mergeActionList = NIL;
    foreach (l, stmt->mergeWhenClauses) {
        MergeWhenClause* mergeWhenClause = (MergeWhenClause*)lfirst(l);
        MergeAction* action = makeNode(MergeAction);

        action->commandType = mergeWhenClause->commandType;
        action->matched = mergeWhenClause->matched;

        /*
         * Transform target lists for each INSERT and UPDATE action stmt
         */
        switch (action->commandType) {
            case CMD_INSERT: {
                List* exprList = NIL;
                ListCell* lc = NULL;
                RangeTblEntry* rte = NULL;
                ListCell* icols = NULL;
                ListCell* attnos = NULL;
                List* icolumns = NIL;
                List* attrnos = NIL;
                List* save_relnamespace = NIL;
                List* save_varnamespace = NIL;
                RangeTblEntry* sourceRelRTE = NULL;

                /*
                 * For MERGE INTO type SQL, in the RTE list, the object with index 1 is the target table, and the
                 * object with index 2 is the source table (the index starts from 1).
                 * For the insert scenario, var namespace needs to be specified as the source table.
                 */
                const unsigned int merge_source_rte_index = 2;
                List* tmp_varnamespace = get_varnamespace(pstate, merge_source_rte_index);

                /*
                 * Assume that the top-level join RTE is at the end.
                 * The source relation is just before that.
                 */
                sourceRelRTE = rt_fetch(list_length(pstate->p_rtable) - 1, pstate->p_rtable);

                /*
                 * Insert can't see target relation, but it can see
                 * source relation.
                 */
                save_relnamespace = pstate->p_relnamespace;
                save_varnamespace = pstate->p_varnamespace;
                pstate->p_relnamespace = list_make1(makeNamespaceItem(sourceRelRTE, false, true));
                pstate->p_varnamespace = tmp_varnamespace;
                /*
                 * Transform the when condition.
                 *
                 * Note that these quals are NOT added to the join quals; instead they
                 * are evaluated separately during execution to decide which of the
                 * WHEN MATCHED or WHEN NOT MATCHED actions to execute.
                 */
                action->qual = transformWhereClause(pstate, mergeWhenClause->condition, "WHEN");
                pstate->p_varnamespace = save_varnamespace;

                pstate->p_is_insert = true;

                List* set_clause_list_copy = mergeWhenClause->cols;
                if (stmt->relation->alias != NULL) {
                    char* aliasname = (char*)(stmt->relation->alias->aliasname);
                    fixResTargetNameWithAlias(set_clause_list_copy, aliasname);
                }
                mergeWhenClause->cols = set_clause_list_copy;

                icolumns = checkInsertTargets(pstate, mergeWhenClause->cols, &attrnos);
                Assert(list_length(icolumns) == list_length(attrnos));

                /*
                 * Handle INSERT much like in transformInsertStmt
                 */
                if (mergeWhenClause->values == NIL) {
                    /*
                     * We have INSERT ... DEFAULT VALUES.  We can handle
                     * this case by emitting an empty targetlist --- all
                     * columns will be defaulted when the planner expands
                     * the targetlist.
                     */
                    exprList = NIL;
                } else {
                    /*
                     * Process INSERT ... VALUES with a single VALUES
                     * sublist.  We treat this case separately for
                     * efficiency.  The sublist is just computed directly
                     * as the Query's targetlist, with no VALUES RTE.  So
                     * it works just like a SELECT without any FROM.
                     */
                    /*
                     * Do basic expression transformation (same as a ROW()
                     * expr, but allow SetToDefault at top level)
                     */
                    exprList = transformExpressionList(pstate, mergeWhenClause->values);

                    /*
                     * If td_compatible_truncation equal true and no foreign table found,
                     * the auto truncation funciton should be enabled.
                     */

                    if (u_sess->attr.attr_sql.sql_compatibility == C_FORMAT && pstate->p_target_relation != NULL &&
                        !RelationIsForeignTable(pstate->p_target_relation) &&
                        !RelationIsStream(pstate->p_target_relation)) {

                        if (u_sess->attr.attr_sql.td_compatible_truncation) {
                            pstate->p_is_td_compatible_truncation = true;
                        } else {
                            pstate->tdTruncCastStatus = NOT_CAST_BECAUSEOF_GUC;
                        }
                    }

                    /* Prepare row for assignment to target table */
                    exprList = transformInsertRow(pstate, exprList, mergeWhenClause->cols, icolumns, attrnos);
                }

                pstate->p_relnamespace = save_relnamespace;

                /*
                 * Generate action's target list using the computed list
                 * of expressions. Also, mark all the target columns as
                 * needing insert permissions.
                 */
                rte = pstate->p_target_rangetblentry;
                icols = list_head(icolumns);
                attnos = list_head(attrnos);
                foreach (lc, exprList) {
                    Expr* expr = (Expr*)lfirst(lc);
                    ResTarget* col = NULL;
                    AttrNumber attr_num;
                    TargetEntry* tle = NULL;

                    col = lfirst_node(ResTarget, icols);
                    attr_num = (AttrNumber)lfirst_int(attnos);

                    tle = makeTargetEntry(expr, attr_num, col->name, false);
                    action->targetList = lappend(action->targetList, tle);

                    rte->insertedCols =
                        bms_add_member(rte->insertedCols, attr_num - FirstLowInvalidHeapAttributeNumber);

                    icols = lnext(icols);
                    attnos = lnext(attnos);
                }
            } break;
            case CMD_UPDATE: {
                List* set_clause_list_copy = mergeWhenClause->targetList;

                List* save_varnamespace = NIL;
                List* tmp_varnamespace = get_varnamespace(pstate);
                save_varnamespace = pstate->p_varnamespace;
                pstate->p_varnamespace = tmp_varnamespace;
                pstate->use_level = true;

                /*
                 * Transform the when condition.
                 *
                 * Note that these quals are NOT added to the join quals; instead they
                 * are evaluated separately during execution to decide which of the
                 * WHEN MATCHED or WHEN NOT MATCHED actions to execute.
                 */
                action->qual = transformWhereClause(pstate, mergeWhenClause->condition, "WHEN");
                pstate->p_varnamespace = save_varnamespace;
                pstate->use_level = false;

                fixResTargetListWithTableNameRef(pstate->p_target_relation, stmt->relation, set_clause_list_copy);
                mergeWhenClause->targetList = set_clause_list_copy;
                pstate->p_is_insert = false;
                action->targetList = transformUpdateTargetList(pstate, mergeWhenClause->targetList);

                /* check if update cols are leagal */
                checkUpdateOnDistributeKey(pstate->p_target_rangetblentry, action->targetList);
                checkUpdateOnJoinKey(pstate, mergeWhenClause, join_var_list, stmt->is_insert_update);

            } break;
            default:
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("unknown action in MERGE WHEN clause")));
        }

        mergeActionList = lappend(mergeActionList, action);
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* we don't support subqueries in action */
    check_sublink_in_action(mergeActionList, stmt->is_insert_update);
#endif
    /* cannot reference system column */
    check_system_column_reference(join_var_list, mergeActionList, stmt->is_insert_update);

    /* insert action targetlist can only refer to source table */
    check_insert_action_targetlist(mergeActionList, qry->mergeSourceTargetList);

    qry->mergeActionList = mergeActionList;
    qry->returningList = NULL;
    qry->hasSubLinks = pstate->p_hasSubLinks;
    assign_query_collations(pstate, qry);

    /* for UPSERT, should also transform the InsertStmt
     * in order by tell whether the UPSERT statement is shippable or not
     * later in the pgxc_FQS_planner
     */
    if (
#ifdef 	ENABLE_MULTIPLE_NODES
	IS_PGXC_COORDINATOR &&
#endif
		stmt->is_insert_update) {
        Assert(stmt->insert_stmt != NULL);
        Assert(IsA(stmt->insert_stmt, InsertStmt));

        /*
         * in the process of grey upgrade, when the version number is ahead
         * of UPSERT_ROW_STORE_VERSION_NUM, do not support update EXCLUDED
         */
        if (
#ifdef ENABLE_MULTIPLE_NODES
			t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM &&
#endif
            check_update_action_targetlist(mergeActionList, qry->mergeSourceTargetList)) {
#ifndef ENABLE_MULTIPLE_NODES
            if (u_sess->attr.attr_sql.enable_upsert_to_merge) {
                return qry;
            }
#endif
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("Invalid column reference in the UPDATE target values"),
                    errhint("Only allow to reference target table's column in the UPDATE clause")));
        }

        Query* insert_query = tryTransformMergeInsertStmt(pstate, stmt);

        if (insert_query == NULL) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    errmsg("failed to transform InsertStmt for INSERT ON DUPLICATE KEY UPDATE statement.")));
        } else {
            if (unlikely(t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM)) {
                /* put the mergeActionList into the Query for nessesary deparse later */
                insert_query->mergeActionList = qry->mergeActionList;
            }

            /* adjust the permission level, UPSERT need SELECT, INSERT, UPDATE permission. */
            RangeTblEntry* rte = (RangeTblEntry*)lfirst(list_nth_cell(insert_query->rtable, 0));
            AclMode targetPerms = rte->requiredPerms;
            targetPerms |= ACL_INSERT;
            targetPerms |= ACL_UPDATE;
            targetPerms |= ACL_SELECT;
            rte->requiredPerms = targetPerms;

            /* save it */
            qry->upsertQuery = insert_query;

            if (rte->is_ustore) {
                ereport(ERROR, ((errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                  errmsg("INSERT ON DUPLICATE KEY UPDATE is not supported on ustore."))));
            }
        }
    }

    qry->hintState = stmt->hintState;

    return qry;
}

/*
 * Expand the source relation to include all attributes of this RTE.
 *
 * This function is very similar to expandRelAttrs except that we don't mark
 * columns for SELECT privileges. That will be decided later when we transform
 * the action targetlists and the WHEN quals for actual references to the
 * source relation.
 */
static List* expandSourceTL(ParseState* pstate, RangeTblEntry* rte, int rtindex)
{
    List* names = NIL;
    List* vars = NIL;
    ListCell* name = NULL;
    ListCell* var = NULL;
    List* te_list = NIL;

    expandRTE(rte, rtindex, 0, -1, false, &names, &vars);

    /* Require read access to the table. */
    rte->requiredPerms |= ACL_SELECT;

    forboth(name, names, var, vars)
    {
        char* label = strVal(lfirst(name));
        Var* varnode = (Var*)lfirst(var);
        TargetEntry* te = NULL;

        te = makeTargetEntry((Expr*)varnode, (AttrNumber)pstate->p_next_resno++, label, false);

        te_list = lappend(te_list, te);
    }

    Assert(name == NULL && var == NULL); /* lists not the same length? */

    return te_list;
}

List* expandTargetTL(List* te_list, Query* parsetree)
{
    List* names = NIL;
    List* vars = NIL;
    ListCell* name = NULL;
    ListCell* var = NULL;

    RangeTblEntry* rt_des = rt_fetch(parsetree->mergeTarget_relation, parsetree->rtable);
    expandRTE(rt_des, parsetree->mergeTarget_relation, 0, -1, false, &names, &vars);

    /* Require read access to the table. */
    rt_des->requiredPerms |= ACL_SELECT;
    AttrNumber resno = list_length(parsetree->targetList) + 1;

    forboth(name, names, var, vars)
    {
        char* label = strVal(lfirst(name));
        Var* varnode = (Var*)lfirst(var);
        TargetEntry* te = NULL;

        te = makeTargetEntry((Expr*)varnode, resno++, label, false);
        te_list = lappend(te_list, te);
    }

    Assert(name == NULL && var == NULL); /* lists not the same length? */

    return te_list;
}

List* expandActionTL(List* te_list, Query* parsetree)
{
    ListCell* lc = NULL;
    ListCell* llc = NULL;
    List* mergeActionList = parsetree->mergeActionList;
    AttrNumber resno = list_length(parsetree->targetList) + 1;

    foreach (lc, mergeActionList) {
        int target_idx = 0;
        MergeAction* action = (MergeAction*)lfirst(lc);
        foreach (llc, action->targetList) {
            target_idx++;
            TargetEntry* oldte = (TargetEntry*)lfirst(llc);
            char label[100] = {0};
            errno_t sret;

            sret = snprintf_s(label,
                sizeof(label),
                sizeof(label) - 1,
                "action %s target %d",
                action->matched ? "UPDATE" : "INSERT",
                target_idx);

            securec_check_ss(sret, "", "");

            TargetEntry* newte = makeTargetEntry((Expr*)oldte->expr, resno++, pstrdup(label), false);

            te_list = lappend(te_list, newte);
        }
    }

    return te_list;
}

/* expand vars in action's qual to make sure we can later reference it */
List* expandQualTL(List* te_list, Query* parsetree)
{
    ListCell* lc = NULL;
    ListCell* llc = NULL;
    List* mergeActionList = parsetree->mergeActionList;
    AttrNumber resno = list_length(parsetree->targetList) + 1;

    foreach (lc, mergeActionList) {
        int target_idx = 0;
        MergeAction* action = (MergeAction*)lfirst(lc);
        List* vars_on_qual = pull_var_clause((Node*)action->qual, PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
        foreach (llc, vars_on_qual) {
            Var* var = (Var*)lfirst(llc);
            char label[100] = {0};
            errno_t sret;
            target_idx++;

            sret = snprintf_s(label,
                sizeof(label),
                sizeof(label) - 1,
                "action %s qual %d",
                action->matched ? "UPDATE" : "INSERT",
                target_idx);

            securec_check_ss(sret, "", "");

            TargetEntry* newte = makeTargetEntry((Expr*)var, resno++, pstrdup(label), false);

            te_list = lappend(te_list, newte);
        }
    }

    return te_list;
}

static void checkUpdateOnJoinKey(
    ParseState* pstate, MergeWhenClause* clause, List* join_var_list, bool is_insert_update)
{
    ListCell* update_target_cell = NULL;
    ListCell* join_var_cell = NULL;

    foreach (update_target_cell, clause->targetList) {
        ResTarget* origTarget = (ResTarget*)lfirst(update_target_cell);
        Assert(IsA(origTarget, ResTarget));
        int attrno = attnameAttNum(pstate->p_target_relation, origTarget->name, true);

        /* we checked already, should not happend */
        if (attrno == InvalidAttrNumber) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" of relation \"%s\" does not exist",
                        origTarget->name,
                        RelationGetRelationName(pstate->p_target_relation)),
                    parser_errposition(pstate, origTarget->location)));
        }
        /* can't update columns that referenced in the ON clause */
        foreach (join_var_cell, join_var_list) {
            Var* join_var = (Var*)lfirst(join_var_cell);
            if (join_var->varno == 1 && join_var->varattno == attrno) {
                if (is_insert_update) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            errmodule(MOD_OPT_REWRITE),
                            errmsg("Columns referenced in the primary or unique index cannot be updated: \"%s\".\"%s\"",
                                RelationGetRelationName(pstate->p_target_relation),
                                origTarget->name)));
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            errmodule(MOD_OPT_REWRITE),
                            errmsg("Columns referenced in the ON Clause cannot be updated: \"%s\".\"%s\"",
                                RelationGetRelationName(pstate->p_target_relation),
                                origTarget->name)));
                }
            }
        }
    }
}

static void check_source_table_replicated(Node* source_relation)
{
    Assert(IsA(source_relation, RangeSubselect));

    RangeSubselect* rangeSubselect = (RangeSubselect*)source_relation;
    if (rangeSubselect->subquery == NULL) {
        return;
    }
    /* NOTE: for now we only permit non-table related expressions in VALUES clause
     * In the futher, maybe replicated table related SelectStmt can be also
     * allowd to us UPSERT.
     */
    SelectStmt* selectStmt = (SelectStmt*)rangeSubselect->subquery;
    if (selectStmt->valuesLists != NIL) {
        return;
    }
    ereport(ERROR,
        (errmodule(MOD_PARSER),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Replicate table only allows to INSERT static values from a VALUES clause "
                   "in INSERT ... ON DUPLICATE KEY UPDATE.")));
}

static bool checkTargetTableReplicated(RangeTblEntry* rte)
{
    RelationLocInfo* rel_loc_info = NULL;

    if (rte == NULL) {
        return false;
    }
    if (rte != NULL && rte->relkind != RELKIND_RELATION) {
        /* Bad relation type */
        return false;
    }
    /* See if we have the partitioned case. */
    rel_loc_info = GetRelationLocInfo(rte->relid);
    /* Any column updation on local relations is fine */
    if (rel_loc_info == NULL) {
        return false;
    }
    /* Only relations distributed by value can be checked */
    if (IsRelationReplicated(rel_loc_info)) {
        return true;
    }
    return false;
}

static void checkTargetTableSystemCatalog(Relation targetRel)
{
    if (IsUnderPostmaster && !g_instance.attr.attr_common.allowSystemTableMods && IsSystemRelation(targetRel)) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied: \"%s\" is a system catalog", RelationGetRelationName(targetRel))));
    }
}

static void check_target_table_columns(ParseState* pstate, bool is_insert_update)
{
    List* rel_valid_cols = NIL;
    List* rel_valid_attrnos = NIL;
    int attrno = 0;
    Node* expr = NULL;
    ListCell* cell = NULL;

    Relation target_relation = pstate->p_target_relation;
    rel_valid_cols = checkInsertTargets(pstate, NIL, &rel_valid_attrnos);

    foreach (cell, rel_valid_attrnos) {
        attrno = lfirst_int(cell);
        expr = build_column_default(target_relation, attrno, false);
        if ((expr != NULL) && (contain_volatile_functions(expr))) {
            ereport(ERROR,
                (errmodule(MOD_PARSER),
                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("It is not allowed to use %s on the replicate table (%s) "
                           "with column (%s) of unstable default value.",
                        is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO",
                        RelationGetRelationName(target_relation),
                        NameStr(target_relation->rd_att->attrs[attrno - 1]->attname))));
        }
    }
    list_free_deep(rel_valid_cols);
}

static void checkUpdateOnDistributeKey(RangeTblEntry* rte, List* targetlist)
{
    RelationLocInfo* rel_loc_info = NULL;
    ListCell* lc = NULL;
    if (rte == NULL) {
        return;
    }

#ifdef ENABLE_MOT
    if (rte != NULL && rte->relkind != RELKIND_RELATION &&
        !(rte->relkind == RELKIND_FOREIGN_TABLE && isMOTFromTblOid(rte->relid))) {
#else
    if (rte != NULL && rte->relkind != RELKIND_RELATION) {
#endif
        /* Bad relation type */
        return;
    }

    /* See if we have the partitioned case. */
    rel_loc_info = GetRelationLocInfo(rte->relid);
    /* Any column updation on local relations is fine */
    if (rel_loc_info == NULL) {
        return;
    }
    /* Only relations distributed by value can be checked */
    if (IsRelationDistributedByValue(rel_loc_info)) {
        /* It is a partitioned table, check partition column in targetList */
        foreach (lc, targetlist) {
            TargetEntry* tle = (TargetEntry*)lfirst(lc);

            /* Nothing to do for a junk entry */
            if (tle->resjunk) {
                continue;
            }
            /*
             * See if we have a constant expression comparing against the
             * designated partitioned column
             */
            List* distributeCol = GetRelationDistribColumn(rel_loc_info);
            ListCell* cell = NULL;
            foreach (cell, distributeCol) {
                if (strcmp(tle->resname, strVal(lfirst(cell))) == 0) {
                    ereport(ERROR,
                        ((errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            (errmsg("Distributed key column can't be updated in current version")))));
                }
            }
        }
    }
}

/*
 * cannot reference system column in
 *   1. on clause
 *   2. action's qual
 * report error if we found any
 */
static void check_system_column_reference(List* joinVarList, List* mergeActionList, bool is_insert_update)
{
    ListCell* lc = NULL;

    /* check action's qual and target list */
    foreach (lc, mergeActionList) {
        MergeAction* action = (MergeAction*)lfirst(lc);

        check_system_column_node((Node*)action->qual, is_insert_update);
        check_system_column_node((Node*)action->targetList, is_insert_update);
    }

    /* check join on clause */
    check_system_column_varlist(joinVarList, is_insert_update);
}

static void check_system_column_node(Node* node, bool is_insert_update)
{
    List* vars = pull_var_clause(node, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

    check_system_column_varlist(vars, is_insert_update);
}

static void check_system_column_varlist(List* varlist, bool is_insert_update)
{
    ListCell* lc = NULL;

    foreach (lc, varlist) {
        Var* var = (Var*)lfirst(lc);

        if (var->varattno < 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("System Column reference are not yet supported for %s",
                        is_insert_update ? "INSERT ... ON DUPLICATE KEY UPDATE" : "MERGE INTO")));
        }
    }
}

/* check if var exist in list */
static bool var_in_list(Var* var, List* list)
{
    ListCell* lc = NULL;
    Var* a = var;
    bool found = false;
    foreach (lc, list) {
        if (IsA(lfirst(lc), Var)) {
            Var* b = (Var*)lfirst(lc);
            if (equal(a, b)) {
                found = true;
            }
        }
    }

    return found;
}

static Node* append_expr(Node* expr1, Node* expr2, A_Expr_Kind kind)
{
    if (expr1 == NULL) {
        expr1 = expr2;
    } else if (expr2 != NULL) {
        expr1 = (Node*)makeA_Expr(kind, NIL, expr1, expr2, -1);
    }
    return expr1;
}

static Bitmapset* get_relation_attno_bitmap_by_names(Relation relation, List* colnames)
{
    Bitmapset* bitmap = NULL;
    ListCell* cell = NULL;
    int attrno = 0;
    foreach (cell, colnames) {
        attrno = attnameAttNum(relation, strVal(lfirst(cell)), false);
        /* check for non-exist column */
        if (attrno == InvalidAttrNumber) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmodule(MOD_OPT_REWRITE),
                    errmsg("column \"%s\" of relation \"%s\" does not exist",
                        strVal(lfirst(cell)),
                        RelationGetRelationName(relation))));
        }
        /* check for duplicates */
        if (bms_is_member(attrno, bitmap)) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_COLUMN),
                    errmodule(MOD_OPT_REWRITE),
                    errmsg("column \"%s\" specified more than once", strVal(lfirst(cell)))));
        }
        bitmap = bms_add_member(bitmap, attrno);
    }
    return bitmap;
}

static Bitmapset* get_relation_default_attno_bitmap(Relation relation)
{
    Bitmapset* bitmap = NULL;
    Form_pg_attribute attr = NULL;
    for (int i = 0; i < RelationGetNumberOfAttributes(relation); i++) {
        attr = relation->rd_att->attrs[i];

        if (attr->atthasdef && !attr->attisdropped) {
            bitmap = bms_add_member(bitmap, attr->attnum);
        }
    }
    return bitmap;
}

static MergeWhenClause* get_merge_when_clause(List* mergeWhenClauses, CmdType cmd_type, bool matched)
{
    /* find the WHEN NOT MATCHED INSERT clause */
    MergeWhenClause* mergeWhenClause = NULL;
    ListCell* cell = NULL;
    foreach (cell, mergeWhenClauses) {
        mergeWhenClause = (MergeWhenClause*)lfirst(cell);
        if ((mergeWhenClause->commandType == cmd_type) && (mergeWhenClause->matched == matched)) {
            break;
        }
    }
    return mergeWhenClause;
}

static SelectStmt* get_valid_select_stmt(SelectStmt* node)
{
    Assert(node != NULL);

    if (node->op == SETOP_NONE) {
        return node;
    } else {
        /* for UNION, INTERSECT, EXCEPT case, only check its left (or right) argument */
        return get_valid_select_stmt(node->larg);
    }
}

static int count_target_columns(Node* query)
{
    int cols_count = 0;
    List* target_list = NIL;
    SelectStmt* select_stmt = NULL;

    if (IsA(query, SelectStmt)) {
        select_stmt = get_valid_select_stmt((SelectStmt*)query);
        if (select_stmt->valuesLists != NIL) {
            target_list = (List*)linitial(select_stmt->valuesLists);
            cols_count = list_length(target_list);
        } else if (select_stmt->targetList != NIL) {
            ListCell* cell = NULL;
            ParseState* pstate = NULL;
            SelectStmt* stmt = NULL;
            Query* query = NULL;

            /* make new parse state and statement cause we should not expand it here. */
            pstate = make_parsestate(NULL);
            stmt = (SelectStmt*)copyObject(select_stmt);

            pstate->p_resolve_unknowns = true;
            query = transformStmt(pstate, (Node*)stmt);
            if (query == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmodule(MOD_PARSER),
                        errmsg("Subquery should at least have a target column.")));
            }
            target_list = query->targetList;

            /* remove those junk columns */
            cols_count = list_length(target_list);
            foreach (cell, target_list) {
                TargetEntry* te = (TargetEntry*)lfirst(cell);
                if (te->resjunk)
                    cols_count--;
            }
            free_parsestate(pstate);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmodule(MOD_PARSER),
                    errmsg("Subquery should at least have a target column.")));
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_PARSER),
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("Unexpected Node type \"%s\"", nodeTagToString(nodeTag(query)))));
    }
    return cols_count;
}

/* @Description: check if there is any primary or unique index
 * @in index_list: index info list
 * @out index_list: unique index list
 * @return: true if there is any primary or unique index
 */
bool check_unique_constraint(List*& index_list)
{
    /* There are no indexes */
    if (index_list == NIL) {
        return false;
    }
    /* check if there are any primary or unique index */
    IndexInfo* index_info = NULL;

    for (int i = list_length(index_list) - 1; i >= 0; i--) {
        index_info = (IndexInfo*)list_nth(index_list, i);
        if (!index_info->ii_Unique) {
            index_list = RemoveListCell(index_list, i + 1);
        }
    }

    if (list_length(index_list) == 0) {
        return false;
    }
    return true;
}

bool check_action_targetlist_condition(List* action_targetlist, List* source_targetlist, bool condition_in)
{
    ListCell* var_cell = NULL;
    /* oops, not insert action found, noting to do */
    if (action_targetlist == NIL) {
        return true;
    }
    /* let's do the hard work */
    List* vars_list =
        pull_var_clause((Node*)action_targetlist, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    List* source_vars_list =
        pull_var_clause((Node*)source_targetlist, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    foreach (var_cell, vars_list) {
        Var* var = (Var*)lfirst(var_cell);
        if (var_in_list(var, source_vars_list) != condition_in) {
            return false;
        }
    }
    return true;
}

bool check_update_action_targetlist(List* merge_action_list, List* source_targetlist)
{
    ListCell* action_cell = NULL;
    List* update_action_targetlist = NIL;

    /* first let's locate the insert action */
    foreach (action_cell, merge_action_list) {
        MergeAction* action = (MergeAction*)lfirst(action_cell);
        if (action->commandType == CMD_UPDATE) {
            update_action_targetlist = action->targetList;

            break;
        }
    }

    return !check_action_targetlist_condition(update_action_targetlist, source_targetlist, false);
}

/* report error if vars in insert_action_targetlist not found in source_targetlist */
void check_insert_action_targetlist(List* merge_action_list, List* source_targetlist)
{
    ListCell* action_cell = NULL;
    List* insert_action_targetlist = NIL;

    /* first let's locate the insert action */
    foreach (action_cell, merge_action_list) {
        MergeAction* action = (MergeAction*)lfirst(action_cell);
        if (action->commandType == CMD_INSERT) {
            insert_action_targetlist = action->targetList;

            break;
        }
    }
    if (!check_action_targetlist_condition(insert_action_targetlist, source_targetlist, true)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("Invalid column reference in the INSERT VALUES Clause"),
                errhint("You may have referenced target table's column")));
    }
}
#endif /* FRONTEND_PARSER */

/* traverse set_clause_list and judge if the ColId is equal to aliasname of table */
void fixResTargetNameWithAlias(List *clause_list, const char *aliasname)
{
    ListCell *cell = NULL;
    foreach (cell, clause_list) {
        if (IsA(lfirst(cell), ResTarget)) {
            ResTarget *res = (ResTarget *)lfirst(cell);
            if (!strcmp(res->name, aliasname) && res->indirection) {
#ifndef FRONTEND_PARSER
                char *resname = pstrdup((char *)(((Value *)lfirst(list_head(res->indirection)))->val.str));
#else
                char *resname = feparser_strdup((char *)(((Value *)lfirst(list_head(res->indirection)))->val.str));
#endif
                ((ResTarget *)lfirst(cell))->name = resname;
                ((ResTarget *)lfirst(cell))->indirection = RemoveListCell(res->indirection, 1);
            }
        }
    }
}
