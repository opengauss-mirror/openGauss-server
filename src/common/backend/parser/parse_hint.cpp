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
 * parse_hint.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/parser/parse_hint.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_hint.h"
#include "parser/scansup.h"
#include "optimizer/prep.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "parser/parse_type.h"
#include "optimizer/var.h"

/* We must include  "parser/gramparse.h" after including "parser/parse_hint.h"
 * Otherwise, we will include the wrong header file.
 * Because we need to include hint_gram.hpp, not gram.hpp
 * Please see gramparse.h for the detail.
 */
#include "parser/gramparse.h"

#define NOTFOUNDRELNAME 0
#define AMBIGUOUSRELNAME (-1)

/* Too much skew value will cost to much memory and execution time, so we add a limit here. */
#define MAX_SKEW_NUM 10

/* free hint's base relnames if there are any */
#define HINT_FREE_RELNAMES(hint)                   \
    do {                                           \
        if ((hint)->base.relnames) {               \
            list_free_deep((hint)->base.relnames); \
        }                                          \
    } while (0)

typedef struct join_relid_relname {
    Relids relids;  /* Join relids*/
    List* rel_list; /* Join rel name.*/
} relid_relname;

static const char* KeywordDesc(HintKeyword keyword);
static void relnamesToBuf(List* relnames, StringInfo buf);
static void JoinMethodHintDesc(JoinMethodHint* hint, StringInfo buf);
static void LeadingHintDesc(LeadingHint* hint, StringInfo buf);
static void RowsHintDesc(RowsHint* hint, StringInfo buf);
static void StreamHintDesc(StreamHint* hint, StringInfo buf);
static void BlockNameHintDesc(BlockNameHint* hint, StringInfo buf);
static void ScanMethodHintDesc(ScanMethodHint* hint, StringInfo buf);
static void SkewHintDesc(SkewHint* hint, StringInfo buf);
static void find_unused_hint_to_buf(List* hint_list, StringInfo hint_buf);
static void JoinMethodHintDelete(JoinMethodHint* hint);
static void LeadingHintDelete(LeadingHint* hint);
static void RowsHintDelete(RowsHint* hint);
static void RewriteHintDelete(RewriteHint* hint);
static void GatherHintDelete(GatherHint* hint);
static void StreamHintDelete(StreamHint* hint);
static void BlockNameHintDelete(BlockNameHint* hint);
static void ScanMethodHintDelete(ScanMethodHint* hint);
static void SkewHintDelete(SkewHint* hint);
static void SkewHintTransfDelete(SkewHintTransf* hint);
static char* get_hints_from_comment(const char* comment_str);
static void drop_duplicate_blockname_hint(HintState* hstate);
static void drop_duplicate_join_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_stream_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_row_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_rewrite_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_gather_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_scan_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_skew_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_predpush_hint(PlannerInfo* root, HintState* hstate);
static void drop_duplicate_predpush_same_level_hint(PlannerInfo* root, HintState* hstate);
static int find_relid_aliasname(Query* parse, const char* aliasname, bool find_in_rtable = false);
static Relids create_bms_of_relids(
    PlannerInfo* root, Query* parse, Hint* hint, List* relnamelist = NIL, Relids currelids = NULL);
static List* set_hint_relids(PlannerInfo* root, Query* parse, List* l);
static void leading_to_join_hint(HintState* hstate, Relids join_relids, Relids inner_relids, List* relname_list);
static void transform_leading_hint(PlannerInfo* root, Query* parse, HintState* hstate);
static void transform_skew_hint(PlannerInfo* root, Query* parse, List* skew_hint_list);
static SkewHintTransf* set_skew_hint(PlannerInfo* root, Query* parse, SkewHint* skew_hint);
static void set_subquery_rel(
    Query* parse, RangeTblEntry* rte, SkewHintTransf* skew_hint_transf, RangeTblEntry* parent_rte = NULL);
static void set_base_rel(Query* parse, RangeTblEntry* rte, SkewHintTransf* skew_hint_transf);
static bool subquery_can_pull_up(RangeTblEntry* subquery_rte, Query* parse);
static void set_skew_column(PlannerInfo* root, Query* parse, SkewHintTransf* skew_hint_transf, Oid** col_typid);
static RangeTblEntry* find_column_in_rtable_subquery(Query* parse, const char* column_name, int* location);
static RangeTblEntry* find_column_in_rel_info_list(
    SkewHintTransf* skew_hint_transf, const char* column_name, int* count, int* location);
static TargetEntry* find_column_in_targetlist_by_location(RangeTblEntry* rte, int location, RangeTblEntry** col_rte);
static TargetEntry* find_column_in_targetlist_by_name(List* targetList, const char* column_name);
static bool check_parent_rte_is_diff(RangeTblEntry* parent_rte, List* parent_rte_list);
void pull_up_expr_varno(Query* parse, RangeTblEntry* rte, SkewColumnInfo* column_info);
static void set_colinfo_by_relation(Oid relid, int location, SkewColumnInfo* column_info, char* column_name);
static void set_colinfo_by_tge(
    TargetEntry* tge, SkewColumnInfo* column_info, char* column_name, RangeTblEntry* rte = NULL, Query* parse = NULL);
static void set_skew_value(PlannerInfo* root, SkewHintTransf* skew_hint_transf, Oid* col_typid);
static bool set_skew_value_to_datum(
    Value* skew_value, Datum* val_datum, bool* constisnull, Oid val_typid, int4 val_typmod, ErrorData** edata);
static int get_col_location(const char* column_name, List* eref_col);
static RangeTblEntry* get_rte(Query* parse, const char* skew_relname);
static char* get_name(ListCell* lc);
static bool support_redistribution(Oid typid);
static List* delete_invalid_hint(PlannerInfo* root, HintState* hint, List* list);
static Relids OuterInnerJoinCreate(Query* parse, HintState* hstate, List* relnames, bool join_order_hint);
static unsigned int get_rewrite_rule_bits(RewriteHint* hint);

extern yyscan_t hint_scanner_init(const char* str, hint_yy_extra_type* yyext);
extern void hint_scanner_destroy(yyscan_t yyscanner);
extern void hint_scanner_yyerror(const char* msg, yyscan_t yyscanner);
extern Datum GetDatumFromString(Oid typeOid, int4 typeMod, char* value);
extern Const* makeConst(Oid consttype, int32 consttypmod, Oid constcollid, int constlen, Datum constvalue,
    bool constisnull, bool constbyval, Cursor_Data* vars = NULL);
extern Numeric int64_to_numeric(int64 v);

static bool IsScanUseDesthint(void* val1, void* val2);

/* Expression kind codes for preprocess_expression */
#define EXPRKIND_QUAL 0
#define EXPRKIND_TARGET 1
#define EXPRKIND_RTFUNC 2
#define EXPRKIND_VALUES 3
#define EXPRKIND_LIMIT 4
#define EXPRKIND_APPINFO 5
#define EXPRKIND_TABLESAMPLE 6

extern Node* preprocess_expression(PlannerInfo* root, Node* expr, int kind);

void yyset_lineno(int line_number, yyscan_t yyscanner);

/* @Description: append space mark into the buffer
 * @inout buf: buffer
 * @in location: the current location
 * @in column_len: number of columns going to write
 */
static void append_space_mark(StringInfo buf, int location, int column_len)
{
    if (location == 0) {
        appendStringInfoCharMacro(buf, '(');
    } else if (column_len > 1) {
        appendStringInfoCharMacro(buf, ')');
        appendStringInfoCharMacro(buf, ' ');
        appendStringInfoCharMacro(buf, '(');
    } else {
        /* For case B, isfirst is always true */
        appendStringInfoCharMacro(buf, ' ');
    }
}

/* @Description: append actual value into the buffer
 * @inout buf: buffer
 * @in value: the Value node going to write
 * @in node: parent node if there is any
 */
static void append_value(StringInfo buf, Value* value, Node* node)
{
    if (IsA(node, Null)) {
        appendStringInfoString(buf, value->val.str);
    } else {
        appendStringInfoCharMacro(buf, '\'');
        appendStringInfoString(buf, value->val.str);
        appendStringInfoCharMacro(buf, '\'');
    }
}

#define HINT_NUM 16
#define HINT_KEYWORD_NUM 21

typedef struct {
    HintKeyword keyword;
    char* keyStr;
} KeywordPair;

const char* G_HINT_KEYWORD[HINT_KEYWORD_NUM] = {
    (char*) HINT_NESTLOOP,
    (char*) HINT_MERGEJOIN,
    (char*) HINT_HASHJOIN,
    (char*) HINT_LEADING,
    (char*) HINT_ROWS,
    (char*) HINT_BROADCAST,
    (char*) HINT_REDISTRIBUTE,
    (char*) HINT_BLOCKNAME,
    (char*) HINT_TABLESCAN,
    (char*) HINT_INDEXSCAN,
    (char*) HINT_INDEXONLYSCAN,
    (char*) HINT_SKEW,
    (char*) HINT_PRED_PUSH,
    (char*) HINT_PRED_PUSH_SAME_LEVEL,
    (char*) HINT_REWRITE,
    (char*) HINT_GATHER,
    (char*) HINT_NO_EXPAND,
    (char*) HINT_SET,
    (char*) HINT_CPLAN,
    (char*) HINT_GPLAN,
    (char*) HINT_NO_GPC,
};

/*
 * @Description: Describe hint keyword to string
 * @in keyword: hint keyword.
 */
static const char* KeywordDesc(HintKeyword keyword)
{
    const char* value = NULL;
    /* In case new tag is added within the old range. Keep the LFS as the newest keyword */
    Assert(HINT_KEYWORD_NO_GPC == HINT_KEYWORD_NUM - 1);
    if ((int)keyword >= HINT_KEYWORD_NUM || (int)keyword < 0) {
        elog(WARNING, "unrecognized keyword %d", (int)keyword);
    } else {
        value = G_HINT_KEYWORD[(int)keyword];
    }

    return value;
}

/*
 * @Description: Append relnames to buf.
 * @in relnames: Rel name list.
 * @out buf: String buf.
 */
static void relnamesToBuf(List* relnames, StringInfo buf)
{
    bool isfirst = true;
    char* relname = NULL;
    ListCell* lc = NULL;

    Assert(buf != NULL);
    foreach (lc, relnames) {
        Node* node = (Node*)lfirst(lc);
        if (IsA(node, String)) {
            Value* string_value = (Value*)node;
            relname = string_value->val.str;

            if (isfirst) {
                appendStringInfoString(buf, quote_identifier(relname));
                isfirst = false;
            } else {
                appendStringInfoCharMacro(buf, ' ');
                appendStringInfoString(buf, quote_identifier(relname));
            }
        } else if (IsA(node, List)) {
            if (isfirst) {
                appendStringInfoString(buf, "(");
                relnamesToBuf((List*)node, buf);
                appendStringInfoString(buf, ")");
                isfirst = false;
            } else {
                appendStringInfoCharMacro(buf, ' ');
                appendStringInfoString(buf, "(");
                relnamesToBuf((List*)node, buf);
                appendStringInfoString(buf, ")");
            }
        }
    }
}

/*
 * @Description: Collect predpush tables in the same level.
 * @in root: Root of the current SQL.
 * @out result: Relids of the tables which are predpush on.
 */
Relids predpush_candidates_same_level(PlannerInfo *root)
{
    HintState *hstate = root->parse->hintState;
    if (hstate == NULL)
        return NULL;

    if (hstate->predpush_hint == NULL)
        return NULL;

    ListCell *lc = NULL;
    Relids result = NULL;
    foreach (lc, hstate->predpush_hint) {
        PredpushHint *predpushHint = (PredpushHint*)lfirst(lc);
        result = bms_union(result, predpushHint->candidates);
    }

    return result;
}

/*
 * is_predpush_same_level_matched
 *    Check if the predpush samwe level is matched.
 * @param predpush same level hint, relids from baserel, param path info.
 * @param check_dest: don't check dest id when create paths.
 * @return true if matched.
 */
bool is_predpush_same_level_matched(PredpushSameLevelHint* hint, Relids relids, ParamPathInfo* ppi)
{
    if (ppi == NULL) {
        return false;
    }
    if (hint->dest_id == 0 || hint->candidates == NULL) {
        return false;
    }

    if (!bms_is_member(hint->dest_id, relids)) {
        return false;
    }

    if (!bms_equal(ppi->ppi_req_outer, hint->candidates)) {
        return false;
    }
    return true;
}

/*
 * @Description: get the prompts for no_gpc hint into subquery.
 * @in hint: subquery no_gpc hint.
 * @out buf: String buf.
 */
static void NoGPCHintDesc(NoGPCHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    appendStringInfo(buf, " %s", KeywordDesc(hint->base.hint_keyword));
}

/*
 * @Description: get the prompts for no_expand hint into subquery.
 * @in hint: subquery no_expand hint.
 * @out buf: String buf.
 */
static void NoExpandHintDesc(NoExpandHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    appendStringInfo(buf, " %s", KeywordDesc(hint->base.hint_keyword));
}

/*
 * @Description: get the prompts for plancache hint into subquery.
 * @in hint: subquery plancache hint.
 * @out buf: String buf.
 */
static void PlanCacheHintDesc(PlanCacheHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    appendStringInfo(buf, " %s", KeywordDesc(hint->base.hint_keyword));
}

/*
 * @Description: get the prompts for set-guc hint into subquery.
 * @in hint: set-guc hint.
 * @out buf: String buf.
 */
static void SetHintDesc(SetHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->name != NULL) {
        appendStringInfo(buf, "%s", hint->name);
    }
    
    if (hint->value != NULL) {
        appendStringInfo(buf, " %s", hint->value);
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: get the prompts for redicate pushdown into subquery.
 * @in hint: predicate pushdown hint.
 * @out buf: String buf.
 */
static void PredpushHintDesc(PredpushHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->candidates != NULL)
        appendStringInfo(buf, "(");

    relnamesToBuf(base_hint.relnames, buf);

    if (hint->dest_name != NULL) {
        appendStringInfo(buf, ", %s", hint->dest_name);
    }

    if (hint->candidates != NULL)
        appendStringInfo(buf, ")");

    appendStringInfoString(buf, ")");
}

/*
 * @Description: get the prompts for predicate pushdown same level.
 * @in hint: predicate pushdown same level hint.
 * @out buf: String buf.
 */
static void PredpushSameLevelHintDesc(PredpushSameLevelHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->candidates != NULL) {
        appendStringInfo(buf, "(");
    }

    relnamesToBuf(base_hint.relnames, buf);
    if (hint->dest_name != NULL) {
        appendStringInfo(buf, ", %s", hint->dest_name);
    }

    if (hint->candidates != NULL) {
        appendStringInfo(buf, ")");
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: get the prompts for rewrite hint into subquery.
 * @in hint: rewrite hint.
 * @out buf: String buf.
 */
static void RewriteHintDesc(RewriteHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    relnamesToBuf(hint->param_names, buf);

    appendStringInfoString(buf, ")");
}

/*
 * @Description: get the prompts for redicate pushdown into subquery.
 * @in hint: predicate pushdown hint.
 * @out buf: String buf.
 */
static void GatherHintDesc(GatherHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->source == HINT_GATHER_REL) {
        appendStringInfo(buf, "REL");
    } else if (hint->source == HINT_GATHER_JOIN) {
        appendStringInfo(buf, "JOIN");
    } else {
        appendStringInfo(buf, "ALL");
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe join hint to string.
 * @in hint: Join hint.
 * @out buf: String buf.
 */
static void JoinMethodHintDesc(JoinMethodHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);
    if (hint->negative) {
        appendStringInfo(buf, " %s", HINT_NO);
    }

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->inner_joinrelids != NULL) {
        appendStringInfo(buf, "(");
    }

    relnamesToBuf(base_hint.relnames, buf);

    if (hint->inner_joinrelids != NULL) {
        appendStringInfo(buf, ")");
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe leading hint to string.
 * @in hint: Leading hint.
 * @in buf: String buf.
 */
static void LeadingHintDesc(LeadingHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);
    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    if (hint->join_order_hint) {
        appendStringInfoString(buf, "(");
    }

    relnamesToBuf(base_hint.relnames, buf);

    if (hint->join_order_hint) {
        appendStringInfoString(buf, ")");
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe rows hint to string.
 * @in hint: Rows hint.
 * @in buf: String buf.
 */
static void RowsHintDesc(RowsHint* hint, StringInfo buf)
{
    Hint base_hint = hint->base;

    Assert(buf != NULL);
    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    relnamesToBuf(base_hint.relnames, buf);

    switch (hint->value_type) {
        case RVT_ABSOLUTE:
            appendStringInfoString(buf, " #");
            break;
        case RVT_ADD:
            appendStringInfoString(buf, " +");
            break;
        case RVT_SUB:
            appendStringInfoString(buf, " -");
            break;
        case RVT_MULTI:
            appendStringInfoString(buf, " *");
            break;
        default:
            break;
    }

    if (hint->rows_str != NULL) {
        appendStringInfo(buf, " %s", hint->rows_str);
    } else {
        appendStringInfo(buf, " %.lf", hint->rows);
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe stream hint to string.
 * @in hint: Stream hint.
 * @in buf: String buf.
 */
static void StreamHintDesc(StreamHint* hint, StringInfo buf)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
    Hint base_hint = hint->base;

    Assert(buf != NULL);
    if (hint->negative) {
        appendStringInfo(buf, " %s", HINT_NO);
    }

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));
    relnamesToBuf(base_hint.relnames, buf);
    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe blockname hint to string.
 * @in hint: block hint.
 * @in buf: String buf.
 */
static void BlockNameHintDesc(BlockNameHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    Hint base_hint = hint->base;
    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));
    relnamesToBuf(base_hint.relnames, buf);
    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe scan hint to string.
 * @in hint: Scan hint.
 * @in buf: String buf.
 */
static void ScanMethodHintDesc(ScanMethodHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    Hint base_hint = hint->base;
    if (hint->negative) {
        appendStringInfo(buf, " %s", HINT_NO);
    }

    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));
    relnamesToBuf(base_hint.relnames, buf);

    if (hint->indexlist) {
        appendStringInfoCharMacro(buf, ' ');
        relnamesToBuf(hint->indexlist, buf);
    }

    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe skew hint to string.
 * @in hint: Skew hint.
 * @in buf: String buf.
 */
static void SkewHintDesc(SkewHint* hint, StringInfo buf)
{
    Assert(buf != NULL);

    Hint base_hint = hint->base;
    appendStringInfo(buf, " %s(", KeywordDesc(hint->base.hint_keyword));

    /* Skew hint formats are:
     *  case A: skew(t (c1) (v1))
     *  case B: skew(t (c1) (v1 v2 v3 ...))
     *  case C: skew(t (c1 c2) (v1 v2))
     *  case D: skew(t (c1 c2) ((v1 v2) (v3 v4) (v5 v6) ...))
     *  case F: skew((t1 t3) (c1 c2) ((v1 v2) (v3 v4) (v5 v6) ...))
     */
    if (list_length(hint->base.relnames) == 1) {
        relnamesToBuf(base_hint.relnames, buf);
    } else {
        appendStringInfoCharMacro(buf, '(');
        relnamesToBuf(base_hint.relnames, buf);
        appendStringInfoString(buf, ")");
    }

    if (hint->column_list) {
        appendStringInfoCharMacro(buf, ' ');
        appendStringInfoCharMacro(buf, '(');
        relnamesToBuf(hint->column_list, buf);
        appendStringInfoString(buf, ")");
    }

    if (hint->value_list) {
        int c_length = list_length(hint->column_list);
        int v_length = list_length(hint->value_list);
        ListCell* lc = NULL;
        bool isfirst = true;
        int location = 0;

        appendStringInfoCharMacro(buf, ' ');

        /* For case D */
        if (c_length != 1 && v_length > c_length) {
            appendStringInfoCharMacro(buf, '(');
        }

        foreach (lc, hint->value_list) {
            if (location % c_length == 0) {
                isfirst = true;
            } else {
                isfirst = false;
            }

            /* For null value. */
            Node* node = (Node*)lfirst(lc);
            Value* string_value = (Value*)node;
            char* null_val = "NULL";
            string_value->val.str = IsA(node, Null) ? null_val : string_value->val.str;

            switch (nodeTag(node)) {
                case T_Null:
                case T_BitString:
                case T_String:
                case T_Float: {
                    if (isfirst) {
                        append_space_mark(buf, location, c_length);
                        append_value(buf, string_value, node);
                    } else {
                        appendStringInfoCharMacro(buf, ' ');
                        append_value(buf, string_value, node);
                    }
                    break;
                }
                case T_Integer: {
                    if (isfirst) {
                        append_space_mark(buf, location, c_length);
                        appendStringInfo(buf, "%ld", string_value->val.ival);
                    } else {
                        appendStringInfoCharMacro(buf, ' ');
                        appendStringInfo(buf, "%ld", string_value->val.ival);
                    }
                    break;
                }
                default:
                    break;
            }
            location++;
        }
        if (c_length != 1 && v_length > c_length) {
            appendStringInfoCharMacro(buf, ')');
        }

        appendStringInfoCharMacro(buf, ')');
    }
    appendStringInfoString(buf, ")");
}

/*
 * @Description: Describe hint to string.
 * @in hint: Hint.
 * @in buf: String buf.
 */
char* descHint(Hint* hint)
{
    StringInfoData str;
    initStringInfo(&str);

    switch (hint->type) {
        case T_JoinMethodHint:
            JoinMethodHintDesc((JoinMethodHint*)hint, &str);
            break;
        case T_LeadingHint:
            LeadingHintDesc((LeadingHint*)hint, &str);
            break;
        case T_RowsHint:
            RowsHintDesc((RowsHint*)hint, &str);
            break;
        case T_StreamHint:
            StreamHintDesc((StreamHint*)hint, &str);
            break;
        case T_BlockNameHint:
            BlockNameHintDesc((BlockNameHint*)hint, &str);
            break;
        case T_ScanMethodHint:
            ScanMethodHintDesc((ScanMethodHint*)hint, &str);
            break;
        case T_SkewHint:
            SkewHintDesc((SkewHint*)hint, &str);
            break;
        case T_PredpushHint:
            PredpushHintDesc((PredpushHint*)hint, &str);
            break;
        case T_PredpushSameLevelHint:
            PredpushSameLevelHintDesc((PredpushSameLevelHint*)hint, &str);
            break;
        case T_RewriteHint:
            RewriteHintDesc((RewriteHint*)hint, &str);
            break;
        case T_GatherHint:
            GatherHintDesc((GatherHint*)hint, &str);
            break;
        case T_SetHint:
            SetHintDesc((SetHint*)hint, &str);
            break;
        case T_PlanCacheHint:
            PlanCacheHintDesc((PlanCacheHint*)hint, &str);
            break;
        case T_NoExpandHint:
            NoExpandHintDesc((NoExpandHint*)hint, &str);
            break;
        case T_NoGPCHint:
            NoGPCHintDesc((NoGPCHint*)hint, &str);
            break;
        default:
            break;
    }

    return str.data;
}

/*
 * @Description: Append unused hint string into buf.
 * @in hint_list: Hint list.
 * @out hint_buf: Not used hint buf.
 */
static void find_unused_hint_to_buf(List* hint_list, StringInfo hint_buf)
{
    ListCell* lc = NULL;

    foreach (lc, hint_list) {
        Hint* hint = (Hint*)lfirst(lc);

        if (hint->state == HINT_STATE_NOTUSED) {
            appendStringInfo(hint_buf, "%s", descHint(hint));
        }
    }
}

/*
 * @Description: Append used or not used hint string into buf.
 * @in hstate: Hint State.
 * @out buf: Keep hint string.
 */
void desc_hint_in_state(PlannerInfo* root, HintState* hstate)
{
    StringInfoData str_buf;
    initStringInfo(&str_buf);

    /* Append already used hint to buf, not used hint to str_buf. */
    find_unused_hint_to_buf(hstate->join_hint, &str_buf);
    find_unused_hint_to_buf(hstate->row_hint, &str_buf);
    find_unused_hint_to_buf(hstate->rewrite_hint, &str_buf);
    find_unused_hint_to_buf(hstate->gather_hint, &str_buf);
    find_unused_hint_to_buf(hstate->stream_hint, &str_buf);
    find_unused_hint_to_buf(hstate->scan_hint, &str_buf);
    find_unused_hint_to_buf(hstate->block_name_hint, &str_buf);
    find_unused_hint_to_buf(hstate->set_hint, &str_buf);
    find_unused_hint_to_buf(hstate->no_gpc_hint, &str_buf);
    find_unused_hint_to_buf(hstate->predpush_same_level_hint, &str_buf);

    /* for skew hint */
    ListCell* lc = NULL;
    foreach (lc, hstate->skew_hint) {
        SkewHintTransf* skew_hint_transf = (SkewHintTransf*)lfirst(lc);
        if (skew_hint_transf->before->base.state == HINT_STATE_NOTUSED) {
            appendStringInfo(&str_buf, "%s", descHint((Hint*)skew_hint_transf->before));
        }
    }

    if (strlen(str_buf.data) > 0) {
        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "unused hint:%s", str_buf.data);
        root->glob->hint_warning = lappend(root->glob->hint_warning, makeString(str.data));
    }

    pfree_ext(str_buf.data);
}

/*
 * @Description: Delete predicate pushdown, free memory.
 * @in hint: predicate pushdown hint.
 */
static void PredpushHintDelete(PredpushHint* hint)
{
    if (hint == NULL)
        return;

    HINT_FREE_RELNAMES(hint);

    bms_free(hint->candidates);
    pfree_ext(hint);
}

/*
 * @Description: Delete predicate pushdown same level, free memory.
 * @in hint: predicate pushdown same level hint.
 */
static void PredpushSameLevelHintDelete(PredpushSameLevelHint* hint)
{
    if (hint == NULL)
        return;

    HINT_FREE_RELNAMES(hint);

    bms_free(hint->candidates);
    pfree_ext(hint);
}

/*
 * @Description: Delete join method hint, free memory.
 * @in hint: Join hint.
 */
static void JoinMethodHintDelete(JoinMethodHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    bms_free(hint->joinrelids);
    bms_free(hint->inner_joinrelids);
    pfree_ext(hint);
}

/*
 * @Description: Delete leading hint.
 * @in hint: Leading hint.
 */
static void LeadingHintDelete(LeadingHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);
    
    pfree_ext(hint);
}

/*
 * @Description: Delete rows hint.
 * @in hint: Rows Hint.
 */
static void RowsHintDelete(RowsHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    bms_free(hint->joinrelids);
    pfree_ext(hint);
}

/*
 * @Description: Delete rows hint.
 * @in hint: Rows Hint.
 */
static void RewriteHintDelete(RewriteHint* hint)
{
    if (hint == NULL)
        return;

    HINT_FREE_RELNAMES(hint);

    pfree_ext(hint);
}

/*
 * @Description: Delete rows hint.
 * @in hint: Rows Hint.
 */
static void GatherHintDelete(GatherHint* hint)
{
    if (hint == NULL)
        return;
    pfree_ext(hint);
}

/*
 * @Description: Delete stream hint.
 * @in hint: Stream hint.
 */
static void StreamHintDelete(StreamHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    bms_free(hint->joinrelids);
    pfree_ext(hint);
}

/*
 * @Description: Delete BlockName hint.
 * @in hint: BlockName hint.
 */
static void BlockNameHintDelete(BlockNameHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    pfree_ext(hint);
}

/*
 * @Description: Delete scan hint.
 * @in hint: scan hint.
 */
static void ScanMethodHintDelete(ScanMethodHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    if (hint->indexlist) {
        list_free_deep(hint->indexlist);
    }

    pfree_ext(hint);
}

/*
 * @Description: Delete skew hint.
 * @in hint: skew hint.
 */
static void SkewHintDelete(SkewHint* hint)
{
    if (hint == NULL) {
        return;
    }

    HINT_FREE_RELNAMES(hint);

    if (hint->column_list) {
        list_free_deep(hint->column_list);
    }

    if (hint->value_list) {
        list_free_deep(hint->value_list);
    }

    pfree_ext(hint);
    hint = NULL;
}

/*
 * @Description: Delete skew hint.
 * @in hint: skew hint.
 */
static void SkewHintTransfDelete(SkewHintTransf* hint)
{
    if (hint == NULL) {
        return;
    }

    if (hint->before) {
        SkewHintDelete(hint->before);
        hint->before = NULL;
    }

    if (hint->rel_info_list) {
        list_free_deep(hint->rel_info_list);
        hint->rel_info_list = NIL;
    }

    if (hint->column_info_list) {
        list_free_deep(hint->column_info_list);
        hint->column_info_list = NIL;
    }

    if (hint->value_info_list) {
        list_free_deep(hint->value_info_list);
        hint->value_info_list = NIL;
    }

    pfree_ext(hint);
}

/*
 * @Description: Delete hint, call different delete function according to type.
 * @in hint: Deleted hint.
 */
void hintDelete(Hint* hint)
{
    Assert(hint != NULL);

    switch (nodeTag(hint)) {
        case T_JoinMethodHint:
            JoinMethodHintDelete((JoinMethodHint*)hint);
            break;
        case T_LeadingHint:
            LeadingHintDelete((LeadingHint*)hint);
            break;
        case T_RowsHint:
            RowsHintDelete((RowsHint*)hint);
            break;
        case T_StreamHint:
            StreamHintDelete((StreamHint*)hint);
            break;
        case T_BlockNameHint:
            BlockNameHintDelete((BlockNameHint*)hint);
            break;
        case T_ScanMethodHint:
            ScanMethodHintDelete((ScanMethodHint*)hint);
            break;
        case T_SkewHint:
            SkewHintDelete((SkewHint*)hint);
            break;
        case T_PredpushHint:
            PredpushHintDelete((PredpushHint*)hint);
            break;
        case T_PredpushSameLevelHint:
            PredpushSameLevelHintDelete((PredpushSameLevelHint*)hint);
            break;
        case T_RewriteHint:
            RewriteHintDelete((RewriteHint*)hint);
            break;
        case T_GatherHint:
            GatherHintDelete((GatherHint*)hint);
            break;
        default:
            elog(WARNING, "unrecognized hint method: %d", (int)nodeTag(hint));
            break;
    }
}

/*
 * @Descroption: Delete HintState struct.
 * @void return
 */
void HintStateDelete(HintState* hintState)
{
    if (hintState == NULL) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, hintState->join_hint) {
        JoinMethodHint* hint = (JoinMethodHint*)lfirst(lc);
        JoinMethodHintDelete(hint);
    }

    foreach (lc, hintState->leading_hint) {
        LeadingHint* hint = (LeadingHint*)lfirst(lc);
        LeadingHintDelete(hint);
    }

    foreach (lc, hintState->row_hint) {
        RowsHint* hint = (RowsHint*)lfirst(lc);
        RowsHintDelete(hint);
    }

    foreach (lc, hintState->rewrite_hint) {
        RewriteHint* hint = (RewriteHint*)lfirst(lc);
        RewriteHintDelete(hint);
    }

    foreach (lc, hintState->gather_hint) {
        GatherHint* hint = (GatherHint*)lfirst(lc);
        GatherHintDelete(hint);
    }

    foreach (lc, hintState->stream_hint) {
        StreamHint* hint = (StreamHint*)lfirst(lc);
        StreamHintDelete(hint);
    }

    foreach (lc, hintState->block_name_hint) {
        BlockNameHint* hint = (BlockNameHint*)lfirst(lc);
        BlockNameHintDelete(hint);
    }

    foreach (lc, hintState->scan_hint) {
        ScanMethodHint* hint = (ScanMethodHint*)lfirst(lc);
        ScanMethodHintDelete(hint);
    }

    foreach (lc, hintState->skew_hint) {
        SkewHint* hint = (SkewHint*)lfirst(lc);
        SkewHintDelete(hint);
    }

    foreach (lc, hintState->predpush_hint) {
        PredpushHint* hint = (PredpushHint*)lfirst(lc);
        PredpushHintDelete(hint);
    }

    foreach (lc, hintState->predpush_same_level_hint) {
        PredpushSameLevelHint* hint = (PredpushSameLevelHint*)lfirst(lc);
        PredpushSameLevelHintDelete(hint);
    }
}

/*
 * @Descroption: Create HintState struct.
 * @return: HintState struct.
 */
HintState* HintStateCreate()
{
    HintState* hstate = NULL;

    hstate = makeNode(HintState);

    hstate->nall_hints = 0;
    hstate->join_hint = NIL;
    hstate->leading_hint = NIL;
    hstate->row_hint = NIL;
    hstate->stream_hint = NIL;
    hstate->scan_hint = NIL;
    hstate->skew_hint = NIL;
    hstate->hint_warning = NIL;
    hstate->multi_node_hint = false;
    hstate->predpush_hint = NIL;
    hstate->predpush_same_level_hint = NIL;
    hstate->rewrite_hint = NIL;
    hstate->gather_hint = NIL;
    hstate->set_hint = NIL;
    hstate->cache_plan_hint = NIL;
    hstate->no_expand_hint = NIL;

    return hstate;
}

/*
 * @Description: Get hints from the comment in client-supplied query string.
 * @in comment_str: Comment string.
 * @return: Hint string.
 */
static char* get_hints_from_comment(const char* comment_str)
{
    char* head = NULL;
    int len;
    int comment_len = strlen(comment_str);
    int hint_start_len = strlen(HINT_START);
    int start_position = 0;
    int end_position = 0;
    errno_t rc;

    /* extract query head comment, hint string start with "\*+" */
    if (strncmp(comment_str, HINT_START, hint_start_len) != 0) {
        return NULL;
    }

    /* Find first is not space character. */
    for (start_position = hint_start_len; start_position < comment_len; start_position++) {
        if (comment_str[start_position] == '\n' || (!isspace(comment_str[start_position]))) {
            break;
        }
    }

    /* Find comment termination position. */
    for (end_position = comment_len - 1; end_position >= 0; end_position--) {
        if (comment_str[end_position] == '*') {
            break;
        }
    }

    /* Make a copy of hint. */
    len = end_position - start_position;

    if (len <= 0) {
        return NULL;
    }

    head = (char*)palloc(len + 1);
    rc = memcpy_s(head, len, comment_str + start_position, len);
    securec_check(rc, "\0", "\0");
    head[len] = '\0';

    return head;
}

/*
 * @Description: Delete repeated BlockName hint. If have more than one,
 * they will all be discarded. BlockName hint only can have one in a query block.
 * @in hstate: Hint state.
 */
static void drop_duplicate_blockname_hint(HintState* hstate)
{
    Assert(list_length(hstate->block_name_hint) > 1);
    ListCell* lc = list_head(hstate->block_name_hint);

    foreach (lc, hstate->block_name_hint) {
        if (lc == list_head(hstate->block_name_hint)) {
            continue;
        }

        /*
         * treat all but first one as duplicate hint, as there should
         * be only one block hint
         */
        Hint* hint = (Hint*)lfirst(lc);
        hint->state = HINT_STATE_DUPLICATION;
    }

    hstate->block_name_hint = delete_invalid_hint(NULL, hstate, hstate->block_name_hint);
}

static List* keep_last_hint_cell(List* hintList)
{
    if(list_length(hintList) <= 1) {
        return hintList;
    }
    Node* node = (Node*)copyObject(llast(hintList));
    list_free_deep(hintList);
    return lappend(NIL, node);
}

static void AddJoinHint(HintState* hstate, Hint* hint)
{
    hstate->join_hint = lappend(hstate->join_hint, hint);
}

static void AddLeadingHint(HintState* hstate, Hint* hint)
{
    hstate->leading_hint = lappend(hstate->leading_hint, hint);
}

static void AddRowsHint(HintState* hstate, Hint* hint)
{
    hstate->row_hint = lappend(hstate->row_hint, hint);
}

static void AddStreamHint(HintState* hstate, Hint* hint)
{
    hstate->stream_hint = lappend(hstate->stream_hint, hint);
}

static void AddBlockNameHint(HintState* hstate, Hint* hint)
{
    hstate->block_name_hint = lappend(hstate->block_name_hint, hint);
}

static void AddScanMethodHint(HintState* hstate, Hint* hint)
{
    hstate->scan_hint = lappend(hstate->scan_hint, hint);
}

static void AddSkewHint(HintState* hstate, Hint* hint)
{
    hstate->skew_hint = lappend(hstate->skew_hint, hint);
}

static void AddMultiNodeHint(HintState* hstate, Hint* hint)
{
    hstate->multi_node_hint = true;
}

static void AddPredpushHint(HintState* hstate, Hint* hint)
{
    hstate->predpush_hint = lappend(hstate->predpush_hint, hint);
}

static void AddPredpushSameLevelHint(HintState* hstate, Hint* hint)
{
    hstate->predpush_same_level_hint = lappend(hstate->predpush_same_level_hint, hint);
}

static void AddRewriteHint(HintState* hstate, Hint* hint)
{
    hstate->rewrite_hint = lappend(hstate->rewrite_hint, hint);
}

static void AddGatherHint(HintState* hstate, Hint* hint)
{
    hstate->gather_hint = lappend(hstate->gather_hint, hint);
}

static void AddSetHint(HintState* hstate, Hint* hint)
{
    hstate->set_hint = lappend(hstate->set_hint, hint);
}

static void AddPlanCacheHint(HintState* hstate, Hint* hint)
{
    hstate->cache_plan_hint = lappend(hstate->cache_plan_hint, hint);
}

static void AddNoExpandHint(HintState* hstate, Hint* hint)
{
    /* only keep one no_expand hint for each subquery */
    if (list_length(hstate->no_expand_hint) == 0) {
        hstate->no_expand_hint = lappend(hstate->no_expand_hint, hint);
    }
}

static void AddNoGPCHint(HintState* hstate, Hint* hint)
{
    /* only keep one no_gpc hint for each subquery */
    if (list_length(hstate->no_gpc_hint) == 0) {
        hstate->no_gpc_hint = lappend(hstate->no_gpc_hint, hint);
    }
}

typedef void (*AddHintFunc)(HintState*, Hint*);

const AddHintFunc G_HINT_CREATOR[HINT_NUM] = {
    AddJoinHint,
    AddLeadingHint,
    AddRowsHint,
    AddStreamHint,
    AddBlockNameHint,
    AddScanMethodHint,
    AddMultiNodeHint,
    AddPredpushHint,
    AddPredpushSameLevelHint,
    AddSkewHint,
    AddRewriteHint,
    AddGatherHint,
    AddSetHint,
    AddPlanCacheHint,
    AddNoExpandHint,
    AddNoGPCHint,
};

/*
 * @Description: Generate hint struct according to hint str.
 * @in hints: Hint string.
 * @return: Hintstate struct.
 */
HintState* create_hintstate(const char* hints)
{
    if (hints == NULL) {
        return NULL;
    }

    char* hint_str = NULL;
    HintState* hstate = NULL;

    hint_str = get_hints_from_comment(hints);
    if (hint_str == NULL) {
        return NULL;
    }

    /* Initilized plan hint variable, which will be set in hint parser */
    u_sess->parser_cxt.hint_list = u_sess->parser_cxt.hint_warning = NIL;

    yyscan_t yyscanner;
    hint_yy_extra_type yyextra;

    /* initialize the flex scanner */
    yyscanner = hint_scanner_init(hint_str, &yyextra);

    yyset_lineno(1, yyscanner);

    /* we will go on whether yyparse is successful or not. */
    (void)yyparse(yyscanner);

    hint_scanner_destroy(yyscanner);

    hstate = HintStateCreate();

    if (u_sess->parser_cxt.hint_list != NULL) {
        ListCell* lc = NULL;
        int firstHintTag = T_JoinMethodHint; /* Do not add hint tag before JoinMethodHint. */
        int lastHintTag = T_NoGPCHint; /* Keep this as the last hint tag in nodes.h. */

        foreach (lc, u_sess->parser_cxt.hint_list) {
            Hint* hint = (Hint*)lfirst(lc);
            if (hint == NULL) {
                continue;
            }
            /* In case new tag is added within the old range. Keep the LFS as the newest keyword */
            Assert(lastHintTag == HINT_NUM - 1 + firstHintTag);
            if (nodeTag(hint) < firstHintTag || nodeTag(hint) > lastHintTag) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("[Internal Error]: Invalid hint type %d", (int)nodeTag(hint)),
                         errhint("Do not add new hint tag between %d and %d.", firstHintTag, lastHintTag))));
            }
            if (G_HINT_CREATOR[nodeTag(hint) - firstHintTag] == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        (errmsg("[Internal Error]: Hint add function not initialized %d", (int)nodeTag(hint)))));
            }
            G_HINT_CREATOR[nodeTag(hint) - firstHintTag](hstate, hint);
            hstate->nall_hints++;
        }

        list_free(u_sess->parser_cxt.hint_list);
        u_sess->parser_cxt.hint_list = NIL;
    }
    hstate->hint_warning = u_sess->parser_cxt.hint_warning;
    u_sess->parser_cxt.hint_warning = NIL;

    /* When nothing specified a hint, we free HintState and returns NULL. */
    if (hstate->nall_hints == 0 && hstate->hint_warning == NIL) {
        pfree_ext(hstate);
        hstate = NULL;
    } else {
        /* Delete repeated BlockName hint. */
        if (list_length(hstate->block_name_hint) > 1) {
            drop_duplicate_blockname_hint(hstate);
        }
        /* Only keep the last cplan/gplanhint */
        hstate->cache_plan_hint = keep_last_hint_cell(hstate->cache_plan_hint);
    }

    pfree_ext(hint_str);
    return hstate;
}

/*
 * @Description: Return index of relation which matches given aliasname.
 * @in pstate: State information used during parse analysis.
 * @in aliasname: rel name.
 * @return: if not found return 0. If same aliasname was used multiple times in a query,
 * return -1 else return relid.
 */
static int find_relid_aliasname(Query* parse, const char* aliasname, bool find_in_rtable)
{
    ListCell* cell = NULL;
    int i = 1;
    int found = NOTFOUNDRELNAME;
    Relids relids = find_in_rtable ? NULL : get_relids_in_jointree((Node*)parse->jointree, false);

    foreach (cell, parse->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(cell);

        /* skip the relation that doesn't exist in jointree, except skew hint */
        if ((find_in_rtable || bms_is_member(i, relids)) &&
            strncmp(aliasname, rte->eref->aliasname, strlen(aliasname) + 1) == 0) {
            if (found == 0) {
                found = i;
            } else {
                return AMBIGUOUSRELNAME;
            }
        }

        i++;
    }

    return found;
}

/*
 * @Description: Create relnames's relids.
 * @in root: query level info.
 * @in pstate: State information used during parse analysis.
 * @in relnames: relname list.
 * @return: relids.
 */
static Relids create_bms_of_relids(PlannerInfo* root, Query* parse, Hint* hint, List* relnamelist, Relids currelids)
{
    int relid;
    Relids relids = currelids;
    List* relnames = (relnamelist == NIL) ? hint->relnames : relnamelist;

    ListCell* lc = NULL;

    if (IsA(hint, JoinMethodHint) && list_length(relnames) == 1) {
        append_warning_to_list(root, hint, "Error hint:%s requires at least two relations.", hint_string);
        return NULL;
    }

    /* For skew hint we will found relation from parse`s rtable. */
    bool find_in_rtable = false;
    if (IsA(hint, SkewHint) || IsA(hint, PredpushHint) || IsA(hint, PredpushSameLevelHint)) {
        find_in_rtable = true;
	}

    foreach (lc, relnames) {
        Node* item = (Node*)lfirst(lc);

        if (IsA(item, String)) {
            Value* string_value = (Value*)lfirst(lc);
            char* relname = string_value->val.str;

            relid = find_relid_aliasname(parse, relname, find_in_rtable);
            if (relid == NOTFOUNDRELNAME) {
                append_warning_to_list(
                    root, hint, "Error hint:%s, relation name \"%s\" is not found.", hint_string, relname);
                if (currelids == NULL) {
                    bms_free(relids);
                }
                return NULL;
            } else if (AMBIGUOUSRELNAME == relid) {
                append_warning_to_list(
                    root, hint, "Error hint:%s, relation name \"%s\" is ambiguous.", hint_string, relname);
                if (currelids == NULL) {
                    bms_free(relids);
                }
                return NULL;
            } else if (bms_is_member(relid, relids)) {
                append_warning_to_list(
                    root, hint, "Error hint:%s, relation name \"%s\" is duplicated.", hint_string, relname);
                if (currelids == NULL) {
                    bms_free(relids);
                }
                return NULL;
            }
            relids = bms_add_member(relids, relid);
        } else {
            Assert(IsA(item, List));
            Relids subrelids = create_bms_of_relids(root, parse, hint, (List*)item, relids);
            if (subrelids != NULL) {
                relids = subrelids;
            } else if (relids != NULL) {
                if (currelids == NULL) {
                    bms_free(relids);
                }
                relids = NULL;
            }
        }
    }

    return relids;
}

/*
 * @Description: Set accurate relids.
 * @in pstate: Query struct.
 * @in l: Hint list.
 */
static List* set_hint_relids(PlannerInfo* root, Query* parse, List* l)
{
    ListCell* lc = list_head(l);
    ListCell* pre = NULL;
    ListCell* next = NULL;

    /*
     * Create bitmap of relids from alias names for each join method hint.
     * Bitmaps are more handy than strings in join searching.
     */
    while (lc != NULL) {
        Relids relids = NULL;

        next = lnext(lc);

        Hint* hint = (Hint*)lfirst(lc);

        relids = create_bms_of_relids(root, parse, hint);
        if (relids != NULL) {
            switch (nodeTag(hint)) {
                case T_JoinMethodHint:
                    ((JoinMethodHint*)hint)->joinrelids = relids;
                    break;
                case T_RowsHint:
                    ((RowsHint*)hint)->joinrelids = relids;
                    break;
                case T_StreamHint:
                    ((StreamHint*)hint)->joinrelids = relids;
                    break;
                case T_ScanMethodHint:
                    ((ScanMethodHint*)hint)->relid = relids;
                    break;
                case T_SkewHint:
                    ((SkewHint*)hint)->relid = relids;
                    break;
                case T_PredpushHint:
                    ((PredpushHint*)hint)->candidates = relids;
                    break;
                case T_PredpushSameLevelHint:
                    ((PredpushSameLevelHint*)hint)->candidates = relids;
                    break;
                default:
                    break;
            }
            pre = lc;
        } else {
            if (!IsA(hint, PredpushHint) || !((PredpushHint*)hint)->negative) {
                l = list_delete_cell(l, lc, pre);
            }
        }

        lc = next;
    }

    return l;
}

/*
 * @Description: Find scan hint according to relid and hint_key_word.
 * @in hstate: HintState.
 * @in relid: Relids.
 * @in keyWord: Hint key word.
 * @return: Scan hint or Null.
 */
ScanMethodHint* find_scan_hint(HintState* hstate, Relids relid, HintKeyword keyWord)
{
    if (hstate == NULL) {
        return NULL;
    }

    ListCell* l = NULL;

    foreach (l, hstate->scan_hint) {
        ScanMethodHint* scanHint = (ScanMethodHint*)lfirst(l);

        if (scanHint->base.hint_keyword == keyWord && bms_equal(scanHint->relid, relid)) {
            return scanHint;
        }
    }

    return NULL;
}

/*
 * @Descriptoion: Find join hint according to relid and join hint key word.
 * @in hstate: HintState.
 * @in joinrelids: Join relids.
 * @in innerrelids: Inner relids.
 * @in keyWord: Join hint key word.
 * @in leading: if leading hint also considered, default true.
 * @return: Join hint or NULL.
 */
List* find_specific_join_hint(
    HintState* hstate, Relids joinrelids, Relids innerrelids, HintKeyword keyWord, bool leading)
{
    if (hstate == NULL) {
        return NIL;
    }

    JoinMethodHint* hint = NULL;
    ListCell* l = NULL;
    List* hint_list = NIL;

    foreach (l, hstate->join_hint) {
        hint = (JoinMethodHint*)lfirst(l);

        HintKeyword hintKeyword = hint->base.hint_keyword;

        if (bms_equal(joinrelids, hint->joinrelids)) {
            /*
             * Join type should be same, when be called in add_path.
             * Leading hint always should be returned, it need not care join type.
             */
            if (hintKeyword == keyWord || (hintKeyword == HINT_KEYWORD_LEADING && leading)) {
                /* If hint's inner rel exist, here need decide inner rel if same. */
                if (hint->inner_joinrelids) {
                    if (bms_equal(hint->inner_joinrelids, innerrelids)) {
                        hint_list = lappend(hint_list, hint);
                    }
                } else {
                    hint_list = lappend(hint_list, hint);
                }
            }
        }
    }

    return hint_list;
}

/*
 * @Descriptoion: Find scan hint according to relid and scan hint key word.
 * @in hstate: HintState.
 * @in relids: Scan relids.
 * @in keyWord: Scan hint key word.
 * @return: Join hint or NULL.
 */
List* find_specific_scan_hint(HintState* hstate, Relids relids, HintKeyword keyWord)
{
    if (hstate == NULL) {
        return NIL;
    }

    ScanMethodHint* hint = NULL;
    ListCell* l = NULL;
    List* hint_list = NIL;

    foreach (l, hstate->scan_hint) {
        hint = (ScanMethodHint*)lfirst(l);

        HintKeyword hintKeyword = hint->base.hint_keyword;

        if (bms_equal(relids, hint->relid)) {
            if (hintKeyword == keyWord) {
                hint_list = lappend(hint_list, hint);
            }
        }
    }

    return hint_list;
}

/*
 * @Description: According to leading hint, generate join hint.
 * @in hstate: Hint state.
 * @in join_relids: Join relids.
 * @in inner_relids: Inner relids.
 * @in relname_list: Rel name string.
 */
static void leading_to_join_hint(HintState* hstate, Relids join_relids, Relids inner_relids, List* relname_list)
{
    JoinMethodHint* hint = makeNode(JoinMethodHint);
    hint->base.hint_keyword = HINT_KEYWORD_LEADING;
    hint->base.state = HINT_STATE_NOTUSED;
    hint->base.relnames = (List*)copyObject(relname_list);
    hint->joinrelids = bms_copy(join_relids);
    hint->inner_joinrelids = inner_relids;

    hstate->join_hint = lappend(hstate->join_hint, hint);
}

/*
 * @Description: Build join hint according to outer inner rels from leading hint.
 * @in parse: Query struct.
 * @in hstate: hint stats of current query level
 * @in relnames: Leading hint relnames.
 * @in join_order_hint: if join order is hinted
 * @return: Join relids.
 */
static Relids OuterInnerJoinCreate(Query* parse, HintState* hstate, List* relnames, bool join_order_hint)
{
    Relids innerrel_ids = NULL;
    Relids joinrel_ids = NULL;
    Relids rel_ids = NULL;
    ListCell* lc = NULL;
    List* tmp_rel_list = NIL;

    foreach (lc, relnames) {
        Node* item = (Node*)lfirst(lc);

        if (IsA(item, String)) {
            rel_ids = bms_make_singleton(find_relid_aliasname(parse, strVal(item)));
        } else if (IsA(item, List)) {
            /* only apply join order in the outer level, so set to false in inner level */
            rel_ids = OuterInnerJoinCreate(parse, hstate, (List*)item, false);
        }
        joinrel_ids = bms_add_members(joinrel_ids, rel_ids);
        tmp_rel_list = lappend(tmp_rel_list, item);

        /* if join order is required, we should add each join pair in the outer level */
        if (join_order_hint && lc != list_head(relnames)) {
            innerrel_ids = rel_ids;
            leading_to_join_hint(hstate, joinrel_ids, innerrel_ids, tmp_rel_list);
        }
    }

    /* if join order is not required, just add whole rel as a overall hint */
    if (!join_order_hint) {
        leading_to_join_hint(hstate, joinrel_ids, innerrel_ids, relnames);
    }

    list_free(tmp_rel_list);

    return joinrel_ids;
}

/*
 * @Description: Transform leading hint.
 * @in pstate: Query struct.
 * @in hstate: Hint state.
 */
static void transform_leading_hint(PlannerInfo* root, Query* parse, HintState* hstate)
{
    LeadingHint* leading_hint = NULL;
    ListCell* lc = NULL;

    foreach (lc, hstate->leading_hint) {
        leading_hint = (LeadingHint*)lfirst(lc);

        Relids joinrelids = NULL;

        joinrelids = create_bms_of_relids(root, parse, &(leading_hint->base));
        /* Leading hint relation is right. */
        if (joinrelids) {
            /* This leading have not specify inner or outer. */
            (void)OuterInnerJoinCreate(parse, hstate, leading_hint->base.relnames, leading_hint->join_order_hint);
            bms_free(joinrelids);
        }

        /*
         * Leading already be converted and keep in join list. we can delete
         * leading hint list.
         */
        hintDelete((Hint*)leading_hint);
    }

    list_free(hstate->leading_hint);
    hstate->leading_hint = NIL;
}

/*
 * @Description: Delete duplicate and error hint from list.
 * @in list: Hint list.
 * @return: New list.
 */
static List* delete_invalid_hint(PlannerInfo* root, HintState* hint, List* list)
{
    ListCell* pre = NULL;
    ListCell* next = NULL;

    StringInfoData str_buf, warning_buf;
    initStringInfo(&str_buf);
    initStringInfo(&warning_buf);

    ListCell* lc = list_head(list);

    while (lc != NULL) {
        next = lnext(lc);

        Hint* hint = (Hint*)lfirst(lc);

        if (hint->state == HINT_STATE_DUPLICATION) {
            appendStringInfo(&str_buf, "%s", descHint(hint));

            hintDelete(hint);
            list = list_delete_cell(list, lc, pre);
        } else {
            pre = lc;
        }

        lc = next;
    }

    appendStringInfo(&warning_buf, "Duplicated or conflict hint:%s, will be discarded.", str_buf.data);

    /*
     * For blockname hint duplication, we detect it in parser phase, so append the info in hint
     * state, or append the info to planner info
     */
    if (root != NULL) {
        root->glob->hint_warning = lappend(root->glob->hint_warning, makeString(warning_buf.data));
    } else {
        /* In parse phase, we use String struct, since we will CopyObject() later */
        hint->hint_warning = lappend(hint->hint_warning, makeString(warning_buf.data));
    }

    return list;
}

/*
 * @Description: Delete duplicate join hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_join_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->join_hint) {
        JoinMethodHint* joinHint = (JoinMethodHint*)lfirst(lc);

        if (joinHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);

            while (lc_next != NULL) {
                JoinMethodHint* join_hint = (JoinMethodHint*)lfirst(lc_next);

                /* Seperately find duplication for leading and non-leading hint */
                if (joinHint->base.hint_keyword == HINT_KEYWORD_LEADING &&
                    join_hint->base.hint_keyword == HINT_KEYWORD_LEADING) {
                    if (bms_equal(joinHint->joinrelids, join_hint->joinrelids)) {
                        /* We prefer to keep one with inner join rels */
                        if (joinHint->inner_joinrelids == NULL && join_hint->inner_joinrelids != NULL) {
                            joinHint->base.state = HINT_STATE_DUPLICATION;
                            hasError = true;
                            break;
                        }
                        join_hint->base.state = HINT_STATE_DUPLICATION;
                        hasError = true;
                    }
                } else if (joinHint->base.hint_keyword != HINT_KEYWORD_LEADING &&
                           join_hint->base.hint_keyword != HINT_KEYWORD_LEADING) {
                    /* This two hint is the same, need delete one. */
                    if (bms_equal(joinHint->joinrelids, join_hint->joinrelids)) {
                        /* we don't allow same keyword or different positive hint */
                        if (joinHint->base.hint_keyword == join_hint->base.hint_keyword ||
                            (!joinHint->negative && !join_hint->negative)) {
                            join_hint->base.state = HINT_STATE_DUPLICATION;
                            hasError = true;
                        }
                    }
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->join_hint = delete_invalid_hint(root, hstate, hstate->join_hint);
    }
}

/*
 * @Description: Delete duplicate stream hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_stream_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->stream_hint) {
        StreamHint* streamHint = (StreamHint*)lfirst(lc);

        if (streamHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);

            while (lc_next != NULL) {
                StreamHint* stream_hint = (StreamHint*)lfirst(lc_next);

                /* This two hint is the same, need delete one. */
                if (bms_equal(streamHint->joinrelids, stream_hint->joinrelids)) {
                    /* we don't allow same keyword or different positive hint */
                    if (streamHint->base.hint_keyword == stream_hint->base.hint_keyword ||
                        (!streamHint->negative && !stream_hint->negative)) {
                        stream_hint->base.state = HINT_STATE_DUPLICATION;
                        hasError = true;
                    }
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->stream_hint = delete_invalid_hint(root, hstate, hstate->stream_hint);
    }
}

/*
 * @Description: Delete duplicate row hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_row_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->row_hint) {
        RowsHint* rowHint = (RowsHint*)lfirst(lc);

        if (rowHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                RowsHint* row_hint = (RowsHint*)lfirst(lc_next);

                if (bms_equal(rowHint->joinrelids, row_hint->joinrelids)) {
                    row_hint->base.state = HINT_STATE_DUPLICATION;
                    hasError = true;
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->row_hint = delete_invalid_hint(root, hstate, hstate->row_hint);
    }
}

/*
 * @Description: Delete duplicate predpush hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_predpush_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->predpush_hint) {
        PredpushHint* predpushHint = (PredpushHint*)lfirst(lc);

        if (predpushHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                PredpushHint* predpush_hint = (PredpushHint*)lfirst(lc_next);

                if (predpushHint->dest_id == predpush_hint->dest_id) {
                    predpush_hint->base.state = HINT_STATE_DUPLICATION;
                    hasError = true;
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->predpush_hint = delete_invalid_hint(root, hstate, hstate->predpush_hint);
    }
}

/*
 * @Description: Delete duplicate predpush same level hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_predpush_same_level_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->predpush_same_level_hint) {
        PredpushSameLevelHint* predpushSameLevelHint = (PredpushSameLevelHint*)lfirst(lc);

        if (predpushSameLevelHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                PredpushSameLevelHint* predpush_same_level_hint = (PredpushSameLevelHint*)lfirst(lc_next);

                if (predpushSameLevelHint->dest_id == predpush_same_level_hint->dest_id) {
                    predpush_same_level_hint->base.state = HINT_STATE_DUPLICATION;
                    hasError = true;
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->predpush_same_level_hint = delete_invalid_hint(root, hstate, hstate->predpush_same_level_hint);
    }
}

/*
 * @Description: Delete duplicate rewrite hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_rewrite_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->rewrite_hint) {
        RewriteHint* rewriteHint = (RewriteHint*)lfirst(lc);

        if (rewriteHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                RewriteHint* rewrite_hint = (RewriteHint*)lfirst(lc_next);

                List *list_diff = list_difference(rewriteHint->param_names, rewrite_hint->param_names);
                if (list_diff == NIL) {
                    rewrite_hint->base.state = HINT_STATE_DUPLICATION;
                    hasError = true;
                }
                list_free(list_diff);

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->rewrite_hint = delete_invalid_hint(root, hstate, hstate->rewrite_hint);
    }
}

/*
 * @Description: Delete duplicate gather hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_gather_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    bool hfirst = true;
    ListCell* lc = NULL;
    if (list_length(hstate->gather_hint) > 1) {
        foreach(lc, hstate->gather_hint) {
            if (hfirst) {
                hfirst = false;
                continue;
            }
            GatherHint* gatherHint = (GatherHint*)lfirst(lc);
            gatherHint->base.state = HINT_STATE_DUPLICATION;
            hasError = true;
        }
        elog(WARNING, "Gather Hint: Multiple Gather Hint found. Execute with the first one instead.");
    }

    if (hasError) {
        hstate->gather_hint = delete_invalid_hint(root, hstate, hstate->gather_hint);
    }
}

/*
 * @Description: Delete duplicate scan hint.
 * @in hstate: Hint state.
 */
static void drop_duplicate_scan_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    foreach (lc, hstate->scan_hint) {
        ScanMethodHint* scanHint = (ScanMethodHint*)lfirst(lc);

        if (scanHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                ScanMethodHint* scan_hint = (ScanMethodHint*)lfirst(lc_next);

                if (bms_equal(scanHint->relid, scan_hint->relid)) {
                    /* if two hints are same keyword, divide into several cases */
                    if (scanHint->base.hint_keyword == scan_hint->base.hint_keyword) {
                        ScanMethodHint* dup_hint = NULL;

                        /*
                         * If A hint has no index, and B hint has index, we have cases:
                         * positive(A) positive(B) A is duplicate.
                         * positive(A) negative(B) not duplicate.
                         * negative(A) positive(B) B is conflict.
                         * negative(A) negative(B) B is duplicate.
                         */
                        if (scanHint->indexlist == NIL && scan_hint->indexlist != NIL) {
                            if (scanHint->negative) {
                                dup_hint = scan_hint;
                            } else if (!scan_hint->negative) {
                                dup_hint = scanHint;
                            }
                        } else if (scan_hint->indexlist == NIL && scanHint->indexlist != NIL) {
                            if (scan_hint->negative) {
                                dup_hint = scanHint;
                            } else if (!scanHint->negative) {
                                dup_hint = scan_hint;
                            }
                        } else {
                            List* diff = list_difference(scanHint->indexlist, scan_hint->indexlist);
                            if (diff == NIL) {
                                dup_hint = scan_hint;
                            } else {
                                list_free(diff);
                            }
                        }
                        
                        if (dup_hint != NULL) {
                            dup_hint->base.state = HINT_STATE_DUPLICATION;
                            hasError = true;
                            if (dup_hint == scanHint) {
                                break;
                            }
                        }
                    } else if (!scanHint->negative && !scan_hint->negative) {
                        /* if two hints are positive with different keyword, mark latter one as duplicate */
                        scan_hint->base.state = HINT_STATE_DUPLICATION;
                        hasError = true;
                    }
                }

                lc_next = lnext(lc_next);
            }
        }
    }

    if (hasError) {
        hstate->scan_hint = delete_invalid_hint(root, hstate, hstate->scan_hint);
    }
}

/*
 * @Description: Delete duplicate skew hint.
 * @root: query info.
 * @in hstate: Hint state.
 */
static void drop_duplicate_skew_hint(PlannerInfo* root, HintState* hstate)
{
    bool hasError = false;
    ListCell* lc = NULL;

    if (list_length(hstate->skew_hint) < 1) {
        return;
    }

    foreach (lc, hstate->skew_hint) {
        SkewHint* skewHint = (SkewHint*)lfirst(lc);

        if (skewHint->base.state != HINT_STATE_DUPLICATION) {
            ListCell* lc_next = lnext(lc);
            while (lc_next != NULL) {
                SkewHint* skew_hint = (SkewHint*)lfirst(lc_next);

                /* Where two hints have same relations */
                List* diff_rel = list_difference(skewHint->base.relnames, skew_hint->base.relnames);
                if (diff_rel == NIL) {
                    List* diff_col = list_difference(skewHint->column_list, skew_hint->column_list);
                    /* If two hints are same keyword, divide into several cases */
                    if (diff_col == NIL) {
                        List* diff_val = list_difference(skewHint->value_list, skew_hint->value_list);
                        if (diff_val == NIL) {
                            skewHint->base.state = HINT_STATE_DUPLICATION;
                            hasError = true;
                        } else {
                            list_free(diff_val);
                        }
                    } else {
                        list_free(diff_col);
                    }
                }
                lc_next = lnext(lc_next);
            }
        }
    }
    if (hasError) {
        /* If there is duplicated hint, then delete the shorter one. */
        hstate->skew_hint = delete_invalid_hint(root, hstate, hstate->skew_hint);
    }
}

/*
 * @Description: Column type whether can do redistribution, and now we do not support_extended_features.
 * @in typid: Column type oid.
 * @return: ture of false.
 */
static bool support_redistribution(Oid typid)
{
    switch (typid) {
        case INT8OID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case NUMERICOID:
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case TIMETZOID:
        case SMALLDATETIMEOID:
        case TEXTOID:
            return true;
        default:
            break;
    }

    return false;
}

/*
 * @Description: Get name from ListCell.
 * @in lc: ListCell stores Value struct..
 * @return: name.
 */
static char* get_name(ListCell* lc)
{
    /* Check ListCell. */
    if (lc == NULL) {
        return NULL;
    }

    Value* val = (Value*)lfirst(lc);
    Assert(nodeTag(val) == T_String);
    char* name = strVal(val);

    return name;
}

/*
 * @Description: Get relation`s RTE.
 * @in parse: Query struct.
 * @in skew_relname: relation name in skew hint.
 * @return: rel`s rte.
 */
static RangeTblEntry* get_rte(Query* parse, const char* skew_relname)
{
    if (parse == NULL) {
        return NULL;
    }

    RangeTblEntry* entry = NULL;
    ListCell* lc = NULL;

    if (parse->rtable == NULL) {
        return NULL;
    }

    foreach (lc, parse->rtable) {
        entry = (RangeTblEntry*)lfirst(lc);
        /* Should use alias in skew first, and alias list maybe null. */
        if (entry->eref != NULL) {
            if (strncmp(entry->eref->aliasname, skew_relname, NAMEDATALEN) == 0) {
                return entry;
            }
        } else if (entry->relname != NULL) {
            /* It means alisa wasn`t used in skew, so find from relname list. */
            if (strncmp(entry->relname, skew_relname, NAMEDATALEN) == 0) {
                return entry;
            }
        } else {
            /* Alias and relname are both NULL at the same time, unexpected. */
            return NULL;
        }
    }

    return NULL;
}

/*
 * @Description: For subquery and CTE, to get the column location in sub targetlist.
 * @in column_name: column name in skew hint.
 * @in l_subquery_eref: subquery`s eref columns list.
 */
static int get_col_location(const char* column_name, List* eref_col)
{
    /* Check rte expanded reference colmun names. */
    if (eref_col == NIL) {
        return -1;
    }

    int location = -1;
    int i = 1;
    ListCell* lc = NULL;
    foreach (lc, eref_col) {
        char* col_name = get_name(lc);

        if (strncmp(column_name, col_name, NAMEDATALEN) == 0) {
            location = i;
            return location;
        }
        i++;
    }
    return location;
}

/* ------------------------------set skew value info---------------------------- */

/*
 * @Description: Transform skew value into datum.
 * @in skew_value: skew value in hint.
 * @in val_typid: column type oid.
 * @in val_typmod: column type mod.
 * @out val_datum: datum after transformed
 * @out constisnull: whether the value is NULL.
 */
static bool set_skew_value_to_datum(
    Value* skew_value, Datum* val_datum, bool* constisnull, Oid val_typid, int4 val_typmod, ErrorData** edata)
{
    bool set = true;
    MemoryContext oldcontext = CurrentMemoryContext;

    /* If something wrong during parsing hint, we only can output warning. */
    PG_TRY();
    {
        switch (nodeTag(skew_value)) {
            /* For null value. */
            case T_Null: {
                *constisnull = true;
                *val_datum = 0;
                break;
            }
            case T_BitString:
            case T_String:
            case T_Float:
                *val_datum = GetDatumFromString(val_typid, val_typmod, strVal(skew_value));
                break;
            case T_Integer: {
                switch (val_typid) {
                    /* 1. Towards to TINYINT */
                    case INT1OID:
                        *val_datum = UInt8GetDatum(intVal(skew_value));
                        break;
                    /* 2. Towards to SMALLINT */
                    case INT2OID:
                        *val_datum = Int16GetDatum(intVal(skew_value));
                        break;
                    /* 3. Towards to INTEGER */
                    case INT4OID:
                        *val_datum = Int32GetDatum(intVal(skew_value));
                        break;
                    /* 4. Towards to BIGINT */
                    case INT8OID:
                        *val_datum = Int64GetDatum(intVal(skew_value));
                        break;
                    /* 5. Towards to NUMERIC./DECIMAL */
                    case NUMERICOID:
                        *val_datum = NumericGetDatum(int64_to_numeric(intVal(skew_value)));
                        break;
                    default: {
                        /* report error */
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                (errmsg("Unsupported typeoid: %u for T_Integer value, please add single quota and try "
                                        "again.",
                                    val_typid))));
                    }
                }
                break;
            }
            default:
                break;
        }
    }
    PG_CATCH();
    {
        /* Save error info */
        (void)MemoryContextSwitchTo(oldcontext);
        *edata = CopyErrorData();

        set = false;

        FlushErrorState();
    }
    PG_END_TRY();

    return set;
}

/*
 * @Description:  Make skew value into const. If has error during transforming, then go to the next
 *                      value instead of returning with null value_info_list.
 * @in root: query level info.
 * @in skew_hint_transf: SkewHintTransf node used to store the info after transform.
 * @in col_typid: column type oid array.
 */
static void set_skew_value(PlannerInfo* root, SkewHintTransf* skew_hint_transf, Oid* col_typid)
{
    if (skew_hint_transf->before->value_list == NIL ||
        skew_hint_transf->before->column_list == NIL ||
        list_length(skew_hint_transf->before->value_list) == 0 ||
        list_length(skew_hint_transf->before->column_list) == 0) {
        return;
    }

    if (list_length(skew_hint_transf->before->value_list) >
        MAX_SKEW_NUM * list_length(skew_hint_transf->before->column_list)) {
        append_warning_to_list(root,
            (Hint*)skew_hint_transf->before,
            "Error hint:%s, do not support more than %d skew values for each column.",
            hint_string,
            MAX_SKEW_NUM);
        skew_hint_transf->value_info_list = NIL;
        return;
    }

    if (list_length(skew_hint_transf->before->value_list) % list_length(skew_hint_transf->before->column_list) != 0) {
        skew_hint_transf->value_info_list = NIL;
        append_warning_to_list(root,
            (Hint*)skew_hint_transf->before,
            "Error hint:%s missing value. Please input enough skew values for every column.",
            hint_string);
        return;
    }

    ListCell* lc = NULL;
    List* l = skew_hint_transf->before->column_list;
    uint32 c_length = list_length(l);
    Oid val_typid;
    int4 val_typmod;
    Oid val_collid;
    int2 val_typlen;
    int col_num = 0;
    Datum val_datum = 0;
    Const* val_const = NULL;
    int i = 0;

    foreach (lc, skew_hint_transf->before->value_list) {
        SkewValueInfo* value_info = makeNode(SkewValueInfo);

        /* Get column from skew hint */
        Value* skew_value = (Value*)lfirst(lc);
        bool constbyval = false;
        bool constisnull = false;
        ErrorData* edata = NULL;

        col_num = i % c_length;
        val_typid = col_typid[col_num];

        Type typ = typeidType(val_typid);
        val_typmod = ((Form_pg_type)GETSTRUCT(typ))->typtypmod;
        val_collid = typeTypeCollation(typ);
        val_typlen = typeLen(typ);
        ReleaseSysCache(typ);

        /* 1.Set skew value into datum. */
        bool set = set_skew_value_to_datum(skew_value, &val_datum, &constisnull, val_typid, val_typmod, &edata);

        if (set == false) {
            append_warning_to_list(root,
                (Hint*)skew_hint_transf->before,
                "Error hint:%s fail to convert skew value to datum. Details: \"%s\"",
                hint_string,
                edata->message);

            /* release error state */
            FreeErrorData(edata);

            pfree_ext(value_info);
            list_free_deep(skew_hint_transf->value_info_list);
            skew_hint_transf->value_info_list = NIL;
            return;
        }

        /* Typlen is from pg_type, if typlen >= 0 and typlen <=8,
         * for the type, all the information stored in datum,
         * then COL_IS_ENCODE() is false but constbyval is true.
         */
        if (!COL_IS_ENCODE(val_typid)) {
            constbyval = true;
        }

        /* 2. Make value info into const. */
        val_const = makeConst(val_typid, val_typmod, val_collid, val_typlen, val_datum, constisnull, constbyval);

        /* Append into skew_hint_transf`s value_info_list */
        value_info->support_redis = true;
        value_info->const_value = val_const;
        skew_hint_transf->value_info_list = lappend(skew_hint_transf->value_info_list, value_info);

        /* For next value */
        i++;
    }
}

/* ------------------------------set skew column info---------------------------- */
/*
 * @Description: pull up varno of expr in column info if need.
 * @in parse: parse tree.
 * @in rte: subquery rte that the column belongs to.
 * @in column_info: column info struct.
 * @return: void.
 */
void pull_up_expr_varno(Query* parse, RangeTblEntry* rte, SkewColumnInfo* column_info)
{
    /* Only for the case that subquery pull up. */
    if (!subquery_can_pull_up(rte, parse)) {
        return;
    }

    Var* var = NULL;
    ListCell* lc = NULL;
    int rtoffset = list_length(parse->rtable) - list_length(rte->subquery->rtable);

    Expr* expr = (Expr*)copyObject(column_info->expr);

    List* var_list = pull_var_clause((Node*)expr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

    foreach (lc, var_list) {
        var = (Var*)lfirst(lc);

        var->varno = var->varno + rtoffset;
        var->varnoold = var->varnoold + rtoffset;
    }

    column_info->expr = expr;
}

/*
 * @Description: Set column info by TargetEntry.
 * @in tge: TargetEntry of column.
 * @in column_name: column name.
 * @out column_info: column info after setting.
 */
static void set_colinfo_by_tge(
    TargetEntry* tge, SkewColumnInfo* column_info, char* column_name, RangeTblEntry* rte, Query* parse)
{
    /* Check tge */
    if (tge == NULL) {
        return;
    }

    /* The column may be the func result. */
    Oid typid = exprType((Node*)tge->expr);

    /* Set column info. */
    column_info->relation_Oid = tge->resorigtbl;
    column_info->column_name = column_name;
    column_info->column_typid = typid;
    column_info->expr = (Expr*)tge->expr;

    /* 1.Rte is null means column uses alisa and comes from tlist. */
    if (rte != NULL && !subquery_can_pull_up(rte, parse)) {
        /* If subquery can not be pulled up then column_info->attnum will be set the num in subquery. */
        column_info->attnum = tge->resno;
    } else if (tge->resorigcol) {
        /* 2.If Column comes from base rel then column_info->attnum will be set the num in base rel.
         * Note that subquery that can not be pulled up alse has the tge->resorigcol. So should go step 1.
         */
        column_info->attnum = tge->resorigcol;
    } else {
        /* 3.Column comes from expr. */
        column_info->attnum = tge->resno;
    }

    /* Pull up varno of column expr for the case that subquery be pulled up. */
    if (rte != NULL) {
        pull_up_expr_varno(parse, rte, column_info);
    }
}

/*
 * @Description: Set column info by base relation.
 * @in relid: relation oid that column belongs to.
 * @in location: same as the column attnum in relation.
 * @in column_name: column name.
 * @out column_info: column info after setting.
 */
static void set_colinfo_by_relation(Oid relid, int location, SkewColumnInfo* column_info, char* column_name)
{
    ResourceOwner currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    ResourceOwner tmpOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "ForSkewHint",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    Relation relation = NULL;

    relation = heap_open(relid, AccessShareLock);

    Assert((location + 1) == relation->rd_att->attrs[location]->attnum);

    /* Set column info. */
    column_info->relation_Oid = relation->rd_att->attrs[location]->attrelid;
    column_info->column_name = column_name;
    column_info->attnum = relation->rd_att->attrs[location]->attnum;
    column_info->column_typid = relation->rd_att->attrs[location]->atttypid;
    column_info->expr = NULL;

    heap_close(relation, AccessShareLock);

    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = NULL;
    ResourceOwnerDelete(tmpOwner);
    t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
}

/*
 * @Description: Check the rel whether comes from same subquery.
 * @in parent_rte: subquery rte.
 * @in parent_rte_list: subquery list.
 * @return: Rel comes from same subquery, return false. Otherwise return true.
 */
static bool check_parent_rte_is_diff(RangeTblEntry* parent_rte, List* parent_rte_list)
{
    if (parent_rte_list == NIL) {
        return true;
    }

    /* Rel comes from same subquery. */
    if (list_member(parent_rte_list, parent_rte)) {
        return false;
    } else {
        /* Rel comes from different subquery, and maybe we should count. */
        return true;
    }
}

/*
 * @Description: Find column in tagetlist by name.
 * @in targetList: current parse`s targetList.
 * @in column_relname: column name.
 * @return: tge of the column.
 */
static TargetEntry* find_column_in_targetlist_by_name(List* targetList, const char* column_name)
{
    ListCell* lc = NULL;
    TargetEntry* tge = NULL;
    TargetEntry* tge_tmp = NULL;

    if (targetList == NIL) {
        return NULL;
    }

    foreach (lc, targetList) {
        tge_tmp = (TargetEntry*)lfirst(lc);
        if (tge_tmp == NULL || tge_tmp->resname == NULL) {
            continue;
        } else if (strncmp(column_name, tge_tmp->resname, NAMEDATALEN) == 0) {
            tge = tge_tmp;
            return tge;
        }
    }

    return tge;
}

/*
 * @Description: Find column in targetlist by location in subquery`s column.
 * @in rte: subquery rte.
 * @in location: location in subquery.
 * @return: tge of the column.
 */
static TargetEntry* find_column_in_targetlist_by_location(RangeTblEntry* rte, int location, RangeTblEntry** col_rte)
{
    /* Check the location. */
    if (location < 0 || location >= list_length(rte->subquery->targetList)) {
        return NULL;
    }

    ListCell* lc = list_nth_cell(rte->subquery->targetList, location);
    TargetEntry* tge = (TargetEntry*)lfirst(lc);

    /* For nested subquery.
     * If the subquery can be pull up then we will continue parse.
     * Untill the subquery can not be pull up or the tge is from base rel.
     * Then we get the tge from current subquery`s targetlist
     */
    if (rte->rtekind == RTE_SUBQUERY && !rte->subquery_pull_up) {
        return tge;
    } else {
        /* 1.Tge comes from base rel, then return */
        if (tge->resorigtbl) {
            return tge;
        } else if (!IsA(tge->expr, Var)) {
            /* 2.Tge comes from expr and if the var in expr does not come from other rte kind, then return. */
            List* var_list = pull_var_clause((Node*)tge->expr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
            /* For DFX. */
            foreach (lc, var_list) {
                Var* var = (Var*)lfirst(lc);
                ListCell* lc_rte = list_nth_cell(rte->subquery->rtable, var->varno - 1);
                RangeTblEntry* sub_rte = (RangeTblEntry*)lfirst(lc_rte);

                if (sub_rte->rtekind != RTE_RELATION && sub_rte->rtekind != RTE_SUBQUERY) {
                    *col_rte = sub_rte;
                }
            }
            return tge;
        } else {
            /* 3.Tge comes from subquery, then continue parse. */
            List* var_list = pull_var_clause((Node*)tge->expr, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
            if (list_length(var_list) == 1) {
                Var* var = (Var*)linitial(var_list);

                /* Check the var num. */
                Assert(var->varno > 0);
                Assert((unsigned int)list_length(rte->subquery->rtable) >= var->varno);
                if (var->varno < 1 || var->varno > (unsigned int)list_length(rte->subquery->rtable)) {
                    return NULL;
                }

                ListCell* lc = list_nth_cell(rte->subquery->rtable, var->varno - 1);
                RangeTblEntry* sub_rte = (RangeTblEntry*)lfirst(lc);

                *col_rte = sub_rte;

                /* Check the rte_kind, here we only deal with subquery. */
                if (sub_rte->rtekind != RTE_SUBQUERY) {
                    return NULL;
                }

                return find_column_in_targetlist_by_location(sub_rte, location, col_rte);
            }
        }
    }
    return NULL;
}

/*
 * @Description: Find column in subquery for the case that subquery pull up.
 * @in parse: parse tree.
 * @in column_name: column name.
 * @out location: column`s location in subquery.
 * @return: rte that the column belongs to.
 */
static RangeTblEntry* find_column_in_rtable_subquery(Query* parse, const char* column_name, int* location)
{
    ListCell* lc = NULL;
    RangeTblEntry* rte = NULL;
    int location_tmp = -1;

    foreach (lc, parse->rtable) {
        RangeTblEntry* rte_tmp = (RangeTblEntry*)lfirst(lc);

        /* Here only deal with subquery not for base rel. */
        if (rte_tmp->rtekind != RTE_SUBQUERY) {
            continue;
        } else if (subquery_can_pull_up(rte_tmp, parse)) {
            /* For the case that skew hint out of subquery. And the skew column use subquery`s col */
            location_tmp = get_col_location(column_name, rte_tmp->eref->colnames);
            if (location_tmp > -1) {
                *location = location_tmp;
                rte = rte_tmp;
                return rte;
            }

            /* For the case that skew hint in subquery and be pulled up with subquery. And the skew column use alisa in
             * sub tlist */
            TargetEntry* tge_tmp = find_column_in_targetlist_by_name(rte_tmp->subquery->targetList, column_name);
            if (tge_tmp != NULL) {
                *location = tge_tmp->resno;
                rte = rte_tmp;
                return rte;
            }
        } else {
            /* Subquery can not be pulled up.
             * For the case that skew hint out of subquery and subquery can not be pulled up.
             * We also support use column in sub tlist to do hint.
             * e.g. (select a,b from t1... group by 1)tmp(aa,bb), then supporting a\b or aa\bb in hint.
             */
            TargetEntry* tge_tmp = find_column_in_targetlist_by_name(rte_tmp->subquery->targetList, column_name);
            if (tge_tmp != NULL) {
                *location = tge_tmp->resno;
                rte = rte_tmp;
                return rte;
            }
        }
    }

    return rte;
}

/*
 * @Description: Find column from rel_info_list of SkewHintTransf.
 * @in SkewHintTransf: SkewHintTransf node used to store the info after transform.
 * @in column_name: column name.
 * @out count: count the times the find column in rels.
 * @out location: column`s location in rel.
 * @return: rte that the column belongs to.
 */
static RangeTblEntry* find_column_in_rel_info_list(
    SkewHintTransf* skew_hint_transf, const char* column_name, int* count, int* location)
{
    ListCell* lc = NULL;
    RangeTblEntry* rte = NULL;
    int rel_num = 0;
    int location_tmp = -1;

    /* For checking the rel in rel_info_list whether comes from the same subquery. */
    List* parent_rte_list = NIL;

    /* Find column in rel info list. */
    foreach (lc, skew_hint_transf->rel_info_list) {
        SkewRelInfo* rel_info = (SkewRelInfo*)lfirst(lc);

        location_tmp = get_col_location(column_name, rel_info->rte->eref->colnames);
        if (location_tmp > -1) {
            /*
             * Note the case: when subquery pull up,  subquery`s rtable and the current parse`s rtable
             * have same name column. In the case, column should not be ambiguous, e.g.
             * (1)select /+ skew((tp t3) (b) (12)) /... from (select t1.a, t1.b from t1, t2 where t1.a = t2.c)tp(aa,bb),
             * t3 where ...; and t1, t2, t3 all have the same column 'b'. (2)select /+ skew(tp (b) (12)) /... from
             * (select t1.a, t1.b from t1, t2 where t1.a = t2.c)tp(a,b); and t1, t2, tp all have the same column 'b'.
             *
             * We only count for base rel and subquery, not for rel form subquery.
             */
            int location_in_parent = -1;

            /* Rel comes from subquery pull up. */
            if (rel_info->parent_rte) {
                /* Check parent rte expanded reference colmun names. */
                if (!rel_info->parent_rte->eref->colnames) {
                    continue;
                }

                location_in_parent = get_col_location(column_name, rel_info->parent_rte->eref->colnames);
                /* If column is in subquery and the rel comes from different subquery, then count! */
                if (location_in_parent > -1 && check_parent_rte_is_diff(rel_info->parent_rte, parent_rte_list)) {
                    rel_num++;

                    /* Set the rte to parent_rte for go to the function 'find_column_in_targetlist_by_loctation' to set
                     * column info. */
                    rte = rel_info->parent_rte;

                    /* The location in subquery finding in tlist to set the column_attnum. */
                    *location = location_in_parent;

                    parent_rte_list = lappend(parent_rte_list, rel_info->parent_rte);
                }
            } else {
                /* Rel it is subquery or base rel, then count! */
                rel_num++;
                rte = rel_info->rte;
                *location = location_tmp;
            }
        }
    }

    *count = rel_num;

    return rte;
}

/*
 * @Description: Set skew column info and store into column_info_list of skew_hint_transf.
 * @in root: query level info.
 * @in parse: parse tree.
 * @in SkewHintTransf: SkewHintTransf node used to store the info after transform.
 * @out col_typid: column type oid.
 */
static void set_skew_column(PlannerInfo* root, Query* parse, SkewHintTransf* skew_hint_transf, Oid** col_typid)
{
    if (skew_hint_transf->before->column_list == NIL) {
        return;
    }

    ListCell* lc = NULL;
    List* l = skew_hint_transf->before->column_list;
    uint32 c_length = list_length(l);

    *col_typid = (Oid*)palloc0(sizeof(Oid) * c_length);

    Oid* type_oid = *col_typid;

    foreach (lc, l) {
        char* column_name = get_name(lc);
        int count = 0;
        int location = -1;
        TargetEntry* tge = NULL;

        SkewColumnInfo* column_info = makeNode(SkewColumnInfo);

        /* 1. Find column in skew rel: for column original name.
         * (1) find column in rel_info_list.
         *  (1-1)if can find column in more then one rel, then the column is ambiguou.
         * (2) if can not found, try to found in current pasre `s rtable. Mainly for the case that subquery pull up.
         *  (2-1) Note that if has skew hint in subquery and be pull up at the same time,
         *            then should find the column in subquery`s targetlist.
         * (3) if found column in subquery, should according to location to get the subquery`s targetlist tge, and set
         * the column_info; (4) if found column in base rel, open the relation and set the column_info;
         * 2. If still not found, try to found in current parse`s targetlist: for column alias.
         */
        /* 1.(1) Find column in rel_info_list. */
        RangeTblEntry* rte = find_column_in_rel_info_list(skew_hint_transf, column_name, &count, &location);

        if (count > 1) {
            append_warning_to_list(root,
                (Hint*)skew_hint_transf->before,
                "Error hint:%s, reference column \"%s\" in skew hint is ambiguous.",
                hint_string,
                column_name);

            pfree_ext(column_info);
            list_free_deep(skew_hint_transf->column_info_list);
            skew_hint_transf->column_info_list = NIL;
            return;
        }

        if (rte == NULL) {
            /* (2) Find column in current pasre `s rtable. Mainly for the case that subquery pull up. And return
             * subquery */
            rte = find_column_in_rtable_subquery(parse, column_name, &location);
        }

        if (rte != NULL) {
            /* (3) If found column in subquery, should according to location to get the subquery`s targetlist tge. */
            if (rte->rtekind == RTE_SUBQUERY) {
                /* col_rte means the rte that column actually comes from. */
                RangeTblEntry* col_rte = rte;
                tge = find_column_in_targetlist_by_location(rte, location - 1, &col_rte);

                /* Enhance  DFX. */
                if (col_rte->rtekind != RTE_SUBQUERY) {
                    append_warning_to_list(root,
                        (Hint*)skew_hint_transf->before,
                        "Error hint:%s, column \"%s\" comes from rel \"%s\" and rel`s RTEkind is %d, but now only "
                        "support RTE_RELATION and RTE_SUBQUERY.",
                        hint_string,
                        column_name,
                        col_rte->eref->aliasname,
                        col_rte->rtekind);

                    pfree_ext(column_info);
                    list_free_deep(skew_hint_transf->column_info_list);
                    skew_hint_transf->column_info_list = NIL;
                    return;
                } else if (tge == NULL) {
                    append_warning_to_list(root,
                        (Hint*)skew_hint_transf->before,
                        "Error hint:%s, reference column \"%s\" can not be found in subquery.",
                        hint_string,
                        column_name);

                    pfree_ext(column_info);
                    list_free_deep(skew_hint_transf->column_info_list);
                    skew_hint_transf->column_info_list = NIL;
                    return;
                }

                set_colinfo_by_tge(tge, column_info, column_name, rte, parse);
            } else if (rte->rtekind == RTE_RELATION) {
                /* (4) If found column in base rel, open the relation and set the column_info. */
                set_colinfo_by_relation(rte->relid, location - 1, column_info, column_name);
            } else {
                /* Only when we found the column comes from RTE_CTE or other kind of rte, we output the warning. */
                append_warning_to_list(root,
                    (Hint*)skew_hint_transf->before,
                    "Error hint:%s, column \"%s\" comes from rel \"%s\" and rel`s RTEkind is %d, but now only support "
                    "RTE_RELATION and RTE_SUBQUERY.",
                    hint_string,
                    column_name,
                    rte->eref->aliasname,
                    rte->rtekind);

                pfree_ext(column_info);
                list_free_deep(skew_hint_transf->column_info_list);
                skew_hint_transf->column_info_list = NIL;
                return;
            }
        } else {
            /* 2. If still not found, try to found in targetlist. */
            tge = find_column_in_targetlist_by_name(parse->targetList, column_name);
            if (tge == NULL) {
                append_warning_to_list(root,
                    (Hint*)skew_hint_transf->before,
                    "Error hint:%s, reference column \"%s\" in skew hint is not found.",
                    hint_string,
                    column_name);

                pfree_ext(column_info);
                list_free_deep(skew_hint_transf->column_info_list);
                skew_hint_transf->column_info_list = NIL;
                return;
            }

            /* Rte will be null in the case that column use alisa in base rel or subquery. not for rel in subquery. */
            set_colinfo_by_tge(tge, column_info, column_name);
        }

        /* Column type if can support redistribution. */
        if (!support_redistribution(column_info->column_typid)) {
            append_warning_to_list(root,
                (Hint*)skew_hint_transf->before,
                "Error hint:%s, reference column \"%s\"(typeoid: %u) can not support redistribution",
                hint_string,
                column_name,
                column_info->column_typid);

            pfree_ext(column_info);
            list_free_deep(skew_hint_transf->column_info_list);
            skew_hint_transf->column_info_list = NIL;
            return;
        }

        if (column_info->expr != NULL) {
            column_info->expr = (Expr*)preprocess_expression(root, (Node*)column_info->expr, EXPRKIND_TARGET);
        }

        *type_oid = column_info->column_typid;
        skew_hint_transf->column_info_list = lappend(skew_hint_transf->column_info_list, column_info);

        type_oid++;
    }
}

/* -------------------------------set skew rel info----------------------------- */

/*
 * @Description: Judge the subquery whether can be pull up.
 * @in subquery_rte: the RangeTblEntry of subquery.
 * @in parse: Query struct.
 */
static bool subquery_can_pull_up(RangeTblEntry* subquery_rte, Query* parse)
{
    if (subquery_rte == NULL || subquery_rte->subquery == NULL) {
        return false;
    }

    List* sub_rel = subquery_rte->subquery->rtable;
    List* dif_l = NIL;

    dif_l = list_difference(sub_rel, parse->rtable);
    if (dif_l == NULL) {
        return true;
    } else if (subquery_rte->subquery_pull_up) {
        return true;
    } else {
        return false;
    }
}

/*
 * @Description: Set base relations into rel_info_list.
 * @in parse: parse tree.
 * @in rte: rte of base rel.
 * @in skew_hint_transf: SkewHintTransf node used to store the info after transform.
 */
static void set_base_rel(Query* parse, RangeTblEntry* rte, SkewHintTransf* skew_hint_transf)
{
    SkewRelInfo* rel_info = makeNode(SkewRelInfo);

    rel_info->relation_name = rte->relname;
    rel_info->relation_oid = rte->relid;
    rel_info->rte = rte;
    rel_info->parent_rte = NULL;

    skew_hint_transf->rel_info_list = lappend(skew_hint_transf->rel_info_list, rel_info);
}

/*
 * @Description: Set subquery all related relations into rel_info_list.
 * @in parse: parse tree.
 * @in rte: rte of subquery.
 * @in skew_hint_transf: SkewHintTransf node used to store the info after transform.
 */
static void set_subquery_rel(
    Query* parse, RangeTblEntry* rte, SkewHintTransf* skew_hint_transf, RangeTblEntry* parent_rte)
{
    ListCell* lc = NULL;
    RangeTblEntry* sub_rte = NULL;
    bool can_pull_up = false;

    /* Judge that the subquery whether can pull up. */
    can_pull_up = subquery_can_pull_up(rte, parse);
    if (can_pull_up) {
        /* Subquery can pull up, so append all rte of subquery into rel_info_list. */
        foreach (lc, rte->subquery->rtable) {
            sub_rte = (RangeTblEntry*)lfirst(lc);
            /* For nested subquery. */
            if (sub_rte->rtekind == RTE_SUBQUERY) {
                /* Set the subquery relation info. Note that all subquery will be pull up to top level parse. */
                set_subquery_rel(parse, sub_rte, skew_hint_transf, rte);
            } else {
                /* Set the base relation info and parent rte. */
                SkewRelInfo* rel_info = makeNode(SkewRelInfo);
                rel_info->relation_name = sub_rte->relname;
                rel_info->relation_oid = sub_rte->relid;
                rel_info->rte = sub_rte;
                rel_info->parent_rte = rte;

                skew_hint_transf->rel_info_list = lappend(skew_hint_transf->rel_info_list, rel_info);
            }
        }
    } else {
        /* Subquery can not pull up, so only append subquery rte into rel_info_list. */
        SkewRelInfo* rel_info = makeNode(SkewRelInfo);
        rel_info->relation_name = rte->eref->aliasname;
        rel_info->relation_oid = 0;
        rel_info->rte = rte;

        /* If rte comes from subquery, then set the parent. */
        if (parent_rte != NULL) {
            rel_info->parent_rte = parent_rte;
        } else {
            rel_info->parent_rte = NULL;
        }

        skew_hint_transf->rel_info_list = lappend(skew_hint_transf->rel_info_list, rel_info);
    }
}

/*
 * @Description: Set skew relation info and store into rel_info_list of skew_hint_transf.
 * @in root: query level info.
 * @in parse: parse tree.
 * @in SkewHintTransf: SkewHintTransf node used to store the info after transform.
 */
static void set_skew_rel(PlannerInfo* root, Query* parse, SkewHintTransf* skew_hint_transf)
{
    ListCell* lc = NULL;

    foreach (lc, skew_hint_transf->before->base.relnames) {
        char* rel_name = get_name(lc);

        /* Find the skew rel from parse tree rtable by rel name. */
        RangeTblEntry* rte = get_rte(parse, rel_name);

        if (rte == NULL) {
            append_warning_to_list(root,
                (Hint*)skew_hint_transf->before,
                "Error hint:%s, relation name \"%s\" is not found.",
                hint_string,
                rel_name);

            skew_hint_transf->rel_info_list = NIL;
            return;
        } else if (rte->rtekind == RTE_SUBQUERY) {
            /* Set the subquery relation info. */
            set_subquery_rel(parse, rte, skew_hint_transf);
        } else if (rte->rtekind == RTE_RELATION) {
            /* Set the base relation info. */
            set_base_rel(parse, rte, skew_hint_transf);
        } else {
            append_warning_to_list(root,
                (Hint*)skew_hint_transf->before,
                "Error hint:%s, relation \"%s\" RTEkind is %d, but now only support RTE_RELATION and RTE_SUBQUERY when "
                "set rel",
                hint_string,
                rel_name,
                rte->rtekind);

            list_free_deep(skew_hint_transf->rel_info_list);
            skew_hint_transf->rel_info_list = NIL;
            return;
        }
    }
}

/*
 * @Description: Transform only one SkewHint into SkewHintTransf form every time.
 * @in root: query level info.
 * @in parse: parse tree.
 * @in skew_hint: SkewHint struct.
 * @return: SkewHintTransf struct or NULL.
 */
static SkewHintTransf* set_skew_hint(PlannerInfo* root, Query* parse, SkewHint* skew_hint)
{
    SkewHintTransf* skew_hint_transf = makeNode(SkewHintTransf);
    skew_hint_transf->before = skew_hint;

    /* 1. Set relation info. */
    set_skew_rel(root, parse, skew_hint_transf);
    if (skew_hint_transf->rel_info_list == NIL) {
        SkewHintTransfDelete(skew_hint_transf);
        skew_hint_transf = NULL;
    } else {
        /* 2. Set column info. */
        Oid* col_typid = NULL;
        set_skew_column(root, parse, skew_hint_transf, &col_typid);
        if (skew_hint_transf->column_info_list == NIL) {
            SkewHintTransfDelete(skew_hint_transf);
            skew_hint_transf = NULL;
        } else {
            /* 3. Set value info. */
            set_skew_value(root, skew_hint_transf, col_typid);
            if (skew_hint_transf->value_info_list == NIL && skew_hint_transf->before->value_list) {
                SkewHintTransfDelete(skew_hint_transf);
                skew_hint_transf = NULL;
            }
        }
    }

    return skew_hint_transf;
}

/*
 * @Description: Transform skew hint into processible type, including:
 *  transfrom relation name to relation oid
 *  transform column name to column type oid
 *  transform value to corresponding type and make into const
 * @in root: query level info.
 * @in parse: parse tree.
 * @in skew_hint_list: SkewHint list.
 */
static void transform_skew_hint(PlannerInfo* root, Query* parse, List* skew_hint_list)
{
    if (skew_hint_list == NULL) {
        return;
    }

    List* skew_hint_transf_l = NULL;
    ListCell* lc = NULL;
    SkewHintTransf* skew_hint_transf = NULL;

    foreach (lc, skew_hint_list) {
        SkewHint* skew_hint = (SkewHint*)lfirst(lc);
        skew_hint_transf = set_skew_hint(root, parse, skew_hint);
        if (skew_hint_transf == NULL) {
            continue;
        } else {
            skew_hint_transf_l = lappend(skew_hint_transf_l, skew_hint_transf);
        }
    }

    /* Put skew hint transform list into parse. */
    parse->hintState->skew_hint = skew_hint_transf_l;
}

/*
 * Check Predpush hint rely on each other.
 */
static void check_predpush_cycle_hint(PlannerInfo *root,
                                      List *predpush_hint_list,
                                      PredpushHint *predpush_hint)
{
    ListCell *lc = NULL;

    if (bms_num_members(predpush_hint->candidates) != 1) {
        return;
    }

    if (predpush_hint->dest_id == 0) {
        return;
    }

    int cur_dest = predpush_hint->dest_id;
    foreach(lc, predpush_hint_list) {
        PredpushHint *prev_hint = (PredpushHint *)lfirst(lc);

        if (prev_hint == predpush_hint) {
            break;
        }

        if (bms_num_members(prev_hint->candidates) != 1) {
            continue;
        }

        if (prev_hint->dest_id == 0) {
            continue;
        }

        int prev_dest = prev_hint->dest_id;
        if (bms_is_member(prev_dest, predpush_hint->candidates) &&
            bms_is_member(cur_dest, prev_hint->candidates)) {
            append_warning_to_list(
                root, (Hint*)predpush_hint, "Error hint:%s, Predpush cannot rely on each other.", hint_string);
        }
    }

    return;
}

/*
 * @Description: Transform predpush hint into processible type, including:
 *  transfrom subquery name
 * @in root: query level info.
 * @in parse: parse tree.
 * @in predpush_hint_list: predpush hint list.
 */
static void transform_predpush_hint(PlannerInfo* root, Query* parse, List* predpush_hint_list)
{
    if (predpush_hint_list == NULL)
        return;

    ListCell* lc = NULL;
    foreach (lc, predpush_hint_list) {
        PredpushHint* predpush_hint = (PredpushHint*)lfirst(lc);
        if (predpush_hint->dest_name == NULL) {
            continue;
        }

        int relid = find_relid_aliasname(parse, predpush_hint->dest_name, true);
        if (relid <= NOTFOUNDRELNAME) {
            continue;
        }

        predpush_hint->dest_id = relid;
        check_predpush_cycle_hint(root, predpush_hint_list, predpush_hint);
    }

    return;
}

/*
 * @Description: Transform predpush same level hint into processible type, including:
 *  transfrom destination name
 * @in root: query level info.
 * @in parse: parse tree.
 * @in predpush_join_hint_list: predpush same level hint list.
 */
static void transform_predpush_same_level_hint(PlannerInfo* root, Query* parse, List* predpush_same_level_hint_list)
{
    if (predpush_same_level_hint_list == NIL) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, predpush_same_level_hint_list) {
        PredpushSameLevelHint* predpush_same_level_hint = (PredpushSameLevelHint*)lfirst(lc);
        if (predpush_same_level_hint->dest_name == NULL) {
            continue;
        }

        int relid = find_relid_aliasname(parse, predpush_same_level_hint->dest_name, true);
        if (relid <= NOTFOUNDRELNAME) {
            continue;
        }

        predpush_same_level_hint->dest_id = relid;
    }

    return;
}

/*
 * @Description: Transform rewrite hint into bitmap.
 * @in root: query level info.
 * @in parse: parse tree.
 * @in rewrite_hint_list: Rewrite Hint list.
 */
static void transform_rewrite_hint(PlannerInfo* root, Query* parse, List* rewrite_hint_list)
{
    if (rewrite_hint_list == NULL)
        return;
    
    ListCell* lc = NULL;
    foreach (lc, rewrite_hint_list) {
        RewriteHint* rewrite_hint = (RewriteHint*)lfirst(lc);
        if (rewrite_hint->param_names == NULL) {
            continue;
        }

        rewrite_hint->param_bits = get_rewrite_rule_bits(rewrite_hint);
    }
}

static unsigned int get_rewrite_rule_bits(RewriteHint* hint)
{
    ListCell* lc = NULL;
    unsigned int bits = 0;

    foreach (lc, hint->param_names)
    {
        Node *param_name_str = (Node*)lfirst(lc);
        if (param_name_str == NULL) {
            continue;
        }
        if (!IsA(param_name_str, String)) {
            continue;
        }
        char* param_name = ((Value*)param_name_str)->val.str;

        if (pg_strcasecmp(param_name, "lazyagg") == 0) {
            bits = bits | LAZY_AGG;
        }
        else if (pg_strcasecmp(param_name, "magicset") == 0) {
            bits = bits | MAGIC_SET;
        }
        else if (pg_strcasecmp(param_name, "partialpush") == 0) {
            bits = bits | PARTIAL_PUSH;
        }
        else if (pg_strcasecmp(param_name, "uniquecheck") == 0) {
            bits = bits | SUBLINK_PULLUP_WITH_UNIQUE_CHECK;
        }
        else if (pg_strcasecmp(param_name, "disablerep") == 0) {
            bits = bits | SUBLINK_PULLUP_DISABLE_REPLICATED;
        }
        else if (pg_strcasecmp(param_name, "intargetlist") == 0) {
            bits = bits | SUBLINK_PULLUP_IN_TARGETLIST;
        }
        else {
            elog(WARNING, "invalid rewrite rule. (Supported rules: lazyagg, magicset, partialpush, uniquecheck, disablerep, intargetlist)");
        }
    }

    return bits;
}


/*
 * @Description: Check rewrite rule constraints hint:
 * @in root: Planner Info
 * @in params: Rewrite rule parameter
 */
bool permit_from_rewrite_hint(PlannerInfo *root, unsigned int params)
{
    unsigned int bits = 0;

    HintState *hstate = root->parse->hintState;
    if (hstate == NULL)
        return true;

    ListCell* lc = NULL;
    foreach (lc, hstate->rewrite_hint) {
        RewriteHint* rewrite_hint = (RewriteHint*)lfirst(lc);

        if (rewrite_hint->param_bits == 0)
            bits = get_rewrite_rule_bits(rewrite_hint);
        else 
            bits = rewrite_hint->param_bits;

        if (bits & params)
            return false;
    }
    return true;
}

/*
 * @Description: Check gather constraints hint:
 * @in root: Planner Info
 * @in src: Gather source
 */
bool permit_gather(PlannerInfo *root, GatherSource src)
{
    HintState *hstate = root->parse->hintState;

    /* if null just return */
    if (hstate == NULL || hstate->gather_hint == NULL) {
        return false;
    }

    GatherHint* gather_hint = (GatherHint*)linitial(hstate->gather_hint);
    gather_hint->base.state = HINT_STATE_USED;

    /* if UNKNOWN just return */
    if (src > HINT_GATHER_ALL) {
        return false;
    }

    /*
     * High-level gather hints also enables low-level gather hints
     *      permit_gather(root, HINT_GATHER_REL) versus GATHER(JOIN) will permit
     *      permit_gather(root, HINT_GATHER_JOIN) versus GATHER(JOIN) will permit
     *      permit_gather(root, HINT_GATHER_JOIN) versus GATHER(REL) will abort
     *      permit_gather(root) will used as guc control cn gather feature
     */
    return (src <= gather_hint->source);
}

/*
 * @Description: Get gather hint source:
 * @in root: Planner Info
 */
GatherSource get_gather_hint_source(PlannerInfo *root) {
    HintState *hstate = root->parse->hintState;

    if (hstate == NULL || hstate->gather_hint == NULL) {
        return HINT_GATHER_UNKNOWN;   /* if null */
    }

    GatherHint* gather_hint = (GatherHint*)linitial(hstate->gather_hint);

    return gather_hint->source;
}

/*
 * @Description: Transform hint into handy form.
 *  create bitmap of relids from alias names, to make it easier to check
 *  whether a join path matches a join method hint.
 *  add join method hints which are necessary to enforce join order
 *  specified by Leading hint
 * @in root: query level info.
 * @in parse: Query struct.
 * @in hstate: hint state.
 */
void transform_hints(PlannerInfo* root, Query* parse, HintState* hstate)
{
    if (hstate == NULL) {
        return;
    }

    hstate->join_hint = set_hint_relids(root, parse, hstate->join_hint);
    hstate->row_hint = set_hint_relids(root, parse, hstate->row_hint);
    hstate->stream_hint = set_hint_relids(root, parse, hstate->stream_hint);
    hstate->scan_hint = set_hint_relids(root, parse, hstate->scan_hint);
    hstate->skew_hint = set_hint_relids(root, parse, hstate->skew_hint);
    hstate->predpush_hint = set_hint_relids(root, parse, hstate->predpush_hint);
    hstate->predpush_same_level_hint = set_hint_relids(root, parse, hstate->predpush_same_level_hint);

    transform_leading_hint(root, parse, hstate);

    /* Transform predpush hint, for subquery name */
    transform_predpush_hint(root, parse, hstate->predpush_hint);
    transform_predpush_same_level_hint(root, parse, hstate->predpush_same_level_hint);

    transform_rewrite_hint(root, parse, hstate->rewrite_hint);

    /* Delete duplicate hint. */
    drop_duplicate_join_hint(root, hstate);
    drop_duplicate_stream_hint(root, hstate);
    drop_duplicate_row_hint(root, hstate);
    drop_duplicate_scan_hint(root, hstate);
    drop_duplicate_skew_hint(root, hstate);
    drop_duplicate_predpush_hint(root, hstate);
    drop_duplicate_predpush_same_level_hint(root, hstate);
    drop_duplicate_rewrite_hint(root, hstate);
    drop_duplicate_gather_hint(root, hstate);

    /* Transform skew hint into handy form, SkewHint structure will be transform to SkewHintTransf. */
    transform_skew_hint(root, parse, hstate->skew_hint);
}

/*
 * @Description: check validity of scan hint
 * @in root: planner info of current query level
 */
void check_scan_hint_validity(PlannerInfo* root)
{
    HintState* hintstate = root->parse->hintState;
    if (hintstate == NULL) {
        return;
    }

    ListCell* lc = list_head(hintstate->scan_hint);
    ListCell* pre = NULL;
    ListCell* next = NULL;

    while (lc != NULL) {
        ScanMethodHint* scanHint = (ScanMethodHint*)lfirst(lc);
        bool deleted = false;
        next = lnext(lc);

        /* check if there's more index specified */
        if (list_length(scanHint->indexlist) > 1) {
            append_warning_to_list(
                root, (Hint*)scanHint, "Error hint:%s, only one index can be specified in scan hint.", hint_string);
            deleted = true;
        } else if (scanHint->indexlist != NIL) {
            /* check if index exists for specified table */
            Relids relids = bms_copy(scanHint->relid);
            int relindex = bms_first_member(relids);
            RelOptInfo* rel = root->simple_rel_array[relindex];
            char* hint_index_name = strVal(linitial(scanHint->indexlist));
            ListCell* lc2 = NULL;
            foreach (lc2, rel->indexlist) {
                IndexOptInfo* info = (IndexOptInfo*)lfirst(lc2);
                char* index_name = get_rel_name(info->indexoid);

                if (index_name && strncmp(hint_index_name, index_name, strlen(index_name) + 1) == 0) {
                    break;
                }
            }
            if (lc2 == NULL) {
                append_warning_to_list(
                    root, (Hint*)scanHint, "Error hint:%s, index \"%s\" doesn't exist.", hint_string, hint_index_name);
                deleted = true;
            }
            pfree_ext(relids);
        }
        if (deleted) {
            hintDelete((Hint*)scanHint);
            root->parse->hintState->scan_hint = list_delete_cell(root->parse->hintState->scan_hint, lc, pre);
        } else {
            pre = lc;
        }

        lc = next;
    }
}

/*
 * @Description: Change scan rel id in scan hint of state, used in hdfs case.
 * @in hstate: Hint State.
 * @in oldIdx, newIdx: old and new table entry index.
 */
void adjust_scanhint_relid(HintState* hstate, Index oldIdx, Index newIdx)
{
    if (hstate == NULL) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, hstate->scan_hint) {
        ScanMethodHint* scan_hint = (ScanMethodHint*)lfirst(lc);
        if (bms_is_member(oldIdx, scan_hint->relid)) {
            bms_free(scan_hint->relid);
            scan_hint->relid = bms_make_singleton(newIdx);
        }
    }
}

bool pull_hint_warning_walker(Node* node, pull_hint_warning_context* context)
{
    if (node == NULL) {
        return false;
    }

    if (IsA(node, Query)) {
        HintState* hstate = ((Query*)node)->hintState;
        if (hstate != NULL) {
            ListCell* lc = NULL;
            foreach (lc, hstate->hint_warning) {
                Value* v = (Value*)lfirst(lc);
                context->warning = lappend(context->warning, copyObject(v));
            }
            hstate->hint_warning = NIL;
        }
        return query_tree_walker((Query*)node, (bool (*)())pull_hint_warning_walker, (void*)context, QTW_IGNORE_DUMMY);
    }
    return expression_tree_walker(node, (bool (*)())pull_hint_warning_walker, (void*)context);
}

List* retrieve_query_hint_warning(Node* parse)
{
    pull_hint_warning_context context;
    context.warning = NIL;
    (void)pull_hint_warning_walker(parse, &context);
    return context.warning;
}

void output_utility_hint_warning(Node* query, int lev)
{
    List* warning = NIL;

    switch (nodeTag(query)) {
        case T_ViewStmt:
            warning = retrieve_query_hint_warning(((ViewStmt*)query)->query);
            break;
        case T_PrepareStmt:
            warning = retrieve_query_hint_warning(((PrepareStmt*)query)->query);
            break;
        case T_DeclareCursorStmt:
            warning = retrieve_query_hint_warning(((DeclareCursorStmt*)query)->query);
            break;
        case T_RuleStmt:
            /* Need to add after RULE is out of black list */
        default:
            break;
    }
    output_hint_warning(warning, lev);
}

/*
 * @Description: Append used or not used hint string into buf.
 * @in hstate: Hint State.
 * @out buf: Keep hint string.
 */
void output_hint_warning(List* warning, int lev)
{
    ListCell* lc = NULL;

    foreach (lc, warning) {
        Value* msg = (Value*)lfirst(lc);
        ereport(lev, (errmodule(MOD_PLANHINT), errmsg("%s", strVal(msg))));
    }

    list_free_deep(warning);
    warning = NIL;
}

/*
 * enable predpush? we assume that 'No predpush' need to be the first element.
 */
bool permit_predpush(PlannerInfo *root)
{
    if (root == NULL) {
        return true;
    }

    HintState *hstate = root->parse->hintState;
    if (hstate == NULL)
        return true;

    if (hstate->predpush_hint == NULL)
        return true;

    PredpushHint *predpushHint = (PredpushHint*)linitial(hstate->predpush_hint);
    return !predpushHint->negative;
}

const unsigned int G_NUM_SET_HINT_WHITE_LIST = 33;
const char* G_SET_HINT_WHITE_LIST[G_NUM_SET_HINT_WHITE_LIST] = {
    /* keep in the ascending alphabetical order of frequency */
    (char*)"best_agg_plan",
    (char*)"cost_weight_index",
    (char*)"cpu_index_tuple_cost",
    (char*)"cpu_operator_cost",
    (char*)"cpu_tuple_cost",
    (char*)"default_limit_rows",
    (char*)"effective_cache_size",
    (char*)"enable_bitmapscan",
    (char*)"enable_broadcast",
    (char*)"enable_fast_query_shipping",
    (char*)"enable_hashagg",
    (char*)"enable_hashjoin",
    (char*)"enable_index_nestloop",
    (char*)"enable_indexonlyscan",
    (char*)"enable_indexscan",
    (char*)"enable_material",
    (char*)"enable_mergejoin",
    (char*)"enable_nestloop",
    (char*)"enable_remotegroup",
    (char*)"enable_remotejoin",
    (char*)"enable_remotelimit",
    (char*)"enable_remotesort",
    (char*)"enable_seqscan",
    (char*)"enable_sort",
    (char*)"enable_stream_operator",
    (char*)"enable_stream_recursive",
    (char*)"enable_tidscan",
    (char*)"enable_trigger_shipping",
    (char*)"node_name",
    (char*)"query_dop",
    (char*)"random_page_cost",
    (char*)"seq_page_cost",
    (char*)"try_vector_engine_strategy"};

static int param_str_cmp(const void *s1, const void *s2)
{
    const char *key = (const char *)s1;
    const char * const *arg = (const char * const *)s2;
    return pg_strcasecmp(key, *arg);
}

bool check_set_hint_in_white_list(const char* name)
{
    if (name == NULL) {
        return false;
    }
    char* res = (char*)bsearch((void *) name,
                               (void *) G_SET_HINT_WHITE_LIST,
                               G_NUM_SET_HINT_WHITE_LIST,
                               sizeof(char*),
                               param_str_cmp);
    return res != NULL;
}

bool has_no_expand_hint(Query* subquery)
{
    if (subquery->hintState == NULL) {
        return false;
    }
    if (subquery->hintState->no_expand_hint != NIL) {
        NoExpandHint* hint = (NoExpandHint*)linitial(subquery->hintState->no_expand_hint);
        hint->base.state = HINT_STATE_USED;
        return true;
    }
    return false;
}

bool has_no_gpc_hint(HintState* hintState)
{
    if (hintState == NULL) {
        return false;
    }
    if (hintState->no_gpc_hint != NIL) {
        NoGPCHint* hint = (NoGPCHint*)linitial(hintState->no_gpc_hint);
        hint->base.state = HINT_STATE_USED;
        return true;
    }
    return false;
}

/*
 * check if is dest hinttype, it's used by function list_cell_clear
 * val1: ScanMethodHint
 * val2: HintKeyword
 */
static bool IsScanUseDesthint(void* val1, void* val2)
{
    ScanMethodHint *scanmethod = (ScanMethodHint*)lfirst((ListCell*)val1);
    HintKeyword* desthint = (HintKeyword*)val2;

    if (scanmethod == NULL) {
        return false;
    }

    if (scanmethod->base.hint_keyword == *desthint) {
        return true;
    } else {
        return false;
    }
}

void RemoveQueryHintByType(Query *query, HintKeyword hint)
{
    if (query->hintState && query->hintState->scan_hint) {
        query->hintState->scan_hint = list_cell_clear(query->hintState->scan_hint, &hint, IsScanUseDesthint);
    }
}

bool CheckNodeNameHint(HintState* hintstate)
{
    if (hintstate == NULL) {
        return false;
    }
    ListCell* lc = NULL;
    foreach (lc, hintstate->set_hint) {
        SetHint* hint = (SetHint*)lfirst(lc);
        if (unlikely(strcmp(hint->name, "node_name") == 0)) {
            return true;
        }
    }
    return false;
}
