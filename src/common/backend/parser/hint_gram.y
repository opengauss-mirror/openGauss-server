%{
#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "parser/parse_hint.h"
#include "parser/parser.h"

#include "parser/gramparse.h"

#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"

extern void yyerror(yyscan_t yyscanner, const char *msg);
extern void hint_scanner_yyerror(const char *msg, yyscan_t yyscanner);

static Value *makeStringValue(char *str);

static Value *makeBitStringValue(char *str);

static Value *makeNullValue();

static Value *makeBoolValue(bool state);

static void doNegateFloat(Value *v);
static Value* integerToString(Value *v);

#define YYMALLOC palloc
#define YYFREE   pfree


static double convert_to_numeric(Node *value);

%}

%define api.pure
%expect 0

%parse-param {yyscan_t yyscanner}
%lex-param   {yyscan_t yyscanner}


%union
{
	int		ival;
	char		*str;
	List		*list;
	Node	*node;
}



%type <node> join_hint_item join_order_hint join_method_hint stream_hint row_hint scan_hint skew_hint expr_const
	pred_push_hint pred_push_same_level_hint rewrite_hint gather_hint set_hint plancache_hint guc_value no_expand_hint
	no_gpc_hint
%type <list> relation_list join_hint_list relation_item relation_list_with_p ident_list skew_relist
             column_list_p column_list value_list_p value_list value_list_item value_type value_list_with_bracket
%token <str>	IDENT FCONST SCONST BCONST XCONST
%token <ival>	ICONST

%token <keyword> NestLoop_P MergeJoin_P HashJoin_P No_P Leading_P Rows_P Broadcast_P Redistribute_P BlockName_P
	TableScan_P IndexScan_P IndexOnlyScan_P Skew_P HINT_MULTI_NODE_P NULL_P TRUE_P FALSE_P Predpush_P
	PredpushSameLevel_P Rewrite_P Gather_P Set_P USE_CPLAN_P USE_GPLAN_P ON_P OFF_P No_expand_P SQL_IGNORE_P NO_GPC_P
	CHOOSE_ADAPTIVE_GPLAN_P

%nonassoc	IDENT NULL_P

%%
//yacc syntax start here  
join_hint_list:
	join_hint_item join_hint_list
	{
		$$ = lcons($1, $2);
		u_sess->parser_cxt.hint_list = $$;
	}
	| /*EMPTY*/             { $$ = NIL; }
	;

join_hint_item:
	join_order_hint
	{
		$$ = $1;
	}
	| join_method_hint
	{
		$$ = $1;
	}
	| No_P join_method_hint
	{
		JoinMethodHint *joinHint = (JoinMethodHint *) $2;
		joinHint->negative = true;
		$$ = (Node *) joinHint;
	}
	| stream_hint
	{
		$$ = $1;
	}
	| No_P stream_hint
	{
		StreamHint	*streamHint = (StreamHint *) $2;
		if (streamHint != NULL) {
			streamHint->negative = true;
		}
		$$ = (Node *) streamHint;
	}
	| row_hint
	{
		$$ = $1;
	}
	| BlockName_P '(' IDENT ')'
	{
		BlockNameHint *blockHint = makeNode(BlockNameHint);
		blockHint->base.relnames = list_make1(makeString($3));
		blockHint->base.hint_keyword = HINT_KEYWORD_BLOCKNAME;
		$$ = (Node *) blockHint;
	}
	| scan_hint
	{
		$$ = $1;
	}
	| No_P scan_hint
	{
		ScanMethodHint	*scanHint = (ScanMethodHint *) $2;
		scanHint->negative = true;
		$$ = (Node *) scanHint;
	}
	| skew_hint
	{
		$$ = $1;
	}
	| HINT_MULTI_NODE_P
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		MultiNodeHint *multi_node_hint = (MultiNodeHint *)makeNode(MultiNodeHint);
		multi_node_hint->multi_node_hint = true;
		$$ = (Node *) multi_node_hint;
#endif /* ENABLE_MULTIPLE_NODES */
	}
    | pred_push_hint
    {
        $$ = $1;
    }
	| pred_push_same_level_hint
    {
        $$ = $1;
    }
	| No_P rewrite_hint
	{
		$$ = $2;
	}
	| gather_hint
	{
		$$ = $1;
	}
	| set_hint
	{
		$$ = $1;
	}
	| plancache_hint
	{
		$$ = $1;
	}
	| no_expand_hint
	{
		$$ = $1;
	}
	| no_gpc_hint
	{
		$$ = $1;
	}
	| SQL_IGNORE_P
	{
		SqlIgnoreHint *sql_ignore_hint = (SqlIgnoreHint *)makeNode(SqlIgnoreHint);
		sql_ignore_hint->sql_ignore_hint = true;
		$$ = (Node *) sql_ignore_hint;
	}

gather_hint:
	Gather_P '(' IDENT ')'
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		GatherHint *gatherHint = makeNode(GatherHint);
		gatherHint->base.hint_keyword = HINT_KEYWORD_GATHER;
		gatherHint->base.state = HINT_STATE_NOTUSED;
		if (pg_strcasecmp($3, "REL") == 0) {
			gatherHint->source = HINT_GATHER_REL;
		} else if (pg_strcasecmp($3, "JOIN") == 0) {
			gatherHint->source = HINT_GATHER_JOIN;
		} else if (pg_strcasecmp($3, "ALL") == 0) {
			gatherHint->source = HINT_GATHER_ALL;
		} else {
			gatherHint->source = HINT_GATHER_UNKNOWN;
		}
		$$ = (Node *) gatherHint;
#endif /* ENABLE_MULTIPLE_NODES */
	}

no_gpc_hint:
	NO_GPC_P
	{
		NoGPCHint *noGPCHint = makeNode(NoGPCHint);
		noGPCHint->base.hint_keyword = HINT_KEYWORD_NO_GPC;
		noGPCHint->base.state = HINT_STATE_NOTUSED;
		$$ = (Node *) noGPCHint;
	}

no_expand_hint:
	No_expand_P
	{
		NoExpandHint *noExpandHint = makeNode(NoExpandHint);
		noExpandHint->base.hint_keyword = HINT_KEYWORD_NO_EXPAND;
		noExpandHint->base.state = HINT_STATE_NOTUSED;
		$$ = (Node *) noExpandHint;
	}

guc_value:
	IDENT				{ $$ = (Node*)makeStringValue($1); }
	| SCONST			{ $$ = (Node*)makeStringValue($1); }
	| TRUE_P			{ $$ = (Node*)makeBoolValue(TRUE); }
	| FALSE_P			{ $$ = (Node*)makeBoolValue(FALSE); }
	| ON_P				{ $$ = (Node*)makeBoolValue(TRUE); }
	| OFF_P				{ $$ = (Node*)makeBoolValue(FALSE); }
	| ICONST			{ $$ = (Node*)makeInteger($1); }
	| '+' ICONST		{ $$ = (Node*)makeInteger($2); }
	| '-' ICONST		{ $$ = (Node*)makeInteger(-$2); }
	| FCONST			{ $$ = (Node*)makeFloat($1); }
	| '+' FCONST		{ $$ = (Node*)makeFloat($2); }
	| '-' FCONST
	{
		Value *fvalue = makeFloat($2);
		doNegateFloat(fvalue);
		$$ = (Node*)fvalue;
	}
	;

set_hint:
	Set_P '(' IDENT guc_value ')'
	{
		char* name = $3;
		if (!check_set_hint_in_white_list(name)) {
			ereport(WARNING, (errmsg("SetHint is invalid. Parameter [%s] is not in whitelist.", name)));
			$$ = NULL;
		} else {
			Value* guc_val = NULL;
			if (IsA($4, Integer)) {
				guc_val = integerToString((Value*)$4);
			} else {
				guc_val = (Value*)$4;
			}
			SetHint *setHint = makeNode(SetHint);
			setHint->base.hint_keyword = HINT_KEYWORD_SET;
			setHint->base.state = HINT_STATE_NOTUSED;
			setHint->name = name;
			setHint->value = strVal(guc_val);
			$$ = (Node *) setHint;
		}
	}
	|
	Set_P '(' Rewrite_P guc_value ')'
	{
		char* name = "rewrite_rule";
		Value* guc_val = NULL;
		if (IsA($4, Integer)) {
			guc_val = integerToString((Value*)$4);
		} else {
			guc_val = (Value*)$4;
		}
		SetHint *setHint = makeNode(SetHint);
		setHint->base.hint_keyword = HINT_KEYWORD_SET;
		setHint->base.state = HINT_STATE_NOTUSED;
		setHint->name = name;
		setHint->value = strVal(guc_val);
		$$ = (Node *) setHint;
	}

plancache_hint:
	USE_CPLAN_P
	{
		PlanCacheHint *planCacheHint = makeNode(PlanCacheHint);
		planCacheHint->base.hint_keyword = HINT_KEYWORD_CPLAN;
		planCacheHint->base.state = HINT_STATE_NOTUSED;
		planCacheHint->chooseCustomPlan = true;
        planCacheHint->method = CHOOSE_NONE_GPLAN;

		$$ = (Node *) planCacheHint;
	}
	| USE_GPLAN_P
	{
		PlanCacheHint *planCacheHint = makeNode(PlanCacheHint);
		planCacheHint->base.hint_keyword = HINT_KEYWORD_GPLAN;
		planCacheHint->base.state = HINT_STATE_NOTUSED;
		planCacheHint->chooseCustomPlan = false;
        planCacheHint->method = CHOOSE_DEFAULT_GPLAN;

		$$ = (Node *) planCacheHint;
	}
    | CHOOSE_ADAPTIVE_GPLAN_P
    {
        PlanCacheHint *planCacheHint = makeNode(PlanCacheHint);
		planCacheHint->base.hint_keyword = HINT_KEYWORD_CHOOSE_ADAPTIVE_GPLAN;
		planCacheHint->base.state = HINT_STATE_NOTUSED;
		planCacheHint->chooseCustomPlan = false;
        planCacheHint->method = CHOOSE_ADAPTIVE_GPLAN;

		$$ = (Node *) planCacheHint;
    }


rewrite_hint:
	Rewrite_P '(' ident_list ')'
	{
		RewriteHint *rewriteHint = makeNode(RewriteHint);
		rewriteHint->param_names = $3;
		rewriteHint->param_bits = 0;
		rewriteHint->base.hint_keyword = HINT_KEYWORD_REWRITE;
		$$ = (Node *) rewriteHint;
	}

pred_push_hint:
    Predpush_P '(' ident_list ')'
    {
        PredpushHint *predpushHint = makeNode(PredpushHint);
        predpushHint->base.relnames = $3;
        predpushHint->base.hint_keyword = HINT_KEYWORD_PREDPUSH;
        predpushHint->base.state = HINT_STATE_NOTUSED;
        predpushHint->dest_name = NULL;
        predpushHint->dest_id = 0;
        predpushHint->candidates = NULL;
        predpushHint->negative = false;
        $$ = (Node *) predpushHint;
    }
    |
    Predpush_P '(' ident_list ',' IDENT ')'
    {
        PredpushHint *predpushHint = makeNode(PredpushHint);
        predpushHint->base.relnames = $3;
        predpushHint->base.hint_keyword = HINT_KEYWORD_PREDPUSH;
        predpushHint->base.state = HINT_STATE_NOTUSED;
        predpushHint->dest_name = $5;
        predpushHint->dest_id = 0;
        predpushHint->candidates = NULL;
        predpushHint->negative = false;
        $$ = (Node *) predpushHint;
    }
    |
    No_P Predpush_P
    {
        PredpushHint *predpushHint = makeNode(PredpushHint);
        predpushHint->base.relnames = NULL;
        predpushHint->base.hint_keyword = HINT_KEYWORD_PREDPUSH;
        predpushHint->base.state = HINT_STATE_NOTUSED;
        predpushHint->dest_name = NULL;
        predpushHint->dest_id = 0;
        predpushHint->candidates = NULL;
        predpushHint->negative = true;
        $$ = (Node *) predpushHint;
    }

pred_push_same_level_hint:
	PredpushSameLevel_P '(' ident_list ',' IDENT ')'
    {
        PredpushSameLevelHint *predpushSameLevelHint = makeNode(PredpushSameLevelHint);
        predpushSameLevelHint->base.relnames = $3;
        predpushSameLevelHint->base.hint_keyword = HINT_KEYWORD_PREDPUSH_SAME_LEVEL;
        predpushSameLevelHint->base.state = HINT_STATE_NOTUSED;
        predpushSameLevelHint->dest_name = $5;
        predpushSameLevelHint->dest_id = 0;
        predpushSameLevelHint->candidates = NULL;
        predpushSameLevelHint->negative = false;
        $$ = (Node *) predpushSameLevelHint;
    }

join_order_hint:
	Leading_P '(' relation_list_with_p ')'
	{
		LeadingHint *leadingHint = makeNode(LeadingHint);
		leadingHint->base.relnames = $3;
		leadingHint->join_order_hint = true;
		leadingHint->base.hint_keyword = HINT_KEYWORD_LEADING;
		$$ = (Node *) leadingHint;
	}
	| Leading_P relation_list_with_p
	{
		LeadingHint *leadingHint = makeNode(LeadingHint);
		leadingHint->base.relnames = $2;
		leadingHint->base.hint_keyword = HINT_KEYWORD_LEADING;
		$$ = (Node *) leadingHint;
	}
	;

join_method_hint:
	NestLoop_P '(' ident_list ')'
	{
		JoinMethodHint *joinHint = makeNode(JoinMethodHint);
		joinHint->base.relnames = $3;
		joinHint->base.hint_keyword = HINT_KEYWORD_NESTLOOP;
		joinHint->base.state = HINT_STATE_NOTUSED;
		joinHint->joinrelids = NULL;
		joinHint->inner_joinrelids = NULL;

		$$ = (Node*)joinHint;
	}
	| MergeJoin_P '(' ident_list ')'
	{
		JoinMethodHint *joinHint = makeNode(JoinMethodHint);
		joinHint->base.relnames = $3;
		joinHint->base.hint_keyword = HINT_KEYWORD_MERGEJOIN;
		joinHint->base.state = HINT_STATE_NOTUSED;
		joinHint->joinrelids = NULL;
		joinHint->inner_joinrelids = NULL;

		$$ = (Node*)joinHint;
	}
	| HashJoin_P '(' ident_list ')'
	{
		JoinMethodHint *joinHint = makeNode(JoinMethodHint);
		joinHint->base.relnames = $3;
		joinHint->base.hint_keyword = HINT_KEYWORD_HASHJOIN;
		joinHint->base.state = HINT_STATE_NOTUSED;
		joinHint->joinrelids = NULL;
		joinHint->inner_joinrelids = NULL;

		$$ = (Node*)joinHint;
	}
	;

stream_hint:
	Broadcast_P '(' ident_list ')'
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		StreamHint *streamHint = makeNode(StreamHint);
		streamHint->base.relnames = $3;
		streamHint->base.hint_keyword = HINT_KEYWORD_BROADCAST;
		streamHint->stream_type = STREAM_BROADCAST;

		$$ = (Node*)streamHint;
#endif /* ENABLE_MULTIPLE_NODES */
	}
	| Redistribute_P '(' ident_list ')'
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		StreamHint *streamHint = makeNode(StreamHint);
		streamHint->base.relnames = $3;
		streamHint->base.hint_keyword = HINT_KEYWORD_REDISTRIBUTE;
		streamHint->stream_type = STREAM_REDISTRIBUTE;

		$$ = (Node*)streamHint;
#endif /* ENABLE_MULTIPLE_NODES */
	}
	;

row_hint:
	Rows_P '(' ident_list '#' expr_const ')'
	{
		RowsHint *rowHint = makeNode(RowsHint);
		rowHint->base.relnames = $3;
		rowHint->base.hint_keyword = HINT_KEYWORD_ROWS;
		rowHint->value_type = RVT_ABSOLUTE;
		rowHint->rows = convert_to_numeric($5);
		if (IsA($5, Float))
			rowHint->rows_str = strVal($5);

		$$ = (Node *) rowHint;
	}
	| Rows_P '(' ident_list '+' expr_const ')'
	{
		RowsHint *rowHint = makeNode(RowsHint);
		rowHint->base.relnames = $3;
		rowHint->base.hint_keyword = HINT_KEYWORD_ROWS;
		rowHint->value_type = RVT_ADD;
		rowHint->rows = convert_to_numeric($5);
		if (IsA($5, Float))
			rowHint->rows_str = strVal($5);

		$$ = (Node *) rowHint;
	}
	| Rows_P '(' ident_list '-' expr_const ')'
	{
		RowsHint *rowHint = makeNode(RowsHint);
		rowHint->base.relnames = $3;
		rowHint->base.hint_keyword = HINT_KEYWORD_ROWS;
		rowHint->value_type = RVT_SUB;
		rowHint->rows = convert_to_numeric($5);
		if (IsA($5, Float))
			rowHint->rows_str = strVal($5);

		$$ = (Node *) rowHint;
	}
	| Rows_P '(' ident_list '*' expr_const ')'
	{
		RowsHint *rowHint = makeNode(RowsHint);
		rowHint->base.relnames = $3;
		rowHint->base.hint_keyword = HINT_KEYWORD_ROWS;
		rowHint->value_type = RVT_MULTI;
		rowHint->rows = convert_to_numeric($5);
		if (IsA($5, Float))
			rowHint->rows_str = strVal($5);

		$$ = (Node *) rowHint;
	}
	;

scan_hint:
	TableScan_P '(' IDENT ')'
	{
		ScanMethodHint	*scanHint = makeNode(ScanMethodHint);
		scanHint->base.relnames = list_make1(makeString($3));
		scanHint->base.hint_keyword = HINT_KEYWORD_TABLESCAN;
		scanHint->base.state = HINT_STATE_NOTUSED;
		$$ = (Node *) scanHint;
	}
	|
	IndexScan_P '(' ident_list ')'
	{
		ScanMethodHint	*scanHint = makeNode(ScanMethodHint);
		scanHint->base.relnames = list_make1(linitial($3));
		scanHint->base.hint_keyword = HINT_KEYWORD_INDEXSCAN;
		scanHint->base.state = HINT_STATE_NOTUSED;
		scanHint->indexlist = list_delete_first($3);
		$$ = (Node *) scanHint;
	}
	|
	IndexOnlyScan_P '(' ident_list ')'
	{
		ScanMethodHint	*scanHint = makeNode(ScanMethodHint);
		scanHint->base.relnames = list_make1(linitial($3));
		scanHint->base.hint_keyword = HINT_KEYWORD_INDEXONLYSCAN;
		scanHint->base.state = HINT_STATE_NOTUSED;
		scanHint->indexlist = list_delete_first($3);
		$$ = (Node *) scanHint;
	}
	;

skew_hint:
	Skew_P '(' skew_relist column_list_p value_list_p ')'
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		SkewHint	*skewHint = makeNode(SkewHint);
		skewHint->base.relnames = $3;
		skewHint->base.hint_keyword = HINT_KEYWORD_SKEW;
		skewHint->base.state = HINT_STATE_NOTUSED;
		skewHint->column_list = $4;
		skewHint->value_list = $5;
		$$ = (Node *) skewHint;
#endif /* ENABLE_MULTIPLE_NODES */
	}
	|
	Skew_P '(' skew_relist column_list_p ')'
	{
#ifndef ENABLE_MULTIPLE_NODES
		hint_scanner_yyerror("unsupport distributed hint", yyscanner);
		$$ = NULL;
#else /* ENABLE_MULTIPLE_NODES */
		SkewHint	*skewHint = makeNode(SkewHint);
		skewHint->base.relnames = $3;
		skewHint->base.hint_keyword = HINT_KEYWORD_SKEW;
		skewHint->base.state = HINT_STATE_NOTUSED;
		skewHint->column_list = $4;
		skewHint->value_list = NIL;
		$$ = (Node *) skewHint;
#endif /* ENABLE_MULTIPLE_NODES */
	}
	;

relation_list_with_p:
	'(' relation_list ')'           { $$ = $2; }
	;

relation_item:
	IDENT                           { $$ = list_make1(makeString($1)); }
	| relation_list_with_p          { $$ = list_make1($1); }
	;

relation_list:
	relation_item relation_item       { $$ = list_concat($1, $2); }
	| relation_list relation_item       { $$ = list_concat($1, $2); }
	;

ident_list:
	IDENT				{ $$ = list_make1(makeString($1)); }
	| ident_list IDENT		{ $$ = lappend($1, makeString($2)); }
	;

expr_const:
	ICONST				{ $$ = (Node *) makeInteger($1); }
	| FCONST			{ $$ = (Node *) makeFloat($1); }
	;	

skew_relist:
	IDENT					{ $$ = list_make1(makeString($1)); }
	|'(' ident_list ')'		{ $$ = $2; }
	;

column_list_p:
	'(' column_list ')'	{ $$ = $2; }
	;

column_list:
	IDENT					{ $$ = list_make1(makeString($1)); }
	| column_list IDENT		{ $$ = lappend($1, makeString($2)); }
	;

value_list_p:
	'(' value_list ')'     { $$ = $2; }
	;

value_list:
	value_list_item				   { $$ = $1; }
	| value_list_with_bracket      { $$ = $1; }
	;

value_list_with_bracket:
	value_list_p		{ $$ = $1; }
	| value_list_with_bracket value_list_p   { $$ = list_concat($1, $2); }
	;

value_list_item:
	value_type			{ $$ = $1; }
	| value_list_item value_type		{$$ = list_concat($1, $2); }
	;

value_type:
	ICONST				{ $$ = list_make1(makeInteger($1)); }
	| FCONST			{ $$ = list_make1(makeFloat($1)); }
	| SCONST			{ $$ = list_make1(makeStringValue($1)); } /* specially process for null value. */
	| BCONST 			{ $$ = list_make1(makeBitStringValue($1)); } /* for bit string litera*/
	| XCONST 			{ $$ = list_make1(makeString($1)); }  /* hexadecimal numeric string*/
	| NULL_P			{ $$ = list_make1(makeNullValue()); }
	| TRUE_P			{ $$ = list_make1(makeBoolValue(TRUE)); } /* for boolean type, we save as string with type T_String. */
	| FALSE_P			{ $$ = list_make1(makeBoolValue(FALSE)); }
	;
%%

void
 yyerror(yyscan_t yyscanner, const char *msg)
{
	hint_scanner_yyerror(msg, yyscanner);
	return;
}

static double
convert_to_numeric(Node *value)
{
	double	d = 0;
	Value	*vvalue = (Value *) value;

	switch(nodeTag(vvalue))
	{
		case T_Integer:
			d = intVal(vvalue);
			break;
		case T_Float:
			d = floatVal(vvalue);
			break;
		default:
			break;
	}

	return d;
}

static Value *
makeStringValue(char *str)
{
	Value	   *val = makeNode(Value);

	if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
	{
		if (NULL == str || (0 == strlen(str) && !ACCEPT_EMPTY_STR))
		{
			val->type = T_Null;
			val->val.str = str;
		}
		else
		{
			val->type = T_String;
			val->val.str = str;
		}
	}
	else
	{
		val->type = T_String;
		val->val.str = str;
	}

	return val;
}

static Value *
makeBitStringValue(char *str)
{
	Value	*val = makeNode(Value);

	val->type = T_BitString;
	val->val.str = str;

	return val;
}

static Value *
makeNullValue()
{
	Value	*val = makeNode(Value);

	val->type = T_Null;

	return val;

}

static Value *
makeBoolValue(bool state)
{
	Value	*val = makeNode(Value);
	val->type = T_String;
	val->val.str = (char *)(state ? "t" : "f");

	return val;
}

static void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;
	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval + 1;	/* just strip the '-' */
	else
	{
		char   *newval = (char *) palloc(strlen(oldval) + 2);

		*newval = '-';
		strcpy(newval + 1, oldval);
		v->val.str = newval;
	}
}

static Value* integerToString(Value *v)
{
    Assert(IsA(v, Integer));
    long num = intVal(v);
    const int max_len_long_type = 11;
    char* str = (char*)palloc0(sizeof(char) * (max_len_long_type + 1));
    errno_t rc = sprintf_s(str, max_len_long_type + 1, "%ld", num);
    securec_check_ss(rc, "\0", "\0");
	Value* ret = makeString(str);
    pfree(v);
    return ret;
}

#include "hint_scan.inc"

