void
pgtsql_parser_init(base_yy_extra_type *yyext)
{
	parser_init(yyext);
}

static void
pgtsql_base_yyerror(YYLTYPE * yylloc, core_yyscan_t yyscanner, const char *msg)
{
	base_yyerror(yylloc, yyscanner, msg);
}

/* TsqlSystemFuncName2()
 * Build a properly-qualified reference to a tsql built-in function.
 */
List *
TsqlSystemFuncName2(char *name)
{
	return list_make2(makeString("sys"), makeString(name));
}

static List* make_func_call_func(List* funcname,  List* args)
{
	FuncCall		*func = NULL;
	ResTarget	*restarget = NULL;

	func = (FuncCall*)makeNode(FuncCall);
	func->funcname = funcname;
	func->args = args;
	func->agg_star = FALSE;
	func->agg_distinct = FALSE;
	func->location = -1;
	func->call_func = false;

	restarget = makeNode(ResTarget);
	restarget->name = NULL;
	restarget->indirection = NIL;
	restarget->val = (Node *)func;
	restarget->location = -1;

	return (list_make1(restarget));
}

static List* make_no_reseed_func(char* table_name, bool with_no_msgs, bool reseed_to_max)
{
    List* funcname = list_make1(makeString("dbcc_check_ident_no_reseed"));
	List* args = list_make3(makeStringConst(table_name, -1), makeBoolConst(with_no_msgs, false), makeBoolConst(reseed_to_max, false));
	return make_func_call_func(funcname, args);
}


static List* make_reseed_func(char* table_name, Node* new_seed, bool with_no_msgs)
{
    List* funcname = list_make1(makeString("dbcc_check_ident_reseed"));
	Node* cast_node = makeTypeCast(new_seed, SystemTypeName("int8"), NULL, NULL, NULL, ((A_Const*)new_seed)->location);
	List* args = list_make3(makeStringConst(table_name, -1), cast_node, makeBoolConst(with_no_msgs, false));
	return make_func_call_func(funcname, args);
}


static char* quote_identifier_wrapper(char* ident, core_yyscan_t yyscanner)
{
	if ((pg_yyget_extra(yyscanner))->core_yy_extra.ident_quoted) {
		return pstrdup(quote_identifier((const char*)ident));
	} else {
		return ident;
	}
}

// To make a node for anonymous block
static Node *
TsqlMakeAnonyBlockFuncStmt(int flag, const char *str)
{
	DoStmt *n = makeNode(DoStmt);
	char *str_body	= NULL;
	DefElem * body	= NULL;
	errno_t		rc = EOK;

	if (BEGIN_P == flag)
	{
		int len1 = strlen("DECLARE \nBEGIN ");
		int len2 = strlen(str);
		str_body = (char *)palloc(len1 + len2 + 1);
		rc = strncpy_s(str_body, len1 + len2 + 1, "DECLARE \nBEGIN ",len1);
		securec_check(rc, "\0", "\0");
		rc = strcpy_s(str_body + len1, len2 + 1, str);
		securec_check(rc, "\0", "\0");
	}
	else
	{
		int len1 = strlen("DECLARE ");
		int len2 = strlen(str);
		str_body = (char *)palloc(len1 + len2 + 1);
		rc = strncpy_s(str_body, len1 + len2 + 1, "DECLARE ", len1);
		securec_check(rc, "\0", "\0");
		rc = strcpy_s(str_body + len1, len2 + 1, str);
		securec_check(rc, "\0", "\0");
	}

	body = makeDefElem("as", (Node*)makeString(str_body));
	if (get_language_oid("pltsql", true) != InvalidOid) {
		n->args = list_make1(makeDefElem("language", (Node *)makeString("pltsql")));
	} else {
		n->args = list_make1(makeDefElem("language", (Node *)makeString("plpgsql")));
	}

	n->args = lappend( n->args, body);

	return (Node*)n;
}

#include "scan-backend.inc"
#undef SCANINC
