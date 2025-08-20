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
	Node* cast_node = makeTypeCast(new_seed, SystemTypeName("int16"), NULL, NULL, NULL, ((A_Const*)new_seed)->location);
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
static Node *
makeTSQLHexStringConst(char *str, int location)
{
    A_Const    *n = makeNode(A_Const);
    n->val.type = T_TSQL_HexString;
    n->val.val.str = str;
    n->location = location;
    return (Node *) n;
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

/* TsqlFunctionTryCast -- An implementation of the TRY_CAST function
 *
 * Returns NULL on some of the error cases rather than throw out the error
 */
Node* TsqlFunctionTryCast(Node* arg, TypeName* typname, int location)
{
    Node* result;
    int32 typmod;
    Oid type_oid;

    typenameTypeIdAndMod(NULL, typname, &type_oid, &typmod);

    if (type_oid == INT2OID) {
        result = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_try_cast_floor_smallint"), list_make1(arg), location);
    } else if (type_oid == INT4OID) {
        result = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_try_cast_floor_int"), list_make1(arg), location);
    } else if (type_oid == INT8OID) {
        result = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_try_cast_floor_bigint"), list_make1(arg), location);
    } else {
        Node* targetType = makeTypeCast(makeNullAConst(location), typname, NULL, NULL, NULL, location);
        List* args;

        switch (arg->type) {
            case T_A_Const:
            case T_TypeCast:
            case T_FuncCall:
            case T_A_Expr:
                args = list_make3(arg, targetType, makeIntConst(typmod, location));
                break;
            default:
                args = list_make3(makeTypeCast(arg, makeTypeName("text"), NULL, NULL, NULL, location), targetType,
                                  makeIntConst(typmod, location));
        }

        result = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_try_cast_to_any"), args, location);
    }

    return result;
}

/* TsqlFunctionConvert -- An implementation of CONVERT and TRY_CONVERT
 *
 * Converts an input type to another type with a possible specified style.
 * is_try is used to decide whether this is a CONVERT or TRY_CONVERT function
 */
Node* TsqlFunctionConvert(TypeName* typname, Node* arg, Node* style, bool is_try, int location)
{
    List* args;
    char* typename_string;

    Node* try_const = makeBoolAConst(is_try, location);
    if (style) {
        args = list_make3(arg, try_const, style);
    } else {
        args = list_make2(arg, try_const);
    }
    return DoTypeCast(typname, is_try, arg, args, location);
}

/**
 * A Helper function for TsqlFunctionConvert
 */
static Node* DoTypeCast(TypeName* typname, bool is_try, Node* arg, List* args, int location)
{
    Node* result;
    char* typename_string;
    Node* helperFuncCall = NULL;
    int32 typmod;
    Oid type_oid;
    typenameTypeIdAndMod(NULL, typname, &type_oid, &typmod);
    typename_string = TypeNameToString(typname);
    if (type_oid == DATEOID) {
        result = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_date"), args, location);
    } else if (type_oid == TIMEOID) {
        helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_time"),
                                             lcons(makeIntConst(typmod, location), args), location);
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else if (type_oid == TIMESTAMPOID) {
        helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_datetime2"),
                                             lcons(makeIntConst(typmod, location), args), location);
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else if (type_oid == TIMESTAMPTZOID) {
        helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_datetimeoffset"),
                                             lcons(makeIntConst(typmod, location), args), location);
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else if (type_oid == SMALLDATETIMEOID) {
        helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_smalldatetime"),
                                             lcons(makeIntConst(typmod, location), args), location);
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else if (is_qualifed_char_type(typename_string)) {
        typename_string = format_type_extended(VARCHAROID, typmod, FORMAT_TYPE_TYPEMOD_GIVEN);
        args = lcons(makeStringConst(typename_string, typname->location), args);
        helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_varchar"), args, location);
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else if (strcmp(typename_string, "varbinary") == 0) {
        if (typmod > VARHDRSZ) {
            helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_varbinary"),
                                                 lcons(makeIntConst(typmod - VARHDRSZ, location), args), location);
        } else {
            helperFuncCall = (Node*)makeFuncCall(TsqlSystemFuncName2("shark_conv_helper_to_varbinary"),
                                                 lcons(makeIntConst(typmod, location), args), location);
        }
        result = makeTypeCast(helperFuncCall, typname, NULL, NULL, NULL, location);
    } else {
        if (is_try) {
            result = TsqlFunctionTryCast(arg, typname, location);
        } else {
            result = makeTypeCast(arg, typname, NULL, NULL, NULL, location);
        }
    }
    return result;
}

static bool is_qualifed_char_type(char* typename_string)
{
    return (strcmp(typename_string, "pg_catalog.varchar") == 0) ||
           (strcmp(typename_string, "pg_catalog.nvarchar2") == 0) ||
           (strcmp(typename_string, "pg_catalog.bpchar") == 0);
}

#include "scan-backend.inc"
#undef SCANINC
