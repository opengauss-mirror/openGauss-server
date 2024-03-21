%{
/* -------------------------------------------------------------------------
 *
 * gram.y				- Parser for the PL/pgSQL procedural language
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/gram.y
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plpgsql_domain.h"
#include "utils/plpgsql.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/gs_package.h"
#include "catalog/gs_dependencies_fn.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "commands/typecmds.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "parser/keywords.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pl_package.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "knl/knl_session.h"
#include "utils/varbit.h"

#include <limits.h>


#define LENGTH_OF_BRACKETS_AND_DOT 4
#define LENGTH_OF_QUOTATION_MARKS 2
#define IS_ANONYMOUS_BLOCK \
    (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL && \
        strcmp(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_signature, "inline_code_block") ==0)
#define IS_PACKAGE \
    (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL && \
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL)
/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
    do { \
        if (N) \
            (Current) = (Rhs)[1]; \
        else \
            (Current) = (Rhs)[0]; \
    } while (0)

/*
 * Helper function to record statements label so that GOTO statements could reach
 * there.
 */
static inline void
record_stmt_label(char * label, PLpgSQL_stmt *stmt)
{
    /*
     * Note, we do plpgsql parsing under "PL/pgSQL Function" memory context,
     * so palloc memory for goto-lable element and the global array under it.
     */
    PLpgSQL_gotoLabel *gl =
            (PLpgSQL_gotoLabel *)palloc0(sizeof(PLpgSQL_gotoLabel));
    gl->label = label;
    gl->stmt = stmt;
    u_sess->plsql_cxt.curr_compile_context->goto_labels = lappend(u_sess->plsql_cxt.curr_compile_context->goto_labels, (void *)gl);
}

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


typedef struct
{
    int			location;
    int			leaderlen;
} sql_error_callback_arg;

#define parser_errposition(pos)  plpgsql_scanner_errposition(pos)

union YYSTYPE;					/* need forward reference for tok_is_keyword */


static	bool			tok_is_keyword(int token, union YYSTYPE *lval,
                                       int kw_token, const char *kw_str);
static	void			word_is_not_variable(PLword *word, int location);
static	void			cword_is_not_variable(PLcword *cword, int location);
static	void			current_token_is_not_variable(int tok);
static void yylex_inparam(StringInfoData* func_inparam,
                                            int *params,
                                            int * tok,
                                            int *tableof_func_dno,
                                            int *tableof_var_dno);
static int   	yylex_outparam(char** fieldnames,
                               int *varnos,
                               int nfields,
                               PLpgSQL_row **row,
                               PLpgSQL_rec **rec,
                               int *token,
                               int *varno,
                               bool is_push_back,
                               bool overload = false);

static bool is_function(const char *name, bool is_assign, bool no_parenthesis, List* funcNameList = NULL);
static bool is_unreservedkeywordfunction(int kwnum, bool no_parenthesis, const char *name);
static bool is_paren_friendly_datatype(TypeName *name);
static void 	plpgsql_parser_funcname(const char *s, char **output,
                                        int numidents);
static PLpgSQL_stmt 	*make_callfunc_stmt(const char *sqlstart,
                                int location, bool is_assign, bool eaten_first_token, List* funcNameList = NULL, int arrayFuncDno = -1, bool isCallFunc = false);
static PLpgSQL_stmt 	*make_callfunc_stmt_no_arg(const char *sqlstart, int location, bool withsemicolon = false, List* funcNameList = NULL);
static PLpgSQL_expr 	*read_sql_construct6(int until,
                                             int until2,
                                             int until3,
                                             int until4,
                                             int until5,
                                             int until6,
                                             const char *expected,
                                             const char *sqlstart,
                                             bool isexpression,
                                             bool valid_sql,
                                             bool trim,
                                             int *startloc,
                                             int *endtoken,
                                             DList* tokenstack = NULL,
                                             bool issaveexpr = false);
                                             
static	PLpgSQL_expr	*read_sql_construct(int until,
                                            int until2,
                                            int until3,
                                            const char *expected,
                                            const char *sqlstart,
                                            bool isexpression,
                                            bool valid_sql,
                                            bool trim,
                                            int *startloc,
                                            int *endtoken);
static	PLpgSQL_expr	*read_sql_expression(int until,
                                             const char *expected);
static	PLpgSQL_expr	*read_sql_expression2(int until, int until2,
                                              const char *expected,
                                              int *endtoken);
static	PLpgSQL_expr	*read_sql_stmt(const char *sqlstart);
static	PLpgSQL_type	*read_datatype(int tok);
static  PLpgSQL_stmt	*parse_lob_open_close(int location);
static	PLpgSQL_stmt	*make_execsql_stmt(int firsttoken, int location);
static	PLpgSQL_stmt_fetch *read_fetch_direction(void);
static	void			 complete_direction(PLpgSQL_stmt_fetch *fetch,
                                            bool *check_FROM);
static	PLpgSQL_stmt	*make_return_stmt(int location);
static	PLpgSQL_stmt	*make_return_next_stmt(int location);
static	PLpgSQL_stmt	*make_return_query_stmt(int location);
static  PLpgSQL_stmt	*make_case(int location, PLpgSQL_expr *t_expr,
                                   List *case_when_list, List *else_stmts);
static	char			*NameOfDatum(PLwdatum *wdatum);
static  char                    *CopyNameOfDatum(PLwdatum *wdatum);
static	void			 check_assignable(PLpgSQL_datum *datum, int location);
static	bool			 read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row,
                                          bool *strict, int firsttoken, bool bulk_collect);
static	PLpgSQL_row		*read_into_scalar_list(char *initial_name,
                                               PLpgSQL_datum *initial_datum,
                                               int initial_dno,
                                               int initial_location);
static	PLpgSQL_row		*read_into_array_table_scalar_list(char *initial_name,
                                               PLpgSQL_datum *initial_datum,
                                               int initial_dno,
                                               int initial_location,
                                               bool bulk_collect);
static void             read_using_target(List **in_expr, PLpgSQL_row **out_row);

static PLpgSQL_row  *read_into_placeholder_scalar_list(char *initial_name,
                      int initial_location);

static	PLpgSQL_row		*make_scalar_list1(char *initial_name,
                                           PLpgSQL_datum *initial_datum,
                                           int initial_dno,
                                           int lineno, int location);
static	void			 check_sql_expr(const char *stmt, int location,
                                        int leaderlen);
static	void			 plpgsql_sql_error_callback(void *arg);
static	PLpgSQL_type	*parse_datatype(const char *string, int location);
static	void			 check_labels(const char *start_label,
                                      const char *end_label,
                                      int end_location);
static	PLpgSQL_expr	*read_cursor_args(PLpgSQL_var *cursor,
                                          int until, const char *expected);
static	List			*read_raise_options(void);
static void read_signal_sqlstate(PLpgSQL_stmt_signal *newp, int tok);
static void read_signal_condname(PLpgSQL_stmt_signal *newp, int tok);
static void read_signal_set(PLpgSQL_stmt_signal *newp, int tok);
static List *read_signal_items(void);
static  int             errstate = ERROR;
static DefElem* get_proc_str(int tok);
static char* get_init_proc(int tok);
static char* get_attrname(int tok);
static AttrNumber get_assign_attrno(PLpgSQL_datum* target,  char* attrname);
static void raw_parse_package_function(char* proc_str, int location, int leaderlen);
static void checkFuncName(List* funcname);
static void IsInPublicNamespace(char* varname);
static void CheckDuplicateCondition (char* name);
static void SetErrorState();
static void AddNamespaceIfNeed(int dno, char* ident);
static void AddNamespaceIfPkgVar(const char* ident, IdentifierLookup save_IdentifierLookup);
bool plpgsql_is_token_keyword(int tok);
static void check_bulk_into_type(PLpgSQL_row* row);
static void check_table_index(PLpgSQL_datum* datum, char* funcName);
static PLpgSQL_type* build_type_from_record_var(int dno, int location);
static PLpgSQL_type * build_array_type_from_elemtype(PLpgSQL_type *elem_type);
static PLpgSQL_var* plpgsql_build_nested_variable(PLpgSQL_var *nest_table, bool isconst, char* name, int lineno);
static void read_multiset(StringInfoData* ds, char* tableName1, Oid typeOid1);
static Oid get_table_type(PLpgSQL_datum* datum);
static void read_multiset(StringInfoData* ds, char* tableName1, Oid typeOid1);
static Oid get_table_type(PLpgSQL_datum* datum);
static Node* make_columnDef_from_attr(PLpgSQL_rec_attr* attr);
static TypeName* make_typename_from_datatype(PLpgSQL_type* datatype);
static Oid plpgsql_build_package_record_type(const char* typname, List* list, bool add2namespace);
static void  plpgsql_build_package_array_type(const char* typname, Oid elemtypoid, char arraytype, TypeDependExtend* dependExtend = NULL);
static void plpgsql_build_package_refcursor_type(const char* typname);
int plpgsql_yylex_single(void);
static void get_datum_tok_type(PLpgSQL_datum* target, int* tok_flag);
static bool copy_table_var_indents(char* tableName, char* idents, int tablevar_namelen);
static int push_back_token_stack(DList* tokenstack);
static int read_assignlist(bool is_push_back, int* token);
static void plpgsql_cast_reference_list(List* idents, StringInfoData* ds, bool isPkgVar);
static bool PkgVarNeedCast(List* idents);
static void CastArrayNameToArrayFunc(StringInfoData* ds, List* idents, bool needDot = true);
static Oid get_table_index_type(PLpgSQL_datum* datum, int *tableof_func_dno);
static int get_nest_tableof_layer(PLpgSQL_var *var, const char *typname, int errstate);
static void SetErrorState();
static void CheckDuplicateFunctionName(List* funcNameList);
static void check_autonomous_nest_tablevar(PLpgSQL_var* var);
static bool is_unreserved_keyword_func(const char *name);
static PLpgSQL_stmt *funcname_is_call(const char* name, int location);
#ifndef ENABLE_MULTIPLE_NODES
static PLpgSQL_type* build_type_from_cursor_var(PLpgSQL_var* var);
static bool checkAllAttrName(TupleDesc tupleDesc);
static void BuildForQueryVariable(PLpgSQL_expr* expr, PLpgSQL_row **row, PLpgSQL_rec **rec,
    const char* refname, int lineno);
#endif
static Oid createCompositeTypeForCursor(PLpgSQL_var* var, PLpgSQL_expr* expr);
static void check_record_type(PLpgSQL_rec_type * var_type, int location, bool check_nested = true);
static void check_record_nest_tableof_index(PLpgSQL_datum* datum);
static void check_tableofindex_args(int tableof_var_dno, Oid argtype);
static bool need_build_row_for_func_arg(PLpgSQL_rec **rec, PLpgSQL_row **row, int out_arg_num, int all_arg, int *varnos, char *p_argmodes);
static void processFunctionRecordOutParam(int varno, Oid funcoid, int* outparam);
%}

%expect 0
%name-prefix "plpgsql_yy"
%locations

%union {
        core_YYSTYPE			core_yystype;
        /* these fields must match core_YYSTYPE: */
        int						ival;
        char					*str;
        const char				*keyword;

        PLword					word;
        PLcword					cword;
        PLwdatum				wdatum;
        bool					boolean;
        Oid						oid;
        VarName					*varname;
        struct
        {
            char *name;
            int  lineno;
            PLpgSQL_datum   *scalar;
            PLpgSQL_rec		*rec;
            PLpgSQL_row		*row;
            int  dno;
        }						forvariable;
        struct
        {
            char *label;
            int  n_initvars;
            int  *initvarnos;
            bool isAutonomous;
        }						declhdr;
        struct
        {
            List *stmts;
            char *end_label;
            int   end_label_location;
        }						loop_body;
        struct
        {
            List *stmts;
            char *end_label;
            int   end_label_location;
        }                                               while_body;
        struct
        {
            PLpgSQL_expr                    *expr;
            char *end_label;
            int   end_label_location;
        }                                               repeat_condition;
        struct
        {
            PLpgSQL_expr  *expr;
            int            endtoken;
        }                                               expr_until_while_loop;
        List					*list;
        PLpgSQL_type			*dtype;
        PLpgSQL_datum			*datum;
        PLpgSQL_var				*var;
        PLpgSQL_expr			*expr;
        PLpgSQL_stmt			*stmt;
        PLpgSQL_condition		*condition;
        PLpgSQL_exception		*exception;
        PLpgSQL_exception_block	*exception_block;
        PLpgSQL_nsitem			*nsitem;
        PLpgSQL_diag_item		*diagitem;
        PLpgSQL_stmt_fetch		*fetch;
        PLpgSQL_case_when		*casewhen;
        PLpgSQL_declare_handler declare_handler_type;
        PLpgSQL_rec_attr	*recattr;
        Node                            *plnode;
        DefElem             *def;
}

%type <plnode> assign_el
%type <declhdr> decl_sect
%type <varname> decl_varname declare_condname
%type <list> decl_varname_list
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval decl_rec_defval decl_cursor_query
%type <dtype>	decl_datatype
%type <oid>		decl_collate
%type <datum>	decl_cursor_args
%type <list>	decl_cursor_arglist assign_list
%type <nsitem>	decl_aliasitem

%type <expr>	expr_until_semi expr_until_rightbracket
%type <expr>	expr_until_then expr_until_loop opt_expr_until_when
%type <expr_until_while_loop> expr_until_while_loop
%type <expr>	opt_exitcond
%type <expr>	repeat_condition_expr

%type <ival>	assign_var foreach_slice error_code cursor_variable
%type <datum>	decl_cursor_arg
%type <forvariable>	for_variable
%type <stmt>	for_control forall_control

%type <def>		spec_proc
%type <str>		any_identifier opt_block_label opt_label goto_block_label label_name init_proc
%type <str>		opt_rollback_to opt_savepoint_name savepoint_name attr_name

%type <list>	proc_sect proc_stmts stmt_elsifs stmt_else forall_body
%type <loop_body>	loop_body
%type <while_body>       while_body
%type <repeat_condition>  repeat_condition
%type <stmt>	proc_stmt pl_block
%type <stmt>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit stmt_goto label_stmts label_stmt
%type <stmt>	stmt_return stmt_raise stmt_execsql
%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag
%type <stmt>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <stmt>	stmt_commit stmt_rollback stmt_savepoint
%type <stmt>	stmt_case stmt_foreach_a
%type <stmt>	stmt_signal stmt_resignal

%type <list>	proc_exceptions declare_stmts
%type <exception_block> exception_sect declare_sect_b
%type <exception>	proc_exception declare_stmt 
%type <condition>	proc_conditions proc_condition cond_list cond_element
%type <declare_handler_type>    handler_type

%type <casewhen>	case_when
%type <list>	case_when_list opt_case_else

%type <boolean>	getdiag_area_opt
%type <list>	getdiag_list condition_information
%type <diagitem> getdiag_list_item condition_information_item
%type <ival>	getdiag_item getdiag_target condition_number condition_number_item condition_information_item_name statement_information_item_name
%type <ival>	varray_var
%type <ival>	table_var
%type <ival>	record_var
%type <expr>	expr_until_parenthesis
%type <ival>	opt_scrollable
%type <fetch>	opt_fetch_direction
%type <expr>	fetch_limit_expr
%type <datum>	fetch_into_target
%type <condition>    condition_value

%type <keyword>	unreserved_keyword

%type <list>	record_attr_list
%type <recattr>	record_attr
%type <boolean> opt_save_exceptions

%type <keyword> unreserved_keyword_func

/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str>	IDENT FCONST SCONST BCONST VCONST XCONST Op CmpOp CmpNullOp COMMENTSTRING SET_USER_IDENT SET_IDENT UNDERSCORE_CHARSET
%token <ival>	ICONST PARAM
%token			TYPECAST ORA_JOINOP DOT_DOT COLON_EQUALS PARA_EQUALS SET_IDENT_SESSION SET_IDENT_GLOBAL

/*
 * Other tokens recognized by plpgsql's lexer interface layer (pl_scanner.c).
 */
%token <word>		T_WORD		/* unrecognized simple identifier */
%token <cword>		T_CWORD		/* unrecognized composite identifier */
%token <wdatum>		T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token <word>		T_PLACEHOLDER		/* place holder , for IN/OUT parameters */
%token <word>		T_LABELLOOP T_LABELWHILE T_LABELREPEAT
%token <wdatum>		T_VARRAY T_ARRAY_FIRST  T_ARRAY_LAST  T_ARRAY_COUNT  T_ARRAY_EXISTS  T_ARRAY_PRIOR  T_ARRAY_NEXT  T_ARRAY_DELETE  T_ARRAY_EXTEND  T_ARRAY_TRIM  T_VARRAY_VAR  T_RECORD
%token <wdatum>		T_TABLE T_TABLE_VAR T_PACKAGE_VARIABLE
%token <wdatum>     T_PACKAGE_CURSOR_ISOPEN T_PACKAGE_CURSOR_FOUND T_PACKAGE_CURSOR_NOTFOUND T_PACKAGE_CURSOR_ROWCOUNT
%token				LESS_LESS
%token				GREATER_GREATER

%token	<wdatum>	T_REFCURSOR		/* token for cursor type */
%token				T_SQL_ISOPEN
%token				T_SQL_FOUND
%token				T_SQL_NOTFOUND
%token				T_SQL_ROWCOUNT
%token				T_SQL_BULK_EXCEPTIONS
%token				T_CURSOR_ISOPEN
%token				T_CURSOR_FOUND
%token				T_CURSOR_NOTFOUND
%token				T_CURSOR_ROWCOUNT
%token				T_DECLARE_CURSOR
%token				T_DECLARE_CONDITION
%token				T_DECLARE_HANDLER

/*
 * Keyword tokens.  Some of these are reserved and some are not;
 * see pl_scanner.c for info.  Be sure unreserved keywords are listed
 * in the "unreserved_keyword" production below.
 */
%token <keyword>	K_ABSOLUTE
%token <keyword>	K_ALIAS
%token <keyword>	K_ALL
%token <keyword>	K_ALTER
%token <keyword>	K_ARRAY
%token <keyword>	K_AS
%token <keyword>	K_BACKWARD
%token <keyword>	K_BEGIN
%token <keyword>	K_BULK
%token <keyword>	K_BY
%token <keyword>        K_CALL
%token <keyword>	K_CASE
%token <keyword>	K_CATALOG_NAME
%token <keyword>	K_CLASS_ORIGIN
%token <keyword>	K_CLOSE
%token <keyword>	K_COLLATE
%token <keyword>	K_COLLECT
%token <keyword>	K_COLUMN_NAME
%token <keyword>	K_COMMIT
%token <keyword>	K_CONDITION
%token <keyword>	K_CONSTANT
%token <keyword>	K_CONSTRAINT_CATALOG
%token <keyword>	K_CONSTRAINT_NAME
%token <keyword>	K_CONSTRAINT_SCHEMA
%token <keyword>	K_CONTINUE
%token <keyword>	K_CURRENT
%token <keyword>	K_CURSOR
%token <keyword>	K_CURSOR_NAME
%token <keyword>	K_DEBUG
%token <keyword>	K_DECLARE
%token <keyword>	K_DEFAULT
%token <keyword>	K_DELETE
%token <keyword>	K_DETAIL
%token <keyword>	K_DETERMINISTIC
%token <keyword>	K_DIAGNOSTICS
%token <keyword>	K_DISTINCT
%token <keyword>        K_DO
%token <keyword>	K_DUMP
%token <keyword>	K_ELSE
%token <keyword>	K_ELSIF
%token <keyword>	K_END
%token <keyword>	K_ERRCODE
%token <keyword>	K_ERROR
%token <keyword>    K_EXCEPT
%token <keyword>	K_EXCEPTION
%token <keyword>	K_EXCEPTIONS
%token <keyword>	K_EXECUTE
%token <keyword>	K_EXIT
%token <keyword>	K_FALSE
%token <keyword>	K_FETCH
%token <keyword>	K_FIRST
%token <keyword>	K_FOR
%token <keyword>	K_FORALL
%token <keyword>	K_FOREACH
%token <keyword>	K_FORWARD
%token <keyword>	K_FOUND
%token <keyword>	K_FROM
%token <keyword>	K_FUNCTION
%token <keyword>	K_GET
%token <keyword>	K_GOTO
%token <keyword>	K_HANDLER
%token <keyword>	K_HINT
%token <keyword>	K_IF
%token <keyword>	K_IMMEDIATE
%token <keyword>    K_INSTANTIATION
%token <keyword>	K_IN
%token <keyword>	K_INDEX
%token <keyword>	K_INFO
%token <keyword>	K_INSERT
%token <keyword>	K_INTERSECT
%token <keyword>	K_INTO
%token <keyword>	K_IS
%token <keyword>        K_ITERATE
%token <keyword>	K_LAST
%token <keyword>        K_LEAVE
%token <keyword>	K_LIMIT
%token <keyword>	K_LOG
%token <keyword>	K_LOOP
%token <keyword>    K_MERGE
%token <keyword>	K_MESSAGE
%token <keyword>	K_MESSAGE_TEXT
%token <keyword>	K_MOVE
%token <keyword>    K_MULTISET
%token <keyword>    K_MULTISETS
%token <keyword>    K_MYSQL_ERRNO
%token <keyword>    K_NUMBER
%token <keyword>	K_NEXT
%token <keyword>	K_NO
%token <keyword>	K_NOT
%token <keyword>	K_NOTICE
%token <keyword>	K_NULL
%token <keyword>	K_OF
%token <keyword>	K_OPEN
%token <keyword>	K_OPTION
%token <keyword>	K_OR
%token <keyword>	K_OUT
%token <keyword>    K_PACKAGE
%token <keyword>	K_PERFORM
%token <keyword>	K_PG_EXCEPTION_CONTEXT
%token <keyword>	K_PG_EXCEPTION_DETAIL
%token <keyword>	K_PG_EXCEPTION_HINT
%token <keyword>	K_PRAGMA
%token <keyword>	K_PRIOR
%token <keyword>	K_PROCEDURE
%token <keyword>	K_QUERY
%token <keyword>	K_RAISE
%token <keyword>	K_RECORD
%token <keyword>	K_REF
%token <keyword>	K_RELATIVE
%token <keyword>	K_RELEASE
%token <keyword>	K_REPEAT
%token <keyword>	K_REPLACE
%token <keyword>	K_RESULT_OID
%token <keyword>	K_RESIGNAL
%token <keyword>	K_RETURN
%token <keyword>	K_RETURNED_SQLSTATE
%token <keyword>	K_REVERSE
%token <keyword>	K_ROLLBACK
%token <keyword>	K_ROWTYPE
%token <keyword>	K_ROW_COUNT
%token <keyword>	K_SAVE
%token <keyword>	K_SAVEPOINT
%token <keyword>	K_SCHEMA_NAME
%token <keyword>	K_SELECT
%token <keyword>	K_SCROLL
%token <keyword>	K_SIGNAL
%token <keyword>	K_SLICE
%token <keyword>	K_SQLEXCEPTION
%token <keyword>	K_SQLSTATE
%token <keyword>	K_SQLWARNING
%token <keyword>	K_STACKED
%token <keyword>	K_STRICT
%token <keyword>	K_SUBCLASS_ORIGIN
%token <keyword>	K_SYS_REFCURSOR
%token <keyword>	K_TABLE
%token <keyword>	K_TABLE_NAME
%token <keyword>	K_THEN
%token <keyword>	K_TO
%token <keyword>	K_TRUE
%token <keyword>	K_TYPE
%token <keyword>	K_UNION
%token <keyword>	K_UNTIL
%token <keyword>	K_UPDATE
%token <keyword>	K_USE_COLUMN
%token <keyword>	K_USE_VARIABLE
%token <keyword>	K_USING
%token <keyword>	K_VARIABLE_CONFLICT
%token <keyword>	K_VARRAY
%token <keyword>	K_WARNING
%token <keyword>	K_WHEN
%token <keyword>	K_WHILE
%token <keyword>	K_WITH

%%

pl_body         : pl_package_spec
                | pl_function
                | pl_package_init
                ;
pl_package_spec : K_PACKAGE { SetErrorState(); } decl_sect K_END
                    {
                        int nDatums = 0;
                        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile!=NULL) {
                            nDatums = u_sess->plsql_cxt.curr_compile_context->plpgsql_nDatums;
                        } else {
                            nDatums = u_sess->plsql_cxt.curr_compile_context->plpgsql_pkg_nDatums;
                        }

                        for (int i = 0; i < nDatums; i++) {
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[i]->ispkg = true;
                        }
                        PLpgSQL_nsitem* ns_cur = plpgsql_ns_top();
                        while (ns_cur != NULL) {
                            if (ns_cur != NULL && ns_cur->pkgname == NULL) {
                                ns_cur->pkgname =  u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature;
                            }
                            ns_cur = ns_cur->prev;
                        }
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->n_initvars = $3.n_initvars;
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->initvarnos = $3.initvarnos;
                    }
                ;
pl_package_init : K_INSTANTIATION { SetErrorState(); } init_proc
                    {
                        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL)
                        {
                            List* raw_parsetree_list = NULL;
                            u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                            raw_parsetree_list = raw_parser($3);
                            DoStmt* stmt;
                            stmt = (DoStmt *)linitial(raw_parsetree_list);
                            stmt->isExecuted = false;
                            if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling) {
                                stmt->isSpec = true;
                            } else {
                                stmt->isSpec = false;
                            }
                            List *proc_list = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->proc_list;
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->proc_list = lappend(proc_list,stmt);
                        } else {
                            yyerror("instantiation only use in package compile");
                        }
                    }

pl_function		: comp_options  { SetErrorState(); } pl_block opt_semi
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_parse_result = (PLpgSQL_stmt_block *) $3;
                    }
                ;

comp_options	:
                | comp_options comp_option
                ;

comp_option		: '#' K_OPTION K_DUMP
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_DumpExecTree = true;
                    }
                | '#' K_VARIABLE_CONFLICT K_ERROR
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_ERROR;
                    }
                | '#' K_VARIABLE_CONFLICT K_USE_VARIABLE
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_VARIABLE;
                    }
                | '#' K_VARIABLE_CONFLICT K_USE_COLUMN
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_COLUMN;
                    }
                ;

opt_semi		:
                | ';'
                ;

pl_block		: decl_sect K_BEGIN declare_sect_b proc_sect exception_sect K_END opt_label
                    {
                        PLpgSQL_stmt_block *newp;

                        newp = (PLpgSQL_stmt_block *)palloc0(sizeof(PLpgSQL_stmt_block));

                        newp->cmd_type	= PLPGSQL_STMT_BLOCK;
                        newp->lineno		= plpgsql_location_to_lineno(@2);
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->label		= $1.label;
                        newp->isAutonomous = $1.isAutonomous;
                        newp->n_initvars = $1.n_initvars;
                        newp->initvarnos = $1.initvarnos;
                        newp->body		= $4;
                        if ($3 != NULL) {
                            if ($5 != NULL) {
                                const char* message = "declare handler and exception cannot be used at the same time";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                        errmsg("declare handler and exception cannot be used at the same time")));
                            } else {
                                newp->exceptions	= $3;
                                newp->isDeclareHandlerStmt  = true;
                            }
                        } else {
                            newp->exceptions	= $5;
                            newp->isDeclareHandlerStmt  = false;
                        }

                        check_labels($1.label, $7, @7);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1.label, (PLpgSQL_stmt *)newp);
                    }
                ;

declare_sect_b  :
                    {
                        /* done with decls, so resume identifier lookup */
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$ = NULL; 
                    }
                | declare_stmts
                    {
                        /* done with decls, so resume identifier lookup */
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        if ($1 == NULL) {
                            $$ = NULL;
                        } else {
                            PLpgSQL_exception_block *newp = (PLpgSQL_exception_block *)palloc(sizeof(PLpgSQL_exception_block));
                            newp->exc_list = $1;

                            $$ = newp;
                        }
                    }
                ;

declare_stmts   : declare_stmts declare_stmt
                    {
                        if ($2 == NULL)
                            $$ = $1;
                        else
                            $$ = lappend($1, $2);
                    }
                | declare_stmt
                    {
                        if ($1 == NULL) {
                            $$ = NULL;
                        } else {
                            $$ = list_make1($1);
                        }
                    }
                ;

declare_stmt    : T_DECLARE_CURSOR decl_varname K_CURSOR opt_scrollable 
                    {
                        IsInPublicNamespace($2->name);
                        plpgsql_ns_push($2->name); 
                    }
                    decl_cursor_args decl_is_for decl_cursor_query
                    {
                        int tok = -1;
                        plpgsql_peek(&tok);
                        if (tok != K_DECLARE) {
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        }
                        PLpgSQL_var *newp;

                        /* pop local namespace for cursor args */
                        plpgsql_ns_pop();

                        newp = (PLpgSQL_var *)
                                plpgsql_build_variable($2->name, $2->lineno,
                                                    plpgsql_build_datatype(REFCURSOROID,
                                                                            -1,
                                                                            InvalidOid),
                                                                            true);

                        newp->cursor_explicit_expr = $8;
                        if ($6 == NULL)
                            newp->cursor_explicit_argrow = -1;
                        else
                            newp->cursor_explicit_argrow = $6->dno;
                        newp->cursor_options = CURSOR_OPT_FAST_PLAN | $4;
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        newp->datatype->cursorCompositeOid = IS_ANONYMOUS_BLOCK ? 
                            InvalidOid : createCompositeTypeForCursor(newp, $8);
                        pfree_ext($2->name);
                        pfree($2);
                        $$ = NULL;
                    }
                | T_DECLARE_CONDITION declare_condname K_CONDITION K_FOR condition_value ';'
                    {
                        int tok = -1;
                        plpgsql_peek(&tok);
                        if (tok != K_DECLARE) {
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        }
                        if ($2->name != NULL) {
                            if (pg_strcasecmp($2->name, "set") == 0) {
                                yyerror("syntax error, set is keyword.");
                            }
                            if (pg_strcasecmp($2->name, "sqlwarning") == 0) {
                                yyerror("syntax error, sqlwarning is keyword.");
                            }
                            if (pg_strcasecmp($2->name, "sqlexception") == 0) {
                                yyerror("syntax error, sqlexception is keyword.");
                            }
                        }
                        
                        CheckDuplicateCondition($2->name);
                        PLpgSQL_condition* cond = $5;
                        cond->condname = pstrdup($2->name);
                        PLpgSQL_condition* old = u_sess->plsql_cxt.curr_compile_context->plpgsql_conditions;
                        if (old != NULL) {
                            cond->next = old;
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_conditions = cond;
                        } else {
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_conditions = cond;
                        }

                        pfree_ext($2->name);
                        pfree($2);
                        $$ = NULL;
                    }
                | T_DECLARE_HANDLER handler_type K_HANDLER K_FOR cond_list proc_stmt
                    {
                        int tok = -1;
                        plpgsql_peek(&tok);
                        if (tok != K_DECLARE) {
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        }
                        PLpgSQL_exception *newp;

                        newp = (PLpgSQL_exception *)palloc0(sizeof(PLpgSQL_exception));
                        newp->lineno     = plpgsql_location_to_lineno(@1);
                        newp->handler_type = $2;
                        newp->conditions = $5;
                        newp->action	 = list_make1($6);

                        $$ = newp;
                    }
                ;

handler_type	: K_EXIT
                    { $$ = PLpgSQL_declare_handler::DECLARE_HANDLER_EXIT; }
                | K_CONTINUE
                    { $$ = PLpgSQL_declare_handler::DECLARE_HANDLER_CONTINUE; }
                ;
cond_list		: cond_list ',' cond_element
                    {
                        PLpgSQL_condition	*old;

                        for (old = $1; old->next != NULL; old = old->next)
                                /* skip */ ;
                        old->next = $3;
                        $$ = $1;
                    }
                | cond_element
                    {
                        $$ = $1;
                    }
                ;

cond_element	: any_identifier
                    {
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = plpgsql_parse_err_condition_b($1);
                    }
                | K_SQLSTATE
                    {
                        PLpgSQL_condition *newp;
                        newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                        /* next token should be a string literal */
                        char   *sqlstatestr;
                        yylex();
                        if (strcmp(yylval.str, "value") ==0) {
                            yylex();
                        }
                        sqlstatestr = yylval.str;
                        
                        if (strlen(sqlstatestr) != 5)
                            yyerror("invalid SQLSTATE code");
                        if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
                            yyerror("invalid SQLSTATE code");
                        if (strncmp(sqlstatestr, "00", 2) == 0) {
                            const char* message = "bad SQLSTATE";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                                        errmsg("bad SQLSTATE '%s'",sqlstatestr)));
                        }

                        newp->sqlerrstate = MAKE_SQLSTATE(sqlstatestr[0],
                                                          sqlstatestr[1],
                                                          sqlstatestr[2],
                                                          sqlstatestr[3],
                                                          sqlstatestr[4]);
                        newp->condname = NULL;
                        newp->sqlstate = pstrdup(sqlstatestr);
                        newp->next = NULL;
                        newp->isSqlvalue = false;
                        $$ = newp;
                    }
                | K_SQLWARNING
                    {
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = plpgsql_parse_err_condition_b("k_sqlwarning");
                    }
                | K_NOT K_FOUND
                    {
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = plpgsql_parse_err_condition_b("not_found");
                    }
                | K_SQLEXCEPTION
                    {
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = plpgsql_parse_err_condition_b("k_sqlexception");
                    }
                | ICONST
                    {
                        if ($1 == 0) {
                            const char* message = "Incorrect CONDITION value: '0'";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                                        errmsg("Incorrect CONDITION value: '0'")));
                        }
                        int num = $1;
                        char *sqlstatestr = (char *)palloc0(sizeof(char) * 6);
                        for (int i= 4; i >= 0; i--) {
                            sqlstatestr[i] = num % 10 + '0';
                            num /= 10;
                        }
                        sqlstatestr[5] = '\0';
                        if (num != 0) {
                            $$ = NULL;
                        }
                        
                        PLpgSQL_condition *newp = (PLpgSQL_condition *)palloc0(sizeof(PLpgSQL_condition));
                        newp->sqlerrstate = MAKE_SQLSTATE(sqlstatestr[0],
                                                          sqlstatestr[1],
                                              	          sqlstatestr[2],
                                              	          sqlstatestr[3],
                                              	          sqlstatestr[4]);
                        newp->condname = NULL;
                        newp->next = NULL;
                        newp->sqlstate = pstrdup(sqlstatestr);
                        newp->isSqlvalue = true;
                        $$ = newp;
                    }
                ;

declare_condname: T_WORD
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = $1.ident;
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;
                    }
                | unreserved_keyword
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;
                    }
                | T_VARRAY
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                | T_RECORD
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                | T_TABLE
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                | T_REFCURSOR
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                | T_TABLE_VAR
                    {
                        VarName* varname = NULL;
                        if ($1.idents != NIL) {
                            yyerror("syntax error");
                        }
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                | T_VARRAY_VAR
                    {
                        VarName* varname = NULL;
                        if ($1.idents != NIL || strcmp($1.ident, "bulk_exceptions") == 0) {
                            yyerror("syntax error");
                        }
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        $$ = varname;

                    }
                ;

condition_value	: K_SQLSTATE
                    {
                        PLpgSQL_condition *newp;
                        newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                        /* next token should be a string literal */
                        char   *sqlstatestr;
                        yylex();
                        if (strcmp(yylval.str, "value") == 0) {
                            yylex();
                        }
                        sqlstatestr = yylval.str;
                        
                        if (strlen(sqlstatestr) != 5)
                            yyerror("invalid SQLSTATE code");
                        if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
                            yyerror("invalid SQLSTATE code");
                        if (strncmp(sqlstatestr, "00", 2) == 0) {
                            const char* message = "bad SQLSTATE";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                                        errmsg("bad SQLSTATE '%s'",sqlstatestr)));
                        }

                        newp->sqlerrstate = MAKE_SQLSTATE(sqlstatestr[0],
                                                          sqlstatestr[1],
                                                          sqlstatestr[2],
                                                          sqlstatestr[3],
                                                          sqlstatestr[4]);
                        newp->condname = NULL;
                        newp->sqlstate = sqlstatestr;
                        newp->next = NULL;
                        newp->isSqlvalue = false;
                        $$ = newp;
                    }
                | ICONST
                    {
                        PLpgSQL_condition *newp;
                        newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                        if ($1 == 0) {
                            const char* message = "Incorrect CONDITION value: '0'";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                                        errmsg("Incorrect CONDITION value: '0'")));
                        }

                        int num = $1;
                        char *sqlstatestr = (char *)palloc0(sizeof(char) * 6);
                        for (int i = 4; i >= 0; i--) {
                            sqlstatestr[i] = num % 10 + '0';
                            num /= 10;
                        }
                        sqlstatestr[5] = '\0';
                        if (num != 0) {
                            $$ = NULL;
                        }

                        newp->sqlerrstate = MAKE_SQLSTATE(sqlstatestr[0],
                                                          sqlstatestr[1],
                                                          sqlstatestr[2],
                                                          sqlstatestr[3],
                                                          sqlstatestr[4]);
                        newp->condname = NULL;
                        newp->sqlstate = pstrdup(sqlstatestr);
                        newp->next = NULL;
                        newp->isSqlvalue = true;
                        $$ = newp;
                    }
                ;

decl_sect		: opt_block_label
                    {
                        /* done with decls, so resume identifier lookup */
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        $$.n_initvars = 0;
                        $$.initvarnos = NULL;
                        $$.isAutonomous = false;
                    }
                | opt_block_label decl_start
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        $$.n_initvars = 0;
                        $$.initvarnos = NULL;
                        $$.isAutonomous = false;
                    }
                | opt_block_label decl_start { SetErrorState(); } decl_stmts
                    {
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        /* Remember variables declared in decl_stmts */
                        $$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
                        $$.isAutonomous = u_sess->plsql_cxt.pragma_autonomous;
                        u_sess->plsql_cxt.pragma_autonomous = false;
                    }
                ;

decl_start		: K_DECLARE
                    {
                        /* Forget any variables created before block */
                        plpgsql_add_initdatums(NULL);
                        u_sess->plsql_cxt.pragma_autonomous = false;
                        /*
                         * Disable scanner lookup of identifiers while
                         * we process the decl_stmts
                         */
                        u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
                    }
                ;

decl_stmts		: decl_stmts decl_stmt
                | decl_stmt
                ;

decl_stmt		: decl_statement
                | K_DECLARE
                    {
                        /* We allow useless extra DECLAREs */
                    }
                | LESS_LESS any_identifier GREATER_GREATER
                    {
                        /*
                         * Throw a helpful error if user tries to put block
                         * label just before BEGIN, instead of before DECLARE.
                         */
                        const char* message = "block label must be placed before DECLARE, not after";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("block label must be placed before DECLARE, not after"),
                                 parser_errposition(@1)));
                    }
                ;

as_is        : K_IS
                | K_AS /* A db */
                ;

decl_statement	: decl_varname_list decl_const decl_datatype decl_collate decl_notnull decl_defval
                    {
                        ListCell *lc = NULL;
                        if ((list_length($1) > 1) && ($3 && $3->typoid == REFCURSOROID))
						    ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("not support declare multiple variable"),
                                 parser_errposition(@1)));

                        /* build declare variable list*/
                        foreach(lc, $1)
                        {
                            VarName* varname = (VarName*) lfirst(lc);
                            PLpgSQL_variable	*var;

                            IsInPublicNamespace(varname->name);

                            /*
                             * If a collation is supplied, insert it into the
                             * datatype.  We assume decl_datatype always returns
                             * a freshly built struct not shared with other
                             * variables.
                             */

                            if ($3 == NULL) {
                                // not allowed going on when plsql_show_all_error is on
#ifndef ENABLE_MULTIPLE_NODES
                                if (!u_sess->attr.attr_common.plsql_show_all_error) {
                                    yyerror("missing data type declaration");
                                } else {
                                    const char* message = "missing data type declaration";
                                    InsertErrorMessage(message, @4);
                                    yyerror("missing data type declaration");
                                    ereport(ERROR,
                                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                             errmsg("missing data type declaration"),
                                             parser_errposition(@4)));
                                }
#else
                                yyerror("missing data type declaration");
#endif
                            } else {

                            if ($3 && $3->typoid == REFCURSOROID && IsOnlyCompilePackage())
                            {
                                yyerror("not allow use ref cursor in package");
                            }

                            if (OidIsValid($4))
                            {
                                if (!OidIsValid($3->collation)) {
                                    const char* message = "collations are not supported by type";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                             (errcode(ERRCODE_DATATYPE_MISMATCH),
                                             errmsg("collations are not supported by type %s",
                                                    format_type_be($3->typoid)),
                                             parser_errposition(@4)));
                                }
                                $3->collation = $4;
                            }

                            var = plpgsql_build_variable(varname->name, varname->lineno,
                                                         $3, true);
                            if ($2)
                            {
                                if (var->dtype == PLPGSQL_DTYPE_VAR)
                                    ((PLpgSQL_var *) var)->isconst = $2;
                                else {
                                    const char* message = "row or record variable cannot be CONSTANT";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("row or record variable cannot be CONSTANT"),
                                             parser_errposition(@2)));
                                }
                            }
                            if ($5)
                            {
                                if (var->dtype == PLPGSQL_DTYPE_VAR)
                                    ((PLpgSQL_var *) var)->notnull = $5;
                                else {
                                    const char* message = "row or record variable cannot be NOT NULL";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                             (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                             errmsg("row or record variable cannot be NOT NULL"),
                                             parser_errposition(@4)));
                                }
                            }
                            if ($6 != NULL)
                            {
                                if (var->dtype == PLPGSQL_DTYPE_VAR)
                                    ((PLpgSQL_var *) var)->default_val = $6;
                                else if (var->dtype == PLPGSQL_DTYPE_ROW || var->dtype == PLPGSQL_DTYPE_RECORD) {
                                    ((PLpgSQL_row *) var)->default_val = $6;
                                } 
                                else {
                                    const char* message = "default value for rec variable is not supported";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg("default value for rec variable is not supported"),
                                            parser_errposition(@5)));
                                }
                            }
                            if (enable_plpgsql_gsdependency()) {
                                gsplsql_build_gs_type_in_body_dependency($3);
                            }
                            }
                            pfree_ext(varname->name);
                        }
                        list_free_deep($1);
                    }
                | decl_varname_list K_ALIAS K_FOR decl_aliasitem ';'
                    {
                        if (list_length($1) != 1)
                            ereport(errstate,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg("alias not support declare multiple variable"),
                                            parser_errposition(@1)));

                        VarName *varname = (VarName*) lfirst(list_head($1));
                        IsInPublicNamespace(varname->name);
                        plpgsql_ns_additem($4->itemtype,
                                                $4->itemno, varname->name);
                        pfree_ext(varname->name);
                        list_free_deep($1);
                    }
                |	K_CURSOR decl_varname opt_scrollable 
                    {
                        IsInPublicNamespace($2->name);
                        plpgsql_ns_push($2->name); 
                    }
                    decl_cursor_args decl_is_for decl_cursor_query
                    {
                        PLpgSQL_var *newp;

                        /* pop local namespace for cursor args */
                        plpgsql_ns_pop();

                        newp = (PLpgSQL_var *)
                                plpgsql_build_variable($2->name, $2->lineno,
                                                    plpgsql_build_datatype(REFCURSOROID,
                                                                            -1,
                                                                            InvalidOid),
                                                                            true);

                        newp->cursor_explicit_expr = $7;
                        if ($5 == NULL)
                            newp->cursor_explicit_argrow = -1;
                        else
                            newp->cursor_explicit_argrow = $5->dno;
                        newp->cursor_options = CURSOR_OPT_FAST_PLAN | $3;
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        newp->datatype->cursorCompositeOid = IS_ANONYMOUS_BLOCK ? 
                            InvalidOid : createCompositeTypeForCursor(newp, $7);
                        pfree_ext($2->name);
						pfree($2);
                    }
                |	K_TYPE decl_varname as_is K_REF K_CURSOR ';'
                    {
                        IsInPublicNamespace($2->name);
                        /* add name of cursor type to PLPGSQL_NSTYPE_REFCURSOR */
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_REFCURSOR,0,$2->name);
                        if (IS_PACKAGE) {
                            plpgsql_build_package_refcursor_type($2->name);
                        }
                        pfree_ext($2->name);
						pfree($2);

                    }
                |	decl_varname_list T_REFCURSOR ';'
                    {
                        if (list_length($1) != 1)
                            ereport(errstate,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg("refcursor not support declare multiple variable"),
                                            parser_errposition(@1)));

                        VarName *varname = (VarName*) lfirst(list_head($1));

                        IsInPublicNamespace(varname->name);
                        if (IsOnlyCompilePackage()) {
                            yyerror("not allow use ref cursor in package");
                        }
                        AddNamespaceIfNeed(-1, $2.ident);

                        plpgsql_build_variable(
                                varname->name, 
                                varname->lineno,
                                plpgsql_build_datatype(REFCURSOROID,-1,InvalidOid),
                                true);
                        pfree_ext(varname->name);
                        list_free_deep($1);
                    }
                |	decl_varname_list K_SYS_REFCURSOR ';'
                    {
                        if (list_length($1) != 1)
                            ereport(errstate,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                            errmsg("refcursor not support declare multiple variable"),
                                            parser_errposition(@1)));

                        VarName *varname = (VarName*)lfirst(list_head($1));
                        IsInPublicNamespace(varname->name);
                        if (IsOnlyCompilePackage()) {
                            yyerror("not allow use ref cursor in package");
                        }
                        plpgsql_build_variable(
                                varname->name, 
                                varname->lineno,
                                plpgsql_build_datatype(REFCURSOROID,-1,InvalidOid),
                                true);
                        pfree_ext(varname->name);
                        list_free_deep($1);
                    }
                /*
                 * Implementing the grammar pattern
                 * "type varrayname is varray(ICONST) of datatype;"
                 * and "varname varrayname := varrayname()"
                 */
                |	K_TYPE decl_varname as_is K_VARRAY '(' ICONST ')'  K_OF decl_datatype ';'
                    {
                        IsInPublicNamespace($2->name);

                        $9->collectionType = PLPGSQL_COLLECTION_ARRAY;
                        $9->tableOfIndexType = InvalidOid;

                        if($9->typinput.fn_oid == F_ARRAY_IN) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or table type nested by array type is not supported yet."),
                                    errdetail("Define array type \"%s\" of array or table type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of array type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        if($9->typoid == REFCURSOROID) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("ref cursor type nested by array is not supported yet."),
                                    errdetail("Define array type \"%s\" of ref cursor type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of array type")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        plpgsql_build_varrayType($2->name, $2->lineno, $9, true);
                        if (IS_PACKAGE) {
                            plpgsql_build_package_array_type($2->name, $9->typoid, TYPCATEGORY_ARRAY, $9->dependExtend);
                        } else if (enable_plpgsql_gsdependency()) {
                            gsplsql_build_gs_type_in_body_dependency($9);
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }
                |       K_TYPE decl_varname as_is K_VARRAY '(' ICONST ')'  K_OF record_var ';'
                    {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array or record type nesting is not supported in distributed database yet."),
                                errdetail("Define array type \"%s\" of record is not supported in distributed database yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of array type")));
                        u_sess->plsql_cxt.have_error = true;
#endif
                        if (IS_ANONYMOUS_BLOCK) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or record type nesting is not supported in anonymous block yet."),
                                    errdetail("Define array type \"%s\" of record is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of array type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        IsInPublicNamespace($2->name);

                        PLpgSQL_type *newp = NULL;
                        newp = build_type_from_record_var($9, @9);
                        newp->collectionType = PLPGSQL_COLLECTION_ARRAY;
                        newp->tableOfIndexType = InvalidOid;
                        plpgsql_build_varrayType($2->name, $2->lineno, newp, true);
                        if (IS_PACKAGE) {
                            plpgsql_build_package_array_type($2->name, newp->typoid, TYPCATEGORY_ARRAY);
                        } else if (enable_plpgsql_gsdependency()) {
                            PLpgSQL_rec_type* rec_var = (PLpgSQL_rec_type*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$9];
                            int i;
                            for (i = 0; i < rec_var->attrnum; i++) {
                                gsplsql_build_gs_type_in_body_dependency(rec_var->types[i]);
                            }
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }

                |       K_TYPE decl_varname as_is K_VARRAY '(' ICONST ')'  K_OF varray_var ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array type nested by array is not supported yet."),
                                errdetail("Define array type \"%s\" of array is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of array type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |       K_TYPE decl_varname as_is K_VARRAY '(' ICONST ')'  K_OF table_var ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("table type nested by array is not supported yet."),
                                errdetail("Define array type \"%s\" of table type is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of array type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |       K_TYPE decl_varname as_is K_VARRAY '(' ICONST ')'  K_OF T_REFCURSOR ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("ref cursor type nested by array is not supported yet."),
                                errdetail("Define array type \"%s\" of ref cursor type is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of array type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |	decl_varname_list decl_const varray_var decl_defval
                    {
                        ListCell* lc = NULL;
                        /* build declare variable list */
                        foreach(lc, $1)
                        {
                            VarName *varname = (VarName*) lfirst(lc);
                            IsInPublicNamespace(varname->name);

                            PLpgSQL_type *var_type = ((PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$3])->datatype;
                            PLpgSQL_var *newp;
                            PLpgSQL_type *new_var_type;

                            new_var_type = build_array_type_from_elemtype(var_type);
                            new_var_type->collectionType = var_type->collectionType;
                            new_var_type->tableOfIndexType = var_type->tableOfIndexType;

                            newp = (PLpgSQL_var *)plpgsql_build_variable(varname->name, varname->lineno, new_var_type, true);
                            newp->isconst = $2;
                            newp->default_val = $4;

                            if (NULL == newp) {
                                const char* message = "build variable failed";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                        errmsg("build variable failed")));
                                u_sess->plsql_cxt.have_error = true;
                            }
                            pfree_ext(varname->name);
                        }
                        list_free_deep($1);
                    }
                |   K_TYPE decl_varname as_is K_TABLE K_OF decl_datatype decl_notnull ';'
                    {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(ERROR,
                            (errmodule(MOD_PLSQL),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("table of is not support in distribute database"),
                                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                erraction("Modify SQL statement according to the manual.")));
#endif
                        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                            ereport(ERROR,
                                (errmodule(MOD_PLSQL),
                                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("table of is only support in A-format database."),
                                    errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                    erraction("Modify SQL statement according to the manual.")));
                        }
                        IsInPublicNamespace($2->name);

                        $6->collectionType = PLPGSQL_COLLECTION_TABLE;
                        $6->tableOfIndexType = InvalidOid;
                        if($6->typinput.fn_oid == F_ARRAY_IN) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or table type nested by table type is not supported yet."),
                                    errdetail("Define table type \"%s\" of array or table type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        if($6->typoid == REFCURSOROID) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("ref cursor type nested by table type is not supported yet."),
                                    errdetail("Define table type \"%s\" of ref cursor type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        plpgsql_build_tableType($2->name, $2->lineno, $6, true);
                        if (IS_PACKAGE) {
                            plpgsql_build_package_array_type($2->name, $6->typoid, TYPCATEGORY_TABLEOF, $6->dependExtend);
                        } else if (enable_plpgsql_gsdependency()) {
                            gsplsql_build_gs_type_in_body_dependency($6);
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }
                |   K_TYPE decl_varname as_is K_TABLE K_OF table_var decl_notnull ';'
                    {
                        IsInPublicNamespace($2->name);
                        PLpgSQL_var *check_var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                        /* get and check nest tableof's depth */
                        int depth = get_nest_tableof_layer(check_var, $2->name, errstate);
                        PLpgSQL_type *nest_type = plpgsql_build_nested_datatype();
                        nest_type->tableOfIndexType = INT4OID;
                        nest_type->collectionType = PLPGSQL_COLLECTION_TABLE;
                        PLpgSQL_var* var = (PLpgSQL_var*)plpgsql_build_tableType($2->name, $2->lineno, nest_type, true);
                        /* nested table type */
                        var->nest_table = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                        var->nest_layers = depth;
                        var->isIndexByTblOf = false;
                        pfree_ext($2->name);
						pfree($2);
                    }
                |   K_TYPE decl_varname as_is K_TABLE K_OF record_var decl_notnull ';'
                    {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array or record type nesting is not supported in distributed database yet."),
                                errdetail("Define table type \"%s\" of record is not supported in distributed database yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
#endif
                        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                            ereport(ERROR,
                                (errmodule(MOD_PLSQL),
                                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("table of is only support in A-format database."),
                                    errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                    erraction("Modify SQL statement according to the manual.")));
                        }

                        if (IS_ANONYMOUS_BLOCK) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or record type nesting is not supported in anonymous block yet."),
                                    errdetail("Define table type \"%s\" of record is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        IsInPublicNamespace($2->name);

                        PLpgSQL_type *newp = NULL;
                        newp = build_type_from_record_var($6, @6);
                        newp->collectionType = PLPGSQL_COLLECTION_TABLE;
                        newp->tableOfIndexType = InvalidOid;
                        plpgsql_build_tableType($2->name, $2->lineno, newp, true);
                        if (IS_PACKAGE) {
                            plpgsql_build_package_array_type($2->name, newp->typoid, TYPCATEGORY_TABLEOF);
                        } else if (enable_plpgsql_gsdependency()) {
                            PLpgSQL_rec_type* rec_var = (PLpgSQL_rec_type*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                            int i;
                            for (i = 0; i < rec_var->attrnum; i++) {
                                gsplsql_build_gs_type_in_body_dependency(rec_var->types[i]);
                            }
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }

                |   K_TYPE decl_varname as_is K_TABLE K_OF varray_var decl_notnull ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array type nested by table type is not supported yet."),
                                errdetail("Define table type \"%s\" of array is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |   K_TYPE decl_varname as_is K_TABLE K_OF T_REFCURSOR decl_notnull ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("ref cursor type nested by table type is not supported yet."),
                                errdetail("Define table type \"%s\" of ref cursor is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |   K_TYPE decl_varname as_is K_TABLE K_OF varray_var decl_notnull K_INDEX K_BY decl_datatype ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array type nested by table type is not supported yet."),
                                errdetail("Define table type \"%s\" of array is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |   K_TYPE decl_varname as_is K_TABLE K_OF T_REFCURSOR decl_notnull K_INDEX K_BY decl_datatype ';'
                    {
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("ref cursor type nested by table type is not supported yet."),
                                errdetail("Define table type \"%s\" of ref cursor type is not supported yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
                    }

                |   K_TYPE decl_varname as_is K_TABLE K_OF decl_datatype decl_notnull K_INDEX K_BY decl_datatype ';'
                    {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(ERROR,
                            (errmodule(MOD_PLSQL),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("table of is not support in distribute database"),
                                errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                erraction("Modify SQL statement according to the manual.")));
#endif
                        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                            ereport(ERROR,
                                (errmodule(MOD_PLSQL),
                                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("table of is only support in A-format database."),
                                    errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                    erraction("Modify SQL statement according to the manual.")));
                        }
                        IsInPublicNamespace($2->name);

                        $6->collectionType = PLPGSQL_COLLECTION_TABLE;
                        if ($10->typoid != VARCHAROID && $10->typoid != INT4OID) {
                            const char* message = "unsupported table index type";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("unsupported table index type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        $6->tableOfIndexType = $10->typoid;
                        if($6->typinput.fn_oid == F_ARRAY_IN) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or table type nested by table type is not supported yet."),
                                    errdetail("Define table type \"%s\" of array or table type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        if($6->typoid == REFCURSOROID) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("ref cursor type nested by table type is not supported yet."),
                                    errdetail("Define table type \"%s\" of ref cursor type is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        PLpgSQL_var* var = (PLpgSQL_var*)plpgsql_build_tableType($2->name, $2->lineno, $6, true);
                        var->isIndexByTblOf = true;

                        if (IS_PACKAGE) {
                            if ($10->typoid == VARCHAROID) {
                                plpgsql_build_package_array_type($2->name, $6->typoid, TYPCATEGORY_TABLEOF_VARCHAR, $6->dependExtend);
                            } else {
                                plpgsql_build_package_array_type($2->name, $6->typoid, TYPCATEGORY_TABLEOF_INTEGER, $6->dependExtend);
                            }
                        } else if (enable_plpgsql_gsdependency()) {
                            gsplsql_build_gs_type_in_body_dependency($6);
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }
                |   K_TYPE decl_varname as_is K_TABLE K_OF table_var decl_notnull K_INDEX K_BY decl_datatype ';'
                    {
                        IsInPublicNamespace($2->name);

                        if ($10->typoid != VARCHAROID && $10->typoid != INT4OID) {
                            const char* message = "unsupported table index type";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("unsupported table index type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        PLpgSQL_var *check_var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                        /* get and check nest tableof's depth */
                        int depth = get_nest_tableof_layer(check_var, $2->name, errstate);
                        PLpgSQL_type *nest_type = plpgsql_build_nested_datatype();
                        nest_type->tableOfIndexType = $10->typoid;
                        nest_type->collectionType = PLPGSQL_COLLECTION_TABLE;
                        PLpgSQL_var* var = (PLpgSQL_var*)plpgsql_build_tableType($2->name, $2->lineno, nest_type, true);
                        /* nested table type */
                        var->nest_table = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                        var->nest_layers = depth;
                        var->isIndexByTblOf = true;
                        pfree_ext($2->name);
						pfree($2);
                    }
                |   K_TYPE decl_varname as_is K_TABLE K_OF record_var decl_notnull K_INDEX K_BY decl_datatype ';'
                    {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array or record type nesting is not supported in distributed database yet."),
                                errdetail("Define table type \"%s\" of record is not supported in distributed database yet.", $2->name),
                                errcause("feature not supported"),
                                erraction("check define of table type")));
                        u_sess->plsql_cxt.have_error = true;
#endif
                        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                            ereport(ERROR,
                                (errmodule(MOD_PLSQL),
                                    errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("table of is only support in A-format database."),
                                    errdetail("N/A"), errcause("PL/SQL uses unsupported feature."),
                                    erraction("Modify SQL statement according to the manual.")));
                        }
                        if (IS_ANONYMOUS_BLOCK) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or record type nesting is not supported in anonymous block yet."),
                                    errdetail("Define table type \"%s\" of record is not supported yet.", $2->name),
                                    errcause("feature not supported"),
                                    erraction("check define of table type")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        IsInPublicNamespace($2->name);

                        PLpgSQL_type *newp = NULL;
                        newp = build_type_from_record_var($6, @6);
                        newp->collectionType = PLPGSQL_COLLECTION_TABLE;

                        if ($10->typoid != VARCHAROID && $10->typoid != INT4OID) {
                            ereport(errstate,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("unsupported table index type")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        newp->tableOfIndexType = $10->typoid;
                        PLpgSQL_var *var = (PLpgSQL_var*)plpgsql_build_tableType($2->name, $2->lineno, newp, true);
                        var->isIndexByTblOf = true;

                        if (IS_PACKAGE) {
                            if ($10->typoid == VARCHAROID) {
                                plpgsql_build_package_array_type($2->name, newp->typoid, TYPCATEGORY_TABLEOF_VARCHAR);
                            } else {
                                plpgsql_build_package_array_type($2->name, newp->typoid, TYPCATEGORY_TABLEOF_INTEGER);
                            }
                        } else if (enable_plpgsql_gsdependency()) {
                            int i;
                            PLpgSQL_rec_type* rec_var = (PLpgSQL_rec_type*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$6];
                            for (i = 0; i < rec_var->attrnum; i++) {
                                gsplsql_build_gs_type_in_body_dependency(rec_var->types[i]);
                            }
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }

                |	decl_varname_list decl_const table_var decl_defval
                    {
                        ListCell* lc = NULL;
                        /* build variable list*/
                        foreach(lc, $1)
                        {
                            VarName* varname = (VarName*) lfirst(lc);
                            IsInPublicNamespace(varname->name);

                            PLpgSQL_type *var_type = ((PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$3])->datatype;
                            PLpgSQL_var *table_type = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$3];

                            PLpgSQL_var *newp;
                            PLpgSQL_type *new_var_type;
                            new_var_type = build_array_type_from_elemtype(var_type);

                            new_var_type->collectionType = var_type->collectionType;
                            new_var_type->tableOfIndexType = var_type->tableOfIndexType;

                            newp = (PLpgSQL_var *)plpgsql_build_variable(varname->name, varname->lineno, new_var_type, true);
                            if (NULL == newp) {
                                const char* message = "build variable failed";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                         errmsg("build variable failed")));
                                u_sess->plsql_cxt.have_error = true;
                            }
                            newp->isconst = $2;
                            newp->default_val = $4;

                            if (table_type->nest_table != NULL) {
                                newp->nest_table = plpgsql_build_nested_variable(table_type->nest_table, $2, varname->name, varname->lineno);
                                newp->nest_layers = table_type->nest_layers;
                            }
                            pfree_ext(varname->name);
                        }
                        list_free_deep($1);
                    }
                |	K_TYPE decl_varname as_is K_RECORD '(' record_attr_list ')' ';'
                    {
                        IsInPublicNamespace($2->name);
                        PLpgSQL_rec_type	*newp = NULL;

                        newp = plpgsql_build_rec_type($2->name, $2->lineno, $6, true);
                        if (NULL == newp) {
                            const char* message = "build variable failed";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("build variable failed")));
                            u_sess->plsql_cxt.have_error = true;
                        }
                        if (IS_PACKAGE) {
                            newp->typoid = plpgsql_build_package_record_type($2->name, $6, true);
                        } else if (enable_plpgsql_gsdependency()) {
                            ListCell* cell =  NULL;
                            foreach(cell, $6) {
                                gsplsql_build_gs_type_in_body_dependency(((PLpgSQL_rec_attr*)lfirst(cell))->type);
                            }
                        }
                        pfree_ext($2->name);
						pfree($2);
                    }
                |	decl_varname_list record_var decl_defval
                    {
                        ListCell* lc = NULL;
                        foreach(lc, $1)
                        {
                            VarName *varname = (VarName*) lfirst(lc);
                            IsInPublicNamespace(varname->name);

                            PLpgSQL_var *newp = NULL;
                            PLpgSQL_type * var_type = (PLpgSQL_type *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$2];

                            newp = (PLpgSQL_var *)plpgsql_build_variable(varname->name,varname->lineno,
                                                                                var_type,true);
                            if (NULL == newp) {
                                const char* message = "build variable failed";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                         errmsg("build variable failed")));
                                u_sess->plsql_cxt.have_error = true;
                            }
                            if ($3 != NULL) {
                                ((PLpgSQL_row *) newp)->default_val = $3;
                            }
                            pfree_ext(varname->name);
                        }
                        list_free_deep($1);
                    }
                |   K_FUNCTION {u_sess->parser_cxt.is_procedure=false;} spec_proc
                    {
                        DefElem *def = $3;
                        bool oldisCreateFunction = u_sess->plsql_cxt.isCreateFunction;
                        u_sess->plsql_cxt.procedure_start_line = GetLineNumber(u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf, @1);
                        u_sess->plsql_cxt.plpgsql_yylloc = @1;
                        u_sess->plsql_cxt.isCreateFunction = true;
                        set_is_create_pkg_function(true);
                        raw_parse_package_function(def->defname, def->location, def->begin_location);
                        set_is_create_pkg_function(false);
                        u_sess->plsql_cxt.isCreateFunction = oldisCreateFunction;
                        u_sess->plsql_cxt.procedure_start_line = 0;
                        u_sess->plsql_cxt.plpgsql_yylloc = 0;
                    }
                |   K_PROCEDURE {u_sess->parser_cxt.is_procedure=true;} spec_proc
                    {
                        DefElem *def = $3;
                        bool oldisCreateFunction = u_sess->plsql_cxt.isCreateFunction;
                        u_sess->plsql_cxt.procedure_start_line = GetLineNumber(u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf, @1);
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        u_sess->plsql_cxt.isCreateFunction = true;
                        set_is_create_pkg_function(true);
                        raw_parse_package_function(def->defname, def->location, def->begin_location);
                        set_is_create_pkg_function(false);
                        u_sess->plsql_cxt.isCreateFunction = oldisCreateFunction;
                        u_sess->plsql_cxt.procedure_start_line = 0;
                        u_sess->plsql_cxt.plpgsql_yylloc = 0;
                    }
                |	K_PRAGMA any_identifier ';'
                    {
                        if (pg_strcasecmp($2, "autonomous_transaction") == 0) {
                            u_sess->plsql_cxt.pragma_autonomous = true;
                            if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile !=NULL) {
                                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->is_autonomous = true;
                            }
                        }
                        else {
                            elog(errstate, "invalid pragma");
                            u_sess->plsql_cxt.have_error = true;
                        }
                    }
                |   K_PRAGMA any_identifier '(' any_identifier ',' error_code ')' ';'
                    {
                        if (pg_strcasecmp($2, "exception_init") == 0) {
                            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                                plpgsql_set_variable($4, $6);
                            }
                        } else {
                            elog(errstate, "invalid pragma.");
                            u_sess->plsql_cxt.have_error = true;
                        }
                    }
                ;

error_code       : ICONST
                 {
                    $$ = $1;
                 }
                 | '-' ICONST
                 {
                    $$ = -$2;
                 }

spec_proc        :
                 {
                     $$ = get_proc_str(yychar);
                     yyclearin;
                 }    

init_proc        :
                 {
                     $$ = get_init_proc(yychar);
                     yyclearin;
                 }    


record_attr_list : record_attr
                   {
                        $$ = list_make1($1);
                   }
                 | record_attr_list ',' record_attr
                    {
                        $$ = lappend($1, $3);
                    }
                 ;

record_attr		: attr_name decl_datatype decl_notnull decl_rec_defval
                  {
                        PLpgSQL_rec_attr	*attr = NULL;
                        if($2->typoid == REFCURSOROID) {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("ref cursor type nested by record is not supported yet."),
                                    errdetail("Define record type of ref cursor type is not supported yet."),
                                    errcause("feature not supported"),
                                    erraction("check define of record type")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        attr = (PLpgSQL_rec_attr*)palloc0(sizeof(PLpgSQL_rec_attr));

                        attr->attrname = $1;
                        attr->type = $2;

                        attr->notnull = $3;
                        if ($4 != NULL)
                        {
                            if (attr->type->ttype == PLPGSQL_TTYPE_SCALAR)
                                attr->defaultvalue = $4;
                            else {
                                const char* message = "default value for row or record variable is not supported";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@3)));
                            }
                        }

                        if ($3 && $4 == NULL) {
                            const char* message = "variables must have default value";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("variables declared as NOT NULL must have "
                                "a default value.")));
                        }

                        $$ = attr;
                    }
			| attr_name record_var decl_notnull decl_rec_defval
                  {
#ifdef ENABLE_MULTIPLE_NODES
                        ereport(errstate,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("array or record type nesting is not supported in distributed database yet."),
                                errdetail("Define a record type of record is not supported in distributed database yet."),
                                errcause("feature not supported"),
                                erraction("check define of record type")));
                        u_sess->plsql_cxt.have_error = true;
#endif
                        if (IS_ANONYMOUS_BLOCK) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmodule(MOD_PLSQL),
                                    errmsg("array or record type nesting is not supported in anonymous block yet."),
                                    errdetail("Define a record type of record is not supported yet."),
                                    errcause("feature not supported"),
                                    erraction("check define of record type")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        PLpgSQL_rec_attr	*attr = NULL;

                        attr = (PLpgSQL_rec_attr*)palloc0(sizeof(PLpgSQL_rec_attr));

                        attr->attrname = $1;
                        attr->type = build_type_from_record_var($2, @2);

                        attr->notnull = $3;
                        if ($4 != NULL)
                        {
                            if (attr->type->ttype == PLPGSQL_TTYPE_SCALAR)
                                attr->defaultvalue = $4;
                            else {
                                ereport(errstate,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@3)));
                                u_sess->plsql_cxt.have_error = true;
                            }
                        }

                        if ($3 && $4 == NULL) {
                            ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("variables declared as NOT NULL must have "
                                "a default value.")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        $$ = attr;
                    }
			| attr_name T_REFCURSOR decl_notnull decl_rec_defval
                  {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmodule(MOD_PLSQL),
                                errmsg("ref cursor type nested by record is not supported yet."),
                                errdetail("Define a record type of ref cursor type is not supported yet."),
                                errcause("feature not supported"),
                                erraction("check define of record type")));
                        u_sess->plsql_cxt.have_error = true;
                  }
            | attr_name varray_var decl_notnull decl_rec_defval
                  {
                        PLpgSQL_rec_attr        *attr = NULL;

                        attr = (PLpgSQL_rec_attr*)palloc0(sizeof(PLpgSQL_rec_attr));

                        attr->attrname = $1;

                        PLpgSQL_type *var_type = ((PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$2])->datatype;
                        PLpgSQL_type *new_var_type = build_array_type_from_elemtype(var_type);
                        new_var_type->collectionType = var_type->collectionType;
                        new_var_type->tableOfIndexType = var_type->tableOfIndexType;
                        attr->type = new_var_type;
                        attr->notnull = $3;
                        if ($4 != NULL)
                        {
                            if (attr->type->ttype == PLPGSQL_TTYPE_SCALAR)
                                attr->defaultvalue = $4;
                            else {
                                ereport(errstate,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@3)));
                                u_sess->plsql_cxt.have_error = true;
                            }
                        }

                        if ($3 && $4 == NULL) {
                            ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("variables declared as NOT NULL must have "
                                "a default value.")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        $$ = attr;
                    }
                        | attr_name table_var decl_notnull decl_rec_defval
                  {
                        PLpgSQL_rec_attr        *attr = NULL;

                        attr = (PLpgSQL_rec_attr*)palloc0(sizeof(PLpgSQL_rec_attr));

                        attr->attrname = $1;

                        PLpgSQL_type *var_type = ((PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$2])->datatype;
                        PLpgSQL_var *table_type = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$2];

                        if (table_type->nest_table != NULL) {
                            ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("nested table of type is not supported as record type attribute"),
                                parser_errposition(@3)));
                                u_sess->plsql_cxt.have_error = true;
                        }
                        PLpgSQL_type *new_var_type = build_array_type_from_elemtype(var_type);
                        new_var_type->collectionType = var_type->collectionType;
                        new_var_type->tableOfIndexType = var_type->tableOfIndexType;
                        attr->type = new_var_type;
                        attr->notnull = $3;
                        if ($4 != NULL)
                        {
                            if (attr->type->ttype == PLPGSQL_TTYPE_SCALAR)
                                attr->defaultvalue = $4;
                            else {
                                ereport(errstate,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@3)));
                                u_sess->plsql_cxt.have_error = true;
                            }
                        }

                        if ($3 && $4 == NULL) {
                            ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("variables declared as NOT NULL must have "
                                "a default value.")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        $$ = attr;
                    }

                ;

opt_scrollable :
                    {
                        $$ = 0;
                    }
                | K_NO K_SCROLL
                    {
                        $$ = CURSOR_OPT_NO_SCROLL;
                    }
                | K_SCROLL
                    {
                        $$ = CURSOR_OPT_SCROLL;
                    }
                ;

decl_cursor_query :
                    {
                        int tok;
                        tok = yylex();
                        plpgsql_push_back_token(tok);

                        /* check cursor syntax, cursor query only accept select query */
                        {
                            $$ = read_sql_stmt("");
                        }
                    }
                ;

decl_cursor_args :
                    {
                        $$ = NULL;
                    }
                | '(' decl_cursor_arglist ')'
                    {
                        PLpgSQL_row *newp;
                        int i;
                        ListCell *l;

                        newp = (PLpgSQL_row *)palloc0(sizeof(PLpgSQL_row));
                        newp->dtype = PLPGSQL_DTYPE_ROW;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->rowtupdesc = NULL;
                        newp->nfields = list_length($2);
                        newp->fieldnames = (char **)palloc(newp->nfields * sizeof(char *));
                        newp->varnos = (int *)palloc(newp->nfields * sizeof(int));
                        newp->isImplicit = true;
                        newp->addNamespace = false;
                        i = 0;
                        foreach (l, $2)
                        {
                            PLpgSQL_variable *arg = (PLpgSQL_variable *) lfirst(l);
                            newp->fieldnames[i] = arg->refname;
                            newp->varnos[i] = arg->dno;
                            i++;
                        }
                        list_free_ext($2);

                        plpgsql_adddatum((PLpgSQL_datum *) newp);
                        $$ = (PLpgSQL_datum *) newp;
                    }
                ;

decl_cursor_arglist : decl_cursor_arg
                    {
                        $$ = list_make1($1);
                    }
                | decl_cursor_arglist ',' decl_cursor_arg
                    {
                        $$ = lappend($1, $3);
                    }
                ;

decl_cursor_arg : decl_varname cursor_in_out_option decl_datatype
                    {
                        $$ = (PLpgSQL_datum *)
                            plpgsql_build_variable($1->name, $1->lineno,
                                                   $3, true);
                        pfree_ext($1->name);
						pfree($1);
                    }
                ;
cursor_in_out_option :  K_IN	|
            K_OUT	|
            /* empty */
        ;

decl_is_for		:	K_IS |		/* A db */
                    K_FOR;		/* SQL standard */

decl_aliasitem	: T_WORD
                    {
                        PLpgSQL_nsitem *nsi;

                        nsi = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                                                $1.ident, NULL, NULL,
                                                NULL);
                        if (nsi == NULL) {
                            const char* message = "variables not exists";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                                     errmsg("variable \"%s\" does not exist",
                                            $1.ident),
                                     parser_errposition(@1)));
                        }
                        $$ = nsi;
                    }
                | T_CWORD
                    {
                        PLpgSQL_nsitem *nsi;

                        if (list_length($1.idents) == 2)
                            nsi = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                                                    strVal(linitial($1.idents)),
                                                    strVal(lsecond($1.idents)),
                                                    NULL,
                                                    NULL);
                        else if (list_length($1.idents) == 3)
                            nsi = plpgsql_ns_lookup(plpgsql_ns_top(), false,
                                                    strVal(linitial($1.idents)),
                                                    strVal(lsecond($1.idents)),
                                                    strVal(lthird($1.idents)),
                                                    NULL);
                        else
                            nsi = NULL;
                        if (nsi == NULL) {
                            int yylloc_bak = yylloc;
                            int isPkg =plpgsql_pkg_adddatum2ns($1.idents);
                            yylloc = yylloc_bak;
                            if (isPkg < 0) {
                                const char* message = "variables not exists";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                         errmsg("variable \"%s\" does not exist",
                                                NameListToString($1.idents)),
                                         parser_errposition(@1)));
                            }
                        }
                        $$ = nsi;
                    }
                ;

/*
 * $$.name will do a strdup outer, so its original space should be free.
 */
decl_varname	: T_WORD
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = $1.ident;
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        /*
                         * Check to make sure name isn't already declared
                         * in the current block.
                         */
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;
                    }
                | unreserved_keyword
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        /*
                         * Check to make sure name isn't already declared
                         * in the current block.
                         */
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;
                    }
                | T_VARRAY
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                | T_RECORD
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                | T_TABLE
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                | T_REFCURSOR
                    {
                        VarName* varname = NULL;
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                | T_TABLE_VAR
                    {
                        VarName* varname = NULL;
                        if ($1.idents != NIL) {
                            yyerror("syntax error");
                        }
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                | T_VARRAY_VAR
                    {
                        VarName* varname = NULL;
                        if ($1.idents != NIL || strcmp($1.ident, "bulk_exceptions") == 0) {
                            yyerror("syntax error");
                        }
                        varname = (VarName *)palloc0(sizeof(VarName));
                        varname->name = pstrdup($1.ident);
                        varname->lineno = plpgsql_location_to_lineno(@1);
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                        $$ = varname;

                    }
                ;
decl_varname_list:
                decl_varname { $$ = list_make1($1); }
                | decl_varname_list ',' decl_varname 
				{
					if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
						    ereport(errstate,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("not support declare multiple variable"),
                                 parser_errposition(@1)));

					$$ = lappend($1, $3); 
				}
                ;
decl_const		:
                    { $$ = false; }
                | K_CONSTANT
                    { $$ = true; }
                ;

decl_datatype	:
                    {
                        /*
                         * If there's a lookahead token, read_datatype
                         * should consume it.
                         */
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = read_datatype(yychar);
                        yyclearin;
                    }
                ;

decl_collate	:
                    { $$ = InvalidOid; }
                | K_COLLATE T_WORD
                    {
                        $$ = get_collation_oid(list_make1(makeString($2.ident)),
                                               false);
                    }
                | K_COLLATE T_CWORD
                    {
                        $$ = get_collation_oid($2.idents, false);
                    }
                ;

decl_notnull	:
                    { $$ = false; }
                | K_NOT K_NULL
                    { $$ = true; }
                ;

decl_defval		: ';'
                    { $$ = NULL; }
                | decl_defkey
                    {
                        $$ = read_sql_expression(';', ";");
                    }
                ;

decl_rec_defval	:
                    {
                        $$ = NULL;
                    }
                | decl_defkey
                    {
                        int tok;

                        $$ = read_sql_expression2(',', ')', ")", &tok);

                        plpgsql_push_back_token(tok);
                    }
                ;

decl_defkey		: assign_operator
                | K_DEFAULT
                ;

assign_operator	: '='
                | COLON_EQUALS
                ;

proc_sect		:
                    { $$ = NIL; }
                | proc_stmts
                    { $$ = $1; }
                ;

proc_stmts		: proc_stmts proc_stmt
                        {
                            if ($2 == NULL)
                                $$ = $1;
                            else
                                $$ = lappend($1, $2);
                        }
                | proc_stmt
                        {
                            if ($1 == NULL)
                                $$ = NIL;
                            else
                                $$ = list_make1($1);
                        }
                ;

/*
 * For stmt_loop, stmt_while, stmt_for, stmt_foreach_a, is already labeled in
 * existing MPPDB, so we reuse its labal to suport GOTO
 */
proc_stmt		: pl_block ';'
                        { $$ = $1; }
                | stmt_loop
                        { $$ = $1; }
                | stmt_while
                        { $$ = $1; }
                | stmt_for
                        { $$ = $1; }
                | stmt_foreach_a
                        { $$ = $1; }
                | stmt_commit
                        { $$ = $1; }
                | stmt_rollback
                        { $$ = $1; }
                | label_stmts
                        { $$ = $1; }
                | stmt_savepoint
                        { $$ = $1; }
                | stmt_signal
                        { $$ = $1; }
                | stmt_resignal
                        { $$ = $1; }
                ;


goto_block_label	:
                    {
                        $$ = NULL;
                    }
                | LESS_LESS any_identifier GREATER_GREATER
                    {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_GOTO_LABEL, 0, "");
                        $$ = $2;
                    }
                ;
                    ;

label_stmts: goto_block_label label_stmt
                    {
                        /*
                         * If label is not null, we record it in current execution
                         * block so that the later or former GOTO can redirect the plpgsql execution steps
                         */
                        record_stmt_label($1, $2);
                        $$ = $2;
                    };

label_stmt		: stmt_assign
                        { $$ = $1; }
                | stmt_if
                        { $$ = $1; }
                | stmt_case
                        { $$ = $1; }
                | stmt_exit
                        { $$ = $1; }
                | stmt_return
                        { $$ = $1; }
                | stmt_raise
                        { $$ = $1; }
                | stmt_execsql
                        { $$ = $1; }
                | stmt_dynexecute
                        { $$ = $1; }
                | stmt_perform
                        { $$ = $1; }
                | stmt_getdiag
                        { $$ = $1; }
                | stmt_goto
                        { $$ = $1; }
                | stmt_open
                        { $$ = $1; }
                | stmt_fetch
                        { $$ = $1; }
                | stmt_move
                        { $$ = $1; }
                | stmt_close
                        { $$ = $1; }
                | stmt_null
                        { $$ = $1; }
                ;

stmt_perform	: K_PERFORM {u_sess->parser_cxt.isPerform = true;} expr_until_semi
                    {
                        PLpgSQL_stmt *stmt;
                        if (enable_out_param_override() && u_sess->parser_cxt.stmt != NULL) {
                            stmt = (PLpgSQL_stmt*)u_sess->parser_cxt.stmt;
                            ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
                        } else {
                            PLpgSQL_stmt_perform *newp;
                            newp = (PLpgSQL_stmt_perform *)palloc0(sizeof(PLpgSQL_stmt_perform));
                            newp->cmd_type = PLPGSQL_STMT_PERFORM;
                            newp->lineno   = plpgsql_location_to_lineno(@1);
                            newp->expr  = $3;
                            newp->sqlString = plpgsql_get_curline_query();
                            stmt = (PLpgSQL_stmt*)newp;
                        }
                        u_sess->parser_cxt.stmt = NULL;
                        u_sess->parser_cxt.isPerform = false;
                        $$ = stmt;
                    }
                ;

stmt_assign		: assign_var assign_operator expr_until_semi
                    {
                        PLpgSQL_stmt_assign *newp;

                        newp = (PLpgSQL_stmt_assign *)palloc0(sizeof(PLpgSQL_stmt_assign));
                        newp->cmd_type = PLPGSQL_STMT_ASSIGN;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->varno = $1;
                        newp->expr  = $3;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                | T_CWORD assign_operator expr_until_semi
                  {
                        cword_is_not_variable(&($1), @1);
                  }
                ;

stmt_getdiag	: K_GET getdiag_area_opt K_DIAGNOSTICS getdiag_list ';'
                    {
                        PLpgSQL_stmt_getdiag	 *newp;
                        ListCell		*lc;

                        newp = (PLpgSQL_stmt_getdiag *)palloc0(sizeof(PLpgSQL_stmt_getdiag));
                        newp->cmd_type = PLPGSQL_STMT_GETDIAG;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->is_stacked = $2;
                        newp->diag_items = $4;
                        newp->has_cond = false;
                        newp->is_cond_item = false;
                        newp->cond_number = 0;
                        newp->sqlString = plpgsql_get_curline_query();

                        /*
                         * Check information items are valid for area option.
                         */
                        foreach(lc, newp->diag_items)
                        {
                            PLpgSQL_diag_item *ditem = (PLpgSQL_diag_item *) lfirst(lc);

                            switch (ditem->kind)
                            {
                                /* these fields are disallowed in stacked case */
                                case PLPGSQL_GETDIAG_RESULT_OID:
                                    if (newp->is_stacked) {
                                        const char* message = "diagnostics item is not allowed in GET STACKED DIAGNOSTICS";
                                        InsertErrorMessage(message, plpgsql_yylloc);
                                        ereport(errstate,
                                                (errcode(ERRCODE_SYNTAX_ERROR),
                                                 errmsg("diagnostics item %s is not allowed in GET STACKED DIAGNOSTICS",
                                                        plpgsql_getdiag_kindname(ditem->kind)),
                                                 parser_errposition(@1)));
                                    }
                                    break;
                                case PLPGSQL_GETDIAG_ROW_COUNT:
                                    if (!B_DIAGNOSTICS) {
                                        if (newp->is_stacked) {
                                            const char* message = "diagnostics item is not allowed in GET STACKED DIAGNOSTICS";
                                            InsertErrorMessage(message, plpgsql_yylloc);
                                            ereport(errstate,
                                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                                    errmsg("diagnostics item %s is not allowed in GET STACKED DIAGNOSTICS",
                                                            plpgsql_getdiag_kindname(ditem->kind)),
                                                    parser_errposition(@1)));
                                        }
                                    }
                                    break;
                                /* these fields are disallowed in current case */
                                case PLPGSQL_GETDIAG_ERROR_CONTEXT:
                                case PLPGSQL_GETDIAG_ERROR_DETAIL:
                                case PLPGSQL_GETDIAG_ERROR_HINT:
                                case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
                                case PLPGSQL_GETDIAG_MESSAGE_TEXT:
                                    if (!newp->is_stacked) {
                                        const char* message = "diagnostics item is not allowed in GET CURRENT DIAGNOSTICS";
                                        InsertErrorMessage(message, plpgsql_yylloc);
                                        ereport(errstate,
                                                (errcode(ERRCODE_SYNTAX_ERROR),
                                                 errmsg("diagnostics item %s is not allowed in GET CURRENT DIAGNOSTICS",
                                                        plpgsql_getdiag_kindname(ditem->kind)),
                                                 parser_errposition(@1)));
                                    }
                                    break;
                                case PLPGSQL_GETDIAG_B_NUMBER:
                                    break;
                                default:
                                    const char* message = "unrecognized diagnostic item kind";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    elog(errstate, "unrecognized diagnostic item kind: %d",
                                         ditem->kind);
                                    break;
                            }
                        }

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                | K_GET getdiag_area_opt K_DIAGNOSTICS K_CONDITION condition_number_item condition_information ';'
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
							ereport(errstate, (errmodule(MOD_PARSER),
								errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("Un-support feature"),
								errdetail("get diagitem syntax is supported only in B compatibility")));
						}
                        PLpgSQL_stmt_getdiag	 *newp;

                        newp = (PLpgSQL_stmt_getdiag *)palloc0(sizeof(PLpgSQL_stmt_getdiag));
                        newp->cmd_type = PLPGSQL_STMT_GETDIAG;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->is_stacked = $2;
                        newp->has_cond = true;
                        newp->is_cond_item = true;
                        newp->cond_number = $5;
                        newp->diag_items = $6;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                | K_GET getdiag_area_opt K_DIAGNOSTICS K_CONDITION condition_number condition_information ';'
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
							ereport(errstate, (errmodule(MOD_PARSER),
								errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("Un-support feature"),
								errdetail("get diagitem syntax is supported only in B compatibility")));
						}
                        PLpgSQL_stmt_getdiag	 *newp;

                        newp = (PLpgSQL_stmt_getdiag *)palloc0(sizeof(PLpgSQL_stmt_getdiag));
                        newp->cmd_type = PLPGSQL_STMT_GETDIAG;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->is_stacked = $2;
                        newp->has_cond = true;
                        newp->is_cond_item = false;
                        newp->cond_number = $5;
                        newp->diag_items = $6;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

getdiag_area_opt :
                    {
                        $$ = false;
                    }
                | K_CURRENT
                    {
                        $$ = false;
                    }
                | K_STACKED
                    {
                        $$ = true;
                    }
                ;

getdiag_list : getdiag_list ',' getdiag_list_item
                    {
                        $$ = lappend($1, $3);
                    }
                | getdiag_list_item
                    {
                        $$ = list_make1($1);
                    }
                ;

getdiag_list_item : getdiag_target assign_operator getdiag_item
                    {
                        PLpgSQL_diag_item *newp;

                        newp = (PLpgSQL_diag_item *)palloc(sizeof(PLpgSQL_diag_item));
                        newp->target = $1;
                        newp->kind = $3;
                        newp->user_ident = NULL;

                        $$ = newp;
                    }
                | getdiag_target '=' K_NUMBER
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
							ereport(errstate, (errmodule(MOD_PARSER),
								errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("Un-support feature"),
								errdetail("get diagitem syntax is supported only in B compatibility")));
						}
                        PLpgSQL_diag_item *newp;

                        newp = (PLpgSQL_diag_item *)palloc(sizeof(PLpgSQL_diag_item));
                        newp->target = $1;
                        newp->kind = PLPGSQL_GETDIAG_B_NUMBER;
                        newp->user_ident = NULL;

                        $$ = newp;
                    }
                | SET_USER_IDENT '=' statement_information_item_name
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
							ereport(errstate, (errmodule(MOD_PARSER),
								errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("Un-support feature"),
								errdetail("get diagitem syntax is supported only in B compatibility")));
						}
                        PLpgSQL_diag_item *newp;

                        newp = (PLpgSQL_diag_item *)palloc(sizeof(PLpgSQL_diag_item));
                        newp->target = 0;
                        newp->kind = $3;
                        newp->user_ident = $1;

                        $$ = newp;
                    }
                ;

getdiag_item :    K_ROW_COUNT
                    {
                        $$ = PLPGSQL_GETDIAG_ROW_COUNT;
                    }
                | K_RESULT_OID
                    {
                        $$ = PLPGSQL_GETDIAG_RESULT_OID;
                    }
                | K_PG_EXCEPTION_DETAIL
                    {
                        $$ = PLPGSQL_GETDIAG_ERROR_DETAIL;
                    }
                | K_PG_EXCEPTION_CONTEXT
                    {
                        $$ = PLPGSQL_GETDIAG_ERROR_CONTEXT;
                    }
                | K_MESSAGE_TEXT
                    {
                        $$ = PLPGSQL_GETDIAG_MESSAGE_TEXT;
                    }
                | K_RETURNED_SQLSTATE
                    {
                        $$ = PLPGSQL_GETDIAG_RETURNED_SQLSTATE;
                    }
                ;

statement_information_item_name :
                  K_NUMBER
                    {
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("get diagnostic number is only supported in database which dbcompatibility='B'.")));
                        $$ = PLPGSQL_GETDIAG_B_NUMBER;
                    }
                | K_ROW_COUNT
                    {
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("get diagnostic number is only supported in database which dbcompatibility='B'.")));
                        $$ = PLPGSQL_GETDIAG_ROW_COUNT;
                    }
                ;

getdiag_target	: T_DATUM
                    {
                        check_assignable($1.datum, @1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW ||
                            $1.datum->dtype == PLPGSQL_DTYPE_REC) {
                            const char* message = "not a scalar variable";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("\"%s\" is not a scalar variable",
                                            NameOfDatum(&($1))),
                                     parser_errposition(@1)));
                        }
                        $$ = $1.dno;
                    }
                | T_WORD
                    {
                        /* just to give a better message than "syntax error" */
                        word_is_not_variable(&($1), @1);
                    }
                | T_CWORD
                    {
                        /* just to give a better message than "syntax error" */
                        cword_is_not_variable(&($1), @1);
                    }
                ;

condition_number_item :
                  T_DATUM
                    {
                        check_assignable($1.datum, @1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW ||
                            $1.datum->dtype == PLPGSQL_DTYPE_REC) {
                            const char* message = "not a scalar variable";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("\"%s\" is not a scalar variable",
                                            NameOfDatum(&($1))),
                                     parser_errposition(@1)));
                        }
                        $$ = $1.dno;
                    }
                ;

condition_number : ICONST                           { $$ = $1; }
                | FCONST                            { $$ = (atof($1) + 0.5); }
                | SCONST                            { $$ = (atof($1) + 0.5); }
                | BCONST					
					{
						Datum val = DirectFunctionCall1(bittoint4, DirectFunctionCall3(bit_in, CStringGetDatum($1), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
						$$ = DatumGetInt32(val);
					}
                | K_TRUE                            { $$ = 1; }
                | K_FALSE                           { $$ = 0; }
                | K_NULL                            { $$ = 0; }
                | SET_USER_IDENT
                    {
                        $$ = getUserVarVal($1);
                    }
                | T_WORD                        { $$ = (atof($1.ident) + 0.5); }
                ;

condition_information :
                  condition_information ',' condition_information_item
                    {
                        $$ = lappend($1, $3);
                    }
                | condition_information_item
                    {
                        $$ = list_make1($1);
                    }
                ;

condition_information_item :
                  getdiag_target '=' condition_information_item_name
                    {
                        PLpgSQL_diag_item *newp;

                        newp = (PLpgSQL_diag_item *)palloc(sizeof(PLpgSQL_diag_item));
                        newp->target = $1;
                        newp->kind = $3;
                        newp->user_ident = NULL;

                        $$ = newp;
                    }
                | SET_USER_IDENT '=' condition_information_item_name
                    {
                        PLpgSQL_diag_item *newp;

                        newp = (PLpgSQL_diag_item *)palloc(sizeof(PLpgSQL_diag_item));
                        newp->target = 0;
                        newp->kind = $3;
                        newp->user_ident = $1;

                        $$ = newp;
                    }
                ;

condition_information_item_name:
                  K_CLASS_ORIGIN                    { $$ = PLPGSQL_GETDIAG_B_CLASS_ORIGIN; }
                | K_SUBCLASS_ORIGIN                 { $$ = PLPGSQL_GETDIAG_B_SUBCLASS_ORIGIN; }
                | K_CONSTRAINT_CATALOG              { $$ = PLPGSQL_GETDIAG_B_CONSTRAINT_CATALOG; }
                | K_CONSTRAINT_SCHEMA               { $$ = PLPGSQL_GETDIAG_B_CONSTRAINT_SCHEMA; }
                | K_CONSTRAINT_NAME                 { $$ = PLPGSQL_GETDIAG_B_CONSTRAINT_NAME; }
                | K_CATALOG_NAME                    { $$ = PLPGSQL_GETDIAG_B_CATALOG_NAME; }
                | K_SCHEMA_NAME                     { $$ = PLPGSQL_GETDIAG_B_SCHEMA_NAME; }
                | K_TABLE_NAME                      { $$ = PLPGSQL_GETDIAG_B_TABLE_NAME; }
                | K_COLUMN_NAME                     { $$ = PLPGSQL_GETDIAG_B_COLUMN_NAME; }
                | K_CURSOR_NAME                     { $$ = PLPGSQL_GETDIAG_B_CURSOR_NAME; }
                | K_MESSAGE_TEXT                    { $$ = PLPGSQL_GETDIAG_B_MESSAGE_TEXT; }
                | K_MYSQL_ERRNO                     { $$ = PLPGSQL_GETDIAG_B_MYSQL_ERRNO; }
                | K_RETURNED_SQLSTATE               { $$ = PLPGSQL_GETDIAG_B_RETURNED_SQLSTATE; }
                ;

table_var		: T_TABLE
                    {
                        $$ = $1.dno;
                        AddNamespaceIfNeed($1.dno, $1.ident);
                    }
                ;

varray_var		: T_VARRAY
                    {
                        $$ = $1.dno;
                        AddNamespaceIfNeed($1.dno, $1.ident);
                    }
                ;

record_var		: T_RECORD
                    {
                        $$ = $1.dno;
                        AddNamespaceIfNeed($1.dno, $1.ident);
                    }
                ;

assign_var
                : T_RECORD assign_list
                    {
                        check_assignable($1.datum, @1);
                        if ($2 == NIL) {
                            $$ = $1.dno;
                        } else {
                            if(IsA((Node*)linitial($2), A_Indices) && list_length($2) == 1) {
                                PLpgSQL_arrayelem       *newp;

                                newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
                                newp->dtype             = PLPGSQL_DTYPE_ARRAYELEM;
                                newp->subscript = (PLpgSQL_expr*)(((A_Indices*)linitial($2))->uidx);
                                newp->arrayparentno = $1.dno;
                                /* initialize cached type data to "not valid" */
                                newp->parenttypoid = InvalidOid;
                                newp->assignattrno = -1;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);

                                $$ = newp->dno;
                            } else {
                                PLpgSQL_assignlist       *newp;
                                newp = (PLpgSQL_assignlist *)palloc0(sizeof(PLpgSQL_assignlist));
                                newp->dtype             = PLPGSQL_DTYPE_ASSIGNLIST;
                                newp->assignlist = $2;
                                newp->targetno = $1.dno;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);
                                $$ = newp->dno;
                            }
                        }

                    }
                | T_VARRAY_VAR assign_list
                    {
                        check_assignable($1.datum, @1);
                        if ($2 == NIL) {
                            $$ = $1.dno;
                        } else {
                            if(IsA((Node*)linitial($2), A_Indices) && list_length($2) == 1) {
                                PLpgSQL_arrayelem       *newp;

                                newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
                                newp->dtype             = PLPGSQL_DTYPE_ARRAYELEM;
                                newp->subscript = (PLpgSQL_expr*)(((A_Indices*)linitial($2))->uidx);
                                newp->arrayparentno = $1.dno;
                                /* initialize cached type data to "not valid" */
                                newp->parenttypoid = InvalidOid;
                                newp->assignattrno = -1;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);

                                $$ = newp->dno;
                            } else {
                                PLpgSQL_assignlist       *newp;
                                newp = (PLpgSQL_assignlist *)palloc0(sizeof(PLpgSQL_assignlist));
                                newp->dtype             = PLPGSQL_DTYPE_ASSIGNLIST;
                                newp->assignlist = $2;
                                newp->targetno = $1.dno;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);
                                $$ = newp->dno;
                            }
                        }

                    }
                | T_TABLE_VAR assign_list
                    {
                        check_assignable($1.datum, @1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_VAR) {
                            check_autonomous_nest_tablevar((PLpgSQL_var*)$1.datum);
                        }
                        if ($2 == NIL) {
                            $$ = $1.dno;
                        } else {
                            if(IsA((Node*)linitial($2), A_Indices) && list_length($2) == 1) {
                                PLpgSQL_tableelem       *newp;
                                newp = (PLpgSQL_tableelem *)palloc0(sizeof(PLpgSQL_tableelem));
                                newp->dtype = PLPGSQL_DTYPE_TABLEELEM;
                                newp->subscript = (PLpgSQL_expr*)(((A_Indices*)linitial($2))->uidx);
                                newp->tableparentno = $1.dno;
                                /* initialize cached type data to "not valid" */
                                newp->tabletypoid = InvalidOid;
                                newp->assignattrno = -1;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);
                                $$ = newp->dno;
                            } else {
                                PLpgSQL_assignlist       *newp;
                                newp = (PLpgSQL_assignlist *)palloc0(sizeof(PLpgSQL_assignlist));
                                newp->dtype             = PLPGSQL_DTYPE_ASSIGNLIST;
                                newp->assignlist = $2;
                                newp->targetno = $1.dno;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);
                                $$ = newp->dno;
                            }
                        }

                    }
                | T_DATUM  assign_list
                    {
                        check_assignable($1.datum, @1);
                        if ($2 == NIL) {
                            check_record_nest_tableof_index($1.datum);
                            $$ = $1.dno;
                        } else {
                            if(IsA((Node*)linitial($2), A_Indices) && list_length($2) == 1) {
                                PLpgSQL_datum* target = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$1.dno];
                                if (target->dtype == PLPGSQL_DTYPE_VAR &&
                                    ((PLpgSQL_var*)target)->datatype->collectionType == PLPGSQL_COLLECTION_TABLE) {
                                    PLpgSQL_tableelem       *newp;
                                    newp = (PLpgSQL_tableelem *)palloc0(sizeof(PLpgSQL_tableelem));
                                    newp->dtype             = PLPGSQL_DTYPE_TABLEELEM;
                                    newp->subscript = (PLpgSQL_expr*)(((A_Indices*)linitial($2))->uidx);
                                    newp->tableparentno = $1.dno;
                                    /* initialize cached type data to "not valid" */
                                    newp->tabletypoid = InvalidOid;
                                    newp->assignattrno = -1;
                                    plpgsql_adddatum((PLpgSQL_datum *) newp);

                                    $$ = newp->dno; 
                                } else {
								    PLpgSQL_arrayelem	*newp;

                                    newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
                                    newp->dtype		= PLPGSQL_DTYPE_ARRAYELEM;
                                    newp->subscript	= (PLpgSQL_expr*)(((A_Indices*)linitial($2))->uidx);
                                    newp->arrayparentno = $1.dno;
                                    /* initialize cached type data to "not valid" */
                                    newp->parenttypoid = InvalidOid;
                                    newp->assignattrno = -1;
                                    plpgsql_adddatum((PLpgSQL_datum *) newp);

                                    $$ = newp->dno;
                                }
                            } else {
                                PLpgSQL_assignlist	*newp;
                                newp = (PLpgSQL_assignlist *)palloc0(sizeof(PLpgSQL_assignlist));
                                newp->dtype		= PLPGSQL_DTYPE_ASSIGNLIST;
                                newp->targetno = $1.dno;
                                newp->assignlist = $2;
                                plpgsql_adddatum((PLpgSQL_datum *) newp);
                                $$ = newp->dno;
                            }
                        }
                    }
                | T_PACKAGE_VARIABLE assign_list
                  {
                        check_assignable($1.datum, @1);
                        if ($2 != NIL) {
                            PLpgSQL_assignlist	*newptr;
                            newptr = (PLpgSQL_assignlist *)palloc0(sizeof(PLpgSQL_assignlist));
                            newptr->dtype		= PLPGSQL_DTYPE_ASSIGNLIST;
                            newptr->targetno = $1.dno;
                            newptr->assignlist = $2;
                            plpgsql_adddatum((PLpgSQL_datum *) newptr);
                            $$ = newptr->dno;
                        } else {
                            check_record_nest_tableof_index($1.datum);
                            $$ = $1.dno;
                        }
                  }
                ;
assign_list     : /*EMPTY*/                             {$$ = NIL; }
                | assign_list assign_el                 {$$ = lappend($1, $2);}
                ;
assign_el      : '.' attr_name
                    {
                        $$ = (Node *) makeString($2);
                    } 
               | '(' expr_until_parenthesis
                    {
                        A_Indices *ai = makeNode(A_Indices);
                        ai->lidx = NULL;
                        ai->uidx = (Node *)$2;
                        $$ = (Node *)ai;
                    }
                | '[' expr_until_rightbracket
                    {
                        A_Indices *ai = makeNode(A_Indices);
                        ai->lidx = NULL;
                        ai->uidx = (Node *)$2;
                        $$ = (Node *)ai;
                    }
                ;
attr_name
                :  
                    {
                        $$ = get_attrname(yychar);
                        yyclearin;
                    }
	        ;
stmt_goto		: K_GOTO label_name
                    {
                        PLpgSQL_stmt_goto *newp;

                        newp = (PLpgSQL_stmt_goto *)palloc0(sizeof(PLpgSQL_stmt_goto));
                        newp->cmd_type  = PLPGSQL_STMT_GOTO;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->label = $2;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;
label_name		: T_WORD ';'
                    {
                        $$ = $1.ident;
                    }
                ;

stmt_if			: K_IF expr_until_then proc_sect stmt_elsifs stmt_else K_END K_IF ';'
                    {
                        PLpgSQL_stmt_if *newp;

                        newp = (PLpgSQL_stmt_if *)palloc0(sizeof(PLpgSQL_stmt_if));
                        newp->cmd_type	= PLPGSQL_STMT_IF;
                        newp->lineno		= plpgsql_location_to_lineno(@1);
                        newp->cond		= $2;
                        newp->then_body	= $3;
                        newp->elsif_list = $4;
                        newp->else_body  = $5;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_elsifs		:
                    {
                        $$ = NIL;
                    }
                | stmt_elsifs K_ELSIF expr_until_then proc_sect
                    {
                        PLpgSQL_if_elsif *newp;

                        newp = (PLpgSQL_if_elsif *)palloc0(sizeof(PLpgSQL_if_elsif));
                        newp->lineno = plpgsql_location_to_lineno(@2);
                        newp->cond   = $3;
                        newp->stmts  = $4;

                        $$ = lappend($1, newp);
                    }
                ;

stmt_else		:
                    {
                        $$ = NIL;
                    }
                | K_ELSE proc_sect
                    {
                        $$ = $2;
                    }
                ;

stmt_case		: K_CASE opt_expr_until_when case_when_list opt_case_else K_END K_CASE ';'
                    {
                        $$ = make_case(@1, $2, $3, $4);
                    }
                ;

opt_expr_until_when	:
                    {
                        PLpgSQL_expr *expr = NULL;
                        int	tok = yylex();

                        if (tok != K_WHEN)
                        {
                            plpgsql_push_back_token(tok);
                            expr = read_sql_expression(K_WHEN, "WHEN");
                        }
                        plpgsql_push_back_token(K_WHEN);
                        $$ = expr;
                    }
                ;

case_when_list	: case_when_list case_when
                    {
                        $$ = lappend($1, $2);
                    }
                | case_when
                    {
                        $$ = list_make1($1);
                    }
                ;

case_when		: K_WHEN expr_until_then proc_sect
                    {
                        PLpgSQL_case_when *newp = (PLpgSQL_case_when *)palloc(sizeof(PLpgSQL_case_when));

                        newp->lineno	= plpgsql_location_to_lineno(@1);
                        newp->expr	= $2;
                        newp->stmts	= $3;
                        newp->sqlString = plpgsql_get_curline_query();
                        $$ = newp;
                    }
                ;

opt_case_else	:
                    {
                        $$ = NIL;
                    }
                | K_ELSE proc_sect
                    {
                        /*
                         * proc_sect could return an empty list, but we
                         * must distinguish that from not having ELSE at all.
                         * Simplest fix is to return a list with one NULL
                         * pointer, which make_case() must take care of.
                         */
                        if ($2 != NIL)
                            $$ = $2;
                        else
                            $$ = list_make1(NULL);
                    }
                ;

stmt_loop		: opt_block_label K_LOOP loop_body
                    {
                        PLpgSQL_stmt_loop *newp;

                        newp = (PLpgSQL_stmt_loop *)palloc0(sizeof(PLpgSQL_stmt_loop));
                        newp->cmd_type = PLPGSQL_STMT_LOOP;
                        newp->lineno   = plpgsql_location_to_lineno(@2);
                        newp->label	  = $1;
                        newp->body	  = $3.stmts;
                        newp->sqlString = plpgsql_get_curline_query();

                        check_labels($1, $3.end_label, $3.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                | label_loop loop_body
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "label: loop"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'label:' is only supported in database which dbcompatibility='B'.")));

                        PLpgSQL_stmt_loop  *newp;
                        newp = (PLpgSQL_stmt_loop *)palloc0(sizeof(PLpgSQL_stmt_loop));
                        newp->cmd_type = PLPGSQL_STMT_LOOP;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->label    = u_sess->plsql_cxt.curr_compile_context->ns_top->name;
                        newp->body        = $2.stmts;
                        newp->sqlString = plpgsql_get_curline_query();

                        if(strcmp(newp->label, "") == 0 || u_sess->plsql_cxt.curr_compile_context->ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
                        {
                            ereport(errstate,
                                   (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("The label name is invalid"),
                                    parser_errposition(@1)));
                        }

                        check_labels(u_sess->plsql_cxt.curr_compile_context->ns_top->name, $2.end_label, $2.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label(u_sess->plsql_cxt.curr_compile_context->ns_top->name, (PLpgSQL_stmt *)newp);
                    }
                ;

label_loop :        T_LABELLOOP
                    | ':' K_LOOP
                ;
stmt_while		: opt_block_label K_WHILE expr_until_while_loop loop_body
                    {
                        /*
                         * Check for correct syntax
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
                        {
                            if($3.endtoken != K_LOOP)
                                 ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("while-loop syntax is mixed with while-do syntax"), parser_errposition(@2)));
                        }

                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@2);
                        newp->label	  = $1;
                        newp->cond	  = $3.expr;
                        newp->body	  = $4.stmts;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition	  = true;

                        check_labels($1, $4.end_label, $4.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                | label_while expr_until_while_loop loop_body
                    {
                       /*
                         * When the database is in mysql compatible mode
                         * support "label: while-loop"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'label:while' is only supported in database which dbcompatibility='B'.")));
                        /*
                         * Check for correct syntax
                         */
                        if($2.endtoken != K_LOOP)
                             ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("while-loop syntax is mixed with while-do syntax"), parser_errposition(@1)));

                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->label       = u_sess->plsql_cxt.curr_compile_context->ns_top->name;
                        newp->cond        = $2.expr;
                        newp->body        = $3.stmts;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition   = true;

                        if(strcmp(newp->label, "") == 0 || u_sess->plsql_cxt.curr_compile_context->ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
                        {
                            ereport(errstate,
                                   (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("The label name is invalid"),
                                    parser_errposition(@1)));
                        }

                        check_labels(u_sess->plsql_cxt.curr_compile_context->ns_top->name, $3.end_label, $3.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label(u_sess->plsql_cxt.curr_compile_context->ns_top->name, (PLpgSQL_stmt *)newp);
                    }
                | opt_block_label K_WHILE expr_until_while_loop while_body
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "while-do"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                        {
                            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("Incorrect use of syntax while-loop")));
                        }
                        else
                        {
                            /*
                             * Check for correct syntax
                             */
                            if($3.endtoken != K_DO)
                                 ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("while-loop syntax is mixed with while-do syntax"), parser_errposition(@2)));
                        }

                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@2);
                        newp->label       = $1;
                        newp->cond        = $3.expr;
                        newp->body        = $4.stmts;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition   = true;

                        check_labels($1, $4.end_label, $4.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                | label_while expr_until_while_loop while_body
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "label:"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                        {
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'label:while' is only supported in database which dbcompatibility='B'.")));
                        }
                        /*
                         * Check for correct syntax
                         */
                        if($2.endtoken != K_DO)
                             ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("while-loop syntax is mixed with while-do syntax"), parser_errposition(@1)));

                        PLpgSQL_stmt_while  *newp;
                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->label    = u_sess->plsql_cxt.curr_compile_context->ns_top->name;
                        newp->cond        = $2.expr;
                        newp->body        = $3.stmts;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition   = true;

                        if(strcmp(newp->label, "") == 0 || u_sess->plsql_cxt.curr_compile_context->ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
                        {
                            ereport(errstate,
                                   (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("The label name is invalid"),
                                    parser_errposition(@1)));
                        }
                        check_labels(u_sess->plsql_cxt.curr_compile_context->ns_top->name, $3.end_label, $3.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label(u_sess->plsql_cxt.curr_compile_context->ns_top->name, (PLpgSQL_stmt *)newp);
                    }
                | opt_block_label K_REPEAT proc_sect K_UNTIL repeat_condition
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "repeat"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'repeat' is only supported in database which dbcompatibility='B'.")));

                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@2);
                        newp->label       = $1;
                        newp->cond        = $5.expr;
                        newp->body        = $3;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition   = false;

                        check_labels($1, $5.end_label, $5.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                | label_repeat proc_sect K_UNTIL repeat_condition
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "repeat"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'label: repeat' is only supported in database which dbcompatibility='B'.")));

                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->label       = u_sess->plsql_cxt.curr_compile_context->ns_top->name;
                        newp->cond        = $4.expr;
                        newp->body        = $2;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->condition   = false;

                        if(strcmp(newp->label, "") == 0 || u_sess->plsql_cxt.curr_compile_context->ns_top->itemtype != PLPGSQL_NSTYPE_LABEL)
                        {
                            ereport(errstate,
                                   (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("The label name is invalid"),
                                    parser_errposition(@1)));
                        }

                        check_labels(u_sess->plsql_cxt.curr_compile_context->ns_top->name, $4.end_label, $4.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label(u_sess->plsql_cxt.curr_compile_context->ns_top->name, (PLpgSQL_stmt *)newp);
                    }
                ;

label_while :       T_LABELWHILE
                    | ':' K_WHILE
                ;

label_repeat:       T_LABELREPEAT
                    | ':' K_REPEAT
                ;

stmt_for		: opt_block_label K_FOR for_control loop_body
                    {
                        /* This runs after we've scanned the loop body */
                        if ($3->cmd_type == PLPGSQL_STMT_FORI)
                        {
                            PLpgSQL_stmt_fori		*newp;

                            newp = (PLpgSQL_stmt_fori *) $3;
                            newp->lineno   = plpgsql_location_to_lineno(@2);
                            newp->label	  = $1;
                            newp->body	  = $4.stmts;
                            newp->sqlString = plpgsql_get_curline_query();
                            $$ = (PLpgSQL_stmt *) newp;

                            /* register the stmt if it is labeled */
                            record_stmt_label($1, (PLpgSQL_stmt *)newp);
                        }
                        else
                        {
                            PLpgSQL_stmt_forq		*newp;

                            AssertEreport($3->cmd_type == PLPGSQL_STMT_FORS ||
                                          $3->cmd_type == PLPGSQL_STMT_FORC ||
                                          $3->cmd_type == PLPGSQL_STMT_DYNFORS,
                                            MOD_PLSQL,
                                            "unexpected node type.");
                            /* forq is the common supertype of all three */
                            newp = (PLpgSQL_stmt_forq *) $3;
                            newp->lineno   = plpgsql_location_to_lineno(@2);
                            newp->label	  = $1;
                            newp->body	  = $4.stmts;
                            newp->sqlString = plpgsql_get_curline_query();
                            $$ = (PLpgSQL_stmt *) newp;

                            /* register the stmt if it is labeled */
                            record_stmt_label($1, (PLpgSQL_stmt *)newp);
                        }

                        check_labels($1, $4.end_label, $4.end_label_location);
                        /* close namespace started in opt_block_label */
                        plpgsql_ns_pop();
                    }
                | opt_block_label K_FORALL forall_control opt_save_exceptions forall_body
                    {
                        /* This runs after we've scanned the loop body */
                        /* A db FORALL support 3 types like below. We implemented the first one.  
                         * FORALL index_name IN lower_bound .. upper_bound
                         * FORALL index_name IN INDICES OF collection between lower_bound and upper_bound
                         * FORALL index_name IN VALUES OF index_collection
                         * forall_body can only have one statement.
                         */
                        if ($3->cmd_type == PLPGSQL_STMT_FORI)
                        {
                            PLpgSQL_stmt_fori		*newm;

                            newm = (PLpgSQL_stmt_fori *) $3;
                            newm->lineno   = plpgsql_location_to_lineno(@2);
                            newm->label	  = NULL;
                            newm->save_exceptions = $4;
                            newm->body	  = $5;
                            newm->sqlString = plpgsql_get_curline_query();
                            $$ = (PLpgSQL_stmt *) newm;
                        }
                        else {
                            const char* message = "please use \'FORALL index_name IN lower_bound .. upper_bound\'";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("please use \'FORALL index_name IN lower_bound .. upper_bound\'")));
                        }
                        /* close namespace started in opt_block_label */
                        plpgsql_ns_pop();
                    }
                ;

for_control		: for_variable K_IN
                    {
                        int			tok = yylex();
                        int			tokloc = yylloc;
                        if (tok == K_EXECUTE)
                        {
                            /* EXECUTE means it's a dynamic FOR loop */
                            PLpgSQL_stmt_dynfors	*newp;
                            PLpgSQL_expr			*expr;
                            int						term;

                            expr = read_sql_expression2(K_LOOP, K_USING,
                                                        "LOOP or USING",
                                                        &term);

                            newp = (PLpgSQL_stmt_dynfors *)palloc0(sizeof(PLpgSQL_stmt_dynfors));
                            newp->cmd_type = PLPGSQL_STMT_DYNFORS;
                            if ($1.rec)
                            {
                                newp->rec = $1.rec;
                                check_assignable((PLpgSQL_datum *) newp->rec, @1);
                            }
                            else if ($1.row)
                            {
                                newp->row = $1.row;
                                check_assignable((PLpgSQL_datum *) newp->row, @1);
                            }
                            else if ($1.scalar)
                            {
                                /* convert single scalar to list */
                                newp->row = make_scalar_list1($1.name, $1.scalar, $1.dno,
                                                             $1.lineno, @1);
                                /* no need for check_assignable */
                            }
                            else
                            {
                                const char* message = "loop variable of loop over rows must be a record or row variable or list of scalar variables";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                                         errmsg("loop variable of loop over rows must be a record or row variable or list of scalar variables"),
                                         parser_errposition(@1)));
                            }

                            newp->query = expr;

                            if (term == K_USING)
                            {
                                do
                                {
                                    expr = read_sql_expression2(',', K_LOOP,
                                                                ", or LOOP",
                                                                &term);
                                    newp->params = lappend(newp->params, expr);
                                } while (term == ',');
                            }

                            $$ = (PLpgSQL_stmt *) newp;
                        }
                        else if ((tok == T_DATUM || tok == T_PACKAGE_VARIABLE) &&
                                 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR &&
                                 ((PLpgSQL_var *) yylval.wdatum.datum)->datatype->typoid == REFCURSOROID)
                        {
                            /* It's FOR var IN cursor */
                            PLpgSQL_stmt_forc	*newp;
                            PLpgSQL_var			*cursor = (PLpgSQL_var *) yylval.wdatum.datum;

                            newp = (PLpgSQL_stmt_forc *) palloc0(sizeof(PLpgSQL_stmt_forc));
                            newp->cmd_type = PLPGSQL_STMT_FORC;
                            newp->curvar = yylval.wdatum.dno;

                            /* Should have had a single variable name */
                            if ($1.scalar && $1.row) {
                                const char* message = "cursor FOR loop must have only one target variable";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("cursor FOR loop must have only one target variable"),
                                         parser_errposition(@1)));
                            }

                            /* can't use an unbound cursor this way */
                            if (cursor->cursor_explicit_expr == NULL) {
                                const char* message = "cursor FOR loop must use a bound cursor variable";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("cursor FOR loop must use a bound cursor variable"),
                                         parser_errposition(tokloc)));
                            }

                            /* collect cursor's parameters if any */
                            newp->argquery = read_cursor_args(cursor,
                                                             K_LOOP,
                                                             "LOOP");
                            TupleDesc tupleDesc = NULL;
                            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK &&
                                cursor->cursor_explicit_expr->query != NULL) {
                                tupleDesc = getCursorTupleDesc(cursor->cursor_explicit_expr, false);
                            }

                            /* create loop's private RECORD variable */
                            newp->rec = plpgsql_build_record($1.name,
                                                            $1.lineno,
                                                            true,
                                                            tupleDesc);

                            $$ = (PLpgSQL_stmt *) newp;
                        }
                        else
                        {
                            PLpgSQL_expr	*expr1;
                            int				expr1loc;
                            bool			reverse = false;

                            /*
                             * We have to distinguish between two
                             * alternatives: FOR var IN a .. b and FOR
                             * var IN query. Unfortunately this is
                             * tricky, since the query in the second
                             * form needn't start with a SELECT
                             * keyword.  We use the ugly hack of
                             * looking for two periods after the first
                             * token. We also check for the REVERSE
                             * keyword, which means it must be an
                             * integer loop.
                             */
                            if (tok_is_keyword(tok, &yylval,
                                               K_REVERSE, "reverse"))
                                reverse = true;
                            else
                                plpgsql_push_back_token(tok);

                            /*
                             * Read tokens until we see either a ".."
                             * or a LOOP. The text we read may not
                             * necessarily be a well-formed SQL
                             * statement, so we need to invoke
                             * read_sql_construct directly.
                             */
                            expr1 = read_sql_construct(DOT_DOT,
                                                       K_LOOP,
                                                       0,
                                                       "LOOP",
                                                       "SELECT ",
                                                       true,
                                                       false,
                                                       true,
                                                       &expr1loc,
                                                       &tok);

                            if (tok == DOT_DOT)
                            {
                                /* Saw "..", so it must be an integer loop */
                                PLpgSQL_expr		*expr2;
                                PLpgSQL_expr		*expr_by;
                                PLpgSQL_var			*fvar;
                                PLpgSQL_stmt_fori	*newp;

                                /* Check first expression is well-formed */
                                check_sql_expr(expr1->query, expr1loc, 7);

                                /* Read and check the second one */
                                expr2 = read_sql_expression2(K_LOOP, K_BY,
                                                             "LOOP",
                                                             &tok);

                                /* Get the BY clause if any */
                                if (tok == K_BY)
                                    expr_by = read_sql_expression(K_LOOP,
                                                                  "LOOP");
                                else
                                    expr_by = NULL;

                                /* Should have had a single variable name */
                                if ($1.scalar && $1.row) {
                                    const char* message = "integer FOR loop must have only one target variable";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                            (errcode(ERRCODE_SYNTAX_ERROR),
                                             errmsg("integer FOR loop must have only one target variable"),
                                             parser_errposition(@1)));
                                }

                                /* create loop's private variable */
                                fvar = (PLpgSQL_var *)
                                    plpgsql_build_variable($1.name,
                                                           $1.lineno,
                                                           plpgsql_build_datatype(INT4OID,
                                                                                  -1,
                                                                                  InvalidOid),
                                                           true);

                                newp = (PLpgSQL_stmt_fori *)palloc0(sizeof(PLpgSQL_stmt_fori));
                                newp->cmd_type = PLPGSQL_STMT_FORI;
                                newp->var	  = fvar;
                                newp->reverse  = reverse;
                                newp->lower	  = expr1;
                                newp->upper	  = expr2;
                                newp->step	  = expr_by;

                                $$ = (PLpgSQL_stmt *) newp;
                            }
                            else
                            {
                                /*
                                 * No "..", so it must be a query loop. We've
                                 * prefixed an extra SELECT to the query text,
                                 * so we need to remove that before performing
                                 * syntax checking.
                                 */
                                char				*tmp_query;
                                PLpgSQL_stmt_fors	*newp;

                                if (reverse) {
                                    const char* message = "cannot specify REVERSE in query FOR loop";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                            (errcode(ERRCODE_SYNTAX_ERROR),
                                             errmsg("cannot specify REVERSE in query FOR loop"),
                                             parser_errposition(tokloc)));
                                }

                                AssertEreport(strncmp(expr1->query, "SELECT ", 7) == 0,
                                                    MOD_PLSQL,
                                                    "It should be SELECT");
                                tmp_query = pstrdup(expr1->query + 7);
                                pfree_ext(expr1->query);
                                expr1->query = tmp_query;
                              
                                check_sql_expr(expr1->query, expr1loc, 0);
                                newp = (PLpgSQL_stmt_fors *)palloc0(sizeof(PLpgSQL_stmt_fors));
                                newp->cmd_type = PLPGSQL_STMT_FORS;
                                if ($1.rec)
                                {
#ifndef ENABLE_MULTIPLE_NODES
                                    /* only A format and not in upgrade, IMPLICIT_FOR_LOOP_VARIABLE is valid */
                                    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT
                                        && IMPLICIT_FOR_LOOP_VARIABLE
                                        && u_sess->attr.attr_common.upgrade_mode == 0) {
                                        BuildForQueryVariable(expr1, &newp->row, &newp->rec, $1.name, $1.lineno);
                                        check_assignable((PLpgSQL_datum *)newp->rec ?
                                            (PLpgSQL_datum *)newp->rec : (PLpgSQL_datum *)newp->row, @1);
                                    } else {
                                        /* check the sql */
                                        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
                                            (void)getCursorTupleDesc(expr1, false, true);
                                        }
                                        newp->rec = $1.rec;
                                        check_assignable((PLpgSQL_datum *) newp->rec, @1);
                                    }
#else
                                    newp->rec = $1.rec;
                                    check_assignable((PLpgSQL_datum *) newp->rec, @1);
#endif
                                }
                                else if ($1.row)
                                {
#ifndef ENABLE_MULTIPLE_NODES
                                    /* only A format and not in upgrade, IMPLICIT_FOR_LOOP_VARIABLE is valid */
                                    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT
                                        && IMPLICIT_FOR_LOOP_VARIABLE
                                        && u_sess->attr.attr_common.upgrade_mode == 0) {
                                        BuildForQueryVariable(expr1, &newp->row, &newp->rec, $1.name, $1.lineno);
                                        check_assignable((PLpgSQL_datum *)newp->rec ?
                                            (PLpgSQL_datum *)newp->rec : (PLpgSQL_datum *)newp->row, @1);
                                    } else {
                                        /* check the sql */
                                        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
                                            (void)getCursorTupleDesc(expr1, false, true);
                                        }
                                        newp->row = $1.row;
                                        check_assignable((PLpgSQL_datum *) newp->row, @1);
                                    }
#else
                                    newp->row = $1.row;
                                    check_assignable((PLpgSQL_datum *) newp->row, @1);
#endif
                                }
                                else if ($1.scalar)
                                {
#ifndef ENABLE_MULTIPLE_NODES
                                    /* only A format and not in upgrade, IMPLICIT_FOR_LOOP_VARIABLE is valid */
                                    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT
                                        && IMPLICIT_FOR_LOOP_VARIABLE
                                        && u_sess->attr.attr_common.upgrade_mode == 0) {
                                        BuildForQueryVariable(expr1, &newp->row, &newp->rec, $1.name, $1.lineno);
                                        check_assignable((PLpgSQL_datum *)newp->rec ?
                                            (PLpgSQL_datum *)newp->rec : (PLpgSQL_datum *)newp->row, @1);
                                    } else {
                                        /* check the sql */
                                        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
                                            (void)getCursorTupleDesc(expr1, false, true);
                                        }
                                        /* convert single scalar to list */
                                        newp->row = make_scalar_list1($1.name, $1.scalar, $1.dno, $1.lineno, @1);
                                        /* no need for check_assignable */
                                    }
#else
                                    /* convert single scalar to list */
                                    newp->row = make_scalar_list1($1.name, $1.scalar, $1.dno, $1.lineno, @1);
                                    /* no need for check_assignable */
#endif
                                }
                                else
                                {
#ifndef ENABLE_MULTIPLE_NODES
                                    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
                                        BuildForQueryVariable(expr1, &newp->row, &newp->rec, $1.name, $1.lineno);
                                        check_assignable((PLpgSQL_datum *)newp->rec ?
                                            (PLpgSQL_datum *)newp->rec : (PLpgSQL_datum *)newp->row, @1);
                                    } else {
                                        PLpgSQL_type dtype;
                                        dtype.ttype = PLPGSQL_TTYPE_REC;
                                        newp->rec = (PLpgSQL_rec *)
                                        plpgsql_build_variable($1.name,$1.lineno, &dtype, true);
                                        check_assignable((PLpgSQL_datum *) newp->rec, @1);
                                    }
#else
                                    PLpgSQL_type dtype;
                                    dtype.ttype = PLPGSQL_TTYPE_REC;
                                    newp->rec = (PLpgSQL_rec *) 
                                    plpgsql_build_variable($1.name,$1.lineno, &dtype, true);
                                    check_assignable((PLpgSQL_datum *) newp->rec, @1);
#endif
                                }

                                newp->query = expr1;
                                
                                $$ = (PLpgSQL_stmt *) newp;
                            }
                        }
                    }
                ;
forall_control         :for_variable K_IN
                    {
                        int		tok;
                        int		expr1loc;
                        PLpgSQL_expr	*expr1;

                        /*
                         * Read tokens until we see either a ".."
                         * or a LOOP. The text we read may not
                         * necessarily be a well-formed SQL
                         * statement, so we need to invoke
                         * read_sql_construct directly.
                         */
                        expr1 = read_sql_construct(DOT_DOT,
                                                   0,
                                                   0,
                                                   "..",
                                                   "SELECT ",
                                                   true,
                                                   false,
                                                   true,
                                                   &expr1loc,
                                                   &tok);

                        if (DOT_DOT == tok)
                        {
                            /* Saw "..", so it must be an integer loop */
                            PLpgSQL_expr		*expr2 = NULL;
                            PLpgSQL_var		*fvar = NULL;
                            PLpgSQL_stmt_fori	*newm = NULL;

                            /* Check first expression is well-formed */
                            check_sql_expr(expr1->query, expr1loc, 7);

                            /* Read and check the second one */
                            expr2 = read_sql_construct6(K_MERGE,
                                                        K_INSERT, 
                                                        K_SELECT,
                                                        K_UPDATE,
                                                        K_DELETE,
                                                        K_SAVE,
                                                        "DML",
                                                        "SELECT ",
                                                        true,
                                                        false,
                                                        true,
                                                        NULL,
                                                        &tok);

                            plpgsql_push_back_token(tok);

                            if (';' == tok) {
                                const char* message = "FORALL must follow DML statement.";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_FORALL_NEED_DML),
                                         errmsg("FORALL must follow DML statement.")));
                            }

                            /* should follow DML statement */
                            if (tok != K_INSERT && tok != K_UPDATE && tok != K_DELETE && tok != K_SELECT && tok != K_MERGE && tok != K_SAVE) { 
                                const char* message = "FORALL must follow DML statement.";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_FORALL_NEED_DML),
                                         errmsg("FORALL must follow DML statement.")));
                            }

                            if (tok == K_SAVE) {
                                CheckSaveExceptionsDML(errstate);
                            }

                            /* Should have had a single variable name */
                            if ($1.scalar && $1.row) {
                                const char* message = "integer FORALL must have just one target variable.";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("integer FORALL must have just one target variable")));
                            }

                            /* create loop's private variable */
                            fvar = (PLpgSQL_var *)
                                plpgsql_build_variable($1.name,
                                                       $1.lineno,
                                                       plpgsql_build_datatype(INT4OID, -1, InvalidOid),
                                                       true);

                            newm = (PLpgSQL_stmt_fori *)palloc0(sizeof(PLpgSQL_stmt_fori));
                            newm->cmd_type = PLPGSQL_STMT_FORI;
                            newm->var	 = fvar;
                            newm->reverse = false;
                            newm->lower	 = expr1;
                            newm->upper	 = expr2;
                            newm->step	 = NULL;

                            $$ = (PLpgSQL_stmt *) newm;
                        }
                        else {
                            const char* message = "please use \'FORALL index_name IN lower_bound .. upper_bound\'";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("please use \'FORALL index_name IN lower_bound .. upper_bound\'")));
                        }
                    }
                ;

opt_save_exceptions : K_SAVE K_EXCEPTIONS
                    {
#ifndef ENABLE_MULTIPLE_NODES
                        $$ = true;
#else
                        const char* message = "SAVE EXCEPTIONS is not supported";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("%s.", message)));
                        $$ = false;
#endif
                    }
                    |
                    {
                        $$ = false;
                    }
/*
 * Processing the for_variable is tricky because we don't yet know if the
 * FOR is an integer FOR loop or a loop over query results.  In the former
 * case, the variable is just a name that we must instantiate as a loop
 * local variable, regardless of any other definition it might have.
 * Therefore, we always save the actual identifier into $$.name where it
 * can be used for that case.  We also save the outer-variable definition,
 * if any, because that's what we need for the loop-over-query case.  Note
 * that we must NOT apply check_assignable() or any other semantic check
 * until we know what's what.
 *
 * However, if we see a comma-separated list of names, we know that it
 * can't be an integer FOR loop and so it's OK to check the variables
 * immediately.  In particular, for T_WORD followed by comma, we should
 * complain that the name is not known rather than say it's a syntax error.
 * Note that the non-error result of this case sets *both* $$.scalar and
 * $$.row; see the for_control production.
 */
for_variable	: T_DATUM
                    {
                        $$.name = NameOfDatum(&($1));
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW)
                        {
                            $$.scalar = NULL;
                            $$.rec = NULL;
                            $$.row = (PLpgSQL_row *) $1.datum;
                            $$.dno = $1.dno;
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_RECORD)
                        {
                            $$.scalar = NULL;
                            $$.rec = NULL;
                            $$.row = (PLpgSQL_row *) $1.datum;
                            $$.dno = $1.dno;
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_REC)
                        {
                            $$.scalar = NULL;
                            $$.rec = (PLpgSQL_rec *) $1.datum;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                        }
                        else
                        {
                            int			tok;

                            $$.scalar = $1.datum;
                            $$.rec = NULL;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                            /* check for comma-separated list */
                            tok = yylex();
                            plpgsql_push_back_token(tok);
                            if (tok == ',')
                                $$.row = read_into_scalar_list($$.name,
                                                               $$.scalar,
                                                               $$.dno,
                                                               @1);
                        }
                    }
                | T_VARRAY_VAR
                    {
                        $$.name = NameOfDatum(&($1));
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW)
                        {
                            $$.scalar = NULL;
                            $$.rec = NULL;
                            $$.row = (PLpgSQL_row *) $1.datum;
                            $$.dno = $1.dno;
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_REC)
                        {
                            $$.scalar = NULL;
                            $$.rec = (PLpgSQL_rec *) $1.datum;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                        }
                        else
                        {
                            int			tok;

                            $$.scalar = $1.datum;
                            $$.rec = NULL;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                            /* check for comma-separated list */
                            tok = yylex();
                            plpgsql_push_back_token(tok);
                            if (tok == ',')
                                $$.row = read_into_scalar_list($$.name,
                                                               $$.scalar,
                                                               $$.dno,
                                                               @1);
                        }
                    }
                | T_TABLE_VAR
                    {
                        $$.name = NameOfDatum(&($1));
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW)
                        {
                            $$.scalar = NULL;
                            $$.rec = NULL;
                            $$.row = (PLpgSQL_row *) $1.datum;
                            $$.dno = $1.dno;
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_REC)
                        {
                            $$.scalar = NULL;
                            $$.rec = (PLpgSQL_rec *) $1.datum;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                        }
                        else
                        {
                            int			tok;

                            $$.scalar = $1.datum;
                            if ($1.datum->dtype == PLPGSQL_DTYPE_VAR) {
                                check_autonomous_nest_tablevar((PLpgSQL_var*)$1.datum);
                            }
                            $$.rec = NULL;
                            $$.row = NULL;
                            $$.dno = $1.dno;
                            /* check for comma-separated list */
                            tok = yylex();
                            plpgsql_push_back_token(tok);
                            if (tok == ',')
                                $$.row = read_into_scalar_list($$.name,
                                                               $$.scalar,
                                                               $$.dno,
                                                               @1);
                        }
                    }
                | T_WORD
                    {
                        int			tok;

                        $$.name = $1.ident;
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        $$.scalar = NULL;
                        $$.rec = NULL;
                        $$.row = NULL;
                        /* check for comma-separated list */
                        tok = yylex();
                        plpgsql_push_back_token(tok);
                        if (tok == ',')
                            word_is_not_variable(&($1), @1);
                    }
                | T_CWORD
                    {
                        /* just to give a better message than "syntax error" */
                        cword_is_not_variable(&($1), @1);
                    }
                ;

stmt_foreach_a	: opt_block_label K_FOREACH for_variable foreach_slice K_IN K_ARRAY expr_until_loop loop_body
                    {
                        PLpgSQL_stmt_foreach_a *newp;

                        newp = (PLpgSQL_stmt_foreach_a *)palloc0(sizeof(PLpgSQL_stmt_foreach_a));
                        newp->cmd_type = PLPGSQL_STMT_FOREACH_A;
                        newp->lineno = plpgsql_location_to_lineno(@2);
                        newp->label = $1;
                        newp->slice = $4;
                        newp->expr = $7;
                        newp->body = $8.stmts;
                        newp->sqlString = plpgsql_get_curline_query();

                        if ($3.rec)
                        {
                            newp->varno = $3.dno;
                            check_assignable((PLpgSQL_datum *) $3.rec, @3);
                        }
                        else if ($3.row)
                        {
                            if ($3.scalar) {
                                newp->varno = $3.row->dno;
                            } else {
                                newp->varno = $3.dno;
                            }
                            check_assignable((PLpgSQL_datum *) $3.row, @3);
                        }
                        else if ($3.scalar)
                        {
                            newp->varno = $3.dno;
                            check_assignable($3.scalar, @3);
                        }
                        else
                        {
                            const char* message = "loop variable of FOREACH must be a known variable or list of variables";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("loop variable of FOREACH must be a known variable or list of variables"),
                                             parser_errposition(@3)));
                        }

                        check_labels($1, $8.end_label, $8.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *) newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

foreach_slice	:
                    {
                        $$ = 0;
                    }
                | K_SLICE ICONST
                    {
                        $$ = $2;
                    }
                ;
forall_body		: stmt_dynexecute
                    { $$ = list_make1($1); }
                | stmt_execsql
                    { $$ = list_make1($1); }
                ;

stmt_exit		: exit_type opt_label opt_exitcond
                    {
                        PLpgSQL_stmt_exit *newp;

                        newp = (PLpgSQL_stmt_exit *)palloc0(sizeof(PLpgSQL_stmt_exit));
                        newp->cmd_type = PLPGSQL_STMT_EXIT;
                        newp->is_exit  = $1;
                        newp->lineno	  = plpgsql_location_to_lineno(@1);
                        newp->label	  = $2;
                        newp->cond	  = $3;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

exit_type		: K_EXIT
                    {
                        $$ = true;
                    }
                | K_CONTINUE
                    {
                        $$ = false;
                    }
                | K_LEAVE
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "leave label"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                             ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("'LEAVE label' is only in database which dbcompatibility='B'.")));

                        $$ = true;
                    }
                | K_ITERATE
                    {
                        /*
                         * When the database is in mysql compatible mode
                         * support "iterate label"
                         */
                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT)
                             ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("'ITERATE label' is only in database which dbcompatibility='B'.")));

                        $$ = false;
                    }
                ;

stmt_return		: K_RETURN
                    {
                        int	tok;

                        tok = yylex();
                        if (tok == 0)
                            yyerror("unexpected end of function definition");

                        if (tok_is_keyword(tok, &yylval,
                                           K_NEXT, "next"))
                        {
                            u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                            $$ = make_return_next_stmt(@1);
                        }
                        else if (tok_is_keyword(tok, &yylval,
                                                K_QUERY, "query"))
                        {
                            u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                            $$ = make_return_query_stmt(@1);
                        }
                        else
                        {
                            plpgsql_push_back_token(tok);
                            $$ = make_return_stmt(@1);
                        }
                    }
                ;

stmt_raise		: K_RAISE
                    {
                        PLpgSQL_stmt_raise		*newp;
                        int	tok;
                        
                        PLpgSQL_row * row = NULL;
                        char message[32] = "";
                        StringInfoData ds;
                        initStringInfo(&ds);

                        newp = (PLpgSQL_stmt_raise *)palloc(sizeof(PLpgSQL_stmt_raise));

                        newp->cmd_type	= PLPGSQL_STMT_RAISE;
                        newp->lineno		= plpgsql_location_to_lineno(@1);
                        newp->elog_level = ERROR;	/* default */
                        newp->condname	= NULL;
                        newp->message	= NULL;
                        newp->params		= NIL;
                        newp->options	= NIL;
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->hasExceptionInit = false;

                        tok = yylex();
                        if (tok == 0)
                            yyerror("unexpected end of function definition");

                        /*
                         * We could have just RAISE, meaning to re-throw
                         * the current error.
                         */
                        if (tok != ';')
                        {
                            if (T_CWORD == tok) {
                                yyerror("not found package");                      
                            }
                            if ((T_DATUM == tok || T_PACKAGE_VARIABLE == tok) && PLPGSQL_DTYPE_ROW == yylval.wdatum.datum->dtype)
                            {
                                PLpgSQL_expr *expr = NULL;
                                errno_t rc = EOK;

                                rc = sprintf_s(message, sizeof(message), "line:%d ", plpgsql_location_to_lineno(@1));
                                securec_check_ss(rc, "\0", "\0");
                                appendStringInfoString(&ds, message);
                                appendStringInfoString(&ds,"%");

                                row = (PLpgSQL_row *)yylval.wdatum.datum;

                                /* condname is system embedded error name, so it is still null in this case. */
                                newp->hasExceptionInit = row->hasExceptionInit;
                                newp->condname = pstrdup(plpgsql_get_sqlstate(row->customErrorCode));
                                newp->message = pstrdup(ds.data);
                                plpgsql_push_back_token(tok);
                                expr = read_sql_construct(';', 0, 0, ";",
                                                          "SELECT ", true, true, true, NULL, &tok);

                                if (tok != ';')
                                    yyerror("syntax error");

                                newp->params = lappend(newp->params, expr);
                            }
                            else
                            {
                                /*
                                 * First is an optional elog severity level.
                                 */
                                if (tok_is_keyword(tok, &yylval,
                                                   K_EXCEPTION, "exception"))
                                {
                                    newp->elog_level = ERROR;
                                    tok = yylex();
                                }
                                else if (tok_is_keyword(tok, &yylval,
                                                        K_WARNING, "warning"))
                                {
                                    newp->elog_level = WARNING;
                                    tok = yylex();
                                }
                                else if (tok_is_keyword(tok, &yylval,
                                                        K_NOTICE, "notice"))
                                {
                                    newp->elog_level = NOTICE;
                                    tok = yylex();
                                }
                                else if (tok_is_keyword(tok, &yylval,
                                                        K_INFO, "info"))
                                {
                                    newp->elog_level = INFO;
                                    tok = yylex();
                                }
                                else if (tok_is_keyword(tok, &yylval,
                                                        K_LOG, "log"))
                                {
                                    newp->elog_level = LOG;
                                    tok = yylex();
                                }
                                else if (tok_is_keyword(tok, &yylval,
                                                        K_DEBUG, "debug"))
                                {
                                    newp->elog_level = DEBUG1;
                                    tok = yylex();
                                }
                                
                                if (tok == 0)
                                    yyerror("unexpected end of function definition");

                            /*
                             * Next we can have a condition name, or
                             * equivalently SQLSTATE 'xxxxx', or a string
                             * literal that is the old-style message format,
                             * or USING to start the option list immediately.
                             */
                            if (tok == SCONST)
                            {
                                /* old style message and parameters */
                                newp->message = yylval.str;
                                /*
                                 * We expect either a semi-colon, which
                                 * indicates no parameters, or a comma that
                                 * begins the list of parameter expressions,
                                 * or USING to begin the options list.
                                 */
                                tok = yylex();
                                if (tok != ',' && tok != ';' && tok != K_USING)
                                    yyerror("syntax error");

                                while (tok == ',')
                                {
                                    PLpgSQL_expr *expr;

                                    expr = read_sql_construct(',', ';', K_USING,
                                                              ", or ; or USING",
                                                              "SELECT ",
                                                              true, true, true,
                                                              NULL, &tok);
                                    newp->params = lappend(newp->params, expr);
                                }
                            }
                            else if (tok != K_USING)
                            {
                                /* must be condition name or SQLSTATE */
                                if (tok_is_keyword(tok, &yylval,
                                                   K_SQLSTATE, "sqlstate"))
                                {
                                    /* next token should be a string literal */
                                    char   *sqlstatestr;

                                    if (yylex() != SCONST)
                                        yyerror("syntax error");
                                    sqlstatestr = yylval.str;

                                    if (strlen(sqlstatestr) != 5)
                                        yyerror("invalid SQLSTATE code");
                                    if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
                                        yyerror("invalid SQLSTATE code");
                                    newp->condname = sqlstatestr;
                                }
                                else
                                {
                                    if (tok != T_WORD)
                                        yyerror("syntax error");
                                    newp->condname = yylval.word.ident;
                                    plpgsql_recognize_err_condition(newp->condname,
                                                                    false);
                                }
                                tok = yylex();
                                if (tok != ';' && tok != K_USING)
                                    yyerror("syntax error");
                            }

                            if (tok == K_USING)
                                newp->options = read_raise_options();
                            }
                        }
                        pfree_ext(ds.data);
                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_signal		: K_SIGNAL
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
                            const char *message = "SIGNAL is supported only in B-format database.";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                (errmodule(MOD_PARSER),
                                    errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("SIGNAL is supported only in B-format database."),
                                    parser_errposition(@1)));
                            $$ = NULL;/* not reached */		
                        }
                        int tok;
                        PLpgSQL_stmt_signal *newp;

                        newp = (PLpgSQL_stmt_signal *)palloc(sizeof(PLpgSQL_stmt_signal));

                        newp->cmd_type = PLPGSQL_STMT_SIGNAL;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->sqlString = plpgsql_get_curline_query();

                        tok = yylex();
                        if (tok == 0) {
                            yyerror("unexpected end of function definition");
                        }

                        /* must be condition name or SQLSTATE */
                        if (tok_is_keyword(tok, &yylval, K_SQLSTATE, "sqlstate")) {
                            tok = yylex();

                            read_signal_sqlstate(newp, tok);
                        } else {
                            read_signal_condname(newp, tok);
                        }

                        tok = yylex();
                        read_signal_set(newp, tok);

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_resignal	: K_RESIGNAL
                    {
                        if (u_sess->attr.attr_sql.sql_compatibility != B_FORMAT) {
                            const char *message = "RESIGNAL is supported only in B-format database.";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                (errmodule(MOD_PARSER),
                                    errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("RESIGNAL is supported only in B-format database."),
                                    parser_errposition(@1)));
                            $$ = NULL;/* not reached */		
                        }
                        int tok;
                        PLpgSQL_stmt_signal *newp;

                        newp = (PLpgSQL_stmt_signal *)palloc(sizeof(PLpgSQL_stmt_signal));

                        newp->cmd_type = PLPGSQL_STMT_RESIGNAL;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->sqlString = plpgsql_get_curline_query();

                        tok = yylex();
                        if (tok == 0) {
                            yyerror("unexpected end of function definition");
                        }

                        if (tok == ';') {
                            newp->sqlerrstate = -1;
                            newp->sqlstate = NULL;
                            newp->condname = NULL;
                            newp->cond_info_item = NIL;
                        } else if (tok == T_WORD && pg_strcasecmp(yylval.word.ident, "set") == 0) {
                            newp->sqlerrstate = -1;
                            newp->sqlstate = NULL;
                            newp->condname = NULL;
                            newp->cond_info_item = read_signal_items();
                        } else if (tok_is_keyword(tok, &yylval, K_SQLSTATE, "sqlstate")) {
                            tok = yylex();
                            
                            read_signal_sqlstate(newp, tok);
                            
                            tok = yylex();
                            read_signal_set(newp, tok);
                        } else {
                            read_signal_condname(newp, tok);
                            
                            tok = yylex();
                            read_signal_set(newp, tok);
                        }

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

loop_body		: proc_sect K_END K_LOOP opt_label ';'
                    {
                        $$.stmts = $1;
                        $$.end_label = $4;
                        $$.end_label_location = @4;
                    }
                ;

while_body              : proc_sect K_END K_WHILE opt_label ';'
                    {
                        $$.stmts = $1;
                        $$.end_label = $4;
                        $$.end_label_location = @4;
                    }
                 ;

repeat_condition        : repeat_condition_expr K_REPEAT opt_label ';'
                    {
                        $$.expr = $1;
                        $$.end_label = $3;
                        $$.end_label_location = @3;
                    }
                ;

repeat_condition_expr :
                     { $$ = read_sql_expression(K_END, "end"); }
                ;

/*
 * T_WORD+T_CWORD match any initial identifier that is not a known plpgsql
 * variable.  (The composite case is probably a syntax error, but we'll let
 * the core parser decide that.)  Normally, we should assume that such a
 * word is a SQL statement keyword that isn't also a plpgsql keyword.
 * However, if the next token is assignment or '[', it can't be a valid
 * SQL statement, and what we're probably looking at is an intended variable
 * assignment.  Give an appropriate complaint for that, instead of letting
 * the core parser throw an unhelpful "syntax error".
 */
stmt_execsql			: K_ALTER
                    {
                        $$ = make_execsql_stmt(K_ALTER, @1);
                    }
                | K_INSERT
                    {
                        $$ = make_execsql_stmt(K_INSERT, @1);
                    }
                | K_REPLACE
                    {
                        $$ = make_execsql_stmt(K_REPLACE, @1);
                    }
                | K_SELECT		/* DML:select */
                    {
                        int tok = -1;

                        tok = yylex();
                        plpgsql_push_back_token(tok);
                        $$ = make_execsql_stmt(K_SELECT, @1);
                    }
                
                | K_UPDATE		/* DML:insert */
                    {
                        int			tok = -1;

                        tok = yylex();
                        plpgsql_push_back_token(tok);
                        $$ = make_execsql_stmt(K_UPDATE, @1);
                    }
                | K_DELETE		/* DML:delete */
                    {
                        int			tok = -1;

			tok = yylex();
			plpgsql_push_back_token(tok);
			$$ = make_execsql_stmt(K_DELETE, @1);
		    }
		| K_WITH
		    {
		    	$$ = make_execsql_stmt(K_WITH, @1);
		    }
		| K_MERGE
		    {
		    	$$ = make_execsql_stmt(K_MERGE, @1);
		    }
        | K_CALL
            {
                if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
                {
                    if(plpgsql_is_token_match2(T_CWORD,'(')
                       || plpgsql_is_token_match2(T_CWORD,';')
                       || plpgsql_is_token_match(T_WORD))
                    {
                        $$ = NULL;
                    }
                    else if(plpgsql_is_token_match2(K_CALL,'(') || plpgsql_is_token_match2(K_CALL,';'))
                    {
                        int    tok = yylex();
                        char*  funcname = yylval.word.ident;
                        PLpgSQL_stmt *stmt = funcname_is_call(funcname, @1);
                        if(stmt != NULL)
                        {
                            $$ = stmt;
                        }
                        else
                        {
                            plpgsql_push_back_token(tok);
                            $$ = make_execsql_stmt(K_CALL, @1);
                        }
                    }
                    else if(plpgsql_is_token_match('(') || plpgsql_is_token_match(';'))
                    {
                        char*  funcname = (char*) $1;
                        PLpgSQL_stmt *stmt = funcname_is_call(funcname, @1);
                        if(stmt != NULL)
                        {
                            $$ = stmt;
                        }
                        else
                        {
                            $$ = make_execsql_stmt(K_CALL, @1);
                        }
                    }
                    else
                    {
                        int    tok = yylex();
                        if(yylval.str != NULL && is_unreserved_keyword_func(yylval.str))
                        {
                            plpgsql_push_back_token(tok);
                            $$ = NULL;
                        }
                        else
                        {
                            plpgsql_push_back_token(tok);
                            $$ = make_execsql_stmt(K_CALL, @1);
                        }
                    }
                }
                else
                {
                    if(plpgsql_is_token_match('(') || plpgsql_is_token_match(';'))
                    {
                        char*  funcname = (char*) $1;
                        PLpgSQL_stmt *stmt = funcname_is_call(funcname, @1);
                        if(stmt != NULL)
                        {
                            $$ = stmt;
                        }
                        else
                        {
                            $$ = make_execsql_stmt(K_CALL, @1);
                        }
                    }
                    else
                    {
                        $$ = make_execsql_stmt(K_CALL, @1);
                    }
                }
            }
        | T_WORD    /*C-style function call */
            {
                int tok = -1;
                bool isCallFunc = false;
                bool FuncNoarg = false;

                if (0 == strcasecmp($1.ident, "DBMS_LOB")
                && (plpgsql_is_token_match2('.', K_OPEN)
                || plpgsql_is_token_match2('.', K_CLOSE)))
                    $$ = parse_lob_open_close(@1);
                else
                {
                    tok = yylex();
                    if ('(' == tok)
                        isCallFunc = is_function($1.ident, false, false);
                    else if ('=' ==tok || COLON_EQUALS == tok || '[' == tok)
                        word_is_not_variable(&($1), @1);
                    else if (';' == tok)
                    {
                        isCallFunc = is_function($1.ident, false, true);
                        FuncNoarg = true;
                    }

                    plpgsql_push_back_token(tok);
                    if(isCallFunc)
                    {
                        if (FuncNoarg)
                        {
                            $$ = make_callfunc_stmt_no_arg($1.ident, @1);
                        }
                        else
                        {
                            PLpgSQL_stmt *stmt = make_callfunc_stmt($1.ident, @1, false, false);
                            if (stmt->cmd_type == PLPGSQL_STMT_PERFORM)
                            {
                                ((PLpgSQL_stmt_perform *)stmt)->expr->is_funccall = true;
                            }
                            else if (stmt->cmd_type == PLPGSQL_STMT_EXECSQL)
                            {
                                ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
                            }
                            $$ = stmt;
                        }
                    }
                    else
                    {
                        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                        $$ = make_execsql_stmt(T_WORD, @1);
                    }
                }
            }
        | unreserved_keyword_func
            {
                int tok = -1;
                bool isCallFunc = false;
                bool FuncNoarg = false;
                tok = yylex();
                if ('(' == tok)
                    isCallFunc = is_function($1, false, false);
                else if (';' == tok)
                {
                    isCallFunc = is_function($1, false, true);
                    FuncNoarg = true;
                }

                plpgsql_push_back_token(tok);
                if(isCallFunc)
                {
                    if (FuncNoarg)
                    {
                        $$ = make_callfunc_stmt_no_arg($1, @1);
                    }
                    else
                    {
                         PLpgSQL_stmt *stmt = make_callfunc_stmt($1, @1, false, false);
                         if (stmt == NULL)
                         {
                             plpgsql_push_back_token(tok);
                             $$ = NULL;
                             yyerror("syntax error");
                         }
                         else
                         {
                             if (stmt->cmd_type == PLPGSQL_STMT_PERFORM)
                             {
                                 ((PLpgSQL_stmt_perform *)stmt)->expr->is_funccall = true;
                             }
                             else if (stmt->cmd_type == PLPGSQL_STMT_EXECSQL)
                             {
                                ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
                             }
                             $$ = stmt;
                        }
                    }
                }
                else
                {
                    plpgsql_push_back_token(tok);
                    $$ = NULL;
                    yyerror("syntax error");
                }
            }
        | T_CWORD '('
            {

                char *name = NULL;
                bool isCallFunc = false;
                bool FuncNoarg = false;
                checkFuncName($1.idents);
                MemoryContext colCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
                name = NameListToString($1.idents);
                (void)MemoryContextSwitchTo(colCxt);
                isCallFunc = is_function(name, false, false, $1.idents);
                if (isCallFunc)
                {    
                    if (FuncNoarg)
                        $$ = make_callfunc_stmt_no_arg(name, @1, false, $1.idents); 
                    else 
                    {    
                        PLpgSQL_stmt *stmt = make_callfunc_stmt(name, @1, false, true, $1.idents);
                        if (stmt == NULL) {
                            const char* message = "call func stmt was null";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(ERROR,
                                    (errcode(ERRCODE_DATA_EXCEPTION),
                                        errmsg("call func stmt was null"),
                                        errdetail("call func stmt was null, you can try to recompile the function")));
                        }
                        if (stmt->cmd_type == PLPGSQL_STMT_PERFORM)
                        {    
                            ((PLpgSQL_stmt_perform *)stmt)->expr->is_funccall = true;
                        }    
                        else if (stmt->cmd_type == PLPGSQL_STMT_EXECSQL)
                        {    
                            ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
                        }    
                        $$ = stmt;
                    }    
                }    
                else 
                {    
                    $$ = make_execsql_stmt(T_CWORD, @1); 
                }    
            }   
        | T_CWORD ';'
            {
                char *name = NULL;
                bool isCallFunc = false;
                checkFuncName($1.idents);
                MemoryContext colCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
                name = NameListToString($1.idents);
                (void)MemoryContextSwitchTo(colCxt);
                isCallFunc = is_function(name, false, true, $1.idents);
                bool FuncNoarg = true;

                if (isCallFunc)
                {
                    if (FuncNoarg)
                        $$ = make_callfunc_stmt_no_arg(name, @1, true, $1.idents);
                    else
                    {
                        PLpgSQL_stmt *stmt = make_callfunc_stmt(name, @1, false, true, $1.idents);
                        if (stmt->cmd_type == PLPGSQL_STMT_PERFORM)
                        {
                            ((PLpgSQL_stmt_perform *)stmt)->expr->is_funccall = true;
                        }
                        else if (stmt->cmd_type == PLPGSQL_STMT_EXECSQL)
                        {
                            ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
                        }
                        $$ = stmt;
                    }
                }
                else
                {
                    $$ = make_execsql_stmt(T_CWORD, @1);
                }
            }
        | T_ARRAY_TRIM
            {
                check_table_index(yylval.wdatum.datum, "trim");
                int dno = yylval.wdatum.dno;
                StringInfoData sqlBuf;
                initStringInfo(&sqlBuf);
                appendStringInfo(&sqlBuf, "array_trim(");
                CastArrayNameToArrayFunc(&sqlBuf, yylval.wdatum.idents);

                int tok = yylex();

                if (';' == tok)
                {
                    appendStringInfo(&sqlBuf, " 1)");
                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                } else {
                    if('(' == tok) {
                        int tok1 = yylex();
                        if (tok1 == ')') {
                            int tok2 = yylex();
                            if (tok2 == ';') {
                                appendStringInfo(&sqlBuf, " 1)");
                                $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                            } else {
                                plpgsql_push_back_token(tok);
                                $$ = NULL;
                                yyerror("syntax error");
                            }
                        } else if (tok1 != ICONST && tok1 != T_WORD && tok1 != SCONST && tok1 != T_DATUM) {
                            plpgsql_push_back_token(tok);
                            $$ = NULL;
                            yyerror("syntax error");
                        } else {
                            if (ICONST == tok1) {
                                appendStringInfo(&sqlBuf, " %d)", yylval.ival);
                            } else if (SCONST == tok1) {
                                appendStringInfo(&sqlBuf, " %s)  ", yylval.str);
                            } else if (T_WORD == tok1) {
                                appendStringInfo(&sqlBuf, " %s)", yylval.word.ident);
                            } else {
                                char *datName = NameOfDatum(&yylval.wdatum);
                                appendStringInfo(&sqlBuf, " %s)  ",  datName);
                                pfree_ext(datName);
                            }
                            int tok2 = yylex();
                            if (tok2 != ')') {
                                plpgsql_push_back_token(tok);
                                $$ = NULL;
                                yyerror("syntax error");
                            } else {
                                int tok3 = yylex();
                                if (tok3 != ';') {
                                    plpgsql_push_back_token(tok);
                                    $$ = NULL;
                                    yyerror("syntax error");
                                } else {
                                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                                }
                            }
                        }
                    } else {
                        plpgsql_push_back_token(tok);
                        $$ = NULL;
                        yyerror("syntax error");
                    }
                }
                pfree_ext(sqlBuf.data);
            }
        | T_ARRAY_EXTEND
            {
                check_table_index(yylval.wdatum.datum, "extend");
                int dno = yylval.wdatum.dno;
                StringInfoData sqlBuf;

                initStringInfo(&sqlBuf);
                appendStringInfo(&sqlBuf, "array_extendnull(");
                CastArrayNameToArrayFunc(&sqlBuf, yylval.wdatum.idents);

                int tok = yylex();
                if (';' == tok)
                {
                    appendStringInfo(&sqlBuf, " 1)");
                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                } else {
                    if('(' == tok) {
                        int tok1 = yylex();
                        if (tok1 == ')') {
                            int tok2 = yylex();
                            if (tok2 == ';') {
                                appendStringInfo(&sqlBuf, " 1)");
                                $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                            } else {
                                plpgsql_push_back_token(tok2);
                                $$ = NULL;
                                yyerror("syntax error");
                            }
                        } else if (tok1 != ICONST && tok1 != T_WORD && tok1 != SCONST && tok1 != T_DATUM) {
                            plpgsql_push_back_token(tok);
                            $$ = NULL;
                            yyerror("syntax error");
                        } else {
                            if (ICONST == tok1) {
                                appendStringInfo(&sqlBuf, " %d)  ",  yylval.ival);
                            } else if (SCONST == tok1) {
                                appendStringInfo(&sqlBuf, " %s)  ", yylval.str);
                            } else if (T_WORD == tok1) {
                                appendStringInfo(&sqlBuf, " %s)  ", yylval.word.ident);
                            } else {
                                char *datName = NameOfDatum(&yylval.wdatum);
                                appendStringInfo(&sqlBuf, " %s)  ",  datName);
                                pfree_ext(datName);
                            }
                            int tok2 = yylex();
                            if (tok2 != ')') {
                                plpgsql_push_back_token(tok);
                                $$ = NULL;
                                yyerror("syntax error");
                            } else {
                                int tok3 = yylex();
                                if (tok3 != ';') {
                                    plpgsql_push_back_token(tok);
                                    $$ = NULL;
                                    yyerror("syntax error");
                                } else {
                                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                                }
                            }
                        }
                    } else {
                        plpgsql_push_back_token(tok);
                        $$ = NULL;
                        yyerror("syntax error");
                    }
                }
                pfree_ext(sqlBuf.data);
            }
        | T_ARRAY_DELETE
            {
                StringInfoData sqlBuf;
                Oid indexType = get_table_index_type(yylval.wdatum.datum, NULL);
                List* idents = yylval.wdatum.idents;
                int dno = yylval.wdatum.dno;
                initStringInfo(&sqlBuf);

                int tok = yylex();

                if (';' == tok)
                {
                    if (indexType == VARCHAROID || indexType == INT4OID) {
                        appendStringInfo(&sqlBuf, "array_indexby_delete(");
                        CastArrayNameToArrayFunc(&sqlBuf, idents, false);
                        appendStringInfo(&sqlBuf, ")");
                    } else {
                        appendStringInfo(&sqlBuf, "array_delete(");
                        CastArrayNameToArrayFunc(&sqlBuf, idents, false);
                        appendStringInfo(&sqlBuf, ")");
                    }
                    
                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                } else {
                    if('(' == tok) {
                        int tok1 = yylex();
                        if (tok1 == ')') {
                            int tok2 = yylex();
                            if (tok2 == ';') {
                                if (indexType == VARCHAROID || indexType == INT4OID) {
                                    appendStringInfo(&sqlBuf, "array_indexby_delete(");
                                    CastArrayNameToArrayFunc(&sqlBuf, idents, false);
                                    appendStringInfo(&sqlBuf, ")");
                                } else {
                                    appendStringInfo(&sqlBuf, "array_delete(");
                                    CastArrayNameToArrayFunc(&sqlBuf, idents, false);
                                    appendStringInfo(&sqlBuf, ")");
                                }
                                
                                $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                            } else {
                                plpgsql_push_back_token(tok);
                                $$ = NULL;
                                yyerror("syntax error");
                            }
                        } else if (tok1 != ICONST && tok1 != T_WORD && tok1 != SCONST && tok1 != T_DATUM && tok1 != '-') {
                            plpgsql_push_back_token(tok);
                            $$ = NULL;
                            yyerror("syntax error");
                        } else {
                            if (tok1 == '-') {
                                if (indexType == VARCHAROID) {
                                    yyerror("syntax error");
                                }

                                int tok3 = yylex();
                                if (tok3 != ICONST && tok3 != T_WORD && tok3 != T_DATUM) {
                                    yyerror("syntax error");
                                }

                                if (ICONST == tok3) {
                                    if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'-%d\')", yylval.ival);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "-%d)", yylval.ival);
                                    }
                                } else if (T_WORD == tok3) {
                                    if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'-%s\')", yylval.word.ident);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "-%s)", yylval.word.ident);
                                    }

                                } else {
                                    char *datName = NameOfDatum(&yylval.wdatum);
                                    if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'-%s\')", datName);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "-%s)", datName);
                                    }

                                    pfree_ext(datName);
                                }
                            } else {
                                if (ICONST == tok1) {
                                    if (indexType == VARCHAROID) {
                                        appendStringInfo(&sqlBuf, "array_varchar_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%d\')", yylval.ival);
                                    } else if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%d\')", yylval.ival);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "%d)", yylval.ival);
                                    }
                                } else if (SCONST == tok1) {
                                    if (indexType == VARCHAROID) {
                                        appendStringInfo(&sqlBuf, "array_varchar_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", yylval.str);
                                    } else if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", yylval.str);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "%s)", yylval.str);
                                    }

                                } else if (T_WORD == tok1) {
                                    if (indexType == VARCHAROID) {
                                        appendStringInfo(&sqlBuf, "array_varchar_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", yylval.word.ident);
                                    } else if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", yylval.word.ident);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "%s)", yylval.word.ident);
                                    }

                                } else {
                                    char *datName = NameOfDatum(&yylval.wdatum);
                                    if (indexType == VARCHAROID) {
                                        appendStringInfo(&sqlBuf, "array_varchar_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", datName);
                                    } else if (indexType == INT4OID) {
                                        appendStringInfo(&sqlBuf, "array_integer_delete(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "\'%s\')", datName);
                                    } else {
                                        appendStringInfo(&sqlBuf, "array_deleteidx(");
                                        CastArrayNameToArrayFunc(&sqlBuf, idents);
                                        appendStringInfo(&sqlBuf, "%s)", datName);
                                    }

                                    pfree_ext(datName);
                                }
                            }
                            
                            int tok2 = yylex();
                            if (tok2 != ')') {
                                plpgsql_push_back_token(tok);
                                $$ = NULL;
                                yyerror("syntax error");
                            } else {
                                int tok3 = yylex();
                                if (tok3 != ';') {
                                    plpgsql_push_back_token(tok);
                                    $$ = NULL;
                                    yyerror("syntax error");
                                } else {
                                    $$ = make_callfunc_stmt(sqlBuf.data, @1, false, false, NULL, dno);
                                }
                            }
                        }
                    } else {
                        plpgsql_push_back_token(tok);
                        $$ = NULL;
                        yyerror("syntax error");
                    }
                }
                pfree_ext(sqlBuf.data);
            }
        ;

stmt_dynexecute : K_EXECUTE
                    {
                        PLpgSQL_stmt_dynexecute *newp;
                        PLpgSQL_expr *expr;
                        int endtoken;


                        if((endtoken = yylex()) != K_IMMEDIATE)
                        {
                            plpgsql_push_back_token(endtoken);
                        }

                        expr = read_sql_construct(K_INTO, K_USING, ';',
                                                      "INTO or USING or ;",
                                                      "SELECT ",
                                                      true, true, true,
                                                      NULL, &endtoken);
                        newp = (PLpgSQL_stmt_dynexecute*)palloc0(sizeof(PLpgSQL_stmt_dynexecute));
                        newp->cmd_type = PLPGSQL_STMT_DYNEXECUTE;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->query = expr;
                        newp->into = false;
                        newp->strict = false;
                        newp->rec = NULL;
                        newp->row = NULL;
                        newp->params = NIL;
                        newp->out_row = NULL;
                        newp->isinouttype = false;
                        newp->ppd = NULL;
                        newp->isanonymousblock = true;
                        newp->sqlString = plpgsql_get_curline_query();

                        if (endtoken == K_BULK)
                        {
                            yyerror("syntax error");
                        }

                        /* If we found "INTO", collect the argument */
                        
                        if (endtoken == K_INTO)
                        {
                            if (newp->into)			/* multiple INTO */
                                yyerror("syntax error");
                            newp->into = true;
                            (void)read_into_target(&newp->rec, &newp->row, &newp->strict, K_EXECUTE, false);
                            endtoken = yylex();
                        }
                        /* If we found "USING", collect the argument */
                        if(endtoken == K_USING)
                        {
                            PLpgSQL_row *out_row;
                            if (newp->params)		/* multiple USING */
                                    yyerror("syntax error");

                            read_using_target(&(newp->params), &out_row );
                            if(out_row)
                            {
                                newp->into   = true;
                                newp->strict = true;
                                newp->isinouttype = true;
                            }
                            endtoken = yylex();
                            if( out_row && newp->row )
                            {
                                const char* message = "target into is conflicted with using out (inout)";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                            errmsg("target into is conflicted with using out (inout)"),
                                            errdetail("\"select clause\" can't has out parameters, can only use \"into\"")));
                                u_sess->plsql_cxt.have_error = true;
                            }
                            newp->out_row = newp->row ? newp->row:out_row;

                        }
                        if (endtoken != ';')
                            yyerror("syntax error");

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_open		: K_OPEN cursor_variable
                    {
                        PLpgSQL_stmt_open *newp;
                        int				  tok;
                        int					 endtoken;
                        PLpgSQL_row			*out_row = NULL;

                        newp = (PLpgSQL_stmt_open *)palloc0(sizeof(PLpgSQL_stmt_open));
                        newp->cmd_type = PLPGSQL_STMT_OPEN;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->curvar = $2;
                        newp->cursor_options = CURSOR_OPT_FAST_PLAN;
                        newp->sqlString = plpgsql_get_curline_query();

#ifndef ENABLE_MULTIPLE_NODES
                        if ((newp->curvar < 0 || newp->curvar >= u_sess->plsql_cxt.curr_compile_context->plpgsql_nDatums)
                            && u_sess->attr.attr_common.plsql_show_all_error) 
                        {
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("open cursor error"), parser_errposition(@2)));
                        }
#endif
                        PLpgSQL_var* cur_var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[$2];

                        if (cur_var->cursor_explicit_expr == NULL)
                        {
                            /* be nice if we could use opt_scrollable here */
                            tok = yylex();
                            if (tok_is_keyword(tok, &yylval,
                                               K_NO, "no"))
                            {
                                tok = yylex();
                                if (tok_is_keyword(tok, &yylval,
                                                   K_SCROLL, "scroll"))
                                {
                                    newp->cursor_options |= CURSOR_OPT_NO_SCROLL;
                                    tok = yylex();
                                }
                            }
                            else if (tok_is_keyword(tok, &yylval,
                                                    K_SCROLL, "scroll"))
                            {
                                newp->cursor_options |= CURSOR_OPT_SCROLL;
                                tok = yylex();
                            }

                            if (tok != K_FOR)
                                yyerror("syntax error, expected \"FOR\"");

                            tok = yylex();
                            if (tok == K_EXECUTE)
                            {
                                newp->dynquery = read_sql_stmt("select ");
                            }
                            else
                            {
                                plpgsql_push_back_token(tok);
                                
                                if (tok == K_SELECT || tok == K_WITH)
                                {
                                    newp->query = read_sql_stmt("");
                                }
                                else
                                {
                                    newp->dynquery =
                                    read_sql_expression2(K_USING, ';',
                                                         "USING or ;",
                                                         &endtoken);
                                    /* If we found "USING", collect argument(s) */
                                    if(K_USING == endtoken)
                                    {
                                        read_using_target(&(newp->params), &out_row);
                                        if(NULL != out_row)
                                            yyerror("syntax error");
                                        if(NULL == newp->params)
                                            yyerror("syntax error");					  	

                                        endtoken = plpgsql_yylex();
                                    }

                                    if(';' != endtoken )
                                        yyerror("syntax error");
                                }
                            }
#ifndef ENABLE_MULTIPLE_NODES
			    if (newp->query != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
                                (void)getCursorTupleDesc(newp->query, false, true);
			    }
                            else if (newp->dynquery != NULL && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK)
			    {
                                (void)getCursorTupleDesc(newp->dynquery, false, true);
			    }
#endif
                        }
                        else
                        {
                            /* predefined cursor query, so read args */
                            newp->argquery = read_cursor_args(cur_var, ';', ";");
                        }

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_fetch		: K_FETCH opt_fetch_direction cursor_variable K_INTO
                    {
                        PLpgSQL_stmt_fetch *fetch = $2;
                        PLpgSQL_rec	   *rec;
                        PLpgSQL_row	   *row;

                        /* We have already parsed everything through the INTO keyword */
                        (void)read_into_target(&rec, &row, NULL, -1, false);

                        if (yylex() != ';')
                            yyerror("syntax error");

                        /*
                         * We don't allow multiple rows in PL/pgSQL's FETCH
                         * statement, only in MOVE.
                         */
                        if (fetch->returns_multiple_rows) {
                            const char* message = "FETCH statement cannot return multiple rows";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("FETCH statement cannot return multiple rows"),
                                     parser_errposition(@1)));
                        }

                        fetch->lineno = plpgsql_location_to_lineno(@1);
                        fetch->rec		= rec;
                        fetch->row		= row;
                        fetch->curvar	= $3;
                        fetch->is_move	= false;
                        fetch->bulk_collect = false;
                        fetch->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)fetch;
                    }
                | K_FETCH opt_fetch_direction cursor_variable K_BULK K_COLLECT K_INTO fetch_into_target fetch_limit_expr
                    {
                        PLpgSQL_stmt_fetch *fetch = $2;
                        if (fetch->has_direction) {
                            yyerror("unexpected fetch direction statement");
                        }

                        /* only A_FORMAT can use bulk collect into */
                        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                             yyerror("syntax error");
                        }

                        PLpgSQL_datum *datum = $7;
                        fetch->rec		= NULL;
                        fetch->row		= NULL;

                        if (datum->dtype == PLPGSQL_DTYPE_REC) {
                            fetch->rec = (PLpgSQL_rec *)datum;
                        } else {
                            fetch->row = (PLpgSQL_row *)datum;
                            check_bulk_into_type((PLpgSQL_row*)datum);
                        }

                        fetch->lineno = plpgsql_location_to_lineno(@1);
                        fetch->curvar	= $3;
                        fetch->how_many	= FETCH_ALL;
                        fetch->is_move	= false;
                        fetch->expr		= $8;
                        fetch->returns_multiple_rows = true;
                        fetch->bulk_collect = true;
                        fetch->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)fetch;
                    }
                ;

stmt_move		: K_MOVE opt_fetch_direction cursor_variable ';'
                    {
                        PLpgSQL_stmt_fetch *fetch = $2;

                        fetch->lineno = plpgsql_location_to_lineno(@1);
                        fetch->curvar	= $3;
                        fetch->is_move	= true;
                        fetch->bulk_collect = false;
                        fetch->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)fetch;
                    }
                ;

opt_fetch_direction	:
                    {
                        $$ = read_fetch_direction();
                    }
                ;

fetch_limit_expr    : K_LIMIT
                    {
                        $$ = read_sql_expression(';', ";");
                    }
                    | ';'
                    {
                        $$ = NULL;
                    }
                ;

fetch_into_target :
                    {
                        PLpgSQL_datum *datum = NULL;
                        PLpgSQL_rec *rec;
                        PLpgSQL_row *row;
                        (void)read_into_target(&rec, &row, NULL, -1, true);

                        if (rec != NULL) {
                            datum = (PLpgSQL_datum *)rec;
                        } else {
                            datum = (PLpgSQL_datum *)row;
                        }
                        $$ = datum;
                    }
                ;

stmt_close		: K_CLOSE cursor_variable ';'
                    {
                        PLpgSQL_stmt_close *newp;

                        newp = (PLpgSQL_stmt_close *)palloc(sizeof(PLpgSQL_stmt_close));
                        newp->cmd_type = PLPGSQL_STMT_CLOSE;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->curvar = $2;
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_null		: K_NULL ';'
                    {
                        /* We do building a node for NULL for GOTO */
                        PLpgSQL_stmt_null *newp;

                        newp = (PLpgSQL_stmt_null *)palloc(sizeof(PLpgSQL_stmt_null));
                        newp->cmd_type = PLPGSQL_STMT_NULL;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->sqlString = plpgsql_get_curline_query();

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_commit		: opt_block_label K_COMMIT ';'
                    {
                        PLpgSQL_stmt_commit *newp;

                        newp = (PLpgSQL_stmt_commit *)palloc(sizeof(PLpgSQL_stmt_commit));
                        newp->cmd_type = PLPGSQL_STMT_COMMIT;
                        newp->lineno = plpgsql_location_to_lineno(@2);
                        newp->sqlString = plpgsql_get_curline_query();
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

savepoint_name : any_identifier
                    {
                        $$ = $1;
                    }
                ;

opt_savepoint_name :
                  K_SAVEPOINT savepoint_name
                    {
                        $$ = $2;
                    }
                | savepoint_name
                    {
                        $$ = $1;
                    }
                ;

opt_rollback_to :
                    {
                        $$ = NULL;
                    }
                | K_TO opt_savepoint_name
                    {
                        $$ = $2;
                    }
                ;

stmt_rollback : opt_block_label K_ROLLBACK opt_rollback_to ';'
                    {
                        if ($3 == NULL) {
                            PLpgSQL_stmt_rollback *newp;

                            newp = (PLpgSQL_stmt_rollback *) palloc(sizeof(PLpgSQL_stmt_rollback));
                            newp->cmd_type = PLPGSQL_STMT_ROLLBACK;
                            newp->lineno = plpgsql_location_to_lineno(@2);
                            newp->sqlString = plpgsql_get_curline_query();
                            plpgsql_ns_pop();

                            $$ = (PLpgSQL_stmt *)newp;
                            record_stmt_label($1, (PLpgSQL_stmt *)newp);
                        } else {
                            PLpgSQL_stmt_savepoint *newp;

                            newp = (PLpgSQL_stmt_savepoint *) palloc(sizeof(PLpgSQL_stmt_savepoint));
                            newp->cmd_type = PLPGSQL_STMT_SAVEPOINT;
                            newp->lineno = plpgsql_location_to_lineno(@2);
                            newp->sqlString = plpgsql_get_curline_query();
                            newp->opType = PLPGSQL_SAVEPOINT_ROLLBACKTO;
                            newp->spName = $3;
                            plpgsql_ns_pop();

                            $$ = (PLpgSQL_stmt *)newp;
                            record_stmt_label($1, (PLpgSQL_stmt *)newp);
                        }
                    }
                ;

stmt_savepoint : opt_block_label K_SAVEPOINT savepoint_name ';'
                    {
                        PLpgSQL_stmt_savepoint *newp;

                        newp = (PLpgSQL_stmt_savepoint *) palloc(sizeof(PLpgSQL_stmt_savepoint));
                        newp->cmd_type = PLPGSQL_STMT_SAVEPOINT;
                        newp->lineno = plpgsql_location_to_lineno(@2);
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->opType = PLPGSQL_SAVEPOINT_CREATE;
                        newp->spName = $3;
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
               | opt_block_label K_RELEASE opt_savepoint_name ';'
                    {
                        PLpgSQL_stmt_savepoint *newp;

                        newp = (PLpgSQL_stmt_savepoint *) palloc(sizeof(PLpgSQL_stmt_savepoint));
                        newp->cmd_type = PLPGSQL_STMT_SAVEPOINT;
                        newp->lineno = plpgsql_location_to_lineno(@2);
                        newp->sqlString = plpgsql_get_curline_query();
                        newp->opType = PLPGSQL_SAVEPOINT_RELEASE;
                        newp->spName = $3;
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

cursor_variable	: T_DATUM
                    {
                        if ($1.datum->dtype != PLPGSQL_DTYPE_VAR) {
                            const char* message = "cursor variable must be a simple variable";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("cursor variable must be a simple variable"),
                                     parser_errposition(@1)));
                        }

                        if (((PLpgSQL_var *) $1.datum)->datatype->typoid != REFCURSOROID) {
                            const char* message = "variable must be of type cursor or refcursor";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("variable \"%s\" must be of type cursor or refcursor",
                                            ((PLpgSQL_var *) $1.datum)->refname),
                                     parser_errposition(@1)));
                        }
                        if (((PLpgSQL_var *) $1.datum)->ispkg) {
                            if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL &&
                                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->is_autonomous) {
                                const char* message =
                                    "package cursor referenced in autonomous procedure is not supported yet";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cursor referenced by \"%s\" in autonomous procedure is not supported yet",
                                         $1.ident),
                                     parser_errposition(@1)));
                            }
                        }
                        $$ = $1.dno;
                    }
                | T_PACKAGE_VARIABLE
                    {
                        if ($1.datum->dtype != PLPGSQL_DTYPE_VAR) {
                            const char* message = "cursor variable must be a simple variable";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("cursor variable must be a simple variable"),
                                     parser_errposition(@1)));
                        }

                        if (((PLpgSQL_var *) $1.datum)->datatype->typoid != REFCURSOROID) {
                            const char* message = "variable must be of type cursor or refcursor";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("variable \"%s\" must be of type cursor or refcursor",
                                            ((PLpgSQL_var *) $1.datum)->refname),
                                     parser_errposition(@1)));
                        }
                        if (list_length($1.idents) == 3) {
                            const char* message =
                                "cursor referenced in schema.package.cursor format is not supported yet";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cursor referenced by \"%s\" is not supported yet",
                                            $1.ident),
                                     parser_errposition(@1)));
                        }
                        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL &&
                            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->is_autonomous) {
                            const char* message =
                                "package cursor referenced in autonomous procedure is not supported yet";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("cursor referenced by \"%s\" in autonomous procedure is not supported yet",
                                            $1.ident),
                                     parser_errposition(@1)));
                        }
                        if (enable_plpgsql_gsdependency()) {
                            gsplsql_build_gs_variable_dependency($1.idents);
                        }
                        $$ = $1.dno;
                    }
                | T_WORD
                    {
                        /* just to give a better message than "syntax error" */
                        word_is_not_variable(&($1), @1);
                    }
                | T_CWORD
                    {
                        /* just to give a better message than "syntax error" */
                        cword_is_not_variable(&($1), @1);
                    }
                ;

exception_sect	:
                    { $$ = NULL; }
                | K_EXCEPTION
                    {
                        /*
                         * We use a mid-rule action to add these
                         * special variables to the namespace before
                         * parsing the WHEN clauses themselves.  The
                         * scope of the names extends to the end of the
                         * current block.
                         */
                        PLpgSQL_exception_block *newp = (PLpgSQL_exception_block *)palloc(sizeof(PLpgSQL_exception_block));

                        $<exception_block>$ = newp;
                    }
                    proc_exceptions
                    {
                        PLpgSQL_exception_block *newp = $<exception_block>2;
                        newp->exc_list = $3;

                        $$ = newp;
                    }
                ;

proc_exceptions	: proc_exceptions proc_exception
                        {
                            $$ = lappend($1, $2);
                        }
                | proc_exception
                        {
                            $$ = list_make1($1);
                        }
                ;

proc_exception	: K_WHEN proc_conditions K_THEN proc_sect
                    {
                        PLpgSQL_exception *newp;

                        newp = (PLpgSQL_exception *)palloc0(sizeof(PLpgSQL_exception));
                        newp->lineno     = plpgsql_location_to_lineno(@1);
                        newp->conditions = $2;
                        newp->action	    = $4;

                        $$ = newp;
                    }
                ;

proc_conditions	: proc_conditions K_OR proc_condition
                        {
                            PLpgSQL_condition	*old;

                            for (old = $1; old->next != NULL; old = old->next)
                                /* skip */ ;
                            old->next = $3;
                            $$ = $1;
                        }
                | proc_condition
                        {
                            $$ = $1;
                        }
                ;

proc_condition	: any_identifier
                        {
                            if (strcmp($1, "sqlstate") != 0)
                            {
                                if (PLPGSQL_DTYPE_ROW == yylval.wdatum.datum->dtype)
                                {
                                    PLpgSQL_condition *newp = NULL;
                                    PLpgSQL_row* row = (PLpgSQL_row*)yylval.wdatum.datum;
                                    if (!row->ispkg) {
                                        row = ( PLpgSQL_row* ) u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[yylval.wdatum.dno];
                                    } else {
                                        PLpgSQL_package* pkg = row->pkg;
                                        row = ( PLpgSQL_row* )pkg->datums[row->dno];
                                    }
                                    TupleDesc	rowtupdesc = row ? row->rowtupdesc : NULL;

                                    if(rowtupdesc && 
                                        0 == strcmp(format_type_be(rowtupdesc->tdtypeid), "exception"))
                                    {
                                        newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                                        newp->sqlerrstate = row->customErrorCode;
                                        newp->condname = pstrdup(row->refname);
                                        newp->next = NULL;
                                    }

                                    if(NULL == newp) {
                                        const char* message = "unrecognized exception condition ";
                                        InsertErrorMessage(message, plpgsql_yylloc);
                                        ereport(errstate,
                                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                                errmsg("unrecognized exception condition \"%s\"",
                                                        row? row->refname : "??")));
                                    }
                                    $$ = newp;
                                }
                                else {
                                    u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
                                    $$ = plpgsql_parse_err_condition($1);
                                }
                            }
                            else
                            {
                                PLpgSQL_condition *newp;
                                char   *sqlstatestr;

                                /* next token should be a string literal */
                                if (yylex() != SCONST)
                                    yyerror("syntax error");
                                sqlstatestr = yylval.str;

                                if (strlen(sqlstatestr) != 5)
                                    yyerror("invalid SQLSTATE code");
                                if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
                                    yyerror("invalid SQLSTATE code");

                                newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                                newp->sqlerrstate =
                                    MAKE_SQLSTATE(sqlstatestr[0],
                                                  sqlstatestr[1],
                                                  sqlstatestr[2],
                                                  sqlstatestr[3],
                                                  sqlstatestr[4]);
                                newp->condname = sqlstatestr;
                                newp->next = NULL;

                                $$ = newp;
                            }
                        }
                ;
expr_until_semi :
                    {
                        /* 
                         * support invoking function with out
                         * argument in a := expression 
                         */
                        PLpgSQL_stmt *stmt = NULL;
                        int tok = -1;
                        char *name = NULL;
                        bool isCallFunc = false;
                        PLpgSQL_expr* expr = NULL;
                        List* funcNameList = NULL;
                        if (!enable_out_param_override() && u_sess->parser_cxt.isPerform) {
                            u_sess->parser_cxt.isPerform = false;
                        }
                        if (plpgsql_is_token_match2(T_WORD, '(') || 
                            plpgsql_is_token_match2(T_CWORD,'('))
                        {
                            tok = yylex();
                            if (T_WORD == tok) {
                                name = yylval.word.ident;
                                isCallFunc = is_function(name, true, false);
                            } else {
                                checkFuncName(yylval.cword.idents);
                                funcNameList = yylval.cword.idents;
                                name = NameListToString(yylval.cword.idents);
                                isCallFunc = is_function(name, true, false, yylval.cword.idents);
                            }

                        } else if (u_sess->parser_cxt.isPerform && enable_out_param_override()) {
                            u_sess->parser_cxt.isPerform = false;
                            const char* message = "perform not support expression when open guc proc_outparam_override";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmsg("perform not support expression when open guc proc_outparam_override")));
                            u_sess->plsql_cxt.have_error = true;
                        }

                        if (isCallFunc || (enable_out_param_override() && u_sess->parser_cxt.isPerform))
                        {
                            if (u_sess->parser_cxt.isPerform) {
                                stmt = make_callfunc_stmt(name, yylloc, false, false, funcNameList, -1, true);
                            } else {
                                stmt = make_callfunc_stmt(name, yylloc, true, false, funcNameList, -1, isCallFunc);
                            }
                            if (u_sess->parser_cxt.isPerform) {
                                u_sess->parser_cxt.stmt = (void*)stmt;
                            }
                            if (PLPGSQL_STMT_EXECSQL == stmt->cmd_type)
                                expr = ((PLpgSQL_stmt_execsql*)stmt)->sqlstmt;
                            else if (PLPGSQL_STMT_PERFORM == stmt->cmd_type)
                                expr = ((PLpgSQL_stmt_perform*)stmt)->expr;

                            expr->is_funccall = true;
                            $$ = expr;
                        }
                        else
                        {
                            if (name != NULL) {
                                plpgsql_push_back_token(tok);
                                name = NULL;
                            }

                            if (!plpgsql_is_token_match2(T_CWORD,'('))
                            {
                                if (plpgsql_is_token_match(T_CWORD)) {
                                    List* wholeName = yylval.cword.idents;
                                    plpgsql_pkg_add_unknown_var_to_namespace(wholeName);
                                }
                            }
                            expr = read_sql_expression(';', ";");
#ifndef ENABLE_MULTIPLE_NODES
                            if (enable_out_param_override() && PLSQL_COMPILE_OUTPARAM
                                && !IsInitdb && IsNormalProcessingMode()) {
                                CheckOutParamIsConst(expr);
                            }
#endif
                            $$ = expr;
                            
                        }	
                    }
                ;

expr_until_rightbracket :
                    { $$ = read_sql_expression(']', "]"); }
                ;

expr_until_parenthesis :
                    { $$ = read_sql_expression(')', ")"); }
                ;

expr_until_then :
                    { $$ = read_sql_expression(K_THEN, "THEN"); }
                ;

expr_until_loop :
                    { $$ = read_sql_expression(K_LOOP, "LOOP"); }
                ;

expr_until_while_loop:
                    {
                        int tok = -1;

                        $$.expr = read_sql_expression2(K_LOOP, K_DO, "LOOP or DO", &tok);
                        $$.endtoken = tok;

                        if(u_sess->attr.attr_sql.sql_compatibility != B_FORMAT && tok == K_DO)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("'while-do' is only supported in database which dbcompatibility='B'.")));
                    }
                ;

opt_block_label	:
                    {
                        plpgsql_ns_push(NULL);
                        $$ = NULL;
                    }
                | LESS_LESS any_identifier GREATER_GREATER
                    {
                        plpgsql_ns_push($2);
                        $$ = $2;
                    }
                ;

opt_label	:
                    {
                        $$ = NULL;
                    }
                | any_identifier
                    {
                        $$ = $1;
                    }
                ;

opt_exitcond	: ';'
                    { $$ = NULL; }
                | K_WHEN expr_until_semi
                    { $$ = $2; }
                ;

/*
 * need both options because scanner will have tried to resolve as variable
 */
any_identifier	: T_WORD
                    {
                        $$ = $1.ident;
                    }
                | T_DATUM
                    {
                        if ($1.ident == NULL && $1.idents == NULL) {
                            yyerror("syntax error");
                        } else if ($1.ident != NULL) {
                            $$ = $1.ident;
                        } else {
                            char* names = NameListToString($1.idents);
                            $$ = names;
                        }
                    }
                ;

unreserved_keyword_func :
	 K_ABSOLUTE
         | K_ALIAS
         | K_BACKWARD
         | K_CONSTANT
         | K_CURRENT
         | K_DEBUG
         | K_DETAIL
         | K_DUMP
         | K_ERRCODE
         | K_ERROR
         | K_EXCEPTIONS
         | K_FIRST
         | K_FORWARD
         | K_HINT
         | K_INDEX
         | K_INFO
         | K_LAST
         | K_LOG
         | K_MESSAGE
         | K_MESSAGE_TEXT
         | K_MULTISET
         | K_NEXT
         | K_NO
         | K_NOTICE
         | K_OPTION
         | K_PACKAGE
         | K_INSTANTIATION
         | K_PG_EXCEPTION_CONTEXT
         | K_PG_EXCEPTION_DETAIL
         | K_PG_EXCEPTION_HINT 
         | K_QUERY
         | K_RECORD
         | K_RELATIVE
         | K_RESULT_OID
         | K_RETURNED_SQLSTATE
         | K_REVERSE
         | K_ROW_COUNT
         | K_SCROLL
         | K_SLICE
         | K_STACKED
         | K_SYS_REFCURSOR
         | K_USE_COLUMN
         | K_USE_VARIABLE
         | K_VARIABLE_CONFLICT
         | K_VARRAY
         | K_WARNING 
         ;
unreserved_keyword	:
                K_ABSOLUTE
                | K_ALIAS
                | K_ALTER
                | K_ARRAY
                | K_BACKWARD
                | K_CALL
                | K_CATALOG_NAME
                | K_CLASS_ORIGIN
                | K_COLUMN_NAME
                | K_COMMIT
                | K_CONDITION
                | K_CONSTANT
                | K_CONSTRAINT_CATALOG
                | K_CONSTRAINT_NAME
                | K_CONSTRAINT_SCHEMA
                | K_CONTINUE
                | K_CURRENT
                | K_CURSOR_NAME
                | K_DEBUG
                | K_DETAIL
                | K_DISTINCT
                | K_DUMP
                | K_ERRCODE
                | K_ERROR
                | K_EXCEPT
                | K_EXCEPTIONS
                | K_FIRST
                | K_FORWARD
                | K_HINT
                | K_INDEX
                | K_INFO
                | K_INTERSECT
                | K_IS
                | K_LAST
                | K_LOG
                | K_MERGE
                | K_MESSAGE
                | K_MESSAGE_TEXT
                | K_MULTISET
                | K_MYSQL_ERRNO
                | K_NEXT
                | K_NO
                | K_NOTICE
                | K_OPTION
                | K_PACKAGE
                | K_INSTANTIATION
                | K_PG_EXCEPTION_CONTEXT
                | K_PG_EXCEPTION_DETAIL
                | K_PG_EXCEPTION_HINT
                | K_PRIOR
                | K_QUERY
                | K_RECORD
                | K_RELATIVE
                | K_RESIGNAL
                | K_RESULT_OID
                | K_RETURNED_SQLSTATE
                | K_REVERSE
                | K_ROLLBACK
                | K_ROW_COUNT
                | K_ROWTYPE
                | K_SAVE
                | K_SCHEMA_NAME
                | K_SCROLL
                | K_SIGNAL
                | K_SLICE
                | K_SQLSTATE
                | K_STACKED
                | K_SUBCLASS_ORIGIN
                | K_SYS_REFCURSOR
                | K_TABLE
                | K_TABLE_NAME
                | K_UNION
                | K_USE_COLUMN
                | K_USE_VARIABLE
                | K_VARIABLE_CONFLICT
                | K_VARRAY
                | K_WARNING
                | K_WITH
                ;

%%

#define MAX_EXPR_PARAMS  1024

typedef struct {
    YYSTYPE lval; /* semantic information */
    YYLTYPE lloc; /* offset in scanbuf */
    int tok;     /* token */
    int leng;
} TokenData;
static DList* push_token_stack(int tok, DList* tokenstack);
static TokenData* build_token_data(int token);

/*
 * Check whether a token represents an "unreserved keyword".
 * We have various places where we want to recognize a keyword in preference
 * to a variable name, but not reserve that keyword in other contexts.
 * Hence, this kluge.
 */
static bool
tok_is_keyword(int token, union YYSTYPE *lval,
               int kw_token, const char *kw_str)
{
    if (token == kw_token)
    {
        /* Normal case, was recognized by scanner (no conflicting variable) */
        return true;
    }
    else if (token == T_DATUM)
    {
        /*
         * It's a variable, so recheck the string name.  Note we will not
         * match composite names (hence an unreserved word followed by "."
         * will not be recognized).
         */
        if (!lval->wdatum.quoted && lval->wdatum.ident != NULL &&
            strcmp(lval->wdatum.ident, kw_str) == 0)
            return true;
    }
    return false;				/* not the keyword */
}

/*
 * Convenience routine to complain when we expected T_DATUM and got T_WORD,
 * ie, unrecognized variable.
 */
static void
word_is_not_variable(PLword *word, int location)
{
    const char* message = "it is not a known variable ";
    InsertErrorMessage(message, plpgsql_yylloc);
    ereport(errstate,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("\"%s\" is not a known variable",
                    word->ident),
             parser_errposition(location)));
}

/* Same, for a CWORD */
static void
cword_is_not_variable(PLcword *cword, int location)
{
    const char* message = "it is not a known variable ";
    InsertErrorMessage(message, plpgsql_yylloc);
    ereport(errstate,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("\"%s\" is not a known variable",
                    NameListToString(cword->idents)),
             parser_errposition(location)));
}

/*
 * Convenience routine to complain when we expected T_DATUM and got
 * something else.  "tok" must be the current token, since we also
 * look at yylval and yylloc.
 */
static void
current_token_is_not_variable(int tok)
{
    if (tok == T_WORD)
        word_is_not_variable(&(yylval.word), yylloc);
    else if (tok == T_CWORD)
        cword_is_not_variable(&(yylval.cword), yylloc);
    else
        yyerror("syntax error");
}

/*
 * Brief		: handle the C-style function IN arguments.
 * Description	: 
 * Notes		:
 */ 
static void 
yylex_inparam(StringInfoData* func_inparam, 
              int *nparams,
              int * tok,
              int *tableof_func_dno,
              int *tableof_var_dno)
{
    PLpgSQL_expr * expr =   NULL;

    if (nparams != NULL) {
        if (*nparams) {
            appendStringInfoString(func_inparam, ",");
        } 
        /* 
        * handle the problem that the function 
        * arguments can only be variable. the argment validsql is set FALSE to
        * ignore sql expression check to the "$n" type of arguments. 
        */
        expr = read_sql_construct(',', ')', 0, ",|)", "", true, false, false, NULL, tok);

        if (*nparams >= MAX_EXPR_PARAMS) {
            const char* message = "too many variables specified in SQL statement ";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("too many variables specified in SQL statement ,more than %d", MAX_EXPR_PARAMS)));
        }

        (*nparams)++;
    } else {
        expr = read_sql_construct(',', ')', 0, ",|)", "", true, false, false, NULL, tok);
    }

    if (*tableof_func_dno != -1 && *tableof_func_dno != expr->tableof_func_dno) {
        yyerror("do not support more than 2 table of index by variables call functions in function");
    } else {
        *tableof_func_dno = expr->tableof_func_dno;
    }

    *tableof_var_dno = expr->tableof_var_dno;
    /* 
     * handle the problem that the function 
     * arguments can only be variable. After revising, the arguments can be any
     * expression. 
     */
    appendStringInfoString(func_inparam, expr->query);
}

/*
 * Brief		: handle the C-style function OUT arguments.
 * Description	: 
 * Notes		:
 */
static int 
yylex_outparam(char** fieldnames,
               int *varnos,
               int nfields,
               PLpgSQL_row **row,
               PLpgSQL_rec **rec,
               int *token,
               int *retvarno,
               bool is_push_back,
               bool overload)
{
    *token = yylex();
    int loc = yylloc;
    if (T_DATUM == *token)
    {
        if (PLPGSQL_DTYPE_ROW == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            int varno = yylval.wdatum.dno;
            TokenData* temptokendata = build_token_data(*token);
            int temtoken = yylex();
            plpgsql_push_back_token(temtoken);
            yylloc = temptokendata->lloc;
            yylval = temptokendata->lval;
            u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
            /* if is a.b.c.d? */
            if ('.' == temtoken) {
                varnos[nfields] = read_assignlist(is_push_back, token);
            } else {
                varnos[nfields] = varno;
                *row = (PLpgSQL_row *)yylval.wdatum.datum;
                *retvarno = varno;
            }
            if (is_push_back) {
                yylloc = temptokendata->lloc;
                yylval = temptokendata->lval;
                u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
            }
            pfree_ext(temptokendata);
        }
        else if (PLPGSQL_DTYPE_RECORD == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.dno;
            *row = (PLpgSQL_row *)yylval.wdatum.datum;
        }
        else if (PLPGSQL_DTYPE_REC == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.dno;
            *rec = (PLpgSQL_rec *)yylval.wdatum.datum;
        }
        else if (PLPGSQL_DTYPE_VAR == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.dno;
        }
        /* when out param is rec.a which res is not define, we pass the check */
        else if (PLPGSQL_DTYPE_RECFIELD == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.datum->dno;
        }
        else {
            const char* message = "it is not a scalar variable ";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("\"%s\" is not a scalar variable",
                NameOfDatum(&yylval.wdatum))));
        }
    }
    else if (T_VARRAY_VAR == *token || T_TABLE_VAR == *token)
    {
        /* Temporary processing array value for out param */
        check_assignable(yylval.wdatum.datum, yylloc);
        fieldnames[nfields] = CopyNameOfDatum(&yylval.wdatum);
        varnos[nfields] = read_assignlist(is_push_back, token);
    }
    else if (T_PACKAGE_VARIABLE == *token)
    {
        if (PLPGSQL_DTYPE_ROW == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            int varno = yylval.wdatum.dno;
            TokenData* temptokendata = build_token_data(*token);
            int temtoken = yylex();
            plpgsql_push_back_token(temtoken);
            yylloc = temptokendata->lloc;
            yylval = temptokendata->lval;
            u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
            /* if is a.b.c.d? */
            if ('.' == temtoken) {
                varnos[nfields] = read_assignlist(is_push_back, token);
            } else {
                varnos[nfields] = varno;
                *retvarno = varno;
                *row = (PLpgSQL_row *)yylval.wdatum.datum;
            }
            if (is_push_back) {
                yylloc = temptokendata->lloc;
                yylval = temptokendata->lval;
                u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
            }
            pfree_ext(temptokendata);
        }
        else if (PLPGSQL_DTYPE_RECORD == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.dno;
            *row = (PLpgSQL_row *)yylval.wdatum.datum;
        }
        else if (PLPGSQL_DTYPE_VAR == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            int type_flag = -1;
            get_datum_tok_type(yylval.wdatum.datum, &type_flag);
            if (type_flag == -1) {
                fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
                varnos[nfields] = yylval.wdatum.dno;
            } else {
                /* Temporary processing array value for out param */
                fieldnames[nfields] = CopyNameOfDatum(&yylval.wdatum);
                varnos[nfields] = read_assignlist(is_push_back, token);
            }
        }
        else {
            const char* message = "it is not a scalar variable ";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("\"%s\" is not a scalar variable",
                NameOfDatum(&yylval.wdatum))));
        }
    }
    else if (overload)
    {
        fieldnames[nfields] = NULL;
        varnos[nfields] = -1;
    }
    return loc;
}

static int read_assignlist(bool is_push_back, int* token)
{    
    int varno = yylval.wdatum.dno;
    TokenData* temptokendata = build_token_data(*token);
    DList* tokenstack = NULL;
    List* assignlist = NULL;

    for (;;) {
        int temtoken = yylex();
        if ('(' != temtoken && '[' != temtoken && '.' != temtoken) {
            plpgsql_push_back_token(temtoken);
            break;
        }
        
        if (is_push_back) {
            TokenData* temptokendata1 = build_token_data(temtoken);
            tokenstack = dlappend(tokenstack, temptokendata1);
        }
        
        PLpgSQL_expr* expr = NULL;
        char* attrname = NULL;
        if ('(' == temtoken) {
            expr = read_sql_construct6(')', 0, 0, 0, 0, 0, ")", "SELECT ",
                true, true, true, NULL, NULL, tokenstack, true);
        } else if ('[' == temtoken) {
            expr = read_sql_construct6(']', 0, 0, 0, 0, 0, "]", "SELECT ",
                true, true, true, NULL, NULL, tokenstack, true);
        } else {
            int attrtoken = plpgsql_yylex_single();
            attrname = get_attrname(attrtoken);
            if (tokenstack != NULL) {
                TokenData* temptokendata1 = build_token_data(attrtoken);
                tokenstack = dlappend(tokenstack, temptokendata1);
            }
        }

        MemoryContext oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_cxt);
        if (expr != NULL) {
            A_Indices *ai = makeNode(A_Indices);
            ai->lidx = NULL;
            ai->uidx = (Node*)expr;
            assignlist = lappend(assignlist, (Node*)ai);
        } else {
            Value* v = makeString(pstrdup(attrname));
            assignlist = lappend(assignlist, (Node*)v);
        }
        MemoryContextSwitchTo(oldCxt);
    }
    if (assignlist != NULL) {
        MemoryContext oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_cxt);
        PLpgSQL_assignlist *newp = NULL;
        newp = (PLpgSQL_assignlist*)palloc0(sizeof(PLpgSQL_assignlist));
        newp->dtype = PLPGSQL_DTYPE_ASSIGNLIST;
        newp->assignlist = assignlist;
        newp->targetno = varno;
        varno = plpgsql_adddatum((PLpgSQL_datum*)newp);
        MemoryContextSwitchTo(oldCxt);
    }

    if (is_push_back) {
        if (tokenstack != NULL) {
            push_back_token_stack(tokenstack);
        }

        /* need restore yylloc or yylval if push back twice */
        yylloc = temptokendata->lloc;
        yylval = temptokendata->lval;
        u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
    }
    
    pfree_ext(temptokendata);
    return varno;
}

/* push token in tokenstack */
static DList* push_token_stack(int tok, DList* tokenstack)
{
    TokenData* temptokendata = NULL;
    if (tokenstack != NULL) {
        temptokendata = build_token_data(tok);
        tokenstack = dlappend(tokenstack, temptokendata);
    }
    return tokenstack;
}

/* build a TokenData for input token  */
static TokenData* build_token_data(int token)
{
    TokenData* temptokendata = (TokenData*)palloc0(sizeof(TokenData));
    temptokendata->lloc = yylloc;
    temptokendata->lval = yylval;
    temptokendata->tok = token;
    temptokendata->leng = u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng;
    return temptokendata;
}

/* push back token stored in tokenstack */
static int push_back_token_stack(DList* tokenstack)
{
    int tok = 0;
    if(tokenstack->length > MAX_PUSHBACKS) {
        dlist_free(tokenstack, true);
        yyerror("array with subscript words too much for overloaded function.");
    } else {
        DListCell* dlc = NULL;
        for (dlc = dlist_tail_cell(tokenstack); dlc != NULL; dlc = lprev(dlc)) {
            /* plpgsql_push_back_token function only push yylloc and yylval,
             * so assign the token value to them.
             */
            TokenData* n = (TokenData*)lfirst(dlc);
            tok = n->tok;
            yylloc = n->lloc;
            yylval = n->lval;
            u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = n->leng;
            plpgsql_push_back_token(tok);
        }
        dlist_free(tokenstack, true);
    }
    return tok;
}

/*
 * Brief: passe the called function name and return it into output.
 * Description: 
 * in s: original idents
 * inout output:  splited idents.
 * in numidnets: max ident number.
 * returns: void
 * Notes	: No need to consider double quoted ident because it has been handled in lex.
 * Ident here has been downcase if no double quoted, and been without quote if it had double quote before.
 */ 
void 
plpgsql_parser_funcname(const char *s, char **output, int numidents)
{
    int			ident_num = 0;
    char *outer_ptr = NULL;
    char *p = NULL;
    char delimiter[] = ".";
    errno_t ret;
    char *str = (char *)palloc0(sizeof(char) * (strlen(s) + 1));

    for (int i = 0; i < numidents; i++)
        output[i] = (char *)palloc0(sizeof(char) * NAMEDATALEN * 2);

    ret = strcpy_s(str, sizeof(char) * (strlen(s) + 1), s);
    securec_check(ret, "\0", "\0");

    p = strtok_s(str, delimiter, &outer_ptr);

    while (p != NULL)
    {
        ret = strcpy_s(output[ident_num++], sizeof(char) * NAMEDATALEN * 2, p);
        securec_check(ret, "\0", "\0");
        p = strtok_s(NULL, delimiter, &outer_ptr);
    }

    pfree_ext(str);
}

static int get_func_out_arg_num(char* p_argmodes, int all_arg)
{
    if (all_arg == 0 || p_argmodes == NULL) {
        return 0;
    }
    int out_arg_num = 0;
    for (int i = 0; i < all_arg; i++) {
        if (p_argmodes[i] == 'o' || p_argmodes[i] == 'b') {
            out_arg_num++;
        }
    }
    return out_arg_num;
}

/*
 * Brief		: handle A-style function call.
 * Description	: handle A-style function call. First read the function 
 *  			  name, then read the arguments. At last assembing the function
 *				  name and arguments into the postgresql-style function call.
 * in sqlstart: the sql stmt string to be handle.
 * in location: the location number to erreport.
 * in is_assign: is an assign stmt or not.
 * returns: the parsed stmt.
 * Notes		:
 */ 
static PLpgSQL_stmt *
make_callfunc_stmt(const char *sqlstart, int location, bool is_assign, bool eaten_first_token, List* funcNameList, int arrayFuncDno, bool isCallFunc)
{
    int nparams = 0;
    int nfields = 0;
    int narg = 0;
    int inArgNum = 0;
    int i= 0;
    int tok = 0;
    Oid *p_argtypes = NULL;
    char *cp[3];
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    /* pos_outer is the postion got by matching the name. */
    int pos_outer = 0;
    /* pos_inner is the position got by its real postion in function invoke */
    int pos_inner = -1;
    int	varnos[FUNC_MAX_ARGS] = {-1};
    bool	namedarg[FUNC_MAX_ARGS];
    char*	namedargnamses[FUNC_MAX_ARGS];
    char *fieldnames[FUNC_MAX_ARGS];
    bool outParamInvalid = false;
    bool is_plpgsql_func_with_outparam = false;
    int out_param_dno = -1;
    List *funcname = NIL;
    PLpgSQL_row *row = NULL;
    PLpgSQL_rec *rec = NULL;
    PLpgSQL_expr* expr = NULL;
    HeapTuple proctup = NULL;
    Form_pg_proc procStruct;
    FuncCandidateList clist = NULL;
    StringInfoData func_inparas;
    bool noargs = FALSE;
    int ndefaultargs = 0;
    StringInfoData argname;
    int j = 0;
    int placeholders = 0;
    char *quoted_sqlstart = NULL;
    bool is_have_tableof_index_func = false;

    MemoryContext oldCxt = NULL;
    bool	multi_func = false;
    const char *varray_delete = "array_delete(\"";
    const char *varray_indexby_delete = "array_indexby_delete(\"";
    const char *varray_deleteidx = "array_deleteidx(\"";
    const char *varray_deletevarchar = "array_varchar_delete(\"";
    const char *varray_deleteinteger = "array_integer_delete(\"";
    const char *varray_extend= "array_extendnull(\"";
    const char *varray_trim = "array_trim(\"";

    /*get the function's name*/
    cp[0] = NULL;
    cp[1] = NULL;
    cp[2] = NULL;

    if (sqlstart != NULL && (strncmp(sqlstart, varray_delete, strlen(varray_delete)) == 0
        || strncmp(sqlstart, varray_indexby_delete, strlen(varray_indexby_delete)) == 0
        || strncmp(sqlstart, varray_deleteidx, strlen(varray_deleteidx)) == 0
        || strncmp(sqlstart, varray_extend, strlen(varray_extend)) == 0
        || strncmp(sqlstart, varray_trim, strlen(varray_trim)) == 0
        || strncmp(sqlstart, varray_deletevarchar, strlen(varray_deletevarchar)) == 0
        || strncmp(sqlstart, varray_deleteinteger, strlen(varray_deleteinteger)) == 0)) {
        unsigned int chIdx = 0;

        if (strncmp(sqlstart, varray_delete, strlen(varray_delete)) == 0) {
            chIdx = strlen(varray_delete);
        } else if (strncmp(sqlstart, varray_indexby_delete, strlen(varray_indexby_delete)) == 0) {
            chIdx = strlen(varray_indexby_delete);
            is_have_tableof_index_func = true;
        } else if (strncmp(sqlstart, varray_deleteidx, strlen(varray_deleteidx)) == 0) {
            chIdx = strlen(varray_deleteidx);
        } else if (strncmp(sqlstart, varray_deletevarchar, strlen(varray_deletevarchar)) == 0) {
            chIdx = strlen(varray_deletevarchar);
            is_have_tableof_index_func = true;
        } else if (strncmp(sqlstart, varray_deleteinteger, strlen(varray_deleteinteger)) == 0) {
            chIdx = strlen(varray_deleteinteger);
            is_have_tableof_index_func = true;
        } else if (strncmp(sqlstart, varray_trim, strlen(varray_trim)) == 0) {
            chIdx = strlen(varray_trim);
        } else {
            chIdx = strlen(varray_extend);
        }

        initStringInfo(&func_inparas);
        appendStringInfoString(&func_inparas, "SELECT ");
        appendStringInfoString(&func_inparas, sqlstart);

        if (arrayFuncDno == -1) {
            return NULL;
        }

        expr = (PLpgSQL_expr *)palloc0(sizeof(PLpgSQL_expr));
        expr->dtype = PLPGSQL_DTYPE_EXPR;
        expr->query = pstrdup(func_inparas.data);
        expr->plan = NULL;
        expr->paramnos = NULL;
        expr->ns = plpgsql_ns_top();
        expr->idx = (uint32)-1;
        expr->out_param_dno = -1;
        expr->is_have_tableof_index_func = is_have_tableof_index_func;

        PLpgSQL_stmt_assign *perform = (PLpgSQL_stmt_assign*)palloc0(sizeof(PLpgSQL_stmt_assign));
        perform->cmd_type = PLPGSQL_STMT_ASSIGN;;
        perform->lineno   = plpgsql_location_to_lineno(location);
        perform->varno = arrayFuncDno;
        perform->expr  = expr;
        perform->sqlString = pstrdup(func_inparas.data);
        pfree_ext(func_inparas.data);
        return (PLpgSQL_stmt *)perform;
    }

    /* the function make_callfunc_stmt is only to assemble a sql statement, so the context is set to tmp context */
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    if (funcNameList == NULL) {
        plpgsql_parser_funcname(sqlstart, cp, 3);
    } else {
        funcname= funcNameList;
    }
    if (funcNameList == NULL) {
        if (cp[2] && cp[2][0] != '\0')
            funcname = list_make3(makeString(cp[0]), makeString(cp[1]), makeString(cp[2]));
        else if (cp[1] && cp[1][0] != '\0')
            funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
        else
            funcname = list_make1(makeString(cp[0]));
    }


    /* search the function */
    clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
    if (!clist)
    {
        const char* message = "function not exist";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function \"%s\" doesn't exist ", sqlstart)));
        return NULL;
    }

    if (clist->next)
    {
        multi_func = true;
        char* schemaname = NULL;
        char* pkgname = NULL;
        char* funcStrName = NULL;
        if (funcNameList != NULL) {
            DeconstructQualifiedName(funcNameList, &schemaname, &funcStrName, &pkgname);
        } else {
            DeconstructQualifiedName(funcname, &schemaname, &funcStrName, &pkgname);
        }
        if (IsPackageFunction(funcname) == false && IsPackageSchemaOid(SchemaNameGetSchemaOid(schemaname, true)) == false)
        {
            const char* message = "function is not exclusive";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
             errmsg("function \"%s\" isn't exclusive ", sqlstart)));
        }
    }

    if (multi_func == false)
    {
        proctup = SearchSysCache(PROCOID,
                                 ObjectIdGetDatum(clist->oid),
                                 0, 0, 0);

        /*
         * function may be deleted after clist be searched.
         */
        if (!HeapTupleIsValid(proctup))
        {
            const char* message = "function does not exist";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function \"%s\" doesn't exist ", sqlstart)));
            return NULL;
        }
        /* get the all args informations, only "in" parameters if p_argmodes is null */
        narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
        if (p_argmodes) {
            for (i = 0; i < narg; i++) {
                if (p_argmodes[i] == 'i' || p_argmodes[i] == 'b') {
                    inArgNum++;
                }
            }
        } else {
            inArgNum = narg;
        }
        procStruct = (Form_pg_proc) GETSTRUCT(proctup);
        ndefaultargs = procStruct->pronargdefaults;
        ReleaseSysCache(proctup);
    }

    initStringInfo(&func_inparas);

    tok = yylex();

    /* 
     *  check the format for the function without parameters
     *  if eaten_first_token is true,first token is '(' ,so if next token is ')',it means
     *  no args
     */
    if (eaten_first_token==true) {
        if (tok == ')')
            noargs = TRUE;
    } else {
        if ((tok = yylex()) == ')')
            noargs = TRUE;
    }
    plpgsql_push_back_token(tok);

    if (isCallFunc && is_function_with_plpgsql_language_and_outparam(clist->oid)) {
        is_assign = false;
        is_plpgsql_func_with_outparam = true;
    }
    if (u_sess->parser_cxt.isPerform) {
        is_assign = false;
    }
    /* has any "out" parameters, user execsql stmt */
    if (is_assign)
    {
        appendStringInfoString(&func_inparas, "SELECT ");
    }
    else
    {
        appendStringInfoString(&func_inparas, "CALL ");
    }

    int tableof_func_dno = -1;
    int tableof_var_dno = -1;
    /*
     * Properly double-quote schema name and function name to handle uppercase
     * and special characters when making 'CALL func_name;' statement.
     */
    quoted_sqlstart = NameListToQuotedString(funcname);
    appendStringInfoString(&func_inparas, quoted_sqlstart);
    pfree_ext(quoted_sqlstart);

    appendStringInfoString(&func_inparas, "(");

    /* analyse all parameters */
    if (noargs)
    {
        i = 0;
        tok = yylex();
    }
    else if (!multi_func)
    {
        if (p_argmodes)
        {
            for ( i = 0; i < narg ; i++)
            {
                initStringInfo(&argname);
                pos_outer = -1;
                if ('o' == p_argmodes[i] || 'b' == p_argmodes[i])
                    pos_inner++;

                /*
                 * see if an arg is 'defargname => realargname' type.
                 * if so, p_argnames must be searched to match the defined
                 * argument name.
                 */
                if (plpgsql_is_token_match2(T_DATUM, PARA_EQUALS)
                    || plpgsql_is_token_match2(T_WORD, PARA_EQUALS))
                {

                    tok = yylex();
                    if (T_DATUM == tok)
                        appendStringInfoString(&argname, NameOfDatum(&yylval.wdatum));
                    else
                        appendStringInfoString(&argname, yylval.word.ident);
                    plpgsql_push_back_token(tok);
                    /*
                     * search the p_argnames to match the right argname with current arg,
                     * so the real postion of current arg can be determined 
                     */
                    bool argMatch = false;
                    for (j = 0; j < narg; j++)
                    {
                        if ('o' == p_argmodes[j] || 'b' == p_argmodes[j])
                            pos_outer++;
                        if (0 == pg_strcasecmp(argname.data, p_argnames[j]))
                        {
                            /*
                             * if argmodes is 'i', just append the text, else, 
                             * row or rec should be assigned to store the out arg values
                             */
                            int varno = -1;
                            switch (p_argmodes[j])
                            {
                                case 'i':
                                    yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                                    check_tableofindex_args(tableof_var_dno, p_argtypes[j]);
                                    break;
                                case 'o':
                                case 'b':
                                    if (is_assign && 'o' == p_argmodes[j])
                                    {
                                        (void)yylex();
                                        (void)yylex();
                                        tok = yylex();
                                        if (T_DATUM == tok || T_VARRAY_VAR == tok 
                                            || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok)
                                        {
                                            plpgsql_push_back_token(tok);
                                            (void)read_sql_expression2(',', ')', ",|)", &tok);
                                        }
                                        else
                                            yyerror("syntax error");
                                        break;
                                    }

                                    if (nparams)
                                        appendStringInfoChar(&func_inparas, ',');

                                    tok = yylex();
                                    if (T_DATUM == tok)
                                        appendStringInfoString(&func_inparas, NameOfDatum(&yylval.wdatum));
                                    else
                                        appendStringInfoString(&func_inparas, yylval.word.ident);

                                    appendStringInfoString(&func_inparas, "=>");
                                    /* pass => */
                                    (void)yylex();
                                    yylex_outparam(fieldnames, varnos, pos_outer, &row, &rec, &tok, &varno, true);
                                    processFunctionRecordOutParam(varno, clist->oid, &out_param_dno);
                                    if ((!enable_out_param_override() && p_argmodes[j] == 'b' && is_assign)
                                            || T_DATUM == tok || T_VARRAY_VAR == tok 
                                            || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok)
                                    {
                                        nfields++;
                                        plpgsql_push_back_token(tok);
                                        /* don't need inparam add ',' */
                                        yylex_inparam(&func_inparas, NULL, &tok, &tableof_func_dno, &tableof_var_dno);
                                        check_tableofindex_args(tableof_var_dno, p_argtypes[j]);
                                    } else {
                                        outParamInvalid = true;
                                    }
                                    nparams++;
                                    break;
                                default:
                                    const char* message = "parameter mode  not exist";
                                    InsertErrorMessage(message, plpgsql_yylloc);
                                    ereport(errstate,
                                            (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                             errmsg("parameter mode %c doesn't exist", p_argmodes[j])));
                            }
                            argMatch = true;
                            break;
                        }
                    }
                    if (!argMatch) {
                        const char* message = "invoking function error,check function";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, no argments match \"%s\"", sqlstart, argname.data)));
                    }
                    if (outParamInvalid) {
                        const char* message = "invoking function error,check function";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, no destination for argments \"%s\"", sqlstart, argname.data)));
                    }
                }
                else
                {
                    tok = yylex();
                    int varno = -1;
                    /* p_argmodes may be null, 'i'->in , 'o'-> out ,'b' inout,others error */
                    switch (p_argmodes[i])
                    {
                        case 'i':
                            if (T_PLACEHOLDER == tok)
                                placeholders++;
                            plpgsql_push_back_token(tok);
                            yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                            check_tableofindex_args(tableof_var_dno, p_argtypes[i]);
                            break;
                        case 'o':
                            /*
                             * if the function is in an assign expr, just ignore the 
                             * out parameters.
                             */
                            if (is_assign)
                            {
                                if (T_DATUM == tok || T_VARRAY_VAR == tok || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok || T_PLACEHOLDER == tok)
                                {
                                    plpgsql_push_back_token(tok);
                                    (void)read_sql_expression2(',', ')', ",|)", &tok);
                                }
                                else
                                    yyerror("syntax error");
                                break;
                            }

                            if (T_PLACEHOLDER == tok)
                            {
                                placeholders++;
                            }
                            else if (T_DATUM == tok || T_VARRAY_VAR == tok || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok || T_PLACEHOLDER == tok)
                            {
                                nfields++;
                            } else {
                                outParamInvalid = true;
                            }

                            plpgsql_push_back_token(tok);
                            yylex_outparam(fieldnames, varnos, pos_inner, &row, &rec, &tok, &varno, true);
                            processFunctionRecordOutParam(varno, clist->oid, &out_param_dno);
                            plpgsql_push_back_token(tok);
                            yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                            check_tableofindex_args(tableof_var_dno, p_argtypes[i]);
                            break;
                        case 'b':
                            if (is_assign)
                            {
                                if (!enable_out_param_override() 
                                        || (enable_out_param_override() 
                                               && (T_DATUM == tok || T_VARRAY_VAR == tok 
                                                      || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok)))
                                {
                                    plpgsql_push_back_token(tok);
                                    yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                                }
                                else
                                    yyerror("syntax error");
                                break;
                            }

                            if (T_PLACEHOLDER == tok)
                                placeholders++;
                            plpgsql_push_back_token(tok);
                            yylex_outparam(fieldnames, varnos, pos_inner, &row, &rec, &tok, &varno, true);
                            processFunctionRecordOutParam(varno, clist->oid, &out_param_dno);
                            if (T_DATUM == tok || T_VARRAY_VAR == tok || T_TABLE_VAR == tok || T_PACKAGE_VARIABLE == tok) {
                                nfields++;
                            } else {
                                outParamInvalid = true;
                            }
                            
                            plpgsql_push_back_token(tok);
                            
                            yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                            check_tableofindex_args(tableof_var_dno, p_argtypes[i]);
                            break;
                        default:
                            const char* message = "parameter mode  not exist";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                     errmsg("parameter mode %c doesn't exist", p_argmodes[i])));
                    }
                    if (is_plpgsql_func_with_outparam && outParamInvalid) {
                        const char* message = "invoking function error,check function";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, no destination for argments \"%s\"", sqlstart, argname.data)));
                    }

                }

                if (')' == tok)
                {
                    i++;
                    break;
                }

                if (narg - 1 == i) {
                    const char* message = "maybe input something superfluous";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \")\", maybe input something superfluous.", sqlstart)));
                }

                if (',' != tok) {
                    const char* message = "invoking function error,check function";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \",\"", sqlstart)));
                }
                pfree_ext(argname.data);
            }
        }
        else
        {	
            for ( i = 0; i < narg; i++)
            {
                tok = yylex();
                if (T_PLACEHOLDER == tok)
                    placeholders++;
                plpgsql_push_back_token(tok);
        
                yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
        
                if (')' == tok)
                {
                    i++;
                    break;
                }
        
                if (narg - 1 == i) {
                    const char* message = "maybe input something superfluous";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \")\", maybe input something superfluous.", sqlstart)));
                }
            }
        }
    }
    else
    {
        while (true)
        {
            int varno = -1;
            /* for named arguemnt */
            if (plpgsql_is_token_match2(T_DATUM, PARA_EQUALS)
                || plpgsql_is_token_match2(T_WORD, PARA_EQUALS))
            {
                tok = yylex();
                if (nparams)
                    appendStringInfoString(&func_inparas, ",");
                if (T_DATUM == tok)
                {
                    appendStringInfoString(&func_inparas, NameOfDatum(&yylval.wdatum));
                    namedargnamses[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
                }
                else
                {
                    appendStringInfoString(&func_inparas, yylval.word.ident);
                    namedargnamses[nfields] = yylval.word.ident;
                }

                appendStringInfoString(&func_inparas, "=>");
                (void)yylex();
                yylex_outparam(fieldnames, varnos, nfields, &row, &rec, &tok, &varno, true, true);
                plpgsql_push_back_token(tok);
                expr = read_sql_construct(',', ')', 0, ",|)", "", true, false, false, NULL, &tok);
                appendStringInfoString(&func_inparas, expr->query);
                pfree_ext(expr->query);
                pfree_ext(expr);

                nparams++;
                namedarg[nfields] = true;
            }
            else
            {
                yylex_outparam(fieldnames, varnos, nfields, &row, &rec, &tok, &varno, true, true);
                plpgsql_push_back_token(tok);
                yylex_inparam(&func_inparas, &nparams, &tok, &tableof_func_dno, &tableof_var_dno);
                namedarg[nfields] = false;
                namedargnamses[nfields] = NULL;
            }
            nfields++;
            if (')' == tok)
            {
                break;
            }
        }
    }
    appendStringInfoString(&func_inparas, ")");
    /* read the end token */
    if ((tok = yylex()) != ';')
    {
        if (!is_assign)
        {
            if (u_sess->parser_cxt.isPerform) {
                const char* message = "perform not support expression when open guc proc_outparam_override";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("perform not support expression when open guc proc_outparam_override")));
                u_sess->plsql_cxt.have_error = true;
            } else {
                const char* message = "maybe input something superfluous";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("when invoking function %s, maybe input something superfluous.", sqlstart)));
                u_sess->plsql_cxt.have_error = true;
            }
        }
        else
        {
            /* there may be other thing after the function invoke, just append it */
            plpgsql_push_back_token(tok);
            expr = read_sql_construct(';', 0, 0, ";", "", true, false, true, NULL, &tok);
            appendStringInfoString(&func_inparas, expr->query);
            pfree_ext(expr->query);
            pfree_ext(expr);
        }
    }
    
    if (!multi_func && inArgNum - i >  ndefaultargs) {
        const char* message = "function has no enough parameters";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s has no enough parameters", sqlstart)));
    }

    (void)MemoryContextSwitchTo(oldCxt);

    /* generate the expression */
    expr 				= (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(func_inparas.data);
    expr->plan			= NULL;
    expr->paramnos 		= NULL;
    expr->ns			= plpgsql_ns_top();
    expr->idx			= (uint32)-1;
    expr->out_param_dno	= out_param_dno;
    expr->is_have_tableof_index_func = tableof_func_dno != -1 ? true : false;

    if (multi_func)
    {
        PLpgSQL_function *function = NULL;
        PLpgSQL_execstate *estate = (PLpgSQL_execstate*)palloc(sizeof(PLpgSQL_execstate));
        expr->func = (PLpgSQL_function *) palloc0(sizeof(PLpgSQL_function));
        function = expr->func;
        function->fn_is_trigger = PLPGSQL_NOT_TRIGGER;
        function->fn_input_collation = InvalidOid;
        function->out_param_varno = -1;		/* set up for no OUT param */
        function->resolve_option = GetResolveOption();
        function->fn_cxt = CurrentMemoryContext;

        PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
        estate->ndatums = curr_compile->plpgsql_nDatums;
        estate->datums = (PLpgSQL_datum **)palloc(sizeof(PLpgSQL_datum *) * curr_compile->plpgsql_nDatums);
        for (int i = 0; i < curr_compile->plpgsql_nDatums; i++)
            estate->datums[i] = curr_compile->plpgsql_Datums[i];

        function->cur_estate = estate;
        function->cur_estate->func = function;
        
        Oid funcid = getMultiFuncInfo(func_inparas.data, expr);
        if (expr->func != NULL)
            pfree_ext(expr->func);
        if (estate->datums != NULL)
            pfree_ext(estate->datums);
        if (estate != NULL)
            pfree_ext(estate);
        expr->func = NULL;
        int	all_arg = 0;
        if (OidIsValid(funcid))
        {
            proctup = SearchSysCache(PROCOID,
                                     ObjectIdGetDatum(funcid),
                                     0, 0, 0);
            
            /*
             * function may be deleted after clist be searched.
             */
            if (!HeapTupleIsValid(proctup))
            {
                const char* message = "function does not exist";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("function \"%s\" doesn't exist ", sqlstart)));
                return NULL;
            }
            /* get the all args informations, only "in" parameters if p_argmodes is null */
            all_arg = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
            procStruct = (Form_pg_proc) GETSTRUCT(proctup);
            ndefaultargs = procStruct->pronargdefaults;
            narg = procStruct->pronargs;
            ReleaseSysCache(proctup);

            for (int k = 0; k < all_arg; k++)  {
                check_tableofindex_args(varnos[k], p_argtypes[k]);
            }

            /* if there is no "out" parameters ,use perform stmt,others use exesql */
            if ((0 == all_arg || NULL == p_argmodes))
            {
                PLpgSQL_stmt_perform *perform = NULL;
                perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
                perform->cmd_type = PLPGSQL_STMT_PERFORM;
                perform->lineno   = plpgsql_location_to_lineno(location);
                perform->expr	  = expr;
                perform->sqlString = plpgsql_get_curline_query();
                return (PLpgSQL_stmt *)perform;
            }
            else if (all_arg >= narg)
            {
                int out_arg_num = get_func_out_arg_num(p_argmodes, all_arg);
                /* out arg number > 1 should build a row */
                bool need_build_row = need_build_row_for_func_arg(&rec, &row, out_arg_num, all_arg, varnos, p_argmodes);
                if (need_build_row)
                {
                    int new_nfields = 0;
                    int i = 0, j = 0;
                    bool varnosInvalid = false;
                    for (i = 0; i < all_arg; i++)
                    {
                        if (p_argmodes[i] == 'i')
                            continue;	
                        else
                            new_nfields++;
                    }
                    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
                    row->dtype = PLPGSQL_DTYPE_ROW;
                    row->refname = pstrdup("*internal*");
                    row->lineno = plpgsql_location_to_lineno(location);
                    row->rowtupdesc = NULL;
                    row->nfields = new_nfields;
                    row->fieldnames = (char **)palloc0(sizeof(char *) * new_nfields);
                    row->varnos = (int *)palloc0(sizeof(int) * new_nfields);
                    row->isImplicit = true;

                    /* fetch out argument from fieldnames into row->fieldnames */
                    for (i = 0; i < nfields; i++)
                    {
                        if (namedarg[i] == false)
                        {
                            if (p_argmodes[i] == 'i')
                                continue;
                            else if (varnos[i] >= 0)
                            {
                                row->fieldnames[j] = fieldnames[i];
                                row->varnos[j] = varnos[i];
                                j++;
                            } else {
                                varnosInvalid = true;
                            }
                        }
                        else
                        {
                            char argmode = FUNC_PARAM_IN;
                            pos_outer = -1;
                            for (int curpos = 0; curpos < all_arg; curpos++)
                            {
                                char* paraname = p_argnames[curpos];

                                if (p_argmodes[curpos] != 'i')
                                    pos_outer++;
                                else
                                {
                                    continue;
                                }

                                if (paraname != NULL && !strcmp(paraname, namedargnamses[i]))
                                {
                                    if (!p_argmodes)
                                        argmode = FUNC_PARAM_IN;
                                    else
                                        argmode = p_argmodes[curpos];

                                    break;
                                }
                            }
                            
                            if (argmode == 'i')
                                continue;

                            if (fieldnames[i] == NULL)
                            {
                                const char* message = "Named argument can not be a const";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmsg("Named argument \"%s\" can not be a const", namedargnamses[i])));
                            }

                            if (row->varnos[pos_outer] > 0)
                            {
                                const char* message = "parameter is assigned more than once";
                                InsertErrorMessage(message, plpgsql_yylloc);
                                ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("parameter \"%s\" is assigned more than once", row->fieldnames[pos_outer])));
                            }
                            if (varnos[i] >= 0)
                            {
                                row->fieldnames[pos_outer] = fieldnames[i];
                                row->varnos[pos_outer] = varnos[i];
                            } else {
                                varnosInvalid = true;
                            }
                        }
                    }
                    if (varnosInvalid) {
                        pfree_ext(row->refname);
                        pfree_ext(row->fieldnames);
                        pfree_ext(row->varnos);
                        pfree_ext(row);
                    } else if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                        expr->out_param_dno = plpgsql_adddatum((PLpgSQL_datum *)row);
                    } else {
                        plpgsql_adddatum((PLpgSQL_datum *)row);
                    }
                }

                PLpgSQL_stmt_execsql * execsql = NULL;
                execsql = (PLpgSQL_stmt_execsql *)palloc(sizeof(PLpgSQL_stmt_execsql));
                execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
                execsql->lineno  = plpgsql_location_to_lineno(location);
                execsql->sqlstmt = expr;
                execsql->into	 = row || rec ? true:false;
                execsql->bulk_collect = false;
                execsql->strict  = true;
                execsql->rec	 = rec;
                execsql->row	 = row;
                execsql->placeholders = placeholders;
                execsql->multi_func = multi_func;
                if (u_sess->parser_cxt.isPerform) {
                    execsql->sqlString = func_inparas.data;
                } else {
                    execsql->sqlString = plpgsql_get_curline_query();
                }
                return (PLpgSQL_stmt *)execsql;
            }
        }
        else
        {
            const char* message = "function is not exclusive";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
             errmsg("function \"%s\" isn't exclusive ", sqlstart)));
        }
    }
    else
    {
        /*
         * if have out parameters and no row/rec, generate row type to store the results.
         * if have row/rec and more than 2 out parameters , also generate row type.
         */
        if (((nfields && NULL == rec &&  NULL == row) || nfields > 1) && !outParamInvalid)
        {
            row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
            row->dtype = PLPGSQL_DTYPE_ROW;
            row->refname = pstrdup("*internal*");
            row->lineno = plpgsql_location_to_lineno(location);
            row->rowtupdesc = NULL;
            row->nfields = nfields;
            row->fieldnames = (char **)palloc(sizeof(char *) * nfields);
            row->varnos = (int *)palloc(sizeof(int) * nfields);
            row->isImplicit = true;
            while (--nfields >= 0)
            {
                row->fieldnames[nfields] = fieldnames[nfields];
                row->varnos[nfields] = varnos[nfields];
            }
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                expr->out_param_dno = plpgsql_adddatum((PLpgSQL_datum *)row);
            } else {
                plpgsql_adddatum((PLpgSQL_datum *)row);
            }
        }
        /* has invalid out param, set it to null */
        if ((rec != NULL || row != NULL) && outParamInvalid)
        {
            rec = NULL;
            row = NULL;
        }
        
        /* if there is no "out" parameters ,use perform stmt,others use exesql */
        if (0 == narg || NULL == p_argmodes)
        {
            PLpgSQL_stmt_perform *perform = NULL;
            perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
            perform->cmd_type = PLPGSQL_STMT_PERFORM;
            perform->lineno   = plpgsql_location_to_lineno(location);
            perform->expr	  = expr;
            perform->sqlString = plpgsql_get_curline_query();
            return (PLpgSQL_stmt *)perform;
        }
        else
        {
            PLpgSQL_stmt_execsql * execsql = NULL;
            execsql = (PLpgSQL_stmt_execsql *)palloc(sizeof(PLpgSQL_stmt_execsql));
            execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
            execsql->lineno  = plpgsql_location_to_lineno(location);
            execsql->sqlstmt = expr;
            execsql->into	 = row || rec ? true:false;
            execsql->bulk_collect = false;
            execsql->strict  = true;
            execsql->rec	 = rec;
            execsql->row	 = row;
            execsql->placeholders = placeholders;
            execsql->multi_func = multi_func;
            execsql->sqlString = plpgsql_get_curline_query();
            return (PLpgSQL_stmt *)execsql;
        }
    }

    pfree_ext(func_inparas.data);
    /* skip compile warning */
    return NULL;
}


static bool
is_unreserved_keyword_func(const char *name)
{
    int i;
    char *func_name[] = {"absolute", "alias", "backward", "constant","current", "debug", "detail","dump" ,"errcode","error", "exceptions", "first", "forward", "hint", "index", "info", "last", "log","message", "message_text", "multiset", "next", "no", "notice", "option", "package", "instantiation","pg_exception_context", "pg_exception_detail", "pg_exception_hint", "query", "record", "relative", "result_oid", "returned_sqlstate", "reverse","row_count","sroll", "slice", "stacked","sys_refcursor","use_column","use_variable","variable_conflict","varray","warning"};
    for (i = 0; i <= 45; i++) {
        if (pg_strcasecmp(func_name[i], name) == 0) {
            return true;
        }
    }
    return false;
}
/*
 * Brief                : check if it is an log function invoke
 * Description  : 
 * in keyword: keyword info
 * in no_parenthesis: if no-parenthesis function is called.
 * in name: the function name
 * returns: true if is a log function.
 * Notes                : 
 */
static bool
is_unreservedkeywordfunction(int kwnum, bool no_parenthesis, const char *name)
{
    if (kwnum >= 0 && no_parenthesis && ScanKeywordCategories[kwnum] == UNRESERVED_KEYWORD && !is_unreserved_keyword_func(name))
        return false;
    else
        return true;
}

/*
 * Brief		: check if it is an function invoke
 * Description	: 
 * in name: the ident name
 * in is_assign: is an assign stmt or not.
 * in no_parenthesis: if no-parenthesis function is called.
 * returns: true if is a function.
 * Notes		: 
 */ 
static bool 
is_function(const char *name, bool is_assign, bool no_parenthesis, List* funcNameList)
{
    List *funcname = NIL;
    FuncCandidateList clist = NULL;
    bool have_outargs = false;
    bool have_inoutargs = false;
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    Oid *p_argtypes = NULL;
    HeapTuple proctup = NULL;
    int narg = 0;
    int i = 0;
    char *cp[3] = {0};

    /* the function is_function is only to judge if it's a function call, so memory used in it is all temp */
    AutoContextSwitch plCompileCxtGuard(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    if (funcNameList == NULL) {
        plpgsql_parser_funcname(name, cp, 3);
    }

    /* 
     * if A.B.C, it's not a function invoke, because can not use function of 
     * other database. if A.B, then it must be a function invoke. 
     */
    if (cp[0] && cp[0][0] != '\0')
    {
        /* these are function in pg_proc who are overloaded, so they can not be
         * execured by := or direct call.
         */
        if (pg_strcasecmp("ts_stat", cp[0]) == 0 ||
            pg_strcasecmp("ts_token_type", cp[0]) == 0 ||
            pg_strcasecmp("ts_parse", cp[0]) == 0 ||
            pg_strcasecmp("dblink_get_notify", cp[0]) == 0 ||
            pg_strcasecmp("ts_debug", cp[0]) == 0)
            return false;

        int kwnum = ScanKeywordLookup(cp[0], &ScanKeywords);
        /* function name can not be reserved keyword */
        if (kwnum >= 0 && ScanKeywordCategories[kwnum] == RESERVED_KEYWORD)
            return false;
        /* function name can not be unreserved keyword when no-parenthesis function is called. except log function*/
        if (!is_unreservedkeywordfunction(kwnum, no_parenthesis, cp[0]))
	{
            return false;
        }
        if (funcNameList == NULL) {
            if (cp[2] && cp[2][0] != '\0') 
                funcname = list_make3(makeString(cp[0]), makeString(cp[1]),makeString(cp[2]));
            else if (cp[1] && cp[1][0] != '\0')
                funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
            else
                funcname = list_make1(makeString(cp[0]));
            /* search the function */
            clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
        } else {
            clist = FuncnameGetCandidates(funcNameList, -1, NIL, false, false, false);
        }
        if (!clist)
        {
            if (!is_assign) {
                const char* message = "function does not exist";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function \"%s\" doesn't exist ", name)));
            }
            return false;
        }
        else if (clist->next)
        {
            if (is_assign)
                return false;
            else
                return true;
        }
        else
        {
            proctup = SearchSysCache(PROCOID,
                             ObjectIdGetDatum(clist->oid),
                             0, 0, 0);
            if (!HeapTupleIsValid(proctup))
            {
                if (!is_assign) {
                    const char* message = "function does not exist";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("function \"%s\" doesn't exist ", name)));
                }
                return false;
            }

            /* get the all args informations, only "in" parameters if p_argmodes is null */
            narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
            if (p_argmodes)
            {
                for (i = 0; i < narg; i++)
                {
                    if ('o' == p_argmodes[i])
                    {
                        have_outargs = true;
                        break;
                    }
                }
                for (i = 0; i < narg; i++)
                {
                    if ('b' == p_argmodes[i])
                    {
                        have_inoutargs = true;
                        break;
                    }
                }
            }
            ReleaseSysCache(proctup);
        }

        if (have_inoutargs && is_function_with_plpgsql_language_and_outparam(clist->oid))
            return true;
        if (!have_outargs && is_assign)
            return false;
        
        return true;/* passed all test */
    } else if (funcNameList != NULL) {
        clist = FuncnameGetCandidates(funcNameList, -1, NIL, false, false, false);
        if (!clist)
        {
            if (!is_assign) {
                const char* message = "function does not exist";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function \"%s\" doesn't exist ", name)));
            }
            return false;
        }
        else if (clist->next)
        {
            if (is_assign)
                return false;
            else
                return true;
        }
        else
        {
            proctup = SearchSysCache(PROCOID,
                             ObjectIdGetDatum(clist->oid),
                             0, 0, 0);
            if (!HeapTupleIsValid(proctup))
            {
                if (!is_assign) {
                    const char* message = "function does not exist";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("function \"%s\" doesn't exist ", name)));
                }
                return false;
            }

            /* get the all args informations, only "in" parameters if p_argmodes is null */
            narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
            if (p_argmodes)
            {
                for (i = 0; i < narg; i++)
                {
                    if ('o' == p_argmodes[i])
                    {
                        have_outargs = true;
                        break;
                    }
                }
                for (i = 0; i < narg; i++)
                {
                    if ('b' == p_argmodes[i])
                    {
                        have_inoutargs = true;
                        break;
                    }
                }
            }
            ReleaseSysCache(proctup);
        }

        if (have_inoutargs && is_function_with_plpgsql_language_and_outparam(clist->oid))
            return true;
        if (!have_outargs && is_assign)
            return false;
        
        return true;/* passed all test */
    }
    return false;
}

static void checkFuncName(List* funcname)
{
    MemoryContext colCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    char* name = NameListToString(funcname);
    Oid packageOid = InvalidOid;
    Oid namespaceId = InvalidOid;
    if (list_length(funcname) > 2) {
        if (list_length(funcname) == 3) {
            char* schemaname = strVal(linitial(funcname));
            char* pkgname = strVal(lsecond(funcname));
            namespaceId = SchemaNameGetSchemaOid(schemaname, false);
            if (OidIsValid(namespaceId)) {
                packageOid = PackageNameGetOid(pkgname, namespaceId);
            }
        }
        if (!OidIsValid(packageOid)) {
            const char* message = "function does not exist";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cross-database references are not implemented: %s", name)));
            u_sess->plsql_cxt.have_error = true;
        }
    }
    (void)MemoryContextSwitchTo(colCxt);
}

/*
 * @brief is_datatype
 *  check if a given type is a datatype
 * @param name      constructed type name
 * @return true     is datatype
 * @return false    not a datatype
 */
static bool is_paren_friendly_datatype(TypeName *name)
{
    Type type_tup = LookupTypeNameExtended(NULL, name, NULL, false);
    if (!HeapTupleIsValid(type_tup)) {
        return false;
    }
    ReleaseSysCache(type_tup);

    /* deal with internal type casts, e.g. timestamp(6) */
    Oid typoid = LookupTypeNameOid(name);
    bool preferred = false;
    char category = TYPCATEGORY_INVALID;
    get_type_category_preferred(typoid, &category, &preferred);
    if (category != TYPCATEGORY_ARRAY && category != TYPCATEGORY_COMPOSITE
        && category != TYPCATEGORY_TABLEOF && category != TYPCATEGORY_TABLEOF_VARCHAR
        && category != TYPCATEGORY_TABLEOF_INTEGER) {
        return false;
    }
    return true;
}

/*
 * @brief init_array_parse_context
 *  Initialize array parse context.
 */
static inline void init_array_parse_context(ArrayParseContext *context)
{
    context->list_left_bracket = NULL;
    context->list_right_bracket = NULL;
    context->list_array_state = NULL;
    context->list_datatype = NULL;
    context->array_is_empty = false;
    context->array_is_nested = false;
}

/*
 * @brief push_array_parse_stack
 *  Push array parse context stacks.
 * @param context       array parse context
 * @param parenlevel    parentheses level, set -1 to push nothing
 * @param state         array state flag, set -1 to push nothing
 */
static inline void push_array_parse_stack(ArrayParseContext *context, int parenlevel, int state)
{
    if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
        return;
    }
    if (likely(parenlevel >= 0)) {
        context->list_left_bracket = lcons_int(parenlevel, context->list_left_bracket);
        context->list_right_bracket = lcons_int(parenlevel, context->list_right_bracket);
    }
    if (state >= 0) {
        context->list_array_state = lcons_int(state, context->list_array_state);
    }
}

/*
 * @brief construct_array_start
 *  Extracted from read_sql_constuct, start of T_VARRAY/T_TABLE array FSM.
 * @param ds            sql string builder
 * @param context       array state context
 * @param var_type      element type
 * @param tok           current token, should be T_VARRAY/T_TABLE
 * @param parenlevel    parentheses stack
 * @param loc           T_VARRAY starting location
 */
static bool construct_array_start(StringInfo ds, ArrayParseContext *context, PLpgSQL_type *var_type, int *tok,
                                  int parenlevel, int loc)
{
    /* Save array token for datatype casts */
    context->list_datatype = lcons(var_type, context->list_datatype);
    *tok = yylex(); /* always yylex to parentheses */
    plpgsql_push_back_token(*tok);
    /* varray constructor */
    if (*tok == '(') {
        appendStringInfoString(ds, "ARRAY");
        push_array_parse_stack(context, parenlevel, ARRAY_START);
    } else if (*tok == '[') {
        yyerror("syntax error");    /* typename + '[' is not allowed */
        return false;   /* compiler happy */
    } else {    /* coerce? no need for extra check, leave it to main parser */
        plpgsql_append_source_text(ds, loc, yylloc);
    }
    return true;
}

/*
 * @brief get_real_elemtype
 *  Get the real element type from a table or array
 * @param type              any type
 * @return PLpgSQL_type*    element type
 */
static PLpgSQL_type *get_real_elemtype(PLpgSQL_type *type)
{
    HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type->typoid));
    if (!HeapTupleIsValid(type_tup)) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for type %u, type Oid is invalid", type->typoid)));
    }
    if (((Form_pg_type)GETSTRUCT(type_tup))->typcategory == TYPCATEGORY_ARRAY &&
        type->collectionType == PLPGSQL_COLLECTION_NONE) {
        type->collectionType = PLPGSQL_COLLECTION_ARRAY;
    }
    if (type->collectionType != PLPGSQL_COLLECTION_TABLE &&
        type->collectionType != PLPGSQL_COLLECTION_ARRAY) {
        /* Guarding condition(white list) */
        ReleaseSysCache(type_tup);
        return type;
    }
    PLpgSQL_type* typ = NULL;
    Oid base_oid = ((Form_pg_type)GETSTRUCT(type_tup))->typelem;
    HeapTuple base_type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(base_oid));
    if (!HeapTupleIsValid(base_type_tup)) {
        ereport(ERROR, (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for type %u, type Oid is invalid", base_oid)));
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL)
    {
        typ = build_datatype(base_type_tup, type->atttypmod, 0);
    } else {
        typ = build_datatype(base_type_tup, type->atttypmod,
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation);
    }
    typ->collectionType = type->collectionType;
    ReleaseSysCache(base_type_tup);
    ReleaseSysCache(type_tup);
    pfree_ext(type);
    return typ;
}

/*
 * @brief construct_object_type
 *  Construct object type to strings.
 * @param ds            sql string builder
 * @param context       array state context
 * @param name          object typename
 * @param tok           current token, should be T_WORD/T_CWORD
 * @param parenlevel    parentheses stack
 * @param curloc        current parsing location, usually the following token
 * @param loc           T_WORD/T_CWORD starting location
 */
static bool construct_object_type(StringInfo ds, ArrayParseContext *context, TypeName *name, int *tok,
                                  int parenlevel, int curloc, int loc)
{
    if (*tok != '(' || u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
        curloc = yylloc;    /* new token location */
        plpgsql_append_source_text(ds, loc, curloc);
        context->array_is_nested = false;
        return true;
    }

    if (context->array_is_nested) {
        push_array_parse_stack(context, parenlevel, ARRAY_ACCESS);
        curloc = yylloc;    /* new token location */
        plpgsql_append_source_text(ds, loc, curloc);
        context->array_is_nested = false;
        return true;
    }

    /*
     * Support various datatype constructor. We expect the T_WORD with format like:
     *   'var := foo(bar)' or inside other objects.
     */
    char *name_str = NameListToString(name->names);
    int toksz = strlen(name_str);
    if (yylloc - curloc != toksz || !is_paren_friendly_datatype(name)) { /* NOTE: curloc here is T_WORD/T_CWORD location */
        curloc = yylloc;    /* new token location */
        plpgsql_append_source_text(ds, loc, curloc);
        pfree_ext(name_str);
        return true;
    }
    PLpgSQL_type *type = parse_datatype(name_str, loc);\
    pfree_ext(name_str);

    type = get_real_elemtype(type);
    if (type->collectionType == PLPGSQL_COLLECTION_ARRAY || \
        type->collectionType == PLPGSQL_COLLECTION_TABLE) {
        return construct_array_start(ds, context, type, tok, parenlevel, loc);
    }
    appendStringInfoString(ds, "ROW");
    return true;
}

/*
 * @brief construct_word
 *  Construct T_WORD, extracted from read_sql_constuct.
 * @param ds            sql string builder
 * @param context       array state context
 * @param tok           current token, should be T_WORD
 * @param parenlevel    parentheses stack
 * @param loc           location of T_WORD
 */
static bool construct_word(StringInfo ds, ArrayParseContext *context, int *tok, int parenlevel, int loc)
{
    char *name = NULL;
    name = yylval.word.ident;
    int curloc = yylloc;
    *tok = yylex();
    plpgsql_push_back_token(*tok);
    return construct_object_type(ds, context, makeTypeName(name), tok, parenlevel, curloc, loc);
}

/*
 * @brief construct_cword
 *  Construct T_CWORD, extracted from read_sql_constuct.
 * @param ds            sql string builder
 * @param context       array state context
 * @param tok           current token, should be T_CWORD
 * @param parenlevel    parentheses stack
 * @param loc           location of T_CWORD
 */
static bool construct_cword(StringInfo ds, ArrayParseContext *context, int *tok, int parenlevel, int loc)
{
    int nnames = 0;
    List *idents = yylval.cword.idents;
    char *word1 = NULL;
    char *word2 = NULL;
    char *word3 = NULL;
    if (list_length(idents) == 2) {
        word1 = strVal(linitial(idents));
        word2 = strVal(lsecond(idents));
    } else if (list_length(idents) == 3) {
        word1 = strVal(linitial(idents));
        word2 = strVal(lsecond(idents));
        word3 = strVal(lthird(idents));
    } else {
        yyerror("syntax error");
    }
    if (enable_plpgsql_gsdependency()) {
        FuncCandidateList clist = FuncnameGetCandidates(idents, -1, NIL, false, false, true);
        if (clist == NULL) {
            gsplsql_build_gs_variable_dependency(idents);
        }
    }
    if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
        int dno = -1;
        char *name = NameListToString(idents);
        if (plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, word3, &nnames) == NULL) {
            dno = plpgsql_pkg_add_unknown_var_to_namespace(yylval.cword.idents);
        }
        if (dno != -1) {
            appendStringInfoString(ds, name);
            appendStringInfoString(ds, " ");
        }
        pfree_ext(name);
        /* This originally was a fall through to default branch */
        *tok = yylex();
        int curloc = yylloc;
        plpgsql_push_back_token(*tok);
        plpgsql_append_source_text(ds, loc, curloc);
        return true;
    }
    int curloc = yylloc;
    *tok = yylex();
    plpgsql_push_back_token(*tok);
    bool result;
    CreatePlsqlType oldCreatePlsqlType = u_sess->plsql_cxt.createPlsqlType;
    PG_TRY();
    {
        set_create_plsql_type_not_check_nsp_oid();
        result = construct_object_type(ds, context, makeTypeNameFromNameList(idents), tok, parenlevel, curloc, loc);
        set_create_plsql_type(oldCreatePlsqlType);
    }
    PG_CATCH();
    {
        set_create_plsql_type(oldCreatePlsqlType);
        PG_RE_THROW();
    }
    PG_END_TRY();
    return result;
}

/* Convenience routine to read an expression with one possible terminator */
static PLpgSQL_expr *
read_sql_expression(int until, const char *expected)
{
    return read_sql_construct(until, 0, 0, expected,
                              "SELECT ", true, true, true, NULL, NULL);
}

/* Convenience routine to read an expression with two possible terminators */
static PLpgSQL_expr *
read_sql_expression2(int until, int until2, const char *expected,
                     int *endtoken)
{
    return read_sql_construct(until, until2, 0, expected,
                              "SELECT ", true, true, true, NULL, endtoken);
}

/* Convenience routine to read a SQL statement that must end with ';' */
static PLpgSQL_expr *
read_sql_stmt(const char *sqlstart)
{
    return read_sql_construct(';', 0, 0, ";",
                              sqlstart, false, true, true, NULL, NULL);
}

/*
 * Brief		: read a sql construct who has 4 expected tokens.
 * Description	: just like read_sql_construct function below, except that 
 * 				  this function has 4 expected tokens.
 * Notes		:
 */ 

/*
 * Read a SQL construct and build a PLpgSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * until4:		token code for alternate terminator (pass 0 if none)
 * until5:		token code for alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * trim:		trim trailing whitespace
 * startloc:	if not NULL, location of first token is stored at *startloc
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 */
static PLpgSQL_expr *
read_sql_construct6(int until,
                   int until2,
                   int until3,
                   int until4,
                   int until5,
                   int until6,
                   const char *expected,
                   const char *sqlstart,
                   bool isexpression,
                   bool valid_sql,
                   bool trim,
                   int *startloc,
                   int *endtoken,
                   DList* tokenstack,
                   bool issaveexpr)
{
    int					tok = 0;
    int					prev_tok = 0;
    StringInfoData		ds;
    IdentifierLookup	save_IdentifierLookup;
    int					startlocation = -1;
    int					parenlevel = 0;
    PLpgSQL_expr		*expr;
    MemoryContext		oldCxt = NULL;

    /* 
     * buf stores the cursor attribute internal variables or 
     * varray ident with operate function, so NAMEDATALEN + 128 is enough 
     */
    char				buf[NAMEDATALEN + 128];
    bool				ds_changed = false;
    ArrayParseContext	context;
    List				*idents = 0;
    const char			left_bracket[2] = "[";
    const char			right_bracket[2] = "]";
    const char			left_parentheses[2] = "(";
    const char			right_parentheses[2] = ")";
    int					loc = 0;
    int					curloc = 0;
    int					brack_cnt = 0;
     /* mark if there are 2 table of index by var call functions in an expr */
    int tableof_func_dno = -1;
    int tableof_var_dno = -1;
    bool is_have_tableof_index_var = false;
    List* tableof_index_list = NIL;

    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /*
     * ds will do a lot of enlarge, which need to relloc the space, and the old space 
     * will be return to current context. So if we don't switch the context, the original 
     * context will be Plpgsql function context, which has a long term life cycle, 
     * may cause memory accumulation.
     */
    oldCxt = MemoryContextSwitchTo(curr_compile->compile_tmp_cxt);
    initStringInfo(&ds);
    MemoryContextSwitchTo(oldCxt);

    appendStringInfoString(&ds, sqlstart);

    /* special lookup mode for identifiers within the SQL text */
    save_IdentifierLookup = curr_compile->plpgsql_IdentifierLookup;
    curr_compile->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

    init_array_parse_context(&context);
    for (;;)
    {
        prev_tok = tok;
        tok = yylex();
        tokenstack = push_token_stack(tok, tokenstack);
        loc = yylloc;
        if (startlocation < 0)			/* remember loc of first token */
            startlocation = yylloc;
        if (tok == until && parenlevel == 0)
            break;
        if (tok == until2 && parenlevel == 0)
            break;
        if (tok == until3 && parenlevel == 0)
            break;
        if (tok == until4 && parenlevel == 0)
            break;
        if (tok == until5 && parenlevel == 0)
            break;
        if (tok == until6 && parenlevel == 0)
            break;
        if (tok == '(' || tok == '[')
            parenlevel++;
        else if (tok == ')' || tok == ']')
        {
            parenlevel--;
            if (parenlevel < 0)
                yyerror("mismatched parentheses", true);
        }
        /*
         * End of function definition is an error, and we don't expect to
         * hit a semicolon either (unless it's the until symbol, in which
         * case we should have fallen out above).
         */
        if (tok == 0 || tok == ';')
        {
            if (parenlevel != 0)
                yyerror("mismatched parentheses", true);
            if (isexpression) {
                const char* message = "missing something at end of SQL expression";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("missing \"%s\" at end of SQL expression",
                                expected),
                         parser_errposition(yylloc)));
            } else {
                const char* message = "missing something at end of SQL expression";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("missing \"%s\" at end of SQL statement",
                                expected),
                         parser_errposition(yylloc)));
            }
        }

        switch (tok)
        {		     
            case T_SQL_FOUND:
                appendStringInfo(&ds, " __gsdb_sql_cursor_attri_found__ ");
                ds_changed = true;
                break;
            case T_CURSOR_FOUND:	
                appendStringInfo(&ds, " __gsdb_cursor_attri_%d_found__ ", yylval.ival);
                ds_changed = true;
                break;
            case T_SQL_NOTFOUND:
                appendStringInfo(&ds, " __gsdb_sql_cursor_attri_notfound__ ");
                ds_changed = true;
                break;
            case T_CURSOR_NOTFOUND:
                appendStringInfo(&ds, " __gsdb_cursor_attri_%d_notfound__ ", yylval.ival);
                ds_changed = true;
                break;
            case T_SQL_ISOPEN:
                appendStringInfo(&ds, " __gsdb_sql_cursor_attri_isopen__ ");
                ds_changed = true;
                break;
            case T_CURSOR_ISOPEN:
                appendStringInfo(&ds, " __gsdb_cursor_attri_%d_isopen__ ", yylval.ival);
                ds_changed = true;
                break;
            case T_SQL_ROWCOUNT:
                appendStringInfo(&ds, " __gsdb_sql_cursor_attri_rowcount__ ");
                ds_changed = true;
                break;
            case T_CURSOR_ROWCOUNT:
                appendStringInfo(&ds, " __gsdb_cursor_attri_%d_rowcount__ ", yylval.ival);
                ds_changed = true;
                break;
            case T_PACKAGE_CURSOR_NOTFOUND:
                appendStringInfo(&ds, " %s.__gsdb_cursor_attri_%d_notfound__ ",
                    yylval.wdatum.ident, yylval.wdatum.dno);
                ds_changed = true;
                break;
            case T_PACKAGE_CURSOR_ISOPEN:
                appendStringInfo(&ds, " %s.__gsdb_cursor_attri_%d_isopen__ ",
                    yylval.wdatum.ident, yylval.wdatum.dno);
                ds_changed = true;
                break;
            case T_PACKAGE_CURSOR_ROWCOUNT:
                appendStringInfo(&ds, " %s.__gsdb_cursor_attri_%d_rowcount__ ",
                    yylval.wdatum.ident, yylval.wdatum.dno);
                ds_changed = true;
                break;
            case T_PACKAGE_CURSOR_FOUND:
                appendStringInfo(&ds, " %s.__gsdb_cursor_attri_%d_found__ ",
                    yylval.wdatum.ident, yylval.wdatum.dno);
                ds_changed = true;
                break;
            case T_VARRAY_VAR:
                idents = yylval.wdatum.idents;
                if (idents == NIL) {
                    AddNamespaceIfPkgVar(yylval.wdatum.ident, save_IdentifierLookup);
                }
                tok = yylex();
                if (tok == '(' || tok == '[') {
                    push_array_parse_stack(&context, parenlevel, ARRAY_ACCESS);
                }
                curloc = yylloc;
                plpgsql_push_back_token(tok);
                if (list_length(idents) >= 3) {
                    plpgsql_cast_reference_list(idents, &ds, false);
                    ds_changed = true;
                    break;
                } else {
                    plpgsql_append_source_text(&ds, loc, curloc);
                    ds_changed = true;
                    break;
                }
            case T_ARRAY_FIRST:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID) {
                    appendStringInfo(&ds, "ARRAY_VARCHAR_FIRST(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents, false);
                } else if (indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INTEGER_FIRST(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents, false);
                } else {
                    appendStringInfo(&ds, "ARRAY_LOWER(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                    appendStringInfo(&ds, "1 ");
                }
                
                int tok = yylex();
                if(tok == '(') {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    appendStringInfo(&ds, ") ");
                    ds_changed = true;
                    plpgsql_push_back_token(tok);
                }
                break;
            }
            case T_ARRAY_LAST:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID) {
                    appendStringInfo(&ds, "ARRAY_VARCHAR_LAST(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents, false);
                } else if (indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INTEGER_LAST(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents, false);
                } else {
                    appendStringInfo(&ds, "ARRAY_UPPER(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                    appendStringInfo(&ds, "1 ");
                }
                
                int tok = yylex();
                if(tok == '(') {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    appendStringInfo(&ds, ") ");
                    ds_changed = true;
                    plpgsql_push_back_token(tok);
                }
                break;
            }
            case T_ARRAY_COUNT:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID || indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INDEXBY_LENGTH(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                    appendStringInfo(&ds, "1 ");
                } else {
                    appendStringInfo(&ds, "ARRAY_LENGTH(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                    appendStringInfo(&ds, "1 ");
                }

                int tok = yylex();
                if(tok == '(') {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    appendStringInfo(&ds, ") ");
                    ds_changed = true;
                    plpgsql_push_back_token(tok);
                }
                break;
            }
            case T_ARRAY_EXISTS:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID) {
                    appendStringInfo(&ds, "ARRAY_VARCHAR_EXISTS(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else if (indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INTEGER_EXISTS(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else {
                    appendStringInfo(&ds, "ARRAY_EXISTS(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                }
                
                int tok = yylex();
                if('(' == tok) {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    plpgsql_push_back_token(tok);
                    yyerror("syntax error");
                }
                break;
            }
            case T_ARRAY_PRIOR:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID) {
                    appendStringInfo(&ds, "ARRAY_VARCHAR_PRIOR(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else if (indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INTEGER_PRIOR(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else {
                    appendStringInfo(&ds, "ARRAY_PRIOR(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                }

                int tok = yylex();
                if('(' == tok) {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    plpgsql_push_back_token(tok);
                    yyerror("syntax error");
                }
                break;
            }
            case T_ARRAY_NEXT:
            {
                Oid indexType = get_table_index_type(yylval.wdatum.datum, &tableof_func_dno);
                if (indexType == VARCHAROID) {
                    appendStringInfo(&ds, "ARRAY_VARCHAR_NEXT(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else if (indexType == INT4OID) {
                    appendStringInfo(&ds, "ARRAY_INTEGER_NEXT(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                } else {
                    appendStringInfo(&ds, "ARRAY_NEXT(");
                    CastArrayNameToArrayFunc(&ds, yylval.wdatum.idents);
                }

                int tok = yylex();
                if('(' == tok) {
                    push_array_parse_stack(&context, -1, NOT_AN_ARRAY);
                    parenlevel++;
                    ds_changed = true;
                    tokenstack = push_token_stack(tok, tokenstack);
                } else {
                    plpgsql_push_back_token(tok);
                    yyerror("syntax error");
                }
                break;
            }
            case ']':
                brack_cnt--;
                /* fall through */
            case ')':
                if (context.list_right_bracket && context.list_right_bracket->length
                    && linitial_int(context.list_right_bracket) == parenlevel) {
                    /* append bracket instead of parentheses */
                    appendStringInfoString(&ds, right_bracket);
                    ds_changed = true;
                    context.list_right_bracket = list_delete_first(context.list_right_bracket);

                    if (context.array_is_empty) {
                        /* NOTE: empty array state will remain ARRAY_START until now */
                        PLpgSQL_type *type = (PLpgSQL_type *)linitial(context.list_datatype);
                        plpgsql_append_object_typename(&ds, type);
                        appendStringInfoString(&ds, left_bracket);
                        appendStringInfoString(&ds, right_bracket);
                        context.array_is_empty = false;
                    }

                    /*
                     * N-D Array accessing pattern:
                     * If another pair of brackets(parentheses) are right behind an array
                     * access statment like 'array(i)' or 'array[i], they are very likely
                     * to be part of the array statement too. Therefore, we need to make
                     * sure we interpret them as brackets.
                     */
                    if (IS_ARRAY_STATE(context.list_array_state, ARRAY_ACCESS)) {
                        tok = yylex();
                        plpgsql_push_back_token(tok);
                        if (tok == '(' || tok == '[') { /* array(1)(1) */
                            push_array_parse_stack(&context, parenlevel, -1);
                            break;
                        } else if (tok == '.') { /* array(1).col(1) */
                            context.array_is_nested = true;
                        }
                    } else {
                        /* It is important to pop the array dno stack at the end. */
                        context.list_datatype = list_delete_first(context.list_datatype);
                    }

                    /* always pop the array state stack, including ARRAY_ACCESS */
                    context.list_array_state = list_delete_first(context.list_array_state);
                } else {
                    /* pop excess array state stack, see case '(' below */
                    if (IS_ARRAY_STATE(context.list_array_state, NOT_AN_ARRAY)) {
                        context.list_array_state = list_delete_first(context.list_array_state);
                    }

                    if (tok ==']') {
                        appendStringInfoString(&ds, right_bracket);
                    } else {
                        appendStringInfoString(&ds, right_parentheses);
                    }
                }
                ds_changed = true;
                break;
            case '[':
                brack_cnt++;
                /* fall through */
            case '(':
                if (context.list_left_bracket && context.list_left_bracket->length
                    && linitial_int(context.list_left_bracket) == parenlevel - 1) {
                    appendStringInfoString(&ds, left_bracket);
                    context.list_left_bracket = list_delete_first(context.list_left_bracket);

                    /* in the case of '()' or '[]', we need to append NULL */
                    prev_tok = tok;
                    tok = yylex();
                    plpgsql_push_back_token(tok);
                    if ((prev_tok == '(' && tok == ')') || (prev_tok == '[' && tok == ']')) {
                        if (IS_ARRAY_STATE(context.list_array_state, ARRAY_ACCESS)) {
                            yyerror("empty index");
#ifndef ENABLE_MULTIPLE_NODES
                            if (u_sess->attr.attr_common.plsql_show_all_error) {
                                u_sess->plsql_cxt.have_error = true;
                                tok = yylex(); // lex ) or ]
                                tok = yylex(); // lex ;
                                return NULL;
                            }
#endif
                        }
                        context.array_is_empty = true;
                    } else if (IS_ARRAY_STATE(context.list_array_state, ARRAY_START)) {
                        SET_ARRAY_STATE(context.list_array_state, ARRAY_ELEMENT);
                        /* always append left parentheses at start of each element */
                        appendStringInfoString(&ds, left_parentheses);
                    }
                } else {
                    /* array state stack up with none  */
                    if (IS_ARRAY_STATE(context.list_array_state, ARRAY_ELEMENT) ||
                        IS_ARRAY_STATE(context.list_array_state, NOT_AN_ARRAY)) {
                        context.list_array_state = lcons_int(NOT_AN_ARRAY, context.list_array_state);
                    }

                    if (tok =='[') {
                        appendStringInfoString(&ds, left_bracket);
                    } else {
                        appendStringInfoString(&ds, left_parentheses);
                    }
                }
                ds_changed = true;
                break;
            case '.':
                tok = yylex();
                plpgsql_push_back_token(tok);
                curloc = yylloc;
                if (context.array_is_nested && tok != T_WORD && tok != T_CWORD) {
                    context.list_array_state = list_delete_first(context.list_array_state);
                    context.array_is_nested = false;
                }
                plpgsql_append_source_text(&ds, loc, curloc);
                ds_changed = true;
                break;
            case T_TABLE_VAR:
            {
                /* 
                 * table var name may be schema.pkg.table_var
                 * so the name length should NAMEDATALEN with quotation marks *3
                 */
                const int tablevar_namelen = NAMEDATALEN * 3 + 6;
                char tableName1[tablevar_namelen] = {0};
                idents = yylval.wdatum.idents;
                if (idents == NIL) {
                    AddNamespaceIfPkgVar(yylval.wdatum.ident, save_IdentifierLookup);
                }
                copy_table_var_indents(tableName1, yylval.wdatum.ident, tablevar_namelen);
                PLpgSQL_datum* datum = yylval.wdatum.datum;
                int var_dno = yylval.wdatum.dno;
                if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                    check_autonomous_nest_tablevar((PLpgSQL_var*)datum);
                }
                tok = yylex();
                if('(' == tok) {
                    push_array_parse_stack(&context, parenlevel, ARRAY_ACCESS);
                    tableof_index_list = lappend_int(tableof_index_list, ((PLpgSQL_var*)datum)->dno);
                } else if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && K_MULTISET == tok) {
                    Oid typeOid = get_table_type(datum);
                    read_multiset(&ds, tableName1, typeOid);
                    ds_changed = true;
                    break;
                } else {
                    PLpgSQL_var* var = (PLpgSQL_var*)datum;
                    if (OidIsValid(var->datatype->tableOfIndexType) && 
                        (',' == tok || ')' == tok || ';' == tok)) {
                        is_have_tableof_index_var = true;
                        /* tableof_var_dno is  only used for args */
                        if (',' == tok || ')' == tok) {
                            tableof_var_dno = var_dno;
                        }
                    }
                }

                curloc = yylloc;
                plpgsql_push_back_token(tok);
                if (list_length(idents) >= 3) {
                    plpgsql_cast_reference_list(idents, &ds, false);
                    ds_changed = true;
                    break;
                } else {
                    plpgsql_append_source_text(&ds, loc, curloc);
                    ds_changed = true;
                    break;
                }
            }
            case T_PACKAGE_VARIABLE:
            {
                int type_flag = -1;
                get_datum_tok_type(yylval.wdatum.datum, &type_flag);
                idents = yylval.wdatum.idents;
                int var_dno = yylval.wdatum.dno;

                if (enable_plpgsql_gsdependency()) {
                    gsplsql_build_gs_variable_dependency(idents);
                }
                if (type_flag == PLPGSQL_TOK_TABLE_VAR) {
                    /*
                     * table var name may be schema.pkg.table_var
                     * so the name length should NAMEDATALEN with quotation marks *3
                     */
                    const int tablevar_namelen = NAMEDATALEN * 3 + 6;

                    char tableName1[tablevar_namelen] = {0};
                    idents = yylval.wdatum.idents;
                    copy_table_var_indents(tableName1, yylval.wdatum.ident, tablevar_namelen);
                    PLpgSQL_datum* datum = yylval.wdatum.datum;
                    tok = yylex();
                    if('(' == tok)
                    {
                        push_array_parse_stack(&context, parenlevel, NOT_AN_ARRAY);
                    } else if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && K_MULTISET == tok) {
                        Oid typeOid = get_table_type(datum);
                        read_multiset(&ds, tableName1, typeOid);
                        ds_changed = true;
                        break;
                    } else {
                        PLpgSQL_var* var = (PLpgSQL_var*)datum;
                        if (OidIsValid(var->datatype->tableOfIndexType) &&
                            (',' == tok || ')' == tok || ';' == tok)) {
                            is_have_tableof_index_var = true;
                            /* tableof_var_dno is  only used for args */
                            if (',' == tok || ')' == tok) {
                                tableof_var_dno = var_dno;
                            }
                        }
                    }
                    curloc = yylloc;
                    plpgsql_push_back_token(tok);
                    if (PkgVarNeedCast(idents)) {
                        plpgsql_cast_reference_list(idents, &ds, true);
                    } else {
                        plpgsql_append_source_text(&ds, loc, curloc);
                    }
                    ds_changed = true;
                    break;
                } else if (type_flag == PLPGSQL_TOK_VARRAY_VAR) {
                    tok = yylex();
                    if (tok == '(' || tok == '[') {
                        push_array_parse_stack(&context, parenlevel, ARRAY_ACCESS);
                    }
                    curloc = yylloc;
                    plpgsql_push_back_token(tok);
                    if (PkgVarNeedCast(idents)) {
                        plpgsql_cast_reference_list(idents, &ds, true);
                    } else {
                        plpgsql_append_source_text(&ds, loc, curloc);
                    }
                    ds_changed = true;
                    break;
                } else {
                    tok = yylex();
                    curloc = yylloc;
                    plpgsql_push_back_token(tok);

                    if (PkgVarNeedCast(idents)) {
                        plpgsql_cast_reference_list(idents, &ds, true);
                    } else {
                        plpgsql_append_source_text(&ds, loc, curloc);
                    }

                    ds_changed = true;
                    break;
                }
            }
            case T_TABLE:
            {
                int dno = yylval.wdatum.datum->dno;
                PLpgSQL_var *var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
                /* table of index by only support: a = a(); */
                if ((OidIsValid(var->datatype->tableOfIndexType) && var->nest_table == NULL) ||
                   	(var->nest_table != NULL && var->isIndexByTblOf)) {
                    TokenData* temptokendata = build_token_data(tok);
                    int first_tok = yylex();
                    DList* tokenstack = NULL;
                    TokenData* temptokendata1 = build_token_data(first_tok);
                    tokenstack = dlappend(tokenstack, temptokendata1);
                    if (first_tok != '(') {
                        yyerror("table of index by does not support syntax");
                    }

                    int second_tok = yylex();
                    temptokendata1 = build_token_data(second_tok);
                    if (second_tok != ')') {
                        yyerror("table of index by does not support syntax");
                    }
                    tokenstack = dlappend(tokenstack, temptokendata1);
                    /* restore yylex */
                    push_back_token_stack(tokenstack);
                    yylloc = temptokendata->lloc;
                    yylval = temptokendata->lval;
                    u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = temptokendata->leng;
                }
                ds_changed = construct_array_start(&ds, &context, var->datatype, &tok, parenlevel, loc);
                break;
            }
            case T_VARRAY:
            {
                if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                    /* non A_FORMAT case, append 'NULL' and leave it. */
                    int first_tok = yylex();
                    tokenstack= push_token_stack(first_tok, tokenstack);
                    
                    int second_tok = yylex();
                    tokenstack = push_token_stack(first_tok, tokenstack);
                    if (first_tok == '(' && second_tok == ')') {
                        snprintf(buf, sizeof(buf), " NULL ");
                        appendStringInfoString(&ds, buf);
                    }
                    ds_changed = true;
                    break;
                }
                int dno = yylval.wdatum.datum->dno;
                PLpgSQL_var *var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
                ds_changed = construct_array_start(&ds, &context, var->datatype, &tok, parenlevel, loc);
                break;
            }
            case T_DATUM:
                idents = yylval.wdatum.idents;
                if(prev_tok != '.' && list_length(idents) >= 3) {
                    plpgsql_cast_reference_list(idents, &ds, false);
                    ds_changed = true;
                    break;
                } else {
                    tok = yylex();
                    curloc = yylloc;
                    plpgsql_push_back_token(tok);
                    plpgsql_append_source_text(&ds, loc, curloc);
                    ds_changed = true;
                    break;
                }
            case T_WORD:
                AddNamespaceIfPkgVar(yylval.word.ident, save_IdentifierLookup);
                ds_changed = construct_word(&ds, &context, &tok, parenlevel, loc);
                break;
            case T_CWORD:
                ds_changed = construct_cword(&ds, &context, &tok, parenlevel, loc);
                break;
            default:
                tok = yylex();

                if(tok > INT_MAX)
                {
                    const char* message = "token value is bigger than INT_MAX";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                             errmsg("token value %d is bigger than INT_MAX ", tok)));
                }

                curloc = yylloc;
                plpgsql_push_back_token(tok);
                if (tok == T_VARRAY_VAR && pushed_bulk_exception()) {
                    plpgsql_append_source_text(&ds, loc, curloc - 4);
                } else {
                    plpgsql_append_source_text(&ds, loc, curloc);
                }
                ds_changed = true;
                break;
        }

        /* peek one token */
        tok = yylex();
        plpgsql_push_back_token(tok);

        /* we are expecting an element, but a seperator/end of array is found, need coerce */
        if ((tok == ',' || tok == ')') && IS_ARRAY_STATE(context.list_array_state, ARRAY_ELEMENT)) {
            SET_ARRAY_STATE(context.list_array_state, ARRAY_COERCE);
        }

        /* this is done after seperator is appended, get next element */
        if (IS_ARRAY_STATE(context.list_array_state, ARRAY_SEPERATOR)) {
            /* always append left parentheses at start of each element */
            appendStringInfoString(&ds, left_parentheses);
            /* Note: we assume that all seperators takes EXACTLY ONE iteration to process */
            SET_ARRAY_STATE(context.list_array_state, ARRAY_ELEMENT);
        }

        /* add coerce at the end of each element, then append the seperator next iteration */
        if (IS_ARRAY_STATE(context.list_array_state, ARRAY_COERCE)) {
            /* always append right parentheses at end of each element */
            appendStringInfoString(&ds, right_parentheses);
            plpgsql_append_object_typename(&ds, (PLpgSQL_type *)linitial(context.list_datatype));
            SET_ARRAY_STATE(context.list_array_state, ARRAY_SEPERATOR);
        }
    }

    if (brack_cnt != 0) {
        yyerror("mismatched brackets");
    }

    curr_compile->plpgsql_IdentifierLookup = save_IdentifierLookup;

    if (startloc)
        *startloc = startlocation;
    if (endtoken)
        *endtoken = tok;

    /* give helpful complaint about empty input */
    if (startlocation >= yylloc)
    {
        if (isexpression)
            yyerror("missing expression");
        else
            yyerror("missing SQL statement");
    }

    if (!ds_changed)
        plpgsql_append_source_text(&ds, startlocation, yylloc);
    
    /* trim any trailing whitespace, for neatness */
    if (trim)
    {
        while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
            ds.data[--ds.len] = '\0';
    }
    if (issaveexpr) {
        oldCxt = MemoryContextSwitchTo(curr_compile->compile_cxt);
    }

    if (tableof_index_list != NULL && tableof_func_dno != -1) {
        ListCell* cell = NULL;
        foreach (cell, tableof_index_list) {
            int dno = lfirst_int(cell);
            if (dno != tableof_func_dno) {
                yyerror("do not support more than 2 table of index by variables call functions in an expr");
            }
        }
    }
    list_free_ext(tableof_index_list);

    expr = (PLpgSQL_expr *)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(ds.data);
    expr->plan			= NULL;
    expr->paramnos		= NULL;
    expr->ns			= plpgsql_ns_top();
    expr->isouttype 		= false;
    expr->idx			= (uint32)-1;
    expr->out_param_dno = -1;
    expr->is_have_tableof_index_var = is_have_tableof_index_var;
    expr->tableof_var_dno = tableof_var_dno;
    expr->is_have_tableof_index_func = tableof_func_dno != -1 ? true : false;
    expr->tableof_func_dno = tableof_func_dno;

    pfree_ext(ds.data);

    if (valid_sql)
        check_sql_expr(expr->query, startlocation, strlen(sqlstart));

    if (issaveexpr) {
        MemoryContextSwitchTo(oldCxt);
    }

    return expr;
}

/*
 * read multiset grammer
 */
static void read_multiset(StringInfoData* ds, char* tableName1, Oid typeOid1)
{
    /*
     * table var name may be schema.pkg.table_var
     * so the name length should NAMEDATALEN with quotation marks *3
     */
    const int tablevar_namelen = NAMEDATALEN * 3 + 6;

    /* len = funcname + tablename1 + tablename2 */
    const int funclen = tablevar_namelen * 2 + 128;
    char funcName[funclen] = {0};
    int tok1 = yylex();
    errno_t rc = EOK;
    bool isTableVar = false;
    int tok_type = -1;

    switch (tok1)
    {
        case K_UNION:
            rc = snprintf_s(funcName, sizeof(funcName), sizeof(funcName) - 1, " array_union");
            securec_check_ss(rc, "", "");
            break;
        case K_INTERSECT:
            rc = snprintf_s(funcName, sizeof(funcName), sizeof(funcName) - 1, " array_intersect");
            securec_check_ss(rc, "", "");
            break;
        case K_EXCEPT:
            rc = snprintf_s(funcName, sizeof(funcName), sizeof(funcName) - 1, " array_except");
            securec_check_ss(rc, "", "");
            break;
        default:
            yyerror("unexpected keyword after multiset.");
    }

    int suffixlen = tablevar_namelen * 2 + 64;
    char suffix[suffixlen] = {0};
    int tok2 = yylex();
    int tok3;
    Oid typeOid2 = InvalidOid;
    char tableName2[tablevar_namelen] = {0};
    if (K_DISTINCT == tok2) {
        tok3 = yylex();
        tok_type = -1;
        if (tok3 == T_PACKAGE_VARIABLE) {
            get_datum_tok_type(yylval.wdatum.datum, &tok_type);
        }
        isTableVar = (tok3 == T_TABLE_VAR) || (tok_type == PLPGSQL_TOK_TABLE_VAR);
        if (isTableVar) {
            typeOid2 = get_table_type(yylval.wdatum.datum);
            copy_table_var_indents(tableName2, yylval.wdatum.ident, tablevar_namelen);
            rc = snprintf_s(suffix, sizeof(suffix), sizeof(suffix) - 1, "_distinct(\"%s\", \"%s\")", tableName1, tableName2);
            securec_check_ss(rc, "", "");
        } else {
            yyerror("unexpected keyword after distinct.");
        }
    } else if (K_ALL == tok2) {
        tok3 = yylex();
        tok_type = -1;
        if (tok3 == T_PACKAGE_VARIABLE) {
            get_datum_tok_type(yylval.wdatum.datum, &tok_type);
        }
        isTableVar = (tok3 == T_TABLE_VAR) || (tok_type == PLPGSQL_TOK_TABLE_VAR);
        if (isTableVar) {
            typeOid2 = get_table_type(yylval.wdatum.datum);
            copy_table_var_indents(tableName2, yylval.wdatum.ident, tablevar_namelen);
            rc = snprintf_s(suffix, sizeof(suffix), sizeof(suffix) - 1, "(\"%s\", \"%s\")", tableName1, tableName2);
            securec_check_ss(rc, "", "");
        } else {
            yyerror("unexpected keyword after all.");
        }
    } else {
        tok_type = -1;
        if (tok2 == T_PACKAGE_VARIABLE) {
            get_datum_tok_type(yylval.wdatum.datum, &tok_type);
        }
        isTableVar = (tok2 == T_TABLE_VAR) || (tok_type == PLPGSQL_TOK_TABLE_VAR);
        if (isTableVar) {
            typeOid2 = get_table_type(yylval.wdatum.datum);
            copy_table_var_indents(tableName2, yylval.wdatum.ident, tablevar_namelen);
            rc = snprintf_s(suffix, sizeof(suffix), sizeof(suffix) - 1, "(\"%s\", \"%s\")", tableName1, tableName2);
            securec_check_ss(rc, "", "");
        } else {
            yyerror("unexpected type after multiset.");
        }
    }

    if (typeOid1 != typeOid2) {
        ereport(errstate,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("%s and %s type are not match.", tableName1, tableName2)));
        u_sess->plsql_cxt.have_error = true;
    }
    strlcat(funcName, suffix, sizeof(funcName));
    appendStringInfoString(ds, funcName);
}

static bool copy_table_var_indents(char* tableName, char* idents, int tablevar_namelen)
{
    List* namelist = NIL;
    ListCell* l = NULL;
    errno_t rc = 0;
    StringInfoData	ds;
    initStringInfo(&ds);

    if (!SplitIdentifierString(idents, '.', &namelist))
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));

    if (namelist == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));

    foreach (l, namelist) {
        char* curname = (char*)lfirst(l);
        appendStringInfoString(&ds, curname);
        if (l->next != NULL) {
            appendStringInfoString(&ds, "\".\"");
        }
    }
    rc = snprintf_s(tableName, tablevar_namelen, tablevar_namelen - 1, "%s", ds.data);
    securec_check_ss(rc, "", "");
    pfree_ext(ds.data);
    list_free_ext(namelist);

    return true;
}

/*
 * Read a SQL construct and build a PLpgSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * trim:		trim trailing whitespace
 * startloc:	if not NULL, location of first token is stored at *startloc
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 */
static PLpgSQL_expr *
read_sql_construct(int until,
                   int until2,
                   int until3,
                   const char *expected,
                   const char *sqlstart,
                   bool isexpression,
                   bool valid_sql,
                   bool trim,
                   int *startloc,
                   int *endtoken)
{
    return read_sql_construct6(until, until2, until3, until3, 0, 0,
                               expected, sqlstart, isexpression, valid_sql, trim, startloc, endtoken);
}
/*
 * get function declare or definition string in package.
 */
static DefElem*
get_proc_str(int tok)
{
    int     blocklevel = 0;
    int     pre_tok = 0;
    DefElem *def;
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL) {
        yyerror("not allowed create procedure in function or procedure.");
    }
    tok = yylex();
    def = makeDefElem(NULL, NULL); /* make an empty DefElem */
    def->location = yylloc;
    StringInfoData      ds;  
    bool is_defining_proc = false;
    char * spec_proc_str = NULL;
    int loc = yylloc;
    int curloc = yylloc;
    MemoryContext oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    initStringInfo(&ds);
    MemoryContextSwitchTo(oldCxt);
    if (u_sess->parser_cxt.is_procedure==false){
        appendStringInfoString(&ds, " CREATE OR REPLACE FUNCTION ");
    } else {
        appendStringInfoString(&ds, " CREATE OR REPLACE PROCEDURE ");
    }
    def->begin_location = ds.len;
    u_sess->parser_cxt.in_package_function_compile = true;
    while(true)
    {    

        if (tok == YYEOF) {
            if (u_sess->parser_cxt.is_procedure==false){
                yyerror("function is not ended correctly");
                break;
            } else {
                yyerror("procedure is not ended correctly");
                break;
            }
        }

        if (tok == ';' && !is_defining_proc) 
        {
            break;
        }
        /* procedure or function can have multiple blocklevel*/
        if (tok == K_BEGIN) 
        {
            blocklevel++;
        }

        /* if have is or as,it means in body*/
        if ((tok == K_IS || tok == K_AS) && !is_defining_proc)
        {
            if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling) {
                yyerror("not allow define function or procedure in package specification");
                break;
            }
            is_defining_proc = true;
        }

        if (tok == K_END) 
        {
            tok = yylex();
            /* process end loop*/
            if (tok == K_LOOP || tok == K_WHILE || tok == K_REPEAT) {
                continue;
            }
            if (blocklevel == 1 && (pre_tok == ';' || pre_tok == K_BEGIN) && (tok == ';' || tok == 0))
            {
                curloc = yylloc;
                plpgsql_append_source_text(&ds, loc, curloc);
                break;
            }
            if (blocklevel > 1 && (pre_tok == ';' || pre_tok == K_BEGIN) && (tok == ';' || tok == 0))
            {
                blocklevel--;
            }
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && blocklevel == 1 && pre_tok == ';' && tok == T_WORD)
            {
                curloc = yylloc;
                plpgsql_append_source_text(&ds, loc, curloc);
                tok = yylex();
                break;
            }
        }
        pre_tok = tok;

        tok = yylex();
        curloc = yylloc;
        plpgsql_append_source_text(&ds, loc, curloc);

        loc = yylloc;
    }    
    u_sess->parser_cxt.in_package_function_compile = false;

    /* 
        To use the SQL Parser, we add a string to make SQL parser recognize. 
        Package specification use SQL parser parse the string too.
    */
    if (is_defining_proc == false) {
        appendStringInfoString(&ds, " AS BEGIN ");
        appendStringInfoString(&ds, " END;\n");
        u_sess->parser_cxt.isFunctionDeclare = true;
    } else {
        u_sess->parser_cxt.isFunctionDeclare = false;
    }

    def->defname = pstrdup(ds.data);
    spec_proc_str = pstrdup(ds.data);
    pfree_ext(ds.data);
    return def;
}
static char* get_init_proc(int tok) 
{
    int startlocation = yylloc;
    char* init_package_str = NULL;
    StringInfoData      ds;  
    initStringInfo(&ds);
    while(true)
    {  
        /* 
          package instantiation still need a token 'END' as end,
          but it's from package end, For details about how to instantiate
          a package, see the package instantiation grammer.
        */
        if (tok == K_BEGIN)
            startlocation = yylloc;
        if (tok == YYEOF) {
            yyerror("procedure is not ended correctly");
            break;
        }
        if (tok == K_END) 
        {
            tok = yylex();
            if (tok == ';' || tok == 0)
            {
                break;
            }
        }
        tok = yylex();
    } 
    /*
      package instantiation not need specify end.
    */
    plpgsql_append_source_text(&ds, startlocation, yylloc);
    init_package_str = pstrdup(ds.data);
    pfree_ext(ds.data);
    return init_package_str;
}

/* get assign attrname of arrayelement */
static char * get_attrname(int tok)
{
    if (tok == YYEMPTY || tok == '.') {
        tok = plpgsql_yylex_single();
    }
    plpgsql_location_to_lineno(yylloc);
    switch (tok) {
	case T_ARRAY_FIRST:
	case T_ARRAY_LAST:
	case T_ARRAY_COUNT:
	case T_ARRAY_EXTEND:
    case T_CWORD:/* composite name not OK */
        yyerror("syntax error");
        break;
    case T_REFCURSOR:
	case T_VARRAY:
	case T_VARRAY_VAR:
    case T_TABLE:
    case T_TABLE_VAR:
	case T_RECORD:
    case T_DATUM:
        if (yylval.wdatum.idents != NIL) { /* composite name not OK */
            yyerror("syntax error");
        } else {
            return yylval.wdatum.ident;
        }
        break;
	case T_WORD:
	    return yylval.word.ident;
	    break;
    default:
        if (plpgsql_is_token_keyword(tok)) {
            return pstrdup(yylval.keyword);
        } else {
            yyerror("missing or illegal attribute name");
        }
        break;
    }
    return NULL;
}

static PLpgSQL_type *
read_datatype(int tok)
{
    StringInfoData		ds;
    char			   *type_name;
    int					startlocation;
    PLpgSQL_type		*result = NULL;
    int					parenlevel = 0;
    List                *dtnames = NIL;

    /* Should only be called while parsing DECLARE sections */
    AssertEreport(u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_DECLARE,
                        MOD_PLSQL,
                        "Should only be called while parsing DECLARE sections");

    /* Often there will be a lookahead token, but if not, get one */
    if (tok == YYEMPTY)
        tok = yylex();

    startlocation = yylloc;

    /*
     * If we have a simple or composite identifier, check for %TYPE
     * and %ROWTYPE constructs.
     */
    bool iskeyword = plpgsql_is_token_keyword(tok);
    if (tok == T_WORD || tok == T_VARRAY_VAR || tok == T_TABLE_VAR || iskeyword)
    {
        char   *dtname;
        if (iskeyword) {
            dtname = pstrdup(yylval.keyword);
        } else if (tok == T_WORD) {
            dtname = yylval.word.ident;
        } else {
            dtname = yylval.wdatum.ident;
        }

        tok = yylex();
        if (tok == '%')
        {
            tok = yylex();
            if (tok_is_keyword(tok, &yylval,
                               K_TYPE, "type"))
            {
                result = plpgsql_parse_wordtype(dtname);
                if (result) {
                    if(iskeyword) {
                        pfree_ext(dtname);
                    }
                    return result;
                }
            }
            else if (tok_is_keyword(tok, &yylval,
                                    K_ROWTYPE, "rowtype"))
            {
                PLpgSQL_nsitem *ns;
                ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, 
                            dtname, NULL, NULL, NULL);
                if (ns && ns->itemtype == PLPGSQL_NSTYPE_VAR)
                {
                    PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                    if(var && var->datatype 
                           && var->datatype->typoid == REFCURSOROID)
                    {
#ifndef ENABLE_MULTIPLE_NODES
                        if (OidIsValid(var->datatype->cursorCompositeOid))
                            return build_type_from_cursor_var(var);
                        else
#endif
                            return plpgsql_build_datatype(RECORDOID, -1, InvalidOid);
                        
                    }
                } else if (ns && ns->itemtype == PLPGSQL_NSTYPE_ROW)
                {
                    PLpgSQL_row* row = (PLpgSQL_row*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                    if (row && row->rowtupdesc && row->rowtupdesc->tdtypeid != RECORDOID)
                    {
                        if (OidIsValid(row->rowtupdesc->tdtypeid))
                            return plpgsql_build_datatype(row->rowtupdesc->tdtypeid, -1, InvalidOid);
                    }
                }
                result = plpgsql_parse_wordrowtype(dtname);
                if (result) {
                    if(iskeyword) {
                        pfree_ext(dtname);
                    }
                    return result;
                }
            }
        }
        if(iskeyword) {
           pfree_ext(dtname);
        }
    }
    else if (tok == T_CWORD)
    {
        dtnames = yylval.cword.idents;

        tok = yylex();
        if (tok == '%')
        {
            tok = yylex();
            if (tok_is_keyword(tok, &yylval,
                               K_TYPE, "type"))
            {
                TypeDependExtend* typeDependExtend = NULL;
                if (enable_plpgsql_gsdependency()) {
                    InstanceTypeNameDependExtend(&typeDependExtend);
                }
                /* find val.col%TYPE first */
                HeapTuple tup = NULL;
                int collectionType = PLPGSQL_COLLECTION_NONE;
                Oid tableOfIndexType = InvalidOid;
                int32 typMod = -1;
                tup = FindRowVarColType(dtnames, &collectionType, &tableOfIndexType, &typMod, typeDependExtend);
                if (tup != NULL) {
                    Oid typOid = typeTypeId(tup);
                    ReleaseSysCache(tup);
                    PLpgSQL_type* type = plpgsql_build_datatype(typOid, typMod, InvalidOid, typeDependExtend);
                    if (OidIsValid(tableOfIndexType)) {
                        type->collectionType = collectionType;
                        type->tableOfIndexType = tableOfIndexType;
                    }
                    return type;
                }

                /* find pkg.var%TYPE second */
                PLpgSQL_datum* datum = GetPackageDatum(dtnames);
                if (datum != NULL) {
                    if (datum->dtype == PLPGSQL_DTYPE_VAR) {
                        PLpgSQL_var* var = (PLpgSQL_var*)datum;
                        Oid typOid =  var->datatype->typoid;
                        int32 typmod = var->datatype->atttypmod;
                        Oid collation = var->datatype->collation;
                        int collectionType = var->datatype->collectionType;
                        Oid tableOfIndexType = var->datatype->tableOfIndexType;
                        if (var->pkg != NULL && enable_plpgsql_gsdependency()) {
                            typeDependExtend->objectName = pstrdup(var->refname);
                            typeDependExtend->packageName = pstrdup(var->pkg->pkg_signature);
                            typeDependExtend->schemaName = get_namespace_name(var->pkg->namespaceOid);
                        }
                        PLpgSQL_type* type = plpgsql_build_datatype(typOid, typmod, collation, typeDependExtend);
                        type->collectionType = collectionType;
                        type->tableOfIndexType = tableOfIndexType;
                        return type;
                    } else if (datum->dtype == PLPGSQL_DTYPE_ROW){
                        PLpgSQL_row* row = (PLpgSQL_row*)datum;
                        if (row->rowtupdesc && row->rowtupdesc->tdtypeid != RECORDOID &&
                            OidIsValid(row->rowtupdesc->tdtypeid)) {
                            if (row->pkg != NULL && enable_plpgsql_gsdependency()) {
                                typeDependExtend->objectName = pstrdup(row->refname);
                                typeDependExtend->packageName = pstrdup(row->pkg->pkg_signature);
                                typeDependExtend->schemaName = get_namespace_name(row->pkg->namespaceOid);
                            }
                            return plpgsql_build_datatype(row->rowtupdesc->tdtypeid, -1, InvalidOid, typeDependExtend);
                        }
                    }
                }
                result = plpgsql_parse_cwordtype(dtnames, typeDependExtend);
                if (result)
                    return result;
                if (enable_plpgsql_undefined()) {
                    Oid tryUndefObjOid = gsplsql_try_build_exist_pkg_undef_var(dtnames);
                    if (OidIsValid(tryUndefObjOid)) {
                        typeDependExtend->undefDependObjOid = tryUndefObjOid;
                        typeDependExtend->dependUndefined = true;
                        return plpgsql_build_datatype(UNDEFINEDOID, -1, InvalidOid, typeDependExtend);
                    }
                }
            }
            else if (tok_is_keyword(tok, &yylval,
                                    K_ROWTYPE, "rowtype"))
            {
                PLpgSQL_datum* datum = GetPackageDatum(dtnames);
                if (datum != NULL && datum->dtype == PLPGSQL_DTYPE_ROW) {
                    PLpgSQL_row* row = (PLpgSQL_row*)datum;
                    if (row->rowtupdesc && row->rowtupdesc->tdtypeid != RECORDOID)
                    {
                        if (OidIsValid(row->rowtupdesc->tdtypeid))
                            return plpgsql_build_datatype(row->rowtupdesc->tdtypeid, -1, InvalidOid);
                    }
                }
                result = plpgsql_parse_cwordrowtype(dtnames);
                if (result)
                    return result;
            }
        }
    }

    while (tok != ';')
    {
        if (tok == 0)
        {
            if (parenlevel != 0)
                yyerror("mismatched parentheses", true);
            else
                yyerror("incomplete data type declaration");
            break;
        }
        /* Possible followers for datatype in a declaration */
        if (tok == K_COLLATE || tok == K_NOT ||
            tok == '=' || tok == COLON_EQUALS || tok == K_DEFAULT || tok == K_INDEX)
            break;
        /* Possible followers for datatype in a cursor_arg list */
        if ((tok == ',' || tok == ')') && parenlevel == 0)
            break;
        if (tok == '(')
            parenlevel++;
        else if (tok == ')')
            parenlevel--;

        tok = yylex();
    }

    /* set up ds to contain complete typename text */
    initStringInfo(&ds);
    plpgsql_append_source_text(&ds, startlocation, yylloc);
    type_name = ds.data;

    if (type_name[0] == '\0') {
#ifndef ENABLE_MULTIPLE_NODES
        if (u_sess->attr.attr_common.plsql_show_all_error) {
            const char* message = "missing data type declaration";
            InsertErrorMessage(message, yylloc);
        }
#endif
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("missing data type declaration"),
                parser_errposition(yylloc)));
        if (tok == ';') {
            plpgsql_push_back_token(tok);
        }
    } else {
        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
        result = parse_datatype(type_name, startlocation);

        pfree_ext(ds.data);

        plpgsql_push_back_token(tok);
    }
    
    return result;
}

static PLpgSQL_stmt *
make_execsql_stmt(int firsttoken, int location)
{
    StringInfoData		ds;
    IdentifierLookup	save_IdentifierLookup;
    PLpgSQL_stmt_execsql *execsql;
    PLpgSQL_expr		*expr;
    PLpgSQL_row			*row = NULL;
    PLpgSQL_rec			*rec = NULL;
    int					tok;
    int					prev_tok;
    int                                 prev_prev_tok = 0;
    int					curloc;
    bool				have_into = false;
    bool				have_bulk_collect = false;
    bool				have_strict = false;
    bool				array_begin = false;
    int					into_start_loc = -1;
    int					into_end_loc = -1;
    int					placeholders = 0;
    int					parenlevel = 0;
    List				*list_bracket = 0;		/* stack structure bracket tracker */
    List				*list_bracket_loc = 0;	/* location tracker */
    bool                                label_begin = false;
    initStringInfo(&ds);

    /* special lookup mode for identifiers within the SQL text */
    save_IdentifierLookup = u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup;
    u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

    /*
     * Scan to the end of the SQL command.  Identify any INTO-variables
     * clause lurking within it, and parse that via read_into_target().
     *
     * Because INTO is sometimes used in the main SQL grammar, we have to be
     * careful not to take any such usage of INTO as a PL/pgSQL INTO clause.
     * There are currently three such cases:
     *
     * 1. SELECT ... INTO.  We don't care, we just override that with the
     * PL/pgSQL definition.
     *
     * 2. INSERT INTO.  This is relatively easy to recognize since the words
     * must appear adjacently; but we can't assume INSERT starts the command,
     * because it can appear in CREATE RULE or WITH.  Unfortunately, INSERT is
     * *not* fully reserved, so that means there is a chance of a false match;
     * but it's not very likely.
     *
     * 3. ALTER TABLE PARTITION MERGE or SPLIT INTO.
     * We just check for ALTER as the command's first token.
     *
     * Fortunately, INTO is a fully reserved word in the main grammar, so
     * at least we need not worry about it appearing as an identifier.
     *
     * Any future additional uses of INTO in the main grammar will doubtless
     * break this logic again ... beware!
     */
    tok = firsttoken;

    /* For support InsertStmt:Insert into table_name values record_var */
    bool insert_stmt = false;
    bool prev_values = false;
    bool is_user_var = false;
    bool insert_record = false;
    bool insert_array_record = false; 
    int values_end_loc = -1;
    int before_semi_loc = -1;
    const char* err_msg = "The label name can only contain letters, digits and underscores";
    PLpgSQL_row* row_data = NULL;
    PLpgSQL_rec* rec_data = NULL;
    PLpgSQL_var* array_data = NULL;
    for (;;)
    {
        prev_tok = tok;
        tok = yylex();
        if (tok == COMMENTSTRING) {
            prev_prev_tok = prev_tok;
        }

        if (have_into && into_end_loc < 0)
            into_end_loc = yylloc;		/* token after the INTO part */

        /* Only support one record variable after insertstmt's values token */
        if (insert_record && tok != ';' && tok != ' ') {
            yyerror("unexpected record variable number in insert stmt.");
        }

        if (tok == ';') {
            before_semi_loc = yylloc;
            break;
        }

        if (tok == 0)
            yyerror("unexpected end of function definition");

        if(tok == T_PLACEHOLDER)
        {
            placeholders++;
        }

        if (tok == K_BULK) {
            /* only A_FORMAT can use bulk collect into */
            if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
                 yyerror("syntax error");
            }

            have_bulk_collect = true;
            into_start_loc = yylloc; /* save BULK COLLECT INTO starting point */
            tok = yylex();
            if (tok != K_COLLECT) {
                yyerror("expect \'COLLECT\' after \'BULK\'");
            }
            tok = yylex();
            if (tok != K_INTO) {
                yyerror("expect \'INTO\' after \'BULK COLLECT\'");
            }
        }

        if (tok == K_INTO)
        {
            if (prev_tok == K_INSERT || prev_tok == K_REPLACE || (prev_tok == COMMENTSTRING && (prev_prev_tok == K_INSERT || prev_prev_tok == K_REPLACE))) {
                insert_stmt = true;
                continue;	/* INSERT INTO is not an INTO-target */
            }
            if (firsttoken == K_ALTER)
                continue;	/* ALTER ... INTO is not an INTO-target */
            if (prev_tok == K_MERGE)
                continue;	/* MERGE INTO is not an INTO-target */
            if (have_into || is_user_var)
                yyerror("INTO specified more than once");
            have_into = true;
            if (!have_bulk_collect) {
                into_start_loc = yylloc;
            }
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
            is_user_var = read_into_target(&rec, &row, &have_strict, firsttoken, have_bulk_collect);
            if (is_user_var) {
                u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = save_IdentifierLookup;
                have_into = false;
            } else {
                u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
            }
        }

	/*
	 * When the database is in mysql compatible mode, 
	 * the loop syntax of mysql is compatible (label: loop)
	 */
        if (tok == ':' && prev_tok == T_WORD)
        {
            StringInfoData  lb;
            initStringInfo(&lb);
            int  lb_end = yylloc;
            int  tok1 = yylex();
            if(tok1 == K_LOOP || tok1 == K_WHILE || tok1 == K_REPEAT)
            {
                if(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
                {
                    int  count = 0;
                    label_begin =  true;
                    plpgsql_push_back_token(tok1);
                    plpgsql_push_back_token(tok);
                    plpgsql_append_source_text(&lb, location, lb_end);

                    for(int i = lb.len-1; i > 0; i--)
                    {
                        if(lb.data[i] == ' ')
                        {
                            count++;
                        }
                        else
                            break;
                    }
                    if(count > 0 && lb.len-count > 0)
                    {
                        char*  name = NULL;
                        errno_t rc = 0;
                        int num = -1;

                        int len = Min(NAMEDATALEN, lb.len - count + 1);
                        name = (char*)palloc(len);
                        rc = strncpy_s(name, len, lb.data, len - 1);
                        securec_check_c(rc, "\0", "\0");
                        num = strspn(pg_strtolower(name), "abcdefghijklmnopqrstuvwxyz0123456789_");

                        if(num != len - 1 || (name[0] >= '0' && name[0] <= '9')) {
                            pfree(name);
                            pfree_ext(lb.data);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg(err_msg),
                                    parser_errposition(location + num)));
                        }

                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(name));
                        pfree(name);
                    }
                    else {
                        int valid_len = lb.len;
                        if(lb.len >= NAMEDATALEN)
                        {
                            lb.data[NAMEDATALEN - 1] = '\0';
                            valid_len = NAMEDATALEN - 1;
                        }
                        int len = -1;
                        len = strspn(pg_strtolower(lb.data), "abcdefghijklmnopqrstuvwxyz0123456789_");

                        if(len != valid_len) {
                            pfree_ext(lb.data);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg(err_msg),
                                    parser_errposition(location + len)));
                        }
                        if(lb.data[0] >= '0' && lb.data[0] <= '9') {
                            pfree_ext(lb.data);
                            ereport(errstate,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg(err_msg),
                                    parser_errposition(location)));
                        }
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(lb.data));
                    }
                    pfree_ext(lb.data);
                    break;
                }
                else
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("'label:' is only supported in database which dbcompatibility='B'.")));
            }
        }

        if ((tok == T_LABELLOOP || tok == T_LABELWHILE || tok == T_LABELREPEAT) && prev_tok == T_WORD)
        {
            StringInfoData  lb;
            initStringInfo(&lb);
            int  lb_end = yylloc;
            if(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
            {
                int  count = 0;
                label_begin =  true;
                plpgsql_push_back_token(tok);
                plpgsql_append_source_text(&lb, location, lb_end);

                for(int i = lb.len-1; i > 0; i--)
                {
                    if(lb.data[i] == ' ')
                    {
                        count++;
                    }
                    else
                        break;
                }
                if(count > 0 && lb.len-count > 0)
                {
                    char*  name = NULL;
                    errno_t rc = 0;
                    int len = -1;

                    name = (char*)palloc(lb.len-count+1);
                    rc = strncpy_s(name, lb.len-count+1, lb.data, lb.len-count);
                    securec_check_c(rc, "\0", "\0");
                    len = strspn(pg_strtolower(name), "abcdefghijklmnopqrstuvwxyz0123456789_");

                    if(len != lb.len - count || (name[0] >= '0' && name[0] <= '9')) {
                        pfree(name);
                        pfree_ext(lb.data);
                        ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg(err_msg),
                                parser_errposition(location + len)));
                    }

                    if(lb.len-count >= NAMEDATALEN)
                    {
                        char*   namedata = NULL;
                        errno_t rc = 0;
                        namedata = (char*)palloc(NAMEDATALEN);
                        rc = strncpy_s(namedata, NAMEDATALEN, name, NAMEDATALEN-1);
                        securec_check_c(rc, "\0", "\0");
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(namedata));
                        pfree(namedata);
                    }
                    else
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(name));
                    pfree(name);
                }
                else
                {
                    int len = -1;
                    len = strspn(pg_strtolower(lb.data), "abcdefghijklmnopqrstuvwxyz0123456789_");

                    if(len != lb.len) {
                        pfree_ext(lb.data);
                        ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg(err_msg),
                                parser_errposition(location + len)));
                    }
                    if(lb.data[0] >= '0' && lb.data[0] <= '9') {
                        pfree_ext(lb.data);
                        ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg(err_msg),
                                parser_errposition(location)));
                    }
                    if(lb.len >= NAMEDATALEN)
                    {
                        char*   namedata = NULL;
                        errno_t rc = 0;
                        namedata = (char*)palloc(NAMEDATALEN);
                        rc = strncpy_s(namedata, NAMEDATALEN, lb.data, NAMEDATALEN-1);
                        securec_check_c(rc, "\0", "\0");
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(namedata));
                        pfree(namedata);
                    }
                    else
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, pg_strtolower(lb.data));
                }
                pfree_ext(lb.data);
                break;
            }
            else
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("'label:' is only supported in database which dbcompatibility='B'.")));
        }

        if (tok == T_CWORD && prev_tok!=K_SELECT 
                           && prev_tok!= K_PERFORM) {
            List  *dtname = yylval.cword.idents;
            tok = yylex();
            if (tok == '(') {
                plpgsql_push_back_token(tok);
                continue;
            }
            plpgsql_push_back_token(tok);
            plpgsql_pkg_add_unknown_var_to_namespace(dtname);
            continue;
        }

        /*
         * A_FORMAT VARRAY related handling
         * To make parentheses-based varray access compatible with current
         * PL/pgSQL array, we need to replace all parentheses with brackets.
         * Like what we did in read_sql_contruct5(), we interpret the input
         * query with array states.
         *
         * When array_begin is set to true, it means we have an array comming
         * in next iteration, we need to append a bracket instead of parentheses.
         * Or, array_begin is set to false that the query statement will remain
         * unchanged.
         */
        if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
            continue;
        }


        /* Find 'values' position for InsertStmt */
        if (insert_stmt && tok == T_WORD) {
            if (pg_strcasecmp(yylval.word.ident, "values") == 0) {
                tok = yylex();
                if (tok == '(') {
                    prev_values = false;
                } else {
                    prev_values = true;
                }
                plpgsql_push_back_token(tok);
                continue;
            }
        }

        /* Check the variable type after 'values' */
        if (insert_stmt && prev_values && (tok == T_WORD || tok == T_VARRAY_VAR || tok == T_TABLE_VAR)) {
            if (tok == T_WORD) {
                PLpgSQL_nsitem* ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, yylval.word.ident, NULL, NULL, NULL);
                if (ns == NULL) {
                    yyerror("insert an nonexistent variable.");
                    continue;
                }

                PLpgSQL_datum* datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                insert_record = true;
                prev_values = false;
                values_end_loc = yylloc;
                if (PLPGSQL_DTYPE_RECORD == datum->dtype 
                        || PLPGSQL_DTYPE_ROW == datum->dtype) {
                    row_data = (PLpgSQL_row*)datum;
                    continue;
                } else if (PLPGSQL_DTYPE_REC == datum->dtype) {
                    rec_data = (PLpgSQL_rec*)datum;
                    if (rec_data->tupdesc == NULL) {
                        yyerror("unsupported insert into table from record type without desc, "
                                "may need set behavior_compat_options to allow_procedure_compile_check.");
                    }
                    continue;
                }

                yyerror("unsupported insert into table from non record type.", true);
            } else if (tok == T_VARRAY_VAR || tok == T_TABLE_VAR) {
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR) {
                    array_data = (PLpgSQL_var *) yylval.wdatum.datum;
                    insert_array_record = true;
                    prev_values = false;
                    values_end_loc = yylloc;
                    continue;
                }
                yyerror("unsupported insert into table from non record type.");
            } else {
                yyerror("unsupported insert into table from non record type.");
            }
        }

        if (tok == '(' || tok == '[')
            parenlevel++;
        else if (tok == ')' || tok == ']')
        {
            parenlevel--;
            if (parenlevel < 0)
                yyerror("mismatched parentheses", true);
        }
        switch(tok) {
            case T_VARRAY_VAR:
                curloc = yylloc;    /* always save current location before yylex() */
                tok = yylex();
                if (tok == '(' || tok == '[')
                    array_begin = true;
                plpgsql_push_back_token(tok);
                break;
            case T_TABLE_VAR:
                curloc = yylloc;    /* always save current location before yylex() */
                tok = yylex();
                if (tok == '(')
                    array_begin = true;
                else if (tok == '[') 
                    yyerror("syntax error");

                plpgsql_push_back_token(tok);
                break;
            case ']':
            case ')':
                if (list_bracket && linitial_int(list_bracket) == parenlevel) {
                    prev_tok = tok;
                    curloc = yylloc;    /* always save current location before yylex() */
                    tok = yylex();
                    if (tok == '(' || tok == '[' || tok == '.') {
                        array_begin = true; /* N-D array access */
                    }
                    plpgsql_push_back_token(tok);

                    /* stack pop */
                    list_bracket = list_delete_first(list_bracket);
                    if (prev_tok == ')') {
                        /* append location of right bracket */
                        list_bracket_loc = lappend_int(list_bracket_loc, curloc - location);
                    }
                }
                break;
            case '[':
            case '(':
                if (array_begin) {
                    /* stack push */
                    list_bracket = lcons_int(parenlevel - 1, list_bracket);
                    if (tok == '(') {
                        /* cancat location of left bracket */
                        list_bracket_loc = lcons_int(yylloc - location, list_bracket_loc);
                    }
                    array_begin = false;
                }
                break;
            case T_WORD:
            case T_CWORD:
                if (array_begin) {
                    curloc = yylloc;    /* always save current location before yylex() */
                    tok = yylex();
                    if (tok != '(' && tok != '[') {
                        array_begin = false;
                    }
                    plpgsql_push_back_token(tok);
                }
                break;
            case '.':
                if (array_begin) {
                    curloc = yylloc;    /* always save current location before yylex() */
                    tok = yylex();
                    if (tok != T_WORD && tok != T_CWORD) {
                        array_begin = false;
                    }
                    plpgsql_push_back_token(tok);
                }
                break;
            default:
                /* do nothing */
                break;
        }

    }

    u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = save_IdentifierLookup;

    if (have_into)
    {
        /*
         * Insert an appropriate number of spaces corresponding to the
         * INTO text, so that locations within the redacted SQL statement
         * still line up with those in the original source text.
         */
        plpgsql_append_source_text(&ds, location, into_start_loc);
        appendStringInfoSpaces(&ds, into_end_loc - into_start_loc);
        plpgsql_append_source_text(&ds, into_end_loc, yylloc);

        if (parenlevel != 0 ||
            list_length(list_bracket) != 0 ||
            list_length(list_bracket_loc) % 2)
        {
            yyerror("mismatched parentheses");
        }
        plpgsql_process_stmt_array(&ds, list_bracket_loc);
        list_free_ext(list_bracket_loc);
    } else if (insert_record) {
        /* Support grammar 'Insert into table_name values record_type' in Procedure.
         * because SQL Parser does's support that, so record type will be broken into
         * record variable's sub attribute variable for 'Insert into table_name 
         * values(var,...).
         */
        plpgsql_append_source_text(&ds, location, values_end_loc);
        appendStringInfoString(&ds, "(");
        if (row_data != NULL) {
        int nfields = row_data->nfields;
            for (int i = 0; i < nfields; i++) {
                appendStringInfo(&ds, "%s.%s", row_data->refname, row_data->fieldnames[i]);
                if (i != (nfields - 1)) {
                    appendStringInfoString(&ds, ",");
                }
            }
        } else {
            int nattr = rec_data->tupdesc->natts;
            for (int i = 0; i < nattr; i++) {
               Form_pg_attribute pg_att_form = TupleDescAttr(rec_data->tupdesc, i);
               appendStringInfo(&ds, "%s.%s", rec_data->refname, NameStr(pg_att_form->attname));
               if (i != (nattr - 1)) {
                   appendStringInfoString(&ds, ",");
               }
            }
        }
        appendStringInfoString(&ds, ")");
    } else if (insert_array_record) {
        /* Support grammar 'Insert into table_name values array[][]...' in Procedure.
         * because SQL Parser does's support that, so record type will be broken into
         * record variable's sub attribute variable for 'Insert into table_name 
         * values(array[][].var,...).
         */
        Oid base_type_oid = get_element_type(array_data->datatype->typoid);
        if (!OidIsValid(base_type_oid)) {
           yyerror("invalid array's element type for insert.");
        }

        HeapTuple type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(base_type_oid));     
        if (!HeapTupleIsValid(type_tuple)) {
            yyerror("invalid array's element type tuple for insert.");
        }

        Form_pg_type type_pg_type = (Form_pg_type)GETSTRUCT(type_tuple);
        int rel_oid = type_pg_type->typrelid;
        ReleaseSysCache(type_tuple);
        HeapTuple rel_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));
        if (!HeapTupleIsValid(rel_tuple)) {
            yyerror("invalid type's rel tuple for insert.", true);
        } else {

            plpgsql_append_source_text(&ds, location, values_end_loc);
            appendStringInfoString(&ds, "(");
            int replace_start_poc = ds.len;
            Form_pg_class rel_pg_class = (Form_pg_class) GETSTRUCT(rel_tuple);
            int att_num = rel_pg_class->relnatts;
            ReleaseSysCache(rel_tuple);

            for (int i = 1; i <= att_num; i++) {
                HeapTuple att_tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(rel_oid), Int16GetDatum(i));
                if (!HeapTupleIsValid(att_tuple)) {
                    yyerror("invalid element att tuple for insert.");
                }

                Form_pg_attribute att_pg_attribute = (Form_pg_attribute)GETSTRUCT(att_tuple);
                plpgsql_append_source_text(&ds, values_end_loc, before_semi_loc);
                appendStringInfo(&ds, ".%s", att_pg_attribute->attname.data);
                if (i != att_num) {
                    appendStringInfoString(&ds, ",");
                }

                ReleaseSysCache(att_tuple);
            }
            int replace_end_poc = ds.len - 1;
            appendStringInfoString(&ds, ")");

            int bracket_level = 0;
            /* replace table/array's '('/')' to '['/']' SQL supported */ 
            for (int i = replace_start_poc; i <= replace_end_poc; i++) {
                if (ds.data[i] == '(') {
                    bracket_level++;
                    if (bracket_level == 1) {
                        ds.data[i] = '[';
                    }
                } else if (ds.data[i] == ')') {
                    if (bracket_level == 1) {
                        ds.data[i] = ']';
                    }
                    bracket_level--;
                }
            }
        }
    } else if (label_begin)
    {
        appendStringInfoString(&ds, "\n");
    } else {
        plpgsql_append_source_text(&ds, location, yylloc);

        if (parenlevel != 0 ||
            list_length(list_bracket) != 0 ||
            list_length(list_bracket_loc) % 2)
        {
            yyerror("mismatched parentheses");
        }
        plpgsql_process_stmt_array(&ds, list_bracket_loc);
        list_free_ext(list_bracket_loc);
    }

    /* trim any trailing whitespace, for neatness */
    while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
        ds.data[--ds.len] = '\0';

    expr = (PLpgSQL_expr *)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(ds.data);
    expr->plan			= NULL;
    expr->paramnos		= NULL;
    expr->ns			= plpgsql_ns_top();
    expr->idx			= (uint32)-1;
    expr->out_param_dno = -1;
    pfree_ext(ds.data);

    check_sql_expr(expr->query, location, 0);
#ifndef ENABLE_MULTIPLE_NODES
    if (firsttoken == K_SELECT && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && ALLOW_PROCEDURE_COMPILE_CHECK) {
        (void)getCursorTupleDesc(expr, false, true);
    }
#endif
    execsql = (PLpgSQL_stmt_execsql *)palloc(sizeof(PLpgSQL_stmt_execsql));
    execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
    execsql->lineno  = plpgsql_location_to_lineno(location);
    execsql->sqlstmt = expr;
    execsql->into	 = have_into;
    execsql->bulk_collect = have_bulk_collect;
    execsql->strict	 = have_strict;
    execsql->rec	 = rec;
    execsql->row	 = row;
    execsql->placeholders = placeholders;
    execsql->sqlString = plpgsql_get_curline_query();

    return (PLpgSQL_stmt *) execsql;
}


/*
 * Read FETCH or MOVE direction clause (everything through FROM/IN).
 */
static PLpgSQL_stmt_fetch *
read_fetch_direction(void)
{
    PLpgSQL_stmt_fetch *fetch;
    int			tok;
    bool		check_FROM = true;

    /*
     * We create the PLpgSQL_stmt_fetch struct here, but only fill in
     * the fields arising from the optional direction clause
     */
    fetch = (PLpgSQL_stmt_fetch *) palloc0(sizeof(PLpgSQL_stmt_fetch));
    fetch->cmd_type = PLPGSQL_STMT_FETCH;
    /* set direction defaults: */
    fetch->direction = FETCH_FORWARD;
    fetch->how_many  = 1;
    fetch->expr		 = NULL;
    fetch->returns_multiple_rows = false;
    fetch->bulk_collect = false;
    fetch->has_direction = true;
    fetch->sqlString = plpgsql_get_curline_query();

    tok = yylex();
    if (tok == 0)
        yyerror("unexpected end of function definition");

    if (tok_is_keyword(tok, &yylval,
                       K_NEXT, "next"))
    {
        /* use defaults */
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_PRIOR, "prior"))
    {
        fetch->direction = FETCH_BACKWARD;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_FIRST, "first"))
    {
        fetch->direction = FETCH_ABSOLUTE;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_LAST, "last"))
    {
        fetch->direction = FETCH_ABSOLUTE;
        fetch->how_many  = -1;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_ABSOLUTE, "absolute"))
    {
        fetch->direction = FETCH_ABSOLUTE;
        fetch->expr = read_sql_expression2(K_FROM, K_IN,
                                           "FROM or IN",
                                           NULL);
        check_FROM = false;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_RELATIVE, "relative"))
    {
        fetch->direction = FETCH_RELATIVE;
        fetch->expr = read_sql_expression2(K_FROM, K_IN,
                                           "FROM or IN",
                                           NULL);
        check_FROM = false;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_ALL, "all"))
    {
        fetch->how_many = FETCH_ALL;
        fetch->returns_multiple_rows = true;
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_FORWARD, "forward"))
    {
        complete_direction(fetch, &check_FROM);
    }
    else if (tok_is_keyword(tok, &yylval,
                            K_BACKWARD, "backward"))
    {
        fetch->direction = FETCH_BACKWARD;
        complete_direction(fetch, &check_FROM);
    }
    else if (tok == K_FROM || tok == K_IN)
    {
        /* empty direction */
        check_FROM = false;
    }
    else if (tok == T_DATUM || tok == T_PACKAGE_VARIABLE)
    {
        /* Assume there's no direction clause and tok is a cursor name */
        plpgsql_push_back_token(tok);
        fetch->has_direction = false;
        check_FROM = false;
    }
    else
    {
        /*
         * Assume it's a count expression with no preceding keyword.
         * Note: we allow this syntax because core SQL does, but we don't
         * document it because of the ambiguity with the omitted-direction
         * case.  For instance, "MOVE n IN c" will fail if n is a variable.
         * Perhaps this can be improved someday, but it's hardly worth a
         * lot of work.
         */
        plpgsql_push_back_token(tok);
        fetch->expr = read_sql_expression2(K_FROM, K_IN,
                                           "FROM or IN",
                                           NULL);
        fetch->returns_multiple_rows = true;
        check_FROM = false;
    }

    /* check FROM or IN keyword after direction's specification */
    if (check_FROM)
    {
        tok = yylex();
        if (tok != K_FROM && tok != K_IN)
            yyerror("expected FROM or IN");
    }

    return fetch;
}

/*
 * Process remainder of FETCH/MOVE direction after FORWARD or BACKWARD.
 * Allows these cases:
 *   FORWARD expr,  FORWARD ALL,  FORWARD
 *   BACKWARD expr, BACKWARD ALL, BACKWARD
 */
static void
complete_direction(PLpgSQL_stmt_fetch *fetch,  bool *check_FROM)
{
    int			tok;

    tok = yylex();
    if (tok == 0)
        yyerror("unexpected end of function definition");

    if (tok == K_FROM || tok == K_IN)
    {
        *check_FROM = false;
        return;
    }

    if (tok == K_ALL)
    {
        fetch->how_many = FETCH_ALL;
        fetch->returns_multiple_rows = true;
        *check_FROM = true;
        return;
    }

    plpgsql_push_back_token(tok);
    fetch->expr = read_sql_expression2(K_FROM, K_IN,
                                       "FROM or IN",
                                       NULL);
    fetch->returns_multiple_rows = true;
    *check_FROM = false;
}


static PLpgSQL_stmt *
make_return_stmt(int location)
{
    PLpgSQL_stmt_return *newp;
    int token = -1;

    newp = (PLpgSQL_stmt_return *)palloc0(sizeof(PLpgSQL_stmt_return));
    newp->cmd_type = PLPGSQL_STMT_RETURN;
    newp->lineno   = plpgsql_location_to_lineno(location);
    newp->expr	  = NULL;
    newp->retvarno = -1;
    newp->sqlString = plpgsql_get_curline_query();

    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_retset)
    {
        if (yylex() != ';') {
            const char* message = "RETURN cannot have a parameter in function returning set";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("RETURN cannot have a parameter in function returning set"),
                     errhint("Use RETURN NEXT or RETURN QUERY."),
                     parser_errposition(yylloc)));
        }
    }

    // adapting A db, where return value is independent from OUT parameters 
    else if (';' == (token = yylex()) && u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->out_param_varno >= 0)
        newp->retvarno = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->out_param_varno;
    else
    {
        plpgsql_push_back_token(token);
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_rettype == VOIDOID)
        {
            if (yylex() != ';') {
                const char* message = "RETURN cannot have a parameter in function returning void";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("RETURN cannot have a parameter in function returning void"),
                         parser_errposition(yylloc)));
            }
        }
        else if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_retistuple)
        {
            int     tok = yylex();
            if(tok < 0)
            {
                const char* message = "token value is smaller than 0 ";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("token value %d is smaller than 0 ", tok)));
                return NULL;
            }

            switch (tok)
            {
                case K_NULL:
                    /* we allow this to support RETURN NULL in triggers */
                    break;

                case T_DATUM:
                    if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                        yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC ||
                        yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_RECORD)
                        newp->retvarno = yylval.wdatum.dno;
                    else {
                        const char* message = "RETURN must specify a record or row variable in function returning row";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                                (errcode(ERRCODE_DATATYPE_MISMATCH),
                                 errmsg("RETURN must specify a record or row variable in function returning row"),
                                 parser_errposition(yylloc)));
                    }
                    break;

                default:
                    const char* message = "RETURN must specify a record or row variable in function returning row";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("RETURN must specify a record or row variable in function returning row"),
                             parser_errposition(yylloc)));
                    break;
            }
            if (yylex() != ';')
            yyerror("syntax error");
        }
        else
        {
            /*
             * Note that a well-formed expression is _required_ here;
             * anything else is a compile-time error.
             */
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                newp->retvarno = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->out_param_varno;
            }
            newp->expr = read_sql_expression(';', ";");
            if (newp->expr->is_have_tableof_index_var) {
                yyerror("table of index type is not supported as function return type.");
            }
        }
    }

    return (PLpgSQL_stmt *) newp;
}


static PLpgSQL_stmt *
make_return_next_stmt(int location)
{
    PLpgSQL_stmt_return_next *newp;

    if (!u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_retset) {
        const char* message = "cannot use RETURN NEXT in a non-SETOF function";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cannot use RETURN NEXT in a non-SETOF function"),
                 parser_errposition(location)));
    }

    newp = (PLpgSQL_stmt_return_next *)palloc0(sizeof(PLpgSQL_stmt_return_next));
    newp->cmd_type	= PLPGSQL_STMT_RETURN_NEXT;
    newp->lineno		= plpgsql_location_to_lineno(location);
    newp->expr		= NULL;
    newp->retvarno	= -1;
    newp->sqlString = plpgsql_get_curline_query();

    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->out_param_varno >= 0)
    {
        if (yylex() != ';') {
            const char* message = "RETURN NEXT cannot have a parameter in function with OUT parameters";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("RETURN NEXT cannot have a parameter in function with OUT parameters"),
                     parser_errposition(yylloc)));
        }
        newp->retvarno = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->out_param_varno;
    }
    else if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_retistuple)
    {
        switch (yylex())
        {
            case T_DATUM:
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_RECORD ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
                    newp->retvarno = yylval.wdatum.dno;
                else {
                    const char* message = "RETURN NEXT must specify a record or row variable in function returning row";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("RETURN NEXT must specify a record or row variable in function returning row"),
                             parser_errposition(yylloc)));
                }
                break;

            default:
                const char* message = "RETURN NEXT must specify a record or row variable in function returning row";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("RETURN NEXT must specify a record or row variable in function returning row"),
                         parser_errposition(yylloc)));
                break;
        }
        if (yylex() != ';')
            yyerror("syntax error");
    }
    else
        newp->expr = read_sql_expression(';', ";");

    return (PLpgSQL_stmt *) newp;
}


static PLpgSQL_stmt *
make_return_query_stmt(int location)
{
    PLpgSQL_stmt_return_query *newp;
    int			tok;

    if (!u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_retset) {
        const char* message = "cannot use RETURN QUERY in a non-SETOF function";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cannot use RETURN QUERY in a non-SETOF function"),
                 parser_errposition(location)));
    }

    newp = (PLpgSQL_stmt_return_query *)palloc0(sizeof(PLpgSQL_stmt_return_query));
    newp->cmd_type = PLPGSQL_STMT_RETURN_QUERY;
    newp->lineno = plpgsql_location_to_lineno(location);
    newp->sqlString = plpgsql_get_curline_query();

    /* check for RETURN QUERY EXECUTE */
    if ((tok = yylex()) != K_EXECUTE)
    {
        /* ordinary static query */
        plpgsql_push_back_token(tok);
        newp->query = read_sql_stmt("");
    }
    else
    {
        /* dynamic SQL */
        int		term;

        newp->dynquery = read_sql_expression2(';', K_USING, "; or USING",
                                             &term);
        if (term == K_USING)
        {
            do
            {
                PLpgSQL_expr *expr;

                expr = read_sql_expression2(',', ';', ", or ;", &term);
                newp->params = lappend(newp->params, expr);
            } while (term == ',');
        }
    }

    return (PLpgSQL_stmt *) newp;
}


/* convenience routine to fetch the name of a T_DATUM */
static char *
NameOfDatum(PLwdatum *wdatum)
{
    if (wdatum->ident)
        return wdatum->ident;
    AssertEreport(wdatum->idents != NIL,
                        MOD_PLSQL,
                        "It should not be null");
    return NameListToString(wdatum->idents);
}

/* convenience routine to copy the name of a T_DATUM */
static char *
CopyNameOfDatum(PLwdatum *wdatum)
{
    if (wdatum->ident)
        return pstrdup(wdatum->ident);
    AssertEreport(wdatum->idents != NIL,
                        MOD_PLSQL,
                        "It should not be null");
    return NameListToString(wdatum->idents);
}

static void check_record_nest_tableof_index(PLpgSQL_datum* datum)
{
    if (datum->dtype == PLPGSQL_DTYPE_RECORD || datum->dtype == PLPGSQL_DTYPE_ROW) {
        PLpgSQL_row* row = (PLpgSQL_row*)datum;
        for (int i = 0; i < row->nfields; i++) {
            PLpgSQL_datum* row_element = NULL; 

            /* check wether attisdropped */
            if (row->varnos[i] == -1 && row->fieldnames[i] == NULL) {
                continue;
            }

            if (row->ispkg) {
                row_element = (PLpgSQL_datum*)(row->pkg->datums[row->varnos[i]]);
            } else {
                row_element = (PLpgSQL_datum*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[row->varnos[i]]);
            }
            /* Notice: do not deal record nest record nest table of index, because table of index type can not get */
            if (row_element->dtype == PLPGSQL_DTYPE_VAR) {
                PLpgSQL_var* var_element = (PLpgSQL_var*)row_element;
                if (OidIsValid(var_element->datatype->tableOfIndexType)) {
                    yyerror("record nested table of index variable do not support entire assign");
                }
            } 
        }
    }
}

static void
check_assignable(PLpgSQL_datum *datum, int location)
{
    PLpgSQL_var *var = NULL;
    switch (datum->dtype)
    {
        case PLPGSQL_DTYPE_VAR:
            if (((PLpgSQL_var *) datum)->isconst) {
                const char* message = "variable is declared CONSTANT";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
                         errmsg("\"%s\" is declared CONSTANT",
                                ((PLpgSQL_var *) datum)->refname),
                         parser_errposition(location)));
            }
            break;
        case PLPGSQL_DTYPE_ROW:
            /* always assignable? */
            break;
        case PLPGSQL_DTYPE_REC:
            /* always assignable?  What about NEW/OLD? */
            break;
        case PLPGSQL_DTYPE_RECORD:
            break;
        case PLPGSQL_DTYPE_RECFIELD:
            /* always assignable? */
            break;
        case PLPGSQL_DTYPE_ARRAYELEM:
            var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[((PLpgSQL_arrayelem *)datum)->arrayparentno];
            if (var->isconst) {
                const char* message = "array is declared CONSTANT";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate, (errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
                                errmsg("\"%s\" is declared CONSTANT", var->refname),
                                parser_errposition(location)));
                u_sess->plsql_cxt.have_error = true;
            }
            break;
        case PLPGSQL_DTYPE_TABLEELEM:
            var = (PLpgSQL_var *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[((PLpgSQL_tableelem *)datum)->tableparentno];
            if (var->isconst) {
                const char* message = "table is declared CONSTANT";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate, (errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
                                   errmsg("\"%s\" is declared CONSTANT", var->refname),
                                   parser_errposition(location)));
                u_sess->plsql_cxt.have_error = true;
            }
            break;
        case PLPGSQL_DTYPE_UNKNOWN:
            /* package variable? */
            break;
        default:
            elog(errstate, "unrecognized dtype: %d", datum->dtype);
            break;
    }
}

static Oid get_table_index_type(PLpgSQL_datum* datum, int *tableof_func_dno)
{
    PLpgSQL_var* var = (PLpgSQL_var*)datum;
    if (OidIsValid(var->datatype->tableOfIndexType) && tableof_func_dno != NULL) {
        if (*tableof_func_dno == -1) {
            *tableof_func_dno = var->dno;
        } else if (*tableof_func_dno != var->dno) {
            yyerror("do not support more than 2 table of index by variables call functions in an expr");
        }
    }
   
    return var->datatype->tableOfIndexType;
}

static void check_bulk_into_type(PLpgSQL_row* row)
{
    if (row->nfields == 0) {
        char errormsg[128] = {0};
        errno_t rc = EOK;
        rc = snprintf_s(errormsg, sizeof(errormsg), sizeof(errormsg) - 1,
            "bulk collect into target can't be null.");
        securec_check_ss(rc, "", "");
        yyerror(errormsg);
    }

    for (int i = 0; i < row->nfields; i++) {
        PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[row->varnos[i]];
        if (var->datatype != NULL && var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE && var->datatype->tableOfIndexType == VARCHAROID) {
            char errormsg[128] = {0};
            errno_t rc = EOK;
            rc = snprintf_s(errormsg, sizeof(errormsg), sizeof(errormsg) - 1,
                "index by varchar type %s don't support bulk collect into.", var->refname);
            securec_check_ss(rc, "", "");
            yyerror(errormsg);
        }
    }
}

static void check_tableofindex_args(int tableof_var_dno, Oid argtype)
{
    if (tableof_var_dno < 0 || u_sess->plsql_cxt.curr_compile_context == NULL) {
        return ;
    }
    PLpgSQL_datum* tableof_var_datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[tableof_var_dno];
    if (tableof_var_datum == NULL) {
        return ;
    } else if (tableof_var_datum->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* var = (PLpgSQL_var*)tableof_var_datum;
        Oid base_oid = InvalidOid;
        Oid indexby_oid = InvalidOid;
        
        if (isTableofType(argtype, &base_oid, &indexby_oid)) {
            if (var->datatype->tableOfIndexType != indexby_oid ||
                var->datatype->typoid != base_oid) {
                yyerror("procedure table of arg types not match");
            }
        }
    } else if (tableof_var_datum->dtype == PLPGSQL_DTYPE_RECORD) {
        check_record_nest_tableof_index(tableof_var_datum);
    }  
}

static void check_table_index(PLpgSQL_datum* datum, char* funcName)
{
    PLpgSQL_var* var = (PLpgSQL_var*)datum;
    if ((var->datatype->tableOfIndexType == VARCHAROID || var->datatype->tableOfIndexType == INT4OID) &&
        var->nest_table == NULL) {
        char errormsg[128] = {0};
        errno_t rc = EOK;
        rc = snprintf_s(errormsg, sizeof(errormsg), sizeof(errormsg) - 1,
            "index by type don't support %s function", funcName);
        securec_check_ss(rc, "", "");
        yyerror(errormsg);
    }
}
/*
 * check if the table type has index by,
 * return real type
 */
static Oid get_table_type(PLpgSQL_datum* datum)
{
    PLpgSQL_var* var = (PLpgSQL_var*)datum;
    if (OidIsValid(var->datatype->tableOfIndexType)) {
        yyerror("multiset don't support index by table of type.");
    }
    Oid typeOid = var->datatype->typoid;
    if (!OidIsValid(typeOid)) {
        ereport(errstate,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("type %s type Oid is invalid.", var->datatype->typname)));
        u_sess->plsql_cxt.have_error = true;
    }
    return typeOid;
}

/* get the attrno of attribute when assign value to arrary element attibute. */
static AttrNumber get_assign_attrno(PLpgSQL_datum* target,  char* attrname)
{
    int nsubscripts = 0;
    Oid parenttypoid;
    int32 arraytypmod;
    Oid arraytypoid;
    Oid elemtypoid;
    AttrNumber attrno = -1;
    PLpgSQL_arrayelem* arrayelem = NULL;
    PLpgSQL_tableelem* tableelem = NULL;

    /*
     * To handle constructs like x[1][2] := something, we have to
     * be prepared to deal with a chain of arrayelem datums. Chase
     * back to find the base array datum.
     */
    if (target->dtype == PLPGSQL_DTYPE_ARRAYELEM) {
        do {
            arrayelem = (PLpgSQL_arrayelem*)target;
            if (nsubscripts >= MAXDIM) {
                const char* message = "number of array dimensions exceeds the maximum allowed in assignment.";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmodule(MOD_PLSQL),
                        errmsg("number of array dimensions exceeds the maximum allowed in assignment."),
                        errdetail("number of array dimensions (%d) exceeds the maximum allowed (%d) in assignment. ",
                            nsubscripts + 1, MAXDIM),
                        errcause("too large array dimensions"),
                        erraction("reduce the array dimensions")));
                u_sess->plsql_cxt.have_error = true;
            }
            nsubscripts++;
            target = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[arrayelem->arrayparentno];
        } while (target->dtype == PLPGSQL_DTYPE_ARRAYELEM);
    } else {
        do {
            tableelem = (PLpgSQL_tableelem*)target;
            if (nsubscripts >= MAXDIM) {
                const char* message = "number of array dimensions exceeds the maximum allowed in assignment.";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmodule(MOD_PLSQL),
                        errmsg("number of array dimensions exceeds the maximum allowed in assignment."),
                        errdetail("number of array dimensions (%d) exceeds the maximum allowed (%d) in assignment. ",
                            nsubscripts + 1, MAXDIM),
                        errcause("too large array dimensions"),
                        erraction("reduce the array dimensions")));
                u_sess->plsql_cxt.have_error = true;
            }
            nsubscripts++;
            target = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[tableelem->tableparentno];
        } while (target->dtype == PLPGSQL_DTYPE_TABLEELEM);
    }
    /* array element type must be PLPGSQL_DTYPE_VAR, otherwise user referencing array incorrectly. */
    if (target->dtype != PLPGSQL_DTYPE_VAR) {
        const char* message = "subscripted object in assignment is not an array";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("subscripted object in assignment is not an array"),
                errdetail("subscripted variable in assignment is not an array"),
                errcause("incorrectly referencing variables"),
                erraction("modify assign variable")));
        u_sess->plsql_cxt.have_error = true;
    }

	/* get the array element typoid */
    PLpgSQL_var* var = (PLpgSQL_var*)target;
    parenttypoid = var->datatype->typoid;
    arraytypmod = var->datatype->atttypmod;
    arraytypoid = getBaseTypeAndTypmod(parenttypoid, &arraytypmod);
    elemtypoid = get_element_type(arraytypoid);
    if (!OidIsValid(elemtypoid)) {
        const char* message = "subscripted object in assignment is not an array";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("subscripted object in assignment is not an array"),
                errdetail("subscripted variable \"%s\" in assignment is not an array", var->refname),
                errcause("incorrectly referencing variables"),
                erraction("modify assign variable")));
        u_sess->plsql_cxt.have_error = true;
    }

	/* get tupledesc by typoid */
    TupleDesc elemtupledesc =
        lookup_rowtype_tupdesc_noerror(elemtypoid, arraytypmod,true);
    if (elemtupledesc == NULL){
        const char* message = "array element type is not composite in assignment";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                errmodule(MOD_PLSQL),
                errmsg("array element type is not composite in assignment"),
                errdetail("array variable \"%s\" must be composite when assign value to attibute", var->refname),
                errcause("incorrectly referencing variables"),
                erraction("modify assign variable")));
    }

	/* search the matched attribute */
    for (int i = 0; i < elemtupledesc->natts; i++) {
        if (namestrcmp(&(elemtupledesc->attrs[i].attname), attrname) == 0) {
            attrno = i;
            break;
        }
    }
    ReleaseTupleDesc(elemtupledesc);

	/* attrno = -1 means there is no matched attribute */
    if (attrno == -1) {
        const char* message = "attribute does not exists";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
            errmodule(MOD_PLSQL),
            errmsg("attribute \"%s\" does not exist", attrname),
            errdetail("attribute \"%s\" does not exist in array variable \"%s\"", attrname, var->refname),
            errcause("incorrectly referencing variables"),
            erraction("modify assign variable")));
        u_sess->plsql_cxt.have_error = true;
	}

    return attrno;
}

/*
 * Brief		: support array variable as select into and using out target. 
 * Description	: if an array element is detected, add it to the u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[]
 *				  and return true, else return false.  
 * Notes		: 
 */ 
static bool 
read_into_using_add_arrayelem(char **fieldnames, int *varnos, int *nfields, int tmpdno, int *tok)
{
    PLpgSQL_arrayelem *newp = NULL;
    char           tokExpected[2];
    int            toktmp = 0;

    /* validation of arguments */
    if ((NULL == fieldnames) || (NULL == varnos) || (NULL == nfields) || (NULL == tok))
    {
        const char* message = "pointer is null in read_into_add_arrayelem function!";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                 errmsg(" pointer is null in read_into_add_arrayelem function! ")));
        return false;
    }

    if (('[' == *tok) || ('(' == *tok))
    {
        if ('[' == *tok)
            toktmp = ']';
        else
            toktmp = ')';

        tokExpected[0] = (char)toktmp;
        tokExpected[1] = '\0';

        newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
        newp->arrayparentno = tmpdno;
        newp->dtype      = PLPGSQL_DTYPE_ARRAYELEM;
        /* get the array index expression */
        newp->subscript = read_sql_expression(toktmp, &tokExpected[0]);
        
        if(NULL == newp->subscript) {
            const char* message = " error near arrary name!";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                (errcode(ERRCODE_SYNTAX_ERROR),
                  errmsg(" error near arrary name! ")));
            u_sess->plsql_cxt.have_error = true;
        }
        newp->assignattrno = -1;

        *tok = yylex();
        if (*tok == '.'){
            
            newp->assignattrno = get_assign_attrno((PLpgSQL_datum *)newp, get_attrname(*tok));
            *tok = yylex();
            if(*tok == '.') {
                yyerror("assign value to deep level attribute is not supported in SELECT/FETCH INTO method");
            }
        }
        plpgsql_adddatum((PLpgSQL_datum *)newp);

        fieldnames[*nfields]   = pstrdup("arrayelem");
        varnos[(*nfields)++]   = newp->dno;

        /* is an array element, return true */
        return true;
    }

    return false;
}

/*
 * Brief		: support array variable as select into and using out target. 
 * Description	: if an array element is detected, add it to the u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[]
 *				  and return true, else return false.  
 * Notes		: 
 */ 
static bool 
read_into_using_add_tableelem(char **fieldnames, int *varnos, int *nfields, int tmpdno, int *tok)
{
    PLpgSQL_tableelem *newp = NULL;
    char           tokExpected[2];
    int            toktmp = 0;

    /* validation of arguments */
    if ((NULL == fieldnames) || (NULL == varnos) || (NULL == nfields) || (NULL == tok))
    {
        const char* message = " pointer is null in read_into_add_arrayelem function!";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                 errmsg(" pointer is null in read_into_add_arrayelem function! ")));
        u_sess->plsql_cxt.have_error = true;
        return false;
    }

    if (('[' == *tok) || ('(' == *tok))
    {
        if ('[' == *tok)
            toktmp = ']';
        else
            toktmp = ')';

        tokExpected[0] = (char)toktmp;
        tokExpected[1] = '\0';

        newp = (PLpgSQL_tableelem *)palloc0(sizeof(PLpgSQL_tableelem));
        newp->tableparentno = tmpdno;
        newp->dtype      = PLPGSQL_DTYPE_TABLEELEM;
        /* get the array index expression */
        newp->subscript = read_sql_expression(toktmp, &tokExpected[0]);
        
        if(NULL == newp->subscript) {
            const char* message = " error near arrary name!";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                (errcode(ERRCODE_SYNTAX_ERROR),
                  errmsg(" error near arrary name! ")));
            u_sess->plsql_cxt.have_error = true;
        }
        newp->assignattrno = -1;

        *tok = yylex();
        if (*tok == '.'){
            *tok = yylex();
            newp->assignattrno = get_assign_attrno((PLpgSQL_datum *)newp, get_attrname(*tok));
            *tok = yylex();
        }
        plpgsql_adddatum((PLpgSQL_datum *)newp);

        fieldnames[*nfields]   = pstrdup("arrayelem");
        varnos[(*nfields)++]   = newp->dno;

        /* is an array element, return true */
        return true;
    }

    return false;
}

/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword. If it is into_user_defined_variable_list_clause return true.
 */
static bool
read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row, bool *strict, int firsttoken, bool bulk_collect)
{
    int			tok;

    /* Set default results */
    *rec = NULL;
    *row = NULL;
    if (strict) {
        if (DB_IS_CMPT(PG_FORMAT | B_FORMAT) && firsttoken == K_SELECT && SELECT_INTO_RETURN_NULL) {
            *strict = false;
        } else {
            *strict = true;
        }
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (strict)
        *strict = true;
#endif
    tok = yylex();
    if (tok == '@' || tok == SET_USER_IDENT) {
        return true;
    }
    if (strict && tok == K_STRICT)
    {
        *strict = true;
        tok = yylex();
    } else if (strict && bulk_collect) {
        /* bulk into target can be assigned null */
        *strict = false;
    }

    /*
     * Currently, a row or record variable can be the single INTO target,
     * but not a member of a multi-target list.  So we throw error if there
     * is a comma after it, because that probably means the user tried to
     * write a multi-target list.  If this ever gets generalized, we should
     * probably refactor read_into_scalar_list so it handles all cases.
     */
    switch (tok)
    {
        case T_DATUM:
        case T_VARRAY_VAR:
        case T_TABLE_VAR:
        case T_PACKAGE_VARIABLE:
            if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW
            || yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_RECORD)
            {
                check_assignable(yylval.wdatum.datum, yylloc);
                *row = (PLpgSQL_row *) yylval.wdatum.datum;
                tok = yylex();
                if (tok == ',') {
                    const char* message = "record or row variable cannot be part of multiple-item INTO list";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("record or row variable cannot be part of multiple-item INTO list"),
                             parser_errposition(yylloc)));
                }
                if (tok == '.') {
                    const char* message = "Improper use of '.*'. The '.*' operator cannot be used with a row type variable.";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("Improper use of '.*'. The '.*' operator cannot be used with a row type variable."),
                             parser_errposition(yylloc)));
                }
                if (!DB_IS_CMPT(PG_FORMAT) && (tok == T_DATUM || tok == T_VARRAY_VAR
                    || tok == T_TABLE_VAR || tok == T_PACKAGE_VARIABLE)) {
                    const char* message = "syntax error, expected \",\"";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("syntax error, expected \",\""),
                             parser_errposition(yylloc)));
                }
                plpgsql_push_back_token(tok);
            }
            else if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
            {
                check_assignable(yylval.wdatum.datum, yylloc);
                *rec = (PLpgSQL_rec *) yylval.wdatum.datum;
                tok = yylex();
                if (tok == ',') {
                    const char* message = "record or row variable cannot be part of multiple-item INTO list";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("record or row variable cannot be part of multiple-item INTO list"),
                             parser_errposition(yylloc)));
                }
                if (!DB_IS_CMPT(PG_FORMAT) && (tok == T_DATUM || tok == T_VARRAY_VAR
                    || tok == T_TABLE_VAR || tok == T_PACKAGE_VARIABLE)) {
                    const char* message = "syntax error, expected \",\"";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("syntax error, expected \",\""),
                             parser_errposition(yylloc)));
                }
                plpgsql_push_back_token(tok);
            }
            else
            {
                *row = read_into_array_table_scalar_list(NameOfDatum(&(yylval.wdatum)),
                                             yylval.wdatum.datum, yylval.wdatum.dno, yylloc, bulk_collect);
            }
            break;
        case  T_PLACEHOLDER:
                *row = read_into_placeholder_scalar_list(yylval.word.ident, yylloc);
            break;
        default:
            /* just to give a better message than "syntax error" */
            current_token_is_not_variable(tok);
            break;
    }
    return false;
}

/*
 *  Read the argument of an USING IN / OUT clause. 
 */
static void
read_using_target(List  **in_expr, PLpgSQL_row **out_row)
{
    int				tok		= 0;
    int				out_nfields=   0;
    int				out_varnos[1024] = {0};
    char			*out_fieldnames[1024] = {0};
    bool          	isin	= false;

    *in_expr = NULL;
    *out_row = NULL;

    do 
    {
        tok=yylex();
        if(K_IN == tok)
        {
            tok = yylex();
            isin = true; 
        }

        if(K_OUT == tok)
        {
            char * tempvar = NULL;
            int    tempdno = 0;
            PLpgSQL_expr * tempexpr = NULL;

            tok = yylex();

            switch(tok)
            {
                case T_DATUM:
                    tempvar = pstrdup(NameOfDatum(&(yylval.wdatum)));
                    tempdno = yylval.wdatum.dno;
                    plpgsql_push_back_token(tok);
                    tempexpr  =read_sql_construct(',',';',',',", or ;","SELECT ",true,true,true,NULL,&tok);
                    tempexpr->isouttype = true;
                    *in_expr=lappend((*in_expr), tempexpr);

                    if (!read_into_using_add_arrayelem(out_fieldnames, out_varnos, &out_nfields, tempdno, &tok)) 
                    {
                        out_fieldnames[out_nfields] = tempvar;
                        out_varnos[out_nfields++]	= tempdno;
                    }
                    else
                    {
                        if (isin) {
                            const char* message = " using can't support array parameter with in out ";
                            InsertErrorMessage(message, plpgsql_yylloc);
                            ereport(errstate,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg(" using can't support array parameter with in out !")));
                        }

                    }

                    break;

                default:
                    const char* message = "not all the parameters are scalar variables.";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("not all the parameters are scalar variables.")));
            }
        }
        else
        {
            PLpgSQL_expr * expr = NULL;
            plpgsql_push_back_token(tok);
            expr  = read_sql_construct(',',';',',',", or ;","SELECT ",true,true,true,NULL,&tok);
            *in_expr=lappend((*in_expr), expr);

            isin = false;

        }

    }while(tok== ',');

    /*
    * We read an extra, non-comma token from yylex(), so push it
    * back onto the input stream
    */
    plpgsql_push_back_token(tok);

    if(out_nfields)
    {
        (*out_row) = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
        (*out_row)->dtype = PLPGSQL_DTYPE_ROW;
        (*out_row)->refname = pstrdup("*internal*");
        (*out_row)->lineno = plpgsql_location_to_lineno(yylloc);
        (*out_row)->rowtupdesc = NULL;
        (*out_row)->nfields = out_nfields;
        (*out_row)->fieldnames = (char **)palloc(sizeof(char *) * out_nfields);
        (*out_row)->varnos = (int *)palloc(sizeof(int) * out_nfields);
        (*out_row)->isImplicit = true;
        while (--out_nfields >= 0)
        {
            (*out_row)->fieldnames[out_nfields] = out_fieldnames[out_nfields];
            (*out_row)->varnos[out_nfields] = out_varnos[out_nfields];
        }
        plpgsql_adddatum((PLpgSQL_datum *)(*out_row));
    }
}

/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars.
 */
static PLpgSQL_row *
read_into_scalar_list(char *initial_name,
                      PLpgSQL_datum *initial_datum,
                      int initial_dno,
                      int initial_location)
{
    int				 nfields;
    char			*fieldnames[1024] = {NULL};
    int				 varnos[1024] = {0};
    PLpgSQL_row		*row;
    int				 tok;

    check_assignable(initial_datum, initial_location);
    fieldnames[0] = initial_name;
    varnos[0]	  = initial_dno;
    nfields		  = 1;

    while ((tok = yylex()) == ',')
    {
        /* Check for array overflow */
        if (nfields >= 1024) {
            const char* message = "too many INTO variables specified.";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO variables specified"),
                     parser_errposition(yylloc)));
            u_sess->plsql_cxt.have_error = true;
            break;
        }

        tok = yylex();
        switch (tok)
        {
            case T_DATUM:
                check_assignable(yylval.wdatum.datum, yylloc);
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC) {
                    const char* message = "variable is not a scalar variable";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("\"%s\" is not a scalar variable",
                                    NameOfDatum(&(yylval.wdatum))),
                             parser_errposition(yylloc)));
                    u_sess->plsql_cxt.have_error = true;
                    break;
                }
                fieldnames[nfields] = NameOfDatum(&(yylval.wdatum));
                varnos[nfields++]	= yylval.wdatum.dno;
                break;

            default:
                /* just to give a better message than "syntax error" */
                current_token_is_not_variable(tok);
                break;
        }
    }

    /*
     * We read an extra, non-comma token from yylex(), so push it
     * back onto the input stream
     */
    plpgsql_push_back_token(tok);

    row = (PLpgSQL_row *)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = pstrdup("*internal*");
    row->lineno = plpgsql_location_to_lineno(initial_location);
    row->rowtupdesc = NULL;
    row->nfields = nfields;
    row->fieldnames = (char **)palloc(sizeof(char *) * nfields);
    row->varnos = (int *)palloc(sizeof(int) * nfields);
    row->isImplicit = true;
    while (--nfields >= 0)
    {
        row->fieldnames[nfields] = fieldnames[nfields];
        row->varnos[nfields] = varnos[nfields];
    }

    plpgsql_adddatum((PLpgSQL_datum *)row);

    return row;
}

/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars.
 */
static PLpgSQL_row *
read_into_array_table_scalar_list(char *initial_name,
                      PLpgSQL_datum *initial_datum,
                      int initial_dno,
                      int initial_location,
                      bool bulk_collect)
{
    int				 nfields = 0;
    char			*fieldnames[1024] = {NULL};
    int				 varnos[1024] = {0};
    PLpgSQL_row		*row;
    int				 tok;
    int				 toktmp = 0;
    int 			 tmpdno = 0;
    int              type_flag = -1;
    bool             isarrayelem = false;
    char* 			 nextname = NULL;

    check_assignable(initial_datum, initial_location);

    tmpdno = initial_dno;
    tok = yylex();
    get_datum_tok_type(initial_datum, &type_flag);

    if (type_flag == PLPGSQL_TOK_TABLE_VAR) {
        isarrayelem = read_into_using_add_tableelem(fieldnames, varnos, &nfields, tmpdno, &tok);
    } else if (type_flag == PLPGSQL_TOK_VARRAY_VAR) {
        isarrayelem = read_into_using_add_arrayelem(fieldnames, varnos, &nfields, tmpdno, &tok);
    } else {
        isarrayelem = false;
    }
    if (!isarrayelem)
    {
        fieldnames[0] = initial_name;
        varnos[0]	  = initial_dno;
        nfields		  = 1;
    }
    while (',' == tok)
    {
        /* Check for array overflow */
        if (nfields >= 1024)
        {
            const char* message = "too many INTO variables specified";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO variables specified"),
                     parser_errposition(yylloc)));
            return NULL;
        }

        toktmp = yylex();
        type_flag = -1;

        switch (toktmp)
        {
            case T_DATUM:
                check_assignable(yylval.wdatum.datum, yylloc);
                
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC) {
                    const char* message = "it not a scalar variable";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("\"%s\" is not a scalar variable",
                                    NameOfDatum(&(yylval.wdatum))),
                            parser_errposition(yylloc)));
                }

                tmpdno = yylval.wdatum.dno;
                nextname = NameOfDatum(&(yylval.wdatum));
                fieldnames[nfields] = nextname;
                varnos[nfields++] = tmpdno;
                tok = yylex();
                break;
            case T_PACKAGE_VARIABLE:
                check_assignable(yylval.wdatum.datum, yylloc);
                
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC) {
                    const char* message = "it not a scalar variable";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("\"%s\" is not a scalar variable",
                                    NameOfDatum(&(yylval.wdatum))),
                            parser_errposition(yylloc)));
                    u_sess->plsql_cxt.have_error = true;
                }

                tmpdno = yylval.wdatum.dno;
                nextname = NameOfDatum(&(yylval.wdatum));
                type_flag = -1;
                get_datum_tok_type(yylval.wdatum.datum, &type_flag);
                if (type_flag == -1) {
                    fieldnames[nfields] = nextname;
                    varnos[nfields++] = tmpdno;
                    tok = yylex();
                    break;
                } else if (type_flag == PLPGSQL_TOK_TABLE_VAR) {
                    tok = yylex();
                    if (tok < -1)
                        return NULL;
                    if (!read_into_using_add_tableelem(fieldnames, varnos, &nfields, tmpdno, &tok))
                    {
                        if (bulk_collect) {
                            fieldnames[nfields] = nextname;
                            varnos[nfields++] = tmpdno;
                            break;
                        }
                        const char* message = "error when read table var subscript";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg(" error when read table var subscript! ")));
                        u_sess->plsql_cxt.have_error = true;
                        return NULL;
                    }		
                    break;
                } else {
                    tok = yylex();
                    if (tok < -1)
                        return NULL;
                    if (!read_into_using_add_arrayelem(fieldnames, varnos, &nfields, tmpdno, &tok))
                    {
                        if (bulk_collect) {
                            fieldnames[nfields] = nextname;
                            varnos[nfields++] = tmpdno;
                            break;
                        }
                        const char* message = "error when read array var subscript";
                        InsertErrorMessage(message, plpgsql_yylloc);
                        ereport(errstate,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg(" error when read array var subscript! ")));
                        u_sess->plsql_cxt.have_error = true;
                        return NULL;
                    }		
                    break;
                }
            case T_VARRAY_VAR:
                check_assignable(yylval.wdatum.datum, yylloc);
                tmpdno = yylval.wdatum.dno;
                nextname = NameOfDatum(&(yylval.wdatum));
                tok = yylex();
                if (tok < -1)
                    return NULL;
                if (!read_into_using_add_arrayelem(fieldnames, varnos, &nfields, tmpdno, &tok))
                {
                    if (bulk_collect) {
                        fieldnames[nfields] = nextname;
                        varnos[nfields++] = tmpdno;
                        break;
                    }
                    const char* message = "error near arrary name!";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg(" error near arrary name! ")));
                    return NULL;
                }
                break;
            case T_TABLE_VAR:
                check_assignable(yylval.wdatum.datum, yylloc);
                tmpdno = yylval.wdatum.dno;
                nextname = NameOfDatum(&(yylval.wdatum));
                tok = yylex();
                if (tok < -1)
                    return NULL;
                if (!read_into_using_add_tableelem(fieldnames, varnos, &nfields, tmpdno, &tok))
                {
                    if (bulk_collect) {
                        fieldnames[nfields] = nextname;
                        varnos[nfields++] = tmpdno;
                        break;
                    }
                    const char* message = "error near table name";
                    InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(errstate,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg(" error near table name! ")));
                    u_sess->plsql_cxt.have_error = true;
                    return NULL;
                }		
                break;
            default:
                tok = yylex();
                if (tok < -1)
                    return NULL;

                /* just to give a better message than "syntax error" */
                current_token_is_not_variable(tok);
                break;
        }
    }

    if (!DB_IS_CMPT(PG_FORMAT) && (tok == T_DATUM || tok == T_VARRAY_VAR
                    || tok == T_TABLE_VAR || tok == T_PACKAGE_VARIABLE)) {
        const char* message = "syntax error, expected \",\"";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
            (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("syntax error, expected \",\""),
            parser_errposition(yylloc)));
    }
    
    /*
     * We read an extra, non-comma token from yylex(), so push it
     * back onto the input stream
     */
    plpgsql_push_back_token(tok);

    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = pstrdup("*internal*");
    row->lineno = plpgsql_location_to_lineno(initial_location);
    row->rowtupdesc = NULL;
    row->nfields = nfields;
    row->fieldnames = (char**)palloc(sizeof(char *) * nfields);
    row->varnos = (int*)palloc(sizeof(int) * nfields);
    row->isImplicit = true;
    while (--nfields >= 0)
    {
        row->fieldnames[nfields] = fieldnames[nfields];
        row->varnos[nfields] = varnos[nfields];
    }

    plpgsql_adddatum((PLpgSQL_datum *)row);

    return row;
}

/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars. The function is for placeholders.
 */
static PLpgSQL_row *
read_into_placeholder_scalar_list(char *initial_name,
                      int initial_location)
{
    int				 intoplaceholders;
    PLpgSQL_row		*row;
    int				 tok;

    intoplaceholders		  = 1;

    while ((tok = yylex()) == ',')
    {
        /* Check for array overflow */
        if ( intoplaceholders >= 1024 ) {
            const char* message = "too many INTO placeholders specified";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO placeholders specified"),
                     parser_errposition(yylloc)));
            u_sess->plsql_cxt.have_error = true;
            break;
        }

        tok = yylex();
        switch (tok)
        {
            case T_PLACEHOLDER:
                intoplaceholders++;
                break;

            default:
                const char* message = "invalid placeholder specified";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("invalid placeholder specified"),
                         parser_errposition(yylloc)));
                break;
        }
    }

    /*
     * We read an extra, non-comma token from yylex(), so push it
     * back onto the input stream
     */
    plpgsql_push_back_token(tok);

    row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = pstrdup("*internal*");
    row->lineno = plpgsql_location_to_lineno(initial_location);
    row->rowtupdesc = NULL;
    row->nfields = 0;
    row->fieldnames = NULL;
    row->varnos = NULL;
    row->intoplaceholders = intoplaceholders;
    row->intodatums = NULL;
    row->isImplicit = true;

    plpgsql_adddatum((PLpgSQL_datum *)row);

    return row;
}



/*
 * Convert a single scalar into a "row" list.  This is exactly
 * like read_into_scalar_list except we never consume any input.
 *
 * Note: lineno could be computed from location, but since callers
 * have it at hand already, we may as well pass it in.
 */
static PLpgSQL_row *
make_scalar_list1(char *initial_name,
                  PLpgSQL_datum *initial_datum,
                  int initial_dno,
                  int lineno, int location)
{
    PLpgSQL_row		*row;

    check_assignable(initial_datum, location);

    row = (PLpgSQL_row *)palloc0(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = pstrdup("*internal*");
    row->lineno = lineno;
    row->rowtupdesc = NULL;
    row->nfields = 1;
    row->fieldnames = (char **)palloc(sizeof(char *));
    row->varnos = (int *)palloc(sizeof(int));
    row->fieldnames[0] = initial_name;
    row->varnos[0] = initial_dno;
    row->isImplicit = true;

    plpgsql_adddatum((PLpgSQL_datum *)row);

    return row;
}

/*
 * When the PL/pgSQL parser expects to see a SQL statement, it is very
 * liberal in what it accepts; for example, we often assume an
 * unrecognized keyword is the beginning of a SQL statement. This
 * avoids the need to duplicate parts of the SQL grammar in the
 * PL/pgSQL grammar, but it means we can accept wildly malformed
 * input. To try and catch some of the more obviously invalid input,
 * we run the strings we expect to be SQL statements through the main
 * SQL parser.
 *
 * We only invoke the raw parser (not the analyzer); this doesn't do
 * any database access and does not check any semantic rules, it just
 * checks for basic syntactic correctness. We do this here, rather
 * than after parsing has finished, because a malformed SQL statement
 * may cause the PL/pgSQL parser to become confused about statement
 * borders. So it is best to bail out as early as we can.
 *
 * It is assumed that "stmt" represents a copy of the function source text
 * beginning at offset "location", with leader text of length "leaderlen"
 * (typically "SELECT ") prefixed to the source text.  We use this assumption
 * to transpose any error cursor position back to the function source text.
 * If no error cursor is provided, we'll just point at "location".
 */
static void
check_sql_expr(const char *stmt, int location, int leaderlen)
{
    sql_error_callback_arg cbarg;
    ErrorContextCallback  syntax_errcontext;
    MemoryContext oldCxt;

    if (!u_sess->plsql_cxt.curr_compile_context->plpgsql_check_syntax)
        return;

    cbarg.location = location;
    cbarg.leaderlen = leaderlen;

    syntax_errcontext.callback = plpgsql_sql_error_callback;
    syntax_errcontext.arg = &cbarg;
    syntax_errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &syntax_errcontext;

    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
    RawParserHook parser_hook= raw_parser;
#if (!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS))
    if (u_sess->attr.attr_sql.whale || u_sess->attr.attr_sql.dolphin) {
        int id = GetCustomParserId();
        if (id >= 0 && g_instance.raw_parser_hook[id] != NULL) {
            parser_hook = (RawParserHook)g_instance.raw_parser_hook[id];
        }
    }
#endif
    (void)parser_hook(stmt, NULL);
    MemoryContextSwitchTo(oldCxt);

    /* Restore former ereport callback */
    t_thrd.log_cxt.error_context_stack = syntax_errcontext.previous;
}

static void
plpgsql_sql_error_callback(void *arg)
{
    sql_error_callback_arg *cbarg = (sql_error_callback_arg *) arg;
    int			errpos;

    /*
     * First, set up internalerrposition to point to the start of the
     * statement text within the function text.  Note this converts
     * location (a byte offset) to a character number.
     */
    parser_errposition(cbarg->location);

    /*
     * If the core parser provided an error position, transpose it.
     * Note we are dealing with 1-based character numbers at this point.
     */
    errpos = geterrposition();
    if (errpos > cbarg->leaderlen)
    {
        int		myerrpos = getinternalerrposition();

        if (myerrpos > 0)		/* safety check */
            internalerrposition(myerrpos + errpos - cbarg->leaderlen - 1);
    }

    /* In any case, flush errposition --- we want internalerrpos only */
    errposition(0);
}

/*
 * Parse a SQL datatype name and produce a PLpgSQL_type structure.
 *
 * The heavy lifting is done elsewhere.  Here we are only concerned
 * with setting up an errcontext link that will let us give an error
 * cursor pointing into the plpgsql function source, if necessary.
 * This is handled the same as in check_sql_expr(), and we likewise
 * expect that the given string is a copy from the source text.
 */
static PLpgSQL_type *
parse_datatype(const char *string, int location)
{
    Oid			type_id;
    int32		typmod;
    sql_error_callback_arg cbarg;
    ErrorContextCallback  syntax_errcontext;
    MemoryContext oldCxt = NULL;

    cbarg.location = location;
    cbarg.leaderlen = 0;

    syntax_errcontext.callback = plpgsql_sql_error_callback;
    syntax_errcontext.arg = &cbarg;
    syntax_errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &syntax_errcontext;


    /*
     * parseTypeString is only for getting type_id and typemod, who are both scalars, 
     * so memory used in it is all temp.
     */
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);


    u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
    /* Let the main parser try to parse it under standard SQL rules */
    TypeDependExtend* typeDependExtend = NULL;
    if (enable_plpgsql_gsdependency()) {
        InstanceTypeNameDependExtend(&typeDependExtend);
        CreatePlsqlType oldCreatePlsqlType = u_sess->plsql_cxt.createPlsqlType;
        PG_TRY();
        {
            set_create_plsql_type_not_check_nsp_oid();
            parseTypeString(string, &type_id, &typmod, typeDependExtend);
            set_create_plsql_type(oldCreatePlsqlType);
        }
        PG_CATCH();
        {
            set_create_plsql_type(oldCreatePlsqlType);
            PG_RE_THROW();
        }
        PG_END_TRY();
    } else {
        parseTypeString(string, &type_id, &typmod, typeDependExtend);
    }

    (void)MemoryContextSwitchTo(oldCxt);

    /* Restore former ereport callback */
    t_thrd.log_cxt.error_context_stack = syntax_errcontext.previous;

    /* Okay, build a PLpgSQL_type data structure for it */
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL)
    {
        return plpgsql_build_datatype(type_id, typmod, 0, typeDependExtend);
    }

    return plpgsql_build_datatype(type_id, typmod,
                                  u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation, typeDependExtend);
}

/* Build a arrary_type by elem_type. */
static PLpgSQL_type * build_array_type_from_elemtype(PLpgSQL_type *elem_type)
{
    PLpgSQL_type *arrary_type;
    Oid arraytypoid = get_array_type(elem_type->typoid);
    if (arraytypoid == InvalidOid) {
        ereport(errstate,
            (errmodule(MOD_PLSQL),
            errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("array type not found"),
            errdetail("array type of \"%s\" does not exist.", elem_type->typname),
            errcause("undefined object"),
            erraction("check typename")));
    }
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL) {
        arrary_type = plpgsql_build_datatype(arraytypoid, elem_type->atttypmod, 0);
    } else {
        arrary_type = plpgsql_build_datatype(arraytypoid, elem_type->atttypmod,
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_input_collation);
    }
    return arrary_type;
}

static PLpgSQL_var* plpgsql_build_nested_variable(PLpgSQL_var *nest_table, bool isconst, char* name, int lineno)
{
    PLpgSQL_var *build_nest_table = NULL;
    PLpgSQL_type *new_var_type = NULL;
    char nestname[NAMEDATALEN] = {0};
    errno_t rc = EOK;
    rc = snprintf_s(nestname, sizeof(nestname), sizeof(nestname) - 1, "%s_nest", name);
    securec_check_ss(rc, "", "");
    /* set nested table type */
    new_var_type = build_array_type_from_elemtype(nest_table->datatype);
    new_var_type->tableOfIndexType = nest_table->datatype->tableOfIndexType;
    new_var_type->collectionType = nest_table->datatype->collectionType;
    build_nest_table = (PLpgSQL_var *)plpgsql_build_variable(nestname, lineno, new_var_type, true);
    build_nest_table->isconst = isconst;
    build_nest_table->default_val = NULL;
    build_nest_table->nest_layers = nest_table->nest_layers;
    if (nest_table->nest_table != NULL) {
        build_nest_table->nest_table = plpgsql_build_nested_variable(nest_table->nest_table, isconst, name, lineno);
    }
    return build_nest_table;
}

static int get_nest_tableof_layer(PLpgSQL_var *var, const char *typname, int errstate)
{
    int depth = 0;
    while (var != NULL) {
        depth++;
        var = var->nest_table;
    }
    if (depth + 1 > MAXDIM) {
        ereport(errstate,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmodule(MOD_PLSQL),
                errmsg("Layer number of nest tableof type exceeds the maximum allowed."),
                errdetail("Define nest tableof type \"%s\" layers (%d) exceeds the maximum allowed (%d).", typname, depth + 1, MAXDIM),
                errcause("too many nested layers"),
                erraction("check define of table of type")));
        u_sess->plsql_cxt.have_error = true;
    }
    return depth + 1;
}

static void getPkgFuncTypeName(char* typname, char** functypname, char** pkgtypname)
{
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        *functypname = CastPackageTypeName(typname,
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_oid, false);
        if (strlen(*functypname) >= NAMEDATALEN) {
            const char* message = "record name too long";
            InsertErrorMessage(message, plpgsql_yylloc, true);
            ereport(ERROR,
                (errcode(ERRCODE_NAME_TOO_LONG),
                    errmsg("type name too long"),
                    errdetail("record name %s with func oid %d should be less the %d letters.",
                        typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_oid, NAMEDATALEN)));
        }
    } 
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        *pkgtypname = CastPackageTypeName(typname,
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, true);
        if (strlen(*pkgtypname) >= NAMEDATALEN) {
            const char* message = "record name too long";
            InsertErrorMessage(message, plpgsql_yylloc, true);
            ereport(ERROR,
                (errcode(ERRCODE_NAME_TOO_LONG),
                    errmsg("type name too long"),
                    errdetail("record name %s with package name %s should be less the %d letters.",
                        typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature, NAMEDATALEN)));
        }
    }
}

/* 
 * look up composite type from namespace
 */
static PLpgSQL_nsitem* lookupCompositeType(char* functypname, char* pkgtypname)
{
    PLpgSQL_nsitem* ns = NULL;
    /* if compile func, search func namesapce first */
    if (functypname != NULL) {
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, functypname, NULL, NULL, NULL);
        if (ns != NULL || pkgtypname == NULL) {
            return ns;
        }
    }

    /* next search package namespace */
    PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
    ns = plpgsql_ns_lookup(pkg->public_ns, false, pkgtypname, NULL, NULL, NULL);
    if (ns != NULL) {
        return ns;
    }

    /* may compile pkg spec now, search top again */
    ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, pkgtypname, NULL, NULL, NULL);
    if (ns != NULL) {
        return ns;
    }

    /* if compile package body, search private namespace */
    if (!pkg->is_spec_compiling) {
        /* private type should cast '$' */
        StringInfoData  castTypName;
        initStringInfo(&castTypName);
        appendStringInfoString(&castTypName, "$");
        appendStringInfoString(&castTypName, pkgtypname);
 
        /* search private namespace, when complie, it is the top */
        ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, castTypName.data, NULL, NULL, NULL);
        pfree_ext(castTypName.data);
    }

    return ns;
}

static Oid getOldTypeOidByTypeName(char** typname, char** schamaName, char* functypname, char* pkgtypname, Oid* pkgoid)
{
    Oid nameSpaceOid = InvalidOid;
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        *pkgoid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_oid;
        *typname = functypname;
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_searchpath != NULL) {
            nameSpaceOid = linitial_oid(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_searchpath->schemas);
        }
    } else {
        *pkgoid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        *typname = pkgtypname;
        nameSpaceOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    }

    if (OidIsValid(nameSpaceOid)) {
        *schamaName = get_namespace_name(nameSpaceOid);
    } else {
        nameSpaceOid = getCurrentNamespace();
    }

    Oid oldtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(*typname),
        ObjectIdGetDatum(nameSpaceOid));
    return oldtypeoid;
}

static RangeVar* createRangeVarForCompositeType(char* schamaName, char* typname)
{
    RangeVar *r = makeNode(RangeVar);
    r->catalogname = NULL;
    r->schemaname = schamaName;
    r->relname = pstrdup(typname);
    r->relpersistence = RELPERSISTENCE_PERMANENT;
    r->location = -1;
    r->ispartition = false;
    r->isbucket = false;
    r->buckets = NIL;
    return r;
}

static void buildDependencyForCompositeType(Oid newtypeoid)
{
    ObjectAddress myself, referenced;
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL) {
        myself.classId = ProcedureRelationId;
        myself.objectId = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->fn_oid;
        myself.objectSubId = 0;
    } else {
        myself.classId = PackageRelationId;
        myself.objectId = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        myself.objectSubId = 0;
    }
    referenced.classId = TypeRelationId;
    referenced.objectId = newtypeoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&referenced, &myself, DEPENDENCY_AUTO);
    CommandCounterIncrement();
}

static bool checkDuplicateAttrName(TupleDesc tupleDesc)
{
    int attrnum = tupleDesc->natts;
    for (int i = 0; i < attrnum; i++) {
        Form_pg_attribute attr1 = &tupleDesc->attrs[i];
        for (int j = i + 1; j < attrnum; j++) {
            Form_pg_attribute attr2 = &tupleDesc->attrs[j];
            if (strcmp(NameStr(attr1->attname), NameStr(attr2->attname)) == 0) {
                return true;
            }
        }
    }
    return false;
}
#ifndef ENABLE_MULTIPLE_NODES
static bool checkAllAttrName(TupleDesc tupleDesc)
{
    int attrnum = tupleDesc->natts;
    for (int i = 0; i < attrnum; i++) {
        Form_pg_attribute pg_att_form = &tupleDesc->attrs[i];
        char* att_name = NameStr(pg_att_form->attname);
        if (strcmp(att_name, "?column?") != 0) {
            return false;
        }
    }
    return true;
}
#endif
static Oid createCompositeTypeForCursor(PLpgSQL_var* var, PLpgSQL_expr* expr)
{
#ifdef ENABLE_MULTIPLE_NODES
    return InvalidOid;
#endif
    TupleDesc tupleDesc = getCursorTupleDesc(expr, false);
    if (tupleDesc == NULL || tupleDesc->natts == 0) {
        return InvalidOid;
    }
    /* 
     * if cursor is select a,a from t1, cannot have same attr name.
     */
    bool isHaveDupName = checkDuplicateAttrName(tupleDesc);
    if (isHaveDupName) {
        return InvalidOid;
    }

    /* concatenate name string with function name for composite type, which to avoid conflict. */
    char* functypname = NULL;
    char* pkgtypname = NULL;
    char* typname = NULL;
    char* schamaname = NULL;
    Oid pkgoid = InvalidOid;
    getPkgFuncTypeName(var->refname, &functypname, &pkgtypname);
    Oid oldtypeoid = getOldTypeOidByTypeName(&typname, &schamaname, functypname, pkgtypname, &pkgoid);
    Oid newtypeoid = InvalidOid;
    if (OidIsValid(oldtypeoid)) {
            /* already build one, just use it */
            newtypeoid = oldtypeoid;
    } else {
        RangeVar* r = createRangeVarForCompositeType(schamaname, typname);

        List* codeflist = NULL;
        int attrnum = tupleDesc->natts;
        for (int i = 0; i < attrnum; i++) {
            ColumnDef *n = makeNode(ColumnDef);
            Form_pg_attribute attr = &tupleDesc->attrs[i];
            n->colname = pstrdup(NameStr(attr->attname));
            n->typname = makeTypeNameFromOid(attr->atttypid, attr->atttypmod);
            n->inhcount = 0;
            n->is_local = true;
            n->is_not_null = false;
            n->is_from_type = false;
            n->storage = 0;
            n->raw_default = NULL;
            n->cooked_default = NULL;
            n->collClause = NULL;
            n->clientLogicColumnRef=NULL;
            n->collOid = InvalidOid;
            n->constraints = NIL;
            codeflist = lappend(codeflist, (Node*)n);
        }
        DefineCompositeType(r, codeflist);
        newtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname),
            ObjectIdGetDatum(getCurrentNamespace()));
        pfree_ext(r);
        ListCell* cell = NULL;
        foreach (cell, codeflist) {
            ColumnDef* n = (ColumnDef*)lfirst(cell);
            pfree_ext(n->colname);
        }
        list_free_deep(codeflist);

        /* build dependency on created composite type. */
        buildDependencyForCompositeType(newtypeoid);
    }
    
    pfree_ext(functypname);
    pfree_ext(pkgtypname);
    return newtypeoid;
}

#ifndef ENABLE_MULTIPLE_NODES
static PLpgSQL_type* build_type_from_cursor_var(PLpgSQL_var* var)
{
    PLpgSQL_type *newp = NULL;
    Oid typeOid = var->datatype->cursorCompositeOid;
    /* build datatype of the created composite type. */
    newp = plpgsql_build_datatype(typeOid, -1, InvalidOid);
    newp->dtype = PLPGSQL_DTYPE_COMPOSITE;
    newp->ttype = PLPGSQL_TTYPE_ROW;

    /* add the composite type to datum and namespace. */
    int varno = plpgsql_adddatum((PLpgSQL_datum*)newp);
    plpgsql_ns_additem(PLPGSQL_NSTYPE_COMPOSITE, varno, var->datatype->typname );

    return newp;
}
#endif

/*
 * the record type will be nested or referenced by another package, check if it valid.
 */
static void check_record_type(PLpgSQL_rec_type * var_type, int location, bool check_nested)
{
    /* for now, record type with table of with index by, is not supported be nested */
    PLpgSQL_type* type = NULL;
    char* errstr = NULL;
    if (check_nested) {
        errstr = "nested.";
    } else {
        errstr = "referenced by another package.";
    }
    for (int i = 0; i < var_type->attrnum; i++) {
        type = var_type->types[i];
        if (type->ttype == PLPGSQL_TTYPE_SCALAR && OidIsValid(type->tableOfIndexType)) {
            ereport(errstate,
                (errmodule(MOD_PLSQL),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("record type with table of attribute is not suppoted to be %s", errstr),
                errdetail("attribute \"%s\" of record type \"%s\" is table of with index by, which is not supported to be %s",
                    var_type->attrnames[i],var_type->typname, errstr),
                errcause("feature not suppoted"),
                parser_errposition(location),
                erraction("modify type definition")));
        }
    }
}

/*
 * Build a composite type by execute SQL, when record type is nested.
 */
static PLpgSQL_type* build_type_from_record_var(int dno, int location)
{
    PLpgSQL_type *newp = NULL;
    PLpgSQL_rec_type * var_type = (PLpgSQL_rec_type *)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
    check_record_type(var_type, location);

    /* concatenate name string with function name for composite type, which to avoid conflict. */
    char*  functypname = NULL;
    char*  pkgtypname = NULL;
    getPkgFuncTypeName(var_type->typname, &functypname, &pkgtypname);
    /* look up composite type from namespace, if exists, we have already build one. */
    PLpgSQL_nsitem* ns = lookupCompositeType(functypname, pkgtypname);
    
    if (ns == NULL) {
        /* we need to build a composite type, and drop any remaining types. */
        Oid newtypeoid = InvalidOid;
        char* typname;
        char* schamaname = NULL;
        Oid pkgoid = InvalidOid;
        
        Oid oldtypeoid = getOldTypeOidByTypeName(&typname, &schamaname, functypname, pkgtypname, &pkgoid);
        if (OidIsValid(oldtypeoid)) {
            /* already build one, just use it */
            if(IsPackageDependType(oldtypeoid, pkgoid)) {
                newtypeoid = oldtypeoid;
                if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE) {
                    (void)gsplsql_flush_undef_ref_type_dependency(newtypeoid);
                }
            } else {
                ereport(errstate,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("duplicate type name"),
                        errdetail("type name of \"%s\" duplicated with an existed type when build package or function type.", typname),
                        errcause("duplicate type name"),
                        erraction("modify type name")));
            }
        } else {
            RangeVar* r = createRangeVarForCompositeType(schamaname, typname);
            List* codeflist = NULL;
            int attrnum = var_type->attrnum;
            for (int i = 0; i < attrnum; i++) {
                ColumnDef *n = makeNode(ColumnDef);
                n->colname = pstrdup(var_type->attrnames[i]);
                n->typname = make_typename_from_datatype(var_type->types[i]);
                n->inhcount = 0;
                n->is_local = true;
                n->is_not_null = false;
                n->is_from_type = false;
                n->storage = 0;
                n->raw_default = NULL;
                n->cooked_default = NULL;
                n->collClause = NULL;
                n->clientLogicColumnRef=NULL;
                n->collOid = InvalidOid;
                n->constraints = NIL;
                codeflist = lappend(codeflist, (Node*)n);
            }

            DefineCompositeType(r, codeflist);
            newtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname),
                ObjectIdGetDatum(getCurrentNamespace()));
            pfree_ext(r);
            list_free_deep(codeflist);

            /* build dependency on created composite type. */
            buildDependencyForCompositeType(newtypeoid);
            if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE) {
                (void)gsplsql_flush_undef_ref_type_dependency(newtypeoid);
            }
        }

        /* build datatype of the created composite type. */
        newp = plpgsql_build_datatype(newtypeoid, -1, InvalidOid);
        newp->dtype = PLPGSQL_DTYPE_COMPOSITE;
        newp->ttype = PLPGSQL_TTYPE_ROW;

        /* add the composite type to datum and namespace. */
        int varno = plpgsql_adddatum((PLpgSQL_datum*)newp);
        plpgsql_ns_additem(PLPGSQL_NSTYPE_COMPOSITE, varno, typname);

    } else {
        /* we have already build one, just take it from datums. */
        if (ns->itemtype == PLPGSQL_NSTYPE_COMPOSITE) {
            newp = (PLpgSQL_type*)(u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno]);
            /* build a new one, avoid conflict by array or table of */
            newp = plpgsql_build_datatype(newp->typoid, -1, InvalidOid);
            newp->dtype = PLPGSQL_DTYPE_COMPOSITE;
            newp->ttype = PLPGSQL_TTYPE_ROW;
        } else {
            ereport(errstate,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("duplicate declaration"),
                    errdetail("record %s is duplicate with other objects.", var_type->typname)));
            u_sess->plsql_cxt.have_error = true;
        }
    }
    pfree_ext(functypname);
    pfree_ext(pkgtypname);
    return newp;
}

static Oid plpgsql_build_package_record_type(const char* typname, List* list, bool add2namespace)
{
    Oid oldtypeoid = InvalidOid;
    Oid newtypeoid = InvalidOid;
    char* schamaName = NULL;
    Oid pkgOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    Oid pkgNamespaceOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    if (OidIsValid(pkgNamespaceOid)) {
        schamaName = get_namespace_name(pkgNamespaceOid);
    } else {
        pkgNamespaceOid = getCurrentNamespace();
    }
    char* casttypename = CastPackageTypeName(typname,
        pkgOid, true,
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling);
    if (strlen(casttypename) >= NAMEDATALEN ) {
        ereport(errstate,
            (errcode(ERRCODE_NAME_TOO_LONG),
                errmsg("type name too long"),
                errdetail("record name %s with package name %s should be less the %d letters.",
                    typname,
                    u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature,
                    NAMEDATALEN)));
        u_sess->plsql_cxt.have_error = true;
        pfree_ext(casttypename);
    }

    oldtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(casttypename), ObjectIdGetDatum(pkgNamespaceOid));
    bool oldTypeOidIsValid = OidIsValid(oldtypeoid);
    if (oldTypeOidIsValid) {
        /* already build on, just use it */
        if(IsPackageDependType(oldtypeoid, pkgOid)) {
            newtypeoid = oldtypeoid;
            if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE) {
                (void)gsplsql_flush_undef_ref_type_dependency(newtypeoid);
            }
        } else {
            ereport(errstate,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("duplicate type name"),
                    errdetail("type name of \"%s\" duplicated with an existed type when build package type.", casttypename),
                    errcause("duplicate type name"),
                    erraction("modify package type name")));
        }
    } else {
        RangeVar *r = makeNode(RangeVar);
        r->catalogname = NULL;
        r->schemaname = schamaName;
        r->relname = pstrdup(casttypename);
        r->relpersistence = RELPERSISTENCE_PERMANENT;
        r->location = -1;
        r->ispartition = false;
        r->isbucket = false;
        r->buckets = NIL;

        List* codeflist = NULL;
        ListCell* cell = NULL;
        PLpgSQL_rec_attr* attr = NULL;
    
        foreach (cell, list) {
            attr = (PLpgSQL_rec_attr*)lfirst(cell);
            codeflist = lappend(codeflist, make_columnDef_from_attr(attr));
        }

        DefineCompositeType(r, codeflist);
    
        newtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(casttypename),
            ObjectIdGetDatum(getCurrentNamespace()));

        /* build dependency on created composite type. */
        ObjectAddress myself, referenced;
        myself.classId = PackageRelationId;
        myself.objectId = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        myself.objectSubId = 0;
        referenced.classId = TypeRelationId;
        referenced.objectId = newtypeoid;
        referenced.objectSubId = 0;
        recordDependencyOn(&referenced, &myself, DEPENDENCY_AUTO);
        CommandCounterIncrement();
        pfree_ext(r);
        list_free_deep(codeflist);
        if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE) {
            gsplsql_build_ref_type_dependency(newtypeoid);
        }
    }

    PLpgSQL_type *newtype = NULL;
    newtype = plpgsql_build_datatype(newtypeoid, -1, InvalidOid);
    newtype->dtype = PLPGSQL_DTYPE_COMPOSITE;
    newtype->ttype = PLPGSQL_TTYPE_ROW;

    int varno = plpgsql_adddatum((PLpgSQL_datum*)newtype);

    plpgsql_ns_additem(PLPGSQL_NSTYPE_COMPOSITE, varno, casttypename);
    pfree_ext(casttypename);

    return newtypeoid;
}

static void  plpgsql_build_package_array_type(const char* typname,Oid elemtypoid, char arraytype, TypeDependExtend* dependExtend)
{
    char typtyp;
    ObjectAddress myself, referenced;

    char* casttypename = CastPackageTypeName(typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, true,
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling);
    if (strlen(casttypename) >= NAMEDATALEN ) {
        ereport(errstate,
            (errcode(ERRCODE_NAME_TOO_LONG),
                errmsg("type name too long"),
                errdetail("array or nested table type name %s with package name %s should be less the %d letters.",
                    typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature, NAMEDATALEN)));
        u_sess->plsql_cxt.have_error = true;
    }

    Oid pkgNamespaceOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    if (!OidIsValid(pkgNamespaceOid)) {
        pkgNamespaceOid = getCurrentNamespace();
    }

    Oid pkgOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    Oid oldtypeoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(casttypename),
        ObjectIdGetDatum(pkgNamespaceOid));
    bool oldtypeoidIsValid = OidIsValid(oldtypeoid);
    if (oldtypeoidIsValid) {
        /* alread build one, just return */
        if(IsPackageDependType(oldtypeoid, pkgOid)) {
            if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE) {
                (void)gsplsql_flush_undef_ref_type_dependency(oldtypeoid);
            }
            return;
        } else {
            ereport(errstate,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("duplicate type name"),
                    errdetail("type name of \"%s\" duplicated with an existed type when build package type.", casttypename),
                    errcause("duplicate type name"),
                    erraction("modify package type name")));
        }
    }

    if (arraytype == TYPCATEGORY_TABLEOF ||
        arraytype == TYPCATEGORY_TABLEOF_VARCHAR ||
        arraytype == TYPCATEGORY_TABLEOF_INTEGER) {
        if (UNDEFINEDOID != elemtypoid) {
            elemtypoid = get_array_type(elemtypoid);
        }
        typtyp = TYPTYPE_TABLEOF;
    } else {
        typtyp = TYPTYPE_BASE;
    }
    Oid ownerId = InvalidOid;
    ownerId = GetUserIdFromNspId(pkgNamespaceOid);
    if (!OidIsValid(ownerId)) {
        ownerId = GetUserId();
    }

    referenced = TypeCreate(InvalidOid, /* force the type's OID to this */
        casttypename,               /* Array type name */
        pkgNamespaceOid,               /* Same namespace as parent */
        InvalidOid,                 /* Not composite, no relationOid */
        0,                          /* relkind, also N/A here */
        ownerId,                    /* owner's ID */
        -1,                         /* Internal size (varlena) */
        typtyp,               /* Not composite - typelem is */
        arraytype,          /* type-category (array or table of) */
        false,                      /* array types are never preferred */
        DEFAULT_TYPDELIM,           /* default array delimiter */
        F_ARRAY_IN,                 /* array input proc */
        F_ARRAY_OUT,                /* array output proc */
        F_ARRAY_RECV,               /* array recv (bin) proc */
        F_ARRAY_SEND,               /* array send (bin) proc */
        InvalidOid,                 /* typmodin procedure - none */
        InvalidOid,                 /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,         /* array analyze procedure */
        elemtypoid,               /* array element type - the rowtype */
        false,                       /* yes, this is an array type */
        InvalidOid,                 /* this has no array type */
        InvalidOid,                 /* domain base type - irrelevant */
        NULL,                       /* default value - none */
        NULL,                       /* default binary representation */
        false,                      /* passed by reference */
        'd',                        /* alignment - must be the largest! */
        'x',                        /* fully TOASTable */
        -1,                         /* typmod */
        0,                          /* array dimensions for typBaseType */
        false,                      /* Type NOT NULL */
        get_typcollation(elemtypoid),
        dependExtend);

    CommandCounterIncrement();

    /* build dependency on created composite type. */
    myself.classId = PackageRelationId;
    myself.objectId = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    myself.objectSubId = 0;
    recordDependencyOn(&referenced, &myself, DEPENDENCY_AUTO);
    CommandCounterIncrement();
    if (CompileWhich() == PLPGSQL_COMPILE_PACKAGE && typtyp != TYPTYPE_TABLEOF) {
        (void)gsplsql_build_ref_type_dependency(referenced.objectId);
    }
    pfree_ext(casttypename);
}


static void plpgsql_build_package_refcursor_type(const char* typname)
{
    CreateSynonymStmt stmt;
    stmt.replace = true;
    Node* lc = NULL;
    List* synList = NULL;
    List* objList = NULL;
    
    char* casttypename = CastPackageTypeName(typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, true,
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling);
    if (strlen(casttypename) >= NAMEDATALEN ) {
        ereport(errstate,
            (errcode(ERRCODE_NAME_TOO_LONG),
                errmsg("type name too long"),
                errdetail("cursor type name %s with package name %s should be less the %d letters.",
                    typname, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature, NAMEDATALEN)));
        u_sess->plsql_cxt.have_error = true;
    }

    Oid pkgNamespaceOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    if (!OidIsValid(pkgNamespaceOid)) {
        pkgNamespaceOid = getCurrentNamespace();
    }

    HeapTuple tuple = NULL;
    tuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(casttypename), ObjectIdGetDatum(pkgNamespaceOid));
    if (HeapTupleIsValid(tuple)) {
        Oid synOid = HeapTupleGetOid(tuple);
        if(IsPackageDependType(synOid, u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, true)) {
            ReleaseSysCache(tuple);
            return;
        } else {
            ReleaseSysCache(tuple);
            ereport(errstate,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("duplicate type name"),
                    errdetail("type name of \"%s\" duplicated with an existed type when build package type.", casttypename),
                    errcause("duplicate type name"),
                    erraction("modify package type name")));
        }
    }

    char* pkgSchemaName = NULL;
    if (OidIsValid(pkgNamespaceOid)) {
        pkgSchemaName = get_namespace_name(pkgNamespaceOid);
        lc = (Node*)makeString(pkgSchemaName);
        synList = lappend(synList, lc);
    }

    lc = (Node*)makeString(casttypename);
    synList = lappend(synList, lc);

    lc = (Node*)makeString("pg_catalog");
    objList = lappend(objList, lc);
    lc = (Node*)makeString("refcursor");
    objList = lappend(objList, lc);

    stmt.synName = synList;
    stmt.objName = objList;

    CreateSynonym(&stmt);

    CommandCounterIncrement();

    tuple = NULL;
    tuple = SearchSysCache2(SYNONYMNAMENSP, PointerGetDatum(casttypename), ObjectIdGetDatum(pkgNamespaceOid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("synonym \"%s\" does not exist", casttypename)));
    }
    Oid synOid = HeapTupleGetOid(tuple);
    ReleaseSysCache(tuple);

    /* build dependency on Synonym. */
    ObjectAddress myself, referenced;
    myself.classId = PackageRelationId;
    myself.objectId = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    myself.objectSubId = 0;
    referenced.classId = PgSynonymRelationId;
    referenced.objectId = synOid;
    referenced.objectSubId = 0;
    recordDependencyOn(&referenced, &myself, DEPENDENCY_AUTO);
    CommandCounterIncrement();

    pfree_ext(casttypename);
    pfree_ext(pkgSchemaName);

}

static Node* make_columnDef_from_attr(PLpgSQL_rec_attr* attr)
{
    ColumnDef *n = makeNode(ColumnDef);
    n->colname = pstrdup(attr->attrname);
    n->typname = make_typename_from_datatype(attr->type);
    n->inhcount = 0;
    n->is_local = true;
    n->is_not_null = false;
    n->is_from_type = false;
    n->storage = 0;
    n->raw_default = NULL;
    n->cooked_default = NULL;
    n->collClause = NULL;
    n->clientLogicColumnRef=NULL;
    n->collOid = InvalidOid;
    n->constraints = NIL;

    return (Node*)n;
}

static TypeName* make_typename_from_datatype(PLpgSQL_type* datatype)
{
    return makeTypeNameFromOid(datatype->typoid, datatype->atttypmod, datatype->dependExtend);
}

/*
 * Check block starting and ending labels match.
 */
static void
check_labels(const char *start_label, const char *end_label, int end_location)
{
    if (end_label)
    {
        if (!start_label) {
            const char* message = "end label specified for unlabelled block";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("end label \"%s\" specified for unlabelled block",
                            end_label),
                     parser_errposition(end_location)));
        }

        if (start_label == NULL || end_label == NULL) {
            if (errstate == NOTICE) {
                u_sess->plsql_cxt.have_error = true;
                return;
            } else {
                const char* message = "end label specified for unlabelled block";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("end label \"%s\" differs from block's label \"%s\"",
                            end_label, start_label),
                     parser_errposition(end_location)));
            }
        }
        if (strcmp(start_label, end_label) != 0) {
            const char* message = "end label differs from block's label";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("end label \"%s\" differs from block's label \"%s\"",
                            end_label, start_label),
                     parser_errposition(end_location)));
        }
    }
}

/*
 * Read the arguments (if any) for a cursor, followed by the until token
 *
 * If cursor has no args, just swallow the until token and return NULL.
 * If it does have args, we expect to see "( arg [, arg ...] )" followed
 * by the until token, where arg may be a plain expression, or a named
 * parameter assignment of the form argname := expr. Consume all that and
 * return a SELECT query that evaluates the expression(s) (without the outer
 * parens).
 */
static PLpgSQL_expr *
read_cursor_args(PLpgSQL_var *cursor, int until, const char *expected)
{
    PLpgSQL_expr *expr;
    PLpgSQL_row *row;
    int			tok;
    int			argc;
    char	  **argv;
    StringInfoData ds;
    char	   *sqlstart = "SELECT ";
    bool		any_named = false;

    tok = yylex();
    if (cursor->cursor_explicit_argrow < 0)
    {
        /* No arguments expected */
        if (tok == '(') {
            const char* message = "cursor has no arguments";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("cursor \"%s\" has no arguments",
                            cursor->refname),
                     parser_errposition(yylloc)));
        }

        if (tok != until)
            yyerror("syntax error");

        return NULL;
    }

    /* Else better provide arguments */
    if (tok != '(') {
        const char* message = "cursor has arguments";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cursor \"%s\" has arguments",
                        cursor->refname),
                 parser_errposition(yylloc)));
    }

    bool isPkgCur = cursor->ispkg &&
        (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL
        || u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid != cursor->pkg->pkg_oid);
    if (isPkgCur) {
        ereport(errstate,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("package cursor with arguments is only supported to be opened in the same package."),
            errdetail("cursor \"%s.%s\" is only supported to be opened in the package \"%s\"",
                cursor->pkg->pkg_signature,
                cursor->varname == NULL ? cursor->refname : cursor->varname,
                cursor->pkg->pkg_signature),
            errcause("feature not supported"),
            erraction("define this cursor without arguments or open this cursor in same package"),
            parser_errposition(yylloc)));
    }

    /*
     * Read the arguments, one by one.
     */
    if (cursor->ispkg) {
        row = (PLpgSQL_row *) cursor->pkg->datums[cursor->cursor_explicit_argrow];
    } else {
        row = (PLpgSQL_row *) u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[cursor->cursor_explicit_argrow];
    }
    argv = (char **) palloc0(row->nfields * sizeof(char *));

    for (argc = 0; argc < row->nfields; argc++)
    {
        PLpgSQL_expr *item;
        int		endtoken;
        int		argpos;
        int		tok1,
                tok2;
        int		arglocation;

        /* Check if it's a named parameter: "param := value" */
        plpgsql_peek2(&tok1, &tok2, &arglocation, NULL);
        if (tok1 == IDENT && tok2 == COLON_EQUALS)
        {
            char   *argname;
            IdentifierLookup save_IdentifierLookup;

            /* Read the argument name, ignoring any matching variable */
            save_IdentifierLookup = u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup;
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
            yylex();
            argname = yylval.str;
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = save_IdentifierLookup;

            /* Match argument name to cursor arguments */
            for (argpos = 0; argpos < row->nfields; argpos++)
            {
                if (strcmp(row->fieldnames[argpos], argname) == 0)
                    break;
            }
            if (argpos == row->nfields) {
                const char* message = "cursor has no argument named";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("cursor \"%s\" has no argument named \"%s\"",
                                cursor->refname, argname),
                         parser_errposition(yylloc)));
            }

            /*
             * Eat the ":=". We already peeked, so the error should never
             * happen.
             */
            tok2 = yylex();
            if (tok2 != COLON_EQUALS)
                yyerror("syntax error");

            any_named = true;
        }
        else
            argpos = argc;

        if (argv[argpos] != NULL) {
            const char* message = "value for parameter of cursor specified more than once";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("value for parameter \"%s\" of cursor \"%s\" specified more than once",
                            row->fieldnames[argpos], cursor->refname),
                     parser_errposition(arglocation)));
        }

        /*
         * Read the value expression. To provide the user with meaningful
         * parse error positions, we check the syntax immediately, instead of
         * checking the final expression that may have the arguments
         * reordered. Trailing whitespace must not be trimmed, because
         * otherwise input of the form (param -- comment\n, param) would be
         * translated into a form where the second parameter is commented
         * out.
         */
        item = read_sql_construct(',', ')', 0,
                                  ",\" or \")",
                                  sqlstart,
                                  true, true,
                                  false, /* do not trim */
                                  NULL, &endtoken);

        argv[argpos] = item->query + strlen(sqlstart);

        if (endtoken == ')' && !(argc == row->nfields - 1)) {
            const char* message = "not enough arguments for cursor";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("not enough arguments for cursor \"%s\"",
                            cursor->refname),
                     parser_errposition(yylloc)));
        }

        if (endtoken == ',' && (argc == row->nfields - 1)) {
            const char* message = "too many arguments for cursor";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("too many arguments for cursor \"%s\"",
                            cursor->refname),
                     parser_errposition(yylloc)));
        }
    }

    /* Make positional argument list */
    initStringInfo(&ds);
    appendStringInfoString(&ds, sqlstart);
    for (argc = 0; argc < row->nfields; argc++)
    {
        AssertEreport(argv[argc] != NULL,
                            MOD_PLSQL,
                            "It should not be null");

        /*
         * Because named notation allows permutated argument lists, include
         * the parameter name for meaningful runtime errors.
         */
        appendStringInfoString(&ds, argv[argc]);
        if (any_named)
            appendStringInfo(&ds, " AS %s",
                             quote_identifier(row->fieldnames[argc]));
        if (argc < row->nfields - 1)
            appendStringInfoString(&ds, ", ");
    }
    appendStringInfoChar(&ds, ';');

    expr = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(ds.data);
    expr->plan			= NULL;
    expr->paramnos		= NULL;
    expr->ns            = plpgsql_ns_top();
    expr->idx			= (uint32)-1;
    expr->out_param_dno = -1;
    pfree_ext(ds.data);

    /* Next we'd better find the until token */
    tok = yylex();
    if (tok != until)
        yyerror("syntax error");

    return expr;
}

/*
 * Parse RAISE ... USING options
 */
static List *
read_raise_options(void)
{
    List	   *result = NIL;

    for (;;)
    {
        PLpgSQL_raise_option *opt;
        int		tok;

        if ((tok = yylex()) == 0)
            yyerror("unexpected end of function definition");

        opt = (PLpgSQL_raise_option *) palloc(sizeof(PLpgSQL_raise_option));

        if (tok_is_keyword(tok, &yylval,
                           K_ERRCODE, "errcode"))
            opt->opt_type = PLPGSQL_RAISEOPTION_ERRCODE;
        else if (tok_is_keyword(tok, &yylval,
                                K_MESSAGE, "message"))
            opt->opt_type = PLPGSQL_RAISEOPTION_MESSAGE;
        else if (tok_is_keyword(tok, &yylval,
                                K_DETAIL, "detail"))
            opt->opt_type = PLPGSQL_RAISEOPTION_DETAIL;
        else if (tok_is_keyword(tok, &yylval,
                                K_HINT, "hint"))
            opt->opt_type = PLPGSQL_RAISEOPTION_HINT;
        else
            yyerror("unrecognized RAISE statement option");

        tok = yylex();
        if (tok != '=' && tok != COLON_EQUALS)
            yyerror("syntax error, expected \"=\"");

        opt->expr = read_sql_expression2(',', ';', ", or ;", &tok);

        result = lappend(result, opt);

        if (tok == ';')
            break;
    }

    return result;
}

/* 
 * Parse SIGNAL/RESIGNAL ... SQLSTATE
 */
static void read_signal_sqlstate(PLpgSQL_stmt_signal *newp, int tok)
{
    char *sqlstate_value;

    if (tok == 0 || tok == ';') {
        yyerror("unexpected end of function definition");
    }

    if (tok != SCONST && tok != T_WORD) {
        yyerror("syntax error, the expected value is a string.");
    }

    if (tok == T_WORD) {
        if (strcmp(yylval.str, "value") == 0) {
            if (yylex() != SCONST) {
                yyerror("syntax error, the expected value is a string.");
            }
        } else {
            yyerror("syntax error, the expected word is value.");
        }
    }

    sqlstate_value = yylval.str;

    if (strlen(sqlstate_value) != 5 ||
        strspn(sqlstate_value, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5 ||
        strncmp(sqlstate_value, "00", 2) == 0) {
            const char *message = "bad SQLSTATE";
            InsertErrorMessage(message, plpgsql_yylloc);
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                        errmsg("bad SQLSTATE '%s'", sqlstate_value)));
    }

    newp->sqlstate = sqlstate_value;
    newp->sqlerrstate = MAKE_SQLSTATE(sqlstate_value[0],
                                      sqlstate_value[1],
                                      sqlstate_value[2],
                                      sqlstate_value[3],
                                      sqlstate_value[4]);
    newp->condname = NULL;
    return;
}

/*
 * Parse SIGNAL/RESIGNAL ... condname
 */
static void read_signal_condname(PLpgSQL_stmt_signal *newp, int tok)
{
    char *condname = NULL;

    if (tok == T_WORD) {
        condname = yylval.word.ident;
    } else if (tok == T_DATUM) {
        condname = NameOfDatum(&yylval.wdatum);
    } else {
        yyerror("syntax error, the condition name is expected.");
    }

    PLpgSQL_condition *newcon = plpgsql_parse_err_condition_b_signal(condname);
    if (newcon->isSqlvalue) {
        const char *message = "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                errmsg("SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE")));
    }
    newp->sqlerrstate = newcon->sqlerrstate;
    newp->sqlstate = newcon->sqlstate;
    newp->condname = newcon->condname;
    return;
}

/*
 * Parse SIGNAL/RESIGNAL ... set
 */
static void read_signal_set(PLpgSQL_stmt_signal *newp, int tok)
{
    if (tok == T_WORD) {
        if (pg_strcasecmp(yylval.word.ident, "set") == 0) {
            newp->cond_info_item = read_signal_items();
        } else {
            yyerror("invalid keyword");
        }
    } else if (tok == ';') {
        newp->cond_info_item = NIL;
    } else {
        yyerror("syntax error");
    }
    return;
}

/*
 * Parse SIGNAL/RESIGNAL ... SET items
 */
static List *read_signal_items(void)
{
    List *result = NIL;

    for (;;) {
        PLpgSQL_signal_info_item *item;
        int tok;
        ListCell *lc;

        if ((tok = yylex()) == 0) {
            yyerror("unexpected end of siganl information definition");
        }

        item = (PLpgSQL_signal_info_item *)palloc(sizeof(PLpgSQL_signal_info_item));

        if (tok_is_keyword(tok, &yylval, K_CLASS_ORIGIN, "class_origin")) {
            item->con_info_value = PLPGSQL_CLASS_ORIGIN;
            item->con_name = pstrdup("CLASS_ORIGIN");
        } else if (tok_is_keyword(tok, &yylval, K_SUBCLASS_ORIGIN, "subclass_origin")) {
            item->con_info_value = PLPGSQL_SUBCLASS_ORIGIN;
            item->con_name = pstrdup("SUBCLASS_ORIGIN");
        } else if (tok_is_keyword(tok, &yylval, K_MESSAGE_TEXT, "message_text")) {
            item->con_info_value = PLPGSQL_MESSAGE_TEXT;
            item->con_name = pstrdup("MESSAGE_TEXT");
        } else if (tok_is_keyword(tok, &yylval, K_MYSQL_ERRNO, "mysql_errno")) {
            item->con_info_value = PLPGSQL_MYSQL_ERRNO;
            item->con_name = pstrdup("MYSQL_ERRNO");
        } else if (tok_is_keyword(tok, &yylval, K_CONSTRAINT_CATALOG, "constraint_catalog")) {
            item->con_info_value = PLPGSQL_CONSTRAINT_CATALOG;
            item->con_name = pstrdup("CONSTRAINT_CATALOG");
        } else if (tok_is_keyword(tok, &yylval, K_CONSTRAINT_SCHEMA, "constraint_schema")) {
            item->con_info_value = PLPGSQL_CONSTRAINT_SCHEMA;
            item->con_name = pstrdup("CONSTRAINT_SCHEMA");
        } else if (tok_is_keyword(tok, &yylval, K_CONSTRAINT_NAME, "constraint_name")) {
            item->con_info_value = PLPGSQL_CONSTRAINT_NAME;
            item->con_name = pstrdup("CONSTRAINT_NAME");
        } else if (tok_is_keyword(tok, &yylval, K_CATALOG_NAME, "catalog_name")) {
            item->con_info_value = PLPGSQL_CATALOG_NAME;
            item->con_name = pstrdup("CATALOG_NAME");
        } else if (tok_is_keyword(tok, &yylval, K_SCHEMA_NAME, "schema_name")) {
            item->con_info_value = PLPGSQL_SCHEMA_NAME;
            item->con_name = pstrdup("SCHEMA_NAME");
        } else if (tok_is_keyword(tok, &yylval, K_TABLE_NAME, "table_name")) {
            item->con_info_value = PLPGSQL_TABLE_NAME;
            item->con_name = pstrdup("TABLE_NAME");
        } else if (tok_is_keyword(tok, &yylval, K_COLUMN_NAME, "column_name")) {
            item->con_info_value = PLPGSQL_COLUMN_NAME;
            item->con_name = pstrdup("COLUMN_NAME");
        } else if (tok_is_keyword(tok, &yylval, K_CURSOR_NAME, "corsor_name")) {
            item->con_info_value = PLPGSQL_CURSOR_NAME;
            item->con_name = pstrdup("CURSOR_NAME");
        } else {
            yyerror("syntax error, unrecognized SIGNAL/RESIGNAL statement item");
        }

        tok = yylex();
        if (tok != '=') {
            yyerror("syntax error, expected \"=\"");
        }

        item->expr = read_sql_expression2(',', ';', ", or ;", &tok);

        foreach (lc, result) {
            PLpgSQL_signal_info_item *signal_item = (PLpgSQL_signal_info_item *)lfirst(lc);
            if (signal_item->con_info_value == item->con_info_value) {
                const char *message = "Duplicate condition information item";
                InsertErrorMessage(message, plpgsql_yylloc);
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                            errmsg("Duplicate condition information item '%s'", item->con_name)));
            }
        }

        result = lappend(result, item);

        if (tok == ';') {
            break;
        }
    }

    return result;
}

/*
 * Fix up CASE statement
 */
static PLpgSQL_stmt *
make_case(int location, PLpgSQL_expr *t_expr,
          List *case_when_list, List *else_stmts)
{
    PLpgSQL_stmt_case 	*newp;

    newp = (PLpgSQL_stmt_case 	*)palloc(sizeof(PLpgSQL_stmt_case));
    newp->cmd_type = PLPGSQL_STMT_CASE;
    newp->lineno = plpgsql_location_to_lineno(location);
    newp->t_expr = t_expr;
    newp->t_varno = 0;
    newp->case_when_list = case_when_list;
    newp->have_else = (else_stmts != NIL);
    newp->sqlString = plpgsql_get_curline_query();
    /* Get rid of list-with-NULL hack */
    if (list_length(else_stmts) == 1 && linitial(else_stmts) == NULL)
        newp->else_stmts = NIL;
    else
        newp->else_stmts = else_stmts;

    /*
     * When test expression is present, we create a var for it and then
     * convert all the WHEN expressions to "VAR IN (original_expression)".
     * This is a bit klugy, but okay since we haven't yet done more than
     * read the expressions as text.  (Note that previous parsing won't
     * have complained if the WHEN ... THEN expression contained multiple
     * comma-separated values.)
     */
    if (t_expr)
    {
        char	varname[32];
        PLpgSQL_var *t_var;
        ListCell *l;

        /* use a name unlikely to collide with any user names */
        snprintf(varname, sizeof(varname), "__Case__Variable_%d__",
                 u_sess->plsql_cxt.curr_compile_context->plpgsql_nDatums);

        /*
         * We don't yet know the result datatype of t_expr.  Build the
         * variable as if it were INT4; we'll fix this at runtime if needed.
         */
        t_var = (PLpgSQL_var *)
            plpgsql_build_variable(varname, newp->lineno,
                                   plpgsql_build_datatype(INT4OID,
                                                          -1,
                                                          InvalidOid),
                                   true, true);
        newp->t_varno = t_var->dno;

        foreach(l, case_when_list)
        {
            PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
            PLpgSQL_expr *expr = cwt->expr;
            StringInfoData	ds;

            /* copy expression query without SELECT keyword (expr->query + 7) */
            AssertEreport(strncmp(expr->query, "SELECT ", 7) == 0,
                                MOD_PLSQL,
                                "copy expression query without SELECT keyword");

            /* And do the string hacking */
            initStringInfo(&ds);

            appendStringInfo(&ds, "SELECT \"%s\" IN (%s)",
                             varname, expr->query + 7);

            pfree_ext(expr->query);
            expr->query = pstrdup(ds.data);
            /* Adjust expr's namespace to include the case variable */
            expr->ns = plpgsql_ns_top();

            pfree_ext(ds.data);
        }
    }

    return (PLpgSQL_stmt *) newp;
}

static PLpgSQL_stmt *
make_callfunc_stmt_no_arg(const char *sqlstart, int location, bool withsemicolon, List* funcNameList)
{
    char *cp[3] = {0};
    HeapTuple proctup = NULL;
    Oid *p_argtypes = NULL;
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    int narg = 0;
    List *funcname = NIL;
    PLpgSQL_expr* expr = NULL;
    FuncCandidateList clist = NULL;
    StringInfoData func_inparas;
    char *quoted_sqlstart = NULL;
    MemoryContext oldCxt = NULL;

    /*
     * the function make_callfunc_stmt_no_arg is only to assemble a sql statement, 
     * so the context is set to tmp context. 
     */
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.curr_compile_context->compile_tmp_cxt);
    /* get the function's name. */
    if (funcNameList == NULL) {
        plpgsql_parser_funcname(sqlstart, cp, 3);
    } else {
        funcname = funcNameList;
    }
    if (funcNameList == NULL) {
        if (cp[2] != NULL && cp[2][0] != '\0')
            funcname = list_make3(makeString(cp[0]), makeString(cp[1]), makeString(cp[2]));
        else if (cp[1] && cp[1][0] != '\0')
            funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
        else
            funcname = list_make1(makeString(cp[0]));
    }

    /* search the function */
    clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
    if (clist == NULL)
    {
        const char* message = "function doesn't exist";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function \"%s\" doesn't exist ", sqlstart)));
    }

    proctup = SearchSysCache(PROCOID,
                            ObjectIdGetDatum(clist->oid),
                            0, 0, 0);
    /* function may be deleted after clist be searched. */
    if (!HeapTupleIsValid(proctup))
    {
        const char* message = "function doesn't exist";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function \"%s\" doesn't exist ", sqlstart)));
    }
    /* get the all args informations, only "in" parameters if p_argmodes is null */
    narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
    int default_args = ((Form_pg_proc)GETSTRUCT(proctup))->pronargdefaults;
    ReleaseSysCache(proctup);

    // For function with default values, SQL will handle the arg with default value.
    if (narg != 0 && narg > default_args) {
        const char* message = "function has no enough parameters";
        InsertErrorMessage(message, plpgsql_yylloc);
        ereport(errstate,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s has no enough parameters", sqlstart)));
    }

    initStringInfo(&func_inparas);

    appendStringInfoString(&func_inparas, "CALL ");

    quoted_sqlstart = NameListToQuotedString(funcname);
    appendStringInfoString(&func_inparas, quoted_sqlstart);
    pfree_ext(quoted_sqlstart);

    appendStringInfoString(&func_inparas, "(");

    appendStringInfoString(&func_inparas, ")");

    /* read the end token */
    if (!withsemicolon) {
        yylex();
    }
    (void)MemoryContextSwitchTo(oldCxt);

    /* generate the expression */
    expr = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype = PLPGSQL_DTYPE_EXPR;
    expr->query = pstrdup(func_inparas.data);
    expr->plan = NULL;
    expr->paramnos = NULL;
    expr->ns = plpgsql_ns_top();
    expr->idx = (uint32)-1;
    expr->out_param_dno = -1;

    PLpgSQL_stmt_perform *perform = NULL;
    perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
    perform->cmd_type = PLPGSQL_STMT_PERFORM;
    perform->lineno = plpgsql_location_to_lineno(location);
    perform->expr = expr;
    perform->sqlString = plpgsql_get_curline_query();

    return (PLpgSQL_stmt *)perform;
}

/*
 * Brief		: special handling of dbms_lob.open and dbms_lob.close 
 * Description	: open and close are keyword, which need to be handled separately
 * Notes		: 
 */ 
static PLpgSQL_stmt *
parse_lob_open_close(int location)
{
	StringInfoData func;
	int tok = yylex();
	char *mode = NULL;
	bool is_open = false;
	PLpgSQL_expr *expr = NULL;
	PLpgSQL_stmt_perform *perform = NULL;

	initStringInfo(&func);
	appendStringInfoString(&func, "CALL DBMS_LOB.");
	tok = yylex();
	if (K_OPEN == tok)
	{
		is_open = true;
		appendStringInfoString(&func,"OPEN(");
	}
	else
		appendStringInfoString(&func,"CLOSE(");

	if ('(' == (tok = yylex()))
	{
		tok = yylex();
		if (T_DATUM == tok)
			appendStringInfoString(&func, NameOfDatum(&yylval.wdatum));
		else if (T_PLACEHOLDER == tok)
			appendStringInfoString(&func, yylval.word.ident);
		else
			yyerror("syntax error");

		if (is_open)
		{
			if (',' == (tok = yylex()) && T_CWORD == (tok = yylex()))
			{
				mode = NameListToString(yylval.cword.idents);
				if (strcasecmp(mode, "DBMS_LOB.LOB_READWRITE") != 0 
					&& strcasecmp(mode, "DBMS_LOB.LOB_READWRITE") != 0)
					yyerror("syntax error");
				else if (!(')' == yylex() && ';' == yylex()))
					yyerror("syntax error");

				appendStringInfoChar(&func, ')');
			}
			else
				yyerror("syntax error");
		}
		else if(')' == yylex() && ';' == yylex())
			appendStringInfoChar(&func, ')');
		else
			yyerror("syntax error");

		expr = (PLpgSQL_expr *)palloc0(sizeof(PLpgSQL_expr));
		expr->dtype = PLPGSQL_DTYPE_EXPR;
		expr->query = pstrdup(func.data);
		expr->plan = NULL;
		expr->paramnos = NULL;
		expr->ns = plpgsql_ns_top();
        expr->idx = (uint32)-1;
        expr->out_param_dno = -1;

		perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
		perform->cmd_type = PLPGSQL_STMT_PERFORM;
		perform->lineno = plpgsql_location_to_lineno(location);
		perform->expr = expr;
        perform->sqlString = plpgsql_get_curline_query();
		return (PLpgSQL_stmt *)perform;
	}
	yyerror("syntax error");
	return NULL;
}

static void raw_parse_package_function_callback(void *arg)
{
    sql_error_callback_arg *cbarg = (sql_error_callback_arg*)arg;
    int cur_pos = geterrposition();

    if (cur_pos > cbarg->leaderlen)
    {
        cur_pos += cbarg->location - cbarg->leaderlen;
        errposition(cur_pos);
    }
}

static void raw_parse_package_function(char* proc_str, int location, int leaderlen)
{
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL)
    {
        sql_error_callback_arg cbarg;
        ErrorContextCallback  syntax_errcontext;
        List* raw_parsetree_list = NULL;
        u_sess->plsql_cxt.plpgsql_yylloc = plpgsql_yylloc;
        u_sess->plsql_cxt.rawParsePackageFunction = true;

        cbarg.location = location;
        cbarg.leaderlen = leaderlen;

        syntax_errcontext.callback = raw_parse_package_function_callback;
        syntax_errcontext.arg = &cbarg;
        syntax_errcontext.previous = t_thrd.log_cxt.error_context_stack;
        t_thrd.log_cxt.error_context_stack = &syntax_errcontext;
        raw_parsetree_list = raw_parser(proc_str);
        /* Restore former ereport callback */
        t_thrd.log_cxt.error_context_stack = syntax_errcontext.previous;

        CreateFunctionStmt* stmt;
        u_sess->plsql_cxt.rawParsePackageFunction = false;
        int rc = 0;
        if (raw_parsetree_list == NULL) {
            return;
        }
        stmt = (CreateFunctionStmt *)linitial(raw_parsetree_list);
        stmt->queryStr = proc_str;
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->is_spec_compiling) {
            stmt->isPrivate = false;
        } else {
            stmt->isPrivate = true;
        }
        if (u_sess->parser_cxt.isFunctionDeclare) {
            stmt->isFunctionDeclare = true;
        } else {
            stmt->isFunctionDeclare = false;
        }

        rc = CompileWhich();
        if (rc == PLPGSQL_COMPILE_PACKAGE) {
            stmt->startLineNumber = u_sess->plsql_cxt.procedure_start_line;
            stmt->firstLineNumber = u_sess->plsql_cxt.procedure_first_line;
        }
        /* check function name */
        CheckDuplicateFunctionName(stmt->funcname);
        List *proc_list = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->proc_list;
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->proc_list = lappend(proc_list,stmt);
    } else {
        yyerror("kerword PROCEDURE only use in package compile");
    }
}

static void CheckDuplicateFunctionName(List* funcNameList)
{
    char* schemaname = NULL;
    char* pkgname = NULL;
    char* funcname = NULL;
    DeconstructQualifiedName(funcNameList, &schemaname, &funcname, &pkgname);
    if (plpgsql_ns_lookup(plpgsql_ns_top(), true, funcname, NULL, NULL, NULL) != NULL)
        yyerror("duplicate declaration");
}

static void IsInPublicNamespace(char* varname) {
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile == NULL && 
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        if (plpgsql_ns_lookup(pkg->public_ns, true, varname, NULL, NULL, NULL) != NULL) {
            const char* message = "duplicate declaration";
            InsertErrorMessage(message, plpgsql_yylloc);
            ereport(errstate,
                (errmodule(MOD_PLSQL), errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("duplicate declaration"),
                    errdetail("name \"%s\" already defined", varname),
                    errcause("name maybe defined in public variable or type"),
                    erraction("rename variable or type")));
        }
    }
}

static void CheckDuplicateCondition (char* name) {
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_conditions != NULL) {
        PLpgSQL_condition* cond = u_sess->plsql_cxt.curr_compile_context->plpgsql_conditions;
        while(cond) {
            if (strcmp(cond->condname, name) == 0) {
                const char* message = "duplicate declaration";
                InsertErrorMessage(message, plpgsql_yylloc);
                ereport(errstate,
                    (errmodule(MOD_PLSQL), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("duplicate declaration"),
                        errdetail("condition \"%s\" already defined", name)));
                break;
            }
            cond = cond->next;
        }
    }
}
static void AddNamespaceIfNeed(int dno, char* ident)
{
    if (getCompileStatus() != COMPILIE_PKG_FUNC) {
        return;
    }

    if (ident == NULL) {
        yyerror("null string when add package type to procedure namespace");
    }

    if (plpgsql_ns_lookup(plpgsql_ns_top(), true, ident, NULL, NULL, NULL) != NULL) {
        return;
    }

    if (dno == -1) {
        plpgsql_ns_additem(PLPGSQL_NSTYPE_REFCURSOR, 0, ident);
        return;
    }

    PLpgSQL_datum* datum = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[dno];
    switch(datum->dtype) {
        case PLPGSQL_DTYPE_RECORD_TYPE : {
            plpgsql_ns_additem(PLPGSQL_NSTYPE_RECORD, dno, ident);
        } break;
        case PLPGSQL_DTYPE_VARRAY : {
            plpgsql_ns_additem(PLPGSQL_NSTYPE_VARRAY, dno, ident);
        } break;
        case PLPGSQL_DTYPE_TABLE : {
            plpgsql_ns_additem(PLPGSQL_NSTYPE_TABLE, dno, ident);
        } break;
        default : {
            yyerror("not recognized type when add package type to procedure namespace");
        } break;
    }

    return;
}

static void AddNamespaceIfPkgVar(const char* ident, IdentifierLookup save_IdentifierLookup)
{
    if (getCompileStatus() != COMPILIE_PKG_FUNC) {
        return;
    }

    /* only declare session need to add */
    if (save_IdentifierLookup != IDENTIFIER_LOOKUP_DECLARE) {
        return;
    }

    if (ident == NULL) {
        yyerror("null string when add package variable to procedure namespace");
    }

    if (plpgsql_ns_lookup(plpgsql_ns_top(), true, ident, NULL, NULL, NULL) != NULL) {
        return;
    }

    PLpgSQL_nsitem* ns = NULL;
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->plpgsql_curr_compile_package != NULL) {
        PLpgSQL_package* pkg = curr_compile->plpgsql_curr_compile_package;
        ns = plpgsql_ns_lookup(pkg->public_ns, false, ident, NULL, NULL, NULL);
        if (ns == NULL) {
            ns = plpgsql_ns_lookup(pkg->private_ns, false, ident, NULL, NULL, NULL);
        }
    }
    if (ns != NULL && (ns->itemtype == PLPGSQL_NSTYPE_VAR || ns->itemtype == PLPGSQL_NSTYPE_ROW)) {
        plpgsql_ns_additem(ns->itemtype, ns->itemno, ident);
    }

    return;
}

static void get_datum_tok_type(PLpgSQL_datum* target, int* tok_flag)
{
    if (target->dtype == PLPGSQL_DTYPE_VAR) {
        PLpgSQL_var* var = (PLpgSQL_var*)(target);
        if (var != NULL && var->datatype != NULL &&
            var->datatype->typinput.fn_oid == F_ARRAY_IN) {
            if (var->datatype->collectionType == PLPGSQL_COLLECTION_TABLE) {
                *tok_flag = PLPGSQL_TOK_TABLE_VAR;
            } else {
                *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
            }
        } else if (var != NULL && var->datatype &&
            var->datatype->typinput.fn_oid == F_DOMAIN_IN) {
            HeapTuple type_tuple =
                SearchSysCache1(TYPEOID, ObjectIdGetDatum(var->datatype->typoid));
            if (HeapTupleIsValid(type_tuple)) {
                Form_pg_type type_form = (Form_pg_type)GETSTRUCT(type_tuple);
                if (F_ARRAY_OUT == type_form->typoutput) {
                    *tok_flag = PLPGSQL_TOK_VARRAY_VAR;
                }
            }
            ReleaseSysCache(type_tuple);
        }
    }
}

static void plpgsql_cast_reference_list(List* idents, StringInfoData* ds, bool isPkgVar)
{
    char* word = NULL;
    if (isPkgVar) {
        word = strVal(linitial(idents));
        appendStringInfoString(ds, "(");
        appendStringInfoString(ds, word);
        appendStringInfoString(ds, ".");
        word = strVal(lsecond(idents));
        appendStringInfoString(ds, word);
        word = strVal(lthird(idents));
        appendStringInfoString(ds, ".");
        appendStringInfoString(ds, word);
        appendStringInfoString(ds, ")");
        word = strVal(lfourth(idents));
        appendStringInfoString(ds, ".");
        appendStringInfoString(ds, word);
        appendStringInfoString(ds, " ");
    } else {
        word = strVal(linitial(idents));
        appendStringInfoString(ds, "(");
        appendStringInfoString(ds, word);
        appendStringInfoString(ds, ".");
        word = strVal(lsecond(idents));
        appendStringInfoString(ds, word);
        appendStringInfoString(ds, ")");
        word = strVal(lthird(idents));
        appendStringInfoString(ds, ".");
        appendStringInfoString(ds, word);
        if (list_length(idents) == 4) {
            word = strVal(lfourth(idents));
            appendStringInfoString(ds, ".");
            appendStringInfoString(ds, word);
        }
        appendStringInfoString(ds, " ");
    }
}

static void CastArrayNameToArrayFunc(StringInfoData* ds, List* idents, bool needDot)
{
    char* arrayName1 = NULL;
    char* arrayName2 = NULL;
    char* arrayName3 = NULL;

    switch (list_length(idents)) {
        case 2: {
            arrayName1 = strVal(linitial(idents));
            appendStringInfo(ds, "\"%s\"", arrayName1);
        } break;
        case 3: {
            arrayName1 = strVal(linitial(idents));
            arrayName2 = strVal(lsecond(idents));
            appendStringInfo(ds, "\"%s\".\"%s\"", arrayName1, arrayName2);
        } break;
        case 4: {
            arrayName1 = strVal(linitial(idents));
            arrayName2 = strVal(lsecond(idents));
            arrayName3 = strVal(lthird(idents));
            appendStringInfo(ds, "\"%s\".\"%s\".\"%s\"", arrayName1, arrayName2, arrayName3);
        } break;
        default: {
            yyerror("syntax error of array functions");
        } break;
    }

    if (needDot) {
        appendStringInfo(ds, ", ");
    } else {
        appendStringInfo(ds, " ");
    }

    return;
}

static bool PkgVarNeedCast(List* idents)
{
    /* only pkg.row.col1.col2 need cast */
    if (list_length(idents) != 4) {
        return false;
    }

    char* word1 = strVal(linitial(idents));
    char* word2 = strVal(lsecond(idents));
    char* word3 = strVal(lthird(idents));
    PLpgSQL_nsitem* ns = NULL;
    int nnames = 0;

    ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, word1, word2, word3, &nnames);
    /* nnames = 2, means pkg.row.col2.col2 form */
    if (nnames == 2) {
        return true;
    }

    return false;

}

static void check_autonomous_nest_tablevar(PLpgSQL_var* var)
{
    if (unlikely(var->ispkg && var->nest_table != NULL &&
                 u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile != NULL &&
                 u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->is_autonomous)) {
        ereport(errstate, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("Un-support feature: nest tableof variable \"%s\" not support pass through autonm function",
                                  var->refname)));
    }
}

static void SetErrorState() 
{
#ifndef ENABLE_MULTIPLE_NODES
        if (u_sess->attr.attr_common.plsql_show_all_error) {
            errstate = NOTICE;
        } else {
            errstate = ERROR;
        }
#else
        errstate = ERROR;
#endif
}

static bool need_build_row_for_func_arg(PLpgSQL_rec **rec, PLpgSQL_row **row, int out_arg_num, int all_arg, int *varnos, char *p_argmodes)
{
    /* out arg number > 1 should build a row */
    if (out_arg_num > 1) {
        return true;
    }

    /* no row or rec, should build a row */
    if (*rec == NULL && *row == NULL) {
        return true;
    }

    /* no out arg, need not build */
    if (out_arg_num == 0) {
        return false;
    }
  
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    PLpgSQL_datum *tempDatum = NULL;
    for (int i = 0; i < all_arg; i++) {
        if (p_argmodes[i] == 'i') {
            continue;
        }

        /* out param no destination */
        if (varnos[i] == -1) {
            *rec = NULL;
            *row = NULL;
            return false;
        }

        tempDatum = curr_compile->plpgsql_Datums[varnos[i]];
        /* the out param is the rec or row */
        if (tempDatum == (PLpgSQL_datum*)(*rec) || tempDatum == (PLpgSQL_datum*)(*row)) {
            return false;
        }
        /* the out param is scalar, need build row */
        if (tempDatum->dtype == PLPGSQL_DTYPE_VAR
            || tempDatum->dtype == PLPGSQL_DTYPE_RECFIELD
            || tempDatum->dtype == PLPGSQL_DTYPE_ASSIGNLIST) {
            return true;
        }

        /* need not build a new row, but need to replace the correct row */
        if (tempDatum->dtype == PLPGSQL_DTYPE_ROW || tempDatum->dtype == PLPGSQL_DTYPE_RECORD) {
            *row = (PLpgSQL_row *)tempDatum;
            return false;
        }
        if (tempDatum->dtype == PLPGSQL_DTYPE_REC) {
            *rec = (PLpgSQL_rec *)tempDatum;
            return false;
        }

        /* arrive here, means out param invalid, set row and rec to null*/
        *rec = NULL;
        *row = NULL;
        return false;
    }

    /* should not arrive here */
    *rec = NULL;
    *row = NULL;
    return false;
}

#ifndef ENABLE_MULTIPLE_NODES
static void BuildForQueryVariable(PLpgSQL_expr* expr, PLpgSQL_row **row, PLpgSQL_rec **rec,
    const char* refname, int lineno)
{
    TupleDesc desc = getCursorTupleDesc(expr, true);
    if (desc == NULL || desc->natts == 0 || checkAllAttrName(desc)) {
        PLpgSQL_type dtype;
        dtype.ttype = PLPGSQL_TTYPE_REC;
        *rec = (PLpgSQL_rec *)
        plpgsql_build_variable(refname, lineno, &dtype, true);
    } else {
        *row = build_row_from_tuple_desc(refname, lineno, desc);
    }
}
#endif

static PLpgSQL_stmt *
funcname_is_call(const char* name, int location)
{
    PLpgSQL_stmt *stmt = NULL;
    bool isCallFunc = false;
    bool FuncNoarg = false;
    if(plpgsql_is_token_match(';'))
    {
        FuncNoarg = true;
    }
    isCallFunc = is_function(name, false, FuncNoarg);
    if(isCallFunc)
    {
        if(FuncNoarg)
        {
            stmt = make_callfunc_stmt_no_arg(name, location);
        }
        else
        {
            stmt = make_callfunc_stmt(name, location, false, false);
            if (stmt->cmd_type == PLPGSQL_STMT_PERFORM)
            {
                ((PLpgSQL_stmt_perform *)stmt)->expr->is_funccall = true;
            }
            else if (stmt->cmd_type == PLPGSQL_STMT_EXECSQL)
            {
                ((PLpgSQL_stmt_execsql *)stmt)->sqlstmt->is_funccall = true;
            }
        }
    }

    return stmt;
}

static void processFunctionRecordOutParam(int varno, Oid funcoid, int* outparam)
{
    if (varno != -1 && is_function_with_plpgsql_language_and_outparam(funcoid)) {
        int dtype = u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[varno]->dtype;
        if (dtype == PLPGSQL_DTYPE_ROW) {
            *outparam = yylval.wdatum.dno;
        }
    }
}
