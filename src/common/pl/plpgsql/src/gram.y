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

#include "plpgsql.h"

#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
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
#include "utils/memutils.h"
#include "utils/syscache.h"

#include <limits.h>


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
    u_sess->plsql_cxt.goto_labels = lappend(u_sess->plsql_cxt.goto_labels, (void *)gl);
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
                                            int * tok);
static void 	yylex_outparam(char** fieldnames,
                               int *varnos,
                               int nfields,
                               PLpgSQL_row **row,
                               PLpgSQL_rec **rec,
                               int *token,
                               bool overload = false);

static bool is_function(const char *name, bool is_assign, bool no_parenthesis);
static void 	plpgsql_parser_funcname(const char *s, char **output,
                                        int numidents);
static PLpgSQL_stmt 	*make_callfunc_stmt(const char *sqlstart,
                                int location, bool is_assign);
static PLpgSQL_stmt 	*make_callfunc_stmt_no_arg(const char *sqlstart, int location);
static PLpgSQL_expr 	*read_sql_construct5(int until,
                                             int until2,
                                             int until3,
                                             int until4,
                                             int until5,
                                             const char *expected,
                                             const char *sqlstart,
                                             bool isexpression,
                                             bool valid_sql,
                                             bool trim,
                                             int *startloc,
                                             int *endtoken);
                                             
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
static	void			 check_assignable(PLpgSQL_datum *datum, int location);
static	void			 read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row,
                                          bool *strict);
static	PLpgSQL_row		*read_into_scalar_list(char *initial_name,
                                               PLpgSQL_datum *initial_datum,
                                               int initial_location);
static	PLpgSQL_row		*read_into_array_scalar_list(char *initial_name,
                                               PLpgSQL_datum *initial_datum,
                                               int initial_location);
static void             read_using_target(List **in_expr, PLpgSQL_row **out_row);

static PLpgSQL_row  *read_into_placeholder_scalar_list(char *initial_name,
                      int initial_location);

static	PLpgSQL_row		*make_scalar_list1(char *initial_name,
                                           PLpgSQL_datum *initial_datum,
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
static  bool            last_pragma;

%}

%expect 0
%name-prefix="plpgsql_yy"
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
        struct
        {
            char *name;
            int  lineno;
        }						varname;
        struct
        {
            char *name;
            int  lineno;
            PLpgSQL_datum   *scalar;
            PLpgSQL_rec		*rec;
            PLpgSQL_row		*row;
        }						forvariable;
        struct
        {
            char *label;
            int  n_initvars;
            int  *initvarnos;
            bool autonomous;
        }						declhdr;
        struct
        {
            List *stmts;
            char *end_label;
            int   end_label_location;
        }						loop_body;
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
        PLpgSQL_rec_attr	*recattr;
}

%type <declhdr> decl_sect
%type <varname> decl_varname
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval decl_rec_defval decl_cursor_query
%type <dtype>	decl_datatype
%type <oid>		decl_collate
%type <datum>	decl_cursor_args
%type <list>	decl_cursor_arglist
%type <nsitem>	decl_aliasitem

%type <expr>	expr_until_semi expr_until_rightbracket
%type <expr>	expr_until_then expr_until_loop opt_expr_until_when
%type <expr>	opt_exitcond

%type <ival>	assign_var foreach_slice
%type <var>		cursor_variable
%type <datum>	decl_cursor_arg
%type <forvariable>	for_variable
%type <stmt>	for_control forall_control

%type <str>		any_identifier opt_block_label opt_label goto_block_label label_name

%type <list>	proc_sect proc_stmts stmt_elsifs stmt_else forall_body
%type <loop_body>	loop_body
%type <stmt>	proc_stmt pl_block
%type <stmt>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit stmt_goto label_stmts label_stmt
%type <stmt>	stmt_return stmt_raise stmt_execsql
%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag
%type <stmt>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <stmt>	stmt_commit stmt_rollback
%type <stmt>	stmt_case stmt_foreach_a

%type <list>	proc_exceptions
%type <exception_block> exception_sect
%type <exception>	proc_exception
%type <condition>	proc_conditions proc_condition

%type <casewhen>	case_when
%type <list>	case_when_list opt_case_else

%type <boolean>	getdiag_area_opt
%type <list>	getdiag_list
%type <diagitem> getdiag_list_item
%type <ival>	getdiag_item getdiag_target
%type <ival>	varray_var
%type <ival>	record_var
%type <expr>	expr_until_parenthesis
%type <ival>	opt_scrollable
%type <fetch>	opt_fetch_direction

%type <keyword>	unreserved_keyword

%type <list>	record_attr_list
%type <recattr>	record_attr


/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op CmpOp COMMENTSTRING
%token <ival>	ICONST PARAM
%token			TYPECAST ORA_JOINOP DOT_DOT COLON_EQUALS PARA_EQUALS

/*
 * Other tokens recognized by plpgsql's lexer interface layer (pl_scanner.c).
 */
%token <word>		T_WORD		/* unrecognized simple identifier */
%token <cword>		T_CWORD		/* unrecognized composite identifier */
%token <wdatum>		T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token <word>		T_PLACEHOLDER		/* place holder , for IN/OUT parameters */
%token <wdatum>		T_VARRAY T_ARRAY_FIRST  T_ARRAY_LAST  T_ARRAY_COUNT  T_ARRAY_EXTEND  T_VARRAY_VAR  T_RECORD
%token				LESS_LESS
%token				GREATER_GREATER

%token				T_REFCURSOR		/* token for cursor type */
%token				T_SQL_ISOPEN
%token				T_SQL_FOUND
%token				T_SQL_NOTFOUND
%token				T_SQL_ROWCOUNT
%token				T_CURSOR_ISOPEN
%token				T_CURSOR_FOUND
%token				T_CURSOR_NOTFOUND
%token				T_CURSOR_ROWCOUNT

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
%token <keyword>	K_BACKWARD
%token <keyword>	K_BEGIN
%token <keyword>	K_BY
%token <keyword>	K_CASE
%token <keyword>	K_CLOSE
%token <keyword>	K_COLLATE
%token <keyword>	K_COMMIT
%token <keyword>	K_CONSTANT
%token <keyword>	K_CONTINUE
%token <keyword>	K_CURRENT
%token <keyword>	K_CURSOR
%token <keyword>	K_DEBUG
%token <keyword>	K_DECLARE
%token <keyword>	K_DEFAULT
%token <keyword>	K_DELETE
%token <keyword>	K_DETAIL
%token <keyword>	K_DIAGNOSTICS
%token <keyword>	K_DUMP
%token <keyword>	K_ELSE
%token <keyword>	K_ELSIF
%token <keyword>	K_END
%token <keyword>	K_ERRCODE
%token <keyword>	K_ERROR
%token <keyword>	K_EXCEPTION
%token <keyword>	K_EXECUTE
%token <keyword>	K_EXIT
%token <keyword>	K_FETCH
%token <keyword>	K_FIRST
%token <keyword>	K_FOR
%token <keyword>	K_FORALL
%token <keyword>	K_FOREACH
%token <keyword>	K_FORWARD
%token <keyword>	K_FROM
%token <keyword>	K_GET
%token <keyword>	K_GOTO
%token <keyword>	K_HINT
%token <keyword>	K_IF
%token <keyword>	K_IMMEDIATE
%token <keyword>	K_IN
%token <keyword>	K_INFO
%token <keyword>	K_INSERT
%token <keyword>	K_INTO
%token <keyword>	K_IS
%token <keyword>	K_LAST
%token <keyword>	K_LOG
%token <keyword>	K_LOOP
%token <keyword>    K_MERGE
%token <keyword>	K_MESSAGE
%token <keyword>	K_MESSAGE_TEXT
%token <keyword>	K_MOVE
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
%token <keyword>	K_PERFORM
%token <keyword>	K_PG_EXCEPTION_CONTEXT
%token <keyword>	K_PG_EXCEPTION_DETAIL
%token <keyword>	K_PG_EXCEPTION_HINT
%token <keyword>	K_PRAGMA
%token <keyword>	K_PRIOR
%token <keyword>	K_QUERY
%token <keyword>	K_RAISE
%token <keyword>	K_RECORD
%token <keyword>	K_REF
%token <keyword>	K_RELATIVE
%token <keyword>	K_RESULT_OID
%token <keyword>	K_RETURN
%token <keyword>	K_RETURNED_SQLSTATE
%token <keyword>	K_REVERSE
%token <keyword>	K_ROLLBACK
%token <keyword>	K_ROWTYPE
%token <keyword>	K_ROW_COUNT
%token <keyword>	K_SAVEPOINT
%token <keyword>	K_SELECT
%token <keyword>	K_SCROLL
%token <keyword>	K_SLICE
%token <keyword>	K_SQLSTATE
%token <keyword>	K_STACKED
%token <keyword>	K_STRICT
%token <keyword>	K_SYS_REFCURSOR
%token <keyword>	K_THEN
%token <keyword>	K_TO
%token <keyword>	K_TYPE
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

pl_function		: comp_options pl_block opt_semi
                    {
                        u_sess->plsql_cxt.plpgsql_parse_result = (PLpgSQL_stmt_block *) $2;
                    }
                ;

comp_options	:
                | comp_options comp_option
                ;

comp_option		: '#' K_OPTION K_DUMP
                    {
                        u_sess->plsql_cxt.plpgsql_DumpExecTree = true;
                    }
                | '#' K_VARIABLE_CONFLICT K_ERROR
                    {
                        u_sess->plsql_cxt.plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_ERROR;
                    }
                | '#' K_VARIABLE_CONFLICT K_USE_VARIABLE
                    {
                        u_sess->plsql_cxt.plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_VARIABLE;
                    }
                | '#' K_VARIABLE_CONFLICT K_USE_COLUMN
                    {
                        u_sess->plsql_cxt.plpgsql_curr_compile->resolve_option = PLPGSQL_RESOLVE_COLUMN;
                    }
                ;

opt_semi		:
                | ';'
                ;

pl_block		: decl_sect K_BEGIN proc_sect exception_sect K_END opt_label
                    {
                        PLpgSQL_stmt_block *newp;

                        newp = (PLpgSQL_stmt_block *)palloc0(sizeof(PLpgSQL_stmt_block));

                        newp->cmd_type	= PLPGSQL_STMT_BLOCK;
                        newp->lineno		= plpgsql_location_to_lineno(@2);
                        newp->label		= $1.label;
#ifndef ENABLE_MULTIPLE_NODES
                        newp->autonomous = $1.autonomous;
#endif
                        newp->n_initvars = $1.n_initvars;
                        newp->initvarnos = $1.initvarnos;
                        newp->body		= $3;
                        newp->exceptions	= $4;

                        check_labels($1.label, $6, @6);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1.label, (PLpgSQL_stmt *)newp);
                    }
                ;


decl_sect		: opt_block_label
                    {
                        /* done with decls, so resume identifier lookup */
                        u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        $$.n_initvars = 0;
                        $$.initvarnos = NULL;
                        $$.autonomous = false;
                    }
                | opt_block_label decl_start
                    {
                        u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        $$.n_initvars = 0;
                        $$.initvarnos = NULL;
                        $$.autonomous = false;
                    }
                | opt_block_label decl_start decl_stmts
                    {
                        u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
                        $$.label	  = $1;
                        /* Remember variables declared in decl_stmts */
                        $$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
                        $$.autonomous = last_pragma;
                        last_pragma = false;
                    }
                ;

decl_start		: K_DECLARE
                    {
                        /* Forget any variables created before block */
                        plpgsql_add_initdatums(NULL);
                        last_pragma = false;
                        /*
                         * Disable scanner lookup of identifiers while
                         * we process the decl_stmts
                         */
                        u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
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
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("block label must be placed before DECLARE, not after"),
                                 parser_errposition(@1)));
                    }
                ;

decl_statement	: decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
                    {
                        PLpgSQL_variable	*var;

                        /*
                         * If a collation is supplied, insert it into the
                         * datatype.  We assume decl_datatype always returns
                         * a freshly built struct not shared with other
                         * variables.
                         */
                        if (OidIsValid($4))
                        {
                            if (!OidIsValid($3->collation))
                                ereport(ERROR,
                                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                                         errmsg("collations are not supported by type %s",
                                                format_type_be($3->typoid)),
                                         parser_errposition(@4)));
                            $3->collation = $4;
                        }

                        var = plpgsql_build_variable($1.name, $1.lineno,
                                                     $3, true);
                        if ($2)
                        {
                            if (var->dtype == PLPGSQL_DTYPE_VAR)
                                ((PLpgSQL_var *) var)->isconst = $2;
                            else
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("row or record variable cannot be CONSTANT"),
                                         parser_errposition(@2)));
                        }
                        if ($5)
                        {
                            if (var->dtype == PLPGSQL_DTYPE_VAR)
                                ((PLpgSQL_var *) var)->notnull = $5;
                            else
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("row or record variable cannot be NOT NULL"),
                                         parser_errposition(@4)));

                        }
                        if ($6 != NULL)
                        {
                            if (var->dtype == PLPGSQL_DTYPE_VAR)
                                ((PLpgSQL_var *) var)->default_val = $6;
                            else
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@5)));
                        }

                        pfree_ext($1.name);
                    }
                | decl_varname K_ALIAS K_FOR decl_aliasitem ';'
                    {
                        plpgsql_ns_additem($4->itemtype,
                                           $4->itemno, $1.name);
                        pfree_ext($1.name);
                    }
                |	K_CURSOR decl_varname opt_scrollable 
                    { plpgsql_ns_push($2.name); }
                    decl_cursor_args decl_is_for decl_cursor_query
                    {
                        PLpgSQL_var *newp;

                        /* pop local namespace for cursor args */
                        plpgsql_ns_pop();

                        newp = (PLpgSQL_var *)
                                plpgsql_build_variable($2.name, $2.lineno,
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

                        pfree_ext($2.name);
                    }
                |	K_TYPE decl_varname K_IS K_REF K_CURSOR ';'
                    {
                        /* add name of cursor type to PLPGSQL_NSTYPE_REFCURSOR */
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_REFCURSOR,0,$2.name);
                        pfree_ext($2.name);
                    }
                |	decl_varname T_REFCURSOR ';'
                    {
                        plpgsql_build_variable(
                                $1.name, 
                                $1.lineno,
                                plpgsql_build_datatype(REFCURSOROID,-1,InvalidOid),
                                true);
                        pfree_ext($1.name);
                    }

                |	decl_varname K_SYS_REFCURSOR ';'
                    {
                        plpgsql_build_variable(
                                $1.name, 
                                $1.lineno,
                                plpgsql_build_datatype(REFCURSOROID,-1,InvalidOid),
                                true);
                        pfree_ext($1.name);
                    }
                /*
                 * Implementing the grammar pattern
                 * "type varrayname is varray(ICONST) of datatype;"
                 * and "varname varrayname := varrayname()"
                 */
                |	K_TYPE decl_varname K_IS K_VARRAY '(' ICONST ')'  K_OF decl_datatype ';'
                    {
                        plpgsql_build_varrayType($2.name, $2.lineno, $9, true);
                        pfree_ext($2.name);
                    }
                |	decl_varname varray_var decl_defval
                    {
                        char *type_name;
                        errno_t ret;
                        PLpgSQL_type * var_type = ((PLpgSQL_var *)u_sess->plsql_cxt.plpgsql_Datums[$2])->datatype;
                        PLpgSQL_var *newp;
                        int len = strlen(var_type->typname) + 3;
                        type_name = (char *)palloc0(len);
                        ret = strcpy_s(type_name, len, var_type->typname);
                        securec_check(ret, "", "");
                        ret = strcat_s(type_name, len, "[]");
                        securec_check(ret, "", "");
                        var_type = parse_datatype(type_name, yylloc);

                        newp = (PLpgSQL_var *)plpgsql_build_variable($1.name, $1.lineno,var_type,true);
                        if (NULL == newp)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("build variable failed")));
                        pfree_ext($1.name);
                        pfree_ext(type_name);
                    }
                |	K_TYPE decl_varname K_IS K_RECORD '(' record_attr_list ')' ';'
                    {
                        PLpgSQL_rec_type	*newp = NULL;

                        newp = plpgsql_build_rec_type($2.name, $2.lineno, $6, true);
                        if (NULL == newp)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("build variable failed")));
                        pfree_ext($2.name);
                    }
                |	decl_varname record_var ';'
                    {
                        PLpgSQL_var *newp = NULL;
                        PLpgSQL_type * var_type = (PLpgSQL_type *)u_sess->plsql_cxt.plpgsql_Datums[$2];

                        newp = (PLpgSQL_var *)plpgsql_build_variable($1.name,$1.lineno,
                                                                    var_type,true);
                        if (NULL == newp)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                     errmsg("build variable failed")));
                        pfree_ext($1.name);
                    }
		|	K_PRAGMA any_identifier ';'
		    {
			if (pg_strcasecmp($2, "autonomous_transaction") == 0)
#ifndef ENABLE_MULTIPLE_NODES			
				last_pragma = true;
#else			     

				ereport(ERROR,
		                	(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                         errmsg("autonomous transaction is not yet supported.")));
#endif				
			else
				elog(ERROR, "invalid pragma");
		    }
                ;

record_attr_list : record_attr
                   {
                        $$ = list_make1($1);
                   }
                 | record_attr_list ',' record_attr
                    {
                        $$ = lappend($1, $3);
                    }
                 ;

record_attr		: decl_varname decl_datatype decl_notnull decl_rec_defval
                  {
                        PLpgSQL_rec_attr	*attr = NULL;

                        attr = (PLpgSQL_rec_attr*)palloc0(sizeof(PLpgSQL_rec_attr));

                        attr->attrname = $1.name;
                        attr->type = $2;

                        attr->notnull = $3;
                        if ($4 != NULL)
                        {
                            if (attr->type->ttype == PLPGSQL_TTYPE_SCALAR)
                                attr->defaultvalue = $4;
                            else
                                ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                         errmsg("default value for row or record variable is not supported"),
                                         parser_errposition(@3)));
                        }

                        if ($3 && $4 == NULL)
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("variables declared as NOT NULL must have "
                                "a default value.")));

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
                            plpgsql_build_variable($1.name, $1.lineno,
                                                   $3, true);
                        pfree_ext($1.name);
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
                        if (nsi == NULL)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                                     errmsg("variable \"%s\" does not exist",
                                            $1.ident),
                                     parser_errposition(@1)));
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
                        if (nsi == NULL)
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                                     errmsg("variable \"%s\" does not exist",
                                            NameListToString($1.idents)),
                                     parser_errposition(@1)));
                        $$ = nsi;
                    }
                ;

/*
 * $$.name will do a strdup outer, so its original space should be free.
 */
decl_varname	: T_WORD
                    {
                        $$.name = $1.ident;
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        /*
                         * Check to make sure name isn't already declared
                         * in the current block.
                         */
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1.ident, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
                    }
                | unreserved_keyword
                    {
                        $$.name = pstrdup($1);
                        $$.lineno = plpgsql_location_to_lineno(@1);
                        /*
                         * Check to make sure name isn't already declared
                         * in the current block.
                         */
                        if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
                                              $1, NULL, NULL,
                                              NULL) != NULL)
                            yyerror("duplicate declaration");
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
                ;


goto_block_label	:
                    {
                        $$ = NULL;
                    }
                | LESS_LESS any_identifier GREATER_GREATER
                    {
                        plpgsql_ns_push($2);
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

stmt_perform	: K_PERFORM expr_until_semi
                    {
                        PLpgSQL_stmt_perform *newp;

                        newp = (PLpgSQL_stmt_perform *)palloc0(sizeof(PLpgSQL_stmt_perform));
                        newp->cmd_type = PLPGSQL_STMT_PERFORM;
                        newp->lineno   = plpgsql_location_to_lineno(@1);
                        newp->expr  = $2;

                        $$ = (PLpgSQL_stmt *)newp;
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

                        $$ = (PLpgSQL_stmt *)newp;
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

                        /*
                         * Check information items are valid for area option.
                         */
                        foreach(lc, newp->diag_items)
                        {
                            PLpgSQL_diag_item *ditem = (PLpgSQL_diag_item *) lfirst(lc);

                            switch (ditem->kind)
                            {
                                /* these fields are disallowed in stacked case */
                                case PLPGSQL_GETDIAG_ROW_COUNT:
                                case PLPGSQL_GETDIAG_RESULT_OID:
                                    if (newp->is_stacked)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_SYNTAX_ERROR),
                                                 errmsg("diagnostics item %s is not allowed in GET STACKED DIAGNOSTICS",
                                                        plpgsql_getdiag_kindname(ditem->kind)),
                                                 parser_errposition(@1)));
                                    break;
                                /* these fields are disallowed in current case */
                                case PLPGSQL_GETDIAG_ERROR_CONTEXT:
                                case PLPGSQL_GETDIAG_ERROR_DETAIL:
                                case PLPGSQL_GETDIAG_ERROR_HINT:
                                case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
                                case PLPGSQL_GETDIAG_MESSAGE_TEXT:
                                    if (!newp->is_stacked)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_SYNTAX_ERROR),
                                                 errmsg("diagnostics item %s is not allowed in GET CURRENT DIAGNOSTICS",
                                                        plpgsql_getdiag_kindname(ditem->kind)),
                                                 parser_errposition(@1)));
                                    break;
                                default:
                                    elog(ERROR, "unrecognized diagnostic item kind: %d",
                                         ditem->kind);
                                    break;
                            }
                        }

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

                        $$ = newp;
                    }
                ;

getdiag_item :
                    {
                        int	tok = yylex();

                        if (tok_is_keyword(tok, &yylval,
                                           K_ROW_COUNT, "row_count"))
                            $$ = PLPGSQL_GETDIAG_ROW_COUNT;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_RESULT_OID, "result_oid"))
                            $$ = PLPGSQL_GETDIAG_RESULT_OID;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_PG_EXCEPTION_DETAIL, "pg_exception_detail"))
                            $$ = PLPGSQL_GETDIAG_ERROR_DETAIL;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_PG_EXCEPTION_HINT, "pg_exception_hint"))
                            $$ = PLPGSQL_GETDIAG_ERROR_HINT;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_PG_EXCEPTION_CONTEXT, "pg_exception_context"))
                            $$ = PLPGSQL_GETDIAG_ERROR_CONTEXT;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_MESSAGE_TEXT, "message_text"))
                            $$ = PLPGSQL_GETDIAG_MESSAGE_TEXT;
                        else if (tok_is_keyword(tok, &yylval,
                                                K_RETURNED_SQLSTATE, "returned_sqlstate"))
                            $$ = PLPGSQL_GETDIAG_RETURNED_SQLSTATE;
                        else
                            yyerror("unrecognized GET DIAGNOSTICS item");
                    }
                ;

getdiag_target	: T_DATUM
                    {
                        check_assignable($1.datum, @1);
                        if ($1.datum->dtype == PLPGSQL_DTYPE_ROW ||
                            $1.datum->dtype == PLPGSQL_DTYPE_REC)
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("\"%s\" is not a scalar variable",
                                            NameOfDatum(&($1))),
                                     parser_errposition(@1)));
                        $$ = $1.datum->dno;
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

varray_var		: T_VARRAY
                    {
                        $$ = $1.datum->dno;
                    }
                ;

record_var		: T_RECORD
                    {
                        $$ = $1.datum->dno;
                    }
                ;

assign_var		: T_DATUM
                    {
                        check_assignable($1.datum, @1);
                        $$ = $1.datum->dno;
                    }
                | T_RECORD
                    {
                        check_assignable($1.datum, @1);
                        $$ = $1.datum->dno;
                    }
                | T_VARRAY_VAR
                    {
                        check_assignable($1.datum, @1);
                        $$ = $1.datum->dno;
                    }
                | assign_var '[' expr_until_rightbracket
                    {
                        PLpgSQL_arrayelem	*newp;

                        newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
                        newp->dtype		= PLPGSQL_DTYPE_ARRAYELEM;
                        newp->subscript	= $3;
                        newp->arrayparentno = $1;
                        /* initialize cached type data to "not valid" */
                        newp->parenttypoid = InvalidOid;

                        plpgsql_adddatum((PLpgSQL_datum *) newp);

                        $$ = newp->dno;
                    }

                | assign_var '(' expr_until_parenthesis
                    {
                        PLpgSQL_arrayelem	*newp;
                        newp = (PLpgSQL_arrayelem *)palloc0(sizeof(PLpgSQL_arrayelem));
                        newp->dtype		= PLPGSQL_DTYPE_ARRAYELEM;
                        newp->subscript	= $3;
                        newp->arrayparentno = $1;
                        /* initialize cached type data to "not valid" */
                        newp->parenttypoid = InvalidOid;
                        plpgsql_adddatum((PLpgSQL_datum *)newp);
                        $$ = newp->dno;
                    }
                ;

stmt_goto		: K_GOTO label_name
                    {
                        PLpgSQL_stmt_goto *newp;

                        newp = (PLpgSQL_stmt_goto *)palloc0(sizeof(PLpgSQL_stmt_goto));
                        newp->cmd_type  = PLPGSQL_STMT_GOTO;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->label = $2;

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

                        check_labels($1, $3.end_label, $3.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

stmt_while		: opt_block_label K_WHILE expr_until_loop loop_body
                    {
                        PLpgSQL_stmt_while *newp;

                        newp = (PLpgSQL_stmt_while *)palloc0(sizeof(PLpgSQL_stmt_while));
                        newp->cmd_type = PLPGSQL_STMT_WHILE;
                        newp->lineno   = plpgsql_location_to_lineno(@2);
                        newp->label	  = $1;
                        newp->cond	  = $3;
                        newp->body	  = $4.stmts;

                        check_labels($1, $4.end_label, $4.end_label_location);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;

                        /* register the stmt if it is labeled */
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
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
                            $$ = (PLpgSQL_stmt *) newp;

                            /* register the stmt if it is labeled */
                            record_stmt_label($1, (PLpgSQL_stmt *)newp);
                        }

                        check_labels($1, $4.end_label, $4.end_label_location);
                        /* close namespace started in opt_block_label */
                        plpgsql_ns_pop();
                    }
                | opt_block_label K_FORALL forall_control forall_body
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
                            newm->label	  = NULL;
                            newm->body	  = $4;
                            $$ = (PLpgSQL_stmt *) newm;
                        }
                        else
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("please use \'FORALL index_name IN lower_bound .. upper_bound\'")));

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
                                newp->row = make_scalar_list1($1.name, $1.scalar,
                                                             $1.lineno, @1);
                                /* no need for check_assignable */
                            }
                            else
                            {
                                ereport(ERROR,
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
                        else if (tok == T_DATUM &&
                                 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR &&
                                 ((PLpgSQL_var *) yylval.wdatum.datum)->datatype->typoid == REFCURSOROID)
                        {
                            /* It's FOR var IN cursor */
                            PLpgSQL_stmt_forc	*newp;
                            PLpgSQL_var			*cursor = (PLpgSQL_var *) yylval.wdatum.datum;

                            newp = (PLpgSQL_stmt_forc *) palloc0(sizeof(PLpgSQL_stmt_forc));
                            newp->cmd_type = PLPGSQL_STMT_FORC;
                            newp->curvar = cursor->dno;

                            /* Should have had a single variable name */
                            if ($1.scalar && $1.row)
                                ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("cursor FOR loop must have only one target variable"),
                                         parser_errposition(@1)));

                            /* can't use an unbound cursor this way */
                            if (cursor->cursor_explicit_expr == NULL)
                                ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("cursor FOR loop must use a bound cursor variable"),
                                         parser_errposition(tokloc)));

                            /* collect cursor's parameters if any */
                            newp->argquery = read_cursor_args(cursor,
                                                             K_LOOP,
                                                             "LOOP");

                            /* create loop's private RECORD variable */
                            newp->rec = plpgsql_build_record($1.name,
                                                            $1.lineno,
                                                            true);

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
                                if ($1.scalar && $1.row)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_SYNTAX_ERROR),
                                             errmsg("integer FOR loop must have only one target variable"),
                                             parser_errposition(@1)));

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

                                if (reverse)
                                    ereport(ERROR,
                                            (errcode(ERRCODE_SYNTAX_ERROR),
                                             errmsg("cannot specify REVERSE in query FOR loop"),
                                             parser_errposition(tokloc)));

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
                                    newp->row = make_scalar_list1($1.name, $1.scalar,
                                                                 $1.lineno, @1);
                                    /* no need for check_assignable */
                                }
                                else
                                {
                                    PLpgSQL_type dtype;
                                    dtype.ttype = PLPGSQL_TTYPE_REC;
                                    newp->rec = (PLpgSQL_rec *) 
                                        plpgsql_build_variable($1.name,$1.lineno, &dtype, true);
                                    check_assignable((PLpgSQL_datum *) newp->rec, @1);
                                }

                                newp->query = expr1;
                                $$ = (PLpgSQL_stmt *) newp;
                            }
                        }
                    }
                ;
forall_control		:for_variable K_IN
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
                            expr2 = read_sql_construct5(K_MERGE,
                                                        K_INSERT, 
                                                        K_SELECT,
                                                        K_UPDATE,
                                                        K_DELETE,
                                                        "DML",
                                                        "SELECT ",
                                                        true,
                                                        false,
                                                        true,
                                                        NULL,
                                                        &tok);

                            plpgsql_push_back_token(tok);

                            if (';' == tok)
                                ereport(ERROR,
                                        (errcode(ERRCODE_FORALL_NEED_DML),
                                         errmsg("FORALL must follow DML statement.")));

                            /* should follow DML statement */
                            if (tok != K_INSERT && tok != K_UPDATE && tok != K_DELETE && tok != K_SELECT && tok != K_MERGE)
                                ereport(ERROR,
                                        (errcode(ERRCODE_FORALL_NEED_DML),
                                         errmsg("FORALL must follow DML statement.")));

                            /* Should have had a single variable name */
                            if ($1.scalar && $1.row)
                                ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                         errmsg("integer FORALL must have just one target variable")));

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
                        else
                            ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                     errmsg("please use \'FORALL index_name IN lower_bound .. upper_bound\'")));						
                    }
                ;
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
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_RECORD)
                        {
                            $$.scalar = NULL;
                            $$.rec = NULL;
                            $$.row = (PLpgSQL_row *) $1.datum;
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_REC)
                        {
                            $$.scalar = NULL;
                            $$.rec = (PLpgSQL_rec *) $1.datum;
                            $$.row = NULL;
                        }
                        else
                        {
                            int			tok;

                            $$.scalar = $1.datum;
                            $$.rec = NULL;
                            $$.row = NULL;
                            /* check for comma-separated list */
                            tok = yylex();
                            plpgsql_push_back_token(tok);
                            if (tok == ',')
                                $$.row = read_into_scalar_list($$.name,
                                                               $$.scalar,
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
                        }
                        else if ($1.datum->dtype == PLPGSQL_DTYPE_REC)
                        {
                            $$.scalar = NULL;
                            $$.rec = (PLpgSQL_rec *) $1.datum;
                            $$.row = NULL;
                        }
                        else
                        {
                            int			tok;

                            $$.scalar = $1.datum;
                            $$.rec = NULL;
                            $$.row = NULL;
                            /* check for comma-separated list */
                            tok = yylex();
                            plpgsql_push_back_token(tok);
                            if (tok == ',')
                                $$.row = read_into_scalar_list($$.name,
                                                               $$.scalar,
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

                        if ($3.rec)
                        {
                            newp->varno = $3.rec->dno;
                            check_assignable((PLpgSQL_datum *) $3.rec, @3);
                        }
                        else if ($3.row)
                        {
                            newp->varno = $3.row->dno;
                            check_assignable((PLpgSQL_datum *) $3.row, @3);
                        }
                        else if ($3.scalar)
                        {
                            newp->varno = $3.scalar->dno;
                            check_assignable($3.scalar, @3);
                        }
                        else
                        {
                            ereport(ERROR,
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
                            $$ = make_return_next_stmt(@1);
                        }
                        else if (tok_is_keyword(tok, &yylval,
                                                K_QUERY, "query"))
                        {
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

                        tok = yylex();
                        if (tok == 0)
                            yyerror("unexpected end of function definition");

                        /*
                         * We could have just RAISE, meaning to re-throw
                         * the current error.
                         */
                        if (tok != ';')
                        {
                            if (T_DATUM == tok && PLPGSQL_DTYPE_ROW == yylval.wdatum.datum->dtype)
                            {
                                PLpgSQL_expr *expr = NULL;

                                sprintf(message, "line:%d ", plpgsql_location_to_lineno(@1));
                                appendStringInfoString(&ds, message);
                                appendStringInfoString(&ds,"%");

                                row = (PLpgSQL_row *)yylval.wdatum.datum;

                                /* condname is system embedded error name, so it is still null in this case. */
                                newp->condname = pstrdup(unpack_sql_state(row->customErrorCode));
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

loop_body		: proc_sect K_END K_LOOP opt_label ';'
                    {
                        $$.stmts = $1;
                        $$.end_label = $4;
                        $$.end_label_location = @4;
                    }
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
		| K_SAVEPOINT
		    {
		    	ereport(ERROR,
			    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			     errmsg("SAVEPOINT in function/procedure is not yet supported.")));
		    }
		| K_MERGE
		    {
		    	$$ = make_execsql_stmt(K_MERGE, @1);
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
                            PLpgSQL_stmt *stmt = make_callfunc_stmt($1.ident, @1, false);
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
                        $$ = make_execsql_stmt(T_WORD, @1);
                    }
                }
            }
        | T_CWORD
            {
                int tok = yylex();
                char *name = NULL;
                bool isCallFunc = false;
                bool FuncNoarg = false;

                if ('(' == tok)
                {
                    MemoryContext colCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
                    name = NameListToString($1.idents);
                    (void)MemoryContextSwitchTo(colCxt);
                    isCallFunc = is_function(name, false, false);
                }
                else if ('=' == tok || COLON_EQUALS == tok || '[' == tok)
                    cword_is_not_variable(&($1), @1);
                else if (';' == tok) {
                    MemoryContext colCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
                    name = NameListToString($1.idents);
                    (void)MemoryContextSwitchTo(colCxt);
                    isCallFunc = is_function(name, false, true);
                    FuncNoarg = true;
                }

                plpgsql_push_back_token(tok);
                if (isCallFunc)
                {
                    if (FuncNoarg)
                        $$ = make_callfunc_stmt_no_arg(name, @1);
                    else
                    {
                        PLpgSQL_stmt *stmt = make_callfunc_stmt(name, @1, false);
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
        | T_ARRAY_EXTEND
            {
                int tok = yylex();
                if (';' == tok)
                {
                    $$ =  NULL;
                }
                else
                {
                    plpgsql_push_back_token(tok);
                    $$ = make_callfunc_stmt("array_extend", @1, false);
                }
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

                        /* If we found "INTO", collect the argument */
                        
                        if (endtoken == K_INTO)
                        {
                            if (newp->into)			/* multiple INTO */
                                yyerror("syntax error");
                            newp->into = true;
                            read_into_target(&newp->rec, &newp->row, &newp->strict);
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
                                    ereport(ERROR,
                                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                             errmsg("target into is conflicted with using out (inout)"),
                                             errdetail("\"select clause\" can't has out parameters, can only use \"into\"")));
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
                        newp->curvar = $2->dno;
                        newp->cursor_options = CURSOR_OPT_FAST_PLAN;

                        if ($2->cursor_explicit_expr == NULL)
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
                        }
                        else
                        {
                            /* predefined cursor query, so read args */
                            newp->argquery = read_cursor_args($2, ';', ";");
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
                        read_into_target(&rec, &row, NULL);

                        if (yylex() != ';')
                            yyerror("syntax error");

                        /*
                         * We don't allow multiple rows in PL/pgSQL's FETCH
                         * statement, only in MOVE.
                         */
                        if (fetch->returns_multiple_rows)
                            ereport(ERROR,
                                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                     errmsg("FETCH statement cannot return multiple rows"),
                                     parser_errposition(@1)));

                        fetch->lineno = plpgsql_location_to_lineno(@1);
                        fetch->rec		= rec;
                        fetch->row		= row;
                        fetch->curvar	= $3->dno;
                        fetch->is_move	= false;

                        $$ = (PLpgSQL_stmt *)fetch;
                    }
                ;

stmt_move		: K_MOVE opt_fetch_direction cursor_variable ';'
                    {
                        PLpgSQL_stmt_fetch *fetch = $2;

                        fetch->lineno = plpgsql_location_to_lineno(@1);
                        fetch->curvar	= $3->dno;
                        fetch->is_move	= true;

                        $$ = (PLpgSQL_stmt *)fetch;
                    }
                ;

opt_fetch_direction	:
                    {
                        $$ = read_fetch_direction();
                    }
                ;

stmt_close		: K_CLOSE cursor_variable ';'
                    {
                        PLpgSQL_stmt_close *newp;

                        newp = (PLpgSQL_stmt_close *)palloc(sizeof(PLpgSQL_stmt_close));
                        newp->cmd_type = PLPGSQL_STMT_CLOSE;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        newp->curvar = $2->dno;

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_null		: K_NULL ';'
                    {
                        /* We do building a node for NULL for GOTO */
                        PLpgSQL_stmt *newp;

                        newp = (PLpgSQL_stmt_null *)palloc(sizeof(PLpgSQL_stmt_null));
                        newp->cmd_type = PLPGSQL_STMT_NULL;
                        newp->lineno = plpgsql_location_to_lineno(@1);

                        $$ = (PLpgSQL_stmt *)newp;
                    }
                ;

stmt_commit		: opt_block_label K_COMMIT ';'
                    {
                        PLpgSQL_stmt_commit *newp;

                        newp = (PLpgSQL_stmt_commit *)palloc(sizeof(PLpgSQL_stmt_commit));
                        newp->cmd_type = PLPGSQL_STMT_COMMIT;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

stmt_rollback		: opt_block_label K_ROLLBACK ';'
                    {
                        PLpgSQL_stmt_rollback *newp;

                        newp = (PLpgSQL_stmt_rollback *) palloc(sizeof(PLpgSQL_stmt_rollback));
                        newp->cmd_type = PLPGSQL_STMT_ROLLBACK;
                        newp->lineno = plpgsql_location_to_lineno(@1);
                        plpgsql_ns_pop();

                        $$ = (PLpgSQL_stmt *)newp;
                        record_stmt_label($1, (PLpgSQL_stmt *)newp);
                    }
                ;

cursor_variable	: T_DATUM
                    {
                        if ($1.datum->dtype != PLPGSQL_DTYPE_VAR)
                            ereport(ERROR,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("cursor variable must be a simple variable"),
                                     parser_errposition(@1)));

                        if (((PLpgSQL_var *) $1.datum)->datatype->typoid != REFCURSOROID)
                            ereport(ERROR,
                                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                                     errmsg("variable \"%s\" must be of type cursor or refcursor",
                                            ((PLpgSQL_var *) $1.datum)->refname),
                                     parser_errposition(@1)));
                        $$ = (PLpgSQL_var *) $1.datum;
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
                        int			lineno = plpgsql_location_to_lineno(@1);
                        PLpgSQL_exception_block *newp = (PLpgSQL_exception_block *)palloc(sizeof(PLpgSQL_exception_block));
                        PLpgSQL_variable *var;

                        var = plpgsql_build_variable("sqlstate", lineno,
                                                     plpgsql_build_datatype(TEXTOID,
                                                                            -1,
                                                                            u_sess->plsql_cxt.plpgsql_curr_compile->fn_input_collation),
                                                     true);
                        ((PLpgSQL_var *) var)->isconst = true;
                        newp->sqlstate_varno = var->dno;

                        var = plpgsql_build_variable("sqlerrm", lineno,
                                                     plpgsql_build_datatype(TEXTOID,
                                                                            -1,
                                                                            u_sess->plsql_cxt.plpgsql_curr_compile->fn_input_collation),
                                                     true);
                        ((PLpgSQL_var *) var)->isconst = true;
                        newp->sqlerrm_varno = var->dno;

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
                                    PLpgSQL_row * row = ( PLpgSQL_row* ) u_sess->plsql_cxt.plpgsql_Datums[yylval.wdatum.datum->dno];
                                    TupleDesc	rowtupdesc = row ? row->rowtupdesc : NULL;

                                    if(rowtupdesc && 
                                        0 == strcmp(format_type_be(rowtupdesc->tdtypeid), "exception"))
                                    {
                                        newp = (PLpgSQL_condition *)palloc(sizeof(PLpgSQL_condition));
                                        newp->sqlerrstate = row->customErrorCode;
                                        newp->condname = pstrdup(row->refname);
                                        newp->next = NULL;
                                    }

                                    if(NULL == newp)
                                        ereport(ERROR,
                                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                                errmsg("unrecognized exception condition \"%s\"",
                                                        row? row->refname : "??")));
                                    $$ = newp;
                                }
                                else
                                    $$ = plpgsql_parse_err_condition($1);
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

                        if (plpgsql_is_token_match2(T_WORD, '(') || 
                            plpgsql_is_token_match2(T_CWORD,'('))
                        {
                            tok = yylex();
                            if (T_WORD == tok)
                                name = yylval.word.ident;
                            else
                                name = NameListToString(yylval.cword.idents);

                            isCallFunc = is_function(name, true, false);
                        }

                        if (isCallFunc)
                        {
                            stmt = make_callfunc_stmt(name, yylloc, true);
                            if (PLPGSQL_STMT_EXECSQL == stmt->cmd_type)
                                expr = ((PLpgSQL_stmt_execsql*)stmt)->sqlstmt;
                            else if (PLPGSQL_STMT_PERFORM == stmt->cmd_type)
                                expr = ((PLpgSQL_stmt_perform*)stmt)->expr;

                            expr->is_funccall = true;
                            $$ = expr;
                        }
                        else
                        {
                            if (name != NULL)
                                plpgsql_push_back_token(tok);
                            $$ = read_sql_expression(';', ";");
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
#if 0
                        if (plpgsql_ns_lookup_label(plpgsql_ns_top(), $1) == NULL)
                            yyerror("label does not exist");
#endif
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
                        if ($1.ident == NULL) /* composite name not OK */
                            yyerror("syntax error");
                        $$ = $1.ident;
                    }
                ;

unreserved_keyword	:
                K_ABSOLUTE
                | K_ALIAS
                | K_ALTER
                | K_ARRAY
                | K_BACKWARD
                | K_COMMIT
                | K_CONSTANT
                | K_CONTINUE
                | K_CURRENT
                | K_DEBUG
                | K_DETAIL
                | K_DUMP
                | K_ERRCODE
                | K_ERROR
                | K_FIRST
                | K_FORWARD
                | K_HINT
                | K_INFO
                | K_IS
                | K_LAST
                | K_LOG
                | K_MERGE
                | K_MESSAGE
                | K_MESSAGE_TEXT
                | K_NEXT
                | K_NO
                | K_NOTICE
                | K_OPTION
                | K_PG_EXCEPTION_CONTEXT
                | K_PG_EXCEPTION_DETAIL
                | K_PG_EXCEPTION_HINT
                | K_PRIOR
                | K_QUERY
                | K_RECORD
                | K_RELATIVE
                | K_RESULT_OID
                | K_RETURNED_SQLSTATE
                | K_REVERSE
                | K_ROLLBACK
                | K_ROW_COUNT
                | K_ROWTYPE
                | K_SCROLL
                | K_SLICE
                | K_SQLSTATE
                | K_STACKED
                | K_SYS_REFCURSOR
                | K_USE_COLUMN
                | K_USE_VARIABLE
                | K_VARIABLE_CONFLICT
                | K_VARRAY
                | K_WARNING
                | K_WITH
                ;

%%

#define MAX_EXPR_PARAMS  1024

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
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("\"%s\" is not a known variable",
                    word->ident),
             parser_errposition(location)));
}

/* Same, for a CWORD */
static void
cword_is_not_variable(PLcword *cword, int location)
{
    ereport(ERROR,
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
              int * tok)
{
    PLpgSQL_expr * expr =   NULL;

    if (*nparams)
        appendStringInfoString(func_inparam, ",");

     /* 
      * handle the problem that the function 
      * arguments can only be variable. the argment validsql is set FALSE to
      * ignore sql expression check to the "$n" type of arguments. 
      */
    expr = read_sql_construct(',', ')', 0, ",|)", "", true, false, false, NULL, tok);

    if (*nparams >= MAX_EXPR_PARAMS)
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg("too many variables specified in SQL statement ,more than %d", MAX_EXPR_PARAMS)));	

    (*nparams)++;

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
static void 
yylex_outparam(char** fieldnames,
               int *varnos,
               int nfields,
               PLpgSQL_row **row,
               PLpgSQL_rec **rec,
               int *token,
               bool overload)
{
    *token = yylex();

    if (T_DATUM == *token)
    {
        if (PLPGSQL_TTYPE_ROW == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.datum->dno;
            *row = (PLpgSQL_row *)yylval.wdatum.datum;
        }
        else if (PLPGSQL_TTYPE_REC == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.datum->dno;
            *rec = (PLpgSQL_rec *)yylval.wdatum.datum;
        }
        else if (PLPGSQL_TTYPE_SCALAR == yylval.wdatum.datum->dtype)
        {
            check_assignable(yylval.wdatum.datum, yylloc);
            fieldnames[nfields] = pstrdup(NameOfDatum(&yylval.wdatum));
            varnos[nfields] = yylval.wdatum.datum->dno;
        }
        else
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("\"%s\" is not a scalar variable",
                NameOfDatum(&yylval.wdatum))));
    }
    else if (overload)
    {
        fieldnames[nfields] = NULL;
        varnos[nfields] = -1;
    }
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
make_callfunc_stmt(const char *sqlstart, int location, bool is_assign)
{
    int nparams = 0;
    int nfields = 0;
    int narg = 0;
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
    int	varnos[FUNC_MAX_ARGS];
    bool	namedarg[FUNC_MAX_ARGS];
    char*	namedargnamses[FUNC_MAX_ARGS];
    char *fieldnames[FUNC_MAX_ARGS];

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

    MemoryContext oldCxt = NULL;
    bool	multi_func = false;
    /*get the function's name*/
    cp[0] = NULL;
    cp[1] = NULL;
    cp[2] = NULL;
    /* the function make_callfunc_stmt is only to assemble a sql statement, so the context is set to tmp context */
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    plpgsql_parser_funcname(sqlstart, cp, 3);

    if (cp[2] && cp[2][0] != '\0')
        funcname = list_make3(makeString(cp[0]), makeString(cp[1]), makeString(cp[2]));
    else if (cp[1] && cp[1][0] != '\0')
        funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
    else
        funcname = list_make1(makeString(cp[0]));


    /* search the function */
    clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
    if (!clist)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function \"%s\" doesn't exist ", sqlstart)));
        return NULL;
    }

    if (clist->next)
    {
        multi_func = true;
        if (IsPackageFunction(funcname) == false)
        {
            ereport(ERROR,
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
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function \"%s\" doesn't exist ", sqlstart)));
            return NULL;
        }
        /* get the all args informations, only "in" parameters if p_argmodes is null */
        narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
        procStruct = (Form_pg_proc) GETSTRUCT(proctup);
        ndefaultargs = procStruct->pronargdefaults;
        ReleaseSysCache(proctup);
    }

    initStringInfo(&func_inparas);

    tok = yylex();

    /* check the format for the function without parameters */
    if ((tok = yylex()) == ')')
        noargs = TRUE;
    plpgsql_push_back_token(tok);

    /* has any "out" parameters, user execsql stmt */
    if (is_assign)
    {
        appendStringInfoString(&func_inparas, "SELECT ");
    }
    else
    {
        appendStringInfoString(&func_inparas, "CALL ");
    }

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
                            switch (p_argmodes[j])
                            {
                                case 'i':
                                    yylex_inparam(&func_inparas, &nparams, &tok);
                                    break;
                                case 'o':
                                case 'b':
                                    if (is_assign && 'o' == p_argmodes[j])
                                    {
                                        (void)yylex();
                                        (void)yylex();
                                        if (T_DATUM == (tok = yylex()))
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
                                    (void)yylex();
                                    yylex_outparam(fieldnames, varnos, pos_outer, &row, &rec, &tok);
                                    if (T_DATUM == tok)
                                    {
                                        nfields++;
                                        appendStringInfoString(&func_inparas, NameOfDatum(&yylval.wdatum));
                                    }

                                    tok = yylex();
                                    nparams++;
                                    break;
                                default:
                                    ereport(ERROR,
                                            (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                             errmsg("parameter mode %c doesn't exist", p_argmodes[j])));
                            }
                            break;
                        }
                    }
                }
                else
                {
                    tok = yylex();
                    /* p_argmodes may be null, 'i'->in , 'o'-> out ,'b' inout,others error */
                    switch (p_argmodes[i])
                    {
                        case 'i':
                            if (T_PLACEHOLDER == tok)
                                placeholders++;
                            plpgsql_push_back_token(tok);
                            yylex_inparam(&func_inparas, &nparams, &tok);
                            break;
                        case 'o':
                            /*
                             * if the function is in an assign expr, just ignore the 
                             * out parameters.
                             */
                            if (is_assign)
                            {
                                if (T_DATUM == tok || T_PLACEHOLDER == tok)
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
                                if (nparams)
                                    appendStringInfoChar(&func_inparas, ',');
                                appendStringInfoString(&func_inparas, yylval.word.ident);
                                nparams++;
                            }
                            else if (T_DATUM == tok)
                            {
                                if (nparams)
                                    appendStringInfoString(&func_inparas, ",");
                                appendStringInfoString(&func_inparas, NameOfDatum(&yylval.wdatum));
                                nparams++;
                                nfields++;
                            }

                            plpgsql_push_back_token(tok);
                            yylex_outparam(fieldnames, varnos, pos_inner, &row, &rec, &tok);

                            tok = yylex();
                            break;
                        case 'b':
                            if (is_assign)
                            {
                                /*
                                 * if the function is in an assign expr, read the inout
                                 * parameters.
                                 */
                                if (T_DATUM == tok)
                                {
                                    plpgsql_push_back_token(tok);
                                    yylex_inparam(&func_inparas, &nparams, &tok);
                                }
                                else
                                    yyerror("syntax error");
                                break;
                            }

                            if (T_PLACEHOLDER == tok)
                                placeholders++;
                            plpgsql_push_back_token(tok);
                            yylex_outparam(fieldnames, varnos, pos_inner, &row, &rec, &tok);
                            if (T_DATUM == tok)
                                nfields++;
                            plpgsql_push_back_token(tok);
                            yylex_inparam(&func_inparas, &nparams, &tok);
                            break;
                        default:
                            ereport(ERROR,
                                    (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                                     errmsg("parameter mode %c doesn't exist", p_argmodes[i])));
                    }
                }

                if (')' == tok)
                {
                    i++;
                    break;
                }

                if (narg - 1 == i)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \")\", maybe input something superfluous.", sqlstart)));

                if (',' != tok)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \",\"", sqlstart)));
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
        
                yylex_inparam(&func_inparas, &nparams, &tok);
        
                if (')' == tok)
                {
                    i++;
                    break;
                }
        
                if (narg - 1 == i)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("when invoking function %s, expected \")\", maybe input something superfluous.", sqlstart)));
            }
        }
    }
    else
    {
        while (true)
        {

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
                
                yylex_outparam(fieldnames, varnos, nfields, &row, &rec, &tok, true);
                int loc = yylloc;
                tok = yylex();
                int curloc = yylloc;
                plpgsql_push_back_token(tok);
                plpgsql_append_source_text(&func_inparas, loc, curloc);	
                
                tok = yylex();
                nparams++;
                namedarg[nfields] = true;
            }
            else
            {
                yylex_outparam(fieldnames, varnos, nfields, &row, &rec, &tok, true);
                plpgsql_push_back_token(tok);
                yylex_inparam(&func_inparas, &nparams, &tok);
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
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("when invoking function %s, maybe input something superfluous.", sqlstart)));
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
    
    if (!multi_func && narg - i >  ndefaultargs)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s has no enough parameters", sqlstart)));

    (void)MemoryContextSwitchTo(oldCxt);

    /* generate the expression */
    expr 				= (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(func_inparas.data);
    expr->plan			= NULL;
    expr->paramnos 		= NULL;
    expr->ns			= plpgsql_ns_top();
    expr->idx			= (uint32)-1;

    if (multi_func)
    {
        PLpgSQL_function *function = NULL;
        PLpgSQL_execstate *estate = (PLpgSQL_execstate*)palloc(sizeof(PLpgSQL_execstate));
        expr->func = (PLpgSQL_function *) palloc0(sizeof(PLpgSQL_function));
        function = expr->func;
        function->fn_is_trigger = false;
        function->fn_input_collation = InvalidOid;
        function->out_param_varno = -1;		/* set up for no OUT param */
        function->resolve_option = (PLpgSQL_resolve_option)u_sess->plsql_cxt.plpgsql_variable_conflict;
        function->fn_cxt = CurrentMemoryContext;

        estate->ndatums = u_sess->plsql_cxt.plpgsql_nDatums;
        estate->datums = (PLpgSQL_datum **)palloc(sizeof(PLpgSQL_datum *) * u_sess->plsql_cxt.plpgsql_nDatums);
        for (int i = 0; i < u_sess->plsql_cxt.plpgsql_nDatums; i++)
            estate->datums[i] = u_sess->plsql_cxt.plpgsql_Datums[i];

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
                ereport(ERROR,
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

            /* if there is no "out" parameters ,use perform stmt,others use exesql */
            if ((0 == all_arg || NULL == p_argmodes))
            {
                PLpgSQL_stmt_perform *perform = NULL;
                perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
                perform->cmd_type = PLPGSQL_STMT_PERFORM;
                perform->lineno   = plpgsql_location_to_lineno(location);
                perform->expr	  = expr;
                return (PLpgSQL_stmt *)perform;
            }
            else if (all_arg >= narg)
            {
                if (NULL == rec &&  NULL == row)
                {
                    int new_nfields = 0;
                    int i = 0, j = 0;
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
                                ereport(ERROR,
                                    (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmsg("Named argument \"%s\" can not be a const", namedargnamses[i])));
                            }

                            if (row->varnos[pos_outer] > 0)
                            {
                                ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg("parameter \"%s\" is assigned more than once", row->fieldnames[pos_outer])));
                            }
                            
                            if (varnos[i] >= 0)
                            {
                                row->fieldnames[pos_outer] = fieldnames[i];
                                row->varnos[pos_outer] = varnos[i];
                            }
                        }
                    }
                    plpgsql_adddatum((PLpgSQL_datum *)row);
                }

                PLpgSQL_stmt_execsql * execsql = NULL;
                execsql = (PLpgSQL_stmt_execsql *)palloc(sizeof(PLpgSQL_stmt_execsql));
                execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
                execsql->lineno  = plpgsql_location_to_lineno(location);
                execsql->sqlstmt = expr;
                execsql->into	 = row || rec ? true:false;
                execsql->strict  = true;
                execsql->rec	 = rec;
                execsql->row	 = row;
                execsql->placeholders = placeholders;
                execsql->multi_func = multi_func;
                return (PLpgSQL_stmt *)execsql;
            }
        }
        else
        {
            ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
             errmsg("function \"%s\" isn't exclusive ", sqlstart)));
        }
    }
    else
    {
        /* if have out parameters, generate row type to store the results. */
        if (nfields && NULL == rec &&  NULL == row)
        {
            row = (PLpgSQL_row*)palloc0(sizeof(PLpgSQL_row));
            row->dtype = PLPGSQL_DTYPE_ROW;
            row->refname = pstrdup("*internal*");
            row->lineno = plpgsql_location_to_lineno(location);
            row->rowtupdesc = NULL;
            row->nfields = nfields;
            row->fieldnames = (char **)palloc(sizeof(char *) * nfields);
            row->varnos = (int *)palloc(sizeof(int) * nfields);
            while (--nfields >= 0)
            {
                row->fieldnames[nfields] = fieldnames[nfields];
                row->varnos[nfields] = varnos[nfields];
            }
            plpgsql_adddatum((PLpgSQL_datum *)row);
        }
        
        /* if there is no "out" parameters ,use perform stmt,others use exesql */
        if (0 == narg || NULL == p_argmodes)
        {
            PLpgSQL_stmt_perform *perform = NULL;
            perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
            perform->cmd_type = PLPGSQL_STMT_PERFORM;
            perform->lineno   = plpgsql_location_to_lineno(location);
            perform->expr	  = expr;
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
            execsql->strict  = true;
            execsql->rec	 = rec;
            execsql->row	 = row;
            execsql->placeholders = placeholders;
            execsql->multi_func = multi_func;
            return (PLpgSQL_stmt *)execsql;
        }
    }

    pfree_ext(func_inparas.data);
    /* skip compile warning */
    return NULL;
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
is_function(const char *name, bool is_assign, bool no_parenthesis)
{
    ScanKeyword * keyword = NULL;
    List *funcname = NIL;
    FuncCandidateList clist = NULL;
    bool have_outargs = false;
    char **p_argnames = NULL;
    char *p_argmodes = NULL;
    Oid *p_argtypes = NULL;
    HeapTuple proctup = NULL;
    int narg = 0;
    int i = 0;
    char *cp[3] = {0};

    /* the function is_function is only to judge if it's a function call, so memory used in it is all temp */
    AutoContextSwitch plCompileCxtGuard(u_sess->plsql_cxt.compile_tmp_cxt);

    plpgsql_parser_funcname(name, cp, 3);

    /* 
     * if A.B.C, it's not a function invoke, because can not use function of 
     * other database. if A.B, then it must be a function invoke. 
     */
    if (cp[2] && cp[2][0] != '\0')
        return false;
    else if (cp[0] && cp[0][0] != '\0')
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

        keyword = (ScanKeyword *)ScanKeywordLookup(cp[0], ScanKeywords, NumScanKeywords);
        /* function name can not be reserved keyword */
        if (keyword && RESERVED_KEYWORD == keyword->category)
            return false;
        /* and function name can not be unreserved keyword when no-parenthesis function is called.*/
        if(keyword && no_parenthesis && UNRESERVED_KEYWORD == keyword->category)
            return false;

        if (cp[1] && cp[1][0] != '\0')
            funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
        else
            funcname = list_make1(makeString(cp[0]));
        /* search the function */
        clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
        if (!clist)
        {
            if (!is_assign)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                     errmsg("function \"%s\" doesn't exist ", name)));
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
                if (!is_assign)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_FUNCTION),
                         errmsg("function \"%s\" doesn't exist ", name)));
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
            }
            ReleaseSysCache(proctup);
        }

        if (!have_outargs && is_assign)
            return false;
        
        return true;/* passed all test */
    }
    return false;
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
read_sql_construct5(int until,
                   int until2,
                   int until3,
                   int until4,
                   int until5,
                   const char *expected,
                   const char *sqlstart,
                   bool isexpression,
                   bool valid_sql,
                   bool trim,
                   int *startloc,
                   int *endtoken)
{
    int					tok;
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
    List				*list_left_bracket = 0;
    List*				list_right_bracket = 0;
    const char			left_bracket[2] = "[";
    const char			right_bracket[2] = "]";
    const char			left_brack[2] = "(";
    const char			right_brack[2] = ")";
    int					loc = 0;
    int					curloc = 0;

    /*
     * ds will do a lot of enlarge, which need to relloc the space, and the old space 
     * will be return to current context. So if we don't switch the context, the original 
     * context will be Plpgsql function context, which has a long term life cycle, 
     * may cause memory accumulation.
     */
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    initStringInfo(&ds);
    MemoryContextSwitchTo(oldCxt);

    appendStringInfoString(&ds, sqlstart);

    /* special lookup mode for identifiers within the SQL text */
    save_IdentifierLookup = u_sess->plsql_cxt.plpgsql_IdentifierLookup;
    u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

    for (;;)
    {
        tok = yylex();
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
        if (tok == '(' || tok == '[')
            parenlevel++;
        else if (tok == ')' || tok == ']')
        {
            parenlevel--;
            if (parenlevel < 0)
                yyerror("mismatched parentheses");
        }
        /*
         * End of function definition is an error, and we don't expect to
         * hit a semicolon either (unless it's the until symbol, in which
         * case we should have fallen out above).
         */
        if (tok == 0 || tok == ';')
        {
            if (parenlevel != 0)
                yyerror("mismatched parentheses");
            if (isexpression)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("missing \"%s\" at end of SQL expression",
                                expected),
                         parser_errposition(yylloc)));
            else
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("missing \"%s\" at end of SQL statement",
                                expected),
                         parser_errposition(yylloc)));
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
            case T_VARRAY_VAR:
                tok = yylex();
                if('(' == tok)
                {
                    list_left_bracket = lcons_int(parenlevel,list_left_bracket);
                    list_right_bracket = lcons_int(parenlevel,list_right_bracket);
                }	
                curloc = yylloc;
                plpgsql_push_back_token(tok);
                plpgsql_append_source_text(&ds, loc, curloc);
                ds_changed = true;	
                break;
            case T_ARRAY_FIRST:
                appendStringInfo(&ds, " ARRAY_LOWER(%s, 1) ", ((Value *)linitial(yylval.wdatum.idents))->val.str);
                ds_changed = true;
                break;
            case T_ARRAY_LAST:
                appendStringInfo(&ds, " ARRAY_UPPER(%s, 1) ", ((Value *)linitial(yylval.wdatum.idents))->val.str);
                ds_changed = true;
                break;
            case T_ARRAY_COUNT:
                appendStringInfo(&ds, " ARRAY_LENGTH(%s, 1) ", ((Value *)linitial(yylval.wdatum.idents))->val.str);
                ds_changed = true;
                break;
            case ')':
                if (list_right_bracket && list_right_bracket->length
                    && linitial_int(list_right_bracket) == parenlevel)
                {
                    appendStringInfoString(&ds, right_bracket);
                    list_right_bracket = list_delete_first(list_right_bracket);
                }
                else
                    appendStringInfoString(&ds,right_brack);
                ds_changed = true;
                break;
            case '(':
                if (list_left_bracket && list_left_bracket->length
                    && linitial_int(list_left_bracket) == parenlevel-1)
                {
                    appendStringInfoString(&ds, left_bracket);
                    list_left_bracket = list_delete_first(list_left_bracket);
                }
                else
                    appendStringInfoString(&ds, left_brack);
                ds_changed = true;
                break;
            case T_VARRAY:
                if ('(' == yylex() && ')' == yylex())
                {
                    snprintf(buf, sizeof(buf), " NULL ");
                    appendStringInfoString(&ds, buf);
                    ds_changed = true;
                }
                else
                {
                    /* do nothing */
                }
                break;
            default:
                tok = yylex();

                if(tok > INT_MAX)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                             errmsg("token value %d is bigger than INT_MAX ", tok)));
                }

                curloc = yylloc;
                plpgsql_push_back_token(tok);
                plpgsql_append_source_text(&ds, loc, curloc);
                ds_changed = true;
                break;
        }
    }

    u_sess->plsql_cxt.plpgsql_IdentifierLookup = save_IdentifierLookup;

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

    expr = (PLpgSQL_expr *)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype			= PLPGSQL_DTYPE_EXPR;
    expr->query			= pstrdup(ds.data);
    expr->plan			= NULL;
    expr->paramnos		= NULL;
    expr->ns			= plpgsql_ns_top();
    expr->isouttype 		= false;
    expr->idx			= (uint32)-1;

    pfree_ext(ds.data);

    if (valid_sql)
        check_sql_expr(expr->query, startlocation, strlen(sqlstart));

    return expr;
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
    return read_sql_construct5(until, until2, until3, until3, 0,
                               expected, sqlstart, isexpression, valid_sql, trim, startloc, endtoken);
}

static PLpgSQL_type *
read_datatype(int tok)
{
    StringInfoData		ds;
    char			   *type_name;
    int					startlocation;
    PLpgSQL_type		*result;
    int					parenlevel = 0;

    /* Should only be called while parsing DECLARE sections */
    AssertEreport(u_sess->plsql_cxt.plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_DECLARE,
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
    if (tok == T_WORD)
    {
        char   *dtname = yylval.word.ident;

        tok = yylex();
        if (tok == '%')
        {
            tok = yylex();
            if (tok_is_keyword(tok, &yylval,
                               K_TYPE, "type"))
            {
                result = plpgsql_parse_wordtype(dtname);
                if (result)
                    return result;
            }
            else if (tok_is_keyword(tok, &yylval,
                                    K_ROWTYPE, "rowtype"))
            {
                PLpgSQL_nsitem *ns;
                ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, 
                            dtname, NULL, NULL, NULL);
                if (ns && ns->itemtype == PLPGSQL_NSTYPE_VAR)
                {
                    PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.plpgsql_Datums[ns->itemno];
                    if(var && var->datatype 
                           && var->datatype->typoid == REFCURSOROID)
                    {
                        return plpgsql_build_datatype(RECORDOID, -1, InvalidOid);
                    }
                }
                result = plpgsql_parse_wordrowtype(dtname);
                if (result)
                    return result;
            }
        }
    }
    else if (tok == T_CWORD)
    {
        List   *dtnames = yylval.cword.idents;

        tok = yylex();
        if (tok == '%')
        {
            tok = yylex();
            if (tok_is_keyword(tok, &yylval,
                               K_TYPE, "type"))
            {
                result = plpgsql_parse_cwordtype(dtnames);
                if (result)
                    return result;
            }
            else if (tok_is_keyword(tok, &yylval,
                                    K_ROWTYPE, "rowtype"))
            {
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
                yyerror("mismatched parentheses");
            else
                yyerror("incomplete data type declaration");
        }
        /* Possible followers for datatype in a declaration */
        if (tok == K_COLLATE || tok == K_NOT ||
            tok == '=' || tok == COLON_EQUALS || tok == K_DEFAULT)
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

    if (type_name[0] == '\0')
        yyerror("missing data type declaration");

    result = parse_datatype(type_name, startlocation);

    pfree_ext(ds.data);

    plpgsql_push_back_token(tok);

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
    bool				have_into = false;
    bool				have_strict = false;
    int					into_start_loc = -1;
    int					into_end_loc = -1;
    int					placeholders = 0;

    initStringInfo(&ds);

    /* special lookup mode for identifiers within the SQL text */
    save_IdentifierLookup = u_sess->plsql_cxt.plpgsql_IdentifierLookup;
    u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

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

    for (;;)
    {
        prev_tok = tok;
        tok = yylex();

        if (have_into && into_end_loc < 0)
            into_end_loc = yylloc;		/* token after the INTO part */
        if (tok == ';')
            break;
        if (tok == 0)
            yyerror("unexpected end of function definition");

        if(tok == T_PLACEHOLDER)
        {
            placeholders++;
        }

        if (tok == K_INTO)
        {
            if (prev_tok == K_INSERT)
                continue;	/* INSERT INTO is not an INTO-target */
            if (firsttoken == K_ALTER)
                continue;	/* ALTER ... INTO is not an INTO-target */
            if (prev_tok == K_MERGE)
                continue;	/* MERGE INTO is not an INTO-target */
            if (have_into)
                yyerror("INTO specified more than once");
            have_into = true;
            into_start_loc = yylloc;
            u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
            read_into_target(&rec, &row, &have_strict);
            u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
        }
    }

    u_sess->plsql_cxt.plpgsql_IdentifierLookup = save_IdentifierLookup;

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
    }
    else
        plpgsql_append_source_text(&ds, location, yylloc);

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
    pfree_ext(ds.data);

    check_sql_expr(expr->query, location, 0);

    execsql = (PLpgSQL_stmt_execsql *)palloc(sizeof(PLpgSQL_stmt_execsql));
    execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
    execsql->lineno  = plpgsql_location_to_lineno(location);
    execsql->sqlstmt = expr;
    execsql->into	 = have_into;
    execsql->strict	 = have_strict;
    execsql->rec	 = rec;
    execsql->row	 = row;
    execsql->placeholders = placeholders;

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
    else if (tok == T_DATUM)
    {
        /* Assume there's no direction clause and tok is a cursor name */
        plpgsql_push_back_token(tok);
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

    if (u_sess->plsql_cxt.plpgsql_curr_compile->fn_retset)
    {
        if (yylex() != ';')
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("RETURN cannot have a parameter in function returning set"),
                     errhint("Use RETURN NEXT or RETURN QUERY."),
                     parser_errposition(yylloc)));
    }

    // adapting A db, where return value is independent from OUT parameters 
    else if (';' == (token = yylex()) && u_sess->plsql_cxt.plpgsql_curr_compile->out_param_varno >= 0)
        newp->retvarno = u_sess->plsql_cxt.plpgsql_curr_compile->out_param_varno;
    else
    {
        plpgsql_push_back_token(token);
        if (u_sess->plsql_cxt.plpgsql_curr_compile->fn_rettype == VOIDOID)
        {
            if (yylex() != ';')
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("RETURN cannot have a parameter in function returning void"),
                         parser_errposition(yylloc)));
        }
        else if (u_sess->plsql_cxt.plpgsql_curr_compile->fn_retistuple)
        {
            int     tok = yylex();
            if(tok < 0)
            {
                ereport(ERROR,
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
                        yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
                        newp->retvarno = yylval.wdatum.datum->dno;
                    else
                        ereport(ERROR,
                                (errcode(ERRCODE_DATATYPE_MISMATCH),
                                 errmsg("RETURN must specify a record or row variable in function returning row"),
                                 parser_errposition(yylloc)));
                    break;

                default:
                    ereport(ERROR,
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
            newp->expr = read_sql_expression(';', ";");
        }
    }

    return (PLpgSQL_stmt *) newp;
}


static PLpgSQL_stmt *
make_return_next_stmt(int location)
{
    PLpgSQL_stmt_return_next *newp;

    if (!u_sess->plsql_cxt.plpgsql_curr_compile->fn_retset)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cannot use RETURN NEXT in a non-SETOF function"),
                 parser_errposition(location)));

    newp = (PLpgSQL_stmt_return_next *)palloc0(sizeof(PLpgSQL_stmt_return_next));
    newp->cmd_type	= PLPGSQL_STMT_RETURN_NEXT;
    newp->lineno		= plpgsql_location_to_lineno(location);
    newp->expr		= NULL;
    newp->retvarno	= -1;

    if (u_sess->plsql_cxt.plpgsql_curr_compile->out_param_varno >= 0)
    {
        if (yylex() != ';')
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("RETURN NEXT cannot have a parameter in function with OUT parameters"),
                     parser_errposition(yylloc)));
        newp->retvarno = u_sess->plsql_cxt.plpgsql_curr_compile->out_param_varno;
    }
    else if (u_sess->plsql_cxt.plpgsql_curr_compile->fn_retistuple)
    {
        switch (yylex())
        {
            case T_DATUM:
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
                    newp->retvarno = yylval.wdatum.datum->dno;
                else
                    ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                             errmsg("RETURN NEXT must specify a record or row variable in function returning row"),
                             parser_errposition(yylloc)));
                break;

            default:
                ereport(ERROR,
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

    if (!u_sess->plsql_cxt.plpgsql_curr_compile->fn_retset)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("cannot use RETURN QUERY in a non-SETOF function"),
                 parser_errposition(location)));

    newp = (PLpgSQL_stmt_return_query *)palloc0(sizeof(PLpgSQL_stmt_return_query));
    newp->cmd_type = PLPGSQL_STMT_RETURN_QUERY;
    newp->lineno = plpgsql_location_to_lineno(location);

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

static void
check_assignable(PLpgSQL_datum *datum, int location)
{
    switch (datum->dtype)
    {
        case PLPGSQL_DTYPE_VAR:
            if (((PLpgSQL_var *) datum)->isconst)
                ereport(ERROR,
                        (errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
                         errmsg("\"%s\" is declared CONSTANT",
                                ((PLpgSQL_var *) datum)->refname),
                         parser_errposition(location)));
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
            /* always assignable? */
            break;
        default:
            elog(ERROR, "unrecognized dtype: %d", datum->dtype);
            break;
    }
}

/*
 * Brief		: support array variable as select into and using out target. 
 * Description	: if an array element is detected, add it to the u_sess->plsql_cxt.plpgsql_Datums[]
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
        ereport(ERROR,
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
        
        if(NULL == newp->subscript)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                  errmsg(" error near arrary name! ")));

        plpgsql_adddatum((PLpgSQL_datum *)newp);

        fieldnames[*nfields]   = pstrdup("arrayelem");
        varnos[(*nfields)++]   = newp->dno;

        *tok = yylex();

        /* is an array element, return true */
        return true;
    }

    return false;
}

/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
static void
read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row, bool *strict)
{
    int			tok;

    /* Set default results */
    *rec = NULL;
    *row = NULL;
    if (strict)
        *strict = true;

    tok = yylex();
    if (strict && tok == K_STRICT)
    {
        *strict = true;
        tok = yylex();
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
            if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW
            || yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_RECORD)
            {
                check_assignable(yylval.wdatum.datum, yylloc);
                *row = (PLpgSQL_row *) yylval.wdatum.datum;

                if ((tok = yylex()) == ',')
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("record or row variable cannot be part of multiple-item INTO list"),
                             parser_errposition(yylloc)));
                plpgsql_push_back_token(tok);
            }
            else if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
            {
                check_assignable(yylval.wdatum.datum, yylloc);
                *rec = (PLpgSQL_rec *) yylval.wdatum.datum;

                if ((tok = yylex()) == ',')
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("record or row variable cannot be part of multiple-item INTO list"),
                             parser_errposition(yylloc)));
                plpgsql_push_back_token(tok);
            }
            else
            {
                *row = read_into_array_scalar_list(NameOfDatum(&(yylval.wdatum)),
                                             yylval.wdatum.datum, yylloc);
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
                    tempdno = yylval.wdatum.datum->dno;
                    plpgsql_push_back_token(tok);
                    tempexpr  =read_sql_construct(',',';',',',", or ;","SELECT ",true,true,true,NULL,&tok);
                    tempexpr->isouttype = true;
                    *in_expr=lappend((*in_expr), tempexpr);

                    if (!read_into_using_add_arrayelem(out_fieldnames, out_varnos, &out_nfields,tempdno,&tok)) 
                    {
                        out_fieldnames[out_nfields] = tempvar;
                        out_varnos[out_nfields++]	= tempdno;
                    }
                    else
                    {
                        if (isin)
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                    errmsg(" using can't support array parameter with in out !")));

                    }

                    break;

                default:
                    ereport(ERROR,
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
                      int initial_location)
{
    int				 nfields;
    char			*fieldnames[1024] = {NULL};
    int				 varnos[1024] = {0};
    PLpgSQL_row		*row;
    int				 tok;

    check_assignable(initial_datum, initial_location);
    fieldnames[0] = initial_name;
    varnos[0]	  = initial_datum->dno;
    nfields		  = 1;

    while ((tok = yylex()) == ',')
    {
        /* Check for array overflow */
        if (nfields >= 1024)
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO variables specified"),
                     parser_errposition(yylloc)));

        tok = yylex();
        switch (tok)
        {
            case T_DATUM:
                check_assignable(yylval.wdatum.datum, yylloc);
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("\"%s\" is not a scalar variable",
                                    NameOfDatum(&(yylval.wdatum))),
                             parser_errposition(yylloc)));
                fieldnames[nfields] = NameOfDatum(&(yylval.wdatum));
                varnos[nfields++]	= yylval.wdatum.datum->dno;
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
read_into_array_scalar_list(char *initial_name,
                      PLpgSQL_datum *initial_datum,
                      int initial_location)
{
    int				 nfields = 0;
    char			*fieldnames[1024] = {NULL};
    int				 varnos[1024] = {0};
    PLpgSQL_row		*row;
    int				 tok;
    int				 toktmp = 0;
    int 			 tmpdno = 0;
    char* 			 nextname = NULL;

    check_assignable(initial_datum, initial_location);
    tmpdno = yylval.wdatum.datum->dno;
    tok = yylex();
    if (!read_into_using_add_arrayelem(fieldnames, varnos, &nfields, tmpdno, &tok))
    {
        fieldnames[0] = initial_name;
        varnos[0]	  = initial_datum->dno;
        nfields		  = 1;
    }
    while (',' == tok)
    {
        /* Check for array overflow */
        if (nfields >= 1024)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO variables specified"),
                     parser_errposition(yylloc)));
            return NULL;
        }

        toktmp = yylex();

        switch (toktmp)
        {
            case T_DATUM:
                check_assignable(yylval.wdatum.datum, yylloc);
                
                if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
                    yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
                    ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("\"%s\" is not a scalar variable",
                                    NameOfDatum(&(yylval.wdatum))),
                            parser_errposition(yylloc)));

                tmpdno = yylval.wdatum.datum->dno;
                nextname = NameOfDatum(&(yylval.wdatum));
                fieldnames[nfields] = nextname;
                varnos[nfields++] = tmpdno;
                tok = yylex();
                break;
            case T_VARRAY_VAR:
                check_assignable(yylval.wdatum.datum, yylloc);
                tmpdno = yylval.wdatum.datum->dno;
                tok = yylex();
                if (tok < -1)
                    return NULL;
                if (!read_into_using_add_arrayelem(fieldnames, varnos, &nfields, tmpdno, &tok))
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg(" error near arrary name! ")));
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
        if ( intoplaceholders >= 1024 )
            ereport(ERROR,
                    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                     errmsg("too many INTO placeholders specified"),
                     parser_errposition(yylloc)));

        tok = yylex();
        switch (tok)
        {
            case T_PLACEHOLDER:
                intoplaceholders++;
                break;

            default:
                ereport(ERROR,
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
                  int lineno, int location)
{
    PLpgSQL_row		*row;

    check_assignable(initial_datum, location);

    row = (PLpgSQL_row *)palloc(sizeof(PLpgSQL_row));
    row->dtype = PLPGSQL_DTYPE_ROW;
    row->refname = pstrdup("*internal*");
    row->lineno = lineno;
    row->rowtupdesc = NULL;
    row->nfields = 1;
    row->fieldnames = (char **)palloc(sizeof(char *));
    row->varnos = (int *)palloc(sizeof(int));
    row->fieldnames[0] = initial_name;
    row->varnos[0] = initial_datum->dno;

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

    if (!u_sess->plsql_cxt.plpgsql_check_syntax)
        return;

    cbarg.location = location;
    cbarg.leaderlen = leaderlen;

    syntax_errcontext.callback = plpgsql_sql_error_callback;
    syntax_errcontext.arg = &cbarg;
    syntax_errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &syntax_errcontext;

    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    (void) raw_parser(stmt);
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
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);

    /* Let the main parser try to parse it under standard SQL rules */
    parseTypeString(string, &type_id, &typmod);

    (void)MemoryContextSwitchTo(oldCxt);

    /* Restore former ereport callback */
    t_thrd.log_cxt.error_context_stack = syntax_errcontext.previous;

    /* Okay, build a PLpgSQL_type data structure for it */
    return plpgsql_build_datatype(type_id, typmod,
                                  u_sess->plsql_cxt.plpgsql_curr_compile->fn_input_collation);
}

/*
 * Check block starting and ending labels match.
 */
static void
check_labels(const char *start_label, const char *end_label, int end_location)
{
    if (end_label)
    {
        if (!start_label)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("end label \"%s\" specified for unlabelled block",
                            end_label),
                     parser_errposition(end_location)));

        if (strcmp(start_label, end_label) != 0)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("end label \"%s\" differs from block's label \"%s\"",
                            end_label, start_label),
                     parser_errposition(end_location)));
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
        if (tok == '(')
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("cursor \"%s\" has no arguments",
                            cursor->refname),
                     parser_errposition(yylloc)));

        if (tok != until)
            yyerror("syntax error");

        return NULL;
    }

    /* Else better provide arguments */
    if (tok != '(')
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("cursor \"%s\" has arguments",
                        cursor->refname),
                 parser_errposition(yylloc)));

    /*
     * Read the arguments, one by one.
     */
    row = (PLpgSQL_row *) u_sess->plsql_cxt.plpgsql_Datums[cursor->cursor_explicit_argrow];
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
            save_IdentifierLookup = u_sess->plsql_cxt.plpgsql_IdentifierLookup;
            u_sess->plsql_cxt.plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
            yylex();
            argname = yylval.str;
            u_sess->plsql_cxt.plpgsql_IdentifierLookup = save_IdentifierLookup;

            /* Match argument name to cursor arguments */
            for (argpos = 0; argpos < row->nfields; argpos++)
            {
                if (strcmp(row->fieldnames[argpos], argname) == 0)
                    break;
            }
            if (argpos == row->nfields)
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("cursor \"%s\" has no argument named \"%s\"",
                                cursor->refname, argname),
                         parser_errposition(yylloc)));

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

        if (argv[argpos] != NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("value for parameter \"%s\" of cursor \"%s\" specified more than once",
                            row->fieldnames[argpos], cursor->refname),
                     parser_errposition(arglocation)));

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

        if (endtoken == ')' && !(argc == row->nfields - 1))
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("not enough arguments for cursor \"%s\"",
                            cursor->refname),
                     parser_errposition(yylloc)));

        if (endtoken == ',' && (argc == row->nfields - 1))
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("too many arguments for cursor \"%s\"",
                            cursor->refname),
                     parser_errposition(yylloc)));
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
                 u_sess->plsql_cxt.plpgsql_nDatums);

        /*
         * We don't yet know the result datatype of t_expr.  Build the
         * variable as if it were INT4; we'll fix this at runtime if needed.
         */
        t_var = (PLpgSQL_var *)
            plpgsql_build_variable(varname, newp->lineno,
                                   plpgsql_build_datatype(INT4OID,
                                                          -1,
                                                          InvalidOid),
                                   true);
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
make_callfunc_stmt_no_arg(const char *sqlstart, int location)
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
    oldCxt = MemoryContextSwitchTo(u_sess->plsql_cxt.compile_tmp_cxt);
    /* get the function's name. */
    plpgsql_parser_funcname(sqlstart, cp, 3);

    if (cp[2] != NULL && cp[2][0] != '\0')
        funcname = list_make3(makeString(cp[0]), makeString(cp[1]), makeString(cp[2]));
    else if (cp[1] && cp[1][0] != '\0')
        funcname = list_make2(makeString(cp[0]), makeString(cp[1]));
    else
        funcname = list_make1(makeString(cp[0]));

    /* search the function */
    clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
    if (clist == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function \"%s\" doesn't exist ", sqlstart)));
    }

    proctup = SearchSysCache(PROCOID,
                            ObjectIdGetDatum(clist->oid),
                            0, 0, 0);
    /* function may be deleted after clist be searched. */
    if (!HeapTupleIsValid(proctup))
    {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function \"%s\" doesn't exist ", sqlstart)));
    }
    /* get the all args informations, only "in" parameters if p_argmodes is null */
    narg = get_func_arg_info(proctup,&p_argtypes, &p_argnames, &p_argmodes);
    ReleaseSysCache(proctup);
    if (narg != 0)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("function %s has no enough parameters", sqlstart)));

    initStringInfo(&func_inparas);

    appendStringInfoString(&func_inparas, "CALL ");

    quoted_sqlstart = NameListToQuotedString(funcname);
    appendStringInfoString(&func_inparas, quoted_sqlstart);
    pfree_ext(quoted_sqlstart);

    appendStringInfoString(&func_inparas, "(");

    appendStringInfoString(&func_inparas, ")");

    /* read the end token */
    yylex();

    (void)MemoryContextSwitchTo(oldCxt);

    /* generate the expression */
    expr = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype = PLPGSQL_DTYPE_EXPR;
    expr->query = pstrdup(func_inparas.data);
    expr->plan = NULL;
    expr->paramnos = NULL;
    expr->ns = plpgsql_ns_top();
    expr->idx = (uint32)-1;

    PLpgSQL_stmt_perform *perform = NULL;
    perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
    perform->cmd_type = PLPGSQL_STMT_PERFORM;
    perform->lineno = plpgsql_location_to_lineno(location);
    perform->expr = expr;

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

		perform = (PLpgSQL_stmt_perform*)palloc0(sizeof(PLpgSQL_stmt_perform));
		perform->cmd_type = PLPGSQL_STMT_PERFORM;
		perform->lineno = plpgsql_location_to_lineno(location);
		perform->expr = expr;
		return (PLpgSQL_stmt *)perform;
	}
	yyerror("syntax error");
	return NULL;
}
