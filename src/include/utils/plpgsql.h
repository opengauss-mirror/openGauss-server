/*---------------------------------------------------------------------------------------
 * 
 * plpgsql.h
 *     Definitions for the PL/pgSQL procedural language
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * Portions Copyright (c) 2021, openGauss Contributors
 * IDENTIFICATION
 *        src/include/utils/plpgsql.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PLPGSQL_H
#define PLPGSQL_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "executor/functions.h"
#include "parser/scanner.h"
#include "utils/pl_global_package_runtime_cache.h"

/**********************************************************************
 * Definitions
 **********************************************************************/

/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("plpgsql")

#undef _
#define _(x) dgettext(TEXTDOMAIN, x)

#define TABLEOFINDEXBUCKETNUM 128

/*
 * Compile status mark
 */
enum {
    /* compile new function */
    COMPILIE_FUNC,
    /* compile new package */
    COMPILIE_PKG,
    /* compile new function in package */
    COMPILIE_PKG_FUNC,
    /* compile anonyous block in package */
    COMPILIE_PKG_ANON_BLOCK,
    /* compile anonyous block's func in package */
    COMPILIE_PKG_ANON_BLOCK_FUNC,
    /* compile anonyous block */
    COMPILIE_ANON_BLOCK,
    NONE_STATUS
};

/* ----------
 * Compiler's namespace item types
 * ----------
 */
enum {
    PLPGSQL_NSTYPE_LABEL,
    PLPGSQL_NSTYPE_VAR,
    PLPGSQL_NSTYPE_ROW,
    PLPGSQL_NSTYPE_REC,
    PLPGSQL_NSTYPE_RECORD,
    PLPGSQL_NSTYPE_REFCURSOR,
    PLPGSQL_NSTYPE_VARRAY,
    PLPGSQL_NSTYPE_TABLE,
    PLPGSQL_NSTYPE_PROC,
    PLPGSQL_NSTYPE_UNKNOWN,
    PLPGSQL_NSTYPE_COMPOSITE,
    PLPGSQL_NSTYPE_GOTO_LABEL
};

/* ----------
 * Datum array node types
 * ----------
 */
enum {
    PLPGSQL_DTYPE_VAR,
    PLPGSQL_DTYPE_ROW,
    PLPGSQL_DTYPE_REC,
    PLPGSQL_DTYPE_RECORD, /* record variable */
    PLPGSQL_DTYPE_RECFIELD,
    PLPGSQL_DTYPE_ARRAYELEM,
    PLPGSQL_DTYPE_TABLEELEM,
    PLPGSQL_DTYPE_EXPR,
    PLPGSQL_DTYPE_UNKNOWN,
    PLPGSQL_DTYPE_VARRAY,
    PLPGSQL_DTYPE_TABLE,
    PLPGSQL_DTYPE_ASSIGNLIST,
    PLPGSQL_DTYPE_COMPOSITE, /* composite type */
    PLPGSQL_DTYPE_RECORD_TYPE /* record type */
};

/* ----------
 * Compile status
 * ----------
 */
enum {
    PLPGSQL_COMPILE_PACKAGE,
    PLPGSQL_COMPILE_PROC,
    PLPGSQL_COMPILE_PACKAGE_PROC,
    PLPGSQL_COMPILE_NULL
};


/* ----------
 * drop status
 * ----------
 */
enum {
    PLPGSQL_PACKAGE,
    PLPGSQL_PACKAGE_BODY,
    PLPGSQL_PROC
};

// enums token flag
enum {
    PLPGSQL_TOK_REFCURSOR,
    PLPGSQL_TOK_VARRAY,
    PLPGSQL_TOK_VARRAY_FIRST,
    PLPGSQL_TOK_VARRAY_LAST,
    PLPGSQL_TOK_VARRAY_COUNT,
    PLPGSQL_TOK_VARRAY_EXTEND,
    PLPGSQL_TOK_VARRAY_EXISTS,
    PLPGSQL_TOK_VARRAY_PRIOR,
    PLPGSQL_TOK_VARRAY_NEXT,
    PLPGSQL_TOK_VARRAY_DELETE,
    PLPGSQL_TOK_VARRAY_TRIM,
    PLPGSQL_TOK_VARRAY_VAR,
    PLPGSQL_TOK_TABLE,
    PLPGSQL_TOK_TABLE_VAR,
    PLPGSQL_TOK_RECORD,
    PLPGSQL_TOK_PACKAGE_VARIABLE
};

/* ----------
 * Variants distinguished in PLpgSQL_type structs
 * ----------
 */
enum {
    PLPGSQL_TTYPE_SCALAR, /* scalar types and domains */
    PLPGSQL_TTYPE_ROW,    /* composite types */
    PLPGSQL_TTYPE_REC,    /* RECORD pseudotype */
    PLPGSQL_TTYPE_RECORD, /* RECORD pseudotype complitable A db */
    PLPGSQL_TTYPE_PSEUDO  /* other pseudotypes */
};

/* ----------
 * distinguish array and table in PLpgSQL_type structs
 * ----------
 */
enum {
    PLPGSQL_COLLECTION_NONE = 0,
    PLPGSQL_COLLECTION_ARRAY,
    PLPGSQL_COLLECTION_TABLE
};

/* ----------
 * Execution tree node types
 * ----------
 */
enum PLpgSQL_stmt_types {
    PLPGSQL_STMT_BLOCK,
    PLPGSQL_STMT_ASSIGN,
    PLPGSQL_STMT_IF,
    PLPGSQL_STMT_GOTO,
    PLPGSQL_STMT_CASE,
    PLPGSQL_STMT_LOOP,
    PLPGSQL_STMT_WHILE,
    PLPGSQL_STMT_FORI,
    PLPGSQL_STMT_FORS,
    PLPGSQL_STMT_FORC,
    PLPGSQL_STMT_FOREACH_A,
    PLPGSQL_STMT_EXIT,
    PLPGSQL_STMT_RETURN,
    PLPGSQL_STMT_RETURN_NEXT,
    PLPGSQL_STMT_RETURN_QUERY,
    PLPGSQL_STMT_RAISE,
    PLPGSQL_STMT_EXECSQL,
    PLPGSQL_STMT_DYNEXECUTE,
    PLPGSQL_STMT_DYNFORS,
    PLPGSQL_STMT_GETDIAG,
    PLPGSQL_STMT_OPEN,
    PLPGSQL_STMT_FETCH,
    PLPGSQL_STMT_CLOSE,
    PLPGSQL_STMT_PERFORM,
    PLPGSQL_STMT_COMMIT,
    PLPGSQL_STMT_ROLLBACK,
    PLPGSQL_STMT_NULL,
    PLPGSQL_STMT_SAVEPOINT
};

/* ----------
 * Execution node return codes
 * ----------
 */
enum { PLPGSQL_RC_OK, PLPGSQL_RC_EXIT, PLPGSQL_RC_RETURN, PLPGSQL_RC_CONTINUE, PLPGSQL_RC_GOTO_UNRESOLVED };

/* ----------
 * GET DIAGNOSTICS information items
 * ----------
 */
enum {
    PLPGSQL_GETDIAG_ROW_COUNT,
    PLPGSQL_GETDIAG_RESULT_OID,
    PLPGSQL_GETDIAG_ERROR_CONTEXT,
    PLPGSQL_GETDIAG_ERROR_DETAIL,
    PLPGSQL_GETDIAG_ERROR_HINT,
    PLPGSQL_GETDIAG_RETURNED_SQLSTATE,
    PLPGSQL_GETDIAG_MESSAGE_TEXT
};

/* --------
 * RAISE statement options
 * --------
 */
enum { PLPGSQL_RAISEOPTION_ERRCODE, PLPGSQL_RAISEOPTION_MESSAGE, PLPGSQL_RAISEOPTION_DETAIL, PLPGSQL_RAISEOPTION_HINT };

/* --------
 * Behavioral modes for plpgsql variable resolution
 * --------
 */
typedef enum {
    PLPGSQL_RESOLVE_ERROR,    /* throw error if ambiguous */
    PLPGSQL_RESOLVE_VARIABLE, /* prefer plpgsql var to table column */
    PLPGSQL_RESOLVE_COLUMN    /* prefer table column to plpgsql var */
} PLpgSQL_resolve_option;

/* --------
 * State of cursor found/notfound variable
 * --------
 */
typedef enum { PLPGSQL_TRUE, PLPGSQL_FALSE, PLPGSQL_NULL } PLpgSQL_state;

/**********************************************************************
 * Node and structure definitions
 **********************************************************************/
/*
 * PLpgSQL_datum is the common supertype for PLpgSQL_expr, PLpgSQL_var,
 * PLpgSQL_row, PLpgSQL_rec, PLpgSQL_recfield, and PLpgSQL_arrayelem
 */
typedef struct PLpgSQL_datum { /* Generic datum array item		*/
    int dtype;
    int dno;
    bool ispkg;
} PLpgSQL_datum;
/*
 * The variants PLpgSQL_var, PLpgSQL_row, and PLpgSQL_rec share these
 * fields
 */
typedef struct { /* Scalar or composite variable */
    int dtype;
    int dno;
    bool ispkg;
    char* refname;
    int lineno;
    bool isImplicit;
    bool addNamespace;
    bool varname;
} PLpgSQL_variable;

typedef struct PLpgSQL_expr { /* SQpL Query to plan and execute	*/
    int dtype;
    int dno;
    bool ispkg;
    char* query;
    SPIPlanPtr plan;
    Bitmapset* paramnos; /* all dnos referenced by this query */

    /* function containing this expr (not set until we first parse query) */
    struct PLpgSQL_function* func;

    /* namespace chain visible to this expr */
    struct PLpgSQL_nsitem* ns;

    /* fields for "simple expression" fast-path execution: */
    Expr* expr_simple_expr;         /* NULL means not a simple expr */
    int expr_simple_generation;     /* plancache generation we checked */
    Oid expr_simple_type;           /* result type Oid, if simple */
	int32 expr_simple_typmod; /* result typmod, if simple */
	bool expr_simple_mutable;	/* true if simple expr is mutable */
    bool expr_simple_need_snapshot; /* true means need snapshot */

	/*
	 * If the expression was ever determined to be simple, we remember its
	 * CachedPlanSource and CachedPlan here.  If expr_simple_plan_lxid matches
	 * current LXID, then we hold a refcount on expr_simple_plan in the
	 * current transaction.  Otherwise we need to get one before re-using it.
	 */
	CachedPlanSource *expr_simple_plansource;	/* extracted from "plan" */
	CachedPlan *expr_simple_plan;	/* extracted from "plan" */
	LocalTransactionId expr_simple_plan_lxid;

    /*
     * if expr is simple AND prepared in current transaction,
     * expr_simple_state and expr_simple_in_use are valid. Test validity by
     * seeing if expr_simple_lxid matches current LXID.  (If not,
     * expr_simple_state probably points at garbage!)
     */
    ExprState* expr_simple_state; /* eval tree for expr_simple_expr */
    bool expr_simple_in_use;      /* true if eval tree is active */
    LocalTransactionId expr_simple_lxid;
    bool isouttype;               /* the parameter will output */
    bool is_funccall;
    int out_param_dno;            /* if expr is func call and param is seperate from return values */
    uint32 idx;
    bool is_cachedplan_shared;
    /* if the expr have table of index var, we should found its param. */
    bool is_have_tableof_index_var;
    int tableof_var_dno;
    /* if the expr have table of index function, we should found its args->param. */
    bool is_have_tableof_index_func;
    /* dno maybe is 0, so need an extra variable */
    int tableof_func_dno;
} PLpgSQL_expr;

typedef struct { /* openGauss data type */
    int dtype;
    int dno;
    bool ispkg;
    char* typname; /* (simple) name of the type */
    Oid typoid;    /* OID of the data type */
    int ttype;     /* PLPGSQL_TTYPE_ code */
    int16 typlen;  /* stuff copied from its pg_type entry */
    bool typbyval;
    Oid typrelid;
    Oid typioparam;
    Oid collation;     /* from pg_type, but can be overridden */
    FmgrInfo typinput; /* lookup info for typinput function */
    int32 atttypmod;   /* typmod (taken from someplace else) */
    char* typnamespace; /* name of typnamespace */
    int collectionType;     /* PLPGSQL_COLLECTION_ code */
    Oid tableOfIndexType;
    /*
     * If a defined cursor is found during the compilation of the function,
     * parse and rewrite the sql immediately to get the query targetlist,
     * then convert to tuple descriptior.
     */
    Oid cursorCompositeOid = InvalidOid;
} PLpgSQL_type;

typedef struct {
    Oid userId;
    int secContext;
    int level;
} transactionNode;

typedef struct PLpgSQL_var { /* Scalar variable */
    int dtype;
    int dno;
    bool ispkg;
    char* refname;
    int lineno;
    bool isImplicit;
    bool addNamespace;
    char* varname;

    PLpgSQL_type* datatype;
    int isconst;
    int notnull;
    PLpgSQL_expr* default_val;
    PLpgSQL_expr* cursor_explicit_expr;
    int cursor_explicit_argrow;
    int cursor_options;

    Datum value;
    bool isnull;
    bool freeval;
    bool is_cursor_var;  /* variable is a refcursor */
    bool is_cursor_open; /* mark var is isopen for isopen option shoule be always not null */
    bool cursor_closed; /* if the var is cursor, mark the cursor is closed by close cursor? */
    List* pkg_name = NULL;
    PLpgSQL_package* pkg = NULL;
    Oid tableOfIndexType = InvalidOid; /* type Oid of table of */
    bool isIndexByTblOf;
    struct PLpgSQL_var* nest_table; /* origin nest table type, copy from it when add new nest table */
    HTAB* tableOfIndex = NULL; /* mapping of table of index */
    int nest_layers = 0;
} PLpgSQL_var;

typedef struct { /* Row variable */
    int dtype;
    int dno;
    bool ispkg;
    char* refname;
    int lineno;
    bool isImplicit;
    bool addNamespace;
    char* varname;

    TupleDesc rowtupdesc;

    /*
     * Note: TupleDesc is only set up for named rowtypes, else it is NULL.
     *
     * Note: if the underlying rowtype contains a dropped column, the
     * corresponding fieldnames[] entry will be NULL, and there is no
     * corresponding var (varnos[] will be -1).
     */
    int nfields;
    char** fieldnames;
    int* varnos; /* only use for unpkg's var, pkg's var is in row->pkg->datums */
    int customErrorCode; /* only for exception variable. */

    int intoplaceholders; /* number of placeholders, for anonymous block in dynamic stmt */
    PLpgSQL_datum** intodatums;
    List* pkg_name = NULL;
    PLpgSQL_package* pkg = NULL;
    PLpgSQL_expr* default_val = NULL;
    Oid recordVarTypOid; /* package record var's composite type oid */
    bool hasExceptionInit;
} PLpgSQL_row;

typedef struct {
    char* attrname;
    PLpgSQL_type* type;
    bool notnull;
    PLpgSQL_expr* defaultvalue;
} PLpgSQL_rec_attr;

typedef struct {
    int dtype;
    int dno;
    bool ispkg;
    char* typname; /* (simple) name of the type */
    Oid typoid;    /* OID of the data type */
    int ttype;     /* PLPGSQL_TTYPE_ code */
    int16 typlen;  /* stuff copied from its pg_type entry */
    bool typbyval;
    Oid typrelid;
    Oid typioparam;
    Oid collation;     /* from pg_type, but can be overridden */
    FmgrInfo typinput; /* lookup info for typinput function */
    int32 atttypmod;   /* typmod (taken from someplace else) */
    int attrnum;
    char** attrnames;
    PLpgSQL_type** types;
    bool* notnulls;
    bool addNamespace;
    PLpgSQL_expr** defaultvalues;
} PLpgSQL_rec_type;

typedef struct { /* Record variable (non-fixed structure) */
    int dtype;
    int dno;
    bool ispkg;
    char* refname;
    int lineno;
    bool isImplicit;
    bool addNamespace;
    char* varname;

    HeapTuple tup;
    TupleDesc tupdesc;
    bool freetup;
    bool freetupdesc;
    List* pkg_name = NULL;
    PLpgSQL_package* pkg = NULL;
} PLpgSQL_rec;

typedef struct { /* Field in record */
    int dtype;
    int dno;
    bool ispkg;
    char* fieldname;
    int recparentno; /* dno of parent record */
} PLpgSQL_recfield;

typedef struct { /* Element of array variable */
    int dtype;
    int dno;
    bool ispkg;
    PLpgSQL_expr* subscript;
    int arrayparentno; /* dno of parent array variable */
    /* Remaining fields are cached info about the array variable's type */
    Oid parenttypoid;   /* type of array variable; 0 if not yet set */
    int32 parenttypmod; /* typmod of array variable */
    Oid arraytypoid;    /* OID of actual array type */
    int32 arraytypmod;  /* typmod of array (and its elements too) */
    int16 arraytyplen;  /* typlen of array type */
    Oid elemtypoid;     /* OID of array element type */
    int16 elemtyplen;   /* typlen of element type */
    bool elemtypbyval;  /* element type is pass-by-value? */
    char elemtypalign;  /* typalign of element type */
    AttrNumber assignattrno; /* index of assign attribute */
    List* pkg_name = NULL;
    PLpgSQL_package* pkg = NULL;
} PLpgSQL_arrayelem;

typedef struct { /* assign list */
    int dtype;
    int dno;
    bool ispkg;
    List* assignlist; /* assign list, contains subscripts or attributes */
    int targetno; /* the dno of assign target */
} PLpgSQL_assignlist;

typedef struct { /* Scalar variable */
    int dtype;
    int dno;
    bool ispkg;
    bool isnull;
    Datum value;
    char* attrname;
    uint32 attnum;
    bool isarrayelem;
    int* subscriptvals;
    int nsubscripts;
    Oid typoid;
    int32 typmod;
} PLpgSQL_temp_assignvar;

typedef struct { /* Element of table variable */
    int dtype;
    int dno;
    bool ispkg;
    PLpgSQL_expr* subscript;
    int tableparentno; /* dno of parent table variable */
    /* Remaining fields are cached info about the array variable's type */
    Oid parenttypoid;   /* type of table variable; 0 if not yet set */
    int32 parenttypmod; /* typmod of table variable */
    Oid tabletypoid;    /* OID of actual table type */
    int32 tabletypmod;  /* typmod of table (and its elements too) */
    int16 tabletyplen;  /* typlen of table type */
    Oid elemtypoid;     /* OID of table element type */
    int16 elemtyplen;   /* typlen of element type */
    bool elemtypbyval;  /* element type is pass-by-value? */
    char elemtypalign;  /* typalign of element type */
    AttrNumber assignattrno; /* index of assign attribute */
    List* pkg_name = NULL;
    PLpgSQL_package* pkg = NULL;
} PLpgSQL_tableelem;

typedef struct {
    Oid exprtypeid;
    Datum exprdatum;
} TableOfIndexKey;

typedef struct {
    TableOfIndexKey key;
    int index;
    struct PLpgSQL_var* var;
} TableOfIndexEntry;

typedef struct PLpgSQL_nsitem { /* Item in the compilers namespace tree */
    int itemtype;
    int itemno;
    struct PLpgSQL_nsitem* prev;
    char* pkgname;
    char* schemaName;
    char name[FLEXIBLE_ARRAY_MEMBER]; /* actually, as long as needed */
} PLpgSQL_nsitem;

typedef struct { /* Generic execution node		*/
    int cmd_type;
    int lineno;
} PLpgSQL_stmt;

typedef struct PLpgSQL_stmt_null {
    int cmd_type;
    int lineno;
    char* sqlString;
} PLpgSQL_stmt_null;

extern THR_LOCAL List* goto_labels;

typedef struct {
    char* label;
    PLpgSQL_stmt* stmt;
} PLpgSQL_gotoLabel;

typedef struct PLpgSQL_condition { /* One EXCEPTION condition name */
    int sqlerrstate;               /* SQLSTATE code */
    char* condname;                /* condition name (for debugging) */
    struct PLpgSQL_condition* next;
} PLpgSQL_condition;

typedef struct {
    List* exc_list; /* List of WHEN clauses */
} PLpgSQL_exception_block;

typedef struct { /* One EXCEPTION ... WHEN clause */
    int lineno;
    PLpgSQL_condition* conditions;
    List* action; /* List of statements */
} PLpgSQL_exception;

typedef struct PLpgSQL_stmt_block { /* Block of statements			*/
    int cmd_type;
    int lineno;
    char* label;
    bool isAutonomous;
    List* body; /* List of statements */
    int n_initvars;
    int* initvarnos;
    PLpgSQL_exception_block* exceptions;
    char* sqlString;
} PLpgSQL_stmt_block;

typedef struct { /* Assign statement			*/
    int cmd_type;
    int lineno;
    int varno;
    PLpgSQL_expr* expr;
    char* sqlString;
} PLpgSQL_stmt_assign;

typedef struct { /* PERFORM statement		*/
    int cmd_type;
    int lineno;
    PLpgSQL_expr* expr;
    char* sqlString;
} PLpgSQL_stmt_perform;

/*
 * COMMIT statement
 */
typedef struct {
    int cmd_type;
    int lineno;
    char* sqlString;
} PLpgSQL_stmt_commit;

/*
 * ROLLBACK statement
 */
typedef struct {
    int cmd_type;
    int lineno;
    char* sqlString;
} PLpgSQL_stmt_rollback;

typedef struct { /* Get Diagnostics item		*/
    int kind;    /* id for diagnostic value desired */
    int target;  /* where to assign it */
} PLpgSQL_diag_item;

typedef struct { /* Get Diagnostics statement		*/
    int cmd_type;
    int lineno;
    bool is_stacked;  /* STACKED or CURRENT diagnostics area? */
    List* diag_items; /* List of PLpgSQL_diag_item */
    char* sqlString;
} PLpgSQL_stmt_getdiag;

typedef struct { /* IF statement				*/
    int cmd_type;
    int lineno;
    PLpgSQL_expr* cond; /* boolean expression for THEN */
    List* then_body;    /* List of statements */
    List* elsif_list;   /* List of PLpgSQL_if_elsif structs */
    List* else_body;    /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_if;

typedef struct { /* GOTO statement				*/
    int cmd_type;
    int lineno;
    char* label;
    char* sqlString;
} PLpgSQL_stmt_goto;

typedef struct /* one ELSIF arm of IF statement */
{
    int lineno;
    PLpgSQL_expr* cond; /* boolean expression for this case */
    List* stmts;        /* List of statements */
} PLpgSQL_if_elsif;

typedef struct /* CASE statement */
{
    int cmd_type;
    int lineno;
    PLpgSQL_expr* t_expr; /* test expression, or NULL if none */
    int t_varno;          /* var to store test expression value into */
    List* case_when_list; /* List of PLpgSQL_case_when structs */
    bool have_else;       /* flag needed because list could be empty */
    List* else_stmts;     /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_case;

typedef struct /* one arm of CASE statement */
{
    int lineno;
    PLpgSQL_expr* expr; /* boolean expression for this case */
    List* stmts;        /* List of statements */
    char* sqlString;
} PLpgSQL_case_when;

typedef struct { /* Unconditional LOOP statement		*/
    int cmd_type;
    int lineno;
    char* label;
    List* body; /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_loop;

typedef struct { /* WHILE cond LOOP statement		*/
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_expr* cond;
    List* body; /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_while;

typedef struct { /* FOR statement with integer loopvar	*/
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_var* var;
    PLpgSQL_expr* lower;
    PLpgSQL_expr* upper;
    PLpgSQL_expr* step; /* NULL means default (ie, BY 1) */
    int reverse;
    List* body; /* List of statements */
    char* sqlString;
    bool save_exceptions;
} PLpgSQL_stmt_fori;

/*
 * PLpgSQL_stmt_forq represents a FOR statement running over a SQL query.
 * It is the common supertype of PLpgSQL_stmt_fors, PLpgSQL_stmt_forc
 * and PLpgSQL_dynfors.
 */
typedef struct {
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_rec* rec;
    PLpgSQL_row* row;
    List* body; /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_forq;

typedef struct { /* FOR statement running over SELECT	*/
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_rec* rec;
    PLpgSQL_row* row;
    List* body; /* List of statements */
    char* sqlString;
    /* end of fields that must match PLpgSQL_stmt_forq */
    PLpgSQL_expr* query;
    bool addNamespace;
} PLpgSQL_stmt_fors;

typedef struct { /* FOR statement running over cursor	*/
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_rec* rec;
    PLpgSQL_row* row;
    List* body; /* List of statements */
    char* sqlString;
    /* end of fields that must match PLpgSQL_stmt_forq */
    int curvar;
    PLpgSQL_expr* argquery; /* cursor arguments if any */
} PLpgSQL_stmt_forc;

typedef struct { /* FOR statement running over EXECUTE	*/
    int cmd_type;
    int lineno;
    char* label;
    PLpgSQL_rec* rec;
    PLpgSQL_row* row;
    List* body; /* List of statements */
    char* sqlString;
    /* end of fields that must match PLpgSQL_stmt_forq */
    PLpgSQL_expr* query;
    List* params; /* USING expressions */
} PLpgSQL_stmt_dynfors;

typedef struct { /* FOREACH item in array loop */
    int cmd_type;
    int lineno;
    char* label;
    int varno;          /* loop target variable */
    int slice;          /* slice dimension, or 0 */
    PLpgSQL_expr* expr; /* array expression */
    List* body;         /* List of statements */
    char* sqlString;
} PLpgSQL_stmt_foreach_a;

typedef struct { /* OPEN a curvar					*/
    int cmd_type;
    int lineno;
    int curvar;
    int cursor_options;
    PLpgSQL_row* returntype;
    PLpgSQL_expr* argquery;
    PLpgSQL_expr* query;
    PLpgSQL_expr* dynquery;
    List* params; /* USING expressions */
    char* sqlString;
} PLpgSQL_stmt_open;

typedef struct { /* FETCH or MOVE statement */
    int cmd_type;
    int lineno;
    PLpgSQL_rec* rec; /* target, as record or row */
    PLpgSQL_row* row;
    int curvar;                 /* cursor variable to fetch from */
    FetchDirection direction;   /* fetch direction */
    long how_many;              /* count, if constant (expr is NULL) */
    PLpgSQL_expr* expr;         /* count, if expression */
    bool is_move;               /* is this a fetch or move? */
    bool bulk_collect;          /* is bulk collect? */
    bool has_direction;         /* has direction clause */
    bool returns_multiple_rows; /* can return more than one row? */
    char* sqlString;
} PLpgSQL_stmt_fetch;

typedef struct { /* CLOSE curvar						*/
    int cmd_type;
    int lineno;
    int curvar;
    char* sqlString;
} PLpgSQL_stmt_close;

typedef struct { /* EXIT or CONTINUE statement			*/
    int cmd_type;
    int lineno;
    bool is_exit; /* Is this an exit or a continue? */
    char* label;  /* NULL if it's an unlabelled EXIT/CONTINUE */
    PLpgSQL_expr* cond;
    char* sqlString;
} PLpgSQL_stmt_exit;

typedef struct { /* RETURN statement			*/
    int cmd_type;
    int lineno;
    PLpgSQL_expr* expr;
    int retvarno;
    char* sqlString;
} PLpgSQL_stmt_return;

typedef struct { /* RETURN NEXT statement */
    int cmd_type;
    int lineno;
    PLpgSQL_expr* expr;
    int retvarno;
    char* sqlString;
} PLpgSQL_stmt_return_next;

typedef struct { /* RETURN QUERY statement */
    int cmd_type;
    int lineno;
    PLpgSQL_expr* query;    /* if static query */
    PLpgSQL_expr* dynquery; /* if dynamic query (RETURN QUERY EXECUTE) */
    List* params;           /* USING arguments for dynamic query */
    char* sqlString;
} PLpgSQL_stmt_return_query;

typedef struct { /* RAISE statement			*/
    int cmd_type;
    int lineno;
    int elog_level;
    char* condname; /* condition name, SQLSTATE, or NULL */
    char* message;  /* old-style message format literal, or NULL */
    List* params;   /* list of expressions for old-style message */
    List* options;  /* list of PLpgSQL_raise_option */
    char* sqlString;
    bool hasExceptionInit;
} PLpgSQL_stmt_raise;

typedef struct { /* RAISE statement option */
    int opt_type;
    PLpgSQL_expr* expr;
} PLpgSQL_raise_option;

typedef struct { /* Generic SQL statement to execute */
    int cmd_type;
    int lineno;
    PLpgSQL_expr* sqlstmt;
    bool mod_stmt; /* is the stmt INSERT/UPDATE/DELETE? */
    /* note: mod_stmt is set when we plan the query */
    bool into;        /* INTO supplied? */
    bool bulk_collect;  /* BULK COLLECT? */
    bool strict;      /* INTO STRICT flag */
    PLpgSQL_rec* rec; /* INTO target, if record */
    PLpgSQL_row* row; /* INTO target, if row */
    // A db function invoke feature
    int placeholders;
    bool multi_func;
    char* sqlString;
} PLpgSQL_stmt_execsql;

// Added USING IN/OUT/IN OUT for plpgsql
typedef struct { /* Dynamic SQL string to execute */
    int cmd_type;
    int lineno;
    PLpgSQL_expr* query; /* string expression */
    bool into;           /* INTO supplied? */
    bool strict;         /* INTO STRICT flag */
    PLpgSQL_rec* rec;    /* INTO target, if record */
    union {
        PLpgSQL_row* row;     /* INTO target, if row */
        PLpgSQL_row* out_row; /* USING output */
    };
    List* params;          /* USING expressions */
    bool isinouttype;      /* IN OUT parameters, differ from INTO statment */
    bool isanonymousblock; /* check if it is anonymous block */
    void* ppd;             /* IN or IN OUT parameters */
    char* sqlString;
} PLpgSQL_stmt_dynexecute;

/* ----------
 * SAVEPOINT operate types
 * ----------
 */
enum {
    PLPGSQL_SAVEPOINT_CREATE,
    PLPGSQL_SAVEPOINT_ROLLBACKTO,
    PLPGSQL_SAVEPOINT_RELEASE
};

/*
 * SAVEPOINT statement
 */
typedef struct {
    int cmd_type;
    int lineno;
    int opType;         /* create or rollback to or release */
    char* spName;       /* savepoint's identifier */
    char* sqlString;
} PLpgSQL_stmt_savepoint;

typedef struct PLpgSQL_func_hashkey { /* Hash lookup key for functions */
    Oid funcOid;

    bool isTrigger; /* true if called as a trigger */

    /* be careful that pad bytes in this struct get zeroed! */

    /*
     * For a trigger function, the OID of the relation triggered on is part of
     * the hash key --- we want to compile the trigger separately for each
     * relation it is used with, in case the rowtype is different.	Zero if
     * not called as a trigger.
     */
    Oid trigrelOid;

    /*
     * We must include the input collation as part of the hash key too,
     * because we have to generate different plans (with different Param
     * collations) for different collation settings.
     */
    Oid inputCollation;

    /*
     * We include actual argument types in the hash key to support polymorphic
     * PLpgSQL functions.  Be careful that extra positions are zeroed!
     */
    Oid argtypes[FUNC_MAX_ARGS];

    Oid packageOid; /* package oid if is in a package */
} PLpgSQL_func_hashkey;

typedef struct PLpgSQL_error { /* Element of table variable */
    int line;
    char* errmsg;
} PLpgSQL_error;

typedef struct FuncInvalItem {
    NodeTag type;
    int cacheId;      /* a syscache ID, see utils/syscache.h */
    Oid objId;
} FuncInvalItem;

typedef struct PLpgSQL_function { /* Complete compiled function	  */
    char* fn_signature;
    Oid fn_oid;
    Oid pkg_oid;
    OverrideSearchPath* fn_searchpath;
    Oid fn_owner;
    TransactionId fn_xmin;
    ItemPointerData fn_tid;
    bool is_private;
    bool fn_is_trigger;
    Oid fn_input_collation;
    PLpgSQL_func_hashkey* fn_hashkey; /* back-link to hashtable key */
    MemoryContext fn_cxt;

    Oid fn_rettype;
    int fn_rettyplen;
    bool fn_retbyval;
    FmgrInfo fn_retinput;
    Oid fn_rettypioparam;
    bool fn_retistuple;
    bool fn_retset;
    bool fn_readonly;

    int fn_nargs;
    int fn_argvarnos[FUNC_MAX_ARGS];
    int out_param_varno;
    int found_varno;

    // magic variables' varno for implicit cursor attributes
    int sql_cursor_found_varno;
    int sql_notfound_varno;
    int sql_isopen_varno;
    int sql_rowcount_varno;
    int sql_bulk_exceptions_varno;

    // the magic sqlcode no
    int sqlcode_varno;
    int sqlstate_varno;
    int sqlerrm_varno;
    int new_varno;
    int old_varno;
    int tg_name_varno;
    int tg_when_varno;
    int tg_level_varno;
    int tg_op_varno;
    int tg_relid_varno;
    int tg_relname_varno;
    int tg_table_name_varno;
    int tg_table_schema_varno;
    int tg_nargs_varno;
    int tg_argv_varno;

    List* invalItems; /* other dependencies, like other pkg's type or variable */

    PLpgSQL_resolve_option resolve_option;

    int ndatums;
    PLpgSQL_datum** datums;
    bool* datum_need_free; /* need free datum when free function memory? */
    PLpgSQL_stmt_block* action;
    List* goto_labels;

    /* these fields change when the function is used */
    struct PLpgSQL_execstate* cur_estate;
    unsigned long use_count;

    /* these fields are used during trigger pre-parsing */
    bool pre_parse_trig;
    Relation tg_relation;

    /* pl debugger ptr */
    struct DebugInfo* debug;
    struct PLpgSQL_nsitem* ns_top;

    bool is_autonomous;
    bool is_plpgsql_func_with_outparam;
    bool is_insert_gs_source;
} PLpgSQL_function;

class AutonomousSession;
typedef struct PLpgSQL_execstate { /* Runtime execution data	*/
    PLpgSQL_function* func;        /* function being executed */

    Datum retval;
    bool retisnull;
    Oid rettype; /* type of current retval */
    Datum paramval;
    bool paramisnull;
    Oid paramtype;

    Oid fn_rettype; /* info about declared function rettype */
    bool retistuple;
    bool retisset;

    bool readonly_func;

    TupleDesc rettupdesc;
    TupleDesc paramtupdesc;
    char* exitlabel;      /* the "target" label of the current EXIT or
                           * CONTINUE stmt, if any */
    ErrorData* cur_error; /* current exception handler's error */

    Tuplestorestate* tuple_store; /* SRFs accumulate results here */
    MemoryContext tuple_store_cxt;
    ResourceOwner tuple_store_owner;
    ReturnSetInfo* rsi;

    int found_varno;

    /*
     * The execute state variable estate->rowcount keeps
     * the current rowcount while executing store procedure
     */
    int32 rowcount;

    /* magic variables' varno for implicit cursor attributes */
    int sql_cursor_found_varno;
    int sql_notfound_varno;
    int sql_isopen_varno;
    int sql_rowcount_varno;
    int sqlcode_varno;
    int sqlstate_varno;
    int sqlerrm_varno;
    int sql_bulk_exceptions_varno;
    int ndatums;
    int datums_alloc;
    PLpgSQL_datum** datums;

    /* EState and resowner to use for "simple" expression evaluation */
    EState *simple_eval_estate;
    ResourceOwner simple_eval_resowner;

    ParamListInfo paramLI;

    /* temporary state for results from evaluation of query or expr */
    SPITupleTable* eval_tuptable;
    uint32 eval_processed;
    Oid eval_lastoid;
    ExprContext* eval_econtext; /* for executing simple expressions */
    PLpgSQL_expr* cur_expr;     /* current query/expr being evaluated */

    /* status information for error context reporting */
    PLpgSQL_stmt* err_stmt; /* current stmt */
    const char* err_text;   /* additional state info */

    void* plugin_info; /* reserved for use by optional plugin */

    /* support GOTO */
    List* goto_labels;
    char* goto_target_label;
    PLpgSQL_stmt* goto_target_stmt; /* current GOTO */
    int block_level;                /* block level, 0 for topmost */
    Cursor_Data* cursor_return_data;
    int cursor_return_numbers;

    int64 stack_entry_start;    /* ExprContext's starting number for eval simple expression */
    Oid curr_nested_table_type;
    bool is_exception;
} PLpgSQL_execstate;

typedef struct PLpgSQL_pkg_execstate { /* Runtime execution data	*/
    Oid pkgoid;
    char pkgkind;
    MemoryContext pkgcontext;
    int ndatums;
    int n_initvars;
    int* initvarnos;
    PLpgSQL_datum** datums;
    bool is_bodycompiled;
    struct PLpgSQL_nsitem* public_ns;
    /* namespace chain visible to this package */
    struct PLpgSQL_nsitem* private_ns;
} PLpgSQL_pkg_execstate;

typedef struct PLpgSQL_func_tableof_index {
    int varno;
    Oid tableOfIndexType;
    HTAB* tableOfIndex;
} PLpgSQL_func_tableof_index;

/*
 * A PLpgSQL_plugin structure represents an instrumentation plugin.
 * To instrument PL/pgSQL, a plugin library must access the rendezvous
 * variable "PLpgSQL_plugin" and set it to point to a PLpgSQL_plugin struct.
 * Typically the struct could just be static data in the plugin library.
 * We expect that a plugin would do this at library load time (_PG_init()).
 * It must also be careful to set the rendezvous variable back to NULL
 * if it is unloaded (_PG_fini()).
 *
 * This structure is basically a collection of function pointers --- at
 * various interesting points in pl_exec.c, we call these functions
 * (if the pointers are non-NULL) to give the plugin a chance to watch
 * what we are doing.
 *
 *	func_setup is called when we start a function, before we've initialized
 *	the local variables defined by the function.
 *
 *	func_beg is called when we start a function, after we've initialized
 *	the local variables.
 *
 *	func_end is called at the end of a function.
 *
 *	stmt_beg and stmt_end are called before and after (respectively) each
 *	statement.
 *
 * Also, immediately before any call to func_setup, PL/pgSQL fills in the
 * error_callback and assign_expr fields with pointers to its own
 * plpgsql_exec_error_callback and exec_assign_expr functions.	This is
 * a somewhat ad-hoc expedient to simplify life for debugger plugins.
 */

typedef struct PLpgSQL_plugin {
    /* Function pointers set up by the plugin */
    void (*func_setup)(PLpgSQL_execstate* estate, PLpgSQL_function* func);
    void (*func_beg)(PLpgSQL_execstate* estate, PLpgSQL_function* func);
    void (*func_end)(PLpgSQL_execstate* estate, PLpgSQL_function* func);
    void (*stmt_beg)(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt);
    void (*stmt_end)(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt);

    /* Function pointers set by PL/pgSQL itself */
    void (*error_callback)(void* arg);
    void (*assign_expr)(PLpgSQL_execstate* estate, PLpgSQL_datum* target, PLpgSQL_expr* expr);
    Datum (*eval_expr)(PLpgSQL_execstate* estate, PLpgSQL_expr* expr, bool* isNull, Oid* rettype, HTAB** tableOfIndex);
    void (*assign_value)(PLpgSQL_execstate* estate, PLpgSQL_datum* target, Datum value,
        Oid valtype, bool* isNull, HTAB* tableOfIndex);
    void (*eval_cleanup)(PLpgSQL_execstate* estate);
    int (*validate_line)(PLpgSQL_stmt_block* block, int linenum);
} PLpgSQL_plugin;

/* Struct types used during parsing */

typedef struct {
    char* ident; /* palloc'd converted identifier */
    bool quoted; /* Was it double-quoted? */
} PLword;

typedef struct {
    List* idents; /* composite identifiers (list of String) */
} PLcword;

typedef struct {
    PLpgSQL_datum* datum; /* referenced variable */
    char* ident;          /* valid if simple name */
    bool quoted;
    List* idents; /* valid if composite name */
    int dno;
} PLwdatum;


typedef struct PLpgSQL_pkg_hashkey { /* Hash lookup key for functions */
    Oid pkgOid;
} PLpgSQL_pkg_hashkey;

typedef struct plpgsql_pkg_hashent {

    PLpgSQL_pkg_hashkey key;

    PLpgSQL_package* package;

    DListCell* cell; /* Dlist cell for delete package compile results. */
    
} plpgsql_pkg_HashEnt;

typedef struct PLpgSQL_package { /* Complete compiled package   */
    char* pkg_signature;
    Oid pkg_oid;
    OverrideSearchPath* pkg_searchpath;
    Oid pkg_owner;
    TransactionId pkg_xmin;
    ItemPointerData pkg_tid;
    MemoryContext pkg_cxt;
    PLpgSQL_pkg_hashkey* pkg_hashkey; /* back-link to hashtable key */
    bool is_spec_compiling;
    bool is_bodycompiled;

    /* namespace chain visible to this package */
    struct PLpgSQL_nsitem* public_ns;

    /*proc list in package*/
    List* proc_list;
    /* namespace chain visible to this package */
    struct PLpgSQL_nsitem* private_ns;

    /*compiled list in package*/
    List* proc_compiled_list;

    // magic variables' varno for implicit cursor attributes
    int sql_cursor_found_varno;
    int sql_notfound_varno;
    int sql_isopen_varno;
    int sql_rowcount_varno;
    PLpgSQL_resolve_option resolve_option;
    int ndatums;
    int public_ndatums;
    int datums_alloc;
    PLpgSQL_datum** datums;
    bool* datum_need_free; /* need free datum when free package memory? */
    int n_initvars;
    int* initvarnos;

    List* invalItems; /* other dependencies, like other pkg's type or variable */
    unsigned long use_count; /* count for other func use */

    Cursor_Data* cursor_return_data;
    char* plpgsql_error_funcname;
    knl_u_plpgsql_pkg_context* u_pkg;
    Oid namespaceOid;
    bool isInit;
} PLpgSQL_package;

typedef struct ExecTableOfIndexInfo {
    ExprContext* econtext;
    HTAB* tableOfIndex;
    Oid tableOfIndexType;
    bool isnestedtable;
    int tableOfLayers;
    int paramid;
    Oid paramtype;
} ExecTableOfIndexInfo;

/**********************************************************************
 * Pl debugger
 **********************************************************************/
typedef void (*PlpgsqlDebugFunc)(PLpgSQL_function* func, PLpgSQL_execstate* estate);
typedef void (*PlpgsqlStartUpFunc)(PLpgSQL_function* func);

typedef struct CodeLine {
    int lineno;
    char* code;
    bool canBreak;
} CodeLine;

typedef struct PLDebug_variable {
    NodeTag type;
    char* name;
    char* var_type;
    char* value;
    char* pkgname;
    bool isconst;
} PLDebug_variable;

typedef struct PLDebug_frame {
    NodeTag type;
    int frameno;
    char* funcname;
    int lineno;
    char* query;
    int funcoid;
} PLDebug_frame;

typedef struct DebugInfoComm {
    int comm_idx;
    /* buffer */
    char* send_buffer;
    char* rec_buffer;
    int send_ptr; /* send msg len */
    int rec_ptr; /* received msg len */
    int send_buf_len; /* send_buffer's size */
    int rec_buf_len; /* rec_buffer's size */
} DebugInfoSocket;

typedef struct DebugInfo {
    int lineno;
    bool stop_next_stmt;
    char cur_opt;
    int debugStackIdx;
    PlpgsqlDebugFunc debugCallback;
    PlpgsqlStartUpFunc startUp;
    PLpgSQL_function* func;
    MemoryContext debug_cxt;
    DebugInfo* inner_called_debugger;
    /* debug socket */
    DebugInfoComm* comm;
    /* break point */
    List* bp_list;
    /* current stmt */
    PLpgSQL_stmt* cur_stmt;
    PLpgSQL_execstate* estate;
} DebugInfo;

typedef struct PLDebug_breakPoint {
    NodeTag type;
    int bpIndex;
    Oid funcoid;
    int lineno;
    bool active;
    bool deleted;
    char* query;
} PLDebug_breakPoint;

typedef struct DebugClientInfo {
    int comm_idx;
    MemoryContext context;
    /* buffer */
    char* send_buffer;
    char* rec_buffer;
    int send_ptr;
    int rec_ptr;
    int send_buf_len;
    int rec_buf_len;
} DebugClientInfo;

typedef struct PlDebugEntry {
    Oid key;
    int commIdx;
    PLpgSQL_function* func;
} PlDebugEntry;

typedef List* (*RawParserHook)(const char*, List**);

const int MAXINT8LEN = 25;
const int DEFAULT_DEBUG_BUF_SIZE = 1024;
const long NANOSECOND_PER_SECOND = 1000000000L; /* 1s */
const int MAX_INT_SIZE = 12;

extern const char* DEFAULT_UNKNOWN_VALUE;

/* after header use upper case letter */
const char DEBUG_NOTHING_HEADER = 'e';
const char DEBUG_ATTACH_HEADER = 'p';
const char DEBUG_LOCALS_HEADER = 'v';
const char DEBUG_NEXT_HEADER = 'n';
const char DEBUG_NEXT_HEADER_AFTER = 'N';
const char DEBUG_ABORT_HEADER = 'a';
const char DEBUG_ABORT_HEADER_AFTER = 'A';
const char DEBUG_CONTINUE_HEADER = 'c';
const char DEBUG_CONTINUE_HEADER_AFTER = 'C';
const char DEBUG_ADDBREAKPOINT_HEADER = 'b';
const char DEBUG_DELETEBREAKPOINT_HEADER = 'r';
const char DEBUG_ENABLEBREAKPOINT_HEADER = 'e';
const char DEBUG_DISABLEBREAKPOINT_HEADER = 'g';
const char DEBUG_FINISH_HEADER = 'f';
const char DEBUG_FINISH_HEADER_AFTER = 'F';
const char DEBUG_BREAKPOINT_HEADER = 'q';
const char DEBUG_STEP_INTO_HEADER = 's';
const char DEBUG_STEP_INTO_HEADER_AFTER = 'S';
const char DEBUG_BACKTRACE_HEADER = 't';
const char DEBUG_SET_VARIABLE_HEADER = 'h';

/* server return message */
const int DEBUG_SERVER_SUCCESS = 0;

enum {
    DEBUG_SERVER_SET_NO_VAR = 1,
    DEBUG_SERVER_SET_EXEC_FAILURE,
    DEBUG_SERVER_SET_CURSOR,
    DEBUG_SERVER_SET_CONST
};

const int DEBUG_SERVER_PRINT_VAR_FRAMENO_EXCEED = 1;


#define PLDEBUG_FEATURE_NOT_SUPPORT_IN_DISTRIBUTED()                                   \
    do {                                                                               \
        ereport(ERROR,                                                                 \
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                                   \
                errmsg("Un-support feature"),                                          \
                errdetail("Pldebug is not supported for distribute currently.")));     \
    } while (0)

#define PLDEBUG_FUNCTION_NOT_SUPPORT(funcname)                                  \
    do {                                                                                      \
        ereport(ERROR,                                                                        \
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                                          \
                errmsg("Un-support feature"),                                                 \
                errdetail("func %s is not supported currently.", funcname)));  \
    } while (0)

#define CHECK_DEBUG_COMM_VALID(idx)                                  \
    do {                                                                                      \
        if (!g_instance.pldebug_cxt.debug_comm[idx].Used())                                   \
            ereport(ERROR,                                                                    \
                    (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_PLDEBUGGER_ERROR),            \
                     errmsg("Debug Comm %d has been released or not turn on yet.", idx)));                        \
    } while (0)

static const char *stringFromCompileStatus(int strindex)
{
    static const char *strings[] = {"COMPILIE_FUNC",
                                    "COMPILIE_PKG",
                                    "COMPILIE_PKG_FUNC",
                                    "COMPILIE_PKG_ANON_BLOCK",
                                    "COMPILIE_ANON_BLOCK",
                                    "NONE_STATUS"};

    return strings[strindex];
}

inline void spi_nest_compile_log(MemoryContext cxt, const char* filename, int lineno, const char* funcname)
{
    if (cxt && u_sess->attr.attr_common.log_min_messages <= DEBUG3 && module_logging_is_on(MOD_NEST_COMPILE)) {
        ereport(DEBUG3, (errmodule(MOD_NEST_COMPILE), errcode(ERRCODE_LOG),
            errmsg("compile cxt(Location %s,%d, Funcname:%s)curr_compile_context: %p, init compile context status: %s,"\
                   "new compile_cxt: %p, context name: %s, context parent name: %s, level: %d",
                filename, lineno, funcname, u_sess->plsql_cxt.curr_compile_context,
                stringFromCompileStatus(u_sess->plsql_cxt.compile_status),
                cxt, cxt->name, cxt->parent != NULL ? cxt->parent->name : NULL,
                list_length(u_sess->plsql_cxt.compile_context_list) + 1)));
    }
}

#define SPI_NESTCOMPILE_LOG(cxt) \
    (spi_nest_compile_log(cxt, __FILE__, __LINE__, __func__))


extern Datum debug_server_turn_on(PG_FUNCTION_ARGS);
extern Datum debug_server_turn_off(PG_FUNCTION_ARGS);

extern Datum debug_client_attatch(PG_FUNCTION_ARGS);
extern Datum debug_client_abort(PG_FUNCTION_ARGS);
extern Datum debug_client_next(PG_FUNCTION_ARGS);
extern Datum debug_client_local_variables(PG_FUNCTION_ARGS);
extern Datum debug_client_continue(PG_FUNCTION_ARGS);
extern Datum debug_client_finish(PG_FUNCTION_ARGS);

/* Reserved interface */
extern Datum debug_client_add_breakpoint(PG_FUNCTION_ARGS);
extern Datum debug_client_delete_breakpoint(PG_FUNCTION_ARGS);
extern Datum debug_client_enable_breakpoint(PG_FUNCTION_ARGS);
extern Datum debug_client_disable_breakpoint(PG_FUNCTION_ARGS);
extern Datum debug_client_info_breakpoints(PG_FUNCTION_ARGS);
extern Datum debug_client_backtrace(PG_FUNCTION_ARGS);
extern Datum debug_client_info_code(PG_FUNCTION_ARGS);
extern Datum debug_client_info_step(PG_FUNCTION_ARGS);
extern Datum local_debug_server_info(PG_FUNCTION_ARGS);

void check_debug(PLpgSQL_function* func, PLpgSQL_execstate* estate);
void server_pass_upper_debug_opt(DebugInfo* debug);
void clean_up_debug_client(bool hasError = false);
void clean_up_debug_server(DebugInfo* debug, bool sessClose, bool hasError);
void server_send_end_msg(DebugInfo* debug);
int GetValidDebugCommIdx();
void WaitSendMsg(int commIdx, bool isClient, char** destBuffer, int* destLen);
bool WakeUpReceiver(int commIdx, bool isClient);
extern void PlDebugerCleanUp(int code, Datum arg);
extern void ReportInvalidMsg(const char* buf);
extern char* AssignStr(char* src, bool copy = true);
extern PlDebugEntry* has_debug_func(Oid key, bool* found);
extern bool delete_debug_func(Oid key);
extern void RecvUnixMsg(const char* buf, int bufLen, char* destBuf, int destLen);
extern char* ResizeDebugBufferIfNecessary(char* buffer, int* oldSize, int needSize);
extern void ReleaseDebugCommIdx(int idx);
extern void SendUnixMsg(int socket, const char* val, int len, bool is_client);

/**********************************************************************
 * Function declarations
 **********************************************************************/

/* ----------
 * Functions in pl_comp.c
 * ----------
 */

typedef struct plpgsql_hashent {
    PLpgSQL_func_hashkey key;
    PLpgSQL_function* function;
    DListCell* cell; /* Dlist cell for delete function compile results. */
} plpgsql_HashEnt;

extern PLpgSQL_function* plpgsql_compile(FunctionCallInfo fcinfo, bool forValidator);
extern void delete_function(PLpgSQL_function* func, bool fromPackage = false);
extern PLpgSQL_function* plpgsql_compile_nohashkey(FunctionCallInfo fcinfo); /* parse trigger func */
extern PLpgSQL_function* plpgsql_compile_inline(char* proc_source);
extern void plpgsql_parser_setup(struct ParseState* pstate, PLpgSQL_expr* expr);
extern void plpgsql_parser_setup_bind(struct ParseState* pstate, List** expr);
extern void plpgsql_parser_setup_describe(struct ParseState* pstate, List** expr);

extern bool plpgsql_parse_word(char* word1, const char* yytxt, PLwdatum* wdatum, PLword* word, int* tok_flag);
extern bool plpgsql_parse_dblword(char* word1, char* word2, PLwdatum* wdatum, PLcword* cword, int* nsflag);
extern bool plpgsql_parse_tripword(char* word1, char* word2, char* word3, PLwdatum* wdatum,
    PLcword* cword, int* tok_flag);
extern bool plpgsql_parse_quadword(char* word1, char* word2, char* word3, char* word4, PLwdatum* wdatum,
    PLcword* cword, int* tok_flag);
extern PLpgSQL_type* plpgsql_parse_wordtype(char* ident);
extern PLpgSQL_type* plpgsql_parse_cwordtype(List* idents);
extern PLpgSQL_type* plpgsql_parse_wordrowtype(char* ident);
extern PLpgSQL_type* plpgsql_parse_cwordrowtype(List* idents);
extern PLpgSQL_type* plpgsql_build_datatype(Oid typeOid, int32 typmod, Oid collation);
extern PLpgSQL_type* build_datatype(HeapTuple type_tup, int32 typmod, Oid collation);
extern PLpgSQL_type* plpgsql_build_nested_datatype();
extern const char *plpgsql_code_int2cstring(int sqlcode);
extern const int plpgsql_code_cstring2int(const char *codename);
extern void plpgsql_set_variable(const char* varname, int value);
extern PLpgSQL_variable* plpgsql_build_variable(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace, bool isImplicit = false,
    const char* varname = NULL, knl_pl_body_type plType = PL_BODY_FUNCTION);
PLpgSQL_variable* plpgsql_build_varrayType(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace);
PLpgSQL_variable* plpgsql_build_tableType(const char* refname, int lineno, PLpgSQL_type* dtype, bool add2namespace);
extern PLpgSQL_rec_type* plpgsql_build_rec_type(const char* typname, int lineno, List* list, bool add2namespace);
extern PLpgSQL_rec* plpgsql_build_record(const char* refname, int lineno, bool add2namespace);
extern int plpgsql_recognize_err_condition(const char* condname, bool allow_sqlstate);
extern PLpgSQL_condition* plpgsql_parse_err_condition(char* condname);
extern int plpgsql_adddatum(PLpgSQL_datum* newm, bool isChange = true);
extern int plpgsql_add_initdatums(int** varnos);
extern void plpgsql_HashTableInit(void);
extern PLpgSQL_row* build_row_from_tuple_desc(const char* rowname, int lineno, TupleDesc desc); 
extern PLpgSQL_row* build_row_from_rec_type(const char* rowname, int lineno, PLpgSQL_rec_type* type);
extern bool plpgsql_check_colocate(Query* query, RangeTblEntry* rte, void* plpgsql_func);
extern void plpgsql_HashTableDeleteAll();
extern void plpgsql_hashtable_delete_and_check_invalid_item(int classId, Oid objId);
extern void delete_package_and_check_invalid_item(Oid pkgOid);
extern void plpgsql_HashTableDelete(PLpgSQL_function* func);
extern bool plpgsql_get_current_value_stp_with_exception();
extern void plpgsql_restore_current_value_stp_with_exception(bool saved_current_stp_with_exception);
extern void plpgsql_set_current_value_stp_with_exception();
extern void delete_pkg_in_HashTable(Oid pkgOid);
extern PLpgSQL_package* plpgsql_pkg_compile(Oid pkgOid, bool for_validator, bool isSpec, bool isCreate=false);
extern PLpgSQL_datum* plpgsql_pkg_adddatum(const List* wholeName, char** objname, char** pkgname);
extern int plpgsql_pkg_adddatum2ns(const List* name);
extern bool plpgsql_check_insert_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func);
extern int plpgsql_pkg_add_unknown_var_to_namespace(List* name);
extern int plpgsql_build_pkg_variable(List* name, PLpgSQL_datum* datum, bool isSamePkg = false);
extern void plpgsql_pkg_HashTableDelete(PLpgSQL_package* pkg);
extern PLpgSQL_datum* plpgsql_lookup_datum(
    bool localmode, const char* name1, const char* name2, const char* name3, int* names_used);
extern PLpgSQL_type* plpgsql_get_row_field_type(int dno, const char* fieldname, MemoryContext old_cxt);
extern PLpgSQL_resolve_option GetResolveOption();
extern Node* plpgsql_check_match_var(Node* node, ParseState* pstate, ColumnRef* cref);

/* ----------
 * Functions in pl_handler.c
 * ----------
 */
extern "C" void _PG_init(void);
extern "C" Datum plpgsql_call_handler(PG_FUNCTION_ARGS);
extern "C" Datum plpgsql_inline_handler(PG_FUNCTION_ARGS);
extern "C" Datum plpgsql_validator(PG_FUNCTION_ARGS);
extern "C" PLpgSQL_package* plpgsql_package_validator(Oid packageOid, bool isSpec, bool isCreate=false);
extern void record_pkg_function_dependency(PLpgSQL_package* pkg, List** invalItems, Oid funcid, Oid pkgid);
extern void DecreasePackageUseCount(PLpgSQL_function* func);
extern void AddPackageUseCount(PLpgSQL_function* func);

/* --- --- ---
 * Functions in plsql_packages.c
 * --- ---
 */
extern "C" {
Datum regexp_substr(PG_FUNCTION_ARGS);
Datum intervaltonum(PG_FUNCTION_ARGS);
Datum rawtohex(PG_FUNCTION_ARGS);
Datum report_application_error(PG_FUNCTION_ARGS);
}

extern THR_LOCAL PLpgSQL_execstate* plpgsql_estate;

/* ----------
 * Functions in pl_exec.c
 * ----------
 */
#define BULK_COLLECT_MAX ((Size)0x3FFFFFF)    /* maximum number of rows can be bulk collected (by 3FFFFFFF/16) */

extern Datum plpgsql_exec_function(PLpgSQL_function* func, FunctionCallInfo fcinfo, bool dynexec_anonymous_block);
extern Datum plpgsql_exec_autonm_function(PLpgSQL_function* func, FunctionCallInfo fcinfo, char* source_text);
extern HeapTuple plpgsql_exec_trigger(PLpgSQL_function* func, TriggerData* trigdata);
extern void plpgsql_xact_cb(XactEvent event, void* arg);
extern void plpgsql_subxact_cb(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg);

extern Oid exec_get_datum_type(PLpgSQL_execstate* estate, PLpgSQL_datum* datum);
extern void exec_get_datum_type_info(PLpgSQL_execstate* estate, PLpgSQL_datum* datum, Oid* typid, int32* typmod,
    Oid* collation, Oid* tableOfIndexType, PLpgSQL_function* func = NULL);
extern Datum exec_simple_cast_datum(
    PLpgSQL_execstate* estate, Datum value, Oid valtype, Oid reqtype, int32 reqtypmod, bool isnull);
extern void ResetCursorOption(Portal portal, bool reset);
#ifndef ENABLE_MULTIPLE_NODES
extern void ResetCursorAtrribute(Portal portal);
#endif
extern void exec_assign_value(PLpgSQL_execstate *estate,
				  PLpgSQL_datum *target,
				  Datum value, Oid valtype, bool *isNull, HTAB* tableOfIndex = NULL);
extern void exec_eval_datum(PLpgSQL_execstate *estate,
				PLpgSQL_datum *datum,
				Oid *typeId,
				int32 *typetypmod,
				Datum *value,
				bool *isnull,
				bool isretry);
extern void exec_eval_cleanup(PLpgSQL_execstate *estate);
extern void free_expr(PLpgSQL_expr* expr);
extern void free_assignlist(List* assignlist);
extern HeapTuple make_tuple_from_row(PLpgSQL_execstate* estate, PLpgSQL_row* row, TupleDesc tupdesc);
void exec_assign_expr(PLpgSQL_execstate* estate, PLpgSQL_datum* target, PLpgSQL_expr* expr);
extern int getTableOfIndexByDatumValue(TableOfIndexKey key, HTAB* tableOfIndex, PLpgSQL_var** node = NULL);
extern Datum fillNestedTableArray(ArrayType* arrayval, Oid parenttypoid, Oid elemtypoid, int value, int idx);
extern int plpgsql_estate_adddatum(PLpgSQL_execstate* estate, PLpgSQL_datum* newm);
extern void CheckCurrCompileDependOnPackage(Oid pkgOid);
#ifndef ENABLE_MULTIPLE_NODES
extern void estate_cursor_set(FormatCallStack* plcallstack);
#endif

/* ----------
 * Functions for namespace handling in pl_funcs.c
 * ----------
 */
extern void plpgsql_ns_init(void);
extern void plpgsql_ns_push(const char* label);
extern void add_pkg_compile();
extern void plpgsql_add_pkg_ns(PLpgSQL_package* pkg);
extern void plpgsql_add_pkg_public_ns(PLpgSQL_package* pkg);
extern void plpgsql_ns_pop(void);
extern PLpgSQL_nsitem* plpgsql_ns_top(void);
extern void plpgsql_ns_additem(
    int itemtype, int itemno, const char* name, const char* pkgname = NULL, const char* schemaName = NULL);
extern PLpgSQL_nsitem* plpgsql_ns_lookup(
    PLpgSQL_nsitem* ns_cur, bool localmode, const char* name1, const char* name2, const char* name3, int* names_used);
extern PLpgSQL_nsitem* plpgsql_ns_lookup_label(PLpgSQL_nsitem* ns_cur, const char* name);

/* ----------
 * Other functions in pl_funcs.c
 * ----------
 */
extern const char* plpgsql_stmt_typename(PLpgSQL_stmt* stmt);
extern const char* plpgsql_getdiag_kindname(int kind);
extern void plpgsql_free_function_memory(PLpgSQL_function* func, bool fromPackage = false);
extern void plpgsql_free_package_memory(PLpgSQL_package* pkg);
extern void plpgsql_dumptree(PLpgSQL_function* func);
extern bool plpgsql_is_trigger_shippable(PLpgSQL_function* func);

/* ----------
 * Other functions in ruleutils.cpp
 * ----------
 */
extern char* pg_get_functiondef_worker(Oid funcid, int* headerlines);

/* ----------
 * Scanner functions in pl_scanner.c
 * ----------
 */
extern int plpgsql_base_yylex(void);
extern int plpgsql_yylex(void);
extern void plpgsql_push_back_token(int token);
extern void plpgsql_append_source_text(StringInfo buf, int startlocation, int endlocation);
extern void plpgsql_peek2(int* tok1_p, int* tok2_p, int* tok1_loc, int* tok2_loc);
extern int plpgsql_scanner_errposition(int location);
extern void plpgsql_yyerror(const char* message, bool isError = false);
extern int plpgsql_location_to_lineno(int location);
extern int plpgsql_latest_lineno(void);
extern void plpgsql_scanner_init(const char* str);
extern void plpgsql_scanner_finish(void);
extern char* plpgsql_get_curline_query();
extern void plpgsql_process_stmt_array(StringInfo buf, List* bracket_loc);
extern void plpgsql_append_object_typename(StringInfo buf, PLpgSQL_type *var_type);
extern void CheckSaveExceptionsDML(int errstate);

/* ----------
 * Externs in gram.y
 * ----------
 */
typedef enum {              /* PLpgSQL_ArrayState: array parsing FSM flags(states) */
    NOT_AN_ARRAY,           /* Initial state, not an array */
    ARRAY_START,            /* Start of an array */
    ARRAY_ELEMENT,          /* Array element */
    ARRAY_COERCE,           /* Array element coerce, always set before ARRAY_SEPERATOR */
    ARRAY_SEPERATOR,        /* Array element seperator, i.e. ',' */
    ARRAY_ACCESS            /* Accessing the array, placeholder */
} PLpgSQL_ArrayState;

typedef struct ArrayParseContext{
    List *list_datatype;
    List *list_left_bracket;
    List *list_right_bracket;
    List *list_array_state;
    bool array_is_empty;    /* mark array as empty */
    bool array_is_nested;   /* mark array as nested */
} ArrayParseContext;

typedef struct PackageRuntimeState {
    Oid packageId;
    PLpgSQL_datum** datums;
    int size;
} PackageRuntimeState;

typedef struct AutoSessionFuncValInfo {
    bool found;
    int sql_cursor_found;
    int sql_notfound;
    bool sql_isopen;
    int sql_rowcount;
    int sqlcode;
    bool sqlcode_isnull;
} AutoSessionFuncValInfo;

typedef struct AutoSessionPortalData {
    int outParamIndex;
    PortalStrategy strategy;
    int cursorOptions;
    const char* sourceText;
    const char* commandTag;
    bool atStart;
    bool atEnd;
    bool posOverflow;
    long portalPos;
    Tuplestorestate* holdStore;
    MemoryContext holdContext;
    TupleDesc tupDesc;
    bool is_open;
    bool found;
    bool not_found;
    int row_count;
    bool null_open;
    bool null_fetch;
} AutoSessionPortalData;

typedef enum {             /* PLpgSQL_PortalContextState */
    CONTEXT_NEW,           /* Initial state, new */
    CONTEXT_USED,          /* already used by one portal */
} PLpgSQL_PortalContextState;

typedef struct AutoSessionPortalContextData {
    PLpgSQL_PortalContextState status;
    MemoryContext portalHoldContext;
} AutoSessionPortalContextData;

/* Context for exception block */
typedef struct ExceptionContext {
    MemoryContext oldMemCxt;        /* CurrentMemoryContext saved at exception's entry */
    ResourceOwner oldResOwner;      /* CurrentResourceOwner saved at exception's entry */
    TransactionId oldTransactionId; /* top transaction id saved at exception entry */
    SubTransactionId subXid;        /* exception subtransaction's id */

    int curExceptionCounter;        /* serial number for this exception block */
    bool hasReleased;               /* whehter or not exception subtransaction has released. */

    ErrorData* cur_edata;           /* ErrorData captured by this exception block. */
    ErrorData* old_edata;           /* saved ErrorData before this Exception block. */

    int spi_connected;              /* SPI connected level before exception. */
    int64 stackId;                  /* the start stack Id before entry exception block. */
} ExceptionContext;

/* Quick access array state */
#define IS_ARRAY_STATE(state_list, state) ((state_list && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) ? \
                                          (linitial_int(state_list) == state) : false)
#define SET_ARRAY_STATE(state_list, state) (linitial_int(state_list) = state)

extern int plpgsql_yyparse(void);
extern bool plpgsql_is_token_match2(int token, int token_next);
extern bool plpgsql_is_token_match(int token);
extern void pl_validate_function_sql(PLpgSQL_function* func, bool is_replace);
extern void validate_stmt_dynexecute(PLpgSQL_stmt_dynexecute* stmt, PLpgSQL_function* func,
    SQLFunctionParseInfoPtr pinfo, List** stmt_list);
extern void pl_validate_expression(PLpgSQL_expr* expr, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan);
extern void pl_validate_stmt_block(PLpgSQL_stmt_block *block, PLpgSQL_function* func, SQLFunctionParseInfoPtr pinfo,
    SPIPlanPtr* plan, List** dynexec_list);
extern void pl_validate_stmt_block_in_subtransaction(PLpgSQL_stmt_block* block, PLpgSQL_function* func,
    SQLFunctionParseInfoPtr pinfo, SPIPlanPtr* plan, List** dynexec_list);
extern TupleDesc getCursorTupleDesc(PLpgSQL_expr* expr, bool isOnlySelect, bool isOnlyParse = false);

extern int CompileStatusSwtichTo(int newCompileStatus);
extern void checkCompileMemoryContext(MemoryContext cxt);
extern int getCompileStatus();
extern void pushCompileContext();
extern PLpgSQL_compile_context* popCompileContext();
void popToOldCompileContext(PLpgSQL_compile_context* save);
extern PLpgSQL_compile_context* createCompileContext(char* const context_name);
extern void clearCompileContext(PLpgSQL_compile_context* compile_cxt);
extern void clearCompileContextList(const int releaseLength);
extern PLpgSQL_datum* GetPackageDatum(List* name, bool* isSamePackage = NULL);
extern bool pushed_bulk_exception();

extern bool CheckElementParsetreeTag(Node* parsetree);
extern Datum transVaratt1BTo4B(Datum value);
extern PLpgSQL_datum* deepCopyPlpgsqlDatum(PLpgSQL_datum* datum);
extern PLpgSQL_var* copyPlpgsqlVar(PLpgSQL_var* src);
extern void assign_text_var(PLpgSQL_var* var, const char* str);
extern MemoryContext GetAvailableHoldContext(List* PortalContextList);

extern void stp_reset_xact();
extern void stp_reset_stmt();
extern void stp_reserve_subxact_resowner(ResourceOwner resowner);
extern void stp_cleanup_subxact_resowner(int64 minStackId);

#endif /* PLPGSQL_H */
