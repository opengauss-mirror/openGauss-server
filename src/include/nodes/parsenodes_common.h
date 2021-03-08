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
 * parsenodes_common.h
 *
 * IDENTIFICATION
 *	  src\include\nodes\parsenodes_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PARSENODES_COMMON_H
#define PARSENODES_COMMON_H

#ifdef FRONTEND_PARSER
#include "postgres_fe.h"
#endif

#include "datatypes.h"
#include "nodes/params.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "catalog/pg_attribute.h"
#include "access/tupdesc.h"
#include "client_logic/client_logic_enums.h"

/* Sort ordering options for ORDER BY and CREATE INDEX */
typedef enum RoleLockType { DO_NOTHING, LOCK_ROLE, UNLOCK_ROLE } RoleLockType;

/*
 * When a command can act on several kinds of objects with only one
 * parse structure required, use these constants to designate the
 * object type.  Note that commands typically don't support all the types.
 */
typedef enum ObjectType {
    OBJECT_AGGREGATE,
    OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_CONSTRAINT,
    OBJECT_CONTQUERY,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DATA_SOURCE,
    OBJECT_DOMAIN,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_INDEX_PARTITION,
    OBJECT_INTERNAL,
    OBJECT_INTERNAL_PARTITION,
    OBJECT_LANGUAGE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PARTITION,
    OBJECT_RLSPOLICY,
    OBJECT_PARTITION_INDEX,
    OBJECT_ROLE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_STREAM,
    OBJECT_SYNONYM,
    OBJECT_TABLE,
    OBJECT_TABLE_PARTITION,
    OBJECT_TABLESPACE,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER,
    OBJECT_VIEW,
    OBJECT_DIRECTORY,
    OBJECT_GLOBAL_SETTING,
    OBJECT_COLUMN_SETTING
} ObjectType;

typedef enum DropBehavior {
    DROP_RESTRICT, /* drop fails if any dependent objects */
    DROP_CASCADE   /* remove dependent objects too */
} DropBehavior;
/* ----------------------
 * Drop Table|Sequence|View|Index|Type|Domain|Conversion|Schema Statement
 * ----------------------
 */

typedef struct DropStmt {
    NodeTag type;
    List *objects;         /* list of sublists of names (as Values) */
    List *arguments;       /* list of sublists of arguments (as Values) */
    ObjectType removeType; /* object type */
    DropBehavior behavior; /* RESTRICT or CASCADE behavior */
    bool missing_ok;       /* skip error if object is missing? */
    bool concurrent;       /* drop index concurrently? */
    bool isProcedure;      /* true if it is DROP PROCEDURE */
} DropStmt;

typedef struct AlterRoleStmt {
    NodeTag type;
    char* role;    /* role name */
    List* options; /* List of DefElem nodes */
    int action;    /* +1 = add members, -1 = drop members */
    RoleLockType lockstatus;
} AlterRoleStmt;

/*
 * TypeName - specifies a type in definitions
 *
 * For TypeName structures generated internally, it is often easier to
 * specify the type by OID than by name.  If "names" is NIL then the
 * actual type OID is given by typeOid, otherwise typeOid is unused.
 * Similarly, if "typmods" is NIL then the actual typmod is expected to
 * be prespecified in typemod, otherwise typemod is unused.
 *
 * If pct_type is TRUE, then names is actually a field name and we look up
 * the type of that field.  Otherwise (the normal case), names is a type
 * name possibly qualified with schema and database name.
 */
typedef struct TypeName {
    NodeTag type;
    List *names;       /* qualified name (list of Value strings) */
    Oid typeOid;       /* type identified by OID */
    bool setof;        /* is a set? */
    bool pct_type;     /* %TYPE specified? */
    List *typmods;     /* type modifier expression(s) */
    int32 typemod;     /* prespecified type modifier */
    List *arrayBounds; /* array bounds */
    int location;      /* token location, or -1 if unknown */
} TypeName;

typedef enum FunctionParameterMode {
    /* the assigned enum values appear in pg_proc, don't change 'em! */
    FUNC_PARAM_IN = 'i',       /* input only */
    FUNC_PARAM_OUT = 'o',      /* output only */
    FUNC_PARAM_INOUT = 'b',    /* both */
    FUNC_PARAM_VARIADIC = 'v', /* variadic (always input) */
    FUNC_PARAM_TABLE = 't'     /* table function output column */
} FunctionParameterMode;

typedef struct FunctionParameter {
    NodeTag type;
    char *name;                 /* parameter name, or NULL if not given */
    TypeName *argType;          /* TypeName for parameter type */
    FunctionParameterMode mode; /* IN/OUT/etc */
    Node *defexpr;              /* raw default expr, or NULL if not given */
} FunctionParameter;

/*
 * Note: FuncWithArgs carries only the types of the input parameters of the
 * function.  So it is sufficient to identify an existing function, but it
 * is not enough info to define a function nor to call it.
 */
typedef struct FuncWithArgs {
    NodeTag type;
    List *funcname; /* qualified name of function */
    List *funcargs; /* list of Typename nodes */
} FuncWithArgs;

/*
 * DefElem - a generic "name = value" option definition
 *
 * In some contexts the name can be qualified.  Also, certain SQL commands
 * allow a SET/ADD/DROP action to be attached to option settings, so it's
 * convenient to carry a field for that too.  (Note: currently, it is our
 * practice that the grammar allows namespace and action only in statements
 * where they are relevant; C code can just ignore those fields in other
 * statements.)
 */
typedef enum DefElemAction {
    DEFELEM_UNSPEC, /* no action given */
    DEFELEM_SET,
    DEFELEM_ADD,
    DEFELEM_DROP
} DefElemAction;

typedef struct DefElem {
    NodeTag type;
    char *defnamespace; /* NULL if unqualified name */
    char *defname;
    Node *arg;               /* a (Value *) or a (TypeName *) */
    DefElemAction defaction; /* unspecified action, or SET/ADD/DROP */
} DefElem;

typedef enum SortByDir {
    SORTBY_DEFAULT,
    SORTBY_ASC,
    SORTBY_DESC,
    SORTBY_USING /* not allowed in CREATE INDEX ... */
} SortByDir;

typedef enum SortByNulls {
    SORTBY_NULLS_DEFAULT,
    SORTBY_NULLS_FIRST,
    SORTBY_NULLS_LAST
} SortByNulls;

/*
 * SortBy - for ORDER BY clause
 */
typedef struct SortBy {
    NodeTag type;
    Node *node;               /* expression to sort on */
    SortByDir sortby_dir;     /* ASC/DESC/USING/default */
    SortByNulls sortby_nulls; /* NULLS FIRST/LAST */
    List *useOp;              /* name of op to use, if SORTBY_USING */
    int location;             /* operator location, or -1 if none/unknown */
} SortBy;

/*
 * WindowDef - raw representation of WINDOW and OVER clauses
 *
 * For entries in a WINDOW list, "name" is the window name being defined.
 * For OVER clauses, we use "name" for the "OVER window" syntax, or "refname"
 * for the "OVER (window)" syntax, which is subtly different --- the latter
 * implies overriding the window frame clause.
 */
typedef struct WindowDef {
    NodeTag type;
    char *name;            /* window's own name */
    char *refname;         /* referenced window name, if any */
    List *partitionClause; /* PARTITION BY expression list */
    List *orderClause;     /* ORDER BY (list of SortBy) */
    int frameOptions;      /* frame_clause options, see below */
    Node *startOffset;     /* expression for starting bound, if any */
    Node *endOffset;       /* expression for ending bound, if any */
    int location;          /* parse location, or -1 if none/unknown */
} WindowDef;

/*
 * IndexElem - index parameters (used in CREATE INDEX)
 *
 * For a plain index attribute, 'name' is the name of the table column to
 * index, and 'expr' is NULL.  For an index expression, 'name' is NULL and
 * 'expr' is the expression tree.
 */
typedef struct IndexElem {
    NodeTag type;
    char *name;                 /* name of attribute to index, or NULL */
    Node *expr;                 /* expression to index, or NULL */
    char *indexcolname;         /* name for index column; NULL = default */
    List *collation;            /* name of collation; NIL = default */
    List *opclass;              /* name of desired opclass; NIL = default */
    SortByDir ordering;         /* ASC/DESC/default */
    SortByNulls nulls_ordering; /* FIRST/LAST/default */
} IndexElem;

/*
 * WithClause -
 * representation of WITH clause
 *
 * Note: WithClause does not propagate into the Query representation;
 * but CommonTableExpr does.
 */
typedef struct WithClause {
    NodeTag type;
    List *ctes;     /* list of CommonTableExprs */
    bool recursive; /* true = WITH RECURSIVE */
    int location;   /* token location, or -1 if unknown */
} WithClause;

/*
 * A_Indices - array subscript or slice bounds ([lidx:uidx] or [uidx])
 */
typedef struct A_Indices {
    NodeTag type;
    Node *lidx; /* NULL if it's a single subscript */
    Node *uidx;
} A_Indices;

/*
 * ResTarget -
 * result target (used in target list of pre-transformed parse trees)
 *
 * In a SELECT target list, 'name' is the column label from an
 * 'AS ColumnLabel' clause, or NULL if there was none, and 'val' is the
 * value expression itself.  The 'indirection' field is not used.
 *
 * INSERT uses ResTarget in its target-column-names list.  Here, 'name' is
 * the name of the destination column, 'indirection' stores any subscripts
 * attached to the destination, and 'val' is not used.
 *
 * In an UPDATE target list, 'name' is the name of the destination column,
 * 'indirection' stores any subscripts attached to the destination, and
 * 'val' is the expression to assign.
 *
 * See A_Indirection for more info about what can appear in 'indirection'.
 */
typedef struct ResTarget {
    NodeTag type;
    char *name;        /* column name or NULL */
    List *indirection; /* subscripts, field names, and '*', or NIL */
    Node *val;         /* the value expression to compute or assign */
    int location;      /* token location, or -1 if unknown */
} ResTarget;

/*
 * Describes a context of hint processing.
 */
typedef struct HintState {
    NodeTag type;

    int nall_hints;        /* Hint num */
    List* join_hint;       /* Join hint list */
    List* leading_hint;    /* Leading hint list */
    List* row_hint;        /* Rows hint list */
    List* stream_hint;     /* stream hint list */
    List* block_name_hint; /* block name hint list */
    List* scan_hint;       /* scan hint list */
    List* skew_hint;       /* skew hint list */
    List* hint_warning;    /* hint warning list */
    bool  multi_node_hint; /* multinode hint */
    List* predpush_hint;
    List* rewrite_hint;    /* rewrite hint list */
} HintState;

/* ----------------------
 * Insert Statement
 *
 * The source expression is represented by SelectStmt for both the
 * SELECT and VALUES cases.  If selectStmt is NULL, then the query
 * is INSERT ... DEFAULT VALUES.
 * ----------------------
 */

typedef struct UpsertClause {
    NodeTag type;
    List *targetList;
    int location;
} UpsertClause;

typedef struct InsertStmt {
    NodeTag type;
    RangeVar *relation;         /* relation to insert into */
    List *cols;                 /* optional: names of the target columns */
    Node *selectStmt;           /* the source SELECT/VALUES, or NULL */
    List *returningList;        /* list of expressions to return */
    WithClause *withClause;     /* WITH clause */
    UpsertClause *upsertClause; /* DUPLICATE KEY UPDATE clause */
    HintState *hintState;
    bool isRewritten;           /* is this Stmt created by rewritter or end user? */
} InsertStmt;

/* ----------------------
 * Delete Statement
 * ----------------------
 */
typedef struct DeleteStmt {
    NodeTag type;
    RangeVar *relation;     /* relation to delete from */
    List *usingClause;      /* optional using clause for more tables */
    Node *whereClause;      /* qualifications */
    List *returningList;    /* list of expressions to return */
    WithClause *withClause; /* WITH clause */
    HintState *hintState;
    Node *limitClause;      /* limit row count */
} DeleteStmt;

/* ----------------------
 * Update Statement
 * ----------------------
 */
typedef struct UpdateStmt {
    NodeTag type;
    RangeVar *relation;     /* relation to update */
    List *targetList;       /* the target list (of ResTarget) */
    Node *whereClause;      /* qualifications */
    List *fromClause;       /* optional from clause for more tables */
    List *returningList;    /* list of expressions to return */
    WithClause *withClause; /* WITH clause */
    HintState *hintState;
} UpdateStmt;

/* ----------------------
 * 		PREPARE Statement
 * ----------------------
 */
typedef struct PrepareStmt {
    NodeTag type;
    char *name;     /* Name of plan, arbitrary */
    List *argtypes; /* Types of parameters (List of TypeName) */
    Node *query;    /* The query itself (as a raw parsetree) */
} PrepareStmt;


/* ----------------------
 * 		EXECUTE Statement
 * ----------------------
 */

typedef struct ExecuteStmt {
    NodeTag type;
    char *name;   /* The name of the plan to execute */
    List *params; /* Values to assign to parameters */
} ExecuteStmt;

/* ----------------------
 * SET Statement (includes RESET)
 *
 * "SET var TO DEFAULT" and "RESET var" are semantically equivalent, but we
 * preserve the distinction in VariableSetKind for CreateCommandTag().
 * ----------------------
 */
typedef enum {
    VAR_SET_VALUE,   /* SET var = value */
    VAR_SET_DEFAULT, /* SET var TO DEFAULT */
    VAR_SET_CURRENT, /* SET var FROM CURRENT */
    VAR_SET_MULTI,   /* special case for SET TRANSACTION ... */
    VAR_SET_ROLEPWD, /* special case for SET ROLE PASSWORD... */
    VAR_RESET,       /* RESET var */
    VAR_RESET_ALL    /* RESET ALL */
} VariableSetKind;

typedef struct VariableSetStmt {
    NodeTag type;
    VariableSetKind kind;
    char *name;    /* variable to be set */
    List *args;    /* List of A_Const nodes */
    bool is_local; /* SET LOCAL? */
} VariableSetStmt;

typedef struct AlterRoleSetStmt {
    NodeTag type;
    char *role;               /* role name */
    char *database;           /* database name, or NULL */
    VariableSetStmt *setstmt; /* SET or RESET subcommand */
} AlterRoleSetStmt;


typedef struct AlterDatabaseSetStmt {
    NodeTag type;
    char *dbname;             /* database name */
    VariableSetStmt *setstmt; /* SET or RESET subcommand */
} AlterDatabaseSetStmt;

/*
 * Note: the "query" field of DeclareCursorStmt is only used in the raw grammar
 * output.	After parse analysis it's set to null, and the Query points to the
 * DeclareCursorStmt, not vice versa.
 * ----------------------
 */
#define CURSOR_OPT_BINARY 0x0001      /* BINARY */
#define CURSOR_OPT_SCROLL 0x0002      /* SCROLL explicitly given */
#define CURSOR_OPT_NO_SCROLL 0x0004   /* NO SCROLL explicitly given */
#define CURSOR_OPT_INSENSITIVE 0x0008 /* INSENSITIVE */
#define CURSOR_OPT_HOLD 0x0010        /* WITH HOLD */
/* these planner-control flags do not correspond to any SQL grammar: */
#define CURSOR_OPT_FAST_PLAN 0x0020    /* prefer fast-start plan */
#define CURSOR_OPT_GENERIC_PLAN 0x0040 /* force use of generic plan */
#define CURSOR_OPT_CUSTOM_PLAN 0x0080  /* force use of custom plan */

typedef struct DeclareCursorStmt {
    NodeTag type;
    char *portalname; /* name of the portal (cursor) */
    int options;      /* bitmask of options (see above) */
    Node *query;      /* the raw SELECT query */
} DeclareCursorStmt;

/* ----------------------
 * Select Statement
 *
 * A "simple" SELECT is represented in the output of gram.y by a single
 * SelectStmt node; so is a VALUES construct.  A query containing set
 * operators (UNION, INTERSECT, EXCEPT) is represented by a tree of SelectStmt
 * nodes, in which the leaf nodes are component SELECTs and the internal nodes
 * represent UNION, INTERSECT, or EXCEPT operators.  Using the same node
 * type for both leaf and internal nodes allows gram.y to stick ORDER BY,
 * LIMIT, etc, clause values into a SELECT statement without worrying
 * whether it is a simple or compound SELECT.
 * ----------------------
 */
typedef enum SetOperation {
    SETOP_NONE = 0,
    SETOP_UNION,
    SETOP_INTERSECT,
    SETOP_EXCEPT
} SetOperation;

typedef struct SelectStmt {
    NodeTag type;

    /*
     * These fields are used only in "leaf" SelectStmts.
     */
    List *distinctClause;   /* NULL, list of DISTINCT ON exprs, or
                             * lcons(NIL,NIL) for all (SELECT DISTINCT) */
    IntoClause *intoClause; /* target for SELECT INTO */
    List *targetList;       /* the target list (of ResTarget) */
    List *fromClause;       /* the FROM clause */
    Node *whereClause;      /* WHERE qualification */
    List *groupClause;      /* GROUP BY clauses */
    Node *havingClause;     /* HAVING conditional-expression */
    List *windowClause;     /* WINDOW window_name AS (...), ... */
    WithClause *withClause; /* WITH clause */

    /*
     * In a "leaf" node representing a VALUES list, the above fields are all
     * null, and instead this field is set.  Note that the elements of the
     * sublists are just expressions, without ResTarget decoration. Also note
     * that a list element can be DEFAULT (represented as a SetToDefault
     * node), regardless of the context of the VALUES list. It's up to parse
     * analysis to reject that where not valid.
     */
    List *valuesLists; /* untransformed list of expression lists */

    /*
     * These fields are used in both "leaf" SelectStmts and upper-level
     * SelectStmts.
     */
    List *sortClause;    /* sort clause (a list of SortBy's) */
    Node *limitOffset;   /* # of result tuples to skip */
    Node *limitCount;    /* # of result tuples to return */
    List *lockingClause; /* FOR UPDATE (list of LockingClause's) */
    HintState *hintState;

    /*
     * These fields are used only in upper-level SelectStmts.
     */
    SetOperation op;         /* type of set op */
    bool all;                /* ALL specified? */
    struct SelectStmt *larg; /* left child */
    struct SelectStmt *rarg; /* right child */

    /*
     * These fields are used by operator "(+)"
     */
    bool hasPlus;
    /* Eventually add fields for CORRESPONDING spec here */
} SelectStmt;

/*
 * CollateClause - a COLLATE expression
 */
typedef struct CollateClause {
    NodeTag type;
    Node *arg;      /* input expression */
    List *collname; /* possibly-qualified collation name */
    int location;   /* token location, or -1 if unknown */
} CollateClause;

/* ----------------------
 * Create Schema Statement
 *
 * NOTE: the schemaElts list contains raw parsetrees for component statements
 * of the schema, such as CREATE TABLE, GRANT, etc.  These are analyzed and
 * executed after the schema itself is created.
 * ----------------------
 */
typedef enum TempType {
    Temp_None,
    Temp_Rel,
    Temp_Toast
} TempType;

typedef struct CreateSchemaStmt {
    NodeTag type;
    char *schemaname;  /* the name of the schema to create */
    char *authid;      /* the owner of the created schema */
    List *schemaElts;  /* schema components (list of parsenodes) */
    TempType temptype; /* if the schema is temp table's schema */
    List *uuids;       /* the list of uuid(only create sequence or table with serial type need) */
} CreateSchemaStmt;

/* ----------------------
 * Alter Table
 * ----------------------
 */
typedef struct AlterTableStmt {
    NodeTag type;
    RangeVar *relation;    /* table to work on */
    List *cmds;            /* list of subcommands */
    ObjectType relkind;    /* type of object */
    bool missing_ok;       /* skip error if table missing */
    bool fromCreate;       /* from create stmt */
    bool need_rewrite_sql; /* after rewrite rule, need to rewrite query string */
} AlterTableStmt;

typedef enum AlterTableType {
    AT_AddColumn,        /* add column */
    AT_AddColumnRecurse, /* internal to commands/tablecmds.c */
    AT_AddColumnToView,  /* implicitly via CREATE OR REPLACE VIEW */
    AT_AddPartition,
    AT_ColumnDefault,     /* alter column default */
    AT_DropNotNull,       /* alter column drop not null */
    AT_SetNotNull,        /* alter column set not null */
    AT_SetStatistics,     /* alter column set statistics */
    AT_AddStatistics,     /* alter column add statistics */
    AT_DeleteStatistics,  /* alter column delete statistics */
    AT_SetOptions,        /* alter column set ( options ) */
    AT_ResetOptions,      /* alter column reset ( options ) */
    AT_SetStorage,        /* alter column set storage */
    AT_DropColumn,        /* drop column */
    AT_DropColumnRecurse, /* internal to commands/tablecmds.c */
    AT_DropPartition,
    AT_AddIndex,                  /* add index */
    AT_ReAddIndex,                /* internal to commands/tablecmds.c */
    AT_AddConstraint,             /* add constraint */
    AT_AddConstraintRecurse,      /* internal to commands/tablecmds.c */
    AT_ValidateConstraint,        /* validate constraint */
    AT_ValidateConstraintRecurse, /* internal to commands/tablecmds.c */
    AT_ProcessedConstraint,       /* pre-processed add constraint (local in
                                   * parser/parse_utilcmd.c) */
    AT_AddIndexConstraint,        /* add constraint using existing index */
    AT_DropConstraint,            /* drop constraint */
    AT_DropConstraintRecurse,     /* internal to commands/tablecmds.c */
    AT_AlterColumnType,           /* alter column type */
    AT_AlterColumnGenericOptions, /* alter column OPTIONS (...) */
    AT_ChangeOwner,               /* change owner */
    AT_ClusterOn,                 /* CLUSTER ON */
    AT_DropCluster,               /* SET WITHOUT CLUSTER */
    AT_AddOids,                   /* SET WITH OIDS */
    AT_AddOidsRecurse,            /* internal to commands/tablecmds.c */
    AT_DropOids,                  /* SET WITHOUT OIDS */
    AT_SetTableSpace,             /* SET TABLESPACE */
    AT_SetPartitionTableSpace,    /* SET TABLESPACE FOR PARTITION */
    AT_SetRelOptions,             /* SET (...) -- AM specific parameters */
    AT_ResetRelOptions,           /* RESET (...) -- AM specific parameters */
    AT_ReplaceRelOptions,         /* replace reloption list in its entirety */
    AT_UnusableIndex,
    AT_UnusableIndexPartition,
    AT_UnusableAllIndexOnPartition,
    AT_RebuildIndex,
    AT_RebuildIndexPartition,
    AT_RebuildAllIndexOnPartition,
    AT_EnableTrig,        /* ENABLE TRIGGER name */
    AT_EnableAlwaysTrig,  /* ENABLE ALWAYS TRIGGER name */
    AT_EnableReplicaTrig, /* ENABLE REPLICA TRIGGER name */
    AT_DisableTrig,       /* DISABLE TRIGGER name */
    AT_EnableTrigAll,     /* ENABLE TRIGGER ALL */
    AT_DisableTrigAll,    /* DISABLE TRIGGER ALL */
    AT_EnableTrigUser,    /* ENABLE TRIGGER USER */
    AT_DisableTrigUser,   /* DISABLE TRIGGER USER */
    AT_EnableRule,        /* ENABLE RULE name */
    AT_EnableAlwaysRule,  /* ENABLE ALWAYS RULE name */
    AT_EnableReplicaRule, /* ENABLE REPLICA RULE name */
    AT_DisableRule,       /* DISABLE RULE name */
    AT_EnableRls,         /* ENABLE ROW LEVEL SECURITY */
    AT_DisableRls,        /* DISABLE ROW LEVEL SECURITY */
    AT_ForceRls,          /* FORCE ROW LEVEL SECURITY */
    AT_NoForceRls,        /* NO FORCE ROW LEVEL SECURITY */
    AT_AddInherit,        /* INHERIT parent */
    AT_DropInherit,       /* NO INHERIT parent */
    AT_AddOf,             /* OF <type_name> */
    AT_DropOf,            /* NOT OF */
    AT_ReplicaIdentity,   /* REPLICA IDENTITY */
    AT_SET_COMPRESS,      /* SET COMPRESS/NOCOMPRESS */
#ifdef PGXC
    AT_DistributeBy,   /* DISTRIBUTE BY ... */
    AT_SubCluster,     /* TO [ NODE nodelist | GROUP groupname ] */
    AT_AddNodeList,    /* ADD NODE nodelist */
    AT_DeleteNodeList, /* DELETE NODE nodelist */
#endif
    AT_GenericOptions, /* OPTIONS (...) */
    AT_EnableRowMoveMent,
    AT_DisableRowMoveMent,
    AT_TruncatePartition,
    AT_ExchangePartition, /* ALTER TABLE EXCHANGE PARTITION WITH TABLE */
    AT_MergePartition,    /* MERGE PARTITION */
    AT_SplitPartition,    /* SPLIT PARTITION */
    /* this will be in a more natural position in 9.3: */
    AT_ReAddConstraint /* internal to commands/tablecmds.c */
} AlterTableType;

typedef enum AlterTableStatProperty { /* Additional Property for AlterTableCmd */
    AT_CMD_WithPercent,               /* ALTER TABLE ALTER COLUMN SET STATISTICS PERCENT */
    AT_CMD_WithoutPercent             /* ALTER TABLE ALTER COLUMN SET STATISTICS */
} AlterTableStatProperty;

typedef struct AlterTableCmd { /* one subcommand of an ALTER TABLE */
    NodeTag type;
    AlterTableType subtype;                     /* Type of table alteration to apply */
    char *name;                                 /* column, constraint, or trigger to act on,
                                                 * or new owner or tablespace */
    Node *def;                                  /* definition of new column, index,
                                                 * constraint, or parent table */
    DropBehavior behavior;                      /* RESTRICT or CASCADE for DROP cases */
    bool missing_ok;                            /* skip error if missing? */
    RangeVar *exchange_with_rel;                /* the ordinary table of exchange with */
    bool check_validation;                      /* Checking the tuple of ordinary table
                                                   whether can insert into the partition */
    bool exchange_verbose;                      /* When check_validation is true, if tuple
                                                   of ordinary table can not insert into
                                                   the partition, insert the tuple the right partition */
    char *target_partition_tablespace;          /* using in merge partition */
    AlterTableStatProperty additional_property; /* additional property for AlterTableCmd */
    List *bucket_list;                          /* bucket list to drop */
    bool alterGPI;                              /* check whether is global partition index alter statement */
} AlterTableCmd;

/* ----------------------
 * REINDEX Statement
 * ----------------------
 */

/* struct for adaptive memory allocation for specific utility */
typedef struct AdaptMem {
    int work_mem; /* estimate mem for the utility */
    int max_mem;  /* max spread mem for the utility */
} AdaptMem;

typedef struct ReindexStmt {
    NodeTag type;
    ObjectType kind;    /* OBJECT_INDEX, OBJECT_TABLE, OBJECT_INTERNAL, OBJECT_DATABASE */
    RangeVar *relation; /* Table or index to reindex */
    const char *name;   /* name of database to reindex */
    bool do_system;     /* include system tables in database case */
    bool do_user;       /* include user tables in database case */
    AdaptMem memUsage;  /* adaptive memory assigned for the stmt */
} ReindexStmt;

typedef struct Position {
    NodeTag type;
    char *colname; /* name of column */
    int fixedlen;
    int position;
} Position;

/* -------------------------------------------
 * Create Row Level Security Policy Statement
 * -------------------------------------------
 */
typedef struct CreateRlsPolicyStmt {
    NodeTag type;
    bool isPermissive;  /* restrictive or permissive policy */
    bool fromExternal;  /* this command from external(user) or internal(system) */
    char *policyName;   /* Policy's name */
    RangeVar *relation; /* the table name the policy applies to */
    char *cmdName;      /* the command name the policy applies to */
    List *roleList;     /* the roles associated with the policy */
    Node *usingQual;    /* the policy's condition */
} CreateRlsPolicyStmt;

/* ------------------------------------------
 * Alter Row Level Security Policy Statement
 * ------------------------------------------
 */
typedef struct AlterRlsPolicyStmt {
    NodeTag type;
    char *policyName;   /* Policy's name */
    RangeVar *relation; /* the table name the policy applies to */
    List *roleList;     /* the roles associated with the policy */
    Node *usingQual;    /* the policy's condition */
} AlterPolicyStmt;

// CLIENT_LOGIC GLOBAL_SETTINGS
typedef enum class ClientLogicGlobalProperty {
    CLIENT_GLOBAL_FUNCTION,
    CLIENT_GLOBAL_ARGS,
    CMK_KEY_STORE,
    CMK_KEY_PATH,
    CMK_ALGORITHM
} ClientLogicGlobalProperty;


typedef struct ClientLogicGlobalParam {
    NodeTag type;
    ClientLogicGlobalProperty key;
    char *value;
    unsigned int len;
    int location; /* token location, or -1 if unknown */
} ClientLogicGlobalParam;


typedef struct CreateClientLogicGlobal {
    NodeTag type;
    List *global_key_name;
    List *global_setting_params;
} CreateClientLogicGlobal;

// CLIENT_LOGIC COLUMN_SETTINGS
typedef enum class ClientLogicColumnProperty {
    CLIENT_GLOBAL_SETTING,
    CEK_ALGORITHM,
    CEK_EXPECTED_VALUE,
    COLUMN_ENCRYPTION_TYPE,
    COLUMN_COLUMN_FUNCTION,
    COLUMN_COLUMN_ARGS
} ClientLogicColumnProperty;

typedef struct ClientLogicColumnParam {
    NodeTag type;
    ClientLogicColumnProperty key;
    char *value;
    unsigned int len;
    List *qualname;
    int location; /* token location, or -1 if unknown */
} ClientLogicColumnParam;

typedef struct CreateClientLogicColumn {
    NodeTag type;
    List *column_key_name;
    List *column_setting_params;
} CreateClientLogicColumn;

typedef struct ClientLogicColumnRef {
    NodeTag type;
    EncryptionType columnEncryptionAlgorithmType;
    List *column_key_name;
    TypeName *orig_typname; /* original type of column */
    TypeName *dest_typname; /* real type of column */
    int location;           /* token location, or -1 if unknown */
    Oid encryptionoid;
} ClientLogicColumnRef;
/*
 * ColumnDef - column definition (used in various creates)
 *
 * If the column has a default value, we may have the value expression
 * in either "raw" form (an untransformed parse tree) or "cooked" form
 * (a post-parse-analysis, executable expression tree), depending on
 * how this ColumnDef node was created (by parsing, or by inheritance
 * from an existing relation).  We should never have both in the same node!
 *
 * Similarly, we may have a COLLATE specification in either raw form
 * (represented as a CollateClause with arg==NULL) or cooked form
 * (the collation's OID).
 *
 * The constraints list may contain a CONSTR_DEFAULT item in a raw
 * parsetree produced by gram.y, but transformCreateStmt will remove
 * the item and set raw_default instead.  CONSTR_DEFAULT items
 * should not appear in any subsequent processing.
 */
typedef struct ColumnDef {
    NodeTag type;
    char *colname;             /* name of column */
    TypeName *typname;         /* type of column */
    int kvtype;                /* kv attribute type if use kv storage */
    int inhcount;              /* number of times column is inherited */
    bool is_local;             /* column has local (non-inherited) def'n */
    bool is_not_null;          /* NOT NULL constraint specified? */
    bool is_from_type;         /* column definition came from table type */
    bool is_serial;            /* column is serial type or not */
    char storage;              /* attstorage setting, or 0 for default */
    int8 cmprs_mode;           /* compression method applied to this column */
    Node *raw_default;         /* default value (untransformed parse tree) */
    Node *cooked_default;      /* default value (transformed expr tree) */
    CollateClause *collClause; /* untransformed COLLATE spec, if any */
    Oid collOid;               /* collation OID (InvalidOid if not set) */
    List *constraints;         /* other constraints on column */
    List *fdwoptions;          /* per-column FDW options */
    ClientLogicColumnRef *clientLogicColumnRef;
    Position *position;
    Form_pg_attribute dropped_attr; /* strcuture for dropped attribute during create table like OE */
} ColumnDef;

/*
 * definition of a range partition.
 * range partition pattern: PARTITION [partitionName] LESS THAN [boundary]
 *
 */
typedef struct RangePartitionDefState {
    NodeTag type;
    char *partitionName;  /* name of range partition */
    List *boundary;       /* the boundary of a range partition */
    char *tablespacename; /* table space to use, or NULL */
    Const *curStartVal;
    char *partitionInitName;
} RangePartitionDefState;

typedef struct RangePartitionStartEndDefState {
    NodeTag type;
    char *partitionName;  /* name of range partition */
    List *startValue;     /* the start value of a start/end clause */
    List *endValue;       /* the end value of a start/end clause */
    List *everyValue;     /* the interval value of a start/end clause */
    char *tableSpaceName; /* table space to use, or NULL */
} RangePartitionStartEndDefState;

/* *
 * definition of a range partition.
 * interval pattern: INTERVAL ([interval]) [tablespaceLists]
 */
typedef struct IntervalPartitionDefState {
    NodeTag type;
    Node *partInterval;        /* the interval of table which is a  constant expression  */
    List *intervalTablespaces; /* list of tablespace */
} IntervalPartitionDefState;
/* *
 * definition of a partitioned table.
 */
typedef enum RowMovementValue {
    ROWMOVEMENT_DISABLE,
    ROWMOVEMENT_ENABLE,
    ROWMOVEMENT_DEFAULT
} RowMovementValue;

typedef struct PartitionState {
    NodeTag type;
    char partitionStrategy;
    /*
     * 'i': interval partition
     * 'r': range partition
     * 'v': value partition (HDFS table only)
     * 'i': interval partition. (unsupported yet)
     * 'l': list partition (unsupported yet)
     * 'h': hash partition (unsupported yet)
     */

    IntervalPartitionDefState *intervalPartDef; /* interval definition */
    List *partitionKey;                         /* partition key of partitioned table , which is list of ColumnRef */
    List *partitionList;                        /* list of partition definition */
    RowMovementValue rowMovement; /* default: for colum-stored table means true, for row-stored means false */
} PartitionState;

typedef struct AddPartitionState { /* ALTER TABLE ADD PARTITION */
    NodeTag type;
    List *partitionList;
    bool isStartEnd;
} AddPartitionState;

typedef struct SplitPartitionState { /* ALTER TABLE SPLIT PARTITION INTO */
    NodeTag type;
    char *src_partition_name;
    List *partition_for_values;
    List *split_point;
    List *dest_partition_define_list;
} SplitPartitionState;

typedef struct ReplicaIdentityStmt {
    NodeTag type;
    char identity_type;
    char *name;
} ReplicaIdentityStmt;

/* ----------------------
 * Create Table Statement
 *
 * NOTE: in the raw gram.y output, ColumnDef and Constraint nodes are
 * intermixed in tableElts, and constraints is NIL.  After parse analysis,
 * tableElts contains just ColumnDefs, and constraints contains just
 * Constraint nodes (in fact, only CONSTR_CHECK nodes, in the present
 * implementation).
 * ----------------------
 */

typedef struct CreateStmt {
    NodeTag type;
    RangeVar *relation;             /* relation to create */
    List *tableElts;                /* column definitions (list of ColumnDef) */
    List *inhRelations;             /* relations to inherit from (list of
                                     * inhRelation) */
    TypeName *ofTypename;           /* OF typename */
    List *constraints;              /* constraints (list of Constraint nodes) */
    List *options;                  /* options from WITH clause */
    List *clusterKeys;              /* partial cluster key for table */
    OnCommitAction oncommit;        /* what do we do at COMMIT? */
    char *tablespacename;           /* table space to use, or NULL */
    bool if_not_exists;             /* just do nothing if it already exists? */
    bool ivm;                       /* incremental view maintenance is used by materialized view */
    int8 row_compress;              /* row compression flag */
    PartitionState *partTableState; /* the PartitionState */
#ifdef PGXC
    DistributeBy *distributeby; /* distribution to use, or NULL */
    PGXCSubCluster *subcluster; /* subcluster of table */
#endif

    List *tableEltsDup; /* Used for cstore constraint check */
    char *internalData; /* Used for create table like */

    List *uuids;        /* list of uuid, used for create sequence(like 'create table t(a serial))' */
    Oid oldBucket;      /* bucketoid of resizing table */
    List *oldNode;      /* relfilenode of resizing table */
    List *oldToastNode; /* toastnode of resizing table  */
    char relkind;       /* type of object */
} CreateStmt;

/* ----------------------
 * 		Copy Statement
 *
 * We support "COPY relation FROM file", "COPY relation TO file", and
 * "COPY (query) TO file".	In any given CopyStmt, exactly one of "relation"
 * and "query" must be non-NULL.
 * ----------------------
 */
typedef struct CopyStmt {
    NodeTag type;
    RangeVar *relation; /* the relation to copy */
    Node *query;        /* the SELECT query to copy */
    List *attlist;      /* List of column names (as Strings), or NIL
                         * for all columns */
    bool is_from;       /* TO or FROM */
    char *filename;     /* filename, or NULL for STDIN/STDOUT */
    List *options;      /* List of DefElem nodes */

    /* adaptive memory assigned for the stmt */
    AdaptMem memUsage;
    bool encrypted;
} CopyStmt;

#define ATT_KV_UNDEFINED (0)
#define ATT_KV_TAG (1)
#define ATT_KV_FIELD (2)
#define ATT_KV_TIMETAG (3)
#define ATT_KV_HIDE (4)
// valid value for ColumnDef.cmprs_mode
//
#define ATT_CMPR_UNDEFINED (0x7F)
//
// default value for system tables' attrs.
// modify %PGATTR_DEFAULTS in src/backend/catalog/genbki.pl if you modify this mocro
//
#define ATT_CMPR_NOCOMPRESS (0)
#define ATT_CMPR_DELTA (1)
#define ATT_CMPR_DICTIONARY (2)
#define ATT_CMPR_PREFIX (3)
#define ATT_CMPR_NUMSTR (4)

/* ----------
 * Definitions for constraints in CreateStmt
 *
 * Note that column defaults are treated as a type of constraint,
 * even though that's a bit odd semantically.
 *
 * For constraints that use expressions (CONSTR_CHECK, CONSTR_DEFAULT)
 * we may have the expression in either "raw" form (an untransformed
 * parse tree) or "cooked" form (the nodeToString representation of
 * an executable expression tree), depending on how this Constraint
 * node was created (by parsing, or by inheritance from an existing
 * relation).  We should never have both in the same node!
 *
 * FKCONSTR_ACTION_xxx values are stored into pg_constraint.confupdtype
 * and pg_constraint.confdeltype columns; FKCONSTR_MATCH_xxx values are
 * stored into pg_constraint.confmatchtype.  Changing the code values may
 * require an initdb!
 *
 * If skip_validation is true then we skip checking that the existing rows
 * in the table satisfy the constraint, and just install the catalog entries
 * for the constraint.  A new FK constraint is marked as valid iff
 * initially_valid is true.  (Usually skip_validation and initially_valid
 * are inverses, but we can set both true if the table is known empty.)
 *
 * Constraint attributes (DEFERRABLE etc) are initially represented as
 * separate Constraint nodes for simplicity of parsing.  parse_utilcmd.c makes
 * a pass through the constraints list to insert the info into the appropriate
 * Constraint node.
 * ----------
 */

typedef enum ConstrType { /* types of constraints */
    CONSTR_NULL,          /* not SQL92, but a lot of people expect it */
    CONSTR_NOTNULL,
    CONSTR_DEFAULT,
    CONSTR_CHECK,
    CONSTR_PRIMARY,
    CONSTR_UNIQUE,
    CONSTR_EXCLUSION,
    CONSTR_FOREIGN,
    CONSTR_CLUSTER,
    CONSTR_ATTR_DEFERRABLE, /* attributes for previous constraint node */
    CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_DEFERRED,
    CONSTR_ATTR_IMMEDIATE
} ConstrType;

typedef struct Constraint {
    NodeTag type;
    ConstrType contype; /* see above */

    /* Fields used for most/all constraint types: */
    char *conname;     /* Constraint name, or NULL if unnamed */
    bool deferrable;   /* DEFERRABLE? */
    bool initdeferred; /* INITIALLY DEFERRED? */
    int location;      /* token location, or -1 if unknown */

    /* Fields used for constraints with expressions (CHECK and DEFAULT): */
    bool is_no_inherit; /* is constraint non-inheritable? */
    Node *raw_expr;     /* expr, as untransformed parse tree */
    char *cooked_expr;  /* expr, as nodeToString representation */

    /* Fields used for unique constraints (UNIQUE and PRIMARY KEY) or cluster partial key for colstore: */
    List *keys;      /* String nodes naming referenced column(s) */
    List *including; /* String nodes naming referenced nonkey column(s) */

    /* Fields used for EXCLUSION constraints: */
    List *exclusions; /* list of (IndexElem, operator name) pairs */

    /* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
    List *options;    /* options from WITH clause */
    char *indexname;  /* existing index to use; otherwise NULL */
    char *indexspace; /* index tablespace; NULL for default */
    /* These could be, but currently are not, used for UNIQUE/PKEY: */
    char *access_method; /* index access method; NULL for default */
    Node *where_clause;  /* partial index predicate */

    /* Fields used for FOREIGN KEY constraints: */
    RangeVar *pktable;   /* Primary key table */
    List *fk_attrs;      /* Attributes of foreign key */
    List *pk_attrs;      /* Corresponding attrs in PK table */
    char fk_matchtype;   /* FULL, PARTIAL, UNSPECIFIED */
    char fk_upd_action;  /* ON UPDATE action */
    char fk_del_action;  /* ON DELETE action */
    List *old_conpfeqop; /* pg_constraint.conpfeqop of my former self */
    Oid old_pktable_oid; /* pg_constraint.confrelid of my former self */

    /* Fields used for constraints that allow a NOT VALID specification */
    bool skip_validation; /* skip validation of existing rows? */
    bool initially_valid; /* mark the new constraint as valid? */

    /*
     * @hdfs
     * Field used for soft constraint, which works on HDFS foreign table.
     */
    InformationalConstraint *inforConstraint;
} Constraint;

/*
 * TableLikeClause - CREATE TABLE ( ... LIKE ... ) clause
 */
typedef struct TableLikeClause {
    NodeTag type;
    RangeVar *relation;
    bits32 options; /* OR of TableLikeOption flags */
} TableLikeClause;

#define MAX_TABLE_LIKE_OPTIONS (10)
typedef enum TableLikeOption {
    CREATE_TABLE_LIKE_DEFAULTS = 1 << 0,
    CREATE_TABLE_LIKE_CONSTRAINTS = 1 << 1,
    CREATE_TABLE_LIKE_INDEXES = 1 << 2,
    CREATE_TABLE_LIKE_STORAGE = 1 << 3,
    CREATE_TABLE_LIKE_COMMENTS = 1 << 4,
    CREATE_TABLE_LIKE_PARTITION = 1 << 5,
    CREATE_TABLE_LIKE_RELOPTIONS = 1 << 6,
    CREATE_TABLE_LIKE_DISTRIBUTION = 1 << 7,
    CREATE_TABLE_LIKE_OIDS = 1 << 8,
    CREATE_TABLE_LIKE_DEFAULTS_SERIAL = 1 << 9, /* Backward compatibility. Inherits serial defaults by default. */
    CREATE_TABLE_LIKE_ALL = 0x7FFFFFFF
} TableLikeOption;

/* Foreign key matchtype codes */
#define FKCONSTR_MATCH_FULL 'f'
#define FKCONSTR_MATCH_PARTIAL 'p'
#define FKCONSTR_MATCH_UNSPECIFIED 'u'

/* Foreign key action codes */
#define FKCONSTR_ACTION_NOACTION 'a'
#define FKCONSTR_ACTION_RESTRICT 'r'
#define FKCONSTR_ACTION_CASCADE 'c'
#define FKCONSTR_ACTION_SETNULL 'n'
#define FKCONSTR_ACTION_SETDEFAULT 'd'

/* ***************************************************************************
 * Supporting data structures for Parse Trees
 *
 * Most of these node types appear in raw parsetrees output by the grammar,
 * and get transformed to something else by the analyzer.  A few of them
 * are used as-is in transformed querytrees.
 * ************************************************************************** */
/*
 * ColumnRef - specifies a reference to a column, or possibly a whole tuple
 *
 * The "fields" list must be nonempty.  It can contain string Value nodes
 * (representing names) and A_Star nodes (representing occurrence of a '*').
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 *
 * Note: any array subscripting or selection of fields from composite columns
 * is represented by an A_Indirection node above the ColumnRef.  However,
 * for simplicity in the normal case, initial field selection from a table
 * name is represented within ColumnRef and not by adding A_Indirection.
 */
typedef struct ColumnRef {
    NodeTag type;
    List *fields; /* field names (Value strings) or A_Star */
    int location; /* token location, or -1 if unknown */
} ColumnRef;

/*
 * A_Star - '*' representing all columns of a table or compound field
 *
 * This can appear within ColumnRef.fields, A_Indirection.indirection, and
 * ResTarget.indirection lists.
 */
typedef struct A_Star {
    NodeTag type;
} A_Star;

/*
 * CommonTableExpr -
 * representation of WITH list element
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
typedef struct CommonTableExpr {
    NodeTag type;
    char *ctename;       /* query name (never qualified) */
    List *aliascolnames; /* optional list of column names */
    /* SelectStmt/InsertStmt/etc before parse analysis, Query afterwards: */
    Node *ctequery; /* the CTE's subquery */
    int location;   /* token location, or -1 if unknown */
    /* These fields are set during parse analysis: */
    bool cterecursive;      /* is this CTE actually recursive? */
    int cterefcount;        /* number of RTEs referencing this CTE
                             * (excluding internal self-references) */
    List *ctecolnames;      /* list of output column names */
    List *ctecoltypes;      /* OID list of output column type OIDs */
    List *ctecoltypmods;    /* integer list of output column typmods */
    List *ctecolcollations; /* OID list of column collation OIDs */
    char locator_type;      /* the location type of cte */
} CommonTableExpr;

/*
 * FuncCall - a function or aggregate invocation
 *
 * agg_order (if not NIL) indicates we saw 'foo(... ORDER BY ...)'.
 * agg_star indicates we saw a 'foo(*)' construct, while agg_distinct
 * indicates we saw 'foo(DISTINCT ...)'.  In any of these cases, the
 * construct *must* be an aggregate call.  Otherwise, it might be either an
 * aggregate or some other kind of function.  However, if OVER is present
 * it had better be an aggregate or window function.
 */
typedef struct FuncCall {
    NodeTag type;
    List *funcname;  /* qualified name of function */
    char *colname;   /* column name for the function */
    List *args;      /* the arguments (list of exprs) */
    List *agg_order; /* ORDER BY (list of SortBy) */
    bool agg_within_group;
    bool agg_star;          /* argument was really '*' */
    bool agg_distinct;      /* arguments were labeled DISTINCT */
    bool func_variadic;     /* last argument was labeled VARIADIC */
    struct WindowDef *over; /* OVER clause, if any */
    int location;           /* token location, or -1 if unknown */
    bool call_func;         /* call function, false is select function */
} FuncCall;

/*
 * GroupingSet -
 * representation of CUBE, ROLLUP and GROUPING SETS clauses
 *
 * In a Query with grouping sets, the groupClause contains a flat list of
 * SortGroupClause nodes for each distinct expression used.  The actual
 * structure of the GROUP BY clause is given by the groupingSets tree
 *
 * In the raw parser output, GroupingSet nodes (of all types except SIMPLE
 * which is not used) are potentially mixed in with the expressions in the
 * groupClause of the SelectStmt.  (An expression can't contain a GroupingSet,
 * but a list may mix GroupingSet and expression nodes.)  At this stage, the
 * content of each node is a list of expressions, some of which may be RowExprs
 * which represent sublists rather than actual row constructors, and nested
 * GroupingSet nodes where legal in the grammar.  The structure directly
 * reflects the query syntax.
 *
 * In parse analysis, the transformed expressions are used to build the tlist
 * and groupClause list (of SortGroupClause nodes), and the groupingSets tree
 * is eventually reduced to a fixed format:
 *
 * EMPTY nodes represent (), and obviously have no content
 *
 * SIMPLE nodes represent a list of one or more expressions to be treated as an
 * atom by the enclosing structure; the content is an integer list of
 * ressortgroupref values (see SortGroupClause)
 *
 * CUBE and ROLLUP nodes contain a list of one or more SIMPLE nodes.
 *
 * SETS nodes contain a list of EMPTY, SIMPLE, CUBE or ROLLUP nodes, but after
 * parse analysis they cannot contain more SETS nodes; enough of the syntactic
 * transforms of the spec have been applied that we no longer have arbitrarily
 * deep nesting (though we still preserve the use of cube/rollup).
 *
 * Note that if the groupingSets tree contains no SIMPLE nodes (only EMPTY
 * nodes at the leaves), then the groupClause will be empty, but this is still
 * an aggregation query (similar to using aggs or HAVING without GROUP BY).
 *
 * As an example, the following clause:
 *
 * GROUP BY GROUPING SETS ((a,b), CUBE(c,(d,e)))
 *
 * looks like this after raw parsing:
 *
 * SETS( RowExpr(a,b) , CUBE( c, RowExpr(d,e) ) )
 *
 * and parse analysis converts it to:
 *
 * SETS( SIMPLE(1,2), CUBE( SIMPLE(3), SIMPLE(4,5) ) )
 */
typedef enum {
    GROUPING_SET_EMPTY,
    GROUPING_SET_SIMPLE,
    GROUPING_SET_ROLLUP,
    GROUPING_SET_CUBE,
    GROUPING_SET_SETS
} GroupingSetKind;

typedef struct GroupingSet {
    NodeTag type;
    GroupingSetKind kind;
    List *content;
    int location;
} GroupingSet;

/*
 * LockingClause - raw representation of FOR UPDATE/SHARE options
 *
 * Note: lockedRels == NIL means "all relations in query".  Otherwise it
 * is a list of RangeVar nodes.  (We use RangeVar mainly because it carries
 * a location field --- currently, parse analysis insists on unqualified
 * names in LockingClause.)
 */
typedef struct LockingClause {
    NodeTag type;
    List *lockedRels; /* FOR UPDATE or FOR SHARE relations */
    bool forUpdate;   /* true = FOR UPDATE, false = FOR SHARE */
    bool noWait;      /* NOWAIT option */
} LockingClause;

/*
 * RangeTableSample - TABLESAMPLE appearing in a raw FROM clause
 *
 * This node, appearing only in raw parse trees, represents
 * <relation> TABLESAMPLE <method> (<params>) REPEATABLE (<num>)
 * Currently, the <relation> can only be a RangeVar, but we might in future
 * allow RangeSubselect and other options.  Note that the RangeTableSample
 * is wrapped around the node representing the <relation>, rather than being
 * a subfield of it.
 */
typedef struct RangeTableSample {
    NodeTag type;
    Node *relation;   /* relation to be sampled */
    List *method;     /* sampling method name (possibly qualified) */
    List *args;       /* argument(s) for sampling method */
    Node *repeatable; /* REPEATABLE expression, or NULL if none */
    int location;     /* method name location, or -1 if unknown */
} RangeTableSample;

/*
 * RangeFunction - function call appearing in a FROM clause
 */
typedef struct RangeFunction {
    NodeTag type;
    bool lateral;       /* does it have LATERAL prefix? */
    Node *funccallnode; /* untransformed function call tree */
    Alias *alias;       /* table alias & optional column aliases */
    List *coldeflist;   /* list of ColumnDef nodes to describe result
                         * of function returning RECORD */
} RangeFunction;

/*
 * RangeSubselect - subquery appearing in a FROM clause
 */
typedef struct RangeSubselect {
    NodeTag type;
    bool lateral;   /* does it have LATERAL prefix? */
    Node *subquery; /* the untransformed sub-select clause */
    Alias *alias;   /* table alias & optional column aliases */
} RangeSubselect;

/*
 * A_Expr - infix, prefix, and postfix expressions
 */
typedef enum A_Expr_Kind {
    AEXPR_OP,  /* normal operator */
    AEXPR_AND, /* booleans - name field is unused */
    AEXPR_OR,
    AEXPR_NOT,
    AEXPR_OP_ANY,   /* scalar op ANY (array) */
    AEXPR_OP_ALL,   /* scalar op ALL (array) */
    AEXPR_DISTINCT, /* IS DISTINCT FROM - name must be "=" */
    AEXPR_NULLIF,   /* NULLIF - name must be "=" */
    AEXPR_OF,       /* IS [NOT] OF - name must be "=" or "<>" */
    AEXPR_IN        /* [NOT] IN - name must be "=" or "<>" */
} A_Expr_Kind;

typedef struct A_Expr {
    NodeTag type;
    A_Expr_Kind kind; /* see above */
    List *name;       /* possibly-qualified name of operator */
    Node *lexpr;      /* left argument, or NULL if none */
    Node *rexpr;      /* right argument, or NULL if none */
    int location;     /* token location, or -1 if unknown */
} A_Expr;

/*
 * ParamRef - specifies a $n parameter reference
 */
typedef struct ParamRef {
    NodeTag type;
    int number;   /* the number of the parameter */
    int location; /* token location, or -1 if unknown */
} ParamRef;

/*
 * A_Indirection - select a field and/or array element from an expression
 *
 * The indirection list can contain A_Indices nodes (representing
 * subscripting), string Value nodes (representing field selection --- the
 * string value is the name of the field to select), and A_Star nodes
 * (representing selection of all fields of a composite type).
 * For example, a complex selection operation like
 * (foo).field1[42][7].field2
 * would be represented with a single A_Indirection node having a 4-element
 * indirection list.
 *
 * Currently, A_Star must appear only as the last list element --- the grammar
 * is responsible for enforcing this!
 */
typedef struct A_Indirection {
    NodeTag type;
    Node *arg;         /* the thing being selected from */
    List *indirection; /* subscripts and/or field names and/or * */
} A_Indirection;

/*
 * A_ArrayExpr - an ARRAY[] construct
 */
typedef struct A_ArrayExpr {
    NodeTag type;
    List *elements; /* array element expressions */
    int location;   /* token location, or -1 if unknown */
} A_ArrayExpr;

/*
 * frameOptions is an OR of these bits.  The NONDEFAULT and BETWEEN bits are
 * used so that ruleutils.c can tell which properties were specified and
 * which were defaulted; the correct behavioral bits must be set either way.
 * The START_foo and END_foo options must come in pairs of adjacent bits for
 * the convenience of gram.y, even though some of them are useless/invalid.
 * We will need more bits (and fields) to cover the full SQL:2008 option set.
 */
#define FRAMEOPTION_NONDEFAULT 0x00001                /* any specified? */
#define FRAMEOPTION_RANGE 0x00002                     /* RANGE behavior */
#define FRAMEOPTION_ROWS 0x00004                      /* ROWS behavior */
#define FRAMEOPTION_BETWEEN 0x00008                   /* BETWEEN given? */
#define FRAMEOPTION_START_UNBOUNDED_PRECEDING 0x00010 /* start is U. P. */
#define FRAMEOPTION_END_UNBOUNDED_PRECEDING 0x00020   /* (disallowed) */
#define FRAMEOPTION_START_UNBOUNDED_FOLLOWING 0x00040 /* (disallowed) */
#define FRAMEOPTION_END_UNBOUNDED_FOLLOWING 0x00080   /* end is U. F. */
#define FRAMEOPTION_START_CURRENT_ROW 0x00100         /* start is C. R. */
#define FRAMEOPTION_END_CURRENT_ROW 0x00200           /* end is C. R. */
#define FRAMEOPTION_START_VALUE_PRECEDING 0x00400     /* start is V. P. */
#define FRAMEOPTION_END_VALUE_PRECEDING 0x00800       /* end is V. P. */
#define FRAMEOPTION_START_VALUE_FOLLOWING 0x01000     /* start is V. F. */
#define FRAMEOPTION_END_VALUE_FOLLOWING 0x02000       /* end is V. F. */

#define FRAMEOPTION_START_VALUE (FRAMEOPTION_START_VALUE_PRECEDING | FRAMEOPTION_START_VALUE_FOLLOWING)
#define FRAMEOPTION_END_VALUE (FRAMEOPTION_END_VALUE_PRECEDING | FRAMEOPTION_END_VALUE_FOLLOWING)

#define FRAMEOPTION_DEFAULTS (FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW)

/*
 * XMLSERIALIZE (in raw parse tree only)
 */
typedef struct XmlSerialize {
    NodeTag type;
    XmlOptionType xmloption; /* DOCUMENT or CONTENT */
    Node *expr;
    TypeName *typname;
    int location; /* token location, or -1 if unknown */
} XmlSerialize;

/*
 * TypeCast - a CAST expression
 */
typedef struct TypeCast {
    NodeTag type;
    Node *arg;         /* the expression being casted */
    TypeName *typname; /* the target type */
    int location;      /* token location, or -1 if unknown */
} TypeCast;

/*
 * A_Const - a literal constant
 */
typedef struct A_Const {
    NodeTag type;
    Value val;    /* value (includes type info, see value.h) */
    int location; /* token location, or -1 if unknown */
} A_Const;


/* Possible sources of a Query */
typedef enum QuerySource {
    QSRC_ORIGINAL,          /* original parsetree (explicit query) */
    QSRC_PARSER,            /* added by parse analysis in MERGE */
    QSRC_INSTEAD_RULE,      /* added by unconditional INSTEAD rule */
    QSRC_QUAL_INSTEAD_RULE, /* added by conditional INSTEAD rule */
    QSRC_NON_INSTEAD_RULE   /* added by non-INSTEAD rule */
} QuerySource;

typedef enum TdTruncCastStatus {
    UNINVOLVED_QUERY = 0,
    NOT_CAST_BECAUSEOF_GUC,
    TRUNC_CAST_QUERY
} TdTruncCastStatus;
#define TRUNCAST_VERSION_NUM 92023

/* ****************************************************************************
 * 	Query Tree
 * *************************************************************************** */

/*
 * Query -
 * 	  Parse analysis turns all statements into a Query tree
 * 	  for further processing by the rewriter and planner.
 *
 * 	  Utility statements (i.e. non-optimizable statements) have the
 * 	  utilityStmt field set, and the Query itself is mostly dummy.
 * 	  DECLARE CURSOR is a special case: it is represented like a SELECT,
 * 	  but the original DeclareCursorStmt is stored in utilityStmt.
 *
 * 	  Planning converts a Query tree into a Plan tree headed by a PlannedStmt
 * 	  node --- the Query structure is not used by the executor.
 */
typedef struct Query {
    NodeTag type;

    CmdType commandType; /* select|insert|update|delete|merge|utility */

    QuerySource querySource; /* where did I come from? */

    uint64 queryId; /* query identifier (can be set by plugins) */

    bool canSetTag; /* do I set the command result tag? */

    Node* utilityStmt; /* non-null if this is DECLARE CURSOR or a
                        * non-optimizable statement */

    int resultRelation; /* rtable index of target relation for
                         * INSERT/UPDATE/DELETE/MERGE; 0 for SELECT */

    bool hasAggs;         /* has aggregates in tlist or havingQual */
    bool hasWindowFuncs;  /* has window functions in tlist */
    bool hasSubLinks;     /* has subquery SubLink */
    bool hasDistinctOn;   /* distinctClause is from DISTINCT ON */
    bool hasRecursive;    /* WITH RECURSIVE was specified */
    bool hasModifyingCTE; /* has INSERT/UPDATE/DELETE in WITH */
    bool hasForUpdate;    /* FOR UPDATE or FOR SHARE was specified */
    bool hasRowSecurity;  /* rewriter has applied some RLS policy */
    bool hasSynonyms;     /* has synonym mapping in rtable */

    List* cteList; /* WITH list (of CommonTableExpr's) */

    List* rtable;       /* list of range table entries */
    FromExpr* jointree; /* table join tree (FROM and WHERE clauses) */

    List* targetList; /* target list (of TargetEntry) */

    List* starStart; /* Corresponding p_star_start in ParseState */

    List* starEnd; /* Corresponding p_star_end in ParseState */

    List* starOnly; /* Corresponding p_star_only in ParseState */

    List* returningList; /* return-values list (of TargetEntry) */

    List* groupClause; /* a list of SortGroupClause's */

    List* groupingSets; /* a list of GroupingSet's if present */

    Node* havingQual; /* qualifications applied to groups */

    List* windowClause; /* a list of WindowClause's */

    List* distinctClause; /* a list of SortGroupClause's */

    List* sortClause; /* a list of SortGroupClause's */

    Node* limitOffset; /* # of result tuples to skip (int8 expr) */
    Node* limitCount;  /* # of result tuples to return (int8 expr) */

    List* rowMarks; /* a list of RowMarkClause's */

    Node* setOperations; /* set-operation tree if this is top level of
                          * a UNION/INTERSECT/EXCEPT query */

    List *constraintDeps; /* a list of pg_constraint OIDs that the query
                           * depends on to be semantically valid */
    HintState* hintState;
#ifdef PGXC
    /* need this info for PGXC Planner, may be temporary */
    char* sql_statement;                 /* original query */
    bool is_local;                       /* enforce query execution on local node
                                          * this is used by EXECUTE DIRECT especially. */
    bool has_to_save_cmd_id;             /* true if the query is such an INSERT SELECT
                                          * that inserts into a child by selecting
                                          * from its parent OR a WITH query that
                                          * updates a table in main query and inserts
                                          * a row to the same table in WITH query */
    bool vec_output;                     /* true if it's vec output. this flag is used in FQS planning	*/
    TdTruncCastStatus tdTruncCastStatus; /* Auto truncation Cast added, only used for stmt in stored procedure or
                                            prepare stmt. */
    List* equalVars;                     /* vars appears in UPDATE/DELETE clause */
#endif
    ParamListInfo boundParamsQ;

    int mergeTarget_relation;
    List* mergeSourceTargetList;
    List* mergeActionList; /* list of actions for MERGE (only) */
    Query* upsertQuery;    /* insert query for INSERT ON DUPLICATE KEY UPDATE (only) */
    UpsertExpr* upsertClause; /* DUPLICATE KEY UPDATE [NOTHING | ...] */

    bool isRowTriggerShippable; /* true if all row triggers are shippable. */
    bool use_star_targets;      /* true if use * for targetlist. */

    bool is_from_full_join_rewrite; /* true if the query is created when doing
                                     * full join rewrite. If true, we should not
                                     * do some expression processing.
                                     * Please refer to subquery_planner.
                                     */
    uint64 uniqueSQLId;             /* used by unique sql id */
    bool can_push;
    bool        unique_check;               /* true if the subquery is generated by general
                                             * sublink pullup, and scalar output is needed */
    Oid* fixed_paramTypes; /* For plpy CTAS query. CTAS is a recursive call.CREATE query is the first rewrited.
                            * thd 2nd rewrited query is INSERT SELECT.whithout this attribute, DB will have
                            * an error that has no idea about $x when INSERT SELECT query is analyzed. */
    int fixed_numParams;
} Query;

/* ----------------------
 * {Begin|Commit|Rollback} Transaction Statement
 * ----------------------
 */

typedef enum TransactionStmtKind {
    TRANS_STMT_BEGIN,
    TRANS_STMT_START, /* semantically identical to BEGIN */
    TRANS_STMT_COMMIT,
    TRANS_STMT_ROLLBACK,
    TRANS_STMT_SAVEPOINT,
    TRANS_STMT_RELEASE,
    TRANS_STMT_ROLLBACK_TO,
    TRANS_STMT_PREPARE,
    TRANS_STMT_COMMIT_PREPARED,
    TRANS_STMT_ROLLBACK_PREPARED
} TransactionStmtKind;

typedef struct TransactionStmt {
    NodeTag type;
    TransactionStmtKind kind; /* see above */
    List *options;            /* for BEGIN/START and savepoint commands */
    char *gid;                /* for two-phase-commit related commands */
    CommitSeqNo csn;          /* for gs_clean two-phase-commit related commands */
} TransactionStmt;
/* ----------------------
 * Create View Statement
 * ----------------------
 */
typedef struct ViewStmt {
    NodeTag type;
    RangeVar *view;      /* the view to be created */
    List *aliases;       /* target column names */
    Node *query;         /* the SELECT query */
    bool replace;        /* replace an existing view? */
    bool ivm;            /* incremental materialized view? */
    List *options;       /* options from WITH clause */
    char *sql_statement; /* used for resize rule, replace the original statement */
    ObjectType relkind;  /* type of object */
    Node* mv_stmt;
    char *mv_sql;
#ifdef ENABLE_MULTIPLE_NODES
    struct PGXCSubCluster* subcluster; /* subcluster of table */
#endif
} ViewStmt;

/* ----------------------
 * 		Merge Statement
 * ----------------------
 */
typedef struct MergeStmt {
    NodeTag type;
    RangeVar *relation;     /* target relation to merge into */
    Node *source_relation;  /* source relation */
    Node *join_condition;   /* join condition between source and target */
    List *mergeWhenClauses; /* list of MergeWhenClause(es) */
    bool is_insert_update;  /* TRUE if the stmt is from INSERT UPDATE */
    Node *insert_stmt;      /* insert stmt from INSERT UPDATE */
    HintState *hintState;
} MergeStmt;

typedef struct MergeWhenClause {
    NodeTag type;
    bool matched;        /* true=MATCHED, false=NOT MATCHED */
    CmdType commandType; /* INSERT/UPDATE/DELETE */
    Node *condition;     /* WHERE conditions (raw parser) */
    List *targetList;    /* INSERT/UPDATE targetlist */
    /* the following members are only useful for INSERT action */
    List *cols;   /* optional: names of the target columns */
    List *values; /* VALUES to INSERT, or NULL */
} MergeWhenClause;

/*
 * WHEN [NOT] MATCHED THEN action info
 */
typedef struct MergeAction {
    NodeTag type;
    bool matched;        /* true=MATCHED, false=NOT MATCHED */
    Node *qual;          /* transformed WHERE conditions */
    CmdType commandType; /* INSERT/UPDATE/DELETE */
    List *targetList;    /* the target list (of ResTarget) */
    /*
     * the replaced targetlist after simple subquery pullup. In stream plan,
     * we don't do the replacement to targetlist and quals, but this pulluped
     * targetlist, and then choose distribute key from this pulluped targetlist
     */
    List *pulluped_targetList;
} MergeAction;

/* PGXC_BEGIN */
typedef enum {
    EXEC_DIRECT_ON_LIST,
    EXEC_DIRECT_ON_ALL_CN,
    EXEC_DIRECT_ON_ALL_DN,
    EXEC_DIRECT_ON_ALL_NODES,
    EXEC_DIRECT_ON_NONE
} ExecDirectOption;
/*
 * EXECUTE DIRECT statement
 */
typedef struct ExecDirectStmt {
    NodeTag type;
    List *node_names;
    ExecDirectOption exec_option;
    char *query;
    int location;
} ExecDirectStmt;

/* ----------------------
 *		Create Function Statement
 * ----------------------
 */
typedef struct CreateFunctionStmt {
    NodeTag type;
    bool isOraStyle;      /* T => a db compatible function or procedure */
    bool replace;         /* T => replace if already exists */
    List* funcname;       /* qualified name of function to create */
    List* parameters;     /* a list of FunctionParameter */
    TypeName* returnType; /* the return type */
    List* options;        /* a list of DefElem */
    List* withClause;     /* a list of DefElem */
    bool isProcedure;     /* true if it is a procedure */
} CreateFunctionStmt;

/* ----------------------
 *		Alter Object Rename Statement
 * ----------------------
 */
typedef struct RenameStmt {
    NodeTag type;
    ObjectType renameType;   /* OBJECT_TABLE, OBJECT_COLUMN, etc */
    ObjectType relationType; /* if column name, associated relation type */
    RangeVar* relation;      /* in case it's a table */
    List* object;            /* in case it's some other object */
    List* objarg;            /* argument types, if applicable */
    char* subname;           /* name of contained object (column, rule,
                              * trigger, etc) */
    char* newname;           /* the new name */
    DropBehavior behavior;   /* RESTRICT or CASCADE behavior */
    bool missing_ok;         /* skip error if missing? */
} RenameStmt;

#endif /* PARSENODES_COMMONH */
