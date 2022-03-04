/* -------------------------------------------------------------------------
 *
 * parsenodes.h
 *	  definitions for parse tree nodes
 *
 * Many of the node types used in parsetrees include a "location" field.
 * This is a byte (not character) offset in the original source text, to be
 * used for positioning an error cursor when there is an error related to
 * the node.  Access to the original source text is needed to make use of
 * the location.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/nodes/parsenodes.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSENODES_H
#define PARSENODES_H

#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#ifdef PGXC
#include "access/tupdesc.h"
#include "pgxc/locator.h"
#endif
#include "tcop/dest.h"
#include "nodes/parsenodes_common.h"

/*
 * Relids
 *              Set of relation identifiers (indexes into the rangetable).
 */
typedef Bitmapset* Relids;

typedef enum RelOrientation {
    REL_ORIENT_UNKNOWN,
    REL_COL_ORIENTED, /* It represents CU sotre foramt also. */
    REL_ROW_ORIENTED,
    REL_PAX_ORIENTED,
    REL_TIMESERIES_ORIENTED
} RelOrientation;

/*
 * It keeps file system which the relatoin store.
 * LOCAL_STORE represents local file system.
 * HDFS_STORE represents Hadoop file system.
 */
typedef enum RelstoreType { LOCAL_STORE, HDFS_STORE } RelstoreType;

#define SAMPLEARGSNUM 2
/* Method of tablesample */
typedef enum TableSampleType { SYSTEM_SAMPLE = 0, BERNOULLI_SAMPLE, HYBRID_SAMPLE } TableSampleType;

/*
 * Grantable rights are encoded so that we can OR them together in a bitmask.
 * The present representation of AclItem limits us to 16 distinct rights,
 * even though AclMode is defined as uint32.  See utils/acl.h.
 *
 * Caution: changing these codes breaks stored ACLs, hence forces initdb.
 */
typedef uint32 AclMode; /* a bitmask of privilege bits */

#define ACL_INSERT (1 << 0) /* for relations */
#define ACL_SELECT (1 << 1)
#define ACL_UPDATE (1 << 2)
#define ACL_DELETE (1 << 3)
#define ACL_TRUNCATE (1 << 4)
#define ACL_REFERENCES (1 << 5)
#define ACL_TRIGGER (1 << 6)
#define ACL_EXECUTE (1 << 7) /* for functions */
#define ACL_USAGE                                                         \
    (1 << 8)                      /* for languages, namespaces, FDWs, and \
                                   * servers */
#define ACL_CREATE (1 << 9)       /* for namespaces and databases */
#define ACL_CREATE_TEMP (1 << 10) /* for databases */
#define ACL_CONNECT (1 << 11)     /* for databases */
#define ACL_COMPUTE (1 << 12)     /* for node group */
#define ACL_READ (1 << 13)        /* for pg_directory */
#define ACL_WRITE (1 << 14)       /* for pg_directory */
#define N_ACL_RIGHTS 15           /* 1 plus the last 1<<x */
#define ACL_NO_RIGHTS 0
/* Currently, SELECT ... FOR [KEY] UPDATE/FOR SHARE requires UPDATE privileges */
#define ACL_SELECT_FOR_UPDATE ACL_UPDATE

/* grantable rights for DDL operations */
#define ACL_ALTER ((1 << 0) | (1 << 15))    /* for all objects */
#define ACL_DROP ((1 << 1) | (1 << 15))     /* for all objects */
#define ACL_COMMENT ((1 << 2) | (1 << 15))  /* for all objects */
#define ACL_INDEX ((1 << 3) | (1 << 15))    /* for relations */
#define ACL_VACUUM ((1 << 4) | (1 << 15))   /* for relations */

#define N_ACL_DDL_RIGHTS 5           /* 1 plus the last 1<<x */
#define FLAG_FOR_DDL_ACL (1 << 15)  /* add this flag to Aclitem when for grantable rights of DDL operations */
#define ACL_NO_DDL_RIGHTS ((AclMode)FLAG_FOR_DDL_ACL & 0xFFFF)

/****************************************************************************
 *	Supporting data structures for Parse Trees
 *
 *	Most of these node types appear in raw parsetrees output by the grammar,
 *	and get transformed to something else by the analyzer.	A few of them
 *	are used as-is in transformed querytrees.
 ****************************************************************************/

/*
 * TableSampleClause - TABLESAMPLE appearing in a transformed FROM clause
 *
 * Unlike RangeTableSample, this is a subnode of the relevant RangeTblEntry.
 */
typedef struct TableSampleClause {
    NodeTag type;
    TableSampleType sampleType; /* Sample type, system or bernoulli. */
    List* args;                 /* tablesample argument expression(s) */
    Expr* repeatable;           /* REPEATABLE expression, or NULL if none */
} TableSampleClause;


/****************************************************************************
 *	Nodes for a Query tree
 ****************************************************************************/

/* --------------------
 * RangeTblEntry -
 *	  A range table is a List of RangeTblEntry nodes.
 *
 *	  A range table entry may represent a plain relation, a sub-select in
 *	  FROM, or the result of a JOIN clause.  (Only explicit JOIN syntax
 *	  produces an RTE, not the implicit join resulting from multiple FROM
 *	  items.  This is because we only need the RTE to deal with SQL features
 *	  like outer joins and join-output-column aliasing.)  Other special
 *	  RTE types also exist, as indicated by RTEKind.
 *
 *	  Note that we consider RTE_RELATION to cover anything that has a pg_class
 *	  entry.  relkind distinguishes the sub-cases.
 *
 *	  alias is an Alias node representing the AS alias-clause attached to the
 *	  FROM expression, or NULL if no clause.
 *
 *	  eref is the table reference name and column reference names (either
 *	  real or aliases).  Note that system columns (OID etc) are not included
 *	  in the column list.
 *	  eref->aliasname is required to be present, and should generally be used
 *	  to identify the RTE for error messages etc.
 *
 *	  In RELATION RTEs, the colnames in both alias and eref are indexed by
 *	  physical attribute number; this means there must be colname entries for
 *	  dropped columns.	When building an RTE we insert empty strings ("") for
 *	  dropped columns.	Note however that a stored rule may have nonempty
 *	  colnames for columns dropped since the rule was created (and for that
 *	  matter the colnames might be out of date due to column renamings).
 *	  The same comments apply to FUNCTION RTEs when the function's return type
 *	  is a named composite type.
 *
 *	  In JOIN RTEs, the colnames in both alias and eref are one-to-one with
 *	  joinaliasvars entries.  A JOIN RTE will omit columns of its inputs when
 *	  those columns are known to be dropped at parse time.	Again, however,
 *	  a stored rule might contain entries for columns dropped since the rule
 *	  was created.	(This is only possible for columns not actually referenced
 *	  in the rule.)  When loading a stored rule, we replace the joinaliasvars
 *	  items for any such columns with NULL Consts.	(We can't simply delete
 *	  them from the joinaliasvars list, because that would affect the attnums
 *	  of Vars referencing the rest of the list.)
 *
 *	  inh is TRUE for relation references that should be expanded to include
 *	  inheritance children, if the rel has any.  This *must* be FALSE for
 *	  RTEs other than RTE_RELATION entries.
 *
 *	  inFromCl marks those range variables that are listed in the FROM clause.
 *	  It's false for RTEs that are added to a query behind the scenes, such
 *	  as the NEW and OLD variables for a rule, or the subqueries of a UNION.
 *	  This flag is not used anymore during parsing, since the parser now uses
 *	  a separate "namespace" data structure to control visibility, but it is
 *	  needed by ruleutils.c to determine whether RTEs should be shown in
 *	  decompiled queries.
 *
 *	  requiredPerms and checkAsUser specify run-time access permissions
 *	  checks to be performed at query startup.	The user must have *all*
 *	  of the permissions that are OR'd together in requiredPerms (zero
 *	  indicates no permissions checking).  If checkAsUser is not zero,
 *	  then do the permissions checks using the access rights of that user,
 *	  not the current effective user ID.  (This allows rules to act as
 *	  setuid gateways.)  Permissions checks only apply to RELATION RTEs.
 *
 *	  For SELECT/INSERT/UPDATE permissions, if the user doesn't have
 *	  table-wide permissions then it is sufficient to have the permissions
 *	  on all columns identified in selectedCols (for SELECT) and/or
 *	  modifiedCols (for INSERT/UPDATE; we can tell which from the query type).
 *	  selectedCols and modifiedCols are bitmapsets, which cannot have negative
 *	  integer members, so we subtract FirstLowInvalidHeapAttributeNumber from
 *	  column numbers before storing them in these fields.  A whole-row Var
 *	  reference is represented by setting the bit for InvalidAttrNumber.
 * --------------------
 */
typedef enum RTEKind {
    RTE_RELATION, /* ordinary relation reference */
    RTE_SUBQUERY, /* subquery in FROM */
    RTE_JOIN,     /* join */
    RTE_FUNCTION, /* function in FROM */
    RTE_VALUES,   /* VALUES (<exprlist>), (<exprlist>), ... */
    RTE_CTE,       /* common table expr (WITH list element) */
#ifdef PGXC
    RTE_REMOTE_DUMMY, /* RTEs created by remote plan reduction */
#endif               /* PGXC */
    RTE_RESULT       /* RTE represents an empty FROM clause; such
                      * RTEs are added by the planner, they're not
                      * present during parsing or rewriting */
} RTEKind;

typedef struct RangeTblEntry {
    NodeTag type;

    RTEKind rtekind; /* see above */

    /*
     * XXX the fields applicable to only some rte kinds should be merged into
     * a union.  I didn't do this yet because the diffs would impact a lot of
     * code that is being actively worked on.
     */

#ifdef PGXC
    char* relname;
    List* partAttrNum;
#endif

    /*
     * Fields valid for a plain relation RTE (else zero):
     */
    Oid relid;                  /* OID of the relation */
    Oid partitionOid;           /*
                                 * OID of a partition if relation is partitioned table.
                                 * Select * from table_name partition (partition_name);
                                 * or select * from table_name partition for (partition_key_value_list)
                                 */
    bool isContainPartition;    /* select from caluse whether contains partition
                                 * if contains partition isContainPartition=true,
                                 * otherwise isContainPartition=false
                                 */
    Oid subpartitionOid;        /*
                                 * OID of a subpartition if relation is partitioned table.
                                 * Select * from table_name subpartition (subpartition_name);
                                 */
    bool isContainSubPartition; /* select from caluse whether contains subpartition
                                 * if contains subpartition isContainSubPartition=true,
                                 * otherwise isContainSubPartition=false
                                 */
    Oid refSynOid;           /* OID of synonym object if relation is referenced by some synonym object. */
    List* partid_list;

    char relkind;                   /* relation kind (see pg_class.relkind) */
    bool isResultRel;       /* used in target of SELECT INTO or similar */
    TableSampleClause* tablesample; /* sampling method and parameters */
    TimeCapsuleClause* timecapsule; /* user-specified time capsule point */

    bool ispartrel; /* is it a partitioned table */

    /* works just for _readRangeTblEntry(). set true if plan is running in the compute pool. */
    bool ignoreResetRelid;

    /*
     * Fields valid for a subquery RTE (else NULL):
     */
    Query* subquery;       /* the sub-query */
    bool security_barrier; /* subquery from security_barrier view */

    /*
     * Fields valid for a join RTE (else NULL/zero):
     *
     * joinaliasvars is a list of Vars or COALESCE expressions corresponding
     * to the columns of the join result.  An alias Var referencing column K
     * of the join result can be replaced by the K'th element of joinaliasvars
     * --- but to simplify the task of reverse-listing aliases correctly, we
     * do not do that until planning time.	In a Query loaded from a stored
     * rule, it is also possible for joinaliasvars items to be NULL Consts,
     * denoting columns dropped since the rule was made.
     */
    JoinType jointype;   /* type of join */
    List* joinaliasvars; /* list of alias-var expansions */

    /*
     * Fields valid for a function RTE (else NULL):
     *
     * If the function returns RECORD, funccoltypes lists the column types
     * declared in the RTE's column type specification, funccoltypmods lists
     * their declared typmods, funccolcollations their collations.	Otherwise,
     * those fields are NIL.
     */
    Node* funcexpr;          /* expression tree for func call */
    List* funccoltypes;      /* OID list of column type OIDs */
    List* funccoltypmods;    /* integer list of column typmods */
    List* funccolcollations; /* OID list of column collation OIDs */

    /*
     * Fields valid for a values RTE (else NIL):
     */
    List* values_lists;      /* list of expression lists */
    List* values_collations; /* OID list of column collation OIDs */

    /*
     * Fields valid for a CTE RTE (else NULL/zero):
     */
    char* ctename;          /* name of the WITH list item */
    Index ctelevelsup;      /* number of query levels up */
    bool self_reference;    /* is this a recursive self-reference? */
    bool cterecursive;      /* is this a recursive cte */
    List* ctecoltypes;      /* OID list of column type OIDs */
    List* ctecoltypmods;    /* integer list of column typmods */
    List* ctecolcollations; /* OID list of column collation OIDs */
    bool  swConverted;      /* indicate the current CTE rangetable entry is converted
                               from StartWith ... Connect By clause */
    List *origin_index;     /* rewrite rtes of cte for startwith */
    bool swAborted;         /* RTE has been replaced by CTE */
    bool swSubExist;         /* under subquery contains startwith */
    char locator_type;      /*      keep subplan/cte's locator type */

    /*
     * Fields valid in all RTEs:
     */
    Alias* alias; /* user-written alias clause, if any */
    Alias* eref;  /* expanded reference names */
    Alias* pname; /* partition name, if any */
    List* plist;
    bool lateral;               /* subquery or function is marked LATERAL? */
    bool inh;                   /* inheritance requested? */
    bool inFromCl;              /* present in FROM clause? */
    AclMode requiredPerms;      /* bitmask of required access permissions */
    Oid checkAsUser;            /* if valid, check access as this role */
    Bitmapset* selectedCols;    /* columns needing SELECT permission */
    Bitmapset* modifiedCols;    /* columns needing INSERT/UPDATE permission, not used in current version
                                 * we split it to insertedCols and updatedCols for MERGEINTO
                                 */
    Bitmapset* insertedCols;    /* columns needing INSERT permission */
    Bitmapset* updatedCols;     /* columns needing UPDATE permission */
    RelOrientation orientation; /* column oriented or row oriented */
    bool is_ustore;             /* is a ustore rel */

    char* mainRelName;
    char* mainRelNameSpace;
    List* securityQuals; /* security barrier quals to apply */

    /* For skew hint */
    bool subquery_pull_up; /* mark that the subquery whether been pull up */

    /*
     * Indicate current RTE is correlated with a Recursive CTE, the flag is set in
     * check_plan_correlation()
     */
    bool correlated_with_recursive_cte;
    /* For hash buckets */
    bool relhasbucket; /* the rel has underlying buckets, get from pg_class */
    bool isbucket;	  /* the sql only want some buckets from the rel */
    int  bucketmapsize;
    List* buckets;	  /* the bucket id wanted */

    bool isexcluded; /* the rel is the EXCLUDED relation for UPSERT */
    /* For sublink in targetlist pull up */
    bool sublink_pull_up;       /* mark the subquery is sublink pulled up */
    Bitmapset *extraUpdatedCols; /* generated columns being updated */
    bool pulled_from_subquery; /* mark whether it is pulled-up from subquery to the current level, for upsert remote
                                  query deparse */
} RangeTblEntry;

/*
 * SortGroupClause -
 *		representation of ORDER BY, GROUP BY, PARTITION BY,
 *		DISTINCT, DISTINCT ON items
 *
 * You might think that ORDER BY is only interested in defining ordering,
 * and GROUP/DISTINCT are only interested in defining equality.  However,
 * one way to implement grouping is to sort and then apply a "uniq"-like
 * filter.	So it's also interesting to keep track of possible sort operators
 * for GROUP/DISTINCT, and in particular to try to sort for the grouping
 * in a way that will also yield a requested ORDER BY ordering.  So we need
 * to be able to compare ORDER BY and GROUP/DISTINCT lists, which motivates
 * the decision to give them the same representation.
 *
 * tleSortGroupRef must match ressortgroupref of exactly one entry of the
 *		query's targetlist; that is the expression to be sorted or grouped by.
 * eqop is the OID of the equality operator.
 * sortop is the OID of the ordering operator (a "<" or ">" operator),
 *		or InvalidOid if not available.
 * nulls_first means about what you'd expect.  If sortop is InvalidOid
 *		then nulls_first is meaningless and should be set to false.
 * hashable is TRUE if eqop is hashable (note this condition also depends
 *		on the datatype of the input expression).
 *
 * In an ORDER BY item, all fields must be valid.  (The eqop isn't essential
 * here, but it's cheap to get it along with the sortop, and requiring it
 * to be valid eases comparisons to grouping items.)  Note that this isn't
 * actually enough information to determine an ordering: if the sortop is
 * collation-sensitive, a collation OID is needed too.	We don't store the
 * collation in SortGroupClause because it's not available at the time the
 * parser builds the SortGroupClause; instead, consult the exposed collation
 * of the referenced targetlist expression to find out what it is.
 *
 * In a grouping item, eqop must be valid.	If the eqop is a btree equality
 * operator, then sortop should be set to a compatible ordering operator.
 * We prefer to set eqop/sortop/nulls_first to match any ORDER BY item that
 * the query presents for the same tlist item.	If there is none, we just
 * use the default ordering op for the datatype.
 *
 * If the tlist item's type has a hash opclass but no btree opclass, then
 * we will set eqop to the hash equality operator, sortop to InvalidOid,
 * and nulls_first to false.  A grouping item of this kind can only be
 * implemented by hashing, and of course it'll never match an ORDER BY item.
 *
 * The hashable flag is provided since we generally have the requisite
 * information readily available when the SortGroupClause is constructed,
 * and it's relatively expensive to get it again later.  Note there is no
 * need for a "sortable" flag since OidIsValid(sortop) serves the purpose.
 *
 * A query might have both ORDER BY and DISTINCT (or DISTINCT ON) clauses.
 * In SELECT DISTINCT, the distinctClause list is as long or longer than the
 * sortClause list, while in SELECT DISTINCT ON it's typically shorter.
 * The two lists must match up to the end of the shorter one --- the parser
 * rearranges the distinctClause if necessary to make this true.  (This
 * restriction ensures that only one sort step is needed to both satisfy the
 * ORDER BY and set up for the Unique step.  This is semantically necessary
 * for DISTINCT ON, and presents no real drawback for DISTINCT.)
 */
typedef struct SortGroupClause {
    NodeTag type;
    Index tleSortGroupRef; /* reference into targetlist */
    Oid eqop;              /* the equality operator ('=' op) */
    Oid sortop;            /* the ordering operator ('<' op), or 0 */
    bool nulls_first;      /* do NULLs come before normal values? */
    bool hashable;         /* can eqop be implemented by hashing? */
    bool groupSet;         /* It will be set to true If this expr in group clause and not include
                              in all group clause when groupingSet is not null. It mean it's value can be altered. */
} SortGroupClause;

/*
 * WindowClause -
 *		transformed representation of WINDOW and OVER clauses
 *
 * A parsed Query's windowClause list contains these structs.  "name" is set
 * if the clause originally came from WINDOW, and is NULL if it originally
 * was an OVER clause (but note that we collapse out duplicate OVERs).
 * partitionClause and orderClause are lists of SortGroupClause structs.
 * winref is an ID number referenced by WindowFunc nodes; it must be unique
 * among the members of a Query's windowClause list.
 * When refname isn't null, the partitionClause is always copied from there;
 * the orderClause might or might not be copied (see copiedOrder); the framing
 * options are never copied, per spec.
 */
typedef struct WindowClause {
    NodeTag type;
    char* name;            /* window name (NULL in an OVER clause) */
    char* refname;         /* referenced window name, if any */
    List* partitionClause; /* PARTITION BY list */
    List* orderClause;     /* ORDER BY list */
    int frameOptions;      /* frame_clause options, see WindowDef */
    Node* startOffset;     /* expression for starting bound, if any */
    Node* endOffset;       /* expression for ending bound, if any */
    Index winref;          /* ID referenced by window functions */
    bool copiedOrder;      /* did we copy orderClause from refname? */
} WindowClause;

/*
 * RowMarkClause -
 *	   parser output representation of FOR [KEY] UPDATE/SHARE clauses
 *
 * Query.rowMarks contains a separate RowMarkClause node for each relation
 * identified as a FOR [KEY] UPDATE/SHARE target.  If one of these clauses
 * is applied to a subquery, we generate RowMarkClauses for all normal and
 * subquery rels in the subquery, but they are marked pushedDown = true to
 * distinguish them from clauses that were explicitly written at this query
 * level.  Also, Query.hasForUpdate tells whether there were explicit FOR
 * UPDATE/SHARE/KEY SHARE clauses in the current query level
 */
typedef struct RowMarkClause {
    NodeTag type;
    Index rti;       /* range table index of target relation */
    bool forUpdate;  /* for compatibility, we reserve this filed but don't use it */
    bool noWait;     /* NOWAIT option */
    int waitSec;      /* WAIT time Sec */
    bool pushedDown; /* pushed down from higher query level? */
    LockClauseStrength strength;
} RowMarkClause;

/*
 * - Brief: data structure in parse state to save StartWith clause in current subquery
 *          level, normally a StartWithTargetRelInfo indicates a RangeVar(rte-rel) e.g.
 *          baserel, subselect where we add StartWith transform needed information to
 *          construct a start-with clause
 */
typedef struct StartWithTargetRelInfo {
    NodeTag type;

    /* fields to describe original relation info */
    char *relname;
    char *aliasname;
    char *ctename;
    List *columns;
    Node *tblstmt;

	/* fields to record origin RTE related info */
    RTEKind rtekind;
    RangeTblEntry *rte;
    RangeTblRef* rtr;
} StartWithTargetRelInfo;

/* Convenience macro to get the output tlist of a CTE's query */
#define GetCTETargetList(cte)                                                                        \
    (AssertMacro(IsA((cte)->ctequery, Query)),                                                       \
        ((Query*)(cte)->ctequery)->commandType == CMD_SELECT ? ((Query*)(cte)->ctequery)->targetList \
                                                             : ((Query*)(cte)->ctequery)->returningList)

/*****************************************************************************
 *		Optimizable Statements
 *****************************************************************************/


/* ----------------------
 *		Set Operation node for post-analysis query trees
 *
 * After parse analysis, a SELECT with set operations is represented by a
 * top-level Query node containing the leaf SELECTs as subqueries in its
 * range table.  Its setOperations field shows the tree of set operations,
 * with leaf SelectStmt nodes replaced by RangeTblRef nodes, and internal
 * nodes replaced by SetOperationStmt nodes.  Information about the output
 * column types is added, too.	(Note that the child nodes do not necessarily
 * produce these types directly, but we've checked that their output types
 * can be coerced to the output column type.)  Also, if it's not UNION ALL,
 * information about the types' sort/group semantics is provided in the form
 * of a SortGroupClause list (same representation as, eg, DISTINCT).
 * The resolved common column collations are provided too; but note that if
 * it's not UNION ALL, it's okay for a column to not have a common collation,
 * so a member of the colCollations list could be InvalidOid even though the
 * column has a collatable type.
 * ----------------------
 */
typedef struct SetOperationStmt {
    NodeTag type;
    SetOperation op; /* type of set op */
    bool all;        /* ALL specified? */
    Node* larg;      /* left child */
    Node* rarg;      /* right child */
    /* Eventually add fields for CORRESPONDING spec here */

    /* Fields derived during parse analysis: */
    List* colTypes;      /* OID list of output column type OIDs */
    List* colTypmods;    /* integer list of output column typmods */
    List* colCollations; /* OID list of output column collation OIDs */
    List* groupClauses;  /* a list of SortGroupClause's */
                         /* groupClauses is NIL if UNION ALL, but must be set otherwise */
} SetOperationStmt;

/*****************************************************************************
 *		Other Statements (no optimizations required)
 *
 *		These are not touched by parser/analyze.c except to put them into
 *		the utilityStmt field of a Query.  This is eventually passed to
 *		ProcessUtility (by-passing rewriting and planning).  Some of the
 *		statements do need attention from parse analysis, and this is
 *		done by routines in parser/parse_utilcmd.c after ProcessUtility
 *		receives the command for execution.
 *****************************************************************************/

/* ----------------------
 *	Alter Domain
 *
 * The fields are used in different ways by the different variants of
 * this command.
 * ----------------------
 */
typedef struct AlterDomainStmt {
    NodeTag type;
    char subtype;          /*------------
                            *	T = alter column default
                            *	N = alter column drop not null
                            *	O = alter column set not null
                            *	C = add constraint
                            *	X = drop constraint
                            * ------------
                            */
    List* typname;         /* domain to work on */
    char* name;            /* column or constraint name to act on */
    Node* def;             /* definition of default or constraint */
    DropBehavior behavior; /* RESTRICT or CASCADE for DROP cases */
    bool missing_ok;       /* skip error if missing? */
} AlterDomainStmt;

/* ----------------------
 *		Grant|Revoke Statement
 * ----------------------
 */
typedef enum GrantTargetType {
    ACL_TARGET_OBJECT,        /* grant on specific named object(s) */
    ACL_TARGET_ALL_IN_SCHEMA, /* grant on all objects in given schema(s) */
    ACL_TARGET_DEFAULTS       /* ALTER DEFAULT PRIVILEGES */
} GrantTargetType;

typedef enum GrantObjectType {
    ACL_OBJECT_COLUMN,         /* column */
    ACL_OBJECT_RELATION,       /* table, view */
    ACL_OBJECT_SEQUENCE,       /* sequence */
    ACL_OBJECT_DATABASE,       /* database */
    ACL_OBJECT_DOMAIN,         /* domain */
    ACL_OBJECT_FDW,            /* foreign-data wrapper */
    ACL_OBJECT_FOREIGN_SERVER, /* foreign server */
    ACL_OBJECT_FUNCTION,       /* function */
    ACL_OBJECT_PACKAGE,        /* package */
    ACL_OBJECT_LANGUAGE,       /* procedural language */
    ACL_OBJECT_LARGEOBJECT,    /* largeobject */
    ACL_OBJECT_NAMESPACE,      /* namespace */
    ACL_OBJECT_NODEGROUP,      /* nodegroup */
    ACL_OBJECT_TABLESPACE,     /* tablespace */
    ACL_OBJECT_TYPE,           /* type */
    ACL_OBJECT_DATA_SOURCE,    /* data source */
    ACL_OBJECT_GLOBAL_SETTING, /* Global Setting */
    ACL_OBJECT_COLUMN_SETTING, /* Column Setting */
    ACL_OBJECT_DIRECTORY       /* directory */
} GrantObjectType;

typedef struct GrantStmt {
    NodeTag type;
    bool is_grant;            /* true = GRANT, false = REVOKE */
    GrantTargetType targtype; /* type of the grant target */
    GrantObjectType objtype;  /* kind of object being operated on */
    List* objects;            /* list of RangeVar nodes, FuncWithArgs nodes,
                               * or plain names (as Value strings) */
    List* privileges;         /* list of AccessPriv nodes */
    /* privileges == NIL denotes ALL PRIVILEGES */
    List* grantees;        /* list of PrivGrantee nodes */
    bool grant_option;     /* grant or revoke grant option */
    DropBehavior behavior; /* drop behavior (for REVOKE) */
} GrantStmt;

typedef struct PrivGrantee {
    NodeTag type;
    char* rolname; /* if NULL then PUBLIC */
} PrivGrantee;

/*
 * An access privilege, with optional list of column names
 * priv_name == NULL denotes ALL PRIVILEGES (only used with a column list)
 * cols == NIL denotes "all columns"
 * Note that simple "ALL PRIVILEGES" is represented as a NIL list, not
 * an AccessPriv with both fields null.
 */
typedef struct AccessPriv {
    NodeTag type;
    char* priv_name; /* string name of privilege */
    List* cols;      /* list of Value strings */
} AccessPriv;

/* ----------------------
 *		Grant/Revoke Role Statement
 *
 * Note: because of the parsing ambiguity with the GRANT <privileges>
 * statement, granted_roles is a list of AccessPriv; the execution code
 * should complain if any column lists appear.	grantee_roles is a list
 * of role names, as Value strings.
 * ----------------------
 */
typedef struct GrantRoleStmt {
    NodeTag type;
    List* granted_roles;   /* list of roles to be granted/revoked */
    List* grantee_roles;   /* list of member roles to add/delete */
    bool is_grant;         /* true = GRANT, false = REVOKE */
    bool admin_opt;        /* with admin option */
    char* grantor;         /* set grantor to other than current role */
    DropBehavior behavior; /* drop behavior (for REVOKE) */
} GrantRoleStmt;

/* ----------------------
 * Grant/Revoke Database Privilege Statement
 * ----------------------
 */
typedef struct GrantDbStmt {
    NodeTag type;
    bool is_grant;       /* true = GRANT, false = REVOKE */
    bool admin_opt;      /* with admin option */
    List* privileges;    /* list of DbPriv nodes */
    List* grantees;      /* list of PrivGrantee nodes */
} GrantDbStmt;

typedef struct DbPriv {
    NodeTag type;
    char* db_priv_name;    /* string name of sys privilege */
} DbPriv;

/* ----------------------
 *	Alter Default Privileges Statement
 * ----------------------
 */
typedef struct AlterDefaultPrivilegesStmt {
    NodeTag type;
    List* options;     /* list of DefElem */
    GrantStmt* action; /* GRANT/REVOKE action (with objects=NIL) */
} AlterDefaultPrivilegesStmt;

/* ----------------------
 * Show Statement
 * ----------------------
 */
typedef struct VariableShowStmt {
    NodeTag type;
    char* name;
    char* likename;
} VariableShowStmt;

/* ----------------------
 * Shutdown Statement
 * ----------------------
 */
typedef struct ShutdownStmt {
    NodeTag type;
    char* mode;
} ShutdownStmt;

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
 * for the constraint.	A new FK constraint is marked as valid iff
 * initially_valid is true.  (Usually skip_validation and initially_valid
 * are inverses, but we can set both true if the table is known empty.)
 *
 * Constraint attributes (DEFERRABLE etc) are initially represented as
 * separate Constraint nodes for simplicity of parsing.  parse_utilcmd.c makes
 * a pass through the constraints list to insert the info into the appropriate
 * Constraint node.
 * ----------
 */

#define GetConstraintType(type)                \
    ({                                         \
        const char* tname = NULL;              \
        switch (type) {                        \
            case CONSTR_NULL:                  \
                tname = "NULL";                \
                break;                         \
            case CONSTR_NOTNULL:               \
                tname = "NOT NULL";            \
                break;                         \
            case CONSTR_DEFAULT:               \
                tname = "DEFAULT";             \
                break;                         \
            case CONSTR_CHECK:                 \
                tname = "CHECK";               \
                break;                         \
            case CONSTR_PRIMARY:               \
                tname = "PRIMARY KEY";         \
                break;                         \
            case CONSTR_UNIQUE:                \
                tname = "UNIQUE";              \
                break;                         \
            case CONSTR_EXCLUSION:             \
                tname = "EXCLUSION";           \
                break;                         \
            case CONSTR_FOREIGN:               \
                tname = "FOREIGN KEY";         \
                break;                         \
            case CONSTR_CLUSTER:               \
                tname = "CLUSTER";             \
                break;                         \
            case CONSTR_ATTR_DEFERRABLE:       \
                tname = "ATTR DEFERRABLE";     \
                break;                         \
            case CONSTR_ATTR_NOT_DEFERRABLE:   \
                tname = "ATTR NOT DEFERRABLE"; \
                break;                         \
            case CONSTR_ATTR_DEFERRED:         \
                tname = "ATTR DEFERRED";       \
                break;                         \
            case CONSTR_ATTR_IMMEDIATE:        \
                tname = "ATTR IMMEDIATE";      \
                break;                         \
            case CONSTR_GENERATED:             \
                tname = "GENERATED COL";       \
                break;                         \
        }                                      \
        tname;                                 \
    })

/* ----------------------
 *		Create/Drop Table Space Statements
 * ----------------------
 */
typedef struct CreateTableSpaceStmt {
    NodeTag type;
    char* tablespacename;
    char* owner;
    char* location;
    char* maxsize;
    List* options;
    bool relative; /* location is relative to data directory */
} CreateTableSpaceStmt;

typedef struct DropTableSpaceStmt {
    NodeTag type;
    char* tablespacename;
    bool missing_ok; /* skip error if missing? */
} DropTableSpaceStmt;

typedef struct AlterTableSpaceOptionsStmt {
    NodeTag type;
    char* tablespacename;
    char* maxsize;
    List* options;
    bool isReset;
} AlterTableSpaceOptionsStmt;

/* ----------------------
 *		Create/Alter Extension Statements
 * ----------------------
 */
typedef struct CreateExtensionStmt {
    NodeTag type;
    char* extname;
    bool if_not_exists; /* just do nothing if it already exists? */
    List* options;      /* List of DefElem nodes */
} CreateExtensionStmt;

/* Only used for ALTER EXTENSION UPDATE; later might need an action field */
typedef struct AlterExtensionStmt {
    NodeTag type;
    char* extname;
    List* options; /* List of DefElem nodes */
} AlterExtensionStmt;

typedef struct AlterExtensionContentsStmt {
    NodeTag type;
    char* extname;      /* Extension's name */
    int action;         /* +1 = add object, -1 = drop object */
    ObjectType objtype; /* Object's type */
    List* objname;      /* Qualified name of the object */
    List* objargs;      /* Arguments if needed (eg, for functions) */
} AlterExtensionContentsStmt;

/* ----------------------
 *		Create/Alter FOREIGN DATA WRAPPER Statements
 * ----------------------
 */
typedef struct CreateFdwStmt {
    NodeTag type;
    char* fdwname;      /* foreign-data wrapper name */
    List* func_options; /* HANDLER/VALIDATOR options */
    List* options;      /* generic options to FDW */
} CreateFdwStmt;

typedef struct AlterFdwStmt {
    NodeTag type;
    char* fdwname;      /* foreign-data wrapper name */
    List* func_options; /* HANDLER/VALIDATOR options */
    List* options;      /* generic options to FDW */
} AlterFdwStmt;

/* ----------------------
 *		Create Weak Password Statements
 * ----------------------
 */
typedef struct CreateWeakPasswordDictionaryStmt {
    NodeTag type;
    List* weak_password_string_list;
} CreateWeakPasswordDictionaryStmt;

/* ----------------------
 *		Drop Weak Password Statements
 * ----------------------
 */
typedef struct DropWeakPasswordDictionaryStmt {
    NodeTag type;
} DropWeakPasswordDictionaryStmt;

/* ----------------------
 *		Create/Alter FOREIGN SERVER Statements
 * ----------------------
 */
typedef struct CreateForeignServerStmt {
    NodeTag type;
    char* servername; /* server name */
    char* servertype; /* optional server type */
    char* version;    /* optional server version */
    char* fdwname;    /* FDW name */
    List* options;    /* generic options to server */
} CreateForeignServerStmt;

typedef struct AlterForeignServerStmt {
    NodeTag type;
    char* servername; /* server name */
    char* version;    /* optional server version */
    List* options;    /* generic options to server */
    bool has_version; /* version specified */
} AlterForeignServerStmt;

/* ----------------------
 *		Create FOREIGN TABLE Statements
 * ----------------------
 */
typedef struct ForeignPartState {
    NodeTag type;

    /* partition key of partitioned table , which is list of ColumnRef */
    List* partitionKey;
} ForeignPartState;

struct DistributeBy;

typedef struct CreateForeignTableStmt {
    CreateStmt base;
    char* servername;
    Node* error_relation;
    List* options;
    List* extOptions;
#ifdef PGXC
    DistributeBy* distributeby;
#endif
    bool write_only;
    ForeignPartState* part_state;
} CreateForeignTableStmt;

/* ----------------------
 *		Create/Drop USER MAPPING Statements
 * ----------------------
 */
typedef struct CreateUserMappingStmt {
    NodeTag type;
    char* username;   /* username or PUBLIC/CURRENT_USER */
    char* servername; /* server name */
    List* options;    /* generic options to server */
} CreateUserMappingStmt;

typedef struct AlterUserMappingStmt {
    NodeTag type;
    char* username;   /* username or PUBLIC/CURRENT_USER */
    char* servername; /* server name */
    List* options;    /* generic options to server */
} AlterUserMappingStmt;

typedef struct DropUserMappingStmt {
    NodeTag type;
    char* username;   /* username or PUBLIC/CURRENT_USER */
    char* servername; /* server name */
    bool missing_ok;  /* ignore missing mappings */
} DropUserMappingStmt;

/* ----------------------
 *      Create Synonym Statement
 * ----------------------
 */
typedef struct CreateSynonymStmt {
    NodeTag type;
    bool replace;  /* T => replace if already exists */
    List* synName; /* qualified name of synonym */
    List* objName; /* referenced object name, eg. relation, view, function, procedure. */
} CreateSynonymStmt;

/* ----------------------
 *      Drop Synonym Statement
 * ----------------------
 */
typedef struct DropSynonymStmt {
    NodeTag type;
    List* synName;         /* qualified name of synonym */
    DropBehavior behavior; /* RESTRICT or CASCADE behavior */
    bool missing;          /* skip error if a synonym is missing? */
} DropSynonymStmt;

/* ----------------------
 *		Create/Alter DATA SOURCE Statements
 * ----------------------
 */
typedef struct CreateDataSourceStmt {
    NodeTag type;
    char* srcname; /* source name */
    char* srctype; /* optional source type */
    char* version; /* optional source version */
    List* options; /* generic options to server */
} CreateDataSourceStmt;

typedef struct AlterDataSourceStmt {
    NodeTag type;
    char* srcname;    /* source name */
    char* srctype;    /* optional source type */
    char* version;    /* optional source version */
    List* options;    /* generic options to source */
    bool has_version; /* version specified */
} AlterDataSourceStmt;

/* ----------------------
 *		Create TRIGGER Statement
 * ----------------------
 */
typedef struct CreateTrigStmt {
    NodeTag type;
    char* trigname;     /* TRIGGER's name */
    RangeVar* relation; /* relation trigger is on */
    List* funcname;     /* qual. name of function to call */
    List* args;         /* list of (T_String) Values or NIL */
    bool row;           /* ROW/STATEMENT */
    /* timing uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
    int16 timing; /* BEFORE, AFTER, or INSTEAD */
    /* events uses the TRIGGER_TYPE bits defined in catalog/pg_trigger.h */
    int16 events;      /* "OR" of INSERT/UPDATE/DELETE/TRUNCATE */
    List* columns;     /* column names, or NIL for all columns */
    Node* whenClause;  /* qual expression, or NULL if none */
    bool isconstraint; /* This is a constraint trigger */
    /* The remaining fields are only used for constraint triggers */
    bool deferrable;     /* [NOT] DEFERRABLE */
    bool initdeferred;   /* INITIALLY {DEFERRED|IMMEDIATE} */
    RangeVar* constrrel; /* opposite relation, if RI trigger */
} CreateTrigStmt;

/* ----------------------
 *		Create PROCEDURAL LANGUAGE Statements
 * ----------------------
 */
typedef struct CreatePLangStmt {
    NodeTag type;
    bool replace;      /* T => replace if already exists */
    char* plname;      /* PL name */
    List* plhandler;   /* PL call handler function (qual. name) */
    List* plinline;    /* optional inline function (qual. name) */
    List* plvalidator; /* optional validator function (qual. name) */
    bool pltrusted;    /* PL is trusted */
} CreatePLangStmt;

/* ----------------------
 *	Create/Alter/Drop Role Statements
 *
 * Note: these node types are also used for the backwards-compatible
 * Create/Alter/Drop User/Group statements.  In the ALTER and DROP cases
 * there's really no need to distinguish what the original spelling was,
 * but for CREATE we mark the type because the defaults vary.
 * ----------------------
 */
typedef enum RoleStmtType { ROLESTMT_ROLE, ROLESTMT_USER, ROLESTMT_GROUP } RoleStmtType;

typedef struct CreateRoleStmt {
    NodeTag type;
    RoleStmtType stmt_type; /* ROLE/USER/GROUP */
    char* role;             /* role name */
    List* options;          /* List of DefElem nodes */
} CreateRoleStmt;

/* ----------------------
 *		{Create|Alter} SEQUENCE Statement
 * ----------------------
 */

typedef struct CreateSeqStmt {
    NodeTag type;
    RangeVar* sequence; /* the sequence to create */
    List* options;
    Oid ownerId; /* ID of owner, or InvalidOid for default */
#ifdef PGXC
    bool is_serial; /* Indicates if this sequence is part of SERIAL process */
#endif
    int64 uuid;            /* UUID of the sequence, mark unique sequence globally */
    bool canCreateTempSeq; /* create sequence when "create table (like )" */
    bool is_large;
} CreateSeqStmt;

typedef struct AlterSeqStmt {
    NodeTag type;
    RangeVar* sequence; /* the sequence to alter */
    List* options;
    bool missing_ok; /* skip error if a role is missing? */
#ifdef PGXC
    bool is_serial; /* Indicates if this sequence is part of SERIAL process */
#endif
    bool is_large; /* Indicates if this is a large or normal sequence */
} AlterSeqStmt;

/* ----------------------
 *		Create {Aggregate|Operator|Type} Statement
 * ----------------------
 */
typedef struct DefineStmt {
    NodeTag type;
    ObjectType kind;  /* aggregate, operator, type */
    bool oldstyle;    /* hack to signal old CREATE AGG syntax */
    List* defnames;   /* qualified name (list of Value strings) */
    List* args;       /* a list of TypeName (if needed) */
    List* definition; /* a list of DefElem */
} DefineStmt;

/* ----------------------
 *		Create Domain Statement
 * ----------------------
 */
typedef struct CreateDomainStmt {
    NodeTag type;
    List* domainname;          /* qualified name (list of Value strings) */
    TypeName* typname;         /* the base type */
    CollateClause* collClause; /* untransformed COLLATE spec, if any */
    List* constraints;         /* constraints (list of Constraint nodes) */
} CreateDomainStmt;

/* ----------------------
 *		Create Operator Class Statement
 * ----------------------
 */
typedef struct CreateOpClassStmt {
    NodeTag type;
    List* opclassname;  /* qualified name (list of Value strings) */
    List* opfamilyname; /* qualified name (ditto); NIL if omitted */
    char* amname;       /* name of index AM opclass is for */
    TypeName* datatype; /* datatype of indexed column */
    List* items;        /* List of CreateOpClassItem nodes */
    bool isDefault;     /* Should be marked as default for type? */
} CreateOpClassStmt;

#define OPCLASS_ITEM_OPERATOR 1
#define OPCLASS_ITEM_FUNCTION 2
#define OPCLASS_ITEM_STORAGETYPE 3

typedef struct CreateOpClassItem {
    NodeTag type;
    int itemtype; /* see codes above */
    /* fields used for an operator or function item: */
    List* name;         /* operator or function name */
    List* args;         /* argument types */
    int number;         /* strategy num or support proc num */
    List* order_family; /* only used for ordering operators */
    List* class_args;   /* only used for functions */
    /* fields used for a storagetype item: */
    TypeName* storedtype; /* datatype stored in index */
} CreateOpClassItem;

/* ----------------------
 *		Create Operator Family Statement
 * ----------------------
 */
typedef struct CreateOpFamilyStmt {
    NodeTag type;
    List* opfamilyname; /* qualified name (list of Value strings) */
    char* amname;       /* name of index AM opfamily is for */
} CreateOpFamilyStmt;

/* ----------------------
 *		Alter Operator Family Statement
 * ----------------------
 */
typedef struct AlterOpFamilyStmt {
    NodeTag type;
    List* opfamilyname; /* qualified name (list of Value strings) */
    char* amname;       /* name of index AM opfamily is for */
    bool isDrop;        /* ADD or DROP the items? */
    List* items;        /* List of CreateOpClassItem nodes */
} AlterOpFamilyStmt;


#ifdef ENABLE_MOT
typedef struct DropForeignStmt {
    NodeTag type;
    char relkind;
    Oid reloid;
    Oid indexoid;
    char* name;
} DropForeignStmt;
#endif

/* ----------------------
 *				Truncate Table Statement
 * ----------------------
 */
typedef struct TruncateStmt {
    NodeTag type;
    List* relations;       /* relations (RangeVars) to be truncated */
    bool restart_seqs;     /* restart owned sequences? */
    DropBehavior behavior; /* RESTRICT or CASCADE behavior */
    bool purge;
} TruncateStmt;

/* ----------------------
 *				Comment On Statement
 * ----------------------
 */
typedef struct CommentStmt {
    NodeTag type;
    ObjectType objtype; /* Object's type */
    List* objname;      /* Qualified name of the object */
    List* objargs;      /* Arguments if needed (eg, for functions) */
    char* comment;      /* Comment to insert, or NULL to remove */
} CommentStmt;

/* ----------------------
 *				SECURITY LABEL Statement
 * ----------------------
 */
typedef struct SecLabelStmt {
    NodeTag type;
    ObjectType objtype; /* Object's type */
    List* objname;      /* Qualified name of the object */
    List* objargs;      /* Arguments if needed (eg, for functions) */
    char* provider;     /* Label provider (or NULL) */
    char* label;        /* New security label to be assigned */
} SecLabelStmt;

/* ----------------------
 *		Close Portal Statement
 * ----------------------
 */
typedef struct ClosePortalStmt {
    NodeTag type;
    char* portalname; /* name of the portal (cursor) */
                      /* NULL means CLOSE ALL */
} ClosePortalStmt;

/* ----------------------
 *		Fetch Statement (also Move)
 * ----------------------
 */
typedef enum FetchDirection {
    /* for these, howMany is how many rows to fetch; FETCH_ALL means ALL */
    FETCH_FORWARD,
    FETCH_BACKWARD,
    /* for these, howMany indicates a position; only one row is fetched */
    FETCH_ABSOLUTE,
    FETCH_RELATIVE
} FetchDirection;

#define FETCH_ALL LONG_MAX

typedef struct FetchStmt {
    NodeTag type;
    FetchDirection direction; /* see above */
    long howMany;             /* number of rows, or position argument */
    char* portalname;         /* name of portal (cursor) */
    bool ismove;              /* TRUE if MOVE */
} FetchStmt;

/* ----------------------
 *		Create Index Statement
 *
 * This represents creation of an index and/or an associated constraint.
 * If isconstraint is true, we should create a pg_constraint entry along
 * with the index.  But if indexOid isn't InvalidOid, we are not creating an
 * index, just a UNIQUE/PKEY constraint using an existing index.  isconstraint
 * must always be true in this case, and the fields describing the index
 * properties are empty.
 * ----------------------
 */
typedef struct IndexStmt {
    NodeTag type;
    char* schemaname;           /* namespace of new index, or NULL for default */
    char* idxname;              /* name of new index, or NULL for default */
    RangeVar* relation;         /* relation to build index on */
    char* accessMethod;         /* name of access method (eg. btree) */
    char* tableSpace;           /* tablespace, or NULL for default */
    List* indexParams;          /* columns to index: a list of IndexElem */
    List* indexIncludingParams; /* additional columns to index: a list of IndexElem */
    List* options;              /* WITH clause options: a list of DefElem */
    Node* whereClause;          /* qualification (partial-index predicate) */
    List* excludeOpNames;       /* exclusion operator names, or NIL if none */
    char* idxcomment;           /* comment to apply to index, or NULL */
    Oid indexOid;               /* OID of an existing index, if any */
    Oid oldNode;                /* relfilenode of existing storage, if any */
    Oid oldPSortOid;            /* oid of existing psort storage for col-storage, if any */
    List* partIndexOldNodes;    /* partition relfilenode of existing storage, if any */
    List* partIndexOldPSortOid; /* partition psort oid, if any */
    Node* partClause;           /* partition index define */
    bool* partIndexUsable;      /* is partition index usable */
    /* @hdfs
     * is a partitioned index? The foreign table dose not index. The isPartitioned
     * value is false when relation is a foreign table.
     */
    bool isPartitioned;
    bool isGlobal;                            /* is GLOBAL partition index */
    bool crossbucket;                         /* is crossbucket index */
    bool unique;                              /* is index unique? */
    bool primary;                             /* is index a primary key? */
    bool isconstraint;                        /* is it for a pkey/unique constraint? */
    bool deferrable;                          /* is the constraint DEFERRABLE? */
    bool initdeferred;                        /* is the constraint INITIALLY DEFERRED? */
    bool concurrent;                          /* should this be a concurrent index build? */
    InformationalConstraint* inforConstraint; /* Soft constraint info, Currently only the HDFS foreign table support it
                                               */

    /*
     * Internal_flag is set to true when we use informational constraint feature,
     * at the same time not_enforeced is also set to true.
     */
    bool internal_flag;
    /*
     * For memory adapation, we will check the size of table before index create,
     * but if it's constraint index during table creation, we don't need to do the size
     * check, so set this flag to skip this
     */
    bool skip_mem_check;

    /* adaptive memory assigned for the stmt */
    AdaptMem memUsage;
} IndexStmt;

typedef struct AlterFunctionStmt {
    NodeTag type;
    FuncWithArgs* func; /* name and args of function */
    List* actions;      /* list of DefElem */
} AlterFunctionStmt;

typedef struct InlineCodeBlock {
    NodeTag type;
    char* source_text;  /* source text of anonymous code block */
    Oid langOid;        /* OID of selected language */
    bool langIsTrusted; /* trusted property of the language */
    bool atomic;        /* Atomic execution context, does not allow transactions */
} InlineCodeBlock;

typedef struct CallContext
{
	NodeTag     type;
	bool        atomic; /* Atomic execution context, does not allow transactions */
} CallContext;

/* ----------------------
 *		ALTER object SET SCHEMA Statement
 * ----------------------
 */
typedef struct AlterObjectSchemaStmt {
    NodeTag type;
    ObjectType objectType; /* OBJECT_TABLE, OBJECT_TYPE, etc */
    RangeVar* relation;    /* in case it's a table */
    List* object;          /* in case it's some other object */
    List* objarg;          /* argument types, if applicable */
    char* addname;         /* additional name if needed */
    char* newschema;       /* the new schema */
    bool missing_ok;       /* skip error if missing? */
} AlterObjectSchemaStmt;

/* ----------------------
 *		Alter Object Owner Statement
 * ----------------------
 */
typedef struct AlterOwnerStmt {
    NodeTag type;
    ObjectType objectType; /* OBJECT_TABLE, OBJECT_TYPE, etc */
    RangeVar* relation;    /* in case it's a table */
    List* object;          /* in case it's some other object */
    List* objarg;          /* argument types, if applicable */
    char* addname;         /* additional name if needed */
    char* newowner;        /* the new owner */
} AlterOwnerStmt;

/* ----------------------
 *		Create Rule Statement
 * ----------------------
 */
typedef struct RuleStmt {
    NodeTag type;
    RangeVar* relation; /* relation the rule is for */
    char* rulename;     /* name of the rule */
    Node* whereClause;  /* qualifications */
    CmdType event;      /* SELECT, INSERT, etc */
    bool instead;       /* is a 'do instead'? */
    List* actions;      /* the action statements */
    bool replace;       /* OR REPLACE */
    char* sql_statement; /* used for resize rule */
} RuleStmt;

/* ----------------------
 *		Notify Statement
 * ----------------------
 */
typedef struct NotifyStmt {
    NodeTag type;
    char* conditionname; /* condition name to notify */
    char* payload;       /* the payload string, or NULL if none */
} NotifyStmt;

/* ----------------------
 *		Listen Statement
 * ----------------------
 */
typedef struct ListenStmt {
    NodeTag type;
    char* conditionname; /* condition name to listen on */
} ListenStmt;

/* ----------------------
 *		Unlisten Statement
 * ----------------------
 */
typedef struct UnlistenStmt {
    NodeTag type;
    char* conditionname; /* name to unlisten on, or NULL for all */
} UnlistenStmt;

/* ----------------------
 *		Create Type Statement, composite types
 * ----------------------
 */
typedef struct CompositeTypeStmt {
    NodeTag type;
    RangeVar* typevar; /* the composite type to be created */
    List* coldeflist;  /* list of ColumnDef nodes */
} CompositeTypeStmt;

/* ----------------------
 *		Create Type Statement, table of types
 * ----------------------
 */
typedef struct TableOfTypeStmt {
    NodeTag type;
    List* typname;         /* the table of type to be quoted */
    TypeName* reftypname;  /* the name of the type being referenced */
} TableOfTypeStmt;

/* ----------------------
 *		Create Type Statement, enum types
 * ----------------------
 */
typedef struct CreateEnumStmt {
    NodeTag type;
    List* typname; /* qualified name (list of Value strings) */
    List* vals;    /* enum values (list of Value strings) */
} CreateEnumStmt;

/* ----------------------
 *		Create Type Statement, range types
 * ----------------------
 */
typedef struct CreateRangeStmt {
    NodeTag type;
    List* typname; /* qualified name (list of Value strings) */
    List* params;  /* range parameters (list of DefElem) */
} CreateRangeStmt;

/* ----------------------
 *		Alter Type Statement, enum types
 * ----------------------
 */
typedef struct AlterEnumStmt {
    NodeTag type;
    List* typname;           /* qualified name (list of Value strings) */
    char* oldVal;            /* old enum value's name, if renaming */
    char* newVal;            /* new enum value's name */
    char* newValNeighbor;    /* neighboring enum value, if specified */
    bool newValIsAfter;      /* place new enum value after neighbor? */
    bool skipIfNewValExists; /* no error if new already exists? */
} AlterEnumStmt;

/* ----------------------
 *		Load Statement
 * ----------------------
 */

typedef enum LOAD_DATA_TYPE {
    LOAD_DATA_APPEND,
    LOAD_DATA_TRUNCATE,
    LOAD_DATA_REPLACE,
    LOAD_DATA_INSERT,
    LOAD_DATA_UNKNOWN
} LOAD_DATA_TYPE;

typedef struct LoadWhenExpr {
    NodeTag type;
    int whentype; /* 0 poition 1 field name */
    int start;
    int end;
    char *val;
    const char *attname;
    int attnum;
    char *oper;
    int operid;
} LoadWhenExpr;

typedef struct LoadStmt {
    NodeTag type;
    char* filename; /* file to load */

    List *pre_load_options;

    bool is_load_data;
    bool is_only_special_filed;

    // load data options
    List *load_options;

    // relation options
    LOAD_DATA_TYPE load_type;
    RangeVar *relation;
    List *rel_options;
} LoadStmt;

/* ----------------------
 *		Createdb Statement
 * ----------------------
 */
typedef struct CreatedbStmt {
    NodeTag type;
    char* dbname;  /* name of database to create */
    List* options; /* List of DefElem nodes */
} CreatedbStmt;

/* ----------------------
 *	Alter Database
 * ----------------------
 */
typedef struct AlterDatabaseStmt {
    NodeTag type;
    char* dbname;  /* name of database to alter */
    List* options; /* List of DefElem nodes */
} AlterDatabaseStmt;

/* ----------------------
 *		Dropdb Statement
 * ----------------------
 */
typedef struct DropdbStmt {
    NodeTag type;
    char* dbname;    /* database to drop */
    bool missing_ok; /* skip error if db is missing? */
} DropdbStmt;

#ifndef ENABLE_MULTIPLE_NODES
/* ----------------------
 *             Alter System Statement
 * ----------------------
 */
typedef struct AlterSystemStmt {
    NodeTag type;
    VariableSetStmt *setstmt;   /* SET subcommand */
} AlterSystemStmt;
#endif
/* ----------------------
 *		Cluster Statement (support pbrown's cluster index implementation)
 * ----------------------
 */
typedef struct ClusterStmt {
    NodeTag type;
    RangeVar* relation; /* relation being indexed, or NULL if all */
    char* indexname;    /* original index defined */
    bool verbose;       /* print progress info */
    AdaptMem memUsage;  /* adaptive memory assigned for the stmt */
} ClusterStmt;

/* ----------------------
 *		Vacuum and Analyze Statements
 *
 * Even though these are nominally two statements, it's convenient to use
 * just one node type for both.  Note that at least one of VACOPT_VACUUM
 * and VACOPT_ANALYZE must be set in options.  VACOPT_FREEZE is an internal
 * convenience for the grammar and is not examined at runtime --- the
 * freeze_min_age and freeze_table_age fields are what matter.
 * ----------------------
 */
typedef enum VacuumOption {
    VACOPT_VACUUM = 1 << 0,  /* do VACUUM */
    VACOPT_ANALYZE = 1 << 1, /* do ANALYZE */
    VACOPT_VERBOSE = 1 << 2, /* print progress info */
    VACOPT_FREEZE = 1 << 3,  /* FREEZE option */
    VACOPT_FULL = 1 << 4,    /* FULL (non-concurrent) vacuum */
    VACOPT_NOWAIT = 1 << 5,  /* don't wait to get lock (autovacuum only) */
    VACOPT_MERGE = 1 << 6,   /* move data from delta table to main table */
    VACOPT_MULTICOLUMN = 1 << 7,
    VACOPT_VERIFY = 1 << 8,        /* VERITY to check the data file */
    VACOPT_FAST = 1 << 9,          /* verify fast option */
    VACOPT_COMPLETE = 1 << 10,     /* verify complete option */
#ifdef ENABLE_MOT
    VACOPT_AUTOVAC = 1 << 11,      /* mark automatic vacuum initiation */
#endif
    VACOPT_COMPACT = 1 << 30,      /* compact hdfs file with invalid data just for DFS table */
    VACOPT_HDFSDIRECTORY = 1 << 31 /* just clean empty hdfs directory */
} VacuumOption;

#define DEFAULT_SAMPLE_ROWCNT 30000
#define ANALYZE_MODE_MAX_NUM 4
#define GET_ESTTOTALROWCNTS_FLAG ((double)(-1.0))
#define INVALID_ESTTOTALROWS ((double)(-1.0))

/*
 * Currently, the HDFS table need collect three statistics information
 * in pg_statistic. we define AnalyzeMode enum strunct to realize global
 * analyze.
 * ANALYZENORMAL:   Execute normal analyze command.
 * ANALYZEMAIN:     Collect only HDFS table information when execute global analyze.
 * ANALYZEDELTA:    Collect only Delta table information when execute global analyze.
 * ANALYZECOMPLEX:  Collect HDFS table and Delta table information when execute global analyze.
 */
typedef enum AnalyzeMode { ANALYZENORMAL = 0, ANALYZEMAIN = 1, ANALYZEDELTA = 2, ANALYZECOMPLEX = 3 } AnalyzeMode;

typedef struct GlobalStatInfoEx {
    AnalyzeMode eAnalyzeMode; /* The mode of table whitch will collect stat info, normal table or HDFS table.
                                  It need collect three statistics information for HDFS table. */
    double sampleRate;        /* CN compute it and send to DN. */
    double secsamplerows;     /* real sample rows num for second sampling. */
    bool isReplication;       /* the current relation is replication or not. */
    bool exec_query; /* do query for dfs table and delta table to get sample or not according to the ratio with rows of
                        complex. */
    double totalRowCnts;    /* How many tuples receive from DN. */
    int64 topRowCnts;       /* Top tuple numbers receive from DN. */
    int64 topMemSize;       /* memory_size(KB)=com_size*oneTupleSize for DN, top memory size of all DNs for CN. */
    int attnum;             /* column num for current table. */
    double dn1totalRowCnts; /* reltuples from DN1. */
    double* dndistinct;     /* stadndistinct for DN1 */
    double* correlations;   /* correlation of stats for DN1 */
    int num_samples;        /* how many sample rows receive from DN. */
    HeapTuple* sampleRows;  /* sample rows receive from DN. */
    TupleDesc tupleDesc;    /* sample row's tuple descriptor. */
} GlobalStatInfoEx;

typedef enum HdfsSampleRowsFlag {
    SAMPLEFLAG_DFS = 1 << 0,   /* sample rows for dfs table. */
    SAMPLEFLAG_DELTA = 1 << 1, /* sample rows for delta table */
} HdfsSampleRowsFlag;

/* One sample row of HDFS table. */
typedef struct {
    double totalrows;     /* estimate total rows */
    double samplerows;    /* real sample rows num for first sampling. */
    double secsamplerows; /* real sample rows num for second sampling. */
    int8* flag;           /* Identify which category(main/delta/complex) the sample row belong. */
    HeapTuple* rows;      /* sample row data. */
} HDFS_SAMPLE_ROWS;

/* All sample rows of HDFS table for global stats. */
typedef struct {
    MemoryContext hdfs_sample_context;                 /* using to save sample rows. */
    double totalSampleRowCnt;                          /* total sample row count include dfs table and delta table */
    HDFS_SAMPLE_ROWS stHdfsSampleRows[ANALYZECOMPLEX]; /* sample rows include dfs table and delta table. */
} GBLSTAT_HDFS_SAMPLE_ROWS;

struct SplitMap;

typedef struct VacuumStmt {
    NodeTag type;
    int options;          /* OR of VacuumOption flags */
    int flags;            /* flags to distinguish partition or btree */
                          /* the values for this flags are in vacuum.h */
    Oid rely_oid;         /* for btree, it is the heap btree or it is InvalidOid */
    int64 freeze_min_age;   /* min freeze age, or -1 to use default */
    int64 freeze_table_age; /* age at which to scan whole table */
    RangeVar* relation;   /* single table to process, or NULL */
    List* va_cols;        /* list of column names, or NIL for all */

    Relation onepartrel; /* for tracing the opened relation */
    Partition onepart;   /* for tracing the opened partition */
    Relation parentpartrel; /* for tracing the opened parent relation of a subpartition */
    Partition parentpart;   /* for tracing the opened parent partition of a subpartition */
    bool issubpartition;
    List* partList;
#ifdef PGXC
    void* HDFSDnWorkFlow; /* @hdfs HDFSDnWorkFlow stores analyze operation related information */
#endif
    bool isForeignTables;      /* @hdfs This parameter is true when we run "analyze [verbose] foreign table;" command */
    bool isPgFdwForeignTables; /* This parameter is true when the fdw of foreign table is gc_fdw */
#ifdef ENABLE_MOT
    bool isMOTForeignTable;
#endif

    /*
     * @hdfs
     * parameter totalFileCnt and nodeNo is set by CNSchedulingForAnalyze
     * CNSchedulingForAnalyze(	  int *totalFilesCnt,
     *						  int *nodeNo,
     *                                         Oid foreignTableId)
     */
    unsigned int totalFileCnt; /* @hdfs The count of file to be sampled in analyze foreign table operation */
    int nodeNo;                /* @hdfs Which data node will do analyze operation,
                                  @global stats: Other coordinators will get statistics from which coordinator node. */

    /*
     * @hdfs total number of Data nodes, we use this number to adjust reltuples cnt stored in pg_class
     * eg: We do the operation "analyze tablename", we have x data nodes and tablename is a hdfs foreign
     *	   table. Data node finish analyze command, CN get tuples number information from DN. This number
     *       is 1/x of the total tuples number. We adjust this number to the real one in CN.
     */
    unsigned int DnCnt;

    /*
     * Add param for global stats.
     */
    DestReceiver* dest;    /* used to DN send sample rows to CN. */
    int num_samples;       /* how many sample rows receive from DN. */
    HeapTuple* sampleRows; /* sample rows receive from DN. */
    TupleDesc tupleDesc;   /* sample row's tuple descriptor for normal table. */
    int tableidx;          /* set current index which table need to set sample rate or total row counts */
    GlobalStatInfoEx pstGlobalStatEx[ANALYZE_MODE_MAX_NUM - 1]; /* the auxiliary info for global stats, it extend to
                                                                   identify hdfs table. */
    unsigned int orgCnNodeNo; /* the nodeId identify which CN receive analyze command from client, other CN need to get
                                 stats from it. */
    List* hdfsforeignMapDnList; /* identify some datanodes belone to split map used for CN get total reltuples from
                                   them. */
    bool sampleTableRequired;   /* require sample table for get statistic. */
    List* tmpSampleTblNameList; /* identify sample table name if under debugging. */
    bool isAnalyzeTmpTable;     /* true if analyze's table is temp table. */
#ifdef PGXC
    DistributionType disttype; /* Distribution type for analyze's table. */
#endif
    AdaptMem memUsage; /* adaptive memory assigned for the stmt */
    Oid curVerifyRel;  /* the current relation is for database mode to send remote query */
    bool isCascade;    /* used to verify table */
} VacuumStmt;
/* Only support analyze, can not support vacuum analyze in transaction block. */
#define IS_ONLY_ANALYZE_TMPTABLE (((stmt)->isAnalyzeTmpTable) && !((stmt)->options & VACOPT_VACUUM))

#ifdef PGXC
/*
 * ----------------------
 *      Barrier Statement
 */
typedef struct BarrierStmt {
    NodeTag type;
    char* id; /* User supplied barrier id, if any */
} BarrierStmt;

/*
 * ----------------------
 *      Create Node statement
 */
typedef struct CreateNodeStmt {
    NodeTag type;
    char* node_name;
    List* options;
} CreateNodeStmt;

/*
 * ----------------------
 *     Alter Node statement
 */
typedef struct AlterNodeStmt {
    NodeTag type;
    char* node_name;
    List* options;
} AlterNodeStmt;

typedef struct AlterCoordinatorStmt {
    NodeTag type;
    char* node_name;
    char* set_value;
    List* coor_nodes;
} AlterCoordinatorStmt;

/*
 * ----------------------
 *      Drop Node statement
 */
typedef struct DropNodeStmt {
    NodeTag type;
    char* node_name;
    bool missing_ok;    /* skip error if db is missing? */
    List* remote_nodes; /* specify where to drop node remotely */
} DropNodeStmt;

/*
 * ----------------------
 *      Create Group statement
 */
typedef struct CreateGroupStmt {
    NodeTag type;
    char* group_name;
    char* group_parent;
    char* src_group_name;
    List* nodes;
    List* buckets;
    int   bucketcnt;
    bool  vcgroup;
} CreateGroupStmt;

/*
 * ----------------------
 *      Alter Group statement
 */
typedef enum {
    AG_SET_DEFAULT,
    AG_SET_VCGROUP,
    AG_SET_NOT_VCGROUP,
    AG_SET_RENAME,
    AG_SET_TABLE_GROUP,
    AG_SET_BUCKETS,
    AG_ADD_NODES,
    AG_DELETE_NODES,
    AG_RESIZE_GROUP,
    AG_CONVERT_VCGROUP,
    AG_SET_SEQ_ALLNODES,
    AG_SET_SEQ_SELFNODES
} AlterGroupType;

typedef struct AlterGroupStmt {
    NodeTag type;
    char* group_name;
    char* install_name;
    List* nodes;
    AlterGroupType alter_type;
} AlterGroupStmt;

/*
 * ----------------------
 *      Drop Group statement
 */
typedef struct DropGroupStmt {
    NodeTag type;
    char* group_name;
    char* src_group_name;
    bool to_elastic_group;
} DropGroupStmt;

/*
 * ----------------------
 *      Create Policy Label statement
 */
typedef struct CreatePolicyLabelStmt {
    NodeTag type;
    bool if_not_exists;
    char* label_type;
    char* label_name;
    List* label_items;
} CreatePolicyLabelStmt;

/*
 * ----------------------
 *      Alter Policy Label statement
 */
typedef struct AlterPolicyLabelStmt {
    NodeTag type;
    char* stmt_type;
    char* label_name;
    List* label_items;
} AlterPolicyLabelStmt;

/*
 * ----------------------
 *      Drop Policy Label statement
 */
typedef struct DropPolicyLabelStmt {
    NodeTag type;
    bool if_exists;
    List* label_names;
} DropPolicyLabelStmt;

/*
 * ----------------------
 *
 */
typedef struct PolicyFilterNode
{
    NodeTag     type;
    char        *node_type;     /* operator or filter node */
    char        *op_value;      /* for operator type node usage */
    char        *filter_type;   /* for filter type node usage */
    List        *values;        /* for filter type node usage */
    bool        has_not_operator; /* for filter type node usage */
    Node        *left;          /* for operator type node usage */
    Node        *right;         /* for operator type node usage */
} PolicyFilterNode;

/*
 * ----------------------
 *      Create Audit Policy statement
 */
typedef struct CreateAuditPolicyStmt
{
    NodeTag     type;
    bool        if_not_exists;
    char        *policy_type;
    char        *policy_name;
    List        *policy_targets;
    List        *policy_filters;
    bool        policy_enabled;
} CreateAuditPolicyStmt;

/*
 * ----------------------
 *      Alter Audit Policy statement
 */
typedef struct AlterAuditPolicyStmt
{
    NodeTag     type;
    bool        missing_ok;
    char        *policy_name;
    char        *policy_action;
    char		*policy_type;
    List        *policy_items;
    List		*policy_filters;
    char        *policy_comments;
    Node        *policy_enabled;
} AlterAuditPolicyStmt;

/*
 * ----------------------
 *      Drop Audit Policy statement
 */
typedef struct DropAuditPolicyStmt
{
    NodeTag     type;
    bool        missing_ok;
    List        *policy_names;
} DropAuditPolicyStmt;

/*
 * ----------------------
 *      Masking Policy Condition
 */
typedef struct MaskingPolicyCondition
{
    NodeTag     type;
    RangeVar    *fqdn;
    char        *_operator;
    Node        *arg;
} MaskingPolicyCondition;

/*
 * ----------------------
 *      Create Masking Policy statement
 */
typedef struct CreateMaskingPolicyStmt
{
    NodeTag     type;
    bool        if_not_exists;
    char        *policy_name;
    List        *policy_data;
    Node        *policy_condition;
    List        *policy_filters;
    bool        policy_enabled;
} CreateMaskingPolicyStmt;

/*
 * ----------------------
 *      Alter Masking Policy statement
 */
typedef struct AlterMaskingPolicyStmt
{
    NodeTag     type;
    char        *policy_name;
    char        *policy_action;
    List        *policy_items;
    Node        *policy_condition;
    List        *policy_filters;
    char        *policy_comments;
    Node        *policy_enabled;
} AlterMaskingPolicyStmt;

/*
 * ----------------------
 *      Drop Masking Policy statement
 */
typedef struct DropMaskingPolicyStmt
{
    NodeTag     type;
    bool        if_exists;
    List        *policy_names;
} DropMaskingPolicyStmt;

typedef struct AlterSchemaStmt {
    NodeTag type;
    char *schemaname; /* the name of the schema to create */
    char *authid;      /* the owner of the created schema */
    bool hasBlockChain;  /* whether this schema has blockchain */
} AlterSchemaStmt;

/*
 * ----------------------
 *      Create Resource Pool statement
 */
typedef struct CreateResourcePoolStmt {
    NodeTag type;
    char* pool_name;
    List* options;
} CreateResourcePoolStmt;

/*
 * ----------------------
 *      Alter Resource Pool statement
 */
typedef struct AlterResourcePoolStmt {
    NodeTag type;
    char* pool_name;
    List* options;
} AlterResourcePoolStmt;

/*
 * ----------------------
 *      Drop Resource Pool statement
 */
typedef struct DropResourcePoolStmt {
    NodeTag type;
    bool missing_ok;
    char* pool_name;
} DropResourcePoolStmt;

typedef struct AlterGlobalConfigStmt {
    NodeTag type;
    List* options;
} AlterGlobalConfigStmt;

typedef struct DropGlobalConfigStmt {
    NodeTag type;
    List* options;
} DropGlobalConfigStmt;

/*
 * ----------------------
 *      Create Workload Group statement
 */
typedef struct CreateWorkloadGroupStmt {
    NodeTag type;
    char* group_name;
    char* pool_name;
    List* options;
} CreateWorkloadGroupStmt;

/*
 * ----------------------
 *      Alter Workload Group statement
 */
typedef struct AlterWorkloadGroupStmt {
    NodeTag type;
    char* group_name;
    char* pool_name;
    List* options;
} AlterWorkloadGroupStmt;

/*
 * ----------------------
 *      Drop Workload Group statement
 */
typedef struct DropWorkloadGroupStmt {
    NodeTag type;
    bool missing_ok;
    char* group_name;
} DropWorkloadGroupStmt;

/*
 * ----------------------
 *      Create App Workload Group Mapping statement
 */
typedef struct CreateAppWorkloadGroupMappingStmt {
    NodeTag type;
    char* app_name;
    List* options;
} CreateAppWorkloadGroupMappingStmt;

/*
 * ----------------------
 *      Alter App Workload Group Mapping statement
 */
typedef struct AlterAppWorkloadGroupMappingStmt {
    NodeTag type;
    char* app_name;
    List* options;
} AlterAppWorkloadGroupMappingStmt;

/*
 * ----------------------
 *      Drop App Workload Group Mapping statement
 */
typedef struct DropAppWorkloadGroupMappingStmt {
    NodeTag type;
    bool missing_ok;
    char* app_name;
} DropAppWorkloadGroupMappingStmt;

#endif

struct PlanInformation;
/* ----------------------
 *		Explain Statement
 *
 * The "query" field is either a raw parse tree (SelectStmt, InsertStmt, etc)
 * or a Query node if parse analysis has been done.  Note that rewriting and
 * planning of the query are always postponed until execution of EXPLAIN.
 * ----------------------
 */
typedef struct ExplainStmt {
    NodeTag type;
    Node* statement; /* statement_id for EXPLAIN PLAN */
    Node* query;     /* the query (see comments above) */
    List* options;   /* list of DefElem nodes */
    PlanInformation* planinfo;
} ExplainStmt;

/* ----------------------
 *     REFRESH MATERIALIZED VIEW Statement
 * ----------------------
 */
typedef struct RefreshMatViewStmt
{
   NodeTag     type;
   bool        skipData;       /* true for WITH NO DATA */
   bool        incremental;    /* true for INCREMENTALLY */
   RangeVar   *relation;       /* relation to insert into */
} RefreshMatViewStmt;

/* ----------------------
 * Checkpoint Statement
 * ----------------------
 */
typedef struct CheckPointStmt {
    NodeTag type;
} CheckPointStmt;

/* ----------------------
 * Discard Statement
 * ----------------------
 */
typedef enum DiscardMode { DISCARD_ALL, DISCARD_PLANS, DISCARD_TEMP } DiscardMode;

typedef struct DiscardStmt {
    NodeTag type;
    DiscardMode target;
} DiscardStmt;

/* ----------------------
 *		LOCK Statement
 * ----------------------
 */
typedef struct LockStmt {
    NodeTag type;
    List* relations; /* relations to lock */
    int mode;        /* lock mode */
    bool nowait;     /* no wait mode */
    bool cancelable; /* send term to lock holder */
    int waitSec;      /* WAIT time Sec */
} LockStmt;

/* ----------------------
 *		SET CONSTRAINTS Statement
 * ----------------------
 */
typedef struct ConstraintsSetStmt {
    NodeTag type;
    List* constraints; /* List of names as RangeVars */
    bool deferred;
} ConstraintsSetStmt;

#ifdef ENABLE_MOT
typedef struct ReindexForeignStmt {
    NodeTag type;
    char relkind;
    Oid reloid;
    Oid indexoid;
    char* name;
} ReindexForeignStmt;
#endif

/* ----------------------
 *		CREATE CONVERSION Statement
 * ----------------------
 */
typedef struct CreateConversionStmt {
    NodeTag type;
    List* conversion_name;   /* Name of the conversion */
    char* for_encoding_name; /* source encoding name */
    char* to_encoding_name;  /* destination encoding name */
    List* func_name;         /* qualified conversion function name */
    bool def;                /* is this a default conversion? */
} CreateConversionStmt;

/* ----------------------
 *	CREATE CAST Statement
 * ----------------------
 */
typedef struct CreateCastStmt {
    NodeTag type;
    TypeName* sourcetype;
    TypeName* targettype;
    FuncWithArgs* func;
    CoercionContext context;
    bool inout;
} CreateCastStmt;

/* ----------------------
 *		DEALLOCATE Statement
 * ----------------------
 */
typedef struct DeallocateStmt {
    NodeTag type;
    char* name; /* The name of the plan to remove */
                /* NULL means DEALLOCATE ALL */
} DeallocateStmt;

/*
 *		DROP OWNED statement
 */
typedef struct DropOwnedStmt {
    NodeTag type;
    List* roles;
    DropBehavior behavior;
} DropOwnedStmt;

/*
 *		REASSIGN OWNED statement
 */
typedef struct ReassignOwnedStmt {
    NodeTag type;
    List* roles;
    char* newrole;
} ReassignOwnedStmt;

/*
 * TS Dictionary stmts: DefineStmt, RenameStmt and DropStmt are default
 */
typedef struct AlterTSDictionaryStmt {
    NodeTag type;
    List* dictname; /* qualified name (list of Value strings) */
    List* options;  /* List of DefElem nodes */
} AlterTSDictionaryStmt;

/*
 * TS Configuration stmts: DefineStmt, RenameStmt and DropStmt are default
 */
typedef struct AlterTSConfigurationStmt {
    NodeTag type;
    List* cfgname; /* qualified name (list of Value strings) */

    /*
     * dicts will be non-NIL if ADD/ALTER MAPPING was specified. If dicts is
     * NIL, but tokentype isn't, DROP MAPPING was specified.
     */
    List* tokentype; /* list of Value strings */
    List* dicts;     /* list of list of Value strings */
    List* cfoptions; /* list of configuration options */
    bool override;   /* if true - remove old variant */
    bool replace;    /* if true - replace dictionary by another */
    bool missing_ok; /* for DROP - skip error if missing? */
    bool is_reset;   /* if true - reset options */
} AlterTSConfigurationStmt;

/*
 * CLEAN CONNECTION statement
 */
typedef struct CleanConnStmt {
    NodeTag type;
    List* nodes;    /* list of nodes dropped */
    char* dbname;   /* name of database to drop connections */
    char* username; /* name of user whose connections are dropped */
    bool is_coord;  /* type of connections dropped */
    bool is_force;  /* option force */
    bool is_check;  /* option check */
} CleanConnStmt;

/*
 * CreateTableLike Context
 */
typedef struct TableLikeCtx {
    NodeTag type;
    bits32 options;  /* OR of TableLikeOption flags */
    bool temp_table; /* temporary table or not */
    bool hasoids;    /* has oids or not */
    List* columns;   /* the list of ColumnDef */
    List* ckconstraints;
    List* comments;
    List* cluster_keys;
    PartitionState* partition;
    List* inh_indexes;
    List* reloptions;
} TableLikeCtx;

typedef struct CreateDirectoryStmt {
    NodeTag type;
    bool replace;        /* T => replace if already exists */
    char* directoryname; /* the name of directory to create */
    char* owner;         /* directory's owner */
    char* location;      /* the real path of directory */
} CreateDirectoryStmt;

typedef struct AlterDirectoryStmt {
    NodeTag type;
    char* directoryname; /* the name of directory to create */
    char* owner;         /* directory's owner */
    char* location;      /* the real path of directory */
    List* options;       /* list of options */
} AlterDirectoryStmt;

typedef struct DropDirectoryStmt {
    NodeTag type;
    char* directoryname; /* the name of directory to drop */
    bool missing_ok;     /* skip error if db is missing? */

} DropDirectoryStmt;

#endif /* PARSENODES_H */

