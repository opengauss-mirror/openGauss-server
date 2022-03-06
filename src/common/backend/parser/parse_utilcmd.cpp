/* -------------------------------------------------------------------------
 *
 * parse_utilcmd.cpp
 *	  Perform parse analysis work for various utility commands
 *
 * Formerly we did this work during parse_analyze() in analyze.c.  However
 * that is fairly unsafe in the presence of querytree caching, since any
 * database state that we depend on in making the transformations might be
 * obsolete by the time the utility command is executed; and utility commands
 * have no infrastructure for holding locks or rechecking plan validity.
 * Hence these functions are now called at the start of execution of their
 * respective utility commands.
 *
 * NOTE: in general we must avoid scribbling on the passed-in raw parse
 * tree, since it might be in a plan cache.  The simplest solution is
 * a quick copyObject() call before manipulating the query tree.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *	src/common/backend/parser/parse_utilcmd.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <string.h>
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/gs_column_keys.h"
#include "catalog/gs_encrypted_columns.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_oper.h"
#include "parser/parse_coerce.h"
#ifdef PGXC
#include "optimizer/pgxcship.h"
#include "pgstat.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/nodegroups.h"
#include "pgxc/execRemote.h"
#include "pgxc/redistrib.h"
#include "executor/node/nodeModifyTable.h"
#endif
#include "parser/parser.h"
#include "rewrite/rewriteManip.h"
#include "executor/node/nodeModifyTable.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/extended_statistics.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/partitionkey.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "mb/pg_wchar.h"
#include "gaussdb_version.h"
#include "gs_ledger/ledger_utils.h"
#include "client_logic/client_logic.h"
#include "client_logic/client_logic_enums.h"
#include "storage/checksum_impl.h"

/* State shared by transformCreateSchemaStmt and its subroutines */
typedef struct {
    const char* stmtType; /* "CREATE SCHEMA" or "ALTER SCHEMA" */
    char* schemaname;     /* name of schema */
    char* authid;         /* owner of schema */
    List* sequences;      /* CREATE SEQUENCE items */
    List* tables;         /* CREATE TABLE items */
    List* views;          /* CREATE VIEW items */
    List* indexes;        /* CREATE INDEX items */
    List* triggers;       /* CREATE TRIGGER items */
    List* grants;         /* GRANT items */
} CreateSchemaStmtContext;

#define ALTER_FOREIGN_TABLE "ALTER FOREIGN TABLE"
#define CREATE_FOREIGN_TABLE "CREATE FOREIGN TABLE"
#define ALTER_TABLE "ALTER TABLE"
#define CREATE_TABLE "CREATE TABLE"

#define RELATION_ISNOT_REGULAR_PARTITIONED(relation)                                                              \
    (((relation)->rd_rel->relkind != RELKIND_RELATION && (relation)->rd_rel->relkind != RELKIND_FOREIGN_TABLE && \
      (relation)->rd_rel->relkind != RELKIND_STREAM) || RelationIsNonpartitioned((relation)))

static void transformColumnDefinition(CreateStmtContext* cxt, ColumnDef* column, bool preCheck);
static void transformTableConstraint(CreateStmtContext* cxt, Constraint* constraint);
static void transformTableLikeClause(
    CreateStmtContext* cxt, TableLikeClause* table_like_clause, bool preCheck, bool isFirstNode, bool is_row_table, TransformTableType transformType);
/* decide if serial column should be copied under analyze */
static bool IsCreatingSeqforAnalyzeTempTable(CreateStmtContext* cxt);
static void transformTableLikePartitionProperty(Relation relation, HeapTuple partitionTableTuple, List** partKeyColumns,
    List* partitionList, List** partitionDefinitions);
static IntervalPartitionDefState* TransformTableLikeIntervalPartitionDef(HeapTuple partitionTableTuple);
static void transformTableLikePartitionKeys(
    Relation relation, HeapTuple partitionTableTuple, List** partKeyColumns, List** partKeyPosList);
static void transformTableLikePartitionBoundaries(
    Relation relation, List* partKeyPosList, List* partitionList, List** partitionDefinitions);
static void transformOfType(CreateStmtContext* cxt, TypeName* ofTypename);
static List* get_collation(Oid collation, Oid actual_datatype);
static List* get_opclass(Oid opclass, Oid actual_datatype);
static void checkPartitionValue(CreateStmtContext* cxt, CreateStmt* stmt);
static void TransformListDistributionValue(ParseState* pstate, Node** listDistDef, bool needCheck);
static void checkClusterConstraints(CreateStmtContext* cxt);
static void checkPsortIndexCompatible(IndexStmt* stmt);
static void checkCBtreeIndexCompatible(IndexStmt* stmt);
static void checkCGinBtreeIndexCompatible(IndexStmt* stmt);

static void checkReserveColumn(CreateStmtContext* cxt);
static void CheckDistributionSyntax(CreateStmt* stmt);
static void CheckListRangeDistribClause(CreateStmtContext *cxt, CreateStmt *stmt);

static void transformIndexConstraints(CreateStmtContext* cxt);
static void checkConditionForTransformIndex(
    Constraint* constraint, CreateStmtContext* cxt, Oid index_oid, Relation index_rel);
static IndexStmt* transformIndexConstraint(Constraint* constraint, CreateStmtContext* cxt, bool mustGlobal = false);
static void transformFKConstraints(CreateStmtContext* cxt, bool skipValidation, bool isAddConstraint);
static void transformConstraintAttrs(CreateStmtContext* cxt, List* constraintList);
static void transformColumnType(CreateStmtContext* cxt, ColumnDef* column);
static void setSchemaName(char* context_schema, char** stmt_schema_name);
/*
 * @hdfs
 * The following three functions are used for HDFS foreign talbe constraint.
 */
static void setInternalFlagIndexStmt(List* IndexList);
static void checkInformationalConstraint(Node* node, bool isForeignTbl);
static void checkConstraint(CreateStmtContext* cxt, Node* node);
static void setMemCheckFlagForIdx(List* IndexList);

/* check partition name */
static void check_partition_name_less_than(List* partitionList, bool isPartition);
static void check_partition_name_start_end(List* partitionList, bool isPartition);

/* for range partition: start/end syntax */
static void precheck_start_end_defstate(List* pos, Form_pg_attribute* attrs,
    RangePartitionStartEndDefState* defState, bool isPartition);
static Datum get_partition_arg_value(Node* node, bool* isnull);
static Datum evaluate_opexpr(
    ParseState* pstate, List* oprname, Node* leftarg, Node* rightarg, Oid* restypid, int location);
static Const* coerce_partition_arg(ParseState* pstate, Node* node, Oid targetType);
static Oid choose_coerce_type(Oid leftid, Oid rightid);
static void get_rel_partition_info(Relation partTableRel, List** pos, Const** upBound);
static void get_src_partition_bound(Relation partTableRel, Oid srcPartOid, Const** lowBound, Const** upBound);
static Oid get_split_partition_oid(Relation partTableRel, SplitPartitionState* splitState);
static List* add_range_partition_def_state(List* xL, List* boundary, const char* partName, const char* tblSpaceName);
static bool CheckStepInRange(ParseState *pstate, Const *startVal, Const *endVal, Const *everyVal,
    Form_pg_attribute attr);
static List* divide_start_end_every_internal(ParseState* pstate, char* partName, Form_pg_attribute attr,
    Const* startVal, Const* endVal, Node* everyExpr, int* numPart,
    int maxNum, bool isinterval, bool needCheck, bool isPartition);
static List* DividePartitionStartEndInterval(ParseState* pstate, Form_pg_attribute attr, char* partName,
    Const* startVal, Const* endVal, Const* everyVal, Node* everyExpr, int* numPart, int maxNum, bool isPartition);
extern Node* makeAConst(Value* v, int location);
static bool IsElementExisted(List* indexElements, IndexElem* ielem);
static char* CreatestmtGetOrientation(CreateStmt *stmt);
#define REDIS_SCHEMA "data_redis"
/*
 * transformCreateStmt -
 *	  parse analysis for CREATE TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed CreateStmt, but there may be additional actions
 * to be done before and after the actual DefineRelation() call.
 *
 * SQL92 allows constraints to be scattered all over, so thumb through
 * the columns and collect all constraints into one place.
 * If there are any implied indices (e.g. UNIQUE or PRIMARY KEY)
 * then expand those into multiple IndexStmt blocks.
 *	  - thomas 1997-12-02
 */
List* transformCreateStmt(CreateStmt* stmt, const char* queryString, const List* uuids, bool preCheck,
Oid *namespaceid, bool isFirstNode)
{
    ParseState* pstate = NULL;
    CreateStmtContext cxt;
    List* result = NIL;
    List* save_alist = NIL;
    ListCell* elements = NULL;
    Oid existing_relid;
    bool is_ledger_nsp = false;
    bool is_row_table = is_ledger_rowstore(stmt->options);
    /*
     * We must not scribble on the passed-in CreateStmt, so copy it.  (This is
     * overkill, but easy.)
     */
    stmt = (CreateStmt*)copyObject(stmt);
    
    if (uuids != NIL) {
        list_free_deep(stmt->uuids);
        stmt->uuids = (List*)copyObject(uuids);
    }
    
    if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP && stmt->relation->schemaname)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION), errmsg("temporary tables cannot specify a schema name")));

    /*
     * Look up the creation namespace.	This also checks permissions on the
     * target namespace, locks it against concurrent drops, checks for a
     * preexisting relation in that namespace with the same name, and updates
     * stmt->relation->relpersistence if the select namespace is temporary.
     */
    *namespaceid = RangeVarGetAndCheckCreationNamespace(stmt->relation, NoLock, &existing_relid, RELKIND_RELATION);

    /*
     * Check whether relation is in ledger schema. If it is, we add hash column.
     */
    HeapTuple tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(*namespaceid));
    if (HeapTupleIsValid(tp)) {
        bool is_null = true;
        Datum datum = SysCacheGetAttr(NAMESPACEOID, tp, Anum_pg_namespace_nspblockchain, &is_null);
        if (!is_null) {
            is_ledger_nsp = DatumGetBool(datum);
        }
        ReleaseSysCache(tp);
    }

    if (is_ledger_nsp && stmt->partTableState != NULL && stmt->partTableState->subPartitionState != NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 (errmsg("Un-support feature"), errdetail("Subpartition table does not support ledger user table."),
                  errcause("The function is not implemented."), erraction("Use other actions instead."))));
    }

    /*
     * If the relation already exists and the user specified "IF NOT EXISTS",
     * bail out with a NOTICE.
     */
    if (stmt->if_not_exists && OidIsValid(existing_relid)) {
        bool exists_ok = true;
        /* 
         * Emit the right error or warning message for a "CREATE" command issued on a exist relation.
         * remote node : should have relation if recieve "IF NOT EXISTS" stmt.
         */
        if (!u_sess->attr.attr_common.xc_maintenance_mode && !IS_SINGLE_NODE) {
            if (!(IS_PGXC_COORDINATOR && !IsConnFromCoord())) {
                exists_ok = false;
            }
        }
        if (exists_ok) {
            ereport(NOTICE,
                (errcode(ERRCODE_DUPLICATE_TABLE),
                    errmsg("relation \"%s\" already exists, skipping", stmt->relation->relname)));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_TABLE),
                    errmsg("relation \"%s\" already exists", stmt->relation->relname)));
        }
        return NIL;
    }

    /*
     * Transform node group name of table in logic cluster.
     * If not TO GROUP clause, add default node group to the CreateStmt;
     * If logic cluster is redistributing, modify node group to target node group
     * except delete delta table.
     */
    if (IS_PGXC_COORDINATOR && in_logic_cluster()) {
        char* group_name = NULL;
        if (stmt->subcluster == NULL && !IsA(stmt, CreateForeignTableStmt)) {
            group_name = PgxcGroupGetCurrentLogicCluster();

            if (group_name != NULL) {
                stmt->subcluster = makeNode(PGXCSubCluster);
                stmt->subcluster->clustertype = SUBCLUSTER_GROUP;
                stmt->subcluster->members = list_make1(makeString(group_name));
            }
        } else if (stmt->subcluster != NULL && stmt->subcluster->clustertype == SUBCLUSTER_GROUP) {
            Assert(stmt->subcluster->members->length == 1);
            group_name = strVal(linitial(stmt->subcluster->members));
            Assert(group_name != NULL);

            if (IsLogicClusterRedistributed(group_name)) {
                /*Specially handle delete delta table.*/
                bool is_delete_delta = false;
                if (!IsA(stmt, CreateForeignTableStmt) && stmt->relation->relpersistence == RELPERSISTENCE_UNLOGGED) {
                    is_delete_delta = RelationIsDeleteDeltaTable(stmt->relation->relname);
                }

                /*Logic cluster is redistributing, modify node group to target node group */
                if (!is_delete_delta) {
                    Value* val = (Value*)linitial(stmt->subcluster->members);
                    group_name = PgxcGroupGetStmtExecGroupInRedis();

                    if (group_name != NULL) {
                        pfree_ext(strVal(val));
                        strVal(val) = group_name;
                    }
                }
            }
        }
    }

    /*
     * If the target relation name isn't schema-qualified, make it so.  This
     * prevents some corner cases in which added-on rewritten commands might
     * think they should apply to other relations that have the same name and
     * are earlier in the search path.	But a local temp table is effectively
     * specified to be in pg_temp, so no need for anything extra in that case.
     */
    if (stmt->relation->schemaname == NULL && stmt->relation->relpersistence != RELPERSISTENCE_TEMP) {
        /* If create schema already set, use the latest one */
        if (t_thrd.proc->workingVersionNum >= 92408 && u_sess->catalog_cxt.setCurCreateSchema) {
            stmt->relation->schemaname = pstrdup(u_sess->catalog_cxt.curCreateSchema);
        } else {
            stmt->relation->schemaname = get_namespace_name(*namespaceid);
        }
        if (t_thrd.proc->workingVersionNum >= 92408 && IS_MAIN_COORDINATOR &&
            !u_sess->catalog_cxt.setCurCreateSchema) {
            u_sess->catalog_cxt.setCurCreateSchema = true;
            u_sess->catalog_cxt.curCreateSchema =
                MemoryContextStrdup(u_sess->top_transaction_mem_cxt,stmt->relation->schemaname);
        }
    }

    /* Set up pstate and CreateStmtContext */
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    cxt.pstate = pstate;
    if (IsA(stmt, CreateForeignTableStmt))
        cxt.stmtType = CREATE_FOREIGN_TABLE;
    else
        cxt.stmtType = CREATE_TABLE;
    cxt.relation = stmt->relation;
    cxt.rel = NULL;
    cxt.inhRelations = stmt->inhRelations;
    cxt.subcluster = stmt->subcluster;
    cxt.isalter = false;
    cxt.columns = NIL;
    cxt.ckconstraints = NIL;
    cxt.fkconstraints = NIL;
    cxt.ixconstraints = NIL;
    cxt.clusterConstraints = NIL;
    cxt.inh_indexes = NIL;
    cxt.blist = NIL;
    cxt.alist = NIL;
    cxt.pkey = NULL;
    cxt.csc_partTableState = NULL;
    cxt.reloptions = NIL;
    cxt.hasoids = false;

#ifdef PGXC
    cxt.fallback_dist_col = NULL;
    cxt.distributeby = NULL;
#endif
    cxt.node = (Node*)stmt;
    cxt.internalData = stmt->internalData;
    cxt.isResizing = false;
    cxt.ofType = (stmt->ofTypename != NULL);

    /* We have gen uuids, so use it */
    if (stmt->uuids != NIL)
        cxt.uuids = stmt->uuids;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && stmt->internalData != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Do not support create table with INERNAL DATA clause.")));
    }

    if (IsA(stmt, CreateForeignTableStmt)) {
        CreateForeignTableStmt* fStmt = (CreateForeignTableStmt*)stmt;
        cxt.canInfomationalConstraint = CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_STMT(fStmt);
    } else {
        cxt.canInfomationalConstraint = false;
    }

    AssertEreport(stmt->ofTypename == NULL || stmt->inhRelations == NULL, MOD_OPT, "");

    if (stmt->ofTypename)
        transformOfType(&cxt, stmt->ofTypename);

    /*
     * Run through each primary element in the table creation clause. Separate
     * column defs from constraints, and do preliminary analysis.
     */
    foreach (elements, stmt->tableElts) {
        TableLikeClause* tbl_like_clause = NULL;
        TransformTableType transformType = TRANSFORM_INVALID;
        Node* element = (Node*)lfirst(elements);
        cxt.uuids = stmt->uuids;

        switch (nodeTag(element)) {
            case T_ColumnDef:
                if (is_ledger_nsp && strcmp(((ColumnDef*)element)->colname, "hash") == 0 &&
                    !IsA(stmt, CreateForeignTableStmt) && stmt->relation->relpersistence == RELPERSISTENCE_PERMANENT &&
                    is_row_table) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("\"hash\" column is reserved for system in ledger schema.")));
                }
                transformColumnDefinition(&cxt, (ColumnDef*)element, !isFirstNode && preCheck);

                if (((ColumnDef *)element)->clientLogicColumnRef != NULL) {
                    if (stmt->partTableState != NULL && stmt->partTableState->subPartitionState != NULL) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             (errmsg("Un-support feature"),
                              errdetail("For subpartition table, cryptographic database is not yet supported."),
                              errcause("The function is not implemented."), erraction("Use other actions instead."))));
                    }
                }

                break;

            case T_Constraint:
                transformTableConstraint(&cxt, (Constraint*)element);
                break;

            case T_TableLikeClause:
                tbl_like_clause = (TableLikeClause*)element;
#ifndef ENABLE_MULTIPLE_NODES
                if (tbl_like_clause->options & CREATE_TABLE_LIKE_DISTRIBUTION)
                    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
#endif
                if (PointerIsValid(stmt->partTableState) && (tbl_like_clause->options & CREATE_TABLE_LIKE_PARTITION)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("unsupport \"like clause including partition\" for partitioned table"),
                            errdetail("use either \"like clause including partition\" or \"partition by\" clause")));
                }
                if (PointerIsValid(stmt->options) && (tbl_like_clause->options & CREATE_TABLE_LIKE_RELOPTIONS)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("unsupport \"like clause including reloptions\" together with \"with\""),
                            errdetail("use either \"like clause including reloptions\" or \"with\" clause")));
                }
#ifdef PGXC
                if (IS_PGXC_COORDINATOR && (tbl_like_clause->options & CREATE_TABLE_LIKE_DISTRIBUTION)) {
                    if (PointerIsValid(stmt->distributeby)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                errmsg(
                                    "unsupport \"like clause including distribution\" together with \"distribute by\""),
                                errdetail(
                                    "use either \"like clause including distribution\" or \"distribute by\" clause")));
                    }
                }
#endif
                if (!(tbl_like_clause->options & CREATE_TABLE_LIKE_RELOPTIONS)) {
                    //transformType is only use in hashbucket data transfer
                    if (is_ledger_hashbucketstore(stmt->options)) {
                        transformType = TRANSFORM_TO_HASHBUCKET;
                    } else {
                        transformType = TRANSFORM_TO_NONHASHBUCKET;
                    }
                }
                transformTableLikeClause(&cxt, (TableLikeClause*)element, !isFirstNode && preCheck, isFirstNode, is_row_table, transformType);
                if (stmt->relation->relpersistence != RELPERSISTENCE_TEMP &&
                    tbl_like_clause->relation->relpersistence == RELPERSISTENCE_TEMP)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("do not support create non-temp table like temp table")));
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(element))));
                break;
        }
    }

    /*
     * add hash column for table in ledger scheam;
     */
    if (is_ledger_nsp && !IsA(stmt, CreateForeignTableStmt) &&
        stmt->relation->relpersistence == RELPERSISTENCE_PERMANENT && is_ledger_rowstore(stmt->options)) {
        ColumnDef* col = makeNode(ColumnDef);
        col->colname = pstrdup("hash");
        col->typname = SystemTypeName("hash16");
        col->kvtype = 0;
        col->inhcount = 0;
        col->is_local = true;
        col->is_not_null = false;
        col->is_from_type = false;
        col->storage = 0;
        col->cmprs_mode = ATT_CMPR_UNDEFINED;
        col->raw_default = NULL;
        col->cooked_default = NULL;
        col->collClause = NULL;
        col->collOid = InvalidOid;
        col->constraints = NIL;
        col->fdwoptions = NIL;
        transformColumnDefinition(&cxt, col, !isFirstNode && preCheck);
    }

    // cxt.csc_partTableState is the partitionState generated
    // from like including partition clause
    if (cxt.csc_partTableState != NULL) {
        Assert(stmt->partTableState == NULL);
        stmt->partTableState = cxt.csc_partTableState;
    }
    /* check syntax for CREATE TABLE */
    checkPartitionSynax(stmt);

    /*
     * @hdfs
     * If the table is foreign table, must be gotten the ispartitioned value
     * from part_state struct.
     */
    if (IsA(stmt, CreateForeignTableStmt)) {
        CreateForeignTableStmt* ftblStmt = (CreateForeignTableStmt*)stmt;
        if (NULL != ftblStmt->part_state) {
            cxt.ispartitioned = true;
            cxt.partitionKey = ftblStmt->part_state->partitionKey;
            cxt.subPartitionKey = NULL;
        } else {
            cxt.ispartitioned = false;
        }
    } else {
        cxt.ispartitioned = PointerIsValid(stmt->partTableState);
        if (cxt.ispartitioned) {
            cxt.partitionKey = stmt->partTableState->partitionKey;
            if (stmt->partTableState->subPartitionState != NULL) {
                cxt.subPartitionKey = stmt->partTableState->subPartitionState->partitionKey;
            } else {
                cxt.subPartitionKey = NULL;
            }
        }
    }

    checkPartitionValue(&cxt, stmt);

    /*
     * transform START/END into LESS/THAN:
     * Put this part behind checkPartitionValue(), since we assume start/end/every-paramters
     * have already been transformed from A_Const into Const.
     */
    if (stmt->partTableState && is_start_end_def_list(stmt->partTableState->partitionList)) {
        List* pos = NIL;
        TupleDesc desc;

        /* get partition key position */
        pos = GetPartitionkeyPos(stmt->partTableState->partitionKey, cxt.columns);

        /* get descriptor */
        desc = BuildDescForRelation(cxt.columns);

        /* entry of transform */
        stmt->partTableState->partitionList = transformRangePartStartEndStmt(
            pstate, stmt->partTableState->partitionList, pos, desc->attrs, 0, NULL, NULL, true);
    }

    if (PointerIsValid(stmt->partTableState)) {
// only check partition name duplication on primary coordinator
#ifdef PGXC
        if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) {
            if (stmt->partTableState->subPartitionState != NULL) {
                checkSubPartitionName(stmt->partTableState->partitionList);
            } else {
                checkPartitionName(stmt->partTableState->partitionList);
            }
        }
#else
        checkPartitionName(stmt->partTableState->partitionList);
#endif
    }

    /* like clause-including reloptions: cxt.reloptions is produced by like including reloptions clause */
    /* output to stmt->options */
    if (cxt.reloptions != NIL) {
        stmt->options = list_concat(stmt->options, cxt.reloptions);
    }
    /* like clause-including oids: cxt.hasoids is produced by like including oids clause, output to stmt->options */
    if (cxt.hasoids) {
        stmt->options = lappend(stmt->options, makeDefElem("oids", (Node*)makeInteger(cxt.hasoids)));
    }
    cxt.hasoids = interpretOidsOption(stmt->options);

#ifdef PGXC
    if (cxt.distributeby != NULL) {
        stmt->distributeby = cxt.distributeby;
    } else {
        cxt.distributeby = stmt->distributeby;
    }

    if (stmt->distributeby != NULL) {
        CheckDistributionSyntax(stmt);
        /* in slice reference case we don't need to check other distribution clauses */
        if (!OidIsValid(stmt->distributeby->referenceoid)) {
            CheckListRangeDistribClause(&cxt, stmt);
        }
    }
#endif
    /*
     * transformIndexConstraints wants cxt.alist to contain only index
     * statements, so transfer anything we already have into saveAlist.
     */
    save_alist = cxt.alist;
    cxt.alist = NIL;

    AssertEreport(stmt->constraints == NIL, MOD_OPT, "");

    /*
     * Postprocess constraints that give rise to index definitions.
     */
    transformIndexConstraints(&cxt);

    /*
     * @hdfs
     * If the table is HDFS foreign table, set internal_flag to true
     * in order to create informational constraint. The primary key and
     * unique informaiotnal constraints do not build a index, but informational
     * constraint is build in DefineIndex function.
     */
    if (cxt.alist != NIL) {
        if (cxt.canInfomationalConstraint)
            setInternalFlagIndexStmt(cxt.alist);
        else
            setMemCheckFlagForIdx(cxt.alist);
    }

    /*
     * Postprocess foreign-key constraints.
     */
    transformFKConstraints(&cxt, true, false);

    /*
     * Check partial cluster key constraints
     */
    checkClusterConstraints(&cxt);

    /*
     * Check reserve column
     */
    checkReserveColumn(&cxt);

    /*
     * Output results.
     */
    stmt->tableEltsDup = stmt->tableElts;
    stmt->tableElts = cxt.columns;
    stmt->constraints = cxt.ckconstraints;
    stmt->clusterKeys = cxt.clusterConstraints;
    if (stmt->internalData == NULL) {
        stmt->internalData = cxt.internalData;
    }
    result = lappend(cxt.blist, stmt);
    result = list_concat(result, cxt.alist);
    result = list_concat(result, save_alist);

#ifdef PGXC
    /*
     * If the user did not specify any distribution clause and there is no
     * inherits clause, try and use PK or unique index
     */
#ifdef ENABLE_MOT
    if ((!IsA(stmt, CreateForeignTableStmt) || IsSpecifiedFDW(((CreateForeignTableStmt*)stmt)->servername, MOT_FDW)) &&
        !stmt->distributeby && !stmt->inhRelations && cxt.fallback_dist_col) {
#else
    if (!IsA(stmt, CreateForeignTableStmt) && !stmt->distributeby && !stmt->inhRelations && cxt.fallback_dist_col) {
#endif
        stmt->distributeby = makeNode(DistributeBy);
        stmt->distributeby->disttype = DISTTYPE_HASH;
        stmt->distributeby->colname = cxt.fallback_dist_col;
    }
#endif

    return result;
}

/*
 * createSeqOwnedByTable -
 *		create a sequence owned by table, need to add record to pg_depend.
 *		used in CREATE TABLE and CREATE TABLE ... LIKE
 */
static void createSeqOwnedByTable(CreateStmtContext* cxt, ColumnDef* column, bool preCheck, bool large)
{
    Oid snamespaceid;
    char* snamespace = NULL;
    char* sname = NULL;
    char* qstring = NULL;
    A_Const* snamenode = NULL;
    TypeCast* castnode = NULL;
    FuncCall* funccallnode = NULL;
    CreateSeqStmt* seqstmt = NULL;
    AlterSeqStmt* altseqstmt = NULL;
    List* attnamelist = NIL;
    Constraint* constraint = NULL;

    /*
     * Determine namespace and name to use for the sequence.
     *
     * Although we use ChooseRelationName, it's not guaranteed that the
     * selected sequence name won't conflict; given sufficiently long
     * field names, two different serial columns in the same table could
     * be assigned the same sequence name, and we'd not notice since we
     * aren't creating the sequence quite yet.	In practice this seems
     * quite unlikely to be a problem, especially since few people would
     * need two serial columns in one table.
     */
    if (cxt->rel)
        snamespaceid = RelationGetNamespace(cxt->rel);
    else {
        snamespaceid = RangeVarGetCreationNamespace(cxt->relation);
        RangeVarAdjustRelationPersistence(cxt->relation, snamespaceid);
    }
    snamespace = get_namespace_name(snamespaceid);
    sname = ChooseRelationName(cxt->relation->relname, column->colname, "seq", strlen("seq"), snamespaceid);

    if (!preCheck || IS_SINGLE_NODE)
        ereport(NOTICE,
            (errmsg("%s will create implicit sequence \"%s\" for serial column \"%s.%s\"",
                cxt->stmtType,
                sname,
                cxt->relation->relname,
                column->colname)));

    /*
     * Build a CREATE SEQUENCE command to create the sequence object, and
     * add it to the list of things to be done before this CREATE/ALTER
     * TABLE.
     */
    seqstmt = makeNode(CreateSeqStmt);
    seqstmt->sequence = makeRangeVar(snamespace, sname, -1);
    seqstmt->options = NIL;
#ifdef PGXC
    seqstmt->is_serial = true;
#endif
    seqstmt->is_large = large;

    /* Assign UUID for create sequence */
    if (!IS_SINGLE_NODE)
        seqstmt->uuid = gen_uuid(cxt->uuids);
    else
        seqstmt->uuid = INVALIDSEQUUID;

    /*
     * If this is ALTER ADD COLUMN, make sure the sequence will be owned
     * by the table's owner.  The current user might be someone else
     * (perhaps a superuser, or someone who's only a member of the owning
     * role), but the SEQUENCE OWNED BY mechanisms will bleat unless table
     * and sequence have exactly the same owning role.
     */
    if (cxt->rel)
        seqstmt->ownerId = cxt->rel->rd_rel->relowner;
    else
        seqstmt->ownerId = InvalidOid;

    /*
     * When under analyzing, we may create temp sequence which has serial column,
     * but we cannot create temp sequence for now. Besides, create temp table (like t)
     * can be successfully created, but it should not happen. So here we set canCreateTempSeq
     * to true to handle this two cases.
     */
    if (u_sess->analyze_cxt.is_under_analyze || u_sess->attr.attr_common.enable_beta_features) {
        seqstmt->canCreateTempSeq = true;
    }

    cxt->blist = lappend(cxt->blist, seqstmt);

    /*
     * Build an ALTER SEQUENCE ... OWNED BY command to mark the sequence
     * as owned by this column, and add it to the list of things to be
     * done after this CREATE/ALTER TABLE.
     */
    altseqstmt = makeNode(AlterSeqStmt);
    altseqstmt->sequence = makeRangeVar(snamespace, sname, -1);
#ifdef PGXC
    altseqstmt->is_serial = true;
#endif
    attnamelist = list_make3(makeString(snamespace), makeString(cxt->relation->relname), makeString(column->colname));
    altseqstmt->options = list_make1(makeDefElem("owned_by", (Node*)attnamelist));
    altseqstmt->is_large = large;

    cxt->alist = lappend(cxt->alist, altseqstmt);

    /*
     * Create appropriate constraints for SERIAL.  We do this in full,
     * rather than shortcutting, so that we will detect any conflicting
     * constraints the user wrote (like a different DEFAULT).
     *
     * Create an expression tree representing the function call
     * nextval('sequencename').  We cannot reduce the raw tree to cooked
     * form until after the sequence is created, but there's no need to do
     * so.
     */
    qstring = quote_qualified_identifier(snamespace, sname);
    snamenode = makeNode(A_Const);
    snamenode->val.type = T_String;
    snamenode->val.val.str = qstring;
    snamenode->location = -1;
    castnode = makeNode(TypeCast);
    castnode->typname = (TypeName*)SystemTypeName("regclass");
    castnode->arg = (Node*)snamenode;
    castnode->location = -1;
    funccallnode = makeNode(FuncCall);
    funccallnode->funcname = SystemFuncName("nextval");
    funccallnode->args = list_make1(castnode);
    funccallnode->agg_order = NIL;
    funccallnode->agg_star = false;
    funccallnode->agg_distinct = false;
    funccallnode->func_variadic = false;
    funccallnode->over = NULL;
    funccallnode->location = -1;

    constraint = makeNode(Constraint);
    constraint->contype = CONSTR_DEFAULT;
    constraint->location = -1;
    constraint->raw_expr = (Node*)funccallnode;
    constraint->cooked_expr = NULL;
    column->constraints = lappend(column->constraints, constraint);
    column->raw_default = constraint->raw_expr;

    constraint = makeNode(Constraint);
    constraint->contype = CONSTR_NOTNULL;
    constraint->location = -1;
    column->constraints = lappend(column->constraints, constraint);
}

static bool isColumnEncryptionAllowed(CreateStmtContext *cxt, ColumnDef *column)
{
    if (column->is_serial) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("encrypted serial column is not implemented"),
            parser_errposition(cxt->pstate, column->typname->location)));
        return false;
    }
    return true;
}

/*
 * transformColumnDefinition -
 *		transform a single ColumnDef within CREATE TABLE
 *		Also used in ALTER TABLE ADD COLUMN
 */
static void transformColumnDefinition(CreateStmtContext* cxt, ColumnDef* column, bool preCheck)
{
    bool is_serial = false;
    bool large = false;
    bool saw_nullable = false;
    bool saw_default = false;
    bool saw_generated = false;
    Constraint* constraint = NULL;
    ListCell* clist = NULL;
    ClientLogicColumnRef* clientLogicColumnRef = NULL;

    /* Check the constraint type.*/
    checkConstraint(cxt, (Node*)column);

    cxt->columns = lappend(cxt->columns, (Node*)column);

    /* Check for SERIAL pseudo-types */
    is_serial = false;
    if (column->typname && list_length(column->typname->names) == 1 && !column->typname->pct_type) {
        char* typname = strVal(linitial(column->typname->names));

        if (strcmp(typname, "smallserial") == 0 || strcmp(typname, "serial2") == 0) {
            is_serial = true;
            column->typname->names = NIL;
            column->typname->typeOid = INT2OID;
        } else if (strcmp(typname, "serial") == 0 || strcmp(typname, "serial4") == 0) {
            is_serial = true;
            column->typname->names = NIL;
            column->typname->typeOid = INT4OID;
        } else if (strcmp(typname, "bigserial") == 0 || strcmp(typname, "serial8") == 0) {
            is_serial = true;
            column->typname->names = NIL;
            column->typname->typeOid = INT8OID;
        } else if (strcmp(typname, "largeserial") == 0 || strcmp(typname, "serial16") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("Pldebug is not supported for distribute currently.")));
#endif
            is_serial = true;
            column->typname->names = NIL;
            column->typname->typeOid = NUMERICOID;
            large = true;
#ifdef ENABLE_MULTIPLE_NODES
        } else if ((is_enc_type(typname) && IS_MAIN_COORDINATOR) ||
#else
        } else if (is_enc_type(typname) ||
#endif
            (!u_sess->attr.attr_common.enable_beta_features && strcmp(typname, "int16") == 0)) {
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                    errmsg("It's not supported to create %s column", typname)));
        }

        if (is_serial) {
            /*
             * We have to reject "serial[]" explicitly, because once we've set
             * typeid, LookupTypeName won't notice arrayBounds.  We don't need any
             * special coding for serial(typmod) though.
             */
            if (column->typname->arrayBounds != NIL)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("array of serial is not implemented"),
                        parser_errposition(cxt->pstate, column->typname->location)));

            if (cxt->relation && cxt->relation->relpersistence == RELPERSISTENCE_TEMP)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("It's not supported to create serial column on temporary table")));

            if (0 == pg_strncasecmp(cxt->stmtType, ALTER_TABLE, strlen(cxt->stmtType)))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("It's not supported to alter table add serial column")));
        }
    }

    /* Do necessary work on the column type declaration */
    if (column->typname)
        transformColumnType(cxt, column);

    /* Special actions for SERIAL pseudo-types */
    column->is_serial = is_serial;
    if (is_serial) {
        createSeqOwnedByTable(cxt, column, preCheck, large);
    }

    /* Process column constraints, if any... */
    transformConstraintAttrs(cxt, column->constraints);

    saw_nullable = false;
    saw_default = false;

    foreach (clist, column->constraints) {
        constraint = (Constraint*)lfirst(clist);

        switch (constraint->contype) {
            case CONSTR_NULL:
                if (saw_nullable && column->is_not_null)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
                                column->colname,
                                cxt->relation->relname),
                            parser_errposition(cxt->pstate, constraint->location)));
                column->is_not_null = FALSE;
                saw_nullable = true;
                break;

            case CONSTR_NOTNULL:
                if (saw_nullable && !column->is_not_null)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("conflicting NULL/NOT NULL declarations for column \"%s\" of table \"%s\"",
                                column->colname,
                                cxt->relation->relname),
                            parser_errposition(cxt->pstate, constraint->location)));
                column->is_not_null = TRUE;
                saw_nullable = true;
                break;

            case CONSTR_DEFAULT:
                if (saw_default)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("multiple default values specified for column \"%s\" of table \"%s\"",
                                column->colname,
                                cxt->relation->relname),
                            parser_errposition(cxt->pstate, constraint->location)));
                column->raw_default = constraint->raw_expr;
                AssertEreport(constraint->cooked_expr == NULL, MOD_OPT, "");
                saw_default = true;
                break;

            case CONSTR_GENERATED:
                if (cxt->ofType) {
                    ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("generated columns are not supported on typed tables")));
                }
                if (saw_generated) {
                    ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("multiple generation clauses specified for column \"%s\" of table \"%s\"",
                        column->colname, cxt->relation->relname),
                        parser_errposition(cxt->pstate, constraint->location)));
                }
                column->generatedCol = ATTRIBUTE_GENERATED_STORED;
                column->raw_default = constraint->raw_expr;
                Assert(constraint->cooked_expr == NULL);
                saw_generated = true;
                break;

            case CONSTR_CHECK:
                cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
                break;

            case CONSTR_PRIMARY:
            case CONSTR_UNIQUE:
                if (constraint->keys == NIL)
                    constraint->keys = list_make1(makeString(column->colname));
                cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
                break;

            case CONSTR_EXCLUSION:
                /* grammar does not allow EXCLUDE as a column constraint */
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("column exclusion constraints are not supported")));
                break;

            case CONSTR_FOREIGN:

                /*
                 * Fill in the current attribute's name and throw it into the
                 * list of FK constraints to be processed later.
                 */
                constraint->fk_attrs = list_make1(makeString(column->colname));
                cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
                break;

            case CONSTR_ATTR_DEFERRABLE:
            case CONSTR_ATTR_NOT_DEFERRABLE:
            case CONSTR_ATTR_DEFERRED:
            case CONSTR_ATTR_IMMEDIATE:
                /* transformConstraintAttrs took care of these */
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized constraint type: %d", constraint->contype)));
                break;
        }
    }
    if (column->clientLogicColumnRef != NULL) {
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_MAIN_COORDINATOR && !u_sess->attr.attr_common.enable_full_encryption) {
#else
        if (!u_sess->attr.attr_common.enable_full_encryption) {
#endif
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                    errmsg("Un-support to define encrypted column when client encryption is disabled.")));
        }
        if (isColumnEncryptionAllowed(cxt, column)) {
            clientLogicColumnRef = (ClientLogicColumnRef*)column->clientLogicColumnRef;
            clientLogicColumnRef->orig_typname = column->typname;
            typenameTypeIdAndMod(NULL, clientLogicColumnRef->orig_typname, &clientLogicColumnRef->orig_typname->typeOid, &clientLogicColumnRef->orig_typname->typemod);
                if (clientLogicColumnRef->dest_typname) {
                    typenameTypeIdAndMod(NULL, clientLogicColumnRef->dest_typname, &clientLogicColumnRef->dest_typname->typeOid, &clientLogicColumnRef->dest_typname->typemod);
                    column->typname =   makeTypeNameFromOid(clientLogicColumnRef->dest_typname->typeOid, clientLogicColumnRef->orig_typname->typeOid);
                }
            transformColumnType(cxt, column);
        }
    }

    if (saw_default && saw_generated)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("both default and generation expression specified for column \"%s\" of table \"%s\"",
                            column->colname, cxt->relation->relname),
                     parser_errposition(cxt->pstate,
                                        constraint->location)));

    /*
     * Generate ALTER FOREIGN TABLE ALTER COLUMN statement which adds
     * per-column foreign data wrapper options for this column.
     */
    if (column->fdwoptions != NIL) {
        AlterTableStmt* stmt = NULL;
        AlterTableCmd* cmd = NULL;

        cmd = makeNode(AlterTableCmd);
        cmd->subtype = AT_AlterColumnGenericOptions;
        cmd->name = column->colname;
        cmd->def = (Node*)column->fdwoptions;
        cmd->behavior = DROP_RESTRICT;
        cmd->missing_ok = false;

        stmt = makeNode(AlterTableStmt);
        stmt->relation = cxt->relation;
        stmt->cmds = NIL;
        stmt->relkind = OBJECT_FOREIGN_TABLE;
        stmt->cmds = lappend(stmt->cmds, cmd);

        cxt->alist = lappend(cxt->alist, stmt);
    }
}

/*
 * transformTableConstraint
 *		transform a Constraint node within CREATE TABLE or ALTER TABLE
 */
static void transformTableConstraint(CreateStmtContext* cxt, Constraint* constraint)
{
    switch (constraint->contype) {
        case CONSTR_PRIMARY:
        case CONSTR_UNIQUE:
        case CONSTR_EXCLUSION:
            cxt->ixconstraints = lappend(cxt->ixconstraints, constraint);
            break;

        case CONSTR_CHECK:
            cxt->ckconstraints = lappend(cxt->ckconstraints, constraint);
            break;

        case CONSTR_CLUSTER:
            cxt->clusterConstraints = lappend(cxt->clusterConstraints, constraint);
            break;

        case CONSTR_FOREIGN:
            cxt->fkconstraints = lappend(cxt->fkconstraints, constraint);
            break;

        case CONSTR_NULL:
        case CONSTR_NOTNULL:
        case CONSTR_DEFAULT:
        case CONSTR_GENERATED:
        case CONSTR_ATTR_DEFERRABLE:
        case CONSTR_ATTR_NOT_DEFERRABLE:
        case CONSTR_ATTR_DEFERRED:
        case CONSTR_ATTR_IMMEDIATE:
            ereport(ERROR,
                (errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
                    errmsg("invalid context for constraint type %d", constraint->contype)));
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized constraint type: %d", constraint->contype)));
            break;
    }

    /* Check the constraint type. */
    checkConstraint(cxt, (Node*)constraint);
}

/*
 * searchSeqidFromExpr
 *
 * search default expression for sequence oid.
 */
Oid searchSeqidFromExpr(Node* cooked_default)
{
    Const* first_arg = NULL;
    FuncExpr* nextval_expr = NULL;

    if (IsA(cooked_default, FuncExpr)) {
        if (((FuncExpr*)cooked_default)->funcid == NEXTVALFUNCOID) {
            nextval_expr = (FuncExpr*)cooked_default;
        } else {
            List* args = ((FuncExpr*)cooked_default)->args;
            if (args != NULL) {
                Node* nextval = (Node*)linitial(args);
                if (IsA(nextval, FuncExpr) && ((FuncExpr*)nextval)->funcid == NEXTVALFUNCOID) {
                    nextval_expr = (FuncExpr*)nextval;
                }
            }
        }
    }
    if (nextval_expr == NULL)
        return InvalidOid;

    first_arg = (Const*)linitial(nextval_expr->args);
    Assert(IsA(first_arg, Const));

    return DatumGetObjectId(first_arg->constvalue);
}

/*
 * checkTableLikeSequence
 *
 * Analyze default expression of table column, if default is nextval function,
 * it means the first argument of nextval function is sequence, we need to
 * check whether the sequence exists in current datanode.
 * We check sequence oid only because stringToNode in transformTableLikeFromSerialData
 * has already checked sequence name (see _readFuncExpr in readfuncs.cpp).
 * Suppose create a table like this:  CREATE TABLE t1 (id serial, a int) TO NODE GROUP ng1;
 * a sequence named t1_id_seq will be created and the sequence exists in NodeGroup ng1.
 * If create table like t1 in another NodeGroup ng2, error will be reported because t1_id_seq
 * does not exists some datanodes of NodeGroup ng2.
 */

static void checkTableLikeSequence(Node* cooked_default)
{
    char* seq_name = NULL;
    Oid seqId = searchSeqidFromExpr(cooked_default);
    if (!OidIsValid(seqId))
        return;

    seq_name = get_rel_name(seqId);
    if (seq_name == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("CREATE TABLE LIKE with column sequence "
                       "in different NodeGroup is not supported."),
                errdetail("Recommend to LIKE table with sequence in installation NodeGroup.")));
    }      
    pfree_ext(seq_name);
}

/*
 * transformTableLikeFromSerialData
 *
 * Get meta info of a table from serialized data, the serialized data come from CN.
 * The function is used for CREATE TABLE ... LIKE across node group.
 */
static void transformTableLikeFromSerialData(CreateStmtContext* cxt, TableLikeClause* table_like_clause)
{
    ListCell* cell = NULL;
    TableLikeCtx* meta_info = NULL;
    meta_info = (TableLikeCtx*)stringToNode(cxt->internalData);

    table_like_clause->options = meta_info->options;
    cxt->hasoids = meta_info->hasoids;
    cxt->columns = meta_info->columns;
    cxt->csc_partTableState = meta_info->partition;
    cxt->inh_indexes = meta_info->inh_indexes;
    cxt->clusterConstraints = meta_info->cluster_keys;
    cxt->ckconstraints = meta_info->ckconstraints;
    cxt->alist = meta_info->comments;
    cxt->reloptions = meta_info->reloptions;

    if (meta_info->temp_table) {
        table_like_clause->relation->relpersistence = RELPERSISTENCE_TEMP;
        ExecSetTempObjectIncluded();
    }
    /* Special actions for SERIAL pseudo-types */
    foreach (cell, cxt->columns) {
        ColumnDef* column = (ColumnDef*)lfirst(cell);
        if (column->is_serial) {
            bool large = (column->typname->typeOid == NUMERICOID);
            createSeqOwnedByTable(cxt, column, false, large);
        } else if (column->cooked_default != NULL) {
            checkTableLikeSequence(column->cooked_default);
        }
    }
}

/*
 * Get all tag for tsdb
 */

static DistributeBy* GetHideTagDistribution(TupleDesc tupleDesc)
{
    DistributeBy* distributeby = makeNode(DistributeBy);
    distributeby->disttype = DISTTYPE_HASH;
    for (int attno = 1; attno <= tupleDesc->natts; attno++) {
        Form_pg_attribute attribute = tupleDesc->attrs[attno - 1];
        char* attributeName = NameStr(attribute->attname);
        if (attribute->attkvtype == ATT_KV_TAG) {
            distributeby->colname = lappend(distributeby->colname, makeString(attributeName));
        }
    }
    return distributeby;
}

/*
 * Support Create table like on table with subpartitions
 * In transform phase, we need fill PartitionState
 * 1. Recursively fill partitionList in PartitionState also including subpartitionList
 * 2. Recursively fill PartitionState also including SubPartitionState
 */
static PartitionState *transformTblSubpartition(Relation relation, HeapTuple partitionTuple,
                                    List* partitionList, List* subPartitionList)
{
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    ListCell *lc3 = NULL;

    List *partKeyColumns = NIL;
    List *partitionDefinitions = NIL;
    PartitionState *partState = NULL;
    PartitionState *subPartState = NULL;
    Form_pg_partition tupleForm = NULL;
    Form_pg_partition partitionForm = NULL;
    Form_pg_partition subPartitionForm = NULL;

    tupleForm = (Form_pg_partition)GETSTRUCT(partitionTuple);

    /* prepare partition definitions */
    transformTableLikePartitionProperty(
        relation, partitionTuple, &partKeyColumns, partitionList, &partitionDefinitions);

    partState = makeNode(PartitionState);
    partState->partitionKey = partKeyColumns;
    partState->partitionList = partitionDefinitions;
    partState->partitionStrategy = tupleForm->partstrategy;

    partState->rowMovement = relation->rd_rel->relrowmovement ? ROWMOVEMENT_ENABLE : ROWMOVEMENT_DISABLE;

    /* prepare subpartition definitions */
    forboth(lc1, partitionList, lc2, subPartitionList) {
        List *subPartKeyColumns = NIL;
        List *subPartitionDefinitions = NIL;
        RangePartitionDefState *partitionDef = NULL;

        HeapTuple partTuple = (HeapTuple)lfirst(lc1);
        List *subPartitions = (List *)lfirst(lc2);

        HeapTuple subPartTuple = (HeapTuple)linitial(subPartitions);
        subPartitionForm = (Form_pg_partition)GETSTRUCT(subPartTuple);
        partitionForm = (Form_pg_partition)GETSTRUCT(partTuple);

        if (subPartitionForm->partstrategy != PART_STRATEGY_RANGE) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("Create Table like with subpartition only support range strategy.")));
        }

        Oid partOid = HeapTupleGetOid(partTuple);
        Partition part = partitionOpen(relation, partOid, AccessShareLock);
        Relation partRel = partitionGetRelation(relation, part);

        transformTableLikePartitionProperty(
            partRel, partTuple, &subPartKeyColumns, subPartitions, &subPartitionDefinitions);

        if (subPartState == NULL) {
            subPartState = makeNode(PartitionState);
            subPartState->partitionKey = subPartKeyColumns;
            subPartState->partitionList = NULL;
            subPartState->partitionStrategy = subPartitionForm->partstrategy;
        }

        /* Here do this for reserve origin subpartitions order */
        foreach(lc3, partitionDefinitions) {
            RangePartitionDefState *rightDef = (RangePartitionDefState*)lfirst(lc3);

            if (pg_strcasecmp(NameStr(partitionForm->relname), rightDef->partitionName) == 0) {
                partitionDef = rightDef;
                break;
            }
        }

        Assert(partitionDef != NULL);
        partitionDef->subPartitionDefState = subPartitionDefinitions;

        releaseDummyRelation(&partRel);
        partitionClose(relation, part, NoLock);
    }

    partState->subPartitionState = subPartState;

    return partState;
}

/*
 * transformTableLikeClause
 *
 * Change the LIKE <srctable> portion of a CREATE TABLE statement into
 * column definitions which recreate the user defined column portions of
 * <srctable>.
 */
static void transformTableLikeClause(
    CreateStmtContext* cxt, TableLikeClause* table_like_clause, bool preCheck, bool isFirstNode, bool is_row_table, TransformTableType transformType)
{
    AttrNumber parent_attno;
    Relation relation;
    TupleDesc tupleDesc;
    TupleConstr* constr = NULL;
    AttrNumber* attmap = NULL;
    AclResult aclresult;
    char* comment = NULL;
    ParseCallbackState pcbstate;
    TableLikeCtx meta_info;
    bool multi_nodegroup = false;
    errno_t rc;

    setup_parser_errposition_callback(&pcbstate, cxt->pstate, table_like_clause->relation->location);

    /*
     * We may run into a case where LIKE clause happens between two tables with different
     * node groups, we don't check validation in coordinator nodes as in cluster expansion
     * scenarios we first dump/restore table's metadata in new added DNs without sync
     * pgxc_class, then invoke LIKE command. So we have to allow a case where source table's
     * nodegroup fully include target table's
     */
    if (IS_PGXC_DATANODE) {
        RangeVar* relvar = table_like_clause->relation;
        Oid relid = RangeVarGetRelidExtended(relvar, NoLock, true, false, false, true, NULL, NULL);
        if (relid == InvalidOid) {
            if (cxt->internalData != NULL) {
                cancel_parser_errposition_callback(&pcbstate);
                transformTableLikeFromSerialData(cxt, table_like_clause);
                return;
            }

            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("Table %s.%s does not exist in current datanode.", relvar->schemaname, relvar->relname)));
        }
    }

    relation = relation_openrv_extended(table_like_clause->relation, AccessShareLock, false, true);

    if (relation->rd_rel->relkind != RELKIND_RELATION &&
        relation->rd_rel->relkind != RELKIND_VIEW &&
        relation->rd_rel->relkind != RELKIND_CONTQUERY &&
        relation->rd_rel->relkind != RELKIND_MATVIEW &&
        relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
        relation->rd_rel->relkind != RELKIND_STREAM &&
        relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a table, view, composite type, or foreign table",
                    RelationGetRelationName(relation))));

    cancel_parser_errposition_callback(&pcbstate);

    // If specify 'INCLUDING ALL' for non-partitioned table, just remove the option 'INCLUDING PARTITION'.
    // Right shift 10 bits can handle both 'INCLUDING ALL' and 'INCLUDING ALL EXCLUDING option(s)'.
    // if add a new option, the number '10'(see marco 'MAX_TABLE_LIKE_OPTIONS') should be changed.
    if ((table_like_clause->options >> MAX_TABLE_LIKE_OPTIONS) && !RELATION_IS_PARTITIONED(relation) &&
        !RelationIsValuePartitioned(relation))
        table_like_clause->options = table_like_clause->options & ~CREATE_TABLE_LIKE_PARTITION;

    if (table_like_clause->options & CREATE_TABLE_LIKE_PARTITION) {
        if (RELATION_ISNOT_REGULAR_PARTITIONED(relation)) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("could not specify \"INCLUDING PARTITION\" for non-partitioned-table relation:\"%s\"",
                        RelationGetRelationName(relation))));
        }
        if (cxt->csc_partTableState != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("could not specify 2 or more \"INCLUDING PARTITION\" clauses, only one is allowed")));
        }
    }

    if (table_like_clause->options & CREATE_TABLE_LIKE_RELOPTIONS) {
        if (cxt->reloptions != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("could not specify 2 or more \"INCLUDING RELOPTIONS\" clauses, only one is allowed")));
        }
    }

    if (table_like_clause->options & CREATE_TABLE_LIKE_DISTRIBUTION) {
        if (cxt->distributeby != NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("could not specify 2 or more \"INCLUDING DISTRIBUTION\" clauses, only one is allowed")));
        }
    }

    /* Initialize meta_info struct. */
    rc = memset_s(&meta_info, sizeof(meta_info), 0, sizeof(meta_info));
    securec_check_ss(rc, "\0", "\0");

#ifdef PGXC
    /*
     * Check if relation is temporary and assign correct flag.
     * This will override transaction direct commit as no 2PC
     * can be used for transactions involving temporary objects.
     */
    if (IsTempTable(RelationGetRelid(relation))) {
        table_like_clause->relation->relpersistence = RELPERSISTENCE_TEMP;
        ExecSetTempObjectIncluded();
        meta_info.temp_table = true;
    }

    /*
     * Block the creation of tables using views in their LIKE clause.
     * Views are not created on Datanodes, so this will result in an error
     * In order to fix this problem, it will be necessary to
     * transform the query string of CREATE TABLE into something not using
     * the view definition. Now openGauss only uses the raw string...
     * There is some work done with event triggers in 9.3, so it might
     * be possible to use that code to generate the SQL query to be sent to
     * remote nodes. When this is done, this error will be removed.
     */
    if (relation->rd_rel->relkind == RELKIND_VIEW || relation->rd_rel->relkind == RELKIND_CONTQUERY)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("openGauss does not support VIEW in LIKE clauses"),
                errdetail("The feature is not currently supported")));
#endif

    /*
     *  Judge whether create table ... like in multiple node group or not.
     *  If multi_nodegroup is true, table metainfo need to append to meta_info fields.
     *  At the end of transformTableLikeClause, meta_info need to serialize to string for datanodes.
     */
    multi_nodegroup = false;
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        multi_nodegroup = is_multi_nodegroup_createtbllike(cxt->subcluster, relation->rd_id);
    }

    /*
     * Check for privileges
     */
    if (relation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) {
        aclresult = pg_type_aclcheck(relation->rd_rel->reltype, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_TYPE, RelationGetRelationName(relation));
    } else {
        /*
         * Just return aclok when current user is superuser, although pg_class_aclcheck
         * also used superuser() function but it forbid the INSERT/DELETE/SELECT/UPDATE
         * for superuser in independent condition. Here CreateLike is no need to forbid.
         */
        if (superuser())
            aclresult = ACLCHECK_OK;
        else
            aclresult = pg_class_aclcheck(RelationGetRelid(relation), GetUserId(), ACL_SELECT);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_CLASS, RelationGetRelationName(relation));
    }

    tupleDesc = RelationGetDescr(relation);
    constr = tupleDesc->constr;

    /*
     * Initialize column number map for map_variable_attnos().  We need this
     * since dropped columns in the source table aren't copied, so the new
     * table can have different column numbers.
     */
    attmap = (AttrNumber*)palloc0(sizeof(AttrNumber) * tupleDesc->natts);

    /*
     * Insert the copied attributes into the cxt for the new table definition.
     */
    bool hideTag = false;
    for (parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++) {
        Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];
        char* attributeName = NameStr(attribute->attname);
        ColumnDef* def = NULL;

        /*
         * Ignore dropped columns in the parent.  attmap entry is left zero.
         */
        if (attribute->attisdropped && (!u_sess->attr.attr_sql.enable_cluster_resize || RelationIsTsStore(relation)))
            continue;
        /*
         * Ignore hide tag(tsdb)
         */
        if (attribute->attkvtype == ATT_KV_HIDE && table_like_clause->options != CREATE_TABLE_LIKE_ALL) {
            hideTag = true;
            continue;
        }

        if (u_sess->attr.attr_sql.enable_cluster_resize && attribute->attisdropped) {
            def = makeNode(ColumnDef);
            def->type = T_ColumnDef;
            def->colname = pstrdup(attributeName);
            def->dropped_attr = (Form_pg_attribute)palloc0(sizeof(FormData_pg_attribute));
            copyDroppedAttribute(def->dropped_attr, attribute);
        } else {
            /*
             * Create a new column, which is marked as NOT inherited.
             *
             * For constraints, ONLY the NOT NULL constraint is inherited by the
             * new column definition per SQL99.
             */
            def = makeNode(ColumnDef);
            def->colname = pstrdup(attributeName);
            if (attribute->atttypid == BYTEAWITHOUTORDERCOLOID || attribute->atttypid == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
                /* this is client logic column. need to rewrite */
                    HeapTuple   col_tup = SearchSysCache2(CERELIDCOUMNNAME, 
                        ObjectIdGetDatum(RelationGetRelid(relation)), PointerGetDatum (def->colname));
                    
                    if (!col_tup) {
                        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CL_COLUMN),
                            errmsg("could not find encrypted column for %s", def->colname)));
                    } else {
                        
                        Form_gs_encrypted_columns columns_rel_data = (Form_gs_encrypted_columns)GETSTRUCT(col_tup);
                        const Oid ColSettingOid  = columns_rel_data->column_key_id;
                        HeapTuple col_setting_tup  = SearchSysCache1(COLUMNSETTINGOID, ObjectIdGetDatum(ColSettingOid));
                        if(!col_setting_tup) {
                            	ReleaseSysCache(col_tup);
                                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_KEY),
                                    errmsg("could not find column encryption keys for column %s", def->colname)));
                        }
                        Form_gs_column_keys column_settings_rel_data = (Form_gs_column_keys)GETSTRUCT(col_setting_tup);
                        def->clientLogicColumnRef = makeNode(ClientLogicColumnRef);
                        def->clientLogicColumnRef->column_key_name =
                                list_make1(makeString(NameStr(column_settings_rel_data->column_key_name)));
                        def->clientLogicColumnRef->columnEncryptionAlgorithmType = static_cast<EncryptionType>(columns_rel_data->encryption_type);
                        def->clientLogicColumnRef->orig_typname = makeTypeNameFromOid(columns_rel_data->data_type_original_oid,
                                                columns_rel_data->data_type_original_mod);;
                        def->clientLogicColumnRef->dest_typname =
                            makeTypeNameFromOid(attribute->atttypid, attribute->atttypmod);
                        def->typname = makeTypeNameFromOid(columns_rel_data->data_type_original_oid,
                            columns_rel_data->data_type_original_mod);
                        ReleaseSysCache(col_tup);
                        ReleaseSysCache(col_setting_tup);
                    }
            } else {
                def->typname = makeTypeNameFromOid(attribute->atttypid, attribute->atttypmod);
            }
            def->kvtype = attribute->attkvtype;
            def->inhcount = 0;
            def->is_local = true;
            def->is_not_null = attribute->attnotnull;
            def->is_from_type = false;
            def->storage = 0;
            /* copy compression mode from source table */
            def->cmprs_mode = attribute->attcmprmode;
            /*
             * When analyzing a column-oriented table with default_statistics_target < 0, we will create a row-oriented
             * temp table. In this case, ignore the compression flag.
             */
            if (u_sess->analyze_cxt.is_under_analyze || (IsConnFromCoord() && is_row_table)) {
                if (def->cmprs_mode != ATT_CMPR_UNDEFINED) {
                    def->cmprs_mode = ATT_CMPR_NOCOMPRESS;
                }
            }
            def->raw_default = NULL;
            def->cooked_default = NULL;
            def->collClause = NULL;
            def->collOid = attribute->attcollation;
            def->constraints = NIL;
            def->dropped_attr = NULL;
        }

        /*
         * Add to column list
         */
        cxt->columns = lappend(cxt->columns, def);

        attmap[parent_attno - 1] = list_length(cxt->columns);

        /*
         * Copy default, if present and the default has been requested
         */
        if (attribute->atthasdef) {
            Node* this_default = NULL;
            AttrDefault* attrdef = NULL;
            int i;
            Oid seqId = InvalidOid;

            /* Find default in constraint structure */
            Assert(constr != NULL);
            attrdef = constr->defval;
            for (i = 0; i < constr->num_defval; i++) {
                if (attrdef[i].adnum == parent_attno) {
                    this_default = (Node*)stringToNode_skip_extern_fields(attrdef[i].adbin);
                    break;
                }
            }
            Assert(this_default != NULL);

            /*
             *  Whether default expr is serial type and the sequence is owned by the table.
             */
            if (!IsCreatingSeqforAnalyzeTempTable(cxt) && (table_like_clause->options & CREATE_TABLE_LIKE_DEFAULTS_SERIAL)) {
                seqId = searchSeqidFromExpr(this_default);
                if (OidIsValid(seqId)) {
                    List* seqs = getOwnedSequences(relation->rd_id);
                    if (seqs != NULL && list_member_oid(seqs, DatumGetObjectId(seqId))) {
                        /* is serial type */
                        def->is_serial = true;
                        bool large = (get_rel_relkind(seqId) == RELKIND_LARGE_SEQUENCE);
                        /* Special actions for SERIAL pseudo-types */

                        createSeqOwnedByTable(cxt, def, preCheck, large);
                    }
                }
            }

            if (!def->is_serial && (table_like_clause->options & CREATE_TABLE_LIKE_DEFAULTS) &&
                !GetGeneratedCol(tupleDesc, parent_attno - 1)) {
                /*
                 * If default expr could contain any vars, we'd need to fix 'em,
                 * but it can't; so default is ready to apply to child.
                 */
                def->cooked_default = this_default;
            } else if (!def->is_serial && (table_like_clause->options & CREATE_TABLE_LIKE_GENERATED) &&
                GetGeneratedCol(tupleDesc, parent_attno - 1)) {
                bool found_whole_row = false;
                def->cooked_default =
                    map_variable_attnos(this_default, 1, 0, attmap, tupleDesc->natts, &found_whole_row);
                if (found_whole_row) {
                    ereport(ERROR, (errmodule(MOD_GEN_COL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert whole-row table reference"),
                        errdetail(
                        "Generation expression for column \"%s\" contains a whole-row reference to table \"%s\".",
                        attributeName, RelationGetRelationName(relation))));
                }
                def->generatedCol = GetGeneratedCol(tupleDesc, parent_attno - 1);
            }
        }

        /* Likewise, copy storage if requested */
        if (table_like_clause->options & CREATE_TABLE_LIKE_STORAGE)
            def->storage = attribute->attstorage;

        if (multi_nodegroup) {
            /*need to copy ColumnDef deeply because we will modify it.*/
            ColumnDef* dup = (ColumnDef*)copyObject(def);
            if (def->is_serial) {
                /* Momory will be freed when ExecutorEnd  */
                dup->constraints = NULL;
                dup->raw_default = NULL;
            }
            meta_info.columns = lappend(meta_info.columns, dup);
        }

        /* Likewise, copy comment if requested */
        if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
            (comment = GetComment(attribute->attrelid, RelationRelationId, attribute->attnum)) != NULL) {
            CommentStmt* stmt = (CommentStmt*)makeNode(CommentStmt);

            stmt->objtype = OBJECT_COLUMN;
            stmt->objname = list_make3(
                makeString(cxt->relation->schemaname), makeString(cxt->relation->relname), makeString(def->colname));
            stmt->objargs = NIL;
            stmt->comment = comment;

            cxt->alist = lappend(cxt->alist, stmt);

            if (multi_nodegroup) {
                /* don't need to copy CommentStmt deeply */
                meta_info.comments = lappend(meta_info.comments, stmt);
            }
        }
    }

    /*
     * Copy CHECK constraints if requested, being careful to adjust attribute
     * numbers so they match the child.
     */
    if ((table_like_clause->options & CREATE_TABLE_LIKE_CONSTRAINTS) && tupleDesc->constr) {
        int ccnum;

        /* check expr constraint */
        for (ccnum = 0; ccnum < tupleDesc->constr->num_check; ccnum++) {
            char* ccname = tupleDesc->constr->check[ccnum].ccname;
            char* ccbin = tupleDesc->constr->check[ccnum].ccbin;
            Constraint* n = makeNode(Constraint);
            Node* ccbin_node = NULL;
            bool found_whole_row = false;

            ccbin_node =
                map_variable_attnos((Node*)stringToNode(ccbin), 1, 0, attmap, tupleDesc->natts, &found_whole_row);

            /*
             * We reject whole-row variables because the whole point of LIKE
             * is that the new table's rowtype might later diverge from the
             * parent's.  So, while translation might be possible right now,
             * it wouldn't be possible to guarantee it would work in future.
             */
            if (found_whole_row)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert whole-row table reference"),
                        errdetail("Constraint \"%s\" contains a whole-row reference to table \"%s\".",
                            ccname,
                            RelationGetRelationName(relation))));

            n->contype = CONSTR_CHECK;
            n->location = -1;
            n->conname = pstrdup(ccname);
            n->raw_expr = NULL;
            n->cooked_expr = nodeToString(ccbin_node);
            cxt->ckconstraints = lappend(cxt->ckconstraints, n);
            if (multi_nodegroup) {
                /* don't need to copy Constraint deeply */
                meta_info.ckconstraints = lappend(meta_info.ckconstraints, n);
            }

            /* Copy comment on constraint */
            if ((table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) &&
                (comment = GetComment(get_relation_constraint_oid(RelationGetRelid(relation), n->conname, false),
                    ConstraintRelationId,
                    0)) != NULL) {
                CommentStmt* stmt = makeNode(CommentStmt);

                stmt->objtype = OBJECT_CONSTRAINT;
                stmt->objname = list_make3(
                    makeString(cxt->relation->schemaname), makeString(cxt->relation->relname), makeString(n->conname));
                stmt->objargs = NIL;
                stmt->comment = comment;

                cxt->alist = lappend(cxt->alist, stmt);
                if (multi_nodegroup) {
                    /* don't need to copy CommentStmt deeply */
                    meta_info.comments = lappend(meta_info.comments, stmt);
                }
            }
        }

        /* paritial cluster key constraint like */
        if (tupleDesc->constr->clusterKeyNum > 0) {
            int pckNum;
            Constraint* n = makeNode(Constraint);

            for (pckNum = 0; pckNum < tupleDesc->constr->clusterKeyNum; pckNum++) {
                AttrNumber attrNum = tupleDesc->constr->clusterKeys[pckNum];
                Form_pg_attribute attribute = tupleDesc->attrs[attrNum - 1];
                char* attrName = NameStr(attribute->attname);

                n->contype = CONSTR_CLUSTER;
                n->location = -1;
                n->keys = lappend(n->keys, makeString(pstrdup(attrName)));
            }

            cxt->clusterConstraints = lappend(cxt->clusterConstraints, n);

            if (multi_nodegroup) {
                /* don't need to copy Constraint deeply */
                meta_info.cluster_keys = lappend(meta_info.cluster_keys, n);
            }

            /* needn't copy comment on partial cluster key constraint
             * the constraint name was not like the source, refer to primary/unique constraint
             */
        }
    }

    /*
     * Likewise, copy partition definitions if requested. Then, copy index,
     * because partitioning might have effect on how to create indexes
     */
    if (table_like_clause->options & CREATE_TABLE_LIKE_PARTITION) {
        PartitionState* n = NULL;
        HeapTuple partitionTableTuple = NULL;
        Form_pg_partition partitionForm = NULL;
        List* partitionList = NIL;
        List* subPartitionList = NIL;

        // read out partitioned table tuple, and partition tuple list
        partitionTableTuple =
            searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, ObjectIdGetDatum(relation->rd_id));
        partitionList = searchPgPartitionByParentId(PART_OBJ_TYPE_TABLE_PARTITION, ObjectIdGetDatum(relation->rd_id));

        if (RelationIsSubPartitioned(relation)) {
            subPartitionList = searchPgSubPartitionByParentId(PART_OBJ_TYPE_TABLE_SUB_PARTITION, partitionList);
        }

        if (partitionTableTuple != NULL) {
            partitionForm = (Form_pg_partition)GETSTRUCT(partitionTableTuple);

            if (partitionForm->partstrategy == PART_STRATEGY_LIST ||
                partitionForm->partstrategy == PART_STRATEGY_HASH) {
                freePartList(partitionList);
                heap_freetuple_ext(partitionTableTuple);
                heap_close(relation, NoLock);
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature"),
                        errdetail("The Like feature is not supported currently for List and Hash.")));
            }
            bool value_partition_rel = (partitionForm->partstrategy == PART_STRATEGY_VALUE);

            /*
             * We only have to create PartitionState for a range partition table
             * with known partitions or a value partition table(HDFS).
             */
            if ((NIL != partitionList && subPartitionList == NIL) || value_partition_rel) {
                {
                    List* partKeyColumns = NIL;
                    List* partitionDefinitions = NIL;

                    transformTableLikePartitionProperty(
                        relation, partitionTableTuple, &partKeyColumns, partitionList, &partitionDefinitions);

                    // set PartitionState fields, 5 following
                    // (1)partition key
                    // (2)partition definition list
                    // (3)interval definition
                    // (4)partitionStrategy
                    // (5)rowMovement
                    n = makeNode(PartitionState);
                    n->partitionKey = partKeyColumns;
                    n->partitionList = partitionDefinitions;
                    n->partitionStrategy = partitionForm->partstrategy;
                    if (partitionForm->partstrategy == PART_STRATEGY_INTERVAL) {
                        n->intervalPartDef = TransformTableLikeIntervalPartitionDef(partitionTableTuple);
                    } else {
                        n->intervalPartDef = NULL;
                    }

                    n->rowMovement = relation->rd_rel->relrowmovement ? ROWMOVEMENT_ENABLE : ROWMOVEMENT_DISABLE;

                    // store the produced partition state in CreateStmtContext
                    cxt->csc_partTableState = n;

                    freePartList(partitionList);
                }
            } else if (subPartitionList != NULL) {
                n = transformTblSubpartition(relation,
                            partitionTableTuple,
                            partitionList,
                            subPartitionList);

                /* store the produced partition state in CreateStmtContext */
                cxt->csc_partTableState = n;

                freePartList(partitionList);
                freePartList(subPartitionList);
            }

            heap_freetuple_ext(partitionTableTuple);
        }
    }

    /*
     * Likewise, copy indexes if requested
     */
    if ((table_like_clause->options & CREATE_TABLE_LIKE_INDEXES) && relation->rd_rel->relhasindex) {
        List* parent_indexes = NIL;
        ListCell* l = NULL;
        parent_indexes = RelationGetIndexList(relation);

        foreach (l, parent_indexes) {
            Oid parent_index_oid = lfirst_oid(l);
            Relation parent_index;
            IndexStmt* index_stmt = NULL;

            parent_index = index_open(parent_index_oid, AccessShareLock);

            /* Build CREATE INDEX statement to recreate the parent_index */
            index_stmt = generateClonedIndexStmt(cxt, parent_index, attmap, tupleDesc->natts, relation, transformType);

            /* Copy comment on index, if requested */
            if (table_like_clause->options & CREATE_TABLE_LIKE_COMMENTS) {
                comment = GetComment(parent_index_oid, RelationRelationId, 0);

                /*
                 * We make use of IndexStmt's idxcomment option, so as not to
                 * need to know now what name the index will have.
                 */
                index_stmt->idxcomment = comment;
            }

            /* Save it in the inh_indexes list for the time being */
            cxt->inh_indexes = lappend(cxt->inh_indexes, index_stmt);

            index_close(parent_index, AccessShareLock);
        }
    }

    /*
     * Likewise, copy reloptions if requested
     */
    if (table_like_clause->options & CREATE_TABLE_LIKE_RELOPTIONS) {
        Datum reloptions = (Datum)0;
        bool isNull = false;
        HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relation->rd_id));

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed on source like relation %u for reloptions", relation->rd_id)));
        reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isNull);
        if (isNull)
            reloptions = (Datum)0;
        cxt->reloptions = untransformRelOptions(reloptions);

        /* remove on_commit_delete_rows option */
        if (cxt->relation->relpersistence != RELPERSISTENCE_GLOBAL_TEMP) {
            cxt->reloptions = RemoveRelOption(cxt->reloptions, "on_commit_delete_rows", NULL);
        }

        /* remove redis options first. */
        RemoveRedisRelOptionsFromList(&(cxt->reloptions));

        meta_info.reloptions = cxt->reloptions;

        ReleaseSysCache(tuple);
    }
#ifdef PGXC
    /*
     * Likewise, copy distribution if requested
     */
    if (table_like_clause->options & CREATE_TABLE_LIKE_DISTRIBUTION) {
        cxt->distributeby = (IS_PGXC_COORDINATOR) ? 
                        (hideTag ? GetHideTagDistribution(tupleDesc) : getTableDistribution(relation->rd_id)) : 
                        getTableHBucketDistribution(relation);
    }
#endif

    /*
     * Likewise, copy oids if requested
     */
    if (table_like_clause->options & CREATE_TABLE_LIKE_OIDS) {
        cxt->hasoids = tupleDesc->tdhasoid;
    }

    if (multi_nodegroup) {
        meta_info.type = T_TableLikeCtx;
        meta_info.options = table_like_clause->options;
        meta_info.hasoids = cxt->hasoids;

        /* partition info and inh_indexes is only from transformTableLikeClause,
         *  so we don't need to copy them.
         */
        meta_info.partition = cxt->csc_partTableState;
        meta_info.inh_indexes = cxt->inh_indexes;

        cxt->internalData = nodeToString(&meta_info);

        /* Memory of meta_info will be freed when ExecutorEnd  */
    }

    if (u_sess->attr.attr_sql.enable_cluster_resize) {
         cxt->isResizing = RelationInClusterResizing(relation);
    }

    /*
     * Close the parent rel, but keep our AccessShareLock on it until xact
     * commit.	That will prevent someone else from deleting or ALTERing the
     * parent before the child is committed.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !isFirstNode)
        heap_close(relation, AccessShareLock);
    else
        heap_close(relation, NoLock);
}

// this function is used to output 2 list,
// one for partitionkey, a list of column ref,
// another for partiton boundary, a list of
static void transformTableLikePartitionProperty(Relation relation, HeapTuple partitionTableTuple, List** partKeyColumns,
    List* partitionList, List** partitionDefinitions)
{
    List* partKeyPosList = NIL;
    transformTableLikePartitionKeys(relation, partitionTableTuple, partKeyColumns, &partKeyPosList);
    transformTableLikePartitionBoundaries(relation, partKeyPosList, partitionList, partitionDefinitions);
}

static IntervalPartitionDefState* TransformTableLikeIntervalPartitionDef(HeapTuple partitionTableTuple)
{
    IntervalPartitionDefState* intervalPartDef = makeNode(IntervalPartitionDefState);
    Relation partitionRel = relation_open(PartitionRelationId, RowExclusiveLock);
    char* intervalStr = ReadIntervalStr(partitionTableTuple, RelationGetDescr(partitionRel));
    Assert(intervalStr != NULL);
    intervalPartDef->partInterval = makeAConst(makeString(intervalStr), -1);
    oidvector* tablespaceIdVec = ReadIntervalTablespace(partitionTableTuple, RelationGetDescr(partitionRel));
    intervalPartDef->intervalTablespaces = NULL;
    if (tablespaceIdVec != NULL && tablespaceIdVec->dim1 > 0) {
        for (int i = 0; i < tablespaceIdVec->dim1; ++i) {
            char* tablespaceName = get_tablespace_name(tablespaceIdVec->values[i]);
            if (tablespaceName == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("tablespace with OID %u does not exist", tablespaceIdVec->values[i])));
            }
            intervalPartDef->intervalTablespaces =
                lappend(intervalPartDef->intervalTablespaces, makeString(tablespaceName));
        }
    }

    relation_close(partitionRel, RowExclusiveLock);
    return intervalPartDef;
}

static void transformTableLikePartitionKeys(
    Relation relation, HeapTuple partitionTableTuple, List** partKeyColumns, List** partKeyPosList)
{
    ColumnRef* c = NULL;
    Relation partitionRel = NULL;
    TupleDesc relationTupleDesc = NULL;
    Form_pg_attribute* relationAtts = NULL;
    int relationAttNumber = 0;
    Datum partkey_raw = (Datum)0;
    ArrayType* partkey_columns = NULL;
    int16* attnums = NULL;
    bool isNull = false;
    int n_key_column, i;

    /* open pg_partition catalog */
    partitionRel = relation_open(PartitionRelationId, RowExclusiveLock);

    /* Get the raw data which contain patition key's columns */
    partkey_raw = heap_getattr(partitionTableTuple, Anum_pg_partition_partkey, RelationGetDescr(partitionRel), &isNull);
    /* if the raw value of partition key is null, then report error */
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null partition key value for relation \"%s\"", RelationGetRelationName(relation))));
    }
    /* convert Datum to ArrayType */
    partkey_columns = DatumGetArrayTypeP(partkey_raw);

    /* Get number of partition key columns from int2verctor*/
    n_key_column = ARR_DIMS(partkey_columns)[0];

    /* CHECK: the ArrayType of partition key is valid */
    if (ARR_NDIM(partkey_columns) != 1 || n_key_column < 0 || ARR_HASNULL(partkey_columns) ||
        ARR_ELEMTYPE(partkey_columns) != INT2OID) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("partition key column's number of relation \"%s\" is not a 1-D smallint array",
                    RelationGetRelationName(relation))));
    }

    AssertEreport(n_key_column <= RANGE_PARTKEYMAXNUM, MOD_OPT, "");

    /* Get int2 array of partition key column numbers */
    attnums = (int16*)ARR_DATA_PTR(partkey_columns);

    /*
     * get the partition key number,
     * make ColumnRef node from name of partition key
     */
    relationTupleDesc = relation->rd_att;
    relationAttNumber = relationTupleDesc->natts;
    relationAtts = relationTupleDesc->attrs;

    for (i = 0; i < n_key_column; i++) {
        int attnum = (int)(attnums[i]);
        if (attnum >= 1 && attnum <= relationAttNumber) {
            c = makeNode(ColumnRef);
            c->fields = list_make1(makeString(pstrdup(NameStr(relationAtts[attnum - 1]->attname))));
            *partKeyColumns = lappend(*partKeyColumns, c);
            *partKeyPosList = lappend_int(*partKeyPosList, attnum - 1);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                    errmsg("partition key column's number of %s not in the range of all its columns",
                        RelationGetRelationName(relation))));
        }
    }

    /* close pg_partition catalog */
    relation_close(partitionRel, RowExclusiveLock);
}

static void transformTableLikePartitionBoundaries(
    Relation relation, List* partKeyPosList, List* partitionList, List** partitionDefinitions)
{
    ListCell* partitionCell = NULL;
    List* orderedPartitionList = NIL;

    if (relation->partMap == NULL)
        return;

    // form into a new ordered list
    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionMap* rangePartMap = (RangePartitionMap*)relation->partMap;
        int i;
        int rangePartitions = rangePartMap->rangeElementsNum;

        for (i = 0; i < rangePartitions; i++) {
            Oid partitionOid = rangePartMap->rangeElements[i].partitionOid;

            foreach (partitionCell, partitionList) {
                HeapTuple partitionTuple = (HeapTuple)lfirst(partitionCell);
                if (partitionOid == HeapTupleGetOid(partitionTuple)) {
                    orderedPartitionList = lappend(orderedPartitionList, partitionTuple);
                    break;
                }
            }
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("\" including partition \" for list partitioned relation: \"%s\" not implemented yet",
                    RelationGetRelationName(relation))));
    }
    /* open pg_partition catalog */
    Relation partitionRel = relation_open(PartitionRelationId, AccessShareLock);

    foreach (partitionCell, orderedPartitionList) {
        HeapTuple partitionTuple = (HeapTuple)lfirst(partitionCell);
        Form_pg_partition partitionForm = (Form_pg_partition)GETSTRUCT(partitionTuple);
        /* no need to copy interval partition */
        if (partitionForm->partstrategy == PART_STRATEGY_INTERVAL) {
            continue;
        }
        bool attIsNull = false;
        Datum tableSpace = (Datum)0;
        Datum boundaries = (Datum)0;
        RangePartitionDefState* partitionNode = NULL;

        // in mppdb, we only support range partition by now(2014.05)
        // so here produce RangePartitionDefState node
        partitionNode = makeNode(RangePartitionDefState);

        // set RangePartitionDefState: 1.partition name
        partitionNode->partitionName = pstrdup(NameStr(partitionForm->relname));

        // set RangePartitionDefState: 2.partition tablespace
        tableSpace =
            heap_getattr(partitionTuple, Anum_pg_partition_reltablespace, RelationGetDescr(partitionRel), &attIsNull);
        if (attIsNull)
            partitionNode->tablespacename = NULL;
        else
            partitionNode->tablespacename = get_tablespace_name(DatumGetObjectId(tableSpace));

        // set RangePartitionDefState: 3.boundaries
        boundaries =
            heap_getattr(partitionTuple, Anum_pg_partition_boundaries, RelationGetDescr(partitionRel), &attIsNull);
        if (attIsNull) {
            partitionNode->boundary = NIL;
        } else {
            /* unstransform string items to Value list */
            List* boundaryValueList = NIL;
            List* resultBoundaryList = NIL;
            ListCell* boundaryCell = NULL;
            ListCell* partKeyCell = NULL;
            Value* boundaryValue = NULL;
            Datum boundaryDatum = (Datum)0;
            Node* boundaryNode = NULL;
            Form_pg_attribute* relation_atts = NULL;
            Form_pg_attribute att = NULL;
            int partKeyPos = 0;
            int16 typlen = 0;
            bool typbyval = false;
            char typalign;
            char typdelim;
            Oid typioparam = InvalidOid;
            Oid func = InvalidOid;
            Oid typid = InvalidOid;
            Oid typelem = InvalidOid;
            Oid typcollation = InvalidOid;
            int32 typmod = -1;

            boundaryValueList = untransformPartitionBoundary(boundaries);

            // transform Value(every is string Value) node into Const node.
            // (1)the first step is transform text into datum,
            // (2)then datum into corresponding int, float or string format
            // (3)the last step is make A_Const node using int, float or string
            relation_atts = relation->rd_att->attrs;
            forboth(boundaryCell, boundaryValueList, partKeyCell, partKeyPosList)
            {
                boundaryValue = (Value*)lfirst(boundaryCell);
                partKeyPos = (int)lfirst_int(partKeyCell);
                att = relation_atts[partKeyPos];

                /* get the oid/mod/collation/ of partition key */
                typid = att->atttypid;
                typmod = att->atttypmod;
                typcollation = att->attcollation;
                /* deal with null */
                if (!PointerIsValid(boundaryValue->val.str)) {
                    boundaryNode = (Node*)makeMaxConst(typid, typmod, typcollation);
                } else {
                    /* get the typein function's oid of current type */
                    get_type_io_data(typid, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &func);
                    typelem = get_element_type(typid);

                    /* now call the typein function with collation,string, element_type, typemod
                     * as it's parameters.
                     */
                    boundaryDatum = OidFunctionCall3Coll(func,
                        typcollation,
                        CStringGetDatum(boundaryValue->val.str),
                        ObjectIdGetDatum(typelem),
                        Int32GetDatum(typmod));

                    // produce const node
                    boundaryNode =
                        (Node*)makeConst(typid, typmod, typcollation, typlen, boundaryDatum, false, typbyval);
                }
                resultBoundaryList = lappend(resultBoundaryList, boundaryNode);
            }
            partitionNode->boundary = resultBoundaryList;
        }

        // now, append the result RangePartitionDefState node to output list
        *partitionDefinitions = lappend(*partitionDefinitions, partitionNode);
    }
    /* close pg_partition catalog */
    relation_close(partitionRel, AccessShareLock);

    // free the new ordered list
    list_free_ext(orderedPartitionList);
}

static void transformOfType(CreateStmtContext* cxt, TypeName* ofTypename)
{
    HeapTuple tuple;
    TupleDesc tupdesc;
    int i;
    Oid ofTypeId;

    AssertArg(ofTypename);

    tuple = typenameType(NULL, ofTypename, NULL);
    check_of_type(tuple);
    ofTypeId = HeapTupleGetOid(tuple);
    ofTypename->typeOid = ofTypeId; /* cached for later */

    tupdesc = lookup_rowtype_tupdesc(ofTypeId, -1);
    for (i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        ColumnDef* n = NULL;

        if (attr->attisdropped)
            continue;

        n = makeNode(ColumnDef);
        n->colname = pstrdup(NameStr(attr->attname));
        n->typname = makeTypeNameFromOid(attr->atttypid, attr->atttypmod);
        n->kvtype = ATT_KV_UNDEFINED;
        n->generatedCol = '\0';
        n->inhcount = 0;
        n->is_local = true;
        n->is_not_null = false;
        n->is_from_type = true;
        n->storage = 0;
        /* CREATE TYPE CANNOT provied compression feature, so the default is set. */
        n->cmprs_mode = ATT_CMPR_UNDEFINED;
        n->raw_default = NULL;
        n->cooked_default = NULL;
        n->collClause = NULL;
        n->collOid = attr->attcollation;
        n->constraints = NIL;
        cxt->columns = lappend(cxt->columns, n);
    }
    DecrTupleDescRefCount(tupdesc);

    ReleaseSysCache(tuple);
}

char* getTmptableIndexName(const char* srcSchema, const char* srcIndex)
{
    char *idxname;
    errno_t rc;
    char srcIndexName[NAMEDATALEN * 2];
    Assert(strlen(srcSchema) < NAMEDATALEN && strlen(srcIndex) < NAMEDATALEN);

    rc = memset_s(srcIndexName, NAMEDATALEN * 2, 0, NAMEDATALEN * 2);
    securec_check(rc, "", "");
    /* Generate idxname based on src schema and src index name */
    rc = strncpy_s(srcIndexName, NAMEDATALEN, srcSchema, NAMEDATALEN - 1);
    securec_check(rc, "", "");
    rc = strncpy_s(srcIndexName + NAMEDATALEN, NAMEDATALEN, srcIndex, NAMEDATALEN - 1);
    securec_check(rc, "", "");
    /* Calculates feature values based on schemas and indexes. */
    uint32 srcIndexNum = pg_checksum_block(srcIndexName, NAMEDATALEN * 2);

    /* In redistribution, idx name is "data_redis_tmp_index_srcIndexNum" */
    uint len = OIDCHARS + strlen("data_redis_tmp_index_") + 1;
    idxname = (char*)palloc0(len);
    int ret = snprintf_s(idxname, len, len -1, "data_redis_tmp_index_%u", srcIndexNum);
    securec_check_ss(ret, "", "");
    return idxname;
}

static bool* getPartIndexUsableStat(Relation indexRelation)
{
    Oid relation_oid = IndexGetRelation(indexRelation->rd_id, false);
    Relation rel = relation_open(relation_oid, NoLock);
    List* relation_oid_list = relationGetPartitionOidList(rel);
    List* partitions = indexGetPartitionList(indexRelation, ExclusiveLock);
    bool *usable = (bool *)palloc0(sizeof(bool) * list_length(partitions));
    ListCell* cell = NULL;
    int i = 0;

    foreach (cell, relation_oid_list) {
        Oid relid = lfirst_oid(cell);
        ListCell* parCell = NULL;
        bool found = false;
        foreach (parCell, partitions) {
            Partition indexPartition = (Partition)lfirst(parCell);
            if (relid == indexPartition->pd_part->indextblid) {
                usable[i++] = indexPartition->pd_part->indisusable;
                found = true;
                break;
            }
        }
        Assert(found);
    }
    releasePartitionList(indexRelation, &partitions, ExclusiveLock);
    list_free_ext(relation_oid_list);
    list_free_ext(partitions);
    relation_close(rel, NoLock);
    return usable;
}

void GenerateClonedIndexHbucketTransfer(Relation rel, IndexStmt* index, TransformTableType transformType, bool constainsExp)
{
    if (PointerIsValid(rel) && RelationInClusterResizing(rel) && RELATION_HAS_BUCKET(rel) &&
        transformType == TRANSFORM_TO_NONHASHBUCKET) {
        if (index->options != NULL && is_contain_crossbucket(index->options)) {
            index->options = list_delete_name(index->options, "crossbucket");
        }
    }

    if (PointerIsValid(rel) && RelationInClusterResizing(rel) && !RELATION_HAS_BUCKET(rel) &&
        transformType == TRANSFORM_TO_HASHBUCKET) {
        if (constainsExp || index->whereClause) {
            index->options = lappend(index->options, makeDefElem("crossbucket", (Node*)makeString("off")));
        }
    }

}
/*
 * Generate an IndexStmt node using information from an already existing index
 * "source_idx".  Attribute numbers should be adjusted according to attmap.
 */
IndexStmt* generateClonedIndexStmt(
    CreateStmtContext* cxt, Relation source_idx, const AttrNumber* attmap, int attmap_length, Relation rel, TransformTableType transformType)
{
    Oid source_relid = RelationGetRelid(source_idx);
    Form_pg_attribute* attrs = RelationGetDescr(source_idx)->attrs;
    HeapTuple ht_idxrel;
    HeapTuple ht_idx;
    Form_pg_class idxrelrec;
    Form_pg_index idxrec;
    Form_pg_am amrec;
    oidvector* indcollation = NULL;
    oidvector* indclass = NULL;
    IndexStmt* index = NULL;
    List* indexprs = NIL;
    ListCell* indexpr_item = NULL;
    Oid indrelid;
    int keyno;
    Oid keycoltype;
    Datum datum;
    bool isnull = false;
    int indnkeyatts;

    /*
     * Fetch pg_class tuple of source index.  We can't use the copy in the
     * relcache entry because it doesn't include optional fields.
     */
    ht_idxrel = SearchSysCache1(RELOID, ObjectIdGetDatum(source_relid));
    if (!HeapTupleIsValid(ht_idxrel))
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", source_relid)));
    idxrelrec = (Form_pg_class)GETSTRUCT(ht_idxrel);

    /* Fetch pg_index tuple for source index from relcache entry */
    ht_idx = source_idx->rd_indextuple;
    idxrec = (Form_pg_index)GETSTRUCT(ht_idx);
    indrelid = idxrec->indrelid;
    indnkeyatts = IndexRelationGetNumberOfKeyAttributes(source_idx);

    /* Fetch pg_am tuple for source index from relcache entry */
    amrec = source_idx->rd_am;

    /* Extract indcollation from the pg_index tuple */
    datum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indcollation, &isnull);
    Assert(!isnull);

    indcollation = (oidvector*)DatumGetPointer(datum);

    /* Extract indclass from the pg_index tuple */
    datum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indclass, &isnull);
    Assert(!isnull);

    indclass = (oidvector*)DatumGetPointer(datum);

    /* Begin building the IndexStmt */
    index = makeNode(IndexStmt);
    index->relation = cxt->relation;
    index->accessMethod = pstrdup(NameStr(amrec->amname));
    if (OidIsValid(idxrelrec->reltablespace))
        index->tableSpace = get_tablespace_name(idxrelrec->reltablespace);
    else
        index->tableSpace = NULL;
    index->excludeOpNames = NIL;
    index->idxcomment = NULL;
    index->indexOid = InvalidOid;
    index->oldNode = InvalidOid;
    index->oldPSortOid = InvalidOid;
    index->unique = idxrec->indisunique;
    index->primary = idxrec->indisprimary;
    index->concurrent = false;
    // mark if the resulting indexStmt is a partitioned index
    index->isPartitioned = RelationIsPartitioned(source_idx);

    /*
     * If the src table is in resizing, means we are going to do create table like for tmp table,
     * then we preserve the index name by src index.
     * Otherwise, set idxname to NULL, let DefineIndex() choose a reasonable name.
     */
    if (PointerIsValid(rel) && RelationInClusterResizing(rel)) {
        /* Generate idxname based on src index name */
        char *srcSchema = get_namespace_name(source_idx->rd_rel->relnamespace);
        char *srcIndex = NameStr(source_idx->rd_rel->relname);
        index->idxname = getTmptableIndexName(srcSchema, srcIndex);
        pfree_ext(srcSchema);
        if (index->isPartitioned) {
            index->partIndexUsable = getPartIndexUsableStat(source_idx);
        }
    } else {
        index->idxname = NULL;
    }

    /*
     * If the index is marked PRIMARY or has an exclusion condition, it's
     * certainly from a constraint; else, if it's not marked UNIQUE, it
     * certainly isn't.  If it is or might be from a constraint, we have to
     * fetch the pg_constraint record.
     */
    if (index->primary || index->unique || idxrec->indisexclusion) {
        Oid constraintId = get_index_constraint(source_relid);
        if (OidIsValid(constraintId)) {
            HeapTuple ht_constr;
            Form_pg_constraint conrec;

            ht_constr = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintId));
            if (!HeapTupleIsValid(ht_constr))
                ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_OPT), errmsg("cache lookup failed for constraint %u", constraintId)));
            conrec = (Form_pg_constraint)GETSTRUCT(ht_constr);

            index->isconstraint = true;
            index->deferrable = conrec->condeferrable;
            index->initdeferred = conrec->condeferred;

            /* If it's an exclusion constraint, we need the operator names */
            if (idxrec->indisexclusion) {
                Datum* elems = NULL;
                int nElems;
                int i;

                Assert(conrec->contype == CONSTRAINT_EXCLUSION);
                /* Extract operator OIDs from the pg_constraint tuple */
                datum = SysCacheGetAttr(CONSTROID, ht_constr, Anum_pg_constraint_conexclop, &isnull);
                if (isnull)
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmodule(MOD_OPT), errmsg("null conexclop for constraint %u", constraintId)));

                deconstruct_array(DatumGetArrayTypeP(datum), OIDOID, sizeof(Oid), true, 'i', &elems, NULL, &nElems);

                for (i = 0; i < nElems; i++) {
                    Oid operid = DatumGetObjectId(elems[i]);
                    HeapTuple opertup;
                    Form_pg_operator operform;
                    char* oprname = NULL;
                    char* nspname = NULL;
                    List* namelist = NIL;

                    opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operid));
                    if (!HeapTupleIsValid(opertup))
                        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                                errmodule(MOD_OPT), errmsg("cache lookup failed for operator %u", operid)));

                    operform = (Form_pg_operator)GETSTRUCT(opertup);
                    oprname = pstrdup(NameStr(operform->oprname));
                    /* For simplicity we always schema-qualify the op name */
                    nspname = get_namespace_name(operform->oprnamespace);
                    namelist = list_make2(makeString(nspname), makeString(oprname));
                    index->excludeOpNames = lappend(index->excludeOpNames, namelist);
                    ReleaseSysCache(opertup);
                }
            }

            ReleaseSysCache(ht_constr);
        } else
            index->isconstraint = false;
    } else
        index->isconstraint = false;

    /* Get the index expressions, if any */
    datum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indexprs, &isnull);
    if (!isnull) {
        char* exprsString = NULL;

        exprsString = TextDatumGetCString(datum);
        indexprs = (List*)stringToNode(exprsString);
    } else
        indexprs = NIL;

    /* Build the list of IndexElem */
    index->indexParams = NIL;
    index->indexIncludingParams = NIL;

    indexpr_item = list_head(indexprs);
    for (keyno = 0; keyno < indnkeyatts; keyno++) {
        IndexElem* iparam = NULL;
        AttrNumber attnum = idxrec->indkey.values[keyno];
        uint16 opt = (uint16)source_idx->rd_indoption[keyno];

        iparam = makeNode(IndexElem);

        if (AttributeNumberIsValid(attnum)) {
            /* Simple index column */
            char* attname = NULL;

            attname = get_relid_attribute_name(indrelid, attnum);
            keycoltype = get_atttype(indrelid, attnum);

            iparam->name = attname;
            iparam->expr = NULL;
        } else {
            /* Expressional index */
            Node* indexkey = NULL;
            bool found_whole_row = false;

            if (indexpr_item == NULL)
                ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmodule(MOD_OPT), errmsg("too few entries in indexprs list")));

            indexkey = (Node*)lfirst(indexpr_item);
            indexpr_item = lnext(indexpr_item);

            /* Adjust Vars to match new table's column numbering */
            indexkey = map_variable_attnos(indexkey, 1, 0, attmap, attmap_length, &found_whole_row);

            /* As in transformTableLikeClause, reject whole-row variables */
            if (found_whole_row)
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot convert whole-row table reference"),
                        errdetail("Index \"%s\" contains a whole-row table reference.",
                            RelationGetRelationName(source_idx))));

            iparam->name = NULL;
            iparam->expr = indexkey;

            keycoltype = exprType(indexkey);
        }

        /* Copy the original index column name */
        iparam->indexcolname = pstrdup(NameStr(attrs[keyno]->attname));

        /* Add the collation name, if non-default */
        iparam->collation = get_collation(indcollation->values[keyno], keycoltype);

        /* Add the operator class name, if non-default */
        iparam->opclass = get_opclass(indclass->values[keyno], keycoltype);

        iparam->ordering = SORTBY_DEFAULT;
        iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;

        /* Adjust options if necessary */
        if (amrec->amcanorder) {
            /*
             * If it supports sort ordering, copy DESC and NULLS opts. Don't
             * set non-default settings unnecessarily, though, so as to
             * improve the chance of recognizing equivalence to constraint
             * indexes.
             */
            if (((uint16)opt) & INDOPTION_DESC) {
                iparam->ordering = SORTBY_DESC;
                if ((((uint16)opt) & INDOPTION_NULLS_FIRST) == 0)
                    iparam->nulls_ordering = SORTBY_NULLS_LAST;
            } else {
                if (((uint16)opt) & INDOPTION_NULLS_FIRST)
                    iparam->nulls_ordering = SORTBY_NULLS_FIRST;
            }
        }

        index->indexParams = lappend(index->indexParams, iparam);
    }

    /* Handle included columns separately */
    for (int i = indnkeyatts; i < idxrec->indnatts; i++) {
        AttrNumber attnum = idxrec->indkey.values[i];
        if (attnum == TableOidAttributeNumber) {
            /* global-partition-index */
            index->isGlobal = true;
            index->isPartitioned = true;
            break;
        }
    }

    /* Copy reloptions if any */
    datum = SysCacheGetAttr(RELOID, ht_idxrel, Anum_pg_class_reloptions, &isnull);
    if (!isnull) {
        index->options = untransformRelOptions(datum);
    }

    /* If it's a partial index, decompile and append the predicate */
    datum = SysCacheGetAttr(INDEXRELID, ht_idx, Anum_pg_index_indpred, &isnull);
    if (!isnull) {
        char* pred_str = NULL;
        Node* pred_tree = NULL;
        bool found_whole_row = false;

        /* Convert text string to node tree */
        pred_str = TextDatumGetCString(datum);
        pred_tree = (Node*)stringToNode(pred_str);

        /* Adjust Vars to match new table's column numbering */
        pred_tree = map_variable_attnos(pred_tree, 1, 0, attmap, attmap_length, &found_whole_row);

        /* As in transformTableLikeClause, reject whole-row variables */
        if (found_whole_row)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot convert whole-row table reference"),
                    errdetail(
                        "Index \"%s\" contains a whole-row table reference.", RelationGetRelationName(source_idx))));

        index->whereClause = pred_tree;
    }
    GenerateClonedIndexHbucketTransfer(rel, index, transformType, indexprs != NULL);
    /* Clean up */
    ReleaseSysCache(ht_idxrel);

    return index;
}

/*
 * get_collation		- fetch qualified name of a collation
 *
 * If collation is InvalidOid or is the default for the given actual_datatype,
 * then the return value is NIL.
 */
static List* get_collation(Oid collation, Oid actual_datatype)
{
    List* result = NIL;
    HeapTuple ht_coll;
    Form_pg_collation coll_rec;
    char* nsp_name = NULL;
    char* coll_name = NULL;

    if (!OidIsValid(collation))
        return NIL; /* easy case */
    if (collation == get_typcollation(actual_datatype))
        return NIL; /* just let it default */

    ht_coll = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
    if (!HeapTupleIsValid(ht_coll))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_OPT),
                errmsg("cache lookup failed for collation %u", collation)));

    coll_rec = (Form_pg_collation)GETSTRUCT(ht_coll);

    /* For simplicity, we always schema-qualify the name */
    nsp_name = get_namespace_name(coll_rec->collnamespace);
    coll_name = pstrdup(NameStr(coll_rec->collname));
    result = list_make2(makeString(nsp_name), makeString(coll_name));

    ReleaseSysCache(ht_coll);
    return result;
}

/*
 * get_opclass			- fetch qualified name of an index operator class
 *
 * If the opclass is the default for the given actual_datatype, then
 * the return value is NIL.
 */
static List* get_opclass(Oid opclass, Oid actual_datatype)
{
    List* result = NIL;
    HeapTuple ht_opc;
    Form_pg_opclass opc_rec;

    ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
    if (!HeapTupleIsValid(ht_opc))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_OPT),
                errmsg("cache lookup failed for opclass %u", opclass)));
    opc_rec = (Form_pg_opclass)GETSTRUCT(ht_opc);

    if (GetDefaultOpClass(actual_datatype, opc_rec->opcmethod) != opclass) {
        /* For simplicity, we always schema-qualify the name */
        char* nsp_name = get_namespace_name(opc_rec->opcnamespace);
        char* opc_name = pstrdup(NameStr(opc_rec->opcname));

        result = list_make2(makeString(nsp_name), makeString(opc_name));
    }

    ReleaseSysCache(ht_opc);
    return result;
}

static bool IsPartitionKeyInParmaryKeyAndUniqueKey(const char *pkname, const List* constraintKeys)
{
    bool found = false;
    ListCell *ixcell = NULL;

    foreach (ixcell, constraintKeys) {
        char *ikname = strVal(lfirst(ixcell));

        if (!strcmp(pkname, ikname)) {
            found = true;
            break;
        }
    }

    return found;
}

static bool IsPartitionKeyAllInParmaryKeyAndUniqueKey(const List *partitionKey, const List *constraintKeys)
{
    ListCell *pkcell = NULL;
    bool found = true;

    foreach (pkcell, partitionKey) {
        ColumnRef *colref = (ColumnRef *)lfirst(pkcell);
        char *pkname = ((Value *)linitial(colref->fields))->val.str;

        found = found && IsPartitionKeyInParmaryKeyAndUniqueKey(pkname, constraintKeys);

        if (!found) {
            return false;
        }
    }

    return found;
}

/*
 * transformIndexConstraints
 *		Handle UNIQUE, PRIMARY KEY, EXCLUDE constraints, which create indexes.
 *		We also merge in any index definitions arising from
 *		LIKE ... INCLUDING INDEXES.
 */
static void transformIndexConstraints(CreateStmtContext* cxt)
{
    IndexStmt* index = NULL;
    List* indexlist = NIL;
    ListCell* lc = NULL;

    /*
     * Run through the constraints that need to generate an index. For PRIMARY
     * KEY, mark each column as NOT NULL and create an index. For UNIQUE or
     * EXCLUDE, create an index as for PRIMARY KEY, but do not insist on NOT
     * NULL.
     */
    foreach (lc, cxt->ixconstraints) {
        Constraint* constraint = (Constraint*)lfirst(lc);
        bool mustGlobal = false;

        AssertEreport(IsA(constraint, Constraint), MOD_OPT, "");
        AssertEreport(constraint->contype == CONSTR_PRIMARY || constraint->contype == CONSTR_UNIQUE ||
                          constraint->contype == CONSTR_EXCLUSION,
            MOD_OPT,
            "");
        if (cxt->ispartitioned && !cxt->isalter) {
            AssertEreport(PointerIsValid(cxt->partitionKey), MOD_OPT, "");

            /*
             * @hdfs
             * Columns of PRIMARY KEY/UNIQUE could be any columns on HDFS partition table.
             * If the partition foreign table will support real index, the following code must
             * be modified.
             */
            if (IsA(cxt->node, CreateForeignTableStmt) &&
                isObsOrHdfsTableFormSrvName(((CreateForeignTableStmt*)cxt->node)->servername)) {
                /* Do nothing */
            } else if (constraint->contype == CONSTR_EXCLUSION) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Partitioned table does not support EXCLUDE index")));
            } else {
                bool checkPartitionKey = IsPartitionKeyAllInParmaryKeyAndUniqueKey(cxt->partitionKey, constraint->keys);
                if (cxt->subPartitionKey != NULL) {
                    // for subpartition table.
                    bool checkSubPartitionKey =
                        IsPartitionKeyAllInParmaryKeyAndUniqueKey(cxt->subPartitionKey, constraint->keys);
                    mustGlobal = !(checkPartitionKey && checkSubPartitionKey);
                } else {
                    // for partition table.
                    mustGlobal = !checkPartitionKey;
                }
            }
        }

        index = transformIndexConstraint(constraint, cxt, mustGlobal);

        indexlist = lappend(indexlist, index);
    }

    /* Add in any indexes defined by LIKE ... INCLUDING INDEXES */
    foreach (lc, cxt->inh_indexes) {
        index = (IndexStmt*)lfirst(lc);
        if (index->primary) {
            if (cxt->pkey != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("multiple primary keys for table \"%s\" are not allowed", cxt->relation->relname)));
            cxt->pkey = index;
        }

        indexlist = lappend(indexlist, index);
    }

    /*
     * Scan the index list and remove any redundant index specifications. This
     * can happen if, for instance, the user writes UNIQUE PRIMARY KEY. A
     * strict reading of SQL92 would suggest raising an error instead, but
     * that strikes me as too anal-retentive. - tgl 2001-02-14
     *
     * XXX in ALTER TABLE case, it'd be nice to look for duplicate
     * pre-existing indexes, too.
     */
    AssertEreport(cxt->alist == NIL, MOD_OPT, "");
    if (cxt->pkey != NULL) {
        /* Make sure we keep the PKEY index in preference to others... */
        cxt->alist = list_make1(cxt->pkey);
    }

    foreach (lc, indexlist) {
        bool keep = true;
        ListCell* k = NULL;

        index = (IndexStmt*)lfirst(lc);
        /* if it's pkey, it's already in cxt->alist */
        if (index == cxt->pkey)
            continue;

        /*
         * For create table like, if the table is resizing, don't remove redundant index,
         * because we need to keep the index totally same with origin table's indices.
         */
        if (cxt->isResizing) {
            cxt->alist = lappend(cxt->alist, index);
            continue;
        }

        foreach (k, cxt->alist) {
            IndexStmt* priorindex = (IndexStmt*)lfirst(k);
            bool accessMethodEqual = index->accessMethod == priorindex->accessMethod ||
                (index->accessMethod != NULL && priorindex->accessMethod != NULL &&
                 strcmp(index->accessMethod, priorindex->accessMethod) == 0);

            if (equal(index->indexParams, priorindex->indexParams) &&
                equal(index->indexIncludingParams, priorindex->indexIncludingParams) &&
                equal(index->whereClause, priorindex->whereClause) &&
                equal(index->excludeOpNames, priorindex->excludeOpNames) &&
                accessMethodEqual &&
                index->deferrable == priorindex->deferrable && index->initdeferred == priorindex->initdeferred) {
                priorindex->unique = priorindex->unique || index->unique;

                /*
                 * If the prior index is as yet unnamed, and this one is
                 * named, then transfer the name to the prior index. This
                 * ensures that if we have named and unnamed constraints,
                 * we'll use (at least one of) the names for the index.
                 */
                if (priorindex->idxname == NULL)
                    priorindex->idxname = index->idxname;
                keep = false;
                break;
            }
        }

        if (keep)
            cxt->alist = lappend(cxt->alist, index);
    }
}

/*
 * If it's ALTER TABLE ADD CONSTRAINT USING INDEX,
 * verify the index is usable.
 */
static void checkConditionForTransformIndex(
    Constraint* constraint, CreateStmtContext* cxt, Oid index_oid, Relation index_rel)
{
    if (constraint == NULL || cxt == NULL || index_rel == NULL)
        return;

    char* index_name = constraint->indexname;
    Form_pg_index index_form = index_rel->rd_index;
    Relation heap_rel = cxt->rel;

    /* Check that it does not have an associated constraint already */
    if (OidIsValid(get_index_constraint(index_oid)))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("index \"%s\" is already associated with a constraint", index_name),
                parser_errposition(cxt->pstate, constraint->location)));

    /* Perform validity checks on the index */
    if (index_form->indrelid != RelationGetRelid(heap_rel))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("index \"%s\" does not belong to table \"%s\"", index_name, RelationGetRelationName(heap_rel)),
                parser_errposition(cxt->pstate, constraint->location)));

    if (!IndexIsValid(index_form))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("index \"%s\" is not valid", index_name),
                parser_errposition(cxt->pstate, constraint->location)));

    if (!index_form->indisunique)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is not a unique index", index_name),
                errdetail("Cannot create a primary key or unique constraint using such an index."),
                parser_errposition(cxt->pstate, constraint->location)));

    if (RelationGetIndexExpressions(index_rel) != NIL)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("index \"%s\" contains expressions", index_name),
                errdetail("Cannot create a primary key or unique constraint using such an index."),
                parser_errposition(cxt->pstate, constraint->location)));

    if (RelationGetIndexPredicate(index_rel) != NIL)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is a partial index", index_name),
                errdetail("Cannot create a primary key or unique constraint using such an index."),
                parser_errposition(cxt->pstate, constraint->location)));

    /*
     * It's probably unsafe to change a deferred index to non-deferred. (A
     * non-constraint index couldn't be deferred anyway, so this case
     * should never occur; no need to sweat, but let's check it.)
     */
    if (!index_form->indimmediate && !constraint->deferrable)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is a deferrable index", index_name),
                errdetail("Cannot create a non-deferrable constraint using a deferrable index."),
                parser_errposition(cxt->pstate, constraint->location)));

    /*
     * Insist on it being a btree.	That's the only kind that supports
     * uniqueness at the moment anyway; but we must have an index that
     * exactly matches what you'd get from plain ADD CONSTRAINT syntax,
     * else dump and reload will produce a different index (breaking
     * pg_upgrade in particular).
     */
    if (index_rel->rd_rel->relam != get_am_oid(DEFAULT_INDEX_TYPE, false) &&
        index_rel->rd_rel->relam != get_am_oid(DEFAULT_USTORE_INDEX_TYPE, false))
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("index \"%s\" is not a btree", index_name),
                parser_errposition(cxt->pstate, constraint->location)));
}

/*
 * transformIndexConstraint
 *		Transform one UNIQUE, PRIMARY KEY, or EXCLUDE constraint for
 *		transformIndexConstraints.
 */
static IndexStmt* transformIndexConstraint(Constraint* constraint, CreateStmtContext* cxt, bool mustGlobal)
{
    IndexStmt* index = NULL;
    ListCell* lc = NULL;

    index = makeNode(IndexStmt);

    index->unique = (constraint->contype != CONSTR_EXCLUSION);
    index->primary = (constraint->contype == CONSTR_PRIMARY);
    if (index->primary) {
        if (cxt->pkey != NULL) {
            if (0 == pg_strncasecmp(cxt->stmtType, CREATE_FOREIGN_TABLE, strlen(cxt->stmtType)) ||
                0 == pg_strncasecmp(cxt->stmtType, ALTER_FOREIGN_TABLE, strlen(cxt->stmtType))) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg(
                            "Multiple primary keys for foreign table \"%s\" are not allowed.", cxt->relation->relname),
                        parser_errposition(cxt->pstate, constraint->location)));
            } else {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("multiple primary keys for table \"%s\" are not allowed", cxt->relation->relname),
                        parser_errposition(cxt->pstate, constraint->location)));
            }
        }
        cxt->pkey = index;

        /*
         * In ALTER TABLE case, a primary index might already exist, but
         * DefineIndex will check for it.
         */
    }
    index->isconstraint = true;
    index->deferrable = constraint->deferrable;
    index->initdeferred = constraint->initdeferred;

    if (constraint->conname != NULL)
        index->idxname = pstrdup(constraint->conname);
    else
        index->idxname = NULL; /* DefineIndex will choose name */

    index->relation = cxt->relation;
    index->accessMethod = constraint->access_method;
    index->options = constraint->options;
    index->tableSpace = constraint->indexspace;
    index->whereClause = constraint->where_clause;
    index->indexParams = NIL;
    index->indexIncludingParams = NIL;
    index->excludeOpNames = NIL;
    index->idxcomment = NULL;
    index->indexOid = InvalidOid;
    index->oldNode = InvalidOid;
    index->oldPSortOid = InvalidOid;
    index->concurrent = false;
    index->isGlobal = mustGlobal;

    /*
     * @hdfs
     * The foreign table dose not have index. the HDFS foreign table has informational
     * constraint which is not a index.
     * If the partition foreign table will support real index, the following code must
     * be modified.
     */
    if (0 == pg_strncasecmp(cxt->stmtType, CREATE_FOREIGN_TABLE, strlen(cxt->stmtType)) ||
        0 == pg_strncasecmp(cxt->stmtType, ALTER_FOREIGN_TABLE, strlen(cxt->stmtType))) {
        index->isPartitioned = false;
    } else {
        index->isPartitioned = cxt->ispartitioned;

    }

    index->inforConstraint = constraint->inforConstraint;

    /*
     * If it's ALTER TABLE ADD CONSTRAINT USING INDEX, look up the index and
     * verify it's usable, then extract the implied column name list.  (We
     * will not actually need the column name list at runtime, but we need it
     * now to check for duplicate column entries below.)
     */
    if (constraint->indexname != NULL) {
        char* index_name = constraint->indexname;
        Relation heap_rel = cxt->rel;
        Oid index_oid;
        Relation index_rel;
        Form_pg_index index_form;
        oidvector* indclass = NULL;
        Datum indclassDatum;
        bool isnull = true;
        int i;
        int indnkeyatts;

        /* Grammar should not allow this with explicit column list */
        AssertEreport(constraint->keys == NIL, MOD_OPT, "");

        /* Grammar should only allow PRIMARY and UNIQUE constraints */
        AssertEreport(constraint->contype == CONSTR_PRIMARY || constraint->contype == CONSTR_UNIQUE, MOD_OPT, "");

        /* Must be ALTER, not CREATE, but grammar doesn't enforce that */
        if (!cxt->isalter)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("cannot use an existing index in CREATE TABLE"),
                    parser_errposition(cxt->pstate, constraint->location)));

        /* Look for the index in the same schema as the table */
        index_oid = get_relname_relid(index_name, RelationGetNamespace(heap_rel));

        if (!OidIsValid(index_oid))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("index \"%s\" does not exist", index_name),
                    parser_errposition(cxt->pstate, constraint->location)));

        /* Open the index (this will throw an error if it is not an index) */
        index_rel = index_open(index_oid, AccessShareLock);
        index_form = index_rel->rd_index;
        indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index_rel);

        /* check the conditons for this function,
         * and verify the index is usable
         */
        checkConditionForTransformIndex(constraint, cxt, index_oid, index_rel);

        /* Must get indclass the hard way */
        indclassDatum = SysCacheGetAttr(INDEXRELID, index_rel->rd_indextuple, Anum_pg_index_indclass, &isnull);
        AssertEreport(!isnull, MOD_OPT, "");
        indclass = (oidvector*)DatumGetPointer(indclassDatum);

        for (i = 0; i < index_form->indnatts; i++) {
            int2 attnum = index_form->indkey.values[i];
            Form_pg_attribute attform;
            char* attname = NULL;
            Oid defopclass;

            /*
             * We shouldn't see attnum == 0 here, since we already rejected
             * expression indexes.	If we do, SystemAttributeDefinition will
             * throw an error.
             */
            if (attnum > 0) {
                AssertEreport(attnum <= heap_rel->rd_att->natts, MOD_OPT, "");
                attform = heap_rel->rd_att->attrs[attnum - 1];
            } else
                attform = SystemAttributeDefinition(attnum, heap_rel->rd_rel->relhasoids,  RELATION_HAS_BUCKET(heap_rel), RELATION_HAS_UIDS(heap_rel));
            attname = pstrdup(NameStr(attform->attname));

            if (i < indnkeyatts) {
                /*
                 * Insist on default opclass and sort options.  While the
                 * index would still work as a constraint with non-default
                 * settings, it might not provide exactly the same uniqueness
                 * semantics as you'd get from a normally-created constraint;
                 * and there's also the dump/reload problem mentioned above.
                 */
                defopclass = GetDefaultOpClass(attform->atttypid, index_rel->rd_rel->relam);
                if (indclass->values[i] != defopclass || index_rel->rd_indoption[i] != 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("index \"%s\" does not have default sorting behavior", index_name),
                            errdetail("Cannot create a primary key or unique constraint using such an index."),
                            parser_errposition(cxt->pstate, constraint->location)));
                constraint->keys = lappend(constraint->keys, makeString(attname));
            } else {
                constraint->including = lappend(constraint->including, makeString(attname));
            }

        }

        /* Close the index relation but keep the lock */
        relation_close(index_rel, NoLock);

        index->indexOid = index_oid;
    }

    /*
     * If it's an EXCLUDE constraint, the grammar returns a list of pairs of
     * IndexElems and operator names.  We have to break that apart into
     * separate lists.
     */
    if (constraint->contype == CONSTR_EXCLUSION) {
        foreach (lc, constraint->exclusions) {
            List* pair = (List*)lfirst(lc);
            IndexElem* elem = NULL;
            List* opname = NIL;

            Assert(list_length(pair) == 2);
            elem = (IndexElem*)linitial(pair);
            Assert(IsA(elem, IndexElem));
            opname = (List*)lsecond(pair);
            Assert(IsA(opname, List));

            index->indexParams = lappend(index->indexParams, elem);
            index->excludeOpNames = lappend(index->excludeOpNames, opname);
        }
    } else {

        /*
         * For UNIQUE and PRIMARY KEY, we just have a list of column names.
         *
         * Make sure referenced keys exist.  If we are making a PRIMARY KEY index,
         * also make sure they are NOT NULL, if possible. (Although we could leave
         * it to DefineIndex to mark the columns NOT NULL, it's more efficient to
         * get it right the first time.)
         */
        foreach (lc, constraint->keys) {
            char* key = strVal(lfirst(lc));
            bool found = false;
            ColumnDef* column = NULL;
            ListCell* columns = NULL;
            IndexElem* iparam = NULL;

            foreach (columns, cxt->columns) {
                column = (ColumnDef*)lfirst(columns);
                AssertEreport(IsA(column, ColumnDef), MOD_OPT, "");
                if (strcmp(column->colname, key) == 0) {
                    found = true;
                    break;
                }
            }
            if (found) {
                /* found column in the new table; force it to be NOT NULL */
                if (constraint->contype == CONSTR_PRIMARY && !constraint->inforConstraint->nonforced)
                    column->is_not_null = TRUE;
            } else if (SystemAttributeByName(key, cxt->hasoids) != NULL) {
                /*
                 * column will be a system column in the new table, so accept it.
                 * System columns can't ever be null, so no need to worry about
                 * PRIMARY/NOT NULL constraint.
                 */
                found = true;
            } else if (cxt->inhRelations != NIL) {
                /* try inherited tables */
                ListCell* inher = NULL;

                foreach (inher, cxt->inhRelations) {
                    RangeVar* inh = (RangeVar*)lfirst(inher);
                    Relation rel;
                    int count;

                    AssertEreport(IsA(inh, RangeVar), MOD_OPT, "");
                    rel = heap_openrv(inh, AccessShareLock);
                    if (rel->rd_rel->relkind != RELKIND_RELATION)
                        ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("inherited relation \"%s\" is not a table", inh->relname)));
                    for (count = 0; count < rel->rd_att->natts; count++) {
                        Form_pg_attribute inhattr = rel->rd_att->attrs[count];
                        char* inhname = NameStr(inhattr->attname);

                        if (inhattr->attisdropped)
                            continue;
                        if (strcmp(key, inhname) == 0) {
                            found = true;

                            /*
                             * We currently have no easy way to force an inherited
                             * column to be NOT NULL at creation, if its parent
                             * wasn't so already. We leave it to DefineIndex to
                             * fix things up in this case.
                             */
                            break;
                        }
                    }
                    heap_close(rel, NoLock);
                    if (found)
                        break;
                }
            }

            /*
             * In the ALTER TABLE case, don't complain about index keys not
             * created in the command; they may well exist already. DefineIndex
             * will complain about them if not, and will also take care of marking
             * them NOT NULL.
             */
            if (!found && !cxt->isalter)
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" named in key does not exist", key),
                        parser_errposition(cxt->pstate, constraint->location)));

            /* Check for PRIMARY KEY(foo, foo) */
            foreach (columns, index->indexParams) {
                iparam = (IndexElem*)lfirst(columns);
                if (iparam->name && strcmp(key, iparam->name) == 0) {
                    if (index->primary)
                        ereport(ERROR,
                            (errcode(ERRCODE_DUPLICATE_COLUMN),
                                errmsg("column \"%s\" appears twice in primary key constraint", key),
                                parser_errposition(cxt->pstate, constraint->location)));
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_DUPLICATE_COLUMN),
                                errmsg("column \"%s\" appears twice in unique constraint", key),
                                parser_errposition(cxt->pstate, constraint->location)));
                }
            }

#ifdef PGXC
            /*
             * Set fallback distribution column.
             * If not set, set it to first column in index.
             * If primary key, we prefer that over a unique constraint.
             */
            if (index->indexParams == NIL && (index->primary || cxt->fallback_dist_col == NULL)) {
                if (cxt->fallback_dist_col != NULL) {
                    list_free_deep(cxt->fallback_dist_col);
                    cxt->fallback_dist_col = NULL;
                }
                cxt->fallback_dist_col = lappend(cxt->fallback_dist_col, makeString(pstrdup(key)));
            }
#endif

            /* OK, add it to the index definition */
            iparam = makeNode(IndexElem);
            iparam->name = pstrdup(key);
            iparam->expr = NULL;
            iparam->indexcolname = NULL;
            iparam->collation = NIL;
            iparam->opclass = NIL;
            iparam->ordering = SORTBY_DEFAULT;
            iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
            index->indexParams = lappend(index->indexParams, iparam);
        }
    }

    /* Add included columns to index definition */
    foreach (lc, constraint->including) {
        char* key = strVal(lfirst(lc));
        bool found = false;
        ColumnDef* column = NULL;
        ListCell* columns = NULL;
        IndexElem* iparam = NULL;

        foreach (columns, cxt->columns) {
            column = lfirst_node(ColumnDef, columns);
            if (strcmp(column->colname, key) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            if (SystemAttributeByName(key, cxt->hasoids) != NULL) {
                /*
                 * column will be a system column in the new table, so accept
                 * it. System columns can't ever be null, so no need to worry
                 * about PRIMARY/NOT NULL constraint.
                 */
                found = true;
            } else if (cxt->inhRelations) {
                /* try inherited tables */
                ListCell* inher = NULL;

                foreach (inher, cxt->inhRelations) {
                    RangeVar* inh = lfirst_node(RangeVar, inher);
                    Relation rel = NULL;
                    int count = 0;

                    rel = heap_openrv(inh, AccessShareLock);
                    /* check user requested inheritance from valid relkind */
                    if (rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE
                        && rel->rd_rel->relkind != RELKIND_STREAM) {
                        ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("inherited relation \"%s\" is not a table or foreign table", inh->relname)));
                    }
                    for (count = 0; count < rel->rd_att->natts; count++) {
                        Form_pg_attribute inhattr = rel->rd_att->attrs[count];
                        char* inhname = NameStr(inhattr->attname);

                        if (inhattr->attisdropped)
                            continue;
                        if (strcmp(key, inhname) == 0) {
                            found = true;

                            /*
                             * We currently have no easy way to force an
                             * inherited column to be NOT NULL at creation, if
                             * its parent wasn't so already. We leave it to
                             * DefineIndex to fix things up in this case.
                             */
                            break;
                        }
                    }
                    heap_close(rel, NoLock);
                    if (found)
                        break;
                }
            }
        }

        /*
         * In the ALTER TABLE case, don't complain about index keys not
         * created in the command; they may well exist already. DefineIndex
         * will complain about them if not, and will also take care of marking
         * them NOT NULL.
         */
        if (!found && !cxt->isalter)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" named in key does not exist", key),
                    parser_errposition(cxt->pstate, constraint->location)));

        /* OK, add it to the index definition */
        iparam = makeNode(IndexElem);
        iparam->name = pstrdup(key);
        iparam->expr = NULL;
        iparam->indexcolname = NULL;
        iparam->collation = NIL;
        iparam->opclass = NIL;
        index->indexIncludingParams = lappend(index->indexIncludingParams, iparam);
    }

    return index;
}

/*
 * transformFKConstraints
 *		handle FOREIGN KEY constraints
 */
static void transformFKConstraints(CreateStmtContext* cxt, bool skipValidation, bool isAddConstraint)
{
    ListCell* fkclist = NULL;

    if (cxt->fkconstraints == NIL)
        return;

    /*
     * If CREATE TABLE or adding a column with NULL default, we can safely
     * skip validation of FK constraints, and nonetheless mark them valid.
     * (This will override any user-supplied NOT VALID flag.)
     */
    if (skipValidation) {
        foreach (fkclist, cxt->fkconstraints) {
            Constraint* constraint = (Constraint*)lfirst(fkclist);

            constraint->skip_validation = true;
            constraint->initially_valid = true;
#ifdef PGXC
            /*
             * Set fallback distribution column.
             * If not yet set, set it to first column in FK constraint
             * if it references a partitioned table
             */
            if (IS_PGXC_COORDINATOR && cxt->fallback_dist_col == NIL && list_length(constraint->pk_attrs) != 0) {
                if (list_length(constraint->pk_attrs) != list_length(constraint->fk_attrs)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FOREIGN_KEY),
                            errmsg("number of referencing and referenced columns for foreign key disagree")));
                }

                Oid pk_rel_id = RangeVarGetRelid(constraint->pktable, NoLock, false);
                RelationLocInfo* locInfo = GetRelationLocInfo(pk_rel_id);

                if (locInfo != NULL && locInfo->partAttrNum != NIL) {
                    int i = 0;
                    char* colstr = NULL;
                    ListCell *cell = NULL, *pk_cell = NULL;
                    AttrNumber attnum, pk_attnum;

                    foreach (cell, locInfo->partAttrNum) {
                        attnum = lfirst_int(cell);
                        /*This table is replication*/
                        if (0 == attnum) {
                            break;
                        }

                        i = 0;
                        foreach (pk_cell, constraint->pk_attrs) {
                            pk_attnum = get_attnum(pk_rel_id, strVal(lfirst(pk_cell)));
                            if (attnum == pk_attnum) {
                                break;
                            }
                            i++;
                        }
                        if (pk_cell == NULL) {
                            list_free_deep(cxt->fallback_dist_col);
                            cxt->fallback_dist_col = NULL;
                            break;
                        } else {
                            colstr = strdup(strVal(list_nth(constraint->fk_attrs, i)));
                            cxt->fallback_dist_col = lappend(cxt->fallback_dist_col, makeString(pstrdup(colstr)));
                        }

                        if (colstr != NULL)
                            free(colstr);
                        colstr = NULL;
                    }
                }
            }
#endif
        }
    }

    /*
     * For CREATE TABLE or ALTER TABLE ADD COLUMN, gin up an ALTER TABLE ADD
     * CONSTRAINT command to execute after the basic command is complete. (If
     * called from ADD CONSTRAINT, that routine will add the FK constraints to
     * its own subcommand list.)
     *
     * Note: the ADD CONSTRAINT command must also execute after any index
     * creation commands.  Thus, this should run after
     * transformIndexConstraints, so that the CREATE INDEX commands are
     * already in cxt->alist.
     */
    if (!isAddConstraint) {
        AlterTableStmt* alterstmt = makeNode(AlterTableStmt);

        alterstmt->relation = cxt->relation;
        alterstmt->cmds = NIL;
        alterstmt->relkind = OBJECT_TABLE;

        foreach (fkclist, cxt->fkconstraints) {
            Constraint* constraint = (Constraint*)lfirst(fkclist);
            AlterTableCmd* altercmd = makeNode(AlterTableCmd);

            altercmd->subtype = AT_ProcessedConstraint;
            altercmd->name = NULL;
            altercmd->def = (Node*)constraint;
            alterstmt->cmds = lappend(alterstmt->cmds, altercmd);
        }

        cxt->alist = lappend(cxt->alist, alterstmt);
    }
}

/*
 * transformIndexStmt - parse analysis for CREATE INDEX and ALTER TABLE
 *
 * Note: this is a no-op for an index not using either index expressions or
 * a predicate expression.	There are several code paths that create indexes
 * without bothering to call this, because they know they don't have any
 * such expressions to deal with.
 */
IndexStmt* transformIndexStmt(Oid relid, IndexStmt* stmt, const char* queryString)
{
    Relation rel;
    ParseState* pstate = NULL;
    RangeTblEntry* rte = NULL;
    ListCell* l = NULL;
    int crossbucketopt = -1; /* -1 means the SQL statement doesn't contain crossbucket option */

    /*
     * We must not scribble on the passed-in IndexStmt, so copy it.  (This is
     * overkill, but easy.)
     */
    stmt = (IndexStmt*)copyObject(stmt);

    /* Set up pstate */
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    /*
     * Put the parent table into the rtable so that the expressions can refer
     * to its fields without qualification.  Caller is responsible for locking
     * relation, but we still need to open it.
     */
    rel = relation_open(relid, NoLock);
    rte = addRangeTableEntry(pstate, stmt->relation, NULL, false, true, true, false, true);

#ifdef ENABLE_MOT
    if (RelationIsForeignTable(rel) && isMOTFromTblOid(RelationGetRelid(rel))) {
        stmt->internal_flag = true;
    }
#endif

    /* default partition index is set to Global index */
    if (RELATION_IS_PARTITIONED(rel) && !stmt->isPartitioned && DEFAULT_CREATE_GLOBAL_INDEX) {
        stmt->isGlobal = true;
    }

    /* set to crossbucket flag as necessary */
    if (RELATION_HAS_BUCKET(rel) && !stmt->crossbucket) {
        stmt->crossbucket = get_crossbucket_option(&stmt->options, stmt->isGlobal, stmt->accessMethod, &crossbucketopt);
    }

    bool isColStore = RelationIsColStore(rel);
    if (stmt->accessMethod == NULL) {
        if (!isColStore) {
            /* row store using btree index by default */
            if (!RelationIsUstoreFormat(rel)) {
                stmt->accessMethod = DEFAULT_INDEX_TYPE;
            } else {
                stmt->accessMethod = DEFAULT_USTORE_INDEX_TYPE;
            }
        } else {
            if (stmt->isGlobal) {
                if (stmt->isconstraint) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("Invalid PRIMARY KEY/UNIQUE constraint for column partitioned table"),
                                errdetail("Columns of PRIMARY KEY/UNIQUE constraint Must contain PARTITION KEY")));
                }
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                        errmsg("Global partition index does not support column store.")));
            }
            if (crossbucketopt > 0) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                        errmsg("cross-bucket index does not support column store")));
            }
            if (stmt->isconstraint && (stmt->unique || stmt->primary)) {
                /* cstore unique and primary key only support cbtree */
                stmt->accessMethod = CSTORE_BTREE_INDEX_TYPE;
            } else {
                /* column store using psort index by default */
                stmt->accessMethod = DEFAULT_CSTORE_INDEX_TYPE;
            }
        }
    } else if (stmt->isGlobal) {
        /* Global partition index only support btree index */
        if (pg_strcasecmp(stmt->accessMethod, DEFAULT_INDEX_TYPE) != 0 &&
            pg_strcasecmp(stmt->accessMethod, DEFAULT_USTORE_INDEX_TYPE) != 0) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("Global partition index only support btree.")));
        }
        if (isColStore) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("Global partition index does not support column store.")));
        }
    } else if (crossbucketopt > 0) {
        if (pg_strcasecmp(stmt->accessMethod, DEFAULT_INDEX_TYPE) != 0) {
            /* report error when presenting crossbucket option with non-btree access method */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("cross-bucket index only supports btree")));
        }
        if (isColStore) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), 
                    errmsg("cross-bucket index does not support column store")));
        }
    } else {
        bool isDfsStore = RelationIsDfsStore(rel);
        const bool isPsortMothed = (0 == pg_strcasecmp(stmt->accessMethod, DEFAULT_CSTORE_INDEX_TYPE));

        /* check if this is the cstore btree index */
        bool isCBtreeMethod = false;
        if (isColStore && ((0 == pg_strcasecmp(stmt->accessMethod, DEFAULT_INDEX_TYPE)) ||
                              (0 == pg_strcasecmp(stmt->accessMethod, CSTORE_BTREE_INDEX_TYPE)))) {
            stmt->accessMethod = CSTORE_BTREE_INDEX_TYPE;
            isCBtreeMethod = true;
        }

        /* check if this is the cstore gin btree index */
        bool isCGinBtreeMethod = false;
        if (isColStore && ((0 == pg_strcasecmp(stmt->accessMethod, DEFAULT_GIN_INDEX_TYPE)) ||
                              (0 == pg_strcasecmp(stmt->accessMethod, CSTORE_GINBTREE_INDEX_TYPE)))) {
            stmt->accessMethod = CSTORE_GINBTREE_INDEX_TYPE;
            isCGinBtreeMethod = true;
        }

        if (isCGinBtreeMethod && is_feature_disabled(MULTI_VALUE_COLUMN)) {
            /* cgin index is disabled */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Unsupport cgin index in this version")));
        }

        if (!isColStore && (0 != pg_strcasecmp(stmt->accessMethod, DEFAULT_INDEX_TYPE)) &&
            (0 != pg_strcasecmp(stmt->accessMethod, DEFAULT_GIN_INDEX_TYPE)) &&
            (0 != pg_strcasecmp(stmt->accessMethod, DEFAULT_GIST_INDEX_TYPE)) &&
            (0 != pg_strcasecmp(stmt->accessMethod, DEFAULT_USTORE_INDEX_TYPE)) &&
            (0 != pg_strcasecmp(stmt->accessMethod, DEFAULT_HASH_INDEX_TYPE))) {
            /* row store only support btree/ubtree/gin/gist/hash index */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"%s\" does not support row store", stmt->accessMethod)));
        }

        if (0 == pg_strcasecmp(stmt->accessMethod, DEFAULT_HASH_INDEX_TYPE) && 
            t_thrd.proc->workingVersionNum < SUPPORT_HASH_XLOG_VERSION_NUM) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"%s\" does not support row store", stmt->accessMethod)));
        }

        if (isColStore && (!isPsortMothed && !isCBtreeMethod && !isCGinBtreeMethod)) {
            /* column store support psort/cbtree/gin index */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"%s\" does not support column store", stmt->accessMethod)));
        } else if (isColStore && isCGinBtreeMethod && isDfsStore) {
            /* dfs store does not support cginbtree index currently */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"%s\" does not support dfs store", stmt->accessMethod)));
        }
    }

    /* no to join list, yes to namespaces */
    addRTEtoQuery(pstate, rte, false, true, true);

    /* take care of the where clause */
    if (stmt->whereClause) {
        stmt->whereClause = transformWhereClause(pstate, stmt->whereClause, "WHERE");
        /* we have to fix its collations too */
        assign_expr_collations(pstate, stmt->whereClause);
    }
    List* indexElements = NIL;
    /* take care of any index expressions */
    foreach (l, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(l);

        if (ielem->expr) {
            /* Extract preliminary index col name before transforming expr */
            if (ielem->indexcolname == NULL)
                ielem->indexcolname = FigureIndexColname(ielem->expr);

            /* Now do parse transformation of the expression */
            ielem->expr = transformExpr(pstate, ielem->expr);

            /* We have to fix its collations too */
            assign_expr_collations(pstate, ielem->expr);

            /*
             * We check only that the result type is legitimate; this is for
             * consistency with what transformWhereClause() checks for the
             * predicate.  DefineIndex() will make more checks.
             */
#ifndef ENABLE_MULTIPLE_NODES
            ExcludeRownumExpr(pstate, (Node*)ielem->expr);
#endif
            if (expression_returns_set(ielem->expr))
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("index expression cannot return a set")));
        }

        if (IsElementExisted(indexElements, ielem)) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_COLUMN), errmsg("duplicate column name")));
        }
        indexElements = lappend(indexElements, (IndexElem*)ielem);
    }

    list_free(indexElements);

    /*
     * Check that only the base rel is mentioned.
     */
    if (list_length(pstate->p_rtable) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("index expressions and predicates can refer only to the table being indexed")));

    free_parsestate(pstate);

    /* Close relation */
    heap_close(rel, NoLock);

    /* check psort index compatible */
    if (0 == pg_strcasecmp(stmt->accessMethod, DEFAULT_CSTORE_INDEX_TYPE)) {
        checkPsortIndexCompatible(stmt);
    }

    /* check psort index compatible */
    if (0 == pg_strcasecmp(stmt->accessMethod, CSTORE_BTREE_INDEX_TYPE)) {
        checkCBtreeIndexCompatible(stmt);
    }

    /* check cgin btree index compatible */
    if (0 == pg_strcasecmp(stmt->accessMethod, CSTORE_GINBTREE_INDEX_TYPE)) {
        checkCGinBtreeIndexCompatible(stmt);
    }

    return stmt;
}

static bool IsElementExisted(List* indexElements, IndexElem* ielem)
{
    ListCell* lc = NULL;
    foreach (lc, indexElements) {
        IndexElem* theElement = (IndexElem*)lfirst(lc);
        if (equal(theElement, ielem)) {
            return true;
        }
    }
    return false;
}

/*
 * transformRuleStmt -
 *	  transform a CREATE RULE Statement. The action is a list of parse
 *	  trees which is transformed into a list of query trees, and we also
 *	  transform the WHERE clause if any.
 *
 * actions and whereClause are output parameters that receive the
 * transformed results.
 *
 * Note that we must not scribble on the passed-in RuleStmt, so we do
 * copyObject() on the actions and WHERE clause.
 */
void transformRuleStmt(RuleStmt* stmt, const char* queryString, List** actions, Node** whereClause)
{
    Relation rel;
    ParseState* pstate = NULL;
    RangeTblEntry* oldrte = NULL;
    RangeTblEntry* newrte = NULL;

    /*
     * To avoid deadlock, make sure the first thing we do is grab
     * AccessExclusiveLock on the target relation.	This will be needed by
     * DefineQueryRewrite(), and we don't want to grab a lesser lock
     * beforehand.
     */
    rel = heap_openrv(stmt->relation, AccessExclusiveLock);

    if (rel->rd_rel->relkind == RELKIND_MATVIEW)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("rules on materialized views are not supported")));

    if (rel->rd_rel->relkind == RELKIND_CONTQUERY)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("rules on contqueries are not supported")));

    /* Set up pstate */
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    /*
     * NOTE: 'OLD' must always have a varno equal to 1 and 'NEW' equal to 2.
     * Set up their RTEs in the main pstate for use in parsing the rule
     * qualification.
     */
    oldrte = addRangeTableEntryForRelation(pstate, rel, makeAlias("old", NIL), false, false);
    newrte = addRangeTableEntryForRelation(pstate, rel, makeAlias("new", NIL), false, false);
    /* Must override addRangeTableEntry's default access-check flags */
    oldrte->requiredPerms = 0;
    newrte->requiredPerms = 0;

    /*
     * They must be in the namespace too for lookup purposes, but only add the
     * one(s) that are relevant for the current kind of rule.  In an UPDATE
     * rule, quals must refer to OLD.field or NEW.field to be unambiguous, but
     * there's no need to be so picky for INSERT & DELETE.  We do not add them
     * to the joinlist.
     */
    switch (stmt->event) {
        case CMD_SELECT:
            addRTEtoQuery(pstate, oldrte, false, true, true);
            break;
        case CMD_UPDATE:
            addRTEtoQuery(pstate, oldrte, false, true, true);
            addRTEtoQuery(pstate, newrte, false, true, true);
            break;
        case CMD_INSERT:
            addRTEtoQuery(pstate, newrte, false, true, true);
            break;
        case CMD_DELETE:
            addRTEtoQuery(pstate, oldrte, false, true, true);
            break;
        case CMD_UTILITY:
            addRTEtoQuery(pstate, newrte, false, true, true);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                    errmodule(MOD_OPT),
                    errmsg("unrecognized event type: %d", (int)stmt->event)));

            break;
    }

    /* take care of the where clause */
    *whereClause = transformWhereClause(pstate, (Node*)copyObject(stmt->whereClause), "WHERE");
    /* we have to fix its collations too */
    assign_expr_collations(pstate, *whereClause);

    if (list_length(pstate->p_rtable) != 2) /* naughty, naughty... */
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("rule WHERE condition cannot contain references to other relations")));

    /* aggregates not allowed (but subselects are okay) */
    if (pstate->p_hasAggs)
        ereport(
            ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in rule WHERE condition")));
    if (pstate->p_hasWindowFuncs)
        ereport(
            ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("cannot use window function in rule WHERE condition")));

    /*
     * 'instead nothing' rules with a qualification need a query rangetable so
     * the rewrite handler can add the negated rule qualification to the
     * original query. We create a query with the new command type CMD_NOTHING
     * here that is treated specially by the rewrite system.
     */
    if (stmt->actions == NIL) {
        Query* nothing_qry = makeNode(Query);

        nothing_qry->commandType = CMD_NOTHING;
        nothing_qry->rtable = pstate->p_rtable;
        nothing_qry->jointree = makeFromExpr(NIL, NULL); /* no join wanted */

        *actions = list_make1(nothing_qry);
    } else {
        ListCell* l = NULL;
        List* newactions = NIL;

        /*
         * transform each statement, like parse_sub_analyze()
         */
        foreach (l, stmt->actions) {
            Node* action = (Node*)lfirst(l);
            ParseState* sub_pstate = make_parsestate(NULL);
            Query *sub_qry = NULL, *top_subqry = NULL;
            bool has_old = false, has_new = false;

#ifdef PGXC
            if (IsA(action, NotifyStmt))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("Rule may not use NOTIFY, it is not yet supported")));
#endif
            /*
             * Since outer ParseState isn't parent of inner, have to pass down
             * the query text by hand.
             */
            sub_pstate->p_sourcetext = queryString;

            /*
             * Set up OLD/NEW in the rtable for this statement.  The entries
             * are added only to relnamespace, not varnamespace, because we
             * don't want them to be referred to by unqualified field names
             * nor "*" in the rule actions.  We decide later whether to put
             * them in the joinlist.
             */
            oldrte = addRangeTableEntryForRelation(sub_pstate, rel, makeAlias("old", NIL), false, false);
            newrte = addRangeTableEntryForRelation(sub_pstate, rel, makeAlias("new", NIL), false, false);
            oldrte->requiredPerms = 0;
            newrte->requiredPerms = 0;
            addRTEtoQuery(sub_pstate, oldrte, false, true, false);
            addRTEtoQuery(sub_pstate, newrte, false, true, false);

            /* Transform the rule action statement */
            top_subqry = transformStmt(sub_pstate, (Node*)copyObject(action));
            /*
             * We cannot support utility-statement actions (eg NOTIFY) with
             * nonempty rule WHERE conditions, because there's no way to make
             * the utility action execute conditionally.
             */
            if (top_subqry->commandType == CMD_UTILITY && *whereClause != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("rules with WHERE conditions can only have SELECT, INSERT, UPDATE, or DELETE actions")));

            /*
             * If the action is INSERT...SELECT, OLD/NEW have been pushed down
             * into the SELECT, and that's what we need to look at. (Ugly
             * kluge ... try to fix this when we redesign querytrees.)
             */
            sub_qry = getInsertSelectQuery(top_subqry, NULL);
            /*
             * If the sub_qry is a setop, we cannot attach any qualifications
             * to it, because the planner won't notice them.  This could
             * perhaps be relaxed someday, but for now, we may as well reject
             * such a rule immediately.
             */
            if (sub_qry->setOperations != NULL && *whereClause != NULL)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));

            /*
             * Validate action's use of OLD/NEW, qual too
             */
            has_old = rangeTableEntry_used((Node*)sub_qry, PRS2_OLD_VARNO, 0) ||
                      rangeTableEntry_used(*whereClause, PRS2_OLD_VARNO, 0);
            has_new = rangeTableEntry_used((Node*)sub_qry, PRS2_NEW_VARNO, 0) ||
                      rangeTableEntry_used(*whereClause, PRS2_NEW_VARNO, 0);

            switch (stmt->event) {
                case CMD_SELECT:
                case CMD_UTILITY:
                    if (has_old)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), 
                            errmsg("ON SELECT or UTILITY rule cannot use OLD")));
                    if (has_new)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), 
                            errmsg("ON SELECT or UTILITY rule cannot use NEW")));
                    break;
                case CMD_UPDATE:
                    /* both are OK */
                    break;
                case CMD_INSERT:
                    if (has_old)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("ON INSERT rule cannot use OLD")));
                    break;
                case CMD_DELETE:
                    if (has_new)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("ON DELETE rule cannot use NEW")));
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmodule(MOD_OPT),
                            errmsg("unrecognized event type: %d", (int)stmt->event)));
                    break;
            }

            /*
             * OLD/NEW are not allowed in WITH queries, because they would
             * amount to outer references for the WITH, which we disallow.
             * However, they were already in the outer rangetable when we
             * analyzed the query, so we have to check.
             *
             * Note that in the INSERT...SELECT case, we need to examine the
             * CTE lists of both top_subqry and sub_qry.
             *
             * Note that we aren't digging into the body of the query looking
             * for WITHs in nested sub-SELECTs.  A WITH down there can
             * legitimately refer to OLD/NEW, because it'd be an
             * indirect-correlated outer reference.
             */
            if (rangeTableEntry_used((Node*)top_subqry->cteList, PRS2_OLD_VARNO, 0) ||
                rangeTableEntry_used((Node*)sub_qry->cteList, PRS2_OLD_VARNO, 0))
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot refer to OLD within WITH query")));
            if (rangeTableEntry_used((Node*)top_subqry->cteList, PRS2_NEW_VARNO, 0) ||
                rangeTableEntry_used((Node*)sub_qry->cteList, PRS2_NEW_VARNO, 0))
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot refer to NEW within WITH query")));

            /*
             * For efficiency's sake, add OLD to the rule action's jointree
             * only if it was actually referenced in the statement or qual.
             *
             * For INSERT, NEW is not really a relation (only a reference to
             * the to-be-inserted tuple) and should never be added to the
             * jointree.
             *
             * For UPDATE, we treat NEW as being another kind of reference to
             * OLD, because it represents references to *transformed* tuples
             * of the existing relation.  It would be wrong to enter NEW
             * separately in the jointree, since that would cause a double
             * join of the updated relation.  It's also wrong to fail to make
             * a jointree entry if only NEW and not OLD is mentioned.
             */
            if (has_old || (has_new && stmt->event == CMD_UPDATE)) {
                /*
                 * If sub_qry is a setop, manipulating its jointree will do no
                 * good at all, because the jointree is dummy. (This should be
                 * a can't-happen case because of prior tests.)
                 */
                if (sub_qry->setOperations != NULL)
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("conditional UNION/INTERSECT/EXCEPT statements are not implemented")));
                /* hack so we can use addRTEtoQuery() */
                sub_pstate->p_rtable = sub_qry->rtable;
                sub_pstate->p_joinlist = sub_qry->jointree->fromlist;
                addRTEtoQuery(sub_pstate, oldrte, true, false, false);
                sub_qry->jointree->fromlist = sub_pstate->p_joinlist;
            }

            newactions = lappend(newactions, top_subqry);

            free_parsestate(sub_pstate);
        }

        *actions = newactions;
    }

    free_parsestate(pstate);

    /* Close relation, but keep the exclusive lock */
    heap_close(rel, NoLock);
}

/*
 * transformAlterTableStmt -
 *		parse analysis for ALTER TABLE
 *
 * Returns a List of utility commands to be done in sequence.  One of these
 * will be the transformed AlterTableStmt, but there may be additional actions
 * to be done before and after the actual AlterTable() call.
 */
List* transformAlterTableStmt(Oid relid, AlterTableStmt* stmt, const char* queryString)
{
    Relation rel;
    ParseState* pstate = NULL;
    CreateStmtContext cxt;
    List* result = NIL;
    List* save_alist = NIL;
    ListCell *lcmd = NULL, *l = NULL;
    List* newcmds = NIL;
    bool skipValidation = true;
    AlterTableCmd* newcmd = NULL;
    Node* rangePartDef = NULL;
    AddPartitionState* addDefState = NULL;
    AddSubPartitionState* addSubdefState = NULL;
    SplitPartitionState* splitDefState = NULL;
    ListCell* cell = NULL;

    /*
     * We must not scribble on the passed-in AlterTableStmt, so copy it. (This
     * is overkill, but easy.)
     */
    stmt = (AlterTableStmt*)copyObject(stmt);
    /* Caller is responsible for locking the relation */
    rel = relation_open(relid, NoLock);
    if (IS_FOREIGNTABLE(rel) || IS_STREAM_TABLE(rel)) {
        /*
         * In the security mode, the useft privilege of a user must be
         * checked before the user alters a foreign table.
         */
        if (isSecurityMode && !have_useft_privilege()) {
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("permission denied to alter foreign table in security mode")));
        }
    }

    /* Set up pstate and CreateStmtContext */
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    cxt.pstate = pstate;
    if (stmt->relkind == OBJECT_FOREIGN_TABLE || stmt->relkind == OBJECT_STREAM) {
        cxt.stmtType = ALTER_FOREIGN_TABLE;
    } else {
        cxt.stmtType = ALTER_TABLE;
    }
    cxt.relation = stmt->relation;
    cxt.relation->relpersistence = RelationGetRelPersistence(rel);
    cxt.rel = rel;
    cxt.inhRelations = NIL;
    cxt.isalter = true;
    cxt.hasoids = false; /* need not be right */
    cxt.columns = NIL;
    cxt.ckconstraints = NIL;
    cxt.fkconstraints = NIL;
    cxt.ixconstraints = NIL;
    cxt.clusterConstraints = NIL;
    cxt.inh_indexes = NIL;
    cxt.blist = NIL;
    cxt.alist = NIL;
    cxt.pkey = NULL;
    cxt.ispartitioned = RelationIsPartitioned(rel);

#ifdef PGXC
    cxt.fallback_dist_col = NULL;
    cxt.distributeby = NULL;
    cxt.subcluster = NULL;
#endif
    cxt.node = (Node*)stmt;
    cxt.isResizing = false;
    cxt.ofType = false;

    if (RelationIsForeignTable(rel) || RelationIsStream(rel)) {
        cxt.canInfomationalConstraint = CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(RelationGetRelid(rel));
    } else {
        cxt.canInfomationalConstraint = false;
    }

    /*
     * The only subtypes that currently require parse transformation handling
     * are ADD COLUMN and ADD CONSTRAINT.  These largely re-use code from
     * CREATE TABLE.
     */
    foreach (lcmd, stmt->cmds) {
        AlterTableCmd* cmd = (AlterTableCmd*)lfirst(lcmd);

        elog(ES_LOGLEVEL, "[transformAlterTableStmt] cmd subtype: %d", cmd->subtype);

        switch (cmd->subtype) {
            case AT_AddColumn:
            case AT_AddColumnToView: {
                ColumnDef* def = (ColumnDef*)cmd->def;

                AssertEreport(IsA(def, ColumnDef), MOD_OPT, "");
                transformColumnDefinition(&cxt, def, false);

                /*
                 * If the column has a non-null default, we can't skip
                 * validation of foreign keys.
                 */
                if (def->raw_default != NULL)
                    skipValidation = false;

                /*
                 * All constraints are processed in other ways. Remove the
                 * original list
                 */
                def->constraints = NIL;

                newcmds = lappend(newcmds, cmd);
                break;
            }
            case AT_AddConstraint:

                /*
                 * The original AddConstraint cmd node doesn't go to newcmds
                 */
                if (IsA(cmd->def, Constraint)) {
                    transformTableConstraint(&cxt, (Constraint*)cmd->def);
                    if (((Constraint*)cmd->def)->contype == CONSTR_FOREIGN) {
                        skipValidation = false;
                    }
                } else
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_OPT),
                            errmsg("unrecognized node type: %d", (int)nodeTag(cmd->def))));
                break;

            case AT_ProcessedConstraint:

                /*
                 * Already-transformed ADD CONSTRAINT, so just make it look
                 * like the standard case.
                 */
                cmd->subtype = AT_AddConstraint;
                newcmds = lappend(newcmds, cmd);
                break;
            case AT_AddPartition:
                /* transform the boundary of range partition,
                 * this step transform it from A_Const into Const */
                addDefState = (AddPartitionState*)cmd->def;
                if (!PointerIsValid(addDefState)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("missing definition of adding partition")));
                }
                /* A_Const -->Const */
                foreach (cell, addDefState->partitionList) {
                    rangePartDef = (Node*)lfirst(cell);
                    transformPartitionValue(pstate, rangePartDef, true);
                }

                /* transform START/END into LESS/THAN:
                 * Put this part behind the transformPartitionValue().
                 */
                if (addDefState->isStartEnd) {
                    List* pos = NIL;
                    int32 partNum;
                    Const* lowBound = NULL;

                    if (!RELATION_IS_PARTITIONED(rel))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OPERATION),
                                errmodule(MOD_OPT),
                                errmsg("can not add partition against NON-PARTITIONED table")));

                    /* get partition number */
                    partNum = getNumberOfPartitions(rel);
                    if (partNum >= MAX_PARTITION_NUM)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_OPERATION),
                                errmodule(MOD_OPT),
                                errmsg("the current relation have already reached max number of partitions")));

                    /* get partition info */
                    get_rel_partition_info(rel, &pos, &lowBound);

                    /* entry of transform */
                    addDefState->partitionList = transformRangePartStartEndStmt(
                        pstate, addDefState->partitionList, pos, rel->rd_att->attrs, partNum, lowBound, NULL, true);
                }

                newcmds = lappend(newcmds, cmd);
                break;

            case AT_AddSubPartition:
                /* transform the boundary of subpartition,
                 * this step transform it from A_Const into Const */
                addSubdefState = (AddSubPartitionState*)cmd->def;
                if (!PointerIsValid(addSubdefState)) {
                    ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_OPT),
                        errmsg("Missing definition of adding subpartition"),
                        errdetail("The AddSubPartitionState in ADD SUBPARTITION command is not found"),
                        errcause("Try ADD SUBPARTITION without subpartition defination"),
                        erraction("Please check DDL syntax for \"ADD SUBPARTITION\"")));
                }
                /* A_Const -->Const */
                foreach (cell, addSubdefState->subPartitionList) {
                    rangePartDef = (Node*)lfirst(cell);
                    transformPartitionValue(pstate, rangePartDef, true);
                }

                newcmds = lappend(newcmds, cmd);
                break;

            case AT_DropPartition:
            case AT_DropSubPartition:
            case AT_TruncatePartition:
            case AT_ExchangePartition:
            case AT_TruncateSubPartition:
                /* transform the boundary of range partition,
                 * this step transform it from A_Const into Const */
                rangePartDef = (Node*)cmd->def;
                if (PointerIsValid(rangePartDef)) {
                    transformPartitionValue(pstate, rangePartDef, false);
                }

                newcmds = lappend(newcmds, cmd);
                break;

            case AT_SplitPartition:
                if (!RELATION_IS_PARTITIONED(rel)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmodule(MOD_OPT),
                            errmsg("can not split partition against NON-PARTITIONED table")));
                }
                if (RelationIsSubPartitioned(rel)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             (errmsg("Un-support feature"),
                              errdetail("For subpartition table, split partition is not supported yet."),
                              errcause("The function is not implemented."), erraction("Use other actions instead."))));
                }
                if (rel->partMap->type == PART_TYPE_LIST || rel->partMap->type == PART_TYPE_HASH) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("can not split LIST/HASH partition table")));
                }

                /* transform the boundary of range partition: from A_Const into Const */
                splitDefState = (SplitPartitionState*)cmd->def;
                if (!PointerIsValid(splitDefState->split_point)) {
                    foreach (cell, splitDefState->dest_partition_define_list) {
                        rangePartDef = (Node*)lfirst(cell);
                        transformPartitionValue(pstate, rangePartDef, true);
                    }
                }
                if (splitDefState->partition_for_values)
                    splitDefState->partition_for_values =
                        transformRangePartitionValueInternal(pstate, splitDefState->partition_for_values, true, true);

                /* transform the start/end into less/than */
                if (is_start_end_def_list(splitDefState->dest_partition_define_list)) {
                    List* pos = NIL;
                    int32 partNum;
                    Const* lowBound = NULL;
                    Const* upBound = NULL;
                    Oid srcPartOid = InvalidOid;

                    /* get partition number */
                    partNum = getNumberOfPartitions(rel);

                    /* get partition info */
                    get_rel_partition_info(rel, &pos, NULL);

                    /* get source partition bound */
                    srcPartOid = get_split_partition_oid(rel, splitDefState);
                    if (!OidIsValid(srcPartOid)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_UNDEFINED_TABLE),
                                errmsg("split partition \"%s\" does not exist.", splitDefState->src_partition_name)));
                    }
                    get_src_partition_bound(rel, srcPartOid, &lowBound, &upBound);

                    /* entry of transform */
                    splitDefState->dest_partition_define_list = transformRangePartStartEndStmt(pstate,
                        splitDefState->dest_partition_define_list,
                        pos,
                        rel->rd_att->attrs,
                        partNum - 1,
                        lowBound,
                        upBound,
                        true);
                }

                newcmds = lappend(newcmds, cmd);
                break;

            case AT_SplitSubPartition:
                splitDefState = (SplitPartitionState*)cmd->def;
                if (splitDefState->splitType == LISTSUBPARTITIION) {
                    newcmds = lappend(newcmds, cmd);
                } else if (splitDefState->splitType == RANGESUBPARTITIION) {
                    if (!PointerIsValid(splitDefState->split_point)) {
                        foreach (cell, splitDefState->dest_partition_define_list) {
                            rangePartDef = (Node *)lfirst(cell);
                            transformPartitionValue(pstate, rangePartDef, true);
                        }
                    }
                    newcmds = lappend(newcmds, cmd);
                }
                break;
            default:
                newcmds = lappend(newcmds, cmd);
                break;
        }
    }

    /*
     * transformIndexConstraints wants cxt.alist to contain only index
     * statements, so transfer anything we already have into save_alist
     * immediately.
     */
    save_alist = cxt.alist;
    cxt.alist = NIL;

    /* Postprocess index and FK constraints */
    transformIndexConstraints(&cxt);

    transformFKConstraints(&cxt, skipValidation, true);

    /*
     * Check partial cluster key constraints
     */
    checkClusterConstraints(&cxt);

    /*
     * Check reserve column
     */
    checkReserveColumn(&cxt);

    if ((stmt->relkind == OBJECT_FOREIGN_TABLE || stmt->relkind == OBJECT_STREAM) && cxt.alist != NIL) {
        Oid relationId;
        relationId = RelationGetRelid(rel);

#ifdef ENABLE_MOT
        if (isMOTFromTblOid(relationId) || CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(relationId)) {
#else
        if (CAN_BUILD_INFORMATIONAL_CONSTRAINT_BY_RELID(relationId)) {
#endif
            setInternalFlagIndexStmt(cxt.alist);
        }
    }

    /*
     * Push any index-creation commands into the ALTER, so that they can be
     * scheduled nicely by tablecmds.c.  Note that tablecmds.c assumes that
     * the IndexStmt attached to an AT_AddIndex or AT_AddIndexConstraint
     * subcommand has already been through transformIndexStmt.
     */
    foreach (l, cxt.alist) {
        IndexStmt* idxstmt = (IndexStmt*)lfirst(l);
        AssertEreport(IsA(idxstmt, IndexStmt), MOD_OPT, "");
        idxstmt = transformIndexStmt(relid, idxstmt, queryString);
        newcmd = makeNode(AlterTableCmd);
        newcmd->subtype = OidIsValid(idxstmt->indexOid) ? AT_AddIndexConstraint : AT_AddIndex;
        newcmd->def = (Node*)idxstmt;
        newcmds = lappend(newcmds, newcmd);
    }
    cxt.alist = NIL;

    /* Append any CHECK or FK constraints to the commands list */
    foreach (l, cxt.ckconstraints) {
        newcmd = makeNode(AlterTableCmd);
        newcmd->subtype = AT_AddConstraint;
        newcmd->def = (Node*)lfirst(l);
        newcmds = lappend(newcmds, newcmd);
    }
    foreach (l, cxt.fkconstraints) {
        newcmd = makeNode(AlterTableCmd);
        newcmd->subtype = AT_AddConstraint;
        newcmd->def = (Node*)lfirst(l);
        newcmds = lappend(newcmds, newcmd);
    }

    foreach (l, cxt.clusterConstraints) {
        newcmd = makeNode(AlterTableCmd);
        newcmd->subtype = AT_AddConstraint;
        newcmd->def = (Node*)lfirst(l);
        newcmds = lappend(newcmds, newcmd);
    }
    /* Close rel */
    relation_close(rel, NoLock);

    /*
     * Output results.
     */
    stmt->cmds = newcmds;

    result = lappend(cxt.blist, stmt);
    result = list_concat(result, cxt.alist);
    result = list_concat(result, save_alist);

    return result;
}

/*
 * Preprocess a list of column constraint clauses
 * to attach constraint attributes to their primary constraint nodes
 * and detect inconsistent/misplaced constraint attributes.
 *
 * NOTE: currently, attributes are only supported for FOREIGN KEY, UNIQUE,
 * EXCLUSION, and PRIMARY KEY constraints, but someday they ought to be
 * supported for other constraint types.
 */
static void transformConstraintAttrs(CreateStmtContext* cxt, List* constraintList)
{
    Constraint* lastprimarycon = NULL;
    bool saw_deferrability = false;
    bool saw_initially = false;
    ListCell* clist = NULL;

#define SUPPORTS_ATTRS(node)                                                                     \
    ((node) != NULL && ((node)->contype == CONSTR_PRIMARY || (node)->contype == CONSTR_UNIQUE || \
                           (node)->contype == CONSTR_EXCLUSION || (node)->contype == CONSTR_FOREIGN))

    foreach (clist, constraintList) {
        Constraint* con = (Constraint*)lfirst(clist);

        if (!IsA(con, Constraint))
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(con))));
        switch (con->contype) {
            case CONSTR_ATTR_DEFERRABLE:
                if (!SUPPORTS_ATTRS(lastprimarycon))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("misplaced DEFERRABLE clause"),
                            parser_errposition(cxt->pstate, con->location)));
                if (saw_deferrability)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
                            parser_errposition(cxt->pstate, con->location)));
                saw_deferrability = true;
                lastprimarycon->deferrable = true;
                break;

            case CONSTR_ATTR_NOT_DEFERRABLE:
                if (!SUPPORTS_ATTRS(lastprimarycon))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("misplaced NOT DEFERRABLE clause"),
                            parser_errposition(cxt->pstate, con->location)));
                if (saw_deferrability)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("multiple DEFERRABLE/NOT DEFERRABLE clauses not allowed"),
                            parser_errposition(cxt->pstate, con->location)));
                saw_deferrability = true;
                lastprimarycon->deferrable = false;
                if (saw_initially && lastprimarycon->initdeferred)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
                            parser_errposition(cxt->pstate, con->location)));
                break;

            case CONSTR_ATTR_DEFERRED:
                if (!SUPPORTS_ATTRS(lastprimarycon))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("misplaced INITIALLY DEFERRED clause"),
                            parser_errposition(cxt->pstate, con->location)));
                if (saw_initially)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
                            parser_errposition(cxt->pstate, con->location)));
                saw_initially = true;
                lastprimarycon->initdeferred = true;

                /*
                 * If only INITIALLY DEFERRED appears, assume DEFERRABLE
                 */
                if (!saw_deferrability)
                    lastprimarycon->deferrable = true;
                else if (!lastprimarycon->deferrable)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
                            parser_errposition(cxt->pstate, con->location)));
                break;

            case CONSTR_ATTR_IMMEDIATE:
                if (!SUPPORTS_ATTRS(lastprimarycon))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("misplaced INITIALLY IMMEDIATE clause"),
                            parser_errposition(cxt->pstate, con->location)));
                if (saw_initially)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("multiple INITIALLY IMMEDIATE/DEFERRED clauses not allowed"),
                            parser_errposition(cxt->pstate, con->location)));
                saw_initially = true;
                lastprimarycon->initdeferred = false;
                break;

            default:
                /* Otherwise it's not an attribute */
                lastprimarycon = con;
                /* reset flags for new primary node */
                saw_deferrability = false;
                saw_initially = false;
                break;
        }
    }
}

/*
 * Special handling of type definition for a column
 */
static void transformColumnType(CreateStmtContext* cxt, ColumnDef* column)
{
    /*
     * All we really need to do here is verify that the type is valid,
     * including any collation spec that might be present.
     */
    Type ctype = typenameType(cxt->pstate, column->typname, NULL);

    if (column->collClause) {
        Form_pg_type typtup = (Form_pg_type)GETSTRUCT(ctype);

        LookupCollation(cxt->pstate, column->collClause->collname, column->collClause->location);
        /* Complain if COLLATE is applied to an uncollatable type */
        if (!OidIsValid(typtup->typcollation))
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("collations are not supported by type %s", format_type_be(HeapTupleGetOid(ctype))),
                    parser_errposition(cxt->pstate, column->collClause->location)));
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow package or procedure type as column type */
    if (IsPackageDependType(typeTypeId(ctype), InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmodule(MOD_PLSQL),
                errmsg("type \"%s\" is not supported as column type", TypeNameToString(column->typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(column->typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif
    ReleaseSysCache(ctype);
}

/*
 * transformCreateSchemaStmt -
 *	  analyzes the CREATE SCHEMA statement
 *
 * Split the schema element list into individual commands and place
 * them in the result list in an order such that there are no forward
 * references (e.g. GRANT to a table created later in the list). Note
 * that the logic we use for determining forward references is
 * presently quite incomplete.
 *
 * SQL92 also allows constraints to make forward references, so thumb through
 * the table columns and move forward references to a posterior alter-table
 * command.
 *
 * The result is a list of parse nodes that still need to be analyzed ---
 * but we can't analyze the later commands until we've executed the earlier
 * ones, because of possible inter-object references.
 *
 * Note: this breaks the rules a little bit by modifying schema-name fields
 * within passed-in structs.  However, the transformation would be the same
 * if done over, so it should be all right to scribble on the input to this
 * extent.
 */
List* transformCreateSchemaStmt(CreateSchemaStmt* stmt)
{
    CreateSchemaStmtContext cxt;
    List* result = NIL;
    ListCell* elements = NULL;

    cxt.stmtType = "CREATE SCHEMA";
    cxt.schemaname = stmt->schemaname;
    cxt.authid = stmt->authid;
    cxt.sequences = NIL;
    cxt.tables = NIL;
    cxt.views = NIL;
    cxt.indexes = NIL;
    cxt.triggers = NIL;
    cxt.grants = NIL;

    /*
     * Run through each schema element in the schema element list. Separate
     * statements by type, and do preliminary analysis.
     */
    foreach (elements, stmt->schemaElts) {
        Node* element = (Node*)lfirst(elements);

        switch (nodeTag(element)) {
            case T_CreateSeqStmt: {
                CreateSeqStmt* elp = (CreateSeqStmt*)element;

                setSchemaName(cxt.schemaname, &elp->sequence->schemaname);
                cxt.sequences = lappend(cxt.sequences, element);
            } break;

            case T_CreateStmt: {
                CreateStmt* elp = (CreateStmt*)element;

                setSchemaName(cxt.schemaname, &elp->relation->schemaname);

                cxt.tables = lappend(cxt.tables, element);
            } break;

            case T_ViewStmt: {
                ViewStmt* elp = (ViewStmt*)element;

                setSchemaName(cxt.schemaname, &elp->view->schemaname);

                cxt.views = lappend(cxt.views, element);
            } break;

            case T_IndexStmt: {
                IndexStmt* elp = (IndexStmt*)element;

                setSchemaName(cxt.schemaname, &elp->relation->schemaname);
                cxt.indexes = lappend(cxt.indexes, element);
            } break;

            case T_CreateTrigStmt: {
                CreateTrigStmt* elp = (CreateTrigStmt*)element;

                setSchemaName(cxt.schemaname, &elp->relation->schemaname);
                cxt.triggers = lappend(cxt.triggers, element);
            } break;

            case T_GrantStmt:
            case T_AlterRoleStmt:
                cxt.grants = lappend(cxt.grants, element);
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(element))));
        }
    }

    result = NIL;
    result = list_concat(result, cxt.sequences);
    result = list_concat(result, cxt.tables);
    result = list_concat(result, cxt.views);
    result = list_concat(result, cxt.indexes);
    result = list_concat(result, cxt.triggers);
    result = list_concat(result, cxt.grants);

    return result;
}

/*
 * setSchemaName
 *		Set or check schema name in an element of a CREATE SCHEMA command
 */
static void setSchemaName(char* context_schema, char** stmt_schema_name)
{
    if (*stmt_schema_name == NULL)
        *stmt_schema_name = context_schema;
    else if (strcmp(context_schema, *stmt_schema_name) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_SCHEMA_DEFINITION),
                errmsg("CREATE specifies a schema (%s) "
                       "different from the one being created (%s)",
                    *stmt_schema_name,
                    context_schema)));
}

NodeTag GetPartitionStateType(char type)
{
    NodeTag partitionType = T_Invalid;
    switch (type) {
        case 'r':
            partitionType = T_RangePartitionDefState;
            break;
        case 'l':
            partitionType = T_ListPartitionDefState;
            break;
        case 'h':
            partitionType = T_HashPartitionDefState;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                            errmsg("unsupported subpartition type: %c", type)));
            break;
    }
    return partitionType;
}

char* GetPartitionDefStateName(Node *partitionDefState)
{
    char* partitionName = NULL;
    switch (nodeTag(partitionDefState)) {
        case T_RangePartitionDefState:
            partitionName = ((RangePartitionDefState *)partitionDefState)->partitionName;
            break;
        case T_ListPartitionDefState:
            partitionName = ((ListPartitionDefState *)partitionDefState)->partitionName;
            break;
        case T_HashPartitionDefState:
            partitionName = ((HashPartitionDefState *)partitionDefState)->partitionName;
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unsupported subpartition type")));
            break;
    }
    return partitionName;
}

List* GetSubPartitionDefStateList(Node *partitionDefState)
{
    List* subPartitionList = NIL;
    switch (nodeTag(partitionDefState)) {
        case T_RangePartitionDefState:
            subPartitionList = ((RangePartitionDefState *)partitionDefState)->subPartitionDefState;
            break;
        case T_ListPartitionDefState:
            subPartitionList = ((ListPartitionDefState *)partitionDefState)->subPartitionDefState;
            break;
        case T_HashPartitionDefState:
            subPartitionList = ((HashPartitionDefState *)partitionDefState)->subPartitionDefState;
            break;
        default:
            subPartitionList = NIL;
            break;
    }
    return subPartitionList;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: check synax for range partition defination
 * Description	:
 * Notes		:
 */
void checkPartitionSynax(CreateStmt* stmt)
{
    ListCell* cell = NULL;
    bool value_partition = false;

    /* unsupport inherits clause */
    if (stmt->inhRelations) {
        if (stmt->partTableState) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unsupport inherits clause for partitioned table")));
        } else {
            foreach (cell, stmt->inhRelations) {
                RangeVar* inh = (RangeVar*)lfirst(cell);
                Relation rel;

                AssertEreport(IsA(inh, RangeVar), MOD_OPT, "");
                rel = heap_openrv(inh, AccessShareLock);
                /* @hdfs
                 * Deal with error mgs for foreign table, the foreign table
                 * is not inherited
                 */
                if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE || rel->rd_rel->relkind == RELKIND_STREAM) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("inherited relation \"%s\" is a foreign table", inh->relname),
                            errdetail("can not inherit from a foreign table")));
                } else if (rel->rd_rel->relkind != RELKIND_RELATION) {
                    ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("inherited relation \"%s\" is not a table", inh->relname)));
                }
                if (RELATION_IS_PARTITIONED(rel)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmodule(MOD_OPT),
                            errmsg("inherited relation \"%s\" is a partitioned table", inh->relname),
                            errdetail("can not inherit from partitioned table")));
                }
                heap_close(rel, NoLock);
            }
        }
    }
    /* is it a partitoned table? */
    if (!stmt->partTableState) {
        return;
    }

    /* check syntax for value-partitioned table */
    if (stmt->partTableState->partitionStrategy == PART_STRATEGY_VALUE) {
        value_partition = true;

        /* do partition-key null check as part of sytax check */
        if (list_length(stmt->partTableState->partitionKey) == 0) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Value-based partition table should have one column at least")));
        }

        /*
         * for value partitioned table we only do a simple sanity check to
         * ensure that any uncessary fileds are set with NULL
         */
        if (stmt->partTableState->intervalPartDef || stmt->partTableState->partitionList) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("Value-Based partition table creation encounters unexpected data in unnecessary fields"),
                    errdetail("save context and get assistance from DB Dev team")));
        }
    }

    /* unsupport om commit clause */
    if (stmt->oncommit) {
        ereport(
            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("ON COMMIT option is not supported for partitioned table")));
    }

    /* unsupport typed table */
    if (stmt->ofTypename) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Typed table can't not be partitioned")));
    }

    /* unsupport typed table */
    if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("unsupported feature with temporary/unlogged table for partitioned table")));
    }

    /* unsupport oids option */
    foreach (cell, stmt->options) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (!def->defnamespace && !pg_strcasecmp(def->defname, "oids")) {
            ereport(
                ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("OIDS option is not supported for partitioned table")));
        }
    }

    /* check partition key number for none value-partition table */
    if (!value_partition && stmt->partTableState->partitionKey->length > MAX_PARTITIONKEY_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("too many partition keys for partitioned table"),
                errhint("Partittion key columns can not be more than %d", MAX_PARTITIONKEY_NUM)));
    }

    /* check range partition number for none value-partition table */
    if (!value_partition && stmt->partTableState->partitionList->length > MAX_PARTITION_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("too many partitions for partitioned table"),
                errhint("Number of partitions can not be more than %d", MAX_PARTITION_NUM)));
    }

    /* check interval synax */
    if (stmt->partTableState->intervalPartDef) {
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("Interval partitioned table is only supported in single-node mode.")));
#else
        if (1 < stmt->partTableState->partitionKey->length) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Range partitioned table with INTERVAL clause has more than one column"),
                    errhint("Only support one partition key for interval partition")));
        }
        if (!IsA(stmt->partTableState->intervalPartDef->partInterval, A_Const) ||
            ((A_Const*)stmt->partTableState->intervalPartDef->partInterval)->val.type != T_String) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                    errmsg("invalid input syntax for type interval")));
        }
        int32 typmod = -1;
        Interval* interval = NULL;
        A_Const* node = (A_Const*)stmt->partTableState->intervalPartDef->partInterval;
        interval = char_to_interval(node->val.val.str, typmod);
        pfree(interval);
#endif
    }

    /* check subpartition synax */
    if (stmt->partTableState->subPartitionState != NULL) {
        NodeTag subPartitionType = GetPartitionStateType(stmt->partTableState->subPartitionState->partitionStrategy);
        List* partitionList = stmt->partTableState->partitionList;
        ListCell* lc1 = NULL;
        ListCell* lc2 = NULL;
        foreach (lc1, partitionList) {
            Node* partitionDefState = (Node*)lfirst(lc1);
            List* subPartitionList = GetSubPartitionDefStateList(partitionDefState);
            foreach (lc2, subPartitionList) {
                Node *subPartitionDefState = (Node *)lfirst(lc2);
                if ((nodeTag(subPartitionDefState) != subPartitionType)) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                                    errmsg("The syntax format of subpartition is incorrect, the declaration and "
                                           "definition of the subpartition do not match."),
                                    errdetail("The syntax format of subpartition %s is incorrect.",
                                              GetPartitionDefStateName(subPartitionDefState)),
                                    errcause("The declaration and definition of the subpartition do not match."),
                                    erraction("Consistent declaration and definition of subpartition.")));
                }
            }
        }
    } else {
        /* If there is a definition of subpartition, there must be a declaration of subpartition.
         * However, if there is a declaration of subpartition, there may not be a definition of subpartition.
         * This is because the system creates a default subpartition.
         */
        List* partitionList = stmt->partTableState->partitionList;
        ListCell* lc1 = NULL;
        foreach (lc1, partitionList) {
            Node* partitionDefState = (Node*)lfirst(lc1);
            List *subPartitionList = GetSubPartitionDefStateList(partitionDefState);
            if (subPartitionList != NIL) {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_INVALID_OPERATION),
                     errmsg("The syntax format of subpartition is incorrect, missing declaration of subpartition."),
                     errdetail("N/A"), errcause("Missing declaration of subpartition."),
                     erraction("Supplements declaration of subpartition.")));
            }
        }
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: check partition value
 * Description	:
 * Notes		: partition key value must be const or const-evaluable expression
 */
static void checkPartitionValue(CreateStmtContext* cxt, CreateStmt* stmt)
{
    PartitionState* partdef = NULL;
    ListCell* cell = NULL;

    partdef = stmt->partTableState;
    if (partdef == NULL) {
        return;
    }
    /* transform expression in partition definition and evaluate the expression */
    foreach (cell, partdef->partitionList) {
        Node* state = (Node*)lfirst(cell);
        transformPartitionValue(cxt->pstate, state, true);
    }
}

/*
 * check_partition_name_less_than
 *  check partition name with less/than stmt.
 *
 * [IN] partitionList: partition list
 *
 * RETURN: void
 */
static void check_partition_name_less_than(List* partitionList, bool isPartition)
{
    ListCell* cell = NULL;
    ListCell* lc = NULL;
    char* cur_partname = NULL;
    char* ref_partname = NULL;

    foreach (cell, partitionList) {
        lc = cell;
        ref_partname = ((RangePartitionDefState*)lfirst(cell))->partitionName;

        while (NULL != (lc = lnext(lc))) {
            cur_partname = ((RangePartitionDefState*)lfirst(lc))->partitionName;

            if (!strcmp(ref_partname, cur_partname)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("duplicate %s name: \"%s\"",
                             (isPartition ? "partition" : "slice"), ref_partname)));
            }
        }
    }
}

/*
 * check_partition_name_start_end
 *  check partition name with start/end stmt.
 *
 * [IN] partitionList: partition list
 *
 * RETURN: void
 */
static void check_partition_name_start_end(List* partitionList, bool isPartition)
{
    ListCell* cell = NULL;
    ListCell* lc = NULL;
    RangePartitionStartEndDefState* defState = NULL;
    RangePartitionStartEndDefState* lastState = NULL;

    foreach (cell, partitionList) {
        lc = cell;
        lastState = (RangePartitionStartEndDefState*)lfirst(cell);

        while ((lc = lnext(lc)) != NULL) {
            defState = (RangePartitionStartEndDefState*)lfirst(lc);
            if (!strcmp(lastState->partitionName, defState->partitionName)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("duplicate %s name: \"%s\"",
                            (isPartition ? "partition" : "slice"), defState->partitionName)));
            }
        }
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: check partition name
 * Description	: duplicate partition name is not allowed
 * Notes		:
 */
void checkPartitionName(List* partitionList, bool isPartition)
{
    ListCell* cell = NULL;

    cell = list_head(partitionList);
    if (cell != NULL) {
        Node* state = (Node*)lfirst(cell);

        if (IsA(state, RangePartitionDefState))
            check_partition_name_less_than(partitionList, isPartition);
        else
            check_partition_name_start_end(partitionList, isPartition);
    }
}

List* GetPartitionNameList(List* partitionList)
{
    ListCell* cell = NULL;
    ListCell* lc = NULL;
    List* subPartitionDefStateList = NIL;
    List* partitionNameList = NIL;

    foreach (cell, partitionList) {
        if (IsA((Node*)lfirst(cell), RangePartitionDefState)) {
            RangePartitionDefState* partitionDefState = (RangePartitionDefState*)lfirst(cell);
            subPartitionDefStateList = partitionDefState->subPartitionDefState;
            partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
        } else if (IsA((Node*)lfirst(cell), ListPartitionDefState)) {
            ListPartitionDefState* partitionDefState = (ListPartitionDefState*)lfirst(cell);
            subPartitionDefStateList = partitionDefState->subPartitionDefState;
            partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
        } else {
            HashPartitionDefState* partitionDefState = (HashPartitionDefState*)lfirst(cell);
            subPartitionDefStateList = partitionDefState->subPartitionDefState;
            partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
        }

        foreach (lc, subPartitionDefStateList) {
            if (IsA((Node *)lfirst(lc), RangePartitionDefState)) {
                RangePartitionDefState *partitionDefState = (RangePartitionDefState *)lfirst(lc);
                partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
            } else if (IsA((Node *)lfirst(lc), ListPartitionDefState)) {
                ListPartitionDefState *partitionDefState = (ListPartitionDefState *)lfirst(lc);
                partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
            } else {
                HashPartitionDefState *partitionDefState = (HashPartitionDefState *)lfirst(lc);
                partitionNameList = lappend(partitionNameList, partitionDefState->partitionName);
            }
        }
    }

    return partitionNameList;
}

void checkSubPartitionName(List* partitionList)
{
    ListCell* cell = NULL;
    ListCell* lc = NULL;
    List* partitionNameList = GetPartitionNameList(partitionList);

    foreach (cell, partitionNameList) {
        char *subPartitionName1 = (char *)lfirst(cell);
        lc = cell;
        while ((lc = lnext(lc)) != NULL) {
            char *subPartitionName2 = (char *)lfirst(lc);
            if (!strcmp(subPartitionName1, subPartitionName2)) {
                list_free_ext(partitionNameList);
                ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
                                errmsg("duplicate subpartition name: \"%s\"", subPartitionName2)));
            }
        }
    }
    list_free_ext(partitionNameList);
}

static void CheckDistributionSyntax(CreateStmt* stmt)
{
    DistributeBy *distBy = stmt->distributeby;
    if (distBy->disttype == DISTTYPE_ROUNDROBIN) {
        if (IsA(stmt, CreateForeignTableStmt)) {
            if (IsSpecifiedFDW(((CreateForeignTableStmt*)stmt)->servername, DIST_FDW)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("For foreign table ROUNDROBIN distribution type is built-in support.")));
            }
        } else {
            FEATURE_NOT_PUBLIC_ERROR("Unsupport ROUNDROBIN distribute type");
        }
    } else if (distBy->disttype == DISTTYPE_MODULO) {
        FEATURE_NOT_PUBLIC_ERROR("Unsupport MODULO distribute type");
    }
}

static void CheckSliceValue(CreateStmtContext *cxt, CreateStmt *stmt)
{
    DistributeBy *distBy = stmt->distributeby;
    DistState *distState = distBy->distState;
    ListCell *cell = NULL;

    foreach (cell, distState->sliceList) {
        Node *slice = (Node *)lfirst(cell);
        switch (nodeTag(slice)) {
            case T_RangePartitionDefState: {
                RangePartitionDefState *def = (RangePartitionDefState *)slice;
                def->boundary = transformRangePartitionValueInternal(cxt->pstate, def->boundary, true, true, false);
                break;
            }
            
            case T_RangePartitionStartEndDefState: {
                RangePartitionStartEndDefState *def = (RangePartitionStartEndDefState *)slice;
                def->startValue = transformRangePartitionValueInternal(cxt->pstate, def->startValue, true, true, false);
                def->endValue = transformRangePartitionValueInternal(cxt->pstate, def->endValue, true, true, false);
                def->everyValue = transformRangePartitionValueInternal(cxt->pstate, def->everyValue, true, true, false);
                break;
            }
            case T_ListSliceDefState: {
                ListCell* cell2 = NULL;
                List* newlist = NIL; /* a new list is necessary since boundaries is nested */
                ListSliceDefState *listSlice = (ListSliceDefState *)slice;

                foreach (cell2, listSlice->boundaries) {
                    Node *boundary = (Node *)lfirst(cell2);  /* could be multi-column(multi-values) */
                    TransformListDistributionValue(cxt->pstate, &boundary, true);
                    newlist = lappend(newlist, boundary);
                }

                listSlice->boundaries = newlist;
                break;
            }
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized node type: %d", (int)nodeTag(slice))));
        }
    }
}

static void TransformListDistributionValue(ParseState* pstate, Node** listDistDef, bool needCheck) 
{
    *listDistDef = (Node *)transformRangePartitionValueInternal(
        pstate, (List *)*listDistDef, needCheck, true, false);
    return;
}

static List* GetDistributionkeyPos(List* partitionkeys, List* schema)
{
    if (schema == NULL) {
        return NULL;
    }

    ListCell* cell = NULL;
    ListCell* schemaCell = NULL;
    ColumnDef* schemaDef = NULL;
    int columnCount = 0;
    bool* isExist = NULL;
    char* distributionkeyName = NULL;
    List* pos = NULL;
    int len = -1;

    len = schema->length;
    isExist = (bool*)palloc(len * sizeof(bool));
    errno_t rc = EOK;
    rc = memset_s(isExist, len * sizeof(bool), 0, len * sizeof(bool));
    securec_check(rc, "\0", "\0");

    foreach (cell, partitionkeys) {
        distributionkeyName = strVal(lfirst(cell));

        foreach (schemaCell, schema) {
            schemaDef = (ColumnDef*)lfirst(schemaCell);
            if (!strcmp(distributionkeyName, schemaDef->colname)) {
                if (isExist[columnCount]) {
                    pfree_ext(isExist);
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_COLUMN),
                         errmsg("Include identical distribution column: \"%s\"", distributionkeyName)));
                }

                isExist[columnCount] = true;
                break;
            }

            columnCount++;
        }

        if (columnCount >= len) {
            pfree_ext(isExist);
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("Invalid distribution column specified")));
        }

        pos = lappend_int(pos, columnCount);
        columnCount = 0;
        distributionkeyName = NULL;
    }

    pfree_ext(isExist);
    return pos;
}

/* get storage type from CreateStmt.optitions, which is generated by WITH clause */
static char* CreatestmtGetOrientation(CreateStmt *stmt)
{
    ListCell *lc = NULL;
    foreach (lc, stmt->options) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (pg_strcasecmp(def->defname, "orientation") == 0) {
            return defGetString(def);
        }
    }
    /* default orientation by row store */
    return ORIENTATION_ROW;
}

static void ConvertSliceStartEnd2LessThan(CreateStmtContext *cxt, CreateStmt *stmt)
{
    DistributeBy *distBy = stmt->distributeby;
    DistState *distState = distBy->distState;
    List *pos = NIL;
    TupleDesc desc;

    pos = GetDistributionkeyPos(distBy->colname, cxt->columns);
    char *storeChar = CreatestmtGetOrientation(stmt);
    if ((pg_strcasecmp(storeChar, ORIENTATION_ROW) != 0) && (pg_strcasecmp(storeChar, ORIENTATION_COLUMN) != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-supported feature"),
                    errdetail("Orientation type %s is not supported for range distributed table.", storeChar)));
    }
    desc = BuildDescForRelation(cxt->columns, (Node *)makeString(storeChar));

    distState->sliceList = transformRangePartStartEndStmt(
        cxt->pstate, distState->sliceList, pos, desc->attrs, 0, NULL, NULL, true, false);

    list_free_ext(pos);
    pfree(desc);
}

static void CheckListSliceName(List* sliceList)
{
    ListCell* cell = NULL;
    ListCell* next = NULL;
    char* refName = NULL;
    char* curName = NULL;

    foreach (cell, sliceList) {
        next = cell;
        refName = ((ListSliceDefState *)lfirst(cell))->name;
        while ((next = lnext(next)) != NULL) {
            curName = ((ListSliceDefState *)lfirst(next))->name;
            if (strcmp(refName, curName) == 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                         errmsg("duplicate slice name: \"%s\"", refName)));
            }
        }
    }

    return;
}

static void CheckSliceName(DistributeBy *distBy)
{
    if (distBy->disttype == DISTTYPE_RANGE) {
        checkPartitionName(distBy->distState->sliceList, false);
    } else {
        /* DISTTYPE_LIST */
        CheckListSliceName(distBy->distState->sliceList);
    }
}

static void CheckListRangeDistribClause(CreateStmtContext *cxt, CreateStmt *stmt)
{
    DistributeBy *distBy = stmt->distributeby;
    DistState *distState = distBy->distState;
    if (distState == NULL) {
        return;
    }

    CheckSliceValue(cxt, stmt);
    if (distBy->disttype == DISTTYPE_RANGE && is_start_end_def_list(distState->sliceList)) {
        ConvertSliceStartEnd2LessThan(cxt, stmt);
    }
    CheckSliceName(distBy);
}

/*
 * Check partial cluster key constraints
 */
static void checkClusterConstraints(CreateStmtContext* cxt)
{
    AssertEreport(cxt != NULL, MOD_OPT, "");

    if (cxt->clusterConstraints == NIL) {
        return;
    }

    ListCell* lc = NULL;
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    foreach (lc, cxt->clusterConstraints) {
        Constraint* constraint = (Constraint*)lfirst(lc);

        // for each keys find out whether have same key
        foreach (lc1, constraint->keys) {
            char* key1 = strVal(lfirst(lc1));
            lc2 = lnext(lc1);

            for (; lc2 != NULL; lc2 = lnext(lc2)) {
                char* key2 = strVal(lfirst(lc2));
                if (0 == strcasecmp(key1, key2)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_COLUMN),
                            errmsg("column \"%s\" appears twice in partial cluster key constraint", key1),
                            parser_errposition(cxt->pstate, constraint->location)));
                }
            }
        }
    }
}

/*
 * Check reserve column
 */
static void checkReserveColumn(CreateStmtContext* cxt)
{
    AssertEreport(cxt != NULL, MOD_OPT, "");

    if (cxt->columns == NIL) {
        return;
    }

    List* columns = cxt->columns;
    ListCell* lc = NULL;

    foreach (lc, columns) {
        ColumnDef* col = (ColumnDef*)lfirst(lc);
        AssertEreport(col != NULL, MOD_OPT, "");

        if (CHCHK_PSORT_RESERVE_COLUMN(col->colname)) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_COLUMN),
                    errmsg("column name \"%s\" conflicts with a system column name", col->colname)));
        }
    }
}

static void checkPsortIndexCompatible(IndexStmt* stmt)
{
    if (stmt->whereClause) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("access method \"psort\" does not support WHERE clause")));
    }

    /* psort index can not support index expressions */
    ListCell* lc = NULL;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        if (ielem->expr) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"psort\" does not support index expressions")));
        }
    }
}

static void checkCBtreeIndexCompatible(IndexStmt* stmt)
{
    if (stmt->whereClause) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("access method \"cbtree\" does not support WHERE clause")));
    }

    /* psort index can not support index expressions */
    ListCell* lc = NULL;
    foreach (lc, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(lc);

        if (ielem->expr) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("access method \"cbtree\" does not support index expressions")));
        }
    }
}

static void checkCGinBtreeIndexCompatible(IndexStmt* stmt)
{
    Assert(stmt);

    if (stmt->whereClause) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("access method \"cgin\" does not support WHERE clause")));
    }

    /* cgin index can not support null text search parser */
    ListCell* l = NULL;
    foreach (l, stmt->indexParams) {
        IndexElem* ielem = (IndexElem*)lfirst(l);
        Node* expr = ielem->expr;

        if (expr != NULL) {
            Assert(IsA(expr, FuncExpr));
            if (IsA(expr, FuncExpr)) {
                FuncExpr* funcexpr = (FuncExpr*)expr;
                Node* firstarg = (Node*)lfirst(funcexpr->args->head);

                if (IsA(firstarg, Const)) {
                    Const* constarg = (Const*)firstarg;
                    if (constarg->constisnull)
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("access method \"cgin\" does not support null text search parser")));
                }
            }
        }
    }
}

List* transformListPartitionValue(ParseState* pstate, List* boundary, bool needCheck, bool needFree)
{
    List* newValueList = NIL;
    ListCell* valueCell = NULL;
    Node* elem = NULL;
    Node* result = NULL;

    /* scan value of partition key of per partition */
    foreach (valueCell, boundary) {
        elem = (Node*)lfirst(valueCell);
        result = transformIntoConst(pstate, elem);
        if (PointerIsValid(result) && needCheck && ((Const*)result)->constisnull && !((Const*)result)->ismaxvalue) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("Partition key value can not be null"),
                    errdetail("partition bound element must be one of: string, datetime or interval literal, number, "
                              "or MAXVALUE, and not null")));
        }
        newValueList = lappend(newValueList, result);
    }

    if (needFree && boundary != NIL)
        list_free_ext(boundary); /* avoid mem leak */

    return newValueList;
}

void transformRangeSubPartitionValue(ParseState* pstate, List* subPartitionDefStateList)
{
    if (subPartitionDefStateList == NIL) {
        return;
    }
    ListCell *cell = NULL;
    foreach (cell, subPartitionDefStateList) {
        Node *subPartitionDefState = (Node *)lfirst(cell);
        transformPartitionValue(pstate, subPartitionDefState, true);
    }
}

void transformPartitionValue(ParseState* pstate, Node* rangePartDef, bool needCheck)
{
    Assert(rangePartDef); /* never null */

    switch (rangePartDef->type) {
        case T_RangePartitionDefState: {
            RangePartitionDefState* state = (RangePartitionDefState*)rangePartDef;
            /* only one boundary need transform */
            state->boundary = transformRangePartitionValueInternal(pstate, state->boundary, needCheck, true);
            transformRangeSubPartitionValue(pstate, state->subPartitionDefState);
            break;
        }
        case T_RangePartitionStartEndDefState: {
            RangePartitionStartEndDefState* state = (RangePartitionStartEndDefState*)rangePartDef;

            /* transform each point, null-case is also covered */
            state->startValue = transformRangePartitionValueInternal(pstate, state->startValue, needCheck, true);
            state->endValue = transformRangePartitionValueInternal(pstate, state->endValue, needCheck, true);
            state->everyValue = transformRangePartitionValueInternal(pstate, state->everyValue, needCheck, true);
            break;
        }
        case T_ListPartitionDefState: {
            ListPartitionDefState* state = (ListPartitionDefState*)rangePartDef;
            state->boundary = transformListPartitionValue(pstate, state->boundary, needCheck, true);
            transformRangeSubPartitionValue(pstate, state->subPartitionDefState);
            break;
        }
        case T_HashPartitionDefState: {
            HashPartitionDefState* state = (HashPartitionDefState*)rangePartDef;
            /* only one boundary need transform */
            state->boundary = transformListPartitionValue(pstate, state->boundary, needCheck, true);
            transformRangeSubPartitionValue(pstate, state->subPartitionDefState);
            break;
        }

        default:
            Assert(false); /* never happen */
    }
}

List* transformRangePartitionValueInternal(ParseState* pstate, List* boundary, bool needCheck, bool needFree, 
    bool isPartition)
{
    List* newMaxValueList = NIL;
    ListCell* valueCell = NULL;
    Node* maxElem = NULL;
    Node* result = NULL;

    /* scan max value of partition key of per partition */
    foreach (valueCell, boundary) {
        maxElem = (Node*)lfirst(valueCell);
        result = transformIntoConst(pstate, maxElem, isPartition);
        if (PointerIsValid(result) && needCheck && ((Const*)result)->constisnull && !((Const*)result)->ismaxvalue) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("%s key value can not be null", (isPartition ? "Partition" : "Distribution")),
                        errdetail("%s bound element must be one of: string, datetime or interval literal, number, "
                            "or MAXVALUE, and not null", (isPartition ? "partition" : "distribution"))));
        }
        newMaxValueList = lappend(newMaxValueList, result);
    }

    if (needFree && boundary != NIL)
        list_free_ext(boundary); /* avoid mem leak */

    return newMaxValueList;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Input		:
 * Output	:
 * Return		:
 * Notes		:
 */
Node* transformIntoConst(ParseState* pstate, Node* maxElem, bool isPartition)
{
    Node* result = NULL;
    FuncExpr* funcexpr = NULL;
    /* transform expression first */
    maxElem = transformExpr(pstate, maxElem);

    /* then, evaluate expression */
    switch (nodeTag(maxElem)) {
        case T_Const:
            result = maxElem;
            break;
        /* MaxValue for Date must be a function expression(to_date) */
        case T_FuncExpr: {
            funcexpr = (FuncExpr*)maxElem;
            result = (Node*)evaluate_expr(
                (Expr*)funcexpr, exprType((Node*)funcexpr), exprTypmod((Node*)funcexpr), funcexpr->funccollid);

            /*
             * if the function expression cannot be evaluated and output a const,
             * than report error
             */
            if (T_Const != nodeTag((Node*)result)) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("%s key value must be const or const-evaluable expression",
                            (isPartition ? "partition" : "distribution"))));
            }
        } break;
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("%s key value must be const or const-evaluable expression",
                        (isPartition ? "partition" : "distribution"))));
        } break;
    }
    return result;
}
/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:
 * Description	:
 * Notes		:
 */
Oid generateClonedIndex(Relation source_idx, Relation source_relation, char* tempIndexName, Oid targetTblspcOid,
    bool skip_build, bool partitionedIndex)
{
    CreateStmtContext cxt;
    IndexStmt* index_stmt = NULL;
    AttrNumber* attmap = NULL;
    int attmap_length, i;
    Oid heap_relid;
    Relation heap_rel;
    TupleDesc tupleDesc;
    Oid source_relid = RelationGetRelid(source_idx);
    Oid ret;

    /* get the relation that the index is created on */
    heap_relid = IndexGetRelation(source_relid, false);
    heap_rel = relation_open(heap_relid, AccessShareLock);

    /* create cxt.relation */
    cxt.relation = makeRangeVar(
        get_namespace_name(RelationGetNamespace(source_relation)), RelationGetRelationName(source_relation), -1);

    /* initialize attribute array */
    tupleDesc = RelationGetDescr(heap_rel);
    attmap_length = tupleDesc->natts;
    attmap = (AttrNumber*)palloc0(sizeof(AttrNumber) * attmap_length);
    for (i = 0; i < attmap_length; i++)
        attmap[i] = i + 1;

    /* generate an index statement */
    index_stmt = generateClonedIndexStmt(&cxt, source_idx, attmap, attmap_length, NULL, TRANSFORM_INVALID);

    if (tempIndexName != NULL)
        index_stmt->idxname = tempIndexName;

    if (OidIsValid(targetTblspcOid)) {
        /* generateClonedIndexStmt() maybe set tablespace name, so free it first. */
        if (index_stmt->tableSpace) {
            pfree_ext(index_stmt->tableSpace);
        }
        /* set target tablespace's name into index_stmt */
        index_stmt->tableSpace = get_tablespace_name(targetTblspcOid);
    }

    /* set is partitioned field */
    index_stmt->isPartitioned = partitionedIndex;

    /* don't do mem check, since there's no distribution info for new added temp table */
    index_stmt->skip_mem_check = true;

    /* Run parse analysis ... */
    index_stmt = transformIndexStmt(RelationGetRelid(source_relation), index_stmt, NULL);

    /* ... and do it */
    WaitState oldStatus = pgstat_report_waitstatus(STATE_CREATE_INDEX);
    ret = DefineIndex(RelationGetRelid(source_relation),
        index_stmt,
        InvalidOid, /* no predefined OID */
        false,      /* is_alter_table */
        true,       /* check_rights */
        skip_build, /* skip_build */
        false);     /* quiet */
    (void)pgstat_report_waitstatus(oldStatus);

    /* clean up */
    pfree_ext(attmap);
    relation_close(heap_rel, AccessShareLock);

    return ret;
}

/*
 * @hdfs
 * Brief        : set informational constraint flag in IndexStmt.
 * Description  : Set indexStmt's internal_flag. This flag will be set to false
 *                if indexStmt is built by "Creat index", otherwise be set to true.
 * Input        : the IndexStmt list.
 * Output       : none.
 * Return Value : none.
 * Notes        : This function is only used for HDFS foreign table.
 */
static void setInternalFlagIndexStmt(List* IndexList)
{
    ListCell* lc = NULL;

    Assert(IndexList != NIL);
    foreach (lc, IndexList) {
        IndexStmt* index = NULL;
        index = (IndexStmt*)lfirst(lc);
        index->internal_flag = true;
    }
}

/*
 * Brief        : Check the foreign table constraint type.
 * Description  : This function checks HDFS foreign table constraint type. The supported constraint
 *                types and some useful comment are:
 *                1. Only the primary key, unique, not null and null will be supported.
 *                2. Only "NOT ENFORCED" clause is supported for HDFS foreign table informational constraint.
 *                3. Multi-column combined informational constraint is forbidden.
 * Input        : node, the node needs to be checked.
 * Output       : none.
 * Return Value : none.
 * Notes        : none.
 */
void checkInformationalConstraint(Node* node, bool isForeignTbl)
{
    if (node == NULL) {
        return;
    }

    Constraint* constr = (Constraint*)node;

    /* Common table unsupport not force Constraint. */
    if (!isForeignTbl) {
        if (constr->inforConstraint && constr->inforConstraint->nonforced) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("It is not allowed to support \"NOT ENFORCED\" informational constraint.")));
        }
        return;
    }

    if (constr->contype == CONSTR_NULL || constr->contype == CONSTR_NOTNULL) {
        return;
    } else if (constr->contype == CONSTR_PRIMARY || constr->contype == CONSTR_UNIQUE) {
        /* HDFS foreign table only support not enforced informational primary key and unique Constraint. */
        if (constr->inforConstraint == NULL || !constr->inforConstraint->nonforced) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The foreign table only support \"NOT ENFORCED\" informational constraint.")));
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Only the primary key, unique, not null and null be supported.")));
    }

    if (constr->keys != NIL && list_length(constr->keys) != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Multi-column combined informational constraint is forbidden.")));
    }
}

/*
 * @Description: Check Constraint.
 * @in cxt: CreateStmtContext or AlterTableStmt struct.
 * @in node: Constraint or ColumnDef.
 */
static void checkConstraint(CreateStmtContext* cxt, Node* node)
{
    bool canBuildInfoConstraint = cxt->canInfomationalConstraint;

    /* Judge constraint is valid. */
    if (IsA(node, Constraint)) {
        checkInformationalConstraint(node, canBuildInfoConstraint);
    } else if (IsA(node, ColumnDef)) {
        List* constList = ((ColumnDef*)node)->constraints;
        ListCell* cell = NULL;

        foreach (cell, constList) {
            Node* element = (Node*)lfirst(cell);

            if (IsA(element, Constraint)) {
                checkInformationalConstraint(element, canBuildInfoConstraint);
            }
        }
    }
}

/*
 * @Description: set skip mem check flag for index stmt. If the
 *	index is created just after table creation, we will not do
 *	memory check and adaption
 * @in IndexList: index list after table creation
 */
static void setMemCheckFlagForIdx(List* IndexList)
{
    ListCell* lc = NULL;

    Assert(IndexList != NIL);
    foreach (lc, IndexList) {
        IndexStmt* index = NULL;
        index = (IndexStmt*)lfirst(lc);
        index->skip_mem_check = true;
    }
}

/*
 * add_range_partition_def_state
 * 	add one partition def state into a List
 *
 * [IN] xL: List to be appended
 * [IN] boundary: a list of the end point (list_length must be 1)
 * [IN] partName: partition name
 * [IN] tblSpaceName: tablespace name
 *
 * RETURN: the partitionDefState List
 */
static List* add_range_partition_def_state(List* xL, List* boundary, const char* partName, const char* tblSpaceName)
{
    RangePartitionDefState* addState = makeNode(RangePartitionDefState);
    addState->boundary = boundary;
    addState->partitionName = pstrdup(partName);
    addState->tablespacename = pstrdup(tblSpaceName);
    addState->curStartVal = NULL;
    addState->partitionInitName = NULL;

    return lappend(xL, addState);
}

/*
 * get_range_partition_name_prefix
 * 	get partition name's prefix
 *
 * [out] namePrefix: an array of length NAMEDATALEN to store name prefix
 * [IN] srcName: src name
 * [IN] printNotice: print notice or not, default false
 *
 * RETURN: void
 */
void get_range_partition_name_prefix(char* namePrefix, char* srcName, bool printNotice, bool isPartition)
{
    errno_t ret = EOK;
    int len;

    /* namePrefix is an array of length NAMEDATALEN, so it's safe to store string */
    Assert(namePrefix && srcName);
    ret = sprintf_s(namePrefix, NAMEDATALEN, "%s", srcName);
    securec_check_ss(ret, "\0", "\0");

    len = strlen(srcName);
    if (len > LEN_PARTITION_PREFIX) {
        int k = pg_mbcliplen(namePrefix, len, LEN_PARTITION_PREFIX);
        namePrefix[k] = '\0';
        if (printNotice)
            ereport(NOTICE,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("%s name's prefix \"%s\" will be truncated to \"%s\"",
                        (isPartition ? "Partition" : "Slice"), srcName, namePrefix)));
    }
}

/* get_rel_partition_info
 * 	get detail info of a partition rel
 *
 * [IN] partTableRel: partition relation
 * [OUT] pos: position of the partition key
 * [OUT] upBound: up boundary of last partition
 *
 * RETURN: void
 */
static void get_rel_partition_info(Relation partTableRel, List** pos, Const** upBound)
{
    RangePartitionMap* partMap = NULL;
    int2vector* partitionKey = NULL;
    int partKeyNum;

    if (!RELATION_IS_PARTITIONED(partTableRel)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("CAN NOT get detail info from a NON-PARTITIONED relation.")));
    }

    if (pos == NULL && upBound == NULL)
        return; /* nothing to do */

    partMap = (RangePartitionMap*)partTableRel->partMap;
    partitionKey = partMap->partitionKey;
    partKeyNum = partMap->partitionKey->dim1;

    /* get position of the partition key */
    if (pos != NULL) {
        List* m_pos = NULL;
        for (int i = 0; i < partKeyNum; i++)
            m_pos = lappend_int(m_pos, partitionKey->values[i] - 1);

        *pos = m_pos;
    }

    /* get up boundary of the last partition */
    if (upBound != NULL) {
        int partNum = getNumberOfPartitions(partTableRel);
        *upBound = (Const*)copyObject(partMap->rangeElements[partNum - 1].boundary[0]);
    }
}

/* get_src_partition_bound
 * 	get detail info of a partition rel
 *
 * [IN] partTableRel: partition relation
 * [IN] srcPartOid: src partition oid
 * [OUT] lowBound: low boundary of the src partition
 * [OUT] upBound: up boundary of the src partition
 *
 * RETURN: void
 */
static void get_src_partition_bound(Relation partTableRel, Oid srcPartOid, Const** lowBound, Const** upBound)
{
    RangePartitionMap* partMap = NULL;
    int srcPartSeq;

    if (!RELATION_IS_PARTITIONED(partTableRel)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("CAN NOT get detail info from a NON-PARTITIONED relation.")));
    }

    if (lowBound == NULL && upBound == NULL)
        return; /* nothing to do */

    if (srcPartOid == InvalidOid)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("CAN NOT get detail info from a partitioned relation WITHOUT specified partition.")));

    Assert(partTableRel->partMap->type == PART_TYPE_RANGE || partTableRel->partMap->type == PART_TYPE_INTERVAL);
    partMap = (RangePartitionMap*)partTableRel->partMap;

    srcPartSeq = partOidGetPartSequence(partTableRel, srcPartOid) - 1;
    if (lowBound != NULL) {
        if (srcPartSeq > 0)
            *lowBound = (Const*)copyObject(partMap->rangeElements[srcPartSeq - 1].boundary[0]);
        else
            *lowBound = NULL;
    }

    if (upBound != NULL)
        *upBound = (Const*)copyObject(partMap->rangeElements[srcPartSeq].boundary[0]);
}

/* get_split_partition_oid
 * 	get oid of the split partition
 *
 * [IN] partTableRel: partition relation
 * [IN] splitState: split partition state
 *
 * RETURN: oid of the partition to be splitted
 */
static Oid get_split_partition_oid(Relation partTableRel, SplitPartitionState* splitState)
{
    RangePartitionMap* partMap = NULL;
    Oid srcPartOid = InvalidOid;

    if (!RELATION_IS_PARTITIONED(partTableRel)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("CAN NOT get partition oid from a NON-PARTITIONED relation.")));
    }

    partMap = (RangePartitionMap*)partTableRel->partMap;

    if (PointerIsValid(splitState->src_partition_name)) {
        srcPartOid = partitionNameGetPartitionOid(RelationGetRelid(partTableRel),
            splitState->src_partition_name,
            PART_OBJ_TYPE_TABLE_PARTITION,
            AccessExclusiveLock,
            true,
            false,
            NULL,
            NULL,
            NoLock);
    } else {
        Assert(PointerIsValid(splitState->partition_for_values));
        splitState->partition_for_values = transformConstIntoTargetType(
            partTableRel->rd_att->attrs, partMap->partitionKey, splitState->partition_for_values);
        srcPartOid = partitionValuesGetPartitionOid(
            partTableRel, splitState->partition_for_values, AccessExclusiveLock, true, true, false);
    }

    return srcPartOid;
}

#define precheck_point_value_internal(a, isPartition)                                                                 \
    do {                                                                                                              \
        Node* pexpr = (Node*)linitial(a);                      /* original value */                                   \
        Const* pval = GetPartitionValue(pos, attrs, a, false, isPartition); /* cast(ori)::int */                      \
        if (!pval->ismaxvalue) {                                                                                      \
            Const* c = (Const*)evaluate_expr((Expr*)pexpr, exprType(pexpr), exprTypmod(pexpr), exprCollation(pexpr)); \
            if (partitonKeyCompare(&pval, &c, 1) != 0)                                                                \
                ereport(ERROR,                                                                                        \
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),                                                       \
                        errmsg("start/end/every value must be an const-integer for %s \"%s\"",                        \
                            (isPartition ? "partition" : "slice"), defState->partitionName)));                        \
        }                                                                                                             \
    } while (0)

/*
 * precheck_start_end_defstate
 *    precheck start/end value of a range partition defstate
 */
static void precheck_start_end_defstate(List* pos, Form_pg_attribute* attrs,
    RangePartitionStartEndDefState* defState, bool isPartition)
{
    ListCell* cell = NULL;

    if (pos == NULL || attrs == NULL || defState == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unexpected parameter for precheck start/end defstate.")));

    Assert(pos->length == 1); /* already been checked in caller */
    foreach (cell, pos) {
        int i = lfirst_int(cell);

        switch (attrs[i]->atttypid) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
                if (defState->startValue)
                    precheck_point_value_internal(defState->startValue, isPartition);
                if (defState->endValue)
                    precheck_point_value_internal(defState->endValue, isPartition);
                if (defState->everyValue)
                    precheck_point_value_internal(defState->everyValue, isPartition);
                break;

            default:
                break; /* don't check */
        }
    }

    return;
}

/* is_start_end_def_list
 * 	check the partition state and return the type of state
 * 	true: start/end stmt; false: less/than stmt
 *
 * [IN] state: partition state
 *
 * RETURN: if it is start/end stmt
 */
bool is_start_end_def_list(List* def_list)
{
    ListCell* cell = NULL;

    if (def_list == NULL)
        return false;

    /* count start/end clause */
    foreach (cell, def_list) {
        Node* defState = (Node*)lfirst(cell);

        if (!IsA(defState, RangePartitionStartEndDefState))
            return false; /* not in start/end syntax, stop here */
    }

    return true;
}

/*
 * get_partition_arg_value
 * 	Get the actual value from the expression. There are only a limited range
 * 	of cases we must cover because the parser guarantees constant input.
 *
 * [IN] node: input node expr
 * [out] isnull: indicate the NULL of result datum
 *
 * RETURN: a datum produced by node
 */
static Datum get_partition_arg_value(Node* node, bool* isnull)
{
    Const* c = NULL;

    c = (Const*)evaluate_expr((Expr*)node, exprType(node), exprTypmod(node), exprCollation(node));
    if (!IsA(c, Const))
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("partition parameter is not constant.")));

    *isnull = c->constisnull;
    return c->constvalue;
}

/*
 * evaluate_opexpr
 * Evaluate a basic operator expression from a partitioning specification.
 * The expression will only be an op expr but the sides might contain
 * a coercion function. The underlying value will be a simple constant,
 * however.
 *
 * If restypid is non-NULL and *restypid is set to InvalidOid, we tell the
 * caller what the return type of the operator is. If it is anything but
 * InvalidOid, coerce the operation's result to that type.
 *
 * [IN] pstate: parser state
 * [IN] oprname: name of opreator which can be <, +, -, etc.
 * [IN] leftarg: left arg
 * [IN] rightarg: right arg
 * [IN/OUT] restypid: result type id, if given coerce to it, otherwise return the compute result-type.
 * [IN] location: location of the expr, not necessary
 *
 * RETURN: a datum produced by the expr: "leftarg oprname rightarg"
 */
static Datum evaluate_opexpr(
    ParseState* pstate, List* oprname, Node* leftarg, Node* rightarg, Oid* restypid, int location)
{
    Datum res = 0;
    Datum lhs = 0;
    Datum rhs = 0;
    OpExpr* opexpr = NULL;
    bool byval = false;
    int16 len;
    Oid oprcode;
    Type typ;
    bool isnull = false;

    opexpr = (OpExpr*)make_op(pstate, oprname, leftarg, rightarg, location);

    oprcode = get_opcode(opexpr->opno);
    if (oprcode == InvalidOid) /* should not fail */
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_OPT),
                errmsg("cache lookup failed for operator %u", opexpr->opno)));

    opexpr->opfuncid = oprcode;

    /* compute result */
    lhs = get_partition_arg_value((Node*)linitial(opexpr->args), &isnull);
    if (!isnull) {
        rhs = get_partition_arg_value((Node*)lsecond(opexpr->args), &isnull);
        if (!isnull)
            res = OidFunctionCall2(opexpr->opfuncid, lhs, rhs);
    }

    /* If the caller supplied a target result type, coerce if necesssary */
    if (PointerIsValid(restypid)) {
        if (OidIsValid(*restypid)) {
            if (*restypid != opexpr->opresulttype) {
                Expr* e = NULL;
                int32 typmod;
                Const* c = NULL;
                bool isnull = false;

                typ = typeidType(opexpr->opresulttype);
                c = makeConst(opexpr->opresulttype,
                    ((Form_pg_type)GETSTRUCT(typ))->typtypmod,
                    ((Form_pg_type)GETSTRUCT(typ))->typcollation,
                    typeLen(typ),
                    res,
                    false,
                    typeByVal(typ));
                ReleaseSysCache(typ);

                typ = typeidType(*restypid);
                typmod = ((Form_pg_type)GETSTRUCT(typ))->typtypmod;
                ReleaseSysCache(typ);

                /* coerce from oprresulttype to resttypid */
                e = (Expr*)coerce_type(NULL,
                    (Node*)c,
                    opexpr->opresulttype,
                    *restypid,
                    typmod,
                    COERCION_ASSIGNMENT,
                    COERCE_IMPLICIT_CAST,
                    -1);

                res = get_partition_arg_value((Node*)e, &isnull);
            }
        } else {
            *restypid = opexpr->opresulttype;
        }
    } else {
        return res;
    }

    /* copy result, done */
    Assert(OidIsValid(*restypid));
    typ = typeidType(*restypid);
    byval = typeByVal(typ);
    len = typeLen(typ);
    ReleaseSysCache(typ);

    res = datumCopy(res, byval, len);
    return res;
}

/*
 * coerce_partition_arg
 * 	coerce a partition parameter (start/end/every) to targetType
 *
 * [IN] pstate: parse state
 * [IN] node: Node to be coerced
 * [IN] targetType: target type
 *
 * RETURN: a const
 */
static Const* coerce_partition_arg(ParseState* pstate, Node* node, Oid targetType)
{
    Datum res;
    Oid curtyp;
    Const* c = NULL;
    Type typ = typeidType(targetType);
    int32 typmod = ((Form_pg_type)GETSTRUCT(typ))->typtypmod;
    int16 typlen = ((Form_pg_type)GETSTRUCT(typ))->typlen;
    bool typbyval = ((Form_pg_type)GETSTRUCT(typ))->typbyval;
    Oid typcollation = ((Form_pg_type)GETSTRUCT(typ))->typcollation;
    bool isnull = false;

    ReleaseSysCache(typ);

    curtyp = exprType(node);
    Assert(OidIsValid(curtyp));

    if (curtyp != targetType && OidIsValid(targetType)) {
        node = coerce_type(pstate, node, curtyp, targetType, typmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

        if (!PointerIsValid(node))
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not coerce partitioning parameter.")));
    }
    res = get_partition_arg_value(node, &isnull);
    c = makeConst(targetType, typmod, typcollation, typlen, res, isnull, typbyval);

    return c;
}

/*
 * choose_coerce_type
 * 	choose a coerce type
 * Note: this function may help us to fix ambiguous problem
 */
static Oid choose_coerce_type(Oid leftid, Oid rightid)
{
    if (leftid == FLOAT8OID && rightid == NUMERICOID)
        return NUMERICOID; /* make_op chooses function float8pl to compute "float8 + numeric" */
    else
        return InvalidOid; /* let make_op decide */
}

/* check if everyVal <= endVal - startVal, but think of overflow, we should be careful */
static bool CheckStepInRange(ParseState *pstate, Const *startVal, Const *endVal, Const *everyVal,
    Form_pg_attribute attr)
{
    List *opr_mi = list_make1(makeString("-"));
    List *opr_lt = list_make1(makeString("<"));
    List *opr_le = list_make1(makeString("<="));

    Datum res;
    Oid targetType = attr->atttypid;
    Oid targetCollation = attr->attcollation;
    int16 targetLen = attr->attlen;
    bool targetByval = attr->attbyval;
    bool targetIsVarlena = (!targetByval) && (targetLen == -1);
    int32 targetTypmod;
    bool targetIsTimetype = (targetType == DATEOID || targetType == TIMESTAMPOID || targetType == TIMESTAMPTZOID);
    if (targetIsTimetype) {
        targetTypmod = -1; /* avoid accuracy-problem of date */
    } else {
        targetTypmod = attr->atttypmod;
    }

    bool flag1 = false; /* whether startVal < 0 */
    bool flag2 = false; /* whether endVal < 0 */
    if (!targetIsTimetype) {
        Const *zeroVal = NULL;
        if (targetType == NUMERICOID) {
            zeroVal = makeConst(NUMERICOID, targetTypmod, targetCollation, targetLen,
                (Datum)DirectFunctionCall3(numeric_in, CStringGetDatum("0.0"), ObjectIdGetDatum(0), Int32GetDatum(-1)),
                false, targetByval);
        } else if (targetIsVarlena) {
            /* if it's a varlena type, we make cstring type, type cast will be done in evaluate_opexpr */
            zeroVal = makeConst(UNKNOWNOID, -1, InvalidOid, -2, CStringGetDatum(""), false, false);
        } else {
            zeroVal = makeConst(targetType, targetTypmod, targetCollation, targetLen, (Datum)0, false, targetByval);
        }

        res = evaluate_opexpr(pstate, opr_lt, (Node *)startVal, (Node *)zeroVal, NULL, -1);
        flag1 = DatumGetBool(res); /* whether startVal < 0 */
        res = evaluate_opexpr(pstate, opr_lt, (Node *)endVal, (Node *)zeroVal, NULL, -1);
        flag2 = DatumGetBool(res); /* whether endVal < 0 */
    }

    /* we've checked startVal < endVal, so if startVal >= 0, make sure endVal >= 0 too,
     * (!flag1 && flag2) can never happen */
    Assert(flag1 || !flag2);

    /* there are some errors on interval compare, because it treats one month as 30days, but one year as 12 months.
     * so for time type, we don't use interval type. */
    if ((flag1 && !flag2) || targetIsTimetype) { /* startVal < 0 && endVal >= 0 */
        /* check if startVal <= endVal - everyVal */
        res = evaluate_opexpr(pstate, opr_mi, (Node *)endVal, (Node *)everyVal, &targetType, -1);
        Const *secheck = makeConst(targetType, targetTypmod, targetCollation, targetLen, res, false, targetByval);
        res = evaluate_opexpr(pstate, opr_le, (Node *)startVal, (Node *)secheck, NULL, -1);
    } else { /* startVal < 0 && endVal < 0, or startVal >= 0 && endVal >= 0 */
        /* check if everyVal <= endVal - startVal */
        res = evaluate_opexpr(pstate, opr_mi, (Node *)endVal, (Node *)startVal, &everyVal->consttype, -1);
        Const *secheck =
            makeConst(everyVal->consttype, targetTypmod, targetCollation, targetLen, res, false, targetByval);
        res = evaluate_opexpr(pstate, opr_le, (Node *)everyVal, (Node *)secheck, NULL, -1);
    }
    return DatumGetBool(res);
}

/*
 * divide_start_end_every_internal
 * 	internal implementaion for dividing an interval indicated by any-datatype
 * 	for example:
 * 	-- start(1) end(100) every(30)
 * 	-- start(123.01) end(345.09) every(111.99)
 * 	-- start('12-01-2012') end('12-05-2018') every('1 year')
 *
 * If (end-start) is divided by every with a remainder, then last partition is smaller
 * than others.
 *
 * [IN] pstate: parse state
 * [IN] partName: partition name
 * [IN] attr: pg_attribute of the target type
 * [IN] startVal: start value
 * [IN] endVal: end value
 * [IN] everyVal: interval value
 * [OUT] numPart: number of partitions
 * [IN] maxNum: max partition number allowed
 * [IN] isinterval: if EVERY is a interval value
 *
 * RETURN: end points of all sub-intervals
 */
static List* divide_start_end_every_internal(ParseState* pstate, char* partName, Form_pg_attribute attr,
    Const* startVal, Const* endVal, Node* everyExpr, int* numPart,
    int maxNum, bool isinterval, bool needCheck, bool isPartition)
{
    List* result = NIL;
    List* opr_pl = NIL;
    List* opr_mi = NIL;
    List* opr_lt = NIL;
    List* opr_le = NIL;
    List* opr_mul = NIL;
    List* opr_eq = NIL;
    Datum res;
    Const* pnt = NULL;
    Oid restypid;
    Const* curpnt = NULL;
    int32 nPart;
    Const* everyVal = NULL;
    Oid targetType;
    bool targetByval = false;
    int16 targetLen;
    int32 targetTypmod;
    Oid targetCollation;
    const char* partTypeName = (isPartition) ? "partition" : "slice";

    Assert(maxNum > 0 && maxNum <= MAX_PARTITION_NUM);

    opr_pl = list_make1(makeString("+"));
    opr_mi = list_make1(makeString("-"));
    opr_lt = list_make1(makeString("<"));
    opr_le = list_make1(makeString("<="));
    opr_mul = list_make1(makeString("*"));
    opr_eq = list_make1(makeString("="));

    /* get target type info */
    targetType = attr->atttypid;
    targetByval = attr->attbyval;
    targetCollation = attr->attcollation;
    if (targetType == DATEOID || targetType == TIMESTAMPOID || targetType == TIMESTAMPTZOID) {
        targetTypmod = -1; /* avoid accuracy-problem of date */
    } else {
        targetTypmod = attr->atttypmod;
    }
    targetLen = attr->attlen;

    /*
     * cast everyExpr to targetType
     * Note: everyExpr goes through transformExpr and transformIntoConst already.
     */
    everyVal = (Const*)GetTargetValue(attr, (Const*)everyExpr, isinterval);

    /* first compare start/end value */
    res = evaluate_opexpr(pstate, opr_le, (Node*)endVal, (Node*)startVal, NULL, -1);
    if (DatumGetBool(res)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("start value must be less than end value for %s \"%s\".",
                    partTypeName, partName)));
    }

    /* second, we should make sure that everyVal <= endVal - startVal */
    if (!CheckStepInRange(pstate, startVal, endVal, everyVal, attr)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s step is too big for %s \"%s\".", partTypeName, partTypeName, partName)));
    }

    /* build result */
    curpnt = startVal;
    nPart = 0;
    while (true) {
        /* if too many partitions, report error */
        if (nPart >= maxNum) {
            pfree_ext(pnt);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("too many %ss after split %s \"%s\".", partTypeName, partTypeName, partName),
                    errhint("number of %ss can not be more than %d, MINVALUE will be auto-included if not assigned.",
                        partTypeName, MAX_PARTITION_NUM)));
        }

        /* compute curpnt + everyval, but if curpnt + everyVal > endVal, we use endVal instead, 
         * so we can make sure that pnt <= endVal */
        if (!CheckStepInRange(pstate, curpnt, endVal, everyVal, attr)) {
            pnt = (Const*)copyObject(endVal);
        } else {
            res = evaluate_opexpr(pstate, opr_pl, (Node*)curpnt, (Node*)everyVal, &targetType, -1);
            pnt = makeConst(targetType, targetTypmod, targetCollation, targetLen, res, false, targetByval);
            pnt = (Const*)GetTargetValue(attr, (Const*)pnt, false);
        }

        /* necessary check in first pass */
        if (nPart == 0) {
            /*
             * check ambiguous partition rule
             *
             * 1. start(1) end (1.00007) every(0.00001)  -- for float4 datatype
             *      cast(1 + 0.00001 as real)  !=  (1 + 0.00001)::numeric
             * This rule is ambiguous, error out.
             */
            if (needCheck) {
                Const* c = NULL;
                Const* uncast = NULL;
                Type typ;

                /* get every value, uncast */
                restypid = exprType(everyExpr);
                c = coerce_partition_arg(pstate, everyExpr, restypid);

                /* calculate start+every cast to proper type */
                restypid = choose_coerce_type(targetType, restypid);
                res = evaluate_opexpr(pstate, opr_pl, (Node*)startVal, (Node*)c, &restypid, -1);
                typ = typeidType(restypid);
                uncast = makeConst(restypid,
                    ((Form_pg_type)GETSTRUCT(typ))->typtypmod,
                    ((Form_pg_type)GETSTRUCT(typ))->typcollation,
                    typeLen(typ),
                    res,
                    false,
                    typeByVal(typ));
                ReleaseSysCache(typ);

                res = evaluate_opexpr(pstate, opr_eq, (Node*)pnt, (Node*)uncast, NULL, -1);
                if (!DatumGetBool(res))
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg((isPartition ? 
                                "ambiguous partition rule is raised by EVERY parameter in partition \"%s\"." : 
                                "ambiguous distribution rule is raised by EVERY parameter in slice \"%s\"."),
                                partName)));
            }

            /* check partition step */
            res = evaluate_opexpr(pstate, opr_le, (Node*)pnt, (Node*)startVal, NULL, -1);
            if (DatumGetBool(res))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s step is too small for %s \"%s\".", partTypeName, partTypeName, partName)));
        }

        /* pnt must be less than or equal to endVal */
        Assert(DatumGetBool(evaluate_opexpr(pstate, opr_le, (Node*)pnt, (Node*)endVal, NULL, -1)));
        result = lappend(result, pnt);
        nPart++;

        /* check to determine if it is the final partition */
        res = evaluate_opexpr(pstate, opr_eq, (Node*)pnt, (Node*)endVal, NULL, -1);
        if (DatumGetBool(res)) {
            break;
        }
        curpnt = pnt;
    }

    /* done */
    Assert(result && result->length == nPart);
    if (numPart != NULL) {
        *numPart = nPart;
    }

    return result;
}

template <bool isTimestampz> void CheckTIMESTAMPISFinite(Const* startVal, Const* endVal, char* partName) 
{
    if (isTimestampz) {
        TimestampTz t1 = DatumGetTimestampTz(startVal->constvalue);
        TimestampTz t2 = DatumGetTimestampTz(endVal->constvalue);
        if (TIMESTAMP_NOT_FINITE(t1) || TIMESTAMP_NOT_FINITE(t2))
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("partition \"%s\" is invalid.", partName),
                    errhint("INF can not appear in a (START, END, EVERY) clause.")));
    } else {
        Timestamp t1 = DatumGetTimestamp(startVal->constvalue);
        Timestamp t2 = DatumGetTimestamp(endVal->constvalue);
        if (TIMESTAMP_NOT_FINITE(t1) || TIMESTAMP_NOT_FINITE(t2))
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("partition \"%s\" is invalid.", partName),
                    errhint("INF can not appear in a (START, END, EVERY) clause.")));
    }
}

/*
 * DividePartitionStartEndInterval
 * 	divide the partition interval of start/end into specified sub-intervals
 *
 * [IN] pstate: parse state
 * [IN] attr: pg_attribute
 * [IN] partName: partition name
 * [IN] startVal: start value
 * [IN] endVal: end value
 * [IN] everyVal: interval value
 * [OUT] numPart: number of partitions
 * [IN] maxNum: max partition number allowed
 *
 * RETURN: end points of all sub-intervals
 */
static List* DividePartitionStartEndInterval(ParseState* pstate, Form_pg_attribute attr, char* partName,
    Const* startVal, Const* endVal, Const* everyVal, Node* everyExpr, int* numPart, int maxNum, bool isPartition)
{
    List* result = NIL;
    const char* partTypeName = (isPartition) ? "partition" : "slice";

    Assert(maxNum > 0 && maxNum <= MAX_PARTITION_NUM);
    Assert(attr != NULL);

    /* maxvalue is not allowed in start/end stmt */
    Assert(startVal && IsA(startVal, Const) && !startVal->ismaxvalue);
    Assert(endVal && IsA(endVal, Const) && !endVal->ismaxvalue);
    Assert(everyVal && IsA(everyVal, Const) && !everyVal->ismaxvalue);

    /* Form each partition node const */
    switch (attr->atttypid) {
        case NUMERICOID: {
            Numeric v1 = DatumGetNumeric(startVal->constvalue);
            Numeric v2 = DatumGetNumeric(endVal->constvalue);
            Numeric d = DatumGetNumeric(everyVal->constvalue);
            /* NAN is not allowed */
            if (NUMERIC_IS_NAN(v1) || NUMERIC_IS_NAN(v2) || NUMERIC_IS_NAN(d))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("%s \"%s\" is invalid.",
                            partTypeName, partName),
                        errhint("NaN can not appear in a (START, END, EVERY) clause.")));

            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, false, true, isPartition);
            break;
        }

        case FLOAT4OID: {
            float4 v1 = DatumGetFloat4(startVal->constvalue);
            float4 v2 = DatumGetFloat4(endVal->constvalue);
            float4 d = DatumGetFloat4(everyVal->constvalue);
            /* INF is not allowed */
            if (isinf(d) || isinf(v1) || isinf(v2))
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("%s \"%s\" is invalid.",
                            partTypeName, partName),
                        errhint("INF can not appear in a (START, END, EVERY) clause.")));

            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, false, true, isPartition);
            break;
        }

        case FLOAT8OID: {
            float8 v1 = DatumGetFloat8(startVal->constvalue);
            float8 v2 = DatumGetFloat8(endVal->constvalue);
            float8 d = DatumGetFloat8(everyVal->constvalue);
            /* INF is not allowed */
            if (isinf(d) || isinf(v1) || isinf(v2))
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("%s \"%s\" is invalid.",
                            partTypeName, partName),
                        errhint("INF can not appear in a (START, END, EVERY) clause.")));

            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, false, true, isPartition);
            break;
        }

        case INT2OID:
        case INT4OID:
        case INT8OID: {
            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, false, false, isPartition);
            break;
        }

        case DATEOID:
        case TIMESTAMPOID: {
            Timestamp t1 = DatumGetTimestamp(startVal->constvalue);
            Timestamp t2 = DatumGetTimestamp(endVal->constvalue);
            if (TIMESTAMP_NOT_FINITE(t1) || TIMESTAMP_NOT_FINITE(t2))
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("%s \"%s\" is invalid.",
                            partTypeName, partName),
                        errhint("INF can not appear in a (START, END, EVERY) clause.")));
            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, true, false, isPartition);
            break;
        }

        case TIMESTAMPTZOID: {
            TimestampTz t1 = DatumGetTimestampTz(startVal->constvalue);
            TimestampTz t2 = DatumGetTimestampTz(endVal->constvalue);
            if (TIMESTAMP_NOT_FINITE(t1) || TIMESTAMP_NOT_FINITE(t2))
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("%s \"%s\" is invalid.",
                            partTypeName, partName),
                        errhint("INF can not appear in a (START, END, EVERY) clause.")));
            result = divide_start_end_every_internal(
                pstate, partName, attr, startVal, endVal, everyExpr, numPart, maxNum, true, false, isPartition);
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("unsupported datatype served as a %s key in the start/end clause.",
                        (isPartition ? "partition" : "distribution")),
                    errhint("Valid datatypes are: smallint, int, bigint, float4/real, float8/double, numeric, date and "
                            "timestamp [with time zone].")));
    }

    return result;
}

#define add_last_single_start_partition                                                                              \
    do {                                                                                                             \
        Const* laststart = NULL;                                                                                     \
        /* last DefState is a single START, so add the last partition here */                                        \
        Assert(lastState->startValue && !lastState->endValue);                                                       \
        pnt = (Const*)copyObject(startVal);                                                                          \
        boundary = list_make1(pnt);                                                                                  \
        laststart = GetPartitionValue(pos, attrs, lastState->startValue, false, isPartition);                        \
        if (partitonKeyCompare(&laststart, &startVal, 1) >= 0)                                                       \
            ereport(ERROR,                                                                                           \
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),                                                          \
                    errmsg("start value of %s \"%s\" is too low.",                                                   \
                        (isPartition ? "partition" : "slice"), defState->partitionName),                             \
                    errhint("%s gap or overlapping is not allowed.",                                                 \
                                     (isPartition ? "partition" : "slice"))));                                       \
        if (lowBound == NULL && curDefState == 1) {                                                                  \
            /* last single START is the first DefState and MINVALUE is included */                                   \
            get_range_partition_name_prefix(namePrefix, lastState->partitionName, false, isPartition);               \
            ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 1);                                     \
            securec_check_ss(ret, "\0", "\0");                                                                       \
            newPartList = add_range_partition_def_state(newPartList, boundary, partName, lastState->tableSpaceName); \
            totalPart++;                                                                                             \
        } else {                                                                                                     \
            newPartList = add_range_partition_def_state(                                                             \
                newPartList, boundary, lastState->partitionName, lastState->tableSpaceName);                         \
            totalPart++;                                                                                             \
        }                                                                                                            \
        pfree_ext(laststart);                                                                                        \
    } while (0)

/*
 * transformRangePartStartEndStmt
 * 	entry of transform range partition which is defined by "start/end" syntax
 *
 * [IN] pstate: parse state
 * [IN] partitionList: partition list to be rewrote
 * [IN] attrs: pg_attribute item
 * [IN] pos: position of partition key in ColDef
 * [IN] existPartNum: number of partitions already exists
 * [IN] lowBound: low-boundary of all paritions
 * [IN] upBound: up-boundary of all partitions
 * [IN] needFree: free input partitionList or not, true: free, false: not
 * [IN] isPartition: true when checking partition definition, false when checking list/range distribution definition.
 *
 * lowBound/upBound rules:
 *
 *                                lowBound                       upBound
 *   not-NULL:    check SP == lowBound      check EP == upBound
 *                                                        (START) include upBound
 *
 *         NULL:         include MINVALUE      (START) include MAXVALUE
 * SP: first start point of the def; EP: final end point of the def
 * (START) include xxx: for a single start as final clause, include xxx.
 *
 * -- CREATE TABLE PARTITION: lowBound=NULL, upBound=NULL
 * -- ADD PARTITION: lowBound=ExistUpBound, upBound=NULL
 * -- SPLIT PARTITION: lowBound=CurrentPartLowBound, upBound=CurrentPartUpBound
 *
 * RETURN: a new partition list (wrote by "less/than" syntax).
 */
List* transformRangePartStartEndStmt(ParseState* pstate, List* partitionList, List* pos, Form_pg_attribute* attrs,
    int32 existPartNum, Const* lowBound, Const* upBound, bool needFree, bool isPartition)
{
    ListCell* cell = NULL;
    int i, j;
    Oid target_type = InvalidOid;
    List* newPartList = NIL;
    char partName[NAMEDATALEN] = {0};
    char namePrefix[NAMEDATALEN] = {0};
    errno_t ret = EOK;
    Const* startVal = NULL;
    Const* endVal = NULL;
    Const* everyVal = NULL;
    Const* lastVal = NULL;
    int totalPart = 0;
    int numPart = 0;
    List* resList = NIL;
    List* boundary = NIL;
    Const* pnt = NULL;
    RangePartitionStartEndDefState* defState = NULL;
    RangePartitionStartEndDefState* lastState = NULL;
    int curDefState;
    int kc;
    ListCell* lc = NULL;
    char* curName = NULL;
    char* preName = NULL;
    bool isinterval = false;
    Form_pg_attribute attr = NULL;
    const char *partTypeName = (isPartition) ? "partition" : "slice";

    if (partitionList == NULL || list_length(partitionList) == 0 || attrs == NULL || pos == NULL)
        return partitionList; /* untouched */

    Assert(existPartNum >= 0 && existPartNum <= MAX_PARTITION_NUM);

    /* only one partition key is allowed */
    if (pos->length != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg((isPartition ? "partitioned table has too many partition keys." : 
                    "distributed table has too many distribution keys.")),
                errhint((isPartition ? "start/end syntax requires a partitioned table with only one partition key." : 
                    "start/end syntax requires a distributed table with only one distribution key."))));
    }

    /*
     * Now, it is start/end stmt, check following key-points:
     *
     * - mixture of "start/end" and "less/than" is forbidden
     * - only one partition key is given
     * - datatype of partition key
     * - continuity of partitions
     * - number of partitions <= MAX_PARTITION_NUM
     * - validation of partition namePrefix
     */
    foreach (cell, partitionList) {
        RangePartitionStartEndDefState* defState = (RangePartitionStartEndDefState*)lfirst(cell);
        if ((defState->startValue && defState->startValue->length != 1) ||
            (defState->endValue && defState->endValue->length != 1) ||
            (defState->everyValue && defState->everyValue->length != 1))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg((isPartition ? "too many partition keys for partition \"%s\"." : 
                        "too many distribution keys for slice \"%s\"."), defState->partitionName),
                            errhint(isPartition ? "only one partition key is allowed in start/end clause." : 
                                "only one distribution key is allowed in start/end clause.")));
    }

    /* check partition name */
    check_partition_name_start_end(partitionList, isPartition);

    /* check: datatype of partition key */
    foreach (cell, pos) {
        i = lfirst_int(cell);
        attr = attrs[i];
        target_type = attr->atttypid;

        switch (target_type) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
            case NUMERICOID:
            case FLOAT4OID:
            case FLOAT8OID:
                isinterval = false;
                break;

            case DATEOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
                isinterval = true;
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("datatype of column \"%s\" is unsupported for %s key in start/end clause.",
                            NameStr(attrs[i]->attname), (isPartition ? "partition" : "distribution")),
                        errhint("Valid datatypes are: smallint, int, bigint, float4/real, float8/double, numeric, date "
                                "and timestamp [with time zone].")));
                break;
        }
    }

    /* check exist partition number */
    if (existPartNum >= MAX_PARTITION_NUM)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("can not add more %ss as %s number is already at its maximum.", partTypeName, partTypeName)));

    /*
     * Start transform (including check)
     *
     * Recall the syntax:
     *   start_end_item [, ...]
     *
     * where start_end_item:
     * 	{ start(a) end (b) [every(d)] }  |  start(a)  |  end (b)
     */
    curDefState = 0;
    totalPart = existPartNum;
    lastState = NULL;
    defState = NULL;
    lastVal = NULL;
    foreach (cell, partitionList) {
        lastState = defState;
        defState = (RangePartitionStartEndDefState*)lfirst(cell);
        Assert(defState);

        /* precheck defstate */
        precheck_start_end_defstate(pos, attrs, defState, isPartition);

        /* type-1: start + end + every */
        if (defState->startValue && defState->endValue && defState->everyValue) {
            Node* everyExpr = (Node*)linitial(defState->everyValue);
            startVal = GetPartitionValue(pos, attrs, defState->startValue, false, isPartition);
            endVal = GetPartitionValue(pos, attrs, defState->endValue, false, isPartition);
            everyVal = GetPartitionValue(pos, attrs, defState->everyValue, isinterval, isPartition);

            /* check value */
            if (startVal->ismaxvalue || endVal->ismaxvalue || everyVal->ismaxvalue)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("%s \"%s\" is invalid.", partTypeName, defState->partitionName),
                        errhint("MAXVALUE can not appear in a (START, END, EVERY) clause.")));
            if (partitonKeyCompare(&startVal, &endVal, 1) >= 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg(
                            "start value must be less than end value for %s \"%s\".",
                            partTypeName, defState->partitionName)));

            if (lastVal != NULL) {
                if (lastVal->ismaxvalue)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("%s \"%s\" is not allowed behind MAXVALUE.",
                                partTypeName, defState->partitionName)));

                kc = partitonKeyCompare(&lastVal, &startVal, 1);
                if (kc > 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too low.",
                                partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.",
                                partTypeName)));
                if (kc < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too high.",
                                partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.",
                                partTypeName)));
            }

            /* build necessary MINVALUE, check lowBound,  append for last single START, etc. */
            if (lastVal == NULL) {
                if (lastState != NULL) {
                    /* last DefState is a single START */
                    add_last_single_start_partition;
                } else {
                    /* this is the first DefState (START, END, EVERY) */
                    if (lowBound == NULL) {
                        /* this is the first DefState (START, END, EVERY), add MINVALUE */
                        pnt = (Const*)copyObject(startVal);
                        boundary = list_make1(pnt);
                        get_range_partition_name_prefix(namePrefix, defState->partitionName, false, isPartition);
                        ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 0);
                        securec_check_ss(ret, "\0", "\0");
                        newPartList =
                            add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);
                        totalPart++;
                    } else {
                        /* this is the first DefState (START, END, EVERY), but do not include MINVALUE */
                        /* check SP: case for ADD_PARTITION, SPLIT_PARTITION */
                        /* ignore: case for ADD_PARTITION, check SP: SPLIT_PARTITION */
                        if (NULL != lowBound && NULL != upBound) {
                            if (partitonKeyCompare(&lowBound, &startVal, 1) != 0)
                                ereport(ERROR,
                                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                        errmsg(
                                            "start value of %s \"%s\" NOT EQUAL up-boundary of last %s.",
                                            partTypeName, defState->partitionName, partTypeName)));
                        }
                    }
                }
            }

            /* add current DefState */
            get_range_partition_name_prefix(namePrefix, defState->partitionName, true, isPartition);
            Assert(totalPart < MAX_PARTITION_NUM);
            Assert(everyExpr);
            resList = DividePartitionStartEndInterval(pstate,
                attr,
                defState->partitionName,
                startVal,
                endVal,
                everyVal,
                everyExpr,
                &numPart,
                MAX_PARTITION_NUM - totalPart,
                isPartition);
            Assert(resList && numPart == resList->length);

            j = 1;
            foreach (lc, resList) {
                ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, j);
                securec_check_ss(ret, "\0", "\0");
                boundary = list_make1(lfirst(lc));
                newPartList = add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);

                if (j == 1) {
                    ((RangePartitionDefState*)llast(newPartList))->curStartVal = (Const*)copyObject(startVal);
                    ((RangePartitionDefState*)llast(newPartList))->partitionInitName = pstrdup(defState->partitionName);
                }

                j++;
            }
            list_free_ext(resList); /* can not be freed deeply */

            totalPart += numPart;

            /* update lastVal */
            pfree_ext(everyVal);
            if (NULL != lastVal)
                pfree_ext(lastVal);
            lastVal = endVal;
        } else if (defState->startValue && defState->endValue) {
            startVal = GetPartitionValue(pos, attrs, defState->startValue, false, isPartition);
            endVal = GetPartitionValue(pos, attrs, defState->endValue, false, isPartition);
            Assert(startVal != NULL && endVal != NULL);

            /* check value */
            if (startVal->ismaxvalue)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("start value can not be MAXVALUE for %s \"%s\".", 
                            partTypeName, defState->partitionName)));

            if (partitonKeyCompare(&startVal, &endVal, 1) >= 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg(
                            "start value must be less than end value for %s \"%s\".", 
                            partTypeName, defState->partitionName)));

            if (lastVal != NULL) {
                if (lastVal->ismaxvalue)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("%s \"%s\" is not allowed behind MAXVALUE.", 
                                partTypeName, defState->partitionName)));

                kc = partitonKeyCompare(&lastVal, &startVal, 1);
                if (kc > 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too low.", partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.", partTypeName)));
                if (kc < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too high.", partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.", partTypeName)));
            }

            /* build less than defstate */
            if (lastVal != NULL) {
                /* last DefState is (START END EVERY) or (START END) or (END) */
                pnt = (Const*)copyObject(endVal);
                boundary = list_make1(pnt);
                newPartList = add_range_partition_def_state(
                    newPartList, boundary, defState->partitionName, defState->tableSpaceName);
                totalPart++;
            } else {
                if (lastState != NULL) {
                    /* last DefState is a single START */
                    add_last_single_start_partition;

                    /* add current DefState */
                    pnt = (Const*)copyObject(endVal);
                    boundary = list_make1(pnt);
                    newPartList = add_range_partition_def_state(
                        newPartList, boundary, defState->partitionName, defState->tableSpaceName);
                    totalPart++;
                } else if (lowBound == NULL) {
                    /* this is the first DefState (START, END), and MINVALUE will be included */
                    get_range_partition_name_prefix(namePrefix, defState->partitionName, false, isPartition);

                    /* MINVALUE */
                    pnt = (Const*)copyObject(startVal);
                    boundary = list_make1(pnt);
                    ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 0);
                    securec_check_ss(ret, "\0", "\0");
                    newPartList = add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);
                    totalPart++;

                    pnt = (Const*)copyObject(endVal);
                    boundary = list_make1(pnt);
                    ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 1);
                    securec_check_ss(ret, "\0", "\0");
                    newPartList = add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);
                    totalPart++;
                } else {
                    /* this is first DefState (START, END), but do not include MINVALUE */
                    /* check SP: case for ADD_PARTITION, SPLIT_PARTITION */
                    /* ignore: case for ADD_PARTITION, check SP: SPLIT_PARTITION */
                    if (NULL != lowBound && NULL != upBound) {
                        if (partitonKeyCompare(&lowBound, &startVal, 1) != 0)
                            ereport(ERROR,
                                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                    errmsg("start value of %s \"%s\" NOT EQUAL up-boundary of last %s.",
                                        partTypeName, defState->partitionName, partTypeName)));
                    }

                    /* add endVal as a pnt */
                    pnt = (Const*)copyObject(endVal);
                    boundary = list_make1(pnt);
                    newPartList = add_range_partition_def_state(
                        newPartList, boundary, defState->partitionName, defState->tableSpaceName);
                    if (NULL != newPartList) {
                        ((RangePartitionDefState*)llast(newPartList))->curStartVal = (Const*)copyObject(startVal);
                    }

                    totalPart++;
                }
            }

            if (NULL != lastVal)
                pfree_ext(lastVal);
            lastVal = endVal;
        } else if (defState->startValue) {
            startVal = GetPartitionValue(pos, attrs, defState->startValue, false, isPartition);
            Assert(startVal != NULL);

            /* check value */
            if (startVal->ismaxvalue)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("start value can not be MAXVALUE for %s \"%s\".", 
                            partTypeName, defState->partitionName)));

            if (lastVal != NULL) {
                if (lastVal->ismaxvalue)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("%s \"%s\" is not allowed behind MAXVALUE.", 
                                partTypeName, defState->partitionName)));

                kc = partitonKeyCompare(&lastVal, &startVal, 1);
                if (kc > 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too low.", partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.", partTypeName)));
                if (kc < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("start value of %s \"%s\" is too high.", partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.", partTypeName)));
            }

            /* build less than defstate */
            if (lastVal == NULL) {
                if (lastState != NULL) {
                    /* last DefState is a single START */
                    add_last_single_start_partition;
                } else {
                    /* this is the first DefState */
                    if (lowBound == NULL) {
                        /* this is the first DefState, and MINVALUE will be included */
                        get_range_partition_name_prefix(namePrefix, defState->partitionName, false, isPartition);
                        pnt = (Const*)copyObject(startVal);
                        boundary = list_make1(pnt);
                        ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 0);
                        securec_check_ss(ret, "\0", "\0");

                        /* add MINVALUE here, the other partition will be added in next DefState because the endVal is
                         * unknown right now */
                        newPartList =
                            add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);
                        totalPart++;
                    } else {
                        /* this is the first DefState, do not include MINVALUE */
                        /* check SP: case for ADD_PARTITION, SPLIT_PARTITION */
                        /* ignore: case for ADD_PARTITION, check SP: SPLIT_PARTITION */
                        if (NULL != lowBound && NULL != upBound) {
                            if (partitonKeyCompare(&lowBound, &startVal, 1) != 0)
                                ereport(ERROR,
                                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                        errmsg(
                                            "start value of %s \"%s\" NOT EQUAL up-boundary of last %s.",
                                            partTypeName, defState->partitionName, partTypeName)));
                        }
                    }
                }
            }

            if (NULL != lastVal)
                pfree_ext(lastVal);
            lastVal = NULL;
        } else if (defState->endValue) {
            endVal = GetPartitionValue(pos, attrs, defState->endValue, false, isPartition);
            Assert(endVal != NULL);

            /* check value */
            if (lastVal != NULL) {
                if (lastVal->ismaxvalue)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("%s \"%s\" is not allowed behind MAXVALUE.", 
                                partTypeName, defState->partitionName)));

                if (partitonKeyCompare(&lastVal, &endVal, 1) >= 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("end value of %s \"%s\" is too low.", partTypeName, defState->partitionName),
                            errhint("%s gap or overlapping is not allowed.", partTypeName)));
                }
            }

            /* build a less than defState: we need a last partition, or it is a first partition here */
            if (lastVal == NULL) {
                if (lastState != NULL) {
                    /* last def is a single START, invalid definition */
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                            errmsg("%s \"%s\" is an invalid definition clause.", partTypeName, defState->partitionName),
                            errhint("Do not use a single END after a single START.")));
                } else {
                    /* this is the first def state END, check lowBound if any */
                    /* case for ADD_PARTITION, SPLIT_PARTITION */
                    if (lowBound && partitonKeyCompare(&lowBound, &endVal, 1) >= 0)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                                errmsg(
                                    "end value of %s \"%s\" MUST be greater than up-boundary of last %s.",
                                    partTypeName, defState->partitionName, partTypeName)));
                }
            }
            pnt = (Const*)copyObject(endVal);
            boundary = list_make1(pnt);
            newPartList =
                add_range_partition_def_state(newPartList, boundary, defState->partitionName, defState->tableSpaceName);
            totalPart++;

            if (lastVal != NULL) {
                pfree_ext(lastVal); 
            }
            lastVal = endVal;
            startVal = NULL;
        } else {
            Assert(false); /* unexpected syntax */
        }

        /* -- */
        /* check partition numbers */
        if (totalPart >= MAX_PARTITION_NUM) {
            if (totalPart == MAX_PARTITION_NUM && !lnext(cell)) {
                break;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("too many %ss after split %s \"%s\".",
                            partTypeName, partTypeName, defState->partitionName),
                        errhint("number of %ss can not be more than %d, MINVALUE will be auto-included if not "
                                "assigned.",
                            partTypeName, MAX_PARTITION_NUM)));
            }
        }

        curDefState++;
    }

    /* Final stage: add upBound for a single START at last */
    if (!defState->endValue) {
        /* this is a single START */
        Assert(defState->startValue);

        /* first check upBound */
        if (upBound == NULL) {
            /* no upBound, means up-Boundary is MAXVALUE: case for CREATE, ADD_PARTITION */
            pnt = makeNode(Const);
            pnt->ismaxvalue = true;
            boundary = list_make1(pnt);
        } else {
            /* have upBound: case for SPLIT PARTITION */
            if (partitonKeyCompare(&upBound, &startVal, 1) <= 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("start value of %s \"%s\" MUST be less than up-boundary of the %s to be "
                               "splitted.",
                            partTypeName, defState->partitionName, partTypeName)));

            pnt = (Const*)copyObject(upBound);
            boundary = list_make1(pnt);
        }

        /* second check lowBound */
        if (lowBound == NULL && curDefState == 1) {
            /* we have no lowBound, and this is a first def, so MINVALUE already been added */
            get_range_partition_name_prefix(namePrefix, defState->partitionName, false, isPartition);
            ret = sprintf_s(partName, sizeof(partName), "%s_%d", namePrefix, 1);
            securec_check_ss(ret, "\0", "\0");
            newPartList = add_range_partition_def_state(newPartList, boundary, partName, defState->tableSpaceName);
        } else {
            newPartList =
                add_range_partition_def_state(newPartList, boundary, defState->partitionName, defState->tableSpaceName);
            if (NULL != newPartList && NULL != defState->startValue) {
                ((RangePartitionDefState*)llast(newPartList))->curStartVal =
                    (Const*)copyObject(linitial(defState->startValue));
            }
        }
        totalPart++;
    } else {
        /* final def has endVal, just check upBound if any, case for SPLIT_PARTITION */
        if (upBound && partitonKeyCompare(&upBound, &endVal, 1) != 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("end value of %s \"%s\" NOT EQUAL up-boundary of the %s to be splitted.",
                        partTypeName, defState->partitionName, partTypeName)));
        }
    }

    /* necessary check */
    if (totalPart > MAX_PARTITION_NUM) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                errmsg("too many %ss after split partition \"%s\".", partTypeName, defState->partitionName),
                errhint("number of %ss can not be more than %d, MINVALUE will be auto-included if not assigned.",
                    partTypeName, MAX_PARTITION_NUM)));
    }

    /* since splitting partition is done, check partition name again */
    foreach (cell, newPartList) {
        lc = cell;
        preName = ((RangePartitionDefState*)lfirst(cell))->partitionName;
        while (NULL != (lc = lnext(lc))) {
            curName = ((RangePartitionDefState*)lfirst(lc))->partitionName;
            if (!strcmp(curName, preName)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                        errmsg("duplicate %s name: \"%s\".", partTypeName, curName),
                            errhint("%ss defined by (START, END, EVERY) are named as \"%sName_x\" where x is "
                                "an integer and starts from 0 or 1.", partTypeName, partTypeName)));
            }
        }
    }

    /* it's ok, done */
    Assert(newPartList && newPartList->length == totalPart - existPartNum);
    if (needFree) {
        list_free_deep(partitionList); /* deep free is ok */
        partitionList = NULL;
    }

    if (NULL != startVal)
        pfree_ext(startVal);
    if (NULL != lastVal)
        pfree_ext(lastVal);

    return newPartList;
}

/*
 * Check if CreateStmt contains TableLikeClause, and the table to be defined is
 * on different nodegrop with the parent table.
 *
 * CreateStmt: the Stmt need check.
 */
bool check_contains_tbllike_in_multi_nodegroup(CreateStmt* stmt)
{
    ListCell* elements = NULL;
    Relation relation = NULL;
    foreach (elements, stmt->tableElts) {
        if (IsA(lfirst(elements), TableLikeClause)) {
            TableLikeClause* clause = (TableLikeClause*)lfirst(elements);
            relation = relation_openrv(clause->relation, AccessShareLock);

            if (is_multi_nodegroup_createtbllike(stmt->subcluster, relation->rd_id)) {
                heap_close(relation, AccessShareLock);
                return true;
            }

            heap_close(relation, AccessShareLock);
        }
    }

    return false;
}

/*
 * Check if the parent table and the table to be define in the same cluseter.
 * oid : the parent Table OID
 * subcluster: the new table where to create
 */
bool is_multi_nodegroup_createtbllike(PGXCSubCluster* subcluster, Oid oid)
#ifdef ENABLE_MULTIPLE_NODES
{
    Oid like_group_oid;
    bool multi_nodegroup = false;
    char* group_name = NULL;
    Oid new_group_oid = ng_get_installation_group_oid();

    if (subcluster != NULL) {
        ListCell* lc = NULL;
        foreach (lc, subcluster->members) {
            group_name = strVal(lfirst(lc));
        }
        if (group_name != NULL)
            new_group_oid = get_pgxc_groupoid(group_name);
    }

    like_group_oid = get_pgxc_class_groupoid(oid);
    multi_nodegroup = (new_group_oid != like_group_oid);

    return multi_nodegroup;
}
#else
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}
#endif

static bool IsCreatingSeqforAnalyzeTempTable(CreateStmtContext* cxt)
{
    if (cxt->relation->relpersistence != RELPERSISTENCE_TEMP) {
            return false;
    }
    bool isUnderAnalyze = false;
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        isUnderAnalyze = u_sess->analyze_cxt.is_under_analyze;
    } else if (IS_PGXC_DATANODE) {
        /*
         * cannot send 'u_sess->analyze_cxt.is_under_analyze' to dn, so we have to compare the relname with 'pg_analyze'
         * to determine the analyze state of dn.
         */
        isUnderAnalyze = (strncmp(cxt->relation->relname, ANALYZE_TEMP_TABLE_PREFIX,
                                 strlen(ANALYZE_TEMP_TABLE_PREFIX)) == 0);
    }
    return isUnderAnalyze;
}
