/* -------------------------------------------------------------------------
 *
 * tablecmds.h
 *	  prototypes for tablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/commands/tablecmds.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TABLECMDS_H
#define TABLECMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_partition_fn.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "storage/lock/lock.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"

#define MAX_MERGE_PARTITIONS 300
#define ATT_DEFAULT_LEN 128

/*
 * State information for ALTER TABLE
 *
 * The pending-work queue for an ALTER TABLE is a List of AlteredTableInfo
 * structs, one for each table modified by the operation (the named table
 * plus any child tables that are affected).  We save lists of subcommands
 * to apply to this table (possibly modified by parse transformation steps);
 * these lists will be executed in Phase 2.  If a Phase 3 step is needed,
 * necessary information is stored in the constraints and newvals lists.
 *
 * Phase 2 is divided into multiple passes; subcommands are executed in
 * a pass determined by subcommand type.
 */
#define AT_PASS_DROP 0       /* DROP (all flavors) */
#define AT_PASS_ALTER_TYPE 1 /* ALTER COLUMN TYPE */
#define AT_PASS_OLD_INDEX 2  /* re-add existing indexes */
#define AT_PASS_OLD_CONSTR 3 /* re-add existing constraints */
#define AT_PASS_COL_ATTRS 4  /* set other column attributes */
/* We could support a RENAME COLUMN pass here, but not currently used */
#define AT_PASS_ADD_COL 5    /* ADD COLUMN */
#define AT_PASS_ADD_INDEX 6  /* ADD indexes */
#define AT_PASS_ADD_CONSTR 7 /* ADD constraints, defaults */

#define AT_PASS_ADD_PARTITION 8

#define AT_PASS_MISC 9 /* other stuff */
#ifdef PGXC
#define AT_PASS_DISTRIB 10 /* Redistribution pass */
#define AT_COMMENT 11
#define AT_NUM_PASSES 12
#else
#define AT_NUM_PASSES 10
#endif

typedef struct AlteredTableInfo {
    /* Information saved before any work commences: */
    Oid relid;         /* Relation to work on */
    Oid partid;        /* Partition to work on */
    char relkind;      /* Its relkind */
    TupleDesc oldDesc; /* Pre-modification tuple descriptor */
    /* Information saved by Phase 1 for Phase 2: */
    List* subcmds[AT_NUM_PASSES]; /* Lists of AlterTableCmd */
    /* Information saved by Phases 1/2 for Phase 3: */
    List* constraints; /* List of NewConstraint */
    List* newvals;     /* List of NewColumnValue */
    bool new_notnull;  /* T if we added new NOT NULL constraints */
    int rewrite;      /* Reason if a rewrite is forced */
    Oid newTableSpace; /* new tablespace; 0 means no change */
    /* Objects to rebuild after completing ALTER TYPE operations */
    List* changedConstraintOids; /* OIDs of constraints to rebuild */
    List* changedConstraintDefs; /* string definitions of same */
    List* changedIndexOids;      /* OIDs of indexes to rebuild */
    List* changedIndexDefs;      /* string definitions of same */
    bool isDeltaTable;                  /* delta table or not */
    List* changedGeneratedCols; /* attribute number of generated column to rebuild */
    List* changedRLSPolicies;   /* oid of RLSPolicies to rebuild */
    List* changedViewOids;      /* OIDs of views to rebuild */
    List* changedViewDefs;      /* string definitions of same */
    List* changedTriggerOids;      /* OIDs of triggers to rebuild */
    List* changedTriggerDefs;      /* string definitions of same */
    bool is_first_after;         /* modify first|after and add firs|after */
    bool is_modify_primary;      /* modify column first|after with primary key, we should pre-record AT_SetNotNull */
    uint2 opt; /* opt of relation before update */
    Datum oldOptions; /* relOptions of relation before update */
    Datum newOptions;
} AlteredTableInfo;

#define FOREIGNTABLE_SUPPORT_AT_CMD(cmd)                                                                           \
    ((cmd) == AT_ChangeOwner || (cmd) == AT_AddNodeList || (cmd) == AT_SubCluster || (cmd) == AT_DeleteNodeList || \
        (cmd) == AT_UpdateSliceLike || (cmd) == AT_GenericOptions)

#define DIST_OBS_SUPPORT_AT_CMD(cmd)                                                                               \
    ((cmd) == AT_ChangeOwner || (cmd) == AT_AddNodeList || (cmd) == AT_DeleteNodeList || (cmd) == AT_SubCluster || \
        (cmd) == AT_GenericOptions || (cmd) == AT_DropNotNull || (cmd) == AT_SetNotNull ||                         \
        (cmd) == AT_SetStatistics || (cmd) == AT_AlterColumnType || (cmd) == AT_AlterColumnGenericOptions ||       \
        (cmd) == AT_AddIndex || (cmd) == AT_DropConstraint || (cmd) == AT_UpdateSliceLike)

extern ObjectAddress DefineRelation(CreateStmt* stmt, char relkind, Oid ownerId, ObjectAddress* typaddress, bool isCTAS = false);

extern void RemoveRelationsonMainExecCN(DropStmt* drop, ObjectAddresses* objects);

extern void RemoveRelations(DropStmt* drop, StringInfo tmp_queryString, RemoteQueryExecType* exec_type);

extern void ShrinkRealtionChunk(ShrinkStmt *shrink);

extern void RemoveObjectsonMainExecCN(DropStmt* drop, ObjectAddresses* objects, bool isFirstNode);

extern ObjectAddresses* PreCheckforRemoveObjects(DropStmt* stmt, StringInfo tmp_queryString,
    RemoteQueryExecType* exec_type, bool isFirstNode, bool is_securityadmin = false);

extern ObjectAddresses* PreCheckforRemoveRelation(
    DropStmt* drop, StringInfo tmp_queryString, RemoteQueryExecType* exec_type);

extern Oid AlterTableLookupRelation(AlterTableStmt* stmt, LOCKMODE lockmode, bool unlock = false);

extern void AlterTable(Oid relid, LOCKMODE lockmode, AlterTableStmt* stmt);

extern LOCKMODE AlterTableGetLockLevel(List* cmds);

extern void ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode);

extern void AlterTableInternal(Oid relid, List* cmds, bool recurse);

extern ObjectAddress AlterTableNamespace(AlterObjectSchemaStmt* stmt, Oid *oldschema);

extern void AlterTableNamespaceInternal(Relation rel, Oid oldNspOid, Oid nspOid, ObjectAddresses* objsMoved);

extern void AlterRelationNamespaceInternal(
    Relation classRel, Oid relOid, Oid oldNspOid, Oid newNspOid, bool hasDependEntry, ObjectAddresses* objsMoved);

extern void CheckTableNotInUse(Relation rel, const char* stmt);
extern void CheckPartitionNotInUse(Partition part, const char* stmt);
#ifdef PGXC
extern void ExecuteTruncate(TruncateStmt* stmt, const char* sql_statement);
#else
extern void ExecuteTruncate(TruncateStmt* stmt);
#endif

extern void SetRelationHasSubclass(Oid relationId, bool relhassubclass);

extern ObjectAddress renameatt(RenameStmt* stmt);

extern ObjectAddress RenameConstraint(RenameStmt* stmt);

extern ObjectAddress RenameRelation(RenameStmt* stmt);

extern void RenameRelationInternal(Oid myrelid, const char* newrelname, char* newschema = NULL);

extern void find_composite_type_dependencies(Oid typeOid, Relation origRelation, const char* origTypeName);

extern void check_of_type(HeapTuple typetuple);

extern void register_on_commit_action(Oid relid, OnCommitAction action);
extern void remove_on_commit_action(Oid relid);

extern void PreCommit_on_commit_actions(void);
extern void AtEOXact_on_commit_actions(bool isCommit);
extern void AtEOSubXact_on_commit_actions(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);
#ifdef PGXC
extern bool IsTempTable(Oid relid);
extern bool IsGlobalTempTable(Oid relid);
extern bool IsGlobalTempTableParallelTrunc();
extern bool IsRelaionView(Oid relid);
extern bool IsIndexUsingTempTable(Oid relid);
extern bool IsOnCommitActions(void);
extern void DropTableThrowErrorExternal(RangeVar* relation, ObjectType removeType, bool missing_ok);
#endif

extern void RangeVarCallbackOwnsTable(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* arg);
extern void RangeVarCallbackOwnsMatView(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* arg);
extern void RangeVarCallbackOwnsRelation(
    const RangeVar* relation, Oid relId, Oid oldRelId, bool target_is_partition, void* noCatalogs);
extern void checkPartNotInUse(Partition part, const char* stmt);
extern List* transformConstIntoTargetType(FormData_pg_attribute* attrs, int2vector* partitionKey, List* boundary, bool partkeyIsFunc = false);
extern List* transformIntoTargetType(FormData_pg_attribute* attrs, int2 pos, List* boundary);

extern void RenameDistributedTable(Oid distributedTableOid, const char* distributedTableNewName);
extern void renamePartitionedTable(Oid partitionedTableOid, const char* partitionedTableNewName);
extern ObjectAddress renamePartition(RenameStmt* stmt);
extern ObjectAddress renamePartitionIndex(RenameStmt* stmt);
extern void renamePartitionInternal(Oid partitionedTableOid, Oid partitionOid, const char* partitionNewName);

extern Oid addPartitionBySN(Relation rel, int seqnum);
extern Datum caculateBoundary(Datum transpoint, Oid attrtypid, Datum intervalue, Oid intertypid, int seqnum);
extern void ATExecSetIndexUsableState(Oid objclassOid, Oid objOid, bool newState);
extern bool checkPartitionLocalIndexesUsable(Oid partitionOid);
extern bool checkRelationLocalIndexesUsable(Relation relation);
extern List* GetPartitionkeyPos(List* partitionkeys, List* schema, bool* partkeyIsFunc = NULL);

extern bool IsPartKeyFunc(Relation rel, bool isPartRel, bool forSubPartition, PartitionExprKeyInfo* partExprKeyInfo = NULL);
extern void ComparePartitionValue(List* pos, FormData_pg_attribute* attrs, List *partitionList, bool isPartition = true, bool partkeyIsFunc = false);
extern void CompareListValue(const List* pos, FormData_pg_attribute* attrs, List *partitionList, bool partkeyIsFunc = false);
extern void clearAttrInitDefVal(Oid relid);

extern void ATMatviewGroup(List* stmts, Oid mvid, LOCKMODE lockmode);
extern void AlterCreateChainTables(Oid relOid, Datum reloptions, CreateStmt *mainTblStmt);

extern void CheckAutoIncrementDatatype(Oid typid, const char* colname);

/**
 * @Description: Whether judge the column is partition column.
 * @in rel, A relation.
 * @in att_no, Attribute number.
 * @return If the the column is partition column, return true, otherwise return false.
 */
extern bool is_partition_column(Relation rel, AttrNumber att_no);
extern Const* GetPartitionValue(List* pos, FormData_pg_attribute* attrs, List* value, bool isinterval, bool isPartition, bool partkeyIsFunc = false);
extern Node* GetTargetValue(Form_pg_attribute attrs, Const* src, bool isinterval, bool partkeyIsFunc = false);
extern void ATExecEnableDisableRls(Relation rel, RelationRlsStatus changeType, LOCKMODE lockmode);
extern bool isQueryUsingTempRelation(Query *query);
extern void addToastTableForNewPartition(Relation relation, Oid newPartId, bool isForSubpartition = false);
extern void fastDropPartition(Relation rel, Oid partOid, const char *stmt, Oid intervalPartOid = InvalidOid,
    bool sendInvalid = true);
extern void ExecutePurge(PurgeStmt* stmt);
extern void ExecuteTimeCapsule(TimeCapsuleStmt* stmt);
extern void truncate_check_rel(Relation rel);
extern void CheckDropViewValidity(ObjectType stmtType, char relKind, const char* relname);
extern int getPartitionElementsIndexByOid(Relation partTableRel, Oid partOid);

extern void SetPartionIndexType(IndexStmt* stmt, Relation rel, bool is_alter_table);
extern bool ConstraintSatisfyAutoIncrement(HeapTuple tuple, TupleDesc desc, AttrNumber attrnum, char contype);
extern void CheckRelAutoIncrementIndex(Oid relid, LOCKMODE lockmode);
extern void RebuildDependViewForProc(Oid proc_oid);
extern void CheckPgRewriteWithDroppedColumn(Oid rel_oid, Oid rw_oid, Form_pg_attribute attForm, int2 old_attnum,
    char** attName, List **old_query_str);
extern void ReplaceViewQueryFirstAfter(List *query_str);
#ifdef USE_SPQ
extern void spq_btbuild_update_pg_class(Relation heap, Relation index);
#endif
#endif /* TABLECMDS_H */
