/* -------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/parser/parse_utilcmd.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

/* State shared by transformCreateStmt and its subroutines */
typedef struct {
    ParseState* pstate;             /* overall parser state */
    const char* stmtType;           /* "CREATE [FOREIGN] TABLE" or "ALTER TABLE" */
    RangeVar* relation;             /* relation to create */
    Relation rel;                   /* opened/locked rel, if ALTER */
    List* inhRelations;             /* relations to inherit from */
    bool isalter;                   /* true if altering existing table */
    bool ispartitioned;             /* true if it is for a partitioned table */
    bool hasoids;                   /* does relation have an OID column? */
    bool canInfomationalConstraint; /* If the value id true, it means that we can build informational constraint. */
    List* columns;                  /* ColumnDef items */
    List* ckconstraints;            /* CHECK constraints */
    List* clusterConstraints;       /* PARTIAL CLUSTER KEY constraints */
    List* fkconstraints;            /* FOREIGN KEY constraints */
    List* ixconstraints;            /* index-creating constraints */
    List* inh_indexes;              /* cloned indexes from INCLUDING INDEXES */
    List* blist;                    /* "before list" of things to do before creating the table */
    List* alist;                    /* "after list" of things to do after creating the table */
    PartitionState* csc_partTableState;
    List* reloptions;
    List* partitionKey; /* partitionkey for partiitoned table */
    List* subPartitionKey; /* subpartitionkey for subpartiitoned table */
    IndexStmt* pkey;    /* PRIMARY KEY index, if any */
#ifdef PGXC
    List* fallback_dist_col;    /* suggested column to distribute on */
    DistributeBy* distributeby; /* original distribute by column of CREATE TABLE */
    PGXCSubCluster* subcluster; /* original subcluster option of CREATE TABLE */
#endif
    Node* node; /* @hdfs record a CreateStmt or AlterTableStmt object. */
    char* internalData;
    List* uuids;     /* used for create sequence */
    bool isResizing; /* true if the table is resizing */
    bool ofType;         /* true if statement contains OF typename */
    Oid rel_coll_id;    /* relation collation oid */
} CreateStmtContext;

typedef enum TransformTableType { TRANSFORM_INVALID = 0, TRANSFORM_TO_HASHBUCKET, TRANSFORM_TO_NONHASHBUCKET} TransformTableType;

extern void checkPartitionSynax(CreateStmt *stmt);
extern Oid fill_relation_collation(const char* collate, int charset, List** options,
    Oid nsp_coll_oid = InvalidOid);
extern List* transformCreateStmt(CreateStmt* stmt, const char* queryString, const List* uuids,
    bool preCheck, Oid *namespaceid, bool isFirstNode = true);
extern List* transformAlterTableStmt(Oid relid, AlterTableStmt* stmt, const char* queryString);
extern IndexStmt* transformIndexStmt(Oid relid, IndexStmt* stmt, const char* queryString);
extern void transformRuleStmt(RuleStmt* stmt, const char* queryString, List** actions, Node** whereClause);
extern List* transformCreateSchemaStmt(CreateSchemaStmt* stmt);
extern void transformPartitionValue(ParseState* pstate, Node* rangePartDef, bool needCheck);
extern List* transformListPartitionValue(ParseState* pstate, List* boundary, bool needCheck, bool needFree);
extern List* transformRangePartitionValueInternal(ParseState* pstate, List* boundary,
    bool needCheck, bool needFree, bool isPartition = true);
extern Node* transformIntoConst(ParseState* pstate, ParseExprKind exprKind, Node* maxElem, bool isPartition = true);

#ifdef PGXC
extern bool CheckLocalIndexColumn(char loctype, char* partcolname, char* indexcolname);
#endif
extern Oid generateClonedIndex(Relation source_idx, Relation source_relation, char* tempIndexName, Oid targetTblspcOid,
    bool skip_build, bool partitionedIndex);
extern void checkPartitionName(List* partitionList, bool isPartition = true);
extern void checkSubPartitionName(List* partitionList);
extern List* GetPartitionNameList(List* partitionList);
extern char* GetPartitionDefStateName(Node *partitionDefState);
extern NodeTag GetPartitionStateType(char type);

extern Oid searchSeqidFromExpr(Node* cooked_default);
extern bool is_start_end_def_list(List* def_list);
extern void get_range_partition_name_prefix(char* namePrefix, char* srcName, bool printNotice, bool isPartition);
extern List* transformRangePartStartEndStmt(ParseState* pstate, List* partitionList, List* pos, 
	FormData_pg_attribute* attrs, int32 existPartNum, Const* lowBound, Const* upBound, bool needFree, 
	bool isPartition = true);
extern bool check_contains_tbllike_in_multi_nodegroup(CreateStmt* stmt);
extern bool is_multi_nodegroup_createtbllike(PGXCSubCluster* subcluster, Oid oid);
extern char* getTmptableIndexName(const char* srcSchema, const char* srcIndex);

extern IndexStmt* generateClonedIndexStmt(
    CreateStmtContext* cxt, Relation source_idx, const AttrNumber* attmap, int attmap_length, Relation rel,
    TransformTableType transformType);
extern Oid transform_default_collation(const char* collate, int charset, Oid def_coll_oid = InvalidOid,
    bool is_attr = false);
extern Oid check_collation_by_charset(const char* collate, int charset);

#endif /* PARSE_UTILCMD_H */
