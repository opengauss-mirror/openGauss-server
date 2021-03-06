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
 * src/include/parser/parse_utilcmd.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

extern void checkPartitionSynax(CreateStmt *stmt);
extern List* transformCreateStmt(CreateStmt* stmt, const char* queryString, const List* uuids,
    bool preCheck, bool isFirstNode = true);
extern List* transformAlterTableStmt(Oid relid, AlterTableStmt* stmt, const char* queryString);
extern IndexStmt* transformIndexStmt(Oid relid, IndexStmt* stmt, const char* queryString);
extern void transformRuleStmt(RuleStmt* stmt, const char* queryString, List** actions, Node** whereClause);
extern List* transformCreateSchemaStmt(CreateSchemaStmt* stmt);
extern void transformRangePartitionValue(ParseState* pstate, Node* rangePartDef, bool needCheck);
extern List* transformListPartitionValue(ParseState* pstate, List* boundary, bool needCheck, bool needFree);
extern List* transformRangePartitionValueInternal(ParseState* pstate, List* boundary,
    bool needCheck, bool needFree, bool isPartition = true);
extern Node* transformIntoConst(ParseState* pstate, Node* maxElem, bool isPartition = true);

#ifdef PGXC
extern bool CheckLocalIndexColumn(char loctype, char* partcolname, char* indexcolname);
#endif
extern Oid generateClonedIndex(Relation source_idx, Relation source_relation, char* tempIndexName, Oid targetTblspcOid,
    bool skip_build, bool partitionedIndex);
extern void checkPartitionName(List* partitionList, bool isPartition = true);

extern Oid searchSeqidFromExpr(Node* cooked_default);
extern bool is_start_end_def_list(List* def_list);
extern void get_range_partition_name_prefix(char* namePrefix, char* srcName, bool printNotice, bool isPartition);
extern List* transformRangePartStartEndStmt(ParseState* pstate, List* partitionList, List* pos, 
	Form_pg_attribute* attrs, int32 existPartNum, Const* lowBound, Const* upBound, bool needFree, 
	bool isPartition = true);
extern bool check_contains_tbllike_in_multi_nodegroup(CreateStmt* stmt);
extern bool is_multi_nodegroup_createtbllike(PGXCSubCluster* subcluster, Oid oid);
extern char* getTmptableIndexName(const char* srcSchema, const char* srcIndex);

#endif /* PARSE_UTILCMD_H */
