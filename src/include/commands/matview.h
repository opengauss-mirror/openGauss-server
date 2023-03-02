/*-------------------------------------------------------------------------
 *
 * matview.h
 *      prototypes for matview.c.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 * ------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "tcop/dest.h"
#include "utils/relcache.h"

#define MlogAttributeNum 5

#define MlogAttributeAction     1
#define MlogAttributeTime       2
#define MlogAttributeCtid       3
#define MlogAttributeXid        4
#define MlogAttributeSeqno      5

#define MatMapAttributeNum      5
#define MatMapAttributeMatid    1
#define MatMapAttributeMatctid  2
#define MatMapAttributeRelid    3
#define MatMapAttributeRelctid  4
#define MatMapAttributeRelxid   5

#define ActionCreateMat         1
#define ActionRefreshInc        2

/* used to blocking ANALYZE */
#define MLOGLEN     5
#define MATMAPLEN   11
#define MATMAPNAME  "matviewmap_"
#define MLOGNAME    "mlog_"

#define ISMATMAP(relname) (strncmp(relname, MATMAPNAME, MATMAPLEN) == 0)
#define ISMLOG(relname) (strncmp(relname, MLOGNAME, MLOGLEN) == 0)

extern Size MatviewShmemSize(void);
extern void MatviewShmemInit(void);

extern void SetRelationIsScannable(Relation relation);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
                                  ParamListInfo params, char *completionTag);

extern ObjectAddress ExecRefreshMatViewInc(RefreshMatViewStmt *stmt, const char *queryString,
                                  ParamListInfo params, char *completionTag);
extern ObjectAddress ExecCreateMatViewInc(CreateTableAsStmt* stmt, const char* queryString, ParamListInfo params);
extern void ExecRefreshMatViewAll(RefreshMatViewStmt *stmt, const char *queryString,
                                    ParamListInfo params, char *completionTag);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid);

extern void build_matview_dependency(Oid matviewOid, Relation materRel);
extern Oid create_matview_map(Oid intoRelationId);
extern void insert_into_matview_map(Oid mapid, Oid matid, ItemPointer matcitd,
                            Oid relid, ItemPointer relctid, TransactionId xid);
extern Oid find_matview_mlog_table(Oid relid);
extern void insert_into_mlog_table(Relation rel, Oid mlogid, HeapTuple tuple,
                            ItemPointer tid, TransactionId xid, char action);
extern void create_matview_meta(Query *query, RangeVar *rel, bool incremental);

extern void check_matview_op_supported(CreateTableAsStmt *ctas);
extern DistributeBy *infer_incmatview_distkey(CreateTableAsStmt *stmt);
extern void check_basetable_permission(Query *query);
extern bool isIncMatView(RangeVar *rv);
extern void MatviewShmemSetInvalid();
extern void check_basetable(Query *query, bool isCreateMatview, bool isIncremental);
extern List *pull_up_rels_recursive(Node *node);

#endif   /* MATVIEW_H */
