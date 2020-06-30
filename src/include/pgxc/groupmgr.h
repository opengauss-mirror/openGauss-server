/* -------------------------------------------------------------------------
 *
 * groupmgr.h
 *	  Routines for PGXC node group management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/groupmgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GROUPMGR_H
#define GROUPMGR_H

#include "nodes/parsenodes.h"

/* installation mode for storage node group */
#define INSTALLATION_MODE "installation"

extern void PgxcGroupCreate(CreateGroupStmt* stmt);
extern void PgxcGroupAlter(AlterGroupStmt* stmt);
extern void PgxcGroupRemove(DropGroupStmt* stmt);
extern char* PgxcGroupGetInstallationGroup();
extern char* PgxcGroupGetInRedistributionGroup();
extern char* PgxcGroupGetCurrentLogicCluster();
extern Oid PgxcGroupGetRedistDestGroupOid();
extern char* PgxcGroupGetStmtExecGroupInRedis();
extern bool CanPgxcGroupRemove(Oid group_oid, bool ignore_pmk = false);
extern int GetTableCountByDatabase(const char* database_name, char* query);
extern bool GetExecnodeIsInstallationGroup(Oid groupoid);
extern void InitNodeGroupStatus(void);
extern void CleanNodeGroupStatus(void);
extern List* Get_nodegroup_oid_compute(Oid role_id);
extern uint2* GetBucketMapByGroupName(const char* groupname);
extern char* DeduceGroupByNodeList(Oid* nodeoids, int numnodes);
extern void PgxcGroupAddNode(Oid group_oid, Oid nodeid);
extern void PgxcGroupRemoveNode(Oid group_oid, Oid nodeid);
extern bool IsNodeInLogicCluster(Oid* oid_array, int count, Oid excluded);
extern bool IsLogicClusterRedistributed(const char* group_name);
extern char* PgxcGroupGetFirstLogicCluster();
extern bool PgxcGroup_Resizing();

/* Routines for bucketmap cache */
extern uint2* BucketMapCacheGetBucketmap(Oid groupoid);
extern uint2* BucketMapCacheGetBucketmap(const char* groupname);
extern void BucketMapCacheDestroy(void);

#endif /* GROUPMGR_H */
