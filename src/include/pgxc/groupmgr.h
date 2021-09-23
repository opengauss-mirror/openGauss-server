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

#include "c.h"
#include "nodes/parsenodes.h"

typedef struct nodeinfo {
    Oid node_id;
    uint2 old_node_index;
    uint2 new_node_index;
    int old_buckets_num;
#ifdef USE_ASSERT_CHECKING
    int new_buckets_num;
#endif
    bool deleted;
} nodeinfo;

/* installation mode for storage node group */
#define INSTALLATION_MODE "installation"

#define BUCKETMAPLEN_CONFIG_NAME "buckets_len"
#define MIN_BUCKETMAPLEN 32
#define MAX_BUCKETMAPLEN 16384
#define MIN_BUCKETCOUNT  32
#define CHAR_BUF_SIZE 512

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
extern List* GetNodeGroupOidCompute(Oid role_id);
extern uint2* GetBucketMapByGroupName(const char* groupname, int *bucketlen);
extern char* DeduceGroupByNodeList(Oid* nodeoids, int numnodes);
extern void PgxcGroupAddNode(Oid group_oid, Oid nodeid);
extern void PgxcGroupRemoveNode(Oid group_oid, Oid nodeid);
extern bool IsNodeInLogicCluster(Oid* oid_array, int count, Oid excluded);
extern bool IsLogicClusterRedistributed(const char* group_name);
extern char* PgxcGroupGetFirstLogicCluster();
extern bool PgxcGroup_Resizing();
extern oidvector* GetGroupMemberRef(HeapTuple tup, bool* need_free);
extern char GetGroupKind(HeapTuple tup);

/* Routines for bucketmap cache */
extern uint2* BucketMapCacheGetBucketmap(Oid groupoid, int *bucketlen);
extern uint2* BucketMapCacheGetBucketmap(const char* groupname, int *bucketlen);
extern void BucketMapCacheDestroy(void);

extern int GsGlobalConfigGetBucketMapLen(void);
extern void CheckBucketMapLenValid(void);

#endif /* GROUPMGR_H */
