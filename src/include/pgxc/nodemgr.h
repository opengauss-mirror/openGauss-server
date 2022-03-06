/* -------------------------------------------------------------------------
 *
 * nodemgr.h
 *  Routines for node management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/nodemgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEMGR_H
#define NODEMGR_H

#include "nodes/parsenodes.h"

#define PGXC_NODENAME_LENGTH 64

/* Node definition */
typedef struct NodeDefinition {
    Oid nodeoid;
    int nodeid;
    NameData nodename;
    NameData nodehost;
    int nodeport;
    int nodectlport;
    int nodesctpport;
    NameData nodehost1;
    int nodeport1;
    int nodectlport1;
    int nodesctpport1;
    bool hostisprimary;
    bool nodeisprimary;
    bool nodeispreferred;
    bool nodeis_central;
    bool nodeis_active;
    Oid shard_oid;
} NodeDefinition;

typedef struct {
    int num_nodes;                   /* number of data nodes */
    NodeDefinition* nodesDefinition; /* all data nodes' defination */
} GlobalNodeDefinition;

typedef struct SkipNodeDefinition {
    Oid nodeoid;
    NameData nodename;
    int slicenum;
} SkipNodeDefinition;

/* Connection statistics info */
typedef struct {
    Oid primaryNodeId;         /* original primary nodeid */
    int nodeIndex;             /* original primary node subscript */
    bool isSucceed;             /* link setup success flag */
    Oid *nodeList;             /* nodeid about all nodes in the same segment */
} NodeRelationInfo;

typedef struct DNInfo {
    Oid    oid;
    int    idx;         /* the sequence id of dn */
}DNInfo;

extern GlobalNodeDefinition* global_node_definition;

extern void NodeTablesShmemInit(void);
extern Size NodeTablesShmemSize(void);

extern void PgxcNodeListAndCount(void);
extern void PgxcNodeGetOids(Oid** coOids, Oid** dnOids, int* num_coords, int* num_dns, bool update_preferred);
extern void PgxcNodeGetOidsForInit(Oid** coOids, Oid** dnOids,
    int* num_coords, int* num_dns, int * num_primaries, bool update_preferred);
extern void PgxcNodeGetStandbyOids(Oid** coOids, Oid** dnOids, int* numCoords, int* numDns, bool needInitPGXC);
extern NodeDefinition* PgxcNodeGetDefinition(Oid node, bool checkStandbyNodes = false);
extern void PgxcNodeAlter(AlterNodeStmt* stmt);
extern void PgxcNodeCreate(CreateNodeStmt* stmt);
extern char PgxcNodeRemove(DropNodeStmt* stmt);
extern void PgxcCoordinatorAlter(AlterCoordinatorStmt* stmt);

extern List* PgxcNodeGetAllDataNodeNames(void);
extern List* PgxcNodeGetDataNodeNames(List* nodeList);
extern List* PgxcNodeGetDataNodeOids(List* nodeList);
extern int pickup_random_datanode(int numnodes);
extern List* PgxcGetCoordlist(bool exclude_self);
extern bool PgxcIsCentralCoordinator(const char* NodeName);
extern void PgxcGetNodeName(int node_idx, char* nodenamebuf, int len);
extern int PgxcGetNodeIndex(const char* nodename);
extern Oid PgxcGetNodeOid(int node_idx);
extern int PgxcGetCentralNodeIndex();

extern void PgxcNodeFreeDnMatric(void);
extern void PgxcNodeInitDnMatric(void);
extern bool PgxcNodeCheckDnMatric(Oid oid1, Oid oid2);
extern bool PgxcNodeCheckPrimaryHost(Oid oid, const char* host);
extern Oid PgxcNodeGetPrimaryDNFromMatric(Oid oid1);
extern void PgxcNodeCount(int* numCoords, int* numDns);

extern bool set_dnoid_info_from_matric(NodeRelationInfo *needCreateNode, bool *isMatricVisited);

/* interface of dataNode to use hash table */
extern void dn_info_local_hash_create();
extern int  dn_info_hash_search(Oid dn_oid);
extern void dn_info_hash_insert(Oid dn_oid, int row);
extern void dn_info_hash_delete(Oid dn_oid);
extern void dn_info_hash_destory();

typedef struct {
    char host[NAMEDATALEN];
    int port;
} DisasterAddr;

typedef struct {
    int slice;
    int port;
    char host[NAMEDATALEN];
    char host1[NAMEDATALEN];
    char node_name[NAMEDATALEN];
} DisasterNode;

typedef struct {
    List* addrs;
    DisasterNode* disaster_info;
    int num_cn;
    int num_dn;
    int num_one_slice;
} DisasterReadContext;

extern void UpdateConsistencyPoint();
extern void UpdateCacheAndConsistencyPoint(DisasterReadContext* drContext);
#endif /* NODEMGR_H */
