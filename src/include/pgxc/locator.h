/* -------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/locator.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#define LOCATOR_TYPE_REPLICATED 'R'
#define LOCATOR_TYPE_HASH 'H'
#define LOCATOR_TYPE_RANGE 'G'
#define LOCATOR_TYPE_RROBIN 'N'
#define LOCATOR_TYPE_CUSTOM 'C'
#define LOCATOR_TYPE_MODULO 'M'
#define LOCATOR_TYPE_LIST 'L'
#define LOCATOR_TYPE_NONE 'O'
#define LOCATOR_TYPE_DISTRIBUTED                  \
    'D' /* for distributed table without specific \
         * scheme, e.g. result of JOIN of         \
         * replicated and distributed table */
/* We may consider the following type as future use */
#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorNone(x) ((x) == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) ((x) == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) \
    ((x) == LOCATOR_TYPE_HASH || (x) == LOCATOR_TYPE_RROBIN || \
     (x) == LOCATOR_TYPE_MODULO || (x) == LOCATOR_TYPE_DISTRIBUTED || \
     (x) == LOCATOR_TYPE_LIST || (x) == LOCATOR_TYPE_RANGE)
#define IsLocatorDistributedByValue(x) ((x) == LOCATOR_TYPE_HASH || (x) == LOCATOR_TYPE_MODULO || \
    (x) == LOCATOR_TYPE_RANGE || (x) == LOCATOR_TYPE_LIST)

#define IsLocatorDistributedByHash(x) ((x) == LOCATOR_TYPE_HASH || (x) == LOCATOR_TYPE_RROBIN)
#define IsLocatorDistributedBySlice(x) ((x) == LOCATOR_TYPE_RANGE || (x) == LOCATOR_TYPE_LIST)

#include "nodes/primnodes.h"
#include "utils/relcache.h"
#include "utils/partitionmap.h"
#include "nodes/nodes.h"
#include "nodes/params.h"

/*
 * How relation is accessed in the query
 */
typedef enum {
    RELATION_ACCESS_READ,            /* SELECT */
    RELATION_ACCESS_READ_FOR_UPDATE, /* SELECT FOR UPDATE */
    RELATION_ACCESS_UPDATE,          /* UPDATE OR DELETE */
    RELATION_ACCESS_INSERT           /* INSERT */
} RelationAccessType;

/* @hdfs
 * Struct stores the information of each file for hdfs foreign scan schedule.
 */
typedef struct SplitInfo {
    NodeTag type;
    char* filePath;        /* Absolute path of the file or folder. */
    char* fileName;        /* The name of the file or folder. */
    List* partContentList; /* the partition column value list. */
    int64 ObjectSize;
    char* eTag;
    int prefixSlashNum;
} SplitInfo;

/* @hdfs
 * Struct stores the mapping of datanode id and list of split assigned to it.
 */
typedef struct SplitMap {
    NodeTag type;
    int nodeId;             /* DataNode Id     */
    char locatorType;       /* Location type of rel */
    int64 totalSize;        /* Total size of objects */
    int fileNums;           /* the number of files */
    char* downDiskFilePath; /* the absolute path of down disk file, store in memory if it is null */
    List* lengths;          /* the length for each part */
    List* splits;           /* Splits to read */
} SplitMap;

/*@dfs
 * Struct stores the items which will be sent from CN to DN for hdfs foreign scan.
 */
typedef struct DfsPrivateItem {
    NodeTag type;
    List* columnList;      /* The list of all the target columns and the
                            * restriction columns of the relation.
                            */
    List* targetList;      /* The list of all the target columns. */
    List* restrictColList; /* The list of columns in the restriction. */
    List* partList;        /* The list of all the partition columns. */
    /*
     * The list of the primitive restrictions, but inputcollid of the OpExpr is
     * C_COLLATION_OID. we do not care colation in coarse filter.
     */
    List* opExpressionList;
    List* dnTask;        /* The task list assigned to dn. */
    List* hdfsQual;      /* The list of restrictions to push down. */
    int colNum;          /* The number of the columns. */
    double* selectivity; /* The array of selectivities for each column. */
} DfsPrivateItem;

typedef struct {
    Oid relid;                /* OID of relation */
    char locatorType;         /* locator type, see above */
    List* partAttrNum;        /* Distribution column attribute */
    List* nodeList;           /* Node indices where data is located */
    ListCell* roundRobinNode; /* Index of the next node to use */
    NameData gname;           /* Group name */

    /*
     * Caution!!! don't invoke pfree()
     *
     * Because we introduce the bucketmap cache where a gaussdb persistent global memory
     * area is allocated to hold the bucketmap content, just do pointer-assignment.
     */
    uint2* buckets_ptr; /* pointer to local bucket-node mapping */
    int    buckets_cnt; /* bucket-node mapping elements count */
} RelationLocInfo;

#define IsRelationReplicated(rel_loc) IsLocatorReplicated((rel_loc)->locatorType)
#define IsRelationColumnDistributed(rel_loc) IsLocatorColumnDistributed((rel_loc)->locatorType)
#define IsRelationDistributedByValue(rel_loc) IsLocatorDistributedByValue((rel_loc)->locatorType)
#define InvalidateBuckets(rel_loc) ((rel_loc)->buckets_ptr = NULL)

/*
 * Distribution information related with node group
 */
typedef struct Distribution {
    Oid group_oid;
    Bitmapset* bms_data_nodeids;
} Distribution;

/*
 * list/range distributed table's slice boundaries,
 * used for dispatching row/batch in datanode StreamProducer
 */
typedef struct SliceBoundary {
    NodeTag type;
    int nodeIdx;
    int len;
    Const *boundary[MAX_RANGE_PARTKEY_NUMS];
} SliceBoundary;

typedef struct ExecBoundary {
    NodeTag type;
    char locatorType;
    int32 count;
    SliceBoundary **eles;
} ExecBoundary;

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 * Note on dist_vars:
 * dist_vars is a list of Var nodes indicating the columns by which the
 * relations (result of query) are distributed. The result of equi-joins between
 * distributed relations, can be considered to be distributed by distribution
 * columns of either of relation. Hence a list. dist_vars is ignored in case of
 * distribution types other than HASH or MODULO.
 */
typedef struct {
    NodeTag type;
    List* primarynodelist; /* Primary node list indexes */
    List* nodeList;        /* Node list indexes */
    Distribution distribution;
    char baselocatortype;          /* Locator type, see above */
    List* en_expr;                 /* Expression to evaluate at execution time
                                    * if planner can not determine execution
                                    * nodes */
    Oid en_relid;                  /* Relation to determine execution nodes */

    Oid rangelistOid;              /* list/range table oid that slice map references */
    bool need_range_prune;         /* flag for list/range dynamic slice pruning */
    Index en_varno;                /* relation varno */
    ExecBoundary* boundaries; /* slice boundaries that used for list/range redistribution in DML */

    RelationAccessType accesstype; /* Access type to determine execution
                                    * nodes */
    List* en_dist_vars;            /* See above for details */
    int bucketmapIdx;              /* the index of bucketmap
                                    * (1) the value -1 means that the bucketmap is generated
                                    * as default way "hashvalue % member_count
                                    * (2) the others values means the index of
                                    * the struct PlannedStmt -> bucketMap */
    bool nodelist_is_nil;          /* true if nodeList is NIL when initialization */
    List* original_nodeList;       /* used to keep original nodeList when explain analyze pbe */
    List* dynamic_en_expr;         /* dynamic judge en_expr that should be judged later */
    /* runtime pbe purning for hashbucket, we keep another copy for safe */
    int   bucketid;                /* bucket id where the data should be in */
    List *bucketexpr;              /* exprs which can be used for bucket purning */
    Oid   bucketrelid;             /* relid of the purning relation */
    List* hotkeys;                 /* List of HotkeyInfo */
} ExecNodes;

#define INVALID_BUCKET_ID -1

#define IsExecNodesReplicated(en) IsLocatorReplicated((en)->baselocatortype)
#define IsExecNodesColumnDistributed(en) IsLocatorColumnDistributed((en)->baselocatortype)
#define IsExecNodesDistributedByValue(en) IsLocatorDistributedByValue((en)->baselocatortype)
#define IsExecNodesDistributedBySlice(en) IsLocatorDistributedBySlice((en)->baselocatortype)

/* Function for RelationLocInfo building and management */
extern void RelationBuildLocator(Relation rel);
extern void InitBuckets(RelationLocInfo* rel_loc_info, Relation relation);
extern RelationLocInfo* GetRelationLocInfo(Oid relid);
extern RelationLocInfo* TsGetRelationLocInfo(Oid relid);
extern RelationLocInfo* GetRelationLocInfoDN(Oid relid);
extern RelationLocInfo* CopyRelationLocInfo(RelationLocInfo* srcInfo);
extern void FreeRelationLocInfo(RelationLocInfo* relationLocInfo);
extern List* GetRelationDistribColumn(RelationLocInfo* locInfo);
extern char GetLocatorType(Oid relid);
extern List* GetPreferredReplicationNode(List* relNodes);
extern bool IsTableDistOnPrimary(RelationLocInfo* locInfo);
extern bool IsLocatorInfoEqual(RelationLocInfo* locInfo1, RelationLocInfo* locInfo2);
extern bool IsSliceInfoEqualByOid(Oid tabOid1, Oid tabOid2);
extern int GetRoundRobinNode(Oid relid);
extern bool IsTypeDistributable(Oid colType);
extern bool IsTypeDistributableForSlice(Oid colType);
extern bool IsDistribColumn(Oid relid, AttrNumber attNum);
extern HeapTuple SearchTableEntryCopy(char parttype, Oid relid);

extern ExecNodes* GetRelationNodes(RelationLocInfo* rel_loc_info, Datum* values, const bool* nulls, Oid* attr,
    List* idx_dist_by_col, RelationAccessType accessType, bool needDistribution = true, bool use_bucketmap = true);
extern ExecNodes* GetRelationNodesByQuals(void* query, Oid reloid, Index varno, Node* quals,
    RelationAccessType relaccess, ParamListInfo boundParams, bool useDynamicReduce = false);
/* Global locator data */
extern Distribution* NewDistribution();
extern void DestroyDistribution(Distribution* distribution);
extern void FreeExecNodes(ExecNodes** exec_nodes);
extern List* GetAllDataNodes(void);
extern List* GetAllCoordNodes(void);
List* GetNodeGroupNodeList(Oid* members, int nmembers);

extern int compute_modulo(unsigned int numerator, unsigned int denominator);
extern int get_node_from_modulo(int modulo, List* nodeList);
extern int GetMinDnNum();
extern Expr* pgxc_check_distcol_opexpr(Index varno, AttrNumber attrNum, OpExpr* opexpr);
extern void PruningDatanode(ExecNodes* execNodes, ParamListInfo boundParams);
extern void ConstructSliceBoundary(ExecNodes* en);

extern Expr* pgxc_find_distcol_expr(void* query, Index varno, AttrNumber attrNum, Node* quals);
extern bool IsFunctionShippable(Oid foid);

#ifdef USE_SPQ
extern bool IsSpqTypeDistributable(Oid colType);
#endif

#endif /* LOCATOR_H */
