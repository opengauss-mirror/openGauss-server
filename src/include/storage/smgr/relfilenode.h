/* -------------------------------------------------------------------------
 *
 * relfilenode.h
 *	  Physical access information for relations.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/relfilenode.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RELFILENODE_H
#define RELFILENODE_H

#include "storage/backendid.h"
#include "storage/buf/block.h"

typedef enum {
    HEAP_DISK = 0,
    SEGMENT_PAGE,
    INVALID_STORAGE
} StorageType;

/*
 * The physical storage of a relation consists of one or more forks. The
 * main fork is always created, but in addition to that there can be
 * additional forks for storing various metadata. ForkNumber is used when
 * we need to refer to a specific fork in a relation.
 */
typedef int ForkNumber;

/* used for delete forknum */
#define COMPRESS_FORKNUM -9

#define SEGMENT_EXT_8192_FORKNUM -8
#define SEGMENT_EXT_1024_FORKNUM -7
#define SEGMENT_EXT_128_FORKNUM -6
#define SEGMENT_EXT_8_FORKNUM -5

#define PAX_DFS_TRUNCATE_FORKNUM -4
#define PAX_DFS_FORKNUM -3
#define DFS_FORKNUM -2
#define InvalidForkNumber -1
#define MAIN_FORKNUM 0
#define FSM_FORKNUM 1
#define VISIBILITYMAP_FORKNUM 2
#define BCM_FORKNUM 3
#define INIT_FORKNUM 4
// used for data file cache, you can modify than as you like
#define PCA_FORKNUM 5
#define PCD_FORKNUM 6

/*
 * NOTE: if you add a new fork, change MAX_FORKNUM below and update the
 * forkNames array in catalog.c
 */
#define MAX_FORKNUM INIT_FORKNUM

/*
 * RelFileNode must provide all that we need to know to physically access
 * a relation, with the exception of the backend ID, which can be provided
 * separately. Note, however, that a "physical" relation is comprised of
 * multiple files on the filesystem, as each fork is stored as a separate
 * file, and each fork can be divided into multiple segments. See md.c.
 *
 * spcNode identifies the tablespace of the relation.  It corresponds to
 * pg_tablespace.oid.
 *
 * dbNode identifies the database of the relation.	It is zero for
 * "shared" relations (those common to all databases of a cluster).
 * Nonzero dbNode values correspond to pg_database.oid.
 *
 * relNode identifies the specific relation.  relNode corresponds to
 * pg_class.relfilenode (NOT pg_class.oid, because we need to be able
 * to assign new physical files to relations in some situations).
 * Notice that relNode is only unique within a particular database.
 * 
 * bucketNode identifies bucketid of the relation. 1) -1 means non-bucket
 * relation with heap disk storage type , 2) 0~BUCKETDATALEN-1 means bucketid 
 * of a bucket relation, 3) BUCKETDATALEN means a non-bucket segment storage 
 * relation. Both 2) and 3) representing segment storage type.
 *
 * Note: spcNode must be GLOBALTABLESPACE_OID if and only if dbNode is
 * zero.  We support shared relations only in the "global" tablespace.
 *
 * Note: in pg_class we allow reltablespace == 0 to denote that the
 * relation is stored in its database's "default" tablespace (as
 * identified by pg_database.dattablespace).  However this shorthand
 * is NOT allowed in RelFileNode structs --- the real tablespace ID
 * must be supplied when setting spcNode.
 *
 * Note: in pg_class, relfilenode can be zero to denote that the relation
 * is a "mapped" relation, whose current true filenode number is available
 * from relmapper.c.  Again, this case is NOT allowed in RelFileNodes.
 *
 * Note: various places use RelFileNode in hashtable keys.  Therefore,
 * there *must not* be any unused padding bytes in this struct.  That
 * should be safe as long as all the fields are of type Oid.
 */
typedef struct RelFileNode {
    Oid spcNode; /* tablespace */
    Oid dbNode;  /* database */
    Oid relNode; /* relation */
    int2 bucketNode; /* bucketid */
    uint2 opt;
} RelFileNode;

typedef struct RelFileNodeV2 {
    Oid spcNode; /* tablespace */
    Oid dbNode;  /* database */
    Oid relNode; /* relation */
    int4 bucketNode; /* bucketid */
} RelFileNodeV2;


#define IsSegmentFileNode(rnode) ((rnode).bucketNode > InvalidBktId)
#define IsHeapFileNode(rnode)  (!IsSegmentFileNode(rnode))
#define IsSegmentPhysicalRelNode(rNode) (IsSegmentFileNode(rNode) && (rNode).relNode <= 5)

#define IsBucketFileNode(rnode) ((rnode).bucketNode > InvalidBktId && (rnode).bucketNode < SegmentBktId)

/*RelFileNodeOld: Old version relfilenode. Compatible with older versions of relfilenode.*/

typedef struct RelFileNodeOld
{
    Oid         spcNode;        /* tablespace */
    Oid         dbNode;         /* database */
    Oid         relNode;        /* relation */
} RelFileNodeOld;

#define RelFileNodeRelCopy(relFileNodeRel, relFileNode) \
    do { \
        (relFileNodeRel).spcNode = (relFileNode).spcNode; \
        (relFileNodeRel).dbNode = (relFileNode).dbNode; \
        (relFileNodeRel).relNode = (relFileNode).relNode; \
    } while(0)

#define RelFileNodeCopy(relFileNode, relFileNodeRel, bucketid) \
    do {                                                       \
        (relFileNode).spcNode = (relFileNodeRel).spcNode;      \
        (relFileNode).dbNode = (relFileNodeRel).dbNode;        \
        (relFileNode).relNode = (relFileNodeRel).relNode;      \
        (relFileNode).bucketNode = (bucketid);                 \
        (relFileNode).opt = 0;                                 \
    } while (0)

#define RelFileNodeV2Copy(relFileNodeV2, relFileNode)          \
    do {                                                       \
        (relFileNodeV2).spcNode = (relFileNode).spcNode;       \
        (relFileNodeV2).dbNode = (relFileNode).dbNode;         \
        (relFileNodeV2).relNode = (relFileNode).relNode;       \
        (relFileNodeV2).bucketNode = (relFileNode).bucketNode; \
    } while (0)

/*This struct used for remove duplicated file list where we scan part of BCM files*/
typedef struct RelFileNodeKey {
    RelFileNode relfilenode; /*relfilenode*/
    int columnid;            /*column for CU store*/
} RelFileNodeKey;
typedef struct RelFileNodeKeyEntry {
    RelFileNodeKey key;
    int number; /*Times the relfilenode occurence*/
} RelFileNodeKeyEntry;

/*
 * Augmenting a relfilenode with the backend ID provides all the information
 * we need to locate the physical storage.  The backend ID is InvalidBackendId
 * for regular relations (those accessible to more than one backend), or the
 * owning backend's ID for backend-local relations.  Backend-local relations
 * are always transient and removed in case of a database crash; they are
 * never WAL-logged or fsync'd.
 */
typedef struct RelFileNodeBackend {
    RelFileNode node;
    BackendId backend;
} RelFileNodeBackend;

typedef enum StorageEngine { ROW_STORE = 0, COLUMN_STORE } StorageEngine;

#define IsSupportSE(se) (ROW_STORE <= se && se <= COLUMN_STORE)

// key to identify data file
//
typedef struct RelFileNodeForkNum {
    RelFileNodeBackend rnode;
    BlockNumber segno;
    ForkNumber forknumber;
    StorageEngine storage;
} RelFileNodeForkNum;

#define RelFileNodeBackendIsTemp(rnode) ((rnode).backend != InvalidBackendId)

/*
 * Note: RelFileNodeEquals and RelFileNodeBackendEquals compare relNode first
 * since that is most likely to be different in two unequal RelFileNodes.  It
 * is probably redundant to compare spcNode if the other fields are found equal,
 * but do it anyway to be sure.  Likewise for checking the backend ID in
 * RelFileNodeBackendEquals.
 */
#define RelFileNodeEquals(node1, node2) \
    ((node1).relNode == (node2).relNode && (node1).dbNode == (node2).dbNode && (node1).spcNode == (node2).spcNode && \
    (node1).bucketNode == (node2).bucketNode)

#define RelFileColumnNodeRelEquals(node1, node2) \
        ((node1).relNode == (node2).relNode && \
        (node1).dbNode == (node2).dbNode && \
        (node1).spcNode == (node2).spcNode)

#define RelFileNodeRelEquals(node1, node2) \
        ((node1).relNode == (node2).relNode && \
        (node1).dbNode == (node2).dbNode && \
        (node1).spcNode == (node2).spcNode)

#define RelFileNodeBackendEquals(node1, node2)                                                     \
    ((node1).node.relNode == (node2).node.relNode && (node1).node.dbNode == (node2).node.dbNode && \
        (node1).backend == (node2).backend && (node1).node.spcNode == (node2).node.spcNode && \
        (node1).node.bucketNode == (node2).node.bucketNode)

// keep [MAX_FORKNUM, FirstColumnForkNum) unused.
// try my best to reduce the effect on updating.
#define FirstColForkNum (MAX_FORKNUM)

// ColForkNum include column id information
//
#define IsValidColForkNum(forkNum) ((forkNum) > FirstColForkNum)

#define IsValidDfsForkNum(forkNum) ((forkNum) == DFS_FORKNUM)

#define IsValidPaxDfsForkNum(forkNum) ((forkNum) == PAX_DFS_FORKNUM)

#define IsTruncateDfsForkNum(forkNum) ((forkNum) == PAX_DFS_TRUNCATE_FORKNUM)

#define ColForkNum2ColumnId(forkNum) (AssertMacro(IsValidColForkNum(forkNum)), ((forkNum)-FirstColForkNum))

#define ColumnId2ColForkNum(attid) (AssertMacro(AttrNumberIsForUserDefinedAttr(attid)), (FirstColForkNum + (attid)))

// fetch column number from forknum given.
#define GetColumnNum(forkNum) (IsValidColForkNum(forkNum) ? ColForkNum2ColumnId(forkNum) : 0)

typedef struct {
    RelFileNode filenode;
    // for row table, *forknum* <= MAX_FORKNUM.
    // for column table, *forknum* > FirstColForkNum.
    ForkNumber forknum;
    // relation's owner id
    Oid ownerid;
} ColFileNode;

typedef struct {
    RelFileNodeOld filenode;
    // for row table, *forknum* <= MAX_FORKNUM.
    // for column table, *forknum* > FirstColForkNum.
    ForkNumber forknum;
    // relation's owner id
    Oid ownerid;
} ColFileNodeRel;

/*
 *  1) ForkNumber type must be 32-bit;
 *  2) forknum value occupies the lower 16-bit;
 *  3) bucketid value occupies the upper 16-bit;
 *  If the upper three condition changes, please consider this function again.
 */
static inline void forknum_add_bucketid(ForkNumber& forknum, int4 bucketid)
{
    forknum = (ForkNumber)(((uint2)forknum) | (((uint2)(bucketid + 1)) << 16));
}

/*
 *  1) converse ForkNumber to uint to make sure logic right-shift
 *  2) converse the logic-right-shift result to int2 to ensure the negative bucketid value
 */
static inline int4 forknum_get_bucketid(const ForkNumber& forknum)
{
    return (int2)((((uint)forknum >> 16) & 0xffff) - 1);
}

/*
 *  1) converse ForkNumber to uint to make sure the bit-operation safe
 *  2) converse the lower 16-bit to int2 to ensure the negative forknum
 */
static inline ForkNumber forknum_get_forknum(const ForkNumber& forknum)
{
    return (int2)((uint)forknum & 0xffff);
}

/*
 *  1) converse ForkNumber to uint to make sure the bit-operation safe
 *  2) converse the lower 16-bit to int2 to ensure the negative forknum
 */
static inline StorageType forknum_get_storage_type(const ForkNumber& forknum)
{
    return (StorageType)(((uint)forknum & 0xC000) >> 14);
}

#define ColFileNodeCopy(colFileNode, colFileNodeRel)                                          \
    do {                                                                                      \
        (colFileNode)->filenode.spcNode = (colFileNodeRel)->filenode.spcNode;                 \
        (colFileNode)->filenode.dbNode = (colFileNodeRel)->filenode.dbNode;                   \
        (colFileNode)->filenode.relNode = (colFileNodeRel)->filenode.relNode;                 \
        (colFileNode)->filenode.opt = 0;                                                      \
        (colFileNode)->filenode.bucketNode = forknum_get_bucketid((colFileNodeRel)->forknum); \
        (colFileNode)->forknum = forknum_get_forknum((colFileNodeRel)->forknum);              \
        (colFileNode)->ownerid = (colFileNodeRel)->ownerid;                                   \
    } while (0)

#endif /* RELFILENODE_H */
