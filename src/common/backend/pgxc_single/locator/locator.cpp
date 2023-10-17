/* -------------------------------------------------------------------------
 *
 * locator.c
 *		Functions that help manage table location information such as
 * partitioning and replication information.
 *
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		$$
 *
 * -------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/skey.h"
#include "access/gtm.h"
#include "access/tableam.h"
#include "access/relscan.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "nodes/nodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "postmaster/autovacuum.h"
#include "pgxc/nodemgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_slice.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "access/hash.h"
#include "optimizer/streamplan.h"
#include "optimizer/prep.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "utils/elog.h"

#pragma GCC diagnostic ignored "-Wunused-function"

static uint2* tryGetBucketMap(const char* groupname, char* relname, bool isOtherTempNamespace, int *bucketlen);

extern Const* makeNullConst(Oid consttype, int32 consttypmod, Oid constcollid);

static const unsigned int xc_mod_m[] = {0x00000000,
    0x55555555,
    0x33333333,
    0xc71c71c7,
    0x0f0f0f0f,
    0xc1f07c1f,
    0x3f03f03f,
    0xf01fc07f,
    0x00ff00ff,
    0x07fc01ff,
    0x3ff003ff,
    0xffc007ff,
    0xff000fff,
    0xfc001fff,
    0xf0003fff,
    0xc0007fff,
    0x0000ffff,
    0x0001ffff,
    0x0003ffff,
    0x0007ffff,
    0x000fffff,
    0x001fffff,
    0x003fffff,
    0x007fffff,
    0x00ffffff,
    0x01ffffff,
    0x03ffffff,
    0x07ffffff,
    0x0fffffff,
    0x1fffffff,
    0x3fffffff,
    0x7fffffff};

static const unsigned int xc_mod_q[][6] = {{0, 0, 0, 0, 0, 0},
    {16, 8, 4, 2, 1, 1},
    {16, 8, 4, 2, 2, 2},
    {15, 6, 3, 3, 3, 3},
    {16, 8, 4, 4, 4, 4},
    {15, 5, 5, 5, 5, 5},
    {12, 6, 6, 6, 6, 6},
    {14, 7, 7, 7, 7, 7},
    {16, 8, 8, 8, 8, 8},
    {9, 9, 9, 9, 9, 9},
    {10, 10, 10, 10, 10, 10},
    {11, 11, 11, 11, 11, 11},
    {12, 12, 12, 12, 12, 12},
    {13, 13, 13, 13, 13, 13},
    {14, 14, 14, 14, 14, 14},
    {15, 15, 15, 15, 15, 15},
    {16, 16, 16, 16, 16, 16},
    {17, 17, 17, 17, 17, 17},
    {18, 18, 18, 18, 18, 18},
    {19, 19, 19, 19, 19, 19},
    {20, 20, 20, 20, 20, 20},
    {21, 21, 21, 21, 21, 21},
    {22, 22, 22, 22, 22, 22},
    {23, 23, 23, 23, 23, 23},
    {24, 24, 24, 24, 24, 24},
    {25, 25, 25, 25, 25, 25},
    {26, 26, 26, 26, 26, 26},
    {27, 27, 27, 27, 27, 27},
    {28, 28, 28, 28, 28, 28},
    {29, 29, 29, 29, 29, 29},
    {30, 30, 30, 30, 30, 30},
    {31, 31, 31, 31, 31, 31}};

static const unsigned int xc_mod_r[][6] = {{0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000, 0x00000000},
    {0x0000ffff, 0x000000ff, 0x0000000f, 0x00000003, 0x00000001, 0x00000001},
    {0x0000ffff, 0x000000ff, 0x0000000f, 0x00000003, 0x00000003, 0x00000003},
    {0x00007fff, 0x0000003f, 0x00000007, 0x00000007, 0x00000007, 0x00000007},
    {0x0000ffff, 0x000000ff, 0x0000000f, 0x0000000f, 0x0000000f, 0x0000000f},
    {0x00007fff, 0x0000001f, 0x0000001f, 0x0000001f, 0x0000001f, 0x0000001f},
    {0x00000fff, 0x0000003f, 0x0000003f, 0x0000003f, 0x0000003f, 0x0000003f},
    {0x00003fff, 0x0000007f, 0x0000007f, 0x0000007f, 0x0000007f, 0x0000007f},
    {0x0000ffff, 0x000000ff, 0x000000ff, 0x000000ff, 0x000000ff, 0x000000ff},
    {0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff, 0x000001ff},
    {0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff, 0x000003ff},
    {0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff, 0x000007ff},
    {0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff, 0x00000fff},
    {0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff, 0x00001fff},
    {0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff, 0x00003fff},
    {0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff, 0x00007fff},
    {0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff, 0x0000ffff},
    {0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff, 0x0001ffff},
    {0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff, 0x0003ffff},
    {0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff, 0x0007ffff},
    {0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff, 0x000fffff},
    {0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff, 0x001fffff},
    {0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff, 0x003fffff},
    {0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff, 0x007fffff},
    {0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff, 0x00ffffff},
    {0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff, 0x01ffffff},
    {0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff, 0x03ffffff},
    {0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff, 0x07ffffff},
    {0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff, 0x0fffffff},
    {0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff, 0x1fffffff},
    {0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff, 0x3fffffff},
    {0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff, 0x7fffffff}};

/*
 * GetPreferredReplicationNode
 * Pick any Datanode from given list, however fetch a preferred node first.
 */
List* GetPreferredReplicationNode(List* relNodes)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
#else

    ListCell* item = NULL;
    int nodeid = -1;

    if (list_length(relNodes) <= 0)
        elog(ERROR, "a list of nodes should have at least one node");

    foreach (item, relNodes) {
        int cnt_nodes;
        for (cnt_nodes = 0; cnt_nodes < num_preferred_data_nodes && nodeid < 0; cnt_nodes++) {
            if (PGXCNodeGetNodeId(preferred_data_node[cnt_nodes], PGXC_NODE_DATANODE) == lfirst_int(item))
                nodeid = lfirst_int(item);
        }
        if (nodeid >= 0)
            break;
    }
    if (nodeid < 0)
        return list_make1_int(linitial_int(relNodes));

    return list_make1_int(nodeid);

#endif
}

bool IsFunctionShippable(Oid foid) {
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * compute_modulo
 * This function performs modulo in an optimized way
 * It optimizes modulo of any positive number by
 * 1,2,3,4,7,8,15,16,31,32,63,64 and so on
 * for the rest of the denominators it uses % operator
 * The optimized algos have been taken from
 * http://www-graphics.stanford.edu/~seander/bithacks.html
 */
int compute_modulo(unsigned int numerator, unsigned int denominator)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else

    unsigned int d;
    unsigned int m;
    unsigned int s;
    unsigned int mask;
    int k;
    unsigned int q, r;

    if (numerator == 0)
        return 0;

    /* Check if denominator is a power of 2 */
    if ((denominator & (denominator - 1)) == 0)
        return numerator & (denominator - 1);

    /* Check if (denominator+1) is a power of 2 */
    d = denominator + 1;
    if ((d & (d - 1)) == 0) {
        /* Which power of 2 is this number */
        s = 0;
        mask = 0x01;
        for (k = 0; k < 32; k++) {
            if ((d & mask) == mask)
                break;
            s++;
            mask = mask << 1;
        }

        m = (numerator & xc_mod_m[s]) + ((numerator >> s) & xc_mod_m[s]);

        for (q = 0, r = 0; m > denominator; q++, r++)
            m = (m >> xc_mod_q[s][q]) + (m & xc_mod_r[s][r]);

        m = (m == denominator) ? 0 : m;

        return m;
    }
    return numerator % denominator;
#endif
}

/*
 * get_node_from_modulo - determine node based on modulo
 *
 * compute_modulo
 */
int get_node_from_modulo(int modulo, List* nodeList)
{
    if (nodeList == NIL || modulo >= list_length(nodeList) || modulo < 0) {
        Assert(0);
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("Modulo value: %d out of range %d.\n", modulo, list_length(nodeList))));
    }
    return list_nth_int(nodeList, modulo);
}

/*
 * GetRelationDistribColumn
 * Return hash column name for relation or NULL if relation is not distributed.
 */
List* GetRelationDistribColumn(RelationLocInfo* locInfo)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
#else
    #error You SHOULD call the same name function in distribute directory.
#endif
}

/*
 * IsDistribColumn
 * Return whether column for relation is used for distribution or not.
 */
bool IsDistribColumn(Oid relid, AttrNumber attNum)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
#else
    RelationLocInfo* locInfo = GetRelationLocInfo(relid);

    /* No locator info, so leave */
    if (!locInfo)
        return false;

    /* No distribution column if relation is not distributed with a key */
    if (!IsRelationDistributedByValue(locInfo))
        return false;

    /* Finally check if attribute is distributed */
    return locInfo->partAttrNum == attNum;
#endif
}

/*
 * IsTypeDistributable
 * Returns whether the data type is distributable using a column value.
 */
bool IsTypeDistributable(Oid col_type)
{
    /*
     * all the distributable types here should be matched with a "bucketXXXX" function
     * in hashfunc.cpp and the functions should be registered in pg_proc.h too.
     */
    if (col_type == INT8OID || col_type == INT1OID || col_type == INT2OID || col_type == INT4OID ||
        col_type == NUMERICOID || col_type == CHAROID || col_type == BPCHAROID || col_type == VARCHAROID ||
        col_type == NVARCHAR2OID || col_type == DATEOID || col_type == TIMEOID || col_type == TIMESTAMPOID ||
        col_type == TIMESTAMPTZOID || col_type == INTERVALOID || col_type == TIMETZOID ||
        col_type == SMALLDATETIMEOID || col_type == TEXTOID || col_type == CLOBOID || col_type == UUIDOID)
        return true;

    // following types are not allowed as distribution column
    // seldom used, non-standard, imprecise or large data types
    if (g_instance.attr.attr_common.support_extended_features && (col_type == OIDOID || col_type == ABSTIMEOID ||
        col_type == RELTIMEOID || col_type == CASHOID || col_type == BYTEAOID || col_type == RAWOID ||
        col_type == BOOLOID || col_type == NAMEOID || col_type == INT2VECTOROID || col_type == OIDVECTOROID ||
        col_type == FLOAT4OID || col_type == FLOAT8OID || col_type == BYTEAWITHOUTORDERWITHEQUALCOLOID))
        return true;

    return false;
}


bool IsTypeDistributableForSlice(Oid colType)
{
    switch (colType) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case NUMERICOID:
            return true;
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case TEXTOID:
            return true;
        case DATEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return true;
        default:
            return false;
    }
}

/*
 * GetRoundRobinNode
 * Update the round robin node for the relation.
 * PGXC - may not want to bother with locking here, we could track
 * these in the session memory context instead...
 */
int GetRoundRobinNode(Oid relid)
{
    int ret_node;
    Relation rel = relation_open(relid, AccessShareLock);

    Assert(rel->rd_locator_info->locatorType == LOCATOR_TYPE_REPLICATED ||
           rel->rd_locator_info->locatorType == LOCATOR_TYPE_RROBIN);

    ret_node = lfirst_int(rel->rd_locator_info->roundRobinNode);

    /* Move round robin indicator to next node */
    if (rel->rd_locator_info->roundRobinNode->next != NULL)
        rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->roundRobinNode->next;
    else
        /* reset to first one */
        rel->rd_locator_info->roundRobinNode = rel->rd_locator_info->nodeList->head;

    relation_close(rel, AccessShareLock);

    return ret_node;
}

/*
 * IsTableDistOnPrimary
 * Does the table distribution list include the primary node?
 */
bool IsTableDistOnPrimary(RelationLocInfo* rel_loc_info)
{
    ListCell* item = NULL;

    if (!OidIsValid(u_sess->pgxc_cxt.primary_data_node) || rel_loc_info == NULL ||
        list_length(rel_loc_info->nodeList) == 0)
        return false;

    foreach (item, rel_loc_info->nodeList) {
        if (PGXCNodeGetNodeId(u_sess->pgxc_cxt.primary_data_node, PGXC_NODE_DATANODE) == lfirst_int(item))
            return true;
    }
    return false;
}

/*
 * IsLocatorInfoEqual
 * Check equality of given locator information
 */
bool IsLocatorInfoEqual(RelationLocInfo* locInfo1, RelationLocInfo* locInfo2)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
#else
    List* nodeList1 = NULL;
    List* nodeList2 = NULL;
    Assert(locInfo1 && locInfo2);

    nodeList1 = locInfo1->nodeList;
    nodeList2 = locInfo2->nodeList;

    /* Same relation? */
    if (locInfo1->relid != locInfo2->relid)
        return false;

    /* Same locator type? */
    if (locInfo1->locatorType != locInfo2->locatorType)
        return false;

    /* Same attribute number? */
    if (locInfo1->partAttrNum != locInfo2->partAttrNum)
        return false;

    /* Same node list? */
    if (list_difference_int(nodeList1, nodeList2) != NIL || list_difference_int(nodeList2, nodeList1) != NIL)
        return false;

    /* Everything is equal */
    return true;
#endif
}

bool IsSliceInfoEqualByOid(Oid tabOid1, Oid tabOid2)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * InitBuckets
 *
 * Set buckets_ptr of RelationLocInfo
 */
void InitBuckets(RelationLocInfo* rel_loc_info, Relation relation)
{
    Relation rel = relation;

    if (rel == NULL) {
        rel = relation_open(rel_loc_info->relid, AccessShareLock);
    }

    if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
        rel_loc_info->buckets_ptr = tryGetBucketMap(NameStr(rel_loc_info->gname),
            NameStr(rel->rd_rel->relname),
            isOtherTempNamespace(rel->rd_rel->relnamespace),
            &rel_loc_info->buckets_cnt);
    } else {
        rel_loc_info->buckets_ptr = BucketMapCacheGetBucketmap(NameStr(rel_loc_info->gname),
                                                               &rel_loc_info->buckets_cnt);
    }

    if (relation == NULL) {
        relation_close(rel, AccessShareLock);
    }
}

/*
 * GetRelationNodes
 *
 * Get list of relation nodes
 * If the table is replicated and we are reading, we can just pick one.
 * If the table is partitioned, we apply partitioning column value, if possible.
 *
 * If the relation is partitioned, partValue will be applied if present
 * (indicating a value appears for partitioning column), otherwise it
 * is ignored.
 *
 * preferredNodes is only used when for replicated tables. If set, it will
 * use one of the nodes specified if the table is replicated on it.
 * This helps optimize for avoiding introducing additional nodes into the
 * transaction.
 *
 * The returned List is a copy, so it should be freed when finished.
 */

ExecNodes* GetRelationNodes(RelationLocInfo* rel_loc_info, Datum* values, const bool* nulls, Oid* attr,
    List* idx_dist_by_col, RelationAccessType accessType, bool needDistribution, bool use_bucketmap)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else

    ExecNodes* exec_nodes = NULL;
    long hashValue;
    int modulo;
    int nodeIndex;

    if (rel_loc_info == NULL)
        return NULL;

    exec_nodes = makeNode(ExecNodes);
    exec_nodes->baselocatortype = rel_loc_info->locatorType;
    exec_nodes->accesstype = accessType;
    exec_nodes->bucketid = INVALID_BUCKET_ID;

    switch (rel_loc_info->locatorType) {
        case LOCATOR_TYPE_REPLICATED:

            /*
             * When intention is to read from replicated table, return all the
             * nodes so that planner can choose one depending upon the rest of
             * the JOIN tree. But while reading with update lock, we need to
             * read from the primary node (if exists) so as to avoid the
             * deadlock.
             * For write access set primary node (if exists).
             */
            exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
            if (accessType == RELATION_ACCESS_UPDATE || accessType == RELATION_ACCESS_INSERT) {
                /* we need to write to all synchronously */

                /*
                 * Write to primary node first, to reduce chance of a deadlock
                 * on replicated tables. If -1, do not use primary copy.
                 */
                if (IsTableDistOnPrimary(rel_loc_info) && exec_nodes->nodeList &&
                    list_length(exec_nodes->nodeList) > 1) /* make sure more than 1 */
                {
                    exec_nodes->primarynodelist =
                        list_make1_int(PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE));
                    exec_nodes->nodeList =
                        list_delete_int(exec_nodes->nodeList, PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE));
                }
            } else if (accessType == RELATION_ACCESS_READ_FOR_UPDATE && IsTableDistOnPrimary(rel_loc_info)) {
                /*
                 * We should ensure row is locked on the primary node to
                 * avoid distributed deadlock if updating the same row
                 * concurrently
                 */
                exec_nodes->nodeList = list_make1_int(PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE));
            }
            break;

        case LOCATOR_TYPE_HASH:
        case LOCATOR_TYPE_MODULO:
            if (!isValueNull) {
                hashValue = compute_hash(typeOfValueForDistCol, valueForDistCol, rel_loc_info->locatorType);
                modulo = compute_modulo(abs(hashValue), list_length(rel_loc_info->nodeList));
                nodeIndex = get_node_from_modulo(modulo, rel_loc_info->nodeList);
                exec_nodes->nodeList = list_make1_int(nodeIndex);
                exec_nodes->bucketid = compute_modulo(abs((int)hashValue), BUCKETDATALEN);
            } else {
                if (accessType == RELATION_ACCESS_INSERT)
                    /* Insert NULL to first node */
                    exec_nodes->nodeList = list_make1_int(linitial_int(rel_loc_info->nodeList));
                else
                    exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
            }
            break;

        case LOCATOR_TYPE_RROBIN:
            /*
             * round robin, get next one in case of insert. If not insert, all
             * node needed
             */
            if (accessType == RELATION_ACCESS_INSERT)
                exec_nodes->nodeList = list_make1_int(GetRoundRobinNode(rel_loc_info->relid));
            else
                exec_nodes->nodeList = list_copy(rel_loc_info->nodeList);
            break;

        default:
            ereport(ERROR, (errmsg("Error: no such supported locator type: %c\n", rel_loc_info->locatorType)));
            break;
    }

    return exec_nodes;

#endif
}

/*
 * GetRelationNodesByQuals
 * A wrapper around GetRelationNodes to reduce the node list by looking at the
 * quals. varno is assumed to be the varno of reloid inside the quals. No check
 * is made to see if that's correct.
 */
ExecNodes* GetRelationNodesByQuals(void* query_arg, Oid reloid, Index varno, Node* quals, RelationAccessType relaccess,
    ParamListInfo boundParams, bool useDynamicReduce)
{
    Query* query = (Query*)query_arg;
    RelationLocInfo* rel_loc_info = GetRelationLocInfo(reloid);
    Expr* distcol_expr = NULL;
    Expr* distcol_expr_original = NULL;
    ExecNodes* exec_nodes = NULL;
    ListCell* cell = NULL;
    AttrNumber attnum;

    Datum* distcol_value = NULL;
    bool* distcol_isnull = NULL;
    Oid* distcol_type = NULL;
    List* idx_dist = NULL;
    /* datanodes reduction where there are params */
    List* distcol_expr_list = NULL;
    List* bucket_expr_list = NULL;
    bool needDynamicReduce = false;

    int len = 0;
    int i = 0;

    if (rel_loc_info == NULL)
        return NULL;

    /*
     * If the table distributed by value, check if we can reduce the Datanodes
     * by looking at the qualifiers for this relation
     */
    if (IsRelationDistributedByValue(rel_loc_info)) {
        len = list_length(rel_loc_info->partAttrNum);

        distcol_value = (Datum*)palloc(len * sizeof(Datum));
        distcol_isnull = (bool*)palloc(len * sizeof(bool));
        distcol_type = (Oid*)palloc(len * sizeof(Oid));

        foreach (cell, rel_loc_info->partAttrNum) {
            attnum = lfirst_int(cell);

            Oid disttype = get_atttype(reloid, attnum);
            int32 disttypmod = get_atttypmod(reloid, attnum);

            distcol_expr_original = distcol_expr = pgxc_find_distcol_expr(query, varno, attnum, quals);

            /*
             * If the type of expression used to find the Datanode, is not same as
             * the distribution column type, try casting it. This is same as what
             * will happen in case of inserting that type of expression value as the
             * distribution column value.
             */
            if (distcol_expr != NULL) {
                Oid exprtype = exprType((Node*)distcol_expr);
                /*
                 * To variable length data type, here need not consider it's typmode because hash value of the same
                 * value must be the same.
                 *
                 * If consider typmode that may lead to ERROR rather than return 0 row result.
                 *
                 * For example:
                 *		create table test(a numeric(19, 18));
                 *		select * from test where 10 = a;
                 *
                 * '10' can not be converted to numeric(19, 18), that will lead to ERROR.
                 * So we add this branch to handle this problem.
                 *
                 */
                if (disttype == NUMERICOID || disttype == BPCHAROID || disttype == VARCHAROID) {
                    if (can_coerce_type(1, &exprtype, &disttype, COERCION_ASSIGNMENT)) {
                        distcol_expr = (Expr*)coerce_type(NULL,
                            (Node*)distcol_expr,
                            exprtype,
                            disttype,
                            disttypmod,
                            COERCION_ASSIGNMENT,
                            COERCE_IMPLICIT_CAST,
                            -1);
                    } else {
                        distcol_expr = NULL;
                    }
                } else {
                    distcol_expr = (Expr*)coerce_to_target_type(NULL,
                        (Node*)distcol_expr,
                        exprtype,
                        disttype,
                        disttypmod,
                        COERCION_ASSIGNMENT,
                        COERCE_IMPLICIT_CAST,
                        -1);
                }

                /*
                 * PGXC_FQS: We should set the bound parameters here, but we don't have
                 * PlannerInfo struct and we don't handle them right now.
                 * Even if constant expression mutator changes the expression, it will
                 * only simplify it, keeping the semantics same
                 */
                if (boundParams) {
                    distcol_expr = (Expr*)eval_const_expressions_params(NULL, (Node*)distcol_expr, boundParams);
                } else {
                    distcol_expr = (Expr*)eval_const_expressions(NULL, (Node*)distcol_expr);
                }
            }

            if (distcol_expr != NULL) {
                /*
                 * If all distcol_expr are const, no need to do node reduction, which is the same as before.
                 * If there is extern params with the form '$n', we can do dynamic datanode reduction.
                 */
                if (IsA(distcol_expr, Const)) {
                    /* If all const for now, do the same as before; else there is no need to do these */
                    if (!needDynamicReduce) {
                        Const* const_expr = (Const*)distcol_expr;
                        distcol_value[i] = const_expr->constvalue;
                        distcol_isnull[i] = const_expr->constisnull;
                        distcol_type[i] = const_expr->consttype;
                        idx_dist = lappend_int(idx_dist, i);
                        i++;
                    }
                }
                /* node reduction when there are param but no boundParams, i.e. generate a generic plan */
                else if (useDynamicReduce && IsA(distcol_expr_original, Param) && boundParams == NULL) {
                    Param* param_expr = (Param*)distcol_expr_original;
                    /* node reduction only for PARAM_EXTERN params with the form '$n' */
                    if (param_expr->paramkind != PARAM_EXTERN) {
                        break;
                    }
                    needDynamicReduce = true;
                } else {
                    if (IsA(distcol_expr_original, Param) && boundParams == NULL) {
                        Param* param_expr = (Param*)distcol_expr_original;
                        /* node reduction only for PARAM_EXTERN params with the form '$n' */
                        if (param_expr->paramkind != PARAM_EXTERN) {
                            break;
                        }
                    }
                    break;
                }
                distcol_expr_list = lappend(distcol_expr_list, copyObject(distcol_expr));
                bucket_expr_list = lappend(bucket_expr_list, copyObject(distcol_expr));
            } else {
                break;
            }
        }
        /* If needDynamicReduce, we also do cleaning like before */
        if (cell != NULL || needDynamicReduce) {
            for (int j = 0; j < len; j++) {
                distcol_value[j] = 0;
                distcol_isnull[j] = true;
                distcol_type[j] = InvalidOid;
            }
            list_free(idx_dist);
            idx_dist = NULL;
        }
        /* mark that datanodes can be reduced */
        if (cell != NULL && needDynamicReduce) {
            needDynamicReduce = false;
        }
    }

    exec_nodes = GetRelationNodes(rel_loc_info, distcol_value, distcol_isnull, distcol_type, idx_dist, relaccess);
    /*
     * If 'needDynamicReduce' and 'canReduce', we will use 'distcol_expr_list' to get the
     * correct connection during execution.
     * There are two cases: pure param; param and const mixed.
     * Also, we will discard and release the node lists we just got,
     * and append the relation oid to exec nodes.
     */
    if (needDynamicReduce && exec_nodes != NULL) {
        exec_nodes->en_expr = distcol_expr_list;
        list_free(exec_nodes->primarynodelist);
        exec_nodes->primarynodelist = NIL;
        list_free(exec_nodes->nodeList);
        exec_nodes->nodeList = NIL;
        exec_nodes->en_relid = rel_loc_info->relid;
        exec_nodes->nodelist_is_nil = true;
    }

    if (exec_nodes != NULL && list_length(rel_loc_info->partAttrNum) == list_length(bucket_expr_list)) {
        exec_nodes->bucketexpr = bucket_expr_list;
        exec_nodes->bucketrelid = rel_loc_info->relid;
    }

    return exec_nodes;
}

void PruningDatanode(ExecNodes* execNodes, ParamListInfo boundParams)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void ConstructSliceBoundary(ExecNodes* en)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * GetLocatorType
 * Returns the locator type of the table.
 */
char GetLocatorType(Oid relid)
{
    char ret = LOCATOR_TYPE_NONE;

    if (relid == InvalidOid)
        return ret;

    RelationLocInfo* locInfo = GetRelationLocInfo(relid);

    if (locInfo != NULL) {
        ret = locInfo->locatorType;
        FreeRelationLocInfo(locInfo);
    }
#ifdef STREAMPLAN
    else {
        if (is_sys_table(relid))
            ret = LOCATOR_TYPE_REPLICATED;
    }
#endif

    return ret;
}

/*
 * GetAllDataNodes
 * Return a list of all Datanodes.
 * We assume all tables use all nodes in the prototype, so just return a list
 * from first one.
 */
List* GetAllDataNodes(void)
{
    if (IS_STREAM) {
        /* single node only has one node */
        return lappend_int(NIL, 0);
    } else {
        return NIL;
    }
}

/*
 * GetNodeGroupNodeList
 *		Look at the data cached for handles and return datanode list
 *		Reload pooler when find node failed.
 */
List* GetNodeGroupNodeList(Oid* members, int nmembers)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
}

List* SearchSliceEntryCopy(char parttype, Oid relid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
}

HeapTuple SearchTableEntryCopy(char parttype, Oid relid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}


/*
 * GetAllCoordNodes
 * Return a list of all Coordinators
 * This is used to send DDL to all nodes and to clean up pooler connections.
 * Do not put in the list the local Coordinator where this function is launched.
 */
List* GetAllCoordNodes(void)
{
    int i;
    List* nodeList = NIL;

    for (i = 0; i < u_sess->pgxc_cxt.NumCoords; i++) {
        /*
         * Do not put in list the Coordinator we are on,
         * it doesn't make sense to connect to the local Coordinator.
         */
        if (i != u_sess->pgxc_cxt.PGXCNodeId - 1)
            nodeList = lappend_int(nodeList, i);
    }

    return nodeList;
}

/*
 * @Description: temp table does not do redistribute when cluster resizing, so it will
 * still on old node group after redistributing. when the old node group is dropped,
 * so GetBucketMap will encounter "cache lookup failed" error. This function is for
 * giving human readable messages..
 * @in groupname - the node group to be used.
 * @in relname - the relation name which the bucketmap is for.
 * @in isOtherTempNamespace -- If it's our own temp table or other session's temp table,
                                to determin report an error or warning
 * @return: uint2* - the bucket map of the giving relation.
 */
static uint2* tryGetBucketMap(const char* groupname, char* relname, bool isOtherTempNamespace, int *bucketlen)
{
    Relation rel;
    HeapTuple htup;
    int len;
    rel = heap_open(PgxcGroupRelationId, ShareLock);
    len = BUCKETDATALEN * sizeof(uint2);

    htup = SearchSysCache1(PGXCGROUPNAME, CStringGetDatum(groupname));
    if (!HeapTupleIsValid(htup)) {
        if (!isOtherTempNamespace) {
            heap_close(rel, ShareLock);
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("The relation %s is invalid because of cluster resize, "
                           "please quit current session and it will be "
                           "automaticly dropped.",
                        relname)));
        } else {
            heap_close(rel, ShareLock);
            return NULL;
        }
    }

    ReleaseSysCache(htup);
    heap_close(rel, ShareLock);

    return (uint2*)BucketMapCacheGetBucketmap(groupname, bucketlen);
}

/*
 * RelationBuildLocator
 * Build locator information associated with the specified relation.
 */
void RelationBuildLocator(Relation rel)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    Relation pcrel;
    ScanKeyData skey;
    SysScanDesc pcscan;
    HeapTuple htup;
    MemoryContext oldContext;
    RelationLocInfo* relationLocInfo = NULL;
    int j;
    Form_pgxc_class pgxc_class;

    ScanKeyInit(
        &skey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));

    pcrel = heap_open(PgxcClassRelationId, AccessShareLock);
    pcscan = systable_beginscan(pcrel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &skey);
    htup = systable_getnext(pcscan);
    if (!HeapTupleIsValid(htup)) {
        /* Assume local relation only */
        rel->rd_locator_info = NULL;
        systable_endscan(pcscan);
        heap_close(pcrel, AccessShareLock);
        return;
    }

    pgxc_class = (Form_pgxc_class)GETSTRUCT(htup);

    oldContext = MemoryContextSwitchTo(CacheMemoryContext);

    relationLocInfo = (RelationLocInfo*)palloc(sizeof(RelationLocInfo));
    rel->rd_locator_info = relationLocInfo;

    relationLocInfo->relid = RelationGetRelid(rel);
    relationLocInfo->locatorType = pgxc_class->pclocatortype;

    relationLocInfo->partAttrNum = pgxc_class->pcattnum;
    relationLocInfo->nodeList = NIL;

    for (j = 0; j < pgxc_class->nodeoids.dim1; j++)
        relationLocInfo->nodeList = lappend_int(
            relationLocInfo->nodeList, PGXCNodeGetNodeId(pgxc_class->nodeoids.values[j], PGXC_NODE_DATANODE));

    /*
     * If the locator type is round robin, we set a node to
     * use next time. In addition, if it is replicated,
     * we choose a node to use for balancing reads.
     */
    if (relationLocInfo->locatorType == LOCATOR_TYPE_RROBIN ||
        relationLocInfo->locatorType == LOCATOR_TYPE_REPLICATED) {
        int offset;
        /*
         * pick a random one to start with,
         * since each process will do this independently
         */
        offset = compute_modulo(abs(rand()), list_length(relationLocInfo->nodeList));

        srand(time(NULL));
        relationLocInfo->roundRobinNode = relationLocInfo->nodeList->head; /* initialize */
        for (j = 0; j < offset && relationLocInfo->roundRobinNode->next != NULL; j++)
            relationLocInfo->roundRobinNode = relationLocInfo->roundRobinNode->next;
    }

    systable_endscan(pcscan);
    heap_close(pcrel, AccessShareLock);

    MemoryContextSwitchTo(oldContext);

#endif
}

/*
 * GetLocatorRelationInfo
 * Returns the locator information for relation,
 * in a copy of the RelationLocatorInfo struct in relcache
 */
RelationLocInfo* GetRelationLocInfo(Oid relid)
{
    RelationLocInfo* ret_loc_info = NULL;
    Relation rel = relation_open(relid, AccessShareLock);

    /* Relation needs to be valid */
    Assert(rel->rd_isvalid);

    if (rel->rd_locator_info)
        ret_loc_info = CopyRelationLocInfo(rel->rd_locator_info);

    /*
     * NodeGroup --dynamic computation elastic
     *
     * When new datanodes added into cluster, we need invoke SQL function pgxc_pool_reload()
     * to do handler update so that optimizer can do correct planning based on new cluster
     * scale, however pgxc_pool_reload() won't invalidate rel_loc in relcache, so we have to
     * rebuild rel_loc here anyway.
     *
     * It may not be the most efficient solution, the idea way is to invalidate all relcache
     * in pgxc_pool_reload(), -- will improve later.
     */
    if (ret_loc_info && (RELKIND_FOREIGN_TABLE == rel->rd_rel->relkind 
        || RELKIND_STREAM == rel->rd_rel->relkind)) {
        ExecNodes* exec_nodes = RelidGetExecNodes(relid, false);

        if (NIL != exec_nodes->nodeList) {
            ret_loc_info->nodeList = list_copy(exec_nodes->nodeList);
            list_free(exec_nodes->nodeList);
            bms_free(exec_nodes->distribution.bms_data_nodeids);
        }

        pfree(exec_nodes);
    }

    relation_close(rel, AccessShareLock);

    return ret_loc_info;
}

/*
 * GetLocatorRelationInfoDN
 * Returns the locator information for hashbucket relation
 * only relid and partAttrNum is valid, other fields are dummy
 */
RelationLocInfo* GetRelationLocInfoDN(Oid reloid)
{
    RelationLocInfo* ret_loc_info = NULL;
    Relation relation = heap_open(reloid, NoLock);

    if (!REALTION_BUCKETKEY_VALID(relation)) {
        heap_close(relation, NoLock);
        return NULL;
    }

    ret_loc_info = (RelationLocInfo*)palloc0(sizeof(RelationLocInfo));
    ret_loc_info->relid = reloid;
    ret_loc_info->locatorType = LOCATOR_TYPE_HASH;
    ret_loc_info->nodeList = lappend_int(ret_loc_info->nodeList, u_sess->pgxc_cxt.PGXCNodeId);

    int2vector* colids = relation->rd_bucketkey->bucketKey;

    for (int i = 0; i< colids->dim1; i++) {
        ret_loc_info->partAttrNum = lappend_int(ret_loc_info->partAttrNum, colids->values[i]);
    }

    heap_close(relation, NoLock);

    return ret_loc_info;
}

/*
 * CopyRelationLocInfo
 * Copy the RelationLocInfo struct
 */
RelationLocInfo* CopyRelationLocInfo(RelationLocInfo* srcInfo)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
    RelationLocInfo* destInfo = NULL;

    Assert(srcInfo);
    destInfo = (RelationLocInfo*)palloc0(sizeof(RelationLocInfo));

    destInfo->relid = srcInfo->relid;
    destInfo->locatorType = srcInfo->locatorType;
    destInfo->partAttrNum = srcInfo->partAttrNum;
    if (srcInfo->nodeList)
        destInfo->nodeList = list_copy(srcInfo->nodeList);

    /* Note: for roundrobin, we use the relcache entry */
    return destInfo;
#endif
}

/*
 * FreeRelationLocInfo
 * Free RelationLocInfo struct
 */
void FreeRelationLocInfo(RelationLocInfo* relationLocInfo)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    if (relationLocInfo)
        pfree(relationLocInfo);

#endif
}

Distribution* NewDistribution()
{
    Distribution* distribution = (Distribution*)palloc0(sizeof(Distribution));

    if (distribution == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Could not alloc new memory.")));
    }

    return distribution;
}

void DestroyDistribution(Distribution* distribution)
{
    if (distribution != NULL) {
        bms_free(distribution->bms_data_nodeids);
        pfree(distribution);
    }
}

/*
 * FreeExecNodes
 * Free the contents of the ExecNodes expression
 */
void FreeExecNodes(ExecNodes** exec_nodes)
{
    ExecNodes* tmp_en = *exec_nodes;

    /* Nothing to do */
    if (tmp_en == NULL)
        return;
    list_free(tmp_en->primarynodelist);
    list_free(tmp_en->nodeList);
    pfree(tmp_en);
    *exec_nodes = NULL;
}

/*
 * pgxc_find_distcol_expr
 * Search through the quals provided and find out an expression which will give
 * us value of distribution column if exists in the quals. Say for a table
 * tab1 (val int, val2 int) distributed by hash(val), a query "SELECT * FROM
 * tab1 WHERE val = fn(x, y, z) and val2 = 3", fn(x,y,z) is the expression which
 * decides the distribution column value in the rows qualified by this query.
 * Hence return fn(x, y, z). But for a query "SELECT * FROM tab1 WHERE val =
 * fn(x, y, z) || val2 = 3", there is no expression which decides the values
 * distribution column val can take in the qualified rows. So, in such cases
 * this function returns NULL.
 */
Expr* pgxc_find_distcol_expr(void* query_arg, Index varno, AttrNumber attrNum, Node* quals)
{
    List* lquals = NULL;
    ListCell* qual_cell = NULL;
    Query* query = (Query*)query_arg;

    /* If no quals, no distribution column expression */
    if (quals == NULL)
        return NULL;

    /* Convert the qualification into List if it's not already so */
    if (!IsA(quals, List))
        lquals = make_ands_implicit((Expr*)quals);
    else
        lquals = (List*)quals;

    /*
     * For every ANDed expression, check if that expression is of the form
     * <distribution_col> = <expr>. If so return expr.
     */
    foreach (qual_cell, lquals) {
        Expr* qual_expr = (Expr*)lfirst(qual_cell);
        OpExpr* op = NULL;
        Expr* lexpr = NULL;
        Expr* rexpr = NULL;
        Var* var_expr = NULL;
        Expr* distcol_expr = NULL;

        /* if it is 'is null' */
        if (IsA(qual_expr, NullTest)) {
            NullTest* nt = (NullTest*)qual_expr;
            if (nt->nulltesttype == IS_NULL && IsA(nt->arg, Var)) {
                var_expr = (Var*)(nt->arg);
                /*
                 * If Var found is not the distribution column of required relation,
                 * check next qual
                 */
                if (var_expr->varno != varno || var_expr->varattno != attrNum)
                    continue;

                /* Make NullConst */
                distcol_expr = (Expr*)makeNullConst(var_expr->vartype, var_expr->vartypmod, var_expr->varcollid);
                /* Found the distribution column expression return it */
                return distcol_expr;
            }

            continue;
        }

        if (!IsA(qual_expr, OpExpr))
            continue;
        op = (OpExpr*)qual_expr;
        /* If not a binary operator, it can not be '='. */
        if (list_length(op->args) != 2)
            continue;

        lexpr = (Expr*)linitial(op->args);
        rexpr = (Expr*)lsecond(op->args);

        /*
         * If either of the operands is a RelabelType, extract the Var in the RelabelType.
         * A RelabelType represents a "dummy" type coercion between two binary compatible datatypes.
         * If we do not handle these then our optimization does not work in case of varchar
         * For example if col is of type varchar and is the dist key then
         * select * from vc_tab where col = 'abcdefghijklmnopqrstuvwxyz';
         * should be shipped to one of the nodes only
         */
        if (IsA(lexpr, RelabelType))
            lexpr = ((RelabelType*)lexpr)->arg;
        if (IsA(rexpr, RelabelType))
            rexpr = ((RelabelType*)rexpr)->arg;

        /*
         * If either of the operands is a Var expression, assume the other
         * one is distribution column expression. If none is Var check next
         * qual.
         */
        if (IsA(lexpr, Var)) {
            var_expr = (Var*)lexpr;
            distcol_expr = rexpr;
        } else if (IsA(rexpr, Var)) {
            var_expr = (Var*)rexpr;
            distcol_expr = lexpr;
        } else
            continue;

        Var baserel_var = *var_expr;

        /* currently we only handle this level vars */
        if (var_expr->varlevelsup == 0) {
            /* find the base relation for a join var */
            (void)get_real_rte_varno_attno(query, &(baserel_var.varno), &(baserel_var.varattno));

            /* use baserel_var now */
            var_expr = &baserel_var;
        }

        /*
         * If Var found is not the distribution column of required relation,
         * check next qual
         */
        if (var_expr->varno != varno || var_expr->varattno != attrNum)
            continue;

        /*
         * If the operator is not an assignment operator, check next
         * constraint. An operator is an assignment operator if it's
         * mergejoinable or hashjoinable. Beware that not every assignment
         * operator is mergejoinable or hashjoinable, so we might leave some
         * oportunity. But then we have to rely on the opname which may not
         * be something we know to be equality operator as well.
         */
        if (!op_mergejoinable(op->opno, exprType((Node*)lexpr)) && !op_hashjoinable(op->opno, exprType((Node*)lexpr)))
            continue;
        /* Found the distribution column expression return it */
        return distcol_expr;
    }
    /* Exhausted all quals, but no distribution column expression */
    return NULL;
}

/*
 * @Description : check whether the OpExpr contain value of distribution column.
 * Say for a table tab1 (val int, val2 int) distributed by hash(val), a query
 * "SELECT * FROM tab1 WHERE val = fn(x, y, z)", fn(x,y,z) is the expression
 * which decides the distribute column value in the rows qualified by this query.
 * Hence return fn(x, y, z).
 *
 * @in varno : index of the var_expr's relation in the range.
 * @in attrNum :  attribute number of this var_expr.
 * @in opexpr : the opexpr in function statement need be check.
 * @return : true when we get the right opexpr.
 */
Expr* pgxc_check_distcol_opexpr(Index varno, AttrNumber attrNum, OpExpr* opexpr)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: Get min dn number from pgxc_group.
 * @return - return min dn num.
 */
int GetMinDnNum()
{
    int dataNodeNum = u_sess->pgxc_cxt.NumDataNodes;
    Relation rel = NULL;
    TableScanDesc scan;
    HeapTuple tuple;
    bool isNull = false;

    rel = heap_open(PgxcGroupRelationId, ShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Datum group_members_datum = heap_getattr(tuple, Anum_pgxc_group_members, RelationGetDescr(rel), &isNull);
        /* Should not happend */
        if (isNull) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Can't get group member")));
        }
        oidvector* group_members = (oidvector*)PG_DETOAST_DATUM(group_members_datum);

        if (group_members->dim1 < dataNodeNum) {
            dataNodeNum = group_members->dim1;
        }
        if (group_members != (oidvector*)DatumGetPointer(group_members_datum)) {
            pfree_ext(group_members);
        }
    }

    tableam_scan_end(scan);
    heap_close(rel, ShareLock);

    return dataNodeNum;
}

#ifdef USE_SPQ
bool IsSpqTypeDistributable(Oid col_type)
{
    if (col_type == INT8OID || col_type == INT1OID || col_type == INT2OID || col_type == INT4OID ||
        col_type == NUMERICOID || col_type == CHAROID || col_type == BPCHAROID || col_type == VARCHAROID ||
        col_type == NVARCHAR2OID || col_type == DATEOID || col_type == TIMEOID || col_type == TIMESTAMPOID ||
        col_type == TIMESTAMPTZOID || col_type == INTERVALOID || col_type == TIMETZOID ||
        col_type == SMALLDATETIMEOID || col_type == TEXTOID || col_type == CLOBOID || col_type == UUIDOID)
        return true;
    /* SPQ support extended data types*/
    if (col_type == OIDOID || col_type == ABSTIMEOID ||
        col_type == RELTIMEOID || col_type == CASHOID || col_type == BYTEAOID || col_type == RAWOID ||
        col_type == BOOLOID || col_type == NAMEOID || col_type == INT2VECTOROID || col_type == OIDVECTOROID ||
        col_type == FLOAT4OID || col_type == FLOAT8OID || col_type == BYTEAWITHOUTORDERWITHEQUALCOLOID)
        return true;

    return false;
}
#endif
