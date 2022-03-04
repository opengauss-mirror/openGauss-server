/* -------------------------------------------------------------------------
 *
 * groupmgr.c
 *	  Routines to support manipulation of the pgxc_group catalog
 *	  This includes support for DDL on objects NODE GROUP
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "catalog/pgxc_class.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-fe.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "postmaster/autovacuum.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/snapmgr.h"
#include "utils/acl.h"
#include "utils/elog.h"
#include "access/xact.h"
#include "access/sysattr.h"
#include "utils/fmgroids.h"
#include "catalog/pg_authid.h"
#include "pgxc/execRemote.h"
#include "commands/user.h"
#include "commands/sequence.h"
#include "pgxc/pgxcnode.h"
#include "tcop/utility.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/snapmgr.h"
#include "utils/knl_relcache.h"

#define CHAR_BUF_SIZE 512
#define BUCKET_MAP_SIZE 32

#pragma GCC diagnostic ignored "-Wunused-function"

/*
 * The element data structure of bucketmap cache, where store the bucketmap that
 * palloc-ed form t_thrd.top_mem_cxt, it is created on its first being used
 */
typedef struct BucketMapCache {
    /* Search key of bucketmap cache */
    Oid groupoid;
    ItemPointerData ctid;
    char* groupname;

    /* bucketmap content, palloc()-ed form top memory context */
    uint2* bucketmap;
    int    bucketcnt;
} NodeGroupBucketMap;

static void BucketMapCacheAddEntry(Oid groupoid, Datum groupanme_datum, Datum bucketmap_datum, ItemPointer ctid);
static void BucketMapCacheRemoveEntry(Oid groupoid);

#define BUCKETMAP_MODE_DEFAULT 0
#define BUCKETMAP_MODE_REMAP 1

static oidvector* get_group_member_ref(HeapTuple tup, bool* need_free);

static void generateConsistentHashBucketmap(CreateGroupStmt* stmt, oidvector* nodes_array, Relation pgxc_group_rel,
    HeapTuple tuple, uint2* bucket_ptr, int bucketmap_mode = BUCKETMAP_MODE_DEFAULT);

static void contractBucketMap(Relation rel, HeapTuple old_group, oidvector* new_nodes, uint2* bucket_ptr);
static void getBucketsFromBucketList(List* bucketlist, int member_count, uint2* bucket_ptr);
static void GetTablesByDatabase(const char* database_name, char* query, size_t size);
void PgxcCheckOid(Oid noid, char* node_name);
void PgxcOpenGroupRelation(Relation* pgxc_group_rel);

static void set_current_installation_nodegroup(const char* group_name)
{
    /* Local cache value stored in top memory context */
    MemoryContext oldcontext =
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    t_thrd.pgxc_cxt.current_installation_nodegroup = pstrdup(group_name);
    MemoryContextSwitchTo(oldcontext);
}

static void set_current_redistribution_nodegroup(const char* group_name)
{
    /* Local cache value stored in top memory context */
    MemoryContext oldcontext =
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    t_thrd.pgxc_cxt.current_redistribution_nodegroup = pstrdup(group_name);
    MemoryContextSwitchTo(oldcontext);
}

static int nodeinfo_cmp(const void* a, const void* b)
{
    const nodeinfo* node1 = (const nodeinfo*)a;
    const nodeinfo* node2 = (const nodeinfo*)b;

    // deleted node
    if (node1->deleted && !node2->deleted) {
        return 1;
    }
    if (!node1->deleted && node2->deleted) {
        return -1;
    }
    // bucket count
    if (node1->old_buckets_num > node2->old_buckets_num) {
        return 1;
    }
    if (node1->old_buckets_num < node2->old_buckets_num) {
        return -1;
    }
    return 0;
}

static void getBucketsFromBucketList(List* bucketlist, int member_count, uint2* bucket_ptr)
{
    int bucket_num = list_length(bucketlist);
    int* buckets_count = NULL;
    ListCell* cell = NULL;
    int i = 0;
    int max_buckets;
    int min_buckets;

    if (bucket_num != BUCKETDATALEN)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("Get buckets failed.reason:the buckets number(%d) is not correct(%d).",
                    bucket_num,
                    BUCKETDATALEN)));

    buckets_count = (int*)palloc0(member_count * sizeof(int));

    foreach (cell, bucketlist) {
        uint2 b = (uint2)intVal(lfirst(cell));

        if (b >= member_count)
            ereport(
                ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("Bucket id(%d:%d) out of range.", i, b)));

        bucket_ptr[i++] = b;
        buckets_count[b]++;
    }
    Assert(i == BUCKETDATALEN);

    max_buckets = min_buckets = buckets_count[0];
    for (i = 0; i < member_count; i++) {
        if (max_buckets < buckets_count[i]) {
            max_buckets = buckets_count[i];
        }
        if (min_buckets > buckets_count[i]) {
            min_buckets = buckets_count[i];
        }
        if (buckets_count[i] == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Node(%d) has no buckets on it.", i)));
        }
    }

    if (max_buckets > min_buckets + 1)
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg(
                    "Buckets distribution is not even(max_buckets: %d, min_buckets: %d).", max_buckets, min_buckets)));

    pfree(buckets_count);
}

/*
 * input:
 * 	rel, old_group, new_nodes
 * output:
 * 	bucket_ptr
 */
static void contractBucketMap(Relation rel, HeapTuple old_group, oidvector* new_nodes, uint2* bucket_ptr)
{
    bool isNull = false;
    int i, j, k;
    oidvector* old_nodes = NULL;
    text* old_bucket_str = NULL;
    uint2* old_bucket_ptr = NULL;
    int bktlen = 0;
    nodeinfo* node_array = NULL;
    bool need_free = false;

    old_nodes = get_group_member_ref(old_group, &need_free);

    if (old_nodes->dim1 <= new_nodes->dim1)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("new node group contains more nodes than old group.")));

    old_bucket_ptr = (uint2*)palloc0(BUCKETDATALEN * sizeof(uint2));
    old_bucket_str = (text*)heap_getattr(old_group, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);
    if (isNull)
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("can't get old group buckets.")));
    text_to_bktmap(old_bucket_str, old_bucket_ptr, &bktlen);

    node_array = (nodeinfo*)palloc0(old_nodes->dim1 * sizeof(nodeinfo));

    for (i = 0; i < old_nodes->dim1; i++) {
        node_array[i].node_id = old_nodes->values[i];
        node_array[i].old_node_index = i;
        node_array[i].old_buckets_num = 0;
#ifdef USE_ASSERT_CHECKING
        node_array[i].new_buckets_num = 0;
#endif
    }

    /* caculate buckets number for each node. */
    for (i = 0; i < BUCKETDATALEN; i++)
        node_array[old_bucket_ptr[i]].old_buckets_num++;

    // as our node oids have been sorted. we can just scan the old nodes once.
    j = 0;
    for (i = 0; i < new_nodes->dim1; i++) {
        Oid nodeid = new_nodes->values[i];
        bool found = false;
        while (j < old_nodes->dim1) {
            if (nodeid == node_array[j].node_id) {
                node_array[j].deleted = false;
                node_array[j].new_node_index = i;
                found = true;
                j++;
                break;
            } else {
                node_array[j].deleted = true;
                j++;
            }
        }
        if (!found)
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("new node group contains nodes not in old group.")));
    }

    while (j < old_nodes->dim1)
        node_array[j++].deleted = true;

    // sort the node_array so that the remained nodes and nodes with less buckets will be in the head of array.
    qsort((void*)node_array, old_nodes->dim1, sizeof(nodeinfo), nodeinfo_cmp);

#ifdef USE_ASSERT_CHECKING
    for (i = 0; i < new_nodes->dim1; i++)
        Assert(!node_array[i].deleted);

    for (i = new_nodes->dim1; i < old_nodes->dim1; i++)
        Assert(node_array[i].deleted);

    // our bucket map should always be even.
    Assert(node_array[0].old_buckets_num + 1 >= node_array[new_nodes->dim1 - 1].old_buckets_num);
#endif

    k = 0;
    for (i = 0; i < BUCKETDATALEN; i++) {
        for (j = 0; j < new_nodes->dim1; j++) {
            if (old_bucket_ptr[i] == node_array[j].old_node_index)
                break;
        }
        if (j == new_nodes->dim1) { // this means this bucket is on deleted nodes
            // distribute buckets on deleted nodes to remained nodes in round robin way.
            // after the qsort before. nodes with less buckets have a priority of distribution.
            bucket_ptr[i] = node_array[k].new_node_index;
#ifdef USE_ASSERT_CHECKING
            node_array[k].new_buckets_num++;
#endif
            k = (k + 1) % (new_nodes->dim1);
        } else { // this means this bucket is on remained nodes.
            // update to new node index if node index has changed in new group.
            // this may happen when we delete nodes which are not in the tail of the group node list.
            bucket_ptr[i] = node_array[j].new_node_index;
#ifdef USE_ASSERT_CHECKING
            node_array[j].new_buckets_num++;
#endif
        }
    }

#ifdef USE_ASSERT_CHECKING
    // our bucket map should always be even.
    k = BUCKETDATALEN / (new_nodes->dim1);
    for (i = 0; i < new_nodes->dim1; i++) {
        Assert(node_array[i].new_buckets_num == k || node_array[i].new_buckets_num == k + 1);
    }
#endif

    /* Free point if old_nodes is palloc during PG_DETOAST */
    if (need_free)
        pfree_ext(old_nodes);
}

/*
 * get_group_kind - Get group_kind filed from heap tuple.
 *
 */
static char get_group_kind(HeapTuple tup)
{
    bool isNull = true;
    bool is_null = true;
    Datum group_datum;

    group_datum = SysCacheGetAttr(PGXCGROUPOID, tup, Anum_pgxc_group_is_installation, &isNull);

    if (!isNull && DatumGetBool(group_datum)) {
        return PGXC_GROUPKIND_INSTALLATION;
    }

    group_datum = SysCacheGetAttr(PGXCGROUPOID, tup, Anum_pgxc_group_kind, &is_null);

    if (is_null)
        return PGXC_GROUPKIND_NODEGROUP;

    if (!isNull && DatumGetChar(group_datum) == PGXC_GROUPKIND_INSTALLATION)
        return PGXC_GROUPKIND_NODEGROUP;

    return DatumGetChar(group_datum);
}

/*
 * get_bucket_string - Get bucket string from bucket array.
 *
 */
static char* get_bucket_string(uint2* bucket_ptr)
{
    int i;
    errno_t rc = 0;
    char* bucket_str = NULL;
    char one_bucket_str[CHAR_BUF_SIZE] = {0};
    /* build bucket str */
    bucket_str = (char*)palloc0(BUCKETSTRLEN);
    rc = snprintf_truncated_s(bucket_str, BUCKETSTRLEN, "%d", bucket_ptr[0]);
    securec_check_ss(rc, "\0", "\0");
    for (i = 1; i < BUCKETDATALEN; i++) {
        rc = snprintf_s(one_bucket_str, sizeof(one_bucket_str), CHAR_BUF_SIZE - 1, ",%d", bucket_ptr[i]);
        securec_check_ss(rc, "\0", "\0");
        rc = strncat_s(bucket_str, BUCKETSTRLEN, one_bucket_str, strlen(one_bucket_str));
        securec_check(rc, "\0", "\0");
    }
    return bucket_str;
}

/*
 * get_group_member_ref - Get group_member oidvector pointer  from heap tuple.
 *
 * The return value is oidvector pointer, don't need to free.
 */
static oidvector* get_group_member_ref(HeapTuple tup, bool* need_free)
{
    Datum oid_nodes_datum;
    bool isNull = true;
    oidvector* gmember = NULL;

    *need_free = false;

    oid_nodes_datum = SysCacheGetAttr(PGXCGROUPOID, tup, Anum_pgxc_group_members, &isNull);

    if (isNull != false) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("group_members is null for tuple %u", HeapTupleGetOid(tup))));
    }

    gmember = (oidvector*)PG_DETOAST_DATUM(oid_nodes_datum);

    if (gmember != (oidvector*)DatumGetPointer(oid_nodes_datum))
        *need_free = true;

    return gmember;
}

/*
 * get_group_member - Get group_member filed from node group.
 *
 * The return value is oidvector pointer, caller need to free oidvector by pfree.
 */
static oidvector* get_group_member(Oid group_oid)
{
    HeapTuple tup;
    bool isNull = false;
    oidvector* gmember = NULL;

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %u: group not defined", group_oid)));

    gmember = (oidvector*)PG_DETOAST_DATUM_COPY(SysCacheGetAttr(PGXCGROUPOID, tup, Anum_pgxc_group_members, &isNull));

    ReleaseSysCache(tup);

    return gmember;
}

/*
 * exist_logic_cluster - judge whether pgxc_group has logic cluster.
 *
 * The rel parameter must be pgxc_group relation.
 */
static bool exist_logic_cluster(Relation rel)
{
    char group_kind;
    HeapTuple tuple;
    TableScanDesc scan;
    bool exist = false;

    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    while (tuple) {
        group_kind = get_group_kind(tuple);
        if (group_kind == PGXC_GROUPKIND_LCGROUP) {
            exist = true;
            break;
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }
    tableam_scan_end(scan);

    return exist;
}

static int cmp_node(Oid p1, Oid p2)
{
    NameData node1 = {{0}};
    NameData node2 = {{0}};

    return strcmp(get_pgxc_nodename(p1, &node1), get_pgxc_nodename(p2, &node2));
}

/*
 * exclude_nodes
 *
 * exclude oids in excluded from oids in nodes.
 * oids in nodes and excluded must be in ascending sort.
 *
 * for example:
 * nodes is [1,2,3,4,5,6,7], excluded is [2,4,5,7]
 * we should get the result: [1,3,6]
 * if some oids in excluded is not in nodes,  for example:
 * nodes is [1,2,3,4,5,6,7], excluded is [3,9]
 * we should get the result: [1,2,4,5,6,7]
 * If excluded vector includes nodes vector, return NULL;
 */

static oidvector* exclude_nodes(oidvector* nodes, oidvector* excluded)
{
    int i, j, k;
    oidvector* result = NULL;
    if (nodes == NULL)
        return NULL;

    if (excluded == NULL)
        return buildoidvector(nodes->values, nodes->dim1);

    Oid* oid_array = (Oid*)palloc(nodes->dim1 * sizeof(Oid));

    for (i = 0, k = 0; i < nodes->dim1; i++) {
        for (j = 0; j < excluded->dim1; j++) {
            if (nodes->values[i] == excluded->values[j])
                break;
        }
        if (j == excluded->dim1) {
            oid_array[k++] = nodes->values[i];
        }
    }

    result = buildoidvector(oid_array, k);
    pfree(oid_array);

    return result;
}

/*
 * oidvector_eq - compare oidvector.
 *
 * If oid1 == oid2, return true, else return false;
 */
static bool oidvector_eq(oidvector* oid1, oidvector* oid2)
{
    int i;
    if (oid1->dim1 != oid2->dim1)
        return false;

    for (i = 0; i < oid1->dim1; i++) {
        if (oid1->values[i] != oid2->values[i])
            return false;
    }
    return true;
}

/*
 * oidvector_add - Add one oidvector to another.
 *
 * Caller need to free the return oidvector.
 */
static oidvector* oidvector_add(oidvector* nodes, oidvector* added)
{
    oidvector* result = NULL;
    errno_t rc = 0;
    int count = nodes->dim1 + added->dim1;
    Oid* oids = (Oid*)palloc(count * sizeof(Oid));
    if (nodes->dim1 > 0) {
        rc = memcpy_s(oids, count * sizeof(Oid), nodes->values, nodes->dim1 * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }
    if (added->dim1 > 0 && nodes->dim1 >= 0) {
        rc = memcpy_s(&oids[nodes->dim1], added->dim1 * sizeof(Oid), added->values, added->dim1 * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }
    oids = SortRelationDistributionNodes(oids, count);
    result = buildoidvector(oids, count);
    pfree(oids);
    return result;
}

/*
 * oidvector_remove - Remove one oidvector from another.
 *
 * Caller need to free the return oidvector.
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
static oidvector* oidvector_remove(oidvector* nodes, oidvector* removed)
{
    oidvector* result = NULL;
    int oidoffset = 0;
    Oid* oids = (Oid*)palloc(nodes->dim1 * sizeof(Oid));
    for (int i = 0; i < nodes->dim1; i++) {
        for (int j = 0; j < removed->dim1; j++) {
            if (nodes->values[i] == removed->values[j]) {
                nodes->values[i] = InvalidOid;
                break;
            }
        }
        if (InvalidOid != nodes->values[i])
            oids[oidoffset++] = nodes->values[i];
    }
    result = buildoidvector(oids, oidoffset);
    pfree(oids);
    return result;
}
#pragma GCC diagnostic pop

static ExecNodes* create_exec_nodes(oidvector* gmember)
{
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    exec_nodes->nodeList = GetNodeGroupNodeList(gmember->values, gmember->dim1);
    return exec_nodes;
}

static void delete_exec_nodes(ExecNodes* exec_nodes)
{
    if (exec_nodes != NULL) {
        list_free(exec_nodes->nodeList);
        pfree(exec_nodes);
    }
}

static void exec_in_datanode(const char* group_name, ExecNodes* exec_nodes, bool create)
{
    int rc;
    char create_stmt[CHAR_BUF_SIZE];

    if (create) {
        rc = snprintf_s(create_stmt,
            CHAR_BUF_SIZE,
            CHAR_BUF_SIZE - 1,
            "CREATE NODE GROUP \"%s\" WITH (localhost) VCGROUP;",
            group_name);
    } else {
        rc = snprintf_s(create_stmt, CHAR_BUF_SIZE, CHAR_BUF_SIZE - 1, "DROP NODE GROUP \"%s\";", group_name);
    }
    securec_check_ss(rc, "\0", "\0");

    ExecUtilityStmtOnNodes(create_stmt, exec_nodes, false, false, EXEC_ON_DATANODES, false);
}

static void drop_logic_cluster_in_datanode(const char* group_name, oidvector* gmember)
{
    ExecNodes* exec_nodes = create_exec_nodes(gmember);
    exec_in_datanode(group_name, exec_nodes, false);
    delete_exec_nodes(exec_nodes);
}

static void alter_role_in_datanode(const char* rolname, const char* group_name, ExecNodes* exec_nodes)
{
    int rc;
    char alter_stmt[CHAR_BUF_SIZE];

    rc = snprintf_s(
        alter_stmt, CHAR_BUF_SIZE, CHAR_BUF_SIZE - 1, "ALTER ROLE \"%s\" NODE GROUP \"%s\";", rolname, group_name);

    securec_check_ss(rc, "\0", "\0");

    ExecUtilityStmtOnNodes(alter_stmt, exec_nodes, false, false, EXEC_ON_DATANODES, false);
}

/*
 * PgxcCreateVCGroup():
 *
 * Create logic cluster group
 */
static void PgxcCreateVCGroup(const char* group_name, char group_kind, oidvector* gmember, text* bucket_str)
{
    Relation rel;
    HeapTuple tuple;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    errno_t rc;

    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    // create elastic group with empty node members.
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    values[Anum_pgxc_group_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum('n');
    values[Anum_pgxc_group_kind - 1] = CharGetDatum(group_kind);
    values[Anum_pgxc_group_is_installation - 1] = BoolGetDatum(false);
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(gmember);
    if (bucket_str != NULL) {
        values[Anum_pgxc_group_buckets - 1] = PointerGetDatum(bucket_str);
    } else {
        nulls[Anum_pgxc_group_buckets - 1] = true;
    }
    nulls[Anum_pgxc_group_group_acl - 1] = true;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);
    (void)simple_heap_insert(rel, tuple);
    CatalogUpdateIndexes(rel, tuple);
    heap_freetuple(tuple);
    heap_close(rel, RowExclusiveLock);

    CommandCounterIncrement();
}

/*
 * PgxcChangeGroupName():
 *
 * Change node group name.
 */
static void PgxcChangeGroupName(Oid group_oid, const char* group_name)
{
    HeapTuple tup;
    HeapTuple newtuple;
    Relation rel;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;

    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %u: group not defined", group_oid)));

    // change installation group name
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_name - 1] = true;
    values[Anum_pgxc_group_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));

    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple(newtuple);
    ReleaseSysCache(tup);
    heap_close(rel, RowExclusiveLock);

    // make new installation group name visible
    CommandCounterIncrement();
}

/*
 * PgxcChangeGroupMember():
 *
 * Change node group node members.
 */
static void PgxcChangeGroupMember(Oid group_oid, oidvector* gmember)
{
    HeapTuple tup;
    HeapTuple newtuple;
    Relation rel;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;

    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %d: group not defined", group_oid)));

    // change node group member
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_members - 1] = true;
    nulls[Anum_pgxc_group_members - 1] = false;
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(gmember);

    if (get_group_kind(tup) == PGXC_GROUPKIND_INSTALLATION) {
        int i;
        uint2* bucket_ptr = NULL;
        char* bucket_str = NULL;

        bucket_ptr = (uint2*)palloc0(BUCKETDATALEN * sizeof(uint2));
        for (i = 0; i < BUCKETDATALEN; i++) {
            bucket_ptr[i] = i % gmember->dim1; /* initialized as hash distribution for each bucket */
        }

        bucket_str = get_bucket_string(bucket_ptr);

        replaces[Anum_pgxc_group_buckets - 1] = true;
        values[Anum_pgxc_group_buckets - 1] = DirectFunctionCall1(textin, CStringGetDatum(bucket_str));

        pfree(bucket_ptr);
        pfree(bucket_str);
    }

    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple(newtuple);
    ReleaseSysCache(tup);
    heap_close(rel, RowExclusiveLock);

    // make new installation group name visible
    CommandCounterIncrement();
}

/*
 * PgxcChangeRedistribution():
 *
 * Change in_redistribution filed of pgxc_group  to 'y'.
 */
void PgxcChangeRedistribution(Oid group_oid, char in_redistribution)
{
    HeapTuple tup;
    HeapTuple newtuple;
    Relation rel;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;

    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %u: group not defined", group_oid)));

    // change in_redistribution
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_in_redistribution - 1] = true;
    nulls[Anum_pgxc_group_in_redistribution - 1] = false;
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum(in_redistribution);

    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple(newtuple);
    ReleaseSysCache(tup);
    heap_close(rel, RowExclusiveLock);

    CommandCounterIncrement();
}

/*
 * get_groupid_from_tuple():
 *
 * Get node group oid from heap tuple.
 */
static Oid get_groupid_from_tuple(HeapTuple tup)
{
    bool isNull = true;
    Datum ng_datum;

    ng_datum = SysCacheGetAttr(AUTHOID, tup, Anum_pg_authid_rolnodegroup, &isNull);

    return isNull ? InvalidOid : DatumGetObjectId(ng_datum);
}

/*
 * PgxcGroupGetFirstRoleId():
 *
 * Scan pg_authid and get the first roleid which rolnodegroup is group_oid.
 */
static Oid PgxcGetFirstRoleId(Oid group_oid)
{
    HeapTuple tuple;
    Relation auth_rel;
    TableScanDesc scan;
    Oid roleid;
    Form_pg_authid auth;

    roleid = InvalidOid;
    auth_rel = heap_open(AuthIdRelationId, AccessShareLock);
    scan = tableam_scan_begin(auth_rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        auth = (Form_pg_authid)GETSTRUCT(tuple);
        if (group_oid == get_groupid_from_tuple(tuple)) {
            roleid = HeapTupleGetOid(tuple);
            break;
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(auth_rel, AccessShareLock);

    return roleid;
}

/*
 * PgxcGetGroupRoleList
 *
 * Get role list that attached to same logic cluster.
 */
static List* PgxcGetGroupRoleList()
{
    HeapTuple tuple;
    Relation auth_rel;
    TableScanDesc scan;
    Oid roleid;
    Form_pg_authid auth;
    List* roles_list = NULL;

    auth_rel = heap_open(AuthIdRelationId, AccessShareLock);
    scan = tableam_scan_begin(auth_rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        auth = (Form_pg_authid)GETSTRUCT(tuple);
        if (InvalidOid != get_groupid_from_tuple(tuple)) {
            roleid = HeapTupleGetOid(tuple);
            roles_list = list_append_unique_oid(roles_list, roleid);
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(auth_rel, AccessShareLock);

    return roles_list;
}

/*
 * PgxcGetRelationRoleList
 *
 * Get role list that attached to same logic cluster and create relations in the logic cluster.
 * We get the role list by querying shdepend system table. We don't query pgxc_class
 * because we can't visit pgxc_class tables in other databases.
 */
static List* PgxcGetRelationRoleList()
{
    HeapTuple tuple;
    Relation rel;
    TableScanDesc scan;
    Form_pg_shdepend shdepend;
    Form_pg_authid auth;
    Oid roleid;
    Oid rpoid;
    Datum respoolDatum;
    bool isNull = false;
    List* roles_list = NULL;

    rel = heap_open(SharedDependRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        shdepend = (Form_pg_shdepend)GETSTRUCT(tuple);
        if ((shdepend->classid != RelationRelationId && shdepend->classid != NamespaceRelationId &&
                shdepend->classid != ProcedureRelationId) ||
#ifdef ENABLE_MOT
            (shdepend->deptype != SHARED_DEPENDENCY_OWNER &&
                shdepend->deptype != SHARED_DEPENDENCY_ACL &&
                shdepend->deptype != SHARED_DEPENDENCY_MOT_TABLE)) {
#else
            (shdepend->deptype != SHARED_DEPENDENCY_OWNER && shdepend->deptype != SHARED_DEPENDENCY_ACL)) {
#endif
            tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        roles_list = list_append_unique_oid(roles_list, shdepend->refobjid);

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    /* Include all roles related to resource pool (excluding default resource pool). */
    rel = heap_open(AuthIdRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        rpoid = InvalidOid;
        isNull = true;
        auth = (Form_pg_authid)GETSTRUCT(tuple);
        respoolDatum = heap_getattr(tuple, Anum_pg_authid_rolrespool, RelationGetDescr(rel), &isNull);

        if (!isNull) {
            rpoid = get_resource_pool_oid(DatumGetPointer(respoolDatum));
        }
        if (OidIsValid(rpoid) && rpoid != DEFAULT_POOL_OID) {
            roleid = HeapTupleGetOid(tuple);
            roles_list = list_append_unique_oid(roles_list, roleid);
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    return roles_list;
}

/*
 * PgxcChangeUserGroupOid():
 *
 * Change common users groupOid or installation group oid.
 * if group_name is NULL, the users rolnodegroup is changed to empty.
 * For admin users(superuser,systemadmin,securityadmin), don't change.
 * The function is only executed in CN.
 */
static void PgxcChangeUserGroupOid(List* role_list, const char* group_name, ExecNodes* exec_nodes)
{
    HeapTuple rtup;
    HeapTuple newtuple;
    Relation auth_rel;
    Oid roleid;
    ListCell* cell = NULL;
    Form_pg_authid auth;
    Oid nodegroupid;
    bool nulls[Natts_pg_authid];
    bool replaces[Natts_pg_authid];
    Datum values[Natts_pg_authid];
    errno_t rc;
    Oid group_oid = InvalidOid;

    if (role_list == NULL)
        return;

    if (group_name != NULL)
        group_oid = get_pgxc_groupoid(group_name);

    auth_rel = heap_open(AuthIdRelationId, RowExclusiveLock);

    foreach (cell, role_list) {
        roleid = lfirst_oid(cell);

        rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
        if (!HeapTupleIsValid(rtup)) {
            continue;
        }

        auth = (Form_pg_authid)GETSTRUCT(rtup);
        if (auth->rolsuper || auth->rolsystemadmin || auth->rolcreaterole) {
            ReleaseSysCache(rtup);
            continue;
        }

        nodegroupid = get_groupid_from_tuple(rtup);
        if (group_oid == nodegroupid) {
            ReleaseSysCache(rtup);
            continue;
        }

        if (exec_nodes != NULL) {
            if (group_name == NULL) {
                /* Change rolnodegroup to empty in datanodes. */
                alter_role_in_datanode(NameStr(auth->rolname), CNG_OPTION_INSTALLATION, exec_nodes);
            } else {
                /* Change rolnodegroup to the group_name in datanodes. */
                alter_role_in_datanode(NameStr(auth->rolname), group_name, exec_nodes);
            }
        }

        if (OidIsValid(nodegroupid)) {
            grantNodeGroupToRole(nodegroupid, roleid, ACL_ALL_RIGHTS_NODEGROUP, false);
        }
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pg_authid_rolnodegroup - 1] = true;
        values[Anum_pg_authid_rolnodegroup - 1] = ObjectIdGetDatum(group_oid);
        nulls[Anum_pg_authid_rolnodegroup - 1] = (group_oid == InvalidOid);
        if (!OidIsValid(group_oid)) {
            replaces[Anum_pg_authid_rolkind - 1] = true;
            values[Anum_pg_authid_rolkind - 1] = CharGetDatum(ROLKIND_NORMAL);
        }

        newtuple = heap_modify_tuple(rtup, RelationGetDescr(auth_rel), values, nulls, replaces);

        simple_heap_update(auth_rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(auth_rel, newtuple);
        ReleaseSysCache(rtup);
        heap_freetuple(newtuple);
        CommandCounterIncrement();

        if (OidIsValid(group_oid)) {
            grantNodeGroupToRole(group_oid, roleid, ACL_ALL_RIGHTS_NODEGROUP, true);
        }
    }

    heap_close(auth_rel, RowExclusiveLock);
}

/*
 * PgxcGetRedisNodes():
 *
 * Get nodes needed to released to elastic_group when delete the node group that is redistributed.
 * The function is used for deleting node group. We need to check the source redistributed group
 * and the destination redistributed group.
 * For example:
 * source group nodes: [1 3  5 8  9]
 * destination group nodes: [1 3 5 8 9 10 11]
 * if redist_kind is source group, we return empty oids;
 * if redist_kind is destination group, we return [10 11];
 * another example:
 * source group nodes: [1 3  4  6  7]
 * destination group nodes: [1 2 6 8 9 10 11]
 * if redist_kind is source group, we return [3 4 7];
 * if redist_kind is destination group, we return [2 8 9 10 11];
 */
oidvector* PgxcGetRedisNodes(Relation rel, char redist_kind)
{
    HeapTuple tuple;
    TableScanDesc scan;
    char in_redistribution;
    int self_index = -1;
    oidvector* gmember = NULL;
    oidvector* node_oids[2] = {NULL, NULL};
    oidvector* result = NULL;

    node_oids[0] = NULL;
    node_oids[1] = NULL;
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        bool need_free = false;
        gmember = get_group_member_ref(tuple, &need_free);

        in_redistribution = ((Form_pgxc_group)GETSTRUCT(tuple))->in_redistribution;
        if (in_redistribution == PGXC_REDISTRIBUTION_DST_GROUP && node_oids[0] == NULL) {
            node_oids[0] = buildoidvector(gmember->values, gmember->dim1);
            if (in_redistribution == redist_kind) {
                self_index = 0;
            }
        } else if (in_redistribution == PGXC_REDISTRIBUTION_SRC_GROUP && node_oids[1] == NULL) {
            node_oids[1] = buildoidvector(gmember->values, gmember->dim1);
            if (in_redistribution == redist_kind) {
                self_index = 1;
            }
        }
        if (need_free)
            pfree_ext(gmember);

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }
    tableam_scan_end(scan);

    /* node_oids[0] is DST_GROUP and node_oids[1] is SRC_GROUP. */
    if (self_index == 1) {
        /*switch node_oids[0] and node_oids[1] */
        oidvector* tmp = node_oids[1];
        node_oids[1] = node_oids[0];
        node_oids[0] = tmp;
    }

    if (self_index < 0) {
        result = NULL;
    } else if (node_oids[1] == NULL) {
        return node_oids[0];
    } else {
        int i, j, k;
        int loop_num;
        Oid* oid_array = NULL;

        oid_array = (Oid*)palloc(node_oids[0]->dim1 * sizeof(Oid));
        loop_num = Min(node_oids[0]->dim1, node_oids[1]->dim1);

        for (i = 0, j = 0, k = 0; i < loop_num;) {
            if (node_oids[1]->values[j] == node_oids[0]->values[i]) {
                i++;
                j++;
            } else if (cmp_node(node_oids[1]->values[j], node_oids[0]->values[i]) > 0) {
                oid_array[k++] = node_oids[0]->values[i];
                i++;
            } else {
                j++;
            }
        }
        for (; i < node_oids[0]->dim1; i++) {
            oid_array[k++] = node_oids[0]->values[i];
        }
        if (k > 0) {
            result = buildoidvector(oid_array, k);
        }
        pfree(oid_array);
    }

    if (node_oids[0] != NULL)
        pfree(node_oids[0]);
    if (node_oids[1] != NULL)
        pfree(node_oids[1]);

    return result;
}

/*
 * PgxcUpdateRedistSrcGroup():
 *
 * Set in_redistribution for redistributed source node group.
 * We shoule set in_redistribution to 'n'
 * We should call the function in  final phase of expanding logic cluster.
 */
void PgxcUpdateRedistSrcGroup(Relation rel, oidvector* gmember, text* bucket_str, char* group_name)
{
    HeapTuple tuple;
    TableScanDesc scan;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    HeapTuple newtuple;
    char in_redistribution;
    errno_t rc;

    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        in_redistribution = ((Form_pgxc_group)GETSTRUCT(tuple))->in_redistribution;
        if (in_redistribution == PGXC_REDISTRIBUTION_SRC_GROUP) {
            break;
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    if (tuple == NULL) {
        tableam_scan_end(scan);
        /* Can not find redistributed source group. */
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Can not find redistributed source group with in_redistribution 'y'.")));
    }

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_in_redistribution - 1] = true;
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum(PGXC_NON_REDISTRIBUTION_GROUP);

    replaces[Anum_pgxc_group_members - 1] = true;
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(gmember);

    if (bucket_str != NULL) {
        replaces[Anum_pgxc_group_buckets - 1] = true;
        values[Anum_pgxc_group_buckets - 1] = PointerGetDatum(bucket_str);
    }

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), values, nulls, replaces);

    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple(newtuple);

    tableam_scan_end(scan);
}

/*
 * PgxcGroupAddNode():
 *
 * add node group node members.
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
void PgxcGroupAddNode(Oid group_oid, Oid nodeid)
{
    HeapTuple tup;
    oidvector* gmember_ref = NULL;
    oidvector* gmember = NULL;
    oidvector new_node;
    bool need_free = false;

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %u: group not defined", group_oid)));

    gmember_ref = get_group_member_ref(tup, &need_free);

    if (gmember_ref->dim1 == 0) {
        gmember = buildoidvector(&nodeid, 1);
    } else {
        new_node.dim1 = 1;
        new_node.ndim = 1;
        new_node.values[0] = nodeid;

        gmember = oidvector_add(gmember_ref, &new_node);
    }

    PgxcChangeGroupMember(group_oid, gmember);

    ReleaseSysCache(tup);

    if (need_free)
        pfree_ext(gmember_ref);
    pfree_ext(gmember);

}

/*
 * PgxcGroupRemoveNode():
 *
 * Remove node members from node group .
 */
void PgxcGroupRemoveNode(Oid group_oid, Oid nodeid)
{
    HeapTuple tup;
    oidvector* gmember_ref = NULL;
    oidvector* gmember = NULL;
    oidvector deleted_node;
    bool need_free = false;

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("PGXC Group %s: group not defined", get_pgxc_groupname(group_oid))));

    gmember_ref = get_group_member_ref(tup, &need_free);

    for (int i = 0; i < gmember_ref->dim1; i++) {
        if (nodeid == gmember_ref->values[i]) {
            deleted_node.dim1 = 1;
            deleted_node.ndim = 1;
            deleted_node.values[0] = nodeid;

            gmember = oidvector_remove(gmember_ref, &deleted_node);
            PgxcChangeGroupMember(group_oid, gmember);
            pfree_ext(gmember);
            break;
        }
    }
    if (need_free)
        pfree_ext(gmember_ref);

    ReleaseSysCache(tup);

}
#pragma GCC diagnostic pop
/*
 * IsNodeInLogicCluster
 *
 * Check if node in logic cluster
 */
bool IsNodeInLogicCluster(Oid* oid_array, int count, Oid excluded)
{
    Relation relation;
    HeapTuple tuple;
    TableScanDesc scan;
    bool find = false;
    Oid group_oid;
    oidvector* gmember = NULL;
    List* oid_list = NULL;
    int i;

    /* Scan pgxc_group, find all node oids and append them to oid_list. */
    relation = heap_open(PgxcGroupRelationId, AccessShareLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);

    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        group_oid = HeapTupleGetOid(tuple);
        if (group_oid != excluded && PGXC_GROUPKIND_LCGROUP == get_group_kind(tuple)) {
            bool need_free = false;
            gmember = get_group_member_ref(tuple, &need_free);

            for (i = 0; i < gmember->dim1; i++) {
                oid_list = lappend_oid(oid_list, gmember->values[i]);
            }
            if (need_free)
                pfree_ext(gmember);
        }

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(relation, AccessShareLock);

    find = false;
    for (i = 0; i < count; i++) {
        ListCell* cell = NULL;
        foreach (cell, oid_list) {
            if (lfirst_oid(cell) == oid_array[i]) {
                find = true;
                break;
            }
        }
        if (find)
            break;
    }

    list_free(oid_list);
    return find;
}

static Acl* PgxcGroupGetAcl(Oid group_oid, Relation rel)
{
    Acl* acl = NULL;
    bool isNull = false;
    HeapTuple tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_oid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for node group with oid %u", group_oid)));

    Datum aclDatum = heap_getattr(tuple, Anum_pgxc_group_group_acl, RelationGetDescr(rel), &isNull);
    if (!isNull) {
        acl = DatumGetAclPCopy(aclDatum);
    }
    ReleaseSysCache(tuple);
    return acl;
}

void PgxcCheckOid(Oid noid, char* node_name)
{
    if (!OidIsValid(noid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));
    }
    if (get_pgxc_nodetype(noid) != PGXC_NODE_DATANODE) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: only Datanodes can be group members", node_name)));
    }
}

/*
 * PgxcGroupCreate
 *
 * Create a PGXC node group
 */
void PgxcGroupCreate(CreateGroupStmt* stmt)
{
    const char* group_name = stmt->group_name;
    List* nodes = stmt->nodes;
    oidvector* nodes_array = NULL;
    Oid* inTypes = NULL;
    Relation rel;
    HeapTuple tup;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    int member_count = list_length(stmt->nodes);
    ListCell* lc = NULL;
    int i = 0;
    uint2* bucket_ptr = NULL;
    char* bucket_str = NULL;
    TableScanDesc scan;
    HeapTuple tuple;
    char group_kind = 'i';
    bool is_installation = false;
    oidvector* elastic_nodes = NULL;
    oidvector* src_group_nodes = NULL;
    ExecNodes* exec_nodes = NULL;
    bool dummy_nodegroup = false;
    Oid src_group_oid = InvalidOid;
    Acl* group_acl = NULL;

    int len = BUCKETDATALEN * sizeof(uint2);

    errno_t rc = 0;
    List* tmp_list = NIL;

    /* Only a DB administrator can add cluster node groups */
    if (!have_createdb_privilege())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("must be system admin or createdb role to create cluster node groups")));

    /* Check node group is preserved group name */
    if (group_name &&
        (!ng_is_valid_group_name(group_name) || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, group_name) == 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s can not be preserved group name", group_name)));
    }

    /* Check if given group already exists */
    if (OidIsValid(get_pgxc_groupoid(group_name)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group already defined", group_name)));

    /* Don't support logic cluster in multi_standby mode. */
    if (stmt->vcgroup && u_sess->attr.attr_storage.enable_data_replicate &&
        g_instance.attr.attr_storage.replication_type == RT_WITH_MULTI_STANDBY)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Don't support logic cluster in multi_standby mode.")));

    /* Specially handle for CREATE NODE GROUP WITH (localhost) VCGROUP. */
    if (isRestoreMode && list_length(nodes) == 1) {
        char* str = strVal(lfirst(list_head(nodes)));
        if (strcmp(str, "localhost") == 0) {
            dummy_nodegroup = true;
        }
    }
    if (dummy_nodegroup || (IS_PGXC_DATANODE && stmt->vcgroup && !isRestoreMode)) {
        /* Open the relation for read and insertion */
        rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

        // restore values and nulls for insert new node group record
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* Insert Data correctly */
        values[Anum_pgxc_group_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));
        nulls[Anum_pgxc_group_buckets - 1] = true;
        nulls[Anum_pgxc_group_members - 1] = true;
        nulls[Anum_pgxc_group_group_acl - 1] = true;

        values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum(PGXC_NON_REDISTRIBUTION_GROUP);
        values[Anum_pgxc_group_is_installation - 1] = BoolGetDatum(false);
        values[Anum_pgxc_group_kind - 1] = CharGetDatum(PGXC_GROUPKIND_LCGROUP);

        tup = heap_form_tuple(rel->rd_att, values, nulls);

        /* Do the insertion */
        (void)simple_heap_insert(rel, tup);

        CatalogUpdateIndexes(rel, tup);
        heap_freetuple(tup);

        heap_close(rel, RowExclusiveLock);

        CommandCounterIncrement();

        CreateNodeGroupInfoInHTAB(group_name);

        return;
    }

    inTypes = (Oid*)palloc(member_count * sizeof(Oid));

    /* Build list of Oids for each node listed */
    foreach (lc, nodes) {
        char* node_name = strVal(lfirst(lc));
        Oid noid = get_pgxc_nodeoid(node_name);

        PgxcCheckOid(noid, node_name);

        /* Detect dup-nodes in DN lists */
        if (list_member_oid(tmp_list, noid)) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("PGXC node %s is already specified: duplicate Datanodes is not allowed in node-list",
                        node_name)));
        } else
            tmp_list = lappend_oid(tmp_list, noid);

        /* OK to pick up Oid of this node */
        inTypes[i] = noid;
        i++;
    }

    /* inTypes will not be used anymore */
    /* Used it to detect if there are duplicated node names */
    // we don't have to keep different order with other pgxc_class right?
    inTypes = SortRelationDistributionNodes(inTypes, member_count);

    /* Build array of Oids to be inserted */
    /* keep this behind qsort to make sure the order of node array. */
    nodes_array = buildoidvector(inTypes, member_count);

    /* Iterate through all attributes initializing nulls and values */
    for (i = 0; i < Natts_pgxc_group; i++) {
        nulls[i] = false;
        values[i] = (Datum)0;
    }
    bucket_ptr = (uint2*)palloc0(len);

    /* Open the relation for read and insertion */
    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    i = 0;

    /* Generate buckemap for new node group */
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    /*
     * No nodegroup found, it is the first node group creation, so generate bucketmap
     * in default mode
     */
    if (tuple == NULL) {
        /* The first created node group is always considered as installation group */
        if (!isRestoreMode) {
            /*
             * In case of isRestoreMode mode, we don't specified first created node group as
             * installation group as it aiming to restore the original node group status and
             * is_installation is update later (in pg_dumps).
             */
            is_installation = true;

            if (stmt->vcgroup) {
                tableam_scan_end(scan);
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Can not create logic cluster group, create installation group first.")));
            }
        }

        generateConsistentHashBucketmap(stmt, nodes_array, rel, tuple, bucket_ptr, BUCKETMAP_MODE_DEFAULT);
    } else {
        /* get group_kind, we need to know whether logic cluster or common node group exist */
        group_kind = get_group_kind(tuple);

        /*
         * There is at least one nodegroup, so we may in cluster capactiy expansion
         * and multi-nodegroup case
         */
        /* For none-cluster resizing case, we generate bucket map in default mode */
        if (!u_sess->attr.attr_sql.enable_cluster_resize) {
            generateConsistentHashBucketmap(stmt, nodes_array, rel, tuple, bucket_ptr, BUCKETMAP_MODE_DEFAULT);

            if (group_kind == 'i') {
                /* The first node group is installation group, we need to check the second node group */
                tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
                if (tuple) {
                    group_kind = get_group_kind(tuple);
                }
            }
        } else {
            Datum datum;
            bool isNull = false;

            /*
             * In cluster resizing case, we need find previous node group and
             * generate bucketmap from that
             */
            while (tuple) {
                if (stmt->src_group_name) {
                    /* logic cluster resizing case */
                    const char* name = NameStr(((Form_pgxc_group)GETSTRUCT(tuple))->group_name);
                    if (strcmp(stmt->src_group_name, name) == 0)
                        break;
                } else {
                    /* non-logic cluster resizing case */
                    datum = heap_getattr(tuple, Anum_pgxc_group_is_installation, RelationGetDescr(rel), &isNull);

                    if (DatumGetBool(datum)) {
                        /*
                         * Break as we already find installation node group so generate bucketmap
                         * for new installation group base on it
                         */
                        break;
                    }
                }

                tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
                if (tuple && group_kind == 'i') {
                    /* The first node group is installation group, we need to check the second node group */
                    group_kind = get_group_kind(tuple);
                }
            }

            if (!tuple) {
                tableam_scan_end(scan);
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                        errmsg("installation node group or source node group not found")));
            }

            /* Generate bucketmap based on previous installation group */
            generateConsistentHashBucketmap(stmt, nodes_array, rel, tuple, bucket_ptr, BUCKETMAP_MODE_REMAP);
        }

        if ((group_kind == 'n' && stmt->vcgroup) || ((group_kind == 'v' || group_kind == 'e') && !stmt->vcgroup)) {
            tableam_scan_end(scan);
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Do not support logic cluster in coexistence with common node group!")));
        }
    }

    if (stmt->src_group_name) {
        if (!u_sess->attr.attr_common.xc_maintenance_mode) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("CREATE NODE GROUP ... DISTRIBUTE FROM can only be executed in maintenance mode.")));
        }
        if (group_name == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("group_name can not be NULL ")));
        }

        if (strcmp(stmt->src_group_name, group_name) == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("The node group name can not be same as redistribution node group name!")));
        }

        src_group_oid = get_pgxc_groupoid(stmt->src_group_name, false);
        src_group_nodes = get_group_member(src_group_oid);
        if (src_group_nodes == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("The node group %s has not any nodes.", stmt->src_group_name)));
        }

        /* modify in_redistribution of source group to 'y' */
        PgxcChangeRedistribution(src_group_oid, PGXC_REDISTRIBUTION_SRC_GROUP);
    }

    if (stmt->vcgroup) {
        oidvector* gmember = NULL;
        Oid group_oid = InvalidOid;
        bool need_free = false;
        char search_group_kind =
            (group_kind == PGXC_GROUPKIND_INSTALLATION) ? PGXC_GROUPKIND_INSTALLATION : PGXC_GROUPKING_ELASTIC;

        /* Rescan pgxc_group, get groupoid and groupmember of
         *  installation group or elastic group. If group_kind is 'i', we
         * need to query installation group; Or we need to query elastic group.
         */
        tableam_scan_rescan(scan, 0);
        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
        while (tuple) {
            group_kind = get_group_kind(tuple);
            if (group_kind == search_group_kind) {
                group_oid = HeapTupleGetOid(tuple);
                gmember = get_group_member_ref(tuple, &need_free);
                break;
            }

            tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
        }

        if (IsNodeInLogicCluster(nodes_array->values, nodes_array->dim1, src_group_oid)) {
            tableam_scan_end(scan);
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Some nodes have been allocated to logic cluster!")));
        }

        /* If pgxc_group has installation only, we need to judge whether some tables is created. */
        if (search_group_kind == PGXC_GROUPKIND_INSTALLATION && !CanPgxcGroupRemove(group_oid, true)) {
            tableam_scan_end(scan);
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("PGXC Group %s: some tables is distributed in installation group,"
                           "can not create logic cluster.",
                        group_name)));
        }

        /* If src_group_nodes is not NULL, it means we are creating
         *  a logic cluster used for redistribution.
         */
        if (src_group_nodes != NULL) {
            oidvector* add_nodes = NULL;
            if (oidvector_eq(nodes_array, src_group_nodes)) {
                tableam_scan_end(scan);
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Don't need to expand the node group!")));
            }

            add_nodes = exclude_nodes(nodes_array, src_group_nodes);
            if (add_nodes == NULL) {
                /* node group contraction */
                elastic_nodes = NULL;
                if (gmember != NULL)
                    elastic_nodes = buildoidvector(gmember->values, gmember->dim1);
            } else {
                if (gmember != NULL)
                    elastic_nodes = exclude_nodes(gmember, add_nodes);

                pfree(add_nodes);
            }
        } else {
            elastic_nodes = exclude_nodes(gmember, nodes_array);
        }

        /*  search_group_kind is 'i' means it is the first logic cluster,
         *  so we need to create elastic group first.
         *  If search_group_kind is 'e' means elastic group exists already,
         *  so we need to remove the nodes from elastic group.
         */
        if (search_group_kind == PGXC_GROUPKIND_INSTALLATION) {
            PgxcCreateVCGroup(VNG_OPTION_ELASTIC_GROUP, 'e', elastic_nodes, NULL);
            CreateNodeGroupInfoInHTAB(VNG_OPTION_ELASTIC_GROUP);
        } else { /* if search_group_kind == 'e' */
            PgxcChangeGroupMember(group_oid, elastic_nodes);
        }

        if (in_logic_cluster() && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            exec_nodes = create_exec_nodes(nodes_array);
        }
        if (need_free)
            pfree_ext(gmember);
    }

    tableam_scan_end(scan);

    // restore values and nulls for insert new node group record
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* build bucket str */
    bucket_str = get_bucket_string(bucket_ptr);

    /* Insert Data correctly */
    values[Anum_pgxc_group_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(group_name));
    values[Anum_pgxc_group_buckets - 1] = DirectFunctionCall1(textin, CStringGetDatum(bucket_str));
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(nodes_array);
    values[Anum_pgxc_group_in_redistribution - 1] = (src_group_nodes == NULL)
                                                        ? CharGetDatum(PGXC_NON_REDISTRIBUTION_GROUP)
                                                        : CharGetDatum(PGXC_REDISTRIBUTION_DST_GROUP);

    /* User create nodegroup is always non-installation group */
    values[Anum_pgxc_group_is_installation - 1] = BoolGetDatum(is_installation);

    if (is_installation == false) {
        if (OidIsValid(src_group_oid)) {
            group_acl = PgxcGroupGetAcl(src_group_oid, rel);
        }

        nulls[Anum_pgxc_group_group_acl - 1] = (group_acl == NULL);
        values[Anum_pgxc_group_group_acl - 1] = PointerGetDatum(group_acl);

        nulls[Anum_pgxc_group_kind - 1] = false;
        values[Anum_pgxc_group_kind - 1] =
            stmt->vcgroup ? CharGetDatum(PGXC_GROUPKIND_LCGROUP) : CharGetDatum(PGXC_GROUPKIND_NODEGROUP);
    } else {
        /* grant create no node group group1 to public */
        nulls[Anum_pgxc_group_group_acl - 1] = false;
        values[Anum_pgxc_group_group_acl - 1] = PointerGetDatum(getAclNodeGroup());

        nulls[Anum_pgxc_group_kind - 1] = false;
        values[Anum_pgxc_group_kind - 1] = CharGetDatum(PGXC_GROUPKIND_INSTALLATION);
    }

    tup = heap_form_tuple(rel->rd_att, values, nulls);

    /* Do the insertion */
    (void)simple_heap_insert(rel, tup);

    CatalogUpdateIndexes(rel, tup);
    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
    pfree(bucket_ptr);
    pfree(bucket_str);
    pfree(inTypes);
    if (group_acl != NULL)
        pfree(group_acl);
    if (elastic_nodes != NULL) {
        pfree(elastic_nodes);
    }
    if (src_group_nodes != NULL) {
        pfree(src_group_nodes);
    }

    /* create node group element in the hash table */
    if (stmt->vcgroup)
        CreateNodeGroupInfoInHTAB(group_name);

    /* Send to Datanode, only for logic cluster mode */
    if (exec_nodes != NULL) {
        exec_in_datanode(group_name, exec_nodes, true);
        delete_exec_nodes(exec_nodes);
    }

    return;
}

/*
 * PgxcNodeGroupsSetDefault():
 *
 * Alter a PGXC node group, set node group to default
 */
static void PgxcGroupSetDefault(const char* group_name)
{
    Relation relation;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    HeapTuple tup;
    HeapTuple newtuple;
    TableScanDesc scan;
    errno_t rc;

    Oid group_oid = get_pgxc_groupoid(group_name);

    /* Check if group exists */
    if (!OidIsValid(group_oid))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", group_name)));

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_in_redistribution - 1] = true;
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum('n');

    /* Open pgxc_group relation descriptor */
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);

    newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);
    heap_freetuple(newtuple);
    ReleaseSysCache(tup);

    /* update in_redistribution of the left node group, if there is one */
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        if (HeapTupleGetOid(tup) != group_oid) {
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "\0", "\0");
            rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
            securec_check(rc, "\0", "\0");
            rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
            securec_check(rc, "\0", "\0");

            replaces[Anum_pgxc_group_in_redistribution - 1] = true;
            values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum('y');

            newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

            simple_heap_update(relation, &newtuple->t_self, newtuple);
            CatalogUpdateIndexes(relation, newtuple);
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, RowExclusiveLock);
}

/*
 * PgxcGroupConvertVCGroup():
 *
 * Convert installation node group to logic cluster lcname.
 */
static void PgxcGroupConvertVCGroup(const char* group_name, const char* lcname)
{
    Oid group_oid;
    Relation rel;
    HeapTuple tuple;
    TableScanDesc scan;
    char group_kind;
    oidvector* gmember = NULL;
    oidvector* gmember_ref = NULL;
    text* groupbucket = NULL;
    bool isNull = false;
    List* role_list = NULL;
    oidvector* elasitc_group = NULL;
    ExecNodes* exec_nodes = NULL;
    bool need_free = false;

    /* Open the relation for read */
    rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    if (tuple == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
    }

    group_kind = get_group_kind(tuple);
    if (group_kind != 'i') {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Do not allow to convert installation group to logic cluster "
                       "when other node group exists.")));
    }

    if (pg_strcasecmp(group_name, NameStr(((Form_pgxc_group)GETSTRUCT(tuple))->group_name)) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("PGXC Group %s is not installation group.", group_name)));
    }

    group_oid = HeapTupleGetOid(tuple);
    gmember_ref = get_group_member_ref(tuple, &need_free);
    if (gmember_ref->dim1 > 0) {
        gmember = buildoidvector(gmember_ref->values, gmember_ref->dim1);
    } else {
        /* it is impossible that installation group exists but its member is null. */
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The installation group has no members.")));
    }
    groupbucket = (text*)heap_getattr(tuple, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);
    if (isNull) {
        groupbucket = NULL;
    }

    /* check whether other node group exist or not. */

    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    if (tuple) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Do not allow to convert installation group to logic cluster"
                       "when other node group exists.")));
    }
    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    role_list = PgxcGetRelationRoleList();

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        exec_nodes = create_exec_nodes(gmember);
    }

    /* create elastic group with empty node members. */
    elasitc_group = buildoidvector(NULL, 0);
    PgxcCreateVCGroup(VNG_OPTION_ELASTIC_GROUP, 'e', elasitc_group, NULL);
    pfree_ext(elasitc_group);
    CreateNodeGroupInfoInHTAB(VNG_OPTION_ELASTIC_GROUP);

    /* create a new logic cluster. */
    PgxcCreateVCGroup(lcname, 'v', gmember, groupbucket);

    /* Send to Datanode, only for logic cluster mode */
    if (exec_nodes != NULL) {
        exec_in_datanode(lcname, exec_nodes, true);
    }

    /* change groupoid of common users */
    if (role_list != NULL) {
        PgxcChangeUserGroupOid(role_list, lcname, exec_nodes);
        list_free(role_list);
    }

    if (exec_nodes != NULL) {
        delete_exec_nodes(exec_nodes);
    }

    /* create node group element in the hash table */
    CreateNodeGroupInfoInHTAB(lcname);

    pfree_ext(gmember);
    if (need_free)
        pfree_ext(gmember_ref);
}

/*
 * PgxcGroupSetVCGroup():
 *
 * Alter installation node group, move all pgxc tables to logic cluster.
 */
static void PgxcGroupSetVCGroup(const char* group_name, const char* install_name)
{
    Oid group_oid;
    Relation rel;
    HeapTuple tuple;
    TableScanDesc scan;
    char group_kind;
    oidvector* gmember = NULL;
    oidvector* gmember_ref = NULL;
    text* groupbucket = NULL;
    bool isNull = false;
    List* role_list = NULL;
    oidvector* elasitc_group = NULL;
    ExecNodes* exec_nodes = NULL;
    bool need_free = false;

    /* Open the relation for read */
    rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    if (tuple == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
    }

    group_kind = get_group_kind(tuple);
    if (group_kind != 'i') {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Do not allow to convert installation group to logic cluster "
                       "when other node group exists.")));
    }

    if (pg_strcasecmp(group_name, NameStr(((Form_pgxc_group)GETSTRUCT(tuple))->group_name)) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("PGXC Group %s is not installation group.", group_name)));
    }

    group_oid = HeapTupleGetOid(tuple);
    gmember_ref = get_group_member_ref(tuple, &need_free);
    if (gmember_ref->dim1 > 0) {
        gmember = buildoidvector(gmember_ref->values, gmember_ref->dim1);
    } else {
        /* it is impossible that installation group exists but its member is null. */
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The installation group has no members.")));
    }
    groupbucket = (text*)heap_getattr(tuple, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);
    if (isNull) {
        groupbucket = NULL;
    }

    /* check whether other node group exist or not. */

    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    if (tuple) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Do not allow to convert installation group to logic cluster"
                       "when other node group exists.")));
    }
    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    role_list = PgxcGetRelationRoleList();

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        exec_nodes = create_exec_nodes(gmember);
    }

    /* create elastic group with empty node members. */
    elasitc_group = buildoidvector(NULL, 0);
    PgxcCreateVCGroup(VNG_OPTION_ELASTIC_GROUP, 'e', elasitc_group, NULL);
    pfree_ext(elasitc_group);
    CreateNodeGroupInfoInHTAB(VNG_OPTION_ELASTIC_GROUP);

    /* change installation group name */
    PgxcChangeGroupName(group_oid, install_name);

    /* create a new logic cluster. */
    PgxcCreateVCGroup(group_name, 'v', gmember, groupbucket);

    /* Send to Datanode, only for logic cluster mode */
    if (exec_nodes != NULL) {
        exec_in_datanode(group_name, exec_nodes, true);
    }

    /* change groupoid of common users */
    if (role_list != NULL) {
        PgxcChangeUserGroupOid(role_list, group_name, exec_nodes);
        list_free(role_list);
    }

    if (exec_nodes != NULL) {
        delete_exec_nodes(exec_nodes);
    }

    /* create node group element in the hash table */
    CreateNodeGroupInfoInHTAB(group_name);

    pfree_ext(gmember);
    if (need_free)
        pfree_ext(gmember_ref);
}

/*
 * PgxcGroupSetNotVCGroup():
 *
 * Alter installation node group, convert all logic clusters to common node group.
 */
static void PgxcGroupSetNotVCGroup(const char* group_name)
{
    HeapTuple tuple;
    HeapTuple newtuple;
    Relation rel;
    TableScanDesc scan;
    char group_kind;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tuple) {
        group_kind = get_group_kind(tuple);
        if (group_kind == 'i') {
            if (pg_strcasecmp(group_name, NameStr(((Form_pgxc_group)GETSTRUCT(tuple))->group_name)) != 0) {
                heap_close(rel, RowExclusiveLock);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PGXC Group %s is not installation group.", group_name)));
            }
            tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        if (group_kind == 'n') {
            tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        if (group_kind == 'e') {
            Oid elastic_group_oid = get_pgxc_groupoid(VNG_OPTION_ELASTIC_GROUP, false);
            deleteSharedDependencyRecordsFor(PgxcGroupRelationId, elastic_group_oid, 0);
            simple_heap_delete(rel, &tuple->t_self);
            RemoveNodeGroupInfoInHTAB(VNG_OPTION_ELASTIC_GROUP);
            tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        RemoveNodeGroupInfoInHTAB(group_name);

        if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
            bool need_free = false;
            oidvector* gmember = NULL;
            const char* gname = NameStr(((Form_pgxc_group)GETSTRUCT(tuple))->group_name);

            gmember = get_group_member_ref(tuple, &need_free);

            drop_logic_cluster_in_datanode(gname, gmember);

            if (need_free)
                pfree_ext(gmember);
        }

        replaces[Anum_pgxc_group_kind - 1] = true;
        values[Anum_pgxc_group_kind - 1] = CharGetDatum('n');

        newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), values, nulls, replaces);

        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);
        heap_freetuple(newtuple);

        tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    tableam_scan_end(scan);
    heap_close(rel, RowExclusiveLock);

    CommandCounterIncrement();

    /* change groupoid of common users in all nodes */
    List* role_list = PgxcGetGroupRoleList();
    if (role_list != NULL) {
        ExecNodes* exec_nodes = makeNode(ExecNodes);
        exec_nodes->nodeList = GetAllDataNodes();

        PgxcChangeUserGroupOid(role_list, NULL, exec_nodes);
        list_free(role_list);
        delete_exec_nodes(exec_nodes);
    }
}

/*
 * PgxcGroupRename():
 *
 * Rename node group group_name to new_name.
 */
static void PgxcGroupRename(const char* group_name, const char* new_name)
{
    Oid group_oid;

    /* Check node group is preserved group name */
    if (!ng_is_valid_group_name(new_name) || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, new_name) == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s can not be preserved group name", group_name)));
    }

    group_oid = GetSysCacheOid1(PGXCGROUPNAME, PointerGetDatum(new_name));
    if (OidIsValid(group_oid))
        ereport(
            ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group Name %s has already been used!", new_name)));

    group_oid = get_pgxc_groupoid(group_name);

    if (!OidIsValid(group_oid))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));

    /* change node group name */
    PgxcChangeGroupName(group_oid, new_name);

    if (get_pgxc_groupkind(group_oid) != PGXC_GROUPKIND_LCGROUP)
        return;

    RemoveNodeGroupInfoInHTAB(group_name);
    (void)CreateNodeGroupInfoInHTAB(new_name);

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && in_logic_cluster()) {
        int rc;
        int nmembers = 0;
        Oid* members = NULL;
        oidvector* gmember = NULL;
        ExecNodes* exec_nodes = NULL;
        char alter_stmt[CHAR_BUF_SIZE];

        rc = snprintf_s(alter_stmt,
            CHAR_BUF_SIZE,
            CHAR_BUF_SIZE - 1,
            "ALTER NODE GROUP \"%s\" RENAME TO \"%s\";",
            group_name,
            new_name);
        securec_check_ss(rc, "\0", "\0");

        nmembers = get_pgxc_groupmembers(group_oid, &members);

        Assert(members != NULL);
        gmember = buildoidvector(members, nmembers);
        pfree(members);

        exec_nodes = create_exec_nodes(gmember);
        ExecUtilityStmtOnNodes(alter_stmt, exec_nodes, false, false, EXEC_ON_DATANODES, false);
        delete_exec_nodes(exec_nodes);
        pfree(gmember);
    }
}

/*
 * PgxcChangeTableGroupName():
 *
 * Change the pgroup field of pgxc_class.
 */
static void PgxcChangeTableGroupName(const char* group_name, const char* new_name)
{
    HeapTuple tup;
    HeapTuple newtuple;
    Relation rel;
    bool nulls[Natts_pgxc_class];
    Datum values[Natts_pgxc_class];
    bool replaces[Natts_pgxc_class];
    char* pgroup = NULL;
    TableScanDesc scan;
    Datum datum;
    bool isNull = false;
    errno_t rc;

    if (pg_strcasecmp(group_name, new_name) == 0) {
        return;
    }

    /* Check node group is preserved group name */
    if (!ng_is_valid_group_name(new_name) || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, new_name) == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s can not be preserved group name", group_name)));
    }

    /* Initialize fields */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    /* Reserve column */
    nulls[Anum_pgxc_class_option - 1] = true;

    replaces[Anum_pgxc_class_pgroup - 1] = true;
    values[Anum_pgxc_class_pgroup - 1] = DirectFunctionCall1(namein, CStringGetDatum(new_name));

    rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while (tup) {
        datum = SysCacheGetAttr(PGXCCLASSRELID, tup, Anum_pgxc_class_pgroup, &isNull);

        if (isNull) {
            tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }
        pgroup = DatumGetCString(datum);

        if (pg_strcasecmp(pgroup, group_name) != 0) {
            tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }
        /* Update relation */
        newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &tup->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);
        heap_freetuple(newtuple);

        tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }
    tableam_scan_end(scan);
    heap_close(rel, RowExclusiveLock);

    CommandCounterIncrement();
}

/*
 * PgxcCopyBucketsFromNew():
 *
 * Copy Buckets From New Node Group
 */
static void PgxcCopyBucketsFromNew(const char* group_name, const char* src_group_name)
{
    HeapTuple tup;
    HeapTuple srctuple;
    Relation relation;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;
    Oid group_oid = get_pgxc_groupoid(group_name);
    Oid src_group_oid = get_pgxc_groupoid(src_group_name);
    bool isNull = true;
    bool need_free = false;
    oidvector* gmember = NULL;

    if (IS_PGXC_DATANODE)
        return;

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    srctuple = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(src_group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(srctuple)) /* should not happen */
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", src_group_name)));

    gmember = get_group_member_ref(srctuple, &need_free);

    text* groupbuckests = (text*)heap_getattr(srctuple, Anum_pgxc_group_buckets, RelationGetDescr(relation), &isNull);
    if (isNull) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("PGXC Group %s' buckets can not be null", src_group_name)));
    }

    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_members - 1] = true;
    values[Anum_pgxc_group_members - 1] = PointerGetDatum(gmember);

    replaces[Anum_pgxc_group_buckets - 1] = true;
    values[Anum_pgxc_group_buckets - 1] = PointerGetDatum(groupbuckests);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", group_name)));

    HeapTuple newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);
    heap_freetuple(newtuple);
    ReleaseSysCache(tup);
    ReleaseSysCache(srctuple);

    heap_close(relation, NoLock);
}

/*
 * PgxcGroupModifyRedisAttr
 *
 * modify group attr in pgxc_group
 * but now just modify in_redistribution and is_installation attr
 */
static void PgxcGroupModifyRedisAttr(const char* group_name, Form_pgxc_group groupattr)
{
    Relation relation;
    HeapTuple tuple;
    HeapTuple newtuple;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;
    Oid group_oid = get_pgxc_groupoid(group_name);

    /* Check if group exists */
    if (!OidIsValid(group_oid)) {
        if (IS_PGXC_DATANODE)
            return;

        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
    }

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tuple = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);
    if (!HeapTupleIsValid(tuple)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", group_name)));

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_in_redistribution - 1] = true;
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum(groupattr->in_redistribution);
    replaces[Anum_pgxc_group_is_installation - 1] = true;
    values[Anum_pgxc_group_is_installation - 1] = BoolGetDatum(groupattr->is_installation);

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);

    ReleaseSysCache(tuple);
    heap_freetuple(newtuple);

    heap_close(relation, RowExclusiveLock);
}

#ifdef ENABLE_MULTIPLE_NODES
static void CreateRemoteSeqs(const char* queryString, List* uuids, ExecNodes* excluded_nodes)
{
    char* msg = NULL;
    char* uuidinfo = nodeToString(uuids);
    AssembleHybridMessage(&msg, queryString, uuidinfo);

    List* all_nodes = GetAllDataNodes();
    List* exec_nodes = list_difference_int(all_nodes, excluded_nodes->nodeList);

    RemoteQuery* step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->sql_statement = (char*)msg;
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = false;
    step->exec_nodes = makeNode(ExecNodes);
    step->exec_nodes->nodeList = exec_nodes;
    list_free(all_nodes);
    ExecRemoteUtility(step);

    list_free(step->exec_nodes->nodeList);
    pfree_ext(step->exec_nodes);
    pfree_ext(step);
    pfree_ext(msg);
    pfree_ext(uuidinfo);
}

static void DropRemoteSeqs(char* queryString, ExecNodes* excluded_nodes)
{
    List* all_nodes = GetAllDataNodes();
    List* exec_nodes = list_difference_int(all_nodes, excluded_nodes->nodeList);

    RemoteQuery* step = makeNode(RemoteQuery);
    step->combine_type = COMBINE_TYPE_SAME;
    step->sql_statement = (char*)queryString;
    step->exec_type = EXEC_ON_DATANODES;
    step->is_temp = false;
    step->exec_nodes = makeNode(ExecNodes);
    step->exec_nodes->nodeList = exec_nodes;
    list_free(all_nodes);
    ExecRemoteUtility(step);

    list_free(step->exec_nodes->nodeList);
    pfree_ext(step->exec_nodes);
    pfree_ext(step);
}

/*
 * PgxcGroupSetSeqNodes():
 *
 * sync sequences in nodegroup to all datanodes or only current nodegroup.
 * If group_name is installation nodegroup, sync all sequences in all other nodegroups.
 * If not, sync all sequences in this nodegroup.
 * If allnodes is true, sync sequences to all datanodes;
 * If allnodes is false, delete sequences in nodes that don't belong to this nodegroup.
 */
static void PgxcGroupSetSeqNodes(const char* group_name, bool allnodes)
{
    Relation depRel;
    TableScanDesc scan;
    HeapTuple tup;
    const char* seqName = NULL;
    const char* nspName = NULL;
    const char* relName = NULL;
    const char* schName = NULL;
    Oid nspoid;
    Oid relowner;
    char query[512];
    errno_t rc = EOK;
    StringInfoData str;
    List* uuids = NIL;
    Oid* members = NULL;
    ExecNodes* exec_nodes = NULL;
    int nmembers;
    Oid group_oid;
    int seq_count = 0;

    str.data = NULL;

    char* installationGroup = PgxcGroupGetInstallationGroup();
    if (installationGroup != NULL && strcmp(installationGroup, group_name) == 0) {
        char* name = NULL;
        Relation pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);

        scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            name = NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name);

            /* skip installation nodegroup. */
            if (strcmp(name, group_name) == 0)
                continue;

            PgxcGroupSetSeqNodes(name, allnodes);
        }

        tableam_scan_end(scan);
        heap_close(pgxc_group_rel, AccessShareLock);

        return;
    }

    if (strcmp(group_name, VNG_OPTION_ELASTIC_GROUP) == 0)
        return;

    group_oid = get_pgxc_groupoid(group_name);
    if (!OidIsValid(group_oid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
    }

    nmembers = get_pgxc_groupmembers(group_oid, &members);
    if (nmembers == u_sess->pgxc_cxt.NumDataNodes) {
        pfree_ext(members);
        return;
    }

    exec_nodes = makeNode(ExecNodes);
    exec_nodes->nodeList = GetNodeGroupNodeList(members, nmembers);
    pfree_ext(members);

    depRel = heap_open(DependRelationId, AccessShareLock);

    scan = tableam_scan_begin(depRel, SnapshotNow, 0, NULL);

    while (HeapTupleIsValid(tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Form_pg_depend deprec = (Form_pg_depend)GETSTRUCT(tup);

        if (deprec->classid == RelationRelationId && deprec->refclassid == RelationRelationId &&
            deprec->objsubid == 0 && deprec->refobjsubid != 0 && deprec->deptype == DEPENDENCY_AUTO &&
            get_rel_relkind(deprec->objid) == RELKIND_SEQUENCE) {
            if (get_pgxc_class_groupoid(deprec->refobjid) != group_oid)
                continue;

            Relation relseq = relation_open(deprec->objid, AccessShareLock);

            HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(deprec->objid));
            if (!HeapTupleIsValid(tp)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for relation %u", deprec->objid)));
            }

            Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
            relName = NameStr(reltup->relname);
            nspoid = reltup->relnamespace;
            relowner = reltup->relowner;

            schName = get_namespace_name(nspoid);
            seqName = quote_identifier(relName);
            nspName = quote_identifier(schName);

            if (str.data == NULL) {
                initStringInfo(&str);
            }

            if (allnodes) {
                int64 uuid;
                int64 start;
                int64 increment;
                int64 maxvalue;
                int64 minvalue;
                int64 cachevalue;
                bool cycle = false;

                get_sequence_params(relseq, &uuid, &start, &increment, &maxvalue, &minvalue, &cachevalue, &cycle);
                Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(uuid), false, true);
                uuids = lappend(uuids, n);

                rc = snprintf_s(query,
                    sizeof(query),
                    sizeof(query) - 1,
                    "CREATE SEQUENCE %s.%s START WITH %ld INCREMENT BY %ld "
                    "MAXVALUE %ld MINVALUE %ld CACHE %ld %s;",
                    nspName,
                    seqName,
                    start,
                    increment,
                    maxvalue,
                    minvalue,
                    cachevalue,
                    cycle ? "CACHE" : " ");
                securec_check_ss(rc, "\0", "\0");

                appendStringInfoString(&str, query);

                if (relowner != GetUserId()) {
                    char* ownerName = GetUserNameFromId(relowner);
                    rc = snprintf_s(query,
                        sizeof(query),
                        sizeof(query) - 1,
                        "ALTER SEQUENCE %s.%s OWNER TO %s;",
                        nspName,
                        seqName,
                        ownerName);
                    securec_check_ss(rc, "\0", "\0");

                    appendStringInfoString(&str, query);
                    pfree_ext(ownerName);
                }
            } else {
                rc = snprintf_s(query,
                    sizeof(query),
                    sizeof(query) - 1,
                    "DROP SEQUENCE IF EXISTS %s.%s CASCADE;",
                    nspName,
                    seqName);
                securec_check_ss(rc, "\0", "\0");

                appendStringInfoString(&str, query);
            }

            ReleaseSysCache(tp);
            relation_close(relseq, AccessShareLock);

            if (seqName != relName)
                pfree_ext(seqName);
            if (nspName != schName)
                pfree_ext(nspName);
            pfree_ext(schName);

            seq_count++;

            if (seq_count >= 65536) {
                seq_count = 0;
                if (allnodes) {
                    CreateRemoteSeqs(str.data, uuids, exec_nodes);
                    list_free_deep(uuids);
                    uuids = NIL;
                } else {
                    DropRemoteSeqs(str.data, exec_nodes);
                }
                pfree_ext(str.data);
            }
        }
    }

    tableam_scan_end(scan);

    heap_close(depRel, AccessShareLock);

    if (str.data != NULL) {
        if (allnodes) {
            CreateRemoteSeqs(str.data, uuids, exec_nodes);
            list_free_deep(uuids);
            uuids = NIL;
        } else {
            DropRemoteSeqs(str.data, exec_nodes);
        }
        pfree_ext(str.data);
    }

    delete_exec_nodes(exec_nodes);
}
#endif

/*
 * PgxcGroupResize()
 *
 * modify attr of pgxc_group for cluster resize
 */
void PgxcGroupResize(const char* src_group_name, const char* dest_group_name)
{
    if (in_logic_cluster()) {
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Resize is not supported in logical cluster mode")));
    }

    /* Permission check */
    Oid group_oid = get_pgxc_groupoid(dest_group_name);
    if (!OidIsValid(group_oid)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
            errmsg("PGXC Group %s: group not defined", dest_group_name)));
    }
    AclResult aclresult = pg_nodegroup_aclcheck(group_oid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !have_createdb_privilege()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("must have sysadmin or createdb or alter privilege to resize cluster node groups")));
    }

    Form_pgxc_group groupattr = (Form_pgxc_group)palloc(sizeof(*groupattr));

    groupattr->in_redistribution = PGXC_REDISTRIBUTION_SRC_GROUP;
    groupattr->is_installation = true;
    PgxcGroupModifyRedisAttr(src_group_name, groupattr);

    groupattr->in_redistribution = PGXC_REDISTRIBUTION_DST_GROUP;
    groupattr->is_installation = false;
    PgxcGroupModifyRedisAttr(dest_group_name, groupattr);

    pfree(groupattr);
}

/*
 * PgxcGroup_Resizing()
 *
 * judge if in physical cluster resize
 */
bool PgxcGroup_Resizing()
{
    Relation relation;
    HeapTuple tuple;
    TableScanDesc scan;
    char in_redistribution;
    bool isfind = false;

    if (in_logic_cluster())
        return false;

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        in_redistribution = ((Form_pgxc_group)GETSTRUCT(tuple))->in_redistribution;
        if (in_redistribution == PGXC_REDISTRIBUTION_DST_GROUP) {
            isfind = true;
            break;
        }
    }
    tableam_scan_end(scan);
    heap_close(relation, RowExclusiveLock);

    return isfind;
}

/*
 * PgxcGroupResizeComplete()
 *
 * modify destgroup's attr in pgxc_group before we delete srcgroup
 */
void PgxcGroupResizeComplete()
{
    Relation relation;
    HeapTuple tuple;
    HeapTuple newtuple;
    TableScanDesc scan;
    char in_redistribution;
    bool nulls[Natts_pgxc_group];
    Datum values[Natts_pgxc_group];
    bool replaces[Natts_pgxc_group];
    errno_t rc;

    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        in_redistribution = ((Form_pgxc_group)GETSTRUCT(tuple))->in_redistribution;
        if (in_redistribution == PGXC_REDISTRIBUTION_DST_GROUP) {
            break;
        }
    }
    if (!HeapTupleIsValid(tuple))  // should not happen
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("No destgroup in resize process")));

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[Anum_pgxc_group_in_redistribution - 1] = true;
    values[Anum_pgxc_group_in_redistribution - 1] = CharGetDatum(PGXC_NON_REDISTRIBUTION_GROUP);
    replaces[Anum_pgxc_group_kind - 1] = true;
    values[Anum_pgxc_group_kind - 1] = CharGetDatum(PGXC_GROUPKIND_INSTALLATION);
    replaces[Anum_pgxc_group_is_installation - 1] = true;
    values[Anum_pgxc_group_is_installation - 1] = BoolGetDatum(true);

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);

    simple_heap_update(relation, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(relation, newtuple);

    tableam_scan_end(scan);
    heap_freetuple(newtuple);
    heap_close(relation, RowExclusiveLock);
}

/*
 * PgxcGroupAddNodes():
 *
 * add some nodes to nodegroup.
 */
static void PgxcGroupAddNodes(const char* group_name, List* nodes)
{
    Oid group_oid;
    HeapTuple tup;
    oidvector* gmember = NULL;
    ListCell* cell = NULL;
    char group_kind;
    int count;
    int node_count;
    Oid* oids = NULL;
    errno_t rc;
    bool need_free = false;

    /* Check node group is preserved group name */
    if (!ng_is_valid_group_name(group_name)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s is invalid.", group_name)));
    }

    group_oid = get_pgxc_groupoid(group_name);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %d: group not defined", group_oid)));

    group_kind = get_group_kind(tup);
    if (group_kind == PGXC_GROUPKIND_INSTALLATION) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("PGXC GROUP %s: do not support add nodes to installation node group.", group_name)));
    } else if (group_kind != PGXC_GROUPKING_ELASTIC) {
        elog(WARNING,
            "Change nodelist of NodeGroup directly maybe result in data inconsistency,"
            "confirm this is what you want.");
    }

    gmember = get_group_member_ref(tup, &need_free);
    Assert(gmember != NULL);
    count = gmember->dim1 + nodes->length;
    node_count = gmember->dim1;

    oids = (Oid*)palloc(count * sizeof(Oid));
    if (gmember->dim1 > 0) {
        rc = memcpy_s(oids, count * sizeof(Oid), gmember->values, gmember->dim1 * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }

    ReleaseSysCache(tup);
    if (need_free)
        pfree_ext(gmember);

    foreach (cell, nodes) {
        int i;
        char* node_name = strVal(lfirst(cell));
        Oid noid = get_pgxc_nodeoid(node_name);

        PgxcCheckOid(noid, node_name);

        for (i = 0; i < node_count; i++) {
            if (noid == oids[i]) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("PGXC node %s: already exist in node group %s or duplicate in nodelist.",
                            node_name,
                            group_name)));
            }
        }
        oids[node_count++] = noid;
    }

    oids = SortRelationDistributionNodes(oids, node_count);
    gmember = buildoidvector(oids, node_count);
    pfree_ext(oids);

    PgxcChangeGroupMember(group_oid, gmember);
    pfree_ext(gmember);
}

/*
 * PgxcGroupDeleteNodes():
 *
 * delete some nodes to nodegroup.
 */
static void PgxcGroupDeleteNodes(const char* group_name, List* nodes)
{
    Oid group_oid;
    HeapTuple tup;
    oidvector* gmember = NULL;
    ListCell* cell = NULL;
    char group_kind;
    int node_count;
    Oid* oids = NULL;
    bool need_free = false;
    errno_t rc;

    /* Check node group is preserved group name */
    if (!ng_is_valid_group_name(group_name)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s is invalid.", group_name)));
    }

    group_oid = get_pgxc_groupoid(group_name);

    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Group %d: group not defined", group_oid)));

    group_kind = get_group_kind(tup);
    if (group_kind == PGXC_GROUPKIND_INSTALLATION) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("PGXC GROUP %s: do not support delete nodes from installation node group.", group_name)));
    } else if (group_kind != PGXC_GROUPKING_ELASTIC) {
        elog(WARNING,
            "Change nodelist of NodeGroup directly maybe result in data inconsistency,"
            "confirm this is what you want.");
    }

    gmember = get_group_member_ref(tup, &need_free);

    if (gmember->dim1 > 0) {
        oids = (Oid*)palloc(gmember->dim1 * sizeof(Oid));
        rc = memcpy_s(oids, gmember->dim1 * sizeof(Oid), gmember->values, gmember->dim1 * sizeof(Oid));
        securec_check(rc, "\0", "\0");
    }
    node_count = gmember->dim1;
    ReleaseSysCache(tup);

    if (need_free)
        pfree_ext(gmember);

    foreach (cell, nodes) {
        int i;
        bool find_node = false;
        char* node_name = strVal(lfirst(cell));
        Oid noid = get_pgxc_nodeoid(node_name);

        PgxcCheckOid(noid, node_name);

        for (i = 0; i < node_count; i++) {
            Assert(oids != NULL);
            if (noid == oids[i]) {
                oids[i] = InvalidOid;
                find_node = true;
                continue;
            }
            if (find_node) {
                oids[i - 1] = oids[i];
            }
        }

        if (!find_node)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("PGXC node %s: does not exist in node group %s or duplicate in nodelist.",
                        node_name,
                        group_name)));

        node_count--;
    }

    if (node_count == 0 && group_kind != PGXC_GROUPKING_ELASTIC) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC group %s: can not delete all nodes.", group_name)));
    }
    gmember = buildoidvector(oids, node_count);
    if (oids != NULL)
        pfree_ext(oids);

    PgxcChangeGroupMember(group_oid, gmember);
    pfree_ext(gmember);

}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * PgxcNodeGroupsAlter():
 *
 * Alter a PGXC node group
 */
void PgxcGroupAlter(AlterGroupStmt* stmt)
{
    Oid group_oid = get_pgxc_groupoid(stmt->group_name);
    if (!OidIsValid(group_oid)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
            errmsg("PGXC Group %s: group not defined", stmt->group_name)));
    }
    /* Permission check for users to alter node group */
    AclResult aclresult = pg_nodegroup_aclcheck(group_oid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !have_createdb_privilege()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("must have sysadmin or createdb or alter privilege to alter cluster node groups")));
    }

    if (!u_sess->attr.attr_common.xc_maintenance_mode && stmt->alter_type != AG_SET_DEFAULT) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Alter node group can only be executed in maintenance mode.")));
    }

    switch (stmt->alter_type) {
        case AG_SET_DEFAULT:
            PgxcGroupSetDefault(stmt->group_name);
            break;
        case AG_SET_VCGROUP:
            PgxcGroupSetVCGroup(stmt->group_name, stmt->install_name);
            break;
        case AG_SET_NOT_VCGROUP:
            PgxcGroupSetNotVCGroup(stmt->group_name);
            break;
        case AG_SET_RENAME:
            PgxcGroupRename(stmt->group_name, stmt->install_name);
            break;
        case AG_SET_TABLE_GROUP:
            PgxcChangeTableGroupName(stmt->group_name, stmt->install_name);
            break;
        case AG_SET_BUCKETS:
            PgxcCopyBucketsFromNew(stmt->group_name, stmt->install_name);
            break;
        case AG_ADD_NODES:
            PgxcGroupAddNodes(stmt->group_name, stmt->nodes);
            break;
        case AG_DELETE_NODES:
            PgxcGroupDeleteNodes(stmt->group_name, stmt->nodes);
            break;
        case AG_RESIZE_GROUP:
            PgxcGroupResize(stmt->group_name, stmt->install_name);
            break;
        case AG_CONVERT_VCGROUP:
            PgxcGroupConvertVCGroup(stmt->group_name, stmt->install_name);
            break;
        case AG_SET_SEQ_ALLNODES:
            PgxcGroupSetSeqNodes(stmt->group_name, true);
            break;
        case AG_SET_SEQ_SELFNODES:
            PgxcGroupSetSeqNodes(stmt->group_name, false);
            break;
        default:
            break;
    }
}
#endif
/*
 * PgxcNodeGroupsRemove():
 *
 * Remove a PGXC node group
 */
void PgxcGroupRemove(DropGroupStmt* stmt)
{
    Relation relation;
    HeapTuple tup;
    const char* group_name = stmt->group_name;
    Oid group_oid = get_pgxc_groupoid(group_name);
    char group_kind;
    char in_redistribution;
    oidvector* node_oids = NULL;
    ExecNodes* exec_nodes = NULL;
    bool ignore_pmk = false;

    /* Check if group exists */
    if (!OidIsValid(group_oid)) {
        if (IS_PGXC_DATANODE)
            return;

        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
    }

    /* Permission check for users to remove cluster node group */
    AclResult aclresult = pg_nodegroup_aclcheck(group_oid, GetUserId(), ACL_DROP);
    if (aclresult != ACLCHECK_OK && !have_createdb_privilege()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("must have sysadmin or createdb or drop privilege to remove cluster node groups")));
    }

    /* Check node group is preserved group name */
    if (group_name && !ng_is_valid_group_name(group_name)) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("NodeGroup name %s can not be preserved group name", group_name)));
    }

    if (IS_PGXC_DATANODE) {
        relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
        tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);
        if (!HeapTupleIsValid(tup)) /* should not happen */
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", group_name)));

        simple_heap_delete(relation, &tup->t_self);
        ReleaseSysCache(tup);
        deleteSharedDependencyRecordsFor(PgxcGroupRelationId, group_oid, 0);
        heap_close(relation, NoLock);
        CommandCounterIncrement();
        RemoveNodeGroupInfoInHTAB(group_name);
        return;
    }

    /* if resize modify destgroup's attr in pgxc_group */
    char* group = PgxcGroupGetInRedistributionGroup();
    if (PgxcGroup_Resizing() && group && (0 == strcmp(group_name, group))) {
        PgxcGroupResizeComplete();
    }

    /* Check if the group is empty */
    ignore_pmk = in_logic_cluster();
    if (!CanPgxcGroupRemove(group_oid, ignore_pmk)) {
        ereport(ERROR,
            (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                errmsg("cannot drop '%s' because other objects depend on it", group_name)));
    }

    /* Delete the pgxc_group tuple */
    relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
    tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("PGXC Group %s: group not defined", group_name)));

    in_redistribution = ((Form_pgxc_group)GETSTRUCT(tup))->in_redistribution;

    if (stmt->src_group_name != NULL) {
        if (!in_logic_cluster()) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Do not support distribute from clause for non-logic cluster group!")));
        }

        if (!u_sess->attr.attr_common.xc_maintenance_mode) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("DROP NODE GROUP ... DISTRIBUTE FROM can only be executed in maintenance mode.")));
        }

        if (in_redistribution != PGXC_REDISTRIBUTION_DST_GROUP) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("PGXC Group %s: group is not redistributed "
                           "distination group, can not be dropped",
                        group_name)));
        }
    }

    /* Check group is installation group, we don't allow drop installation nodegroup.
     *  But if we are redistributing the logic group or the whole cluster,
     *  we can drop installation group.
     */
    group_kind = get_group_kind(tup);

    if (group_kind == PGXC_GROUPKIND_INSTALLATION && in_redistribution == PGXC_NON_REDISTRIBUTION_GROUP) {
        ereport(ERROR,
            (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                errmsg("PGXC Group %s: group is installation group, can not be dropped", group_name)));
    } else if (group_kind == PGXC_GROUPKING_ELASTIC) {
        /* We can not drop elastic group until all logic cluster is dropped. */
        if (exist_logic_cluster(relation)) {
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg(
                        "PGXC Group %s: cannot drop elastic group because other logic clusters exist.", group_name)));
        }
    } else if (group_kind == PGXC_GROUPKIND_LCGROUP) {
        bool isNull = true;
        oidvector* gmember = NULL;
        Oid roleid;
        bool need_free = false;

        /* We can not drop logic cluster when some users are attached to the logic cluster. */
        roleid = PgxcGetFirstRoleId(group_oid);
        if (roleid != InvalidOid) {
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("cannot drop '%s' because at least one role %u depend on it", group_name, roleid)));
        }

        gmember = get_group_member_ref(tup, &need_free);

        if (in_redistribution == PGXC_REDISTRIBUTION_DST_GROUP || in_redistribution == PGXC_REDISTRIBUTION_SRC_GROUP) {
            if (stmt->src_group_name != NULL) {
                if (stmt->to_elastic_group)
                    node_oids = PgxcGetRedisNodes(relation, PGXC_REDISTRIBUTION_SRC_GROUP);

                if (gmember->dim1 > 0) {
                    text* groupbucket =
                        (text*)heap_getattr(tup, Anum_pgxc_group_buckets, RelationGetDescr(relation), &isNull);

                    if (isNull)
                        groupbucket = NULL;

                    PgxcUpdateRedistSrcGroup(relation, gmember, groupbucket, stmt->src_group_name);
                }
            } else if (stmt->to_elastic_group) {
                node_oids = PgxcGetRedisNodes(relation, in_redistribution);
            }
        } else {
            if (gmember->dim1 > 0 && stmt->to_elastic_group) {
                node_oids = buildoidvector(gmember->values, gmember->dim1);
            }
        }

        if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && in_logic_cluster()) {
            exec_nodes = create_exec_nodes(gmember);
        }

        if (need_free)
            pfree_ext(gmember);
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    /*
     * Removethe cache entry from BucketMapCache
     *
     * Note: As cache entry removal is not rollback-able, we still do cache remove here,
     * because becache the bucketmap-cache mechanism use a lazy-setup behavior to make
     * its persistance, so even in case of cache-remove but rollback transaction, the later
     * cache-read can bring the cache content back.
     *
     * So, it is safe to do cache remove here anyway
     */
    BucketMapCacheRemoveEntry(group_oid);

    deleteSharedDependencyRecordsFor(PgxcGroupRelationId, group_oid, 0);

    if (group_kind == PGXC_GROUPKIND_LCGROUP && node_oids != NULL) {
        bool nulls[Natts_pgxc_group];
        Datum values[Natts_pgxc_group];
        bool replaces[Natts_pgxc_group];
        HeapTuple newtuple;
        oidvector* gmember = NULL;
        oidvector* elastic_nodes = NULL;
        errno_t rc;
        bool need_free = false;

        tup = SearchSysCache1(PGXCGROUPNAME, CStringGetDatum(VNG_OPTION_ELASTIC_GROUP));
        if (!HeapTupleIsValid(tup)) /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("PGXC Group %s: group not defined", VNG_OPTION_ELASTIC_GROUP)));

        gmember = get_group_member_ref(tup, &need_free);

        elastic_nodes = node_oids;
        if (gmember->dim1 > 0) {
            elastic_nodes = oidvector_add(gmember, node_oids);
            pfree(node_oids);
        }

        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
        securec_check(rc, "\0", "\0");

        replaces[Anum_pgxc_group_members - 1] = true;
        values[Anum_pgxc_group_members - 1] = PointerGetDatum(elastic_nodes);

        newtuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);

        simple_heap_update(relation, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(relation, newtuple);
        heap_freetuple(newtuple);

        ReleaseSysCache(tup);
        pfree(elastic_nodes);

        if (need_free)
            pfree_ext(gmember);
    }

    /*
     * relation close with NoLock to let nodegroup-removal process hold the lock longer (until
     * xact commit/abort) to prevent other concurrent TO-GROUP create table request(with ShareLock)
     * run into concurrent update error.
     */
    heap_close(relation, NoLock);

    /* remove from hash table */
    if (stmt->src_group_name == NULL) {
        RemoveNodeGroupInfoInHTAB(group_name);
    }

    /* Send to Datanode */
    if (exec_nodes != NULL) {
        exec_in_datanode(group_name, exec_nodes, false);
        delete_exec_nodes(exec_nodes);
    }
}

/*
 * PgxcGroupGetFirstLogicCluster()
 *
 * Return first logic cluster.caller need to free the return value
 */
char* PgxcGroupGetFirstLogicCluster()
{
    char* group_name = NULL;
    Relation pgxc_group_rel = NULL;
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple tup = NULL;
    char group_kind;

    pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    if (!pgxc_group_rel) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("can not open pgxc_group")));
    }

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTGreaterStrategyNumber, F_OIDGT, ObjectIdGetDatum(0));

    scan = systable_beginscan(pgxc_group_rel, PgxcGroupOidIndexId, true, NULL, 1, skey);

    while ((tup = systable_getnext(scan)) != NULL) {
        group_kind = get_group_kind(tup);
        if (group_kind == 'v') {
            group_name = pstrdup(NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name));
            break;
        }
    }

    systable_endscan(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    return group_name;
}

/*
 * IsLogicClusterRedistributed()
 *
 * Judge whether logic cluster with name group_name is redistributed.
 */
bool IsLogicClusterRedistributed(const char* group_name)
{
    if (in_logic_cluster()) {
        const char* redist_group_name = PgxcGroupGetInRedistributionGroup();
        if (redist_group_name && strcmp(group_name, redist_group_name) == 0)
            return true;
    }
    return false;
}

/*
 * PgxcGroupGetCurrentLogicCluster()
 *
 * Get current logic cluster name, caller need to free the string memory.
 */

char* PgxcGroupGetCurrentLogicCluster()
{
    char* group_name = NULL;

    if (!in_logic_cluster()) {
        if (strcmp(u_sess->attr.attr_sql.default_storage_nodegroup, INSTALLATION_MODE) == 0) {
            group_name = PgxcGroupGetInstallationGroup();
            if (group_name != NULL)
                group_name = pstrdup(group_name);
        } else {
            group_name = pstrdup(u_sess->attr.attr_sql.default_storage_nodegroup);
        }
    } else {
        group_name = (char*)get_current_lcgroup_name();
        if (group_name != NULL)
            group_name = pstrdup(group_name);

        /*
         *  If group_name is null, the user is superuser or not attached to nodegroup.
         *  For superuser, we allow create tables in default storage nodegroup;
         */
        if (group_name == NULL && superuser()) {
            if (strcmp(u_sess->attr.attr_sql.default_storage_nodegroup, INSTALLATION_MODE) == 0) {
                group_name = PgxcGroupGetFirstLogicCluster();
            } else {
                Oid group_oid = get_pgxc_groupoid(u_sess->attr.attr_sql.default_storage_nodegroup);
                if (!OidIsValid(group_oid))
                    ereport(ERROR,
                        (errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("default_storage_nodegroup %s not defined.",
                                u_sess->attr.attr_sql.default_storage_nodegroup)));

                char group_kind = get_pgxc_groupkind(group_oid);
                if (group_kind == PGXC_GROUPKIND_INSTALLATION) {
                    group_name = PgxcGroupGetFirstLogicCluster();
                } else {
                    group_name = pstrdup(u_sess->attr.attr_sql.default_storage_nodegroup);
                }
            }
        }

        if (group_name != NULL && IsLogicClusterRedistributed(group_name)) {
            char* exec_group = PgxcGroupGetStmtExecGroupInRedis();
            if (exec_group != NULL) {
                pfree(group_name);
                group_name = exec_group;
            }
        }
    }
    return group_name;
}

/*
 * PgxcGroupGetRedistDestGroupOid()
 *
 */
Oid PgxcGroupGetRedistDestGroupOid()
{
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;
    Oid group_oid = InvalidOid;

    /* We do not care about group information in datanodes */
    if (!isRestoreMode && IS_PGXC_DATANODE) {
        return InvalidOid;
    }
    PgxcOpenGroupRelation(&pgxc_group_rel);

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pgxc_group_in_redistribution, RelationGetDescr(pgxc_group_rel), &isNull);

        if (PGXC_REDISTRIBUTION_DST_GROUP == DatumGetChar(datum)) {
            group_oid = HeapTupleGetOid(tup);
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    return group_oid;
}

/* PgxcGroupGetStmtExecGroupInRedis()
 *
 * During the redistribution process
 * determine create table on new or old cluster
 */
char* PgxcGroupGetStmtExecGroupInRedis()
{
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    bool isNull = false;
    Datum datum;
    char* group_name = NULL;

    oidvector* destgmember = NULL;
    oidvector* srcgmember = NULL;
    char* destgname = NULL;
    char* srcgname = NULL;
    bool need_free_dst = false;
    bool need_free_src = false;

    /* We do not care about group information in datanodes */
    if (!isRestoreMode && IS_PGXC_DATANODE) {
        return NULL;
    }
    PgxcOpenGroupRelation(&pgxc_group_rel);

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pgxc_group_in_redistribution, RelationGetDescr(pgxc_group_rel), &isNull);

        if (PGXC_REDISTRIBUTION_DST_GROUP == DatumGetChar(datum)) {
            if (destgmember != NULL && strcmp(destgname, NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name)) != 0) {
                ereport(
                    ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("more than one node group in_redistribution='t'")));
            }
            destgmember = get_group_member_ref(tup, &need_free_dst);
            destgname = NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name);
        } else if (PGXC_REDISTRIBUTION_SRC_GROUP == DatumGetChar(datum)) {
            if (srcgmember != NULL && strcmp(srcgname, NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name)) != 0) {
                ereport(
                    ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("more than one node group in_redistribution='y'")));
            }
            srcgmember = get_group_member_ref(tup, &need_free_src);
            srcgname = NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name);
        }
        if (destgmember != NULL && srcgmember != NULL) {
            if (oidvector_eq(destgmember, srcgmember))
                group_name = pstrdup((const char*)srcgname);
            else
                group_name = pstrdup((const char*)destgname);
            break;
        } else if (srcgmember != NULL) {
            group_name = pstrdup((const char*)srcgname);
        }
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    if (need_free_dst)
        pfree_ext(destgmember);
    if (need_free_src)
        pfree_ext(srcgmember);

    return group_name;
}

/*
 * Name: BucketMapCacheAddEntry()
 *
 * Brief: add a bucketmap cache element into global BucketMapCache
 *
 * Parameters:
 *	@in groupoid: bucket cache entry key
 *	@in groupanme_datum: the datum of pgxc_group.groupname
 *	@in bucketmap_datum: the datum of pgxc_group.bucketmap
 *
 * Return: void
 */
static void BucketMapCacheAddEntry(Oid groupoid, Datum groupanme_datum, Datum bucketmap_datum, ItemPointer ctid)
{
    /* BucketmapCache and its underlying element is allocated in u_sess.MEMORY_CONTEXT_EXECUTOR */
    MemoryContext oldcontext = MemoryContextSwitchTo(LocalGBucketMapMemCxt());

    /* Create bucketmap element */
    BucketMapCache* bmc = (BucketMapCache*)palloc0(sizeof(BucketMapCache));

    /* Assign groupname & map content in Bucketmap Cache */
    bmc->groupoid = groupoid;
    bmc->groupname = (char*)palloc0(NAMEDATALEN);
    errno_t rc = strcpy_s(bmc->groupname, NAMEDATALEN, (const char*)DatumGetCString(groupanme_datum));
    securec_check(rc, "\0", "\0");
    bmc->bucketmap = (uint2*)palloc0(BUCKETDATALEN * sizeof(uint2));
    ItemPointerCopy(ctid, &bmc->ctid);
    text_to_bktmap((text*)bucketmap_datum, bmc->bucketmap, &bmc->bucketcnt);

    elog(DEBUG2, "Add [%s][%u]'s bucketmap to BucketMapCache", bmc->groupname, groupoid);

    /* Insert element into bucketmap */
    AppendLocalRelCacheGBucketMapCache((ListCell *)bmc);

    /* Swith back to original memory context */
    MemoryContextSwitchTo(oldcontext);
}

/*
 * Name: BucketMapCacheRemoveEntry()
 *
 * Brief: remove a bucketmap cache element into global BucketMapCache
 *
 * Parameters:
 *	@in groupoid: bucket cache entry key that needs to be removed
 *
 * Return: void
 */
static void BucketMapCacheRemoveEntry(Oid groupoid)
{
    if (LocalRelCacheGBucketMapCache() == NIL) {
        /*
         * We may run into here, when a new node group is drop but no table access
         * of its table happened.
         */
        elog(LOG, "Remove element from BucketMapCache with oid %u, but BucketMapCache is not setup yet", groupoid);

        return;
    }

    ListCell* cell = NULL;
    ListCell* prev = NULL;
    ListCell* next = NULL;
    bool found = false;

    for (cell = list_head(LocalRelCacheGBucketMapCache()); cell; cell = next) {
        BucketMapCache* bmc = (BucketMapCache*)lfirst(cell);
        next = lnext(cell);
        if (bmc->groupoid == groupoid) {
            elog(LOG, "Remove [%s][%u]'s bucketmap From BucketMapCache", bmc->groupname, groupoid);

            found = true;
            pfree_ext(bmc->bucketmap);
            pfree_ext(bmc->groupname);

            /* Remove it from global bucketmap cache list */
            DeteleLocalRelCacheGBucketMapCache(cell, prev);
            pfree_ext(bmc);
            break;
        } else {
            prev = cell;
        }
    }

    /* Report LOG if the to-be removed bucketmap entry is not found from BucketMapCache */
    if (!found) {
        elog(LOG, "Try to remove bucketmap cache entry with group oid %u, but it is not found", groupoid);
    }

    return;
}

/*
 * Name: BucketMapCacheUpdate()
 *
 * Brief: update bucketmap for given groupoid in the global BucketMapCache
 *
 * Parameters:
 *	@in bmc: BucketMapCache that needs to be update
 *
 * Return: void
 */
static void BucketMapCacheUpdate(BucketMapCache* bmc)
{
    bool isNull = true;
    errno_t rc = 0;

    /*get new bucketmap and group name by groupoid*/
    Relation rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    HeapTuple htup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(bmc->groupoid), 0, 0, 0);

    if (!HeapTupleIsValid(htup)) { /* should not happen */
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Bucketmap is not found with given groupoid %u", bmc->groupoid)));
    }

    Datum groupname_datum = heap_getattr(htup, Anum_pgxc_group_name, RelationGetDescr(rel), &isNull);
    Assert(!isNull);
    Datum bucketmap_datum = heap_getattr(htup, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);
    Assert(!isNull);

    elog(LOG, "Update [%s][%u]'s bucketmap in BucketMapCache", bmc->groupname, bmc->groupoid);

    ItemPointerCopy(&(htup->t_self), &bmc->ctid);
    /* update bucketmap and groupname but don't repalloc memory */
    rc = strcpy_s(bmc->groupname, NAMEDATALEN, (const char*)DatumGetCString(groupname_datum));
    securec_check(rc, "\0", "\0");
    text_to_bktmap((text*)bucketmap_datum, bmc->bucketmap, &bmc->bucketcnt);

    ReleaseSysCache(htup);
    heap_close(rel, AccessShareLock);
}

/*
 * Name: ClearInvalidBucketMapCache()
 *
 * Brief: clear invalid cell in LocalRelCacheGBucketMapCache
 *
 * Parameters:
 *    none
 *
 * Return:
 *    none
 */
static void ClearInvalidBucketMapCache(void)
{
    ListCell* cell = NULL;
    ListCell* next = NULL;

    for (cell = list_head(LocalRelCacheGBucketMapCache()); cell; cell = next) {
        next = lnext(cell);
        BucketMapCache* bmc = (BucketMapCache*)lfirst(cell);
        HeapTuple tuple = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(bmc->groupoid));

        if (!HeapTupleIsValid(tuple)) {
            BucketMapCacheRemoveEntry(bmc->groupoid);
            continue;
        }
        ReleaseSysCache(tuple);
    }
    if ((unsigned int)list_length(LocalRelCacheGBucketMapCache()) >= LocalRelCacheMaxBucketMapSize()) {
        EnlargeLocalRelCacheMaxBucketMapSize(2);
    }
}

/*
 * Name: BucketMapCacheDestroy()
 *
 * Brief: destory the bucketmap cache
 *
 * Parameters:
 *    none
 *
 * Return:
 *    none
 */
void BucketMapCacheDestroy(void)
{
    /*
     * In current BuckeMapCache implementation, destroy is not necessary to implement as
     * it persists with its belonging gaussdb session.
     */
    Assert(false);
}

/*
 * Name: BucketMapCacheGetBucketmap(Oid)
 *
 * Brief: fetch bucketmap content with given groupoid, and populate the bucketmap cache
 *        if we do not found in BucketMapCache
 *
 * Parameters:
 *    @in groupoid: groupoid of bucketmap that need to fetch
 *
 * Return:
 *    bucketmap that is found
 */
uint2* BucketMapCacheGetBucketmap(Oid groupoid, int *bucketlen)
{
    Assert(groupoid != InvalidOid);

    if (LocalRelCacheGBucketMapCache() == NIL) {
        elog(DEBUG2, "Global bucketmap cache is not setup");
    }
    if ((unsigned int)list_length(LocalRelCacheGBucketMapCache()) >= LocalRelCacheMaxBucketMapSize()) {
        ClearInvalidBucketMapCache();
    }
    while (LocalRelCacheMaxBucketMapSize() / 2 >
               (unsigned int)list_length(LocalRelCacheGBucketMapCache()) &&
           LocalRelCacheMaxBucketMapSize() / 2 > BUCKET_MAP_SIZE) {
        EnlargeLocalRelCacheMaxBucketMapSize(0.5);
    }

    ListCell* cell = NULL;
    uint2* bucketmap = NULL;

    /* Search bucketmap from cache */
    foreach (cell, LocalRelCacheGBucketMapCache()) {
        BucketMapCache* bmc = (BucketMapCache*)lfirst(cell);

        if (bmc->groupoid == groupoid) {
            /*
             * Note: Here, we still have to check if given groupoid could be found from
             * systable and verify it must not be updated, as BucketMapCache is thread
             * local and not transactional protected, for example "group1" is setup in
             * BucketMapCache but "group1" is dropped or its bucketmap content changed in
             * another gaussdb session, in this case we have to remove it from BucketMapCache
             * and try to rebuild it.
             *
             * Here using a light-weight check to verify the ctid of pgxc_group's tuple
             */
            HeapTuple tup = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(groupoid));
            if (!HeapTupleIsValid(tup)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed on node group %u", groupoid)));
            }

            if (unlikely(ItemPointerCompare(&(bmc->ctid), &(tup->t_self)) != 0)) {
                BucketMapCacheUpdate(bmc);
            }
            bucketmap = bmc->bucketmap;
            *bucketlen = bmc->bucketcnt;

            ReleaseSysCache(tup);

            /* Break, once we found the desired bucketmap or have to rebuild the entry */
            break;
        }
    }

    /* When cache is missing, try to populate it from systable */
    if (bucketmap == NULL) {
        bool isNull = true;
        Relation rel = heap_open(PgxcGroupRelationId, AccessShareLock);
        HeapTuple htup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(groupoid), 0, 0, 0);

        if (!HeapTupleIsValid(htup)) { /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Bucketmap is not found with given groupoid %u", groupoid)));
        }

        Datum groupname_datum = heap_getattr(htup, Anum_pgxc_group_name, RelationGetDescr(rel), &isNull);

        /*
         * Assert to check groupname is not null as group_name in pgxc_group is defined
         * as NOT NULL constraint.
         */
        Assert(groupoid != InvalidOid && !isNull);
        Datum bucketmap_datum = heap_getattr(htup, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);

        if (!isNull) {
            /* Add it into Bucketmap Cache */
            BucketMapCacheAddEntry(groupoid, groupname_datum, bucketmap_datum, &(htup->t_self));
        }

        ReleaseSysCache(htup);
        heap_close(rel, AccessShareLock);

        /* Re-search from bucketmap cache */
        bucketmap = BucketMapCacheGetBucketmap(groupoid, bucketlen);

        Assert(bucketmap != NULL);
    }

    return bucketmap;
}

/*
 * Name: BucketMapCacheGetBucketmap(groupname)
 *
 * Brief: similar with its oid version but the input is the groupname
 *
 * Parameters:
 *    @in groupname: name of group whose bucketmap that need to fetch
 *
 * Return:
 *    bucketmap that is found
 */
uint2* BucketMapCacheGetBucketmap(const char* groupname, int *bucketlen)
{
    Assert(groupname != NULL);

    Oid groupoid = get_pgxc_groupoid(groupname, false /* Missing not OK */);
    return BucketMapCacheGetBucketmap(groupoid, bucketlen);
}

/*
 * PgxcGroupGetInstallationGroup()
 *
 * Return installation group to caller
 */
char* PgxcGroupGetInstallationGroup()
{
    char* group_name = NULL;
    char* installation_node_group = NULL;
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;
    int group_count = 0;
    oidvector* gmember = NULL;

    /* We do not care about group information in datanodes */
    if (!isRestoreMode && IS_PGXC_DATANODE) {
        return NULL;
    }

    /*
     * If current installation node group is catched, return it directly instead of
     * fetch it from pgxc_group.
     */
    if (t_thrd.pgxc_cxt.current_installation_nodegroup != NULL) {
        return t_thrd.pgxc_cxt.current_installation_nodegroup;
    }
    PgxcOpenGroupRelation(&pgxc_group_rel);

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        bool need_free = false;
        gmember = get_group_member_ref(tup, &need_free);

        group_count++;

        /* Fetch group name */
        {
            Datum group_name_datum = heap_getattr(tup, Anum_pgxc_group_name, RelationGetDescr(pgxc_group_rel), &isNull);
            group_name = (char*)pstrdup((const char*)DatumGetCString(group_name_datum));
        }

        /* Determin if the node group is marked as installation */
        TupleDesc tupleDesc = RelationGetDescr(pgxc_group_rel);
        if (tupleDesc->natts >= Anum_pgxc_group_is_installation) {
            datum = heap_getattr(tup, Anum_pgxc_group_is_installation, tupleDesc, &isNull);
        } else {
            datum = BoolGetDatum(true);
        }

        if (DatumGetBool(datum)) {
            installation_node_group = group_name;
            break;
        }
        if (need_free)
            pfree_ext(gmember);
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    /*
     * If installation node group is not found, we are returning the only nodegroup already
     * defined in pgxc_group as installation nodegroup to maximumly *compatible* with existing
     * none multi-nodegroup system
     */
    if (installation_node_group == NULL && group_count == 1) {
        set_current_installation_nodegroup(group_name);
        pfree(group_name);

        return t_thrd.pgxc_cxt.current_installation_nodegroup;
    }

    if (installation_node_group != NULL) {
        set_current_installation_nodegroup(installation_node_group);
        pfree(installation_node_group);

        return t_thrd.pgxc_cxt.current_installation_nodegroup;
    }

    return NULL;
}

/*
 * Brief: generate consistant-hash bucketmap for new installation nodegroup, this case
 * happens in cluster expansion(shunk)
 *
 * Input
 *   @stmt: NodeGroup creation handler
 *   @nodes_array: oid array of new node group
 *   @pgxc_group_rel: relation handler of pgxc_group, opened in PgxcGroupCreate
 *   @tuple: tuple of pgxc_group of old installation group, its bucketmap is used
 *           as input for new bucket generation
 *   @bucket_prt: output paremeter for new generate node group
 */
static void generateConsistentHashBucketmap(CreateGroupStmt* stmt, oidvector* nodes_array, Relation pgxc_group_rel,
    HeapTuple tuple, uint2* bucket_ptr, int bucketmap_mode)
{
    int member_count = list_length(stmt->nodes);
    int i = 0;
    int2 *node_count_array = NULL;
    int2 *node_count_bucket = NULL;
    int2 *new_node_array = NULL;
    int skip_bucket, target_bucket, tmp_skip_bucket;
    uint2* old_bucket_map = NULL;
    int old_bucket_cnt = 0;
    int j, new_member_count, old_member_count, b_node, b_nnode;
    int len = BUCKETDATALEN * sizeof(uint2);
    text* groupbucket = NULL;
    bool isNull = false;
    bool need_free = false;
    oidvector* gmember = NULL;
    errno_t rc = 0;

    /* in dump and restore, restore the bucket info to new CN by the old CN */
    if (stmt->buckets != NIL)
        getBucketsFromBucketList(stmt->buckets, member_count, bucket_ptr);
    else {
        /* Generate default bucketmap */
        if (bucketmap_mode == BUCKETMAP_MODE_DEFAULT) {
            for (i = 0; i < BUCKETDATALEN; i++) {
                bucket_ptr[i] = i % member_count; /* initialized as hash distribution for each bucket */
            }

            return;
        }

        // compute bucket allocation for new node group (after adding some nodes)
        gmember = get_group_member_ref(tuple, &need_free);
        old_member_count = gmember->dim1;
        new_member_count = member_count - old_member_count;

        if (new_member_count == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("new node group must contain different number of nodes with before!")));
        } else if (new_member_count < 0) {
            contractBucketMap(pgxc_group_rel, tuple, nodes_array, bucket_ptr);
        } else {
            //  We'd better to refact this part as a new function.
            node_count_array = (int2*)palloc(old_member_count * sizeof(int2));
            skip_bucket = member_count;
            target_bucket = BUCKETDATALEN / member_count;

            old_bucket_map = (uint2*)palloc0(len);
            groupbucket =
                (text*)heap_getattr(tuple, Anum_pgxc_group_buckets, RelationGetDescr(pgxc_group_rel), &isNull);
            if (!isNull) {
                text_to_bktmap(groupbucket, old_bucket_map, &old_bucket_cnt);
            }

            new_node_array = (int2*)palloc(new_member_count * sizeof(int2));
            rc = memcpy_s(bucket_ptr, len, old_bucket_map, len);
            securec_check(rc, "\0", "\0");

            for (i = 0; i < new_member_count; i++)
                new_node_array[i] = i + old_member_count;
            // number of buckets in each node
            node_count_bucket = (int2*)palloc0(member_count * sizeof(int2));
            for (i = 0; i < BUCKETDATALEN; i++) {
                Assert(bucket_ptr[i] < member_count);
                node_count_bucket[bucket_ptr[i]]++;
            }

            /***************************************************************/
            /* Algorithm to evenly distribute buckets:                     */
            /* For each new node, it should get 1/member_count of total    */
            /* buckets and better those buckets are evenly from old nodes. */
            /* So for each new node, we loop the bucket list and pick up   */
            /* one bucket if its original node has node_count_array[b_node] */
            /* equal to skip_bucket. After the pick, the node_count_array  */
            /* item's value is set as 1. When another bucket of this node  */
            /* is checked, the node_count_array item increases one. The    */
            /* bucket will not be picked until node_count_array[b_node] of */
            /* the node reaches skip_bucket again.                         */
            /* In theory, each old node gives 1/member_count of its buckets */
            /* to each new node. This way, by total, each new node gets    */
            /* 1/member_count of total buckets                             */
            /***************************************************************/
            for (j = 0; j < new_member_count; j++) {
                b_nnode = new_node_array[j];

                tmp_skip_bucket = skip_bucket;
                // each time pick up buckets for one new node
                while (node_count_bucket[b_nnode] < target_bucket) {
                    for (i = 0; i < old_member_count; i++)
                        node_count_array[i] = tmp_skip_bucket;

                    for (i = 0; i < BUCKETDATALEN; i++) {
                        if (bucket_ptr[i] < old_member_count) {
                            // in new bucket list
                            b_node =
                                old_bucket_map[i];  // in old bucket list, actually we can use either new or old list
                            if (node_count_array[b_node] == tmp_skip_bucket &&
                                node_count_bucket[b_node] > target_bucket) {
                                bucket_ptr[i] = b_nnode;
                                node_count_array[b_node] = 1;
                                node_count_bucket[b_node]--;
                                node_count_bucket[b_nnode]++;
                                if (node_count_bucket[b_nnode] >= target_bucket)
                                    break;
                            } else
                                node_count_array[b_node]++;
                        }
                    }
                    if ((tmp_skip_bucket - 1) < 1)
                        tmp_skip_bucket = 1;
                    else
                        tmp_skip_bucket--;  // pick up buckets more frequently from old nodes
                }
            }
            /* when the bucket number cannot be divided by number of datanodes */
            /* the above algorigthm may cause uneven distribution, means one   */
            /* one datanode has two or two more buckets than others 		   */
            /* the following code does re-balance and make sure even distribute */
            tmp_skip_bucket = 0;
            for (j = 0; j < member_count; j++) {
                b_node = 0;
                while (node_count_bucket[j] > target_bucket + 1) {
                    for (i = b_node; i < BUCKETDATALEN; i++) {
                        if (bucket_ptr[i] == j) {
                            b_node = i;
                            // all nodes have at least target_bucket buckets
                            // once find one node with target_bucket buckets, we
                            // advance tmp_skip_bucket; any nodes before should
                            // have at least target_bucket+1 buckets and not intersted
                            // in the following loop
                            for (b_nnode = tmp_skip_bucket; b_nnode < member_count; b_nnode++) {
                                if (node_count_bucket[b_nnode] == target_bucket) {
                                    bucket_ptr[i] = b_nnode;
                                    node_count_bucket[b_nnode]++;
                                    tmp_skip_bucket = b_nnode;
                                    break;
                                }
                            }
                            break;
                        }
                    }
                    // if this node has more than target_bucket+1 buckets, then at least one
                    // datanode will has target_bucket buckets
                    node_count_bucket[j]--;
                }
            }
            pfree(node_count_bucket);
            pfree(node_count_array);
            pfree(new_node_array);
            pfree(old_bucket_map);
        }
    }

    if (need_free)
        pfree_ext(gmember);
}

/*
 * CanPgxcGroupRemove
 *		Check whether the group is empty(no table exists)
 *
 *	-input:
 *		@group_name: target nodegroup that to check whether we can go ahead to remove
 *
 *  -return:
 *		@true: empty, we can go to remove nodegroup
 *		@false: not empty node group contains tables
 */
bool CanPgxcGroupRemove(Oid group_oid, bool ignore_pmk)
{
    char cmd[CHAR_BUF_SIZE];
    char tbs_cmd[CHAR_BUF_SIZE];
    int val = 0;
    int rc = 0;

    char* database_name = NULL;
    Relation pg_database_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;
    bool result = true;

    if (ignore_pmk) {
        rc = snprintf_s(cmd,
            CHAR_BUF_SIZE,
            CHAR_BUF_SIZE - 1,
            "select count(*) from pgxc_class "
            "JOIN pg_class c ON pcrelid = c.oid "
            "JOIN pgxc_group ON pgroup = group_name "
            "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
            "where pgxc_group.oid=%u "
            "AND n.nspname<>'pmk';",
            group_oid);
    } else {
        rc = snprintf_s(cmd,
            CHAR_BUF_SIZE,
            CHAR_BUF_SIZE - 1,
            "select count(*) from pgxc_class, pgxc_group "
            "where pgroup=group_name and pgxc_group.oid=%u;",
            group_oid);
    }
    securec_check_ss(rc, "\0", "\0");

    pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);

    scan = tableam_scan_begin(pg_database_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pg_database_datname, RelationGetDescr(pg_database_rel), &isNull);
        Assert(!isNull);

        database_name = (char*)pstrdup((const char*)DatumGetCString(datum));
        if (strcmp(database_name, "template0") == 0) {
            continue;
        }

        val = GetTableCountByDatabase(database_name, cmd);

        if (val > 0) {
            result = false;
            if (ignore_pmk) {
                rc = snprintf_s(tbs_cmd,
                    CHAR_BUF_SIZE,
                    CHAR_BUF_SIZE - 1,
                    "select n.nspname,c.relname from pgxc_class "
                    "JOIN pg_class c ON pcrelid = c.oid "
                    "JOIN pgxc_group ON pgroup = group_name "
                    "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "where pgxc_group.oid=%u "
                    "AND n.nspname<>'pmk';",
                    group_oid);
            } else {
                rc = snprintf_s(tbs_cmd,
                    CHAR_BUF_SIZE,
                    CHAR_BUF_SIZE - 1,
                    "select n.nspname,c.relname from pgxc_class "
                    "JOIN pg_class c ON pcrelid = c.oid "
                    "JOIN pgxc_group ON pgroup = group_name "
                    "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "where pgxc_group.oid=%u;",
                    group_oid);
            }
            securec_check_ss(rc, "\0", "\0");
            GetTablesByDatabase(database_name, tbs_cmd, strlen(tbs_cmd));
        }
    }

    tableam_scan_end(scan);
    heap_close(pg_database_rel, AccessShareLock);

    return result;
}

/*
 * GetTableCountByDatabase()
 *
 * Get the count of table in a database
 */
int GetTableCountByDatabase(const char* database_name, char* query)
{
    char conninfo[CHAR_BUF_SIZE];
    PGconn* pgconn = NULL;
    int ret;
    PGresult* res = NULL;

    /*
     * As we will lock pg_database with AccessShareLock twice when connect other databases
     * to verify whether the to-be dropped node group is empty, if there is AccessExclusiveLock
     * (e.g. vacuum full xxdb) happens between them, server may run into a deadlock scenario,
     * so we have to add a timeout here to prevent that happens.
     */
    ret = snprintf_s(conninfo,
        sizeof(conninfo),
        sizeof(conninfo) - 1,
        "dbname=%s port=%d connect_timeout=60 options='-c xc_maintenance_mode=on'",
        database_name,
        g_instance.attr.attr_network.PostPortNumber);
    securec_check_ss_c(ret, "\0", "\0");

    pgconn = PQconnectdb(conninfo);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(pgconn) != CONNECTION_OK) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Connection to database failed: %s", PQerrorMessage(pgconn))));
    }

    res = PQexec(pgconn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("execute statement: %s failed: %s", query, PQerrorMessage(pgconn))));
    }
    if (PQntuples(res) <= 0) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), 
            errmsg("PQtuples num is invalid : %d", PQntuples(res))));
    }

    int num = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(pgconn);
    return num;
}

/*
 * GetTablesByDatabase()
 *
 * Get the name of tables in a database
 */
static void GetTablesByDatabase(const char* database_name, char* query, size_t size)
{
    char conninfo[CHAR_BUF_SIZE];
    PGconn* pgconn = NULL;
    int ret;
    PGresult* res = NULL;

    ret = snprintf_s(conninfo,
        sizeof(conninfo),
        sizeof(conninfo) - 1,
        "dbname=%s port=%d connect_timeout=60 options='-c xc_maintenance_mode=on'",
        database_name,
        g_instance.attr.attr_network.PostPortNumber);
    securec_check_ss_c(ret, "\0", "\0");

    pgconn = PQconnectdb(conninfo);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(pgconn) != CONNECTION_OK) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Connection to database failed: %s", PQerrorMessage(pgconn))));
    }

    res = PQexec(pgconn, query);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("execute statement: %s failed: %s", query, PQerrorMessage(pgconn))));
    }

    if (PQntuples(res) == 0) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("fail to get tables in database %s for query remain table", database_name)));
    } else {
        int i_nspname = PQfnumber(res, "nspname");
        int i_relname = PQfnumber(res, "relname");
        for (int i = 0; i < PQntuples(res); i++) {
            char* nspname = PQgetvalue(res, i, i_nspname);
            char* relname = PQgetvalue(res, i, i_relname);
            ereport(LOG, (errmsg("table:%s.%s remain in database %s", nspname, relname, database_name)));
        }
    }

    PQclear(res);
    PQfinish(pgconn);
}

char* PgxcGroupGetInRedistributionGroup()
{
    char* group_name = NULL;
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Datum datum;
    bool isNull = false;

    /* We do not care about group information in datanodes */
    if (!isRestoreMode && IS_PGXC_DATANODE) {
        return NULL;
    }

    if (t_thrd.pgxc_cxt.current_redistribution_nodegroup) {
        return t_thrd.pgxc_cxt.current_redistribution_nodegroup;
    }
    PgxcOpenGroupRelation(&pgxc_group_rel);

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tup, Anum_pgxc_group_in_redistribution, RelationGetDescr(pgxc_group_rel), &isNull);

        if ('y' == DatumGetChar(datum)) {
            datum = heap_getattr(tup, Anum_pgxc_group_name, RelationGetDescr(pgxc_group_rel), &isNull);

            Assert(!isNull);

            group_name = (char*)pstrdup((const char*)DatumGetCString(datum));
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    if (group_name != NULL) {
        set_current_redistribution_nodegroup(group_name);
        pfree(group_name);

        return t_thrd.pgxc_cxt.current_redistribution_nodegroup;
    }

    return NULL;
}
void PgxcOpenGroupRelation(Relation* pgxc_group_rel)
{
    *pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    if (!(*pgxc_group_rel)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("can not open pgxc_group")));
    }
}
List* GetNodeGroupOidCompute(Oid role_id)
{
    List* objects = NIL;
    Relation pgxc_group_rel = NULL;
    TableScanDesc scan;
    HeapTuple tup = NULL;
    Oid group_oid;

    pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);

    if (!pgxc_group_rel) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("can not open pgxc_group")));
    }

    scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        group_oid = HeapTupleGetOid(tup);

        if (OidIsValid(group_oid)) {
            /* Check current user has privilige to this group */
            AclResult aclresult = pg_nodegroup_aclcheck(group_oid, role_id, ACL_COMPUTE | ACL_USAGE);
            if (aclresult == ACLCHECK_OK) {
                objects = lappend_oid(objects, group_oid);
            }
        }
    }

    tableam_scan_end(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    return objects;
}

uint2* GetBucketMapByGroupName(const char* groupname, int *bucketlen)
{
    Relation rel;
    HeapTuple htup;
    bool isNull = false;
    Datum groupbucket;

    uint2* bktmap = (uint2*)palloc0(BUCKETDATALEN * sizeof(uint2));

    rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    htup = SearchSysCache1(PGXCGROUPNAME, CStringGetDatum(groupname));
    if (!HeapTupleIsValid(htup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for pgxc_group %s", groupname)));

    groupbucket = heap_getattr(htup, Anum_pgxc_group_buckets, RelationGetDescr(rel), &isNull);
    if (!isNull) {
        text_to_bktmap((text*)groupbucket, bktmap, bucketlen);
    }

    ReleaseSysCache(htup);
    heap_close(rel, AccessShareLock);

    return bktmap;
}

void InitNodeGroupStatus(void)
{
    /*
     * Skip node group status cache optimization when current backend thread not
     * in normal states or in AutoVacuum process or during upgrade
     */
    if (!IsNormalProcessingMode() || IsAutoVacuumLauncherProcess() || !OidIsValid(u_sess->proc_cxt.MyDatabaseId) ||
        u_sess->attr.attr_common.IsInplaceUpgrade) {
        return;
    }

    if (unlikely(t_thrd.pgxc_cxt.current_installation_nodegroup != NULL)) {
        pfree_ext(t_thrd.pgxc_cxt.current_installation_nodegroup);
    }

    if (unlikely(t_thrd.pgxc_cxt.current_redistribution_nodegroup != NULL)) {
        pfree_ext(t_thrd.pgxc_cxt.current_redistribution_nodegroup);
    }

    /* create transaction level global node group status */
    (void)PgxcGroupGetInstallationGroup();
    (void)PgxcGroupGetInRedistributionGroup();
}

void CleanNodeGroupStatus(void)
{
    /*
     * Skip node group status cache optimization when current backend thread not
     * in normal states
     */
    if (!IsNormalProcessingMode()) {
        return;
    }

    if (t_thrd.pgxc_cxt.current_redistribution_nodegroup != NULL) {
        pfree_ext(t_thrd.pgxc_cxt.current_redistribution_nodegroup);
    }

    if (t_thrd.pgxc_cxt.current_installation_nodegroup != NULL) {
        pfree_ext(t_thrd.pgxc_cxt.current_installation_nodegroup);
    }
}

/*
 * If we ues the grammar "create table to node", when we support
 * multi-nodegroup, it must be converted to "create table to group",
 * but we are not sure to find the group correct through "to node".
 *
 * In non-logic cluster mode, we use "to node" for create table to installatation
 * group or redistribution group in restore mode, so we do this conversion here.
 *
 * In logic cluster mode, we use "to node" for create table to logic cluster
 * node group in restore mode.
 */
char* DeduceGroupByNodeList(Oid* nodeoids, int numnodes)
{
    Oid* members = NULL;
    int nmembers = 0;
    bool check_node = false;
    char* installationgroup = PgxcGroupGetInstallationGroup();
    char* redistributiongroup = PgxcGroupGetInRedistributionGroup();
    char* group_name = NULL;

    SortRelationDistributionNodes(nodeoids, numnodes);

    if (installationgroup != NULL) {
        nmembers = get_pgxc_groupmembers(get_pgxc_groupoid(installationgroup), &members);
        SortRelationDistributionNodes(members, nmembers);
        if (numnodes == nmembers) {
            check_node = true;
            for (int i = 0; i < nmembers; i++) {
                if (nodeoids[i] != members[i]) {
                    check_node = false;
                    break;
                }
            }
        }

        if (check_node != false) {
            group_name = installationgroup;
        }
    }

    if (check_node == false && redistributiongroup != NULL) {
        nmembers = get_pgxc_groupmembers(get_pgxc_groupoid(redistributiongroup), &members);
        SortRelationDistributionNodes(members, nmembers);
        if (numnodes == nmembers) {
            check_node = true;
            for (int i = 0; i < nmembers; i++) {
                if (nodeoids[i] != members[i]) {
                    check_node = false;
                    break;
                }
            }
        }

        if (check_node != false) {
            group_name = redistributiongroup;
        }
    }

    if (check_node == false) {
        Relation pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);
        oidvector* gmember = NULL;
        HeapTuple tup = NULL;
        TableScanDesc scan = NULL;

        scan = tableam_scan_begin(pgxc_group_rel, SnapshotNow, 0, NULL);

        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            bool need_free = false;
            gmember = get_group_member_ref(tup, &need_free);

            /* nodeoids must be sorted by node_name, group_members of pgxc_class
             *  should already be sorted by node_name, so sort is not needed.
             */
            if (numnodes == gmember->dim1) {
                for (int i = 0; i < numnodes; i++) {
                    if (nodeoids[i] != *(gmember->values + i)) {
                        continue;
                    }
                }
                check_node = true;
                const char* gname = NameStr(((Form_pgxc_group)GETSTRUCT(tup))->group_name);
                group_name = (char*)pstrdup(gname);
                break;
            }

            if (need_free != false)
                pfree_ext(gmember);
        }
        tableam_scan_end(scan);
        heap_close(pgxc_group_rel, AccessShareLock);
    }

    if (check_node == false) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not find node group by this node list.")));
    }

    Assert(group_name != NULL);
    return group_name;
}

/*
 * PgxcGroupGetLogicClusterList
 *	get logic clusters from input accordingly.
 *
 * Parameters:
 *	@in nodeids: nodes for the logic clusters
 *
 * Return logic cluster list.
 */
List* PgxcGroupGetLogicClusterList(Bitmapset* nodeids)
{
    List* objects = NIL;
    Bitmapset* left_nodeids = nodeids;
    Oid installation_group = InvalidOid;
    Relation pgxc_group_rel = NULL;
    SysScanDesc scan;
    ScanKeyData skey[1];
    HeapTuple tup = NULL;
    Oid my_group = InvalidOid;
    bool use_my_group = false;

    if (bms_is_empty(left_nodeids)) {
        ereport(DEBUG2, (errmsg("CalculateQueryMem: empty nodeids")));
        bms_free(left_nodeids);
        return NIL;
    }

    /* not for users who belong to no logic cluster */
    my_group = get_pgxc_logic_groupoid(GetCurrentUserId());
    if (!OidIsValid(my_group))
        return NIL;

    installation_group = ng_get_installation_group_oid();
    pgxc_group_rel = heap_open(PgxcGroupRelationId, AccessShareLock);
    if (!pgxc_group_rel) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("can not open pgxc_group")));
    }

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTGreaterStrategyNumber, F_OIDGT, ObjectIdGetDatum(0));
    scan = systable_beginscan(pgxc_group_rel, PgxcGroupOidIndexId, true, NULL, 1, skey);

    while ((tup = systable_getnext(scan)) != NULL) {
        Oid group_oid = HeapTupleGetOid(tup);
        /* skip installation group */
        if (installation_group != group_oid && OidIsValid(group_oid)) {
            /* get nodeids of this group */
            Bitmapset* this_nodeids = NULL;
            int nmembers;
            Oid* nodes = NULL;
            oidvector* group_members_raw = NULL;
            bool need_free = false;

            group_members_raw = get_group_member_ref(tup, &need_free);
            nodes = (Oid*)group_members_raw->values;
            nmembers = group_members_raw->dim1;
            for (int i = 0; i < nmembers; i++) {
                int nodeId = PGXCNodeGetNodeId(nodes[i], PGXC_NODE_DATANODE);
                if (nodeId < 0)
                    continue;

                this_nodeids = bms_add_member(this_nodeids, nodeId);
            }
            if (need_free)
                pfree_ext(group_members_raw);

            /* Check nodeids of this group */
            Bitmapset* inter = bms_intersect(this_nodeids, left_nodeids);
            if (!bms_is_empty(inter)) {
                /* remove nodeids of this group */
                Bitmapset* tmp_nodeids = bms_difference(left_nodeids, this_nodeids);
                bms_free(left_nodeids);
                left_nodeids = tmp_nodeids;

                /* add to the list */
                Distribution* dist = NewDistribution();
                dist->group_oid = group_oid;
                dist->bms_data_nodeids = this_nodeids;
                objects = lappend(objects, dist);
                ereport(DEBUG2, (errmsg("CalculateQueryMem: query contains nodegroup %d", group_oid)));

                /* query use my group node */
                if (group_oid == my_group)
                    use_my_group = true;
            }
            bms_free(inter);

            /* finish check */
            if (bms_is_empty(left_nodeids))
                break;
        }
    }

    systable_endscan(scan);
    heap_close(pgxc_group_rel, AccessShareLock);

    /* add my group to the last */
    if (!use_my_group) {
        Distribution* dist = ng_get_group_distribution(my_group);
        objects = lappend(objects, dist);
    }

    Assert(bms_is_empty(left_nodeids));
    bms_free(left_nodeids);
    return objects;
}

/*
 * gs_get_nodegroup_tablecount
 *	marked unsupported for centralized
 *
 * Parameters:
 *	@in node group name
 *
 * Return total table numbers.
 */
Datum gs_get_nodegroup_tablecount(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        (errmsg("Unsupport feature"),
            errdetail("gs_get_nodegroup_tablecount is not supported for centralize deployment"),
            errcause("The function is not implemented."),
            erraction("Do not use this function with centralize deployment."))));
    PG_RETURN_INT32(0);
}
