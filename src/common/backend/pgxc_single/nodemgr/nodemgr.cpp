/* -------------------------------------------------------------------------
 *
 * nodemgr.c
 *	  Routines to support manipulation of the pgxc_node catalog
 *	  Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "storage/proc.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/groupmgr.h"
#include "optimizer/nodegroups.h"
#include "access/xact.h"
#include "securec.h"
#include "utils/elog.h"

/*
 * How many times should we try to find a unique indetifier
 * in case hash of the node name comes out to be duplicate
 */
#define MAX_TRIES_FOR_NID 200

#define MAX_STAND_BY_DATANODES 7

static Datum generate_node_id(const char* node_name);

/*
 * NodeTablesInit
 *	Initializes shared memory tables of Coordinators and Datanodes.
 */
void NodeTablesShmemInit(void)
{
    bool found = false;
    /*
     * Initialize the table of Coordinators: first sizeof(int) bytes are to
     * store actual number of Coordinators, remaining data in the structure is
     * array of NodeDefinition that can contain up to MaxCoords entries.
     * That is a bit weird and probably it would be better have these in
     * separate structures, but I am unsure about cost of having shmem structure
     * containing just single integer.
     */
    t_thrd.pgxc_cxt.shmemNumCoords = (int*)ShmemInitStruct(
        "Coordinator Table", sizeof(int) + sizeof(NodeDefinition) * g_instance.attr.attr_network.MaxCoords, &found);

    /* Have t_thrd.pgxc_cxt.coDefs pointing right behind t_thrd.pgxc_cxt.shmemNumCoords */
    t_thrd.pgxc_cxt.coDefs = (NodeDefinition*)(t_thrd.pgxc_cxt.shmemNumCoords + 1);

    /* Mark it empty upon creation */
    if (!found)
        *t_thrd.pgxc_cxt.shmemNumCoords = 0;

    t_thrd.pgxc_cxt.shmemNumCoordsInCluster = (int*)ShmemInitStruct("Internal Coordinator Table",
        sizeof(int) + sizeof(NodeDefinition) * g_instance.attr.attr_network.MaxCoords,
        &found);
    t_thrd.pgxc_cxt.coDefsInCluster = (NodeDefinition*)(t_thrd.pgxc_cxt.shmemNumCoordsInCluster + 1);
    if (!found) {
        *t_thrd.pgxc_cxt.shmemNumCoordsInCluster = 0;
    }
    /* Same for Datanodes */
    t_thrd.pgxc_cxt.shmemNumDataNodes = (int*)ShmemInitStruct(
        "Datanode Table", sizeof(int) + sizeof(NodeDefinition) * g_instance.attr.attr_common.MaxDataNodes, &found);

    /* Have t_thrd.pgxc_cxt.coDefs pointing right behind t_thrd.pgxc_cxt.shmemNumDataNodes */
    t_thrd.pgxc_cxt.dnDefs = (NodeDefinition*)(t_thrd.pgxc_cxt.shmemNumDataNodes + 1);

    /* Mark it empty upon creation */
    if (!found)
        *t_thrd.pgxc_cxt.shmemNumDataNodes = 0;

    if (IS_DN_MULTI_STANDYS_MODE()) {
        /* Same for standby Datanodes */
        t_thrd.pgxc_cxt.shmemNumDataStandbyNodes = (int*)ShmemInitStruct("StandbyDatanode Table",
            (sizeof(int) +
                (sizeof(NodeDefinition) * g_instance.attr.attr_common.MaxDataNodes * MAX_STAND_BY_DATANODES)),
            &found);

        /* Have t_thrd.pgxc_cxt.dnStandbyDefs pointing right behind t_thrd.pgxc_cxt.shmemNumDataStandbyNodes */
        t_thrd.pgxc_cxt.dnStandbyDefs = (NodeDefinition*)(t_thrd.pgxc_cxt.shmemNumDataStandbyNodes + 1);

        /* Mark it empty upon creation */
        if (!found)
            *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes = 0;
    }
}

/*
 * NodeTablesShmemSize
 *	Get the size of shared memory dedicated to node definitions
 */
Size NodeTablesShmemSize(void)
{
    Size co_size;
    Size co_size_cluster;
    Size dn_size;

    co_size = mul_size(sizeof(NodeDefinition), g_instance.attr.attr_network.MaxCoords);
    co_size = add_size(co_size, sizeof(int));
    co_size_cluster = mul_size(sizeof(NodeDefinition), g_instance.attr.attr_network.MaxCoords);
    co_size_cluster = add_size(co_size_cluster, sizeof(int));
    dn_size = mul_size(sizeof(NodeDefinition), g_instance.attr.attr_common.MaxDataNodes);
    dn_size = add_size(dn_size, sizeof(int));

    return add_size(add_size(co_size, dn_size), co_size_cluster);
}

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void check_node_options(const char* node_name, List* options, char* node_type, bool* rw, char** node_host,
    int* node_port, int* comm_sctp_port, int* comm_control_port, char** node_host1, int* node_port1,
    int* comm_sctp_port1, int* comm_control_port1, bool* is_primary, bool* is_preferred, bool* hostis_primary,
    bool* nodeis_central, bool* is_active)
{
    ListCell* option = NULL;

    if (options == NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("No options specified")));

    /* Filter options */
    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "port") == 0) {
            *node_port = defGetTypeLength(defel);

            if (*node_port < 1 || *node_port > 65535)
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("port value is out of range")));
        } else if (strcmp(defel->defname, "sctp_port") == 0) {
            *comm_sctp_port = defGetTypeLength(defel);

            if (*comm_sctp_port < 0 || *comm_sctp_port > 65535)
                ereport(
                    ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("sctp_port value is out of range")));
        } else if (strcmp(defel->defname, "control_port") == 0) {
            *comm_control_port = defGetTypeLength(defel);

            if (*comm_control_port < 0 || *comm_control_port > 65535)
                ereport(
                    ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("control_port value is out of range")));
        } else if (strcmp(defel->defname, "port1") == 0) {
            *node_port1 = defGetTypeLength(defel);

            if (*node_port1 < 1 || *node_port1 > 65535)
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("port1 value is out of range")));
        } else if (strcmp(defel->defname, "sctp_port1") == 0) {
            *comm_sctp_port1 = defGetTypeLength(defel);

            if (*comm_sctp_port1 < 0 || *comm_sctp_port1 > 65535)
                ereport(
                    ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("sctp_port1 value is out of range")));
        } else if (strcmp(defel->defname, "control_port1") == 0) {
            *comm_control_port1 = defGetTypeLength(defel);

            if (*comm_control_port1 < 0 || *comm_control_port1 > 65535)
                ereport(ERROR,
                    (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("control_port1 value is out of range")));
        } else if (strcmp(defel->defname, "host") == 0) {
            *node_host = defGetString(defel);
        } else if (strcmp(defel->defname, "host1") == 0) {
            *node_host1 = defGetString(defel);
        } else if (strcmp(defel->defname, "type") == 0) {
            char* type_loc = NULL;

            type_loc = defGetString(defel);
            if (unlikely(type_loc == NULL)) {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null type_loc is invalid")));
            }
            if (strcmp(type_loc, "coordinator") != 0 && strcmp(type_loc, "datanode") != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("type value is incorrect, specify 'coordinator or 'datanode'")));

            if (strcmp(type_loc, "coordinator") == 0)
                *node_type = PGXC_NODE_COORDINATOR;
            else
                *node_type = PGXC_NODE_DATANODE;
        } else if (strcmp(defel->defname, "primary") == 0) {
            *is_primary = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "preferred") == 0) {
            *is_preferred = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "hostprimary") == 0) {
            *hostis_primary = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "nodeis_central") == 0) {
            *nodeis_central = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "nodeis_active") == 0) {
            *is_active = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "rw") == 0) {
            *rw = defGetBoolean(defel);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("incorrect option: %s", defel->defname)));
        }
    }

    /* A primary node has to be a Datanode */
    if (*is_primary && *node_type != PGXC_NODE_DATANODE && *node_type != PGXC_NODE_DATANODE_STANDBY)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode", node_name)));

    if (*nodeis_central && *node_type != PGXC_NODE_COORDINATOR)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("PGXC node %s: cannot be a central node, it has to be a Coordinator", node_name)));

    /* A preferred node has to be a Datanode */
    if (*is_preferred && *node_type != PGXC_NODE_DATANODE && *node_type != PGXC_NODE_DATANODE_STANDBY)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode", node_name)));

    if (*is_active == false && *node_type != PGXC_NODE_COORDINATOR)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg(
                    "PGXC node %s: cannot be not active, only Coordinator can set nodeis_active to false", node_name)));

    /* Node type check */
    if (*node_type == PGXC_NODE_NONE)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: Node type not specified", node_name)));
}

/*
 * generate_node_id
 *
 * Given a node name compute its hash to generate the identifier
 * If the hash comes out to be duplicate , try some other values
 * Give up after a few tries
 */
static Datum generate_node_id(const char* node_name)
{
    Datum node_id;
    uint32 n;
    bool inc = false;
    int i;

    /* Compute node identifier by computing hash of node name */
    node_id = hash_any((unsigned char*)node_name, strlen(node_name));

    /*
     * Check if the hash is near the overflow limit, then we will
     * decrement it , otherwise we will increment
     */
    inc = true;
    n = DatumGetUInt32(node_id);
    if (n >= UINT_MAX - MAX_TRIES_FOR_NID)
        inc = false;

    /*
     * Check if the identifier is clashing with an existing one,
     * and if it is try some other
     */
    for (i = 0; i < MAX_TRIES_FOR_NID; i++) {
        HeapTuple tup;

        tup = SearchSysCache1(PGXCNODEIDENTIFIER, node_id);
        if (tup == NULL)
            break;

        ReleaseSysCache(tup);

        n = DatumGetUInt32(node_id);
        if (inc)
            n++;
        else
            n--;

        node_id = UInt32GetDatum(n);
    }

    /*
     * This has really few chances to happen, but inform backend that node
     * has not been registered correctly in this case.
     */
    if (i >= MAX_TRIES_FOR_NID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Please choose different node name."),
                errdetail("Name \"%s\" produces a duplicate identifier node_name", node_name)));

    return node_id;
}

/* --------------------------------
 *  cmp_nodes_name
 *
 *  Compare the Oids of two XC nodes
 *  to sort them in ascending order by their names
 * --------------------------------
 */
static int cmp_nodes_name(const void* p1, const void* p2)
{
    return strcmp((char*)((NodeDefinition*)p1)->nodename.data, (char*)((NodeDefinition*)p2)->nodename.data);
}

/*
 * PgxcNodeListAndCount
 *
 * Update node definitions in the shared memory tables from the catalog
 */
void PgxcNodeListAndCount(void)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;

    /*
     * First, we get relation lock so that another xact holding relation
     * access exclusive lock won't form deadlock against us when it
     * perform consequent multinode query.
     */
    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    bool TempImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = false;

    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

    *t_thrd.pgxc_cxt.shmemNumCoords = 0;
    *t_thrd.pgxc_cxt.shmemNumCoordsInCluster = 0;
    *t_thrd.pgxc_cxt.shmemNumDataNodes = 0;
    if (IS_DN_MULTI_STANDYS_MODE()) {
        *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes = 0;
    }

    errno_t rc;
    PG_TRY();
    {
        /*
         * Node information initialization is made in one scan:
         * 1) Scan pgxc_node catalog to find the number of nodes for
         *	each node type and make proper allocations
         * 2) Then extract the node Oid
         * 3) Complete primary/preferred node information
         */
        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
            NodeDefinition* node = NULL;
            NodeDefinition* nodeInCluster = NULL;

            /* Take definition for given node type */
            switch (nodeForm->node_type) {
                case PGXC_NODE_COORDINATOR: {
                    nodeInCluster = &t_thrd.pgxc_cxt.coDefsInCluster[(*t_thrd.pgxc_cxt.shmemNumCoordsInCluster)++];
                    if (t_thrd.proc->workingVersionNum >= 91275) {
                        bool isNull = false;
                        Datum isactive = SysCacheGetAttr(PGXCNODEOID, tuple, Anum_pgxc_node_is_active, &isNull);
                        if (DatumGetBool(isactive))
                            node = &t_thrd.pgxc_cxt.coDefs[(*t_thrd.pgxc_cxt.shmemNumCoords)++];
                    } else {
                        node = &t_thrd.pgxc_cxt.coDefs[(*t_thrd.pgxc_cxt.shmemNumCoords)++];
                    }
                    break;
                }
                case PGXC_NODE_DATANODE:
                    node = &t_thrd.pgxc_cxt.dnDefs[(*t_thrd.pgxc_cxt.shmemNumDataNodes)++];
                    break;
                case PGXC_NODE_DATANODE_STANDBY:
                    if (!IS_DN_MULTI_STANDYS_MODE()) {
                        ereport(PANIC,
                            (errmsg("the replication type should be multi-standby, now is %d",
                                g_instance.attr.attr_storage.replication_type)));
                    }
                    node = &t_thrd.pgxc_cxt.dnStandbyDefs[(*t_thrd.pgxc_cxt.shmemNumDataStandbyNodes)++];
                    break;
                default:
                    Assert(false);
                    continue;
            }

            /* Populate the definition */
            if (node != NULL) {
                node->nodeoid = HeapTupleGetOid(tuple);
                rc = memcpy_s(&node->nodename, NAMEDATALEN, &nodeForm->node_name, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                rc = memcpy_s(&node->nodehost, NAMEDATALEN, &nodeForm->node_host, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                node->nodeport = nodeForm->node_port;
                node->nodectlport = nodeForm->control_port;
                node->nodesctpport = nodeForm->sctp_port;
                node->nodeid = nodeForm->node_id;
                rc = memcpy_s(&node->nodehost1, NAMEDATALEN, &nodeForm->node_host1, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                node->nodeport1 = nodeForm->node_port1;
                node->nodectlport1 = nodeForm->control_port1;
                node->nodesctpport1 = nodeForm->sctp_port1;
                node->hostisprimary = nodeForm->hostis_primary;
                node->nodeisprimary = nodeForm->nodeis_primary;
                node->nodeispreferred = nodeForm->nodeis_preferred;
                node->nodeis_central = nodeForm->nodeis_central;
                node->nodeis_active = nodeForm->nodeis_active;
            }
            if (nodeInCluster != NULL) {
                nodeInCluster->nodeoid = HeapTupleGetOid(tuple);
                rc = memcpy_s(&nodeInCluster->nodename, NAMEDATALEN, &nodeForm->node_name, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                rc = memcpy_s(&nodeInCluster->nodehost, NAMEDATALEN, &nodeForm->node_host, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                nodeInCluster->nodeport = nodeForm->node_port;
                nodeInCluster->nodectlport = nodeForm->control_port;
                nodeInCluster->nodesctpport = nodeForm->sctp_port;
                nodeInCluster->nodeid = nodeForm->node_id;
                rc = memcpy_s(&nodeInCluster->nodehost1, NAMEDATALEN, &nodeForm->node_host1, NAMEDATALEN);
                securec_check_c(rc, "\0", "\0");
                nodeInCluster->nodeport1 = nodeForm->node_port1;
                nodeInCluster->nodectlport1 = nodeForm->control_port1;
                nodeInCluster->nodesctpport1 = nodeForm->sctp_port1;
                nodeInCluster->hostisprimary = nodeForm->hostis_primary;
                nodeInCluster->nodeisprimary = nodeForm->nodeis_primary;
                nodeInCluster->nodeispreferred = nodeForm->nodeis_preferred;
                nodeInCluster->nodeis_central = nodeForm->nodeis_central;
                nodeInCluster->nodeis_active = nodeForm->nodeis_active;
            }
        }
        tableam_scan_end(scan);
    }
    PG_CATCH();
    {
        /* Finally sort the lists */
        if (*t_thrd.pgxc_cxt.shmemNumCoords > 1)
            qsort(t_thrd.pgxc_cxt.coDefs, *t_thrd.pgxc_cxt.shmemNumCoords, sizeof(NodeDefinition), cmp_nodes_name);
        if (*t_thrd.pgxc_cxt.shmemNumCoordsInCluster > 1)
            qsort(t_thrd.pgxc_cxt.coDefsInCluster,
                *t_thrd.pgxc_cxt.shmemNumCoordsInCluster,
                sizeof(NodeDefinition),
                cmp_nodes_name);
        if (*t_thrd.pgxc_cxt.shmemNumDataNodes > 1)
            qsort(t_thrd.pgxc_cxt.dnDefs, *t_thrd.pgxc_cxt.shmemNumDataNodes, sizeof(NodeDefinition), cmp_nodes_name);
        if (IS_DN_MULTI_STANDYS_MODE() && *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes > 1)
            qsort(t_thrd.pgxc_cxt.dnStandbyDefs,
                *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes,
                sizeof(NodeDefinition),
                cmp_nodes_name);
        ereport(LOG, (errmsg("PgxcNodeListAndCount CATCH")));
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Finally sort the lists */
    if (*t_thrd.pgxc_cxt.shmemNumCoords > 1)
        qsort(t_thrd.pgxc_cxt.coDefs, *t_thrd.pgxc_cxt.shmemNumCoords, sizeof(NodeDefinition), cmp_nodes_name);
    if (*t_thrd.pgxc_cxt.shmemNumCoordsInCluster > 1)
        qsort(t_thrd.pgxc_cxt.coDefsInCluster,
            *t_thrd.pgxc_cxt.shmemNumCoordsInCluster,
            sizeof(NodeDefinition),
            cmp_nodes_name);
    if (*t_thrd.pgxc_cxt.shmemNumDataNodes > 1)
        qsort(t_thrd.pgxc_cxt.dnDefs, *t_thrd.pgxc_cxt.shmemNumDataNodes, sizeof(NodeDefinition), cmp_nodes_name);

    if (IS_DN_MULTI_STANDYS_MODE() && *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes > 1)
        qsort(t_thrd.pgxc_cxt.dnStandbyDefs,
            *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes,
            sizeof(NodeDefinition),
            cmp_nodes_name);

    LWLockRelease(NodeTableLock);
    t_thrd.int_cxt.ImmediateInterruptOK = TempImmediateInterruptOK;
    heap_close(rel, AccessShareLock);
}

void PgxcNodeInitDnMatric(void)
{
    int i = 0;
    int j = 0;
    int k = 0;
    int standby_num = 0;

    if (u_sess->pgxc_cxt.dn_matrics || !IS_DN_MULTI_STANDYS_MODE() || *t_thrd.pgxc_cxt.shmemNumDataNodes < 1 ||
        *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes < 1)
        return;

    u_sess->pgxc_cxt.dn_matrics = (Oid**)palloc0_noexcept(*t_thrd.pgxc_cxt.shmemNumDataNodes * sizeof(Oid*));
    if (u_sess->pgxc_cxt.dn_matrics == NULL) {
        ereport(PANIC,
            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
            errmsg("DN matric cannot alloc memory.")));
    }

    standby_num = (*t_thrd.pgxc_cxt.shmemNumDataStandbyNodes) / (*t_thrd.pgxc_cxt.shmemNumDataNodes);

    for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++) {
        u_sess->pgxc_cxt.dn_matrics[i] = (Oid*)palloc0_noexcept((standby_num + 1) * sizeof(Oid));
        if (u_sess->pgxc_cxt.dn_matrics[i] == NULL) {
            ereport(PANIC,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                errmsg("DN matric cannot alloc memory.")));
        }

        u_sess->pgxc_cxt.dn_matrics[i][0] = t_thrd.pgxc_cxt.dnDefs[i].nodeoid;
        for (j = 0, k = 1; j < *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes && k < standby_num + 1; j++) {
            if (strcmp(NameStr(t_thrd.pgxc_cxt.dnDefs[i].nodename),
                NameStr(t_thrd.pgxc_cxt.dnStandbyDefs[j].nodename)) == 0) {
                u_sess->pgxc_cxt.dn_matrics[i][k] = t_thrd.pgxc_cxt.dnStandbyDefs[j].nodeoid;
                k++;
            }
        }
    }

    return;
}

void PgxcNodeFreeDnMatric()
{
    int i = 0;

    if (u_sess->pgxc_cxt.dn_matrics == NULL || !IS_DN_MULTI_STANDYS_MODE())
        return;

    LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        if (u_sess->pgxc_cxt.dn_matrics[i]) {
            pfree(u_sess->pgxc_cxt.dn_matrics[i]);
            u_sess->pgxc_cxt.dn_matrics[i] = NULL;
        }
    }

    if (u_sess->pgxc_cxt.dn_matrics) {
        pfree(u_sess->pgxc_cxt.dn_matrics);
        u_sess->pgxc_cxt.dn_matrics = NULL;
    }

    LWLockRelease(NodeTableLock);
}

bool PgxcNodeCheckDnMatric(Oid oid1, Oid oid2)
{
    int i = 0;
    int j = 0;
    int standby_num = 0;
    bool meet_oid1 = false;
    bool meet_oid2 = false;
    bool res = false;

    if (oid1 == oid2)
        return true;

    if (u_sess->pgxc_cxt.dn_matrics == NULL || u_sess->pgxc_cxt.NumDataNodes < 1 ||
        u_sess->pgxc_cxt.NumStandbyDataNodes < 1)
        return false;

    standby_num = u_sess->pgxc_cxt.NumStandbyDataNodes / u_sess->pgxc_cxt.NumDataNodes;

    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        meet_oid1 = false;
        meet_oid2 = false;

        for (j = 0; j < standby_num + 1; j++) {
            if (u_sess->pgxc_cxt.dn_matrics[i][j] == oid1) {
                meet_oid1 = true;
            } else if (u_sess->pgxc_cxt.dn_matrics[i][j] == oid2) {
                meet_oid2 = true;
            }
        }

        if (meet_oid1 && meet_oid2) {
            res = true;
            break;
        } else if (meet_oid1 || meet_oid2) {
            res = false;
            break;
        }
    }

    return res;
}

/**
 * @nodemgr.cpp
 * @Description: Get the primary DN node from matric in active/standby mode.
 * @Details:
 *  The DN matric is traversed cyclically. The primary DN oid of the current segment is saved at first,
 *  and then judge if the input oid1 is in the segment. If the flag is true, the stored main dn oid is returned.
 *  Otherwise, the input parameter oid1 is returned.
 * @in Oid
 * @return Oid: Primary DN node id.
 */
Oid PgxcNodeGetPrimaryDNFromMatric(Oid oid1)
{
    int i = 0;
    int j = 0;
    int standby_num = 0;
    Oid res = InvalidOid;
    bool flag = false;

    if (u_sess->pgxc_cxt.dn_matrics == NULL || !IS_DN_MULTI_STANDYS_MODE() || u_sess->pgxc_cxt.NumDataNodes < 1 ||
        u_sess->pgxc_cxt.NumStandbyDataNodes < 1)
        return oid1;

    standby_num = u_sess->pgxc_cxt.NumStandbyDataNodes / u_sess->pgxc_cxt.NumDataNodes;

    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        flag = false;
        for (j = 0; j < standby_num + 1; j++) {
            /* get the current shard's primary datanode */
            if (OidIsValid(u_sess->pgxc_cxt.dn_matrics[i][j]) &&
                get_pgxc_nodetype_refresh_cache(u_sess->pgxc_cxt.dn_matrics[i][j]) == PGXC_NODE_DATANODE) {
                res = u_sess->pgxc_cxt.dn_matrics[i][j];
            }

            /* check whether the oid1 is in current shard. */
            if (u_sess->pgxc_cxt.dn_matrics[i][j] == oid1) {
                flag = true;
            }
        }
        if (flag) {
            if (!OidIsValid(res)) {
                res = oid1;
            }
            break;
        }
        res = InvalidOid;
    }

    if (!OidIsValid(res))
        res = oid1;

    return res;
}

/*
 * PgxcNodeCount
 *
 * Count the number of Coordinaters and Datanodes from the catalog
 */
void PgxcNodeCount(int* numCoords, int* numDns)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    int num_coords = 0;
    int num_datanodes = 0;

    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);

        /* Take definition for given node type */
        switch (nodeForm->node_type) {
            case PGXC_NODE_COORDINATOR:
                num_coords++;
                break;
            case PGXC_NODE_DATANODE:
                num_datanodes++;
                break;
            case PGXC_NODE_DATANODE_STANDBY:
                continue;
            default:

                break;
        }
    }
    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    *numCoords = num_coords;
    *numDns = num_datanodes;
}

/*
 * PgxcNodeGetIds
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results. Preferred and primary node information can be updated
 * in session if requested.
 * !!! Note that we must consider how to free the memory of coOids and dnOids when
 * invoke this function
 */
void PgxcNodeGetOids(Oid** coOids, Oid** dnOids, int* num_coords, int* num_dns, bool update_preferred)
{
    LWLockAcquire(NodeTableLock, LW_SHARED);

    if (num_coords != NULL)
        *num_coords = *t_thrd.pgxc_cxt.shmemNumCoords;
    if (num_dns != NULL)
        *num_dns = *t_thrd.pgxc_cxt.shmemNumDataNodes;

    if (coOids != NULL) {
        int i;

        *coOids = (Oid*)palloc(*t_thrd.pgxc_cxt.shmemNumCoords * sizeof(Oid));
        for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoords; i++)
            (*coOids)[i] = t_thrd.pgxc_cxt.coDefs[i].nodeoid;
    }

    if (dnOids != NULL) {
        int i;

        *dnOids = (Oid*)palloc(*t_thrd.pgxc_cxt.shmemNumDataNodes * sizeof(Oid));
        for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++)
            (*dnOids)[i] = t_thrd.pgxc_cxt.dnDefs[i].nodeoid;
    }

    /* Update also preferred and primary node informations if requested */
    if (update_preferred) {
        int i;

        /* Initialize primary and preferred node information */
        u_sess->pgxc_cxt.primary_data_node = InvalidOid;
        u_sess->pgxc_cxt.num_preferred_data_nodes = 0;

        for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++) {
            if (t_thrd.pgxc_cxt.dnDefs[i].nodeisprimary)
                u_sess->pgxc_cxt.primary_data_node = t_thrd.pgxc_cxt.dnDefs[i].nodeoid;

            if (t_thrd.pgxc_cxt.dnDefs[i].nodeispreferred) {
                u_sess->pgxc_cxt.preferred_data_node[u_sess->pgxc_cxt.num_preferred_data_nodes] =
                    t_thrd.pgxc_cxt.dnDefs[i].nodeoid;
                u_sess->pgxc_cxt.num_preferred_data_nodes++;
            }
        }
    }

    LWLockRelease(NodeTableLock);
}

void PgxcNodeGetOidsForInit(Oid** coOids, Oid** dnOids, int* num_coords, int* num_dns, int * num_primaries, bool update_preferred)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void PgxcNodeGetStandbyOids(Oid** coOids, Oid** dnOids, int* numCoords, int* numStandbyDns, bool needInitPGXC)
{
    LWLockAcquire(NodeTableLock, LW_SHARED);

    int numCoordinators = *t_thrd.pgxc_cxt.shmemNumCoords;
    int numStandbyDatanodes = *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes;
    if (numCoords != NULL) {
        *numCoords = numCoordinators;
    }
    if (numStandbyDns != NULL) {
        *numStandbyDns = numStandbyDatanodes;
    }

    if (coOids != NULL) {
        int i;
        *coOids = (Oid*)palloc(numCoordinators * sizeof(Oid));
        for (i = 0; i < numCoordinators; i++) {
            (*coOids)[i] = t_thrd.pgxc_cxt.coDefs[i].nodeoid;
        }
    }
    if (dnOids != NULL) {
        int i;
        *dnOids = (Oid*)palloc(numStandbyDatanodes * sizeof(Oid));
        for (i = 0; i < numStandbyDatanodes; i++) {
            (*dnOids)[i] = t_thrd.pgxc_cxt.dnStandbyDefs[i].nodeoid;
        }
    }

    if (needInitPGXC) {
        /* update u_sess info using t_thrd info while in Init period */
        PgxcNodeInitDnMatric();
    }
    LWLockRelease(NodeTableLock);
}

/*
 * Find node definition in the shared memory node table.
 * The structure is a copy palloc'ed in current memory context.
 */
NodeDefinition* PgxcNodeGetDefinition(Oid node, bool checkStandbyNodes)
{
    NodeDefinition* result = NULL;
    int i;
    errno_t rc;

    LWLockAcquire(NodeTableLock, LW_SHARED);

    /* search through the Datanodes first */
    for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++) {
        if (t_thrd.pgxc_cxt.dnDefs[i].nodeoid == node) {
            result = (NodeDefinition*)palloc(sizeof(NodeDefinition));

            rc = memcpy_s(result, sizeof(NodeDefinition), t_thrd.pgxc_cxt.dnDefs + i, sizeof(NodeDefinition));
            securec_check_c(rc, "\0", "\0");

            LWLockRelease(NodeTableLock);

            return result;
        }
    }

    if (IS_DN_MULTI_STANDYS_MODE() || checkStandbyNodes) {
        for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataStandbyNodes; i++) {
            if (t_thrd.pgxc_cxt.dnStandbyDefs[i].nodeoid == node) {
                result = (NodeDefinition*)palloc(sizeof(NodeDefinition));

                errno_t rc =
                    memcpy_s(result, sizeof(NodeDefinition), t_thrd.pgxc_cxt.dnStandbyDefs + i, sizeof(NodeDefinition));
                securec_check(rc, "\0", "\0");

                LWLockRelease(NodeTableLock);

                return result;
            }
        }
    }

    /* if not found, search through the Coordinators */
    for (i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoords; i++) {
        if (t_thrd.pgxc_cxt.coDefs[i].nodeoid == node) {
            result = (NodeDefinition*)palloc(sizeof(NodeDefinition));

            rc = memcpy_s(result, sizeof(NodeDefinition), t_thrd.pgxc_cxt.coDefs + i, sizeof(NodeDefinition));
            securec_check_c(rc, "\0", "\0");

            LWLockRelease(NodeTableLock);

            return result;
        }
    }

    /* not found, return NULL */
    LWLockRelease(NodeTableLock);
    return NULL;
}

/*
 * PgxcNodeCreate
 *
 * Add a PGXC node
 */
void PgxcNodeCreate(CreateNodeStmt* stmt)
{
    Relation pgxcnodesrel;
    HeapTuple htup;
    bool nulls[Natts_pgxc_node];
    Datum values[Natts_pgxc_node];
    Datum node_id;
    const char* node_name = stmt->node_name;
    /* Options with default values */
    char node_type = PGXC_NODE_NONE;
    char* node_host = NULL;
    int node_port = 0;
    int comm_sctp_port = 0;
    int comm_control_port = 0;
    char* node_host1 = NULL;
    int node_port1 = 0;
    int comm_sctp_port1 = 0;
    int comm_control_port1 = 0;
    bool host_is_primary = true;
    bool is_primary = false;
    bool is_preferred = false;
    bool is_central = false;
    bool is_active = true;
    bool rw = true;

    int numCoords = 0;
    int numDatanodes = 0;

    /* Only a DB administrator can add nodes */
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to create cluster nodes")));

    /* Filter options */
    check_node_options(node_name,
        stmt->options,
        &node_type,
        &rw,
        &node_host,
        &node_port,
        &comm_sctp_port,
        &comm_control_port,
        &node_host1,
        &node_port1,
        &comm_sctp_port1,
        &comm_control_port1,
        &is_primary,
        &is_preferred,
        &host_is_primary,
        &is_central,
        &is_active);

    if (node_type == PGXC_NODE_COORDINATOR || IS_DN_DUMMY_STANDYS_MODE() || rw) {
        if (OidIsValid(get_pgxc_nodeoid(node_name)))
            ereport(
                ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Node %s: object already defined", node_name)));
    } else {/* mulit standby */
        if (check_pgxc_node_name_is_exist(node_name, node_host, node_port, comm_sctp_port, comm_control_port))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("PGXC Node %s(host = %s, port = %d, sctp_port = %d, control_port = %d): "
                           "object already defined",
                        node_name,
                        node_host,
                        node_port,
                        comm_sctp_port,
                        comm_control_port)));
    }

    /* Check length of node name */
    if (strlen(node_name) > PGXC_NODENAME_LENGTH)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("Node name \"%s\" is too long", node_name)));

    PgxcNodeCount(&numCoords, &numDatanodes);

    if (node_type == PGXC_NODE_COORDINATOR && numCoords >= g_instance.attr.attr_network.MaxCoords) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Failed to create coordinator, the maximum number of "
                       "coordinators %d specified by 'max_coordinators' has been reached.",
                    g_instance.attr.attr_network.MaxCoords)));
    }

    if (node_type == PGXC_NODE_DATANODE && numDatanodes >= g_instance.attr.attr_common.MaxDataNodes) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("Failed to create datanode, the maximum number of "
                       "datanodes %d specified by 'max_datanodes' has been reached.",
                    g_instance.attr.attr_common.MaxDataNodes)));
    }

    /* Compute node identifier */
    node_id = generate_node_id(node_name);

    /*
     * Check that this node is not created as a primary if one already
     * exists.
     */
    if (is_primary && OidIsValid(u_sess->pgxc_cxt.primary_data_node))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: two nodes cannot be primary", node_name)));

    /*
     * Check if only config one node path ,then make another same.
     */
    if (node_port == 0 && node_host == NULL) {
        node_port = node_port1;
        if (node_host1 != NULL) {
            node_host = pstrdup(node_host1);
        }
    }

    if (node_port1 == 0 && node_host1 == NULL) {
        node_port1 = node_port;
        if (node_host != NULL) {
            node_host1 = pstrdup(node_host);
        }
    }

    /*
     * Then assign default values if necessary
     * First for port.
     */
    if (node_port == 0) {
        node_port = 5432;
        elog(LOG, "PGXC node %s: Applying default port value: %d", node_name, node_port);
    }

    if (node_port1 == 0) {
        node_port1 = 5432;
        elog(LOG, "PGXC node %s: Applying default port1 value: %d", node_name, node_port);
    }

    /* Then apply default value for host */
    if (node_host == NULL) {
        node_host = pstrdup("localhost");
        elog(LOG, "PGXC node %s: Applying default host value: %s", node_name, node_host);
    }

    if (node_host1 == NULL) {
        node_host1 = pstrdup("localhost");
        elog(LOG, "PGXC node %s: Applying default host1 value: %s", node_name, node_host);
    }

    if (IS_DN_MULTI_STANDYS_MODE() && node_type == PGXC_NODE_DATANODE && !rw) {
        node_type = PGXC_NODE_DATANODE_STANDBY;
    }

    /* Iterate through all attributes initializing nulls and values */
    for (int i = 0; i < Natts_pgxc_node; i++) {
        nulls[i] = false;
        values[i] = (Datum)0;
    }

    /*
     * Open the relation for insertion
     * This is necessary to generate a unique Oid for the new node
     * There could be a relation race here if a similar Oid
     * being created before the heap is inserted.
     */
    pgxcnodesrel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

    /* Build entry tuple */
    values[Anum_pgxc_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
    values[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
    values[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
    values[Anum_pgxc_node_sctp_port - 1] = Int32GetDatum(comm_sctp_port);
    values[Anum_pgxc_node_strmctl_port - 1] = Int32GetDatum(comm_control_port);
    values[Anum_pgxc_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
    values[Anum_pgxc_node_port1 - 1] = Int32GetDatum(node_port1);
    values[Anum_pgxc_node_sctp_port1 - 1] = Int32GetDatum(comm_sctp_port1);
    values[Anum_pgxc_node_strmctl_port1 - 1] = Int32GetDatum(comm_control_port1);
    values[Anum_pgxc_node_host1 - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host1));
    values[Anum_pgxc_host_is_primary - 1] = BoolGetDatum(host_is_primary);
    values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
    values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
    values[Anum_pgxc_node_is_central - 1] = BoolGetDatum(is_central);
    values[Anum_pgxc_node_id - 1] = node_id;
    if (t_thrd.proc->workingVersionNum >= 91275) {
        values[Anum_pgxc_node_is_active - 1] = BoolGetDatum(is_active);
    }
    htup = heap_form_tuple(pgxcnodesrel->rd_att, values, nulls);

    /* Insert tuple in catalog */
    (void)simple_heap_insert(pgxcnodesrel, htup);

    CatalogUpdateIndexes(pgxcnodesrel, htup);

    heap_freetuple(htup);

    heap_close(pgxcnodesrel, RowExclusiveLock);

    if (exist_logic_cluster() && node_type == PGXC_NODE_DATANODE) {
        Oid group_oid;
        Oid node_oid;

        CommandCounterIncrement();

        node_oid = get_pgxc_nodeoid(node_name);

        group_oid = get_pgxc_groupoid(PgxcGroupGetInstallationGroup());
        PgxcGroupAddNode(group_oid, node_oid);
    }
}

/*
 * PgxcNodeAlter
 *
 * Alter a PGXC node
 */
void PgxcNodeAlter(AlterNodeStmt* stmt)
{
    const char* node_name = stmt->node_name;
    char* node_host = NULL;
    char* node_host1 = NULL;
    char node_type, node_type_old;
    int node_port;
    int node_port1;
    int node_sctp_port;
    int node_sctp_port1;
    int node_strmctl_port;
    int node_strmctl_port1;
    bool host_is_primary = false;
    bool is_preferred = false;
    bool is_primary = false;
    bool is_central = false;
    bool is_active = true;
    bool rw = true;
    HeapTuple oldtup, newtup;
    Oid nodeOid = get_pgxc_nodeoid(node_name);
    Relation rel;
    Datum new_record[Natts_pgxc_node];
    bool new_record_nulls[Natts_pgxc_node];
    bool new_record_repl[Natts_pgxc_node];
    uint32 node_id;

    /* Only a DB administrator can alter cluster nodes */
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to change cluster nodes")));

    /* Look at the node tuple, and take exclusive lock on it */
    rel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

    /* Check that node exists */
    if (!OidIsValid(nodeOid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));

    /* Open new tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));
    if (!HeapTupleIsValid(oldtup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for object %u", nodeOid)));

    /*
     * check_options performs some internal checks on option values
     * so set up values.
     */
    node_host = get_pgxc_nodehost(nodeOid);
    node_port = get_pgxc_nodeport(nodeOid);
    node_sctp_port = get_pgxc_nodesctpport(nodeOid);
    node_strmctl_port = get_pgxc_nodestrmctlport(nodeOid);
    node_host1 = get_pgxc_nodehost1(nodeOid);
    node_port1 = get_pgxc_nodeport1(nodeOid);
    node_sctp_port1 = get_pgxc_nodesctpport1(nodeOid);
    node_strmctl_port1 = get_pgxc_nodestrmctlport1(nodeOid);

    is_preferred = is_pgxc_nodepreferred(nodeOid);
    is_primary = is_pgxc_nodeprimary(nodeOid);
    is_central = is_pgxc_central_nodeid(nodeOid);
    host_is_primary = is_pgxc_hostprimary(nodeOid);
    node_type = get_pgxc_nodetype(nodeOid);
    node_type_old = node_type;
    node_id = get_pgxc_node_id(nodeOid);

    /* Filter options */
    check_node_options(node_name,
        stmt->options,
        &node_type,
        &rw,
        &node_host,
        &node_port,
        &node_sctp_port,
        &node_strmctl_port,
        &node_host1,
        &node_port1,
        &node_sctp_port1,
        &node_strmctl_port1,
        &is_primary,
        &is_preferred,
        &host_is_primary,
        &is_central,
        &is_active);

    /*
     * Two nodes cannot be primary at the same time. If the primary
     * node is this node itself, well there is no point in having an
     * error.
     */
    if (is_primary && OidIsValid(u_sess->pgxc_cxt.primary_data_node) && nodeOid != u_sess->pgxc_cxt.primary_data_node)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: two nodes cannot be primary", node_name)));

    /* Check type dependency */
    if (node_type_old == PGXC_NODE_COORDINATOR &&
        (node_type == PGXC_NODE_DATANODE || node_type == PGXC_NODE_DATANODE_STANDBY))
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: cannot alter Coordinator to Datanode", node_name)));
    else if ((node_type_old == PGXC_NODE_DATANODE || node_type_old == PGXC_NODE_DATANODE_STANDBY) &&
             node_type == PGXC_NODE_COORDINATOR)
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC node %s: cannot alter Datanode to Coordinator", node_name)));

    /* Update values for catalog entry */
    int ss_rc = memset_s(new_record, sizeof(new_record), 0, sizeof(new_record));
    securec_check(ss_rc, "\0", "\0");
    ss_rc = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(ss_rc, "\0", "\0");
    ss_rc = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(ss_rc, "\0", "\0");
    new_record[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
    new_record_repl[Anum_pgxc_node_port - 1] = true;
    new_record[Anum_pgxc_node_sctp_port - 1] = Int32GetDatum(node_sctp_port);
    new_record_repl[Anum_pgxc_node_sctp_port - 1] = true;
    new_record[Anum_pgxc_node_strmctl_port - 1] = Int32GetDatum(node_strmctl_port);
    new_record_repl[Anum_pgxc_node_strmctl_port - 1] = true;
    new_record[Anum_pgxc_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
    new_record_repl[Anum_pgxc_node_host - 1] = true;

    new_record[Anum_pgxc_node_port1 - 1] = Int32GetDatum(node_port1);
    new_record_repl[Anum_pgxc_node_port1 - 1] = true;
    new_record[Anum_pgxc_node_sctp_port1 - 1] = Int32GetDatum(node_sctp_port1);
    new_record_repl[Anum_pgxc_node_sctp_port1 - 1] = true;
    new_record[Anum_pgxc_node_strmctl_port1 - 1] = Int32GetDatum(node_strmctl_port1);
    new_record_repl[Anum_pgxc_node_strmctl_port1 - 1] = true;
    new_record[Anum_pgxc_node_host1 - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host1));
    new_record_repl[Anum_pgxc_node_host1 - 1] = true;

    new_record[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
    new_record_repl[Anum_pgxc_node_type - 1] = true;
    new_record[Anum_pgxc_host_is_primary - 1] = BoolGetDatum(host_is_primary);
    new_record_repl[Anum_pgxc_host_is_primary - 1] = true;
    new_record[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
    new_record_repl[Anum_pgxc_node_is_primary - 1] = true;
    new_record[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
    new_record_repl[Anum_pgxc_node_is_preferred - 1] = true;

    /*
     * check if exist a coordinator is central node,
     * while setting to a node to be central node.
     */
    if (is_central) {
        if (node_type == 'D')
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Datanode %s: can not be central node.", node_name)));

        HeapTuple tmptup;
        TupleDesc pg_node_dsc = RelationGetDescr(rel);
        SysScanDesc scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
        while (HeapTupleIsValid((tmptup = systable_getnext(scan)))) {
            bool isnull = true;
            Oid scanoid = HeapTupleGetOid(tmptup);
            if (scanoid == nodeOid)
                continue;

            Datum centralDatum = heap_getattr(tmptup, Anum_pgxc_node_is_central, pg_node_dsc, &isnull);

            if (isnull)
                continue;

            if (DatumGetBool(centralDatum)) {
                Datum nameDatum = heap_getattr(tmptup, Anum_pgxc_node_name, pg_node_dsc, &isnull);

                if (!isnull)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("PGXC node %s is central node already.", DatumGetCString(nameDatum))));
            }
        }
        systable_endscan(scan);
    }

    new_record[Anum_pgxc_node_is_central - 1] = BoolGetDatum(is_central);
    new_record_repl[Anum_pgxc_node_is_central - 1] = true;

    new_record[Anum_pgxc_node_id - 1] = UInt32GetDatum(node_id);
    new_record_repl[Anum_pgxc_node_id - 1] = true;

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), new_record, new_record_nulls, new_record_repl);
    simple_heap_update(rel, &oldtup->t_self, newtup);

    /* Update indexes */
    CatalogUpdateIndexes(rel, newtup);

    /* Release lock at Commit */
    heap_close(rel, NoLock);
}

void PgxcCoordinatorAlter(AlterCoordinatorStmt* stmt)
{
    const char* node_name = stmt->node_name;
    HeapTuple oldtup, newtup;
    Oid nodeOid = get_pgxc_nodeoid(node_name);
    Relation rel;
    Datum values[Natts_pgxc_node];
    bool nulls[Natts_pgxc_node];
    bool new_record_repl[Natts_pgxc_node];
    bool result = false;
    errno_t rc;

    if (!parse_bool(stmt->set_value, &result)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("parameter requires a Boolean value")));
    }

    /* Only a DB administrator can alter cluster nodes */
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to change cluster nodes")));

    /* Look at the node tuple, and take exclusive lock on it */
    rel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

    /* Check that node exists */
    if (!OidIsValid(nodeOid))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));

    /* Open new tuple, checks are performed on it and new values */
    oldtup = SearchSysCacheCopy1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));
    if (!HeapTupleIsValid(oldtup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for object %u", nodeOid)));
    }

    Form_pgxc_node oldnode = (Form_pgxc_node)GETSTRUCT(oldtup);

    /* Update values for catalog entry */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(rc, "\0", "\0");

    /* Build entry tuple */
    values[Anum_pgxc_node_name - 1] = NameGetDatum(&oldnode->node_name);
    values[Anum_pgxc_node_type - 1] = CharGetDatum(oldnode->node_type);
    values[Anum_pgxc_node_port - 1] = Int32GetDatum(oldnode->node_port);
    values[Anum_pgxc_node_sctp_port - 1] = Int32GetDatum(oldnode->sctp_port);
    values[Anum_pgxc_node_strmctl_port - 1] = Int32GetDatum(oldnode->control_port);
    values[Anum_pgxc_node_host - 1] = NameGetDatum(&oldnode->node_host);
    values[Anum_pgxc_node_port1 - 1] = Int32GetDatum(oldnode->node_port1);
    values[Anum_pgxc_node_sctp_port1 - 1] = Int32GetDatum(oldnode->sctp_port1);
    values[Anum_pgxc_node_strmctl_port1 - 1] = Int32GetDatum(oldnode->control_port1);
    values[Anum_pgxc_node_host1 - 1] = NameGetDatum(&oldnode->node_host1);
    values[Anum_pgxc_host_is_primary - 1] = BoolGetDatum(oldnode->hostis_primary);
    values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(oldnode->nodeis_primary);
    values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(oldnode->nodeis_preferred);
    values[Anum_pgxc_node_is_central - 1] = BoolGetDatum(oldnode->nodeis_central);
    values[Anum_pgxc_node_id - 1] = Int32GetDatum(oldnode->node_id);
    ;
    values[Anum_pgxc_node_is_active - 1] = BoolGetDatum(result);

    new_record_repl[Anum_pgxc_node_is_active - 1] = true;

    /* Update relation */
    newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, new_record_repl);
    simple_heap_update(rel, &oldtup->t_self, newtup);

    /* Update indexes */
    CatalogUpdateIndexes(rel, newtup);

    /* Release lock at Commit */
    heap_close(rel, NoLock);
}

/*
 * PgxcNodeRemove
 *
 * Remove a PGXC node and return the node type
 */
char PgxcNodeRemove(DropNodeStmt* stmt)
{
    Relation relation;
    HeapTuple tup;
    TableScanDesc scan;
    Form_pgxc_node nform;
    char ntype = PGXC_NODE_NONE;
    const char* node_name = stmt->node_name;
    Oid noid = get_pgxc_nodeoid(node_name);

    /* Only a DB administrator can remove cluster nodes */
    if (!superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to remove cluster nodes")));

    /* Check if node is defined */
    if (!OidIsValid(noid)) {
        if (stmt->missing_ok) {
            ereport(NOTICE, (errmsg("PGXC Node \"%s\" does not exist, skipping", node_name)));
            return PGXC_NODE_NONE;
        } else {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));
        }
    }

    if (strcmp(node_name, g_instance.attr.attr_common.PGXCNodeName) == 0)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PGXC Node %s: cannot drop local node", node_name)));

    if (PGXC_NODE_DATANODE == get_pgxc_nodetype(noid)) {
        if (exist_logic_cluster()) {
            if (IsNodeInLogicCluster(&noid, 1, InvalidOid))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("PGXC Node %s: node is in logic cluster", node_name)));

            Oid group_oid;
            group_oid = get_pgxc_groupoid(VNG_OPTION_ELASTIC_GROUP);
            PgxcGroupRemoveNode(group_oid, noid);

            group_oid = get_pgxc_groupoid(PgxcGroupGetInstallationGroup());
            PgxcGroupRemoveNode(group_oid, noid);
        }
    }

    /* PGXC:
     * Is there any group which has this node as member
     * XC Tables will also have this as a member in their array
     * Do this search in the local data structure.
     * If a node is removed, it is necessary to check if there is a distributed
     * table on it. If there are only replicated table it is OK.
     * However, we have to be sure that there are no pooler agents in the cluster pointing to it.
     */
    /* Delete the pgxc_node tuple(s) */
    relation = heap_open(PgxcNodeRelationId, RowExclusiveLock);
    scan = tableam_scan_begin(relation, SnapshotNow, 0, NULL);
    /* primary and standby DNs may have the same name */
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        nform = (Form_pgxc_node)GETSTRUCT(tup);

        if (pg_strcasecmp(NameStr(nform->node_name), node_name) == 0) {
            char my_type = nform->node_type;
            simple_heap_delete(relation, &tup->t_self);

            /* get the type of the node */
            if (my_type == PGXC_NODE_COORDINATOR) {
                /* there should not be another node of the same name for CN */
                ntype = my_type;
                break;
            } else if (my_type == PGXC_NODE_DATANODE) {
                ntype = my_type;
                /* primary and standby DNs can have the same name */
                if (g_instance.attr.attr_storage.replication_type != RT_WITH_MULTI_STANDBY)
                    break;
            }
        }
    }

    tableam_scan_end(scan);
    heap_close(relation, RowExclusiveLock);

    if (ntype == PGXC_NODE_NONE) { /* should not happen */
        if (stmt->missing_ok)
            ereport(NOTICE, (errmsg("PGXC Node \"%s\" does not exist, skipping", node_name)));
        else
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("PGXC Node %s: object not defined", node_name)));
    }

    ereport(DEBUG2, (errmsg("Drop Node %s: local succeed", node_name)));
    return ntype;
}

/*
 * Get all data node name
 */
List* PgxcNodeGetAllDataNodeNames(void)
{
    List* names = NIL;
    LWLockAcquire(NodeTableLock, LW_SHARED);

    for (int i = 0; i < *t_thrd.pgxc_cxt.shmemNumDataNodes; i++) {
        names = lappend(names, pstrdup(NameStr(t_thrd.pgxc_cxt.dnDefs[i].nodename)));
    }
    LWLockRelease(NodeTableLock);

    return names;
}

List* PgxcNodeGetDataNodeNames(List* nodeList)
{
    List* names = NIL;
    ListCell* lc = NULL;

    LWLockAcquire(NodeTableLock, LW_SHARED);
    foreach (lc, nodeList) {
        int idx = lfirst_int(lc);
        names = lappend(names, pstrdup(NameStr(t_thrd.pgxc_cxt.dnDefs[idx].nodename)));
    }
    LWLockRelease(NodeTableLock);
    return names;
}

List* PgxcNodeGetDataNodeOids(List* nodeList)
{
    List* oids = NIL;
    ListCell* lc = NULL;

    LWLockAcquire(NodeTableLock, LW_SHARED);
    foreach (lc, nodeList) {
        int idx = lfirst_int(lc);
        oids = lappend_int(oids, t_thrd.pgxc_cxt.dnDefs[idx].nodeoid);
    }
    LWLockRelease(NodeTableLock);
    return oids;
}

int pickup_random_datanode(int numnodes)
{
    Assert(numnodes > 0);

    if (u_sess->attr.attr_sql.enable_random_datanode) {
        return ((unsigned int)random()) % numnodes;
    }

    return 0;
}

/*
 * Get all coordinator node index in t_thrd.pgxc_cxt.coDefs array.
 */
List* PgxcGetCoordlist(bool exclude_self)
{
    List* coordlist = NULL;

    LWLockAcquire(NodeTableLock, LW_SHARED);
    for (int i = 0; i < u_sess->pgxc_cxt.NumCoords; ++i) {
        if (exclude_self &&
            strcmp((char*)&t_thrd.pgxc_cxt.coDefs[i].nodename, g_instance.attr.attr_common.PGXCNodeName) == 0)
            continue;

        coordlist = lappend_int(coordlist, i);
    }
    LWLockRelease(NodeTableLock);

    return coordlist;
}

void PgxcGetNodeName(int node_idx, char* nodenamebuf, int len)
{
    Assert(nodenamebuf != NULL);
    LWLockAcquire(NodeTableLock, LW_SHARED);
    errno_t errval = strncpy_s(nodenamebuf, len, NameStr(t_thrd.pgxc_cxt.coDefs[node_idx].nodename), len - 1);
    securec_check_errval(errval, , LOG);
    LWLockRelease(NodeTableLock);
}

/*
 * @Description: get node index
 * @IN nodename: node name
 * @Return: index, if not found ,return -1
 * @See also:
 */
int PgxcGetNodeIndex(const char* nodename)
{
    int index = -1;
    Assert(nodename != NULL);
    LWLockAcquire(NodeTableLock, LW_SHARED);

    for (int i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoords; ++i) {
        if (strcmp(NameStr(t_thrd.pgxc_cxt.coDefs[i].nodename), nodename) == 0) {
            index = i;
            break;
        }
    }

    LWLockRelease(NodeTableLock);
    return index;
}

/* @Description: get node Oid
 * @IN nodename: node Idx
 * @Return: Oid: return InvalidOid if not found
 * @See also:
 */
Oid PgxcGetNodeOid(int nodeIdx)
{
    Oid nodeOid = InvalidOid;
    if (IS_PGXC_COORDINATOR) {
        nodeOid = nodeIdx;
    } else {
        if (global_node_definition != NULL && global_node_definition->nodesDefinition != NULL &&
            nodeIdx < global_node_definition->num_nodes) {
            nodeOid = global_node_definition->nodesDefinition[nodeIdx].nodeoid;
        }
    }
    return nodeOid;
}

/*
 * Judge where the nodename is CCN or not.
 */
bool PgxcIsCentralCoordinator(const char* NodeName)
{
    bool is_ccn = false;

    LWLockAcquire(NodeTableLock, LW_SHARED);

    for (int i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoords; ++i) {
        if (strcmp(NameStr(t_thrd.pgxc_cxt.coDefs[i].nodename), NodeName) == 0) {
            is_ccn = t_thrd.pgxc_cxt.coDefs[i].nodeis_central;
            break;
        }
    }

    LWLockRelease(NodeTableLock);

    return is_ccn;
}

/*
 * @Description: get central node index
 * @IN void
 * @Return: node index
 * @See also:
 */
int PgxcGetCentralNodeIndex()
{
    int index = -1;

    LWLockAcquire(NodeTableLock, LW_SHARED);

    for (int i = 0; i < *t_thrd.pgxc_cxt.shmemNumCoords; ++i) {
        if (t_thrd.pgxc_cxt.coDefs[i].nodeis_central) {
            index = i;
            break;
        }
    }

    LWLockRelease(NodeTableLock);

    return index;
}

/*
 * get_pgxc_primary_datanode_oid
 *		Obtain PGXC primary node Oid for given node Oid
 *		Return Invalid Oid if object does not exist
 */
Oid get_pgxc_primary_datanode_oid(Oid nodeoid)
{
    return PgxcNodeGetPrimaryDNFromMatric(nodeoid);
}

/*
 * @Description: add nodeid about the standby nodes.
 * @IN slaveNodeNums: slave node numbers.
 * @IN matricRow: segment row information.
 * @INOUT needCreateNode: used for the active/standby relationship table.
 * @Return: null
 * @See also:
 */
void set_oid_from_matric(int slaveNodeNums, int matricRow, NodeRelationInfo *needCreateNode)
{
    int i, j;
    
    for (i = 0, j = 1; i < (slaveNodeNums + 1); i++) {
        if (needCreateNode->primaryNodeId != u_sess->pgxc_cxt.dn_matrics[matricRow][i]) {
            needCreateNode->nodeList[j++] = u_sess->pgxc_cxt.dn_matrics[matricRow][i];
        }
    }
    
    return;
}

/*
 * @Description: add nodeid about the standby nodes.
 * @INOUT needCreateNode: used for the active/standby relationship table.
 * @INOUT isMatricVisited: prune to record access nodes and avoid repeated traversal.
 * @Return: isFind
 * @See also:
 */
bool set_dnoid_info_from_matric(NodeRelationInfo *needCreateNode, bool *isMatricVisited)
{
    int i, j, slaveNodeNums;
    bool isFind = false;

    /* single dn */
    if ((u_sess->pgxc_cxt.dn_matrics == NULL) ||
        (u_sess->pgxc_cxt.NumDataNodes <= 0) ||
        (u_sess->pgxc_cxt.NumStandbyDataNodes <= 0)) {
        return true;
    }
    
    slaveNodeNums = u_sess->pgxc_cxt.NumStandbyDataNodes / u_sess->pgxc_cxt.NumDataNodes;
    
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        if (isMatricVisited[i] == true) {
            continue;
        }
        
        for (j = 0; j < (slaveNodeNums + 1); j++) {
            /* check whether the oid1 is in current shard. */
            if (u_sess->pgxc_cxt.dn_matrics[i][j] == needCreateNode->nodeList[0]) {
                isMatricVisited[i] = true;
                isFind = true;
                set_oid_from_matric(slaveNodeNums, i, needCreateNode);
                return isFind;
            }
        }
    }
    
    return isFind;
}
