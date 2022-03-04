/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ledger_check.cpp
 *     functions for ledger hash consistent check and history table repair.
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_ledger/ledger_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/tableam.h"
#include "catalog/cstore_ctlg.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_partition_fn.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "access/hash.h"
#include "access/visibilitymap.h"
#include "libpq/md5.h"
#include "utils/uuid.h"
#include "utils/snapmgr.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "pgxc/execRemote.h"
#endif
#include "catalog/gs_global_chain.h"
#include "gs_ledger/ledger_check.h"
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"
#include "gs_ledger/blockchain.h"

/*
 * gen_usertable_hash_sum -- calculate sum(hash) of the relation
 *
 * rel: user relation
 */
static uint64 gen_usertable_hash_sum(Relation rel)
{
    uint64 rel_hash = 0;
    bool is_null = false;
    int hash_natt = user_hash_attrno(rel->rd_att);
    Assert(hash_natt >= 0);
    HeapTuple tuple;
    TupleDesc desc = rel->rd_att;
    Snapshot snapshot = GetActiveSnapshot();
    TableScanDesc scan;
    if (RELATION_CREATE_BUCKET(rel)) {
        Relation bucket_rel = NULL;
        oidvector *bucket_list = searchHashBucketByOid(rel->rd_bucketoid);
        for (int i = 0; i < bucket_list->dim1; i++) {
            bucket_rel = bucketGetRelation(rel, NULL, bucket_list->values[i]);
            scan = heap_beginscan(bucket_rel, snapshot, 0, NULL);
            while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
                rel_hash += DatumGetUInt64(heap_getattr(tuple, hash_natt + 1, desc, &is_null));
            }
            heap_endscan(scan);
            bucketCloseRelation(bucket_rel);
        }
    } else {
        scan = heap_beginscan(rel, snapshot, 0, NULL);
        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
            rel_hash += DatumGetUInt64(heap_getattr(tuple, hash_natt + 1, desc, &is_null));
        }
        heap_endscan(scan);
    }
    return rel_hash;
}

/*
 * get_usertable_hash_sum -- calculate sum(hash) of the relation of specified oid
 *
 * relid: user table oid
 *
 * NOTICE: if user table is partition table, just use the original relation id.
 */
static uint64 get_usertable_hash_sum(Oid relid)
{
    uint64 rel_hash = 0;
    Relation rel = NULL;
    rel = heap_open(relid, AccessShareLock);
    if (!RelationIsPartitioned(rel)) {
        rel_hash = gen_usertable_hash_sum(rel);
    } else {
        List *partition_list = NIL;
        ListCell *lc = NULL;
        Partition part;
        Relation fake_rel;
        partition_list = relationGetPartitionList(rel, AccessShareLock);
        foreach (lc, partition_list) {
            part = (Partition)lfirst(lc);
            fake_rel = partitionGetRelation(rel, part);
            rel_hash += gen_usertable_hash_sum(fake_rel);
            releaseDummyRelation(&fake_rel);
        }
        releasePartitionList(rel, &partition_list, AccessShareLock);
    }
    heap_close(rel, AccessShareLock);
    return rel_hash;
}

/*
 * get_histtable_hash_sum -- calculate sum(hash_ins) - sum(hash_del) of the history relation of specified oid
 *
 * relid: history table oid
 */
static uint64 get_histtable_hash_sum(Oid hist_oid)
{
    uint64 rel_hash = 0;
    bool is_null = false;
    Relation hist_rel;
    TableScanDesc scan;
    HeapTuple tuple;
    Snapshot snapshot = GetActiveSnapshot();

    hist_rel = heap_open(hist_oid, AccessShareLock);
    scan = heap_beginscan(hist_rel, snapshot, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Datum value = heap_getattr(tuple, USERCHAIN_COLUMN_HASH_INS + 1, hist_rel->rd_att, &is_null);
        if (!is_null) {
            rel_hash += DatumGetUInt64(value);
        }

        value = heap_getattr(tuple, USERCHAIN_COLUMN_HASH_DEL + 1, hist_rel->rd_att, &is_null);
        if (!is_null) {
            rel_hash -= DatumGetUInt64(value);
        }
    }
    heap_endscan(scan);
    heap_close(hist_rel, AccessShareLock);
    return rel_hash;
}

/*
 * has_ledger_consistent_privilege -- calculate sum(hash_ins) - sum(hash_del) of the history relation of specified oid
 *
 * relid: history table oid
 */
static bool has_ledger_consistent_privilege(Oid relid, Oid namespaceId)
{
    if (pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_USAGE) != ACLCHECK_OK) {
        return false;
    }
    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK) {
        return false;
    }
    return true;
}

/*
 * is_hist_hash_identity -- check whether user table hash and history table hash are equal
 *
 * relid: user table oid
 * res_hash: hash sum of history table
 */
bool is_hist_hash_identity(Oid relid, uint64 *res_hash)
{
    uint64 user_hash_sum;
    uint64 hist_hash_sum;
    char hist_name[NAMEDATALEN];
    char *rel_name = get_rel_name(relid);
    if (!get_hist_name(relid, rel_name, hist_name)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("get hist table name failed.")));
    }
    Oid histoid = get_relname_relid(hist_name, PG_BLOCKCHAIN_NAMESPACE);
    if (!OidIsValid(histoid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find hist table of \"%s\".", rel_name)));
    }

    user_hash_sum = get_usertable_hash_sum(relid);
    hist_hash_sum = get_histtable_hash_sum(histoid);

    *res_hash = hist_hash_sum;
    return user_hash_sum == hist_hash_sum;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * StrategyFuncAnd -- collect remote nodes with AND
 *
 * state: parallel function state
 *
 * Note: collect the result of function from remote nodes,
 * set true if all success; set false if any one fail.
 */
static void StrategyFuncAnd(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    bool result = true;

    Assert(state);
    Assert(state->tupstore);
    Assert(state->tupdesc);
    slot = MakeSingleTupleTableSlot(state->tupdesc);

    while (true) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        if (!DatumGetBool(tableam_tslot_getattr(slot, 1, &isnull))) {
            result = false;
            break;
        }
        (void)ExecClearTuple(slot);
    }

    state->result = result;
}

/*
 * StrategyFuncUInt64Sum -- collect remote nodes with Plus
 *
 * state: parallel function state
 *
 * Note: collect the result of function from remote nodes,
 * sum all nodes result with uint64.
 */
static void StrategyFuncUInt64Sum(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    int64 result = 0;

    Assert(state && state->tupstore && state->tupdesc);
    slot = MakeSingleTupleTableSlot(state->tupdesc);

    while (true) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        result += DatumGetUInt64(tableam_tslot_getattr(slot, 1, &isnull));
        ExecClearTuple(slot);
    }

    state->result = result;
}
#endif

/*
 * ledger_hist_check -- check whether user table hash and history table hash are equal
 *
 * parameter1: user table name [type: text]
 * parameter2: namespace of user table [type: text]
 */
Datum ledger_hist_check(PG_FUNCTION_ARGS)
{
    Oid relid;
    Oid nsp_oid;
    uint64 res_hash;
    bool res = false;
    char *table_name;
    char *table_nsp;
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    nsp_oid = get_namespace_oid(table_nsp, false);
    relid = get_relname_relid(table_name, nsp_oid);
    ledger_usertable_check(relid, nsp_oid, table_name, table_nsp);

    if (!has_ledger_consistent_privilege(relid, nsp_oid)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    res = is_hist_hash_identity(relid, &res_hash);

#ifdef ENABLE_MULTIPLE_NODES
    if (!IsConnFromCoord()) {
        StringInfoData buf;
        ParallelFunctionState* state = NULL;
        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.ledger_hist_check('%s', '%s')", table_nsp, table_name);
        /* Get all hash diffs from DNs in distribute scenairo. */
        state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncAnd);
        res &= state->result;
        FreeParallelFunctionState(state);
    }
#endif
    return BoolGetDatum(res);
}

/*
 * get_gchain_relhash_sum -- calculate relhash from gs_global_chain
 *
 * relid: user table oid
 */
static uint64 get_gchain_relhash_sum(Oid relid)
{
    uint64 relhash = 0;
    HeapTuple tuple = NULL;

    /* scan the gs_global_chain catalog by relid */
    Relation gchain_rel = heap_open(GsGlobalChainRelationId, AccessShareLock);
    Form_gs_global_chain rdata = NULL;
    TableScanDesc scan = heap_beginscan(gchain_rel, SnapshotNow, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        rdata = (Form_gs_global_chain)GETSTRUCT(tuple);
        if (rdata == NULL || rdata->relid != relid) {
            continue;
        }
        relhash += rdata->relhash;
    }
    heap_endscan(scan);
    heap_close(gchain_rel, AccessShareLock);
    return relhash;
}

/*
 * get_dn_hist_relhash -- calculate relhash from gs_global_chain or history table
 *
 * parameter1: user table name [type: text]
 * parameter2: namespace of user table [type: text]
 *
 * NOTICE: if current node is cn, it's return the relhash calculated from gs_global_chain,
 * if current node is dn, it's return the relhash calculated from history table.
 */
Datum get_dn_hist_relhash(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return UInt64GetDatum(0);
#else
    if (!IsConnFromCoord()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
        return UInt64GetDatum(0);
    }
    Oid user_relid;
    Oid nsp_oid;
    uint64 res_hash;
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);
    char *table_name;
    char *table_nsp;

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    nsp_oid = get_namespace_oid(table_nsp, false);
    user_relid = get_relname_relid(table_name, nsp_oid);
    ledger_usertable_check(user_relid, nsp_oid, table_name, table_nsp);

    if (!has_ledger_consistent_privilege(user_relid, nsp_oid)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    if (IS_PGXC_DATANODE) {
        if (!is_hist_hash_identity(user_relid, &res_hash)) {
            res_hash = 0;
        }
    } else {
        res_hash = get_gchain_relhash_sum(user_relid);
    }

    return UInt64GetDatum(res_hash);
#endif
}

/*
 * ledger_gchain_check -- calculate relation hash from all cn and all dn, and return cn_hash == dn_hash
 *
 * parameter1: user table name [type: text]
 * parameter2: namespace of user table [type: text]
 */
Datum ledger_gchain_check(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR || IsConnFromCoord()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
#endif
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);
    char *table_name;
    char *table_nsp;
    Oid user_relid;
    Oid nsp_oid;
    uint64 cn_hash;
    uint64 dn_hash;
    bool res;

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    nsp_oid = get_namespace_oid(table_nsp, false);
    user_relid = get_relname_relid(table_name, nsp_oid);
    ledger_usertable_check(user_relid, nsp_oid, table_name, table_nsp);

    if (!has_ledger_consistent_privilege(user_relid, nsp_oid)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }

    res = is_hist_hash_identity(user_relid, &dn_hash);
    if (!res) {
        return BoolGetDatum(res);
    }
    cn_hash = get_gchain_relhash_sum(user_relid);
#ifdef ENABLE_MULTIPLE_NODES
    ParallelFunctionState* state = NULL;
    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.get_dn_hist_relhash('%s', '%s')", table_nsp, table_name);
    /* Get all hash diffs from DNs in distribute scenairo. */
    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncUInt64Sum);
    dn_hash += state->result;
    FreeParallelFunctionState(state);
    if (GetAllCoordNodes() != NIL) {
        /* Get and accumulate all cnhash from all CNs. */
        state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncUInt64Sum, true, EXEC_ON_COORDS);
        cn_hash += state->result;
        FreeParallelFunctionState(state);
    }
#endif
    return BoolGetDatum(dn_hash == cn_hash);
}

/*
 * repaire_hist_table_internal -- compare hash and repair hist table
 *
 * relid: user table oid
 * rel_name: user table name
 * nspoid: user table namespace oid
 * option: choose return tuple_hash_sum if true, or hash_diff if false.
 *
 * Note: the diff means tuple_hash_sum - hist_hash_sum.
 */
static uint64 repaire_hist_table_internal(Oid relid, char *rel_name, Oid nspoid, bool option)
{
    uint64 rel_hash;
    uint64 hash_diff;
    char histname[NAMEDATALEN];
    get_hist_name(relid, rel_name, histname, nspoid);
    Oid histoid = get_relname_relid(histname, PG_BLOCKCHAIN_NAMESPACE);
    if (!OidIsValid(histoid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("The hist table of \"%s\" is not exist.", rel_name)));
    }
    rel_hash = get_usertable_hash_sum(relid);
    hash_diff = rel_hash - get_histtable_hash_sum(histoid);
    if (hash_diff != 0) {
        /* Do hist table repair. */
        hist_table_record_internal(histoid, &hash_diff, NULL);
    }
    return option ? rel_hash : hash_diff;
}

/*
 * ledger_hist_repair -- repair the history table of specified user table
 *
 * parameter1: user table name [type: text]
 * parameter2: namespace of user table [type: text]
 */
Datum ledger_hist_repair(PG_FUNCTION_ARGS)
{
    if (!isRelSuperuser() && !isAuditadmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);
    char *table_name;
    char *table_nsp;
    Oid relid;
    Oid nspoid;
    uint64 delta = 0;

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    nspoid = get_namespace_oid(table_nsp, false);
    relid = get_relname_relid(table_name, nspoid);
    ledger_usertable_check(relid, nspoid, table_name, table_nsp);

    /*
     * Repair hist table of current datanode. Get hash sum of hist
     * table and rel_hash of usertable, append the difference to hist table.
     */
    if (g_instance.role == VDATANODE || g_instance.role == VSINGLENODE) {
        delta = repaire_hist_table_internal(relid, table_name, nspoid, false);
    }

    if (g_instance.role == VCOORDINATOR || g_instance.role == VSINGLENODE) {
#ifdef ENABLE_MULTIPLE_NODES
        ParallelFunctionState* state = NULL;
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_catalog.ledger_hist_repair('%s', '%s')", table_nsp, table_name);
        /* Get all hash diffs from DNs in distribute scenairo. */
        state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncUInt64Sum);
        delta += state->result;

        FreeParallelFunctionState(state);
#endif
        if (delta != 0) {
            ledger_gchain_append(relid, "HIST REPAIR.", delta);
        }
    }

    return UInt64GetDatum(delta);
}

/*
 * ledger_gchain_repair -- repair gs_global_chain of specified user table
 *
 * parameter1: user table name [type: text]
 * parameter2: namespace of user table [type: text]
 */
Datum ledger_gchain_repair(PG_FUNCTION_ARGS)
{
    if (!isRelSuperuser() && !isAuditadmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);
    char *table_name;
    char *table_nsp;
    Oid relid;
    Oid nspoid;
    uint64 dn_hash = 0;

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    nspoid = get_namespace_oid(table_nsp, false);
    relid = get_relname_relid(table_name, nspoid);
    ledger_usertable_check(relid, nspoid, table_name, table_nsp);

    /*
     * Repair hist table of current datanode. Get hash sum of hist
     * table and rel_hash of usertable, append the difference to hist table.
     */
    if (g_instance.role == VDATANODE || g_instance.role == VSINGLENODE) {
        dn_hash = repaire_hist_table_internal(relid, table_name, nspoid, true);
    }

    uint64 rel_hash = dn_hash;
    uint64 cn_hash = 0;
    if (g_instance.role == VCOORDINATOR || g_instance.role == VSINGLENODE) {
        uint64 delta = 0;
        cn_hash = get_gchain_relhash_sum(relid);
        if (!IsConnFromCoord()) {
#ifdef ENABLE_MULTIPLE_NODES
            /*
             * CN accumulate all gchain cn_hash from all CNs, get all dn_hash from all DNs.
             * Then compare cn_hash and dn_hash, and fill up delta hash to gchain for repairing.
             */
            ParallelFunctionState* state = NULL;
            StringInfoData buf;
            initStringInfo(&buf);
            appendStringInfo(&buf, "SELECT pg_catalog.ledger_gchain_repair('%s', '%s')", table_nsp, table_name);
            /* Get and accumulate all dn_hash from all DNs. */
            state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncUInt64Sum);
            dn_hash += state->result;
            FreeParallelFunctionState(state);
            if (GetAllCoordNodes() != NIL) {
                /* Get and accumulate all cn_hash from all CNs. */
                state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncUInt64Sum, true, EXEC_ON_COORDS);
                cn_hash += state->result;
                FreeParallelFunctionState(state);
            }
#endif
            delta = dn_hash - cn_hash;
            if (delta != 0) {
                ledger_gchain_append(relid, "GCHAIN REPAIR.", delta);
            }
        }
        rel_hash = cn_hash;
    }

    return UInt64GetDatum(rel_hash);
}