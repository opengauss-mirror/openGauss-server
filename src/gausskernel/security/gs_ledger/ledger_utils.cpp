/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * ledger_utils.cpp
 *    common functions for ledger tables and ledger hash cache achivement.
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_ledger/ledger_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "gs_ledger/ledger_utils.h"
#include "catalog/gs_global_chain.h"

static pg_atomic_uint64 g_blocknum = 0;
static HTAB *g_recnum_cache = NULL;

/*
 * reload_next_g_blocknum -- load next blocknum from gchain.
 * 
 * Note:If gchain is empty, next blocknum will start from 0.
 */
static uint32 reload_next_g_blocknum()
{
    Relation gchain_rel = NULL;
    HeapTuple tup = NULL;
    TableScanDesc scan;
    uint32 blocknum;
    uint32 max_num = 0;
    bool isnull = false;

    gchain_rel = heap_open(GsGlobalChainRelationId, RowExclusiveLock);
    scan = heap_beginscan(gchain_rel, SnapshotAny, 0, NULL);
    while ((tup = heap_getnext(scan, BackwardScanDirection)) != NULL) {
        blocknum = DatumGetUInt32(heap_getattr(tup, Anum_gs_global_chain_blocknum,
                                               RelationGetDescr(gchain_rel), &isnull));
        if (blocknum > max_num) {
            max_num = blocknum;
        } else {
            break;
        }
    }
    heap_endscan(scan);
    heap_close(gchain_rel, RowExclusiveLock);
    return max_num;
}

/*
 * get_next_g_blocknum -- get next blocknum for gchain record.
 * 
 * Note:provide next blocknum and auto increment itself.
 */
uint64 get_next_g_blocknum()
{
    uint64 res = 0;
    if (g_blocknum == 0) {
        LWLockAcquire(GlobalPrevHashLock, LW_EXCLUSIVE);
        if (g_blocknum == 0) {
            pg_atomic_fetch_add_u64(&g_blocknum, 1);
            int cur_num = reload_next_g_blocknum();
            pg_atomic_fetch_add_u64(&g_blocknum, cur_num);
        }
        LWLockRelease(GlobalPrevHashLock);
    }
    LWLockAcquire(GlobalPrevHashLock, LW_SHARED);
    res = pg_atomic_fetch_add_u64(&g_blocknum, 1);
    LWLockRelease(GlobalPrevHashLock);
    return res;
}

void reset_g_blocknum()
{
    g_blocknum = 0;
}

/*
 * reload_g_rec_num -- load next rec_num from hist table.
 *
 * histoid: hist table oid.
 *
 * Note:return next rec_num and auto increment.
 */
uint64 reload_g_rec_num(Oid histoid)
{
    if (!OidIsValid(histoid)) {
        return false;
    }
    Relation histRelation = NULL;
    HeapTuple tup = NULL;
    TableScanDesc scan;
    uint64 max_rec_num = 0;
    uint64 rec_num;
    bool hist_empty = true;
    bool isnull = false;
    bool found;

    histRelation = heap_open(histoid, AccessShareLock);
    scan = heap_beginscan(histRelation, SnapshotNow, 0, NULL);
    while ((tup = heap_getnext(scan, BackwardScanDirection)) != NULL) {
        rec_num = DatumGetUInt64(heap_getattr(tup, 1, RelationGetDescr(histRelation), &isnull));
        if (rec_num >= max_rec_num) {
            max_rec_num = rec_num;
            hist_empty = false;
        }
    }
    heap_endscan(scan);
    heap_close(histRelation, AccessShareLock);
    RecNumItem *item = (RecNumItem *)hash_search(g_recnum_cache, &histoid, HASH_ENTER, &found);
    if (hist_empty) {
        rec_num = 0;
    } else {
        rec_num = max_rec_num + 1;
    }
    item->rec_num = rec_num + 1;
    return rec_num;
}

/*
 * get_next_recnum -- provide next rec_num.
 *
 * histoid: hist table oid.
 */
uint64 get_next_recnum(Oid histoid)
{
    if (g_recnum_cache == NULL) {
        errno_t rc;
        HASHCTL ctl;
        LWLockAcquire(BlockchainVersionLock, LW_EXCLUSIVE);
        /* To avoid multiple threads double create hash table. */
        if (g_recnum_cache == NULL) {
            rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
            securec_check(rc, "\0", "\0");
            ctl.keysize = sizeof(Oid);
            ctl.entrysize = sizeof(RecNumItem);
            ctl.hash = oid_hash;
            ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY);
            g_recnum_cache = hash_create("global recnum cache", 256, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
            if (g_recnum_cache == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INITIALIZE_FAILED), errmsg("could not init global recnum cache")));
            }
        }
        LWLockRelease(BlockchainVersionLock);
    }

    bool found = false;
    LWLockAcquire(BlockchainVersionLock, LW_SHARED);
    uint64 res;
    RecNumItem *item = (RecNumItem *)hash_search(g_recnum_cache, &histoid, HASH_FIND, &found);
    if (found) {
        res = item->rec_num;
        pg_atomic_add_fetch_u64(&item->rec_num, 1);
    } else {
        LWLockRelease(BlockchainVersionLock);
        LWLockAcquire(BlockchainVersionLock, LW_EXCLUSIVE);
        res = reload_g_rec_num(histoid);
    }
    LWLockRelease(BlockchainVersionLock);
    return res;
}

bool remove_hist_recnum_cache(Oid histoid)
{
    if (!OidIsValid(histoid) || g_recnum_cache == NULL) {
        return false;
    }
    hash_search(g_recnum_cache, &histoid, HASH_REMOVE, NULL);

    return true;
}

/*
 * lock_gchain_cache -- lock g_blocknum cache with lock mode.
 *
 * mode: lockmode
 */
void lock_gchain_cache(LWLockMode mode)
{
    LWLockAcquire(GlobalPrevHashLock, mode);
}

/*
 * release_gchain_cache -- release g_blocknum cache.
 */
void release_gchain_cache()
{
    LWLockRelease(GlobalPrevHashLock);
}

/*
 * lock_hist_hash_cache -- load hist cache.
 */
void lock_hist_hash_cache(LWLockMode mode)
{
    LWLockAcquire(BlockchainVersionLock, mode);
}

/*
 * release_hist_hash_cache -- release hist cache.
 */
void release_hist_hash_cache()
{
    LWLockRelease(BlockchainVersionLock);
}

/*
 * get_target_query_relid -- get result relation oid.
 *
 * rte_list: range table entry list.
 * resultRelation: the index of target relation in rte_list.
 */
Oid get_target_query_relid(List* rte_list, int resultRelation)
{
    Oid relid = InvalidOid;

    if (resultRelation > 0) {
        RangeTblEntry *rte = (RangeTblEntry *)list_nth(rte_list, resultRelation - 1);
        if (rte->relkind == RELKIND_RELATION) {
            relid = rte->relid;
        }
    }
    return relid;
}

/*
 * is_ledger_usertable -- whether relid is ledger user table.
 *
 * relid: usertable oid.
 *
 * Note: ledger usertable has correct hist table in blockchain schema.
 * Or rather, the schema of ledger usertable has WITH BLOCKCHAIN option.
 */
bool is_ledger_usertable(Oid relid)
{
    if (!OidIsValid(relid)) {
        return false;
    }

    Oid nspid = get_rel_namespace(relid);
    char relkind = get_rel_relkind(relid);
    /* only table has its user chain table */
    if (relkind != RELKIND_RELATION) {
        return false;
    }
    /* check table belong to blockchain schema */
    return IsLedgerNameSpace(nspid);
}

/*
 * hash_combiner -- combine all relhash list items.
 *
 * relhash_list: the list contains several relhash.
 */
uint64 hash_combiner(List *relhash_list)
{
    uint64 relhash_sum = 0;
    ListCell *lc = NULL;
    if (relhash_list == NIL) {
        return relhash_sum;
    }

    foreach (lc, relhash_list) {
        Datum *value = (Datum *)lfirst(lc);
        relhash_sum += DatumGetUInt64(value);
    }

    return relhash_sum;
}

/*
 * is_ledger_hist_table -- whether relation is ledger hist table.
 *
 * relid: relation oid.
 */
bool is_ledger_hist_table(Oid relid)
{
    if (!OidIsValid(relid) || IsInitdb) {
        return false;
    }

    Oid relnsp = get_rel_namespace(relid);
    char relkind = get_rel_relkind(relid);
    /* check namespace oid of relation to verify hist table. */
    return relnsp == PG_BLOCKCHAIN_NAMESPACE && relkind == RELKIND_RELATION;
}

/*
 * is_ledger_related_rel -- whether relation is ledger related table.
 *
 * rel: table relation.
 *
 * Note: ledger related rel means: ledger usertable or ledger hist table or gs_global_chain.
 */
bool is_ledger_related_rel(Relation rel)
{
    Oid nspoid = RelationGetNamespace(rel);
    char relkind = RelationGetRelkind(rel);
    bool is_ledger_usertable = rel->rd_isblockchain;
    bool is_ledger_histtable = (nspoid == PG_BLOCKCHAIN_NAMESPACE && relkind == RELKIND_RELATION);
    bool is_ledger_gchain = (RelationGetRelid(rel) == GsGlobalChainRelationId);
    return is_ledger_usertable || is_ledger_histtable || is_ledger_gchain;
}

/*
 * ledger_usertable_check -- check relation is ledger user table.
 */
bool ledger_usertable_check(Oid relid, Oid nspoid, const char *tablename, const char *tablensp)
{
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("table %s.%s not exists.", tablensp, tablename)));
    }
    if (!IsLedgerNameSpace(nspoid)) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("table %s.%s is not ledger user table.", tablensp, tablename)));
    }
    return true;
}

/*
 * namespace_get_depended_relid -- get relid under given schema oid.
 *
 * nspid: schema oid.
 */
List *namespace_get_depended_relid(Oid nspid)
{
    List *relid_list = NIL;
    Relation pg_class_rel = NULL;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    Oid tupid = InvalidOid;

    ScanKeyInit(&skey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspid));
    pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
    sysscan = systable_beginscan(pg_class_rel, ClassNameNspIndexId, true, SnapshotNow, 1, skey);

    while (HeapTupleIsValid(tuple = systable_getnext(sysscan))) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tuple);
        if (reltup->relkind == RELKIND_RELATION) {
            tupid = HeapTupleGetOid(tuple);
            relid_list = lappend_oid(relid_list, tupid);
        }
    }

    systable_endscan(sysscan);
    heap_close(pg_class_rel, AccessShareLock);
    return relid_list;
}

/*
 * get_ledger_msg_hash -- extract relhash from response message.
 *
 * message: response message.
 * hash: buffer to load extracted relhash.
 */
bool get_ledger_msg_hash(char *message, uint64 *hash, int *msg_len)
{
    bool is_from_dml = false;
    int  hash_offset;
    static hash_offset_pair hash_offset_map[] = {
        {"INSERT", 3}, /* INSERT 0 1 HASH */
        {"UPDATE", 2}, /* UPDATE 1 HASH */
        {"DELETE", 2}  /* DELETE 1 HASH */
    };

    if (message == NULL || strlen(message) == 0) {
        return false;
    }
    /* match space times. */
    for (size_t i = 0; i < lengthof(hash_offset_map); i++) {
        if (strncmp(message, hash_offset_map[i].name, 6) == 0) { /* 6: string length of INSERT/UPDATE/DELETE */
            hash_offset = hash_offset_map[i].hash_offset;
            is_from_dml = true;
            break;
        }
    }

    if (is_from_dml) {
        size_t pos;
        size_t len = strlen(message);
        for (pos = 0; pos < len && hash_offset > 0; ++pos) {
            if (message[pos] == ' ') {
                --hash_offset;
            }
        }
        /* message contains Hash value */
        if (hash_offset == 0) {
            size_t remain_len = len - pos;
            if (remain_len > 0) {
                *hash = strtoul(message + pos, NULL, 10); /* 10: Decimal */
                message[pos - 1] = '\0'; /* remove appended hash string. */
                *msg_len = *msg_len - remain_len - 1;
                return true;
            }
        }
    }

    return false;
}

/*
 * get_hist_name -- get hist table name of ledger user table.
 *
 * relid: user table oid.
 * rel_name: user table rel_name.
 * hist_name: buffer to load hist table name.
 * nsp_oid: schema oid of user table.
 * nsp_name: schema name of user table.
 *
 * Note: relid is used for getting namespace.
 * This function is also used for generate hist name when
 * rename user table, then rel_name is newest user table name.
 * Thus, we should not use relid to get user table name.
 */
bool get_hist_name(Oid relid, const char *rel_name, char *hist_name, Oid nsp_oid, const char *nsp_name)
{
    errno_t rc;
    if (!OidIsValid(relid) || rel_name == NULL) {
        return false;
    }
    nsp_oid = OidIsValid(nsp_oid) ? nsp_oid : get_rel_namespace(relid);
    nsp_name = (nsp_name == NULL) ? get_namespace_name(nsp_oid) : nsp_name;
    int part_hist_name_len = strlen(rel_name) + strlen(nsp_name) + 1;
    if (part_hist_name_len + strlen("_hist") >= NAMEDATALEN) {
        rc = snprintf_s(hist_name, NAMEDATALEN, NAMEDATALEN - 1, "%d_%d_hist", nsp_oid, relid);
        securec_check_ss(rc, "", "");
    } else {
        rc = snprintf_s(hist_name, NAMEDATALEN, NAMEDATALEN - 1, "%s_%s_hist", nsp_name, rel_name);
        securec_check_ss(rc, "", "");
    }
    return true;
}

/*
 * querydesc_contains_ledger_usertable -- check querydesc result relation.
 *
 * Note: check result relation of querydesc is ledger user table.
 */
bool querydesc_contains_ledger_usertable(QueryDesc *query_desc)
{
    if (query_desc == NULL || query_desc->estate == NULL) {
        return false;
    }
    EState *estate = query_desc->estate;
    int relnum = estate->es_num_result_relations;
    if (relnum == 0 || estate->es_result_relations == NULL) {
        return false;
    }
    for (int i = 0; i < relnum; ++i) {
        if (estate->es_result_relations[i].ri_RelationDesc->rd_isblockchain) {
            return true;
        }
    }
    return false;
}

/*
 * ledger_check_switch_schema -- check two schema has same blockchain option.
 */
void ledger_check_switch_schema(Oid old_nsp, Oid new_nsp)
{
    if (IsLedgerNameSpace(old_nsp) != IsLedgerNameSpace(new_nsp)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Unsupport to switch schema of a table between ledger schema and normal schema.")));
    }
}

/*
 * is_ledger_rowstore -- check withOpt of CreateStmt.
 *
 * defList: stmt->options.
 *
 * Note: we will check whether option contains orientation, we only support ORIENTATION ROW.
 */
bool is_ledger_rowstore(List *defList)
{
    ListCell *lc = NULL;
    /* Scan list to see if orientation was ROW store. */
    foreach (lc, defList) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (pg_strcasecmp(def->defname, "orientation") == 0 &&
            pg_strcasecmp(defGetString(def), ORIENTATION_ROW) != 0) {
            return false;
        }
    }
    return true;
}

bool is_ledger_hashbucketstore(List *defList)
{
    ListCell *lc = NULL;
    /* Scan list to see if hashbucket store. */
    foreach (lc, defList) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (pg_strcasecmp(def->defname, "bucketcnt") == 0 ||
            (pg_strcasecmp(def->defname, "hashbucket") == 0 &&
            defGetBoolean(def))) {
            return true;
        }
    }
    return false;
}
/*
 * check_ledger_attrs_support -- check attrs is ledger supported.
 *
 * attrs: list of columns.
 *
 * Note: because ledger tuple hash is generated by hash_any,so we
 * should check whether table column types is supported by hash calculate.
 */
void check_ledger_attrs_support(List *attrs)
{
    if (attrs == NIL) {
        return;
    }
    ListCell *lc = NULL;
    foreach (lc, attrs) {
        ColumnDef *colDef = (ColumnDef*)lfirst(lc);
        /* skip the hash column */
        if (strcmp(colDef->colname, "hash") == 0) {
            continue;
        }
        Oid typid = colDef->typname->typeOid;
        if (!OidIsValid(typid)) {
            Type ctype = typenameType(NULL, colDef->typname, NULL);
            if (ctype != NULL) {
                typid = typeTypeId(ctype);
                ReleaseSysCache(ctype);
            }
        }
        switch (typid) {
            case INT8OID:
            case INT1OID:
            case INT2OID:
            case OIDOID:
            case INT4OID:
            case BOOLOID:
            case CHAROID:
            case NAMEOID:
            case INT2VECTOROID:
            case CLOBOID:
            case NVARCHAR2OID:
            case VARCHAROID:
            case TEXTOID:
            case OIDVECTOROID:
            case FLOAT4OID:
            case FLOAT8OID:
            case ABSTIMEOID:
            case RELTIMEOID:
            case CASHOID:
            case BPCHAROID:
            case RAWOID:
            case BYTEAOID:
            case BYTEAWITHOUTORDERCOLOID:
            case BYTEAWITHOUTORDERWITHEQUALCOLOID:
            case INTERVALOID:
            case TIMEOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case DATEOID:
            case TIMETZOID:
            case SMALLDATETIMEOID:
            case NUMERICOID:
            case UUIDOID:
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("Unsupport column type \"%s\" of ledger user table.",
                                       TypeNameToString(colDef->typname))));
        }
    }
}
