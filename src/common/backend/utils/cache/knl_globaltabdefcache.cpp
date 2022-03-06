/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 */

#include "executor/executor.h"
#include "utils/knl_globaltabdefcache.h"
#include "commands/trigger.h"
#include "rewrite/rewriteRlsPolicy.h"
#include "utils/builtins.h"
#include "knl/knl_session.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/knl_relcache.h"
#include "utils/knl_catcache.h"
#include "utils/partitionmap.h"
#include "catalog/indexing.h"
#include "catalog/pg_publication.h"

static void RelationPointerToNULL(Relation rel)
{
    rel->rd_smgr = NULL;
    rel->rd_rel = NULL;
    rel->rd_att = NULL;
    rel->rd_rules = NULL;
    rel->rd_rulescxt = NULL;
    rel->trigdesc = NULL;
    rel->rd_rlsdesc = NULL;
    rel->rd_indexlist = NIL;
    rel->rd_indexattr = NULL;
    rel->rd_keyattr = NULL;
    rel->rd_idattr = NULL;
    rel->rd_pkattr = NULL;
    rel->rd_pubactions = NULL;
    rel->rd_options = NULL;
    rel->rd_index = NULL;
    rel->rd_indextuple = NULL;
    rel->rd_am = NULL;
    rel->rd_indexcxt = NULL;
    rel->rd_aminfo = NULL;
    rel->rd_opfamily = NULL;
    rel->rd_opcintype = NULL;
    rel->rd_support = NULL;
    rel->rd_supportinfo = NULL;
    rel->rd_indoption = NULL;
    rel->rd_indexprs = NULL;
    rel->rd_indpred = NULL;
    rel->rd_exclops = NULL;
    rel->rd_exclprocs = NULL;
    rel->rd_exclstrats = NULL;
    rel->rd_amcache = NULL;
    rel->rd_indcollation = NULL;
    rel->rd_fdwroutine = NULL;
    rel->rd_bucketkey = NULL;
    rel->partMap = NULL;
    rel->pgstat_info = NULL;
    rel->rd_locator_info = NULL;
    rel->sliceMap = NULL;
    rel->parent = NULL;
    /* double linked list node, partition and bucket relation would be stored in fakerels list of resource owner */
    rel->node.prev = NULL;
    rel->node.next = NULL;
}

static void *CopyPubActions(void *pubActions)
{
    if (pubActions == NULL) {
        return NULL;
    }
    PublicationActions *res = (PublicationActions*)palloc(sizeof(PublicationActions));
    errno_t rc = memcpy_s(res, sizeof(PublicationActions), pubActions, sizeof(PublicationActions));
    securec_check(rc, "", "");
    return res;
}

static Form_pg_class CopyRelationRdrel(Relation rel)
{
    Assert(rel->rd_rel != NULL);
    /* Copy the relation tuple form
     *
     * We only allocate space for the fixed fields, ie, CLASS_TUPLE_SIZE. The
     * variable-length fields (relacl, reloptions) are NOT stored in the
     * relcache --- there'd be little point in it, since we don't copy the
     * tuple's nulls bitmap and hence wouldn't know if the values are valid.
     * Bottom line is that relacl *cannot* be retrieved from the relcache. Get
     * it from the syscache if you need it.  The same goes for the original
     * form of reloptions (however, we do store the parsed form of reloptions
     * in rd_options).
     */
    Form_pg_class rd_rel = (Form_pg_class)palloc(sizeof(FormData_pg_class));
    errno_t rc = memcpy_s(rd_rel, sizeof(FormData_pg_class), rel->rd_rel, CLASS_TUPLE_SIZE);
    securec_check(rc, "", "");
    return rd_rel;
}

TupleDesc CopyTupleDesc(TupleDesc tupdesc)
{
    TupleDesc rd_att = CreateTupleDescCopyConstr(tupdesc);
    if (tupdesc->constr == NULL) {
        return rd_att;
    }
    /* TupleConstrCopy dont copy clusterKeys info, so we copy it manually */
    TupleConstr *dst = rd_att->constr;
    TupleConstr *src = tupdesc->constr;
    Assert(src != NULL);
    dst->clusterKeyNum = src->clusterKeyNum;
    Assert(dst->clusterKeys == NULL);
    if (dst->clusterKeyNum == 0) {
        return rd_att;
    }
    size_t len = sizeof(AttrNumber) * dst->clusterKeyNum;
    dst->clusterKeys = (AttrNumber *)palloc(len);
    errno_t rc = memcpy_s(dst->clusterKeys, len, src->clusterKeys, len);
    securec_check(rc, "", "");
    return rd_att;
}

static RuleLock *CopyRelationRules(Relation rel, MemoryContext rules_cxt)
{
    if (rel->rd_rules == NULL) {
        return NULL;
    }
    MemoryContext old = MemoryContextSwitchTo(rules_cxt);
    RuleLock *rd_rules = (RuleLock *)palloc(sizeof(RuleLock));
    rd_rules->numLocks = rel->rd_rules->numLocks;
    RewriteRule **rules = (RewriteRule **)palloc(sizeof(RewriteRule *) * rel->rd_rules->numLocks);
    rd_rules->rules = rules;
    for (int i = 0; i < rel->rd_rules->numLocks; i++) {
        rules[i] = (RewriteRule *)palloc(sizeof(RewriteRule));
        *rules[i] = *rel->rd_rules->rules[i];
        rules[i]->actions = (List *)copyObject(rel->rd_rules->rules[i]->actions);
        rules[i]->qual = (Node *)copyObject(rel->rd_rules->rules[i]->qual);
    }
    MemoryContextSwitchTo(old);
    return rd_rules;
}

static RlsPoliciesDesc *CopyRelationRls(Relation rel, MemoryContext rls_cxt)
{
    if (rel->rd_rlsdesc == NULL) {
        return NULL;
    }
    MemoryContext old = MemoryContextSwitchTo(rls_cxt);
    RlsPoliciesDesc *rd_rlsdesc = (RlsPoliciesDesc *)palloc(sizeof(RlsPoliciesDesc));
    rd_rlsdesc->rlsCxt = rls_cxt;
    rd_rlsdesc->rlsPolicies = NULL;
    ListCell *lc;
    foreach (lc, rel->rd_rlsdesc->rlsPolicies) {
        RlsPolicy *dst_rls = (RlsPolicy *)palloc(sizeof(RlsPolicy));
        RlsPolicy *src_rls = (RlsPolicy *)lfirst(lc);
        *dst_rls = *src_rls;
        dst_rls->policyName = pstrdup(src_rls->policyName);
        dst_rls->roles = DatumGetArrayTypePCopy(src_rls->roles);
        dst_rls->usingExpr = (Expr *)copyObject(src_rls->usingExpr);
        rd_rlsdesc->rlsPolicies = lappend(rd_rlsdesc->rlsPolicies, dst_rls);
    }
    MemoryContextSwitchTo(old);
    return rd_rlsdesc;
}

extern bytea *CopyOption(bytea *options)
{
    if (options == NULL) {
        return NULL;
    }
    bytea *copy = (bytea *)palloc(VARSIZE(options));
    errno_t rc = memcpy_s(copy, VARSIZE(options), options, VARSIZE(options));
    /* this func called also by partcopy, which has no memcxt protect, so pfree when memcpy failed */
    securec_check(rc, (char *)copy, "");
    return copy;
}

static Form_pg_am CopyRelationAm(Relation rel)
{
    if (rel->rd_am == NULL) {
        return NULL;
    }
    Form_pg_am rd_am = (Form_pg_am)palloc(sizeof(FormData_pg_am));
    *rd_am = *rel->rd_am;
    return rd_am;
}

static void CopyRelationIndexAccessInfo(Relation newrel, Relation rel, MemoryContext index_cxt)
{
    if (!RelationIsIndex(rel)) {
        Assert(rel->rd_indnkeyatts == 0);
        Assert(rel->rd_indexcxt == NULL);
        return;
    }

    newrel->rd_indexcxt = index_cxt;
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    int nsupport = rel->rd_am->amsupport * RelationGetNumberOfAttributes(rel);
    newrel->rd_aminfo = NULL;

    newrel->rd_opfamily = (Oid*)MemoryContextAllocZero(index_cxt, indnkeyatts * sizeof(Oid));
    newrel->rd_opcintype = (Oid*)MemoryContextAllocZero(index_cxt, indnkeyatts * sizeof(Oid));

    if (rel->rd_am->amsupport > 0) {
        newrel->rd_support = (RegProcedure*)MemoryContextAllocZero(index_cxt, nsupport * sizeof(RegProcedure));
        newrel->rd_supportinfo = (FmgrInfo*)MemoryContextAllocZero(index_cxt, nsupport * sizeof(FmgrInfo));
    } else {
        newrel->rd_support = NULL;
        newrel->rd_supportinfo = NULL;
    }

    newrel->rd_indcollation = (Oid*)MemoryContextAllocZero(index_cxt, indnkeyatts * sizeof(Oid));

    newrel->rd_indoption = (int16*)MemoryContextAllocZero(index_cxt, indnkeyatts * sizeof(int16));

    errno_t rc = memcpy_s(newrel->rd_opfamily, indnkeyatts * sizeof(Oid), rel->rd_opfamily, indnkeyatts * sizeof(Oid));
    securec_check(rc, "", "");
    rc = memcpy_s(newrel->rd_opcintype, indnkeyatts * sizeof(Oid), rel->rd_opcintype, indnkeyatts * sizeof(Oid));
    securec_check(rc, "", "");
    if (nsupport > 0) {
        rc = memcpy_s(newrel->rd_support, nsupport * sizeof(RegProcedure),
            rel->rd_support, nsupport * sizeof(RegProcedure));
        securec_check(rc, "", "");
        rc = memcpy_s(newrel->rd_supportinfo, nsupport * sizeof(FmgrInfo),
            rel->rd_supportinfo, nsupport * sizeof(FmgrInfo));
        securec_check(rc, "", "");
    }
    rc = memcpy_s(newrel->rd_indcollation, indnkeyatts * sizeof(Oid), rel->rd_indcollation, indnkeyatts * sizeof(Oid));
    securec_check(rc, "", "");
    rc = memcpy_s(newrel->rd_indoption, indnkeyatts * sizeof(int16), rel->rd_indoption, indnkeyatts * sizeof(int16));
    securec_check(rc, "", "");
    MemoryContext oldcxt = MemoryContextSwitchTo(index_cxt);
    if (rel->rd_indexprs) {
        newrel->rd_indexprs = (List *)copyObject(rel->rd_indexprs);
    }
    if (rel->rd_indpred) {
        newrel->rd_indpred = (List *)copyObject(rel->rd_indpred);
    }
    (void)MemoryContextSwitchTo(oldcxt);
    if (rel->rd_exclops == NULL) {
        Assert(rel->rd_exclprocs == NULL);
        Assert(rel->rd_exclstrats == NULL);
        newrel->rd_exclops = NULL;
        newrel->rd_exclprocs = NULL;
        newrel->rd_exclstrats = NULL;
    } else {
        Assert(rel->rd_exclprocs != NULL);
        Assert(rel->rd_exclstrats != NULL);
        newrel->rd_exclops = (Oid*)MemoryContextAlloc(index_cxt, sizeof(Oid) * indnkeyatts);
        newrel->rd_exclprocs = (Oid*)MemoryContextAlloc(index_cxt, sizeof(Oid) * indnkeyatts);
        newrel->rd_exclstrats = (uint16*)MemoryContextAlloc(index_cxt, sizeof(uint16) * indnkeyatts);

        rc = memcpy_s(newrel->rd_exclops, sizeof(Oid) * indnkeyatts, rel->rd_exclops, sizeof(Oid) * indnkeyatts);
        securec_check(rc, "", "");
        rc = memcpy_s(newrel->rd_exclprocs, sizeof(Oid) * indnkeyatts, rel->rd_exclprocs, sizeof(Oid) * indnkeyatts);
        securec_check(rc, "", "");
        rc = memcpy_s(newrel->rd_exclstrats, sizeof(uint16) * indnkeyatts,
            rel->rd_exclstrats, sizeof(uint16) * indnkeyatts);
        securec_check(rc, "", "");
    }

    /* not inited by relcache */
    newrel->rd_amcache = NULL;
}

static RelationBucketKey *CopyRelationBucketKey(Relation rel)
{
    if (rel->rd_bucketkey == NULL) {
        return NULL;
    }
    RelationBucketKey *rd_bucketkey = (RelationBucketKey *)palloc(sizeof(RelationBucketKey));
    rd_bucketkey->bucketKey = int2vectorCopy(rel->rd_bucketkey->bucketKey);
    int n_column = rel->rd_bucketkey->bucketKey->dim1;
    int n_col_len = sizeof(Oid) * n_column;
    rd_bucketkey->bucketKeyType = (Oid *)palloc(n_col_len);
    errno_t rc = memcpy_s(rd_bucketkey->bucketKeyType, n_col_len, rel->rd_bucketkey->bucketKeyType, n_col_len);
    securec_check(rc, "", "");
    return rd_bucketkey;
}

static PartitionMap *CopyRangePartitionMap(RangePartitionMap *src_rpm)
{
    RangePartitionMap *dst_rpm = (RangePartitionMap *)palloc(sizeof(RangePartitionMap));

    *dst_rpm = *src_rpm;

    dst_rpm->partitionKey = int2vectorCopy(src_rpm->partitionKey);

    size_t key_len = sizeof(Oid) * src_rpm->partitionKey->dim1;
    dst_rpm->partitionKeyDataType = (Oid *)palloc(key_len);
    errno_t rc = memcpy_s(dst_rpm->partitionKeyDataType, key_len, src_rpm->partitionKeyDataType, key_len);
    securec_check(rc, "", "");

    dst_rpm->rangeElements =
        copyRangeElements(src_rpm->rangeElements, src_rpm->rangeElementsNum, src_rpm->partitionKey->dim1);

    if (src_rpm->type.type == PART_TYPE_INTERVAL) {
        Assert(src_rpm->partitionKey->dim1 == 1);
        dst_rpm->intervalValue = (Interval *)palloc(sizeof(Interval));
        *dst_rpm->intervalValue = *src_rpm->intervalValue;
#define OidVectorSize(n) (offsetof(oidvector, values) + (n) * sizeof(Oid))
        if (src_rpm->intervalTablespace != NULL) {
            size_t interval_len = OidVectorSize(src_rpm->intervalTablespace->dim1);
            dst_rpm->intervalTablespace = (oidvector *)palloc(interval_len);
            rc = memcpy_s(dst_rpm->intervalTablespace, interval_len, src_rpm->intervalTablespace, interval_len);
            securec_check(rc, "", "");
        } else {
            Assert(dst_rpm->intervalTablespace == NULL);
        }
    }
    return (PartitionMap *)dst_rpm;
}

static PartitionMap *CopyPartitionMap(Relation rel)
{
    if (rel->partMap == NULL) {
        return NULL;
    }
    switch (rel->partMap->type) {
        case PART_TYPE_VALUE: {
            ValuePartitionMap *dst_vpm = (ValuePartitionMap *)palloc0(sizeof(ValuePartitionMap));
            ValuePartitionMap *src_vpm = (ValuePartitionMap *)rel->partMap;
            dst_vpm->type = src_vpm->type;
            dst_vpm->relid = src_vpm->relid;
            dst_vpm->partList = list_copy(src_vpm->partList);
            return (PartitionMap *)dst_vpm;
        }
        case PART_TYPE_RANGE:
        case PART_TYPE_INTERVAL: {
            return (PartitionMap *)CopyRangePartitionMap((RangePartitionMap *)rel->partMap);
        }
        case PART_TYPE_LIST: {
            ListPartitionMap *dst_lpm = (ListPartitionMap *)palloc(sizeof(ListPartitionMap));
            ListPartitionMap *src_lpm = (ListPartitionMap *)rel->partMap;
            *dst_lpm = *src_lpm;
            dst_lpm->partitionKey = int2vectorCopy(src_lpm->partitionKey);
            size_t key_len = sizeof(Oid) * src_lpm->partitionKey->dim1;
            dst_lpm->partitionKeyDataType = (Oid *)palloc(key_len);
            errno_t rc = memcpy_s(dst_lpm->partitionKeyDataType, key_len, src_lpm->partitionKeyDataType, key_len);
            securec_check(rc, "", "");
            dst_lpm->listElements = CopyListElements(src_lpm->listElements, src_lpm->listElementsNum);
            return (PartitionMap *)dst_lpm;
        }
        case PART_TYPE_HASH: {
            HashPartitionMap *dst_hpm = (HashPartitionMap *)palloc(sizeof(HashPartitionMap));
            HashPartitionMap *src_hpm = (HashPartitionMap *)rel->partMap;
            *dst_hpm = *src_hpm;
            dst_hpm->partitionKey = int2vectorCopy(src_hpm->partitionKey);
            size_t key_len = sizeof(Oid) * src_hpm->partitionKey->dim1;
            dst_hpm->partitionKeyDataType = (Oid *)palloc(key_len);
            errno_t rc = memcpy_s(dst_hpm->partitionKeyDataType, key_len, src_hpm->partitionKeyDataType, key_len);
            securec_check(rc, "", "");
            dst_hpm->hashElements =
                CopyHashElements(src_hpm->hashElements, src_hpm->hashElementsNum, src_hpm->partitionKey->dim1);
            return (PartitionMap *)dst_hpm;
        }
        default:
            ereport(ERROR,
                    (errcode(ERRCODE_PARTITION_ERROR),
                     errmsg("Fail to copy partitionmap for partitioned table \"%u\".", RelationGetRelid(rel)),
                     errdetail("Incorrect partition strategy \"%c\" for partitioned table.", rel->partMap->type)));
            return NULL;
    }
    return NULL;
}

static RelationLocInfo *CopyRelationLocInfoWithOutBucketPtr(RelationLocInfo *srcInfo)
{
    /* locator info is used only for IS_PGXC_COORDINATOR */
    if (srcInfo == NULL || !IS_PGXC_COORDINATOR) {
        return NULL;
    }
    RelationLocInfo *dst_info = CopyRelationLocInfo(srcInfo);
    dst_info->buckets_ptr = NULL;
    return dst_info;
}

static PartitionMap *CopyRelationSliceMap(Relation rel)
{
    if (rel->sliceMap == NULL) {
        return NULL;
    }
    return CopyRangePartitionMap((RangePartitionMap *)rel->sliceMap);
}

static void SpecialWorkForGlobalRel(Relation rel)
{
    /* see define in rel.h */
    rel->rd_createSubid = InvalidSubTransactionId;
    rel->rd_newRelfilenodeSubid = InvalidSubTransactionId;
    Assert(rel->rd_isnailed ? rel->rd_refcnt == 1 : rel->rd_refcnt == 0);
    /* see RelationInitLockInfo in lmgr.cpp */
    Assert(rel->rd_lockInfo.lockRelId.bktId == InvalidOid);
    /* global cache never open file */
    Assert(rel->rd_smgr == NULL);
    /* refcnt must be one if isnailed or zero */
    Assert(rel->rd_isnailed ? rel->rd_refcnt == 1 : rel->rd_refcnt == 0);

    Assert(!g_instance.global_sysdbcache.IsCritialForInitSysCache(rel->rd_id) ||
        (rel->rd_isnailed && rel->rd_refcnt == 1));

    /* 0 invalid 1 yes  2 transaction tmp */
    Assert(rel->rd_indexvalid <= 1);
    Assert((rel->rd_node.dbNode == InvalidOid && rel->rd_node.spcNode == GLOBALTABLESPACE_OID) ||
           (rel->rd_node.dbNode != InvalidOid && rel->rd_node.spcNode != GLOBALTABLESPACE_OID));
    Assert(rel->rd_node.spcNode != InvalidOid);
    Assert(rel->rd_node.relNode != InvalidOid);
    if (rel->rd_locator_info != NULL) {
        Assert(IS_PGXC_COORDINATOR && rel->rd_id >= FirstNormalObjectId);
        List *old_node_list = rel->rd_locator_info->nodeList;
        ListCell *lc;
        foreach (lc, old_node_list) {
            lfirst_int(lc) = PGXCNodeGetNodeOid(lfirst_int(lc), PGXC_NODE_DATANODE);
        }
    }
}

Relation CopyRelationData(Relation newrel, Relation rel, MemoryContext rules_cxt, MemoryContext rls_cxt,
                          MemoryContext index_cxt)
{
    /* if you add variable to relation, please check if you need put it in gsc,
     * if not, set it zero when copy, and reinit it when local get the copy result
     * otherwise, do the copy work here
     * if the variable changed, there is no lock and no rel inval msg,
     * set it zero and reinit it when copy into local */
    Assert(sizeof(RelationData) == 520);
    /* all copied exclude pointer */
    *newrel = *rel;
    Assert(rel->rd_createSubid == InvalidSubTransactionId);
    Assert(rel->rd_newRelfilenodeSubid == InvalidSubTransactionId);
    /* init all pointers to NULL, so we can free memory correctly when meeting exception */
    RelationPointerToNULL(newrel);

    newrel->rd_rel = CopyRelationRdrel(rel);
    newrel->rd_att = CopyTupleDesc(rel->rd_att);
    Assert(rel->rd_att->tdrefcount == 1);
    newrel->rd_att->tdrefcount = 1;
    Assert(rel->rd_att->tdhasoid == rel->rd_rel->relhasoids);

    newrel->rd_rulescxt = rules_cxt;
    newrel->rd_rules = CopyRelationRules(rel, rules_cxt);
    /* CopyTriggerDesc check null pointer */
    newrel->trigdesc = CopyTriggerDesc(rel->trigdesc);
    newrel->rd_rlsdesc = CopyRelationRls(rel, rls_cxt);

    /* this is oid list, just copy list */
    newrel->rd_indexlist = list_copy(rel->rd_indexlist);
    /* bms_copy check null pointer */
    newrel->rd_indexattr = bms_copy(rel->rd_indexattr);
    newrel->rd_keyattr = bms_copy(rel->rd_keyattr);
    newrel->rd_idattr = bms_copy(rel->rd_idattr);
    newrel->rd_pkattr = bms_copy(rel->rd_pkattr);

    newrel->rd_pubactions = CopyPubActions(rel->rd_pubactions);

    newrel->rd_options = CopyOption(rel->rd_options);

    newrel->rd_indextuple = heap_copytuple(rel->rd_indextuple);
    if (newrel->rd_indextuple != NULL) {
        newrel->rd_index = (Form_pg_index)GETSTRUCT(newrel->rd_indextuple);
    } else {
        newrel->rd_index = NULL;
    }

    newrel->rd_am = CopyRelationAm(rel);

    CopyRelationIndexAccessInfo(newrel, rel, index_cxt);

    /* not inited by relcache */
    newrel->rd_fdwroutine = NULL;

    newrel->rd_bucketkey = CopyRelationBucketKey(rel);

    newrel->partMap = (PartitionMap *)CopyPartitionMap(rel);

    newrel->pgstat_info = NULL;

    newrel->rd_locator_info = CopyRelationLocInfoWithOutBucketPtr(rel->rd_locator_info);

    Assert(rel->sliceMap == NULL || IsLocatorDistributedBySlice(rel->rd_locator_info->locatorType));
    newrel->sliceMap = CopyRelationSliceMap(rel);

    newrel->entry = NULL;
    return newrel;
}

void GlobalTabDefCache::Insert(Relation rel, uint32 hash_value)
{
    Index hash_index = HASH_INDEX(hash_value, (uint32)m_nbuckets);
    /* dllist is too long, swapout some */
    if (m_bucket_list.GetBucket(hash_index)->dll_len >= MAX_GSC_LIST_LENGTH) {
        GlobalBaseDefCache::RemoveTailElements<true>(hash_index);
        /* maybe no element can be swappedout */
        return;
    }
    Assert((m_is_shared && g_instance.global_sysdbcache.HashSearchSharedRelation(rel->rd_id)) ||
           ((!m_is_shared && !g_instance.global_sysdbcache.HashSearchSharedRelation(rel->rd_id))));

    Assert(rel->rd_bucketkey != (RelationBucketKey *)&(rel->rd_bucketkey));

    GlobalRelationEntry *entry = CreateEntry(rel);
    PthreadRWlockWrlock(LOCAL_SYSDB_RESOWNER, &m_obj_locks[hash_index]);
    bool found = GlobalBaseDefCache::EntryExist(rel->rd_id, hash_index);
    if (found) {
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_obj_locks[hash_index]);
        entry->Free<true>(entry);
        return;
    }

    GlobalBaseDefCache::AddHeadToBucket<true>(hash_index, entry);
    PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, &m_obj_locks[hash_index]);
    pg_atomic_fetch_add_u64(m_newloads, 1);
}

/* write lock need */
GlobalRelationEntry *GlobalTabDefCache::CreateEntry(Relation rel)
{
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    ResourceOwnerEnlargeGlobalBaseEntry(owner);
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    GlobalRelationEntry *entry = (GlobalRelationEntry *)palloc(sizeof(GlobalRelationEntry));
    entry->type = GLOBAL_RELATION_ENTRY;
    entry->rel_mem_manager = NULL;
    entry->oid = rel->rd_id;
    entry->refcount = 0;
    DLInitElem(&entry->cache_elem, (void *)entry);
    ResourceOwnerRememberGlobalBaseEntry(owner, (GlobalBaseEntry *)entry);
    entry->rel_mem_manager =
        AllocSetContextCreate(CurrentMemoryContext, RelationGetRelationName(rel), ALLOCSET_SMALL_MINSIZE,
                              ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE, SHARED_CONTEXT);
    (void)MemoryContextSwitchTo(entry->rel_mem_manager);
    entry->rel = (Relation)palloc0(sizeof(RelationData));
    CopyRelationData(entry->rel, rel, entry->rel_mem_manager, entry->rel_mem_manager, entry->rel_mem_manager);
    SpecialWorkForGlobalRel(entry->rel);
    ResourceOwnerForgetGlobalBaseEntry(owner, (GlobalBaseEntry *)entry);
    MemoryContextSwitchTo(old);

    return entry;
}

void GlobalTabDefCache::Init()
{
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    GlobalBaseDefCache::Init(GLOBAL_INIT_RELCACHE_SIZE);
    m_catalog_lock = (pthread_mutex_t *)palloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(m_catalog_lock, NULL);
    MemoryContextSwitchTo(old);
    m_is_inited = true;
}

TupleDesc GlobalTabDefCache::GetPgClassDescriptor()
{
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    PthreadMutexLock(owner, m_catalog_lock, true);
    if (m_pgclassdesc != NULL) {
        PthreadMutexUnlock(owner, m_catalog_lock, true);
        return m_pgclassdesc;
    }
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    TupleDesc tmp = BuildHardcodedDescriptor(Natts_pg_class, Desc_pg_class, true);
    PthreadMutexUnlock(owner, m_catalog_lock, true);
    m_pgclassdesc = tmp;
    MemoryContextSwitchTo(old);
    return m_pgclassdesc;
}

TupleDesc GlobalTabDefCache::GetPgIndexDescriptor()
{
    ResourceOwner owner = LOCAL_SYSDB_RESOWNER;
    PthreadMutexLock(owner, m_catalog_lock, true);
    if (m_pgindexdesc != NULL) {
        PthreadMutexUnlock(owner, m_catalog_lock, true);
        return m_pgindexdesc;
    }
    MemoryContext old = MemoryContextSwitchTo(m_db_entry->GetRandomMemCxt());
    TupleDesc tmp = BuildHardcodedDescriptor(Natts_pg_index, Desc_pg_index, false);
    PthreadMutexUnlock(owner, m_catalog_lock, true);
    m_pgindexdesc = tmp;
    MemoryContextSwitchTo(old);
    return m_pgindexdesc;
}

GlobalTabDefCache::GlobalTabDefCache(Oid db_id, bool is_shared, struct GlobalSysDBCacheEntry *entry)
    : GlobalBaseDefCache(db_id, is_shared, entry, RELKIND_RELATION)
{
    m_pgclassdesc = NULL;
    m_pgindexdesc = NULL;
    m_catalog_lock = NULL;
    m_is_inited = false;
}

List *GlobalTabDefCache::GetTableStats(Oid rel_oid)
{
    List *table_stat_list = NIL;
    if (!m_is_inited) {
        return table_stat_list;
    }

    /* Remove each tuple in this cache */
    for (int hash_index = 0; hash_index < m_nbuckets; hash_index++) {
        if (m_bucket_list.GetBucket(hash_index)->dll_len == 0) {
            continue;
        }
        pthread_rwlock_t *obj_lock = &m_obj_locks[hash_index];
        PthreadRWlockRdlock(LOCAL_SYSDB_RESOWNER, obj_lock);
        for (Dlelem *elt = DLGetHead(m_bucket_list.GetBucket(hash_index)); elt;) {
            GlobalRelationEntry *entry = (GlobalRelationEntry *)DLE_VAL(elt);
            elt = DLGetSucc(elt);
            if (rel_oid != ALL_REL_OID && entry->rel->rd_id != rel_oid) {
                continue;
            }
            GlobalCatalogTableStat *table_stat = (GlobalCatalogTableStat*)palloc(sizeof(GlobalCatalogTableStat));
            table_stat->db_id = m_db_oid;
            table_stat->db_name = CStringGetTextDatum(m_db_entry->m_dbName);
            table_stat->rel_id = entry->rel->rd_id;
            table_stat->rd_rel = CopyRelationRdrel(entry->rel);
            table_stat->rd_att = CopyTupleDesc(entry->rel->rd_att);
            table_stat_list = lappend(table_stat_list, table_stat);
        }
        PthreadRWlockUnlock(LOCAL_SYSDB_RESOWNER, obj_lock);
    }
    return table_stat_list;
}

template void GlobalTabDefCache::ResetRelCaches<false>();
template void GlobalTabDefCache::ResetRelCaches<true>();