/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 *
 * tcap_truncate.cpp
 *      Routines to support Timecapsule `Recyclebin-based query, restore`.
 *      We use Tr prefix to indicate it in following coding.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/tcap/tcap_truncate.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgstat.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation_fn.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion_fn.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_job.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_object.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_recyclebin.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_rlspolicy.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pgxc_class.h"
#include "catalog/storage.h"
#include "commands/comment.h"
#include "commands/dbcommands.h"
#include "commands/directory.h"
#include "commands/extension.h"
#include "commands/proclang.h"
#include "commands/schemacmds.h"
#include "commands/seclabel.h"
#include "commands/sec_rls_cmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/typecmds.h"
#include "executor/node/nodeModifyTable.h"
#include "rewrite/rewriteRemove.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/smgr/relfilenode.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "storage/tcap.h"
#include "storage/tcap_impl.h"

void TrRelationSetNewRelfilenode(Relation relation, TransactionId freezeXid, void *baseDesc)
{
    TrObjDesc desc;
    TrObjDesc *trBaseDesc = (TrObjDesc *)baseDesc;
    RelFileNodeBackend newrnode;

    /* Indexes, sequences must have Invalid frozenxid; other rels must not. */
    Assert((((relation->rd_rel->relkind == RELKIND_INDEX) || (RELKIND_IS_SEQUENCE(relation->rd_rel->relkind))) ?
        (freezeXid == InvalidTransactionId) :
        TransactionIdIsNormal(freezeXid)) ||
        relation->rd_rel->relkind == RELKIND_RELATION);

    /* Record the old relfilenode to recyclebin. */
    desc = *trBaseDesc;
    if (!TR_IS_BASE_OBJ_EX(trBaseDesc, RelationGetRelid(relation))) {
        TrDescInit(relation, &desc, RB_OPER_TRUNCATE, TrGetObjType(InvalidOid, RelationGetRelkind(relation)), false);
        TrDescWrite(&desc);
    }

    /* Allocate a new relfilenode, create storage for the main fork. */
    newrnode = CreateNewRelfilenode(relation, freezeXid);

    /*
     * NOTE: if the relation was created in this transaction, it will now be
     * present in the pending-delete list twice, once with atCommit true and
     * once with atCommit false.  Hence, it will be physically deleted at end
     * of xact in either case (and the other entry will be ignored by
     * smgrDoPendingDeletes, so no error will occur).  We could instead remove
     * the existing list entry and delete the physical file immediately, but
     * for now I'll keep the logic simple.
     */
    RelationCloseSmgr(relation);

    /*
     * Update pg_class entry for new relfilenode.
     */
    UpdatePgclass(relation, freezeXid, &newrnode);

    /*
     * Make the pg_class row change visible, as well as the relation map
     * change if any.  This will cause the relcache entry to get updated, too.
     */
    CommandCounterIncrement();

    /*
     * Mark the rel as having been given a new relfilenode in the current
     * (sub) transaction.  This is a hint that can be used to optimize later
     * operations on the rel in the same transaction.
     */
    relation->rd_newRelfilenodeSubid = GetCurrentSubTransactionId();

    /* ... and now we have eoxact cleanup work to do */
    u_sess->relcache_cxt.need_eoxact_work = true;
}

bool TrCheckRecyclebinTruncate(const TruncateStmt *stmt)
{
    RangeVar *rel = NULL;
    Oid relid;

    if (/*
         * Disable Recyclebin-based-Truncate when with purge option, or
         */
        /* recyblebin disabled, or */
        !u_sess->attr.attr_storage.enable_recyclebin ||
        /* with purge option, or */
        stmt->purge ||
        /* with restart_seqs option, or */
        stmt->restart_seqs ||
        /* multi objects truncate. */
        list_length(stmt->relations) != 1) {
        return false;
    }

    rel = (RangeVar *)linitial(stmt->relations);
    relid = RangeVarGetRelid(rel, NoLock, false);

    return NeedTrComm(relid);
}

void TrTruncate(const TruncateStmt *stmt)
{
    RangeVar *rv = (RangeVar*)linitial(stmt->relations);
    Relation rel;
    Oid relid;
    Oid toastRelid;
    TrObjDesc baseDesc;

    /*
     * 1. Open relation in AccessExclusiveLock, and check permission, etc.
     */

    rel = heap_openrv(rv, AccessExclusiveLock);
    relid = RelationGetRelid(rel);

    TrForbidAccessRbObject(RelationRelationId, relid, rv->relname);

    truncate_check_rel(rel);

    /*
     * This effectively deletes all rows in the table, and may be done
     * in a serializable transaction.  In that case we must record a
     * rw-conflict in to this transaction from each transaction
     * holding a predicate lock on the table.
     */
    CheckTableForSerializableConflictIn(rel);

    /*
     * 2. Create a new empty storage file for the relation, and assign it
     * as the relfilenode value, and record the old relfilenode to recyclebin.
     */

    TrDescInit(rel, &baseDesc, RB_OPER_TRUNCATE, RB_OBJ_TABLE, true, true);
    baseDesc.id = baseDesc.baseid = TrDescWrite(&baseDesc);
    TrUpdateBaseid(&baseDesc);

    TrRelationSetNewRelfilenode(rel, u_sess->utils_cxt.RecentXmin, &baseDesc);

    /*
     * 3. The same for the toast table, if any.
     */

    toastRelid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(toastRelid)) {
        Relation relToast = relation_open(toastRelid, AccessExclusiveLock);
        TrRelationSetNewRelfilenode(relToast, u_sess->utils_cxt.RecentXmin, &baseDesc);
        heap_close(relToast, NoLock);
    }

    /*
     * 4. Reconstruct the indexes to match, and we're done.
     */

    (void)ReindexRelation(relid, REINDEX_REL_PROCESS_TOAST, REINDEX_ALL_INDEX, &baseDesc);

    /*
     * 5. Report stat, and clean.
     */

    /* report truncate to PgStatCollector */
    pgstat_report_truncate(relid, InvalidOid, false);

    /* Record time of truancate relation. */
    recordRelationMTime(relid, rel->rd_rel->relkind);

    heap_close(rel, NoLock);
}

/*
 * RelationDropStorage
 *		Schedule unlinking of physical storage at transaction commit.
 */
void TrDoPurgeObjectTruncate(TrObjDesc *desc)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    Assert (desc->type == RB_OBJ_TABLE && desc->canpurge);

    rbRel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcybaseid, BTEqualStrategyNumber,
        F_INT8EQ, Int64GetDatum(desc->baseid));

    sd = systable_beginscan(rbRel, RecyclebinBaseidIndexId, true, NULL, 1, skey);
    while (HeapTupleIsValid(tup = systable_getnext(sd))) {
        Form_pg_recyclebin rbForm = (Form_pg_recyclebin)GETSTRUCT(tup);
        RelFileNode rnode;

        rnode.spcNode = ConvertToRelfilenodeTblspcOid(rbForm->rcytablespace);
        rnode.dbNode = (rnode.spcNode == GLOBALTABLESPACE_OID) ? InvalidOid :
                        u_sess->proc_cxt.MyDatabaseId;
        rnode.relNode = rbForm->rcyrelfilenode;
        rnode.bucketNode = InvalidBktId;

        /*
         * Schedule unlinking of the old storage at transaction commit.
         */
        InsertStorageIntoPendingList(
            &rnode, InvalidAttrNumber, InvalidBackendId, rbForm->rcyowner, true, false);

        simple_heap_delete(rbRel, &tup->t_self);

        ereport(LOG, (errmsg("Delete truncated object %u/%u/%u", rnode.spcNode,
            rnode.dbNode, rnode.relNode)));
    }

    systable_endscan(sd);
    heap_close(rbRel, RowExclusiveLock);

    /* ... and now we have eoxact cleanup work to do */
    u_sess->relcache_cxt.need_eoxact_work = true;

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();
}

/* flashback table to before truncate */
void TrRestoreTruncate(const TimeCapsuleStmt *stmt)
{
    TrObjDesc baseDesc;
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData skey[1];
    HeapTuple tup;

    /* 1. Fetch the latest available recycle object. */
    TrOperFetch(stmt->relation, RB_OBJ_TABLE, &baseDesc, RB_OPER_RESTORE_TRUNCATE);

    /* 2. Lock recycle object and base relation. */
    baseDesc.authid = GetUserId();
    TrOperPrep(&baseDesc, RB_OPER_RESTORE_TRUNCATE);

    /* 3. Check base relation whether normal and matched. */
    TrBaseRelMatched(&baseDesc);

    /* 4. Do restore. */
    rbRel = heap_open(RecyclebinRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_recyclebin_rcybaseid, BTEqualStrategyNumber,
        F_INT8EQ, Int64GetDatum(baseDesc.id));

    sd = systable_beginscan(rbRel, RecyclebinBaseidIndexId, true, NULL, 1, skey);
    while (HeapTupleIsValid(tup = systable_getnext(sd))) {
        TrSwapRelfilenode(rbRel, tup);
    }

    systable_endscan(sd);
    heap_close(rbRel, RowExclusiveLock);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next deletion step.
     */
    CommandCounterIncrement();

    return;
}
