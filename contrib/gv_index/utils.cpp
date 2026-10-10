/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 *
 * utils.h
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        contrib/gv_index/utils.cpp
 *
 * --------------------------------------------------------------------------------------
 */

#include "utils.h"
#include "postgres.h"
#include "fmgr.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "nodes/nodes.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/bytea.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "catalog/pg_class.h"
#include "access/htup.h"
#include "access/reloptions.h"
#include "catalog/pg_opfamily.h"
#include "graph_options.h"
#include "utils/snapmgr.h"
#include "access/transam.h"
#include "storage/procarray.h"
#include "access/xact.h"
#include "pg_init.h"
#include "storage/smgr/smgr.h"
#include "access/generic_xlog.h"

static void ValidatePruneAlg(const char *value)
{
    if (pg_strcasecmp(value, "vamana") != 0 && pg_strcasecmp(value, "npu_knn") != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid prune_alg value \"%s\"", value),
                        errhint("Valid values are \"vamana\" and \"npu_knn\".")));
    }
}

static void ValidateStorageTypeOption(const char *value)
{
    if (pg_strcasecmp(value, "auto") != 0 && pg_strcasecmp(value, "astore") != 0 &&
        pg_strcasecmp(value, "ustore") != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid storage_type value \"%s\"", value),
                        errhint("Valid values are \"auto\", \"astore\", and \"ustore\".")));
    }
}

/*
 * 使用 GRAPH_RELOPTIONS X-macro 生成 add_*_reloption 调用。
 */
#define GRAPH_REG_STRING(name, desc, def, minv, maxv, extra, field) \
    add_string_reloption(kind, name, desc, def, (validate_string_relopt)(extra));
#define GRAPH_REG_INT(name, desc, def, minv, maxv, extra, field) \
    add_int_reloption(kind, name, desc, def, minv, maxv);
#define GRAPH_REG_BOOL(name, desc, def, minv, maxv, extra, field) \
    add_bool_reloption(kind, name, desc, def);

#define GRAPH_OPT(n,d,t,def,minv,maxv,extra,field) GRAPH_REG_##t(n,d,def,minv,maxv,extra,field)

void register_graph_reloptions(relopt_kind kind)
{
    GRAPH_RELOPTIONS(GRAPH_OPT);
}
#undef GRAPH_OPT
#undef GRAPH_REG_STRING
#undef GRAPH_REG_INT
#undef GRAPH_REG_BOOL

/*
 * 注册 graph_index特有的 WITH 选项到全局 relopt 表
 */
void gv_graph_index_init(void)
{
    if (gv_graph_kind_id != 0) {
        register_graph_reloptions(gv_graph_kind_id);
    }
}

/*
 * 将 graph 索引的所有页刷入 WAL 日志并提交。
 */
void gv_graph_xlog_write_page(Relation rel, BlockNumber nblocks, RmgrId rmid, uint8 info)
{
    ereport(NOTICE, (errmsg("gv_graph_xlog_write_page: index \"%s\" nblocks %u",
        RelationGetRelationName(rel), nblocks)));
    
    if (!RelationNeedsWAL(rel)) {
        return;
    }

    XLogRecPtr recptr = InvalidXLogRecPtr;
    for (BlockNumber i = 0; i < nblocks; i++) {
        Buffer buff = ReadBuffer(rel, i);
        LockBuffer(buff, BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buff);

        START_CRIT_SECTION();
        MarkBufferDirty(buff);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buff, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
        recptr = XLogInsert(rmid, info, RelationGetBktid(rel));
        PageSetLSN(page, recptr);
        END_CRIT_SECTION();

        UnlockReleaseBuffer(buff);
    }

    if (nblocks > 0) {
        ereport(NOTICE, (errmsg("gv_graph_xlog_write_page: index \"%s\" last lsn %lu",
            RelationGetRelationName(rel), (unsigned long)recptr)));
    }
}

/*
 * 从 pg_class syscache 读取索引的原始 reloptions datum。
 */
Datum get_index_reloptions_datum(Oid indexId)
{
    Datum reloptions = (Datum)0;

    HeapTuple tuple = SearchSysCache1(RELOID, indexId);
    if (HeapTupleIsValid(tuple)) {
        bool isnull;
        Datum cachedReloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);
        if (!isnull) {
            reloptions = datumCopy(cachedReloptions, false, -1);
        }
        ReleaseSysCache(tuple);
    }
    return reloptions;
}

/*
 * 从索引的 opfamily 名称推断距离类型（L2 或 Cosine）。
 */
annlite::VectorDistanceType get_distance_type_from_index(Relation index)
{
    Oid opfamily_oid = index->rd_opfamily[0];
    Relation pg_opfamily_rel;
    TableScanDesc scan;
    HeapTuple tuple;
    annlite::VectorDistanceType dist_type = annlite::L2_DIST_FUNC;

    if (opfamily_oid == InvalidOid) {
        FAST_NOTICE("rd_opfamily[0] is InvalidOid, defaulting to L2_DIST_FUNC");
        return dist_type;
    }

    /* 顺序扫描 pg_opfamily, 按 OID 匹配 */
    pg_opfamily_rel = heap_open(OperatorFamilyRelationId, AccessShareLock);
    scan = heap_beginscan(pg_opfamily_rel, SnapshotSelf, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        if (HeapTupleGetOid(tuple) == opfamily_oid) {
            Form_pg_opfamily opf = (Form_pg_opfamily) GETSTRUCT(tuple);
            const char *opfname = NameStr(opf->opfname);
            if (opfname != NULL && strstr(opfname, "cosine") != NULL) {
                dist_type = annlite::COSINE_DIST_FUNC;
            } else {
                dist_type = annlite::L2_DIST_FUNC;
            }
            FAST_NOTICE("opfamily OID %u name='%s' -> distance_type=%d (%s)",
                opfamily_oid, opfname, (int)dist_type,
                annlite::VectorDistanceTypeNameParser::name(dist_type));
            break;
        }
    }
    if (!HeapTupleIsValid(tuple)) {
        FAST_NOTICE("could not find opfamily OID %u in pg_opfamily, defaulting to L2_DIST_FUNC", opfamily_oid);
    }
    heap_endscan(scan);
    heap_close(pg_opfamily_rel, AccessShareLock);

    return dist_type;
}

bool gv_graph_tuple_equal(IndexTuple itup1, IndexTuple itup2)
{
    if (itup1 == NULL || itup2 == NULL) {
        return false;
    }
    Size len1 = IndexTupleSize(itup1);
    Size len2 = IndexTupleSize(itup2);
    if (len1 == 0 || len2 == 0 || len1 != len2) {
        return false;
    }
    return memcmp(itup1, itup2, len1) == 0;
}

TransactionIdStatus gv_graph_check_xid(TransactionId xid)
{
    TransactionIdStatus ts = TransactionIdGetStatus(xid);
    /* Please refer to HeapTupleSatisfiesVaccum */
    if (ts == XID_INPROGRESS) {
        if (TransactionIdIsInProgress(xid)) {
            /* Inprogress */
        } else if (TransactionIdDidCommit(xid)) {
            ts = XID_COMMITTED;
        } else {
            ts = XID_ABORTED;
        }
    }
    return ts;
}

bool vector_get_tuple_xmin_xmax_simple(const IndexTupleDataWithTransInfo& transInfo, TransactionId oldest_xmin,
        TransactionId *xmin, TransactionId *xmax, bool *xmin_committed, bool *xmax_committed, int2 bucketid)
{
    bool is_dead = false;
    bool need_check_xmin = true;

    *xmin_committed = *xmax_committed = false;

    *xmin = transInfo.trans_info.xmin;
    *xmax = transInfo.trans_info.xmax;

    /* examine xmax */
    if (TransactionIdIsValid(*xmax)) {
        TransactionIdStatus ts = gv_graph_check_xid(*xmax);
        switch (ts) {
            case XID_INPROGRESS:
                if (TransactionIdEquals(*xmin, *xmax)) {
                    need_check_xmin = false;
                }
                break;
            case XID_COMMITTED:
                *xmin_committed = *xmax_committed = true;
                need_check_xmin = false;
                break;
            case XID_ABORTED:
                *xmax = InvalidTransactionId;
                if (TransactionIdEquals(*xmin, *xmax)) {
                    /* xmin xmax aborted */
                    *xmin = InvalidTransactionId;
                    need_check_xmin = false;
                }
                break;
        }
    }

    /* if there is no passed oldest_xmin, use the current oldest xmin */
    if (!TransactionIdIsValid(oldest_xmin)) {
        oldest_xmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    }
    /* we can't do bypass in hotstandby read mode */
    if (RecoveryInProgress()) {
        oldest_xmin = InvalidTransactionId;
    }

    /* examine xmin */
    if (need_check_xmin) {
        if (TransactionIdPrecedes(*xmin, oldest_xmin)) {
            *xmin_committed = true;
        } else if (TransactionIdIsValid(*xmin)) {
            TransactionIdStatus ts = gv_graph_check_xid(*xmin);
            switch (ts) {
                case XID_INPROGRESS:
                    break;
                case XID_COMMITTED:
                    *xmin_committed = true;
                    break;
                case XID_ABORTED:
                    *xmin = InvalidTransactionId;
                    break;
            }
        }
    }

    if (!TransactionIdIsValid(*xmin)) {
        is_dead = true;
    }
    /* before we mark the tuple as DEAD because of xmax, must confirm that xmax has committed */
    if (*xmax_committed && TransactionIdPrecedes(*xmax, oldest_xmin)) {
        is_dead = true;
    }

    if (is_dead) {
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        *xmin_committed = *xmax_committed = false;
    }

    return is_dead;
}

/*
 * Simplified xid visibility check against a snapshot.
 */
static bool vector_check_xid_visibility(TransactionId xmin, TransactionId xmax,
    bool xmin_committed, bool xmax_committed, Snapshot snapshot, int2 bucketid, bool is_upsert)
{
    /* SNAPSHOT_DIRTY: used for dirty read, upsert visibility check */
    if (snapshot->satisfies == SNAPSHOT_DIRTY) {
        if (is_upsert) {
            bool xmax_visible = xmax_committed || TransactionIdIsCurrentTransactionId(xmax);
            if (xmax_visible) {
                return false;
            }
            return true;
        }
    }

    /* only support MVCC and NOW */
    if (snapshot->satisfies != SNAPSHOT_VERSION_MVCC &&
        snapshot->satisfies != SNAPSHOT_MVCC && snapshot->satisfies != SNAPSHOT_NOW) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("unsupported snapshot type %u for graph index scan",
                                   (unsigned int)snapshot->satisfies)));
    }

    /* handle snapshot MVCC and VERSION_MVCC */
    if (snapshot->satisfies == SNAPSHOT_VERSION_MVCC || snapshot->satisfies == SNAPSHOT_MVCC) {
        /* xmax committed: already deleted */
        if (xmax_committed) {
            return false;
        }
        /* xmax in-progress: only invisible if it's the current xid in upsert mode */
        if (TransactionIdIsCurrentTransactionId(xmax) && is_upsert) {
            return false;
        }
        /* xmin not committed and not current: not visible yet */
        if (!xmin_committed && !TransactionIdIsCurrentTransactionId(xmin)) {
            return false;
        }
        /* xmin committed before snapshot xmin: visible */
        if (xmin_committed && TransactionIdPrecedes(xmin, snapshot->xmin)) {
            return true;
        }
    }

    /* handle snapshot NOW */
    if (snapshot->satisfies == SNAPSHOT_NOW) {
        return xmin_committed && !xmax_committed;
    }

    return true;
}

/*
 * Check visibility by comparing snapshot curcid with xmax.
 */
static bool vector_check_cid_visibility(IndexScanDesc scan, TransactionId xmax, bool *need_recheck)
{
    Snapshot snapshot = scan->xs_snapshot;

    /* Do not revisit the heap */
    if (snapshot->curcid == GetCurrentCommandId(false)) {
        /* when the snapshot is the latest, index only scan don't need to recheck */
        *need_recheck = false;
        if (TransactionIdIsCurrentTransactionId(xmax)) {
            /* xmax is current xid: invisible */
            return false;
        }
        /* xmin is current xid, no xmax yet: visible */
        return true;
    }

    *need_recheck = true;
    /* treat as visible, but need recheck */
    return true;
}

/*
 * Simplified visibility check using IndexTupleDataWithTransInfo directly.
 */
static bool vector_check_visibility_simple(IndexScanDesc scan, const IndexTupleDataWithTransInfo& transInfo,
    bool *need_recheck, bool is_alive)
{
    bool need_check_visibility = scan->xs_snapshot->satisfies != SNAPSHOT_ANY &&
                                 scan->xs_snapshot->satisfies != SNAPSHOT_TOAST;
    TransactionId xmin;
    TransactionId xmax;
    bool xmin_committed = false;
    bool xmax_committed = false;
    bool is_dead = false;

    if (!is_alive) {
        xmin = InvalidTransactionId;
        xmax = InvalidTransactionId;
        is_dead = true;
    } else {
        int2 bucketid = RelationGetBktid(scan->indexRelation);
        is_dead = vector_get_tuple_xmin_xmax_simple(transInfo, InvalidTransactionId,
            &xmin, &xmax, &xmin_committed, &xmax_committed, bucketid);
    }

    bool is_visible = !is_dead;
    if (need_check_visibility && !is_dead) {
        if (scan->xs_snapshot->satisfies == SNAPSHOT_MVCC &&
            (TransactionIdIsCurrentTransactionId(xmin) || TransactionIdIsCurrentTransactionId(xmax))) {
            is_visible = vector_check_cid_visibility(scan, xmax, need_recheck);
        } else {
            is_visible = vector_check_xid_visibility(xmin, xmax, xmin_committed, xmax_committed,
                scan->xs_snapshot, RelationGetBktid(scan->indexRelation), scan->isUpsert);
        }
    }
    
    return is_visible;
}

/*
 * Simplified tuple visibility check for show_any_tuples mode.
 */
static bool vector_show_any_tuple_check_simple(IndexScanDesc scan, const IndexTupleDataWithTransInfo& transInfo,
    bool *need_recheck)
{
    *need_recheck = false;

    TransactionId xmin = transInfo.trans_info.xmin;
    TransactionId xmax = transInfo.trans_info.xmax;

    if (TransactionIdIsValid(xmax)) {
        TransactionIdStatus ts = gv_graph_check_xid(xmax);
        if (ts == XID_COMMITTED) {
            return false;
        }
    }

    TransactionId oldest_xmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    /* we can't do bypass in hotstandby read mode */
    if (RecoveryInProgress()) {
        oldest_xmin = InvalidTransactionId;
    }

    if (!TransactionIdPrecedes(xmin, oldest_xmin) && TransactionIdIsValid(xmin)) {
        TransactionIdStatus ts = gv_graph_check_xid(xmin);
        if (ts != XID_COMMITTED) {
            return false;
        }
    }

    if (!TransactionIdIsValid(xmin)) {
        return false;
    }
    return true;
}

/*
 * Main entry point for graph index search visibility check.
 */
IndexTupleData* vector_check_keys_simple(IndexScanDesc scan, const IndexTupleDataWithTransInfo& transInfo,
    ScanDirection dir, bool *need_recheck, bool is_alive)
{
    (void)dir;
    bool show_any_tuple_mode =
        u_sess->attr.attr_common.XactReadOnly && u_sess->attr.attr_storage.enable_show_any_tuples;

    /* Tuple is dead? */
    if (scan->ignore_killed_tuples && !is_alive && !show_any_tuple_mode) {
        return NULL;
    }

    /* Tuple is visible? */
    bool tuple_visible = false;
    if (!show_any_tuple_mode) {
        tuple_visible = vector_check_visibility_simple(scan, transInfo, need_recheck, is_alive);
    } else {
        tuple_visible = vector_show_any_tuple_check_simple(scan, transInfo, need_recheck);
    }
    return tuple_visible ? const_cast<IndexTupleData*>(&transInfo.index_tuple) : NULL;
}

bool gv_graph_get_tuple_xmin_xmax(Page page, OffsetNumber offnum, TransactionId oldest_xmin, TransactionId *xmin,
                        TransactionId *xmax, bool *xminCommitted, bool *xmaxCommitted, bool isToast)
{
    ItemId iid = PageGetItemId(page, offnum);
    IndexTuple itup = (IndexTuple)PageGetItem(page, iid);
    VectorIndexXidData *idxXid = (VectorIndexXidData *)VECTOR_INDEX_TUPLE_GET_XID(itup);
    bool isDead = false;
    bool needCheckXmin = true;

    *xminCommitted = *xmaxCommitted = false;

    if (ItemIdIsDead(iid)) {
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        return true;
    }

    *xmin = idxXid->xmin;
    *xmax = idxXid->xmax;

    /* examine xmax */
    if (TransactionIdIsValid(*xmax)) {
        TransactionIdStatus ts = gv_graph_check_xid(*xmax);
        switch (ts) {
            case XID_INPROGRESS:
                if (TransactionIdEquals(*xmin, *xmax)) {
                    needCheckXmin = false;
                }
                break;
            case XID_COMMITTED:
                *xminCommitted = *xmaxCommitted = true;
                needCheckXmin = false;
                break;
            case XID_ABORTED:
                idxXid->xmax = InvalidTransactionId;
                *xmax = InvalidTransactionId;
                if (TransactionIdEquals(*xmin, *xmax)) {
                    /* xmin xmax aborted */
                    idxXid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    needCheckXmin = false;
                }
                break;
        }
    }

    /* examine xmin */
    if (needCheckXmin) {
        if (IndexItemIdIsFrozen(iid)) {
            *xminCommitted = true;
        } else if (TransactionIdIsValid(*xmin)) {
            TransactionIdStatus ts = gv_graph_check_xid(*xmin);
            switch (ts) {
                case XID_INPROGRESS:
                    break;
                case XID_COMMITTED:
                    *xminCommitted = true;
                    break;
                case XID_ABORTED:
                    idxXid->xmin = InvalidTransactionId;
                    *xmin = InvalidTransactionId;
                    break;
            }
        }
    }

    /* if there is no passed oldest_xmin, we will use the current oldest_xmin */
    if (!TransactionIdIsValid(oldest_xmin)) {
        if (isToast) {
            GetOldestXminForUndo(&oldest_xmin);
        } else {
            oldest_xmin = u_sess->utils_cxt.RecentGlobalDataXmin;
        }
    }
    /* we can't do bypass in hotstandby read mode, or there will be different between index scan and seq scan */
    if (RecoveryInProgress()) {
        oldest_xmin = InvalidTransactionId;
    }

    if (!TransactionIdIsValid(*xmin)) {
        isDead = true;
    }
    /* before we mark the tuple as DEAD because of xmax, must comfirm that xmax has committed */
    if (*xmaxCommitted && TransactionIdPrecedes(*xmax, oldest_xmin)) {
        isDead = true;
    }

    /* before we mark the tuple as FROZEN, must comfirm that xmin has committed */
    if (IndexItemIdIsFrozen(iid)) {
        *xmin = FrozenTransactionId;
    } else if (*xminCommitted && TransactionIdPrecedes(*xmin, oldest_xmin)) {
        IndexItemIdSetFrozen(iid);
        *xmin = FrozenTransactionId;
    }
    
    if (isDead) {
        ItemIdMarkDead(iid);
        *xmin = InvalidTransactionId;
        *xmax = InvalidTransactionId;
        *xminCommitted = *xmaxCommitted = false;
    }

    return isDead;
}
