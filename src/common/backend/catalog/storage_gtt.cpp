/* -------------------------------------------------------------------------
 *
 * storage_gtt.c
 *      code to create and destroy physical storage for global temparary table
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *      src/backend/catalog/storage_gtt.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "catalog/storage_gtt.h"
#include "catalog/storage_xlog.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/freespace.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "threadpool/threadpool.h"
#include "utils/catcache.h"
#include "gs_threadlocal.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include <utils/relcache.h>

/* Copy from bitmapset.c, because gtt used the function in bitmapset.c */
#define WORDNUM(x) ((x) / BITS_PER_BITMAPWORD)

#define BITMAPSET_SIZE(nwords) (offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))

struct gtt_ctl_data {
    LWLock lock;
    int max_entry;
    Size entry_size;
};

struct gtt_fnode {
    Oid dbNode;
    Oid relNode;
};

struct gtt_shared_hash_entry {
    gtt_fnode rnode;
    Bitmapset* map;
    /* bitmap data */
};

struct gtt_relfilenode {
    Oid relfilenode;
    Oid spcnode;

    /* pg_class stat */
    int32 relpages;
    float4 reltuples;
    int32 relallvisible;
    TransactionId relfrozenxid;
};

struct gtt_local_hash_entry {
    Oid relid;

    List* relfilenode_list;

    char relkind;
    bool on_commit_delete;

    /* pg_statistic */
    int natts;
    int* attnum;
    HeapTuple* att_stat_tups;

    Oid oldrelid; /* remember the source of relid, before the switch relfilenode.
                   */
};

static Size action_gtt_shared_hash_entry_size(void);
static void gtt_storage_checkin(Oid relid);
static void gtt_storage_checkout(Oid relid, bool skiplock, bool isCommit);
static void gtt_storage_removeall(int code, Datum arg);
static void insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid);
static void remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid);
static void set_gtt_session_relfrozenxid(void);
static void gtt_reset_statistics(gtt_local_hash_entry* entry);
static void gtt_free_statistics(gtt_local_hash_entry* entry);
static gtt_relfilenode* gtt_search_relfilenode(const gtt_local_hash_entry* entry, Oid relfilenode, bool missingOk);
static gtt_local_hash_entry* gtt_search_by_relid(Oid relid, bool missingOk);

Datum pg_get_gtt_statistics(PG_FUNCTION_ARGS);
Datum pg_get_gtt_relstats(PG_FUNCTION_ARGS);
Datum pg_gtt_attached_pid(PG_FUNCTION_ARGS);
Datum pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS);

static Size action_gtt_shared_hash_entry_size(void)
{
    int wordnum;
    Size hashEntrySize = 0;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return 0;

    wordnum = WORDNUM(MAX_BACKEND_SLOT + 1);
    hashEntrySize += MAXALIGN(sizeof(gtt_shared_hash_entry));
    hashEntrySize += (size_t)MAXALIGN(BITMAPSET_SIZE(wordnum + 1));

    return hashEntrySize;
}

Size active_gtt_shared_hash_size(void)
{
    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return 0;

    Size size = MAXALIGN(sizeof(gtt_ctl_data));
    Size hashEntrySize = action_gtt_shared_hash_entry_size();
    size += hash_estimate_size(u_sess->attr.attr_storage.max_active_gtt, hashEntrySize);

    return size;
}

void active_gtt_shared_hash_init(void)
{
    HASHCTL info;
    bool found;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    t_thrd.shemem_ptr_cxt.gtt_shared_ctl =
        (gtt_ctl_data*)ShmemInitStruct("gtt_shared_ctl", sizeof(gtt_ctl_data), &found);

    if (!found) {
        LWLockRegisterTranche((int)LWTRANCHE_GTT_CTL, "gtt_shared_ctl");
        LWLockInitialize(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, (int)LWTRANCHE_GTT_CTL);
        t_thrd.shemem_ptr_cxt.gtt_shared_ctl->max_entry = u_sess->attr.attr_storage.max_active_gtt;
        t_thrd.shemem_ptr_cxt.gtt_shared_ctl->entry_size = action_gtt_shared_hash_entry_size();
    }

    errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
    securec_check(rc, "", "");
    info.keysize = sizeof(gtt_fnode);
    info.entrysize = action_gtt_shared_hash_entry_size();
    t_thrd.shemem_ptr_cxt.active_gtt_shared_hash = ShmemInitHash("active gtt shared hash",
        t_thrd.shemem_ptr_cxt.gtt_shared_ctl->max_entry,
        t_thrd.shemem_ptr_cxt.gtt_shared_ctl->max_entry,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
}

static void gtt_storage_checkin(Oid relid)
{
    gtt_shared_hash_entry* entry;
    bool found;
    gtt_fnode fnode = {0};

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    fnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
    fnode.relNode = relid;
    (void)LWLockAcquire(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, LW_EXCLUSIVE);
    entry = (gtt_shared_hash_entry*)hash_search(
        t_thrd.shemem_ptr_cxt.active_gtt_shared_hash, &fnode, HASH_ENTER_NULL, &found);
    if (entry == NULL) {
        LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("out of shared memory"),
                errhint("You might need to increase max_active_global_temporary_table.")));
    }

    if (found == false) {
        int wordnum;

        entry->map = (Bitmapset*)((char*)entry + MAXALIGN(sizeof(gtt_shared_hash_entry)));
        wordnum = WORDNUM(MAX_BACKEND_SLOT + 1);
        errno_t rc = memset_s(entry->map, (size_t)BITMAPSET_SIZE(wordnum + 1), 0, (size_t)BITMAPSET_SIZE(wordnum + 1));
        securec_check(rc, "", "");
        entry->map->nwords = wordnum + 1;
    }

    (void)bms_add_member(entry->map, BackendIdForTempRelations);
    LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);
}

static void gtt_storage_checkout(Oid relid, bool skiplock, bool isCommit)
{
    gtt_shared_hash_entry* entry;
    gtt_fnode fnode = {0};

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    fnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
    fnode.relNode = relid;
    if (!skiplock) {
        (void)LWLockAcquire(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, LW_EXCLUSIVE);
    }

    entry = (gtt_shared_hash_entry*)hash_search(t_thrd.shemem_ptr_cxt.active_gtt_shared_hash, &fnode, HASH_FIND, NULL);
    if (entry == NULL) {
        if (!skiplock) {
            LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);
        }
        if (isCommit) {
            elog(WARNING, "relid %u not exist in gtt shared hash when forget", relid);
        }
        return;
    }

    Assert(BackendIdForTempRelations >= 1 && BackendIdForTempRelations <= MAX_BACKEND_SLOT);
    (void)bms_del_member(entry->map, BackendIdForTempRelations);

    if (bms_is_empty(entry->map)) {
        if (!hash_search(t_thrd.shemem_ptr_cxt.active_gtt_shared_hash, &fnode, HASH_REMOVE, NULL)) {
            elog(PANIC, "gtt shared hash table corrupted");
        }
    }

    if (!skiplock)
        LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);

    return;
}

Bitmapset* copy_active_gtt_bitmap(Oid relid)
{
    gtt_shared_hash_entry* entry;
    Bitmapset* mapCopy = NULL;
    gtt_fnode fnode = {0};

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return NULL;

    fnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
    fnode.relNode = relid;
    (void)LWLockAcquire(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, LW_SHARED);
    entry = (gtt_shared_hash_entry*)hash_search(t_thrd.shemem_ptr_cxt.active_gtt_shared_hash, &fnode, HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);
        return NULL;
    }

    Assert(entry->map);
    if (!bms_is_empty(entry->map))
        mapCopy = bms_copy(entry->map);

    LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);

    return mapCopy;
}

bool is_other_backend_use_gtt(Oid relid)
{
    gtt_shared_hash_entry* entry;
    bool inUse = false;
    gtt_fnode fnode = {0};

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return false;

    fnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
    fnode.relNode = relid;
    (void)LWLockAcquire(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, LW_SHARED);
    entry = (gtt_shared_hash_entry*)hash_search(t_thrd.shemem_ptr_cxt.active_gtt_shared_hash, &fnode, HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);
        return false;
    }

    Assert(entry->map);
    Assert(BackendIdForTempRelations >= 1 && BackendIdForTempRelations <= MAX_BACKEND_SLOT);

    int numUse = bms_num_members(entry->map);
    if (numUse == 0) {
        inUse = false;
    } else if (numUse == 1) {
        if (bms_is_member(BackendIdForTempRelations, entry->map)) {
            inUse = false;
        } else {
            inUse = true;
        }
    } else {
        inUse = true;
    }

    LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);

    return inUse;
}

void remember_gtt_storage_info(const RelFileNode rnode, Relation rel)
{
    gtt_local_hash_entry* entry;
    MemoryContext oldcontext;
    gtt_relfilenode* newNode = NULL;
    Oid relid = RelationGetRelid(rel);

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Global temporary table feature is disable"),
                errhint("You might need to increase max_active_global_temporary_table "
                        "to enable this feature.")));
    }

    if (RecoveryInProgress()) {
        elog(ERROR, "readonly mode not support access global temporary table");
    }
    if (rel->rd_rel->relkind == RELKIND_INDEX && rel->rd_index &&
        (!rel->rd_index->indisvalid || !rel->rd_index->indisready)) {
        elog(ERROR, "invalid gtt index %s not allow to create storage", RelationGetRelationName(rel));
    }

    if (u_sess->gtt_ctx.gtt_storage_local_hash == NULL) {
#define GTT_LOCAL_HASH_SIZE 1024
        /* First time through: initialize the hash table */
        HASHCTL ctl;
        errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "", "");
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(gtt_local_hash_entry);
        ctl.hcxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
        u_sess->gtt_ctx.gtt_storage_local_hash = hash_create(
            "global temporary table info", GTT_LOCAL_HASH_SIZE, &ctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

        if (u_sess->cache_mem_cxt == nullptr) {
            u_sess->cache_mem_cxt =
                AllocSetContextCreate(u_sess->top_mem_cxt, "SessionCacheMemoryContext", ALLOCSET_DEFAULT_SIZES);
        }

        u_sess->gtt_ctx.gtt_relstats_context =
            AllocSetContextCreate(u_sess->cache_mem_cxt, "gtt relstats context", ALLOCSET_DEFAULT_SIZES);
    }

    oldcontext = MemoryContextSwitchTo(u_sess->gtt_ctx.gtt_relstats_context);

    entry = gtt_search_by_relid(relid, true);
    if (!entry) {
        bool found = false;
        int natts = 0;

        /* Look up or create an entry */
        entry = (gtt_local_hash_entry*)hash_search(
            u_sess->gtt_ctx.gtt_storage_local_hash, &relid, HASH_ENTER, &found);

        if (found) {
            (void)MemoryContextSwitchTo(oldcontext);
            elog(ERROR, "backend %d relid %u already exists in gtt local hash", BackendIdForTempRelations, relid);
        }

        entry->relfilenode_list = NIL;
        entry->relkind = rel->rd_rel->relkind;
        entry->on_commit_delete = false;
        entry->natts = 0;
        entry->attnum = NULL;
        entry->att_stat_tups = NULL;
        entry->oldrelid = InvalidOid;

        natts = RelationGetNumberOfAttributes(rel);
        entry->attnum = (int*)palloc0(sizeof(int) * (unsigned long)natts);
        entry->att_stat_tups = (HeapTuple*)palloc0(sizeof(HeapTuple) * (unsigned long)natts);
        entry->natts = natts;

        if (entry->relkind == RELKIND_RELATION) {
            if (RELATION_GTT_ON_COMMIT_DELETE(rel)) {
                entry->on_commit_delete = true;
                register_on_commit_action(RelationGetRelid(rel), ONCOMMIT_DELETE_ROWS);
            }
        }

        if (entry->relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(entry->relkind)) {
            gtt_storage_checkin(relid);
        }
    }

    newNode = (gtt_relfilenode*)palloc0(sizeof(gtt_relfilenode));
    newNode->relfilenode = rnode.relNode;
    newNode->spcnode = rnode.spcNode;
    newNode->relpages = 0;
    newNode->reltuples = 0;
    newNode->relallvisible = 0;
    newNode->relfrozenxid = InvalidTransactionId;
    entry->relfilenode_list = lappend(entry->relfilenode_list, newNode);

    /* only heap contain transaction information */
    if (entry->relkind == RELKIND_RELATION) {
        newNode->relfrozenxid = u_sess->utils_cxt.RecentXmin;
        insert_gtt_relfrozenxid_to_ordered_list((Oid)newNode->relfrozenxid);
        set_gtt_session_relfrozenxid();
    }

    gtt_reset_statistics(entry);

    (void)MemoryContextSwitchTo(oldcontext);

    if (!u_sess->gtt_ctx.gtt_cleaner_exit_registered) {
        u_sess->gtt_ctx.gtt_sess_exit = gtt_storage_removeall;
        u_sess->gtt_ctx.gtt_cleaner_exit_registered = true;
    }

    return;
}

void forget_gtt_storage_info(Oid relid, const RelFileNode rnode, bool isCommit)
{
    gtt_local_hash_entry* entry = NULL;
    gtt_relfilenode* dRnode = NULL;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        return;
    }
    entry = gtt_search_by_relid(relid, true);
    if (entry == NULL) {
        if (isCommit) {
            elog(ERROR, "gtt rel %u not found in local hash", relid);
        }
        return;
    }

    dRnode = gtt_search_relfilenode(entry, rnode.relNode, true);
    if (dRnode == NULL) {
        if (isCommit) {
            elog(ERROR, "gtt relfilenode %u not found in rel %u", rnode.relNode, relid);
        } else if (entry->oldrelid != InvalidOid) {
            gtt_local_hash_entry* entry2 = NULL;
            gtt_relfilenode* gttnode2 = NULL;

            entry2 = gtt_search_by_relid(entry->oldrelid, false);
            Assert(entry2 != NULL);
            gttnode2 = gtt_search_relfilenode(entry2, rnode.relNode, false);
            Assert(gttnode2->relfilenode == rnode.relNode);
            Assert(list_length(entry->relfilenode_list) == 1);
            /* rollback switch relfilenode */
            gtt_switch_rel_relfilenode(
                entry2->relid, gttnode2->relfilenode, entry->relid, gtt_fetch_current_relfilenode(entry->relid), false);
            /* clean up footprint */
            entry2->oldrelid = InvalidOid;
            dRnode = gtt_search_relfilenode(entry, rnode.relNode, false);
            Assert(dRnode);
        } else {
            if (entry->relfilenode_list == NIL) {
                if (entry->relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(entry->relkind))
                    gtt_storage_checkout(relid, false, isCommit);

                gtt_free_statistics(entry);
                (void*)hash_search(u_sess->gtt_ctx.gtt_storage_local_hash, (void*)&(relid), HASH_REMOVE, NULL);
            }
            return;
        }
    }

    if (entry->relkind == RELKIND_RELATION) {
        Assert(TransactionIdIsNormal(dRnode->relfrozenxid) || !isCommit);
        if (TransactionIdIsValid(dRnode->relfrozenxid)) {
            remove_gtt_relfrozenxid_from_ordered_list((Oid)dRnode->relfrozenxid);
            set_gtt_session_relfrozenxid();
        }
    }

    entry->relfilenode_list = list_delete_ptr(entry->relfilenode_list, dRnode);
    pfree(dRnode);
    if (entry->relfilenode_list == NIL) {
        if (entry->relkind == RELKIND_RELATION || RELKIND_IS_SEQUENCE(entry->relkind))
            gtt_storage_checkout(relid, false, isCommit);

        if (isCommit && entry->oldrelid != InvalidOid) {
            gtt_local_hash_entry* entry2 = NULL;

            entry2 = gtt_search_by_relid(entry->oldrelid, false);
            Assert(entry2 != NULL);
            /* clean up footprint */
            entry2->oldrelid = InvalidOid;
        }

        gtt_free_statistics(entry);
        (void*)hash_search(u_sess->gtt_ctx.gtt_storage_local_hash, (void*)&(relid), HASH_REMOVE, NULL);
    } else
        gtt_reset_statistics(entry);

    return;
}

/* is the storage file was created in this backend */
bool gtt_storage_attached(Oid relid)
{
    bool found = false;
    gtt_local_hash_entry* entry = NULL;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        return false;
    }

    entry = gtt_search_by_relid(relid, true);
    if (entry) {
        found = true;
    }
    return found;
}

#define INIT_ALLOC_NUMS 32
static void gtt_storage_removeall(int code, Datum arg)
{
    HASH_SEQ_STATUS status;
    gtt_local_hash_entry* entry;
    SMgrRelation* srels = NULL;
    Oid* relids = NULL;
    char* relkinds = NULL;
    unsigned long nrels = 0;
    unsigned long nfiles = 0;
    unsigned long maxrels = 0;
    unsigned long maxfiles = 0;
    unsigned long i = 0;

    if (u_sess->gtt_ctx.gtt_storage_local_hash == NULL)
        return;

    hash_seq_init(&status, u_sess->gtt_ctx.gtt_storage_local_hash);
    while ((entry = (gtt_local_hash_entry*)hash_seq_search(&status)) != NULL) {
        ListCell* lc;

        foreach (lc, entry->relfilenode_list) {
            SMgrRelation srel;
            RelFileNode rnode;
            gtt_relfilenode* gtt_rnode = (gtt_relfilenode*)lfirst(lc);

            rnode.spcNode = gtt_rnode->spcnode;
            rnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
            rnode.relNode = gtt_rnode->relfilenode;
            rnode.opt = 0;
            rnode.bucketNode = InvalidBktId;
            srel = smgropen(rnode, BackendIdForTempRelations);

            if (maxfiles == 0) {
                maxfiles = INIT_ALLOC_NUMS;
                srels = (SMgrRelation*)palloc(sizeof(SMgrRelation) * maxfiles);
            } else if (maxfiles <= nfiles) {
                maxfiles *= 2;
                srels = (SMgrRelation*)repalloc(srels, sizeof(SMgrRelation) * maxfiles);
            }

            srels[nfiles++] = srel;
        }

        if (maxrels == 0) {
            maxrels = INIT_ALLOC_NUMS;
            relids = (Oid*)palloc(sizeof(Oid) * maxrels);
            relkinds = (char*)palloc(sizeof(char) * maxrels);
        } else if (maxrels <= nrels) {
            maxrels *= 2;
            relids = (Oid*)repalloc(relids, sizeof(Oid) * maxrels);
            relkinds = (char*)repalloc(relkinds, sizeof(char) * maxrels);
        }

        relkinds[nrels] = entry->relkind;
        relids[nrels] = entry->relid;
        nrels++;
    }

    if (nfiles > 0) {
        for (i = 0; i < nfiles; i++) {
            smgrdounlink(srels[i], false);
            smgrclose(srels[i]);
        }
        pfree(srels);
    }

    if (nrels) {
        (void)LWLockAcquire(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock, LW_EXCLUSIVE);
        for (i = 0; i < nrels; i++) {
            if (relkinds[i] == RELKIND_RELATION || RELKIND_IS_SEQUENCE(relkinds[i]))
                gtt_storage_checkout(relids[i], true, false);
        }
        LWLockRelease(&t_thrd.shemem_ptr_cxt.gtt_shared_ctl->lock);

        pfree(relids);
        pfree(relkinds);
    }

    if (ENABLE_THREAD_POOL) {
        u_sess->gtt_ctx.gtt_session_frozenxid = InvalidTransactionId;
    } else {
        t_thrd.proc->gtt_session_frozenxid = InvalidTransactionId;
    }

    return;
}

/*
 * Update global temp table relstats(relpage/reltuple/relallvisible)
 * to local hashtable
 */
void up_gtt_relstats(const Relation relation, BlockNumber numPages, double numTuples, BlockNumber numAllVisiblePages,
    TransactionId relfrozenxid)
{
    Oid relid = RelationGetRelid(relation);
    gtt_local_hash_entry* entry;
    gtt_relfilenode* gttRnode = NULL;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    entry = gtt_search_by_relid(relid, true);
    if (entry == NULL)
        return;

    gttRnode = (gtt_relfilenode*)lfirst(list_tail(entry->relfilenode_list));
    if (gttRnode == NULL)
        return;

    if (gttRnode->relpages != int32(numPages)) {
        gttRnode->relpages = int32(numPages);
    }

    if (numTuples >= 0 && gttRnode->reltuples != (float4)numTuples)
        gttRnode->reltuples = (float4)numTuples;

    /* only heap contain transaction information and relallvisible */
    if (entry->relkind == RELKIND_RELATION) {
        if (gttRnode->relallvisible >= 0 && gttRnode->relallvisible != int32(numAllVisiblePages)) {
            gttRnode->relallvisible = int32(numAllVisiblePages);
        }

        if (TransactionIdIsNormal(relfrozenxid) && gttRnode->relfrozenxid != relfrozenxid &&
            (TransactionIdPrecedes(gttRnode->relfrozenxid, relfrozenxid) ||
                TransactionIdPrecedes(ReadNewTransactionId(), gttRnode->relfrozenxid))) {
            remove_gtt_relfrozenxid_from_ordered_list((Oid)(gttRnode->relfrozenxid));
            gttRnode->relfrozenxid = relfrozenxid;
            insert_gtt_relfrozenxid_to_ordered_list((Oid)relfrozenxid);
            set_gtt_session_relfrozenxid();
        }
    }

    return;
}

/*
 * Search global temp table relstats(relpage/reltuple/relallvisible)
 * from local hashtable.
 */
bool get_gtt_relstats(Oid relid, BlockNumber* relpages, double* reltuples, BlockNumber* relallvisible,
    TransactionId* relfrozenxid)
{
    gtt_local_hash_entry* entry;
    gtt_relfilenode* gttRnode = NULL;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return false;

    entry = gtt_search_by_relid(relid, true);
    if (entry == NULL)
        return false;

    Assert(entry->relid == relid);

    gttRnode = (gtt_relfilenode*)lfirst(list_tail(entry->relfilenode_list));
    if (gttRnode == NULL)
        return false;

    if (relpages)
        *relpages = (unsigned int)(gttRnode->relpages);

    if (reltuples)
        *reltuples = gttRnode->reltuples;

    if (relallvisible)
        *relallvisible = (unsigned int)(gttRnode->relallvisible);

    if (relfrozenxid)
        *relfrozenxid = gttRnode->relfrozenxid;

    return true;
}

void remove_gtt_att_statistic(Oid reloid, int attnum)
{
    gtt_local_hash_entry* entry = gtt_search_by_relid(reloid, true);
    if (entry == NULL) {
        return;
    }
    for (int i = 0; i < entry->natts; i++) {
        if (entry->attnum[i] != attnum) {
            continue;
        }
        Assert(entry->att_stat_tups[i]);
        entry->attnum[i] = 0;
        heap_freetuple(entry->att_stat_tups[i]);
        entry->att_stat_tups[i] = NULL;
        break;
    }
    return;
}

void extend_gtt_att_statistic(gtt_local_hash_entry* entry, int natts)
{
    Assert(entry != NULL);
    Assert(entry->natts < natts);

    MemoryContext oldContext = MemoryContextSwitchTo(u_sess->gtt_ctx.gtt_relstats_context);
    int* attNum = (int*)palloc0(sizeof(int) * natts);
    HeapTuple* attStatTups = (HeapTuple*)palloc0(sizeof(HeapTuple) * natts);
    for (int i = 0; i < entry->natts; i++) {
        if (entry->attnum[i] == 0) {
            continue;
        }
        attNum[i] = entry->attnum[i];
        attStatTups[i] = entry->att_stat_tups[i];
    }
    pfree(entry->attnum);
    pfree(entry->att_stat_tups);

    entry->natts = natts;
    entry->attnum = attNum;
    entry->att_stat_tups = attStatTups;
    (void)MemoryContextSwitchTo(oldContext);
    return;
}

/*
 * Update global temp table statistic info(definition is same as pg_statistic)
 * to local hashtable where ananyze global temp table
 */
void up_gtt_att_statistic(Oid reloid, int attnum, int natts, TupleDesc tupleDescriptor, Datum* values, bool* isnull)
{
    gtt_local_hash_entry* entry;
    MemoryContext oldcontext;
    int i = 0;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    entry = gtt_search_by_relid(reloid, true);
    if (entry == NULL)
        return;
    if (entry->natts < natts) {
        extend_gtt_att_statistic(entry, natts);
    }

    oldcontext = MemoryContextSwitchTo(u_sess->gtt_ctx.gtt_relstats_context);
    Assert(entry->relid == reloid);
    for (i = 0; i < entry->natts; i++) {
        if (entry->attnum[i] == 0) {
            entry->attnum[i] = attnum;
            break;
        } else if (entry->attnum[i] == attnum) {
            Assert(entry->att_stat_tups[i]);
            heap_freetuple(entry->att_stat_tups[i]);
            entry->att_stat_tups[i] = NULL;
            break;
        }
    }

    Assert(i < entry->natts);
    Assert(entry->att_stat_tups[i] == NULL);
    entry->att_stat_tups[i] = heap_form_tuple(tupleDescriptor, values, isnull);
    (void)MemoryContextSwitchTo(oldcontext);

    return;
}

/*
 * Search global temp table statistic info(definition is same as pg_statistic)
 * from local hashtable.
 */
HeapTuple get_gtt_att_statistic(Oid reloid, int attnum)
{
    gtt_local_hash_entry* entry;
    int i = 0;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return NULL;

    entry = gtt_search_by_relid(reloid, true);
    if (entry == NULL)
        return NULL;

    for (i = 0; i < entry->natts; i++) {
        if (entry->attnum[i] == attnum) {
            Assert(entry->att_stat_tups[i]);
            return entry->att_stat_tups[i];
        }
    }

    return NULL;
}

void release_gtt_statistic_cache(HeapTuple tup)
{
    /* do nothing */
    return;
}

static void insert_gtt_relfrozenxid_to_ordered_list(Oid relfrozenxid)
{
    MemoryContext oldcontext;
    ListCell* cell;
    int i;

    Assert(TransactionIdIsNormal(relfrozenxid));

    oldcontext = MemoryContextSwitchTo(u_sess->gtt_ctx.gtt_relstats_context);
    /* Does the datum belong at the front? */
    if (u_sess->gtt_ctx.gtt_session_relfrozenxid_list == NIL ||
        TransactionIdFollowsOrEquals(relfrozenxid, linitial_oid(u_sess->gtt_ctx.gtt_session_relfrozenxid_list))) {
        u_sess->gtt_ctx.gtt_session_relfrozenxid_list =
            lcons_oid(relfrozenxid, u_sess->gtt_ctx.gtt_session_relfrozenxid_list);
        MemoryContextSwitchTo(oldcontext);

        return;
    }

    /* No, so find the entry it belongs after */
    i = 0;
    foreach (cell, u_sess->gtt_ctx.gtt_session_relfrozenxid_list) {
        if (TransactionIdFollowsOrEquals(relfrozenxid, lfirst_oid(cell)))
            break;

        i++;
    }
    u_sess->gtt_ctx.gtt_session_relfrozenxid_list =
        list_insert_nth_oid(u_sess->gtt_ctx.gtt_session_relfrozenxid_list, i, relfrozenxid);
    MemoryContextSwitchTo(oldcontext);

    return;
}

static void remove_gtt_relfrozenxid_from_ordered_list(Oid relfrozenxid)
{
    u_sess->gtt_ctx.gtt_session_relfrozenxid_list =
        list_delete_oid(u_sess->gtt_ctx.gtt_session_relfrozenxid_list, relfrozenxid);
}

static void set_gtt_session_relfrozenxid(void)
{
    TransactionId gtt_frozenxid = InvalidTransactionId;

    if (u_sess->gtt_ctx.gtt_session_relfrozenxid_list)
        gtt_frozenxid = llast_oid(u_sess->gtt_ctx.gtt_session_relfrozenxid_list);

    if (ENABLE_THREAD_POOL) {
        u_sess->gtt_ctx.gtt_session_frozenxid = gtt_frozenxid;
    } else {
        t_thrd.proc->gtt_session_frozenxid = gtt_frozenxid;
    }
}

Datum pg_get_gtt_statistics(PG_FUNCTION_ARGS)
{
    HeapTuple tuple;
    int attnum = PG_GETARG_INT32(1);
    Oid reloid = PG_GETARG_OID(0);
    char relPersistence;
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    MemoryContext oldcontext;
    Tuplestorestate* tupstore;
    TupleDesc sd;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot "
                       "accept a set")));
    }

    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not "
                       "allowed in this context")));
    }

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    (void)MemoryContextSwitchTo(oldcontext);

    Relation rel = relation_open(reloid, AccessShareLock);
    relPersistence = get_rel_persistence(reloid);
    if (relPersistence != RELPERSISTENCE_GLOBAL_TEMP) {
        elog(WARNING, "relation OID %u is not a global temporary table", reloid);
        relation_close(rel, NoLock);
        return (Datum)0;
    }

    Relation pgStatistic = relation_open(StatisticRelationId, AccessShareLock);
    sd = RelationGetDescr(pgStatistic);

    tuple = get_gtt_att_statistic(reloid, attnum);
    if (tuple) {
        Datum values[Natts_pg_statistic];
        bool isnull[Natts_pg_statistic];
        HeapTuple res = NULL;

        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
        securec_check(rc, "", "");
        heap_deform_tuple(tuple, sd, values, isnull);
        res = heap_form_tuple(tupdesc, values, isnull);
        tuplestore_puttuple(tupstore, res);
    }

    relation_close(rel, NoLock);
    relation_close(pgStatistic, AccessShareLock);
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

Datum pg_get_gtt_relstats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore;
    MemoryContext oldcontext;
    HeapTuple tuple;
    Oid reloid = PG_GETARG_OID(0);
    char relPersistence;
    BlockNumber relpages = 0;
    double reltuples = 0;
    BlockNumber relallvisible = 0;
    TransactionId relfrozenxid = 0;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot "
                       "accept a set")));
    }

    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed "
                       "in this context")));
    }

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    (void)MemoryContextSwitchTo(oldcontext);

    Relation rel = relation_open(reloid, AccessShareLock);
    relPersistence = get_rel_persistence(reloid);
    if (relPersistence != RELPERSISTENCE_GLOBAL_TEMP) {
        elog(WARNING, "relation OID %u is not a global temporary table", reloid);
        relation_close(rel, NoLock);
        return (Datum)0;
    }

    (void)get_gtt_relstats(reloid, &relpages, &reltuples, &relallvisible, &relfrozenxid);
    Oid relnode = gtt_fetch_current_relfilenode(reloid);
    if (relnode != InvalidOid) {
        // output attribute: relfilenode | relpages | reltuples | relallvisible | relfrozenxid | relminmxid
        Datum values[6];
        bool isnull[6];

        errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
        securec_check(rc, "", "");
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "", "");
        values[0] = UInt32GetDatum(relnode);
        values[1] = Int32GetDatum(relpages);
        values[2] = Float4GetDatum((float4)reltuples);
        values[3] = Int32GetDatum(relallvisible);
        values[4] = UInt32GetDatum(relfrozenxid);
        tuple = heap_form_tuple(tupdesc, values, isnull);
        tuplestore_puttuple(tupstore, tuple);
    }

    tuplestore_donestoring(tupstore);
    relation_close(rel, NoLock);

    return (Datum)0;
}

Datum pg_gtt_attached_pid(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore;
    MemoryContext oldcontext;
    HeapTuple tuple;
    Oid reloid = PG_GETARG_OID(0);
    char relPersistence;
    PGPROC* proc = NULL;
    Bitmapset* map = NULL;
    ThreadId pid = 0;
    int backendid = 0;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot "
                       "accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed "
                       "in this context")));

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    (void)MemoryContextSwitchTo(oldcontext);

    Relation rel = relation_open(reloid, AccessShareLock);
    relPersistence = get_rel_persistence(reloid);
    if (relPersistence != RELPERSISTENCE_GLOBAL_TEMP) {
        elog(WARNING, "relation OID %u is not a global temporary table", reloid);
        relation_close(rel, NoLock);
        return (Datum)0;
    }

    map = copy_active_gtt_bitmap(reloid);
    if (map) {
        backendid = bms_first_member(map);

        do {
            if (ENABLE_THREAD_POOL) {
                ThreadPoolSessControl* sess_ctrl = g_threadPoolControler->GetSessionCtrl();
                knl_session_context* sess = sess_ctrl->GetSessionByIdx(backendid);
                if (sess == NULL) {     // session already exited while processing bitmap
                    backendid = bms_next_member(map, backendid);
                    continue;
                }
                pid = sess->attachPid;
            } else {
                proc = BackendIdGetProc(backendid);
                if (proc == NULL) {     // session already exited while processing bitmap
                    backendid = bms_next_member(map, backendid);
                    continue;
                }
                pid = proc->pid;
            }

            // output attribute: relid | pid
            Datum values[2];
            bool isnull[2];
            errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
            securec_check(rc, "", "");
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            values[0] = UInt32GetDatum(reloid);
            values[1] = UInt64GetDatum(pid);
            tuple = heap_form_tuple(tupdesc, values, isnull);
            tuplestore_puttuple(tupstore, tuple);
            backendid = bms_next_member(map, backendid);
        } while (backendid > 0);

        pfree(map);
    }

    tuplestore_donestoring(tupstore);
    relation_close(rel, NoLock);

    return (Datum)0;
}

Datum pg_list_gtt_relfrozenxids(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore;
    MemoryContext oldcontext;
    HeapTuple tuple;
    int numXid = g_instance.shmem_cxt.MaxBackends + 1;
    ThreadId* pids = NULL;
    TransactionId* xids = NULL;
    int i = 0;
    int j = 0;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot "
                       "accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed "
                       "in this context")));
    }

    oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    (void)MemoryContextSwitchTo(oldcontext);

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        return (Datum)0;
    }

    if (RecoveryInProgress()) {
        return (Datum)0;
    }

    pids = (ThreadId*)palloc0(sizeof(ThreadId) * numXid);
    xids = (TransactionId*)palloc0(sizeof(TransactionId) * numXid);

    TransactionId oldest = InvalidTransactionId;
    if (ENABLE_THREAD_POOL) {
        ThreadPoolSessControl* sessCtl = g_threadPoolControler->GetSessionCtrl();
        oldest = sessCtl->ListAllSessionGttFrozenxids(numXid, pids, xids, &i);
    } else {
        oldest = ListAllThreadGttFrozenxids(numXid, pids, xids, &i);
    }

    if (i > 0) {
        pids[i] = 0;
        xids[i] = oldest;
        i++;

        for (j = 0; j < i; j++) {
            // output attribute: pid | relfrozenxid
            Datum values[2];
            bool isnull[2];

            errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
            securec_check(rc, "", "");
            rc = memset_s(values, sizeof(values), 0, sizeof(values));
            securec_check(rc, "", "");
            values[0] = UInt64GetDatum(pids[j]);
            values[1] = UInt64GetDatum(xids[j]);
            tuple = heap_form_tuple(tupdesc, values, isnull);
            tuplestore_puttuple(tupstore, tuple);
        }
    }
    tuplestore_donestoring(tupstore);
    pfree(pids);
    pfree(xids);

    return (Datum)0;
}

void gtt_force_enable_index(Relation index)
{
    if (!RELATION_IS_GLOBAL_TEMP(index))
        return;

    Assert(index->rd_rel->relkind == RELKIND_INDEX);
    Assert(OidIsValid(RelationGetRelid(index)));

    index->rd_index->indisvalid = true;
    index->rd_index->indisready = true;
}

void gtt_fix_index_state(Relation index)
{
    Oid indexOid = RelationGetRelid(index);
    Oid relOid = index->rd_index->indrelid;

    if (!RELATION_IS_GLOBAL_TEMP(index))
        return;

    if (!index->rd_index->indisvalid)
        return;

    if (gtt_storage_attached(relOid) && !gtt_storage_attached(indexOid)) {
        index->rd_index->indisvalid = false;
        index->rd_index->indisready = false;
    }

    return;
}

/* remove junk files when other session exited unexpected */
static void UnlinkJunkRelFile(Relation rel)
{
    if (rel->rd_smgr == NULL) {
        RelationOpenSmgr(rel);
    }
    if (smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
        elog(WARNING, "gtt relfilenode %u already exists", rel->rd_node.relNode);
        smgrdounlink(rel->rd_smgr, false);
        smgrclose(rel->rd_smgr);
    }

    gtt_local_hash_entry* entry = gtt_search_by_relid(rel->rd_id, true);
    if (entry == NULL) {
        return;
    }
    gtt_relfilenode* dRnode = gtt_search_relfilenode(entry, rel->rd_node.relNode, true);
    if (dRnode != NULL) {
        entry->relfilenode_list = list_delete_ptr(entry->relfilenode_list, dRnode);
        pfree(dRnode);
    }

    return;
}

static void CreateGTTRelFiles(const ResultRelInfo* resultRelInfo)
{
    Relation relation = resultRelInfo->ri_RelationDesc;
    int i;

    /* remove junk files when other session exited unexpected */
    UnlinkJunkRelFile(relation);

    RelationCreateStorage(
        relation->rd_node, RELPERSISTENCE_GLOBAL_TEMP, relation->rd_rel->relowner, InvalidOid, relation);
    for (i = 0; i < resultRelInfo->ri_NumIndices; i++) {
        Relation index = resultRelInfo->ri_IndexRelationDescs[i];
        /* remove junk files when other session exited unexpected */
        UnlinkJunkRelFile(index);

        IndexInfo* info = resultRelInfo->ri_IndexRelationInfo[i];
        Assert(index->rd_index->indisvalid);
        Assert(index->rd_index->indisready);
        index_build(relation, NULL, index, NULL, info, index->rd_index->indisprimary, false, INDEX_CREATE_NONE_PARTITION);
    }

    Oid toastrelid = relation->rd_rel->reltoastrelid;
    if (OidIsValid(toastrelid)) {
        Relation toastrel = relation_open(toastrelid, RowExclusiveLock);
        /* remove junk files when other session exited unexpected */
        UnlinkJunkRelFile(toastrel);

        RelationCreateStorage(
            toastrel->rd_node, RELPERSISTENCE_GLOBAL_TEMP, toastrel->rd_rel->relowner, InvalidOid, toastrel);

        ListCell* indlist = NULL;
        foreach (indlist, RelationGetIndexList(toastrel)) {
            Oid indexId = lfirst_oid(indlist);
            Relation currentIndex;
            IndexInfo* indexInfo = NULL;

            currentIndex = index_open(indexId, RowExclusiveLock);
            /* remove junk files when other session exited unexpected */
            UnlinkJunkRelFile(currentIndex);

            indexInfo = BuildDummyIndexInfo(currentIndex);
            index_build(
                toastrel, NULL, currentIndex, NULL, indexInfo, currentIndex->rd_index->indisprimary, false, INDEX_CREATE_NONE_PARTITION);
            index_close(currentIndex, NoLock);
        }

        relation_close(toastrel, NoLock);
    }

    return;
}

void init_gtt_storage(CmdType operation, ResultRelInfo* resultRelInfo)
{
    Relation relation = resultRelInfo->ri_RelationDesc;

    if (!RELATION_IS_GLOBAL_TEMP(relation)) {
        return;
    }

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Global temporary table feature is disable"),
                errhint("You might need to increase max_active_global_temporary_table "
                        "to enable this feature.")));
    }

    if (!(operation == CMD_UTILITY || operation == CMD_INSERT)) {
        return;
    }

    if (!RELKIND_HAS_STORAGE(relation->rd_rel->relkind)) {
        return;
    }

    if (relation->rd_smgr == NULL) {
        /* Open it at the smgr level if not already done */
        RelationOpenSmgr(relation);
    }

    if (gtt_storage_attached(RelationGetRelid(relation))) {
        return;
    }

    CreateGTTRelFiles(resultRelInfo);
}

static void gtt_reset_statistics(gtt_local_hash_entry* entry)
{
    int i;

    for (i = 0; i < entry->natts; i++) {
        if (entry->att_stat_tups[i]) {
            heap_freetuple(entry->att_stat_tups[i]);
            entry->att_stat_tups[i] = NULL;
        }

        entry->attnum[i] = 0;
    }

    return;
}

static void gtt_free_statistics(gtt_local_hash_entry* entry)
{
    int i;

    for (i = 0; i < entry->natts; i++) {
        if (entry->att_stat_tups[i]) {
            heap_freetuple(entry->att_stat_tups[i]);
            entry->att_stat_tups[i] = NULL;
        }
    }

    if (entry->attnum)
        pfree(entry->attnum);

    if (entry->att_stat_tups)
        pfree(entry->att_stat_tups);

    return;
}

Oid gtt_fetch_current_relfilenode(Oid relid)
{
    gtt_local_hash_entry* entry;
    gtt_relfilenode* gttRnode = NULL;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0) {
        return InvalidOid;
    }

    entry = gtt_search_by_relid(relid, true);
    if (entry == NULL) {
        return InvalidOid;
    }

    Assert(entry->relid == relid);

    gttRnode = (gtt_relfilenode*)lfirst(list_tail(entry->relfilenode_list));
    if (gttRnode == NULL) {
        return InvalidOid;
    }

    return gttRnode->relfilenode;
}

void gtt_switch_rel_relfilenode(Oid rel1, Oid relfilenode1, Oid rel2, Oid relfilenode2, bool footprint)
{
    gtt_local_hash_entry* entry1;
    gtt_local_hash_entry* entry2;
    gtt_relfilenode* gttRnode1 = NULL;
    gtt_relfilenode* gttRnode2 = NULL;
    MemoryContext oldcontext;

    if (u_sess->attr.attr_storage.max_active_gtt <= 0)
        return;

    if (u_sess->gtt_ctx.gtt_storage_local_hash == NULL)
        return;

    entry1 = gtt_search_by_relid(rel1, false);
    Assert(entry1 != NULL);
    gttRnode1 = gtt_search_relfilenode(entry1, relfilenode1, false);

    entry2 = gtt_search_by_relid(rel2, false);
    Assert(entry2 != NULL);
    gttRnode2 = gtt_search_relfilenode(entry2, relfilenode2, false);

    oldcontext = MemoryContextSwitchTo(u_sess->gtt_ctx.gtt_relstats_context);
    entry1->relfilenode_list = list_delete_ptr(entry1->relfilenode_list, gttRnode1);
    entry2->relfilenode_list = lappend(entry2->relfilenode_list, gttRnode1);

    entry2->relfilenode_list = list_delete_ptr(entry2->relfilenode_list, gttRnode2);
    entry1->relfilenode_list = lappend(entry1->relfilenode_list, gttRnode2);
    (void)MemoryContextSwitchTo(oldcontext);

    if (footprint) {
        entry1->oldrelid = rel2;
        entry2->oldrelid = rel1;
    }

    return;
}

static gtt_relfilenode* gtt_search_relfilenode(const gtt_local_hash_entry* entry, Oid relfilenode, bool missingOk)
{
    gtt_relfilenode* rnode = NULL;
    ListCell* lc;

    Assert(entry);

    foreach (lc, entry->relfilenode_list) {
        gtt_relfilenode* gtt_rnode = (gtt_relfilenode*)lfirst(lc);
        if (gtt_rnode->relfilenode == relfilenode) {
            rnode = gtt_rnode;
            break;
        }
    }

    if (!missingOk && rnode == NULL) {
        elog(ERROR, "find relfilenode %u relfilenodelist from relid %u fail", relfilenode, entry->relid);
    }

    return rnode;
}

static gtt_local_hash_entry* gtt_search_by_relid(Oid relid, bool missingOk)
{
    gtt_local_hash_entry* entry = NULL;

    if (u_sess->gtt_ctx.gtt_storage_local_hash == NULL) {
        return NULL;
    }

    entry =
        (gtt_local_hash_entry*)hash_search(u_sess->gtt_ctx.gtt_storage_local_hash, (void*)&(relid), HASH_FIND, NULL);

    if (entry == NULL && !missingOk) {
        elog(ERROR, "relid %u not found in local hash", relid);
    }

    return entry;
}

void gtt_create_storage_files(Oid relid)
{
    if (gtt_storage_attached(relid)) {
        return;
    }

    MemoryContext ctxAlterGtt =
        AllocSetContextCreate(CurrentMemoryContext, "gtt alter table", ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldcontext = MemoryContextSwitchTo(ctxAlterGtt);
    ResultRelInfo* resultRelInfo = makeNode(ResultRelInfo);
    Relation rel = relation_open(relid, NoLock);

    InitResultRelInfo(resultRelInfo, rel, 1, 0);
    if (resultRelInfo->ri_RelationDesc->rd_rel->relhasindex &&
        resultRelInfo->ri_IndexRelationDescs == NULL) {
        ExecOpenIndices(resultRelInfo, false);
    }
    init_gtt_storage(CMD_UTILITY, resultRelInfo);
    relation_close(rel, NoLock);
    ExecCloseIndices(resultRelInfo);
    (void)MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(ctxAlterGtt);
}


void CheckGttTableInUse(Relation rel)
{
    /* We allow to alter/drop global temp table/index only this session use it */
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        Oid relid = (rel->rd_rel->relkind == RELKIND_INDEX) ?
            rel->rd_index->indrelid :
            rel->rd_id;
        if (is_other_backend_use_gtt(relid)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("can not alter/drop table %s when other backend attached this global temp table",
                        get_rel_name(relid))));
        }
    }
}

