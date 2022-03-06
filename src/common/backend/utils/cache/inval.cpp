/* -------------------------------------------------------------------------
 *
 * inval.c
 *	  openGauss cache invalidation dispatcher code.
 *
 *	This is subtle stuff, so pay attention:
 *
 *	When a tuple is updated or deleted, our standard time qualification rules
 *	consider that it is *still valid* so long as we are in the same command,
 *	ie, until the next CommandCounterIncrement() or transaction commit.
 *	(See utils/time/tqual.c, and note that system catalogs are generally
 *	scanned under SnapshotNow rules by the system, or plain user snapshots
 *	for user queries.)	At the command boundary, the old tuple stops
 *	being valid and the new version, if any, becomes valid.  Therefore,
 *	we cannot simply flush a tuple from the system caches during heap_update()
 *	or heap_delete().  The tuple is still good at that point; what's more,
 *	even if we did flush it, it might be reloaded into the caches by a later
 *	request in the same command.  So the correct behavior is to keep a list
 *	of outdated (updated/deleted) tuples and then do the required cache
 *	flushes at the next command boundary.  We must also keep track of
 *	inserted tuples so that we can flush "negative" cache entries that match
 *	the new tuples; again, that mustn't happen until end of command.
 *
 *	Once we have finished the command, we still need to remember inserted
 *	tuples (including new versions of updated tuples), so that we can flush
 *	them from the caches if we abort the transaction.  Similarly, we'd better
 *	be able to flush "negative" cache entries that may have been loaded in
 *	place of deleted tuples, so we still need the deleted ones too.
 *
 *	If we successfully complete the transaction, we have to broadcast all
 *	these invalidation events to other backends (via the SI message queue)
 *	so that they can flush obsolete entries from their caches.	Note we have
 *	to record the transaction commit before sending SI messages, otherwise
 *	the other backends won't see our updated tuples as good.
 *
 *	When a subtransaction aborts, we can process and discard any events
 *	it has queued.	When a subtransaction commits, we just add its events
 *	to the pending lists of the parent transaction.
 *
 *	In short, we need to remember until xact end every insert or delete
 *	of a tuple that might be in the system caches.	Updates are treated as
 *	two events, delete + insert, for simplicity.  (If the update doesn't
 *	change the tuple hash value, catcache.c optimizes this into one event.)
 *
 *	We do not need to register EVERY tuple operation in this way, just those
 *	on tuples in relations that have associated catcaches.	We do, however,
 *	have to register every operation on every tuple that *could* be in a
 *	catcache, whether or not it currently is in our cache.	Also, if the
 *	tuple is in a relation that has multiple catcaches, we need to register
 *	an invalidation message for each such catcache.  catcache.c's
 *	PrepareToInvalidateCacheTuple() routine provides the knowledge of which
 *	catcaches may need invalidation for a given tuple.
 *
 *	Also, whenever we see an operation on a pg_class or pg_attribute tuple,
 *	we register a relcache flush operation for the relation described by that
 *	tuple.
 *
 *	We keep the relcache flush requests in lists separate from the catcache
 *	tuple flush requests.  This allows us to issue all the pending catcache
 *	flushes before we issue relcache flushes, which saves us from loading
 *	a catcache tuple during relcache load only to flush it again right away.
 *	Also, we avoid queuing multiple relcache flush requests for the same
 *	relation, since a relcache flush is relatively expensive to do.
 *	(XXX is it worth testing likewise for duplicate catcache flush entries?
 *	Probably not.)
 *
 *	If a relcache flush is issued for a system relation that we preload
 *	from the relcache init file, we must also delete the init file so that
 *	it will be rebuilt during the next backend restart.  The actual work of
 *	manipulating the init file is in relcache.c, but we keep track of the
 *	need for it here.
 *
 *	The request lists proper are kept in t_thrd.mem_cxt.cur_transaction_mem_cxt of their
 *	creating (sub)transaction, since they can be forgotten on abort of that
 *	transaction but must be kept till top-level commit otherwise.  For
 *	simplicity we keep the controlling list-of-lists in u_sess->top_transaction_mem_cxt.
 *
 *	Currently, inval messages are sent without regard for the possibility
 *	that the object described by the catalog tuple might be a session-local
 *	object such as a temporary table.  This is because (1) this code has
 *	no practical way to tell the difference, and (2) it is not certain that
 *	other backends don't have catalog cache or even relcache entries for
 *	such tables, anyway; there is nothing that prevents that.  It might be
 *	worth trying to avoid sending such inval traffic in the future, if those
 *	problems can be overcome cheaply.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/cache/inval.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_proc.h"
#include "commands/prepare.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/sinval.h"
#include "storage/smgr/smgr.h"
#include "utils/inval.h"
#include "utils/globalplancache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/plpgsql.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "catalog/pgxc_class.h"

/*
 * To minimize palloc traffic, we keep pending requests in successively-
 * larger chunks (a slightly more sophisticated version of an expansible
 * array).	All request types can be stored as SharedInvalidationMessage
 * records.  The ordering of requests within a list is never significant.
 */
typedef struct InvalidationChunk {
    struct InvalidationChunk* next;    /* list link */
    int nitems;                        /* # items currently stored in chunk */
    int maxitems;                      /* size of allocated array in this chunk */
    SharedInvalidationMessage msgs[1]; /* VARIABLE LENGTH ARRAY */
} InvalidationChunk;                   /* VARIABLE LENGTH STRUCTURE */

typedef struct InvalidationListHeader {
    InvalidationChunk* cclist; /* list of chunks holding catcache msgs */
    InvalidationChunk* rclist; /* list of chunks holding relcache msgs */
    InvalidationChunk* pclist; /* list of chunks holding partcache msgs */
} InvalidationListHeader;

/*
 * Invalidation info is divided into two lists:
 *	1) events so far in current command, not yet reflected to caches.
 *	2) events in previous commands of current transaction; these have
 *	   been reflected to local caches, and must be either broadcast to
 *	   other backends or rolled back from local cache when we commit
 *	   or abort the transaction.
 * Actually, we need two such lists for each level of nested transaction,
 * so that we can discard events from an aborted subtransaction.  When
 * a subtransaction commits, we append its lists to the parent's lists.
 *
 * The relcache-file-invalidated flag can just be a simple boolean,
 * since we only act on it at transaction commit; we don't care which
 * command of the transaction set it.
 */

typedef struct TransInvalidationInfo {
    /* Back link to parent transaction's info */
    struct TransInvalidationInfo* parent;

    /* Subtransaction nesting depth */
    int my_level;

    /* head of current-command event list */
    InvalidationListHeader CurrentCmdInvalidMsgs;

    /* head of previous-commands event list */
    InvalidationListHeader PriorCmdInvalidMsgs;

    /* init file must be invalidated? */
    bool RelcacheInitFileInval;
} TransInvalidationInfo;

/* ----------------------------------------------------------------
 *				Invalidation list support functions
 *
 * These three routines encapsulate processing of the "chunked"
 * representation of what is logically just a list of messages.
 * ----------------------------------------------------------------
 */

/*
 * AddInvalidationMessage
 *		Add an invalidation message to a list (of chunks).
 *
 * Note that we do not pay any great attention to maintaining the original
 * ordering of the messages.
 */
static void AddInvalidationMessage(InvalidationChunk** listHdr, SharedInvalidationMessage* msg)
{
    InvalidationChunk* chunk = *listHdr;

    if (chunk == NULL) {
        /* First time through; create initial chunk */
#define FIRSTCHUNKSIZE 32
        chunk = (InvalidationChunk*)MemoryContextAlloc(t_thrd.mem_cxt.cur_transaction_mem_cxt,
            sizeof(InvalidationChunk) + (FIRSTCHUNKSIZE - 1) * sizeof(SharedInvalidationMessage));
        chunk->nitems = 0;
        chunk->maxitems = FIRSTCHUNKSIZE;
        chunk->next = *listHdr;
        *listHdr = chunk;
    } else if (chunk->nitems >= chunk->maxitems) {
        /* Need another chunk; double size of last chunk */
        int chunksize = 2 * chunk->maxitems;

        chunk = (InvalidationChunk*)MemoryContextAlloc(t_thrd.mem_cxt.cur_transaction_mem_cxt,
            sizeof(InvalidationChunk) + (chunksize - 1) * sizeof(SharedInvalidationMessage));
        chunk->nitems = 0;
        chunk->maxitems = chunksize;
        chunk->next = *listHdr;
        *listHdr = chunk;
    }
    /* Okay, add message to current chunk */
    chunk->msgs[chunk->nitems] = *msg;
    chunk->nitems++;
}

/*
 * Append one list of invalidation message chunks to another, resetting
 * the source chunk-list pointer to NULL.
 */
static void AppendInvalidationMessageList(InvalidationChunk** destHdr, InvalidationChunk** srcHdr)
{
    InvalidationChunk* chunk = *srcHdr;

    if (chunk == NULL) {
        return; /* nothing to do */
    }

    while (chunk->next != NULL) {
        chunk = chunk->next;
    }

    chunk->next = *destHdr;

    *destHdr = *srcHdr;

    *srcHdr = NULL;
}

/*
 * Process a list of invalidation messages.
 *
 * This is a macro that executes the given code fragment for each message in
 * a message chunk list.  The fragment should refer to the message as *msg.
 */
#define ProcessMessageList(listHdr, codeFragment)                         \
    do {                                                                  \
        InvalidationChunk* _chunk = NULL;                                 \
        for (_chunk = (listHdr); _chunk != NULL; _chunk = _chunk->next) { \
            int _cindex;                                                  \
            for (_cindex = 0; _cindex < _chunk->nitems; _cindex++) {      \
                SharedInvalidationMessage* msg = &_chunk->msgs[_cindex];  \
                codeFragment;                                             \
            }                                                             \
        }                                                                 \
    } while (0)

/*
 * Process a list of invalidation messages group-wise.
 *
 * As above, but the code fragment can handle an array of messages.
 * The fragment should refer to the messages as msgs[], with n entries.
 */
#define ProcessMessageListMulti(listHdr, codeFragment)                    \
    do {                                                                  \
        InvalidationChunk* _chunk = NULL;                                 \
        for (_chunk = (listHdr); _chunk != NULL; _chunk = _chunk->next) { \
            SharedInvalidationMessage* msgs = _chunk->msgs;               \
            int n = _chunk->nitems;                                       \
            codeFragment;                                                 \
        }                                                                 \
    } while (0)

/* ----------------------------------------------------------------
 *				Invalidation set support functions
 *
 * These routines understand about the division of a logical invalidation
 * list into separate physical lists for catcache and relcache entries.
 * ----------------------------------------------------------------
 */

/*
 * Add a catcache inval entry
 */
static void AddCatcacheInvalidationMessage(InvalidationListHeader* hdr, int id, uint32 hashValue, Oid dbId)
{
    SharedInvalidationMessage msg;

    Assert(id < CHAR_MAX);
    msg.cc.id = (int8)id;
    msg.cc.dbId = dbId;
    msg.cc.hashValue = hashValue;
    AddInvalidationMessage(&hdr->cclist, &msg);
}

/*
 * Add a whole-catalog inval entry
 */
static void AddCatalogInvalidationMessage(InvalidationListHeader* hdr, Oid dbId, Oid catId)
{
    SharedInvalidationMessage msg;

    msg.cat.id = SHAREDINVALCATALOG_ID;
    msg.cat.dbId = dbId;
    msg.cat.catId = catId;
    AddInvalidationMessage(&hdr->cclist, &msg);
}

/*
 * Add a relcache inval entry
 */
static void AddRelcacheInvalidationMessage(InvalidationListHeader* hdr, Oid dbId, Oid relId)
{
    SharedInvalidationMessage msg;

    /* Don't add a duplicate item */
    /* We assume dbId need not be checked because it will never change */
    ProcessMessageList(hdr->rclist, if (msg->rc.id == SHAREDINVALRELCACHE_ID &&
        (msg->rc.relId == relId || msg->rc.relId == InvalidOid)) return);

    /* OK, add the item */
    msg.rc.id = SHAREDINVALRELCACHE_ID;
    msg.rc.dbId = dbId;
    msg.rc.relId = relId;
    AddInvalidationMessage(&hdr->rclist, &msg);
}
static void AddPartcacheInvalidationMessage(InvalidationListHeader* hdr, Oid dbId, Oid partId)
{
    SharedInvalidationMessage msg;

    /* Don't add a duplicate item */
    /* We assume dbId need not be checked because it will never change */
    ProcessMessageList(hdr->pclist, if (msg->pc.id == SHAREDINVALPARTCACHE_ID && msg->pc.partId == partId) return );

    /* OK, add the item */
    msg.pc.id = SHAREDINVALPARTCACHE_ID;
    msg.pc.dbId = dbId;
    msg.pc.partId = partId;
    AddInvalidationMessage(&hdr->pclist, &msg);
}

/* function invalid and it's plan cache is invalid, need be removed */
static void AddFunctionCacheInvalidationMessage(InvalidationListHeader* hdr, Oid dbId, Oid funcOid, Oid pkgOid)
{
    SharedInvalidationMessage msg;
    int cacheId = PACKAGEOID;
    Oid objId = InvalidOid;
    if (pkgOid == InvalidOid) {
        cacheId = PROCOID;
        objId = funcOid;
    } else {
        cacheId = PACKAGEOID;
        objId = pkgOid;
    }

    /* Don't add a duplicate item */
    /* We assume dbId need not be checked because it will never change */
    ProcessMessageList(hdr->rclist,
        if (msg->fm.id == SHAREDINVALFUNC_ID && msg->fm.cacheId == cacheId && msg->fm.objId == objId) return );

    /* OK, add the item */
    msg.fm.id = SHAREDINVALFUNC_ID;
    msg.fm.dbId = dbId;
    msg.fm.cacheId = cacheId;
    msg.fm.objId = objId;
    AddInvalidationMessage(&hdr->rclist, &msg);
}

/*
 * Append one list of invalidation messages to another, resetting
 * the source list to empty.
 */
static void AppendInvalidationMessages(InvalidationListHeader* dest, InvalidationListHeader* src)
{
    AppendInvalidationMessageList(&dest->cclist, &src->cclist);
    AppendInvalidationMessageList(&dest->rclist, &src->rclist);
    AppendInvalidationMessageList(&dest->pclist, &src->pclist);
}

/*
 * Execute the given function for all the messages in an invalidation list.
 * The list is not altered.
 *
 * catcache entries are processed first, for reasons mentioned above.
 */
static void ProcessInvalidationMessages(InvalidationListHeader* hdr, void (*func)(SharedInvalidationMessage* msg))
{
    ProcessMessageList(hdr->cclist, func(msg));
    ProcessMessageList(hdr->rclist, func(msg));
    ProcessMessageList(hdr->pclist, func(msg));
}

/*
 * As above, but the function is able to process an array of messages
 * rather than just one at a time.
 */
static void ProcessInvalidationMessagesMulti(
    InvalidationListHeader* hdr, void (*func)(const SharedInvalidationMessage* msgs, int n))
{
    ProcessMessageListMulti(hdr->cclist, func(msgs, n));
    ProcessMessageListMulti(hdr->rclist, func(msgs, n));
    ProcessMessageListMulti(hdr->pclist, func(msgs, n));
}

/* ----------------------------------------------------------------
 *					  private support functions
 * ----------------------------------------------------------------
 */

/*
 * RegisterCatcacheInvalidation
 *
 * Register an invalidation event for a catcache tuple entry.
 */
static void RegisterCatcacheInvalidation(int cacheId, uint32 hashValue, Oid dbId)
{
    AddCatcacheInvalidationMessage(&GetInvalCxt()->transInvalInfo->CurrentCmdInvalidMsgs, cacheId, hashValue, dbId);
}

/*
 * RegisterCatalogInvalidation
 *
 * Register an invalidation event for all catcache entries from a catalog.
 */
static void RegisterCatalogInvalidation(Oid dbId, Oid catId)
{
    AddCatalogInvalidationMessage(&GetInvalCxt()->transInvalInfo->CurrentCmdInvalidMsgs, dbId, catId);
}

/*
 * RegisterRelcacheInvalidation
 *
 * As above, but register a relcache invalidation event.
 */
static void RegisterRelcacheInvalidation(Oid dbId, Oid relId)
{
    AddRelcacheInvalidationMessage(&GetInvalCxt()->transInvalInfo->CurrentCmdInvalidMsgs, dbId, relId);

    /*
     * Most of the time, relcache invalidation is associated with system
     * catalog updates, but there are a few cases where it isn't.  Quick hack
     * to ensure that the next CommandCounterIncrement() will think that we
     * need to do CommandEndInvalidationMessages().
     */
    (void)GetCurrentCommandId(true);

    /*
     * If the relation being invalidated is one of those cached in the
     * relcache init file, mark that we need to zap that file at commit.
     *
     * During inplace or online upgrade, we rely on the init file to build
     * catalogs' relcache into pre-upgrade version and certainly we cannot zap it.
     */
    if (RelationIdIsInInitFile(relId)) {
        u_sess->inval_cxt.transInvalInfo->RelcacheInitFileInval = true;
    }
}

static void RegisterPartcacheInvalidation(Oid dbId, Oid partId)
{
    AddPartcacheInvalidationMessage(&GetInvalCxt()->transInvalInfo->CurrentCmdInvalidMsgs, dbId, partId);

    /*
     * Most of the time, relcache invalidation is associated with system
     * catalog updates, but there are a few cases where it isn't.  Quick hack
     * to ensure that the next CommandCounterIncrement() will think that we
     * need to do CommandEndInvalidationMessages().
     */
    (void)GetCurrentCommandId(true);

    /*
     * If the relation being invalidated is one of those cached in the
     * relcache init file, mark that we need to zap that file at commit.
     */
}

/*
 * SendCatcacheInvalidation
 *
 * Send an invalidation event for a catcache tuple entry.
 * Normally, catcache invalidation messages should be sent out at
 * transaction commit. However, we have some corner cases where
 * MVCC and transactional safety are not desired and we want the
 * catalog update to be visible among sessions immediately.
 *
 * At present, this is used with heap_inplace_update.
 */
static void SendCatcacheInvalidation(int cacheId, uint32 hashValue, Oid dbId)
{
    SharedInvalidationMessage msg;

    Assert(cacheId < CHAR_MAX);
    msg.cc.id = (int8)cacheId;
    msg.cc.dbId = dbId;
    msg.cc.hashValue = hashValue;
    SendSharedInvalidMessages(&msg, 1);
}

void LocalExecuteThreadAndSessionInvalidationMessage(SharedInvalidationMessage* msg)
{
    LocalExecuteThreadInvalidationMessage(msg);
    LocalExecuteSessionInvalidationMessage(msg);
}

static void ThreadInvalidCatalog(SharedInvalidationMessage* msg)
{
    if (msg->cat.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->cat.dbId == InvalidOid) {
        CatalogCacheFlushCatalog(msg->cat.catId);
        /* CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed */
    }
}
static void ThreadInvalidRelCache(SharedInvalidationMessage* msg)
{
    if (msg->rc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->rc.dbId == InvalidOid) {
        RelationCacheInvalidateEntry(msg->rc.relId);
        knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
        for (int i = 0; i < inval_cxt->relcache_callback_count; i++) {
            struct RELCACHECALLBACK* ccitem = inval_cxt->relcache_callback_list + i;
            (*ccitem->function)(ccitem->arg, msg->rc.relId);
        }
    }
}
static void ThreadInvalidSmgr(SharedInvalidationMessage* msg)
{
    /*
     * We could have smgr entries for relations of other databases, so no
     * short-circuit test is possible here.
     */
    RelFileNodeBackend rnode;
    RelFileNodeCopy(rnode.node, msg->sm.rnode, InvalidBktId);
    rnode.backend = (msg->sm.backend_hi << 16) | (int)msg->sm.backend_lo;
    smgrclosenode(rnode);
}
static void ThreadInvalidHbktSmgr(SharedInvalidationMessage* msg)
{
    RelFileNodeBackend rnode;
    /* Hash bucket table is always regular relations */
    rnode.backend = InvalidBackendId;
    RelFileNodeCopy(rnode.node, msg->hbksm.rnode, (int) msg->hbksm.bucketId);
    smgrclosenode(rnode);
}
static void ThreadInvalidRelmap(SharedInvalidationMessage* msg)
{
    /* We only care about our own database and shared catalogs */
    if (msg->rm.dbId == InvalidOid) {
        RelationMapInvalidate(true);
    } else if (msg->rm.dbId == t_thrd.lsc_cxt.lsc->my_database_id) {
        RelationMapInvalidate(false);
    }
}
static void ThreadInvalidPartCache(SharedInvalidationMessage* msg)
{
    if (msg->pc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->pc.dbId == InvalidOid) {
        PartitionCacheInvalidateEntry(msg->pc.partId);
        knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
        for (int i = 0; i < inval_cxt->partcache_callback_count; i++) {
            struct PARTCACHECALLBACK* ccitem = inval_cxt->partcache_callback_list + i;
            (*ccitem->function)(ccitem->arg, msg->pc.partId);
        }
    }
}
static void ThreadInvalidCatCache(SharedInvalidationMessage* msg)
{
    if (msg->cc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->cc.dbId == InvalidOid) {
        CatalogCacheIdInvalidate(msg->cc.id, msg->cc.hashValue);
        CallThreadSyscacheCallbacks(msg->cc.id, msg->cc.hashValue);
    }
}
/*
 * LocalExecuteThreadInvalidationMessage
 *
 * Process a single invalidation message (which could be of any type).
 * Only the local caches are flushed; this does not transmit the message
 * to other backends.
 */
void LocalExecuteThreadInvalidationMessage(SharedInvalidationMessage* msg)
{
    if (!EnableLocalSysCache()) {
        return;
    }
    Assert(CheckMyDatabaseMatch());
    switch (msg->id) {
        case SHAREDINVALCATALOG_ID: { /* reset system table */
            ThreadInvalidCatalog(msg);
            break;
        }
        case SHAREDINVALRELCACHE_ID: { /* invalid table and call callbackfunc registered on the table */
            ThreadInvalidRelCache(msg);
            break;
        }
        case SHAREDINVALSMGR_ID: { /* invalid smgr struct, dont care which db it belongs to */
            ThreadInvalidSmgr(msg);
            break;
        }
        case SHAREDINVALHBKTSMGR_ID: { /* invalid smgr struct, dont care which db it belongs to */
            ThreadInvalidHbktSmgr(msg);
            break;
        }
        case SHAREDINVALRELMAP_ID: { /* invalid relmap cxt */
            ThreadInvalidRelmap(msg);
            break;
        }
        case SHAREDINVALPARTCACHE_ID: { /* invalid partcache and call callbackfunc registered on the part */
            ThreadInvalidPartCache(msg);
            break;
        }
        case SHAREDINVALFUNC_ID: { /* func hashtable no in thread, so invalid it by session inval msg */
            break;
        }
        default:{
            if (msg->id >= 0) { /* invalid catcache, most cases are ddls on rel */
                ThreadInvalidCatCache(msg);
            } else {
            ereport(FATAL, (errmsg("unrecognized SI message ID: %d", msg->id)));
            }
        }
    }
}

/*
 *
 *
 * Process a single invalidation message (which could be of any type).
 * Only the local caches are flushed; this does not transmit the message
 * to other backends.
 */
static void LocalExecuteInvalidationMessage(SharedInvalidationMessage* msg)
{
    if (msg->id >= 0) {
        if (msg->cc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->cc.dbId == InvalidOid) {
            CatalogCacheIdInvalidate(msg->cc.id, msg->cc.hashValue);

            CallSessionSyscacheCallbacks(msg->cc.id, msg->cc.hashValue);
        }
    } else if (msg->id == SHAREDINVALCATALOG_ID) {
        if (msg->cat.dbId == u_sess->proc_cxt.MyDatabaseId || msg->cat.dbId == InvalidOid) {
            CatalogCacheFlushCatalog(msg->cat.catId);

            /* CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed */
        }
    } else if (msg->id == SHAREDINVALRELCACHE_ID) {
        if (msg->rc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->rc.dbId == InvalidOid) {
            int i;

            RelationCacheInvalidateEntry(msg->rc.relId);

            for (i = 0; i < u_sess->inval_cxt.relcache_callback_count; i++) {
                struct RELCACHECALLBACK* ccitem = u_sess->inval_cxt.relcache_callback_list + i;

                (*ccitem->function)(ccitem->arg, msg->rc.relId);
            }
        }
    } else if (msg->id == SHAREDINVALSMGR_ID) {
        /*
         * We could have smgr entries for relations of other databases, so no
         * short-circuit test is possible here.
         */
        RelFileNodeBackend rnode;
        RelFileNodeCopy(rnode.node, msg->sm.rnode, InvalidBktId);
        rnode.backend = (msg->sm.backend_hi << 16) | (int)msg->sm.backend_lo;
        smgrclosenode(rnode);
    } else if (msg->id == SHAREDINVALHBKTSMGR_ID) {
        RelFileNodeBackend rnode;

        /* Hash bucket table is always regular relations  */
        rnode.backend = InvalidBackendId;
        RelFileNodeCopy(rnode.node, msg->hbksm.rnode, (int) msg->hbksm.bucketId);
        smgrclosenode(rnode);
    } else if (msg->id == SHAREDINVALRELMAP_ID) {
        /* We only care about our own database and shared catalogs */
        if (msg->rm.dbId == InvalidOid) {
            RelationMapInvalidate(true);
        } else if (msg->rm.dbId == u_sess->proc_cxt.MyDatabaseId) {
            RelationMapInvalidate(false);
        }
    } else if (msg->id == SHAREDINVALPARTCACHE_ID) {
        if (msg->pc.dbId == u_sess->proc_cxt.MyDatabaseId || msg->pc.dbId == InvalidOid) {
            int i;

            PartitionCacheInvalidateEntry(msg->pc.partId);

            for (i = 0; i < u_sess->inval_cxt.partcache_callback_count; i++) {
                struct PARTCACHECALLBACK* ccitem = u_sess->inval_cxt.partcache_callback_list + i;

                (*ccitem->function)(ccitem->arg, msg->pc.partId);
            }
        }
    } else if (msg->id == SHAREDINVALFUNC_ID) {
        if (msg->fm.dbId == u_sess->proc_cxt.MyDatabaseId || msg->fm.dbId == InvalidOid) {
            plpgsql_hashtable_delete_and_check_invalid_item(msg->fm.cacheId, msg->fm.objId);
        }
    } else {
        ereport(FATAL, (errmsg("unrecognized SI message ID: %d", msg->id)));
    }

    if (ENABLE_GPC) {
        bool check = GlobalPlanCache::MsgCheck(msg);
#ifdef ENABLE_MULTIPLE_NODES
        if (check == true && u_sess->pcache_cxt.gpc_remote_msg == false) {
#else
        if (check == true && (u_sess->pcache_cxt.gpc_remote_msg == false || pmState == PM_HOT_STANDBY)) {
#endif
            u_sess->pcache_cxt.gpc_in_ddl = true;
        }
    }
}

void LocalExecuteSessionInvalidationMessage(SharedInvalidationMessage* msg)
{
    if (!EnableLocalSysCache()) {
        LocalExecuteInvalidationMessage(msg);
        return;
    }
    Assert(CheckMyDatabaseMatch());
    if (msg->id >= 0) {
        if (msg->cc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->cc.dbId == InvalidOid) {
            CallSessionSyscacheCallbacks(msg->cc.id, msg->cc.hashValue);
        }
    } else if (msg->id == SHAREDINVALCATALOG_ID) {
        if (msg->cat.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->cat.dbId == InvalidOid) {
            t_thrd.lsc_cxt.lsc->systabcache.SessionCatCacheCallBack(msg->cat.catId);
            /* CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed */
        }
    } else if (msg->id == SHAREDINVALRELCACHE_ID) {
        if (msg->rc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->rc.dbId == InvalidOid) {
            knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
            for (int i = 0; i < inval_cxt->relcache_callback_count; i++) {
                struct RELCACHECALLBACK* ccitem = inval_cxt->relcache_callback_list + i;
                (*ccitem->function)(ccitem->arg, msg->rc.relId);
            }
        }
    } else if (msg->id == SHAREDINVALSMGR_ID) {
        /* on GSC mode, smgrcache is in thread memcxt */
    } else if (msg->id == SHAREDINVALHBKTSMGR_ID) {
        /* on GSC mode, smgrcache is in thread memcxt */
    } else if (msg->id == SHAREDINVALRELMAP_ID) {
        /* on GSC mode, relmap is in thread memcxt */
    } else if (msg->id == SHAREDINVALPARTCACHE_ID) {
        if (msg->pc.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->pc.dbId == InvalidOid) {
            knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
            for (int i = 0; i < inval_cxt->partcache_callback_count; i++) {
                struct PARTCACHECALLBACK* ccitem = inval_cxt->partcache_callback_list + i;
                (*ccitem->function)(ccitem->arg, msg->pc.partId);
            }
        }
    } else if (msg->id == SHAREDINVALFUNC_ID) {
        if (msg->fm.dbId == t_thrd.lsc_cxt.lsc->my_database_id || msg->fm.dbId == InvalidOid) {
            plpgsql_hashtable_delete_and_check_invalid_item(msg->fm.cacheId, msg->fm.objId);
        }
    } else {
        ereport(FATAL, (errmsg("unrecognized SI message ID: %d", msg->id)));
    }

    if (ENABLE_GPC) {
        bool check = GlobalPlanCache::MsgCheck(msg);
        if (check == true && u_sess->pcache_cxt.gpc_remote_msg == false) {
            u_sess->pcache_cxt.gpc_in_ddl = true;
        }
    }
}

/*
 *		InvalidateSystemCaches
 *
 *		This blows away all tuples in the system catalog caches and
 *		all the cached relation descriptors and smgr cache entries.
 *		Relation descriptors that have positive refcounts are then rebuilt.
 *
 *		We call this when we see a shared-inval-queue overflow signal,
 *		since that tells us we've lost some shared-inval messages and hence
 *		don't know what needs to be invalidated.
 */
void InvalidateSystemCaches(void)
{
    InvalidateThreadSystemCaches();
    InvalidateSessionSystemCaches();
}

void InvalidateSessionSystemCaches(void)
{
    if (!EnableLocalSysCache()) {
        ResetCatalogCaches();
        RelationCacheInvalidate();
        PartitionCacheInvalidate();
    }
    int i;
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (i = 0; i < inval_cxt->syscache_callback_count; i++) {
        struct SYSCACHECALLBACK* ccitem = inval_cxt->syscache_callback_list + i;
        (*ccitem->function)(ccitem->arg, ccitem->id, 0);
    }

    for (i = 0; i < inval_cxt->relcache_callback_count; i++) {
        struct RELCACHECALLBACK* ccitem = inval_cxt->relcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }

    for (i = 0; i < inval_cxt->partcache_callback_count; i++) {
        struct PARTCACHECALLBACK* ccitem = inval_cxt->partcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }

    /* Add debug log for system caches queue */
    if (!RecoveryInProgress() && u_sess->attr.attr_storage.enable_debug_vacuum) {
        ereport(LOG, (errmsg("Invalidate system caches because shared-inval-queue reach threshold")));
    }
}

void InvalidateThreadSystemCaches(void)
{
    if (!EnableLocalSysCache()) {
        return;
    }
    int i;
    ResetCatalogCaches();
    RelationCacheInvalidate(); /* gets smgr and relmap too */
    PartitionCacheInvalidate();
    knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    for (i = 0; i < inval_cxt->syscache_callback_count; i++) {
        struct SYSCACHECALLBACK* ccitem = inval_cxt->syscache_callback_list + i;
        (*ccitem->function)(ccitem->arg, ccitem->id, 0);
    }

    for (i = 0; i < inval_cxt->relcache_callback_count; i++) {
        struct RELCACHECALLBACK* ccitem = inval_cxt->relcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }

    for (i = 0; i < inval_cxt->partcache_callback_count; i++) {
        struct PARTCACHECALLBACK* ccitem = inval_cxt->partcache_callback_list + i;
        (*ccitem->function)(ccitem->arg, InvalidOid);
    }
}

static void TestCodeToForceCacheFlushes()
{
    /*
     * Test code to force cache flushes anytime a flush could happen.
     *
     * If used with CLOBBER_FREED_MEMORY, CLOBBER_CACHE_ALWAYS provides a
     * fairly thorough test that the system contains no cache-flush hazards.
     * However, it also makes the system unbelievably slow --- the regression
     * tests take about 100 times longer than normal.
     *
     * If you're a glutton for punishment, try CLOBBER_CACHE_RECURSIVELY. This
     * slows things by at least a factor of 10000, so I wouldn't suggest
     * trying to run the entire regression tests that way.	It's useful to try
     * a few simple tests, to make sure that cache reload isn't subject to
     * internal cache-flush hazards, but after you've done a few thousand
     * recursive reloads it's unlikely you'll learn more.
     */
#if defined(CLOBBER_CACHE_ALWAYS)
    {
        static bool in_recursion = false;

        if (!in_recursion) {
            in_recursion = true;
            InvalidateSystemCaches();
            in_recursion = false;
        }
    }
#elif defined(CLOBBER_CACHE_RECURSIVELY)
    InvalidateSystemCaches();
#endif
}

/*
 * AcceptInvalidationMessages
 *		Read and process invalidation messages from the shared invalidation
 *		message queue.
 *
 * Note:
 *		This should be called as the first step in processing a transaction.
 */
void AcceptInvalidationMessages()
{
    if (EnableLocalSysCache()) {
        u_sess->pcache_cxt.gpc_remote_msg = true;
        knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
        ++inval_cxt->DeepthInAcceptInvalidationMessage;
        if (!IS_THREAD_POOL_WORKER) {
            ReceiveSharedInvalidMessages(LocalExecuteThreadAndSessionInvalidationMessage,
                InvalidateSystemCaches, false);
        } else {
            ReceiveSharedInvalidMessages(LocalExecuteThreadInvalidationMessage, InvalidateThreadSystemCaches, false);
            u_sess->pcache_cxt.gpc_remote_msg = false;
            TestCodeToForceCacheFlushes();
            --inval_cxt->DeepthInAcceptInvalidationMessage;

            u_sess->pcache_cxt.gpc_remote_msg = true;
            inval_cxt = &u_sess->inval_cxt;
            ++inval_cxt->DeepthInAcceptInvalidationMessage;
            ReceiveSharedInvalidMessages(LocalExecuteSessionInvalidationMessage, InvalidateSessionSystemCaches, true);
        }
        u_sess->pcache_cxt.gpc_remote_msg = false;
        TestCodeToForceCacheFlushes();
        --inval_cxt->DeepthInAcceptInvalidationMessage;
        return;
    }

    u_sess->pcache_cxt.gpc_remote_msg = true;
    ++u_sess->inval_cxt.DeepthInAcceptInvalidationMessage;
    ReceiveSharedInvalidMessages(LocalExecuteInvalidationMessage, InvalidateSystemCaches, false);
    u_sess->pcache_cxt.gpc_remote_msg = false;
    TestCodeToForceCacheFlushes();
    --u_sess->inval_cxt.DeepthInAcceptInvalidationMessage;
}

/*
 * AtStart_Inval
 *		Initialize inval lists at start of a main transaction.
 */
void AtStart_Inval(void)
{
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    Assert(inval_cxt->transInvalInfo == NULL);
    inval_cxt->transInvalInfo =
        (TransInvalidationInfo*)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt, sizeof(TransInvalidationInfo));
    inval_cxt->transInvalInfo->my_level = GetCurrentTransactionNestLevel();
    inval_cxt->SharedInvalidMessagesArray = NULL;
    inval_cxt->numSharedInvalidMessagesArray = 0;
}

/*
 * PostPrepare_Inval
 *		Clean up after successful PREPARE.
 *
 * Here, we want to act as though the transaction aborted, so that we will
 * undo any syscache changes it made, thereby bringing us into sync with the
 * outside world, which doesn't believe the transaction committed yet.
 *
 * If the prepared transaction is later aborted, there is nothing more to
 * do; if it commits, we will receive the consequent inval messages just
 * like everyone else.
 */
void PostPrepare_Inval(void)
{
    AtEOXact_Inval(false);
}

/*
 * AtSubStart_Inval
 *		Initialize inval lists at start of a subtransaction.
 */
void AtSubStart_Inval(void)
{
    TransInvalidationInfo* myInfo = NULL;
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    Assert(inval_cxt->transInvalInfo != NULL);
    myInfo =
        (TransInvalidationInfo*)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt, sizeof(TransInvalidationInfo));
    myInfo->parent = inval_cxt->transInvalInfo;
    myInfo->my_level = GetCurrentTransactionNestLevel();
    inval_cxt->transInvalInfo = myInfo;
}

/*
 * Collect invalidation messages into GetInvalCxt()->SharedInvalidMessagesArray array.
 */
static void MakeSharedInvalidMessagesArray(const SharedInvalidationMessage* msgs, int n)
{
    /*
     * Initialise array first time through in each commit
     */
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    if (inval_cxt->SharedInvalidMessagesArray == NULL) {
        inval_cxt->maxSharedInvalidMessagesArray = FIRSTCHUNKSIZE;
        inval_cxt->numSharedInvalidMessagesArray = 0;

        /*
         * Although this is being palloc'd we don't actually free it directly.
         * We're so close to EOXact that we now we're going to lose it anyhow.
         */
        inval_cxt->SharedInvalidMessagesArray = (SharedInvalidationMessage*)palloc(
            inval_cxt->maxSharedInvalidMessagesArray * sizeof(SharedInvalidationMessage));
    }

    if ((inval_cxt->numSharedInvalidMessagesArray + n) > inval_cxt->maxSharedInvalidMessagesArray) {
        while ((inval_cxt->numSharedInvalidMessagesArray + n) > inval_cxt->maxSharedInvalidMessagesArray)
            inval_cxt->maxSharedInvalidMessagesArray *= 2;

        inval_cxt->SharedInvalidMessagesArray =
            (SharedInvalidationMessage*)repalloc(inval_cxt->SharedInvalidMessagesArray,
                inval_cxt->maxSharedInvalidMessagesArray * sizeof(SharedInvalidationMessage));
    }

    /*
     * Append the next chunk onto the array
     */
    int rc = memcpy_s(inval_cxt->SharedInvalidMessagesArray + inval_cxt->numSharedInvalidMessagesArray,
        n * sizeof(SharedInvalidationMessage),
        msgs,
        n * sizeof(SharedInvalidationMessage));
    securec_check(rc, "\0", "\0");
    inval_cxt->numSharedInvalidMessagesArray += n;
}

/*
 * xactGetCommittedInvalidationMessages() is executed by
 * RecordTransactionCommit() to add invalidation messages onto the
 * commit record. This applies only to commit message types, never to
 * abort records. Must always run before AtEOXact_Inval(), since that
 * removes the data we need to see.
 *
 * Remember that this runs before we have officially committed, so we
 * must not do anything here to change what might occur *if* we should
 * fail between here and the actual commit.
 *
 * see also xact_redo_commit() and xact_desc_commit()
 */
int xactGetCommittedInvalidationMessages(SharedInvalidationMessage** msgs, bool* RelcacheInitFileInval)
{
    MemoryContext oldcontext;
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    /* Must be at top of stack */
    Assert(inval_cxt->transInvalInfo != NULL && inval_cxt->transInvalInfo->parent == NULL);

    /*
     * Relcache init file invalidation requires processing both before and
     * after we send the SI messages.  However, we need not do anything unless
     * we committed.
     */
    *RelcacheInitFileInval = inval_cxt->transInvalInfo->RelcacheInitFileInval;

    /*
     * Walk through TransInvalidationInfo to collect all the messages into a
     * single contiguous array of invalidation messages. It must be contiguous
     * so we can copy directly into WAL message. Maintain the order that they
     * would be processed in by AtEOXact_Inval(), to ensure emulated behaviour
     * in redo is as similar as possible to original. We want the same bugs,
     * if any, not new ones.
     */
    oldcontext = MemoryContextSwitchTo(t_thrd.mem_cxt.cur_transaction_mem_cxt);

    ProcessInvalidationMessagesMulti(
        &inval_cxt->transInvalInfo->CurrentCmdInvalidMsgs, MakeSharedInvalidMessagesArray);
    ProcessInvalidationMessagesMulti(
        &inval_cxt->transInvalInfo->PriorCmdInvalidMsgs, MakeSharedInvalidMessagesArray);
    MemoryContextSwitchTo(oldcontext);

    Assert(
        !(inval_cxt->numSharedInvalidMessagesArray > 0 && inval_cxt->SharedInvalidMessagesArray == NULL));

    *msgs = inval_cxt->SharedInvalidMessagesArray;

    return inval_cxt->numSharedInvalidMessagesArray;
}

/*
 * ProcessCommittedInvalidationMessages is executed by xact_redo_commit()
 * to process invalidation messages added to commit records.
 *
 * Relcache init file invalidation requires processing both
 * before and after we send the SI messages. See AtEOXact_Inval()
 */
void ProcessCommittedInvalidationMessages(
    SharedInvalidationMessage* msgs, int nmsgs, bool RelcacheInitFileInval, Oid dbid, Oid tsid)
{
    if (nmsgs <= 0) {
        return;
    }

    ereport(trace_recovery(DEBUG4),
        (errmsg("replaying commit with %d messages%s",
            nmsgs,
            (RelcacheInitFileInval ? " and relcache file invalidation" : ""))));

    if (RelcacheInitFileInval) {
        /*
         * RelationCacheInitFilePreInvalidate requires u_sess->proc_cxt.DatabasePath to be set,
         * but we should not use SetDatabasePath during recovery, since it is
         * intended to be used only once by normal backends.  Hence, a quick
         * hack: set u_sess->proc_cxt.DatabasePath directly then unset after use.
         */
        u_sess->proc_cxt.DatabasePath = GetDatabasePath(dbid, tsid);
        ereport(
            trace_recovery(DEBUG4), (errmsg("removing relcache init file in \"%s\"", u_sess->proc_cxt.DatabasePath)));
        RelationCacheInitFilePreInvalidate();
        pfree_ext(u_sess->proc_cxt.DatabasePath);
        u_sess->proc_cxt.DatabasePath = NULL;
    }

    SendSharedInvalidMessages(msgs, nmsgs);

    if (RelcacheInitFileInval) {
        RelationCacheInitFilePostInvalidate();
    }
}

/*
 * AtEOXact_Inval
 *		Process queued-up invalidation messages at end of main transaction.
 *
 * If isCommit, we must send out the messages in our PriorCmdInvalidMsgs list
 * to the shared invalidation message queue.  Note that these will be read
 * not only by other backends, but also by our own backend at the next
 * transaction start (via AcceptInvalidationMessages).	This means that
 * we can skip immediate local processing of anything that's still in
 * CurrentCmdInvalidMsgs, and just send that list out too.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.	No messages need be sent to other backends,
 * since they'll not have seen our changed tuples anyway.  We can forget
 * about CurrentCmdInvalidMsgs too, since those changes haven't touched
 * the caches yet.
 *
 * In any case, reset the various lists to empty.  We need not physically
 * free memory here, since u_sess->top_transaction_mem_cxt is about to be emptied
 * anyway.
 *
 * Note:
 *		This should be called as the last step in processing a transaction.
 */
void AtEOXact_Inval(bool isCommit)
{
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    if (isCommit) {
        /* Must be at top of stack */
        Assert(inval_cxt->transInvalInfo != NULL && inval_cxt->transInvalInfo->parent == NULL);

        /*
         * Relcache init file invalidation requires processing both before and
         * after we send the SI messages.  However, we need not do anything
         * unless we committed.
         */
        if (inval_cxt->transInvalInfo->RelcacheInitFileInval) {
            RelationCacheInitFilePreInvalidate();
        }

        AppendInvalidationMessages(&inval_cxt->transInvalInfo->PriorCmdInvalidMsgs,
            &inval_cxt->transInvalInfo->CurrentCmdInvalidMsgs);

        ProcessInvalidationMessagesMulti(
            &inval_cxt->transInvalInfo->PriorCmdInvalidMsgs, SendSharedInvalidMessages);

        if (inval_cxt->transInvalInfo->RelcacheInitFileInval) {
            RelationCacheInitFilePostInvalidate();
        }
    } else if (inval_cxt->transInvalInfo != NULL) {
        /* Must be at top of stack */
        Assert(inval_cxt->transInvalInfo->parent == NULL);
        ProcessInvalidationMessages(
            &inval_cxt->transInvalInfo->PriorCmdInvalidMsgs, LocalExecuteThreadAndSessionInvalidationMessage);
    }

    /* Need not free anything explicitly */
    inval_cxt->transInvalInfo = NULL;
}

/*
 * AtEOSubXact_Inval
 *		Process queued-up invalidation messages at end of subtransaction.
 *
 * If isCommit, process CurrentCmdInvalidMsgs if any (there probably aren't),
 * and then attach both CurrentCmdInvalidMsgs and PriorCmdInvalidMsgs to the
 * parent's PriorCmdInvalidMsgs list.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.	No messages need be sent to other backends.
 * We can forget about CurrentCmdInvalidMsgs too, since those changes haven't
 * touched the caches yet.
 *
 * In any case, pop the transaction stack.	We need not physically free memory
 * here, since t_thrd.mem_cxt.cur_transaction_mem_cxt is about to be emptied anyway
 * (if aborting).  Beware of the possibility of aborting the same nesting
 * level twice, though.
 */
void AtEOSubXact_Inval(bool isCommit)
{
    int my_level = GetCurrentTransactionNestLevel();
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    TransInvalidationInfo* myInfo = inval_cxt->transInvalInfo;

    if (isCommit) {
        /* Must be at non-top of stack */
        Assert(myInfo != NULL && myInfo->parent != NULL);
        Assert(myInfo->my_level == my_level);

        /* If CurrentCmdInvalidMsgs still has anything, fix it */
        CommandEndInvalidationMessages();

        /* Pass up my inval messages to parent */
        AppendInvalidationMessages(&myInfo->parent->PriorCmdInvalidMsgs, &myInfo->PriorCmdInvalidMsgs);

        /* Pending relcache inval becomes parent's problem too */
        if (myInfo->RelcacheInitFileInval) {
            myInfo->parent->RelcacheInitFileInval = true;
        }

        /* Pop the transaction state stack */
        inval_cxt->transInvalInfo = myInfo->parent;

        /* Need not free anything else explicitly */
        pfree_ext(myInfo);
    } else if (myInfo != NULL && myInfo->my_level == my_level) {
        /* Must be at non-top of stack */
        Assert(myInfo->parent != NULL);
        ProcessInvalidationMessages(&myInfo->PriorCmdInvalidMsgs, LocalExecuteThreadAndSessionInvalidationMessage);

        /* Pop the transaction state stack */
        inval_cxt->transInvalInfo = myInfo->parent;

        /* Need not free anything else explicitly */
        pfree_ext(myInfo);
    }
}

/*
 * CommandEndInvalidationMessages
 *		Process queued-up invalidation messages at end of one command
 *		in a transaction.
 *
 * Here, we send no messages to the shared queue, since we don't know yet if
 * we will commit.	We do need to locally process the CurrentCmdInvalidMsgs
 * list, so as to flush our caches of any entries we have outdated in the
 * current command.  We then move the current-cmd list over to become part
 * of the prior-cmds list.
 *
 * Note:
 *		This should be called during CommandCounterIncrement(),
 *		after we have advanced the command ID.
 */
void CommandEndInvalidationMessages(void)
{
    knl_u_inval_context *inval_cxt = GetInvalCxt();
    /*
     * You might think this shouldn't be called outside any transaction, but
     * bootstrap does it, and also ABORT issued when not in a transaction. So
     * just quietly return if no state to work on.
     */
    if (inval_cxt->transInvalInfo == NULL) {
        return;
    }
    ProcessInvalidationMessagesMulti(
        &inval_cxt->transInvalInfo->CurrentCmdInvalidMsgs, GlobalExecuteSharedInvalidMessages);
    ProcessInvalidationMessages(
        &inval_cxt->transInvalInfo->CurrentCmdInvalidMsgs, LocalExecuteThreadAndSessionInvalidationMessage);
    AppendInvalidationMessages(&inval_cxt->transInvalInfo->PriorCmdInvalidMsgs,
        &inval_cxt->transInvalInfo->CurrentCmdInvalidMsgs);
}

/*
 * CacheInvalidateHeapTuple
 *		Register the given tuple for invalidation at end of command
 *		(ie, current command is creating or outdating this tuple).
 *		Also, detect whether a relcache invalidation is implied.
 *
 * For an insert or delete, tuple is the target tuple and newtuple is NULL.
 * For an update, we are called just once, with tuple being the old tuple
 * version and newtuple the new version.  This allows avoidance of duplicate
 * effort during an update.
 */
void CacheInvalidateHeapTuple(Relation relation, HeapTuple tuple, HeapTuple newtuple)
{
    Oid tupleRelId;
    Oid databaseId;
    Oid relationId;

    /* Do nothing during bootstrap */
    if (IsBootstrapProcessingMode()) {
        return;
    }

    /*
     * We only need to worry about invalidation for tuples that are in system
     * relations; user-relation tuples are never in catcaches and can't affect
     * the relcache either.
     */
    if (!IsSystemRelation(relation)) {
        return;
    }

    /*
     * TOAST tuples can likewise be ignored here. Note that TOAST tables are
     * considered system relations so they are not filtered by the above test.
     */
    if (IsToastRelation(relation)) {
        return;
    }

    /*
     * First let the catcache do its thing
     */
    PrepareToInvalidateCacheTuple(relation, tuple, newtuple, RegisterCatcacheInvalidation);

    /*
     * Now, is this tuple one of the primary definers of a relcache entry?
     *
     * Note we ignore newtuple here; we assume an update cannot move a tuple
     * from being part of one relcache entry to being part of another.
     */
    tupleRelId = RelationGetRelid(relation);

    if (tupleRelId == RelationRelationId) {
        Form_pg_class classtup = (Form_pg_class)GETSTRUCT(tuple);

        relationId = HeapTupleGetOid(tuple);
        if (classtup->relisshared) {
            databaseId = InvalidOid;
        } else {
            databaseId = u_sess->proc_cxt.MyDatabaseId;
        }
    } else if (tupleRelId == AttributeRelationId) {
        Form_pg_attribute atttup = (Form_pg_attribute)GETSTRUCT(tuple);

        relationId = atttup->attrelid;

        /*
         * KLUGE ALERT: we always send the relcache event with u_sess->proc_cxt.MyDatabaseId,
         * even if the rel in question is shared (which we can't easily tell).
         * This essentially means that only backends in this same database
         * will react to the relcache flush request.  This is in fact
         * appropriate, since only those backends could see our pg_attribute
         * change anyway.  It looks a bit ugly though.	(In practice, shared
         * relations can't have schema changes after bootstrap, so we should
         * never come here for a shared rel anyway.)
         */
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    } else if (tupleRelId == IndexRelationId) {
        Form_pg_index indextup = (Form_pg_index)GETSTRUCT(tuple);

        /*
         * When a pg_index row is updated, we should send out a relcache inval
         * for the index relation.	As above, we don't know the shared status
         * of the index, but in practice it doesn't matter since indexes of
         * shared catalogs can't have such updates.
         */
        relationId = indextup->indexrelid;
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    } else if (tupleRelId == PartitionRelationId) {
        Oid partitionId = HeapTupleGetOid(tuple);
        databaseId = u_sess->proc_cxt.MyDatabaseId;
        RegisterPartcacheInvalidation(databaseId, partitionId);
        return;
    } else if (tupleRelId == PgxcClassRelationId) {
        Form_pgxc_class pgxcclasstup = (Form_pgxc_class)GETSTRUCT(tuple);

        /*
         * When distribution key or strategy for a relation is changed, we must
         * also send out a relcache inval for the relation.
         */
        relationId = pgxcclasstup->pcrelid;
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    } else {
        return;
    }

    /*
     * Yes.  We need to register a relcache invalidation event.
     */
    RegisterRelcacheInvalidation(databaseId, relationId);
}

void CacheInvalidateFunction(Oid funcId, Oid pkgId)
{
    AddFunctionCacheInvalidationMessage(&GetInvalCxt()->transInvalInfo->CurrentCmdInvalidMsgs,
                                        u_sess->proc_cxt.MyDatabaseId, funcId, pkgId);
}

/*
 * CacheInvalidateCatalog
 *		Register invalidation of the whole content of a system catalog.
 *
 * This is normally used in VACUUM FULL/CLUSTER, where we haven't so much
 * changed any tuples as moved them around.  Some uses of catcache entries
 * expect their TIDs to be correct, so we have to blow away the entries.
 *
 * Note: we expect caller to verify that the rel actually is a system
 * catalog.  If it isn't, no great harm is done, just a wasted sinval message.
 */
void CacheInvalidateCatalog(Oid catalogId)
{
    Oid databaseId;
    if (IsSharedRelation(catalogId)) {
        databaseId = InvalidOid;
    } else {
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    }
    RegisterCatalogInvalidation(databaseId, catalogId);
}

/*
 * CacheInvalidateRelcache
 *		Register invalidation of the specified relation's relcache entry
 *		at end of command.
 *
 * This is used in places that need to force relcache rebuild but aren't
 * changing any of the tuples recognized as contributors to the relcache
 * entry by CacheInvalidateHeapTuple.  (An example is dropping an index.)
 */
void CacheInvalidateRelcache(Relation relation)
{
    Oid databaseId;
    Oid relationId;

    relationId = RelationGetRelid(relation);
    if (relation->rd_rel->relisshared) {
        databaseId = InvalidOid;
    } else {
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    }
    RegisterRelcacheInvalidation(databaseId, relationId);
}

/*
 * CacheInvalidateRelcacheByTuple
 *		As above, but relation is identified by passing its pg_class tuple.
 */
void CacheInvalidateRelcacheByTuple(HeapTuple classTuple)
{
    Form_pg_class classtup = (Form_pg_class)GETSTRUCT(classTuple);
    Oid databaseId;
    Oid relationId;

    relationId = HeapTupleGetOid(classTuple);
    if (classtup->relisshared) {
        databaseId = InvalidOid;
    } else {
        databaseId = u_sess->proc_cxt.MyDatabaseId;
    }
    RegisterRelcacheInvalidation(databaseId, relationId);
}

/*
 * CacheInvalidateRelcacheByRelid
 *		As above, but relation is identified by passing its OID.
 *		This is the least efficient of the three options; use one of
 *		the above routines if you have a Relation or pg_class tuple.
 */
void CacheInvalidateRelcacheByRelid(Oid relid)
{
    HeapTuple tup;

    tup = SearchSysCache1WithLogLevel(RELOID, ObjectIdGetDatum(relid), LOG);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for relation %u", relid)));
    }
    CacheInvalidateRelcacheByTuple(tup);
    ReleaseSysCache(tup);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Register invalidation of the specified relation's relcache
 *			: entry at end of command.
 * Description	: his is used in places that need to force relcache rebuild but aren't
 *			: changing any of the tuples recognized as contributors to the relcache
 *			: entry by CacheInvalidateHeapTuple.  (An example is dropping an index.)
 */
void CacheInvalidatePartcache(Partition partition)
{
    Oid databaseId;
    Oid relationId;

    relationId = PartitionGetPartid(partition);
    databaseId = u_sess->proc_cxt.MyDatabaseId;
    RegisterPartcacheInvalidation(databaseId, relationId);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: As above, but relation is identified by passing its pg_partition tuple.
 */
void CacheInvalidatePartcacheByTuple(HeapTuple partitionTuple)
{
    Oid databaseId;
    Oid patitionId;

    patitionId = HeapTupleGetOid(partitionTuple);
    databaseId = u_sess->proc_cxt.MyDatabaseId;
    RegisterPartcacheInvalidation(databaseId, patitionId);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: As above, but relation is identified by passing its OID.
 *			: This is the least efficient of the three options; use one of
 *			: the above routines if you have a Relation or pg_class tuple.
 */
void CacheInvalidatePartcacheByPartid(Oid partid)
{
    HeapTuple tup;

    tup = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for partition %u", partid)));
    }
    CacheInvalidatePartcacheByTuple(tup);
    ReleaseSysCache(tup);
}

/*
 * CacheInvalidateSmgr
 *		Register invalidation of smgr references to a physical relation.
 *
 * Sending this type of invalidation msg forces other backends to close open
 * smgr entries for the rel.  This should be done to flush dangling open-file
 * references when the physical rel is being dropped or truncated.	Because
 * these are nontransactional (i.e., not-rollback-able) operations, we just
 * send the inval message immediately without any queuing.
 *
 * Note: in most cases there will have been a relcache flush issued against
 * the rel at the logical level.  We need a separate smgr-level flush because
 * it is possible for backends to have open smgr entries for rels they don't
 * have a relcache entry for, e.g. because the only thing they ever did with
 * the rel is write out dirty shared buffers.
 *
 * Note: because these messages are nontransactional, they won't be captured
 * in commit/abort WAL entries.  Instead, calls to CacheInvalidateSmgr()
 * should happen in low-level smgr.c routines, which are executed while
 * replaying WAL as well as when creating it.
 *
 * Note: In order to avoid bloating SharedInvalidationMessage, we store only
 * three bytes of the backend ID using what would otherwise be padding space.
 * Thus, the maximum possible backend ID is 2^23-1.
 */
void CacheInvalidateSmgr(RelFileNodeBackend rnode)
{
    SharedInvalidationMessage msg;

    if (!IsSegmentFileNode(rnode.node)) {
        msg.sm.id = SHAREDINVALSMGR_ID;
        msg.sm.backend_hi = rnode.backend >> 16;
        msg.sm.backend_lo = rnode.backend & 0xffff;
        RelFileNodeRelCopy(msg.sm.rnode, rnode.node);
    } else {
        msg.hbksm.id = SHAREDINVALHBKTSMGR_ID;
        msg.hbksm.bucketId = rnode.node.bucketNode;
        RelFileNodeRelCopy(msg.hbksm.rnode, rnode.node);
    }

    SendSharedInvalidMessages(&msg, 1);
}

/*
 * CacheInvalidateRelmap
 *		Register invalidation of the relation mapping for a database,
 *		or for the shared catalogs if databaseId is zero.
 *
 * Sending this type of invalidation msg forces other backends to re-read
 * the indicated relation mapping file.  It is also necessary to send a
 * relcache inval for the specific relations whose mapping has been altered,
 * else the relcache won't get updated with the new filenode data.
 *
 * Note: because these messages are nontransactional, they won't be captured
 * in commit/abort WAL entries.  Instead, calls to CacheInvalidateRelmap()
 * should happen in low-level relmapper.c routines, which are executed while
 * replaying WAL as well as when creating it.
 */
void CacheInvalidateRelmap(Oid databaseId)
{
    SharedInvalidationMessage msg;

    msg.rm.id = SHAREDINVALRELMAP_ID;
    msg.rm.dbId = databaseId;
    SendSharedInvalidMessages(&msg, 1);
}

/*
 * CacheInvalidateHeapTupleInplace
 *		This functions differs from CacheInvalidateHeapTuple that
 *		it directly sends the invalidation messages for the given tuple when called.
 *		(CacheInvalidateHeapTuple only sends inval msgs at xact commit)
 *		This functions is to be used with heap_inplace_update where
 *		MVCC and transactional safety are not to be preserved.
 *
 * Since heap_inplace_update directly updates the original tuple physically, we only
 * need to invalidate the new-version tuple. This also means that heap_inplace_update cannot
 * update the catcache key columns, which is the prerequisite of heap_inplace_update.
 *
 * At present, we send the invalidation messages one by one. Since this function is only used
 * in very rare cases, performance is not a big concern. If it is to be used more frequently, please
 * group the inval msgs!
 */
void CacheInvalidateHeapTupleInplace(Relation relation, HeapTuple tuple)
{
    PrepareToInvalidateCacheTuple(relation, tuple, NULL, SendCatcacheInvalidation);
}

/*
 * CacheRegisterThreadSyscacheCallback
 *		Register the specified function to be called for all future
 *		invalidation events in the specified cache.  The cache ID and the
 *		hash value of the tuple being invalidated will be passed to the
 *		function.
 *
 * NOTE: Hash value zero will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 * Yes, there's a possibility of a false match to zero, but it doesn't seem
 * worth troubling over, especially since most of the current callees just
 * flush all cached state anyway.
 */
void CacheRegisterThreadSyscacheCallback(int cacheid, SyscacheCallbackFunction func, Datum arg)
{
    if (!EnableLocalSysCache()) {
        CacheRegisterSessionSyscacheCallback(cacheid, func, arg);
        return;
    }
    knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    for (int i = 0; i < inval_cxt->syscache_callback_count; i++) {
        if (inval_cxt->syscache_callback_list[i].id == cacheid &&
            inval_cxt->syscache_callback_list[i].function == func) {
            Assert(IS_THREAD_POOL_STREAM);
            if (inval_cxt->syscache_callback_list[i].arg != arg) {
                inval_cxt->syscache_callback_list[i].arg = arg;
            }
            return;
        }
    }
    if (inval_cxt->syscache_callback_count >= MAX_SYSCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of syscache_callback_list slots")));
    }
    Assert(func != NULL);
    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].id = cacheid;
    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].function = func;
    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].arg = arg;

    ++inval_cxt->syscache_callback_count;
}

/*
 * CacheRegisterSessionSyscacheCallback
 * make sure the cache the func flush is in u_sess, or you should use CacheRegisterThreadSyscacheCallback
 */
void CacheRegisterSessionSyscacheCallback(int cacheid, SyscacheCallbackFunction func, Datum arg)
{
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (int i = 0; i < inval_cxt->syscache_callback_count; i++) {
        if (inval_cxt->syscache_callback_list[i].id == cacheid &&
            inval_cxt->syscache_callback_list[i].function == func) {
            Assert(inval_cxt->syscache_callback_list[i].arg == arg);
            inval_cxt->syscache_callback_list[i].arg = arg;
            return;
        }
    }
    if (inval_cxt->syscache_callback_count >= MAX_SYSCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of syscache_callback_list slots")));
    }

    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].id = cacheid;
    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].function = func;
    inval_cxt->syscache_callback_list[inval_cxt->syscache_callback_count].arg = arg;

    ++inval_cxt->syscache_callback_count;
}

/*
 * CacheRegisterThreadRelcacheCallback
 *		Register the specified function to be called for all future
 *		relcache invalidation events.  The OID of the relation being
 *		invalidated will be passed to the function.
 *
 * NOTE: InvalidOid will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 */
void CacheRegisterThreadRelcacheCallback(RelcacheCallbackFunction func, Datum arg)
{
    if (!EnableLocalSysCache()) {
        CacheRegisterSessionRelcacheCallback(func, arg);
        return;
    }
    knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    for (int i = 0; i < inval_cxt->relcache_callback_count; i++) {
        if (inval_cxt->relcache_callback_list[i].function == func) {
            Assert(IS_THREAD_POOL_STREAM);
            Assert(inval_cxt->relcache_callback_list[i].arg == arg);
            if (inval_cxt->relcache_callback_list[i].arg != arg) {
                inval_cxt->relcache_callback_list[i].arg = arg;
            }
            return;
        }
    }
    if (inval_cxt->relcache_callback_count >= MAX_RELCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of relcache_callback_list slots")));
    }
    Assert(func != NULL);
    inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].function = func;
    inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].arg = arg;

    ++inval_cxt->relcache_callback_count;
}

/*
 * CacheRegisterSessionRelcacheCallback
 * make sure the cache the func flush is in u_sess, or you should use CacheRegisterThreadRelcacheCallback
 */
void CacheRegisterSessionRelcacheCallback(RelcacheCallbackFunction func, Datum arg)
{
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (int i = 0; i < inval_cxt->relcache_callback_count; i++) {
        if (inval_cxt->relcache_callback_list[i].function == func) {
            if (inval_cxt->relcache_callback_list[i].arg != arg) {
                inval_cxt->relcache_callback_list[i].arg = arg;
            }
            return;
        }
    }
    if (inval_cxt->relcache_callback_count >= MAX_RELCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of relcache_callback_list slots")));
    }
    inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].function = func;
    inval_cxt->relcache_callback_list[inval_cxt->relcache_callback_count].arg = arg;

    ++inval_cxt->relcache_callback_count;
}
/*
 * CacheRegisterThreadPartcacheCallback
 *		Register the specified function to be called for all future
 *		relcache invalidation events.  The OID of the relation being
 *		invalidated will be passed to the function.
 *
 * NOTE: InvalidOid will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 */
void CacheRegisterThreadPartcacheCallback(PartcacheCallbackFunction func, Datum arg)
{
    if (!EnableLocalSysCache()) {
        CacheRegisterSessionPartcacheCallback(func, arg);
        return;
    }
    knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    for (int i = 0; i < inval_cxt->partcache_callback_count; i++) {
        if (inval_cxt->partcache_callback_list[i].function == func) {
            Assert(IS_THREAD_POOL_STREAM);
            Assert(inval_cxt->partcache_callback_list[i].arg == arg);
            if (inval_cxt->partcache_callback_list[i].arg != arg) {
                inval_cxt->partcache_callback_list[i].arg = arg;
            }
            return;
        }
    }
    if (inval_cxt->partcache_callback_count >= MAX_PARTCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of partcache_callback_list slots")));
    }
    Assert(func != NULL);
    inval_cxt->partcache_callback_list[inval_cxt->partcache_callback_count].function = func;
    inval_cxt->partcache_callback_list[inval_cxt->partcache_callback_count].arg = arg;

    ++inval_cxt->partcache_callback_count;
}

/*
 * CacheRegisterSessionPartcacheCallback
 * make sure the cache the func flush is in u_sess, or you should use CacheRegisterThreadPartcacheCallback
 */
void CacheRegisterSessionPartcacheCallback(PartcacheCallbackFunction func, Datum arg)
{
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (int i = 0; i < inval_cxt->partcache_callback_count; i++) {
        if (inval_cxt->partcache_callback_list[i].function == func) {
            if (inval_cxt->partcache_callback_list[i].arg != arg) {
                inval_cxt->partcache_callback_list[i].arg = arg;
            }
            return;
        }
    }
    if (inval_cxt->partcache_callback_count >= MAX_PARTCACHE_CALLBACKS) {
        ereport(FATAL, (errmsg("out of partcache_callback_list slots")));
    }
    inval_cxt->partcache_callback_list[inval_cxt->partcache_callback_count].function = func;
    inval_cxt->partcache_callback_list[inval_cxt->partcache_callback_count].arg = arg;

    ++inval_cxt->partcache_callback_count;
}

/*
 * CallThreadSyscacheCallbacks
 *
 * This is exported so that CatalogCacheFlushCatalog can call it, saving
 * this module from knowing which catcache IDs correspond to which catalogs.
 */
void CallThreadSyscacheCallbacks(int cacheid, uint32 hashvalue)
{
    Assert(EnableGlobalSysCache());
    int i;
    knl_u_inval_context *inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    for (i = 0; i < inval_cxt->syscache_callback_count; i++) {
        struct SYSCACHECALLBACK* ccitem = inval_cxt->syscache_callback_list + i;
        if (ccitem->id == cacheid) {
            (*ccitem->function)(ccitem->arg, cacheid, hashvalue);
        }
    }
}

/*
 * CallSessionSyscacheCallbacks
 * make sure the cache the func flush is in u_sess, or you should use CallThreadSyscacheCallbacks
 */
void CallSessionSyscacheCallbacks(int cacheid, uint32 hashvalue)
{
    int i;
    knl_u_inval_context *inval_cxt = &u_sess->inval_cxt;
    for (i = 0; i < inval_cxt->syscache_callback_count; i++) {
        struct SYSCACHECALLBACK* ccitem = inval_cxt->syscache_callback_list + i;
        if (ccitem->id == cacheid) {
            (*ccitem->function)(ccitem->arg, cacheid, hashvalue);
        }
    }
}

/*
 * PrepareInvalidationState
 * 		Initialize inval lists for the current (sub)transaction.
 */
static void PrepareInvalidationState(void)
{
    TransInvalidationInfo *myInfo;
    knl_u_inval_context *inval_cxt = GetInvalCxt();

    if (inval_cxt->transInvalInfo != NULL &&
        inval_cxt->transInvalInfo->my_level == GetCurrentTransactionNestLevel()) {
        return;
    }

    myInfo =
        (TransInvalidationInfo *)MemoryContextAllocZero(u_sess->top_transaction_mem_cxt, sizeof(TransInvalidationInfo));
    myInfo->parent = inval_cxt->transInvalInfo;
    myInfo->my_level = GetCurrentTransactionNestLevel();

    /*
     * If there's any previous entry, this one should be for a deeper nesting
     * level.
     */
    Assert(inval_cxt->transInvalInfo == NULL || myInfo->my_level > inval_cxt->transInvalInfo->my_level);

    inval_cxt->transInvalInfo = myInfo;
}

/*
 * CacheInvalidateRelcacheAll
 * 		Register invalidation of the whole relcache at the end of command.
 *
 * This is used by alter publication as changes in publications may affect
 * large number of tables.
 */
void CacheInvalidateRelcacheAll(void)
{
    PrepareInvalidationState();

    RegisterRelcacheInvalidation(InvalidOid, InvalidOid);
}
