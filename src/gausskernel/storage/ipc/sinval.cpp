/* -------------------------------------------------------------------------
 *
 * sinval.cpp
 *	  openGauss shared cache invalidation communication code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/ipc/sinval.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "commands/async.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/sinvaladt.h"
#include "utils/globalplancache.h"
#include "utils/inval.h"
#include "utils/plancache.h"
#include "libcomm/libcomm.h"

/*
 * Because backends sitting idle will not be reading sinval events, we
 * need a way to give an idle backend a swift kick in the rear and make
 * it catch up before the sinval queue overflows and forces it to go
 * through a cache reset exercise.  This is done by sending
 * PROCSIG_CATCHUP_INTERRUPT to any backend that gets too far behind.
 *
 * The signal handler will set an interrupt pending flag and will set the
 * processes latch. Whenever starting to read from the client, or when
 * interrupted while doing so, ProcessClientReadInterrupt() will call
 * the function ProcessCatchupInterrupt().
 */
THR_LOCAL volatile sig_atomic_t catchupInterruptPending = false;

static void GlobalInvalidSharedInvalidMessages(const SharedInvalidationMessage* msgs, int n, bool is_commit)
{
    Assert(EnableGlobalSysCache());
    for (int i = 0; i < n; i++) {
        SharedInvalidationMessage *msg = (SharedInvalidationMessage *)(msgs + i);
        if (msg->id >= 0) {
            t_thrd.lsc_cxt.lsc->systabcache.CacheIdHashValueInvalidateGlobal(msg->cc.dbId, msg->cc.id,
                msg->cc.hashValue, is_commit);
        } else if (msg->id == SHAREDINVALCATALOG_ID) {
            t_thrd.lsc_cxt.lsc->systabcache.CatalogCacheFlushCatalogGlobal(msg->cat.dbId, msg->cat.catId, is_commit);
        } else if (msg->id == SHAREDINVALRELCACHE_ID) {
            t_thrd.lsc_cxt.lsc->tabdefcache.InvalidateGlobalRelation(msg->rc.dbId, msg->rc.relId, is_commit);
        } else if (msg->id == SHAREDINVALPARTCACHE_ID) {
            t_thrd.lsc_cxt.lsc->partdefcache.InvalidateGlobalPartition(msg->pc.dbId, msg->pc.partId, is_commit);
        }
        /* global relmap are backups of relmapfile, so no need to deal with the msg */
    }
}

void GlobalExecuteSharedInvalidMessages(const SharedInvalidationMessage* msgs, int n)
{
    /* threads who not support gsc dont need invalid global before commit */
    if (!EnableLocalSysCache()) {
        return;
    }
    GlobalInvalidSharedInvalidMessages(msgs, n, IS_THREAD_POOL_STREAM || IsBgWorkerProcess());
}
/*
 * SendSharedInvalidMessages
 *	Add shared-cache-invalidation message(s) to the global SI message queue.
 */
void SendSharedInvalidMessages(const SharedInvalidationMessage* msgs, int n)
{
    /* threads who not support gsc still need invalid global when commit */
    if (EnableGlobalSysCache()) {
        GlobalInvalidSharedInvalidMessages(msgs, n, true);
    }
    SIInsertDataEntries(msgs, n);
    if (ENABLE_GPC && g_instance.plan_cache != NULL) {
        g_instance.plan_cache->InvalMsg(msgs, n);
    }
}

/*
 * ReceiveSharedInvalidMessages
 *		Process shared-cache-invalidation messages waiting for this backend
 *
 * We guarantee to process all messages that had been queued before the
 * routine was entered.  It is of course possible for more messages to get
 * queued right after our last SIGetDataEntries call.
 *
 * NOTE: it is entirely possible for this routine to be invoked recursively
 * as a consequence of processing inside the invalFunction or resetFunction.
 * Furthermore, such a recursive call must guarantee that all outstanding
 * inval messages have been processed before it exits.	This is the reason
 * for the strange-looking choice to use a statically allocated buffer array
 * and counters; it's so that a recursive call can process messages already
 * sucked out of sinvaladt.c.
 */
void ReceiveSharedInvalidMessages(void (*invalFunction)(SharedInvalidationMessage* msg), void (*resetFunction)(void), 
    bool worksession)
{
    knl_u_inval_context *inval_cxt;
    if (!EnableLocalSysCache()) {
        inval_cxt = &u_sess->inval_cxt;
    } else if (worksession) {
        inval_cxt = &u_sess->inval_cxt;
    } else {
        inval_cxt = &t_thrd.lsc_cxt.lsc->inval_cxt;
    }
    /* Deal with any messages still pending from an outer recursion */
    while (inval_cxt->nextmsg < inval_cxt->nummsgs) {
        SharedInvalidationMessage msg = inval_cxt->messages[inval_cxt->nextmsg++];

        inval_cxt->SIMCounter++;
        invalFunction(&msg);
    }

    do {
        int getResult;

        inval_cxt->nextmsg = inval_cxt->nummsgs = 0;

        /* Try to get some more messages */
        getResult = SIGetDataEntries(inval_cxt->messages, MAXINVALMSGS, worksession);
        if (getResult < 0) {
            /* got a reset message */
            ereport(DEBUG4, (errmsg("cache state reset")));
            inval_cxt->SIMCounter++;
            resetFunction();
            break; /* nothing more to do */
        }

        /* Process them, being wary that a recursive call might eat some */
        inval_cxt->nextmsg = 0;
        inval_cxt->nummsgs = getResult;

        while (inval_cxt->nextmsg < inval_cxt->nummsgs) {
            SharedInvalidationMessage msg = inval_cxt->messages[inval_cxt->nextmsg++];

            inval_cxt->SIMCounter++;
            invalFunction(&msg);
        }

        /*
         * We only need to loop if the last SIGetDataEntries call (which might
         * have been within a recursive call) returned a full buffer.
         */
    } while (inval_cxt->nummsgs == MAXINVALMSGS);

    /*
     * We are now caught up.  If we received a catchup signal, reset that
     * flag, and call SICleanupQueue().  This is not so much because we need
     * to flush dead messages right now, as that we want to pass on the
     * catchup signal to the next slowest backend.  "Daisy chaining" the
     * catchup signal this way avoids creating spikes in system load for what
     * should be just a background maintenance activity.
     */
    if (catchupInterruptPending) {
        catchupInterruptPending = false;
        ereport(DEBUG4, (errmsg("sinval catchup complete, cleaning queue")));
        SICleanupQueue(false, 0);
    }
}

/*
 * HandleCatchupInterrupt
 *
 * This is called when PROCSIG_CATCHUP_INTERRUPT is received.
 *
 * We used to directly call ProcessCatchupInterrupt directly when idle. These days
 * we just set a flag to do it later and notify the process of that fact by
 * setting the process's latch.
 */
void HandleCatchupInterrupt(void)
{
    /*
     * Note: this is called by a SIGNAL HANDLER. You must be very wary what
     * you do here.
     */
    catchupInterruptPending = true;
}

/*
 * ProcessCatchupInterrupt
 *
 * The portion of catchup interrupt handling that runs outside of the signal
 * handler, which allows it to actually process pending invalidations.
 */
void ProcessCatchupInterrupt(void)
{
    while (catchupInterruptPending) {
        /*
         * What we need to do here is cause ReceiveSharedInvalidMessages() to
         * run, which will do the necessary work and also reset the
         * catchupInterruptPending flag.  If we are inside a transaction we
         * can just call AcceptInvalidationMessages() to do this.  If we
         * aren't, we start and immediately end a transaction; the call to
         * AcceptInvalidationMessages() happens down inside transaction start.
         *
         * It is awfully tempting to just call AcceptInvalidationMessages()
         * without the rest of the xact start/stop overhead, and I think that
         * would actually work in the normal case; but I am not sure that
         * things would clean up nicely if we got an error partway through.
         */
        if (IsTransactionOrTransactionBlock()) {
            ereport(DEBUG4, (errmsg("ProcessCatchupInterrupt inside transaction")));
            AcceptInvalidationMessages();
        } else {
            ereport(DEBUG4, (errmsg("ProcessCatchupInterrupt outside transaction")));
            StartTransactionCommand();
            CommitTransactionCommand();
        }
    }
}

