/* -------------------------------------------------------------------------
 *
 * slru.cpp
 *		Simple LRU buffering for transaction status logfiles
 *
 * We use a simple least-recently-used scheme to manage a pool of page
 * buffers.  Under ordinary circumstances we expect that write
 * traffic will occur mostly to the latest page (and to the just-prior
 * page, soon after a page transition).  Read traffic will probably touch
 * a larger span of pages, but in any case a fairly small number of page
 * buffers should be sufficient.  So, we just search the buffers using plain
 * linear search; there's no need for a hashtable or anything fancy.
 * The management algorithm is straight LRU except that we will never swap
 * out the latest page (since we know it's going to be hit again eventually).
 *
 * We use a control LWLock to protect the shared data structures, plus
 * per-buffer LWLocks that synchronize I/O for each buffer.  The control lock
 * must be held to examine or modify any shared state.	A process that is
 * reading in or writing out a page buffer does not hold the control lock,
 * only the per-buffer lock for the buffer it is working on.
 *
 * "Holding the control lock" means exclusive lock in all cases except for
 * SimpleLruReadPage_ReadOnly(); see comments for SlruRecentlyUsed() for
 * the implications of that.
 *
 * When initiating I/O on a buffer, we acquire the per-buffer lock exclusively
 * before releasing the control lock.  The per-buffer lock is released after
 * completing the I/O, re-acquiring the control lock, and updating the shared
 * state.  (Deadlock is not possible here, because we never try to initiate
 * I/O when someone else is already doing I/O on the same buffer.)
 * To wait for I/O to complete, release the control lock, acquire the
 * per-buffer lock in shared mode, immediately release the per-buffer lock,
 * reacquire the control lock, and then recheck state (since arbitrary things
 * could have happened while we didn't have the lock).
 *
 * As with the regular buffer manager, it is possible for another process
 * to re-dirty a page that is currently being written out.	This is handled
 * by re-setting the page's page_dirty flag.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/slru.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/slru.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/csnlog.h"
#include "storage/smgr/fd.h"
#include "storage/shmem.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "storage/file/fio_device.h"

/*
 * During SimpleLruFlush(), we will usually not need to write/fsync more
 * than one or two physical files, but we may need to write several pages
 * per file.  We can consolidate the I/O requests by leaving files open
 * until control returns to SimpleLruFlush().  This data structure remembers
 * which files are open.
 */
#define MAX_FLUSH_BUFFERS 16

typedef struct SlruFlushData {
    int num_files;                  /* # files actually open */
    int fd[MAX_FLUSH_BUFFERS];      /* their FD's */
    int64 segno[MAX_FLUSH_BUFFERS]; /* their log seg#s */
} SlruFlushData;

typedef struct SlruFlushData *SlruFlush;

/*
 * Macro to mark a buffer slot "most recently used".  Note multiple evaluation
 * of arguments!
 *
 * The reason for the if-test is that there are often many consecutive
 * accesses to the same page (particularly the latest page).  By suppressing
 * useless increments of cur_lru_count, we reduce the probability that old
 * pages' counts will "wrap around" and make them appear recently used.
 *
 * We allow this code to be executed concurrently by multiple processes within
 * SimpleLruReadPage_ReadOnly().  As long as int reads and writes are atomic,
 * this should not cause any completely-bogus values to enter the computation.
 * However, it is possible for either cur_lru_count or individual
 * page_lru_count entries to be "reset" to lower values than they should have,
 * in case a process is delayed while it executes this macro.  With care in
 * SlruSelectLRUPage(), this does little harm, and in any case the absolute
 * worst possible consequence is a nonoptimal choice of page to evict.	The
 * gain from allowing concurrent reads of SLRU pages seems worth it.
 */
#define SlruRecentlyUsed(shared, slotno) do { \
    int new_lru_count = (shared)->cur_lru_count;             \
    if (new_lru_count != (shared)->page_lru_count[slotno]) { \
        (shared)->cur_lru_count = ++new_lru_count;           \
        (shared)->page_lru_count[slotno] = new_lru_count;    \
    }                                                        \
} while (0)

static void SimpleLruZeroLSNs(SlruCtl ctl, int slotno);
static void SlruInternalWritePage(SlruCtl ctl, int slotno, SlruFlush fdata);
static bool SlruPhysicalReadPage(SlruCtl ctl, int64 pageno, int slotno);
static bool SlruPhysicalWritePage(SlruCtl ctl, int64 pageno, int slotno, SlruFlush fdata);
static void SlruReportIOError(SlruCtl ctl, int64 pageno, TransactionId xid);
static int SlruSelectLRUPage(SlruCtl ctl, int64 pageno);

static inline int execSimpleLruReadPageReadOnly(SlruCtl ctl, int64 pageno, TransactionId xid)
{
    SlruShared shared = ctl->shared;
    int slotno;

    /* See if page is already in a buffer */
    for (slotno = 0; slotno < shared->num_slots; slotno++) {
        if (shared->page_number[slotno] == pageno && shared->page_status[slotno] != SLRU_PAGE_EMPTY &&
            shared->page_status[slotno] != SLRU_PAGE_READ_IN_PROGRESS) {
            /* See comments for SlruRecentlyUsed macro */
            SlruRecentlyUsed(shared, slotno);
            return slotno;
        }
    }
    /* No luck, so switch to normal exclusive lock and do regular read */
    LWLockRelease(shared->control_lock);
    (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);
    return SimpleLruReadPage(ctl, pageno, true, xid);
}

/* Initialization of shared memory */
Size SimpleLruShmemSize(int nslots, int nlsns)
{
    Size sz;

    /* we assume nslots isn't so large as to risk overflow */
    sz = MAXALIGN(sizeof(SlruSharedData));
    sz += MAXALIGN(nslots * sizeof(char *));         /* page_buffer[] */
    sz += MAXALIGN(nslots * sizeof(SlruPageStatus)); /* page_status[] */
    sz += MAXALIGN(nslots * sizeof(bool));           /* page_dirty[] */
    sz += MAXALIGN(nslots * sizeof(int64));          /* page_number[] */
    sz += MAXALIGN(nslots * sizeof(int));            /* page_lru_count[] */
    sz += MAXALIGN(nslots * sizeof(LWLock *));       /* buffer_locks[] */

    if (nlsns > 0)
        sz += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr)); /* group_lsn[] */

    return BUFFERALIGN(sz) + BLCKSZ * nslots;
}

void SimpleLruInit(SlruCtl ctl, const char *name, int trancheId, int nslots, int nlsns, LWLock *ctllock,
                   const char *subdir, int index)
{
    SlruShared shared;
    bool found = false;
    errno_t rc = EOK;

    shared = (SlruShared)ShmemInitStruct(name, SimpleLruShmemSize(nslots, nlsns), &found);

    if (!IsUnderPostmaster) {
        /* Initialize locks and shared memory area */
        char *ptr = NULL;
        Size offset;
        int slotno;

        Assert(!found);

        rc = memset_s(shared, sizeof(SlruSharedData), 0, sizeof(SlruSharedData));
        securec_check(rc, "\0", "\0");

        shared->control_lock = ctllock;
        shared->num_slots = nslots;
        shared->lsn_groups_per_page = nlsns;
        shared->cur_lru_count = 0;
        shared->force_check_first_xid = false;

        /* shared->latest_page_number will be set later */
        ptr = (char *)shared;
        offset = MAXALIGN(sizeof(SlruSharedData));
        shared->page_buffer = (char **)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(char *));
        shared->page_status = (SlruPageStatus *)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(SlruPageStatus));
        shared->page_dirty = (bool *)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(bool));
        shared->page_number = (int64 *)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(int64));
        shared->page_lru_count = (int *)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(int));
        shared->buffer_locks = (LWLock **)(ptr + offset);
        offset += MAXALIGN(nslots * sizeof(LWLock *));

        if (nlsns > 0) {
            shared->group_lsn = (XLogRecPtr *)(ptr + offset);
            offset += MAXALIGN(nslots * nlsns * sizeof(XLogRecPtr));
        }

        ptr += BUFFERALIGN(offset);

        for (slotno = 0; slotno < nslots; slotno++) {
            shared->page_buffer[slotno] = ptr;
            shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            shared->page_dirty[slotno] = false;
            shared->page_lru_count[slotno] = 0;
            shared->buffer_locks[slotno] = LWLockAssign(trancheId);
            ptr += BLCKSZ;
        }
    } else
        Assert(found);

    /*
     * Initialize the unshared control struct, including directory path. We
     * assume caller set PagePrecedes.
     */
    ctl->shared = shared;
    ctl->do_fsync = true; /* default behavior */
    rc = strncpy_s(ctl->dir, sizeof(ctl->dir), subdir, strlen(subdir));
    securec_check(rc, "\0", "\0");
}

/*
 * Initialize (or reinitialize) a page to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int SimpleLruZeroPage(SlruCtl ctl, int64 pageno, bool *pBZeroPage)
{
    SlruShared shared = ctl->shared;
    errno_t rc = EOK;
    int slotno;

    for (;;) {
        /* Find a suitable buffer slot for the page */
        slotno = SlruSelectLRUPage(ctl, pageno);
        /* Did we find the page in memory? */
        if (shared->page_number[slotno] == pageno && shared->page_status[slotno] != SLRU_PAGE_EMPTY) {
            /*
             * If page is being read in, we must wait for I/O complete and recheck.
             * If page is being written or valid, don't need to zero this page.
             */
            if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS) {
                SimpleLruWaitIO(ctl, slotno);
                /* Now we must recheck state from the top */
                continue;
            } else {
                /* The page has been extended by someone, it's ready to use */
                if (pBZeroPage != NULL) {
                    *pBZeroPage = false;
                }

                SlruRecentlyUsed(shared, slotno);
                return slotno;
            }
        }
        /*
         * case 1: a free slot, or
         * case 2: an existing slot from victim page, or
         * case 3: an existing slot with the same pageno
         */
        if (!(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
              (shared->page_status[slotno] == SLRU_PAGE_VALID && !shared->page_dirty[slotno])))
            ereport(PANIC,
                    (errmodule(MOD_SLRU), errcode(ERRCODE_DATA_EXCEPTION), errmsg("slru zero page under %s", ctl->dir),
                     errdetail("page(%ld) in slot(%d) status(%d) | number(%ld) | dirty(%d) is wrong.", pageno, slotno,
                               shared->page_status[slotno], shared->page_number[slotno], shared->page_dirty[slotno]),
                     errhint("Try it again.")));

        /* Mark the slot as containing this page */
        shared->page_number[slotno] = pageno;
        shared->page_status[slotno] = SLRU_PAGE_VALID;
        shared->page_dirty[slotno] = true;
        SlruRecentlyUsed(shared, slotno);

        /* Set the buffer to zeroes */
        rc = memset_s(shared->page_buffer[slotno], BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");

        /* Set the LSNs for this new page to zero */
        SimpleLruZeroLSNs(ctl, slotno);

        /* Assume this page is now the latest active page */
        shared->latest_page_number = pageno;

        return slotno;
    }
}

/*
 * Zero all the LSNs we store for this slru page.
 *
 * This should be called each time we create a new page, and each time we read
 * in a page from disk into an existing buffer.  (Such an old page cannot
 * have any interesting LSNs, since we'd have flushed them before writing
 * the page in the first place.)
 *
 * This assumes that InvalidXLogRecPtr is bitwise-all-0.
 */
static void SimpleLruZeroLSNs(SlruCtl ctl, int slotno)
{
    errno_t rc = EOK;
    SlruShared shared = ctl->shared;

    if (shared->lsn_groups_per_page > 0) {
        rc = memset_s(&shared->group_lsn[slotno * shared->lsn_groups_per_page],
                      shared->lsn_groups_per_page * sizeof(XLogRecPtr), 0,
                      shared->lsn_groups_per_page * sizeof(XLogRecPtr));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * Wait for any active I/O on a page slot to finish.
 *
 * Important points:
 * 1. This does not guarantee that new I/O hasn't been started before we return, though.
 * 2. In fact the slot might not even contain the same page anymore.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
void SimpleLruWaitIO(SlruCtl ctl, int slotno)
{
    volatile SlruShared shared = ctl->shared;

    /* See notes at top of file */
    LWLockRelease(shared->control_lock);
    (void)LWLockAcquire(shared->buffer_locks[slotno], LW_SHARED);
    LWLockRelease(shared->buffer_locks[slotno]);
    (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);

    /*
     * If the slot is still in an io-in-progress state, then either someone
     * already started a new I/O on the slot, or a previous I/O failed and
     * neglected to reset the page state.  That shouldn't happen, really, but
     * it seems worth a few extra cycles to check and recover from it. We can
     * cheaply test for failure by seeing if the buffer lock is still held (we
     * assume that transaction abort would release the lock).
     */
    if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
        shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS) {
        if (LWLockConditionalAcquire(shared->buffer_locks[slotno], LW_SHARED)) {
            /* indeed, the I/O must have failed */
            if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS)
                shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            else {
                shared->page_status[slotno] = SLRU_PAGE_VALID;
                shared->page_dirty[slotno] = true;
            }
            LWLockRelease(shared->buffer_locks[slotno]);
        }
    }
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 *
 * If write_ok is true then it is OK to return a page that is in
 * WRITE_IN_PROGRESS state; it is the caller's responsibility to be sure
 * that modification of the page is safe.  If write_ok is false then we
 * will not return the page until it is not undergoing active I/O.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
int SimpleLruReadPage(SlruCtl ctl, int64 pageno, bool write_ok, TransactionId xid)
{
    SlruShared shared = ctl->shared;

    /* Outer loop handles restart if we must wait for someone else's I/O */
    for (;;) {
        int slotno;
        bool ok = false;

        /* See if page already is in memory; if not, pick victim slot */
        slotno = SlruSelectLRUPage(ctl, pageno);
        /* Did we find the page in memory? */
        if (shared->page_number[slotno] == pageno && shared->page_status[slotno] != SLRU_PAGE_EMPTY) {
            /*
             * If page is still being read in, we must wait for I/O.  Likewise
             * if the page is being written and the caller said that's not OK.
             */
            if (shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS ||
                (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS && !write_ok)) {
                SimpleLruWaitIO(ctl, slotno);
                /* Now we must recheck state from the top */
                continue;
            }
            /* Otherwise, it's ready to use */
            SlruRecentlyUsed(shared, slotno);
            return slotno;
        }

        /* We found no match; assert we selected a freeable slot */
        if (!(shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
              (shared->page_status[slotno] == SLRU_PAGE_VALID && !shared->page_dirty[slotno])))
            ereport(PANIC,
                    (errmodule(MOD_SLRU), errcode(ERRCODE_DATA_EXCEPTION), errmsg("slru read page under %s", ctl->dir),
                     errdetail("page(%ld) in slot(%d) status(%d) | dirty(%d) is wrong, xid %lu", pageno, slotno,
                               shared->page_status[slotno], shared->page_dirty[slotno], xid)));

        /* Mark the slot read-busy */
        shared->page_number[slotno] = pageno;
        shared->page_status[slotno] = SLRU_PAGE_READ_IN_PROGRESS;
        shared->page_dirty[slotno] = false;

        /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
        (void)LWLockAcquire(shared->buffer_locks[slotno], LW_EXCLUSIVE);

        if (!ENABLE_DSS) {
            /* Release control lock while doing I/O */
            LWLockRelease(shared->control_lock);
        }

        /* Do the read */
        ok = SlruPhysicalReadPage(ctl, pageno, slotno);

        /* Set the LSNs for this newly read-in page to zero */
        SimpleLruZeroLSNs(ctl, slotno);

        if (!ENABLE_DSS) {
            /* Re-acquire control lock and update page state */
            (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);
        }

        if (!(shared->page_number[slotno] == pageno && shared->page_status[slotno] == SLRU_PAGE_READ_IN_PROGRESS &&
              !shared->page_dirty[slotno]))
            ereport(PANIC,
                    (errmodule(MOD_SLRU), errcode(ERRCODE_DATA_EXCEPTION), errmsg("slru read page under %s", ctl->dir),
                     errdetail("page(%ld) in slot(%d) status(%d) | number(%ld) | dirty(%d) is wrong, xid %lu", pageno,
                               slotno, shared->page_status[slotno], shared->page_number[slotno],
                               shared->page_dirty[slotno], xid)));

        shared->page_status[slotno] = ok ? SLRU_PAGE_VALID : SLRU_PAGE_EMPTY;

        LWLockRelease(shared->buffer_locks[slotno]);

        /* Now it's okay to ereport if we failed */
        if (!ok) {
            LWLockRelease(shared->control_lock);
            SlruReportIOError(ctl, pageno, xid);
        }
        SlruRecentlyUsed(shared, slotno);
        return slotno;
    }
}

/*
 * Find a page in a shared buffer, reading it in if necessary.
 * The page number must correspond to an already-initialized page.
 * The caller must intend only read-only access to the page.
 *
 * The passed-in xid is used only for error reporting, and may be
 * InvalidTransactionId if no specific xid is associated with the action.
 *
 * Return value is the shared-buffer slot number now holding the page.
 * The buffer's LRU access info is updated.
 *
 * Control lock must NOT be held at entry, but will be held at exit.
 * It is unspecified whether the lock will be shared or exclusive.
 */
int SimpleLruReadPage_ReadOnly(SlruCtl ctl, int64 pageno, TransactionId xid)
{
    /* Try to find the page while holding only shared lock */
    (void)LWLockAcquire(ctl->shared->control_lock, LW_SHARED);

    return execSimpleLruReadPageReadOnly(ctl, pageno, xid);
}

/**
 * @Description: Same as SimpleLruReadPage_ReadOnly, but the shared lock must be held by the caller
 * and will be held at exit.
 * @in ctl -  the slru control data
 * @in pageno -  the page number to read
 * @in xid - the transaction to be search
 * @return -  return the buffer slot for the input transaction
 */
int SimpleLruReadPage_ReadOnly_Locked(SlruCtl ctl, int64 pageno, TransactionId xid)
{
    Assert(LWLockHeldByMe(ctl->shared->control_lock));

    return execSimpleLruReadPageReadOnly(ctl, pageno, xid);
}

/*
 * Write a page from a shared buffer, if necessary.
 * Does nothing if the specified slot is not dirty.
 *
 * NOTE: only one write attempt is made here.  Hence, it is possible that
 * the page is still dirty at exit (if someone else re-dirtied it during
 * the write).	However, we *do* attempt a fresh write even if the page
 * is already being written; this is for checkpoints.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static void SlruInternalWritePage(SlruCtl ctl, int slotno, SlruFlush fdata)
{
    SlruShared shared = ctl->shared;
    int64 pageno = shared->page_number[slotno];
    bool ok = false;

    /* If a write is in progress, wait for it to finish */
    while (shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS && shared->page_number[slotno] == pageno) {
        SimpleLruWaitIO(ctl, slotno);
    }

    /*
     * Do nothing if page is not dirty, or if buffer no longer contains the
     * same page we were called for.
     */
    if (!shared->page_dirty[slotno] || shared->page_status[slotno] != SLRU_PAGE_VALID ||
        shared->page_number[slotno] != pageno)
        return;

    /*
     * Mark the slot write-busy, and clear the dirtybit.  After this point, a
     * transaction status update on this page will mark it dirty again.
     */
    shared->page_status[slotno] = SLRU_PAGE_WRITE_IN_PROGRESS;
    shared->page_dirty[slotno] = false;

    /* Acquire per-buffer lock (cannot deadlock, see notes at top) */
    (void)LWLockAcquire(shared->buffer_locks[slotno], LW_EXCLUSIVE);

    if (!ENABLE_DSS) {
        /* Release control lock while doing I/O */
        LWLockRelease(shared->control_lock);
    }

    /* Do the write */
    ok = SlruPhysicalWritePage(ctl, pageno, slotno, fdata);
    /* If we failed, and we're in a flush, better close the files */
    if (!ok && (fdata != NULL)) {
        int i;

        for (i = 0; i < fdata->num_files; i++)
            (void)close(fdata->fd[i]);
    }

    if (!ENABLE_DSS) {
        /* Re-acquire control lock and update page state */
        (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);
    }

    if (!(shared->page_number[slotno] == pageno && shared->page_status[slotno] == SLRU_PAGE_WRITE_IN_PROGRESS))
        ereport(PANIC,
                (errmodule(MOD_SLRU), errcode(ERRCODE_DATA_EXCEPTION), errmsg("slru write page under %s", ctl->dir),
                 errdetail("page(%ld) in slot(%d) status(%d) | number(%ld) | dirty(%d) is wrong", pageno, slotno,
                           shared->page_status[slotno], shared->page_number[slotno], shared->page_dirty[slotno])));

    /* If we failed to write, mark the page dirty again */
    if (!ok)
        shared->page_dirty[slotno] = true;

    shared->page_status[slotno] = SLRU_PAGE_VALID;

    LWLockRelease(shared->buffer_locks[slotno]);

    /* Now it's okay to ereport if we failed */
    if (!ok) {
        LWLockRelease(shared->control_lock);
        SlruReportIOError(ctl, pageno, InvalidTransactionId);
    }
}

/*
 * Wrapper of SlruInternalWritePage, for external callers.
 * fdata is always passed a NULL here.
 */
void SimpleLruWritePage(SlruCtl ctl, int slotno)
{
    SlruInternalWritePage(ctl, slotno, NULL);
}

static bool SSPreAllocSegment(int fd, SlruFlush fdata)
{
    struct stat s;
    if (fstat(fd, &s) < 0) {
        t_thrd.xact_cxt.slru_errcause = SLRU_OPEN_FAILED;
        t_thrd.xact_cxt.slru_errno = errno;
        if (fdata == NULL) {
            (void)close(fd);
        }
        return false;
    }

    int64 trunc_size = (int64)(SLRU_PAGES_PER_SEGMENT * BLCKSZ);
    if (s.st_size < trunc_size) {
        /* extend file at once to avoid dss cross-border write issue */
        pgstat_report_waitevent(WAIT_EVENT_SLRU_WRITE);
        errno = 0;
        if (fallocate(fd, 0, s.st_size, trunc_size) != 0) {
            pgstat_report_waitevent(WAIT_EVENT_END);
            if (errno == 0) {
                errno = ENOSPC;
            }
            t_thrd.xact_cxt.slru_errcause = SLRU_WRITE_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            if (fdata == NULL) {
                (void)close(fd);
            }
            return false;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    }

    return true;
}

/*
 * Physical read of a (previously existing) page into a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return FALSE and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * read/write operations.  We could cache one virtual file pointer ...
 */
static bool SlruPhysicalReadPage(SlruCtl ctl, int64 pageno, int slotno)
{
    SlruShared shared = ctl->shared;
    int64 segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int fd;
    int rc = 0;

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%04X%08X", (ctl)->dir, (uint32)((uint64)(segno) >> 32),
                    (uint32)((segno) & (int64)0xFFFFFFFF));
    securec_check_ss(rc, "", "");

    /*
     * In a crash-and-restart situation, it's possible for us to receive
     * commands to set the commit status of transactions whose bits are in
     * already-truncated segments of the commit log (see notes in
     * SlruPhysicalWritePage).	Hence, if we are t_thrd.xlog_cxt.InRecovery, allow the case
     * where the file doesn't exist, and return zeroes instead.
     */
    fd = BasicOpenFile(path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        if (errno != ENOENT || !t_thrd.xlog_cxt.InRecovery) {
            t_thrd.xact_cxt.slru_errcause = SLRU_OPEN_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            return false;
        }

        ereport(LOG, (errmodule(MOD_SLRU), errmsg("file \"%s\" doesn't exist, zero page %ld in buffer", path, pageno)));
        rc = memset_s(shared->page_buffer[slotno], BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        return true;
    }

    if (ENABLE_DSS) {
        struct stat s;
        if (fstat(fd, &s) < 0) {
            t_thrd.xact_cxt.slru_errcause = SLRU_OPEN_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            (void)close(fd);
            return false;
        }
        if (s.st_size <= offset) {
            /* extend the file*/
            int64 trunc_size = (int64)(SLRU_PAGES_PER_SEGMENT * BLCKSZ);
            if (s.st_size < trunc_size) {
                /* extend file at once to avoid dss cross-border write issue*/
                pgstat_report_waitevent(WAIT_EVENT_SLRU_WRITE);
                errno = 0;
                if (fallocate(fd, 0, s.st_size, trunc_size) != 0) {
                    pgstat_report_waitevent(WAIT_EVENT_END);
                    if (errno == 0) {
                        errno = ENOSPC;
                    }
                   t_thrd.xact_cxt.slru_errcause = SLRU_WRITE_FAILED;
                   t_thrd.xact_cxt.slru_errno = errno;
                   (void)close(fd);
                    return false;                   
                }
                pgstat_report_waitevent(WAIT_EVENT_END);
            }
        }
    } else {
        if (lseek(fd, (off_t)offset, SEEK_SET) < 0) {
            t_thrd.xact_cxt.slru_errcause = SLRU_SEEK_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            (void)close(fd);
            return false;
        }
    }

    errno = 0;
    pgstat_report_waitevent(WAIT_EVENT_SLRU_READ);
    if (pread(fd, shared->page_buffer[slotno], BLCKSZ, (off_t)offset) != BLCKSZ) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (!t_thrd.xlog_cxt.InRecovery) {
            t_thrd.xact_cxt.slru_errcause = SLRU_READ_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            (void)close(fd);
            return false;
        }

        ereport(LOG, (errmodule(MOD_SLRU), errmsg("file \"%s\" read error, zero page %ld in buffer", path, pageno)));
        rc = memset_s(shared->page_buffer[slotno], BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        (void)close(fd);
        return true;
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        t_thrd.xact_cxt.slru_errcause = SLRU_CLOSE_FAILED;
        t_thrd.xact_cxt.slru_errno = errno;
        return false;
    }

    return true;
}

/*
 * Physical write of a page from a buffer slot
 *
 * On failure, we cannot just ereport(ERROR) since caller has put state in
 * shared memory that must be undone.  So, we return FALSE and save enough
 * info in static variables to let SlruReportIOError make the report.
 *
 * For now, assume it's not worth keeping a file pointer open across
 * independent read/write operations.  We do batch operations during
 * SimpleLruFlush, though.
 *
 * fdata is NULL for a standalone write, pointer to open-file info during
 * SimpleLruFlush.
 */
static bool SlruPhysicalWritePage(SlruCtl ctl, int64 pageno, int slotno, SlruFlush fdata)
{
    SlruShared shared = ctl->shared;
    int64 segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int fd = -1;
    int rc = 0;

    /*
     * Honor the write-WAL-before-data rule, if appropriate, so that we do not
     * write out data before associated WAL records.  This is the same action
     * performed during FlushBuffer() in the main buffer manager.
     */
    if (shared->group_lsn != NULL) {
        /*
         * We must determine the largest async-commit LSN for the page. This
         * is a bit tedious, but since this entire function is a slow path
         * anyway, it seems better to do this here than to maintain a per-page
         * LSN variable (which'd need an extra comparison in the
         * transaction-commit path).
         */
        XLogRecPtr max_lsn;
        int lsnindex, lsnoff;

        lsnindex = slotno * shared->lsn_groups_per_page;
        max_lsn = shared->group_lsn[lsnindex++];
        for (lsnoff = 1; lsnoff < shared->lsn_groups_per_page; lsnoff++) {
            XLogRecPtr this_lsn = shared->group_lsn[lsnindex++];

            if (XLByteLT(max_lsn, this_lsn))
                max_lsn = this_lsn;
        }

        if (!XLogRecPtrIsInvalid(max_lsn)) {
            /*
             * As noted above, ereport ERROR is not acceptable here, so if
             * XLogFlush were to fail, we must PANIC.  This isn't much of a
             * restriction because XLogFlush is just about all critical
             * section anyway, but let's make sure.
             */
            START_CRIT_SECTION();
            XLogWaitFlush(max_lsn);
            END_CRIT_SECTION();
        }
    }

    /*
     * During a Flush, we may already have the desired file open.
     */
    if (fdata != NULL) {
        int i;

        for (i = 0; i < fdata->num_files; i++) {
            if (fdata->segno[i] == segno) {
                fd = fdata->fd[i];
                break;
            }
        }
    }

    if (fd < 0) {
        /*
         * If the file doesn't already exist, we should create it.  It is
         * possible for this to need to happen when writing a page that's not
         * first in its segment; we assume the OS can cope with that. (Note:
         * it might seem that it'd be okay to create files only when
         * SimpleLruZeroPage is called for the first page of a segment.
         * However, if after a crash and restart the REDO logic elects to
         * replay the log from a checkpoint before the latest one, then it's
         * possible that we will get commands to set transaction status of
         * transactions that have already been truncated from the commit log.
         * Easiest way to deal with that is to accept references to
         * nonexistent files here and in SlruPhysicalReadPage.)
         *
         * Note: it is possible for more than one backend to be executing this
         * code simultaneously for different pages of the same file. Hence,
         * don't use O_EXCL or O_TRUNC or anything like that.
         */
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%04X%08X", (ctl)->dir, (uint32)((uint64)(segno) >> 32),
                        (uint32)((segno) & (int64)0xFFFFFFFF));
        securec_check_ss(rc, "", "");
        fd = BasicOpenFile(path, O_RDWR | O_CREAT | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            t_thrd.xact_cxt.slru_errcause = SLRU_OPEN_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            return false;
        }

        if (fdata != NULL) {
            if (fdata->num_files < MAX_FLUSH_BUFFERS) {
                fdata->fd[fdata->num_files] = fd;
                fdata->segno[fdata->num_files] = segno;
                fdata->num_files++;
            } else {
                /*
                 * In the unlikely event that we exceed MAX_FLUSH_BUFFERS,
                 * fall back to treating it as a standalone write.
                 */
                fdata = NULL;
            }
        }
    }

    if (lseek(fd, (off_t)offset, SEEK_SET) < 0) {
        bool failed = true;
        if (ENABLE_DSS && errno == ERR_DSS_FILE_SEEK) {
            if (!SSPreAllocSegment(fd, fdata)) {
                return false;
            }
            if (lseek(fd, (off_t)offset, SEEK_SET) >= 0) {
                failed = false;
            }
        }

        if (failed) {
            t_thrd.xact_cxt.slru_errcause = SLRU_SEEK_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            if (fdata == NULL) {
                (void)close(fd);
            }
            return false;
        }
    }
    
    if (SS_STANDBY_PROMOTING) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("DMS standby can't write slru page for switchover")));
        return true;
    }

    if (SS_STANDBY_MODE && strlen(shared->page_buffer[slotno])) {
        force_backtrace_messages = true;
        ereport(PANIC, (errmodule(MOD_DMS), errmsg("DMS standby can't write to disk")));
    }

    errno = 0;
    pgstat_report_waitevent(WAIT_EVENT_SLRU_WRITE);
    if (write(fd, shared->page_buffer[slotno], BLCKSZ) != BLCKSZ) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;
        t_thrd.xact_cxt.slru_errcause = SLRU_WRITE_FAILED;
        t_thrd.xact_cxt.slru_errno = errno;
        if (fdata == NULL)
            (void)close(fd);
        return false;
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    /*
     * If not part of Flush, need to fsync now.  We assume this happens
     * infrequently enough that it's not a performance issue.
     */
    if (fdata == NULL) {
        pgstat_report_waitevent(WAIT_EVENT_SLRU_SYNC);
        if (ctl->do_fsync && pg_fsync(fd)) {
            pgstat_report_waitevent(WAIT_EVENT_END);
            t_thrd.xact_cxt.slru_errcause = SLRU_FSYNC_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            (void)close(fd);
            return false;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        if (close(fd)) {
            t_thrd.xact_cxt.slru_errcause = SLRU_CLOSE_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            return false;
        }
    }

    return true;
}

/*
 * Issue the error message after failure of SlruPhysicalReadPage or
 * SlruPhysicalWritePage.  Call this after cleaning up shared-memory state.
 */
static void SlruReportIOError(SlruCtl ctl, int64 pageno, TransactionId xid)
{
    int64 segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    int offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int rc = 0;

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%04X%08X", (ctl)->dir, (uint32)((uint64)(segno) >> 32),
                    (uint32)((segno) & (int64)0xFFFFFFFF));
    securec_check_ss(rc, "", "");
    errno = t_thrd.xact_cxt.slru_errno;
    switch (t_thrd.xact_cxt.slru_errcause) {
        case SLRU_OPEN_FAILED:
            ereport(ERROR, (errmodule(MOD_SLRU), errcode_for_file_access(),
                            errmsg("could not access status of transaction %lu , nextXid is %lu, pageno %ld, "
                                   "t_thrd.pgxact->xmin %lu",
                                   xid, t_thrd.xact_cxt.ShmemVariableCache->nextXid, pageno, t_thrd.pgxact->xmin),
                            errdetail("Could not open file \"%s\": %m.", path)));
            break;
        case SLRU_SEEK_FAILED:
            ereport(ERROR, (errmodule(MOD_SLRU), errcode_for_file_access(),
                            errmsg("could not access status of transaction %lu, nextXid is  %lu", xid,
                                   t_thrd.xact_cxt.ShmemVariableCache->nextXid),
                            errdetail("Could not seek in file \"%s\" to offset %d: %m.", path, offset)));
            break;
        case SLRU_READ_FAILED:
            ereport(ERROR, (errmodule(MOD_SLRU), errcode_for_file_access(),
                            errmsg("could not access status of transaction  %lu, nextXid is %lu", xid,
                                   t_thrd.xact_cxt.ShmemVariableCache->nextXid),
                            errdetail("Could not read from file \"%s\" at offset %d: %m.", path, offset)));
            break;
        case SLRU_WRITE_FAILED:
            ereport(ERROR, (errmodule(MOD_SLRU), errcode_for_file_access(),
                            errmsg("could not access status of transaction  %lu", xid),
                            errdetail("Could not write to file \"%s\" at offset %d: %m.", path, offset)));
            break;
        case SLRU_FSYNC_FAILED:
            ereport(data_sync_elevel(ERROR), (errmodule(MOD_SLRU), errcode_for_file_access(),
                                              errmsg("could not access status of transaction %lu", xid),
                                              errdetail("Could not fsync file \"%s\": %m.", path)));
            break;
        case SLRU_CLOSE_FAILED:
            ereport(ERROR, (errmodule(MOD_SLRU), errcode_for_file_access(),
                            errmsg("could not access status of transaction %lu", xid),
                            errdetail("Could not close file \"%s\": %m.", path)));
            break;
        default:
            /* can't get here, we trust */
            ereport(ERROR, (errmodule(MOD_SLRU), errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("unrecognized SimpleLru error cause: %d", (int)t_thrd.xact_cxt.slru_errcause)));
            break;
    }
}

/*
 * Select the slot to re-use when we need a free slot.
 *
 * The target page number is passed because we need to consider the
 * possibility that some other process reads in the target page while
 * we are doing I/O to free a slot.  Hence, check or recheck to see if
 * any slot already holds the target page, and return that slot if so.
 * Thus, the returned slot is *either* a slot already holding the pageno
 * (could be any state except EMPTY), *or* a freeable slot (state EMPTY
 * or CLEAN).
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int SlruSelectLRUPage(SlruCtl ctl, int64 pageno)
{
    SlruShared shared = ctl->shared;

    /* Outer loop handles restart after I/O */
    for (;;) {
        int slotno;
        int cur_count;
        int best_valid_slot = 0; /* keep compiler quiet */
        int best_valid_delta = -1;
        int64 best_valid_page_number = 0; /* keep compiler quiet */
        int best_invalid_slot = 0;        /* keep compiler quiet */
        int best_invalid_delta = -1;
        int64 best_invalid_page_number = 0; /* keep compiler quiet */

        /* See if page already has a buffer assigned */
        for (slotno = 0; slotno < shared->num_slots; slotno++) {
            if (shared->page_number[slotno] == pageno && shared->page_status[slotno] != SLRU_PAGE_EMPTY)
                return slotno;
        }

        /*
         * If we find any EMPTY slot, just select that one. Else choose a
         * victim page to replace.	We normally take the least recently used
         * valid page, but we will never take the slot containing
         * latest_page_number, even if it appears least recently used.	We
         * will select a slot that is already I/O busy only if there is no
         * other choice: a read-busy slot will not be least recently used once
         * the read finishes, and waiting for an I/O on a write-busy slot is
         * inferior to just picking some other slot.  Testing shows the slot
         * we pick instead will often be clean, allowing us to begin a read at
         * once.
         *
         * Normally the page_lru_count values will all be different and so
         * there will be a well-defined LRU page.  But since we allow
         * concurrent execution of SlruRecentlyUsed() within
         * SimpleLruReadPage_ReadOnly(), it is possible that multiple pages
         * acquire the same lru_count values.  In that case we break ties by
         * choosing the furthest-back page.
         *
         * Notice that this next line forcibly advances cur_lru_count to a
         * value that is certainly beyond any value that will be in the
         * page_lru_count array after the loop finishes.  This ensures that
         * the next execution of SlruRecentlyUsed will mark the page newly
         * used, even if it's for a page that has the current counter value.
         * That gets us back on the path to having good data when there are
         * multiple pages with the same lru_count.
         */
        cur_count = (shared->cur_lru_count)++;
        for (slotno = 0; slotno < shared->num_slots; slotno++) {
            int this_delta;
            int64 this_page_number;

            if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
                return slotno;
            this_delta = cur_count - shared->page_lru_count[slotno];
            if (this_delta < 0) {
                /*
                 * Clean up in case shared updates have caused cur_count
                 * increments to get "lost".  We back off the page counts,
                 * rather than trying to increase cur_count, to avoid any
                 * question of infinite loops or failure in the presence of
                 * wrapped-around counts.
                 */
                shared->page_lru_count[slotno] = cur_count;
                this_delta = 0;
            }
            this_page_number = shared->page_number[slotno];
            if (this_page_number == shared->latest_page_number)
                continue;
            if (shared->page_status[slotno] == SLRU_PAGE_VALID) {
                if (this_delta > best_valid_delta ||
                    (this_delta == best_valid_delta && this_page_number < best_valid_page_number)) {
                    best_valid_slot = slotno;
                    best_valid_delta = this_delta;
                    best_valid_page_number = this_page_number;
                }
            } else {
                if (this_delta > best_invalid_delta ||
                    (this_delta == best_invalid_delta && this_page_number < best_valid_page_number)) {
                    best_invalid_slot = slotno;
                    best_invalid_delta = this_delta;
                    best_invalid_page_number = this_page_number;
                }
            }
        }

        /*
         * If all pages (except possibly the latest one) are I/O busy, we'll
         * have to wait for an I/O to complete and then retry.	In that
         * unhappy case, we choose to wait for the I/O on the least recently
         * used slot, on the assumption that it was likely initiated first of
         * all the I/Os in progress and may therefore finish first.
         */
        if (best_valid_delta < 0) {
            SimpleLruWaitIO(ctl, best_invalid_slot);
            continue;
        }

        /*
         * If the selected page is clean, we're set.
         */
        if (!shared->page_dirty[best_valid_slot])
            return best_valid_slot;

        /*
         * Write the page.
         */
        SlruInternalWritePage(ctl, best_valid_slot, NULL);

        /*
         * Now loop back and try again.  This is the easiest way of dealing
         * with corner cases such as the victim page being re-dirtied while we
         * wrote it.
         */
    }
}

/*
 * Flush dirty pages to disk during checkpoint or database shutdown
 */
int SimpleLruFlush(SlruCtl ctl, bool checkpoint)
{
    SlruShared shared = ctl->shared;
    SlruFlushData fdata = { 0, {0}, {0}};
    int slotno;
    int64 pageno = 0;
    int i;
    bool ok = false;

    /*
     * Find and write dirty pages
     */
    fdata.num_files = 0;

    (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);

    for (slotno = 0; slotno < shared->num_slots; slotno++) {
        SlruInternalWritePage(ctl, slotno, &fdata);

        /*
         * When called during a checkpoint, we cannot assert that the slot is
         * clean now, since another process might have re-dirtied it already.
         * That's okay.
         */
        Assert(checkpoint || shared->page_status[slotno] == SLRU_PAGE_EMPTY ||
               (shared->page_status[slotno] == SLRU_PAGE_VALID && !shared->page_dirty[slotno]));
    }

    LWLockRelease(shared->control_lock);

    /*
     * Now fsync and close any files that were open
     */
    ok = true;
    for (i = 0; i < fdata.num_files; i++) {
        pgstat_report_waitevent(WAIT_EVENT_SLRU_FLUSH_SYNC);
        if (ctl->do_fsync && pg_fsync(fdata.fd[i])) {
            t_thrd.xact_cxt.slru_errcause = SLRU_FSYNC_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
            ok = false;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        if (close(fdata.fd[i])) {
            t_thrd.xact_cxt.slru_errcause = SLRU_CLOSE_FAILED;
            t_thrd.xact_cxt.slru_errno = errno;
            pageno = fdata.segno[i] * SLRU_PAGES_PER_SEGMENT;
            ok = false;
        }
    }
    if (!ok)
        SlruReportIOError(ctl, pageno, InvalidTransactionId);

    return fdata.num_files;
}

/*
 * Remove all segments before the one holding the passed page number
 */
void SimpleLruTruncate(SlruCtl ctl, int64 cutoffPage, int partitionNum)
{
    if (SS_STANDBY_MODE) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("DMS standby can't truncate slru page")));
        return;
    }

    SlruShared shared = NULL;
    int64 slotno;
    bool isCsnLogCtl = false;
    bool isPart = (partitionNum > NUM_SLRU_DEFAULT_PARTITION);

    /* check whether the slru file is csnlog */
    isCsnLogCtl = strcmp(ctl->dir, CSNLOGDIR) == 0;
    
    /*
     * The cutoff point is the start of the segment containing cutoffPage.
     */
    cutoffPage -= cutoffPage % SLRU_PAGES_PER_SEGMENT;

    /*
     * Scan shared memory and remove any pages preceding the cutoff page, to
     * ensure we won't rewrite them later.  (Since this is normally called in
     * or just after a checkpoint, any dirty pages should have been flushed
     * already ... we're just being extra careful here.)
     */
    for (int i = 0; i < partitionNum; i++) {
        shared = (ctl + i)->shared;
        (void)LWLockAcquire(shared->control_lock, LW_EXCLUSIVE);
    
restart:;

        /*
         * While we are holding the lock, make an important safety check: the
         * planned cutoff point must be <= the current endpoint page. Otherwise we
         * have already wrapped around, and proceeding with the truncation would
         * risk removing the current segment. If we use partitioned slru ctl, no need
         * to check the lateset_page_number.
         */
        if (shared->latest_page_number < cutoffPage && !isPart) {
            LWLockRelease(shared->control_lock);
            ereport(LOG, (errmodule(MOD_SLRU),
                errmsg("could not truncate directory \"%s\": apparent wraparound", ctl->dir)));
            return;
        }

        for (slotno = 0; slotno < shared->num_slots; slotno++) {
            if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
                continue;
            if (shared->page_number[slotno] >= cutoffPage)
                continue;

            /*
             * If page is clean, just change state to EMPTY (expected case).
             */
            if (shared->page_status[slotno] == SLRU_PAGE_VALID && !shared->page_dirty[slotno]) {
                shared->page_status[slotno] = SLRU_PAGE_EMPTY;
                continue;
            }

            /*
             * Hmm, we have (or may have) I/O operations acting on the page, so
             * we've got to wait for them to finish and then start again. This is
             * the same logic as in SlruSelectLRUPage.	(XXX if page is dirty,
             * wouldn't it be OK to just discard it without writing it?  For now,
             * keep the logic the same as it was.) Csnlog just discard it without writing it.
             */
            if (shared->page_status[slotno] == SLRU_PAGE_VALID) {
                if (isCsnLogCtl) {
                    shared->page_status[slotno] = SLRU_PAGE_EMPTY;
                    continue;
                }
                SlruInternalWritePage((ctl + i), slotno, NULL);
            } else {
                SimpleLruWaitIO((ctl + i), slotno);
            }     
            goto restart;
        }

        LWLockRelease(shared->control_lock);
    }
    ereport(LOG, (errmodule(MOD_SLRU), errmsg("remove old segments(<%ld) under %s", cutoffPage, ctl->dir)));

    /* Now we can remove the old segment(s) */
    (void)SlruScanDirectory(ctl, SlruScanDirCbDeleteCutoff, &cutoffPage);
}

/*
 * SlruScanDirectory callback
 *		This callback reports true if there's any segment prior to the one
 *		containing the page passed as "data".
 */
bool SlruScanDirCbReportPresence(SlruCtl ctl, const char *filename, int64 segpage, const void *data)
{
    int64 cutoffPage = *(int64 *)data;

    cutoffPage -= cutoffPage % SLRU_PAGES_PER_SEGMENT;

    if (segpage < cutoffPage)
        return true; /* found one; don't iterate any more */

    return false; /* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes segments prior to the one passed in as "data".
 */
bool SlruScanDirCbDeleteCutoff(SlruCtl ctl, const char *filename, int64 segpage, const void *data)
{
    char path[MAXPGPATH];
    int64 cutoffPage = *(int64 *)data;
    int rc = 0;

    if (segpage < cutoffPage) {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", ctl->dir, filename);
        securec_check_ss(rc, "\0", "\0");
        ereport(DEBUG2, (errmodule(MOD_SLRU), errmsg("removing file \"%s\"", path)));
        (void)unlink(path);
    }

    return false; /* keep going */
}

/*
 * SlruScanDirectory callback.
 *		This callback deletes all segments.
 */
bool SlruScanDirCbDeleteAll(SlruCtl ctl, const char *filename, int64 segpage, const void *data)
{
    char path[MAXPGPATH];
    int rc = 0;

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", ctl->dir, filename);
    securec_check_ss(rc, "\0", "\0");
    ereport(DEBUG2, (errmodule(MOD_SLRU), errmsg("removing file \"%s\"", path)));
    (void)unlink(path);

    return false; /* keep going */
}

/*
 * Scan the SimpleLRU directory and apply a callback to each file found in it.
 *
 * If the callback returns true, the scan is stopped.  The last return value
 * from the callback is returned.
 *
 * Note that the ordering in which the directory is scanned is not guaranteed.
 *
 * Note that no locking is applied.
 */
bool SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, const void *data)
{
    bool retval = false;
    DIR *cldir = NULL;
    struct dirent *clde = NULL;
    int64 segno;
    int64 segpage;

    cldir = AllocateDir(ctl->dir);
    while ((clde = ReadDir(cldir, ctl->dir)) != NULL) {
        if (strlen(clde->d_name) == 12 && strspn(clde->d_name, "0123456789ABCDEF") == 12) {
            segno = (int64)pg_strtouint64(clde->d_name, NULL, 16);
            segpage = segno * SLRU_PAGES_PER_SEGMENT;

            ereport(DEBUG2, (errmodule(MOD_SLRU),
                             errmsg("SlruScanDirectory invoking callback on %s/%s", ctl->dir, clde->d_name)));
            retval = callback(ctl, clde->d_name, segpage, data);
            if (retval)
                break;
        }
    }
    (void)FreeDir(cldir);

    return retval;
}

void SimpleLruSetPageEmpty(SlruCtl ctl, const char *name, int trancheId, int nslots, int nlsns, LWLock *ctllock,
    const char *subdir, int index)
{
    bool found = false;
    int slotno;
    SlruShared shared = (SlruShared)ShmemInitStruct(name, SimpleLruShmemSize(nslots, nlsns), &found);

    for (slotno = 0; slotno < nslots; slotno++) {
        shared->page_status[slotno] = SLRU_PAGE_EMPTY;
    }
}

#ifdef ENABLE_UT

void ut_SetErrCause(int errcause)
{
    t_thrd.xact_cxt.slru_errcause = (SlruErrorCause)errcause;
}

void ut_SlruReportIOError(SlruCtl ctl, int64 pageno, TransactionId xid)
{
    SlruReportIOError(ctl, pageno, xid);
}

#endif
