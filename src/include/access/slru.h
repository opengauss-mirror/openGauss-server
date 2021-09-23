/* -------------------------------------------------------------------------
 *
 * slru.h
 *		Simple LRU buffering for transaction status logfiles
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slru.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SLRU_H
#define SLRU_H

#include "access/xlogdefs.h"
#include "storage/lock/lwlock.h"

/*
 * Define SLRU segment size.  A page is the same BLCKSZ as is used everywhere
 * else in openGauss.  The segment size can be chosen somewhat arbitrarily;
 * we make it 2048 pages by default, or 16Mb, i.e. 64M transactions for CLOG
 * or 2M transactions for SUBTRANS.
 *
 * Note: slru.c currently assumes that segment file names will be twelve hex
 * digits.	This sets a lower bound on the segment size ( 4M transactions
 * for 64-bit TransactionIds).
 */
#define SLRU_PAGES_PER_SEGMENT 2048

/* Maximum length of an SLRU name */
#define SLRU_MAX_NAME_LENGTH 64

#define NUM_SLRU_DEFAULT_PARTITION 1
/*
 * Page status codes.  Note that these do not include the "dirty" bit.
 * page_dirty can be TRUE only in the VALID or WRITE_IN_PROGRESS states;
 * in the latter case it implies that the page has been re-dirtied since
 * the write started.
 */
typedef enum {
    SLRU_PAGE_EMPTY,             /* buffer is not in use */
    SLRU_PAGE_READ_IN_PROGRESS,  /* page is being read in */
    SLRU_PAGE_VALID,             /* page is valid and not being written */
    SLRU_PAGE_WRITE_IN_PROGRESS, /* page is being written out */
} SlruPageStatus;

/* Saved info for SlruReportIOError */
typedef enum {
    SLRU_OPEN_FAILED,
    SLRU_SEEK_FAILED,
    SLRU_READ_FAILED,
    SLRU_WRITE_FAILED,
    SLRU_FSYNC_FAILED,
    SLRU_CLOSE_FAILED,
    SLRU_MAX_FAILED  // used to initialize slru_errcause
} SlruErrorCause;

/*
 * Shared-memory state
 */
typedef struct SlruSharedData {
    LWLock* control_lock;

    /* Number of buffers managed by this SLRU structure */
    int num_slots;

    /*
     * Arrays holding info for each buffer slot.  Page number is undefined
     * when status is EMPTY, as is page_lru_count.
     */
    char** page_buffer;
    SlruPageStatus* page_status;
    bool* page_dirty;
    int64* page_number;
    int* page_lru_count;
    LWLock** buffer_locks;

    /*
     * Optional array of WAL flush LSNs associated with entries in the SLRU
     * pages.  If not zero/NULL, we must flush WAL before writing pages (true
     * for pg_clog, false for multixact, pg_subtrans, pg_notify).  group_lsn[]
     * has lsn_groups_per_page entries per buffer slot, each containing the
     * highest LSN known for a contiguous group of SLRU entries on that slot's
     * page.
     */
    XLogRecPtr* group_lsn;
    int lsn_groups_per_page;

    /* ----------
     * We mark a page "most recently used" by setting
     *		page_lru_count[slotno] = ++cur_lru_count;
     * The oldest page is therefore the one with the highest value of
     *		cur_lru_count - page_lru_count[slotno]
     * The counts will eventually wrap around, but this calculation still
     * works as long as no page's age exceeds INT_MAX counts.
     * ----------
     */
    int cur_lru_count;

    /*
     * latest_page_number is the page number of the current end of the log in PG,
     * this is not critical data, since we use it only to avoid swapping out
     * the latest page. But not in MPPDB. Older MPPDB version use it to keep the
     * latest active page. The latest active page is same as the current end page
     * of the log in PG, but keep in mind that these two might be different in MPPDB
     * as the larger xid might occur in datanode before minor xid.
     */
    int64 latest_page_number;

    /*
     * In StartupCLOG or TrimCLOG during recovery, latest_page_number is determined
     * by the computed ShmemVariableCache->nextXid after replaying all xlog records.
     * If ShmemVariableCache->nextXid happens to be the first xid of a new clog page,
     * extension of this page will not occur due to the shortcut mechanism of ExtendCLOG.
     * We distinguish such scenario by setting force_check_first_xid true and bypass the shortcut
     * in order not to miss the clog page extension.
     */
    bool force_check_first_xid;

} SlruSharedData;

typedef SlruSharedData* SlruShared;

/*
 * SlruCtlData is an unshared structure that points to the active information
 * in shared memory.
 */
typedef struct SlruCtlData {
    SlruShared shared;

    /*
     * This flag tells whether to fsync writes (true for pg_clog and multixact
     * stuff, false for pg_subtrans and pg_notify).
     */
    bool do_fsync;

    /*
     * Dir is set during SimpleLruInit and does not change thereafter. Since
     * it's always the same, it doesn't need to be in shared memory.
     */
    char dir[64];
} SlruCtlData;

typedef SlruCtlData* SlruCtl;

extern Size SimpleLruShmemSize(int nslots, int nlsns);
extern void SimpleLruInit(
    SlruCtl ctl, const char* name, int trancheId, int nslots, int nlsns, LWLock* ctllock, const char* subdir, int index = 0);
extern int SimpleLruZeroPage(SlruCtl ctl, int64 pageno, bool* pbZeroPage = NULL);
extern int SimpleLruReadPage(SlruCtl ctl, int64 pageno, bool write_ok, TransactionId xid);
extern int SimpleLruReadPage_ReadOnly(SlruCtl ctl, int64 pageno, TransactionId xid);
extern int SimpleLruReadPage_ReadOnly_Locked(SlruCtl ctl, int64 pageno, TransactionId xid);
extern void SimpleLruWritePage(SlruCtl ctl, int slotno);
extern int SimpleLruFlush(SlruCtl ctl, bool checkpoint);
extern void SimpleLruTruncate(SlruCtl ctl, int64 cutoffPage, int partitionNum);

typedef bool (*SlruScanCallback)(SlruCtl ctl, const char* filename, int64 segpage, const void* data);
extern bool SlruScanDirectory(SlruCtl ctl, SlruScanCallback callback, const void* data);

/* SlruScanDirectory public callbacks */
extern bool SlruScanDirCbReportPresence(SlruCtl ctl, const char* filename, int64 segpage, const void* data);
extern bool SlruScanDirCbDeleteAll(SlruCtl ctl, const char* filename, int64 segpage, const void* data);

#ifdef ENABLE_UT

extern void ut_SetErrCause(int errcause);
extern void ut_SlruReportIOError(SlruCtl ctl, int64 pageno, TransactionId xid);

#endif

extern void SimpleLruWaitIO(SlruCtl ctl, int slotno);
extern bool SlruScanDirCbDeleteCutoff(SlruCtl ctl, const char* filename, int64 segpage, const void* data);

#endif /* SLRU_H */
