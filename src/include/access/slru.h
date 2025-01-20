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
 * To avoid overflowing internal arithmetic and the size_t data type, the
 * number of buffers must not exceed this number.
 */
#define SLRU_MAX_ALLOWED_BUFFERS ((1024 * 1024 * 1024) / BLCKSZ)

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
 * Bank size for the slot array.  Pages are assigned a bank according to their
 * page number, with each bank being this size.  We want a power of 2 so that
 * we can determine the bank number for a page with just bit shifting; we also
 * want to keep the bank size small so that LRU victim search is fast.  16
 * buffers per bank seems a good number.
 */
#define SLRU_BANK_BITSHIFT		4
#define SLRU_BANK_SIZE			(1 << SLRU_BANK_BITSHIFT)

/*
 * Macro to get the bank number to which the slot belongs.
 */
#define SlotGetBankNumber(slotno)	((slotno) >> SLRU_BANK_BITSHIFT)

#define MAX_SLRU_PARTITION_SIZE (256)

#define SLRU_PARTITION_SIZE_RATE (512)

/*
 * Macro to switch between bank locks. If the old lock is different from the new lock,
 * it releases the old lock (if it's not NULL) and acquires the new lock in exclusive mode.
 * Then it updates the old lock to the new lock.
 */
#define SlruBankLockSwitch(oldLock, newLock, lockMode) do { \
    if ((oldLock) != (newLock)) {                           \
        if ((oldLock)!= NULL) {                             \
            LWLockRelease(oldLock);                         \
        }                                                   \
        LWLockAcquire(newLock, LW_EXCLUSIVE);               \
        oldLock = newLock;                                  \
    }                                                       \
} while (0)

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

const int MAX_SLRU_BUFFER_NAME_LENGTH = 64;
typedef struct SlruBufferDesc {
    char name[MAX_SLRU_BUFFER_NAME_LENGTH];
    int defaultBufferNum;
    int minBufferNum;
    int maxBufferNum;
} SlruBufferDesc;

const struct SlruBufferDesc SLRU_BUFFER_INFO[] = {
    {"MXACT_OFFSET", 16, 16, SLRU_MAX_ALLOWED_BUFFERS},
    {"MXACT_MEMBER", 16, 16, SLRU_MAX_ALLOWED_BUFFERS}
};

/*
 * Shared-memory state
 */
typedef struct SlruSharedData {
    /* enable for clog, csnlog, multixact offset log, multixact member log */
    bool enable_banks;

    /* control_lock == NULL if enable_banks */
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

    /* The buffer_locks protects the I/O on each buffer slots */
    LWLock** buffer_locks;

    /* Locks to protect the in memory buffer slot access in SLRU bank. */
    LWLock** bank_locks;

    /*----------
     * A bank-wise LRU counter is maintained because we do a victim buffer
     * search within a bank. Furthermore, manipulating an individual bank
     * counter avoids frequent cache invalidation since we update it every time
     * we access the page.
     *
     * We mark a page "most recently used" by setting
     *		page_lru_count[slotno] = ++bank_cur_lru_count[bankno];
     * The oldest page in the bank is therefore the one with the highest value
     * of
     * 		bank_cur_lru_count[bankno] - page_lru_count[slotno]
     * The counts will eventually wrap around, but this calculation still
     * works as long as no page's age exceeds INT_MAX counts.
     *----------
     */
    int* bank_cur_lru_count;

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
     * Only used if enable_banks is false.
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
    pg_atomic_uint32 clogGroupFirst;

} SlruSharedData;

typedef SlruSharedData* SlruShared;

/*
 * SlruCtlData is an unshared structure that points to the active information
 * in shared memory.
 */
typedef struct SlruCtlData {
    SlruShared shared;

    /*
     * Bitmask to determine bank number from page number
     */
    uint16 bank_mask;

    /*
     * Num of slru partition, used in clog and csnlog, used to calculate the bankno by pageno.
     */
    int slru_partition_num;

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


static inline int SimpleLruGetBankno(SlruCtl ctl, int64 pageno)
{
    return (pageno / (int64)ctl->slru_partition_num) & ctl->bank_mask;
}

/*
 * Get the SLRU bank lock for given SlruCtl and the pageno.
 *
 * This lock needs to be acquired to access the slru buffer slots in the
 * respective bank.
 */
static inline LWLock* SimpleLruGetBankLock(SlruCtl ctl, int64 pageno)
{
    int	bankno;

    bankno = SimpleLruGetBankno(ctl, pageno);
    Assert(ctl->shared->bank_locks[bankno] != NULL);
    return ctl->shared->bank_locks[bankno];
}

extern Size SimpleLruShmemSize(int nslots, int nlsns, bool enableBank = false);
extern void SimpleLruInit(
    SlruCtl ctl, const char *name, int bufferTrancheId, int bankTrancheId, int nslots,
    int nlsns, LWLock *ctllock, const char *subdir, int index = 0, bool enableBank = false, int partitionNum = 1);
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

extern void SimpleLruSetPageEmpty(
    SlruCtl ctl, const char* name, int trancheId, int nslots, int nlsns, const char* subdir, int index = 0);

extern void SetSlruBufferDefaultNum(void);
extern void CheckAndSetSlruBufferInfo(const List* res);
extern void CheckSlruBufferNumRange(void);

#endif /* SLRU_H */
