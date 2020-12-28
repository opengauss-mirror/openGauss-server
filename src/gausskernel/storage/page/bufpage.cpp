/* -------------------------------------------------------------------------
 *
 * bufpage.cpp
 *	  POSTGRES standard buffer page code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/page/bufpage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/htup.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "storage/checksum.h"
#include "storage/pagecompress.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/aiomem.h"

static const uint16 PAGE_CHECKSUM_MAGIC = 0xFFFF;

/* ----------------------------------------------------------------
 *						Page support functions
 * ----------------------------------------------------------------
 */


/*
 * PageIsVerified
 *		Check that the page header and checksum (if any) appear valid.
 *
 * This is called when a page has just been read in from disk.  The idea is
 * to cheaply detect trashed pages before we go nuts following bogus item
 * pointers, testing invalid transaction identifiers, etc.
 *
 * It turns out to be necessary to allow zeroed pages here too.  Even though
 * this routine is *not* called when deliberately adding a page to a relation,
 * there are scenarios in which a zeroed page might be found in a table.
 * (Example: a backend extends a relation, then crashes before it can write
 * any WAL entry about the new page.  The kernel will already have the
 * zeroed page in the file, and it will stay that way after restart.)  So we
 * allow zeroed pages here, and are careful that the page access macros
 * treat such a page as empty and without free space.  Eventually, VACUUM
 * will clean up such a page and make it usable.
 */
bool PageIsVerified(Page page, BlockNumber blkno)
{
    PageHeader p = (PageHeader)page;
    size_t* pagebytes = NULL;
    int i;
    bool checksum_failure = false;
    bool header_sane = false;
    bool all_zeroes = false;
    uint16 checksum = 0;

    /*
     * Don't verify page data unless the page passes basic non-zero test
     */
    if (!PageIsNew(page)) {
        if (PageIsChecksumByFNV1A(page)) {
            checksum = pg_checksum_page((char*)page, blkno);
            if (checksum != p->pd_checksum) {
                checksum_failure = true;
            }
        }

        /*
         * The following checks don't prove the header is correct, only that
         * it looks sane enough to allow into the buffer pool. Later usage of
         * the block can still reveal problems, which is why we offer the
         * checksum option.
         */
        if ((p->pd_flags & ~PD_VALID_FLAG_BITS) == 0 && p->pd_lower <= p->pd_upper && p->pd_upper <= p->pd_special &&
            p->pd_special <= BLCKSZ && p->pd_special == MAXALIGN(p->pd_special)) {
            header_sane = true;
        }

        if (header_sane && !checksum_failure) {
            return true;
        }
    }

    /*
     * Check all-zeroes case. Luckily BLCKSZ is guaranteed to always be a
     * multiple of size_t - and it's much faster to compare memory using the
     * native word size.
     */
    StaticAssertStmt(
        BLCKSZ == (BLCKSZ / sizeof(size_t)) * sizeof(size_t), "BLCKSZ has to be a multiple of sizeof(size_t)");

    all_zeroes = true;
    pagebytes = (size_t*)page;
    for (i = 0; i < (int)(BLCKSZ / sizeof(size_t)); i++) {
        if (pagebytes[i] != 0) {
            all_zeroes = false;
            break;
        }
    }

    if (all_zeroes) {
        return true;
    }

    /*
     * Throw a WARNING if the checksum fails, but only after we've checked for
     * the all-zeroes case.
     */
    if (checksum_failure) {
        ereport(WARNING,
            (ERRCODE_DATA_CORRUPTED,
                errmsg("page verification failed, calculated checksum %hu but expected %hu, the block num is %u",
                    checksum,
                    p->pd_checksum,
                    blkno)));

        if (header_sane && u_sess->attr.attr_common.ignore_checksum_failure) {
            return true;
        }
    }

    return false;
}

static inline bool CheckPageZeroCases(const PageHeader page)
{
    return page->pd_lsn.xlogid != 0 || page->pd_lsn.xrecoff != 0 || (page->pd_flags & ~PD_LOGICAL_PAGE) != 0 ||
        page->pd_lower != 0 || page->pd_upper != 0 || page->pd_special != 0 || page->pd_pagesize_version != 0;
}

/*
 * PageHeaderIsValid
 *		Check that the header fields of a page appear valid.
 *
 * This is called when a page modify in memory.
 * if a page has just been read in from disk, should use PageIsVerified
 */
bool PageHeaderIsValid(PageHeader page)
{
    char* pagebytes = NULL;
    int i;
    uint16 headersize;
    int headeroff;

    headersize = GetPageHeaderSize(page);
    /* Check normal case */
    if (PageGetPageSize(page) == BLCKSZ &&
        (PageGetPageLayoutVersion(page) == PG_COMM_PAGE_LAYOUT_VERSION ||
            PageGetPageLayoutVersion(page) == PG_PAGE_4B_LAYOUT_VERSION ||
            PageGetPageLayoutVersion(page) == PG_HEAP_PAGE_LAYOUT_VERSION) &&
        (page->pd_flags & ~PD_VALID_FLAG_BITS) == 0 && page->pd_lower >= headersize &&
        page->pd_lower <= page->pd_upper && page->pd_upper <= page->pd_special && page->pd_special <= BLCKSZ &&
        page->pd_special == MAXALIGN(page->pd_special))
        return true;

    /* Check all-zeroes case */
    if (CheckPageZeroCases(page))
        return false;

    pagebytes = (char*)page;
    headeroff =
        PageIs8BXidHeapVersion(page) ? offsetof(HeapPageHeaderData, pd_linp) : offsetof(PageHeaderData, pd_linp);
    for (i = headeroff; i < BLCKSZ; i++) {
        if (pagebytes[i] != 0)
            return false;
    }
    return true;
}

/*
 * PageGetTempPage
 *		Get a temporary page in local memory for special processing.
 *		The returned page is not initialized at all; caller must do that.
 */
Page PageGetTempPage(Page page)
{
    Size pageSize;
    Page temp;

    pageSize = PageGetPageSize(page);
    temp = (Page)palloc(pageSize);

    return temp;
}

/*
 * PageGetTempPageCopy
 *		Get a temporary page in local memory for special processing.
 *		The page is initialized by copying the contents of the given page.
 */
Page PageGetTempPageCopy(Page page)
{
    Size pageSize;
    Page temp;
    errno_t rc = EOK;

    pageSize = PageGetPageSize(page);
    temp = (Page)palloc(pageSize);

    rc = memcpy_s(temp, pageSize, page, pageSize);
    securec_check(rc, "\0", "\0");
    return temp;
}


/*
 * PageGetExactFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		without any consideration for adding/removing line pointers.
 */
Size PageGetExactFreeSpace(Page page)
{
    int space;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (int)((PageHeader)page)->pd_upper - (int)((PageHeader)page)->pd_lower;

    if (space < 0) {
        return 0;
    }

    return (Size)(uint32)space;
}


static void upgrade_page_ver_4_to_5(Page page)
{
    HeapPageHeader phdr = (HeapPageHeader)page;

    PageSetPageSizeAndVersion(page, BLCKSZ, PG_HEAP_PAGE_LAYOUT_VERSION);

    phdr->pd_xid_base = 0;
    phdr->pd_multi_base = 0;
    ereport(DEBUG1, (errmsg("The page has been upgraded to version %d ", phdr->pd_pagesize_version)));
    return;
}

/* Upgrade the page from PG_PAGE_4B_LAYOUT_VERSION(4) to PG_PAGE_LAYOUT_VERSION(5) */
void PageLocalUpgrade(Page page)
{
    PageHeader phdr = (PageHeader)page;
    errno_t rc = EOK;
    Size movesize = phdr->pd_lower - SizeOfPageHeaderData;

    Assert(PageIs4BXidVersion(page));

    if (movesize > 0) {
        rc = memmove_s((char*)page + SizeOfHeapPageHeaderData,
            phdr->pd_upper - SizeOfHeapPageHeaderData,
            (char*)page + SizeOfPageHeaderData,
            phdr->pd_lower - SizeOfPageHeaderData);

        securec_check(rc, "", "");
    }

    /* Update PageHeaderInfo */
    phdr->pd_lower += SizeOfHeapPageUpgradeData;
    upgrade_page_ver_4_to_5(page);
}

static inline void AllocPageCopyMem()
{
    if (t_thrd.storage_cxt.pageCopy == NULL) {
        ADIO_RUN()
        {
            t_thrd.storage_cxt.pageCopy = (char*)adio_align_alloc(BLCKSZ);
        }
        ADIO_ELSE()
        {
            t_thrd.storage_cxt.pageCopy = (char*)MemoryContextAlloc(
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), BLCKSZ);
        }
        ADIO_END();
    }
}

/*
 * Block data encrypt,We allocate the memory once for one Thread,this space
 * reused util Thread exit.
 * Since we have only shared lock on the
 * buffer, other processes might be updating hint bits in it, so we must
 * copy the page to private storage if we change it.
 */
char* PageDataEncryptIfNeed(Page page)
{
    size_t plainLength;
    size_t cipherLength = 0;
    uint16 headersize;
    int ret = 0;

    headersize = GetPageHeaderSize(page);
    plainLength = (size_t)(BLCKSZ - headersize);

    /* is encrypted Cluster */
    if (!isEncryptedCluster() || PageIsNew(page)) {
        return (char*)page;
    }

    AllocPageCopyMem();

    ret = memcpy_s(t_thrd.storage_cxt.pageCopy, BLCKSZ, (char*)page, BLCKSZ);
    securec_check_c(ret, "\0", "\0");
    encryptBlockOrCUData(PageGetContents(t_thrd.storage_cxt.pageCopy),
        plainLength,
        PageGetContents(t_thrd.storage_cxt.pageCopy),
        &cipherLength);
    Assert(plainLength == cipherLength);
    PageSetEncrypt((Page)t_thrd.storage_cxt.pageCopy);

    return t_thrd.storage_cxt.pageCopy;
}

/*
 * Block data encrypt,We allocate the memory once for one Thread,this space
 * reused util Thread exit.
 * Since we have only shared lock on the
 * buffer, other processes might be updating hint bits in it, so we must
 * copy the page to private storage if we change it.
 */
void PageDataDecryptIfNeed(Page page)
{
    if (PageIsEncrypt(page)) {
        uint16 headersize;

        headersize = GetPageHeaderSize(page);
        size_t plainLength = 0;
        size_t cipherLength = (size_t)(BLCKSZ - headersize);

        decryptBlockOrCUData(PageGetContents(page), cipherLength, PageGetContents(page), &plainLength);
        Assert(cipherLength == plainLength);
        PageClearEncrypt(page);
    }
}

/*
 * Set checksum for a page in shared buffers.
 *
 * If checksums are disabled, or if the page is not initialized, just return
 * the input.  Otherwise, we must make a copy of the page before calculating
 * the checksum, to prevent concurrent modifications (e.g. setting hint bits)
 * from making the final checksum invalid.  It doesn't matter if we include or
 * exclude hints during the copy, as long as we write a valid page and
 * associated checksum.
 *
 * Returns a pointer to the block-sized data that needs to be written. Uses
 * statically-allocated memory, so the caller must immediately write the
 * returned page and not refer to it again.
 */
char* PageSetChecksumCopy(Page page, BlockNumber blkno)
{
    /* If we don't need a checksum, just return the passed-in data */
    if (PageIsNew(page)) {
        return (char*)page;
    }

    /*
     * We allocate the copy space once and use it over on each subsequent
     * call.  The point of palloc'ing here, rather than having a static char
     * array, is first to ensure adequate alignment for the checksumming code
     * and second to avoid wasting space in processes that never call this.
     */
    AllocPageCopyMem();

    errno_t rc = memcpy_s(t_thrd.storage_cxt.pageCopy, BLCKSZ, (char*)page, BLCKSZ);
    securec_check(rc, "", "");

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(t_thrd.storage_cxt.pageCopy);

    ((PageHeader)t_thrd.storage_cxt.pageCopy)->pd_checksum = pg_checksum_page(t_thrd.storage_cxt.pageCopy, blkno);

    return t_thrd.storage_cxt.pageCopy;
}

/*
 * Set checksum for a page in private memory.
 *
 * This must only be used when we know that no other process can be modifying
 * the page buffer.
 */
void PageSetChecksumInplace(Page page, BlockNumber blkno)
{
    /* If we don't need a checksum, just return */
    if (PageIsNew(page)) {
        return;
    }

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(page);

    ((PageHeader)page)->pd_checksum = pg_checksum_page((char*)page, blkno);
}
