/* -------------------------------------------------------------------------
 *
 * bufpage.cpp
 *	  openGauss standard buffer page code.
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
#include "access/ustore/knl_upage.h"

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
    bool is_exrto_page = bool(p->pd_flags & PD_EXRTO_PAGE);

    /*
     * Don't verify page data unless the page passes basic non-zero test
     */
    if (CheckPageZeroCases((PageHeader)page)) {
        checksum = pg_checksum_page((char*)page, blkno);
        if (checksum != p->pd_checksum) {
            checksum_failure = true;
        }

        /*
         * The following checks don't prove the header is correct, only that
         * it looks sane enough to allow into the buffer pool. Later usage of
         * the block can still reveal problems, which is why we offer the
         * checksum option.
         */
        if (is_exrto_page || ((p->pd_flags & ~PD_VALID_FLAG_BITS) == 0 && p->pd_lower <= p->pd_upper &&
            p->pd_upper <= p->pd_special && p->pd_special <= BLCKSZ && p->pd_special == MAXALIGN(p->pd_special))) {
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
            PageGetPageLayoutVersion(page) == PG_HEAP_PAGE_LAYOUT_VERSION ||
            PageGetPageLayoutVersion(page) == PG_SEGMENT_PAGE_LAYOUT_VERSION) &&
        (page->pd_flags & ~PD_VALID_FLAG_BITS) == 0 && page->pd_lower >= headersize &&
        page->pd_lower <= page->pd_upper && page->pd_upper <= page->pd_special && page->pd_special <= BLCKSZ &&
        page->pd_special == MAXALIGN(page->pd_special))
        return true;

    /*
     * Check all-zeroes case for new page;
     * Currently, pd_flags, lsn and checksum may be not zero even in new page. For example, a new page may be set
     * PD_JUST_AFTER_FPW flag when redoing log_new_page; and then set checksum when flushing to disk. Segment-page
     * storage also sets LSN when creating a new page.
     * So we skip these three variables and test reset variables in the header.
     */
    if (page->pd_lower != 0 || page->pd_upper != 0 || page->pd_special != 0 || page->pd_pagesize_version != 0) {
        return false;
    }

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
 * UPageHeaderIsValid
 *		Check that the header fields of a page appear valid.
 *
 * This is called when a page modify in memory.
 * if a page has just been read in from disk, should use PageIsVerified
 */
bool UPageHeaderIsValid(const UHeapPageHeaderData* page)
{
    char* pagebytes = NULL;
    int i;
    uint16 headersize;
    int headeroff;

    headersize = SizeOfUHeapPageHeaderData;
    /* Check normal case */
    if (page->pd_lower >= headersize && page->pd_lower <= page->pd_upper &&
        page->pd_upper <= page->pd_special && page->pd_special <= BLCKSZ &&
        page->pd_special == MAXALIGN(page->pd_special))
        return true;

    /* Check all-zeroes case */
    if (page->pd_lsn.xlogid != 0 || page->pd_lsn.xrecoff != 0 || (page->pd_flags & UHEAP_VALID_FLAG_BITS) != 0 ||
        page->pd_lower != 0 || page->pd_upper != 0 || page->pd_special != 0 ||
        page->td_count != 0 || page->pd_prune_xid != 0) {
        return false;
    }

    pagebytes = (char*)page;
    headeroff = offsetof(UHeapPageHeaderData, reserved);
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

static inline void AllocPageCopyMem()
{
    if (t_thrd.storage_cxt.pageCopy == NULL) {
        ADIO_RUN()
        {
            t_thrd.storage_cxt.pageCopy = (char*)adio_align_alloc(BLCKSZ);
        }
        ADIO_ELSE()
        {
            if (ENABLE_DSS) {
                t_thrd.storage_cxt.pageCopy_ori = (char*)MemoryContextAlloc(
                    THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (BLCKSZ + ALIGNOF_BUFFER));
                t_thrd.storage_cxt.pageCopy = (char*)BUFFERALIGN(t_thrd.storage_cxt.pageCopy_ori);
            } else {
                t_thrd.storage_cxt.pageCopy = (char*)MemoryContextAlloc(
                    THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), BLCKSZ);
            }
        }
        ADIO_END();
    }
    if (t_thrd.storage_cxt.segPageCopy == NULL) {
        ADIO_RUN()
        {
            t_thrd.storage_cxt.segPageCopy = (char*)adio_align_alloc(BLCKSZ);
        }
        ADIO_ELSE()
        {
            if (ENABLE_DSS) {
                t_thrd.storage_cxt.segPageCopyOri = (char*)MemoryContextAlloc(
                    THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (BLCKSZ + ALIGNOF_BUFFER));
                t_thrd.storage_cxt.segPageCopy = (char*)BUFFERALIGN(t_thrd.storage_cxt.segPageCopyOri);
            } else {
                t_thrd.storage_cxt.segPageCopy = (char*)MemoryContextAlloc(
                    THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), BLCKSZ);
            }
        }
        ADIO_END();
    }
}

/*
 * Block data encrypt, We allocate the memory once for every single Thread, this space
 * reused untill Thread exit.
 * Since we only have shared lock on the
 * buffer, other processes might be updating hint bits in it, so we must
 * copy the page to private storage if we are going to change it.
 */
char* PageDataEncryptIfNeed(Page page, TdeInfo* tde_info, bool need_copy, bool is_segbuf)
{
    size_t plainLength = 0;
    size_t cipherLength = 0;
    errno_t ret = 0;
    int retval = 0;
    TdePageInfo tde_page_info;
    char* dst = NULL;

    if (PageIsNew(page) || !PageIsTDE(page) || !g_instance.attr.attr_security.enable_tde) {
        return (char*)page;
    }
    Assert(!PageIsEncrypt(page));

    plainLength = ((PageHeader)page)->pd_special - ((PageHeader)page)->pd_upper;
    retval = RAND_priv_bytes(tde_info->iv, RANDOM_IV_LEN);
    if (retval != 1) {
        ereport(WARNING, (errmodule(MOD_SEC_TDE), errmsg("generate random iv for tde failed, errcode:%d", retval)));
        return (char*)page;
    }
    if (need_copy) {
        AllocPageCopyMem();
        dst = is_segbuf ? t_thrd.storage_cxt.segPageCopy : t_thrd.storage_cxt.pageCopy;
        ret = memcpy_s(dst, BLCKSZ, (char*)page, BLCKSZ);
        securec_check(ret, "\0", "\0");
    } else {
        dst = (char*)page;
    }

    /* at this part, do the real encryption */
    encryptBlockOrCUData(dst + ((PageHeader)dst)->pd_upper,
        plainLength,
        dst + ((PageHeader)dst)->pd_upper,
        &cipherLength,
        tde_info);
    Assert(plainLength == cipherLength);

    ret = memset_s(&tde_page_info, sizeof(TdePageInfo), 0, sizeof(TdePageInfo));
    securec_check(ret, "\0", "\0");
    transformTdeInfoToPage(tde_info, &tde_page_info);
    ret = memcpy_s(dst + BLCKSZ - sizeof(TdePageInfo), sizeof(TdePageInfo), &tde_page_info, sizeof(TdePageInfo));
    securec_check(ret, "\0", "\0");

    /* set the encryption flag */
    PageSetEncrypt((Page)dst);
    return dst;
}

void PageDataDecryptIfNeed(Page page)
{
    TdeInfo tde_info = {0};
    TdePageInfo* tde_page_info = NULL;

    /* whether this page is both TDE page and encrypted  */
    if (PageIsEncrypt(page) && PageIsTDE(page)) {
        size_t plainLength = 0;
        size_t cipherLength = ((PageHeader)page)->pd_special - ((PageHeader)page)->pd_upper;
        tde_page_info = (TdePageInfo*)((char*)(page) + BLCKSZ - sizeof(TdePageInfo));
        transformTdeInfoFromPage(&tde_info, tde_page_info);

        /* at this part, do the real decryption */
        decryptBlockOrCUData(page + ((PageHeader)page)->pd_upper,
            cipherLength,
            page + ((PageHeader)page)->pd_upper,
            &plainLength,
            &tde_info);
        Assert(cipherLength == plainLength);

        /* clear the encryption flag */
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
char* PageSetChecksumCopy(Page page, BlockNumber blkno, bool is_segbuf)
{
    /* If we don't need a checksum, just return the passed-in data */
    if (!CheckPageZeroCases((PageHeader)page)) {
        return (char*)page;
    }

    /*
     * We allocate the copy space once and use it over on each subsequent
     * call.  The point of palloc'ing here, rather than having a static char
     * array, is first to ensure adequate alignment for the checksumming code
     * and second to avoid wasting space in processes that never call this.
     */
    AllocPageCopyMem();

    char *dst = is_segbuf ? t_thrd.storage_cxt.segPageCopy : t_thrd.storage_cxt.pageCopy;

    errno_t rc = memcpy_s(dst, BLCKSZ, (char*)page, BLCKSZ);
    securec_check(rc, "", "");

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(dst);

    ((PageHeader)dst)->pd_checksum = pg_checksum_page(dst, blkno);

    return dst;
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
    if (!CheckPageZeroCases((PageHeader)page)) {
        return;
    }

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(page);

    ((PageHeader)page)->pd_checksum = pg_checksum_page((char*)page, blkno);
}

/*
 * PageGetFreeSpaceForMultipleTuples
 *	 Returns the size of the free (allocatable) space on a page,
 *	 reduced by the space needed for multiple new line pointers.
 *
 * Note: this should usually only be used on index pages.  Use
 * PageGetHeapFreeSpace on heap pages.
 */
Size PageGetFreeSpaceForMultipleTuples(Page page, int ntups)
{
    int space;

    /*
     * Use signed arithmetic here so that we behave sensibly if pd_lower >
     * pd_upper.
     */
    space = (int)((PageHeader)page)->pd_upper - (int)((PageHeader)page)->pd_lower;

    if (space < (int)(ntups * sizeof(ItemIdData)))
        return 0;
    space -= ntups * sizeof(ItemIdData);

    return (Size) space;
}
