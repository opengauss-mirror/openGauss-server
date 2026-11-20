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
#include "tde_key_management/tde_key_storage.h"
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
static void CheckTdePageLayout(Page page)
{
    PageHeader header = (PageHeader)page;
    if (!PageIsTDE(page) || header->pd_upper < SizeOfPageHeaderData ||
        header->pd_upper > header->pd_special || header->pd_special > BLCKSZ - sizeof(TdePageInfo)) {
        ereport(ERROR, (errmsg("invalid TDE page layout")));
    }
}

void PageSetTdeInfo(Page page, TdeInfo* tde_info)
{
    CheckTdePageLayout(page);
    if (tde_info->dek_cipher[0] == '\0' || tde_info->cmk_id[0] == '\0') {
        ereport(ERROR, (errmsg("missing or invalid TDE context for page")));
    }
    TdePageInfo info = {0};
    transformTdeInfoToPage(tde_info, &info);
    errno_t rc = memcpy_s(page + BLCKSZ - sizeof(info), sizeof(info), &info, sizeof(info));
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
}

/* Only call immediately after PageInit, before adding any tuples or TDE space. */
void PageInitTdeInfo(Page page, TdeInfo* tde_info)
{
    PageHeader header = (PageHeader)page;
    if (PageIsTDE(page) || header->pd_upper != BLCKSZ || header->pd_special != BLCKSZ) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("TDE initialization requires a freshly initialized empty page"),
            errdetail("pd_upper=%u pd_special=%u flags=0x%x",
                header->pd_upper, header->pd_special, header->pd_flags),
            errhint("Initialize TDE immediately after PageInit, before adding tuples or reserving TDE space.")));
    }
    header->pd_upper -= sizeof(TdePageInfo);
    header->pd_special -= sizeof(TdePageInfo);
    PageSetTDE(page);
    PageSetTdeInfo(page, tde_info);
}

void PageInitTdeInfoFromCache(Page page, const RelFileNode& rnode)
{
    TdeInfo info = {0};
    TDE::TDEBufferCache::get_instance().search_cache(rnode, &info);
    PageInitTdeInfo(page, &info);
}

char* PageDataEncryptIfNeed(Page page, TdeInfo* tde_info, bool need_copy, bool isSegbuf)
{
    size_t plainLength = 0;
    size_t cipherLength = 0;
    errno_t ret = 0;
    int retval = 0;
    char* dst = NULL;

    if (PageIsNew(page) || !PageIsTDE(page)) {
        return (char*)page;
    }
    if (!g_instance.attr.attr_security.enable_tde) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("TDE provider is disabled for an encrypted page")));
    }
    if (PageIsEncrypt(page)) {
        ereport(ERROR, (errmsg("TDE page is already encrypted")));
    }
    CheckTdePageLayout(page);

    plainLength = ((PageHeader)page)->pd_special - ((PageHeader)page)->pd_upper;
    retval = RAND_bytes(tde_info->iv, RANDOM_IV_LEN);
    if (retval != 1) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("generate random iv for TDE failed, errcode:%d", retval)));
    }
    if (need_copy) {
        AllocPageCopyMem();
        dst = isSegbuf ? t_thrd.storage_cxt.segPageCopy : t_thrd.storage_cxt.pageCopy;
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

    PageSetTdeInfo(dst, tde_info);

    /* set the encryption flag */
    PageSetEncrypt((Page)dst);
    return dst;
}

/* Physical moves have no relation context; the page footer owns its key identity. */
char* PageDataEncryptForPhysicalWrite(Page page)
{
    if (PageIsNew(page) || !PageIsTDE(page) || PageIsEncrypt(page)) {
        return (char*)page;
    }
    CheckTdePageLayout(page);
    TdeInfo tde_info = {0};
    TdePageInfo* info = (TdePageInfo*)(page + BLCKSZ - sizeof(TdePageInfo));
    transformTdeInfoFromPage(&tde_info, info);
    return PageDataEncryptIfNeed(page, &tde_info, true, true);
}

void PageDataDecryptIfNeed(Page page)
{
    TdeInfo tde_info = {0};
    TdePageInfo* tde_page_info = NULL;

    /* whether this page is both TDE page and encrypted  */
    if (PageIsEncrypt(page) && PageIsTDE(page)) {
        CheckTdePageLayout(page);
        if (!g_instance.attr.attr_security.enable_tde) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("TDE provider is disabled for an encrypted page")));
        }
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
char* PageSetChecksumCopy(Page page, BlockNumber blkno, bool isSegbuf)
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

    char *dst = isSegbuf ? t_thrd.storage_cxt.segPageCopy : t_thrd.storage_cxt.pageCopy;

    errno_t rc = memcpy_s(dst, BLCKSZ, (char*)page, BLCKSZ);
    securec_check(rc, "", "");

    /* set page->pd_flags mark using FNV1A for checksum */
    PageSetChecksumByFNV1A(dst);

    ((PageHeader)dst)->pd_checksum = pg_checksum_page(dst, blkno);

    return dst;
}

char* AdioPageSetChecksumCopy(Page page, BlockNumber blkno, bool isSegbuf)
{
    /* If we don't need a checksum, just return the passed-in data */
    if (!CheckPageZeroCases((PageHeader)page)) {
        return (char*)page;
    }

    char *dst = t_thrd.storage_cxt.inProgressAioPageCopys + t_thrd.storage_cxt.InProgressAioDispatchCount * BLCKSZ;

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
