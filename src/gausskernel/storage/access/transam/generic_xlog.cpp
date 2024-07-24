/*-------------------------------------------------------------------------
 *
 * generic_xlog.cpp
 *   Implementation of generic xlog records.
 *
 * Portions Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/storage/access/transam/generic_xlog.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/xlogproc.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/*-------------------------------------------------------------------------
 * Internally, a delta between pages consists of a set of fragments.  Each
 * fragment represents changes made in a given region of a page.  A fragment
 * is made up as follows:
 *
 * - offset of page region (OffsetNumber)
 * - length of page region (OffsetNumber)
 * - data - the data to place into the region ('length' number of bytes)
 *
 * Unchanged regions of a page are not represented in its delta.  As a
 * result, a delta can be more compact than the full page image.  But having
 * an unchanged region in the middle of two fragments that is smaller than
 * the fragment header (offset and length) does not pay off in terms of the
 * overall size of the delta. For this reason, we break fragments only if
 * the unchanged region is bigger than MATCH_THRESHOLD.
 *
 * The worst case for delta sizes occurs when we did not find any unchanged
 * region in the page.  The size of the delta will be the size of the page plus
 * the size of the fragment header in that case.
 *-------------------------------------------------------------------------
 */
#define FRAGMENT_HEADER_SIZE    (2 * sizeof(OffsetNumber))
#define MATCH_THRESHOLD         FRAGMENT_HEADER_SIZE
#define MAX_DELTA_SIZE          BLCKSZ + FRAGMENT_HEADER_SIZE

/* Struct of generic xlog data for single page */
typedef struct
{
    Buffer buffer;         /* registered buffer */
    char image[BLCKSZ];  /* copy of page image for modification */
    char data[MAX_DELTA_SIZE]; /* delta between page images */
    int dataLen;        /* space consumed in data field */
    int flags; /* flags for this buffer */
} PageData;

/* State of generic xlog record construction */
struct GenericXLogState
{
    bool isLogged;
    PageData pages[MAX_GENERIC_XLOG_PAGES];
};

static void writeFragment(PageData *pageData, OffsetNumber offset,
                          OffsetNumber len, Pointer data);
static void writeDelta(PageData *pageData);
static void applyPageRedo(Page page, Pointer data, Size dataSize);

/*
 * Write next fragment into delta.
 */
static void
writeFragment(PageData *pageData, OffsetNumber offset, OffsetNumber length,
              Pointer data)
{
    errno_t ret = EOK;
    Pointer ptr = pageData->data + pageData->dataLen;

    /* Check if we have enough space */
    Assert(pageData->dataLen + sizeof(offset) +
           sizeof(length) + length <= sizeof(pageData->data));

    /* Write fragment data */
    ret = memcpy_s(ptr, MAX_DELTA_SIZE, &offset, sizeof(offset));
    securec_check(ret, "\0", "\0");
    ptr += sizeof(offset);
    ret = memcpy_s(ptr, MAX_DELTA_SIZE - sizeof(offset), &length, sizeof(length));
    securec_check(ret, "\0", "\0");
    ptr += sizeof(length);
    ret = memcpy_s(ptr, MAX_DELTA_SIZE - sizeof(offset) - sizeof(length), data, length);
    securec_check(ret, "\0", "\0");
    ptr += length;

    pageData->dataLen = ptr - pageData->data;
}

/*
 * Make delta for given page.
 */
static void
writeDelta(PageData *pageData)
{
    Page page = BufferGetPage(pageData->buffer),
         image = (Page) pageData->image;
    int i, fragmentBegin = -1, fragmentEnd = -1;
    uint16 pageLower = ((PageHeader) page)->pd_lower,
           pageUpper = ((PageHeader) page)->pd_upper,
           imageLower = ((PageHeader) image)->pd_lower,
           imageUpper = ((PageHeader) image)->pd_upper;

    for (i = 0; i < BLCKSZ; i++) {
        bool match;

        /*
         * Check if bytes in old and new page images match.  We do not care
         * about data in the unallocated area between pd_lower and pd_upper.
         * We assume the unallocated area to expand with unmatched bytes.
         * Bytes inside the unallocated area are assumed to always match.
         */
        if (i < pageLower) {
            if (i < imageLower)
                match = (page[i] == image[i]);
            else
                match = false;
        } else if (i >= pageUpper) {
            if (i >= imageUpper)
                match = (page[i] == image[i]);
            else
                match = false;
        } else {
            match = true;
        }

        if (match) {
            if (fragmentBegin >= 0) {
                /* Matched byte is potentially part of a fragment. */
                if (fragmentEnd < 0)
                    fragmentEnd = i;

                /*
                 * Write next fragment if sequence of matched bytes is longer
                 * than MATCH_THRESHOLD.
                 */
                if (i - fragmentEnd >= MATCH_THRESHOLD) {
                    writeFragment(pageData, fragmentBegin,
                                  fragmentEnd - fragmentBegin,
                                  page + fragmentBegin);
                    fragmentBegin = -1;
                    fragmentEnd = -1;
                }
            }
        } else {
            /* On unmatched byte, start new fragment if it is not done yet */
            if (fragmentBegin < 0)
                fragmentBegin = i;
            fragmentEnd = -1;
        }
    }

    if (fragmentBegin >= 0)
        writeFragment(pageData, fragmentBegin, BLCKSZ - fragmentBegin, page + fragmentBegin);

#ifdef WAL_DEBUG
    /*
     * If xlog debug is enabled, then check produced delta.  Result of delta
     * application to saved image should be the same as current page state.
     */
    if (XLOG_DEBUG) {
        errno_t ret = EOK;
        char tmp[BLCKSZ];
        ret = memcpy_s(tmp, BLCKSZ, image, BLCKSZ);
        securec_check(ret, "\0", "\0");
        applyPageRedo(tmp, pageData->data, pageData->dataLen);
        if (memcmp(tmp, page, pageLower)
            || memcmp(tmp + pageUpper, page + pageUpper, BLCKSZ - pageUpper))
            elog(ERROR, "result of generic xlog apply does not match");
    }
#endif
}

/*
 * Start new generic xlog record.
 */
GenericXLogState *
GenericXLogStart(Relation relation)
{
    int i;
    GenericXLogState *state;

    if (t_thrd.proc->workingVersionNum < GENERICXLOG_VERSION_NUM) {
        elog(ERROR, "workingVersionNum is lowwer than GENERICXLOG_VERSION_NUM, not supported!");
        return NULL;
    }

    state = (GenericXLogState *) palloc(sizeof(GenericXLogState));

    state->isLogged = RelationNeedsWAL(relation);
    for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++)
        state->pages[i].buffer = InvalidBuffer;

    return state;
}

/*
 * Register new buffer for generic xlog record.
 */
Page
GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer, int flags)
{
    errno_t ret = EOK;
    int block_id;

    if (state == NULL) {
        elog(ERROR, "GenericXLogState invalid!");
        return NULL;
    }

    /* Place new buffer to unused slot in array */
    for (block_id = 0; block_id < MAX_GENERIC_XLOG_PAGES; block_id++) {
        PageData *page = &state->pages[block_id];
        if (BufferIsInvalid(page->buffer)) {
            page->buffer = buffer;
            ret = memcpy_s(page->image, BLCKSZ, BufferGetPage(buffer), BLCKSZ);
            securec_check(ret, "\0", "\0");
            page->dataLen = 0;
            page->flags = flags;
            return (Page)page->image;
        } else if (page->buffer == buffer) {
            /*
             * Buffer is already registered.  Just return the image, which is
             * already prepared.
             */
            return (Page)page->image;
        }
    }

    elog(ERROR, "maximum number of %d generic xlog buffers is exceeded",
         MAX_GENERIC_XLOG_PAGES);

    /* keep compiler quiet */
    return NULL;
}

/*
 * Unregister particular buffer for generic xlog record.
 */
void
GenericXLogUnregister(GenericXLogState *state, Buffer buffer)
{
    int block_id;

        if (state == NULL) {
            elog(ERROR, "GenericXLogState invalid!");
            return;
        }

    /* Find block in array to unregister */
    for (block_id = 0; block_id < MAX_GENERIC_XLOG_PAGES; block_id++) {
        if (state->pages[block_id].buffer == buffer) {
            /*
             * Preserve order of pages in array because it could matter for
             * concurrency.
             */
            int ret = memmove_s(&state->pages[block_id], (MAX_GENERIC_XLOG_PAGES - block_id - 1) * sizeof(PageData), &state->pages[block_id + 1],
                    (MAX_GENERIC_XLOG_PAGES - block_id - 1) * sizeof(PageData));
            securec_check(ret, "\0", "\0");
            state->pages[MAX_GENERIC_XLOG_PAGES - 1].buffer = InvalidBuffer;
            return;
        }
    }

    elog(ERROR, "registered generic xlog buffer not found");
}

/*
 * Put all changes in registered buffers to generic xlog record.
 */
XLogRecPtr
GenericXLogFinish(GenericXLogState *state)
{
    XLogRecPtr lsn = InvalidXLogRecPtr;
    int i;
    errno_t ret = EOK;

    Assert(state != NULL);

    if (state->isLogged) {
        /* Logged relation: make xlog record in critical section. */
        XLogBeginInsert();

        START_CRIT_SECTION();

        for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++) {
            char tmp[BLCKSZ];
            PageData *page = &state->pages[i];

            if (BufferIsInvalid(page->buffer))
                continue;

            /* Swap current and saved page image. */
            ret = memcpy_s(tmp, BLCKSZ, page->image, BLCKSZ);
            securec_check(ret, "\0", "\0");
            ret = memcpy_s(page->image, BLCKSZ, BufferGetPage(page->buffer), BLCKSZ);
            securec_check(ret, "\0", "\0");
            ret = memcpy_s(BufferGetPage(page->buffer), BLCKSZ, tmp, BLCKSZ);
            securec_check(ret, "\0", "\0");

            if (page->flags & GENERIC_XLOG_FULL_IMAGE) {
                /* A full page image does not require anything special */
                XLogRegisterBuffer(i, page->buffer, REGBUF_FORCE_IMAGE);
            } else {
                /*
                 * In normal mode, calculate delta and write it as data
                 * associated with this page.
                 */
                XLogRegisterBuffer(i, page->buffer, REGBUF_STANDARD);
                writeDelta(page);
                XLogRegisterBufData(i, page->data, page->dataLen);
            }
        }

        /* Insert xlog record */
        lsn = XLogInsert(RM_GENERIC_ID, 0);

        /* Set LSN and mark buffers dirty */
        for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++) {
            PageData *page = &state->pages[i];

            if (BufferIsInvalid(page->buffer))
                continue;
            PageSetLSN(BufferGetPage(page->buffer), lsn);
            MarkBufferDirty(page->buffer);
        }
        END_CRIT_SECTION();
    } else {
        /* Unlogged relation: skip xlog-related stuff */
        START_CRIT_SECTION();
        for (i = 0; i < MAX_GENERIC_XLOG_PAGES; i++) {
            PageData *page = &state->pages[i];

            if (BufferIsInvalid(page->buffer))
                continue;
            ret = memcpy_s(BufferGetPage(page->buffer), BLCKSZ, page->image, BLCKSZ);
            securec_check(ret, "\0", "\0");
            MarkBufferDirty(page->buffer);
        }
        END_CRIT_SECTION();
    }

    pfree(state);

    return lsn;
}

/*
 * Abort generic xlog record.
 */
void
GenericXLogAbort(GenericXLogState *state)
{
    pfree(state);
}

/*
 * Apply delta to given page image.
 */
static void
applyPageRedo(Page page, Pointer data, Size dataSize)
{
    errno_t ret = EOK;
    Pointer ptr = data, end = data + dataSize;

    while (ptr < end) {
        OffsetNumber offset, length;

        ret = memcpy_s(&offset, sizeof(offset), ptr, sizeof(offset));
        securec_check(ret, "\0", "\0");
        ptr += sizeof(offset);
        ret =memcpy_s(&length, sizeof(length), ptr, sizeof(length));
        securec_check(ret, "\0", "\0");
        ptr += sizeof(length);

        ret = memcpy_s(page + offset, length, ptr, length);
        securec_check(ret, "\0", "\0");

        ptr += length;
    }
}

/*
 * Redo function for generic xlog record.
 */
void
generic_redo(XLogReaderState *record)
{
    uint8 block_id;
    RedoBufferInfo buffers[MAX_GENERIC_XLOG_PAGES];
    XLogRecPtr lsn = record->EndRecPtr;

    Assert(record->max_block_id < MAX_GENERIC_XLOG_PAGES);

    /* Iterate over blocks */
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        XLogRedoAction action;

        if (!XLogRecHasBlockRef(record, block_id))
            continue;

        action = XLogReadBufferForRedo(record, block_id, &buffers[block_id]);

        /* Apply redo to given block if needed */
        if (action == BLK_NEEDS_REDO) {
            Pointer blockData;
            Size blockDataSize;
            Page page;

            page = BufferGetPage(buffers[block_id].buf);
            blockData = XLogRecGetBlockData(record, block_id, &blockDataSize);
            applyPageRedo(page, blockData, blockDataSize);

            PageSetLSN(page, lsn);
            MarkBufferDirty(buffers[block_id].buf);
        }
    }

    /* Changes are done: unlock and release all buffers */
    for (block_id = 0; block_id <= record->max_block_id; block_id++) {
        if (BufferIsValid(buffers[block_id].buf))
            UnlockReleaseBuffer(buffers[block_id].buf);
    }
}
