/* ---------------------------------------------------------------------------------------
 * *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * pageparse.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/storage/page/pageparse.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include "access/htup.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/xlogdefs.h"
#include "commands/tablespace.h"
#include "catalog/catalog.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postgres.h"
#include "postgres_ext.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/bufpage.h"
#include "storage/buf/buf_internals.h"
#include "storage/checksum.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/segment.h"
#include "storage/smgr/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/palloc.h"
#include "utils/relmapper.h"
#include "pageparse.h"
#include "storage/file/fio_device_com.h"

#define INVALID_FD (-1)

typedef enum {
    BTREE_INDEX = 0,
    UBTREE_INDEX,
    INDEX_BOTT
} INDEX_TYPE;

typedef struct UndoHeader {
    UndoRecordHeader whdr;
    UndoRecordBlock wblk;
    UndoRecordTransaction wtxn;
    UndoRecordPayload wpay;
    UndoRecordOldTd wtd;
    UndoRecordPartition wpart;
    UndoRecordTablespace wtspc;
    StringInfoData rawdata;
} UndoHeader;

void CheckUser(const char *fName)
{
    if (!superuser() && !isOperatoradmin(GetUserId()))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("Must be system admin or operator admin in operation mode can use %s.", fName))));
}

static void ParseUHeapPageHeader(void *page, BlockNumber blkno, BlockNumber endBlk, char *output);

static void ParseUHeapPageTDInfo(void *page, char *output);

static void ParseUHeapPageItem(Item item, UHeapTuple tuple, char *output);

static void ParseUHeapPageItemUndo(void *page, ItemPointer ctid, char *output);

static void ParseUHeapPageData(void *page, BlockNumber blkno, char *output, bool dumpUndo = false);

static void ParseUHeapPageSpecialInfo(void *page, char *output);

static void ParseUHeapPage(void *page, BlockNumber blkno, BlockNumber endBlk, char *output, bool dumpUndo = false);

static void ParseIndexPageHeader(void *page, int type, BlockNumber blkno, BlockNumber endBlk, char *output);

static void ParseIndexPageItem(Item item, int type, uint32 len, char *output);

static void ParseIndexPageSpecialInfo(void *page, int type, char *output);

static void ParseIndexPageData(void *page, int type, char *output);

static void ParseIndexPage(void *page, int type, BlockNumber blkno, BlockNumber endBlk, char *output);

static void formatBitmap(const unsigned char *start, int len, char bit1, char bit0, char *strOutput)
{
    errno_t rc = EOK;
    for (int i = 0; i < len; ++i) {
        unsigned char ch = start[i];
        unsigned char bitmask = 1;
        /* print 8 bits within a loop */
        do {
            rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "%c",
                ((ch & bitmask) ? bit1 : bit0));
            securec_check_ss(rc, "\0", "\0");
            bitmask <<= 1;
        } while (bitmask != 0);
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, " ");
        securec_check_ss(rc, "\0", "\0");
    }
}

/* init RelFileNode and outputFilename */
void PrepForRead(char *path, int64 blocknum, char *relation_type, char *outputFilename, RelFileNode *relnode,
    bool parse_page)
{
    size_t len = strlen(path);
    if (len > strlen(COMPRESS_STR) &&
        strcmp(path + len - strlen(COMPRESS_STR), COMPRESS_STR) == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("compressed table file is not allowed here."))));
    }
    char *pathFirstpart = (char *)palloc(MAXFNAMELEN * sizeof(char));
    errno_t rc = memset_s(pathFirstpart, MAXFNAMELEN, 0, MAXFNAMELEN);
    securec_check(rc, "\0", "\0");

    relnode->opt = 0;
    if ((strcmp(relation_type, "segment") == 0)) {
        relnode->bucketNode = SegmentBktId;
        char *bucketNodestr = strstr(path, "_b");
        if (NULL != bucketNodestr) {
            bucketNodestr += TWO; /* delete first two chars: _b */
            relnode->bucketNode = (int2)pg_strtouint64(bucketNodestr, NULL, TENBASE);
            rc = strncpy_s(pathFirstpart, strlen(path) - strlen(bucketNodestr) - 1, path,
                strlen(path) - strlen(bucketNodestr) - TWO);
            securec_check(rc, "\0", "\0");
        }
    } else {
        relnode->bucketNode = InvalidBktId;
    }
    RelFileNodeForkNum relfilenode;
    if (strlen(pathFirstpart) == 0) {
        relfilenode = relpath_to_filenode(path);
    } else {
        relfilenode = relpath_to_filenode(pathFirstpart);
    }
    if (relfilenode.rnode.node.spcNode == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("The tablespace oid is 0. Please check the first parameter path. "
                "If you are not sure about the table path, please check pg_relation_filepath."))));
    RelFileNodeRelCopy(*relnode, relfilenode.rnode.node);
    relnode->opt = 0;
    char *pagesuffix = "page";
    char *xlogsuffix = "xlog";
    rc = snprintf_s(outputFilename + (int)strlen(outputFilename), MAXFILENAME, MAXFILENAME - 1, "%s/%u_%u_%u_%d.%s",
        t_thrd.proc_cxt.DataDir, relnode->spcNode, relnode->dbNode, relnode->relNode, blocknum,
        (parse_page ? pagesuffix : xlogsuffix));
    securec_check_ss(rc, "\0", "\0");
    pfree_ext(pathFirstpart);
}

static void ParseHeapHeader(const PageHeader page, char *strOutput, BlockNumber blockNum, BlockNumber block_endpoint)
{
    errno_t rc = EOK;
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "Page information of block %u/%u", blockNum, block_endpoint);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\tpd_lsn: %X/%X",
        (uint32)(PageGetLSN(page) >> XIDTHIRTYTWO), (uint32)PageGetLSN(page));
    securec_check_ss(rc, "\0", "\0");
    bool checksum_matched = false;
    if (CheckPageZeroCases(page)) {
        uint16 checksum = pg_checksum_page((char *)page, (BlockNumber)blockNum);
        checksum_matched = (checksum == page->pd_checksum);
    }

    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\tpd_checksum: 0x%X, verify %s", page->pd_checksum, checksum_matched ? "success" : "fail");
    securec_check_ss(rc, "\0", "\0");

    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\tpd_flags: ");
    securec_check(rc, "\0", "\0");
    if (PageHasFreeLinePointers(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_HAS_FREE_LINES ");
        securec_check(rc, "\0", "\0");
    }
    if (PageIsFull(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_PAGE_FULL ");
        securec_check(rc, "\0", "\0");
    }
    if (PageIsAllVisible(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_ALL_VISIBLE ");
        securec_check(rc, "\0", "\0");
    }
    if (PageIsCompressed(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_COMPRESSED_PAGE ");
        securec_check(rc, "\0", "\0");
    }
    if (PageIsLogical(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_LOGICAL_PAGE ");
        securec_check(rc, "\0", "\0");
    }
    if (PageIsEncrypt(page)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "PD_ENCRYPT_PAGE ");
        securec_check(rc, "\0", "\0");
    }
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\tpd_lower: %u, %s",
        page->pd_lower, PageIsEmpty(page) ? "empty" : "non-empty");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\tpd_upper: %u, %s",
        page->pd_upper, PageIsNew(page) ? "new" : "old");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\tpd_special: %u, size %u",
        page->pd_special, PageGetSpecialSize(page));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\tPage size & version: %u, %u", (uint16)PageGetPageSize(page), (uint16)PageGetPageLayoutVersion(page));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\tpd_xid_base: %lu, pd_multi_base: %lu", ((HeapPageHeader)(page))->pd_xid_base,
        ((HeapPageHeader)(page))->pd_multi_base);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\tpd_prune_xid: %lu",
        ((HeapPageHeader)(page))->pd_prune_xid + ((HeapPageHeader)(page))->pd_xid_base);
    securec_check_ss(rc, "\0", "\0");
}

static void PrintInfomask(HeapTupleHeader tup, char *strOutput)
{
    errno_t rc = EOK;
    if (tup->t_infomask & HEAP_HASNULL) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HASNULL ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_HASVARWIDTH) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HASVARWIDTH ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_HASEXTERNAL) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HASEXTERNAL ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_HASOID) {
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "HEAP_HASOID(%d) ",
            HeapTupleHeaderGetOid(tup));
        securec_check_ss(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_COMPRESSED) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_COMPRESSED ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_COMBOCID) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_COMBOCID ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMAX_EXCL_LOCK) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMAX_EXCL_LOCK ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMAX_SHARED_LOCK) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMAX_SHARED_LOCK ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMIN_COMMITTED) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMIN_COMMITTED ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMIN_INVALID) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMIN_INVALID ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMAX_COMMITTED) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMAX_COMMITTED ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMAX_INVALID) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMAX_INVALID ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_XMAX_IS_MULTI) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_XMAX_IS_MULTI ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask & HEAP_UPDATED) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_UPDATED ");
        securec_check(rc, "\0", "\0");
    }
    if ((tup->t_infomask & HEAP_HAS_8BYTE_UID)) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HAS_8BYTE_UID ");
        securec_check(rc, "\0", "\0");
    } else {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HAS_NO_UID ");
        securec_check(rc, "\0", "\0");
    }
    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\t\t\tt_infomask2: ");
    securec_check(rc, "\0", "\0");
    if (tup->t_infomask2 & HEAP_HOT_UPDATED) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_HOT_UPDATED ");
        securec_check(rc, "\0", "\0");
    }
    if (tup->t_infomask2 & HEAP_ONLY_TUPLE) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "HEAP_ONLY_TUPLE ");
        securec_check(rc, "\0", "\0");
    }
}

static void ParseTupleHeader(const PageHeader page, uint lineno, char *strOutput)
{
    errno_t rc = EOK;
    ItemId lp = PageGetItemId(page, lineno);
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\n\t\tTuple #%u is normal: length %u, offset %u", lineno, ItemIdGetLength(lp), ItemIdGetOffset(lp));
    securec_check_ss(rc, "\0", "\0");

    HeapTupleData dummyTuple;
    HeapTupleHeader tup = (HeapTupleHeader)(PageGetItem(page, lp));
    dummyTuple.t_data = tup;
    dummyTuple.t_xid_base = ((HeapPageHeader)(page))->pd_xid_base;
    dummyTuple.t_multi_base = ((HeapPageHeader)(page))->pd_multi_base;
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\t\t\tt_xmin/t_xmax/t_cid: %lu/%lu/%u", HeapTupleGetRawXmin(&dummyTuple), HeapTupleGetRawXmax(&dummyTuple),
        HeapTupleHeaderGetRawCommandId(tup));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\t\t\tctid:(block %u/%u, offset %u)", tup->t_ctid.ip_blkid.bi_hi, tup->t_ctid.ip_blkid.bi_lo,
        tup->t_ctid.ip_posid);
    securec_check_ss(rc, "\0", "\0");

    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\t\t\tt_infomask: ");
    securec_check(rc, "\0", "\0");

    PrintInfomask(tup, strOutput);

    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "Attrs Num: %d",
        HeapTupleHeaderGetNatts(tup, NULL));
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n\t\t\tt_hoff: %u",
        tup->t_hoff);
    securec_check_ss(rc, "\0", "\0");
    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\t\t\tt_bits: ");
    securec_check(rc, "\0", "\0");

    formatBitmap((const unsigned char *)tup->t_bits, BITMAPLEN(HeapTupleHeaderGetNatts(tup, NULL)), 'V', 'N',
        strOutput);
}

static void ParseHeapPage(const PageHeader page, BlockNumber blockNum, char *strOutput, BlockNumber block_endpoint)
{
    errno_t rc = EOK;
    if (PageIsNew(page)) {
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                        "Page information of block %u/%u : new page\n", blockNum, block_endpoint);
        securec_check_ss(rc, "\0", "\0");
        ParseHeapHeader(page, strOutput, blockNum, block_endpoint);
        rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1, "\n");
        securec_check_ss(rc, "\0", "\0");
        return;
    }
    if (page->pd_lower < GetPageHeaderSize(page) || page->pd_lower > page->pd_upper ||
        page->pd_upper > page->pd_special || page->pd_special > BLCKSZ ||
        page->pd_special != MAXALIGN(page->pd_special)) {
        rc = snprintf_s(strOutput + (int)strlen(strOutput),
            MAXOUTPUTLEN,
            MAXOUTPUTLEN - 1,
            "The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u\n",
            page->pd_lower,
            page->pd_upper,
            page->pd_special);
        securec_check_ss(rc, "\0", "\0");
        return;
    }

    ParseHeapHeader(page, strOutput, blockNum, block_endpoint);
    /* parse tuple header */
    rc = strcat_s(strOutput, MAXOUTPUTLEN, "\n\n\tHeap tuple information on this page");
    securec_check(rc, "\0", "\0");

    uint nline = PageGetMaxOffsetNumber((Page)page);
    uint nunused = 0, nnormal = 0, ndead = 0;
    for (uint i = (OffsetNumber)1; i <= nline; i++) {
        ItemId lp = PageGetItemId(page, i);
        if (ItemIdIsNormal(lp)) {
            nnormal++;
            ParseTupleHeader(page, i, strOutput);
        } else if (ItemIdIsDead(lp)) {
            ndead++;
            rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                "\n\n\t\tTuple #%u is dead: length %u, offset %u", i, ItemIdGetLength(lp), ItemIdGetOffset(lp));
            securec_check_ss(rc, "\0", "\0");
        } else {
            nunused++;
            rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
                "\n\n\t\tTuple #%u is nunused: ", i);
            securec_check_ss(rc, "\0", "\0");
        }
    }
    rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN - 1,
        "\n\tSummary (%u total):  %u normal, %u unused, %u dead\n\nNormal Heap Page, special space is %u\n\n", nline,
        nnormal, nunused, ndead, (uint)PageGetSpecialSize(page));
    securec_check_ss(rc, "\0", "\0");
}

static void ParseOnePage(const PageHeader page, BlockNumber blockNum, char *strOutput, char *relation_type,
    BlockNumber block_endpoint, bool dumpUndo)
{
    errno_t rc = EOK;
    if (strcmp(relation_type, "heap") == 0) {
        if ((PG_HEAP_PAGE_LAYOUT_VERSION != (uint16)PageGetPageLayoutVersion(page) &&
                   (uint16)PageGetPageLayoutVersion(page) != 0) || PageGetSpecialSize(page) != 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("The target page is not heap, the given page version is: %u",
                (uint16)PageGetPageLayoutVersion(page)))));
        }
        ParseHeapPage(page, blockNum, strOutput, block_endpoint);
    } else if (strcmp(relation_type, "uheap") == 0) {
        if (PG_UHEAP_PAGE_LAYOUT_VERSION != (uint16)PageGetPageLayoutVersion(page) || PageGetSpecialSize(page) != 0) {
            ereport(LOG,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmodule(MOD_USTORE),
                    errmsg("The target page(%u) is not uheap, the given page version is: %u",
                    blockNum, (uint16)PageGetPageLayoutVersion(page)),
                    errdetail("N/A"),
                    errcause("Page version is not uheap, or page is new."),
                    erraction("Check input parameters or perform checkpoint")));
            rc = snprintf_s(strOutput + (int)strlen(strOutput), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "Page (%u) is not uheap ,or page is new.\n", blockNum);
            securec_check_ss(rc, "\0", "\0");
            return;
        }
        ParseUHeapPage((void *)page, blockNum, block_endpoint, strOutput, dumpUndo);
    } else if (strcmp(relation_type, "btree") == 0) {
        if ((PG_COMM_PAGE_LAYOUT_VERSION != (uint16)PageGetPageLayoutVersion(page) &&
            (uint16)PageGetPageLayoutVersion(page) != 0) || PageGetSpecialSize(page) == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("The target page is not btree, the given page version is: %u",
                (uint16)PageGetPageLayoutVersion(page)))));
        }
        ParseIndexPage((void *)page, BTREE_INDEX, blockNum, block_endpoint, strOutput);
    } else if (strcmp(relation_type, "ubtree") == 0) {
        if ((PG_COMM_PAGE_LAYOUT_VERSION != (uint16)PageGetPageLayoutVersion(page) &&
            (uint16)PageGetPageLayoutVersion(page) != 0) || PageGetSpecialSize(page) == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("The target page is not ubtree, the given page version is: %u",
                (uint16)PageGetPageLayoutVersion(page)))));
        }
        ParseIndexPage((void *)page, UBTREE_INDEX, blockNum, block_endpoint, strOutput);
    } else if (strcmp(relation_type, "segment") == 0) {
        rc = strcat_s(strOutput, MAXOUTPUTLEN, "Parse segment table.");
        securec_check(rc, "\0", "\0");
        ParseHeapPage(page, blockNum, strOutput, block_endpoint);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PARAMETER), (errmsg("Only support heap, uheap, btree, ubtree, segment."))));
    }
}

static void check_rdStatus(SMGR_READ_STATUS rdStatus, BlockNumber blockNum, const PageHeader page, char *strOutput1)
{
    errno_t rc = EOK;
    if (rdStatus == SMGR_RD_CRC_ERROR) {
        uint16 checksum = pg_checksum_page((char *)page, blockNum);
        rc = snprintf_s(strOutput1, SHORTOUTPUTLEN, SHORTOUTPUTLEN - 1,
                        "\nFor page %u, page verification failed, calculated checksum 0x%X but expected 0x%X.\n",
                        blockNum, checksum, page->pd_checksum);
        securec_check_ss(rc, "\0", "\0");
    } else if (rdStatus == SMGR_RD_NO_BLOCK) {
        rc = snprintf_s(strOutput1, SHORTOUTPUTLEN, SHORTOUTPUTLEN - 1, "\tThe page %u does not exist.\n", blockNum);
        securec_check_ss(rc, "\0", "\0");
    }
}

static bool readFromMemory(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum, char *relation_type,
    char *strOutput, FILE *outputfile, char *outputFilename, bool dumpUndo)
{
    errno_t rc = EOK;
    SegPageLocation loc;
    loc.blocknum = 0;
    /* for segment, update to physical blocknum */
    if (strcmp(relation_type, "segment") == 0) {
        loc = seg_get_physical_location(smgr->smgr_rnode.node, forkNum, blockNum);
    }
    BufferTag new_tag;
    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, forkNum, blockNum);
    uint32 new_hash = BufTableHashCode(&new_tag);
    LWLock *partition_lock = BufMappingPartitionLock(new_hash);
    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
    /* LW lock to avoid concurrent read/write */
    (void)LWLockAcquire(partition_lock, LW_SHARED);
    int buf_id = BufTableLookup(&new_tag, new_hash);

    do {
        if (buf_id >= 0) { /* read from memory */
            BufferDesc *bufDesc = GetBufferDescriptor(buf_id);
            bool valid = (strcmp(relation_type, "segment") == 0) ? SegPinBuffer(bufDesc) : PinBuffer(bufDesc, NULL);
            if (!valid)
                break; /* pin failed, read disk */
            LWLockRelease(partition_lock);
            (void)LWLockAcquire(BufferDescriptorGetContentLock(bufDesc), LW_SHARED); /* acquire content_lock */
            Buffer buf = BufferDescriptorGetBuffer(bufDesc);
            const PageHeader page = (const PageHeader)BufferGetPage(buf);
            rc = strcat_s(strOutput, MAXOUTPUTLEN, "The target page is from memory. ");
            securec_check(rc, "\0", "\0");
            if (strcmp(relation_type, "segment") == 0) {
                ParseOnePage(page, loc.blocknum, strOutput, relation_type, loc.blocknum, false); /* physical blocknum */
                LWLockRelease(BufferDescriptorGetContentLock(bufDesc));                   /* release content_lock */
                SegUnpinBuffer(bufDesc);
            } else {
                ParseOnePage(page, blockNum, strOutput, relation_type, blockNum, dumpUndo);
                LWLockRelease(BufferDescriptorGetContentLock(bufDesc)); /* release content_lock */
                UnpinBuffer(bufDesc, true);
            }
            CheckWriteFile(outputfile, outputFilename, strOutput);
            pfree_ext(strOutput);
            return true;
        }
    } while (0);
    LWLockRelease(partition_lock);
    return false;
}

static void CheckSegment(RelFileNode *relnode, ForkNumber forkNum, FILE *outputfile, char *outputFilename)
{
    SegSpace *spc = spc_open(relnode->spcNode, relnode->dbNode, false);
    if (spc == NULL || !spc_datafile_exist(spc, 1, forkNum)) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg("Page doesn't exist."))));
    }
    BlockNumber size = spc_size(spc, 1, forkNum);
    if (relnode->relNode >= size) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("The target page %u doesn't exist, the current max is %u.", relnode->relNode, size - 1))));
    }
    RelFileNode *relnodeHead = (RelFileNode *) palloc(sizeof(RelFileNode));
    relnodeHead->spcNode = relnode->spcNode;
    relnodeHead->dbNode = relnode->dbNode;
    relnodeHead->relNode = 1;
    relnodeHead->opt = relnode->opt;
    relnodeHead->bucketNode = relnode->bucketNode;
    relnodeHead->opt = relnode->opt;
    Buffer buffer_temp = ReadBufferFast(spc, *relnodeHead, forkNum, relnode->relNode, RBM_NORMAL);
    if (!BufferIsValid(buffer_temp)) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR, (errcode_for_file_access(), errmsg("Segment Head is invalid %u/%u/%u %d %u",
        relnodeHead->spcNode, relnodeHead->dbNode, relnodeHead->relNode, forkNum, relnode->relNode)));
    }
    pfree_ext(relnodeHead);
    SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetPage(buffer_temp));
    SegReleaseBuffer(buffer_temp);
    if (!(IsNormalSegmentHead(head) && relnode->bucketNode == SegmentBktId) &&
        !(IsBucketMainHead(head) && IsBucketFileNode(*relnode))) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Page header does not match with corresponding segment type."))));
    }
}

void ValidateParameterPath(RelFileNode rnode, char *str)
{
    char *path = relpathbackend(rnode, InvalidBackendId, MAIN_FORKNUM);
    if ((strcmp(path, str) != 0))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("\"%s\" is invalid input", str)));
}

char *ParsePage(char *path, int64 blocknum, char *relation_type, bool read_memory, bool dumpUndo)
{
    errno_t rc = EOK;

    /* check parameters */
    if (blocknum > MaxBlockNumber || blocknum < -1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("Blocknum should be between -1 and %u.", MaxBlockNumber))));
    /* initialize */
    char *strOutput = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
    rc = memset_s(strOutput, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
    securec_check(rc, "\0", "\0");

    RelFileNode *relnode = (RelFileNode *)palloc(sizeof(RelFileNode));
    char *outputFilename = (char *)palloc(MAXFILENAME * sizeof(char));

    rc = memset_s(outputFilename, MAXFILENAME, 0, MAXFILENAME);
    securec_check(rc, "\0", "\0");

    PrepForRead(path, blocknum, relation_type, outputFilename, relnode, true);
    ValidateParameterPath(*relnode, path);

    FILE *outputfile = fopen(outputFilename, "w");
    CheckOpenFile(outputfile, outputFilename);

    ForkNumber forkNum = MAIN_FORKNUM;
    SMgrRelation smgr = smgropen(*relnode, InvalidBackendId, GetColumnNum(forkNum));
    if (strcmp(relation_type, "segment") == 0)
        CheckSegment(relnode, forkNum, outputfile, outputFilename);
    BlockNumber maxBlockNum = smgrnblocks(smgr, forkNum);
    if ((blocknum != -1 && blocknum > maxBlockNum) || (maxBlockNum == 0)) {
        CheckCloseFile(outputfile, outputFilename, false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), (errmsg(
            "The max blocknum of current file is %u. Target blocknum exceeds current file size.", maxBlockNum))));
    }

    pfree_ext(relnode);
    BlockNumber blockNum = 0;
    BlockNumber block_endpoint = 0;
    /* only parse one block, check if it is in memory */
    if (read_memory) {
        blockNum = (BlockNumber)blocknum;
        if (readFromMemory(smgr, forkNum, blockNum, relation_type, strOutput, outputfile, outputFilename, dumpUndo)) {
            /* found in memory */
            CheckCloseFile(outputfile, outputFilename, true);
            smgrclose(smgr);
            return outputFilename;
        }
    }
    /* read from disk */
    rc = strcat_s(strOutput, MAXOUTPUTLEN, "The target page is from disk. ");
    securec_check(rc, "\0", "\0");
    CheckWriteFile(outputfile, outputFilename, strOutput);
    pfree_ext(strOutput);
    if (blocknum >= 0) { /* only parse one block */
        blockNum = blocknum;
        block_endpoint = blocknum;
    } else { /* blocknum == -1, parse all blocks */
        block_endpoint = maxBlockNum - 1;
    }
    /* if not declare a single block, then loop all blocks */
    while (blockNum <= block_endpoint) {
        CHECK_FOR_INTERRUPTS(); /* Allow cancel/die interrupts */
        char *strOutput = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
        rc = memset_s(strOutput, MAXOUTPUTLEN, 0, MAXOUTPUTLEN);
        securec_check(rc, "\0", "\0");
        char *buffer = (char *)palloc0(BLCKSZ);
        SMGR_READ_STATUS rdStatus = smgrread(smgr, forkNum, blockNum, buffer);
        const PageHeader page = (const PageHeader)buffer;
        char *strOutput1 = (char *)palloc(MAXOUTPUTLEN * sizeof(char));
        rc = memset_s(strOutput1, SHORTOUTPUTLEN, 0, SHORTOUTPUTLEN);
        securec_check(rc, "\0", "\0");
        check_rdStatus(rdStatus, blockNum, page, strOutput1);
        CheckWriteFile(outputfile, outputFilename, strOutput1);
        pfree_ext(strOutput1);
        if (strcmp(relation_type, "segment") == 0) {
            SegPageLocation loc = seg_get_physical_location(smgr->smgr_rnode.node, forkNum, blockNum);
            SegPageLocation endloc = seg_get_physical_location(smgr->smgr_rnode.node, forkNum, block_endpoint);
            ParseOnePage(page, loc.blocknum, strOutput, relation_type, endloc.blocknum, false); /* physical blocknum */
        } else
            ParseOnePage(page, blockNum, strOutput, relation_type, block_endpoint, dumpUndo);

        CheckWriteFile(outputfile, outputFilename, strOutput);
        pfree_ext(strOutput);
        pfree_ext(buffer);
        blockNum++;
    }
    CheckCloseFile(outputfile, outputFilename, true);
    smgrclose(smgr);
    return outputFilename;
}

static void ParseUHeapPageHeader(void *page, BlockNumber blkno, BlockNumber endBlk, char *output)
{
    errno_t rc = EOK;
    bool chksumResult = false;
    UHeapPageHeader pageHeader = NULL;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (UHeapPageHeader)page;
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "Page information of block %u/%u\n",
        blkno, endBlk);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_lsn: %X/%X\n",
        (uint32)(PageGetLSN(pageHeader) >> XIDTHIRTYTWO), (uint32)PageGetLSN(pageHeader));
    securec_check_ss(rc, "\0", "\0");

    if (CheckPageZeroCases((PageHeader)pageHeader)) {
        uint16 checksum = pg_checksum_page((char *)pageHeader, (BlockNumber)blkno);
        chksumResult = (checksum == pageHeader->pd_checksum);
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_checksum: 0x%X, verify %s\n",
        pageHeader->pd_checksum, chksumResult ? "success" : "fail");
    securec_check_ss(rc, "\0", "\0");

    if (UPageHasFreeLinePointers(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PAGE_HAS_FREE_LINES.");
        securec_check_ss(rc, "\0", "\0");
    }

    if (UPageIsFull(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PAGE_FULL.");
        securec_check_ss(rc, "\0", "\0");
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\tpd_lower: %u, %s\n\tpd_upper: %u, %s\n\tpd_special: %u, size %u\n", pageHeader->pd_lower,
        PageIsEmpty(pageHeader) ? "empty" : "non-empty", pageHeader->pd_upper,
        PageIsNew(pageHeader) ? "new page" : "old page", pageHeader->pd_special, PageGetSpecialSize(pageHeader));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tPage size & version: %u, %u\n",
        (uint16)PageGetPageSize(pageHeader), (uint16)PageGetPageLayoutVersion(pageHeader));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpotential_freespace: %u\n",
        pageHeader->potential_freespace);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\ttd_count: %u\n", pageHeader->td_count);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_prune_xid: %lu\n",
        pageHeader->pd_prune_xid);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\tpd_xid_base: %lu, pd_multi_base: %lu\n", pageHeader->pd_xid_base, pageHeader->pd_multi_base);
    securec_check_ss(rc, "\0", "\0");
    return;
}

static void ParseUHeapPageTDInfo(void *page, char *output)
{
    TD *tdInfo = NULL;
    uint16 tdCount = 0;
    UHeapPageTDData *tdPtr = NULL;
    UHeapPageHeader pageHeader = NULL;
    errno_t rc = EOK;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (UHeapPageHeader)page;
    tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    tdCount = pageHeader->td_count;
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\n\n\tUHeap Page TD information, nTDSlots = %u\n", tdCount);
    securec_check_ss(rc, "\0", "\0");

    for (uint16 i = 0; i < tdCount; i++) {
        tdInfo = &(tdPtr->td_info[i]);
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "\n\t\t TD Slot #%d, xid:%lu, urp:%lu\n", i + 1, tdInfo->xactid, tdInfo->undo_record_ptr);
        securec_check_ss(rc, "\0", "\0");
    }
    return;
}

static void ParseUHeapPageItem(Item item, UHeapTuple tuple, char *output)
{
    if (item == NULL || tuple == NULL || output == NULL) {
        return;
    }

    UHeapDiskTuple diskTuple = (UHeapDiskTuple)item;
    errno_t rc = EOK;
    tuple->disk_tuple = diskTuple;
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\t\t\txid:%lu, td:%d, locker_td %d\n",
        UHeapTupleGetRawXid(tuple), diskTuple->td_id, diskTuple->reserved);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\t\t\tFlag:%u", diskTuple->flag);
    securec_check_ss(rc, "\0", "\0");

    if (diskTuple->flag & UHEAP_HAS_NULL) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_HASNULL");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_DELETED) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_DELETED\n");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_INPLACE_UPDATED) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_INPLACE_UPDATED ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_UPDATED) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_UPDATED ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_XID_KEYSHR_LOCK) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_XID_KEYSHR_LOCK ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_XID_NOKEY_EXCL_LOCK) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_XID_NOKEY_EXCL_LOCK ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_XID_EXCL_LOCK) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_XID_EXCL_LOCK ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_MULTI_LOCKERS) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_MULTI_LOCKERS ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & UHEAP_INVALID_XACT_SLOT) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tUHEAP_INVALID_XACT_SLOT ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & SINGLE_LOCKER_XID_IS_LOCK) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tSINGLE_LOCKER_XID_IS_LOCK ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (diskTuple->flag & SINGLE_LOCKER_XID_IS_SUBXACT) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "\n\t\t\tSINGLE_LOCKER_XID_IS_SUBXACT ");
        securec_check_ss(rc, "\0", "\0");
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tFlag2:");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\t\tNumber of columns:: %d\n",
        UHeapTupleHeaderGetNatts(diskTuple));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\t\t\tHoff: %u\n", diskTuple->t_hoff);
    securec_check_ss(rc, "\0", "\0");
    return;
}

static int OpenUndoSeg(int zoneId, BlockNumber blkNo)
{
    char fileName[MAXUNDOFILENAMELEN] = {0};
    errno_t rc = EOK;
    int segno = blkNo / UNDOSEG_SIZE;
    DECLARE_NODE_COUNT();
    GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount);
    const char *persistence = (upersistence == UNDO_PERMANENT) ? "permanent" : ((upersistence == UNDO_TEMP) ? "temp" :
        "unlogged");
    rc = snprintf_s(fileName, sizeof(fileName), sizeof(fileName), "undo/%s/%05X.%07zX", persistence, zoneId, segno);
    securec_check_ss(rc, "\0", "\0");
    int fd = open(fileName, O_RDONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        return INVALID_FD;
    }

    return fd;
}

bool DumpUndoRecordsBytes(char *destptr, int destlen, char **readeptr, char *endptr, int *myBytesRead, int *alreadyRead)
{
    if (*myBytesRead >= destlen) {
        *myBytesRead -= destlen;
        return true;
    }
    int remaining = destlen - *myBytesRead;
    int maxReadOnCurrPage = endptr - *readeptr;
    int canRead = Min(remaining, maxReadOnCurrPage);
    if (canRead == 0) {
        return false;
    }
    errno_t rc = memcpy_s(destptr + *myBytesRead, remaining, *readeptr, canRead);
    securec_check(rc, "\0", "\0");
    *readeptr += canRead;
    *alreadyRead += canRead;
    *myBytesRead = 0;
    return (canRead == remaining);
}

bool ReadUndoRecordByUrp(UndoHeader *urec, char *buffer, int startingByte, int *alreadyRead)
{
    char *readptr = buffer + startingByte;
    char *endptr = buffer + BLCKSZ;
    int myBytesRead = *alreadyRead;

    if (!DumpUndoRecordsBytes((char*)&(urec->whdr), SIZE_OF_UNDO_RECORD_HEADER,
        &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }
    if (!DumpUndoRecordsBytes((char*)&(urec->wblk), SIZE_OF_UNDO_RECORD_BLOCK,
        &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }
    if ((urec->whdr.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        if (!DumpUndoRecordsBytes((char *)&urec->wtxn, SIZE_OF_UNDO_RECORD_TRANSACTION,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        if (!DumpUndoRecordsBytes((char *)&urec->wtd, SIZE_OF_UNDO_RECORD_OLDTD,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        if (!DumpUndoRecordsBytes((char *)&urec->wpart, SIZE_OF_UNDO_RECORD_PARTITION,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        if (!DumpUndoRecordsBytes((char *)&urec->wtspc, SIZE_OF_UNDO_RECORD_TABLESPACE,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((urec->whdr.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        if (!DumpUndoRecordsBytes((char *)&urec->wpay, SIZE_OF_UNDO_RECORD_PAYLOAD,
            &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }

        urec->rawdata.len = urec->wpay.payloadlen;
        if (urec->rawdata.len > 0) {
            if (urec->rawdata.data == NULL) {
                urec->rawdata.data = (char *)malloc(urec->rawdata.len);
                if (NULL == urec->rawdata.data) {
                    ereport(WARNING,
                        (errcode(ERRCODE_OUT_OF_MEMORY),
                            errmodule(MOD_USTORE),
                            errmsg("Alloc memory failed."),
                            errdetail("N/A"),
                            errcause("Out of memory"),
                            erraction("N/A")));
                    return false;
                }
            }
            if (!DumpUndoRecordsBytes((char *)urec->rawdata.data, urec->rawdata.len,
                &readptr, endptr, &myBytesRead, alreadyRead)) {
                return false;
            }
        }
    }
    return true;
}

static bool DumpUndoRecod(UndoRecord *urec)
{
    if (urec == NULL) {
        return false;
    }

    bool found = false;
    char undoBuf[BLCKSZ] = {'\0'};
    off_t seekpos;
    int zoneId = UNDO_PTR_GET_ZONE_ID(urec->Urp());
    BlockNumber blockNo = UNDO_PTR_GET_BLOCK_NUM(urec->Urp());
    uint32 startBytes = UNDO_PTR_GET_PAGE_OFFSET(urec->Urp());
    int alreadyRead = 0;
    uint32 ret = 0;
    int fd = INVALID_FD;
    errno_t rc = EOK;
    UndoHeader *urecInfo = NULL;

    urecInfo = (UndoHeader *)palloc0(sizeof(UndoHeader));

    do {
        fd = OpenUndoSeg(zoneId, blockNo);
        if (fd == INVALID_FD) {
            pfree(urecInfo);
            return false;
        }
        seekpos = (off_t)BLCKSZ * (blockNo % ((BlockNumber)UNDOSEG_SIZE));
        lseek(fd, seekpos, SEEK_SET);
        rc = memset_s(undoBuf, BLCKSZ, 0, BLCKSZ);
        securec_check(rc, "\0", "\0");
        ret = read(fd, (char *)undoBuf, BLCKSZ);
        if (ret != BLCKSZ) {
            ereport(WARNING,
                (errcode(ERRCODE_OPERATE_FAILED),
                    errmodule(MOD_USTORE),
                    errmsg("Read undo with urp(%lu) failed, errno(%d).", urec->Urp(), errno),
                    errdetail("N/A"),
                    errcause("Read size is not a block size."),
                    erraction("N/A")));
            free(urecInfo->rawdata.data);
            pfree(urecInfo);
            close(fd);
            return false;
        }

        if (ReadUndoRecordByUrp(urecInfo, undoBuf, startBytes, &alreadyRead)) {
            found = true;
            /* Set undorecord. */
            urec->SetXid(urecInfo->whdr.xid);
            urec->SetCid(urecInfo->whdr.cid);
            urec->SetReloid(urecInfo->whdr.reloid);
            urec->SetRelfilenode(urecInfo->whdr.relfilenode);
            urec->SetUtype(urecInfo->whdr.utype);
            urec->SetUinfo(urecInfo->whdr.uinfo);

            urec->SetBlkprev(urecInfo->wblk.blkprev);
            urec->SetBlkno(urecInfo->wblk.blkno);
            urec->SetOffset(urecInfo->wblk.offset);
            urec->SetPrevurp(urecInfo->wtxn.prevurp);
            urec->SetOldXactId(urecInfo->wtd.oldxactid);
            urec->SetPartitionoid(urecInfo->wpart.partitionoid);
            urec->SetTablespace(urecInfo->wtspc.tablespace);
            urec->SetPayLoadLen(urecInfo->wpay.payloadlen);
            if (urecInfo->wpay.payloadlen) {
                initStringInfo(urec->Rawdata());
                appendBinaryStringInfo(urec->Rawdata(), urecInfo->rawdata.data, urecInfo->wpay.payloadlen);
            }
            break;
        }

        startBytes = UNDO_LOG_BLOCK_HEADER_SIZE;
        blockNo++;
        close(fd);
        CHECK_FOR_INTERRUPTS();
    } while (true);

    if (urecInfo->rawdata.data != NULL) {
        free(urecInfo->rawdata.data);
    }
    pfree(urecInfo);
    close(fd);
    return found;
}

static void DumpUndoRecordByUrpFromPage(void *page, ItemPointer ctid, UndoRecPtr urp, int tdSlot,
    UHeapDiskTuple diskTuple, char *output)
{
    errno_t rc = EOK;
    int prevTdSlot = tdSlot;
    int currentTdSlot = tdSlot;
    UndoRecPtr currUrp = urp;
fetch_undo:
    VerifyMemoryContext();
    UndoRecord *urec = New(CurrentMemoryContext)UndoRecord();
    urec->SetUrp(currUrp);
    bool fetch = DumpUndoRecod(urec);
    if (fetch) {
       Assert(urec != NULL);
       if (urec->Utype() == UNDO_INSERT || urec->Utype() == UNDO_MULTI_INSERT) {
            diskTuple->flag &= ~UHEAP_VIS_STATUS_MASK;
            diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
        } else if (urec->Utype() == UNDO_INPLACE_UPDATE) {
            Assert(urec->Rawdata() != NULL);
            Assert(urec->Rawdata()->len >= (int)SizeOfUHeapDiskTupleData);
            rc = memcpy_s((char *)diskTuple + OffsetTdId, SizeOfUHeapDiskTupleHeaderExceptXid,
                urec->Rawdata()->data + sizeof(uint8), SizeOfUHeapDiskTupleHeaderExceptXid);
            securec_check(rc, "", "");
            diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
        } else {
            Assert(urec->Rawdata() != NULL);
            Assert(urec->Rawdata()->len >= (int)SizeOfUHeapDiskTupleHeaderExceptXid);
            rc = memcpy_s(((char *)diskTuple + OffsetTdId), SizeOfUHeapDiskTupleHeaderExceptXid,
                urec->Rawdata()->data, SizeOfUHeapDiskTupleHeaderExceptXid);
            securec_check(rc, "", "");
            diskTuple->xid = (ShortTransactionId)FrozenTransactionId;
        }

        currentTdSlot = diskTuple->td_id;

        /* Find matched undo version, and output. */
        if (ItemPointerGetBlockNumber(ctid) == urec->Blkno() && urec->Offset() == ctid->ip_posid) {
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\tUndoRecPtr(%lu): whdr = xid(%lu), cid(%u), reloid(%u), relfilenode(%u), utype(%u), uinfo(%u).\n",
                urec->Urp(), urec->Xid(), urec->Cid(), urec->Reloid(), urec->Relfilenode(), urec->Utype(), urec->Uinfo());
            securec_check_ss(rc, "\0", "\0");

            if ((urec->Uinfo() & UNDO_UREC_INFO_PAYLOAD) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_payload, size = %lu.\n", SIZE_OF_UNDO_RECORD_PAYLOAD);
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_TRANSAC) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_transac, size = %lu.\n", SIZE_OF_UNDO_RECORD_TRANSACTION);
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_BLOCK) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_block, size = %lu.\n", SIZE_OF_UNDO_RECORD_BLOCK);
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_OLDTD) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_oldtd, size = %lu.\n", SIZE_OF_UNDO_RECORD_OLDTD);
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_CONTAINS_SUBXACT) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_subxact.\n");
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_partoid, size = %lu.\n", SIZE_OF_UNDO_RECORD_PARTITION);
                securec_check_ss(rc, "\0", "\0");
            }
            if ((urec->Uinfo() & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tflag_tablespaceoid, size = %lu.\n", SIZE_OF_UNDO_RECORD_TABLESPACE);
                securec_check_ss(rc, "\0", "\0");
            }

            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twblk = blk_prev(%lu), blockno(%u), offset(%u).\n", urec->Blkprev(), urec->Blkno(), urec->Offset());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twtxn = prevurp(%lu).\n", urec->Prevurp2());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twpay = payloadlen(%u).\n", urec->PayLoadLen());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twtd = oldcaxtid(%lu).\n", urec->OldXactId());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twpart_ = partitionoid(%u).\n", urec->Partitionoid());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\twtspc_ = tablespace(%u).\n", urec->Tablespace());
            securec_check_ss(rc, "\0", "\0");
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\n\t\tUndo diskTuple: td_id %u, reserved %u, flag %u, flag2 %u, t_hoff %u.\n",
                diskTuple->td_id, diskTuple->reserved, diskTuple->flag, diskTuple->flag2, diskTuple->t_hoff);
            securec_check_ss(rc, "\0", "\0");
        }
    }

    if (prevTdSlot == currentTdSlot) {
        if (IS_VALID_UNDO_REC_PTR(urec->Blkprev())) {
            currUrp = urec->Blkprev();
            DELETE_EX(urec);
            CHECK_FOR_INTERRUPTS();
            goto fetch_undo;
        }
    } else {
        if (currentTdSlot == UHEAPTUP_SLOT_FROZEN) {
           goto fetch_end;
        } else {
            prevTdSlot = currentTdSlot;
            UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
            UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
            if (currentTdSlot < 1 || currentTdSlot > phdr->td_count) {
                ereport(WARNING,
                    (errcode(ERRCODE_OPERATE_FAILED),
                        errmodule(MOD_USTORE),
                        errmsg("CurrentTd(%d) is out of td(%d) in page header", currentTdSlot, phdr->td_count),
                        errdetail("N/A"),
                        errcause("Td slot is corrupted"),
                        erraction("N/A")));
                goto fetch_end;
            } else {
                TD *thistrans = &tdPtr->td_info[currentTdSlot - 1];
                currUrp = thistrans->undo_record_ptr;
                DELETE_EX(urec);
                CHECK_FOR_INTERRUPTS();
                goto fetch_undo;
            }
        }
    }

fetch_end:
    DELETE_EX(urec);
    return;
}

static void ParseUHeapPageItemUndo(void *page, ItemPointer ctid, char *output)
{
    if (page == NULL || ctid == NULL || output == NULL) {
        return;
    }

    int tdSlot = 0;
    errno_t rc = EOK;
    UHeapPageHeader pHeader = (UHeapPageHeader) page;
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    RowPtr *rp = UPageGetRowPtr(pHeader, ctid->ip_posid);

    if (RowPtrIsNormal(rp)) {
        UHeapDiskTuple udiskTuple = NULL;
        UHeapDiskTupleData inputDiskTuple;
        TD* tdInfo = NULL;
        udiskTuple = (UHeapDiskTuple) UPageGetRowData(pHeader, rp);
        Assert(udiskTuple != NULL);
        tdSlot = udiskTuple->td_id;
        if (tdSlot == UHEAPTUP_SLOT_FROZEN) {
            return;
        }
        tdInfo = &(tdPtr->td_info[tdSlot - 1]);
        rc = memset_s((char *) &inputDiskTuple, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
        securec_check_c(rc, "\0", "\0");
        rc = memcpy_s(&inputDiskTuple, SizeOfUHeapDiskTupleData, udiskTuple, SizeOfUHeapDiskTupleData);
        securec_check_c(rc, "\0", "\0");
        DumpUndoRecordByUrpFromPage(page, ctid, tdInfo->undo_record_ptr, tdSlot, &inputDiskTuple, output);
    } else if (RowPtrIsDeleted(rp)) {
        TD* tdInfo = NULL;
        UHeapDiskTupleData inputDiskTuple;
        tdSlot = RowPtrGetTDSlot(rp);
        if (tdSlot == UHEAPTUP_SLOT_FROZEN) {
            return;
        }
        tdInfo = &(tdPtr->td_info[tdSlot - 1]);
        rc = memset_s((char *) &inputDiskTuple, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
        securec_check_c(rc, "\0", "\0");
        DumpUndoRecordByUrpFromPage(page, ctid, tdInfo->undo_record_ptr, tdSlot, &inputDiskTuple, output);
    }
}

static void ParseUHeapPageData(void *page, BlockNumber blkno, char *output, bool dumpUndo)
{
    errno_t rc;
    uint32 rowPtrCnt = 0;
    uint32 storeCnt = 0;
    uint32 unusedCnt = 0;
    uint32 normalCnt = 0;
    uint32 deadCnt = 0;
    uint32 redirectCnt = 0;
    Item item;
    RowPtr *rowptr;
    UHeapTupleData utuple;
    UHeapPageHeader pageHeader = NULL;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (UHeapPageHeader)page;
    if (pageHeader->pd_lower <= SizeOfUHeapPageHeaderData) {
        rowPtrCnt = 0;
    } else {
        rowPtrCnt =
            (pageHeader->pd_lower - (SizeOfUHeapPageHeaderData + SizeOfUHeapTDData(pageHeader))) / sizeof(RowPtr);
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\n\tUHeap tuple information on this page\n");
    securec_check_ss(rc, "\0", "\0");

    for (OffsetNumber i = FirstOffsetNumber; i <= rowPtrCnt; i++) {
        rowptr = UPageGetRowPtr(pageHeader, i);
        if (RowPtrIsUsed(rowptr)) {
            if (RowPtrHasStorage(rowptr)) {
                storeCnt++;
            }

            if (RowPtrIsNormal(rowptr)) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is normal: length %u, offset %u\n", i, RowPtrGetLen(rowptr),
                    RowPtrGetOffset(rowptr));
                securec_check_ss(rc, "\0", "\0");
                normalCnt++;
                item = UPageGetRowData(pageHeader, rowptr);
                UHeapTupleCopyBaseFromPage(&utuple, pageHeader);
                ParseUHeapPageItem(item, &utuple, output);
                if (dumpUndo) {
                    ItemPointerData ctid;
                    ItemPointerSet(&ctid, blkno, i);
                    ParseUHeapPageItemUndo(page, &ctid, output);
                }
            } else if (RowPtrIsDead(rowptr)) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is dead: length %u, offset %u", i, RowPtrGetLen(rowptr), RowPtrGetOffset(rowptr));
                securec_check_ss(rc, "\0", "\0");
                deadCnt++;
            } else {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is redirected: length %u, offset %u", i, RowPtrGetLen(rowptr),
                    RowPtrGetOffset(rowptr));
                securec_check_ss(rc, "\0", "\0");
                redirectCnt++;
            }
        } else {
            unusedCnt++;
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\tTuple #%u is unused\n", i);
            securec_check_ss(rc, "\0", "\0");
        }
    }
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\tSummary (%u total): %u unused, %u normal, %u dead, %u redirect\n", rowPtrCnt, unusedCnt, normalCnt, deadCnt,
        redirectCnt);
    securec_check_ss(rc, "\0", "\0");
}

static void ParseUHeapPageSpecialInfo(void *page, char *output)
{
    errno_t rc = 0;

    if (page == NULL || output == NULL) {
        return;
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\n\tSpecial area information on this page\n");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\tNot used currently.\n");
    securec_check_ss(rc, "\0", "\0");
}

static void ParseUHeapPage(void *page, BlockNumber blkno, BlockNumber endBlk, char *output, bool dumpUndo)
{
    uint16 pageHeaderSize = 0;
    UHeapPageHeader pageHeader = NULL;
    errno_t rc;

    if (page == NULL || output == NULL) {
        return;
    }

    /* Parse header of uheap page. */
    pageHeader = (UHeapPageHeader)page;

    /* Check invalidation of uheap page head. */
    pageHeaderSize = GetPageHeaderSize(pageHeader);
    if (pageHeader->pd_lower < pageHeaderSize || pageHeader->pd_lower > pageHeader->pd_upper ||
        pageHeader->pd_upper > pageHeader->pd_special || pageHeader->pd_special > BLCKSZ ||
        pageHeader->pd_special != MAXALIGN(pageHeader->pd_special)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u\n",
            pageHeader->pd_lower, pageHeader->pd_upper, pageHeader->pd_special);
        securec_check_ss(rc, "\0", "\0");
        return;
    }

    ParseUHeapPageHeader((void *)pageHeader, blkno, endBlk, output);
    if (PageIsNew(pageHeader)) {
        return;
    }

    /* Parse td slot info of uheap page. */
    ParseUHeapPageTDInfo(pageHeader, output);
    /* Parse items of uheap page. */
    ParseUHeapPageData(pageHeader, blkno, output, dumpUndo);
    /* Parse special area of uheap page. */
    ParseUHeapPageSpecialInfo(pageHeader, output);
}

static void ParseIndexPageHeader(void *page, int type, BlockNumber blkno, BlockNumber endBlk, char *output)
{
    errno_t rc = 0;
    bool chksumResult = false;
    PageHeader pageHeader = NULL;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (PageHeader)page;
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "%s index page information of block %u/%u\n", (type == BTREE_INDEX) ? "Btree" : "UBtree", blkno, endBlk);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_lsn: %X/%X\n",
        (uint32)(PageGetLSN((Page)pageHeader) >> XIDTHIRTYTWO), (uint32)PageGetLSN((Page)pageHeader));
    securec_check_ss(rc, "\0", "\0");

    if (CheckPageZeroCases(pageHeader)) {
        uint16 checksum = pg_checksum_page((char *)pageHeader, (BlockNumber)blkno);
        chksumResult = (checksum == pageHeader->pd_checksum);
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_checksum: 0x%X, verify %s\n",
        pageHeader->pd_checksum, chksumResult ? "success" : "fail");
    securec_check_ss(rc, "\0", "\0");

    if (PageHasFreeLinePointers((Page)pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_HAS_FREE_LINES.");
        securec_check_ss(rc, "\0", "\0");
    }
    if (PageIsFull(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_PAGE_FULL.");
        securec_check_ss(rc, "\0", "\0");
    }
    if (PageIsAllVisible(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_ALL_VISIBLE.");
        securec_check_ss(rc, "\0", "\0");
    }
    if (PageIsCompressed(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_COMPRESSED_PAGE.");
        securec_check_ss(rc, "\0", "\0");
    }
    if (PageIsLogical(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_LOGICAL_PAGE.");
        securec_check_ss(rc, "\0", "\0");
    }
    if (PageIsEncrypt(pageHeader)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "PD_ENCRYPT_PAGE.");
        securec_check_ss(rc, "\0", "\0");
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\tpd_lower: %u, %s\n\tpd_upper: %u, %s\n\tpd_special: %u, size %u\n", pageHeader->pd_lower,
        PageIsEmpty((Page)pageHeader) ? "empty" : "non-empty", pageHeader->pd_upper,
        PageIsNew((Page)pageHeader) ? "new page" : "old page", pageHeader->pd_special,
        PageGetSpecialSize((Page)pageHeader));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tPage size & version: %u, %u\n",
        (uint16)PageGetPageSize(page), (uint16)PageGetPageLayoutVersion(page));
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_prune_xid: %lu\n",
        pageHeader->pd_prune_xid + ((HeapPageHeader)(pageHeader))->pd_xid_base);
    securec_check_ss(rc, "\0", "\0");
    rc =
        snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tpd_xid_base: %lu, pd_multi_base: %lu\n",
        ((HeapPageHeader)(pageHeader))->pd_xid_base, ((HeapPageHeader)(pageHeader))->pd_multi_base);
    securec_check_ss(rc, "\0", "\0");

    return;
}

static void ParseIndexPageItem(Item item, int type, uint32 len, char *output)
{
    if (item == NULL || output == NULL) {
        return;
    }

    errno_t rc = 0;
    IndexTuple itup = (IndexTuple)item;
    bool hasnull = (itup->t_info & INDEX_NULL_MASK);
    unsigned int tuplen = (itup->t_info & INDEX_SIZE_MASK);
    if (tuplen != len) {
        if (type == UBTREE_INDEX) {
            UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                "\t\t\txmin:%d xmax:%d Heap Tid: block %u/%u, offset %u\n", uxid->xmin, uxid->xmax,
                itup->t_tid.ip_blkid.bi_hi, itup->t_tid.ip_blkid.bi_lo, itup->t_tid.ip_posid);
            securec_check_ss(rc, "\0", "\0");
        }
    } else {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "\t\t\tHeap Tid: block %u/%u, offset %u\n", itup->t_tid.ip_blkid.bi_hi, itup->t_tid.ip_blkid.bi_lo,
            itup->t_tid.ip_posid);
        securec_check_ss(rc, "\0", "\0");
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\t\t\tLength: %u", tuplen);
    securec_check_ss(rc, "\0", "\0");

    if (itup->t_info & INDEX_VAR_MASK) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, ", has var-width attrs");
        securec_check_ss(rc, "\0", "\0");
    }

    if (hasnull) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, ", has nulls ");
        securec_check_ss(rc, "\0", "\0");
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n");
    securec_check_ss(rc, "\0", "\0");
    return;
}

static void ParseIndexPageSpecialInfo(void *page, int type, char *output)
{
    errno_t rc = 0;
    BTPageOpaqueInternal opaque = NULL;
    UBTPageOpaqueInternal uopaque = NULL;
    PageHeader pageHeader = NULL;

    if (page == NULL || output == NULL || type >= INDEX_BOTT) {
        return;
    }

    pageHeader = (PageHeader)page;
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer((Page)pageHeader);
    if (PageGetSpecialSize((Page)pageHeader) > MAXALIGN(sizeof(BTPageOpaqueData))) {
        uopaque = (UBTPageOpaqueInternal)opaque;
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n%s index special information:\n",
        (type == BTREE_INDEX) ? "BTree" : "UBTree");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tbtree left sibling: %u\n",
        opaque->btpo_prev);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tbtree right sibling: %u\n",
        opaque->btpo_next);
    securec_check_ss(rc, "\0", "\0");

    if (!P_ISDELETED(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tbtree tree level: %u\n",
            opaque->btpo.level);
        securec_check_ss(rc, "\0", "\0");
    } else {
        if (uopaque) {
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tnext txid (deleted): %lu\n",
                ((UBTPageOpaque)uopaque)->xact);
            securec_check_ss(rc, "\0", "\0");
        } else {
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tnext txid (deleted): %lu\n",
                ((BTPageOpaque)opaque)->xact);
            securec_check_ss(rc, "\0", "\0");
        }
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tbtree flag: ");
    securec_check_ss(rc, "\0", "\0");

    if (P_ISLEAF(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_LEAF ");
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_INTERNAL ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (P_ISROOT(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_ROOT ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (P_ISDELETED(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_DELETED ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (P_ISHALFDEAD(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_HALF_DEAD ");
        securec_check_ss(rc, "\0", "\0");
    }
    if (P_HAS_GARBAGE(opaque)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "BTP_HAS_GARBAGE ");
        securec_check_ss(rc, "\0", "\0");
    }
    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n");
    securec_check_ss(rc, "\0", "\0");

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tbtree cycle ID: %u\n ",
        opaque->btpo_cycleid);
    securec_check_ss(rc, "\0", "\0");

    if (uopaque) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\tubtree active tuples: %d\n",
            uopaque->activeTupleCount);
        securec_check_ss(rc, "\0", "\0");
    }

    return;
}

static void ParseIndexPageData(void *page, int type, char *output)
{
    errno_t rc;
    uint32 rowPtrCnt = 0;
    uint32 storeCnt = 0;
    uint32 notusedCnt = 0;
    uint32 normalCnt = 0;
    uint32 deadCnt = 0;
    uint32 redirectCnt = 0;
    Item item;
    ItemId lp;
    PageHeader pageHeader = NULL;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (PageHeader)page;
    rowPtrCnt = PageGetMaxOffsetNumber((Page)pageHeader);

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\n\tIndex tuple information on this page\n");
    securec_check_ss(rc, "\0", "\0");

    for (uint32 i = FirstOffsetNumber; i <= rowPtrCnt; i++) {
        lp = PageGetItemId((Page)pageHeader, i);
        if (ItemIdIsUsed(lp)) {
            if (ItemIdHasStorage(lp)) {
                storeCnt++;
            }
            if (ItemIdIsNormal(lp) || (IndexItemIdIsFrozen(lp))) {
                rc = ItemIdIsNormal(lp) ? (snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is normal: length %u, offset %u\n", i, ItemIdGetLength(lp), ItemIdGetOffset(lp))) :
                    (snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is frozen: length %u, offset %u\n", i, ItemIdGetLength(lp), ItemIdGetOffset(lp)));
                securec_check_ss(rc, "\0", "\0");
                normalCnt++;
                item = PageGetItem((Page)pageHeader, lp);
                ParseIndexPageItem(item, type, ItemIdGetLength(lp), output);
            } else if (ItemIdIsDead(lp)) {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is dead: length %u, offset %u", i, ItemIdGetLength(lp), ItemIdGetOffset(lp));
                securec_check_ss(rc, "\0", "\0");
                deadCnt++;
            } else {
                rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
                    "\n\t\tTuple #%u is redirected: length %u, offset %u", i, ItemIdGetLength(lp), ItemIdGetOffset(lp));
                securec_check_ss(rc, "\0", "\0");
                redirectCnt++;
            }
        } else {
            notusedCnt++;
            rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN, "\n\t\tTuple #%u is unused\n", i);
            securec_check_ss(rc, "\0", "\0");
        }
    }

    rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
        "\tSummary (%u total): %u unused, %u normal, %u dead, %u redirect\n", rowPtrCnt, notusedCnt, normalCnt, deadCnt,
        redirectCnt);
    securec_check_ss(rc, "\0", "\0");
    return;
}

static void ParseIndexPage(void *page, int type, BlockNumber blkno, BlockNumber endBlk, char *output)
{
    PageHeader pageHeader = NULL;
    uint16 headersize = 0;
    errno_t rc = 0;

    if (page == NULL || output == NULL) {
        return;
    }

    pageHeader = (PageHeader)page;
    headersize = GetPageHeaderSize(pageHeader);
    if (pageHeader->pd_lower < headersize || pageHeader->pd_lower > pageHeader->pd_upper ||
        pageHeader->pd_upper > pageHeader->pd_special || pageHeader->pd_special > BLCKSZ ||
        pageHeader->pd_special != MAXALIGN(pageHeader->pd_special)) {
        rc = snprintf_s(output + (int)strlen(output), MAXOUTPUTLEN, MAXOUTPUTLEN,
            "The page data is corrupted, corrupted page pointers: lower = %u, upper = %u, special = %u\n",
            pageHeader->pd_lower, pageHeader->pd_upper, pageHeader->pd_special);
        securec_check_ss(rc, "\0", "\0");
        return;
    }

    ParseIndexPageHeader((void *)page, type, blkno, endBlk, output);
    if (PageIsNew(pageHeader)) {
        return;
    }

    ParseIndexPageData((void *)page, type, output);
    ParseIndexPageSpecialInfo((void *)page, type, output);
    return;
}

Datum gs_parse_page_bypath(PG_FUNCTION_ARGS)
{
    /* check user's right */
    const char fName[MAXFNAMELEN] = "gs_parse_page_bypath";
    CheckUser(fName);

    /* read in parameters */
    char *path = text_to_cstring(PG_GETARG_TEXT_P(0));
    int64 blocknum = PG_GETARG_INT64(1);
    char *relation_type = text_to_cstring(PG_GETARG_TEXT_P(2));
    bool read_memory = PG_GETARG_BOOL(3);

    /* In order to avoid querying the shared buffer and applying LW locks, blocking the business. */
    /* In the case of finding all pages, force to check disk  */
    if (blocknum == -1) {
        read_memory = false;
    }
    char *outputFilename = ParsePage(path, blocknum, relation_type, read_memory);
    PG_RETURN_TEXT_P(cstring_to_text(outputFilename));
}