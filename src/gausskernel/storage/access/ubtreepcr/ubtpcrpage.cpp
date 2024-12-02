/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrpage.cpp
 *       BTree pcr specific page management code for the openGauss ubtree access method.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrpage.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"


/*
 *	UBTreePCRPageInit() -- Initialize a new page.
 *
 * On return, the page header is initialized; data space is empty;
 * special space is zeroed out.
 */
void UBTreePCRPageInit(Page page, Size size)
{
    PageInit(page, size, sizeof(UBTPCRPageOpaqueData));
    ((UBTPCRPageOpaque)PageGetSpecialPointer(page))->last_delete_xid = 0;
}

/*
 *	UBTreePCRInitMetaPage() -- Fill a page buffer with a correct metapage image
 */
void UBTreePCRInitMetaPage(Page page, BlockNumber rootbknum, uint32 level)
{
    BTMetaPageData *metad = NULL;
    UBTPCRPageOpaque metaopaque;

    UBTreePCRPageInit(page, BLCKSZ);
    
    metad = BTPageGetMeta(page);
    metad->btm_magic = BTREE_MAGIC;
    metad->btm_version = UBTREE_PCR_VERSION;
    metad->btm_root = rootbknum;
    metad->btm_level = level;
    metad->btm_fastroot = rootbknum;
    metad->btm_fastlevel = level;

    metaopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    metaopaque->btpo_flags = BTP_META;

    /*
     * Set pd_lower just past the end of the metadata.	This is not essential
     * but it makes the page look compressible to xlog.c.
     */
    ((PageHeader)page)->pd_lower = (uint16)(((char *)metad + sizeof(BTMetaPageData)) - (char *)page);
}


Buffer UBTreePCRGetRoot(Relation rel, int access)
{
    return NULL;
}

bool UBTreePCRPageRecyclable(Page page)
{
    return false;
}

int UBTreePCRPageDel(Relation rel, Buffer buf, BTStack del_blknos)
{
    return 0;
}

/*
 * UBTreePageIndexTupleDelete
 *
 * This routine does the work of removing a tuple from an index page.
 *
 * Unlike heap pages, we compact out the line pointer for the removed tuple.
 */
void UBTreePCRPageIndexTupleDelete(Page page, OffsetNumber offnum)
{
    PageHeader phdr = (PageHeader)page;
    char *addr = NULL;
    ItemId tup;
    Size size;
    unsigned offset;
    int nbytes;
    int offidx;
    int nline;
    errno_t rc = EOK;

    /*
     * As with PageRepairFragmentation, paranoia seems justified.
     */
    Assert(!PageIsCompressed(page));
    if (phdr->pd_lower < SizeOfPageHeaderData + SizeOfUBTreeTDData(page)|| phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                               phdr->pd_upper, phdr->pd_special)));

    nline = UBTreePcrPageGetMaxOffsetNumber(page);
    if ((int)offnum <= 0 || (int)offnum > nline)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                        errmsg("invalid index offnum: %u", offnum)));

    /* change offset number to offset index */
    offidx = offnum - 1;

    tup = UBTreePCRGetRowPtr(page, offnum);
    Assert(ItemIdHasStorage(tup));
    size = ItemIdGetLength(tup);
    offset = ItemIdGetOffset(tup);
    if (offset < phdr->pd_upper || (offset + size) > phdr->pd_special || offset != (unsigned int)(MAXALIGN(offset)) ||
        size != (unsigned int)(MAXALIGN(size)))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("corrupted item pointer: offset = %u, size = %u", offset, (unsigned int)size)));

    /*
     * First, we want to get rid of the pd_linp entry for the index tuple. We
     * copy all subsequent linp's back one slot in the array. We don't use
     * PageGetItemId, because we are manipulating the _array_, not individual
     * linp's.
     */
    nbytes = phdr->pd_lower - ((char *)&phdr->pd_linp[0] - (char *)phdr + (offidx + 1) * sizeof(ItemIdData) + 
        SizeOfUBTreeTDData(page));

    if (nbytes > 0) {
        rc = memmove_s((char *)&(phdr->pd_linp[0]) + offidx * sizeof(ItemIdData) + SizeOfUBTreeTDData(page), 
            nbytes, (char *)&(phdr->pd_linp[0])+ (offidx + 1 ) * sizeof(ItemIdData) + 
            SizeOfUBTreeTDData(page), nbytes);
        securec_check(rc, "", "");
    }

    /*
     * Now move everything between the old upper bound (beginning of tuple
     * space) and the beginning of the deleted tuple forward, so that space in
     * the middle of the page is left free.  If we've just deleted the tuple
     * at the beginning of tuple space, then there's no need to do the copy
     * (and bcopy on some architectures SEGV's if asked to move zero bytes).
     */
    /* beginning of tuple space */
    addr = (char *)page + phdr->pd_upper;

    if (offset > phdr->pd_upper) {
        rc = memmove_s(addr + size, (int)(offset - phdr->pd_upper), addr, (int)(offset - phdr->pd_upper));
        securec_check(rc, "", "");
    }

    /* adjust free space boundary pointers */
    phdr->pd_upper += size;
    phdr->pd_lower -= sizeof(ItemIdData);

    /*
     * Finally, we need to adjust the linp entries that remain.
     *
     * Anything that used to be before the deleted tuple's data was moved
     * forward by the size of the deleted tuple.
     */
    if (!PageIsEmpty(page)) {
        int i;

        nline--; /* there's one less than when we started */
        for (i = 1; i <= nline; i++) {
            ItemId ii = UBTreePCRGetRowPtr(phdr, i);

            Assert(ItemIdHasStorage(ii));
            if (ItemIdGetOffset(ii) <= offset)
                ii->lp_off += size;
        }
    }
}
