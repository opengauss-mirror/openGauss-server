/* -------------------------------------------------------------------------
 *
 * ginpostinglist.cpp
 *	  routines for dealing with posting lists.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/gin/ginpostinglist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gin_private.h"
#include "access/xloginsert.h"

#ifdef USE_ASSERT_CHECKING
#define CHECK_ENCODING_ROUNDTRIP
#endif

/*
 * For encoding purposes, item pointers are represented as 64-bit unsigned
 * integers. The lowest 11 bits represent the offset number, and the next
 * lowest 32 bits are the block number. That leaves 17 bits unused, i.e.
 * only 43 low bits are used.
 *
 * These 43-bit integers are encoded using varbyte encoding. In each byte,
 * the 7 low bits contain data, while the highest bit is a continuation bit.
 * When the continuation bit is set, the next byte is part of the same
 * integer, otherwise this is the last byte of this integer.  43 bits fit
 * conveniently in at most 6 bytes when varbyte encoded (the 6th byte does
 * not need a continuation bit, because we know the max size to be 43 bits):
 *
 * 0XXXXXXX
 * 1XXXXXXX 0XXXXYYY
 * 1XXXXXXX 1XXXXYYY 0YYYYYYY
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 0YYYYYYY
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 1YYYYYYY 0YYYYYYY
 * 1XXXXXXX 1XXXXYYY 1YYYYYYY 1YYYYYYY 1YYYYYYY YYYYYYYY
 *
 * X = bits used for offset number
 * Y = bits used for block number
 *
 * The bytes are in stored in little-endian order.
 *
 * An important property of this encoding is that removing an item from list
 * never increases the size of the resulting compressed posting list. Proof:
 *
 * Removing number is actually replacement of two numbers with their sum. We
 * have to prove that varbyte encoding of a sum can't be longer than varbyte
 * encoding of its summands. Sum of two numbers is at most one bit wider than
 * the larger of the summands. Widening a number by one bit enlarges its length
 * in varbyte encoding by at most one byte. Therefore, varbyte encoding of sum
 * is at most one byte longer than varbyte encoding of larger summand. Lesser
 * summand is at least one byte, so the sum cannot take more space than the
 * summands, Q.E.D.
 *
 * This property greatly simplifies VACUUM, which can assume that posting
 * lists always fit on the same page after vacuuming. Note that even though
 * that holds for removing items from a posting list, you must also be
 * careful to not cause expansion e.g. when merging uncompressed items on the
 * page into the compressed lists, when vacuuming.
 */
/*
 * How many bits do you need to encode offset number? OffsetNumber is a 16-bit
 * integer, but you can't fit that many items on a page. 11 ought to be more
 * than enough. It's tempting to derive this from MaxHeapTuplesPerPage, and
 * use the minimum number of bits, but that would require changing the on-disk
 * format if MaxHeapTuplesPerPage changes. Better to leave some slack.
 */
/*
 * Change number of bits to encode offset number from 11 to 16. Cstore scan
 * can get tid with offset number that utilize the full bits of 16bits ip_posid.
 */
#define MaxHeapTuplesPerPageBits 11
#define MAXHeapTuplesPerCUBits 16

static inline uint64 itemptr_to_uint64(const ItemPointer iptr, GinPostingListType type)
{
    uint64 val;
    uint16 perPageBits = (type == ROW_STORE_TYPE) ? MaxHeapTuplesPerPageBits : MAXHeapTuplesPerCUBits;

    Assert(ItemPointerIsValid(iptr));
    Assert(iptr->ip_posid < (1 << perPageBits));

    val = iptr->ip_blkid.bi_hi;
    val <<= 16;
    val |= iptr->ip_blkid.bi_lo;
    val <<= perPageBits;
    val |= iptr->ip_posid;

    return val;
}

static inline void uint64_to_itemptr(uint64 val, ItemPointer iptr, GinPostingListType type)
{
    uint16 perPageBits = (type == ROW_STORE_TYPE) ? MaxHeapTuplesPerPageBits : MAXHeapTuplesPerCUBits;

    iptr->ip_posid = val & (((uint)1 << perPageBits) - 1);
    val = val >> perPageBits;
    iptr->ip_blkid.bi_lo = val & 0xFFFF;
    val = val >> 16;
    iptr->ip_blkid.bi_hi = val & 0xFFFF;

    Assert(ItemPointerIsValid(iptr));
}

/*
 * Varbyte-encode 'val' into *ptr. *ptr is incremented to next integer.
 */
static void encode_varbyte(uint64 val, unsigned char **ptr)
{
    unsigned char *p = *ptr;

    while (val > 0x7F) {
        *(p++) = 0x80 | (val & 0x7F);
        val >>= 7;
    }
    *(p++) = (unsigned char)val;

    *ptr = p;
}

/*
 * Decode varbyte-encoded integer at *ptr. *ptr is incremented to next integer.
 */
static uint64 decode_varbyte(unsigned char **ptr, GinPostingListType type)
{
    uint64 val;
    unsigned char *p = *ptr;
    uint64 c;

    c = *(p++);
    val = c & 0x7F;
    if (c & 0x80) {
        c = *(p++);
        val |= (c & 0x7F) << 7;
        if (c & 0x80) {
            c = *(p++);
            val |= (c & 0x7F) << 14;
            if (c & 0x80) {
                c = *(p++);
                val |= (c & 0x7F) << 21;
                if (c & 0x80) {
                    c = *(p++);
                    val |= (c & 0x7F) << 28;
                    if (c & 0x80) {
                        c = *(p++);
                        val |= (c & 0x7F) << 35;
                        if (c & 0x80) {
                            c = *(p++);
                            if (type == ROW_STORE_TYPE) { /* last byte for row store, no continuation bit */
                                val |= c << 42;
                            } else {
                                val |= (c & 0x7F) << 42;
                                if (c & 0x80) {
                                    c = *(p++);
                                    val |= (c & 0x7F) << 49;
                                    if (c & 0x80) {
                                        c = *(p++);
                                        val |= (c & 0x7F) << 56;
                                        if (c & 0x80) {
                                            /* last byte for column store,
                                             * no continuation bit
                                             */
                                            c = *(p++);
                                            val |= c << 63;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    *ptr = p;

    return val;
}

/*
 * Encode a posting list.
 *
 * The encoded list is returned in a palloc'd struct, which will be at most
 * 'maxsize' bytes in size.  The number items in the returned segment is
 * returned in *nwritten. If it's not equal to nipd, not all the items fit
 * in 'maxsize', and only the first *nwritten were encoded.
 *
 * The allocated size of the returned struct is short-aligned, and the padding
 * byte at the end, if any, is zero.
 */
GinPostingList *ginCompressPostingList(const ItemPointer ipd, int nipd, int maxsize, int *nwritten, bool isColStore)
{
    uint64 prev;
    int totalpacked = 0;
    int maxbytes;
    GinPostingList *result = NULL;
    unsigned char *ptr = NULL;
    unsigned char *endptr = NULL;
    errno_t ret = EOK;
    int ipmax = isColStore ? 7 : 6;

    if (ipd == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), (errmsg("invalid item pointer."))));
    }

    maxsize = SHORTALIGN_DOWN((uint)maxsize);

    result = (GinPostingList *)palloc(maxsize);
    result->type = isColStore ? COL_STORE_TYPE : ROW_STORE_TYPE;

    maxbytes = maxsize - offsetof(GinPostingList, bytes);
    Assert(maxbytes > 0);

    /* Store the first special item */
    result->first = ipd[0];

    prev = itemptr_to_uint64(&result->first, result->type);

    ptr = result->bytes;
    endptr = result->bytes + maxbytes;
    for (totalpacked = 1; totalpacked < nipd; totalpacked++) {
        uint64 val = itemptr_to_uint64(&ipd[totalpacked], result->type);
        uint64 delta = val - prev;

        Assert(val > prev);

        if (endptr - ptr >= ipmax) {
            encode_varbyte(delta, &ptr);
        } else {
            /*
             * There are less than 6/7 bytes left. Have to check if the next
             * item fits in that space before writing it out.
             */
            unsigned char buf[ipmax];
            unsigned char *p = buf;

            encode_varbyte(delta, &p);
            if (p - buf > (endptr - ptr)) {
                break; /* output is full */
            }

            ret = memcpy_s(ptr, endptr - ptr, buf, p - buf);
            securec_check(ret, "", "");
            ptr += (p - buf);
        }
        prev = val;
    }
    result->nbytes = ptr - result->bytes;

    /*
     * If we wrote an odd number of bytes, zero out the padding byte at the
     * end.
     */
    if (result->nbytes != SHORTALIGN(result->nbytes))
        result->bytes[result->nbytes] = 0;

    if (nwritten != NULL)
        *nwritten = totalpacked;

    Assert((int)SizeOfGinPostingList(result) <= maxsize);

    /*
     * Check that the encoded segment decodes back to the original items.
     */
#if defined(CHECK_ENCODING_ROUNDTRIP)
    if (assert_enabled) {
        int ndecoded;
        ItemPointer tmp = ginPostingListDecode(result, &ndecoded);
        int i;

        Assert(ndecoded == totalpacked);
        for (i = 0; i < ndecoded; i++)
            Assert(memcmp(&tmp[i], &ipd[i], sizeof(ItemPointerData)) == 0);
        pfree(tmp);
        tmp = NULL;
    }
#endif

    return result;
}

/*
 * Decode a compressed posting list into an array of item pointers.
 * The number of items is returned in *ndecoded.
 */
ItemPointer ginPostingListDecode(GinPostingList *plist, int *ndecoded)
{
    return ginPostingListDecodeAllSegments(plist, SizeOfGinPostingList(plist), ndecoded);
}

/*
 * Decode multiple posting list segments into an array of item pointers.
 * The number of items is returned in *ndecoded_out. The segments are stored
 * one after each other, with total size 'len' bytes.
 */
ItemPointer ginPostingListDecodeAllSegments(GinPostingList *segment, int len, int *ndecoded_out)
{
    ItemPointer result;
    int nallocated;
    uint64 val;
    char *endseg = ((char *)segment) + len;
    int ndecoded;
    unsigned char *ptr = NULL;
    unsigned char *endptr = NULL;

    /*
     * Guess an initial size of the array.
     */
    nallocated = segment->nbytes * 2 + 1;
    result = (ItemPointer)palloc(nallocated * sizeof(ItemPointerData));

    ndecoded = 0;
    while ((char *)segment < endseg) {
        /* enlarge output array if needed */
        if (ndecoded >= nallocated) {
            nallocated *= 2;
            result = (ItemPointer)repalloc(result, nallocated * sizeof(ItemPointerData));
        }

        /* copy the first item */
        Assert(ndecoded == 0 || ginCompareItemPointers(&segment->first, &result[ndecoded - 1]) > 0);
        result[ndecoded] = segment->first;
        ndecoded++;

        val = itemptr_to_uint64(&segment->first, segment->type);
        ptr = segment->bytes;
        endptr = segment->bytes + segment->nbytes;
        while (ptr < endptr) {
            /* enlarge output array if needed */
            if (ndecoded >= nallocated) {
                nallocated *= 2;
                result = (ItemPointer)repalloc(result, nallocated * sizeof(ItemPointerData));
            }

            val += decode_varbyte(&ptr, segment->type);

            uint64_to_itemptr(val, &result[ndecoded], segment->type);
            ndecoded++;
        }
        segment = GinNextPostingListSegment(segment);
    }

    if (ndecoded_out != NULL)
        *ndecoded_out = ndecoded;
    return result;
}

/*
 * Add all item pointers from a bunch of posting lists to a TIDBitmap.
 */
int ginPostingListDecodeAllSegmentsToTbm(GinPostingList *ptr, int len, TIDBitmap *tbm)
{
    int ndecoded;
    ItemPointer items;
    TBMHandler tbm_handler = tbm_get_handler(tbm);

    items = ginPostingListDecodeAllSegments(ptr, len, &ndecoded);
    tbm_handler._add_tuples(tbm, items, ndecoded, false, InvalidOid, InvalidBktId);
    pfree(items);
    return ndecoded;
}

/*
 * Merge two ordered arrays of itempointers, eliminating any duplicates.
 *
 * Returns a palloc'd array, and *nmerged is set to the number of items in
 * the result, after eliminating duplicates.
 */
ItemPointer ginMergeItemPointers(ItemPointerData *a, uint32 na, ItemPointerData *b, uint32 nb, int *nmerged)
{
    ItemPointerData *dst = NULL;
    errno_t ret = EOK;

    dst = (ItemPointer)palloc((na + nb) * sizeof(ItemPointerData));

    /*
     * If the argument arrays don't overlap, we can just append them to each
     * other.
     */
    if (na == 0 || nb == 0 || ginCompareItemPointers(&a[na - 1], &b[0]) < 0) {
        /* na == 0 or nb == 0 can not use memcpy_s  */
        if (na != 0) {
            ret = memcpy_s(dst, na * sizeof(ItemPointerData), a, na * sizeof(ItemPointerData));
            securec_check(ret, "", "");
        }
        if (nb != 0) {
            ret = memcpy_s(&dst[na], nb * sizeof(ItemPointerData), b, nb * sizeof(ItemPointerData));
            securec_check(ret, "", "");
        }

        *nmerged = na + nb;
    } else if (ginCompareItemPointers(&b[nb - 1], &a[0]) < 0) {
        ret = memcpy_s(dst, nb * sizeof(ItemPointerData), b, nb * sizeof(ItemPointerData));
        securec_check(ret, "", "");
        ret = memcpy_s(&dst[nb], na * sizeof(ItemPointerData), a, na * sizeof(ItemPointerData));
        securec_check(ret, "", "");
        *nmerged = na + nb;
    } else {
        ItemPointerData *dptr = dst;
        ItemPointerData *aptr = a;
        ItemPointerData *bptr = b;

        while (aptr - a < na && bptr - b < nb) {
            int cmp = ginCompareItemPointers(aptr, bptr);
            if (cmp > 0)
                *dptr++ = *bptr++;
            else if (cmp == 0) {
                /* only keep one copy of the identical items */
                *dptr++ = *bptr++;
                aptr++;
            } else
                *dptr++ = *aptr++;
        }

        while (aptr - a < na)
            *dptr++ = *aptr++;

        while (bptr - b < nb)
            *dptr++ = *bptr++;

        *nmerged = dptr - dst;
    }

    return dst;
}
