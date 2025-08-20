/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * blscan.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/bloom/blscan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "access/bloom.h"

/*
 * Begin scan of bloom index.
 */
IndexScanDesc blbeginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    BloomScanOpaque so;

    scan = RelationGetIndexScan(index, nkeys, norderbys);

    so = (BloomScanOpaque)palloc(sizeof(BloomScanOpaqueData));
    initBloomState(&so->state, scan->indexRelation);
    so->sign = NULL;
    scan->opaque = so;

    return scan;
}

/*
 * Rescan a bloom index.
 */
void blrescan_internal(IndexScanDesc scan, ScanKey scankey)
{
    BloomScanOpaque so;
    errno_t rc;

    so = (BloomScanOpaque) scan->opaque;

    if (so->sign)
        pfree(so->sign);
    so->sign = NULL;

    if (scankey && scan->numberOfKeys > 0) {
        rc = memcpy_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData),
                      scankey, scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }
}

/*
 * End scan of bloom index.
 */
void blendscan_internal(IndexScanDesc scan)
{
    BloomScanOpaque so = (BloomScanOpaque) scan->opaque;

    if (so->sign)
        pfree(so->sign);
    so->sign = NULL;
}

/*
 * Insert all matching tuples into to a bitmap.
 */
int64 blgetbitmap_internal(IndexScanDesc scan, TIDBitmap *tbm)
{
    int64           ntids = 0;
    BlockNumber     blkno = BLOOM_HEAD_BLKNO;
    BlockNumber     npages;
    int             i;
    BufferAccessStrategy bas;
    BloomScanOpaque so = (BloomScanOpaque) scan->opaque;

    if (so->sign == NULL) {
        /* New search: have to calculate search signature */
        ScanKey        skey = scan->keyData;

        so->sign = (BloomSignatureWord *)palloc0(sizeof(BloomSignatureWord) * so->state.opts.bloomLength);
        for (i = 0; i < scan->numberOfKeys; i++) {
            /*
             * Assume bloom-indexable operators to be strict, so nothing could
             * be found for NULL key.
             */
            if (skey->sk_flags & SK_ISNULL) {
                pfree(so->sign);
                so->sign = NULL;
                return 0;
            }

            /* Add next value to the signature */
            signValue(&so->state, so->sign, skey->sk_argument,
                      skey->sk_attno - 1);

            skey++;
        }
    }

    /*
     * We're going to read the whole index. This is why we use appropriate
     * buffer access strategy.
     */
    bas = GetAccessStrategy(BAS_BULKREAD);
    npages = RelationGetNumberOfBlocks(scan->indexRelation);
    pgstat_count_index_scan(scan->indexRelation);

    for (blkno = BLOOM_HEAD_BLKNO; blkno < npages; blkno++) {
        Buffer      buffer;
        Page        page;

        buffer = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM,
                                    blkno, RBM_NORMAL, bas);

        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);
        if (!PageIsNew(page) && !BloomPageIsDeleted(page)) {
            OffsetNumber offset;
            OffsetNumber maxOffset = BloomPageGetMaxOffset(page);
            for (offset = 1; offset <= maxOffset; offset++) {
                BloomTuple *itup = BloomPageGetTuple(&so->state, page, offset);
                bool res = true;

                /* Check index signature with scan signature */
                for (i = 0; i < so->state.opts.bloomLength; i++) {
                    if ((itup->sign[i] & so->sign[i]) != so->sign[i]) {
                        res = false;
                        break;
                    }
                }

                /* Add matching tuples to bitmap */
                if (res) {
                    TBMHandler tbm_handler = tbm_get_handler(tbm);
                    tbm_handler._add_tuples(tbm, &itup->heapPtr, 1, true, InvalidOid, InvalidBktId);
                    ntids++;
                }
            }
        }

        UnlockReleaseBuffer(buffer);
        CHECK_FOR_INTERRUPTS();
    }
    FreeAccessStrategy(bas);

    return ntids;
}
