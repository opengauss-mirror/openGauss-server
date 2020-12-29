/*
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
 * -------------------------------------------------------------------------
 *
 * xlogreader_common.cpp
 *    common function for xlog read
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/xlogreader_common.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogreader.h"
#include "storage/buf/bufpage.h"
#include "access/redo_common.h"

/*
 * Returns information about the block that a block reference refers to.
 *
 * If the WAL record contains a block reference with the given ID, *rnode,
 * *forknum, and *blknum are filled in (if not NULL), and returns TRUE.
 * Otherwise returns FALSE.
 */
bool XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id, RelFileNode *rnode, ForkNumber *forknum,
                        BlockNumber *blknum)
{
    DecodedBkpBlock *bkpb = NULL;

    if (!record->blocks[block_id].in_use)
        return false;

    bkpb = &record->blocks[block_id];
    if (rnode != NULL)
        *rnode = bkpb->rnode;
    if (forknum != NULL)
        *forknum = bkpb->forknum;
    if (blknum != NULL)
        *blknum = bkpb->blkno;
    return true;
}

/*
 * Returns the data associated with a block reference, or NULL if there is
 * no data (e.g. because a full-page image was taken instead). The returned
 * pointer points to a MAXALIGNed buffer.
 */
char *XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
    DecodedBkpBlock *bkpb = NULL;

    if (!record->blocks[block_id].in_use)
        return NULL;

    bkpb = &record->blocks[block_id];

    if (!bkpb->has_data) {
        if (len != NULL)
            *len = 0;
        return NULL;
    } else {
        if (len != NULL)
            *len = bkpb->data_len;
        return bkpb->data;
    }
}

/*
 * Restore a full-page image from a backup block attached to an XLOG record.
 *
 * Returns the buffer number containing the page.
 *
 * Reconstruct for batchredo
 */
void RestoreBlockImage(const char *bkp_image, uint16 hole_offset, uint16 hole_length, char *page)
{
    errno_t rc = EOK;

    if (hole_length == 0) {
        rc = memcpy_s(page, BLCKSZ, bkp_image, BLCKSZ);
        securec_check(rc, "", "");
    } else {
        rc = memcpy_s(page, BLCKSZ, bkp_image, hole_offset);
        securec_check(rc, "", "");
        /* must zero-fill the hole */
        rc = memset_s(page + hole_offset, BLCKSZ - hole_offset, 0, hole_length);
        securec_check(rc, "", "");

        Assert(hole_offset + hole_length <= BLCKSZ);
        if (hole_offset + hole_length == BLCKSZ)
            return;

        rc = memcpy_s(page + (hole_offset + hole_length), BLCKSZ - (hole_offset + hole_length), bkp_image + hole_offset,
                      BLCKSZ - (hole_offset + hole_length));
        securec_check(rc, "", "");
    }
}
