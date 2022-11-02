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
 * inverse_ptr.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/inverse_ptr.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/executor.h"
#include "storage/smgr/segment.h"

static const char *ExtentUsageName[] = {"Not used", "Non-bucket table segment head", "Non-bucket table fork head",
                                        "Bucket table main head", "Bucket table map block", "Bucket segment head",
                                        "Data extent",

                                        /* must be last one */
                                        "Invalid Usage Type"};

IpBlockLocation GetIpBlock(BlockNumber extent, uint32 extent_size)
{
    /* Exclude MapHead */
    extent -= DF_MAP_HEAD_PAGE + 1;

    /* Offset in the Map Group */
    BlockNumber group_total_blocks = DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE + DF_MAP_GROUP_EXTENTS * extent_size;
    uint32 group_id = extent / group_total_blocks;

    /*
     * Group inverse pointer start, plus 
     *  1. DF_MAP_HEAD_PAGE
     *  2. previous groups
     *  3. Map pages in this group
     */
    BlockNumber group_ip_start = DF_MAP_HEAD_PAGE + 1 + group_id * group_total_blocks + DF_MAP_GROUP_SIZE;
    BlockNumber group_offset = extent % group_total_blocks;

    /* Calculate n-th extent in the Map Group */
    group_offset -= DF_MAP_GROUP_SIZE + IPBLOCK_GROUP_SIZE;
    SegmentCheck(extent_size > 0);
    uint32 extent_id = group_offset / extent_size;

    IpBlockLocation result = {
        .ipblock = group_ip_start + extent_id / EXTENTS_PER_IPBLOCK, 
        .offset = extent_id % EXTENTS_PER_IPBLOCK
    };
    return result;
}

const char *GetExtentUsageName(ExtentInversePointer iptr)
{
    int usage = SPC_INVRSPTR_GET_USAGE(iptr);
    if (usage >= INVALID_EXTNT_USAGE) {
        usage = INVALID_EXTNT_USAGE;
    }

    return ExtentUsageName[usage];
}

/* Inverse pointer read buffer */
Buffer ip_readbuf(SegExtentGroup *seg, BlockNumber blocknum, bool extend)
{
#ifdef USE_ASSERT_CHECKING
    BlockNumber df_size = eg_df_size(seg);
    SegmentCheck(blocknum < df_size);
#endif

    Buffer buf = ReadBufferFast(seg->space, seg->rnode, seg->forknum, blocknum, RBM_NORMAL);
    if (PageIsNew(BufferGetPage(buf))) {
        SegPageInit(BufferGetPage(buf), BLCKSZ);
    }

    return buf;
}

void SetInversePointer(SegExtentGroup *seg, BlockNumber extent, ExtentInversePointer iptr)
{
    IpBlockLocation loc = GetIpBlock(extent, seg->extent_size);
    Buffer ipBuffer = ip_readbuf(seg, loc.ipblock, true);
    SegmentCheck(BufferIsValid(ipBuffer));

    LockBuffer(ipBuffer, BUFFER_LOCK_EXCLUSIVE);

    ExtentInversePointer *eips = (ExtentInversePointer *)PageGetContents(BufferGetBlock(ipBuffer));
    eips[loc.offset] = iptr;

    XLogAtomicOpRegisterBuffer(ipBuffer, REGBUF_KEEP_DATA, SPCXLOG_SET_INVERSE_POINTER,
                               XLOG_COMMIT_UNLOCK_RELEASE_BUFFER);
    XLogAtomicOpRegisterBufData((char *)&loc.offset, sizeof(uint32));
    XLogAtomicOpRegisterBufData((char *)&iptr, sizeof(ExtentInversePointer));
}

/*
 * Return the inverse pointer of a given extent
 *
 * Similar to visibilitymap_test, we try to reuse inverse pointer block buffer during a sequential
 * scan. On entry, *buf should be invalid buffer or valid buffer returned last time; On return, *buf
 * is a valid buffer containing the inverse pointer of the given extent. The caller is responsible
 * to release the buf after it has done scanning.
 */
ExtentInversePointer GetInversePointer(SegExtentGroup *seg, BlockNumber extent, Buffer *buf)
{
    SegmentCheck(buf != NULL);
    IpBlockLocation loc = GetIpBlock(extent, seg->extent_size);

    if (BufferIsValid(*buf)) {
        BufferDesc *bufHdr = GetBufferDescriptor(*buf - 1);

        // old buffer can not be reused
        if (!RelFileNodeRelEquals(bufHdr->tag.rnode, seg->rnode) || bufHdr->tag.blockNum != loc.ipblock) {
            SegReleaseBuffer(*buf);
            *buf = InvalidBuffer;
        }
    }

    if (BufferIsInvalid(*buf)) {
        *buf = ip_readbuf(seg, loc.ipblock, false);
    }

    SegmentCheck(BufferIsValid(*buf));

    LockBuffer(*buf, BUFFER_LOCK_SHARE);
    ExtentInversePointer *eips = (ExtentInversePointer *)PageGetContents(BufferGetBlock(*buf));
    ExtentInversePointer res = eips[loc.offset];
    LockBuffer(*buf, BUFFER_LOCK_UNLOCK);
    SegmentCheck(InversePointerIsValid(res));

    return res;
}

ExtentInversePointer RepairGetInversePointer(SegExtentGroup *seg, BlockNumber extent)
{
    ExtentInversePointer res = {0, 0};
    IpBlockLocation loc = GetIpBlock(extent, seg->extent_size);
    Buffer buf = ip_readbuf(seg, loc.ipblock, false);
    if (BufferIsInvalid(buf)) {
        return res;
    }
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    ExtentInversePointer *eips = (ExtentInversePointer *)PageGetContents(BufferGetBlock(buf));
    res = eips[loc.offset];
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    SegReleaseBuffer(buf);
    return res;
}

