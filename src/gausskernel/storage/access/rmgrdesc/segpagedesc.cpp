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
 * segpagedesc.cpp
 *    rmgr descriptor routines for segment-page related content in catalog/storage.cpp
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/rmgrdesc/segpagedesc.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/catalog.h"
#include "catalog/storage_xlog.h"
#include "storage/custorage.h"
#include "storage/smgr/segment.h"

#define REL_NODE_FORMAT(rnode) rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode

static const char* SegmentXLogOpName[] = {
    "set bitmap",
    "free bitmap",
    "update allocated_extents and hwm",
    "init segment head",
    "update segment head",
    "new level0 page",
    "level0 page add extent",
    "segment head add fork segment",
    "unlink segment head pointer",
    "init bucket table main head",
    "unlink bucket mapblock",
    "add bucket mapblock",
    "init bucket mapblock",
    "insert a bucket segment in mapblock",
    "update high water mark",
    "set inverse pointer",
    "move buckets",
    "bucket add redis info"
};

/* 
 * It should be defined in segxlog.cpp, but pg_xlogdump is an individual binary and
 * only includes segpagedesc.cpp, thus we define the function here.
 */
DecodedXLogBlockOp XLogAtomicDecodeBlockData(char *data, int len)
{
    DecodedXLogBlockOp decoded_op;
    decoded_op.operations = *(uint8 *)data;
    Assert(decoded_op.operations <= MaxOperationsPerBlock);

    data += sizeof(uint8);
    len -= sizeof(uint8);

    for (int i = 0; i < decoded_op.operations; i++) {
        decoded_op.data_len[i] = *(uint32 *)data;
        decoded_op.data_len[i] -= sizeof(uint8); // exclude Op Code
        data += sizeof(uint32);
        len -= sizeof(uint32);

        decoded_op.op[i] = *(uint8 *)data;
        data += sizeof(uint8);
        len -= sizeof(uint8);

        decoded_op.data[i] = data;
        data += decoded_op.data_len[i];
        len -= decoded_op.data_len[i];
    }

    Assert(len == 0);
    return decoded_op;
}

static void segpage_atomic_op_desc(StringInfo buf, DecodedXLogBlockOp* decoded_op, int j)
{
    uint8 op = decoded_op->op[j];
    if (op == SPCXLOG_SET_BITMAP) {
        appendStringInfo(buf, " bit id %u;", *(uint16*)decoded_op->data[j]);
    } else if (op == SPCXLOG_FREE_BITMAP) {
        appendStringInfo(buf, " bit id %u;", *(uint16*)decoded_op->data[j]);
        if (decoded_op->data_len[j] == (sizeof(uint16) + sizeof(df_map_page_t) + sizeof(BlockNumber))) {
            BlockNumber blkNum = *(BlockNumber *)(decoded_op->data[j] + sizeof(uint16) + sizeof(df_map_page_t));
            appendStringInfo(buf, "freed blk %u.", blkNum);
        }
    } else if (op == SPCXLOG_MAPHEAD_ALLOCATED_EXTENTS) {
        XLogDataSpaceAllocateExtent* xlog_data = (XLogDataSpaceAllocateExtent*)decoded_op->data[j];
        appendStringInfo(buf, " hw %u, allocated extends %u, free_group %u, free_page %u;",
            xlog_data->hwm, xlog_data->allocated_extents, xlog_data->free_group, xlog_data->free_page);
    } else if (op == SPCXLOG_INIT_SEGHEAD) {
    } else if (op == SPCXLOG_UPDATE_SEGHEAD) {
        XLogDataUpdateSegmentHead* xlog_data = (XLogDataUpdateSegmentHead*)decoded_op->data[j];
        if (xlog_data->level0_slot >= 0) {
            appendStringInfo(buf, " level0 slot %u, extent %u", xlog_data->level0_slot, xlog_data->level0_value);
        }
        if (xlog_data->level1_slot >= 0) {
            appendStringInfo(buf, " level0 slot %u, extent %u", xlog_data->level1_slot, xlog_data->level1_value);
        }
        appendStringInfo(buf, " nextents %u, total_blocks %u;", xlog_data->nextents, xlog_data->total_blocks);
        if (xlog_data->old_extent != InvalidBlockNumber) {
            appendStringInfo(buf, " shrinked extent %u.", xlog_data->old_extent);
        }
    } else if (op == SPCXLOG_NEW_LEVEL0_PAGE) {
    } else if (op == SPCXLOG_LEVEL0_PAGE_ADD_EXTENT) {
        XLogDataSetLevel0Page* xlog_data = (XLogDataSetLevel0Page*)decoded_op->data[j];
        appendStringInfo(buf, " slot %u, extent %u;", xlog_data->slot, xlog_data->extent);
    } else if (op == SPCXLOG_SEGHEAD_ADD_FORK_SEGMENT) {
        char* data = decoded_op->data[j];
        int forknum = *(int*)data;
        data += sizeof(int);
        BlockNumber forkhead = *(BlockNumber *)data;
        appendStringInfo(buf, " forknum %d fork head %u", forknum, forkhead);
    } else if (op == SPCXLOG_SEG_MOVE_BUCKETS) {
        char* data = decoded_op->data[j];
        uint32 nentry = *(uint32*)data;
        data += sizeof(uint32);
        xl_seg_bktentry_tag_t *mapentry = (xl_seg_bktentry_tag_t*)data;
        appendStringInfo(buf, " bucket move entry count %u", nentry);
        for (uint32 i = 0; i < nentry; i++) {
            appendStringInfo(buf, " [%u, %u]", mapentry[i].bktentry_id, mapentry[i].bktentry_header);
        }
    } else if (op == SPCXLOG_BUCKET_ADD_REDISINFO) {
        SegRedisInfo *redis_info = (SegRedisInfo *)decoded_op->data[j];
        appendStringInfo(buf, " redis xid %ld, redis words count %u:", redis_info->redis_xid, redis_info->nwords);
        for (uint32 i = 0; i < redis_info->nwords; i++) {
            appendStringInfo(buf, " %x", redis_info->words[i]);
        }
    } else if (op == SPCXLOG_SPACE_UPDATE_HWM) {
        XLogDataUpdateSpaceHWM *data = (XLogDataUpdateSpaceHWM *)decoded_op->data[j];
        appendStringInfo(buf, " old hwm %u, new hwm %u old count %u new count %u", data->old_hwm, data->new_hwm,
            data->old_groupcnt, data->new_groupcnt);
    } else if (op == SPCXLOG_SHRINK_SEGHEAD_UPDATE) {
        XLogMoveExtent *xlog_data = (XLogMoveExtent *)decoded_op->data[j];
        appendStringInfo(buf, "update segment head due to extent movement during space shrink, logic rnode: "
                         "%u/%u/%u/%u, forknum: %d, extent id: %d, new extent: %u, old extent: %u",
                         REL_NODE_FORMAT(xlog_data->logic_rnode), xlog_data->forknum, xlog_data->extent_id,
                         xlog_data->new_extent, xlog_data->old_extent);
    }
}

void segpage_smgr_desc(StringInfo buf, XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;    
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blknum;
    
    if (info == XLOG_SEG_ATOMIC_OPERATION) {
        int nbuffers = *(int *)XLogRecGetData(record);
        appendStringInfo(buf, "[segpage] xlog atomic operation, contains %d buffers:\n", nbuffers);
        for (int i = 0; i < nbuffers; i++) {
            RelFileNode rnode;
            ForkNumber forknum;
            BlockNumber blknum;

            XLogRecGetBlockTag(record, i, &rnode, &forknum, &blknum);
            appendStringInfo(buf, "\t buffer #%d [%u, %u, %u, %d] %s %u, ", i, REL_NODE_FORMAT(rnode),
                             forkNames[forknum], blknum);

            Size len = 0;
            char *data = XLogRecGetBlockData(record, i, &len);
            Assert(len != 0);

            DecodedXLogBlockOp decoded_op = XLogAtomicDecodeBlockData(data, len);
            appendStringInfo(buf, "operations (%d): ", decoded_op.operations);

            for (int j = 0; j < decoded_op.operations; j++) {
                appendStringInfo(buf, "#%d %s, datalen %d; ", j, SegmentXLogOpName[decoded_op.op[j]],
                                 decoded_op.data_len[j]);
                segpage_atomic_op_desc(buf, &decoded_op, j);
            }
            appendStringInfo(buf, "\n");
        }
    } else if (info == XLOG_SEG_SEGMENT_EXTEND) {
        XLogDataSegmentExtend *data = (XLogDataSegmentExtend *)XLogRecGetBlockData(record, 0, NULL);
        appendStringInfo(buf,
            "[segpage] segment head extend: relfilenode/fork:<%u/%u>, nblocks[%u->%u], (phy loc %u/%u), reset_zero:%u",
            data->main_fork_head, data->forknum, data->old_nblocks, data->new_nblocks, data->ext_size, data->blocknum,
            data->reset_zero);
    } else if (info == XLOG_SEG_CREATE_EXTENT_GROUP) {
        char *data = XLogRecGetData(record);
        RelFileNode *rnode = (RelFileNode *)data;
        ForkNumber *forknum = (ForkNumber *)(data + sizeof(RelFileNode));
        appendStringInfo(buf, "[segpage] create segment-page tablespace file: <%u, %u, %u> fork: %d", rnode->spcNode,
                         rnode->dbNode, rnode->relNode, *forknum);
    } else if (info == XLOG_SEG_INIT_MAPPAGE) {
        XLogRecGetBlockTag(record, 0, &rnode, &forknum, &blknum);
        BlockNumber firstPage = *(BlockNumber *)XLogRecGetData(record);

        appendStringInfo(buf, "[segpage] init map page for segpage file [%u, %u, %u, %d] fork %d,"
                         "block num %u, map first page is %u.", 
                         rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, forknum, 
                         blknum, firstPage);
    } else if (info == XLOG_SEG_ADD_NEW_GROUP) {
        XLogRecGetBlockTag(record, 0, &rnode, &forknum, &blknum);
        xl_new_map_group_info_t* newMapGroup = (xl_new_map_group_info_t *)XLogRecGetData(record);
        appendStringInfo(buf, "[segpage] add new group for segpage file [%u, %u, %u, %d] fork %d, block num %u,"
                         "group cnt %u, first map page %u, extent size %d, group size %u.", rnode.spcNode,
                         rnode.dbNode, rnode.relNode, rnode.bucketNode, forknum, blknum, newMapGroup->group_count,
                         newMapGroup->first_map_pageno, newMapGroup->extent_size, newMapGroup->group_size);
    } else if (info == XLOG_SEG_TRUNCATE) {
        XLogRecGetBlockTag(record, 0, &rnode, &forknum, &blknum);
        BlockNumber nblocks = *(BlockNumber *)XLogRecGetBlockData(record, 0, NULL);
        appendStringInfo(buf, "[segpage] seg truncate for rel node[%u, %u, %u, %d] fork %d,"
                         "block num %u truncate block to %u.", 
                         rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, forknum, 
                         blknum, nblocks);
    } else if (info == XLOG_SEG_SPACE_SHRINK) {
        XLogDataSpaceShrink *xlog_data = (XLogDataSpaceShrink *)XLogRecGetData(record);
        rnode = xlog_data->rnode;
        appendStringInfo(buf, "[segpage] space shrink for rel node[%u, %u, %u, %d] fork %d shrink target %u",
                         rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, xlog_data->forknum,
                         xlog_data->target_size);
    } else if (info == XLOG_SEG_SPACE_DROP) {
        appendStringInfo(buf, "[segpage] drop tablespace");
    } else if (info == XLOG_SEG_NEW_PAGE) {
        appendStringInfo(buf, "[segpage] copy data to new page");
    } else {
        appendStringInfo(buf, "[segpage] UNKNOWN");
    }
}

const char* segpage_smgr_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_SEG_ATOMIC_OPERATION) {
        return "segsmgr_atomic_operation";
    } else if (info == XLOG_SEG_SEGMENT_EXTEND) {
        return "segsmgr_segment_extend";
    } else if (info == XLOG_SEG_CREATE_EXTENT_GROUP) {
        return "segsmgr_create_extend_group";
    } else if (info == XLOG_SEG_INIT_MAPPAGE) {
        return "segsmgr_init_mappage";
    } else if (info == XLOG_SEG_INIT_INVRSPTR_PAGE) {
        return "segsmgr_init_inversptr_page";
    } else if (info == XLOG_SEG_ADD_NEW_GROUP) {
        return "segsmgr_add_new_group";
    } else if (info == XLOG_SEG_TRUNCATE) {
        return "segsmgr_truncate";
    } else if (info == XLOG_SEG_SPACE_SHRINK) {
        return "segsmgr_space_shrink";
    } else if (info == XLOG_SEG_SPACE_DROP) {
        return "segsmgr_space_drop";
    } else if (info == XLOG_SEG_NEW_PAGE) {
        return "segsmgr_new_page";
    } else {
        return "unknown_type";
    }
}

