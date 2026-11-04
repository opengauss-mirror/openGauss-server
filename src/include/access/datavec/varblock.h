/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
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
 * varblock.h
 *
 * IDENTIFICATION
 *        src/include/access/datavec/varblock.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef VARBLOCK_H
#define VARBLOCK_H

#include "postgres.h"

#include "access/generic_xlog.h"
#include "storage/buf/buf.h"
#include "storage/item/itemptr.h"

/*
 * Variable-length chunk storage: chunk header.
 *
 * This layout lives only inside the item data area; it is not part of
 * PageHeader/ItemId. Payload follows the header immediately.
 */
typedef struct VarBlockChunkHeader {
    ItemPointerData next_ctid;   /* next chunk ctid; Invalid means end of chain */
    uint16          payload_len; /* valid payload bytes in this chunk */
    uint8           level;       /* 0..4, chunk size tier */
} VarBlockChunkHeader;

/* Chunk size tiers (logical size: header + payload + alignment) */
#define VAR_BLOCK_LEVEL0_SIZE    128
#define VAR_BLOCK_LEVEL1_SIZE    512
#define VAR_BLOCK_LEVEL2_SIZE   1024
#define VAR_BLOCK_LEVEL3_SIZE   4096
/*
 * Level 4 is roughly one page of usable data for BM25; we do not tie this
 * header to BM25 page-size constants here -- callers can wrap as needed.
 */
#define VAR_BLOCK_LEVEL4_SIZE   8000

#define VAR_BLOCK_LEVELS        5

/* Level index for VarBlockChunkHeader.level (0..VAR_BLOCK_LEVELS - 1) */
#define VAR_BLOCK_LEVEL_IDX_0   0
#define VAR_BLOCK_LEVEL_IDX_1   1
#define VAR_BLOCK_LEVEL_IDX_2   2
#define VAR_BLOCK_LEVEL_IDX_3   3
#define VAR_BLOCK_LEVEL_IDX_4   4

static inline Size VarBlockSize(uint8 level)
{
    return (level == VAR_BLOCK_LEVEL_IDX_0) ? VAR_BLOCK_LEVEL0_SIZE :
           (level == VAR_BLOCK_LEVEL_IDX_1) ? VAR_BLOCK_LEVEL1_SIZE :
           (level == VAR_BLOCK_LEVEL_IDX_2) ? VAR_BLOCK_LEVEL2_SIZE :
           (level == VAR_BLOCK_LEVEL_IDX_3) ? VAR_BLOCK_LEVEL3_SIZE :
                                              VAR_BLOCK_LEVEL4_SIZE;
}

/*
 * Placeholder chunk: a minimal-size chunk used to preserve offset numbers
 * when freeing a VarBlock chain.  Instead of physically deleting the item
 * (which would change other items' offset numbers on the same page), we
 * replace it with a placeholder of this size.  The placeholder is identified
 * by VAR_BLOCK_LEVEL_PLACEHOLDER in the level field.
 */
#define VAR_BLOCK_PLACEHOLDER_SIZE  MAXALIGN(sizeof(VarBlockChunkHeader))
#define VAR_BLOCK_LEVEL_PLACEHOLDER  0xFF

static inline bool VarBlockIsPlaceholder(const VarBlockChunkHeader *hdr)
{
    return hdr->level == VAR_BLOCK_LEVEL_PLACEHOLDER;
}

/*
 * Callback while walking a chunk chain.
 *
 * hdr    : current chunk header
 * payload: start of this chunk's payload
 * arg    : caller context
 */
typedef struct VarBlockReadContext {
    uint8 reserved;
} VarBlockReadContext;

typedef void (*VarBlockReadCallback)(const VarBlockChunkHeader *hdr, const char *payload, VarBlockReadContext *arg);

/*
 * - VarBlockAllocFirstChunk
 *     Allocate the first chunk (usually level 0); returns its ctid.
 *     building: true during CREATE INDEX build - skip WAL (MarkBufferDirty only).
 *
 * - VarBlockExtendChain
 *     Given the tail ctid, derive the next level from the header, allocate a new
 *     chunk, link it after the tail, return the new tail ctid.
 *     building: same as VarBlockAllocFirstChunk.
 *     Callers that also store postingChainTail in tokenMeta should run VarBlockFindTailCtid
 *     before append if meta and chain might diverge (e.g. crash between link WAL and meta commit).
 *
 * - VarBlockFindTailCtid
 *     From a valid chunk ctid on the chain, follow next_ctid until the end; returns the
 *     last chunk (true tail). Used to refresh stale tokenMeta->postingChainTail.
 *
 * - VarBlockReadChain
 *     Walk the CTID chain from the head, invoking the callback per chunk (forkNum
 *     must match allocation).
 *
 * - VarBlockFreeChain
 *     Collect the full chain from the head, replace each chunk with a placeholder
 *     (PageIndexTupleDelete + PageAddItem) to preserve offset numbers of other
 *     items on the same page; on MAIN fork, update FSM (forkNum must match allocation).
 *     building: true during CREATE INDEX build / reorder - skip WAL (MarkBufferDirty only).
 */
extern ItemPointerData VarBlockAllocFirstChunk(Relation rel, ForkNumber forkNum, int level_hint, bool building);

extern ItemPointerData VarBlockExtendChain(Relation rel, ForkNumber forkNum, const ItemPointerData *tail_ctid,
    bool building);

extern ItemPointerData VarBlockFindTailCtid(Relation rel, ForkNumber forkNum, const ItemPointerData *start_ctid);

extern void VarBlockReadChain(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid,
                              VarBlockReadCallback cb, VarBlockReadContext *cbArg);

extern void VarBlockFreeChain(Relation rel, ForkNumber forkNum, const ItemPointerData *head_ctid, bool building);

extern void VarBlockVacuumCleanup(Relation rel);

#endif /* VARBLOCK_H */
