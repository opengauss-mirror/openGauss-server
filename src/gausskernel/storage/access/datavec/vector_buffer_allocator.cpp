/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/vector_buffer_allocator.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/atomic.h"
#include "vector_buffer_internal.h"

void VbpChunkInit(VbpMemChunk *chunk, VbpChunkState state)
{
    Assert(chunk != NULL);
    Assert(state == VBP_CHUNK_FREE || state == VBP_CHUNK_CLAIMED || state == VBP_CHUNK_ACTIVE ||
        state == VBP_CHUNK_DRAINING);
    if (chunk == NULL || (state != VBP_CHUNK_FREE && state != VBP_CHUNK_CLAIMED &&
        state != VBP_CHUNK_ACTIVE && state != VBP_CHUNK_DRAINING)) {
        return;
    }
    pg_atomic_init_u32(&chunk->control, (uint32)state);
    pg_atomic_init_u32(&chunk->globalFreeNext, VBP_INVALID_INDEX);
}

bool VbpChunkTryAcquireAllocatorRef(VbpMemChunk *chunk)
{
    uint32 expected;

    if (chunk == NULL) {
        return false;
    }

    expected = VbpChunkControlRead(chunk);
    while ((expected & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE &&
        (expected & VBP_CHUNK_ALLOC_REF_MASK) != VBP_CHUNK_ALLOC_REF_MASK) {
        uint32 desired;

        desired = expected + VBP_CHUNK_ALLOC_REF_ONE;
        if (pg_atomic_compare_exchange_u32(&chunk->control, &expected, desired)) {
            return true;
        }
    }
    return false;
}

bool VbpChunkTryBeginDrain(VbpMemChunk *chunk)
{
    uint32 expected;

    if (chunk == NULL) {
        return false;
    }

    expected = VbpChunkControlRead(chunk);
    while ((expected & VBP_CHUNK_STATE_MASK) == VBP_CHUNK_ACTIVE) {
        uint32 desired;

        /*
         * Drop IN_FREELIST_BIT with the drain transition so stale freelist
         * tokens cannot advertise a draining chunk as a reserve candidate.
         * New reserves already require ACTIVE (MatchesVbp / TryAcquireRef).
         */
        desired = (expected & ~VBP_CHUNK_STATE_MASK & ~VBP_CHUNK_IN_FREELIST_BIT) |
            VBP_CHUNK_DRAINING;
        if (pg_atomic_compare_exchange_u32(&chunk->control, &expected, desired)) {
            return true;
        }
    }
    return false;
}

bool VbpChunkAllocatorRefsDrained(const VbpMemChunk *chunk)
{
    if (chunk == NULL) {
        return false;
    }
    return (VbpChunkControlRead(chunk) & VBP_CHUNK_ALLOC_REF_MASK) == 0;
}

static inline bool VbpTaggedIndexIsValid(uint32 indexPlusOne, uint32 slotCount)
{
    return indexPlusOne == 0 || indexPlusOne - 1 < slotCount;
}

static bool VbpFreeEntryStackArgsAreValid(VbpTaggedHead *head, char *slotBase, Size slotSize, uint32 slotCount)
{
    const Size entryAlignment = alignof(VbpEntry);

    return head != NULL && slotBase != NULL && slotSize >= sizeof(VbpEntry) && slotCount > 0 &&
           slotCount <= VBP_ENTRY_FREE_NEXT_MASK && (uintptr_t)slotBase % entryAlignment == 0 &&
           slotSize % entryAlignment == 0 && slotSize <= SIZE_MAX / slotCount;
}

static inline VbpEntry *VbpFreeEntryAt(char *slotBase, Size slotSize, uint32 slotIndex)
{
    return (VbpEntry *)((char *)slotBase + (Size)slotIndex * slotSize);
}

void VbpTaggedHeadInit(VbpTaggedHead *head)
{
    Assert(head != NULL);
    if (head == NULL) {
        return;
    }
    pg_atomic_init_u64(&head->value, VbpPackTaggedIndex(0, 0));
}

bool VbpFreeEntryStackPush(VbpTaggedHead *head, char *slotBase, Size slotSize, uint32 slotCount, uint32 slotIndex)
{
    VbpEntry *entry;
    uint64 expected;

    if (!VbpFreeEntryStackArgsAreValid(head, slotBase, slotSize, slotCount) || slotIndex >= slotCount ||
        slotIndex == VBP_INVALID_INDEX) {
        return false;
    }

    entry = VbpFreeEntryAt(slotBase, slotSize, slotIndex);
    expected = VbpTaggedHeadRead(head);
    while (VbpTaggedIndexIsValid(VbpTaggedIndex(expected), slotCount)) {
        uint32 currentIndex = VbpTaggedIndex(expected);
        uint32 nextVersion;
        uint64 desired;

        uint64 control = pg_atomic_read_u64(&entry->control);
        if (!VbpEntryControlIsFree(control)) {
            return false;
        }
        pg_atomic_write_u64(&entry->control,
            VbpEntryFreeControlPack(VbpEntryControlIncarnation(control), currentIndex));
        pg_write_barrier();
        /* Approved design accepts 32-bit tag wrap: ABA needs a stalled CAS across >2^32 successful head changes. */
        nextVersion = VbpTaggedVersion(expected) + 1;
        desired = VbpPackTaggedIndex(slotIndex + 1, nextVersion);
        if (VbpTaggedHeadCompareExchange(head, &expected, desired)) {
            return true;
        }
    }
    Assert(VbpTaggedIndexIsValid(VbpTaggedIndex(expected), slotCount));
    return false;
}

bool VbpFreeEntryStackPop(VbpTaggedHead *head, char *slotBase, Size slotSize, uint32 slotCount, uint32 *slotIndex)
{
    uint64 expected;

    if (!VbpFreeEntryStackArgsAreValid(head, slotBase, slotSize, slotCount) || slotIndex == NULL) {
        return false;
    }

    expected = VbpTaggedHeadRead(head);
    while (VbpTaggedIndex(expected) != 0) {
        uint32 currentIndex = VbpTaggedIndex(expected);
        uint32 currentSlot;
        VbpEntry *entry;
        uint64 control;
        uint32 nextIndex;
        uint32 nextVersion;
        uint64 desired;

        Assert(VbpTaggedIndexIsValid(currentIndex, slotCount));
        if (!VbpTaggedIndexIsValid(currentIndex, slotCount)) {
            return false;
        }

        currentSlot = currentIndex - 1;
        entry = VbpFreeEntryAt(slotBase, slotSize, currentSlot);
        control = pg_atomic_read_u64(&entry->control);
        nextIndex = VbpEntryControlFreeNext(control);
        if (!VbpEntryControlIsFree(control) || !VbpTaggedIndexIsValid(nextIndex, slotCount)) {
            uint64 observed = VbpTaggedHeadRead(head);
            /* A popped/reused old head slot may have its link poisoned before our CAS; retry a stale snapshot. */
            if (observed != expected) {
                expected = observed;
                continue;
            }
            Assert(VbpTaggedIndexIsValid(nextIndex, slotCount));
            return false;
        }

        nextVersion = VbpTaggedVersion(expected) + 1;
        desired = VbpPackTaggedIndex(nextIndex, nextVersion);
        if (VbpTaggedHeadCompareExchange(head, &expected, desired)) {
            *slotIndex = currentSlot;
            return true;
        }
    }
    return false;
}
