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
 * mm_lf_stack.h
 *    This is a lock-free ABA-aware stack implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_lf_stack.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_LF_STACK_H
#define MM_LF_STACK_H

#include "mm_def.h"
#include <stdint.h>

// This is a lock-free ABA-aware stack implementation

namespace MOT {

/** @typedef An internal node in a lock-free stack. */
struct MemLFStackNode {
    /** @var The pointed value. */
    void* m_value;

    /** @var A pointer to the next link in the stack. */
    MemLFStackNode* m_next;
};

/** @typedef The head node in a lock-free stack. */
struct MemLFStackHead {
    /** @var ABA version counter. */
    uintptr_t m_abaCount;

    /** @var A pointer to the top node in the stack. */
    MemLFStackNode* m_node;
};

/** @typedef Lock-free stack. */
struct MemLFStack {
    /** @var Pre-allocated buffer of nodes. */
    MemLFStackNode* m_nodeBuffer;

    /** @var Head of used nodes stack. */
    MemLFStackHead m_head L1_ALIGNED;

    /** @var Head of free nodes stack. */
    MemLFStackHead m_free L1_ALIGNED;

    /** @var The current size of the used node stack. */
    size_t m_size L1_ALIGNED;
};

/**
 * @brief Initializes a lock-free stack.
 * @param lfStack The lock-free stack to initialize.
 * @param nodeBuffer An externally provided free nodes buffer.
 * @param max_size The number of nodes in the provided node buffer.
 */
extern void MemLFStackInit(MemLFStack* lfStack, MemLFStackNode* nodeBuffer, size_t maxSize);

/**
 * @brief Destroys a lock-free stack.
 * @param lfStack The lock-free stack to destroy.
 */
extern void MemLFStackDestroy(MemLFStack* lfStack);

/**
 * @brief Retrieves the current size of the stack.
 * @param lfStack The lock-free stack.
 * @return The size of the stack.
 */
extern size_t MemLFStackSize(MemLFStack* lfStack);

/**
 * @brief Pushes an item on the stack.
 * @param lfStack The lock-free stack.
 * @param value The item to push.
 * @return Zero if successful, otherwise an error code.
 */
extern int MemLFStackPush(MemLFStack* lfStack, void* value);

/**
 * @brief Pops an item from the stack.
 * @param lfStack The lock-free stack.
 * @return The popped item or NULL if the stack is empty.
 */
extern void* MemLFStackPop(MemLFStack* lfStack);

}  // namespace MOT

#endif /* MM_LF_STACK_H */
