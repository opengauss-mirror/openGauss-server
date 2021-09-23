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
 * mm_lf_stack.cpp
 *    This is a lock-free ABA-aware stack implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_lf_stack.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_lf_stack.h"
#include "mot_atomic_ops.h"
#include "mot_error.h"

namespace MOT {

static MemLFStackNode* MemLFStackPopNode(MemLFStackHead* head);
static void MemLFStackPushNode(MemLFStackHead* head, MemLFStackNode* node);

extern void MemLFStackInit(MemLFStack* lfStack, MemLFStackNode* nodeBuffer, size_t maxSize)
{
    MemLFStackHead headInit = {0, NULL};
    lfStack->m_head = headInit;
    lfStack->m_size = 0;

    lfStack->m_nodeBuffer = nodeBuffer;
    for (size_t i = 0; i < maxSize - 1; i++) {
        lfStack->m_nodeBuffer[i].m_next = lfStack->m_nodeBuffer + i + 1;
    }
    lfStack->m_nodeBuffer[maxSize - 1].m_next = NULL;
    MemLFStackHead freeInit = {0, lfStack->m_nodeBuffer};
    lfStack->m_free = freeInit;
}

extern void MemLFStackDestroy(MemLFStack* lfStack)
{
    // nothing to do
}

extern size_t MemLFStackSize(MemLFStack* lfStack)
{
    return MOT_ATOMIC_LOAD(lfStack->m_size);
}

extern int MemLFStackPush(MemLFStack* lfStack, void* value)
{
    int result = 0;
    MemLFStackNode* node = MemLFStackPopNode(&lfStack->m_free);
    if (node == NULL) {
        result = MOT_ERROR_OOM;
    } else {
        node->m_value = value;
        MemLFStackPushNode(&lfStack->m_head, node);
        MOT_ATOMIC_INC(lfStack->m_size);
    }
    return result;
}

extern void* MemLFStackPop(MemLFStack* lfStack)
{
    void* result = NULL;
    MemLFStackNode* node = MemLFStackPopNode(&lfStack->m_head);
    if (node != NULL) {
        MOT_ATOMIC_DEC(lfStack->m_size);
        result = node->m_value;
        MemLFStackPushNode(&lfStack->m_free, node);
    }
    return result;
}

static MemLFStackNode* MemLFStackPopNode(MemLFStackHead* head)
{
    MemLFStackHead next = {0, 0};
    MemLFStackHead orig = {0, 0};
    __atomic_load(head, &orig, __ATOMIC_SEQ_CST);
    (void)head;
    do {
        if (orig.m_node == NULL) {
            return NULL;  // empty stack
        }
        next.m_abaCount = orig.m_abaCount + 1;
        next.m_node = orig.m_node->m_next;
    } while (!__atomic_compare_exchange(head, &orig, &next, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST));
    return orig.m_node;
}

static void MemLFStackPushNode(MemLFStackHead* head, MemLFStackNode* node)
{
    MemLFStackHead next = {0, 0};
    MemLFStackHead orig = {0, 0};
    __atomic_load(head, &orig, __ATOMIC_SEQ_CST);
    (void)head;
    do {
        node->m_next = orig.m_node;
        next.m_abaCount = orig.m_abaCount + 1;
        next.m_node = node;
    } while (!__atomic_compare_exchange(head, &orig, &next, true, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST));
}

}  // namespace MOT
