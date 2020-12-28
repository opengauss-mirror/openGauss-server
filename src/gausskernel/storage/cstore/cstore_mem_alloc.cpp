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
 * ---------------------------------------------------------------------------------------
 *
 * cstore_mem_alloc.cpp
 *      routines to support ColStore
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_mem_alloc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "storage/cstore/cstore_mem_alloc.h"
#include "utils/aiomem.h"
#include <malloc.h>

THR_LOCAL uint64 CStoreMemAlloc::m_count = 0;
THR_LOCAL uint32 CStoreMemAlloc::m_ptrNodeCacheCount = 0;
THR_LOCAL PointerNode* CStoreMemAlloc::m_ptrNodeCache[MaxPtrNodeCacheLen];
THR_LOCAL PointerList CStoreMemAlloc::m_tab[MaxPointersArryLen];

static inline void* InnerMalloc(Size size)
{
    void* ptr = NULL;
    ADIO_RUN()
    {
        int ret = posix_memalign((void**)&(ptr), SYS_LOGICAL_BLOCK_SIZE, (size_t)(size));
        if (ret != 0) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("posix_memalign fails, The alignment argument was not a power of two, or was not a multiple "
                                   "of sizeof(void *)")));
        }
    }
    ADIO_ELSE()
    {
        ptr = malloc(size);
    }
    ADIO_END();
    return ptr;
}

/*
 * @Description: malloc from system
 * @Param[IN] size: memory size needed
 * @Param[IN] toRegister: if this memory is thread-managered scope, set true;
 *                        if this memory is process-managered scope, set false.
 * @See also:
 */
void* CStoreMemAlloc::Palloc(Size size, bool toRegister)
{
    Assert(size > 0);
    void* ptr = InnerMalloc(size);
    if (ptr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc fails, out of memory: size %lu", size)));
    }

    if (toRegister) {
        /* Step 2: register pointer */
        Register(ptr);
    }
    return ptr;
}

void* CStoreMemAlloc::AllocPointerNode()
{
    PointerNode* ptr = NULL;
    if (m_ptrNodeCacheCount > 0) {
        ptr = m_ptrNodeCache[--m_ptrNodeCacheCount];
    } else {
        ptr = (PointerNode*)malloc(sizeof(PointerNode));
        if (ptr == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("malloc fails, out of memory")));
        }
    }
    return ptr;
}

void CStoreMemAlloc::FreePointerNode(PointerNode* ptr)
{
    if (m_ptrNodeCacheCount < MaxPtrNodeCacheLen) {
        m_ptrNodeCache[m_ptrNodeCacheCount++] = ptr;
    } else {
        free(ptr);
    }
}

/*
 * @Description: remalloc from system
 * @Param[IN] old_size: old memory size
 * @Param[IN] size:     new memory size
 * @Param[IN] pointer:  memory to free
 * @Param[IN] registered: true if passing true to Palloc();
 *                        false if passing false to Palloc();
 * @See also:
 */
void* CStoreMemAlloc::Repalloc(void* pointer, Size size, Size old_size, bool registered)
{
    Assert(pointer != NULL);
    Assert(size > 0);

    void* ptr = InnerMalloc(size);
    if (ptr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    errno_t rc = memcpy_s(ptr, size, pointer, old_size);
    securec_check_c(rc, "\0", "\0");

    if (registered) {
        Register(ptr);
        Unregister(pointer);
        Assert(m_count > 0);
    }

    free(pointer);

    return ptr;
}

/*
 * @Description: free to system
 * @Param[IN] pointer: memory to free
 * @Param[IN] registered: true if passing true to Palloc();
 *                        false if passing false to Palloc();
 * @See also:
 */
void CStoreMemAlloc::Pfree(void* pointer, bool registered)
{
    Assert(pointer);
    if (registered) {
        Unregister(pointer);
    }
    free(pointer);
}

void CStoreMemAlloc::Register(void* pointer)
{
    Assert((MaxPointersArryLen & (MaxPointersArryLen - 1)) == 0);
    int idx = PointerGetDatum(pointer) & (MaxPointersArryLen - 1);
    PointerNode* nodePtr = m_tab[idx].tail;

    if (nodePtr == NULL) {
        Assert(m_tab[idx].header == NULL);
        m_tab[idx].header = m_tab[idx].tail = (PointerNode*)AllocPointerNode();
        m_tab[idx].header->ptr = pointer;
        m_tab[idx].header->next = NULL;
    } else {
        nodePtr->next = (PointerNode*)AllocPointerNode();
        nodePtr->next->ptr = pointer;
        nodePtr->next->next = NULL;
        m_tab[idx].tail = nodePtr->next;
    }
    ++m_count;
}

void CStoreMemAlloc::Unregister(const void* pointer)
{
    Assert(pointer && m_count > 0);
    // Step 1: which node list include this pointer
    //
    Assert((MaxPointersArryLen & (MaxPointersArryLen - 1)) == 0);
    int idx = PointerGetDatum(pointer) & (MaxPointersArryLen - 1);

    // Step 2: Search node list
    //
    PointerNode* nodePtr = m_tab[idx].header;
    Assert(nodePtr);

    PointerNode* prePtr = NULL;
    while (nodePtr != NULL) {
        if (nodePtr->ptr == pointer) {
            if (prePtr == NULL) {
                m_tab[idx].header = nodePtr->next;

                // If this list has only one node
                //
                if (m_tab[idx].tail == nodePtr) {
                    Assert(m_tab[idx].header == NULL);
                    m_tab[idx].tail = NULL;
                }
            } else {
                prePtr->next = nodePtr->next;

                // We need modify tail pointer if free tail node
                //
                if (m_tab[idx].tail == nodePtr) {
                    m_tab[idx].tail = prePtr;
                    Assert(m_tab[idx].tail->next == NULL);
                }
            }

            nodePtr->Reset();
            FreePointerNode(nodePtr);

            break;
        }
        prePtr = nodePtr;
        nodePtr = nodePtr->next;
    }
    --m_count;
    Assert(nodePtr != NULL);
}

void CStoreMemAlloc::Reset()
{
    uint32 i = 0;
    uint32 freeNum = 0;
    if (m_count) {
        // Step 1: free each list in m_tab
        //
        for (i = 0; i < MaxPointersArryLen; ++i) {
            PointerNode* nodePtr = m_tab[i].header;
            PointerNode* tmpPtr = NULL;
            while ((nodePtr != NULL) && (nodePtr->ptr != NULL)) {
                free(nodePtr->ptr);
                tmpPtr = nodePtr->next;
                free(nodePtr);
                nodePtr = tmpPtr;
                ++freeNum;
            }

            // Note that We must reset NULL
            // Because thread can be reused
            //
            m_tab[i].header = NULL;
            m_tab[i].tail = NULL;
        }

        Assert(m_count == freeNum);
        m_count = 0;
    }
    // Step 2: free cached node if need
    //
    for (i = 0; i < m_ptrNodeCacheCount; ++i) {
        free(m_ptrNodeCache[i]);
    }
    m_ptrNodeCacheCount = 0;
}

void CStoreMemAlloc::Init()
{
    m_count = 0;
    Assert((MaxPointersArryLen & (MaxPointersArryLen - 1)) == 0);
    for (uint32 i = 0; i < MaxPointersArryLen; ++i) {
        m_tab[i].header = NULL;
        m_tab[i].tail = NULL;
    }
    m_ptrNodeCacheCount = 0;
}
