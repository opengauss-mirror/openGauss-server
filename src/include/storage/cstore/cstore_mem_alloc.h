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
 * cstore_mem_alloc.h
 *        routines to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/cstore/cstore_mem_alloc.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CSTORE_MEM_ALLOC_H
#define CSTORE_MEM_ALLOC_H

#include "postgres.h"
#include "knl/knl_variable.h"

#define MaxPointersArryLen 512U
#define MaxPtrNodeCacheLen 256U

typedef struct PointerNode {
    void* ptr;
    PointerNode* next;

    void Reset()
    {
        ptr = NULL;
        next = NULL;
    }
} PointerNode;

typedef struct PointerList {
    PointerNode* header;
    PointerNode* tail;
} NodeList;

// CStoreMemAlloc
// It manages the memory pointer from malloc
// When transaction occurs erro, the memory from malloc can be reset in abortTransaction
// When transaction commit, the memory from malloc can be reset
//
class CStoreMemAlloc {
public:
    static void* Palloc(Size size, bool toRegister = true);
    static void Pfree(void* pointer, bool registered = true);
    static void* Repalloc(void* pointer, Size size, Size old_size, bool registered = true);
    static void Register(void* pointer);
    static void Unregister(const void* pointer);
    static void Reset();
    static void Init();

private:
    static void* AllocPointerNode();
    static void FreePointerNode(PointerNode* pointer);

    static THR_LOCAL PointerList m_tab[MaxPointersArryLen];
    static THR_LOCAL uint64 m_count;

    static THR_LOCAL uint32 m_ptrNodeCacheCount;
    static THR_LOCAL PointerNode* m_ptrNodeCache[MaxPtrNodeCacheLen];
};

#endif
