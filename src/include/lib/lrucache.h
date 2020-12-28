/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * lrucache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/lib/lrucache.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LRUCACHE_INCLUDE
#define LRUCACHE_INCLUDE

#include "postgres.h"

typedef bool (*CmpFuncType)(void*, void*);
void CleanHotkeyInfo(struct HotkeyInfo* key);

/* struct with ref count to help release key info */
struct RefNode : public BaseObject {
    void* key;
    volatile uint64 refcount;
    RefNode()
    {
        key = NULL;
        refcount = 1;
    }
    ~RefNode()
    {}
    void Destroy()
    {
        if (key != NULL) {
            CleanHotkeyInfo((HotkeyInfo*)key);
            key = NULL;
        }
        refcount = 0;
    }
};

/* basic node for lru cache operation */
struct DllNode : public BaseObject {
    RefNode* node;
    volatile uint64 value;
    double weight;
    DllNode* next;
    DllNode* prev;
    volatile uint32 state;  // thread that occupies 0 can operate this Node
    MemoryContext cxt;
    DllNode(MemoryContext context)
    {
        value = 0;
        weight = 0;
        next = NULL;
        prev = NULL;
        state = 0;
        cxt = context;
    }
    ~DllNode()
    {}
    void Init()
    {
        node = New(cxt) RefNode();
    }
    void Destroy()
    {
        if (node != NULL) {
            node->Destroy();
            node = NULL;
        }
        value = 0;
        weight = 0;
        next = NULL;
        prev = NULL;
        state = 0;
    }
};

class LRUCache : public BaseObject {
public:
    LRUCache(int size, MemoryContext context);
    ~LRUCache();

    void Init();
    void Destroy();
    void Put(void* node);
    bool Contain(void* node, CmpFuncType type);
    void AddToTail(RefNode* x);
    void Remove(DllNode* n);
    void MoveToTail(DllNode* n);
    void ReLocate(DllNode* x);
    void Clean();
    void DeleteHotkeysInTAB(Oid t_oid);
    void DeleteHotkeysInDB(Oid db_oid);
    void DestroyNode(DllNode* node);
    int GetLength()
    {
        return len;
    }
    DllNode* GetFirstElem()
    {
        return head->next;
    }
    DllNode* GetLastElem()
    {
        return tail->prev;
    }
    DllNode** GetArray()
    {
        return array;
    }
    bool IsFull()
    {
        return len == capacity;
    }
    bool IsEmpty()
    {
        return len == 0;
    }
    pthread_rwlock_t* GetLock()
    {
        return lock;
    }

private:
    int len;
    int capacity;
    MemoryContext cxt;
    DllNode* head;
    DllNode* tail;
    DllNode** array;
    pthread_rwlock_t* lock;
};
#endif /* LRUCACHE_INCLUDE */
