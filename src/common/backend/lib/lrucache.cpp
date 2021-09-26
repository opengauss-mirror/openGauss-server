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
 * lrucache.cpp
 *      Simple implementation of lru cache. Notice: this class is specified for 'hotkey' feature.
 *
 *
 * IDENTIFICATION
 *      src/common/backend/lib/lrucache.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "lib/lrucache.h"
#include "utils/atomic.h"
#include "utils/hotkey.h"

const double DECAY_RATIO = 0.75;
const int ELEM_INIT_COUNT = 2;

/* LRUCACHE */
LRUCache::LRUCache(int size, MemoryContext context) : len(0), capacity(size), cxt(context)
{}

LRUCache::~LRUCache()
{}

void LRUCache::Init()
{
    MemoryContext old = MemoryContextSwitchTo(cxt);
    array = (DllNode**)palloc(sizeof(DllNode*) * capacity);
    lock = (pthread_rwlock_t*)palloc(sizeof(pthread_rwlock_t));
    (void)MemoryContextSwitchTo(old);
    for (int i = 0; i < capacity; i++) {
        array[i] = New(cxt) DllNode(cxt);
        array[i]->Init();
        array[i]->weight = 1;
        array[i]->state = 0;
    }
    (void)pthread_rwlock_init(lock, NULL);

    for (int i = 0; i < capacity - 1; i++) {
        array[i]->next = array[i + 1];
        array[i + 1]->prev = array[i];
    }
    head = New(cxt) DllNode(cxt);
    head->Init();
    tail = New(cxt) DllNode(cxt);
    tail->Init();
    head->next = array[0];
    array[0]->prev = head;
    tail->prev = array[capacity - 1];
    array[capacity - 1]->next = tail;
}

void LRUCache::Destroy()
{
    len = 0;
    capacity = 0;
    cxt = NULL;
    (void)pthread_rwlock_destroy(lock);
    DestroyNode(head);
    DestroyNode(tail);
    for (int i = 0; i < capacity; i++) {
        DestroyNode(array[i]);
    }
    pfree_ext(array);
}

void LRUCache::DestroyNode(DllNode* node)
{
    node->Destroy();
    node = NULL;
}

void LRUCache::Put(void* node)
{
    RefNode* ref = New(cxt) RefNode();

    ref->key = node;
    ref->refcount = 1;

    AddToTail(ref);

    ReLocate(GetLastElem());
    return;
}

bool LRUCache::Contain(void* node, CmpFuncType type)
{
    if (IsEmpty()) {
        return false;
    }

    DllNode* ptr = GetFirstElem();
    bool find = false;
    DllNode* find_node = NULL;
    while (ptr != NULL && ptr->node != NULL) {
        if (!find && ptr->node->key != NULL && IsEqual(type, node, ptr->node->key)) {
            /* update hotkey */
            (void)pthread_rwlock_rdlock(lock);
            ptr->value++;
            ptr->weight = ptr->weight + 1;
            find = true;
            find_node = ptr;
            (void)pthread_rwlock_unlock(lock);
        } else {
            ptr->weight *= DECAY_RATIO;
        }
        ptr = ptr->next;
    }
    ReLocate(find_node);
    /* if find, free it */
    if (find) {
        CleanHotkeyInfo((HotkeyInfo*)node);
    }
    return find;
}

void LRUCache::AddToTail(RefNode* x)
{
    RefNode* temp = NULL;
    DllNode* array_tail = tail->prev;
    uint32 expected = 0;
    while (!pg_atomic_compare_exchange_u32(&(array_tail->state), &expected, 1)) {
        expected = 0;
    }
    temp = array_tail->node;
    array_tail->node = x;
    array_tail->value = ELEM_INIT_COUNT;
    pg_atomic_write_u32(&(array_tail->state), 0);
    array_tail->weight = 1;
    if (temp == NULL || temp->key == NULL) {
        len++;
    } else if (pg_atomic_sub_fetch_u64(&temp->refcount, 1) == 0) {
        temp->Destroy();
        pfree_ext(temp);
    }
    return;
}

void LRUCache::Remove(DllNode* x)
{
    Assert(x != NULL);
    if (len == 0 || x->node == NULL) {
        return;
    }

    RefNode* temp = NULL;
    uint32 expected = 0;
    while (!pg_atomic_compare_exchange_u32(&x->state, &expected, 1)) {
        expected = 0;
    }
    temp = x->node;
    x->node = NULL;
    x->value = 0;
    expected = 1;
    (void)pg_atomic_compare_exchange_u32(&x->state, &expected, 0);
    x->weight = 1;
    len--;
    if (pg_atomic_sub_fetch_u64(&temp->refcount, 1) == 0) {
        temp->Destroy();
        pfree_ext(temp);
    }
    return;
}

void LRUCache::ReLocate(DllNode* x)
{
    if (x == NULL) {
        return;
    }

    DllNode* ptr = GetFirstElem();
    while (ptr != x) {
        if (ptr->weight <= x->weight) {
            break;
        } else {
            ptr = ptr->next;
        }
    }
    /* if ptr == x，doesn't need to swtich */
    if (ptr == x) {
        return;
    }

    /* insert x to front of ptr */
    (void)pthread_rwlock_wrlock(lock);
    DllNode* temp1 = x->next;
    DllNode* temp2 = ptr->prev;
    if (temp2 != NULL) {
        temp2->next = x;
    }
    x->prev->next = temp1;
    x->next = ptr;
    if (temp1 != NULL) {
        temp1->prev = x->prev;
    }
    x->prev = temp2;
    ptr->prev = x;

    (void)pthread_rwlock_unlock(lock);
    return;
}

void LRUCache::MoveToTail(DllNode* x)
{
    (void)pthread_rwlock_wrlock(lock);
    DllNode* temp1 = x->next;
    DllNode* temp2 = tail->prev;
    if (x->prev != NULL) {
        x->prev->next = temp1;
    }
    x->next = tail;
    temp2->next = x;
    if (temp1 != NULL) {
        temp1->prev = x->prev;
    }
    x->prev = temp2;
    tail->prev = x;
    (void)pthread_rwlock_unlock(lock);
    return;
}

/* no need to lock for it's just used in pgstat thread */
void LRUCache::Clean()
{
    if (len == 0) {
        return;
    }
    DllNode* ptr = head->next;
    while (ptr != NULL) {
        Remove(ptr);
        ptr = ptr->next;
    }
}

/* no need to lock for it's just used in pgstat thread */
void LRUCache::DeleteHotkeysInTAB(Oid tOid)
{
    DllNode* temp = NULL;
    DllNode* ptr = head->next;
    while (ptr != NULL && ptr->node != NULL) {
        temp = ptr->next;
        HotkeyInfo* keyInfo = (HotkeyInfo*)ptr->node->key;
        if (keyInfo != NULL && keyInfo->relationOid == tOid) {
            Remove(ptr);
            MoveToTail(ptr);
        }
        ptr = temp;
    }
    return;
}

/* no need to lock for it's just used in pgstat thread */
void LRUCache::DeleteHotkeysInDB(Oid dbOid)
{
    DllNode* temp = NULL;
    DllNode* ptr = head->next;
    while (ptr != NULL && ptr->node != NULL) {
        temp = ptr->next;
        HotkeyInfo* keyInfo = (HotkeyInfo*)ptr->node->key;
        if (keyInfo != NULL && keyInfo->databaseOid == dbOid) {
            Remove(ptr);
            MoveToTail(ptr);
        }
        ptr = temp;
    }
    return;
}
