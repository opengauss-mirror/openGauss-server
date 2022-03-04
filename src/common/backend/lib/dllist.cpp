/* -------------------------------------------------------------------------
 *
 * dllist.cpp
 *	  this is a simple doubly linked list implementation
 *	  the elements of the lists are void*
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/lib/dllist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "lib/dllist.h"
#include "miscadmin.h"

Dllist* DLNewList(void)
{
    Dllist* l = NULL;

    l = (Dllist*)palloc(sizeof(Dllist));

    l->dll_head = NULL;
    l->dll_tail = NULL;
    l->dll_len = 0;

    return l;
}

void DLInitList(Dllist* list)
{
    list->dll_head = NULL;
    list->dll_tail = NULL;
    list->dll_len = 0;
}

/*
 * free up a list and all the nodes in it --- but *not* whatever the nodes
 * might point to!
 */
void DLFreeList(Dllist* list)
{
    Dlelem* curr = NULL;

    while ((curr = DLRemHead(list)) != NULL)
        pfree(curr);

    pfree(list);
}

Dlelem* DLNewElem(void* val)
{
    Dlelem* e = NULL;

    e = (Dlelem*)palloc(sizeof(Dlelem));

    e->dle_next = NULL;
    e->dle_prev = NULL;
    e->dle_val = val;
    e->dle_list = NULL;
    return e;
}

void DLInitElem(Dlelem* e, void* val)
{
    e->dle_next = NULL;
    e->dle_prev = NULL;
    e->dle_val = val;
    e->dle_list = NULL;
}

void DLFreeElem(Dlelem* e)
{
    pfree(e);
}

void DLRemove(Dlelem* e)
{
    Dllist* l = e->dle_list;

    if (e->dle_prev)
        e->dle_prev->dle_next = e->dle_next;
    else {
        /* must be the head element */
        Assert(e == l->dll_head);
        l->dll_head = e->dle_next;
    }
    if (e->dle_next)
        e->dle_next->dle_prev = e->dle_prev;
    else {
        /* must be the tail element */
        Assert(e == l->dll_tail);
        l->dll_tail = e->dle_prev;
    }
    if (l != NULL) {
        l->dll_len--;
    }

    e->dle_next = NULL;
    e->dle_prev = NULL;
    e->dle_list = NULL;
}

void DLAddHead(Dllist* l, Dlelem* e)
{
    e->dle_list = l;

    if (l->dll_head)
        l->dll_head->dle_prev = e;
    e->dle_next = l->dll_head;
    e->dle_prev = NULL;
    l->dll_head = e;

    if (l->dll_tail == NULL) /* if this is first element added */
        l->dll_tail = e;
    l->dll_len++;
}

void DLAddTail(Dllist* l, Dlelem* e)
{
    e->dle_list = l;

    if (l->dll_tail)
        l->dll_tail->dle_next = e;
    e->dle_prev = l->dll_tail;
    e->dle_next = NULL;
    l->dll_tail = e;

    if (l->dll_head == NULL) /* if this is first element added */
        l->dll_head = e;
    l->dll_len++;
}

Dlelem* DLRemHead(Dllist* l)
{
    /* remove and return the head */
    Dlelem* result = l->dll_head;

    if (result == NULL)
        return result;

    if (result->dle_next)
        result->dle_next->dle_prev = NULL;

    l->dll_head = result->dle_next;

    if (result == l->dll_tail) /* if the head is also the tail */
        l->dll_tail = NULL;

    l->dll_len--;
    result->dle_next = NULL;
    result->dle_list = NULL;

    return result;
}

Dlelem* DLRemTail(Dllist* l)
{
    /* remove and return the tail */
    Dlelem* result = l->dll_tail;

    if (result == NULL)
        return result;

    if (result->dle_prev)
        result->dle_prev->dle_next = NULL;

    l->dll_tail = result->dle_prev;

    if (result == l->dll_head) /* if the tail is also the head */
        l->dll_head = NULL;

    l->dll_len--;
    result->dle_prev = NULL;
    result->dle_list = NULL;

    return result;
}

/* Same as DLRemove followed by DLAddHead, but faster */
void DLMoveToFront(Dlelem* e)
{
    Dllist* l = e->dle_list;

    if (l->dll_head == e)
        return; /* Fast path if already at front */

    Assert(e->dle_prev != NULL); /* since it's not the head */
    e->dle_prev->dle_next = e->dle_next;

    if (e->dle_next)
        e->dle_next->dle_prev = e->dle_prev;
    else {
        /* must be the tail element */
        Assert(e == l->dll_tail);
        l->dll_tail = e->dle_prev;
    }

    l->dll_head->dle_prev = e;
    e->dle_next = l->dll_head;
    e->dle_prev = NULL;
    l->dll_head = e;
    /* We need not check dll_tail, since there must have been > 1 entry */
}

/*
 * double-linked list length
 */
uint64 DLListLength(Dllist* list)
{
    Dlelem* cur = list->dll_head;
    uint64 length = 0;

    while (cur != NULL) {
        length++;
        cur = cur->dle_next;
    }
    Assert(length == list->dll_len);
    return length;
}

DllistWithLock::DllistWithLock()
{
    DLInitList(&m_list);
    SpinLockInit(&m_lock);
}

DllistWithLock::~DllistWithLock()
{
    SpinLockFree(&m_lock);
}

bool DllistWithLock::RemoveConfirm(Dlelem* e)
{
    bool found = false;
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
	if (e->dle_list == &m_list) {
        found = true;
        DLRemove(e);
    }
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
    return found;
}

void DllistWithLock::AddHead(Dlelem* e)
{
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
    if (e->dle_list == NULL) {
        DLAddHead(&m_list, e);
    }
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
}

void DllistWithLock::AddTail(Dlelem* e)
{
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
    if (e->dle_list == NULL) {
        DLAddTail(&m_list, e);
    }
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
}

Dlelem* DllistWithLock::RemoveHead()
{
    Dlelem* head = NULL;
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
    head = DLRemHead(&m_list);
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
    return head;
}

Dlelem* DllistWithLock::RemoveTail()
{
    Dlelem* head = NULL;
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
    head = DLRemTail(&m_list);
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
    return head;
}

bool DllistWithLock::IsEmpty()
{
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
    bool ret = DLIsNIL(&m_list);
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
    return ret;
}

Dlelem* DllistWithLock::GetHead()
{
    Dlelem* head = NULL;
    head = m_list.dll_head;
    return head;
}

void DllistWithLock::GetLock()
{
    START_CRIT_SECTION();
    SpinLockAcquire(&(m_lock));
}

Dlelem* DllistWithLock::RemoveHeadNoLock()
{
    Dlelem* head = DLRemHead(&m_list);
    return head;
}

void DllistWithLock::ReleaseLock()
{
    SpinLockRelease(&(m_lock));
    END_CRIT_SECTION();
}
