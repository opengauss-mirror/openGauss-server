/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * gs_set.h
 * set Containers declaration
 *
 *
 * IDENTIFICATION
 * src/include/gs_policy/gs_set.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef INCLUDE_UTILS_GS_SET_H_
#define INCLUDE_UTILS_GS_SET_H_

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/rbtree.h"
#include "utils/palloc.h"
#include "gs_string.h"

namespace gs_stl {
#define PG_SET_MAX_SIZE 1024
typedef int (*CompareKeyFunc)(const void *keyA, const void *keyB);

template <class Key, CompareKeyFunc CompareKeys = defaultCompareKeyFunc<Key>, int max_elements = PG_SET_MAX_SIZE,
    int key_size = sizeof(Key)>

class gs_set {
public:
    /* *
     * Iterator can be used to access the sequence of elements in a range in both directions
     * (towards the end and towards the beginning).
     */
    struct iterator {
        iterator() : first(NULL), m_prev(NULL), m_next(NULL) {}

        iterator(const iterator &arg) : first(arg.first), m_prev(arg.m_prev), m_next(arg.m_next) {}

        void inline reset(void)
        {
            first = NULL;
            m_prev = NULL;
            m_next = NULL;
        }

        const Key &operator*() const
        {
            return *first;
        }

        const Key *operator->() const
        {
            return first;
        }

        bool operator == (const iterator &arg) const
        {
            if (first == NULL && arg.first == NULL)
                return true;
            if (first == NULL || arg.first == NULL)
                return false;
            return *first == *(arg.first);
        }

        bool operator != (const iterator &arg) const
        {
            return !(operator == (arg));
        }

        iterator &operator ++ ()
        {
            if (m_next != NULL) {
                m_prev = m_next->m_prev;
                first = m_next->first;
                m_next = m_next->m_next;
            } else {
                first = NULL;
                m_next = NULL;
                m_prev = this;
            }
            return *this;
        }

        Key *first;
        iterator *m_prev;
        iterator *m_next;
    };

    /*
     * A pair, with its member pg_pair::first set to an iterator pointing to either the newly inserted element
     * or to the end of set, if an equivalent key in the set found. The pg_pair::second element in the pair
     * is set to true if a new element was inserted or false if an equivalent key already existed.
     */
    typedef gs_stl::Pair<iterator, bool> pg_pair;
    typedef iterator const_iterator;

    /* *
     * Wrapper around our iterator to support RB tree.
     */
    struct IteratorTreeNodeWrapper {
        RBNode m_rb_node;
        iterator m_iter;
    };

    /* * Combiner function for rbtree.c
     * An equivalent key (already existed) is not allowed to add
     */
    static void combineDataEntry(RBNode *existing, const RBNode *newdata, void *arg)
    {
        /* Nothing to do */
    }

    /* Comparator function for rbtree.c */
    static int compareDataEntry(const RBNode *a, const RBNode *b, void *arg)
    {
        const IteratorTreeNodeWrapper *it_a = (const IteratorTreeNodeWrapper *)a;
        const IteratorTreeNodeWrapper *it_b = (const IteratorTreeNodeWrapper *)b;

        return CompareKeys(it_a->m_iter.first, it_b->m_iter.first);
    }

    /* Allocator function for rbtree.c */
    static RBNode *allocDataEntry(void *arg)
    {
        MemoryContext oldContext;
        /*
         * We must use the TopMostMemoryContext because the RB tree
         * not bound to a thread and can outlive any of the thread specific
         * contextes.
         */
        oldContext = MemoryContextSwitchTo(GetSetMemory());
        IteratorTreeNodeWrapper *res = (IteratorTreeNodeWrapper *)palloc(sizeof(IteratorTreeNodeWrapper));
        new (static_cast<void *>(res)) IteratorTreeNodeWrapper();
        (void)MemoryContextSwitchTo(oldContext);
        return (RBNode *)res;
    }

    /* Deallocator function for rbtree.c */
    static void deallocDataEntry(RBNode *x, void *arg)
    {
        IteratorTreeNodeWrapper *iter_wrapper = (IteratorTreeNodeWrapper *)x;
        if (std::is_trivial<Key>::value == 0) {
            iter_wrapper->m_iter.first->~Key();
        }

        pfree(iter_wrapper->m_iter.first);
        pfree(iter_wrapper);
    }

    static void copyDataEntry(RBTree* rb, RBNode *dest, const RBNode *src)
    {
        /* We should use our defined function to copy data replace the default copy logic of the rb tree */
        IteratorTreeNodeWrapper *iter_dest = (IteratorTreeNodeWrapper *)dest;
        IteratorTreeNodeWrapper *iter_src = (IteratorTreeNodeWrapper *)src;
        if (iter_dest->m_iter.first != NULL) {
            if (std::is_trivial<Key>::value == 0) {
                iter_dest->m_iter.first->~Key();
            }
            pfree(iter_dest->m_iter.first);
        }
        errno_t rc = memcpy_s(dest + 1, rb->node_size - sizeof(RBNode), src + 1, rb->node_size - sizeof(RBNode));
        securec_check(rc, "\0", "\0");
        MemoryContext oldContext = MemoryContextSwitchTo(GetSetMemory());
        iter_dest->m_iter.first = (Key *)palloc(key_size);
        new (static_cast<void *>(iter_dest->m_iter.first)) Key(*(iter_src->m_iter.first));
        MemoryContextSwitchTo(oldContext);
    }

public:
    /* *
     * Constructs a set container object, initializing its contents.
     */
    gs_set(const gs_set &arg) : m_tree(NULL), m_begin_iter(NULL), m_end_iter(NULL)
    {
        Init();
        insert(const_cast<gs_set &>(arg).begin(), const_cast<gs_set &>(arg).end());
    }

    gs_set() : m_size(0)
    {
        Init();
    }

    /* *
     * Destroys the container object. This destroys all container elements,
     * and deallocates all the storage capacity allocated by the set container using its allocator.
     * Note that: we do not use iterate function of rbtree as something wrong with that,
     * but it works well using flatting iterator which mapping to.
     *
     */
    ~gs_set()
    {
        if ((m_tree == NULL) || (t_thrd.port_cxt.thread_is_exiting == true)) {
            return;
        }

        iterator *i_itr = m_begin_iter;
        while (i_itr != m_end_iter) {
            /* this way we calculate the address of IteratorTreeNodeWrapper */
            IteratorTreeNodeWrapper *ptr = (IteratorTreeNodeWrapper *)((RBNode *)i_itr - 1);
            i_itr = i_itr->m_next;
            if (std::is_trivial<Key>::value == 0) {
                ptr->m_iter.first->~Key();
            }
            pfree(ptr->m_iter.first);
            pfree(ptr);
        }
        pfree(m_tree);
        pfree(m_end_iter);
        m_tree = NULL;
        m_end_iter = NULL;
    }

    void clear()
    {
        if (m_tree && !empty()) {
            /* rb_begin_iterate have to recalled after rb_delete as the iterator is invalid now */
            rb_begin_iterate(m_tree, InvertedWalk);
            RBNode *_node = rb_iterate(m_tree);
            while (_node != NULL) {
                rb_delete(m_tree, _node);
                rb_begin_iterate(m_tree, InvertedWalk);
                _node = rb_iterate(m_tree);
                --m_size;
            }
            m_begin_iter = m_end_iter;
        }
    }

    /* *
     * Test whether container is empty.
     * @return Returns whether the set container is empty (i.e. whether its size is 0).
     */
    bool empty() const
    {
        return (m_size == 0);
    }

    /* *
     * Return container size.
     * @return Returns the number of elements in the set container.
     */
    size_t size() const
    {
        return m_size;
    }

    gs_set &operator = (const gs_set &arg)
    {
        if (this == &arg) {
            return  *this;
        }
        clear();
        insert(const_cast<gs_set &>(arg).begin(), const_cast<gs_set &>(arg).end());
        return *this;
    }

    void insert(iterator it, iterator eit)
    {
        for (; it != eit; ++it) {
            insert(*it.first);
        }
    }

    /* *
     * Extends the container by inserting new elements, effectively increasing
     * the container size by the number of elements inserted.
     * @return return a pair, with its member pair::first set to an iterator pointing to either
     * the newly inserted element or to end of set, if an equivalent key in the set.
     * The pair::second element in the pair is set to true if a new element was inserted
     * or false if an equivalent key already existed.
     */
    pg_pair insert(const Key &val)
    {
        bool isNew = false;
        if (m_size >= max_elements) {
            return pg_pair(end(), false);
        }
        IteratorTreeNodeWrapper data;
        data.m_iter.first = (Key *)&(val);

        IteratorTreeNodeWrapper *it_wrapper =
            (IteratorTreeNodeWrapper *)rb_insert(m_tree, (const RBNode *)&data, &isNew);
        if (!isNew) {
            /* already exist return old one */
            return pg_pair(it_wrapper->m_iter, false);
        }

        /* add this iterator to list of other iterators */
        it_wrapper->m_iter.m_next = m_begin_iter;
        m_begin_iter->m_prev = &it_wrapper->m_iter;
        m_begin_iter = &it_wrapper->m_iter;

        m_size++;
        return pg_pair(it_wrapper->m_iter, true);
    }

    /* *
     * Get iterator to element.
     * Searches the container for an element with a key equivalent to k
     * and returns an iterator to it if found, otherwise it returns an iterator to set::end.
     * @return An iterator to the element, if an element with specified key is found, or set::end otherwise.
     */
    iterator find(const Key &k) const
    {
        IteratorTreeNodeWrapper dummy;
        dummy.m_iter.first = (Key *)&k;

        RBNode *res = rb_find(m_tree, (const RBNode *)&dummy);
        if (res == NULL) {
            return end();
        }

        return ((IteratorTreeNodeWrapper *)res)->m_iter;
    }

    int erase(iterator it)
    {
        return erase(*it.first);
    }

    void update_iterator()
    {
        if (m_tree != NULL && !empty()) {
            rb_begin_iterate(m_tree, DirectWalk); /* use preorder traversal */
            RBNode *node = rb_iterate(m_tree);
            IteratorTreeNodeWrapper *it = (IteratorTreeNodeWrapper *)node;
            m_begin_iter = &it->m_iter;
            m_begin_iter->m_prev = NULL;
            while ((node = rb_iterate(m_tree)) != NULL) {
                IteratorTreeNodeWrapper *cur = (IteratorTreeNodeWrapper *)node;
                it->m_iter.m_next = &cur->m_iter;
                cur->m_iter.m_prev = &it->m_iter;
                it = cur;
            }
            it->m_iter.m_next = m_end_iter;
            m_end_iter->m_prev = &it->m_iter;
        } else {
            m_begin_iter = m_end_iter;
        }
    }

    /* *
     * Erase element.
     * Removes from the set container a single element.
     * @return  the number of elements erased (1 or 0).
     */
    int erase(const Key &k)
    {
        IteratorTreeNodeWrapper dummy;
        dummy.m_iter.first = (Key *)&k;

        RBNode *res = rb_find(m_tree, (const RBNode *)&dummy);
        if (res == NULL) {
            return 0;
        }

        rb_delete(m_tree, res);
        m_size--;
        update_iterator();
        return 1;
    }

    /* *
     * Return iterator to beginning.
     */
    iterator begin() const
    {
        return *m_begin_iter;
    }

    /* *
     * Return iterator to end.
     */
    iterator end() const
    {
        return *m_end_iter;
    }

    /* *
     * Test whether containers has intersection.
     * @return True if there is at least one equivalent key in other container; false otherwise.
     */
    bool has_intersection(const gs_set &other)
    {
        if (this == &other) {
            return true;
        }
        /* no intersection with empty containers */
        if (empty() || other.empty())
            return false;

        iterator it = begin();
        iterator eit = end();
        for (; it != eit; ++it) {
            if (other.find(*it.first) != other.end())
                return true;
        }
        return false;
    }

private:
    inline void Init()
    {
        /*
         * We must use the TopMostMemoryContext because the RB tree
         * not bound to a thread and can outlive any of the thread specific
         * contextes.
         */
        MemoryContext oldContext = MemoryContextSwitchTo(GetSetMemory());

        m_end_iter = (iterator *)palloc(sizeof(iterator));
        new (static_cast<void *>(m_end_iter)) iterator();

        m_begin_iter = m_end_iter;
        m_size = 0;

        m_tree = rb_create(sizeof(IteratorTreeNodeWrapper), compareDataEntry, combineDataEntry, allocDataEntry,
            deallocDataEntry, NULL, copyDataEntry);

        (void)MemoryContextSwitchTo(oldContext);
    }
    /* pointer to RB  tree */
    RBTree *m_tree;
    /* num of elements */
    size_t m_size;
    /* begin iterator */
    iterator *m_begin_iter;
    /* end iterator */
    iterator *m_end_iter;
};
}

#endif /* INCLUDE_UTILS_PG_SET_H_ */
