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
 * map Containers declaration
 *
 *
 * IDENTIFICATION
 * src/include/gs_policy/gs_map.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef INCLUDE_UTILS_GS_MAP_H_
#define INCLUDE_UTILS_GS_MAP_H_

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/rbtree.h"
#include "utils/palloc.h"
#include "gs_string.h"

namespace gs_stl {
#define PG_MAP_MAX_SIZE 1024
typedef int (*CompareKeyFunc)(const void *keyA, const void *keyB);

/*
 * gs_map implemention, there is constraint for Key that Key must supply operator < function as default
 * or defaultCompareKeyFunc<Key> should be put in
 */
template <class Key, class T, CompareKeyFunc CompareKeys = defaultCompareKeyFunc<Key>, int key_size = sizeof(Key),
    int t_size = sizeof(T), int max_elements = PG_MAP_MAX_SIZE>
class gs_map {
public:
    typedef Key key_type;
    typedef T mapped_type;

    /*
     * Note: do not transfer "const key_type &" as template type
     * if do so, the template instance will the T as const key_type & type so that binding to parameter
     * then it will take the wrong life cycle not expected.
     */
    typedef Pair<const key_type, mapped_type> value_type;

    /* *
     * Iterator can be used to access the sequence of elements in a range in both directions
     * (towards the end and towards the beginning).
     */
    struct iterator {
        iterator() : first(NULL), second(NULL), m_prev(NULL), m_next(NULL) {}

        iterator(const iterator &arg) : first(arg.first), second(arg.second), m_prev(arg.m_prev), m_next(arg.m_next) {}

        void inline reset(void)
        {
            first = NULL;
            second = NULL;
            m_prev = NULL;
            m_next = NULL;
        }

        const mapped_type *get_value() const
        {
            return second;
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

        /* adapte stl iterator interface */
        iterator &operator*() const
        {
            return *this;
        }

        const iterator *operator->() const
        {
            return this;
        }

        iterator &operator ++ ()
        {
            if (m_next != NULL) {
                first = m_next->first;
                second = m_next->second;
                m_prev = m_next->m_prev;
                m_next = m_next->m_next;
            }
            return *this;
        }

        key_type *first;
        mapped_type *second;
        iterator *m_prev;
        iterator *m_next;
    };

    /*
     * A pair, with its member pg_pair::first set to an iterator pointing to either the newly inserted element
     * or to the end of map, if an equivalent key in the map found. The pg_pair::second element in the pair
     * is set to true if a new element was inserted or false if an equivalent key already existed.
     */
    typedef Pair<iterator, bool> pg_pair;
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
        oldContext = MemoryContextSwitchTo(GetMapMemory());
        IteratorTreeNodeWrapper *res = (IteratorTreeNodeWrapper *)palloc(sizeof(IteratorTreeNodeWrapper));
        res->m_iter.reset();

        /*
         * put all the sub instance memory alloc/deallc into rb tree
         * upper container(set/map) do the wrapper job but not mix them together
         */
        (void)MemoryContextSwitchTo(oldContext);
        return (RBNode *)res;
    }

    /* Deallocator function for rbtree.c */
    static void deallocDataEntry(RBNode *x, void *arg)
    {
        if (x == NULL) {
            return;
        }
        IteratorTreeNodeWrapper *iter_wrapper = (IteratorTreeNodeWrapper *)x;
        if (std::is_trivial<key_type>::value == 0 && iter_wrapper->m_iter.first != NULL) {
            iter_wrapper->m_iter.first->~key_type();
        }
        if (std::is_trivial<mapped_type>::value == 0 && iter_wrapper->m_iter.second != NULL) {
            iter_wrapper->m_iter.second->~mapped_type();
        }
        if (iter_wrapper->m_iter.first != NULL) {
            pfree(iter_wrapper->m_iter.first);
            iter_wrapper->m_iter.first = NULL;
        }
        if (iter_wrapper->m_iter.second != NULL) {
            pfree(iter_wrapper->m_iter.second);
            iter_wrapper->m_iter.second = NULL;
        }
        pfree(iter_wrapper);
        iter_wrapper = NULL;
    }

    static void copyDataEntry(RBTree* rb, RBNode *dest, const RBNode *src)
    {
        /* We should use our defined function to copy data replace the default copy logic of the rb tree */
        IteratorTreeNodeWrapper *iter_dest = (IteratorTreeNodeWrapper *)dest;
        IteratorTreeNodeWrapper *iter_src = (IteratorTreeNodeWrapper *)src;
        if (iter_dest->m_iter.first != NULL) {
            if (std::is_trivial<key_type>::value == 0) {
                iter_dest->m_iter.first->~key_type();
            }
            pfree(iter_dest->m_iter.first);
        }
        if (iter_dest->m_iter.second != NULL) {
            if (std::is_trivial<mapped_type>::value == 0) {
                iter_dest->m_iter.second->~mapped_type();
            }
            pfree(iter_dest->m_iter.second);
        }
        errno_t rc = memcpy_s(dest + 1, rb->node_size - sizeof(RBNode), src + 1, rb->node_size - sizeof(RBNode));
        securec_check(rc, "\0", "\0");
        MemoryContext oldContext = MemoryContextSwitchTo(GetMapMemory());
        /* allocate for key & value */
        iter_dest->m_iter.first = (key_type *)palloc(key_size);
        iter_dest->m_iter.second = (mapped_type *)palloc(t_size);
        new (static_cast<void *>(iter_dest->m_iter.first)) key_type(*(iter_src->m_iter.first));
        new (static_cast<void *>(iter_dest->m_iter.second)) mapped_type(*(iter_src->m_iter.second));
        MemoryContextSwitchTo(oldContext);
    }

public:
    /* *
     * Constructs a map container object, initializing its contents.
     */
    void init()
    {
        MemoryContext oldContext = MemoryContextSwitchTo(GetMapMemory());

        m_end_iter = (iterator *)palloc(sizeof(iterator));
        m_end_iter->reset();
        m_begin_iter = m_end_iter;
        m_size = 0;

        m_tree = rb_create(sizeof(IteratorTreeNodeWrapper), compareDataEntry, combineDataEntry, allocDataEntry,
            deallocDataEntry, NULL);

        (void)MemoryContextSwitchTo(oldContext);
    }

    gs_map() : m_size(0)
    {
        /*
         * We must use the TopMostMemoryContext because the RB tree
         * not bound to a thread and can outlive any of the thread specific
         * contextes.
         */
        MemoryContext oldContext = MemoryContextSwitchTo(GetMapMemory());

        m_end_iter = (iterator *)palloc(sizeof(iterator));
        m_end_iter->reset();
        m_begin_iter = m_end_iter;

        m_tree = rb_create(sizeof(IteratorTreeNodeWrapper), compareDataEntry, combineDataEntry, allocDataEntry,
            deallocDataEntry, NULL, copyDataEntry);

        (void)MemoryContextSwitchTo(oldContext);
    }

    /*
     * copy construction for a map object, initializing its contents.
     */
    gs_map(const gs_map &arg) : m_tree(NULL), m_begin_iter(NULL), m_end_iter(NULL)
    {
        init();
        if (arg.empty())
            return;

        iterator it = arg.begin();
        iterator eit = arg.end();
        for (; it != eit; ++it) {
            insert(value_type(*(it.first), *(it.second)));
        }
    }

    gs_map(value_type *it, value_type *eit) : m_size(0)
    {
        init();
        for (; it != eit; it = it + 1) {
            insert(*it);
        }
    }

    gs_map &operator = (const gs_map &arg)
    {
        if (this == &arg) {
            return *this;
        }
        clear();
        if (arg.empty())
            return *this;
        iterator it = arg.begin();
        iterator eit = arg.end();
        for (; it != eit; ++it) {
            insert(value_type(*(it.first), *(it.second)));
        }
        return *this;
    }

    /* *
     * Destroys the container object. This destroys all container elements,
     * and deallocates all the storage capacity allocated by the map container using its allocator.
     */
    ~gs_map()
    {
        if (t_thrd.port_cxt.thread_is_exiting == true) {
            return;
        }

        iterator *i_itr = m_begin_iter;
        while (i_itr != m_end_iter) {
            /* this way we calculate the address of IteratorTreeNodeWrapper */
            IteratorTreeNodeWrapper *ptr = (IteratorTreeNodeWrapper *)((RBNode *)i_itr - 1);
            i_itr = i_itr->m_next;
            if (std::is_trivial<key_type>::value == 0) {
                ptr->m_iter.first->~key_type();
            }
            if (std::is_trivial<mapped_type>::value == 0) {
                ptr->m_iter.second->~mapped_type();
            }
            pfree(ptr->m_iter.first);
            pfree(ptr->m_iter.second);
            ptr->m_iter.first = NULL;
            ptr->m_iter.second = NULL;
            pfree(ptr);
            ptr = NULL;
        }
        pfree(m_tree);
        pfree(m_end_iter);
        m_tree = NULL;
        m_end_iter = NULL;
        m_size = 0;
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
     * @return Returns whether the map container is empty (i.e. whether its size is 0).
     */
    bool empty() const
    {
        return (m_size == 0);
    }

    /* *
     * Return container size.
     * @return Returns the number of elements in the map container.
     */
    size_t size() const
    {
        return m_size;
    }

    /* *
     * Extends the container by inserting new elements, effectively increasing
     * the container size by the number of elements inserted.
     * @return return a pair, with its member pair::first set to an iterator pointing to either
     * the newly inserted element or to end of map, if an equivalent key in the map.
     * The pair::second element in the pair is set to true if a new element was inserted
     * or false if an equivalent key already existed.
     */
    pg_pair insert(const value_type &val)
    {
        pg_pair res(*m_end_iter, false);
        bool isNew = false;
        Assert(m_size < max_elements);
        IteratorTreeNodeWrapper data;
        data.m_iter.first = (key_type *)&(val.first);
        data.m_iter.second = (mapped_type *)&(val.second);

        IteratorTreeNodeWrapper *it_wrapper =
            (IteratorTreeNodeWrapper *)rb_insert(m_tree, (const RBNode *)&data, &isNew);
        if (!isNew) {
            /*
             * if key have been existed in rbtree, the Iterator will return as result
             * there will be nothing memory operation here(not need release or alloc anything)
             */
            return pg_pair(it_wrapper->m_iter, false);
        }

        MemoryContext oldContext = MemoryContextSwitchTo(GetMapMemory());

        /* allocate for key & value */
        it_wrapper->m_iter.first = (key_type *)palloc(key_size);
        it_wrapper->m_iter.second = (mapped_type *)palloc(t_size);
        new (static_cast<void *>(it_wrapper->m_iter.first)) key_type(val.first);
        new (static_cast<void *>(it_wrapper->m_iter.second)) mapped_type(val.second);

        MemoryContextSwitchTo(oldContext);
        /* add this iterator to list of other iterators */
        if (m_size == 0) {
            it_wrapper->m_iter.m_next = m_end_iter;
            m_end_iter->m_prev = &(it_wrapper->m_iter);
            m_begin_iter = &(it_wrapper->m_iter);
        } else {
            it_wrapper->m_iter.m_next = m_begin_iter;
            m_begin_iter = &(it_wrapper->m_iter);
            it_wrapper->m_iter.m_next->m_prev = &(it_wrapper->m_iter);
        }
        m_size++;

        res.first = it_wrapper->m_iter;
        res.second = true;
        return res;
    }

    /* *
     * Get iterator to element.
     * Searches the container for an element with a key equivalent to k
     * and returns an iterator to it if found, otherwise it returns an iterator to map::end.
     * @return An iterator to the element, if an element with specified key is found, or map::end otherwise.
     */
    iterator find(const key_type &k) const
    {
        IteratorTreeNodeWrapper dummy;
        dummy.m_iter.first = (key_type *)&k;

        RBNode *res = rb_find(m_tree, (const RBNode *)&dummy);
        if (res == NULL) {
            return *m_end_iter;
        }

        return ((IteratorTreeNodeWrapper *)res)->m_iter;
    }

    /*
     * get operator[] value
     * stl only use insert directly as the method of insert will return value_type even if key exist
     */
    mapped_type &operator[](const key_type &k)
    {
        iterator it = find(k);
        if (it == *m_end_iter) {
            mapped_type t = mapped_type();
            value_type value(k, t);
            return *((((this->insert(value)).first)).second);
        } else {
            return *(it.second);
        }
    }

    void update_iterator()
    {
        if (m_tree != NULL && !empty()) {
            rb_begin_iterate(m_tree, DirectWalk); /* preorder traversal */
            RBNode *node = rb_iterate(m_tree);
            IteratorTreeNodeWrapper *itr = (IteratorTreeNodeWrapper *)node;
            m_begin_iter = &itr->m_iter;
            m_begin_iter->m_prev = NULL;
            while ((node = rb_iterate(m_tree)) != NULL) {
                IteratorTreeNodeWrapper *cur_itr = (IteratorTreeNodeWrapper *)node;
                itr->m_iter.m_next = &cur_itr->m_iter;
                cur_itr->m_iter.m_prev = &itr->m_iter;
                itr = cur_itr;
            }
            itr->m_iter.m_next = m_end_iter;
            m_end_iter->m_prev = &itr->m_iter;
        } else {
            m_begin_iter = m_end_iter;
        }
    }
    /* *
     * Erase element.
     * Removes from the map container a single element.
     * @return  the number of elements erased (1 or 0).
     */
    size_t erase(const key_type &k)
    {
        IteratorTreeNodeWrapper dummy;
        dummy.m_iter.first = (key_type *)&k;

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
    bool has_intersection(const gs_map &other)
    {
        if (this == &other) {
            return true;
        }
        /* no intersection with empty containers */
        if (empty() || other.empty())
            return false;

        iterator *i_itr = m_begin_iter;
        while (i_itr != m_end_iter) {
            if (other.find(*i_itr->first) != other.end())
                return true;
            i_itr = i_itr->m_next;
        }
        return false;
    }

private:
    /*
     * Note all the data memory locate in RBTree, m_begin_iter
     */
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

#endif /* INCLUDE_UTILS_PG_MAP_H_ */
