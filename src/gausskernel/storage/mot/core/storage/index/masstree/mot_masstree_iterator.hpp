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
 * mot_masstree_iterator.hpp
 *    Iterator implementation for Masstree index.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_iterator.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_ITERATOR_HPP
#define MOT_MASSTREE_ITERATOR_HPP

#include "masstree.hh"
#include "masstree_scan.hh"
#include "key.h"

namespace Masstree {
using namespace MOT;

template <typename P>
void basic_table<P>::iteratorScan(const char* keybuf, uint32_t keylen, const bool& matchKey, void* const& it,
    const bool& forwardDirection, bool& result, const uint32_t& pid)
{
    ForwardIterator* fit = nullptr;
    ReverseIterator* rit = nullptr;

    if (forwardDirection) {
        fit = reinterpret_cast<ForwardIterator*>(it);
        fit->Init(this, keybuf, keylen, matchKey, mtSessionThreadInfo);
        result = fit->Begin();
    } else {
        rit = reinterpret_cast<ReverseIterator*>(it);
        rit->Init(this, keybuf, keylen, matchKey, mtSessionThreadInfo);
        result = rit->Begin();
    }
}

/**
 * @class MasstreeIterator
 * @brief Iterator implementation for Masstree index.
 *
 * @tparam CONST_ITERATOR defines if this is a constant iterator or not
 * @tparam FORWARD defines the iterator's direction (forward or reverse)
 * @tparam P default parameters\types for Masstree.
 */
template <bool CONST_ITERATOR, bool FORWARD, typename P>
class alignas(64) MasstreeIterator final : public IteratorCRTP<MasstreeIterator<CONST_ITERATOR, FORWARD, P>> {
private:
    friend class basic_table<P>;

    friend class MasstreeIterator<true, true, P>;
    friend class MasstreeIterator<true, false, P>;

    typedef std::forward_iterator_tag iterator_category;
    typedef std::ptrdiff_t difference_type;
    typedef uint64_t size_type;
    typedef typename basic_table<P>::node_type::leaf_type::leafvalue_type leafvalue_type;
    typedef typename basic_table<P>::value_type value_type;
    typedef typename std::conditional<CONST_ITERATOR, value_type const*, value_type*>::type pointer;
    typedef typename std::conditional<CONST_ITERATOR, const value_type&, value_type&>::type reference;
    typedef typename std::conditional<FORWARD, forward_scan_helper, reverse_scan_helper>::type helper_type;
    typedef scanstackelt<P> mystack_type;
    typedef basic_table<P> table_type;
    typedef threadinfo threadinfo_type;
    typedef typename basic_table<P>::node_type::key_type key_type;

    /** @var Iterator's scan stack. */
    mystack_type m_stack;

    /** @var Leaf value which the iterator is currently pointing at. */
    leafvalue_type m_entry;

    /** @var Helper structure which implements functionality for tree's traversing (forward or reverse). */
    helper_type m_helper;

    /** @var Search key (in Masstree's key format). */
    key_type m_key;

    /** @var Thread info pointer. */
    threadinfo_type* m_ti;

    /** @var Search key (in MOT's key format) pointer. */
    MOT::MaxKey* m_searchKey = nullptr;

    /** @var Current iterator scan state. */
    uint32_t m_state;

    /** @var True when scan ended (e.g. No more keys to return). */
    bool m_done;

    /** @var True if first key search (m_stack.find_initial(...)) was already called. */
    bool m_foundInitial;

    /** @var True if caller expect to receive the key itself in the iterator's returned range (if exists). */
    bool m_matchKey;

    /** @var Search key (in MOT's key format) instance. */
    MOT::MaxKey m_motKey;

    /**
     * @brief Initialize iterator's members.
     * @param table A Masstree table pointer which the iterator should scan.
     * @param key Search key.
     * @param keyLength Search key's length.
     * @param matchKey If search key exists, does the iterator returns its value or not.
     * @param ti Thread info pointer.
     */
    void Init(table_type* const& table, char const* const& key, const uint32_t& keyLength, const bool& matchKey,
        threadinfo_type* const& ti)
    {
        errno_t erc;
        m_searchKey = &m_motKey;
        m_ti = ti;
        m_stack.root_ = table->root_;
        m_entry = leafvalue_type::make_empty();
        m_state = 0;
        m_done = false;
        m_foundInitial = false;
        m_matchKey = matchKey;
        if (key) {
            // the deep copy of the key is done here
            erc = memcpy_s(m_searchKey->GetKeyBuf(), m_searchKey->GetKeyLength(), key, keyLength);
            securec_check(erc, "\0", "\0");
        } else {
            // If key is NULL, set to minimal\maximum key (according to helper's type)
            if (FORWARD) {
                erc = memset_s(m_searchKey->GetKeyBuf(), m_searchKey->GetKeyLength(), 0, keyLength);
                securec_check(erc, "\0", "\0");
            } else {
                erc = memset_s(m_searchKey->GetKeyBuf(), m_searchKey->GetKeyLength(), 0xFF, keyLength);
                securec_check(erc, "\0", "\0");
            }
        }
        // Sharing buffer with _searchKey
        m_key = key_type((const char*)m_searchKey->GetKeyBuf(), keyLength);
    }

    /**
     * @brief Begin iterator scan and set it on the first value that satisfies the criteria.
     * @return True is search key was found (regardless of m_matchKey value).
     */
    bool Begin(void)
    {
        bool found = false;
        while (true) {
            m_state = m_stack.find_initial(m_helper, m_key, m_matchKey, found, m_entry, *m_ti);
            if (m_state != mystack_type::scan_down) {
                break;
            }
            m_key.shift();
        }
        m_foundInitial = true;
        (void)Next();

        return found;
    }

    /**
     * @brief Find the next key\value that satisfies the criteria.
     * @detail If (m_state == mystack_type::scan_emit), stay on the current key\value.
     * @return Current iterator scan state.
     */
    uint32_t Next(void)
    {
        while (true) {
            switch (m_state) {
                case mystack_type::scan_emit:
                    m_searchKey->SetKeyLen(m_key.full_length());
                    goto done;

                case mystack_type::scan_find_next:
                find_next:
                    m_state = m_stack.find_next(m_helper, m_key, m_entry);
                    break;

                case mystack_type::scan_up:
                    do {
                        // the scan is finished when the stack is empty
                        if (m_stack.node_stack_.empty()) {
                            m_done = true;
                            goto done;
                        }
                        m_stack.n_ = static_cast<leaf<P>*>(m_stack.node_stack_.back());
                        m_stack.node_stack_.pop_back();
                        m_stack.root_ = m_stack.node_stack_.back();
                        m_stack.node_stack_.pop_back();
                        m_key.unshift();
                    } while (unlikely(m_key.empty()));

                    m_stack.v_ = m_helper.stable(m_stack.n_, m_key);
                    m_stack.perm_ = m_stack.n_->permutation();
                    m_stack.ki_ = m_helper.lower(m_key, &m_stack);
                    goto find_next;

                case mystack_type::scan_down:
                    m_helper.shift_clear(m_key);
                    goto retry;

                case mystack_type::scan_retry:
                retry:
                    m_state = m_stack.find_retry(m_helper, m_key, *m_ti);
                    break;
            }
        }
    done:
        return m_state;
    }

public:
    /**
     * @brief Default constructor.
     */
    MasstreeIterator(void) : m_ti(nullptr), m_state(0), m_done(true), m_foundInitial(false), m_matchKey(false)
    {}

    /**
     * @brief Default destructor.
     */
    ~MasstreeIterator(void)
    {
        m_ti = nullptr;
        m_searchKey = nullptr;
    }

    /**
     * @brief State that class is non-copy-able, non-assignable and non-movable
     */
    MasstreeIterator(const MasstreeIterator& rhs) = delete;
    MasstreeIterator(MasstreeIterator&& rhs) = delete;
    MasstreeIterator& operator=(const MasstreeIterator& rhs) = delete;
    MasstreeIterator& operator=(MasstreeIterator&& rhs) = delete;

    /** @brief Increment operator.
     *  @detail Search for the next key\value after the current one and point at it.
     *  @return Pointer to this
     */
    MasstreeIterator* operator++(void)
    {
        if (m_foundInitial) {
            m_stack.ki_ = m_helper.next(m_stack.ki_);
            m_state = m_stack.find_next(m_helper, m_key, m_entry);
            (void)Next();
        } else {
            (void)Begin();
        }
        return this;
    }
    /** @brief Dereference operator
     *  @return The current value the iterator is currently points at.
     */
    void* operator*(void)
    {
        return reinterpret_cast<void*>(m_entry.value());
    }

    /** @brief Getter for m_done.
     *  @return Iterator validity state.
     */
    bool Exhausted(void) const
    {
        return m_done;
    }

    /**
     *  @brief Mark iterator as invalid.
     */
    void Invalidate(void)
    {
        m_done = true;
    }

    /**
     *  @brief Destroy instance
     */
    void Destroy(void)
    {}

    /** @brief Getter for searchKey pointer
     *  @return SearchKey (if valid) or null if not.
     */
    MOT::Key* GetSearchKey(void)
    {
        if (!Exhausted())
            return m_searchKey;
        return nullptr;
    }
};
}  // namespace Masstree
#endif  // MOT_MASSTREE_ITERATOR_HPP
