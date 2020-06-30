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
 * mot_masstree_remove.hpp
 *    Masstree index remove interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/storage/index/masstree/mot_masstree_remove.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_REMOVE_HPP
#define MOT_MASSTREE_REMOVE_HPP
#include "masstree.hh"
#include "masstree_get.hh"
#include "btree_leaflink.hh"
#include "circular_int.hh"
namespace Masstree {

template <typename P>
struct gc_layer_rcu_callback_ng : public P::threadinfo_type::mrcu_callback {
    typedef typename P::threadinfo_type threadinfo;
    node_base<P>* root_;
    int len_;
    size_t size_;
    MOT::MasstreePrimaryIndex* index_;
    char s_[0];
    gc_layer_rcu_callback_ng(node_base<P>* root, Str prefix, size_t size)
        : root_(root), len_(prefix.length()), size_(size), index_(mtSessionThreadInfo->get_working_index())
    {
        errno_t erc = memcpy_s(s_, size_, prefix.data(), len_);
        securec_check(erc, "\0", "\0");
    }
    size_t operator()(bool drop_index);
    void operator()(threadinfo& ti);
    size_t size() const
    {
        return size_;
    }

    static void make(node_base<P>* root, Str prefix, threadinfo& ti);
};

template <typename P>
size_t gc_layer_rcu_callback_ng<P>::operator()(bool drop_index)
{
    // If drop_index == true, all index's pools are going to be cleaned, so we can skip gc_layer call (which might add
    // more elements into GC)
    if (drop_index == false) {
        // GC layer remove might delete elements from tree and add them to the limbolist. Index must be provided to
        // allow access to the memory pools.
        mtSessionThreadInfo->set_working_index(index_);
        (*this)(*mtSessionThreadInfo);
        mtSessionThreadInfo->set_working_index(NULL);
    }

    return (*this).size();
}

template <typename P>
void gc_layer_rcu_callback_ng<P>::operator()(threadinfo& ti)
{
    // root_ node while creating gc_layer_rcu_callback_ng might not be the current root. Find updated tree's root.
    while (!root_->is_root()) {
        root_ = root_->maybe_parent();
    }

    // If root was already deleted, do nothing.
    if (root_->deleted()) {
        return;
    }

    tcursor<P> node_cursor(root_, s_, len_);
    if (!node_cursor.gc_layer(ti) || !node_cursor.finish_remove(ti)) {
        node_cursor.n_->unlock();
    }
}

template <typename P>
void gc_layer_rcu_callback_ng<P>::make(node_base<P>* root, Str prefix, threadinfo& ti)
{
    size_t sz = prefix.len + sizeof(gc_layer_rcu_callback_ng<P>);
    // As we are using slab allocator to allocate the memory, sz is will updated in ti.allocate with the real allocated
    // size
    void* data = ti.allocate(sz, memtag_masstree_gc, &sz /*OUT PARAM*/);
    gc_layer_rcu_callback_ng<P>* cb = new (data) gc_layer_rcu_callback_ng<P>(root, prefix, sz);
    ti.rcu_register(cb, sz);
}

template <typename P>
void* basic_table<P>::remove(uint8_t const* const& key, uint32_t length, bool& result, const uint32_t& pid)
{
    cursor_type lp(*this, reinterpret_cast<const unsigned char*>(key), ALIGN8(length));

    bool found = lp.find_locked(*mtSessionThreadInfo);
    if (!(found)) {
        lp.finish(0, *mtSessionThreadInfo);
        result = found;
        return nullptr;
    }
    void* value = found ? reinterpret_cast<void*>(lp.value()) : nullptr;

    lp.finish(-1, *mtSessionThreadInfo);

    result = found;
    return value;
}

}  // namespace Masstree
#endif  // MOT_MASSTREE_REMOVE_HPP
