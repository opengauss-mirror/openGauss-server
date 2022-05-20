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
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_remove.hpp
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
    node_base<P>** root_ref_;
    int len_;
    size_t size_;
    MOT::MasstreePrimaryIndex* index_;
    char s_[0];
    gc_layer_rcu_callback_ng(node_base<P>** root_ref, Str prefix, size_t size)
        : root_ref_(root_ref), len_(prefix.length()), size_(size), index_(mtSessionThreadInfo->get_working_index())
    {
        errno_t erc = memcpy_s(s_, len_, prefix.data(), len_);
        securec_check(erc, "\0", "\0");
    }
    size_t operator()(bool drop_index);
    void operator()(threadinfo& ti);
    size_t size() const
    {
        return size_;
    }

    static void make(node_base<P>** root_ref, Str prefix, threadinfo& ti);
};

template <typename P>
size_t gc_layer_rcu_callback_ng<P>::operator()(bool drop_index)
{
    // If drop_index == true, all index's pools are going to be cleaned, so we can skip gc_layer call (which might add
    // more elements into GC)
    if (drop_index == false) {
        // GC layer remove might delete elements from tree and might create new gc layer removal requests and add them to GC.
        // Index must be provided to allow access to the memory pools.
        mtSessionThreadInfo->set_working_index(index_);
        (*this)(*mtSessionThreadInfo);
        mtSessionThreadInfo->set_working_index(NULL);
    }

    return (*this).size();
}

template <typename P>
void gc_layer_rcu_callback_ng<P>::operator()(threadinfo& ti)
{
    masstree_invariant(root_ref_);

    tcursor<P> node_cursor(root_ref_, s_, len_);
    bool do_remove = node_cursor.gc_layer(ti);
    if (!do_remove || !node_cursor.finish_remove(ti)) {
        node_cursor.n_->unlock();
    }
    ti.add_nodes_to_gc();
}

template <typename P>
void gc_layer_rcu_callback_ng<P>::make(node_base<P>** root_ref, Str prefix, threadinfo& ti)
{
    size_t sz = prefix.len + sizeof(gc_layer_rcu_callback_ng<P>);
    // As we are using slab allocator for allocation, sz is will be updated by ti.allocate with the real allocation
    // size. We need this size for GC deallocation size report
    void* data = ti.allocate(sz, memtag_masstree_gc, &sz /* IN/OUT PARAM */);
    if (!data) {
        // If allocation fails, gc layer removal command will not be added to GC and this layer wont be removed.
        // We might deal with this issue in the future by replacing the current mechanism with one of the following options:
        //    1. Use thread local GC layer removal object (per threadinfo) and keep list of key suffixes to clean (also in threadinfo)
        //    2. Move this feature to VACUUM process: Create special iterator that adds GC Layer callbacks when it finds empty layers
        ti.set_last_error(MT_MERR_GC_LAYER_REMOVAL_MAKE);
        return;
    }

    gc_layer_rcu_callback_ng<P>* cb = new (data) gc_layer_rcu_callback_ng<P>(root_ref, prefix, sz);
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
