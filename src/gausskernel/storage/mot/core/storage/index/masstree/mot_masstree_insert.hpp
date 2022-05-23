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
 * mot_masstree_insert.hpp
 *    Masstree index insert interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_insert.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_INSERT_HPP
#define MOT_MASSTREE_INSERT_HPP
#include "masstree.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_split.hh"

namespace Masstree {
template <typename P>
void* basic_table<P>::insert(MOT::Key const* const& key, void* const& entry, bool& result, const uint32_t& pid)
{

    MOT_LOG_DEBUG("table: %s", name_.c_str());
    // This should be optimized at compile time by bitshifts and using ctz
    cursor_type lp(*this, key->GetKeyBuf(), ALIGN8(key->GetKeyLength()));
    void* value_to_return = nullptr;

    /*  The handler represents a thread and its main purpose is to work lockless
       with the internal structures. As we are not following this principle (pid
       is not stands for the thread id), we must validate that each thread in
       masstree is using different pid.

        How find_insert works:
        Look for the key. If key was found, nothing will be changed in the tree
       and lp->n_ will point to the a leaf which contains the key. If key was not
       found, find_insert will do the following:
          1. Lock the node that should hold the new key
          2. Mark the node's version as inserting
          3. Mark the node's state as insert
          4. Update the tree's structure to support the new key insert (splits,
       new layer ...)
          5. Update the the key slice, keylen, key suffix and key's value in the
       leaf
          6. Add the key's location in permutation's back (key is not visible for
       readers yet) as key's location is not part of the permutation yet, the key
       is not reachable (aka not present). In addition, the leaf is still locked.
        Unlocking the node and enter the key into the permutation will be done
       later in finish_insert (done in lp.finish function). */

    bool found = false;
    if (!lp.find_insert(*mtSessionThreadInfo, found)) {
        // Failed to insert key due to memory allocation failure.
        MOT_ASSERT(!mtSessionThreadInfo->non_disruptive_error());
        MOT_ASSERT(found == false);
        lp.finish(0, *mtSessionThreadInfo);
        result = false;
        return nullptr;
    }

    MOT_ASSERT(mtSessionThreadInfo->non_disruptive_error());

    // If the key is new (not previously existing) then we record the entry under
    // that key
    if (!found) {
#if ISOLATION_LEVEL == SERIALIZABLE
        handler->observe_phantoms(lp.node());
#endif
        lp.value() = reinterpret_cast<value_type>(entry);
    } else {
        // If the insertion was successful, we return nullptr (the sentinel was
        // used). Otherwise, we return the sentinel we found. This is done here as
        // the node is still locked
        value_to_return = reinterpret_cast<void*>(lp.value());
    }

    // Updates the leaf's permutation and unlock it, which makes the key visible
    // and accessible
    lp.finish(1, *mtSessionThreadInfo);

    // if we didn't insert the sentinel (!found == false) then we signal this so
    // that the sentinel can be deallocated outside
    result = !found;

    return value_to_return;
}
}  // namespace Masstree
#endif  // MOT_MASSTREE_INSERT_HPP
