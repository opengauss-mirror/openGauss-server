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
 * mot_masstree_get.hpp
 *    Masstree index find interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_get.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_GET_HPP
#define MOT_MASSTREE_GET_HPP

#include "btree_leaflink.hh"
#include "circular_int.hh"
#include "masstree.hh"
#include "masstree_get.hh"

namespace Masstree {
template <typename P>
void basic_table<P>::find(
    const uint8_t* key, const uint32_t key_len, void*& output, bool& found, const uint32_t& pid) const
{
    unlocked_cursor_type lp(*this, reinterpret_cast<const unsigned char*>(key), ALIGN8(key_len));

    found = lp.find_unlocked(*mtSessionThreadInfo);
    if (found) {
        output = reinterpret_cast<void*>(lp.value());
    }
}

}  // namespace Masstree
#endif  // MOT_MASSTREE_GET_HPP
