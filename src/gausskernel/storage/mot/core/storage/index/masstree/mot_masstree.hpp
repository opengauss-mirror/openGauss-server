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
 * mot_masstree.hpp
 *    Basic Masstree index header file.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_HPP
#define MOT_MASSTREE_HPP

#include "masstree.hh"

namespace Masstree {
template <typename P>
bool basic_table<P>::init(const uint16_t keyLength, const std::string& name, destroy_value_cb_func destroyValue_CB)
{
    keyLength_ = keyLength;
    name_ = name;
    destroyValue_CB_ = destroyValue_CB;
    initialize(*mtSessionThreadInfo);
    return true;
}

template <typename P>
int basic_table<P>::getMemtagMaxSize(enum memtag tag)
{
    int size = 0;
    switch (tag) {
        case memtag_masstree_leaf:
            return MAX_MEMTAG_MASSTREE_LEAF_ALLOCATION_SIZE;

        case memtag_masstree_internode:
            return MAX_MEMTAG_MASSTREE_INTERNODE_ALLOCATION_SIZE;

        case memtag_masstree_ksuffixes:
        case memtag_masstree_gc:  // Using ksuffixes slab for GC requests
            return MAX_MEMTAG_MASSTREE_KSUFFIXES_ALLOCATION_SIZE(P::leaf_width);

        default:
            return -1;
    }

    return -1;
}
}  // namespace Masstree
#endif  // MOT_MASSTREE_HPP
