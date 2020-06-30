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
 * mot_masstree_config.hpp
 *    MOT configurations for Masstree index.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/storage/index/masstree/mot_masstree_config.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_CONFIG_HPP
#define MOT_MASSTREE_CONFIG_HPP

#define MOT_HAVE_CXX_TEMPLATE_ALIAS 1
#define MOT_HAVE_INT64_T_IS_LONG 1
#define MOT_HAVE_SIZE_T_IS_UNSIGNED_LONG 1
#define MOT_HAVE_STD_HASH 1
#define MOT_HAVE_STD_IS_TRIVIALLY_COPYABLE 1
#define MOT_HAVE_STD_IS_TRIVIALLY_DESTRUCTIBLE 1
#define MOT_HAVE_SUPERPAGE 1
#define MOT_HAVE_TYPE_TRAITS 1
#define MOT_HAVE_UNALIGNED_ACCESS 0
#define MOT_HAVE___BUILTIN_CLZ 1
#define MOT_HAVE___BUILTIN_CLZL 1
#define MOT_HAVE___BUILTIN_CLZLL 1
#define MOT_HAVE___BUILTIN_CTZ 1
#define MOT_HAVE___BUILTIN_CTZL 1
#define MOT_HAVE___BUILTIN_CTZLL 1
#define MOT_HAVE___HAS_TRIVIAL_COPY 1
#define MOT_HAVE___HAS_TRIVIAL_DESTRUCTOR 1
#define MOT_HAVE___SYNC_BOOL_COMPARE_AND_SWAP 1
#define MOT_HAVE___SYNC_BOOL_COMPARE_AND_SWAP_8 1
#define MOT_HAVE___SYNC_FETCH_AND_ADD 1
#define MOT_HAVE___SYNC_FETCH_AND_ADD_8 1
#define MOT_HAVE___SYNC_FETCH_AND_OR 1
#define MOT_HAVE___SYNC_FETCH_AND_OR_8 1
#define MOT_HAVE___SYNC_VAL_COMPARE_AND_SWAP 1
#define MOT_HAVE___SYNC_VAL_COMPARE_AND_SWAP_8 1

/* Maximum key length */
#define MOT_MASSTREE_MAXKEYLEN MAX_KEY_SIZE
#define MOT_SIZEOF_INT 4
#define MOT_SIZEOF_LONG 8
#define MOT_SIZEOF_LONG_LONG 8
#define MOT_SIZEOF_SHORT 2
#define MOT_WORDS_BIGENDIAN_SET 1

#define masstree_invariant(x, ...) \
    do {                           \
    } while (0)

#define masstree_precondition(x, ...) \
    do {                              \
    } while (0)

#ifndef invariant
#define invariant masstree_invariant
#endif
#ifndef precondition
#define precondition masstree_precondition
#endif

#endif  // MOT_MASSTREE_CONFIG_HPP
