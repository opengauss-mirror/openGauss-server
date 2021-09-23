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
 * mot_masstree_struct.hpp
 *    Masstree index print helper functions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/masstree/mot_masstree_struct.hpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MASSTREE_STRUCT_HPP
#define MOT_MASSTREE_STRUCT_HPP
#include "masstree.hh"

namespace Masstree {
class key_unparse_printable_string {
public:
    template <typename K>
    static int unparse_key(key<K> key, char* buf, int buflen)
    {
        String s = key.unparse().printable();
        int cplen = std::min(s.length(), buflen);
        errno_t erc = memcpy_s(buf, buflen, s.data(), cplen);
        securec_check(erc, "\0", "\0");
        return cplen;
    }
};

template <typename T>
class value_print {
public:
    static void print(
        T value, FILE* f, const char* prefix, int indent, Str key, kvtimestamp_t initial_timestamp, char* suffix)
    {
        value->print(f, prefix, indent, key, initial_timestamp, suffix);
    }
};

template <>
class value_print<unsigned char*> {
public:
    static void print(
        unsigned char* value, FILE* f, const char* prefix, int indent, Str key, kvtimestamp_t, char* suffix)
    {
        fprintf(f, "%s%*s%.*s = %p%s\n", prefix, indent, "", key.len, key.s, value, suffix);
    }
};

template <>
class value_print<uint64_t> {
public:
    static void print(uint64_t value, FILE* f, const char* prefix, int indent, Str key, kvtimestamp_t, char* suffix)
    {
        fprintf(f, "%s%*s%.*s = %" PRIu64 "%s\n", prefix, indent, "", key.len, key.s, value, suffix);
    }
};

class key_unparse_unsigned {
public:
    static int unparse_key(Masstree::key<long unsigned int> key, char* buf, int buflen)
    {
        errno_t erc = snprintf_s(buf, buflen, buflen - 1, "%" PRIu64, key.ikey());
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
};

typedef key_unparse_unsigned key_unparse_type;

template <typename P>
void leaf<P>::print(FILE* f, const char* prefix, int depth, int kdepth) const
{
    return;
}

template <typename P>
void internode<P>::print(FILE* f, const char* prefix, int depth, int kdepth) const
{
    return;
}
}  // namespace Masstree

#endif  // MOT_MASSTREE_STRUCT_HPP
