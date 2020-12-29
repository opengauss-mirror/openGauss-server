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
 * guc_vars.h
 *
 * IDENTIFICATION
 *	  src\include\libpq\guc_vars.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include "parser/backslash_quotes.h"

/*
    GUC variables (Grand Unified Configuration variables)
    These variables are typically only maintained the server side but some variables the client driver must also keep
   track of. Typically used by the parser's lexer component
*/

typedef enum updateGucValues {
    GUC_NONE = 0x0,
    BACKSLASH_QUOTE = 0x1,
    CONFORMING = 0x2,
    ESCAPE_STRING = 0x4,
    SEARCH_PATH = 0x8,
    GUC_ROLE = 0x10
} updateGucValues;


inline updateGucValues operator | (updateGucValues lhs, updateGucValues rhs)
{
    using T = std::underlying_type<updateGucValues>::type;
    return static_cast<updateGucValues>(static_cast<T>(lhs) | static_cast<T>(rhs));
}

inline updateGucValues operator&(updateGucValues lhs, updateGucValues rhs)
{
    using T = std::underlying_type<updateGucValues>::type;
    return static_cast<updateGucValues>(static_cast<T>(lhs) & static_cast<T>(rhs));
}

inline updateGucValues &operator |= (updateGucValues &lhs, updateGucValues rhs)
{
    lhs = lhs | rhs;
    return lhs;
}

inline updateGucValues &operator &= (updateGucValues &lhs, updateGucValues rhs)
{
    lhs = lhs & rhs;
    return lhs;
}

inline updateGucValues &operator ^= (updateGucValues &lhs, updateGucValues rhs)
{
    lhs = updateGucValues((unsigned int)lhs ^ (unsigned int)rhs);
    return lhs;
}


typedef struct GucParams {
    GucParams()
        : backslash_quote(BACKSLASH_QUOTE_SAFE_ENCODING),
          standard_conforming_strings(true),
          escape_string_warning(true),
          searchpathStr("\"$user\",public") {};
    int backslash_quote;
    bool standard_conforming_strings;
    bool escape_string_warning;
    std::string searchpathStr;
    std::string role;
} GucParams;
