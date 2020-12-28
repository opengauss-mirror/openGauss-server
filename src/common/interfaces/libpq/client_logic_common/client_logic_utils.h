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
 * client_logic_utils.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\client_logic_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_UTILS_H
#define CLIENT_LOGIC_UTILS_H

#include <cctype>
#include <algorithm>
#include "client_logic_cache/dataTypes.def"
struct RangeVar;
typedef unsigned int Oid;
bool concat_col_fqdn(const char *catalogname, const char *schemaname, const char *relname, const char *colname,
    char *fqdn);
bool concat_table_fqdn(const char *catalogname, const char *schemaname, const char *relname, char *fqdn);
bool is_clientlogic_datatype(const Oid o);
template<typename T> inline bool is_const(T &x)
{
    return false;
}

template<typename T> inline bool is_const(T const & x)
{
    return true;
}
char *del_blanks(char *str, const int str_len);

#endif
