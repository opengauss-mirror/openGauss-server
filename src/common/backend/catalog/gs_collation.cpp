/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 *
 * gs_collation.cpp
 *        common interface for collations in b format
 *
 * IDENTIFICATION
 *        src/common/backend/catalog/gs_collation.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_collation.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "parser/parse_type.h"
#include "access/hash.h"
#include "utils/lsyscache.h"
#include "catalog/gs_collation.h"

bool is_b_format_collation(Oid collation)
{
    if (COLLATION_IN_B_FORMAT(collation)) {
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("this collation is not currently supported ")));
#endif
        return true;
    }
    return false;
}

bool is_support_b_format_collation(Oid collation)
{
    if (is_b_format_collation(collation) && !DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("this collation only support in B-format database")));
    }
    return true;
}

/* binary collation only support binary string types, such as : blob. */
void check_binary_collation(Oid collation, Oid type_oid)
{
    if (collation == BINARY_COLLATION_OID && !DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("this collation only support in B-format database")));
    }

    if (IsBinaryType(type_oid) && collation != BINARY_COLLATION_OID) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("binary collation only support binary type in B format")));
    }
}

Datum hash_text_by_builtin_collations(const unsigned char *key, int len, Oid collid)
{
    Datum result = 0;
    switch (collid) {
        case UTF8MB4_GENERAL_CI_COLLATION_OID:
        case UTF8MB4_UNICODE_CI_COLLATION_OID:
        case UTF8_GENERAL_CI_COLLATION_OID:
        case UTF8_UNICODE_CI_COLLATION_OID:
            result = hash_utf8mb4_general_pad_space((unsigned char*)key, len);
            break;
        case GBK_CHINESE_CI_COLLATION_OID:
            result = hash_gbk_chinese_pad_space((unsigned char*)key, len);
            break;
        case GB18030_CHINESE_CI_COLLATION_OID:
            result = hash_gb18030_chinese_pad_space((unsigned char*)key, len);
            break;
        case UTF8MB4_BIN_COLLATION_OID:
        case UTF8_BIN_COLLATION_OID:
        case GBK_BIN_COLLATION_OID:
        case GB18030_BIN_COLLATION_OID:
            result = hash_mb_bin_pad_space((unsigned char*)key, len);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("this collation is not currently supported ")));
            break;
    }

    return result;
}

int varstr_cmp_by_builtin_collations(const char* arg1, int len1, const char* arg2, int len2, Oid collid)
{
    int result = 0;
    switch (collid) {
        case UTF8MB4_GENERAL_CI_COLLATION_OID:
        case UTF8MB4_UNICODE_CI_COLLATION_OID:
        case UTF8_GENERAL_CI_COLLATION_OID:
        case UTF8_UNICODE_CI_COLLATION_OID:
            result = strnncoll_utf8mb4_general_pad_space((unsigned char*)arg1, len1, (unsigned char*)arg2, len2);
            break;
        case BINARY_COLLATION_OID:
            result = strnncoll_binary((unsigned char*)arg1, len1, (unsigned char*)arg2, len2);
            break;
        case GBK_CHINESE_CI_COLLATION_OID:
            result = strnncoll_gbk_chinese_ci_pad_space((unsigned char*)arg1, len1, (unsigned char*)arg2, len2);
            break;
        case GB18030_CHINESE_CI_COLLATION_OID:
            result = strnncoll_gb18030_chinese_ci_pad_space((unsigned char*)arg1, len1, (unsigned char*)arg2, len2);
            break;
        case UTF8MB4_BIN_COLLATION_OID:
        case UTF8_BIN_COLLATION_OID:
        case GBK_BIN_COLLATION_OID:
        case GB18030_BIN_COLLATION_OID:
            result = strnncoll_mb_bin_pad_space((unsigned char*)arg1, len1, (unsigned char*)arg2, len2);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("this collation is not currently supported ")));
            break;
    }

    return result;
}

int match_text_by_builtin_collations(unsigned char* t, int tlen, unsigned char* p, int plen, Oid collation)
{
    int result = 0;
    switch (collation) {
        case UTF8MB4_GENERAL_CI_COLLATION_OID:
        case UTF8MB4_UNICODE_CI_COLLATION_OID:
        case UTF8_GENERAL_CI_COLLATION_OID:
        case UTF8_UNICODE_CI_COLLATION_OID:
            result = matchtext_utf8mb4((unsigned char*)t, tlen, (unsigned char*)p, plen);
            break;
        case BINARY_COLLATION_OID:
            result = matchtext_general((unsigned char*)t, tlen, (unsigned char*)p, plen);
            break;
        case GBK_CHINESE_CI_COLLATION_OID:
            result = matchtext_gbk_chinese((unsigned char*)t, tlen, (unsigned char*)p, plen);
            break;
        case GB18030_CHINESE_CI_COLLATION_OID:
            result = matchtext_gb18030((unsigned char*)t, tlen, (unsigned char*)p, plen);
            break;
        case UTF8MB4_BIN_COLLATION_OID:
        case UTF8_BIN_COLLATION_OID:
        case GBK_BIN_COLLATION_OID:
        case GB18030_BIN_COLLATION_OID:
            result = matchtext_general((unsigned char*)t, tlen, (unsigned char*)p, plen);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Un-support feature"),
                    errdetail("this collation is not currently supported ")));
            break;
    }
    return result;
}

int compare_tail_space(const unsigned char* str_value, int end_space_num, int res)
{
    while(end_space_num > 0){
        if (*str_value++ != ' ') {
            return (str_value[-1] < ' ') ? -res : res;
        }
        end_space_num--;
    }

    return 0;
}

/* bin collation for multibyte charset */
int strnncoll_mb_bin_pad_space(const unsigned char* arg1, size_t len1,
                                const unsigned char* arg2, size_t len2)
{
    int min_length = Min(len1, len2);

    while (min_length > 0){
        if (*arg1++ != *arg2++) {
            return ((int)arg1[-1] - (int)arg2[-1]);
        }
        min_length--;
    }

    int res = len1 > len2 ? compare_tail_space(arg1, len1 - len2, 1) :
          compare_tail_space(arg2, len2 - len1, -1);

    return res;
}

Datum hash_mb_bin_pad_space(const unsigned char *key, size_t len)
{
    return hash_any(key, bpchartruelen((char*)key, len));
}

int matchtext_general(unsigned char* s, int slen, unsigned char* p, int plen)
{
    return GenericMatchText((char*)s, slen, (char*)p, plen);
}