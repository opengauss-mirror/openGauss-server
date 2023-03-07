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
 * gs_utf8_collation.h
 *
 * IDENTIFICATION
 *        src/include/catalog/gs_utf8_collation.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_UTF8_COLLATION_H
#define GS_UTF8_COLLATION_H

int varstr_cmp_by_builtin_collations(char* arg1, int len1, char* arg2, int len2, Oid collid);
Datum hash_text_by_builtin_colltions(const unsigned char *key, size_t len, Oid collid);
void check_binary_collation(Oid collation, Oid type_oid);
bool is_support_b_format_collation(Oid collation);
Oid binary_need_transform_typeid(Oid typeoid, Oid* collation);
int matchtext_utf8mb4(unsigned char* t, int tlen, unsigned char* p, int plen);
bool is_b_format_collation(Oid collation);

#define IS_UTF8_GENERAL_COLLATION(colloid) \
    ((colloid == UTF8MB4_GENERAL_CI_COLLATION_OID) || \
     (colloid == UTF8MB4_UNICODE_CI_COLLATION_OID) || \
     (colloid == UTF8_GENERAL_CI_COLLATION_OID) || \
     (colloid == UTF8_UNICODE_CI_COLLATION_OID))
#endif   /* GS_UTF8_COLLATION_H */