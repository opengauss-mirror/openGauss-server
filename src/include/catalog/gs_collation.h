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
 * gs_collation.h
 *
 * IDENTIFICATION
 *        src/include/catalog/gs_collation.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_COLLATION_H
#define GS_COLLATION_H
#include "postgres.h"
#include "mb/pg_wchar.h"

#define NEXT_WORD_POS(p, p_word_bytes) ((p) += (p_word_bytes))

extern int compare_tail_space(const unsigned char* str_value, int end_space_num, int res);
extern Datum hash_mb_bin_pad_space(const unsigned char *key, size_t len);

/* utf8 */
extern int matchtext_utf8mb4(unsigned char* t, int tlen, unsigned char* p, int plen);
extern int strnncoll_utf8mb4_general_pad_space(const unsigned char* arg1, size_t len1,
                                        const unsigned char* arg2, size_t len2);
extern Datum hash_utf8mb4_general_pad_space(const unsigned char *key, size_t len);
extern int strnncoll_binary(const unsigned char* arg1, size_t len1,
                     const unsigned char* arg2, size_t len2);

/* gbk */
extern int strnncoll_mb_bin_pad_space(const unsigned char* arg1, size_t len1,
                                const unsigned char* arg2, size_t len2);
extern int strnncoll_gbk_chinese_ci_pad_space(const unsigned char* arg1, size_t len1,
                                       const unsigned char* arg2, size_t len2);
extern Datum hash_gbk_chinese_pad_space(const unsigned char *key, size_t len);
extern int matchtext_gbk_chinese(unsigned char* s, int slen, unsigned char* p, int plen);

/* gb18030 */
extern int strnncoll_gb18030_chinese_ci_pad_space(const unsigned char* arg1, size_t len1,
                                           const unsigned char* arg2, size_t len2);
extern Datum hash_gb18030_chinese_pad_space(const unsigned char *key, size_t len);
extern int matchtext_gb18030(unsigned char* t, int tlen, unsigned char* p, int plen);

/* common */
extern int varstr_cmp_by_builtin_collations(const char* arg1, int len1,const char* arg2, int len2, Oid collid);
extern Datum hash_text_by_builtin_collations(const unsigned char *key, int len, Oid collid);
extern int match_text_by_builtin_collations(unsigned char* t, int tlen, unsigned char* p, int plen, Oid collation);
extern void check_binary_collation(Oid collation, Oid type_oid);
extern bool is_support_b_format_collation(Oid collation);
extern bool is_b_format_collation(Oid collation);
extern int matchtext_general(unsigned char* s, int slen, unsigned char* p, int plen);

#endif   /* GS_COLLATION_H */