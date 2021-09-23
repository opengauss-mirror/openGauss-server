/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 *   data_common.cpp
 *   class for TDE data structure
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/data_common.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "tde_key_management/data_common.h"
#include "utils/elog.h"

AdvStrList *malloc_advstr_list(void)
{
    AdvStrList *new_list = NULL;

    new_list = (AdvStrList *)palloc0(sizeof(AdvStrList));
    if (new_list == NULL) {
        return NULL;
    }
    new_list->node_cnt = 0;
    new_list->first_node = NULL;
    return new_list;
}

void tde_append_node(AdvStrList *list, const char *str_val)
{
    /* last_node -> next = new_node */
    AdvStrNode *last_node = NULL;
    AdvStrNode *new_node = NULL;
    char *new_node_str_val = NULL;
    errno_t rc = 0;

    new_node_str_val = (char *)palloc0(strlen(str_val) + 1);
    if (new_node_str_val == NULL) {
        return;
    }
    new_node = (AdvStrNode *)palloc0(sizeof(AdvStrNode));
    if (new_node == NULL) {
        pfree_ext(new_node_str_val);
        return;
    }
    rc = strcpy_s(new_node_str_val, strlen(str_val) + 1, str_val);
    securec_check(rc, "\0", "\0");
    new_node->str_val = new_node_str_val;
    new_node->next = NULL;
    if (list->first_node == NULL) {
        list->first_node = new_node;
    } else {
        last_node = list->first_node;
        for (int i = 0; i < (list->node_cnt - 1); i++) {
            last_node = last_node->next;
        }
        last_node->next = new_node;
    }
    list->node_cnt++;
}

size_t tde_list_len(AdvStrList *list)
{
    return list->node_cnt;
}

AdvStrList *tde_split_node(const char *str, char split_char)
{
    AdvStrList *substr_list = NULL;
    char *cur_substr = NULL;
    size_t str_start = 0;
    errno_t rc = 0;
    substr_list = malloc_advstr_list();
    if ((substr_list == NULL) || (str == NULL)) {
        return NULL;
    }
    for (size_t i = 0; i < strlen(str); i++) {
        if (str[i] == split_char || i == strlen(str) - 1) {
            cur_substr = (char *) palloc0(i - str_start + 1);
            rc = strncpy_s(cur_substr, i - str_start + 1, str + str_start, i - str_start);
            securec_check(rc, "\0", "\0");
            cur_substr[i - str_start] = '\0';
            str_start = i + 1;
            tde_append_node(substr_list, cur_substr);
            pfree_ext(cur_substr);
        }
    }
    if (tde_list_len(substr_list) == 0) {
        free_advstr_list(substr_list);
        substr_list = NULL;
        return NULL;
    }
    return substr_list;
}

void free_advstr_list(AdvStrList *list)
{
    AdvStrNode *cur_node = NULL;
    AdvStrNode *to_free = NULL;

    if (list == NULL) {
        return;
    }
    cur_node = list->first_node;

    while (cur_node != NULL) {
        to_free = cur_node;
        cur_node = cur_node->next;
        pfree_ext(to_free->str_val);
        pfree_ext(to_free);
    }
    pfree_ext(list);
}

char *tde_get_val(AdvStrList *list, int list_pos)
{
    AdvStrNode *target_node = NULL;

    if (list_pos == -1) {
        list_pos = list->node_cnt - 1;
    }
    if (list_pos < 0 || list_pos >= list->node_cnt) {
        return NULL;
    }
    target_node = list->first_node;
    for (int i = 0; i < list_pos; i++) {
        target_node = target_node->next;
    }
    return target_node->str_val;
}

void free_advstr_list_with_skip(AdvStrList *list, int list_pos)
{
    AdvStrNode *cur_node = NULL;
    AdvStrNode *to_free = NULL;

    if (list_pos == -1) {
        list_pos = list->node_cnt - 1;
    }
    cur_node = list->first_node;
    for (int i = 0; i < (int)tde_list_len(list); i++) {
        to_free = cur_node;
        cur_node = cur_node->next;
        if (i != list_pos) {
            pfree_ext(to_free->str_val);
            pfree_ext(to_free);
        }
    }
    pfree_ext(list);
}

TDEData::TDEData()
{
    cmk_id = NULL;
    dek_cipher = NULL;
    dek_plaintext = NULL;
}

TDEData::~TDEData()
{
    errno_t rc = 0;
    if (dek_plaintext != NULL) {
        rc = memset_s(dek_plaintext, strlen(dek_plaintext), 0, strlen(dek_plaintext));
        securec_check(rc, "\0", "\0");
    }
    pfree_ext(cmk_id);
    pfree_ext(dek_cipher);
    pfree_ext(dek_plaintext);
}
