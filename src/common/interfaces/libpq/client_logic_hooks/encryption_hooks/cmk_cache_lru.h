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
 * cmk_cache_lru.h
 *      The LRU cache of cmk plain, if we can find cmk plain from the cmk_cache_list,
 *      we will not read it from 'gs_ktool".
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\cmk_cache_lru.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMK_CACHE_LRU_H
#define CMK_CACHE_LRU_H

#define DEFAULT_CMK_CACHE_LEN 32

typedef struct CmkCacheNode {
    unsigned int cmk_id;
    unsigned char cmk_plain[DEFAULT_CMK_CACHE_LEN];
    struct CmkCacheNode *next;
} CmkCacheNode;

typedef struct CmkCacheList {
    unsigned int cmk_node_cnt;
    CmkCacheNode *first_cmk_node;
} CmkCacheList;

/* LRU cache */
CmkCacheList *init_cmk_cache_list();
bool get_cmk_from_cache(CmkCacheList *cmk_cache_list, const unsigned int cmk_id, unsigned char *cmk_plain);
void push_cmk_to_cache(CmkCacheList *cmk_cache_list, const unsigned int cmk_id, const unsigned char *cmk_plian);
void free_cmk_cache_list(CmkCacheList *cmk_cahce_list);

#endif
