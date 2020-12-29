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
 * cmk_cache_lru.cpp
 *      The LRU cache of cmk plain, if we can find cmk plain from the cmk_cache_list,
 *      we will not read it from 'gs_ktool".
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\cmk_cache_lru.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmk_cache_lru.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const int MAX_CMK_CACHE_NODE_CNT = 100;

/* LRU cache */
CmkCacheList *init_cmk_cache_list()
{
    CmkCacheList *cmk_cache_list = NULL;
    cmk_cache_list = (CmkCacheList *)malloc(sizeof(CmkCacheList));
    if (cmk_cache_list == NULL) {
        printf("ERROR(CLIENT): Failed to malloc memory.\n");
        return NULL;
    }

    cmk_cache_list->cmk_node_cnt = 0;
    cmk_cache_list->first_cmk_node = NULL;
    return cmk_cache_list;
}

bool get_cmk_from_cache(CmkCacheList *cmk_cache_list, const unsigned int cmk_id, unsigned char *cmk_plain)
{
    CmkCacheNode *cur_cmk_node = NULL;
    CmkCacheNode *correct_cmk_node = NULL;

    if (cmk_cache_list->first_cmk_node == NULL) {
        return false;
    }

    /* the head node is not used */
    cur_cmk_node = cmk_cache_list->first_cmk_node;
    /* a. there are only 1 node */
    if (cur_cmk_node->next == NULL) {
        if (cur_cmk_node->cmk_id == cmk_id) {
            for (size_t i = 0; i < DEFAULT_CMK_CACHE_LEN; i++) {
                cmk_plain[i] = cur_cmk_node->cmk_plain[i];
            }
            return true;
        }
    } else { /* case b : there are 2 or more nodes */
        /* 
         * if the first node is the correct node, like this : 
         * to find node '2', and the cache list is : '2' -> '1' -> '3'
         */
        if (cur_cmk_node->cmk_id == cmk_id) { 
            for (size_t i = 0; i < DEFAULT_CMK_CACHE_LEN; i++) {
                cmk_plain[i] = cur_cmk_node->cmk_plain[i];
            }
            return true;
        } else {
            while (cur_cmk_node->next != NULL) {
                if (cur_cmk_node->next->cmk_id == cmk_id) {
                    correct_cmk_node = cur_cmk_node->next;
                    for (size_t i = 0; i < DEFAULT_CMK_CACHE_LEN; i++) {
                        cmk_plain[i] = correct_cmk_node->cmk_plain[i];
                    }

                    /* refresh cache list */
                    cur_cmk_node->next = correct_cmk_node->next;
                    correct_cmk_node->next = cmk_cache_list->first_cmk_node;
                    cmk_cache_list->first_cmk_node = correct_cmk_node;
                    return true;
                }
                
                cur_cmk_node = cur_cmk_node->next;
            }
        }
    }

    return false;
}

void push_cmk_to_cache(CmkCacheList *cmk_cache_list, const unsigned int cmk_id, const unsigned char *cmk_plian)
{
    CmkCacheNode *new_node = NULL;
    CmkCacheNode *last_node = NULL;

    new_node = (CmkCacheNode *)malloc(sizeof(CmkCacheNode));
    if (new_node == NULL) {
        printf("ERROR(CLIENT): Failed to malloc memory.\n");
        return;
    }
    new_node->cmk_id = cmk_id;
    for (size_t i = 0; i < DEFAULT_CMK_CACHE_LEN; i++) {
        new_node->cmk_plain[i] = cmk_plian[i];
    }
    
    if (cmk_cache_list->cmk_node_cnt < MAX_CMK_CACHE_NODE_CNT) {
        new_node->next = cmk_cache_list->first_cmk_node;
        cmk_cache_list->first_cmk_node = new_node;
        cmk_cache_list->cmk_node_cnt++;
    } else {
        last_node = cmk_cache_list->first_cmk_node;
        while (last_node->next->next != NULL) {
            last_node = last_node->next;
        }
        free(last_node->next);
        last_node->next = NULL;
    }
}

void free_cmk_cache_list(CmkCacheList *cmk_cahce_list)
{
    CmkCacheNode *to_free = NULL;
    CmkCacheNode *cur_node = NULL;

    if (cmk_cahce_list == NULL) {
        return;
    }

    cur_node = cmk_cahce_list->first_cmk_node;
    while (cur_node != NULL) {
        to_free = cur_node;
        cur_node = cur_node->next;
        free(to_free);
    }

    free(cmk_cahce_list);
}
