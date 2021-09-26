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
 * cache_id.cpp
 * 	manages a circular array just like process/thread id array.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_cache/cache_id.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cache_id.h"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

const size_t CACHED_ID_MAP_SIZE = 1024; /* bits = sizeof(char) * 256 bytes */
const size_t CHAR_BIT_SIZE = 8;

/* 
 * when traversing g_client_cache_id_map array
 * instead of starting at index 0, we start at any location(=cache_id_start), and we treat the array as a circular array
 * so, all indexs in the map can be recycled.
 */
static unsigned char g_client_cache_id_map[CACHED_ID_MAP_SIZE / CHAR_BIT_SIZE] = {0};
static size_t g_cache_id_start = 1; /* 0 is left as INVALID_CACHED_ID */
static pthread_mutex_t g_cached_id_map_mutex = {0};
static bool g_has_init_mutex = false;

/* 
 * @breif : set bitmap[index] to '1'
 * @param in - bitmap:
 *     if you want a map with 'unsigned int' as the unit
 *     the bitmap should be 'unsgined int *' type, and the map_size should be 32 bits
 *     now, we just keep the param 'map_unit' for future expansion
 */
void set_bitmap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index)
{	
	if (index >= map_size || map_unit == 0) {
		return;
	}
	
	size_t cnt = index / map_unit;
	size_t offset = index % map_unit;
	
	bitmap[cnt] ^= (0x1 << offset);
}

/* 
 * @breif : set bitmap[index] to '0'
 */
void unset_bitmap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index)
{
	if (index >= map_size || map_unit == 0) {
		return;
	}
	
	size_t cnt = index / map_unit;
	size_t offset = index % map_unit;
	
	bitmap[cnt] &= (0x1 << offset) ^ 0xff;
}

/* 
 * @breif : is bitmap[index] equals '1'
 * @return : true : if bitmap[index] equals '1'
 *           false : if bitmap[index] equals '0'
 */
bool get_bitmap(const unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index)
{
	if (index >= map_size || map_unit == 0) {
		return false;
	}
	
	size_t cnt = index / map_unit;
	size_t offset = index % map_unit;
	
	return ((bitmap[cnt] & (0x1 << offset)) > 0) ? true : false; 
}

/* 
 * @breif : if bitmap[index] equals '0':
 *             set it to '1', return true
 *          else:
 *             just return false 
 */
bool try_set_bitamap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index)
{
    if (!get_bitmap(bitmap, map_unit, map_size, index)) {
        set_bitmap(bitmap, map_unit, map_size, index);
        return true;
    }

    return false;
}

/* 
 * @breif : each client has many connections.
 *     in some cases, a cache needs to be created separately for each connection
 *     however, we cannot directly refactor the structure that controls the connection everytime
 *     therefore, we can use cached_id to establish a contact between the cache and the structure
 *     also, we want our client_id to be recycled
 */
size_t get_client_cache_id()
{
    size_t cache_id = INVALID_CACHED_ID;
    
    if (!g_has_init_mutex) {
    	if (pthread_mutex_init(&g_cached_id_map_mutex, NULL) != 0) {
    		return INVALID_CACHED_ID;
		}
		g_has_init_mutex = true;
	}

	if (pthread_mutex_lock(&g_cached_id_map_mutex) != 0) {
        return INVALID_CACHED_ID;
    }

	/* there are 2 'for loops' here, because we're using the g_client_cache_id_map as a ring array. */
    for (size_t i = g_cache_id_start; i < CACHED_ID_MAP_SIZE; i++) {
		if (try_set_bitamap(g_client_cache_id_map, CHAR_BIT_SIZE, CACHED_ID_MAP_SIZE, i)) {
			cache_id = i;
			g_cache_id_start = i;
			break;
		}
    }
    
    if (cache_id == INVALID_CACHED_ID) {
    	for (size_t i = 1; i < g_cache_id_start; i++) {
    		if (try_set_bitamap(g_client_cache_id_map, CHAR_BIT_SIZE, CACHED_ID_MAP_SIZE, i)) {
				cache_id = i;
				g_cache_id_start = i;
				break;
			}
		}
	}

    pthread_mutex_unlock(&g_cached_id_map_mutex);
    
    return cache_id;
}

/*
 * breif : ensure that client_id can be recycled, and we should recycle them. 
 */
void put_client_cache_id_back(size_t cache_id)
{
    if (!g_has_init_mutex) {
    	if (pthread_mutex_init(&g_cached_id_map_mutex, NULL) != 0) {
    		return;
		}
		g_has_init_mutex = true;
	}
    
    if (pthread_mutex_lock(&g_cached_id_map_mutex) == 0) {
        unset_bitmap(g_client_cache_id_map, CHAR_BIT_SIZE, CACHED_ID_MAP_SIZE, cache_id);
        pthread_mutex_unlock(&g_cached_id_map_mutex);
    }
}
