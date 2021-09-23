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
 * cache_id.h
 * 	manages a circular array just like process/thread id array.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_cache/cache_id.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHE_ID_H
#define CACHE_ID_H

#include <stdlib.h>

const size_t INVALID_CACHED_ID = 0;

extern void set_bitmap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index);
extern void unset_bitmap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index);
extern bool get_bitmap(const unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index);
extern bool try_set_bitamap(unsigned char *bitmap, size_t map_unit, size_t map_size, size_t index);

extern size_t get_client_cache_id();
extern void put_client_cache_id_back(size_t cache_id);

#endif