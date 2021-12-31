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
 * cache.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\cache.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef CACHE_H
#define CACHE_H

#include "utils/syscache.h"
#include "access/htup.h"

HeapTuple search_sys_cache_copy_ce_col_name(Oid relid, const char *attname);
bool search_sys_cache_exists_ce_col_name(Oid relid, const char *attname);

#endif /* CACHE_H */