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
 * cache.cpp
 *
 * IDENTIFICATION
 *	  src\common\backend\client_logic\cache.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "client_logic/cache.h"
#include "pgxc/pgxc.h"

/*
 * search_sys_cache_copy_ce_col_name
 *
 * As above, an attisdropped-aware version of SearchSysCacheCopy.
 */
HeapTuple search_sys_cache_copy_ce_col_name(Oid relid, const char *attname)
{
    HeapTuple tuple = SearchSysCache2(CERELIDCOUMNNAME, ObjectIdGetDatum(relid), CStringGetDatum(attname));
    if (!HeapTupleIsValid(tuple)) {
        return tuple;
    }
    HeapTuple newtuple = heap_copytuple(tuple);
    ReleaseSysCache(tuple);
    return newtuple;
}

/*
 * search_sys_cache_exists_ce_col_name
 *
 * As above, an attisdropped-aware version of SearchSysCacheExists.
 */
bool search_sys_cache_exists_ce_col_name(Oid relid, const char *attname)
{
    HeapTuple tuple = SearchSysCache2(CERELIDCOUMNNAME, ObjectIdGetDatum(relid), CStringGetDatum(attname));
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    ReleaseSysCache(tuple);
    return true;
}
