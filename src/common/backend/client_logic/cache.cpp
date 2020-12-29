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
 * search_sys_cache_ce_col_name
 *
 * This routine is equivalent to SearchSysCache on the CERELIDCOUMNNAME cache,
 * except that it will return NULL if the found attribute is marked
 * attisdropped.  This is convenient for callers that want to act as
 * though dropped attributes don't exist.
 */

HeapTuple search_syscache_cek_name(const char *key_name, Oid namespace_id)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR)
        return NULL;
#endif
    HeapTuple tuple;

    tuple = SearchSysCache2(COLUMNSETTINGNAME, PointerGetDatum(key_name), ObjectIdGetDatum(namespace_id));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }

    return tuple;
}

HeapTuple search_syscache_cmk_name(const char *key_name, Oid namespace_id)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR)
        return NULL;
#endif
    HeapTuple tuple;

    tuple = SearchSysCache2(GLOBALSETTINGNAME, PointerGetDatum(key_name), ObjectIdGetDatum(namespace_id));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }

    return tuple;
}

HeapTuple search_sys_cache_ce_col_name(Oid relid, const char *attname)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR)
        return NULL;
#endif
    HeapTuple tuple;

    tuple = SearchSysCache2(CERELIDCOUMNNAME, ObjectIdGetDatum(relid), CStringGetDatum(attname));
    if (!HeapTupleIsValid(tuple)) {
        return NULL;
    }
    return tuple;
}

/*
 * search_sys_cache_copy_ce_col_name
 *
 * As above, an attisdropped-aware version of SearchSysCacheCopy.
 */
HeapTuple search_sys_cache_copy_ce_col_name(Oid relid, const char *attname)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR)
        return NULL;
#endif
    HeapTuple tuple, newtuple;

    tuple = search_sys_cache_ce_col_name(relid, attname);
    if (!HeapTupleIsValid(tuple)) {
        return tuple;
    }
    newtuple = heap_copytuple(tuple);
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
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR) {
        return false;
    }
#endif
    HeapTuple tuple;

    tuple = search_sys_cache_ce_col_name(relid, attname);
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    ReleaseSysCache(tuple);
    return true;
}
