/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_catcache.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_catcache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_CATCACHE_H
#define KNL_CATCACHE_H
#include "utils/catcache.h"
#include "utils/fmgrtab.h"
#include "utils/atomic.h"

extern void CatCacheCopyKeys(TupleDesc tupdesc, int nkeys, const int *attnos, Datum *srckeys, Datum *dstkeys);
extern void CatCacheFreeKeys(TupleDesc tupdesc, int nkeys, const int *attnos, Datum *keys);
extern HeapTuple CreateHeapTuple4BuiltinFunc(const Builtin_func *func, TupleDesc desc);
extern HeapTuple SearchBuiltinProcCacheMiss(int cache_id, int nkeys, Datum* arguments);
extern HeapTuple SearchPgAttributeCacheMiss(int cache_id, TupleDesc cc_tupdesc, int nkeys, const Datum* arguments);
extern bool IndexScanOK(int cache_id);
extern bool CatalogCacheCompareTuple(
    const CCFastEqualFN *cc_fastequal, int nkeys, const Datum* cachekeys, const Datum* searchkeys);
extern uint32 CatalogCacheComputeTupleHashValue(
    int cc_id, int* cc_keyno, TupleDesc cc_tupdesc, CCHashFN *cc_hashfunc, Oid cc_reloid, int nkeys, HeapTuple tuple);
extern uint32 CatalogCacheComputeHashValue(CCHashFN *cc_hashfunc, int nkeys, Datum *arguments);
HeapTuple GetPgAttributeAttrTuple(TupleDesc tupleDesc, const Form_pg_attribute attr);
void GetCCHashEqFuncs(Oid keytype, CCHashFN *hashfunc, RegProcedure *eqfunc, CCFastEqualFN *fasteqfunc);
void SearchCatCacheCheck();
#endif