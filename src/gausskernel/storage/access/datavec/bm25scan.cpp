/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * bm25scan.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25scan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "access/xlog.h"
#include "access/sdir.h"
#include "zlib.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/index.h"
#include "access/tableam.h"
#include "access/datavec/bm25.h"

IndexScanDesc bm25beginscan_internal(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    return scan;
}

void bm25rescan_internal(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    return;
}

bool bm25gettuple_internal(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

void bm25endscan_internal(IndexScanDesc scan)
{
    return;
}


Datum bm25_scores_textarr(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.0);
}

Datum bm25_scores_text(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.0);
}