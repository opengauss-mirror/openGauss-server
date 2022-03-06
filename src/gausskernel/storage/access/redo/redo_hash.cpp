/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * hash.cpp
 *    parse hash xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/hash.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/relscan.h"
#include "access/xlogproc.h"

#include "catalog/index.h"
#include "commands/vacuum.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "storage/buf/bufmgr.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

XLogRecParseState *HashRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    *blocknum = 0;
    ereport(PANIC, (errmsg("HashRedoParseToBlock: unimplemented")));
    return NULL;
}
