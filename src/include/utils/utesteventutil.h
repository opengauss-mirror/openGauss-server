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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/include/utils/utesteventutil.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __UTEST_EVENT_UTIL_H__
#define __UTEST_EVENT_UTIL_H__
#include "c.h" // necessary for ENABLE_UT definition

#ifdef ENABLE_UT

#include "access/xlogproc.h"

enum EnumTestEventType {
    // for extreme_rto
    UTEST_EVENT_RTO_START = 0,
    UTEST_EVENT_RTO_TRXNMGR_DISTRIBUTE_ITEMS,
    UTEST_EVENT_RTO_DISPATCH_REDO_RECORD_TO_FILE,
    UTEST_EVENT_RTO_PAGEMGR_REDO_BEFORE_DISTRIBUTE_ITEMS,
    UTEST_EVENT_RTO_PAGEMGR_REDO_AFTER_DISTRIBUTE_ITEMS,
};

extern void TestXLogRecParseStateEventProbe(EnumTestEventType eventType,
    const char* sourceName, const XLogRecParseState* parseState);
extern void TestXLogReaderProbe(EnumTestEventType eventType,
    const char* sourceName, const XLogReaderState* readerState);

#endif
#endif

