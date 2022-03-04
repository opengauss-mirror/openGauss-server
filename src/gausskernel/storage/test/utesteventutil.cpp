/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 *
 * utesteventutil.cpp
 * Routines to support unit test. Used to retrieve information during running to help test.
 *
 * IDENTIFICATION
 * src/gausskernel/storage/test/utesteventutil.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/utesteventutil.h"

#ifdef ENABLE_UT
/*
 * The following functions only print information. In UT, they can be mocked to
 * retrieve whatever you want.
 */
void TestXLogRecParseStateEventProbe(EnumTestEventType eventType,
    const char* sourceName, const XLogRecParseState* parseState)
{
    ereport(INFO, (errmsg("test XLogRecParseState event probe. event type:%u, source:%s",
        eventType, sourceName)));
}

void TestXLogReaderProbe(EnumTestEventType eventType,
    const char* sourceName, const XLogReaderState* readerState)
{
    ereport(INFO, (errmsg("test XLogReader event probe. event type:%u, source:%s",
        eventType, sourceName)));
}

#endif

