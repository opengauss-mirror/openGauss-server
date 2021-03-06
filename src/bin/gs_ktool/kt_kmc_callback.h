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
 * kt_kmc_callback.cpp
 *  Register callback functions of KMC :
 *      There are 8 types of callback functions, each type has several functions
 *      Wsec[Type]Callbacks : Callback[Func]
 *         type  |    func                                                          |   set
 *      ----------+------------------------------------------------------------------+--------
 *      mem       |   memAlloc, memFree, memCmp                                      |  default
 *      file      |   fileOpen, fileClose, fileRead, fileWrite, fileFlush,           |
 *                |        fileRemove, fileTell, fileSeek, fileEof, fileErrno        |    11
 *      lock      |   createLock, destroyLock, lock, unlock                          |    4
 *      procLock  |   createProcLock, destroyProcLock, procLock, procUnlock          |    4
 *      basicRely |   writeLog, notify, doEvents                                     |    3
 *      rng       |   getRandomNum, getEntropty, cleanupEntopy                       |    3
 *      time      |   gmTimeSate                                                     |    1
 *      hardWare  |   hwGetEncExtraData...(all 12)                                   |  
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_kmc_callback.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_KT_CALLBACK_H
#define GS_KT_CALLBACK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "wsecv2_errorcode.h"
#include "wsecv2_type.h"
#include "kmcv2_itf.h"
#include "wsecv2_itf.h"
#include "securec.h"
#include "securec_check.h"

extern unsigned long RegFunCallback();

#ifdef __cplusplus
}
#endif

#endif