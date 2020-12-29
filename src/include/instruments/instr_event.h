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
 * instr_event.h
 *        definitions for instruments workload
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/instr_event.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_EVENT_H
#define INSTR_EVENT_H
#include "storage/lock/lwlock.h"
void LWLockReportWaitFailed(LWLock *lock);
#endif
