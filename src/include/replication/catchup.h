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
 * catchup.h
 *        Exports from replication/data/catchup.cpp.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/catchup.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CATCHUP_H
#define CATCHUP_H

#include <signal.h>

typedef enum { CATCHUP_NONE = 0, CATCHUP_STARTING, CATCHUP_SEARCHING, RECEIVED_OK, RECEIVED_NONE } CatchupState;

extern volatile bool catchup_online;
extern volatile bool catchupDone;
extern CatchupState catchupState;

extern bool IsCatchupProcess(void);
extern ThreadId StartCatchup(void);
extern void CatchupShutdownIfNoDataSender(void);

#ifdef EXEC_BACKEND
extern void CatchupMain();
#endif

#endif
