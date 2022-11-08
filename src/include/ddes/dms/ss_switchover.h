/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_switchover.h
 *  ss_switchover
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_switchover.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_DDES_SS_SWITCHOVER_H
#define SRC_INCLUDE_DDES_SS_SWITCHOVER_H

#include "ddes/dms/ss_common_attr.h"

#define CHECKPOINT_RECORD_LOCATION1 1
#define CHECKPOINT_RECORD_LOCATION2 2

void SSDoSwitchover();
void SSHandleSwitchoverPromote();
void SSNotifySwitchoverPromote();

#endif
