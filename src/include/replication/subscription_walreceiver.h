/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * Description: openGauss is licensed under Mulan PSL v2.
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
 * subscription_walreceiver.h
 *        subscription walreceiver init for ApplyWorkerMain.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/subscription_walreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SUBSCRIPTIONWALRECEIVER_H
#define SUBSCRIPTIONWALRECEIVER_H

#include "postgres.h"
#include "replication/walprotocol.h"
#include "replication/slot.h"

extern bool sub_connect(char *conninfo, XLogRecPtr *startpoint, char *appname, int channel_identifier);
extern void sub_identify_system();
extern void sub_startstreaming(const LibpqrcvConnectParam *options);
extern void sub_create_slot(const LibpqrcvConnectParam *options);
#endif
