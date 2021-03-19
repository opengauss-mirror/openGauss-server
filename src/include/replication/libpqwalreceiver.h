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
 * libpqwalreceiver.h
 *        libpqwalreceiver init for WalreceiverMain.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/libpqwalreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LIBPQWALRECEIVER_H
#define LIBPQWALRECEIVER_H

extern int32 pg_atoi(char* s, int size, int c);
extern int32 pg_strtoint32(const char* s);
/* Prototypes for interface functions */
extern bool libpqrcv_connect_for_TLI(TimeLineID* timeLineID, char* conninfo);
extern bool libpqrcv_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier);
extern bool libpqrcv_receive(int timeout, unsigned char* type, char** buffer, int* len);
extern void libpqrcv_send(const char* buffer, int nbytes);
extern void libpqrcv_disconnect(void);

#endif
