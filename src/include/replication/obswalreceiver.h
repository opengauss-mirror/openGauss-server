/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * obswalreceiver.h
 *        obswalreceiver init for WalreceiverMain.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/obswalreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OBSWALRECEIVER_H
#define OBSWALRECEIVER_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "replication/walprotocol.h"


extern int32 pg_atoi(char* s, int size, int c);
extern int32 pg_strtoint32(const char* s);
/* Prototypes for interface functions */

extern bool obs_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier);
extern bool obs_receive(int timeout, unsigned char* type, char** buffer, int* len);
extern void obs_send(const char *buffer, int nbytes);
extern void obs_disconnect(void);

#define OBS_XLOG_FILENAME_LENGTH 1024
#define OBS_XLOG_SLICE_NUM_MAX 0x3
#define OBS_XLOG_SLICE_BLOCK_SIZE ((uint32)(4 * 1024 * 1024))
#define OBS_XLOG_SLICE_HEADER_SIZE (sizeof(uint32))
/* sizeof(uint32) + OBS_XLOG_SLICE_BLOCK_SIZE */
#define OBS_XLOG_SLICE_FILE_SIZE (OBS_XLOG_SLICE_BLOCK_SIZE + OBS_XLOG_SLICE_HEADER_SIZE)
#define OBS_XLOG_SAVED_FILES_NUM 25600 /* 100G*1024*1024*1024/OBS_XLOG_SLICE_BLOCK_SIZE */
#define IS_DISASTER_RECOVER_MODE \
    (t_thrd.xlog_cxt.server_mode == STANDBY_MODE &&  !XLogArchivingActive() && getObsReplicationSlot())
#define IS_CNDISASTER_RECOVER_MODE \
    (IS_PGXC_COORDINATOR &&  !XLogArchivingActive() && getObsReplicationSlot())


extern int obs_replication_receive(XLogRecPtr startPtr, char **buffer,
                                    int *bufferLength, int timeout_ms, char* inner_buff);
extern int obs_replication_archive(const ArchiveXlogMessage *xlogInfo);
extern int obs_replication_cleanup(XLogRecPtr recPtr);
extern void update_recovery_barrier();
extern void update_stop_barrier();
extern int obs_replication_get_last_xlog(ArchiveXlogMessage *xloginfo);
extern bool obs_replication_read_barrier(const char* barrierFile, char* barrierOut);

#endif
