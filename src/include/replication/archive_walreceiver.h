/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * archive_walreceiver.h
 *        obswalreceiver init for WalreceiverMain.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/archive_walreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OBSWALRECEIVER_H
#define OBSWALRECEIVER_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "replication/walprotocol.h"
#include "replication/slot.h"


extern int32 pg_atoi(char* s, int size, int c, bool can_ignore);
extern int32 pg_strtoint32(const char* s, bool can_ignore);
/* Prototypes for interface functions */

extern bool archive_connect(char* conninfo, XLogRecPtr* startpoint, char* slotname, int channel_identifier);
extern bool archive_receive(int timeout, unsigned char* type, char** buffer, int* len);
extern void archive_send(const char *buffer, int nbytes);
extern void archive_disconnect(void);

#define OBS_XLOG_FILENAME_LENGTH 1024
#define OBS_XLOG_SLICE_NUM_MAX 0x3
#define OBS_XLOG_SLICE_BLOCK_SIZE ((uint32)(4 * 1024 * 1024))
#define OBS_XLOG_SLICE_HEADER_SIZE (sizeof(uint32))
/* sizeof(uint32) + OBS_XLOG_SLICE_BLOCK_SIZE */
#define OBS_XLOG_SLICE_FILE_SIZE (OBS_XLOG_SLICE_BLOCK_SIZE + OBS_XLOG_SLICE_HEADER_SIZE)
#define OBS_XLOG_SAVED_FILES_NUM 25600 /* 100G*1024*1024*1024/OBS_XLOG_SLICE_BLOCK_SIZE */
#define IS_OBS_DISASTER_RECOVER_MODE \
    (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE && GetArchiveRecoverySlot())
#define IS_CN_OBS_DISASTER_RECOVER_MODE \
    (IS_PGXC_COORDINATOR  && GetArchiveRecoverySlot())
#define OBS_ARCHIVE_STATUS_FILE "obs_archive_start_end_record"
#define OBS_LAST_CLEAN_RECORD "obs_last_clean_record"
#define ARCHIVE_GLOBAL_BARRIER_LIST_PATH "global_barrier_records"
#define FILE_TIME_INTERVAL 600000

extern int archive_replication_receive(XLogRecPtr startPtr, char **buffer,
                                    int *bufferLength, int timeout_ms, char* inner_buff);
extern int ArchiveReplicationAchiver(const ArchiveXlogMessage *xlogInfo);
extern void update_archive_start_end_location_file(XLogRecPtr endPtr, TimestampTz endTime);
extern int archive_replication_cleanup(XLogRecPtr recPtr, ArchiveConfig *archive_config = NULL, bool reverse = false);
extern void update_recovery_barrier();
extern void update_stop_barrier();
extern int archive_replication_get_last_xlog(ArchiveXlogMessage *xloginfo, ArchiveConfig* archive_obs);
extern bool ArchiveReplicationReadFile(const char* fileName, char* content, int contentLen,
    const char *slotName = NULL);
extern char* get_local_key_cn(void);
extern void UpdateGlobalBarrierListOnMedia(const char* id, const char* availableCNName);
extern void WriteGlobalBarrierListStartTimeOnMedia(long cur_time);
extern uint64 ReadBarrierTimelineRecordFromObs(const char* archiveSlotName);
extern char* DeleteStopBarrierRecordsOnMedia(long stopBarrierTimestamp, long endBarrierTimestamp = 0);
extern int GetArchiveXLogFileTotalNum(ArchiveConfig *archiverConfig, XLogRecPtr endLsn);
#endif
