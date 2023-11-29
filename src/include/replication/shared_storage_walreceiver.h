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
 * shared_storage_walreceiver.h
 *        shared_storage_walreceiver init for WalreceiverMain.
 *
 *
 * IDENTIFICATION
 *        src/include/replication/shared_storage_walreceiver.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef INCLUDE_REPLICATION_SHARED_STORAGE_WALRECEIVER_H_
#define INCLUDE_REPLICATION_SHARED_STORAGE_WALRECEIVER_H_
#include "postgres.h"
#include "access/xlogdefs.h"
#include "replication/walprotocol.h"

extern bool shared_storage_connect(char *conninfo, XLogRecPtr *startpoint, char *slotname, int channel_identifier);

extern bool shared_storage_receive(int timeout, unsigned char *type, char **buffer, int *len);

extern void shared_storage_send(const char *buffer, int nbytes);
extern void shared_storage_disconnect(void);
extern bool SharedStorageXlogReadCheck(XLogReaderState *xlogreader, XLogRecPtr readEnd, XLogRecPtr readPageStart,
        char *localBuff, int *readLen);

#define IS_SHARED_STORAGE_MAIN_STANDBY_MODE                              \
    (t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby &&            \
     g_instance.attr.attr_storage.xlog_file_path != NULL)
#define IS_SHARED_STORAGE_CASCADE_STANDBY_MODE                           \
    (t_thrd.postmaster_cxt.HaShmData->is_cascade_standby &&              \
     g_instance.attr.attr_storage.xlog_file_path != NULL)

#define IS_SHARED_STORAGE_STANDBY_CLUSTER                                    \
        (g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_STANDBY && \
         g_instance.attr.attr_storage.xlog_file_path != NULL)


#define IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE                   \
    (t_thrd.xlog_cxt.server_mode == STANDBY_MODE &&                      \
     g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_STANDBY && \
     g_instance.attr.attr_storage.xlog_file_path != NULL)

#define IS_SHARED_STORAGE_PRIMARY_CLUSTER_STANDBY_MODE                   \
    (t_thrd.xlog_cxt.server_mode == STANDBY_MODE &&                      \
     g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_PRIMARY && \
     g_instance.attr.attr_storage.xlog_file_path != NULL)
#define IS_SHARED_STORAGE_PRIMARY_CLUSTER_PRIMARY_MODE                        \
    (((IS_PGXC_DATANODE && t_thrd.xlog_cxt.server_mode == PRIMARY_MODE) ||    \
      (IS_PGXC_COORDINATOR && t_thrd.xlog_cxt.server_mode == NORMAL_MODE)) && \
     g_instance.attr.attr_common.cluster_run_mode == RUN_MODE_PRIMARY &&      \
     g_instance.attr.attr_storage.xlog_file_path != NULL)
#define IS_SHARED_STORAGE_STANBY_MODE \
    (g_instance.attr.attr_storage.xlog_file_path != NULL && t_thrd.xlog_cxt.server_mode == STANDBY_MODE)
#define IS_SHARED_STORAGE_MODE (g_instance.attr.attr_storage.xlog_file_path != NULL)

#define MAX_XLOG_FILE_SIZE_BUFFER 256
#endif /* INCLUDE_REPLICATION_SHARED_STORAGE_WALRECEIVER_H_ */
