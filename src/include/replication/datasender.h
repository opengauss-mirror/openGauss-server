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
 * datasender.h
 *        Exports from replication/datasender.cpp.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/datasender.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DATASENDER_H
#define _DATASENDER_H

#include <signal.h>
#include <string.h>

#include "fmgr.h"
#include "replication/replicainternal.h"

#define AmDataSenderToDummyStandby() (t_thrd.datasender_cxt.MyDataSnd->sendRole == SNDROLE_PRIMARY_DUMMYSTANDBY)

#define AmDataSenderOnDummyStandby() (t_thrd.datasender_cxt.MyDataSnd->sendRole == SNDROLE_DUMMYSTANDBY_STANDBY)

typedef struct {
    char* receivedFileList; /* Buffer where we store the file list*/
    int msgLength;          /* File list length we got from dummy */
    slock_t mutex;
} GlobalIncrementalBcmDefinition;

/* user-settable parameters */
extern volatile uint32 send_dummy_count;
extern GlobalIncrementalBcmDefinition g_incrementalBcmInfo;

extern int DataSenderMain(void);
extern void DataSndSignals(void);
extern Size DataSndShmemSize(void);
extern void DataSndShmemInit(void);

extern void DataSndWakeup(void);
extern bool DataSndInProgress(int type);

extern Datum pg_stat_get_data_senders(PG_FUNCTION_ARGS);

bool DataSndInSearching(void);
void ReplaceOrFreeBcmFileListBuffer(char* file_list, int msglength);
void InitGlobalBcm(void);

#endif /* _DATASENDER_H */
