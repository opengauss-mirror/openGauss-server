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
 * streamMain.h
 *    stream main Interface
 *
 * IDENTIFICATION
 *    src/include/distributelayer/streamMain.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef STREAM_MAIN_H
#define STREAM_MAIN_H

extern int StreamMain();
extern void SetStreamWorkerInfo(class StreamProducer* proObj);
extern void ResetStreamEnv();
extern void ResetSessionEnv();
extern void ExtractProduerInfo();
extern void ExtractProduerSkewInfo();
extern ThreadId ApplyStreamThread(StreamProducer *producer);
extern void RestoreStream();
extern void StreamExit();
extern void RestoreStreamSyncParam(struct StreamSyncParam *syncParam);
extern void WLMReleaseNodeFromHash(void);

#endif