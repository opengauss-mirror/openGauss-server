/**
 * @file cma_thread.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#ifndef CMA_THREADS_H
#define CMA_THREADS_H

void CreateETCDStatusCheckThread();
void CreatePhonyDeadCheckThread();
void CreateCnDnDisconnectCheckThread();
void CreateStartAndStopThread();
void CreateGTMStatusCheckThread();
void CreateCNStatusCheckThread();
void CreateCCNStatusCheckThread(void);
void CreateDNStatusCheckThread(int* i);
void CreateDNXlogCheckThread(int* i);
#ifdef ENABLE_MULTIPLE_NODES
void CreateDNStorageScalingAlarmThread(int* i);
#endif
void CreateFaultDetectThread();
void CreateGtmModeThread();
void CreateConnCmsPThread();
void CreateSendCmsMsgThread();
void CreateKerberosStatusCheckThread();
void CreateAutoRepairCnThread();
void CreateLogFileCompressAndRemoveThread();

#endif
