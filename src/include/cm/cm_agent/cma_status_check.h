/**
 * @file cma_status_check.h
 * @brief
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 *
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 *
 */
#ifndef CMA_STATUS_CHECK_H
#define CMA_STATUS_CHECK_H

#define DN_RESTART_COUNT_CHECK_TIME 600
#define DN_RESTART_COUNT_CHECK_TIME_HOUR 3600

#define MAX_DEVICE_DIR 1024
#define FILE_CPUSTAT "/proc/stat"
#define FILE_DISKSTAT "/proc/diskstats"
#define FILE_MOUNTS "/proc/mounts"

#define ETCD_NODE_UNHEALTH_FRE 15
#define CHECK_INVALID_ETCD_TIMES 15

/* when report_interval has changed to bigger ,this number 3 will also change */
#define CHECK_DUMMY_STATE_TIMES 3

void gtm_status_check_and_report(void);
void coordinator_status_check_and_report(void);
void datanode_status_check_and_report(void);
void fenced_UDF_status_check_and_report(void);
void etcd_status_check_and_report(void);
void kerberos_status_check_and_report();
void CheckDiskForCNDataPathAndReport();
void CheckDiskForDNDataPathAndReport();

void* ETCDStatusCheckMain(void* arg);
void* GTMStatusCheckMain(void* arg);
void* DNStatusCheckMain(void * const arg);
void* CNStatusCheckMain(void* arg);
void* CCNStatusCheckMain(void* const arg);
void* KerberosStatusCheckMain(void* const arg);

void* CNDnDisconnectCheckMain(void* arg);

#endif