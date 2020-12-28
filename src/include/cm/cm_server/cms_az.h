/**
 * @file cms_az.h
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2020-07-31
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMS_AZ_CHECK_H
#define CMS_AZ_CHECK_H

#define AZ_STATUS_RUNNING 0
#define AZ_STAUTS_STOPPED 1

#define PING_TIMEOUT_OPTION " -c 2 -W 2"
/* for the limit of check node of success, when check az1 is success */
#define AZ1_AND_AZ2_CHECK_SUCCESS_NODE_LIMIT 10
#define AZ1_AZ2_CONNECT_PING_TRY_TIMES 3

const int MAX_PING_NODE_NUM = 10;

/* data structure to store input/output of ping-check thread function */
typedef struct PingCheckThreadParmInfo {
    /* the node to ping */
    uint32 azNode;
    /* ping thread idnex */
    uint32 threadIdx;
    /* the array of ping result */
    uint32 *pingResultArrayRef;
}PingCheckThreadParmInfo;

typedef enum {START_AZ, STOP_AZ} OperateType;
typedef enum {SET_ETCD_AZ, GET_ETCD_AZ} EtcdOperateType;
typedef enum {UNKNOWN_AZ_DEPLOYMENT, TWO_AZ_DEPLOYMENT, THREE_AZ_DEPLOYMENT} AZDeploymentType;

extern void* Az1Az2ConnectStateCheck(void* arg);
extern void* MultiAzConnectStateCheckMain(void* arg);
extern void getAZDyanmicStatus(
    int azCount, int* statusOnline, int* statusPrimary, int* statusFail, int* statusDnFail, char azArray[][CM_AZ_NAME]);

extern void* AZStatusCheckAndArbitrate(void* arg);
#endif