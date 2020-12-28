/**
 * @file cma_create_conn_cms.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_CREATE_CONN_CMS_H
#define CMA_CREATE_CONN_CMS_H

typedef enum CMA_OPERATION_ {
	CMA_KILL_SELF_INSTANCES = 0,
} cma_operation;

void* ConnCmsPMain(void* arg);
extern bool isUpgradeCluster();
#endif