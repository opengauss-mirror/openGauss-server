/**
 * @file cms_cluster_switchover.h
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2020-07-31
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMS_CLUSTER_SWITCHOVER_H
#define CMS_CLUSTER_SWITCHOVER_H

#include "cm_server.h"

#define MAX_CYCLE 600

extern char switchover_flag_file_path[MAX_PATH_LEN];
extern void* Deal_switchover_for_init_cluster(void* arg);

#endif