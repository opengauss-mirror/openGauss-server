/**
 * @file cms_arbitrate_cms.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-07
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMS_ARBITRATE_CMS_H
#define CMS_ARBITRATE_CMS_H

extern void CloseHAConnection(CM_Connection* con);
extern void* CM_ThreadHAMain(void* argp);
#endif