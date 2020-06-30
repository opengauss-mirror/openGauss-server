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
 * bbox.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/bbox.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __BBOX_H__
#define __BBOX_H__

#include "bbox_types.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define BBOX_FAST_COMP_RATIO 1
#define BBOX_BEST_COMP_RATIO 9
extern s32 BBOX_SetCoredumpPath(const char* pszPath); /* 设置core文件的路径接口 */

extern s32 BBOX_SetCoreFileCount(s32 iCount); /* 设置最多保留最新core文件的个数接口 */

extern s32 BBOX_TakeSnapShot(char* file_name); /* 生成进程快照接口 */

extern s32 BBOX_CreateCoredump(char* file_name); /* 生成core文件接口 */

extern s32 BBOX_AddBlackListAddress(void* address, u32 len); /* 设置dump信息黑名单接口 */

extern s32 BBOX_RmvBlackListAddress(void* address); /* 删除dump信息黑名单接口 */

extern s32 BBOX_SetCoreCompRatio(u32 uiRatio); /* 设置core文件的压缩比例等级 */

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif
