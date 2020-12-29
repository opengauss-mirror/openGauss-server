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
 * cms_sync_dynamic_info.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_sync_dynamic_info.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMS_SYNC_DYNAMIC_INFO_H
#define CMS_SYNC_DYNAMIC_INFO_H

extern void* SyncDynamicInfoFromEtcd(void* arg);

#endif