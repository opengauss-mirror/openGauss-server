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
 * commgr.h
 *     definitions for statistics control functions
 * 
 * IDENTIFICATION
 *        src/include/workload/commgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef WORKLOAD_COMMGR_H
#define WORKLOAD_COMMGR_H

#include "pgxc/pgxcnode.h"

typedef void (*WLMParseDetail)(const char*, void*);
typedef void (*WLMParseMessage)(StringInfo, void*, int);

typedef enum WLMCollectTag {
    WLM_COLLECT_NONE = 0,
    WLM_COLLECT_UTIL,
    WLM_COLLECT_USERINFO,
    WLM_COLLECT_SESSINFO,
    WLM_COLLECT_IO_RUNTIME,
    WLM_COLLECT_PROCINFO,
    WLM_COLLECT_ANY,
    WLM_COLLECT_OPERATOR_RUNTIME,
    WLM_COLLECT_OPERATOR_SESSION,
    WLM_COLLECT_ADJUST,
    WLM_COLLECT_JOBINFO
} WLMCollectTag;

extern LWLock* LockSessRealTHashPartition(uint32 hashCode, LWLockMode lockMode);
extern LWLock* LockSessHistHashPartition(uint32 hashCode, LWLockMode lockMode);
extern void UnLockSessRealTHashPartition(uint32 hashCode);
extern void UnLockSessHistHashPartition(uint32 hashCode);
extern LWLock* LockInstanceRealTHashPartition(uint32 hashCode, LWLockMode lockMode);
extern LWLock* LockInstanceHistHashPartition(uint32 hashCode, LWLockMode lockMode);
extern void UnLockInstanceRealTHashPartition(uint32 hashCode);
extern void UnLockInstanceHistHashPartition(uint32 hashCode);
extern void WLMLocalInfoCollector(StringInfo msg);
extern List* WLMRemoteJobInfoCollector(const char* keystr, void* suminfo, WLMCollectTag tag);
extern void WLMRemoteInfoCollector(
    const char* keystr, void* suminfo, WLMCollectTag tag, int size, WLMParseMessage parse_func);
extern PGXCNodeAllHandles* WLMRemoteInfoCollectorStart(void);
extern int WLMRemoteInfoSenderByNG(const char* group_name, const char* keystr, WLMCollectTag tag);
extern int WLMRemoteInfoSender(PGXCNodeAllHandles* pgxc_handles, const char* keystr, WLMCollectTag tag);
extern void WLMRemoteInfoReceiverByNG(const char* group_name, void* suminfo, int size, WLMParseMessage parse_func);
extern void WLMRemoteInfoReceiver(
    PGXCNodeAllHandles* pgxc_handles, void* suminfo, int size, WLMParseMessage parse_func);
extern void WLMRemoteInfoCollectorFinish(PGXCNodeAllHandles* pgxc_handles);
extern void WLMRemoteNodeExecuteSql(const char* sql, int nodeid = 0);

#endif
