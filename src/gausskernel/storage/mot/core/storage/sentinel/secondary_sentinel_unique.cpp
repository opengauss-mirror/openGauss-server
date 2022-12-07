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
 * secondary_sentinel_unique.cpp
 *    Secondary Unique Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/secondary_sentinel_unique.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "secondary_sentinel_unique.h"
#include "row.h"

namespace MOT {

class Index;

IMPLEMENT_CLASS_LOGGER(SecondarySentinelUnique, Storage);

PrimarySentinel* SecondarySentinelUnique::GetVisiblePrimaryHeader(uint64_t csn)
{
    PrimarySentinelNode* node = nullptr;
    while (!IsCommited() and IsLocked()) {
        CpuCyclesLevelTime::Sleep(1);
    }
    if (IsCommited()) {
        if (csn > m_startCSN) {
            node = GetTopNode();
        } else {
            // Phantom insert
            return nullptr;
        }
        MOT_ASSERT(node != nullptr);
        while (node) {
            // Check if key is deleted
            // Valid range (start_csn,end_csn]
            if (csn <= node->GetEndCSN() and csn > node->GetStartCSN()) {
                return node->GetPrimarySentinel();
            }
            node = node->GetNextVersion();
        }
    }
    return nullptr;
}

PrimarySentinelNode* SecondarySentinelUnique::GetNodeByCSN(uint64_t csn)
{
    PrimarySentinelNode* node = nullptr;
    while (IsLocked()) {
        CpuCyclesLevelTime::Sleep(1);
    }
    if (IsCommited()) {
        if (csn > m_startCSN) {
            node = GetTopNode();
        } else {
            // Phantom insert
            return nullptr;
        }
        while (node) {
            // Check if key is deleted
            // Valid range (start_csn,end_csn]
            if (csn > node->GetEndCSN()) {
                return nullptr;
            } else {
                if (csn > node->GetStartCSN()) {
                    return node;
                }
            }
            node = node->GetNextVersion();
        }
    }
    return nullptr;
}

void SecondarySentinelUnique::ReleaseAllNodes()
{
    PrimarySentinelNode* node = GetTopNode();
    Index* index = GetIndex();
    SetNextPtr(nullptr);
    ReleaseNodeChain(node, index);
}

void SecondarySentinelUnique::DetachUnusedNodes(uint64_t snapshot, PrimarySentinelNode*& detachedChain)
{
    detachedChain = nullptr;
    PrimarySentinelNode* node = GetTopNode();
    while (node) {
        PrimarySentinelNode* tmp = node->GetNextVersion();
        if (tmp and snapshot >= tmp->GetEndCSN()) {
            node->SetNextVersion(nullptr);
            detachedChain = tmp;
            return;
        }
        node = tmp;
    }
}

void SecondarySentinelUnique::Print()
{
    MOT_LOG_INFO("---------------------------------------------------------------");
    MOT_LOG_INFO("SecondarySentinel: Index name: %s startCSN = %lu indexOrder = SECONDARY_UNIQUE_INDEX",
        GetIndex()->GetName().c_str(),
        GetStartCSN());
    MOT_LOG_INFO("|");
    MOT_LOG_INFO("V");

    if (IsCommited()) {
        PrimarySentinelNode* node = GetTopNode();
        while (node) {
            MOT_LOG_INFO("SecondaryUniqueNode: startCSN = %lu endCSN = %s nextNode = %p",
                node->GetStartCSN(),
                node->GetEndCSNStr().c_str(),
                node->GetNextVersion());
            MOT_LOG_INFO("|");
            MOT_LOG_INFO("V");
            node->GetPrimarySentinel()->Print();
            node = node->GetNextVersion();
        }
    } else {
        MOT_LOG_INFO("NULL");
    }
    MOT_LOG_INFO("---------------------------------------------------------------");
}

void SecondarySentinelUnique::ReleaseNodeChain(PrimarySentinelNode* node, Index* index)
{
    MOT_ASSERT(index != nullptr);
    while (node) {
        PrimarySentinelNode* tmp = node->GetNextVersion();
        index->SentinelNodeRelease(node);
        node = tmp;
    }
}
}  // namespace MOT
