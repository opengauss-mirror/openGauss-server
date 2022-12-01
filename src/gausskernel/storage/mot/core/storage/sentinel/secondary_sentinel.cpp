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
 * secondary_sentinel.cpp
 *    Secondary Index Sentinel
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/secondary_sentinel.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "secondary_sentinel.h"
#include "row.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(SecondarySentinel, Storage);

void SecondarySentinel::Print()
{
    MOT_LOG_INFO("---------------------------------------------------------------");
    MOT_LOG_INFO("SecondarySentinel: Index name: %s"
                 " startCSN = %lu endCSN = %s indexOrder = SECONDARY_NON_UNIQUE_INDEX",
        GetIndex()->GetName().c_str(),
        GetStartCSN(),
        GetEndCSNStr().c_str());
    MOT_LOG_INFO("|");
    MOT_LOG_INFO("V");

    if (IsCommited()) {
        PrimarySentinel* ps = reinterpret_cast<PrimarySentinel*>(GetPrimarySentinel());
        return ps->Print();
    } else {
        MOT_LOG_INFO("NULL");
    }
    MOT_LOG_INFO("---------------------------------------------------------------");
}
}  // namespace MOT
