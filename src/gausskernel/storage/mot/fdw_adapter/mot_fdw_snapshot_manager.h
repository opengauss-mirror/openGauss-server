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
 * mot_fdw_snapshot_manager.h
 *    MOT Foreign Data Wrapper snapshot interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_snapshot_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_FDW_SNAPSHOT_MANAGER_H
#define MOT_FDW_SNAPSHOT_MANAGER_H

#include "icsn_manager.h"

class SnapshotManager final : public MOT::ICSNManager {
public:
    SnapshotManager() noexcept
    {}

    ~SnapshotManager() override
    {}

    /** @brief Get the next csn. */
    uint64_t GetNextCSN() override;

    /** @brief Get the current csn. */
    uint64_t GetCurrentCSN() override;

    /** @brief Get the current csn. */
    uint64_t GetGcEpoch() override;

    /** @brief Used to enforce a csn value. */
    void SetCSN(uint64_t value) override;
};

#endif /* MOT_FDW_SNAPSHOT_MANAGER_H */
