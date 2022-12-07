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
 * csn_manager.h
 *    CSN Manager interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/common/icsn_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ICSN_MANAGER_H
#define ICSN_MANAGER_H

#include "global.h"

namespace MOT {
class ICSNManager {
public:
    ICSNManager(const ICSNManager& orig) = delete;
    ICSNManager& operator=(const ICSNManager& orig) = delete;

    /** @brief Destructor. */
    virtual ~ICSNManager()
    {}

    /** @brief Get the next csn. This method also increment the current csn. */
    virtual uint64_t GetNextCSN() = 0;

    /** @brief Get the current csn. This method does not change the value of the current csn. */
    virtual uint64_t GetCurrentCSN() = 0;

    /** @brief Get the current csn. This method does not change the value of the current csn. */
    virtual uint64_t GetGcEpoch() = 0;

    /**
     * @brief Used to enforce a csn value. It is used only during recovery by the committer (single thread) or at the
     * end of the recovery.
     */
    virtual void SetCSN(uint64_t value) = 0;

protected:
    ICSNManager()
    {}
};
}  // namespace MOT

#endif /* ICSN_MANAGER_H */
