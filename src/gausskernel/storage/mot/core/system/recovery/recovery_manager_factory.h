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
 * recovery_manager_factory.h
 *    Recovery manager factory interface to create recovery manager.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_manager_factory.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_MANAGER_FACTORY_H
#define RECOVERY_MANAGER_FACTORY_H

#include "recovery_manager.h"

namespace MOT {
class RecoveryManagerFactory {
public:
    static IRecoveryManager* CreateRecoveryManager()
    {
        return new (std::nothrow) RecoveryManager();
    }

private:
    RecoveryManagerFactory()
    {}

    ~RecoveryManagerFactory()
    {}
};
}  // namespace MOT

#endif /* RECOVERY_MANAGER_FACTORY_H */
