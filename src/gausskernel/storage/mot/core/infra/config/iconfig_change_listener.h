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
 * iconfig_change_listener.h
 *    Configuration change listener interface.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/iconfig_change_listener.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ICONFIG_CHANGE_LISTENER_H
#define ICONFIG_CHANGE_LISTENER_H

namespace MOT {
/**
 * @class IConfigChangeListener
 * @brief Configuration change listener interface.
 */
class IConfigChangeListener {
public:
    /**
     * @brief Derives classes should react to a notification that configuration changed. New
     * configuration is accessible via the ConfigManager.
     */
    virtual void OnConfigChange() = 0;

protected:
    /** Constructor. */
    IConfigChangeListener()
    {}

    /** Destructor. */
    virtual ~IConfigChangeListener()
    {}
};
}  // namespace MOT

#endif /* ICONFIG_CHANGE_LISTENER_H */
