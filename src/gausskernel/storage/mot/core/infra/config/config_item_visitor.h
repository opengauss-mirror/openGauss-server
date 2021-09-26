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
 * config_item_visitor.h
 *    Visitor used to traverse configuration trees.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_item_visitor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_ITEM_VISITOR_H
#define CONFIG_ITEM_VISITOR_H

#include "config_item.h"

namespace MOT {
/**
 * @class ConfigItemVisitor
 * @brief Visitor used to traverse configuration trees.
 */
class ConfigItemVisitor {
protected:
    /** @brief Constructor. */
    ConfigItemVisitor()
    {}

public:
    /** @brief Destructor. */
    virtual ~ConfigItemVisitor()
    {}

    /**
     * @brief Visits a single configuration item.
     * @param configItem The visited configuration item.
     */
    virtual void OnConfigItem(const ConfigItem* configItem) = 0;
};
}  // namespace MOT

#endif /* CONFIG_ITEM_VISITOR_H */
