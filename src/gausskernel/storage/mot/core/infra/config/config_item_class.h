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
 * config_item_class.h
 *    The class of configuration item.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_item_class.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_ITEM_CLASS_H
#define CONFIG_ITEM_CLASS_H

#include <stdint.h>

namespace MOT {
/**
 * @enum ConfigItemClass
 * @brief The class of configuration item.
 */
enum class ConfigItemClass : uint32_t {
    /** @var The configuration item is a section (node). */
    CONFIG_ITEM_SECTION,

    /** @var The configuration item is a value (leaf). */
    CONFIG_ITEM_VALUE,

    /** @var The configuration item is an array of items. */
    CONFIG_ITEM_ARRAY,

    /** @var The configuration item class is undefined. */
    CONFIG_ITEM_UNDEFINED
};

/**
 * @brief A helper class to map class to enumeration.
 * @tparam T The class type.
 */
template <typename T>
struct ConfigItemClassMapper {
    /**
     * @var The configuraiton item class of the configuration item. By default it is undefined. Must be specialized for
     * each configuraiton item type.
     */
    static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_UNDEFINED;

    /** @var The configuration item class name (used for printing). */
    static constexpr const char* CONFIG_ITEM_CLASS_NAME = "";
};

/**
 * @brief Converts string to item class.
 * @param itemClassStr The item class string.
 * @return The resulting item class.
 */
extern ConfigItemClass ConfigItemClassFromString(const char* itemClassStr);

/**
 * @brief Converts items class to string format.
 * @param configItemClass The item class to convert.
 * @return The resulting string form of the item class.
 */
extern const char* ConfigItemClassToString(ConfigItemClass configItemClass);
}  // namespace MOT

#endif /* CONFIG_ITEM_CLASS_H */
