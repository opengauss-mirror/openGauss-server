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
 * config_item.h
 *    The base interface for all configuration item types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_item.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_ITEM_H
#define CONFIG_ITEM_H

#include "config_item_class.h"
#include "mot_string.h"
#include "mot_list.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
/**
 * @class ConfigItem
 * @brief The base interface for all configuration item types.
 */
class ConfigItem {
protected:
    /** Constructor. */
    ConfigItem(ConfigItemClass itemClass);

public:
    /**
     * @brief Utility function for safe configuration item allocation.
     * @tparam T The configuration item type.
     * @tparam Args (variadic) The arguments required to construct the configuration item object.
     * @param path The configuration path leading to the item.
     * @param name The configuration item name.
     * @return The configuration item if succeeded, otherwise null.
     */
    template <typename T, class... Args>
    static T* CreateConfigItem(const char* path, const char* name, Args&&... args)
    {
        T* result = new (std::nothrow) T(std::forward<Args>(args)...);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Configuration",
                "Failed to allocate configuration item of type %s",
                ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS);
        } else {
            if (!result->Initialize(path, name)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Load Configuration",
                    "Failed to initialize configuration item of type %s with path %s and name %s",
                    ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS_NAME,
                    path,
                    name);
                delete result;
                result = nullptr;
            }
        }
        return result;
    }

    /** Destructor. */
    virtual ~ConfigItem()
    {}

    /**
     * @brief Initializes the configuration item.
     * @param path The configuration path leading to the item.
     * @param name The configuration item name.
     * @return True if initialization succeeded.
     */
    bool Initialize(const char* path, const char* name);

    /**
     * @brief Retrieves the full configuration path from the root to this item. The path does not
     * include the name of this configuration item.
     * @return The configuration path.
     */
    inline const char* GetPath() const
    {
        return m_path.c_str();
    }

    /**
     * @brief Retrieves the name identifying the configuration item.
     * @return The name of the item.
     */
    inline const char* GetName() const
    {
        return m_name.c_str();
    }

    /**
     * @brief Retrieves the full path name of the configuration item.
     * @return The full path name of the item
     */
    inline const char* GetFullPathName() const
    {
        return m_fullPathName.c_str();
    }

    /**
     * @brief Retrieves the class of the configuration item, denoting whether it is a configuration
     * section or a value.
     * @return The class of the configuration item.
     */
    inline ConfigItemClass GetClass() const
    {
        return m_class;
    }

    /**
     * @brief Prints the configuration item to the log.
     * @param logLevel The log level used for printing.
     * @param fullPrint Specifies whether to print full path names of terminal leaves.
     */
    virtual void Print(LogLevel logLevel, bool fullPrint) const = 0;

    /**
     * @brief Retrieves the depth of the item according to its name. Main
     * sections have depth 1. The virtual root section has depth 0. Leaves add an
     * additional depth layer.
     * @return The depth of the section.
     */
    inline uint32_t GetDepth() const
    {
        return m_depth;
    }

    /** @var Constant denoting the character separating configuration item path names. */
    static constexpr char PATH_SEP = '.';

    /** @var Constant denoting the string separating configuration item path names. */
    static constexpr const char* PATH_SEP_STR = ".";

    /** @var The maximum size in characters of a configuration path. */
    static constexpr size_t CFG_MAX_PATH_LEN = 4096;

    /** @var The maximum size in characters of a configuration name. */
    static constexpr size_t CFG_MAX_NAME_LEN = 1024;

    /** @var The maximum size in characters of a full configuration path. */
    static constexpr size_t CFG_MAX_FULL_PATH_LEN = (CFG_MAX_PATH_LEN + CFG_MAX_NAME_LEN + 1);

protected:
    DECLARE_CLASS_LOGGER()

private:
    /** @var The configuration path of this item. */
    mot_string m_path;

    /** @var The name of this item. */
    mot_string m_name;

    /** @var The full path name of this item. */
    mot_string m_fullPathName;

    /** @var The configuration class of this item. */
    ConfigItemClass m_class;

    /** @var The depth of the section. */
    uint32_t m_depth;

    /** @brief Compute section depth. */
    uint32_t ComputeDepth() const;
};

/** @typedef The data structure for holding a list of configuration items. */
typedef mot_list<ConfigItem*> ConfigItemList;
}  // namespace MOT

#endif /* CONFIG_ITEM_H */
