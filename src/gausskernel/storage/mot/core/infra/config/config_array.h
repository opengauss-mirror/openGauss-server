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
 * config_array.h
 *    Configuration item array implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_array.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_ARRAY_H
#define CONFIG_ARRAY_H

#include "config_item_class.h"
#include "config_item_visitor.h"
#include "typed_config_value.h"
#include "mot_vector.h"

namespace MOT {
// forward declaration
class ConfigSection;

class ConfigArray : public ConfigItem {
public:
    /** Constructor. */
    ConfigArray();

    /** Destructor. */
    ~ConfigArray() override;

    /**
     * @brief Utility method for creating a configuration array.
     * @param path The configuration path leading to the item.
     * @param name The configuration item name.
     * @return The configuration array if succeeded, otherwise null.
     */
    static ConfigArray* CreateConfigArray(const char* path, const char* name)
    {
        return ConfigItem::CreateConfigItem<ConfigArray>(path, name);
    }

    /**
     * @brief Prints the configuration section to the log.
     * @param logLevel The log level used for printing.
     * @param fullPrint Specifies whether to print full path names of terminal leaves.
     */
    virtual void Print(LogLevel logLevel, bool fullPrint) const;

    /**
     * @brief Builds the array by adding a sub-item.
     * @param configItem The direct configuration item to add as a child of this
     * array. It could be another section or a terminal value.
     * @return True if succeeded, otherwise false.
     */
    inline bool AddConfigItem(ConfigItem* configItem)
    {
        return mItemArray.push_back(configItem);
    }

    /**
     * @brief Retrieves the configuration item count held by this item array.
     * @return The item count.
     */
    inline uint32_t GetConfigItemCount() const
    {
        return mItemArray.size();
    }

    /**
     * @brief Retrieves a configuration sub-section by index.
     * @param index The index of the sub-section
     * @return The sub-section, or null pointer if index is out of bounds, or the
     * configuration item in the specified index is not a section.
     */
    inline const ConfigSection* GetConfigSectionAt(uint32_t index) const;

    /**
     * @brief Retrieves a typed configuration sub-value by index.
     * @param index The index of the sub-value
     * @return The typed sub-value, or null pointer if index is out of bounds, or
     * the configuration item in the specified index is not a value.
     */
    template <typename T>
    inline const TypedConfigValue<T>* GetConfigValueAt(uint32_t index) const
    {
        const TypedConfigValue<T>* result = nullptr;
        const ConfigValue* value = GetTypedItemAt<ConfigValue>(index);
        if (value != nullptr && value->GetConfigValueType() == ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
            result = static_cast<const TypedConfigValue<T>*>(value);
        }
        return result;
    }

    /**
     * @brief Retrieves a typed configuration sub-array by index.
     * @param index The index of the sub-array.
     * @return The typed sub-array, or null pointer if index is out of bounds, or
     * the configuration item in the specified index is not an array.
     */
    inline const ConfigArray* GetConfigArrayAt(uint32_t index) const
    {
        return GetTypedItemAt<ConfigArray>(index);
    }

    /**
     * @brief Retrieves a configuration item by index.
     * @param index The index of the sub-item.
     * @return The resulting configuration item, or null pointer if index is out of bounds, or
     * the configuration item in the specified index is not a value.
     */
    inline const ConfigItem* GetConfigItemAt(uint32_t index) const
    {
        const ConfigItem* result = nullptr;
        if (index < mItemArray.size()) {
            result = mItemArray[index];
        }
        return result;
    }

    /**
     * @brief Traverses the section with a visitor applied to each sub-item in the array. This is a
     * recursive call.
     * @param visitor The visitor used to visit each sub-item in the array.
     */
    void ForEach(ConfigItemVisitor& visitor) const;

    /** @typedef Configuration item array type. */
    typedef mot_vector<ConfigItem*> ConfigItemVector;

private:
    /** @var Array of all direct sub-sections. */
    ConfigItemVector mItemArray;

    /**
     * @brief Helper method for get a typed configuration item at a specified index.
     * @param index The item index.
     * @return The item if the index in array bounds and the item class matches.
     */
    template <typename T>
    inline const T* GetTypedItemAt(uint32_t index) const;
};

/** @typedef Configuration item array type. */
typedef ConfigArray::ConfigItemVector ConfigItemVector;
}  // namespace MOT

#include "config_section.h"

namespace MOT {
// specialization
template <typename T>
inline const T* ConfigArray::GetTypedItemAt(uint32_t index) const
{
    const T* result = nullptr;
    const ConfigItem* item = GetConfigItemAt(index);
    if (item != nullptr) {
        if (item->GetClass() == ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS) {
            result = static_cast<const T*>(item);
        }
    }
    return result;
}

inline const ConfigSection* ConfigArray::GetConfigSectionAt(uint32_t index) const
{
    return GetTypedItemAt<ConfigSection>(index);
}

// specialization
template <>
struct ConfigItemClassMapper<ConfigArray> {
    /** @var The configuration item class (array). */
    static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_ARRAY;

    /** @var The configuation item class name. */
    static constexpr const char* CONFIG_ITEM_CLASS_NAME = "ConfigArray";
};
}  // namespace MOT

#endif /* CONFIG_ARRAY_H */
