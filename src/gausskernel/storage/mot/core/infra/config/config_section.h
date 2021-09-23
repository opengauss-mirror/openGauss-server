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
 * config_section.h
 *    Configuration section that contains configuration items.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_section.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_SECTION_H
#define CONFIG_SECTION_H

#include "config_item_visitor.h"
#include "type_formatter.h"
#include "typed_config_value.h"
#include "infra_util.h"

namespace MOT {
// forward declaration
class ConfigArray;

/**
 * @brief A configuration section that contains configuration items and other configuration section. In essence this is
 * an internal node in a configuration tree.
 */
class ConfigSection : public ConfigItem {
public:
    /** @brief Constructor. */
    ConfigSection();

    /** @brief Destructor. */
    ~ConfigSection() override;

    /**
     * @brief Utility method for creating a configuration section.
     * @param path The configuration path leading to the item.
     * @param name The configuration item name.
     * @return The configuration section if succeeded, otherwise null.
     */
    static ConfigSection* CreateConfigSection(const char* path, const char* name)
    {
        return ConfigItem::CreateConfigItem<ConfigSection>(path, name);
    }

    /** @brief Queries whether this is the root item. */
    inline bool IsRoot() const
    {
        return GetDepth() == 0;
    }

    /**
     * @brief Prints the configuration section to the log.
     * @param logLevel The log level used for printing.
     * @param fullPrint Specifies whether to print full path names of terminal leaves.
     */
    virtual void Print(LogLevel logLevel, bool fullPrint) const;

    /**
     * @brief Retrieves all the names of all stored sub-sections.
     * @param sectionNames The names of the sub-sections.
     * @return True if retrieval succeeded, otherwise false.
     */
    bool GetConfigSectionNames(mot_string_list& sectionNames) const;

    /**
     * @brief Retrieves all the names of all stored sub-value.
     * @param valueNames The names of the sub-values.
     * @return True if retrieval succeeded, otherwise false.
     */
    bool GetConfigValueNames(mot_string_list& valueNames) const;

    /**
     * @brief Builds the section by adding a sub-item.
     * @param config_item The direct configuration item to add as a child of this section. It could be another section
     * or a terminal value.
     * @param[opt] replaceIfExists Specifies whether to replace an existing item if it already exists by the given name.
     * This is relevant only for configuration values, not for configuration sections or arrays.
     * @return True if the item added or false if a configuration item by that name already exists.
     */
    bool AddConfigItem(ConfigItem* configItem, bool replaceIfExists = false);

    /**
     * @brief Merges the given section into this section.
     * @param configSection The configuration section to merge.
     * @return True if operation succeeded, otherwise false.
     */
    bool Merge(ConfigSection* configSection);

    /**
     * @brief Retrieves a configuration item by name.
     * @param name The name of the sub-value or sub-section
     * @return The resulting configuration item, or null pointer if not found.
     */
    const ConfigItem* GetConfigItem(const char* name) const;

    /**
     * @brief Retrieves a configuration sub-section by name.
     * @param name The name of the sub-section
     * @return The sub-section, or null pointer if not found.
     */
    const ConfigSection* GetConfigSection(const char* name) const;

    /**
     * @brief Queries whether the section contains a configuration item by the given name.
     * @param name The name of the configuration item to query.
     * @return True if the section contains the configuration item.
     */
    inline bool ContainsConfigItem(const char* name) const
    {
        return (ContainsConfigValue(name) || ContainsConfigArray(name));
    }

    /**
     * @brief Queries whether the section contains a configuration value by the given name.
     * @param name The name of the configuration value to query.
     * @return True if the section contains the configuration value.
     */
    inline bool ContainsConfigValue(const char* name) const
    {
        return (m_valueMap.find(name) != m_valueMap.cend());
    }

    /**
     * @brief Queries whether the section contains a configuration array by the given name.
     * @param name The name of the configuration array to query.
     * @return True if the section contains the configuration array.
     */
    inline bool ContainsConfigArray(const char* name) const
    {
        return (m_arrayMap.find(name) != m_arrayMap.cend());
    }

    /**
     * @brief Queries whether the section contains a configuration section by the given name.
     * @param name The name of the configuration section to query.
     * @return True if the section contains the configuration section.
     */
    inline bool ContainsConfigSection(const char* name) const
    {
        return (m_sectionMap.find(name) != m_sectionMap.cend());
    }

    /**
     * @brief Retrieves a typed configuration sub-value by name.
     * @param name The name of the sub-value
     * @return The typed sub-value, or null pointer if not found.
     */
    template <typename T>
    inline const TypedConfigValue<T>* GetConfigValue(const char* name) const
    {
        const TypedConfigValue<T>* result = nullptr;
        ConfigValueMap::const_iterator itr = m_valueMap.find(name);
        if (itr != m_valueMap.cend()) {
            const ConfigValue* configValue = itr->second;
            if (configValue->GetConfigValueType() != ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
                MOT_LOG_ERROR("Unexpected item value type: %s (%s instead of %s %d)",
                    name,
                    ConfigValueTypeToString(configValue->GetConfigValueType()),
                    ConfigValueTypeToString(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE),
                    (int)ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE);
            } else {
                result = static_cast<const TypedConfigValue<T>*>(configValue);
            }
        }
        return result;
    }

    /**
     * @brief Retrieves a typed configuration value.
     * @param name The name of the sub-value
     * @param defaultValue The default value to return if not found.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline T GetUserConfigValue(const char* name, const T& defaultValue) const
    {
        T result = defaultValue;
        const StringConfigValue* configValue = (const StringConfigValue*)GetConfigValue<mot_string>(name);
        if (configValue != nullptr) {
            if (!TypeFormatter<T>::FromString(configValue->GetValue().c_str(), result)) {
                result = defaultValue;
            }
        }
        return result;
    }

    /**
     * @brief Retrieves a typed configuration sub-array by index.
     * @param index The index of the sub-array.
     * @return The typed sub-array, or null pointer if index is out of bounds, or the configuration
     * item in the specified index is not an array.
     */
    const ConfigArray* GetConfigArray(const char* name) const;

    /**
     * @brief Retrieves a configuration item by name for modification purposes.
     * @param name The name of the sub-value or sub-section
     * @return The resulting configuration item, or null pointer if not found.
     */
    ConfigItem* ModifyConfigItem(const char* name);

    /**
     * @brief Retrieves a configuration sub-section by name for modification purposes.
     * @param name The name of the sub-section
     * @return The sub-section, or null pointer if not found.
     */
    ConfigSection* ModifyConfigSection(const char* name);

    /**
     * @brief Retrieves a typed configuration sub-value by name for modification purposes.
     * @param name The name of the sub-value
     * @return The typed sub-value, or null pointer if not found.
     */
    template <typename T>
    inline TypedConfigValue<T>* ModifyConfigValue(const char* name)
    {
        TypedConfigValue<T>* result = nullptr;
        ConfigValueMap::iterator itr = m_valueMap.find(name);
        if (itr != m_valueMap.end()) {
            ConfigValue* configValue = itr->second;
            if (configValue->GetConfigValueType() == ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
                result = static_cast<TypedConfigValue<T>*>(configValue);
            }
        }
        return result;
    }

    /**
     * @brief Retrieves a typed configuration sub-array by index for modification purposes.
     * @param index The index of the sub-array.
     * @return The typed sub-array, or null pointer if index is out of bounds, or the configuration
     * item in the specified index is not an array.
     */
    ConfigArray* ModifyConfigArray(const char* name);

    /**
     * @brief Traverses the section with a visitor applied to each sub-item in the section. This is a
     * recursive call.
     * @param visitor The visitor used to visit each sub-item in the section.
     */
    void ForEach(ConfigItemVisitor& visitor) const;

    /** @var Configuration section map type. */
    typedef mot_map<mot_string, ConfigSection*> ConfigSectionMap;

    /** @var Configuration value map type. */
    typedef mot_map<mot_string, ConfigValue*> ConfigValueMap;

    /** @var Configuration array map type. */
    typedef mot_map<mot_string, ConfigArray*> ConfigArrayMap;

    /** @var Configuration section list type. */
    typedef mot_list<ConfigSection*> ConfigSectionList;

private:
    /** @var Map of all direct sub-sections. */
    ConfigSectionMap m_sectionMap;

    /** @var Map of all direct sub-values */
    ConfigValueMap m_valueMap;

    /** @var Map of all direct sub-arrays */
    ConfigArrayMap m_arrayMap;

    /**
     * @brief Builds the section by adding a sub-section.
     * @param configSection The direct configuration sub-section to add as a child of this section.
     * @return True if the sub-section was added or false if a sub-section by that name already
     * exists.
     */
    bool AddConfigSection(ConfigSection* configSection);

    /**
     * @brief Builds the section by adding a sub-value.
     * @param configValue The direct configuration sub-value to add as a child of this section.
     * @param replaceIfExists Specifies whether to replace an existing item if it already exists by the given name.
     * @return True if the item added or false if a configuration item by that name already exists.
     */
    bool AddConfigValue(ConfigValue* configValue, bool replaceIfExists);

    /**
     * @brief Builds the section by adding a sub-array.
     * @param configArray The direct configuration sub-array to add as a child of this section.
     * @return True if the item added or false if a configuration item by that name already exists.
     */
    bool AddConfigArray(ConfigArray* configArray);

    /**
     * @brief Merges the values of the given section into this section.
     * @param configSection The configuration section to merge.
     * @return True if operation succeeded, otherwise false.
     */
    bool MergeValues(ConfigSection* configSection);

    /**
     * @brief Merges the arrays of the given section into this section.
     * @param configSection The configuration section to merge.
     * @return True if operation succeeded, otherwise false.
     */
    bool MergeArrays(ConfigSection* configSection);

    /**
     * @brief Traverses the section with a visitor applied to each sub-value in the section.
     * @param visitor The visitor used to visit each sub-value in the section.
     */
    void ForEachValue(ConfigItemVisitor& visitor) const;

    /**
     * @brief Traverses the section with a visitor applied to each sub-array in the section. This is a
     * recursive call.
     * @param visitor The visitor used to visit each sub-array in the section.
     */
    void ForEachArray(ConfigItemVisitor& visitor) const;

    /**
     * @brief Traverses the section with a visitor applied to each sub-section in the section. This is
     * a recursive call.
     * @param visitor The visitor used to visit each sub-section in the section.
     */
    void ForEachSection(ConfigItemVisitor& visitor) const;
};

/** @var Configuration section map type. */
typedef ConfigSection::ConfigSectionMap ConfigSectionMap;

/** @var Configuration value map type. */
typedef ConfigSection::ConfigValueMap ConfigValueMap;

/** @var Configuration array map type. */
typedef ConfigSection::ConfigArrayMap ConfigArrayMap;

/** @var Configuration section list type. */
typedef mot_list<ConfigSection*> ConfigSectionList;

// specialization
template <>
struct ConfigItemClassMapper<ConfigSection> {
    /** @var The configuration item class for section types. */
    static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_SECTION;

    /** @var The configuration item class name for configuration sections. */
    static constexpr const char* CONFIG_ITEM_CLASS_NAME = "ConfigSection";
};
}  // namespace MOT

#endif /* CONFIG_SECTION_H */
