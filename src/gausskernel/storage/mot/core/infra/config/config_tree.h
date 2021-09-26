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
 * config_tree.h
 *    Configuration tree implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_tree.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_TREE_H
#define CONFIG_TREE_H

#include "config_array.h"
#include "config_item_visitor.h"
#include "config_section.h"
#include "type_formatter.h"
#include "typed_config_value.h"
#include "infra_util.h"

namespace MOT {
/**
 * @class ConfigTree
 * @brief The main entry point to any loaded configuration. It contains the root
 * section. The root section is a virtual section holding all main (top-level)
 * configuration sections.
 */
class ConfigTree {
public:
    /**
     * @brief Constructor.
     * @param priority The priority of the configuration tree. Derived from the priority of the
     * originating loader.
     * @param isStatic Specifies whether this is a static (immutable) configuration tree or not.
     */
    ConfigTree(uint32_t priority, bool isStatic) : m_priority(priority), m_isStatic(isStatic)
    {}

    /** Destructor. */
    virtual ~ConfigTree()
    {
        MOT_LOG_DEBUG("Destroying configuration tree %p %s [name@%p]", this, GetSource(), GetSource());
    }

    /**
     * @brief Initializes the configuration tree
     * @param source The name of the configuration source.
     * @return True if initialization succeeded, otherwise false.
     */
    inline bool Initialize(const char* source)
    {
        bool result = false;
        if (!m_source.assign(source)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to assign configuration tree source to %s", source);
        } else if (!m_rootSection.Initialize("", "")) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to initialize root section");
        } else {
            result = true;
        }
        return result;
    }

    /**
     * @brief Factory method.
     * @param priority The priority of the configuration tree. Derived from the priority of the
     * originating loader.
     * @param source The name of the configuration loader from which this configuration tree
     * originated.
     * @param isStatic Specifies whether this is a static (immutable) configuration tree or not.
     * @return The created configuration tree if succeeded, otherwise a null pointer.
     */
    static ConfigTree* CreateConfigTree(uint32_t priority, const char* source, bool isStatic)
    {
        ConfigTree* cfgTree = new (std::nothrow) ConfigTree(priority, isStatic);
        if (cfgTree == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to allocate memory for %s configuration tree", source);
        } else if (!cfgTree->Initialize(source)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to initialize %s configuration tree", source);
            delete cfgTree;
            cfgTree = nullptr;
        }
        return cfgTree;
    }

    /**
     * @brief Retrieves the priority of the configuration tree.
     * @return The priority of the configuration tree.
     */
    inline uint32_t GetPriority() const
    {
        return m_priority;
    }

    /**
     * @brief Queries whther this is a static (immutable) configuration tree. A static configuraiton
     * tree does not change during the lifetime of the application.
     * @return True if this is a static configuration tree, otherwise false.
     */
    inline bool IsStatic() const
    {
        return m_isStatic;
    }

    /**
     * @brief Retrieves the source name of the configuration tree.
     * @return The configuration tree source name.
     */
    inline const char* GetSource() const noexcept
    {
        return m_source.c_str();
    }

    /**
     * @brief Builds the tree from a list of parsed sections.
     * @param parsedSections The list of parsed sections.
     * @return True if operation succeeded, otherwise false.
     */
    bool Build(mot_list<ConfigSection*>& parsedSections);

    /**
     * @brief Adds a main section to the root section.
     * @param configSection
     * @return True if the section was added, or false if another main section by
     * the same name already exists.
     */
    inline bool AddMainSection(ConfigSection* configSection)
    {
        return m_rootSection.AddConfigItem(configSection);
    }

    /**
     * @brief Retrieves the virtual root section.
     * @return The root section.
     */
    inline const ConfigSection& GetRootSection() const
    {
        return m_rootSection;
    }

    /**
     * @brief Prints the configuration tree to the log.
     * @param logLevel The log level used for printing.
     */
    inline void Print(LogLevel logLevel) const
    {
        m_rootSection.Print(logLevel, false);
    }

    /**
     * @brief Retrieves a configuration item by its full path name.
     * @param fullPathName The full path name of the configuration item.
     * @return The configuration item, or null pointer if none was found by this name.
     */
    const ConfigItem* GetConfigItem(const char* fullPathName) const;

    /**
     * @brief Retrieves a configuration section by its full path name.
     * @param fullPathName The full path name of the configuration section.
     * @return The configuration section, or null pointer if none was found by this name.
     */
    inline const ConfigSection* GetConfigSection(const char* fullPathName) const
    {
        return GetQualifiedConfigItem<ConfigSection>(fullPathName);
    }

    /**
     * @brief Retrieves a typed configuration array.
     * @param fullPathName The full path name of the configuration array.
     * @return The configuration array, or null pointer if none was found by this name.
     */
    inline const ConfigArray* GetConfigArray(const char* fullPathName) const
    {
        return GetQualifiedConfigItem<ConfigArray>(fullPathName);
    }

    /**
     * @brief Retrieves a typed configuration value.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if none was find.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T& GetConfigValue(const char* fullPathName, const T& defaultValue) const
    {
        const TypedConfigValue<T>* configValue = GetQualifiedConfigItem<TypedConfigValue<T>>(fullPathName);
        if (configValue == nullptr) {
            MOT_LOG_ERROR("Failed to find configuration item: %s", fullPathName);
        } else if (configValue->GetConfigValueType() != ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
            MOT_LOG_ERROR("Unexpected item value type: %s (%s instead of %s %d)",
                fullPathName,
                ConfigValueTypeToString(configValue->GetConfigValueType()),
                ConfigValueTypeToString(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE),
                (int)ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE);
        } else {
            return configValue->GetValue();
        }
        return defaultValue;
    }

    /**
     * @brief Use this variant instead of getConfigValue() template for string members.
     * @details This is due to compiler confusion between the types "const char*" and "string".
     * @param fullPathName
     * @param defaultValue
     * @return
     */
    inline const char* GetStringConfigValue(const char* fullPathName, const char* defaultValue) const
    {
        const char* result = defaultValue;
        const StringConfigValue* configValue = GetQualifiedConfigItem<StringConfigValue>(fullPathName);
        if (configValue != nullptr) {
            result = configValue->GetValue().c_str();
        }
        return result;
    }

    /**
     * @brief Retrieves a typed configuration value.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param parser A user-provided parsing function to convert string to user data type.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline T GetUserConfigValue(const char* fullPathName, const T& defaultValue) const
    {
        T result = defaultValue;
        const StringConfigValue* configValue = GetQualifiedConfigItem<StringConfigValue>(fullPathName);
        if (configValue != nullptr) {
            if (!TypeFormatter<T>::FromString(configValue->GetValue().c_str(), result)) {
                result = defaultValue;
            }
        }
        return result;
    }

    /**
     * @brief Traverses the tree with a visitor applied to each item in the tree.
     * @param visitor The visitor used to visit each node in the tree.
     */
    inline void ForEach(ConfigItemVisitor& visitor) const
    {
        m_rootSection.ForEach(visitor);
    }

private:
    /** @var The priority of the configuration tree. */
    uint32_t m_priority;

    /** @var Specifies whether this is a static (immutable) configuration tree or not. */
    bool m_isStatic;

    /** @var The name of the configuration loader from which this configuration tree originated. */
    mot_string m_source;

    /** @var The virtual root. */
    ConfigSection m_rootSection;

    /**
     * @brief Retrieves a qualified configuration item. Item class is checked according to the
     * template type.
     * @param fullPathName The full path name of the configuration array.
     * @return The configuration item, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T* GetQualifiedConfigItem(const char* fullPathName) const
    {
        const T* result = nullptr;
        const ConfigItem* item = GetConfigItem(fullPathName);
        if ((item != nullptr) && (item->GetClass() == ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS)) {
            result = static_cast<const T*>(item);
        }
        return result;
    }

    /**
     * @brief Consolidates the parsed section list into a section map, while merging all duplicate sections.
     * @param parsedSections The parsed section list.
     * @param[out] sectionMap The resulting consolidated section map.
     * @return True if the operations succeeded, otherwise false.
     */
    bool ConsolidateParsedSections(mot_list<ConfigSection*>& parsedSections, ConfigSectionMap& sectionMap);

    /**
     * @brief Builds the configuration tree from a consolidated configuration section map.
     * @param sectionMap The section map.
     * @return True if the operations succeeded, otherwise false.
     */
    bool LinkConsolidatedSectionMap(ConfigSectionMap& sectionMap);

    /**
     * @brief Retrieves the parent of a section or creates it (recursively).
     * @param section The section whose parent is to be retrieved or created.
     * @param depth The call depth. Used for preventing endless recurrence.
     * @param created Specifies whether the parent section was created.
     * @return The parent section or null if failed.
     */
    ConfigSection* GetOrCreateParent(ConfigSection* section, int depth, bool& created);

    /**
     * @brief Creates the parent of a section (recursively).
     * @param section The section whose parent is to be created.
     * @param depth The call depth. Used for preventing endless recurrence.
     * @return The parent section or null if failed.
     */
    ConfigSection* CreateParent(ConfigSection* section, int depth);

    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* CONFIGTREE_H */
