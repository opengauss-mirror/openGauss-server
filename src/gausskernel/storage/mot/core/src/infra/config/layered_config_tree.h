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
 * layered_config_tree.h
 *    Implements layering of prioritized configuration trees.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/infra/config/layered_config_tree.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LAYERED_CONFIG_TREE_H
#define LAYERED_CONFIG_TREE_H

#include "config_tree.h"

namespace MOT {

/**
 * @class LayeredConfigTree
 * @brief Implements layering of prioritized configuration trees.
 */
class LayeredConfigTree {
public:
    /** @brief Constructor. */
    LayeredConfigTree()
    {}

    /** @brief Destructor. */
    ~LayeredConfigTree();

    /**
     * @brief Adds a prioritized configuration tree.
     * @param configTree The configuration tree to add.
     * @return True if the operation succeeded, otherwise false.
     */
    bool AddConfigTree(ConfigTree* configTree);

    /**
     * @brief Removes a prioritized configuration tree.
     * @param configTree The configuration tree to remove.
     */
    void RemoveConfigTree(ConfigTree* configTree);

    /**
     * @brief Removes all installed prioritized trees.
     */
    inline void Clear()
    {
        ClearConfigTrees();
        m_printList.clear();
    }

    /**
     * @brief Prints the layered configuration tree to the log.
     * @param logLevel The log level used for printing.
     */
    void Print(LogLevel logLevel) const;

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
    inline const ConfigArray* getConfigArray(const char* fullPathName) const
    {
        return GetQualifiedConfigItem<ConfigArray>(fullPathName);
    }

    /**
     * @brief Retrieves a typed configuration value.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T& GetConfigValue(const char* fullPathName, const T& defaultValue) const
    {
        const TypedConfigValue<T>* configValue = GetQualifiedConfigItem<TypedConfigValue<T>>(fullPathName);
        if (configValue != nullptr) {
            if (configValue->GetConfigValueType() != ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
                MOT_LOG_ERROR("Unexpected item value type: %s (%s instead of %s %d)",
                    fullPathName,
                    ConfigValueTypeToString(configValue->GetConfigValueType()),
                    ConfigValueTypeToString(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE),
                    (int)ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE);
            } else {
                return configValue->GetValue();
            }
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
     * @brief Retrieves an integer configuration value with loose semantics.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T GetIntegerConfigValue(const char* fullPathName, const T& defaultValue) const
    {
        const ConfigValue* configValue = GetQualifiedConfigItem<ConfigValue>(fullPathName);
        if (configValue) {
            switch (configValue->GetConfigValueType()) {
                case ConfigValueType::CONFIG_VALUE_INT64:
                    return SafeCast<T, int64_t>((TypedConfigValue<int64_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_INT32:
                    return SafeCast<T, int32_t>((TypedConfigValue<int32_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_INT16:
                    return SafeCast<T, int16_t>((TypedConfigValue<int16_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_INT8:
                    return SafeCast<T, int8_t>((TypedConfigValue<int8_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_UINT64:
                    return SafeCast<T, uint64_t>((TypedConfigValue<uint64_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_UINT32:
                    return SafeCast<T, uint32_t>((TypedConfigValue<uint32_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_UINT16:
                    return SafeCast<T, uint16_t>((TypedConfigValue<uint16_t>*)configValue, defaultValue);
                case ConfigValueType::CONFIG_VALUE_UINT8:
                    return SafeCast<T, uint8_t>((TypedConfigValue<uint8_t>*)configValue, defaultValue);
                default:
                    MOT_LOG_ERROR("Unexpected non-integer item value type: %s (instead it is %s)",
                        fullPathName,
                        ConfigValueTypeToString(configValue->GetConfigValueType()),
                        ConfigValueTypeToString(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE));
                    return defaultValue;
            }
        }
        return defaultValue;
    }

    /** @typedef List of configuration tree sharing the same priority. */
    typedef mot_list<ConfigTree*> ConfigTreeList;

    /** @typedef The data structure for holding layered configuration trees. */
    typedef mot_list<ConfigTreeList> LayeredConfigTrees;

private:
    /** @var Layered configuration trees. */
    LayeredConfigTrees m_configTrees;

    /** @var Flat configuration item list ready for printing. */
    ConfigItemList m_printList;

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
        if (item == nullptr) {
            MOT_LOG_TRACE("Cannot find configuration item: %s", fullPathName);
        } else if (item->GetClass() != ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS) {
            MOT_LOG_ERROR("Unexpected item class: %s (%s instead of %s %d)",
                fullPathName,
                ConfigItemClassToString(item->GetClass()),
                ConfigItemClassToString(ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS),
                (int)ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS);
        } else {
            result = static_cast<const T*>(item);
        }
        return result;
    }

    template <typename T, typename U>
    static inline T SafeCast(TypedConfigValue<U>* configValue, T defaultValue)
    {
        T result = (T)configValue->GetValue();  // use compiler generated cast
        U check = (U)result;                    // cast back to check for overflow
        if (check != configValue->GetValue()) {
            MOT_LOG_ERROR("Overflow in integer configuration value: %s", configValue->GetFullPathName());
            result = defaultValue;
        }
        return result;
    }

    /**
     * @brief Rebuilds the flat configuration item list for printing.
     * @return True if operation succeeded, otherwise false.
     */
    bool BuildPrintList();

    /**
     * @brief Releases all resources associated with all configuration trees.
     */
    void ClearConfigTrees();

    /**
     * @brief Adds a configuration tree to a new configuration tree list at the specified position
     * (before the configuration tree list pointed by the iterator).
     * @param itr The iterator denoting the insertion position.
     * @param configTree The configuration tree to insert.
     * @return True if insertion succeeded, otherwise false.
     */
    bool AddNewConfigTreeAt(LayeredConfigTrees::iterator itr, ConfigTree* configTree);

    /**
     * @brief Adds a configuration tree to an existing list of configuration trees.
     * @param configTree The configuration tree to add.
     * @param configTreeList The configuration tree list.
     * @return True if insertion succeeded, otherwise false.
     */
    bool AddConfigTreeToList(ConfigTree* configTree, ConfigTreeList& configTreeList);

    DECLARE_CLASS_LOGGER()
};

}  // namespace MOT

#endif /* LAYERED_CONFIG_TREE_H */
