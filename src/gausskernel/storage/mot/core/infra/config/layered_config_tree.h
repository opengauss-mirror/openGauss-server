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
 *    src/gausskernel/storage/mot/core/infra/config/layered_config_tree.h
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
        ConfigResult configResult = CONFIG_VALUE_VALID;
        return GetQualifiedConfigItem<ConfigSection>(fullPathName, configResult, true);
    }

    /**
     * @brief Retrieves a typed configuration array.
     * @param fullPathName The full path name of the configuration array.
     * @return The configuration array, or null pointer if none was found by this name.
     */
    inline const ConfigArray* GetConfigArray(const char* fullPathName) const
    {
        ConfigResult configResult = CONFIG_VALUE_VALID;
        return GetQualifiedConfigItem<ConfigArray>(fullPathName, configResult, true);
    }

    /**
     * @brief Retrieves a typed configuration value. For string types use @ref GetStringConfigValue().
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T& GetConfigValue(const char* fullPathName, const T& defaultValue, bool printErrors) const
    {
        ConfigResult configResult = CONFIG_VALUE_VALID;
        const TypedConfigValue<T>* configValue =
            (const TypedConfigValue<T>*)GetQualifiedConfigValue<T>(fullPathName, configResult, printErrors);

        if (!ValidateConfigValue(fullPathName, defaultValue, printErrors, configValue, configResult)) {
            return defaultValue;
        }

        return configValue->GetValue();
    }

    /**
     * @brief Use this variant instead of getConfigValue() template for string members.
     * @details This is due to compiler confusion between the types "const char*" and "mot_string".
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return
     */
    inline const char* GetStringConfigValue(const char* fullPathName, const char* defaultValue, bool printErrors) const
    {
        ConfigResult configResult = CONFIG_VALUE_VALID;
        const StringConfigValue* configValue =
            (const StringConfigValue*)GetQualifiedConfigValue<mot_string>(fullPathName, configResult, printErrors);

        // we cannot use ValidateConfigValue() due to template specialization complexities
        if (configValue == nullptr) {
            if (printErrors) {
                PrintConfigError(fullPathName, defaultValue, configResult);
            }
            return defaultValue;
        }

        if (!configValue->IsValid()) {
            if (printErrors) {
                PrintConfigError(fullPathName, defaultValue, CONFIG_VALUE_INVALID);
            }
            return defaultValue;
        }
        return configValue->GetValue().c_str();
    }

    /**
     * @brief Retrieves a typed configuration value.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline T GetUserConfigValue(const char* fullPathName, const T& defaultValue, bool printErrors) const
    {
        ConfigResult configResult = CONFIG_VALUE_VALID;
        const StringConfigValue* configValue =
            (const StringConfigValue*)GetQualifiedConfigValue<mot_string>(fullPathName, configResult, printErrors);

        if (!ValidateConfigValue(fullPathName, defaultValue, printErrors, configValue, configResult)) {
            return defaultValue;
        }

        T result = defaultValue;
        if (!TypeFormatter<T>::FromString(configValue->GetValue().c_str(), result)) {
            if (printErrors) {
                mot_string stringValue;
                MOT_LOG_ERROR("Failed to convert string configuration item %s to user type, using default value: %s",
                    fullPathName,
                    TypeFormatter<T>::ToString(defaultValue, stringValue));
            }
            return defaultValue;
        }
        return result;
    }

    /**
     * @brief Retrieves an integer configuration value with loose semantics.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return The configuration value, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T GetIntegerConfigValue(const char* fullPathName, const T& defaultValue, bool printErrors) const
    {
        ConfigResult configResult = CONFIG_VALUE_VALID;
        const ConfigValue* configValue = GetQualifiedConfigItem<ConfigValue>(fullPathName, configResult, printErrors);

        if (!ValidateConfigValue(fullPathName, defaultValue, printErrors, configValue, configResult)) {
            return defaultValue;
        }

        switch (configValue->GetConfigValueType()) {
            case ConfigValueType::CONFIG_VALUE_INT64:
                return SafeCast<T, int64_t>((TypedConfigValue<int64_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_INT32:
                return SafeCast<T, int32_t>((TypedConfigValue<int32_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_INT16:
                return SafeCast<T, int16_t>((TypedConfigValue<int16_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_INT8:
                return SafeCast<T, int8_t>((TypedConfigValue<int8_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_UINT64:
                return SafeCast<T, uint64_t>((TypedConfigValue<uint64_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_UINT32:
                return SafeCast<T, uint32_t>((TypedConfigValue<uint32_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_UINT16:
                return SafeCast<T, uint16_t>((TypedConfigValue<uint16_t>*)configValue, defaultValue, printErrors);
            case ConfigValueType::CONFIG_VALUE_UINT8:
                return SafeCast<T, uint8_t>((TypedConfigValue<uint8_t>*)configValue, defaultValue, printErrors);
            default:
                if (printErrors) {
                    mot_string stringValue;
                    if ((configValue->GetConfigValueType() == ConfigValueType::CONFIG_VALUE_STRING) &&
                        (((const StringConfigValue*)configValue)->GetValue().length() == 0)) {
                        const char* defaultValueStr = TypeFormatter<T>::ToString(defaultValue, stringValue);
                        MOT_LOG_WARN("Empty value specified for configuration item %s, using default value: %s",
                            fullPathName,
                            defaultValueStr);
                    } else {
                        MOT_LOG_ERROR("Unexpected non-integer item value type (%s) for configuration item %s, "
                                      "using default value: %s",
                            ConfigValueTypeToString(configValue->GetConfigValueType()),
                            fullPathName,
                            TypeFormatter<T>::ToString(defaultValue, stringValue));
                    }
                }
                return defaultValue;
        }
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

    /** @enum Constants denoting result of retrieving a configuration value. */
    enum ConfigResult {
        /** @var Denotes configuration item not found by the given path. */
        CONFIG_ITEM_NOT_FOUND,

        /** @var Denotes configuration item found, but has wrong item class (not a value, but a section or array). */
        CONFIG_ITEM_CLASS_INVALID,

        /** @var Denotes configuration value found, but value type is not as expected. */
        CONFIG_VALUE_TYPE_INVALID,

        /** @var Denotes configuration value found, but value is empty. */
        CONFIG_VALUE_EMPTY,

        /** @var Denotes configuration value found, but is invalid (probable memory allocation error). */
        CONFIG_VALUE_INVALID,

        /** @var Denotes configuration value found and is valid. */
        CONFIG_VALUE_VALID
    };

    /**
     * @brief Retrieves a qualified configuration item. Item class is checked according to the
     * template type.
     * @param fullPathName The full path name of the configuration array.
     * @param[out] configResult On return, sets the result status of retrieving the configuration value.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return The configuration item, or null pointer if none was found by this name.
     */
    template <typename T>
    inline const T* GetQualifiedConfigItem(const char* fullPathName, ConfigResult& configResult, bool printErrors) const
    {
        const ConfigItem* item = GetConfigItem(fullPathName);
        if (item == nullptr) {
            MOT_LOG_TRACE("Cannot find configuration item: %s", fullPathName);
            configResult = CONFIG_ITEM_NOT_FOUND;
            return nullptr;
        }

        if (item->GetClass() != ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS) {
            if (printErrors) {
                MOT_LOG_ERROR("Unexpected item class: %s (%s instead of %s)",
                    fullPathName,
                    ConfigItemClassToString(item->GetClass()),
                    ConfigItemClassToString(ConfigItemClassMapper<T>::CONFIG_ITEM_CLASS));
            }
            configResult = CONFIG_ITEM_CLASS_INVALID;
            return nullptr;
        }

        return static_cast<const T*>(item);
    }

    /**
     * @brief Retrieves a qualified configuration value. Value type is checked according to the template type.
     * @param fullPathName The full path name of the configuration array.
     * @param[out] configResult On return, sets the result status of retrieving the configuration value.
     * @return The configuration value, or null pointer if none was found by this name, or type does not match.
     */
    template <typename T>
    inline const ConfigValue* GetQualifiedConfigValue(
        const char* fullPathName, ConfigResult& configResult, bool printErrors) const
    {
        const ConfigItem* configItem = GetConfigItem(fullPathName);
        if (configItem == nullptr) {
            MOT_LOG_TRACE("Cannot find configuration item: %s", fullPathName);
            configResult = CONFIG_ITEM_NOT_FOUND;
            return nullptr;
        }

        // check correct item type
        if (configItem->GetClass() != ConfigItemClass::CONFIG_ITEM_VALUE) {
            if (printErrors) {
                MOT_LOG_ERROR("Unexpected configuration item class: %s (%s instead of %s)",
                    fullPathName,
                    ConfigItemClassToString(configItem->GetClass()),
                    ConfigItemClassToString(ConfigItemClass::CONFIG_ITEM_VALUE));
            }
            configResult = CONFIG_ITEM_CLASS_INVALID;
            return nullptr;
        }

        // check empty string, regardless of value type correctness
        const ConfigValue* configValue = static_cast<const ConfigValue*>(configItem);
        if ((configValue->GetConfigValueType() == ConfigValueType::CONFIG_VALUE_STRING) &&
            (((const StringConfigValue*)configValue)->GetValue().length() == 0)) {
            // warning is printed in a nicer way be caller
            MOT_LOG_TRACE("Empty configuration item value: %s", fullPathName);
            configResult = CONFIG_VALUE_EMPTY;
            return nullptr;
        }

        // check correct value type
        if (configValue->GetConfigValueType() != ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE) {
            if (printErrors) {
                MOT_LOG_ERROR("Unexpected configuration item value type: %s (%s instead of %s)",
                    fullPathName,
                    ConfigValueTypeToString(configValue->GetConfigValueType()),
                    ConfigValueTypeToString(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE));
            }
            configResult = CONFIG_VALUE_TYPE_INVALID;
            return nullptr;
        }

        configResult = CONFIG_VALUE_VALID;
        return configValue;
    }

    /**
     * @brief Helper functions for validating a configuration value.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValue The default value to return if not found.
     * @param printErrors Specifies whether to print error and warning messages.
     * @param configValue The configuration value to validate.
     * @param configResult The result of retrieving the configuration value.
     * @return True if the configuration value is valid.
     */
    template <typename T>
    inline bool ValidateConfigValue(const char* fullPathName, const T& defaultValue, bool printErrors,
        const ConfigValue* configValue, ConfigResult configResult) const
    {
        // if value retrieval failed, then print error (unless value not specified in configuration file)
        mot_string stringValue;
        if (configValue == nullptr) {
            if (printErrors) {
                PrintConfigError(fullPathName, TypeFormatter<T>::ToString(defaultValue, stringValue), configResult);
            }
            return false;
        }

        // in case of string configuration value we need to check whether it is valid
        if (!configValue->IsValid()) {
            if (printErrors) {
                PrintConfigError(
                    fullPathName, TypeFormatter<T>::ToString(defaultValue, stringValue), CONFIG_VALUE_INVALID);
            }
            return false;
        }
        return true;
    }

    /**
     * @brief Helper function for printing configuration errors.
     * @param fullPathName The full path name of the configuration value.
     * @param defaultValueStr The string representation of the default value used on error.
     * @param configResult The result of retrieving the configuration value.
     */
    inline void PrintConfigError(const char* fullPathName, const char* defaultValueStr, ConfigResult configResult) const
    {
        // Either user did not specify configuration at all, or item class is invalid, or value type is invalid,
        // or value is empty. In case of no configuration at all we keep silent. In case of invalid item class or
        // invalid value type, error is already printed.
        if (configResult == CONFIG_VALUE_EMPTY) {
            if (defaultValueStr && defaultValueStr[0]) {  // if default is empty string then keep silent
                MOT_LOG_WARN("Empty value specified for configuration item %s, using default value: %s",
                    fullPathName,
                    defaultValueStr);
            }
        } else if (configResult == CONFIG_VALUE_INVALID) {
            MOT_LOG_ERROR("Configuration value of %s is invalid (OOM in string allocation?), using default value: %s",
                fullPathName,
                defaultValueStr);
        } else if (configResult != CONFIG_ITEM_NOT_FOUND) {
            MOT_LOG_WARN("Invalid configuration item %s (mismatching class or type), using default value: %s",
                fullPathName,
                defaultValueStr);
        }
    }

    /**
     * @brief Helper function for safe-cast between types that checks for overflow.
     * @param configValue The configuration value (source value).
     * @param defaultValue The default value to use in case of overflow.
     * @param printErrors Specifies whether to print error and warning messages.
     * @return The resulting value, given in the target type.
     */
    template <typename T, typename U>
    static inline T SafeCast(TypedConfigValue<U>* configValue, T defaultValue, bool printErrors)
    {
        T result = (T)configValue->GetValue();  // use compiler generated cast
        U check = (U)result;                    // cast back to check for overflow
        if (check != configValue->GetValue()) {
            if (printErrors) {
                mot_string stringValue;
                MOT_LOG_ERROR("Overflow in integer configuration value %s, using default value: %s",
                    configValue->GetFullPathName(),
                    TypeFormatter<T>::ToString(defaultValue, stringValue));
            }
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
    bool AddNewConfigTreeAt(const LayeredConfigTrees::iterator& itr, ConfigTree* configTree);

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
