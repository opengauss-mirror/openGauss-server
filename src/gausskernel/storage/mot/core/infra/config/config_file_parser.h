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
 * config_file_parser.h
 *    Helper class for parsing configuration files.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_file_parser.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_FILE_PARSER_H
#define CONFIG_FILE_PARSER_H

#include "config_item.h"
#include "infra.h"
#include "infra_util.h"

namespace MOT {
/**
 * @class ConfigFileParser
 * @brief Helper class for parsing configuration files.
 */
class ConfigFileParser {
public:
    /**
     * @brief Breaks a full section name into its path and name components.
     * @param sectionFullName The full section name.
     * @param[out] sectionPath The resulting section path.
     * @param[out] sectionName The resulting section name.
     * @param[opt] sep Section separator.
     * @return True if parsing succeeded, otherwise false.
     */
    static bool BreakSectionName(const mot_string& sectionFullName, mot_string& sectionPath, mot_string& sectionName,
        char sep = ConfigItem::PATH_SEP);

    /**
     * @brief Parses a <KEY>=<VALUE> line into key and value.
     * @param keyValuePart The part of the line to parse.
     * @param section The containing section (for error message in case of failure).
     * @param[out] key The resulting key.
     * @param[out] value The resulting value.
     * @param[out] arrayIndex The array index in case of array item.
     * @param[out] hasArrayIndex Returns true if the key specifies an array item.
     * @return True if parsing was successful.
     */
    static bool ParseKeyValue(const mot_string& keyValuePart, const mot_string& section, mot_string& key,
        mot_string& value, uint64_t& arrayIndex, bool& hasArrayIndex);

    /**
     * @brief Creates a typed configuration value.
     * @param path The path of the configuration item.
     * @param name The name of the configuration item (the key).
     * @param value The value of the configuration item.
     * @return The resulting configuration item, or null pointer if failed.
     */
    static ConfigItem* MakeConfigValue(const mot_string& path, const mot_string& name, const mot_string& value);

    /**
     * @brief Creates a typed configuration value as an array item.
     * @param path The path of the configuration item.
     * @param arrayIndex The index of the configuration value within its
     * containing array.
     * @param value The value of the configuration item.
     * @return The resulting configuration item, or null pointer if failed.
     */
    static ConfigItem* MakeArrayConfigValue(const mot_string& path, uint64_t arrayIndex, const mot_string& value);

    /**
     * @brief Creates a signed integer configuration value.
     * @param path The path of the configuration item.
     * @param name The name of the configuration item (the key).
     * @param value The value of the configuration item.
     * @return The resulting configuration item, or null pointer if failed.
     */
    static ConfigItem* MakeIntConfigValue(const mot_string& path, const mot_string& name, int64_t value);

    /**
     * @brief Creates an unsigned integer configuration value.
     * @param path The path of the configuration item.
     * @param name The name of the configuration item (the key).
     * @param value The value of the configuration item.
     * @return The resulting configuration item, or null pointer if failed.
     */
    static ConfigItem* MakeUIntConfigValue(const mot_string& path, const mot_string& name, uint64_t value);

    /**
     * @brief Splits a mot_string into sub-strings according to a given separator.
     * @param str The mot_string to split.
     * @param sep The separator used to divide tokens.
     * @param tokens The resulting tokens.
     */
    static bool Split(const char* str, char sep, mot_string_list& tokens);

private:
    /**
     * @brief Creates a typed configuration value, according to type name.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeTypedConfigValue(
        const mot_string& sectionFullName, const mot_string& typeName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates an untyped configuration value, attempting to guess the most fitting type.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUntypedConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a singed integer configuration value, according to most restrictive size possible, considering the
     * actual given value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeIntConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates an unsinged integer configuration value, according to most restrictive size possible, considering
     * the actual given value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUIntConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a double-precision floating-point configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeDoubleConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a boolean configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item. Possible values are (case insensitive): true/false, yes/no,
     * and on/off.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeBoolConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 64-bit signed integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeInt64ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 32-bit signed integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeInt32ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 16-bit signed integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeInt16ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 8-bit signed integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeInt8ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 64-bit unsigned integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUInt64ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 32-bit unsigned integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUInt32ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 16-bit unsigned integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUInt16ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Creates a 8-bit unsigned integer configuration value.
     * @param sectionFullName The full path name of the section containing the value.
     * @param value The value of the configuration item.
     * @return The configuration item, or null if failed.
     */
    static ConfigItem* MakeUInt8ConfigValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value);

    /**
     * @brief Parses a signed integer value given in string form.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The string value of the configuration item.
     * @param[out] intValue The resulting integer value.
     * @return True if parsing succeeded, otherwise false.
     */
    static bool ParseIntValue(
        const mot_string& sectionFullName, const mot_string& key, const mot_string& value, int64_t& intValue);

    /**
     * @brief Parses an unsigned integer value given in string form.
     * @param sectionFullName The full path name of the section containing the value.
     * @param key The key of the configuration item.
     * @param value The string value of the configuration item.
     * @param[out] intValue The resulting integer value.
     * @param[opt] arrayIndex Specifies whether the parsed integer is a configuration value or an array index (used
     * only for formatting error messages).
     * @return True if parsing succeeded, otherwise false.
     */
    static bool ParseUIntValue(const mot_string& sectionFullName, const mot_string& key, const mot_string& value,
        uint64_t& intValue, bool arrayIndex = false);

    // declare a class-level logger
    DECLARE_CLASS_LOGGER()
};
}  // namespace MOT

#endif /* CONFIG_FILE_PARSER_H */
