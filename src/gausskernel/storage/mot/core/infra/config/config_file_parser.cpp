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
 * config_file_parser.cpp
 *    Helper class for parsing configuration files.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_file_parser.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_file_parser.h"
#include "typed_config_value.h"
#include "utilities.h"
#include "mot_error.h"

#include <strings.h>
#include <errno.h>
#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdlib>

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ConfigFileParser, Configuration)

bool ConfigFileParser::BreakSectionName(const mot_string& sectionFullName, mot_string& sectionPath,
    mot_string& sectionName, char sep /* = ConfigItem::PATH_SEP */)
{
    bool result = true;
    uint32_t lastSlashPos = sectionFullName.find_last_of(sep);
    if (lastSlashPos != mot_string::npos) {
        if (!sectionFullName.substr(sectionPath, 0, lastSlashPos) ||
            !sectionFullName.substr(sectionName, lastSlashPos + 1)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to parse section name");
            result = false;
        } else {
            sectionPath.trim();
            sectionName.trim();
        }
    } else if (!sectionPath.assign("") || !sectionName.assign(sectionFullName)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to parse section name");
        result = false;
    }
    return result;
}

bool ConfigFileParser::ParseKeyValue(const mot_string& keyValuePart, const mot_string& section, mot_string& key,
    mot_string& value, uint64_t& arrayIndex, bool& hasArrayIndex)
{
    bool result = false;
    hasArrayIndex = false;
    uint32_t equalPos = keyValuePart.find('=');
    if (equalPos == mot_string::npos) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to parse key/value: missing equal sign (%s)",
            keyValuePart.c_str());
    } else if (!keyValuePart.substr(key, 0, equalPos) || !keyValuePart.substr(value, equalPos + 1)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse key/value");
    } else {
        key.trim();
        value.trim();
        result = true;

        // check for array item
        uint32_t poundPos = key.find('#');
        if (poundPos != mot_string::npos) {
            result = false;
            mot_string intPart;
            if (!key.substr(intPart, poundPos + 1)) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse integer part from array item specifier");
            } else {
                if (ParseUIntValue(section, key, intPart, arrayIndex, true)) {
                    hasArrayIndex = true;
                    key.substr_inplace(0, poundPos);
                    result = true;
                } else {
                    MOT_REPORT_ERROR(MOT_ERROR_INVALID_CFG,
                        "Load Configuration",
                        "Invalid array item specifier encountered: %s",
                        key.c_str());
                }
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    ConfigItem* result = nullptr;

    // key may have type specifier (in the format type:key=value)
    uint32_t colonPos = key.find(':');
    if (colonPos != mot_string::npos) {
        mot_string typeName;
        mot_string keyName;
        if (!key.substr(typeName, 0, colonPos) || !key.substr(keyName, colonPos + 1)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse typed key %s", key.c_str());
        } else {
            result = MakeTypedConfigValue(sectionFullName, typeName, keyName, value);
        }
    } else {
        result = MakeUntypedConfigValue(sectionFullName, key, value);
    }

    return result;
}

ConfigItem* ConfigFileParser::MakeArrayConfigValue(const mot_string& path, uint64_t arrayIndex, const mot_string& value)
{
    ConfigItem* result = nullptr;
    mot_string key;
    if (!key.format("%lu", arrayIndex)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to format array index %lu", arrayIndex);
    } else {
        result = MakeConfigValue(path, key, value);
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeIntConfigValue(const mot_string& path, const mot_string& name, int64_t value)
{
    // try to parse value as integer starting from smallest type
    ConfigItem* result = nullptr;

    if ((value >= SCHAR_MIN) && (value <= SCHAR_MAX)) {
        result = CreateConfigValue<int8_t>(path.c_str(), name.c_str(), (int8_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int8 configuration value");
        }
    } else if ((value >= SHRT_MIN) && (value <= SHRT_MAX)) {
        result = CreateConfigValue<int16_t>(path.c_str(), name.c_str(), (int16_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int16 configuration value");
        }
    } else if ((value >= INT_MIN) && (value <= INT_MAX)) {
        result = CreateConfigValue<int32_t>(path.c_str(), name.c_str(), (int32_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int32 configuration value");
        }
    } else {
        result = CreateConfigValue<int64_t>(path.c_str(), name.c_str(), value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int64 configuration value");
        }
    }

    return result;
}

ConfigItem* ConfigFileParser::MakeUIntConfigValue(const mot_string& path, const mot_string& name, uint64_t value)
{
    // try to parse value as unsigned integer starting from smallest type
    ConfigItem* result = nullptr;

    if (value <= UCHAR_MAX) {
        result = CreateConfigValue<uint8_t>(path.c_str(), name.c_str(), (uint8_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint8 configuration value");
        }
    } else if (value <= USHRT_MAX) {
        result = CreateConfigValue<uint16_t>(path.c_str(), name.c_str(), (uint16_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint16 configuration value");
        }
    } else if (value <= UINT_MAX) {
        result = CreateConfigValue<uint32_t>(path.c_str(), name.c_str(), (uint32_t)value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint32 configuration value");
        }
    } else {
        result = CreateConfigValue<uint64_t>(path.c_str(), name.c_str(), value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint64 configuration value");
        }
    }

    return result;
}

bool ConfigFileParser::Split(const char* str, char sep, mot_string_list& tokens)
{
    do {
        const char* begin = str;
        while (*str != sep && *str) {
            str++;
        }

        // skip empty items
        if (str != begin) {
            mot_string token;
            if (!token.assign(begin, str - begin)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to push a token");
                return false;
            } else {
                if (!tokens.push_back(token)) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to push a token");
                    return false;
                }
            }
        }
    } while (*str++ != 0);
    return true;
}

ConfigItem* ConfigFileParser::MakeIntConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as integer
    ConfigItem* result = nullptr;
    int64_t intValue = 0;
    if (ParseIntValue(sectionFullName, key, value, intValue)) {
        if ((intValue >= SCHAR_MIN) && (intValue <= SCHAR_MAX)) {
            result = CreateConfigValue<int8_t>(sectionFullName.c_str(), key.c_str(), (int8_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int8 configuration value");
            }
        } else if ((intValue >= SHRT_MIN) && (intValue <= SHRT_MAX)) {
            result = CreateConfigValue<int16_t>(sectionFullName.c_str(), key.c_str(), (int16_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int16 configuration value");
            }
        } else if ((intValue >= INT_MIN) && (intValue <= INT_MAX)) {
            result = CreateConfigValue<int32_t>(sectionFullName.c_str(), key.c_str(), (int32_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int32 configuration value");
            }
        } else {
            result = CreateConfigValue<int64_t>(sectionFullName.c_str(), key.c_str(), intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int64 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUIntConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as unsigned integer
    ConfigItem* result = nullptr;
    uint64_t intValue = 0;
    if (ParseUIntValue(sectionFullName, key, value, intValue)) {
        if (intValue <= UCHAR_MAX) {
            result = CreateConfigValue<uint8_t>(sectionFullName.c_str(), key.c_str(), (uint8_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint8 configuration value");
            }
        } else if (intValue <= USHRT_MAX) {
            result = CreateConfigValue<uint16_t>(sectionFullName.c_str(), key.c_str(), (uint16_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint16 configuration value");
            }
        } else if (intValue <= UINT_MAX) {
            result = CreateConfigValue<uint32_t>(sectionFullName.c_str(), key.c_str(), (uint32_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint32 configuration value");
            }
        } else {
            result = CreateConfigValue<uint64_t>(sectionFullName.c_str(), key.c_str(), intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint64 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeDoubleConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as double
    ConfigItem* result = nullptr;
    char* endptr = NULL;
    double doubleValue = strtod(value.c_str(), &endptr);
    if (*value.c_str() == 0) {  // empty input
        MOT_LOG_TRACE("Configuration double at [section %s, key %s] is empty", sectionFullName.c_str(), key.c_str());
    } else if (endptr == value.c_str()) {  // no valid digits
        MOT_LOG_TRACE("Configuration double at [section %s, key %s] has no valid digits at all: %s",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if (*endptr != 0) {  // some invalid trailing digits
        MOT_LOG_TRACE("Configuration double at [section %s, key %s] has invalid trailing characters: %s",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if (((doubleValue == HUGE_VALF) || (doubleValue == HUGE_VALL)) && (errno == ERANGE)) {  // overflow
        MOT_LOG_TRACE("Configuration double at [section %s, key %s] overflows: %s",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else {
        result = CreateConfigValue<double>(sectionFullName.c_str(), key.c_str(), doubleValue);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate double configuration value");
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeBoolConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as Boolean
    ConfigItem* result = nullptr;
    if ((strcasecmp(value.c_str(), "true") == 0) || (strcasecmp(value.c_str(), "on") == 0) ||
        (strcasecmp(value.c_str(), "yes") == 0)) {
        result = CreateConfigValue<bool>(sectionFullName.c_str(), key.c_str(), true);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate Boolean configuration value");
        }
    } else if ((strcasecmp(value.c_str(), "false") == 0) || (strcasecmp(value.c_str(), "off") == 0) ||
               (strcasecmp(value.c_str(), "no") == 0)) {
        result = CreateConfigValue<bool>(sectionFullName.c_str(), key.c_str(), false);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate Boolean configuration value");
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeInt64ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as int64_t
    ConfigItem* result = nullptr;
    int64_t intValue = 0;
    if (ParseIntValue(sectionFullName, key, value, intValue)) {
        result = CreateConfigValue<int64_t>(sectionFullName.c_str(), key.c_str(), intValue);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int64 configuration value");
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeInt32ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as int32_t
    ConfigItem* result = nullptr;
    int64_t intValue = 0;
    if (ParseIntValue(sectionFullName, key, value, intValue)) {
        if ((intValue >= INT_MIN) && (intValue <= INT_MAX)) {
            result = CreateConfigValue<int32_t>(sectionFullName.c_str(), key.c_str(), (int32_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int32 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeInt16ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as int16_t
    ConfigItem* result = nullptr;
    int64_t intValue = 0;
    if (ParseIntValue(sectionFullName, key, value, intValue)) {
        if ((intValue >= SHRT_MIN) && (intValue <= SHRT_MAX)) {
            result = CreateConfigValue<int16_t>(sectionFullName.c_str(), key.c_str(), (int16_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int16 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeInt8ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as int8_t
    ConfigItem* result = nullptr;
    int64_t intValue = 0;
    if (ParseIntValue(sectionFullName, key, value, intValue)) {
        if ((intValue >= SCHAR_MIN) && (intValue <= SCHAR_MAX)) {
            result = CreateConfigValue<int8_t>(sectionFullName.c_str(), key.c_str(), (int8_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate int8 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUInt64ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as uint64_t
    ConfigItem* result = nullptr;
    uint64_t intValue = 0;
    if (ParseUIntValue(sectionFullName, key, value, intValue)) {
        result = CreateConfigValue<uint64_t>(sectionFullName.c_str(), key.c_str(), intValue);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint64 configuration value");
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUInt32ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as uint32_t
    ConfigItem* result = nullptr;
    uint64_t intValue = 0;
    if (ParseUIntValue(sectionFullName, key, value, intValue)) {
        if (intValue <= UINT_MAX) {
            result = CreateConfigValue<uint32_t>(sectionFullName.c_str(), key.c_str(), (uint32_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint32 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUInt16ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as uint16_t
    ConfigItem* result = nullptr;
    uint64_t intValue = 0;
    if (ParseUIntValue(sectionFullName, key, value, intValue)) {
        if (intValue <= USHRT_MAX) {
            result = CreateConfigValue<uint16_t>(sectionFullName.c_str(), key.c_str(), (uint16_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint16 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUInt8ConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to parse value as uint8_t
    ConfigItem* result = nullptr;
    uint64_t intValue = 0;
    if (ParseUIntValue(sectionFullName, key, value, intValue)) {
        if (intValue <= UCHAR_MAX) {
            result = CreateConfigValue<uint8_t>(sectionFullName.c_str(), key.c_str(), (uint8_t)intValue);
            if (result == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate uint8 configuration value");
            }
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeUntypedConfigValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value)
{
    // try to infer the configuration value type
    MOT_LOG_TRACE("Attempting to parse signed integer value");
    ConfigItem* result = MakeIntConfigValue(sectionFullName, key, value);
    if (result == nullptr) {
        MOT_LOG_TRACE("Attempting to parse unsigned integer value");
        result = MakeUIntConfigValue(sectionFullName, key, value);
    }
    if (result == nullptr) {
        MOT_LOG_TRACE("Attempting to parse double precision integer value");
        result = MakeDoubleConfigValue(sectionFullName, key, value);
    }
    if (result == nullptr) {
        MOT_LOG_TRACE("Attempting to parse Boolean value");
        result = MakeBoolConfigValue(sectionFullName, key, value);
    }
    if (result == nullptr) {
        MOT_LOG_TRACE("Defaulting to mot_string value");
        result = CreateConfigValue<mot_string, StringConfigValue>(sectionFullName.c_str(), key.c_str(), value);
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate mot_string configuration value");
        }
    }
    return result;
}

ConfigItem* ConfigFileParser::MakeTypedConfigValue(
    const mot_string& sectionFullName, const mot_string& typeName, const mot_string& key, const mot_string& value)
{
    // build a configuration value by given type name
    ConfigItem* result = nullptr;
    ConfigValueType valueType = ConfigValueTypeFromString(typeName.c_str());
    switch (valueType) {
        case ConfigValueType::CONFIG_VALUE_INT64:
            result = MakeInt64ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_INT32:
            result = MakeInt32ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_INT16:
            result = MakeInt16ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_INT8:
            result = MakeInt8ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_UINT64:
            result = MakeUInt64ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_UINT32:
            result = MakeUInt32ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_UINT16:
            result = MakeUInt16ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_UINT8:
            result = MakeUInt8ConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_DOUBLE:
            result = MakeDoubleConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_BOOL:
            result = MakeBoolConfigValue(sectionFullName, key, value);
            break;

        case ConfigValueType::CONFIG_VALUE_STRING:
            result = CreateConfigValue<mot_string, StringConfigValue>(sectionFullName.c_str(), key.c_str(), value);
            if (result == nullptr) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_OOM, "Load Configuration", "Failed to allocate mot_string configuration value");
            }
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Load Configuration",
                "Invalid configuration value type name: %s",
                typeName.c_str());
            break;
    }

    return result;
}

bool ConfigFileParser::ParseIntValue(
    const mot_string& sectionFullName, const mot_string& key, const mot_string& value, int64_t& intValue)
{
    bool result = false;
    char* endptr = NULL;
    intValue = strtoll(value.c_str(), &endptr, 0);
    if (*value.c_str() == 0) {  // empty input
        MOT_LOG_DIAG2("Configuration integer at [section %s, key %s] is empty", sectionFullName.c_str(), key.c_str());
    } else if (endptr == value.c_str()) {  // no valid digits
        MOT_LOG_DIAG2("Configuration integer at [section %s, key %s] has no valid digits at all: %s",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if (*endptr != 0) {  // some invalid trailing digits
        MOT_LOG_DIAG2("Configuration integer at [section %s, key %s] has invalid trailing characters: %s",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if (((intValue == LLONG_MIN) || (intValue == LLONG_MAX)) && (errno == ERANGE)) {  // overflow
        MOT_LOG_DIAG2("Configuration integer at [section %s, key %s] overflows: %s", value.c_str());
    } else {
        int err = errno;
        MOT_LOG_TRACE("Configuration integer at [section %s, key %s] is ok: %s --> %" PRId64
                      " (value = %p, endptr = %p, *endptr = %u, errno: %d)",
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str(),
            intValue,
            value.c_str(),
            endptr,
            (unsigned)*endptr,
            err);
        result = true;
    }
    return result;
}

bool ConfigFileParser::ParseUIntValue(const mot_string& sectionFullName, const mot_string& key, const mot_string& value,
    uint64_t& intValue, bool arrayIndex /* = false */)
{
    bool result = false;
    char* endptr = NULL;
    intValue = strtoull(value.c_str(), &endptr, 0);
    const char* itemName = arrayIndex ? "arrayIndex" : "integer";
    if (*value.c_str() == 0) {  // empty input
        MOT_LOG_DIAG2(
            "Configuration %s at [section %s, key %s] is empty", itemName, sectionFullName.c_str(), key.c_str());
    } else if (endptr == value.c_str()) {  // no valid digits
        MOT_LOG_DIAG2("Configuration %s at [section %s, key %s] has no valid digits at all: %s",
            itemName,
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if (*endptr != 0) {  // some invalid trailing digits
        MOT_LOG_DIAG2("Configuration %s at [section %s, key %s] has invalid trailing characters: %s",
            itemName,
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str());
    } else if ((intValue == ULLONG_MAX) && (errno == ERANGE)) {  // overflow
        MOT_LOG_DIAG2("Configuration %s at [section %s, key %s] overflows: %s", itemName, value.c_str());
    } else {
        int err = errno;
        MOT_LOG_TRACE("Configuration %s at [section %s, key %s] is ok: %s --> %" PRIu64
                      " (value = %p, endptr = %p, *endptr = %u, errno: %d)",
            itemName,
            sectionFullName.c_str(),
            key.c_str(),
            value.c_str(),
            intValue,
            value.c_str(),
            endptr,
            (unsigned)*endptr,
            err);
        result = true;
    }
    return result;
}
}  // namespace MOT
