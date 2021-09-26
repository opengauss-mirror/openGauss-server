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
 * typed_config_value.h
 *    Helper class for printing configuration values.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/typed_config_value.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TYPED_CONFIG_VALUE_H
#define TYPED_CONFIG_VALUE_H

#include "config_value.h"
#include "type_formatter.h"

namespace MOT {
/**
 * @brief Helper class for printing configuration values.
 * @tparam T The configuration value type.
 */
template <typename T>
struct ConfigValueTypePrinter {
    /**
     * @brief Helper function for printing (avoid class specializations)
     * @tparam T The configuration value type.
     * @param path The configuration path of this item.
     * @param name The name of the item.
     * @param value The value to print
     */
    static inline void PrintConfigValue(LogLevel logLevel, uint32_t depth, const char* name, const T& value)
    {
        // implementation is in each specialization
    }

    DECLARE_CLASS_LOGGER()
};

/**
 * @struct ConfigValueTypeMapper
 * @tparam T The configuration value type.
 * @brief Helper struct used to map type T to its associated ConfigValueType.
 */
template <typename T>
struct ConfigValueTypeMapper {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_UNDEFINED;
};

/**
 * @class TypedConfigValue
 * @tparam The configuration value type.
 * @brief A template for configuration values by type.
 */
template <typename T>
class TypedConfigValue : public ConfigValue {
public:
    /**
     * @brief Constructor.
     * @param value The value of the configuration item.
     */
    explicit TypedConfigValue(const T& value) : ConfigValue(ConfigValueTypeMapper<T>::CONFIG_VALUE_TYPE), m_value(value)
    {}

    /** @brief Destructor. */
    ~TypedConfigValue() override
    {}

    /**
     * @brief Retrieves the value associated with this item.
     * @return The value of this item.
     */
    inline const T& GetValue() const
    {
        return m_value;
    }

    /**
     * @brief Prints the configuration item to the log.
     * @param logLevel The log level used for printing.
     * @param fullPrint Specifies whether to print full path names of terminal leaves.
     */
    virtual void Print(LogLevel logLevel, bool fullPrint) const
    {
        if (fullPrint) {
            ConfigValueTypePrinter<T>::PrintConfigValue(logLevel, 0, GetFullPathName(), m_value);
        } else {
            ConfigValueTypePrinter<T>::PrintConfigValue(logLevel, GetDepth(), GetName(), m_value);
        }
    }

private:
    /** The value stored by this item. */
    T m_value;
};

/**
 * @brief Utility method for creating a configuration value.
 * @tparam T The primitive raw conifguration value type.
 * @tparam V The resulting configuration item type.
 * @param path The configuration path leading to the item.
 * @param name The configuration item name.
 * @param value The primitive raw conifguration value.
 * @return The configuration value if succeeded, otherwise null.
 */
template <typename T, typename V = TypedConfigValue<T>>
static V* CreateConfigValue(const char* path, const char* name, const T& value)
{
    V* result = ConfigItem::CreateConfigItem<V>(path, name, value);
    if (!result->IsValid()) {
        delete result;
        result = nullptr;
    }
    return result;
}

// ConfigItemClassMapper specializations for primitive types
#define SPEC_CONFIG_ITEM_MAPPER(T)                                                                     \
    template <>                                                                                        \
    struct ConfigItemClassMapper<TypedConfigValue<T>> {                                                \
        static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_VALUE; \
        static constexpr const char* CONFIG_ITEM_CLASS_NAME = "ConfigValue";                           \
    };

SPEC_CONFIG_ITEM_MAPPER(uint64_t)
SPEC_CONFIG_ITEM_MAPPER(uint32_t)
SPEC_CONFIG_ITEM_MAPPER(uint16_t)
SPEC_CONFIG_ITEM_MAPPER(uint8_t)
SPEC_CONFIG_ITEM_MAPPER(int64_t)
SPEC_CONFIG_ITEM_MAPPER(int32_t)
SPEC_CONFIG_ITEM_MAPPER(int16_t)
SPEC_CONFIG_ITEM_MAPPER(int8_t)
SPEC_CONFIG_ITEM_MAPPER(double)
SPEC_CONFIG_ITEM_MAPPER(mot_string)
SPEC_CONFIG_ITEM_MAPPER(bool)

// specialization
template <>
inline void ConfigValueTypePrinter<uint64_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const uint64_t& value)
{
    MOT_LOG(logLevel, "%*s%s=%" PRIu64 " [name@%p]", depth, "", name, value, name);
}

// specialization
template <>
inline void ConfigValueTypePrinter<uint32_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const uint32_t& value)
{
    ConfigValueTypePrinter<uint64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<uint16_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const uint16_t& value)
{
    ConfigValueTypePrinter<uint64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<uint8_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const uint8_t& value)
{
    ConfigValueTypePrinter<uint64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<int64_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const int64_t& value)
{
    MOT_LOG(logLevel, "%*s%s=%" PRId64 " [name@%p]", depth, "", name, value, name);
}

// specialization
template <>
inline void ConfigValueTypePrinter<int32_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const int32_t& value)
{
    ConfigValueTypePrinter<int64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<int16_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const int16_t& value)
{
    ConfigValueTypePrinter<int64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<int8_t>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const int8_t& value)
{
    ConfigValueTypePrinter<int64_t>::PrintConfigValue(logLevel, depth, name, value);
}

// specialization
template <>
inline void ConfigValueTypePrinter<double>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const double& value)
{
    MOT_LOG(logLevel, "%*s%s=%0.4f [name@%p]", depth, "", name, value, name);
}

// specialization
template <>
inline void ConfigValueTypePrinter<mot_string>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const mot_string& value)
{
    MOT_LOG(logLevel, "%*s%s=%s [name@%p, value@%p]", depth, "", name, value.c_str(), name, value.c_str());
}

// specialization
template <>
inline void ConfigValueTypePrinter<bool>::PrintConfigValue(
    LogLevel logLevel, uint32_t depth, const char* name, const bool& value)
{
    MOT_LOG(logLevel, "%*s%s=%s [name@%p]", depth, "", name, value ? "TRUE" : "FALSE", name);
}

// specialization
template <>
struct ConfigValueTypeMapper<int64_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_INT64;
};

// specialization
template <>
struct ConfigValueTypeMapper<int32_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_INT32;
};

// specialization
template <>
struct ConfigValueTypeMapper<int16_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_INT16;
};

// specialization
template <>
struct ConfigValueTypeMapper<int8_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_INT8;
};

// specialization
template <>
struct ConfigValueTypeMapper<uint64_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_UINT64;
};

// specialization
template <>
struct ConfigValueTypeMapper<uint32_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_UINT32;
};

// specialization
template <>
struct ConfigValueTypeMapper<uint16_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_UINT16;
};

// specialization
template <>
struct ConfigValueTypeMapper<uint8_t> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_UINT8;
};

// specialization
template <>
struct ConfigValueTypeMapper<double> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_DOUBLE;
};

// specialization
template <>
struct ConfigValueTypeMapper<mot_string> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_STRING;
};

// specialization
template <>
struct ConfigValueTypeMapper<bool> {
    static constexpr const ConfigValueType CONFIG_VALUE_TYPE = ConfigValueType::CONFIG_VALUE_BOOL;
};

// unsigned integral types
typedef TypedConfigValue<uint64_t> UInt64ConfigValue;
typedef TypedConfigValue<uint32_t> UInt32ConfigValue;
typedef TypedConfigValue<uint16_t> UInt16ConfigValue;
typedef TypedConfigValue<uint8_t> UInt8ConfigValue;

// signed integral types
typedef TypedConfigValue<int64_t> Int64ConfigValue;
typedef TypedConfigValue<int32_t> Int32ConfigValue;
typedef TypedConfigValue<int16_t> Int16ConfigValue;
typedef TypedConfigValue<int8_t> Int8ConfigValue;

// floating point type
typedef TypedConfigValue<double> DoubleConfigValue;

// Boolean type
typedef TypedConfigValue<bool> BoolConfigValue;

/**
 * @class StringConfigValue
 * @brief A string configuration value.
 */
class StringConfigValue : public TypedConfigValue<mot_string> {
public:
    /**
     * @brief Constructor.
     * @param path The configuration path of this item.
     * @param name The name of the configuration item.
     * @param value The value of the configuration item.
     */
    explicit StringConfigValue(const mot_string& value) : TypedConfigValue<mot_string>(value)
    {}

    /** @brief Destructor. */
    ~StringConfigValue() final
    {}

    /** @brief Queries if object construction succeeded. */
    bool IsValid() const final
    {
        return GetValue().is_valid();
    }
};

// specialization
template <>
struct ConfigItemClassMapper<StringConfigValue> {
    static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_VALUE;
    static constexpr const char* CONFIG_ITEM_CLASS_NAME = "ConfigValue";
};

// type formatter specialization for all primitive types
#define SPEC_TYPE_FORMAT(typeName, formatStr)                                              \
    template <>                                                                            \
    class TypeFormatter<typeName> {                                                        \
    public:                                                                                \
        static inline const char* ToString(const typeName& value, mot_string& stringValue) \
        {                                                                                  \
            stringValue.format(formatStr, value);                                          \
            return stringValue.c_str();                                                    \
        }                                                                                  \
        static inline bool FromString(const char* stringValue, typeName& value)            \
        {                                                                                  \
            return false;                                                                  \
        }                                                                                  \
    };

SPEC_TYPE_FORMAT(uint64_t, "%" PRIu64)
SPEC_TYPE_FORMAT(uint32_t, "%" PRIu32)
SPEC_TYPE_FORMAT(uint16_t, "%" PRIu16)
SPEC_TYPE_FORMAT(uint8_t, "%" PRIu8)

SPEC_TYPE_FORMAT(int64_t, "%" PRId64)
SPEC_TYPE_FORMAT(int32_t, "%" PRId32)
SPEC_TYPE_FORMAT(int16_t, "%" PRId16)
SPEC_TYPE_FORMAT(int8_t, "%" PRId8)

SPEC_TYPE_FORMAT(double, "%0.2f")

// specialization
template <>
class TypeFormatter<bool> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const bool& value, mot_string& stringValue)
    {
        stringValue.format("%s", value ? "true" : "false");
        return stringValue.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, bool& value)
    {
        // by default not implemented
        bool result = false;
        if ((strcasecmp(stringValue, "true") == 0) || (strcasecmp(stringValue, "on") == 0) ||
            (strcasecmp(stringValue, "yes") == 0)) {
            result = true;
        }
        return result;
    }
};

// specialization
template <>
class TypeFormatter<mot_string> {
public:
    /**
     * @brief Converts a value to string.
     * @param value The value to convert.
     * @param[out] stringValue The resulting string.
     */
    static inline const char* ToString(const mot_string& value, mot_string& stringValue)
    {
        return value.c_str();
    }

    /**
     * @brief Converts a string to a value.
     * @param The string to convert.
     * @param[out] The resulting value.
     * @return Boolean value denoting whether the conversion succeeded or not.
     */
    static inline bool FromString(const char* stringValue, mot_string& value)
    {
        return value.assign(stringValue);
    }
};
}  // namespace MOT

#endif /* TYPED_CONFIG_VALUE_H */
