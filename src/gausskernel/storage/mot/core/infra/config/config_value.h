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
 * config_value.h
 *    The common base class for all configuration value types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_value.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_VALUE_H
#define CONFIG_VALUE_H

#include "config_item.h"
#include "config_value_type.h"

namespace MOT {
/** @brief The common base class for all configuration value types. */
class ConfigValue : public ConfigItem {
protected:
    /**
     * @brief Constructor.
     * @param valueType The value type of this configuration value.
     */
    explicit ConfigValue(ConfigValueType valueType)
        : ConfigItem(ConfigItemClass::CONFIG_ITEM_VALUE),
          m_valueType(valueType),
          m_isIntegral(IsConfigValueIntegral(valueType))
    {}

public:
    /** @brief Destructor. */
    ~ConfigValue() override
    {}

    /**
     * @brief Retrieves the configuration value type.
     * @return The value type.
     */
    inline ConfigValueType GetConfigValueType() const
    {
        return m_valueType;
    }

    /** @brief Queries whether the value type is integral. */
    inline bool IsIntegral() const
    {
        return m_isIntegral;
    }

    /** @brief Queries if object construction succeeded. */
    virtual bool IsValid() const
    {
        return true;
    }

private:
    /** @var The configuration value type. */
    ConfigValueType m_valueType;

    /** @var Specifies whether this is an integral value type. */
    bool m_isIntegral;
};

// specialization
template <>
struct ConfigItemClassMapper<ConfigValue> {
    static constexpr const ConfigItemClass CONFIG_ITEM_CLASS = ConfigItemClass::CONFIG_ITEM_VALUE;

    static constexpr const char* CONFIG_ITEM_CLASS_NAME = "ConfigValue";
};
}  // namespace MOT

#endif /* ABSTRACTCONFIGVALUE_H */
