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
 * config_section.cpp
 *    Configuration section that contains configuration items.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_section.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_section.h"
#include "config_array.h"

namespace MOT {
ConfigSection::ConfigSection() : ConfigItem(ConfigItemClass::CONFIG_ITEM_SECTION)
{}

ConfigSection::~ConfigSection()
{
    // delete all direct values
    ConfigValueMap::iterator vitr = m_valueMap.begin();
    while (vitr != m_valueMap.end()) {
        delete vitr->second;
        ++vitr;
    }

    // delete all direct arrays
    ConfigArrayMap::iterator aitr = m_arrayMap.begin();
    while (aitr != m_arrayMap.end()) {
        delete aitr->second;
        ++aitr;
    }

    // delete all sub-sections
    ConfigSectionMap::iterator sitr = m_sectionMap.begin();
    while (sitr != m_sectionMap.end()) {
        delete sitr->second;
        ++sitr;
    }
}

void ConfigSection::Print(LogLevel logLevel, bool fullPrint) const
{
    // print indented section name
    if (!fullPrint) {
        MOT_LOG(logLevel, "%*s[%s]", GetDepth(), "", IsRoot() ? "ROOT" : GetName());
    } else {
        MOT_LOG(logLevel, "%*s[%s] [%p]", GetDepth(), "", GetName(), GetName());
    }

    // print values
    ConfigValueMap::const_iterator itr1 = m_valueMap.begin();
    while (itr1 != m_valueMap.end()) {
        itr1->second->Print(logLevel, fullPrint);
        ++itr1;
    }

    // print arrays
    ConfigArrayMap::const_iterator itr2 = m_arrayMap.begin();
    while (itr2 != m_arrayMap.end()) {
        itr2->second->Print(logLevel, fullPrint);
        ++itr2;
    }

    // print sub-sections
    ConfigSectionMap::const_iterator itr3 = m_sectionMap.begin();
    while (itr3 != m_sectionMap.end()) {
        itr3->second->Print(logLevel, fullPrint);
        ++itr3;
    }
}

bool ConfigSection::GetConfigSectionNames(mot_string_list& sectionNames) const
{
    bool result = true;
    ConfigSectionMap::const_iterator itr = m_sectionMap.begin();
    while (itr != m_sectionMap.end()) {
        result = sectionNames.push_back(itr->first);
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Configuration",
                "Failed to get section names (%u retrieved up to failed point)",
                sectionNames.size());
            break;
        }
        ++itr;
    }
    return result;
}

bool ConfigSection::GetConfigValueNames(mot_string_list& valueNames) const
{
    ConfigValueMap::const_iterator itr = m_valueMap.begin();
    while (itr != m_valueMap.end()) {
        if (!valueNames.push_back(itr->first)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Configuration",
                "Failed to get value names (%u retrieved up to failed point)",
                valueNames.size());
            return false;
        }
        ++itr;
    }
    ConfigArrayMap::const_iterator itr2 = m_arrayMap.begin();
    while (itr2 != m_arrayMap.end()) {
        if (!valueNames.push_back(itr2->first)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Load Configuration",
                "Failed to get value names (%u retrieved up to failed point)",
                valueNames.size());
            return false;
        }
        ++itr2;
    }
    return true;
}

bool ConfigSection::AddConfigItem(ConfigItem* configItem, bool replaceIfExists /* = false */)
{
    bool result = false;
    switch (configItem->GetClass()) {
        case ConfigItemClass::CONFIG_ITEM_SECTION:
            result = AddConfigSection(static_cast<ConfigSection*>(configItem));
            break;

        case ConfigItemClass::CONFIG_ITEM_VALUE:
            result = AddConfigValue(static_cast<ConfigValue*>(configItem), replaceIfExists);
            break;

        case ConfigItemClass::CONFIG_ITEM_ARRAY:
            result = AddConfigArray(static_cast<ConfigArray*>(configItem));
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Invalid configuration item class %d",
                (int)configItem->GetClass());
            break;
    }
    return result;
}

bool ConfigSection::Merge(ConfigSection* configSection)
{
    return MergeValues(configSection) && MergeArrays(configSection);
}

const ConfigItem* ConfigSection::GetConfigItem(const char* name) const
{
    const ConfigItem* result = nullptr;
    ConfigSectionMap::const_iterator itr = m_sectionMap.find(name);
    if (itr != m_sectionMap.end()) {
        result = itr->second;
    } else {
        ConfigValueMap::const_iterator itr2 = m_valueMap.find(name);
        if (itr2 != m_valueMap.end()) {
            result = itr2->second;
        } else {
            ConfigArrayMap::const_iterator itr3 = m_arrayMap.find(name);
            if (itr3 != m_arrayMap.end()) {
                result = itr3->second;
            }
        }
    }
    return result;
}

const ConfigSection* ConfigSection::GetConfigSection(const char* name) const
{
    const ConfigSection* result = nullptr;
    ConfigSectionMap::const_iterator itr = m_sectionMap.find(name);
    if (itr != m_sectionMap.end()) {
        result = itr->second;
    }
    return result;
}

const ConfigArray* ConfigSection::GetConfigArray(const char* name) const
{
    const ConfigArray* result = nullptr;
    ConfigArrayMap::const_iterator itr = m_arrayMap.find(name);
    if (itr != m_arrayMap.end()) {
        result = itr->second;
    }
    return result;
}

ConfigItem* ConfigSection::ModifyConfigItem(const char* name)
{
    ConfigItem* result = nullptr;

    ConfigSectionMap::iterator itr = m_sectionMap.find(name);
    if (itr != m_sectionMap.end()) {
        result = itr->second;
    } else {
        ConfigValueMap::iterator itr2 = m_valueMap.find(name);
        if (itr2 != m_valueMap.end()) {
            result = itr2->second;
        } else {
            ConfigArrayMap::iterator itr3 = m_arrayMap.find(name);
            if (itr3 != m_arrayMap.end()) {
                result = itr3->second;
            }
        }
    }

    return result;
}

ConfigSection* ConfigSection::ModifyConfigSection(const char* name)
{
    ConfigSection* result = nullptr;
    ConfigSectionMap::iterator itr = m_sectionMap.find(name);
    if (itr != m_sectionMap.end()) {
        result = itr->second;
    }
    return result;
}

ConfigArray* ConfigSection::ModifyConfigArray(const char* name)
{
    ConfigArray* result = nullptr;
    ConfigArrayMap::iterator itr = m_arrayMap.find(name);
    if (itr != m_arrayMap.end()) {
        result = itr->second;
    }
    return result;
}

void ConfigSection::ForEach(ConfigItemVisitor& visitor) const
{
    visitor.OnConfigItem(this);
    ForEachValue(visitor);
    ForEachArray(visitor);
    ForEachSection(visitor);
}

bool ConfigSection::AddConfigSection(ConfigSection* configSection)
{
    ConfigSectionMap::pairis pairis =
        m_sectionMap.insert(ConfigSectionMap::value_type(configSection->GetName(), configSection));
    if (pairis.second == INSERT_EXISTS) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Cannot add configuration section %s to section %s: section already exists",
            configSection->GetName(),
            GetName());
    } else if (pairis.second == INSERT_FAILED) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to add configuration section %s to section %s: reached resource limit",
            configSection->GetName(),
            GetName());
    }
    return (pairis.second == INSERT_SUCCESS);
}

bool ConfigSection::AddConfigValue(ConfigValue* configValue, bool replaceIfExists)
{
    ConfigValueMap::pairis pairis = m_valueMap.insert(ConfigValueMap::value_type(configValue->GetName(), configValue));
    if (pairis.second == INSERT_EXISTS) {
        if (replaceIfExists) {
            ConfigValue* prevValue = pairis.first->second;
            pairis.first->second = configValue;
            delete prevValue;
            pairis.second = INSERT_SUCCESS;
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Cannot add configuration value %s to section %s: value already exists",
                configValue->GetName(),
                GetName());
        }
    } else if (pairis.second == INSERT_FAILED) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to add configuration value %s to section %s: reached resource limit",
            configValue->GetName(),
            GetName());
    }
    return (pairis.second == INSERT_SUCCESS);
}

bool ConfigSection::AddConfigArray(ConfigArray* configArray)
{
    ConfigArrayMap::pairis pairis = m_arrayMap.insert(ConfigArrayMap::value_type(configArray->GetName(), configArray));
    if (pairis.second == INSERT_EXISTS) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Cannot add configuration array %s to section %s: array already exists",
            configArray->GetName(),
            GetName());
    } else if (pairis.second == INSERT_FAILED) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to add configuration array %s to section %s: reached resource limit",
            configArray->GetName(),
            GetName());
    }
    return (pairis.second == INSERT_SUCCESS);
}

void ConfigSection::ForEachValue(ConfigItemVisitor& visitor) const
{
    ConfigValueMap::const_iterator itr = m_valueMap.begin();
    while (itr != m_valueMap.end()) {
        visitor.OnConfigItem(itr->second);
        ++itr;
    }
}

void ConfigSection::ForEachArray(ConfigItemVisitor& visitor) const
{
    ConfigArrayMap::const_iterator itr = m_arrayMap.begin();
    while (itr != m_arrayMap.end()) {
        static_cast<const ConfigArray*>(itr->second)->ForEach(visitor);
        ++itr;
    }
}

void ConfigSection::ForEachSection(ConfigItemVisitor& visitor) const
{
    ConfigSectionMap::const_iterator itr = m_sectionMap.begin();
    while (itr != m_sectionMap.end()) {
        static_cast<const ConfigSection*>(itr->second)->ForEach(visitor);
        ++itr;
    }
}

bool ConfigSection::MergeValues(ConfigSection* configSection)
{
    ConfigValueMap::iterator itr = configSection->m_valueMap.begin();
    while (itr != configSection->m_valueMap.end()) {
        ConfigValue* configValue = itr->second;
        ConfigValueMap::pairis pairis =
            m_valueMap.insert(ConfigValueMap::value_type(configValue->GetName(), configValue));
        if (pairis.second == INSERT_SUCCESS) {
            MOT_LOG_DEBUG("Inserted new value: %s", configValue->GetName());
        } else {
            if (MOT_IS_SEVERE()) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to merge value %s into section %s",
                    configValue->GetName(),
                    GetFullPathName());
                return false;
            }
            // not inserted, so replace existing value
            ConfigValueMap::iterator& itr2 = pairis.first;
            ConfigValue* oldConfigValue = itr2->second;
            MOT_LOG_DEBUG(" *** --> Deleting merged section value %s", oldConfigValue->GetFullPathName());
            oldConfigValue->Print(LogLevel::LL_DEBUG, true);
            (void)m_valueMap.erase(itr2);
            MOT_LOG_DEBUG(" *** --> Delete merged section value done");
            delete oldConfigValue;
            if (!m_valueMap.insert(ConfigValueMap::value_type(configValue->GetName(), configValue)).second) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to merge value %s into section %s (severe error)",
                        configValue->GetName(),
                        GetFullPathName());
                    return false;
                }
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to merge value %s into section %s (unexpected duplicate)",
                    configValue->GetName(),
                    GetFullPathName());
                MOT_ASSERT(false);
                return false;
            } else {
                MOT_LOG_DEBUG("Value merged successfully:");
                configValue->Print(LogLevel::LL_DEBUG, true);
            }
        }
        ++itr;
    }
    configSection->m_valueMap.clear();  // required to make sure items are not deleted in destructor
    return true;
}

bool ConfigSection::MergeArrays(ConfigSection* configSection)
{
    ConfigArrayMap::iterator itr = configSection->m_arrayMap.begin();
    while (itr != configSection->m_arrayMap.end()) {
        ConfigArray* configArray = itr->second;
        ConfigArrayMap::pairis pairis =
            m_arrayMap.insert(ConfigArrayMap::value_type(configArray->GetName(), configArray));
        if (pairis.second != INSERT_SUCCESS) {
            if (MOT_IS_SEVERE()) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to merge array %s into section %s",
                    configArray->GetName(),
                    GetFullPathName());
                return false;
            }
            // not inserted, so replace existing value
            ConfigArrayMap::iterator& itr2 = pairis.first;
            delete itr2->second;
            (void)m_arrayMap.erase(itr2);
            if (!m_arrayMap.insert(ConfigArrayMap::value_type(configArray->GetName(), configArray)).second) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to merge array %s into section %s (severe error)",
                        configArray->GetName(),
                        GetFullPathName());
                    return false;
                }
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to merge array %s into section %s (unexpected duplicate)",
                    configArray->GetName(),
                    GetFullPathName());
                MOT_ASSERT(false);
                return false;
            }
        }
        (void)itr++;
    }
    configSection->m_arrayMap.clear();  // required to make sure items are not deleted in destructor
    return true;
}
}  // namespace MOT
