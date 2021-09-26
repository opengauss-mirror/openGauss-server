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
 * ext_config_loader.cpp
 *    Configuration loader that loads from envelope.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/ext_config_loader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "ext_config_loader.h"
#include "config_file_parser.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ExtConfigLoader, Configuration)

bool ExtConfigLoader::LoadExtConfig()
{
    bool result = false;

    // prepare
    BeginExtConfigLoad();

    // load actual configuration
    result = OnLoadExtConfig();
    if (!result) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to load external configuration");
    } else {
        // wrap up
        result = EndExtConfigLoad();
        if (!result) {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Load Configuration", "Failed to finalize external configuration loading");
        } else {
            if (MOT_CHECK_LOG_LEVEL(LogLevel::LL_TRACE)) {
                MOT_LOG_INFO("Loaded external configuration:");
                m_extConfigTree->Print(LogLevel::LL_TRACE);
            }
        }
    }

    return result;
}

bool ExtConfigLoader::AddExtStringConfigItem(const char* path, const char* key, const char* value)
{
    return AddTypedConfigItem<MOT::mot_string, StringConfigValue>(path, key, value);
}

bool ExtConfigLoader::AddExtUInt64ConfigItem(const char* path, const char* key, uint64_t value)
{
    return AddTypedConfigItem<uint64_t>(path, key, value);
}

bool ExtConfigLoader::AddExtUInt32ConfigItem(const char* path, const char* key, uint32_t value)
{
    return AddTypedConfigItem<uint32_t>(path, key, value);
}

bool ExtConfigLoader::AddExtUInt16ConfigItem(const char* path, const char* key, uint16_t value)
{
    return AddTypedConfigItem<uint16_t>(path, key, value);
}

bool ExtConfigLoader::AddExtUInt8ConfigItem(const char* path, const char* key, uint8_t value)
{
    return AddTypedConfigItem<uint8_t>(path, key, value);
}

bool ExtConfigLoader::AddExtInt64ConfigItem(const char* path, const char* key, int64_t value)
{
    return AddTypedConfigItem<int64_t>(path, key, value);
}

bool ExtConfigLoader::AddExtInt32ConfigItem(const char* path, const char* key, int32_t value)
{
    return AddTypedConfigItem<int32_t>(path, key, value);
}

bool ExtConfigLoader::AddExtInt16ConfigItem(const char* path, const char* key, int16_t value)
{
    return AddTypedConfigItem<int16_t>(path, key, value);
}

bool ExtConfigLoader::AddExtInt8ConfigItem(const char* path, const char* key, int8_t value)
{
    return AddTypedConfigItem<int8_t>(path, key, value);
}

bool ExtConfigLoader::AddExtDoubleConfigItem(const char* path, const char* key, double value)
{
    return AddTypedConfigItem<double>(path, key, value);
}

bool ExtConfigLoader::AddExtBoolConfigItem(const char* path, const char* key, bool value)
{
    return AddTypedConfigItem<bool>(path, key, value);
}

bool ExtConfigLoader::AddExtConfigItem(ConfigItem* configItem)
{
    ConfigSection* section = nullptr;
    ConfigSectionMap::iterator itr = m_sectionMap.find(configItem->GetPath());
    if (itr == m_sectionMap.end()) {
        mot_string sectionPath;
        mot_string sectionName;
        if (!ConfigFileParser::BreakSectionName(
                configItem->GetPath(), sectionPath, sectionName, ConfigItem::PATH_SEP)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse external section name");
            return false;
        }
        section = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
        if (section == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to create new external section");
            return false;
        } else {
            if (!m_parsedSections.push_back(section)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to add new external section");
                delete section;
                return false;
            }
            if (!m_sectionMap.insert(ConfigSectionMap::value_type(section->GetFullPathName(), section)).second) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to insert external section %s",
                        section->GetFullPathName());
                    delete section;
                    return false;
                }
                MOT_LOG_WARN("Failed to add duplicate external section %s", section->GetFullPathName());
            }
        }
    } else {
        section = itr->second;
    }

    if (!section->AddConfigItem(configItem)) {
        if (MOT_IS_SEVERE()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to add external configuration item %s to parent section %s",
                configItem->GetPath(),
                section->GetFullPathName());
            return false;
        }
        MOT_LOG_WARN("Failed to add duplicate external configuration item %s to parent section %s",
            configItem->GetPath(),
            section->GetFullPathName());
    }
    return true;
}

bool ExtConfigLoader::EndExtConfigLoad()
{
    bool result = false;
    m_extConfigTree = ConfigTree::CreateConfigTree(GetPriority(), GetName(), false);
    if (m_extConfigTree != nullptr) {
        result = m_extConfigTree->Build(m_parsedSections);
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to build external configuration tree");
            delete m_extConfigTree;
            m_extConfigTree = nullptr;
        }
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to create external configuration tree");
    }
    return result;
}

void ExtConfigLoader::Cleanup() noexcept
{
    m_sectionMap.clear();
    m_parsedSections.clear();
    if (m_extConfigTree != nullptr) {
        delete m_extConfigTree;
        m_extConfigTree = nullptr;
    }
}
}  // namespace MOT
