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
 * cmdline_config_loader.cpp
 *    Configuration loader that loads from command line arguments.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/cmdline_config_loader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmdline_config_loader.h"
#include "config_file_parser.h"
#include "utilities.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(CmdLineConfigLoader, Configuration)

static const char CMDLINE_SEP = '-';

static bool ParseCmdLineSectionName(const mot_string& line, mot_string& sectionPath, mot_string& keyValuePart)
{
    bool result = false;

    uint32_t lastSlashPos = line.find_last_of(CMDLINE_SEP);
    if (lastSlashPos != mot_string::npos) {
        if (!line.substr(sectionPath, 0, lastSlashPos) || !line.substr(keyValuePart, lastSlashPos + 1)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to parse section name");
        } else {
            sectionPath.trim();
            keyValuePart.trim();
            result = true;
        }
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_CFG,
            "Load Configuration",
            "Malformed command line argument missing separator %c: %s",
            CMDLINE_SEP,
            line.c_str());
    }

    return result;
}

#define CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(errorCode, format, ...)              \
    {                                                                             \
        MOT_REPORT_ERROR(errorCode, "Load Configuration", format, ##__VA_ARGS__); \
        parseError = true;                                                        \
        break;                                                                    \
    }

static ConfigSection* GetCmdLineConfigSection(
    const mot_string& sectionFullName, mot_list<ConfigSection*>& parsedSections, ConfigSectionMap& sectionMap)
{
    mot_string sectionPath;
    mot_string sectionName;
    ConfigSectionMap::iterator itr = sectionMap.find(sectionFullName);
    if (itr == sectionMap.end()) {
        if (!ConfigFileParser::BreakSectionName(sectionFullName, sectionPath, sectionName, CMDLINE_SEP)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse section name");
            return nullptr;
        }

        ConfigSection* currentSection = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
        if (currentSection == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to allocate memory for configuration section");
            return nullptr;
        }

        if (!parsedSections.push_back(currentSection)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to insert parsed section");
            return nullptr;
        }

        itr = sectionMap.insert(ConfigSectionMap::value_type(sectionFullName, currentSection)).first;
    }

    return itr->second;
}

static bool AddCmdLineArrayConfigItem(ConfigSection* currentSection, const mot_string& sectionFullName,
    const mot_string& key, const mot_string& value, uint64_t arrayIndex)
{
    // create array if not created yet
    ConfigArray* configArray = currentSection->ModifyConfigArray(key.c_str());
    if (configArray == nullptr) {
        configArray = ConfigArray::CreateConfigArray(sectionFullName.c_str(), key.c_str());
        if (configArray == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate memory for configuration array");
            return false;
        }

        if (!currentSection->AddConfigItem(configArray)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to add configuration array to parent section");
            return false;
        }
    }

    // create item and add it to array
    if (arrayIndex != configArray->GetConfigItemCount()) {
        // array items must be ordered
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_CFG,
            "Load Configuration",
            "Failed to parse command line arguments: array %s items not well-ordered, expecting %u, got %" PRIu64,
            configArray->GetName(),
            configArray->GetConfigItemCount(),
            arrayIndex);
        return false;
    }

    ConfigItem* configItem = ConfigFileParser::MakeArrayConfigValue(sectionFullName, arrayIndex, value);
    if (configItem == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Load Configuration",
            "Failed to create array configuration value from raw value: %s",
            value.c_str());
        return false;
    }

    if (!configArray->AddConfigItem(configItem)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Load Configuration",
            "Failed to add %" PRIu64 "th item to configuration array %s",
            arrayIndex,
            configArray->GetName());
        return false;
    }

    return true;
}

ConfigTree* CmdLineConfigLoader::ParseCmdLine(char** argv, int argc)
{
    // section names are separated by dashes, it is expected to have the format:
    // --section1-section2-section3-key=value
    ConfigTree* cfgTree = ConfigTree::CreateConfigTree(GetPriority(), GetName(), true);
    if (cfgTree == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to create configuration tree");
        return nullptr;
    }

    mot_string line;
    mot_string sectionFullName;
    mot_string keyValuePart;
    ConfigSection* currentSection = nullptr;
    mot_list<ConfigSection*> parsedSections;
    ConfigSectionMap sectionMap;
    mot_string key;
    mot_string value;
    bool parseError = false;
    uint64_t arrayIndex = 0;
    bool hasArrayIndex = false;

    for (int i = 0; i < argc && !parseError; ++i) {
        if (!line.assign(argv[i])) {
            CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(
                MOT_ERROR_OOM, "Failed to allocate memory for next command line argument");
        }

        if ((line.length() <= 2) || line[0] != '-' || line[1] != '-') {
            MOT_LOG_TRACE("Skipping ill-formed command line argument: %s", argv[i]);
            continue;
        }

        if (!ParseCmdLineSectionName(line, sectionFullName, keyValuePart)) {
            CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL, "Failed to parse command line argument");
        }

        // get the configuration section
        currentSection = GetCmdLineConfigSection(sectionFullName, parsedSections, sectionMap);
        if (currentSection == nullptr) {
            CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL, "Failed to get configuration section");
        }

        // parse the key-value part
        if (!ConfigFileParser::ParseKeyValue(keyValuePart, sectionFullName, key, value, arrayIndex, hasArrayIndex)) {
            // key-value line malformed
            CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL, "Failed to parse key/value");
        }

        // check for array item
        if (!hasArrayIndex) {
            ConfigItem* configItem = ConfigFileParser::MakeConfigValue(sectionFullName, key, value);
            if (configItem == nullptr) {
                CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL,
                    "Failed to create configuration value from raw key/value: %s/%s",
                    key.c_str(),
                    value.c_str());
            } else if (!currentSection->AddConfigItem(configItem)) {
                CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(
                    MOT_ERROR_OOM, "Failed to add configuration value to parent section");
            }
        } else {
            if (!AddCmdLineArrayConfigItem(currentSection, sectionFullName, key, value, arrayIndex)) {
                CMDLINE_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL,
                    "Failed to add array item with arrayIndex %lu (command line argument: %s)",
                    arrayIndex,
                    line.c_str());
            }
        }
    }

    if (parseError) {
        delete cfgTree;
        cfgTree = nullptr;
    } else {
        if (!cfgTree->Build(parsedSections)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to build configuration tree");
            delete cfgTree;
            cfgTree = nullptr;
        }
    }
    return cfgTree;
}
}  // namespace MOT
