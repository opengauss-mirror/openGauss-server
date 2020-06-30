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
 *    src/gausskernel/storage/mot/core/src/infra/config/cmdline_config_loader.cpp
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

#define REPORT_PARSE_ERROR(errorCode, format, ...)                                \
    do {                                                                          \
        MOT_REPORT_ERROR(errorCode, "Load Configuration", format, ##__VA_ARGS__); \
        parseError = true;                                                        \
        break;                                                                    \
    } while (0);

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
    mot_string sectionPath;
    mot_string sectionName;
    ConfigSection* currentSection = nullptr;
    mot_list<ConfigSection*> parsedSections;
    ConfigSectionMap sectionMap;
    mot_string key;
    mot_string value;
    bool parseError = false;
    int arrayIndex = 0;

    for (int i = 0; i < argc && !parseError; ++i) {
        if (!line.assign(argv[i])) {
            REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate memory for next command line argument");
        }
        if ((line.length() <= 2) || line[0] != '-' || line[1] != '-') {
            MOT_LOG_TRACE("Skipping ill-formed command line argument: %s", argv[i]);
        } else {
            if (!ParseCmdLineSectionName(line, sectionFullName, keyValuePart)) {
                REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL, "Failed to parse environment variable");
            } else {
                // get the configuration section
                ConfigSectionMap::iterator itr = sectionMap.find(sectionFullName);
                if (itr == sectionMap.end()) {
                    if (!ConfigFileParser::BreakSectionName(sectionFullName, sectionPath, sectionName, CMDLINE_SEP)) {
                        REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL, "Failed to parse section name");
                    }
                    currentSection = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
                    if (currentSection == nullptr) {
                        REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate memory for configuration section");
                    }
                    if (!parsedSections.push_back(currentSection)) {
                        REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to insert parsed section");
                    }
                    itr = sectionMap.insert(ConfigSectionMap::value_type(sectionFullName, currentSection)).first;
                }
                currentSection = itr->second;

                // parse the key-value part
                if (!ConfigFileParser::ParseKeyValue(keyValuePart, sectionFullName, key, value, arrayIndex)) {
                    // key-value line malformed
                    parseError = true;
                } else {
                    // check for array item
                    if (arrayIndex == -1) {
                        ConfigItem* configItem = ConfigFileParser::MakeConfigValue(sectionFullName, key, value);
                        if (configItem == nullptr) {
                            REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL,
                                "Failed to create configuration value from raw key/value: %s/%s",
                                key.c_str(),
                                value.c_str());
                        } else if (!currentSection->AddConfigItem(configItem)) {
                            REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to add configuration value to parent section");
                        }
                    } else {
                        // create array if not created yet
                        ConfigArray* configArray = currentSection->ModifyConfigArray(key.c_str());
                        if (configArray == nullptr) {
                            configArray = ConfigArray::CreateConfigArray(sectionFullName.c_str(), key.c_str());
                            if (configArray == nullptr) {
                                REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate memory for configuration array");
                            } else if (!currentSection->AddConfigItem(configArray)) {
                                REPORT_PARSE_ERROR(
                                    MOT_ERROR_OOM, "Failed to add configuration array to parent section");
                            }
                        }
                        // create item and add it to array
                        if ((uint32_t)arrayIndex != configArray->GetConfigItemCount()) {
                            // array items must be ordered
                            REPORT_PARSE_ERROR(MOT_ERROR_INVALID_CFG,
                                "Failed to parse command line arguments: array %s items not well-ordered, "
                                "expecting %u, got %u (command line argument: %s)",
                                configArray->GetName(),
                                configArray->GetConfigItemCount(),
                                (uint32_t)arrayIndex,
                                argv[i]);
                        } else {
                            ConfigItem* configItem =
                                ConfigFileParser::MakeArrayConfigValue(sectionFullName, arrayIndex, value);
                            if (configItem == nullptr) {
                                REPORT_PARSE_ERROR(MOT_ERROR_OOM,
                                    "Failed to create array configuration value from raw value: %s",
                                    value.c_str());
                            } else if (!configArray->AddConfigItem(configItem)) {
                                REPORT_PARSE_ERROR(MOT_ERROR_OOM,
                                    "Failed to add %dth item to configuration array %s",
                                    arrayIndex,
                                    configArray->GetName());
                            }
                        }
                    }
                }
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
