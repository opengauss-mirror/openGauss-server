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
 * props_config_file_loader.cpp
 *    Configuration loader that loads from a properties file (Java style).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/infra/config/props_config_file_loader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "props_config_file_loader.h"
#include "config_file_parser.h"
#include "file_line_reader.h"
#include "mot_error.h"

namespace MOT {
DECLARE_LOGGER(PropsConfigFileLoader, Configuration)

static bool ParsePropsSectionName(const mot_string& line, mot_string& sectionPath, mot_string& keyValuePart)
{
    bool result = true;

    // since configuration value might have path names to disk, we need to make sure that section separator appears
    // BEFORE the equals sign
    uint32_t equalsPos = line.find('=');
    if (equalsPos == mot_string::npos) {  // this is an illegal format, there must be an equals sign
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to parse section name: missing equals sign");
        result = false;
    } else {
        uint32_t lastSlashPos = line.find_last_of(ConfigItem::PATH_SEP, equalsPos);
        if (lastSlashPos != mot_string::npos) {
            if (!line.substr(sectionPath, 0, lastSlashPos) || !line.substr(keyValuePart, lastSlashPos + 1)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to parse section name");
                result = false;
            } else {
                sectionPath.trim();
                keyValuePart.trim();
            }
        } else {
            sectionPath.assign("");
            keyValuePart.assign(line);
            keyValuePart.trim();
        }
    }

    return result;
}

#define REPORT_PARSE_ERROR(errorCode, format, ...)                                \
    do {                                                                          \
        MOT_REPORT_ERROR(errorCode, "Load Configuration", format, ##__VA_ARGS__); \
        parseError = true;                                                        \
        break;                                                                    \
    } while (0);

ConfigTree* PropsConfigFileLoader::LoadConfigFile(const char* configFilePath)
{
    MOT_LOG_TRACE("Loading PROPS configuration file from: %s", configFilePath);
    ConfigTree* configTree = ConfigTree::CreateConfigTree(GetPriority(), GetName(), false);
    if (configTree == nullptr) {
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

    // unsupported yet
    FileLineReader reader(configFilePath);
    if (!reader.IsValid()) {
        MOT_LOG_WARN("Failed to load configuration file %s: unable to open file", configFilePath);
        // we return an empty tree to avoid errors during startup, but a warning is still issued
        return configTree;
    }

    while (!reader.Eof() && !parseError) {
        // parse next non-empty line
        if (!line.assign(reader.GetLine().c_str())) {
            REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate memory for next line");
        }
        line.trim();
        if (line.length() && line[0] != '#') {
            // break tokens to section and key-value by last separator
            MOT_LOG_DEBUG("Parsing config line: %s", line.c_str());
            if (!ParsePropsSectionName(line, sectionFullName, keyValuePart)) {
                REPORT_PARSE_ERROR(MOT_ERROR_INVALID_CFG,
                    "Failed to parse configuration file %s at line %d: %s (section name line malformed)",
                    configFilePath,
                    reader.GetLineNumber(),
                    line.c_str());
            } else {
                // get the configuration section
                mot_map<mot_string, ConfigSection*>::iterator itr = sectionMap.find(sectionFullName);
                if (itr == sectionMap.end()) {
                    if (!ConfigFileParser::BreakSectionName(sectionFullName, sectionPath, sectionName)) {
                        REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL, "Failed to parse section name");
                    }
                    currentSection = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
                    if (currentSection == nullptr) {
                        REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate configuration section");
                    }
                    if (!parsedSections.push_back(currentSection)) {
                        REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL, "Failed to add parsed section");
                    }
                    itr = sectionMap.insert(ConfigSectionMap::value_type(sectionFullName, currentSection)).first;
                }
                currentSection = itr->second;

                // parse the key-value part
                if (!ConfigFileParser::ParseKeyValue(keyValuePart, sectionFullName, key, value, arrayIndex)) {
                    // key-value line malformed
                    REPORT_PARSE_ERROR(MOT_ERROR_INVALID_CFG,
                        "Failed to parse configuration file %s at line %d: %s (key/value malformed)",
                        configFilePath,
                        reader.GetLineNumber(),
                        line.c_str());
                } else {
                    // check for array item
                    if (arrayIndex == -1) {
                        ConfigItem* configItem = ConfigFileParser::MakeConfigValue(sectionFullName, key, value);
                        if (configItem == nullptr) {
                            REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL,
                                "Failed to parse configuration file %s at line %d: %s (invalid type specification?)",
                                configFilePath,
                                reader.GetLineNumber(),
                                line.c_str());
                        } else if (!currentSection->AddConfigItem(configItem)) {
                            REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to add configuration item to parent section");
                        }
                    } else {
                        ConfigArray* configArray = currentSection->ModifyConfigArray(key.c_str());
                        if (configArray == nullptr) {
                            configArray = ConfigArray::CreateConfigArray(sectionFullName.c_str(), key.c_str());
                            if (configArray == nullptr) {
                                REPORT_PARSE_ERROR(MOT_ERROR_OOM, "Failed to allocate configuration array");
                            } else if (!currentSection->AddConfigItem(configArray)) {
                                REPORT_PARSE_ERROR(
                                    MOT_ERROR_OOM, "Failed to add configuration array to parent section");
                            }
                        }
                        if ((uint32_t)arrayIndex != configArray->GetConfigItemCount()) {
                            // array items must be ordered
                            REPORT_PARSE_ERROR(MOT_ERROR_INVALID_CFG,
                                "Failed to parse configuration file %s at line %d: %s (array %s items not "
                                "well-ordered, expecting %u, got %u)",
                                configFilePath,
                                reader.GetLineNumber(),
                                line.c_str(),
                                configArray->GetName(),
                                configArray->GetConfigItemCount(),
                                (uint32_t)arrayIndex);
                        } else {
                            ConfigItem* configItem =
                                ConfigFileParser::MakeArrayConfigValue(sectionFullName, arrayIndex, value);
                            if (configItem == nullptr) {
                                REPORT_PARSE_ERROR(MOT_ERROR_INTERNAL, "Failed to create configuration array value");
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
        reader.NextLine();
    }

    if (parseError) {
        delete configTree;
        configTree = nullptr;
    } else {
        if (!configTree->Build(parsedSections)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to build configuration tree");
            delete configTree;
            configTree = nullptr;
        }
    }
    return configTree;
}
}  // namespace MOT
