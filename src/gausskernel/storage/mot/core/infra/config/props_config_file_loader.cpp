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
 *    src/gausskernel/storage/mot/core/infra/config/props_config_file_loader.cpp
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

#define PROPS_REPORT_PARSE_ERROR_AND_BREAK(errorCode, format, ...)                \
    {                                                                             \
        MOT_REPORT_ERROR(errorCode, "Load Configuration", format, ##__VA_ARGS__); \
        parseError = true;                                                        \
        break;                                                                    \
    }

static ConfigSection* GetPropsConfigSection(
    const mot_string& sectionFullName, mot_list<ConfigSection*>& parsedSections, ConfigSectionMap& sectionMap)
{
    mot_string sectionPath;
    mot_string sectionName;
    mot_map<mot_string, ConfigSection*>::iterator itr = sectionMap.find(sectionFullName);
    if (itr == sectionMap.end()) {
        if (!ConfigFileParser::BreakSectionName(sectionFullName, sectionPath, sectionName)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse section name");
            return nullptr;
        }

        ConfigSection* currentSection = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
        if (currentSection == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration section");
            return nullptr;
        }

        if (!parsedSections.push_back(currentSection)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to add parsed section");
            return nullptr;
        }

        itr = sectionMap.insert(ConfigSectionMap::value_type(sectionFullName, currentSection)).first;
    }

    return itr->second;
}

static bool AddPropsArrayConfigItem(const char* configFilePath, const FileLineReader& reader, const mot_string& line,
    ConfigSection* currentSection, const mot_string& sectionFullName, const mot_string& key, const mot_string& value,
    uint64_t arrayIndex)
{
    ConfigArray* configArray = currentSection->ModifyConfigArray(key.c_str());
    if (configArray == nullptr) {
        configArray = ConfigArray::CreateConfigArray(sectionFullName.c_str(), key.c_str());
        if (configArray == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration array");
            return false;
        }

        if (!currentSection->AddConfigItem(configArray)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to add configuration array to parent section");
            return false;
        }
    }

    if (arrayIndex != configArray->GetConfigItemCount()) {
        // array items must be ordered
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_CFG,
            "Load Configuration",
            "Failed to parse configuration file %s at line %u: %s (array %s items not "
            "well-ordered, expecting %u, got %" PRIu64 ")",
            configFilePath,
            reader.GetLineNumber(),
            line.c_str(),
            configArray->GetName(),
            configArray->GetConfigItemCount(),
            arrayIndex);
        return false;
    }

    ConfigItem* configItem = ConfigFileParser::MakeArrayConfigValue(sectionFullName, arrayIndex, value);
    if (configItem == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to create configuration array value");
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
    ConfigSection* currentSection = nullptr;
    mot_list<ConfigSection*> parsedSections;
    ConfigSectionMap sectionMap;
    mot_string key;
    mot_string value;
    bool parseError = false;
    uint64_t arrayIndex = 0;
    bool hasArrayIndex = false;

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
            PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_OOM, "Failed to allocate memory for next line");
        }
        line.trim();
        if (line.length() && line[0] != '#') {
            // break tokens to section and key-value by last separator
            MOT_LOG_DEBUG("Parsing config line: %s", line.c_str());
            if (!ParsePropsSectionName(line, sectionFullName, keyValuePart)) {
                PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INVALID_CFG,
                    "Failed to parse configuration file %s at line %u: %s (section name line malformed)",
                    configFilePath,
                    reader.GetLineNumber(),
                    line.c_str());
            }

            // get the configuration section
            currentSection = GetPropsConfigSection(sectionFullName, parsedSections, sectionMap);
            if (currentSection == nullptr) {
                PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL, "Failed to get configuration section");
            }

            // parse the key-value part
            if (!ConfigFileParser::ParseKeyValue(
                keyValuePart, sectionFullName, key, value, arrayIndex, hasArrayIndex)) {
                // key-value line malformed
                PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INVALID_CFG,
                    "Failed to parse configuration file %s at line %u: %s (key/value malformed)",
                    configFilePath,
                    reader.GetLineNumber(),
                    line.c_str());
            }

            // check for array item
            if (!hasArrayIndex) {
                ConfigItem* configItem = ConfigFileParser::MakeConfigValue(sectionFullName, key, value);
                if (configItem == nullptr) {
                    PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_INTERNAL,
                        "Failed to parse configuration file %s at line %u: %s (invalid type specification?)",
                        configFilePath,
                        reader.GetLineNumber(),
                        line.c_str());
                } else if (!currentSection->AddConfigItem(configItem, true)) {
                    PROPS_REPORT_PARSE_ERROR_AND_BREAK(
                        MOT_ERROR_OOM, "Failed to add configuration item to parent section");
                }
            } else {
                if (!AddPropsArrayConfigItem(
                    configFilePath, reader, line, currentSection, sectionFullName, key, value, arrayIndex)) {
                    PROPS_REPORT_PARSE_ERROR_AND_BREAK(MOT_ERROR_OOM,
                        "Failed to add array item with arrayIndex %lu in configuration file %s at line %u: %s",
                        arrayIndex,
                        configFilePath,
                        reader.GetLineNumber(),
                        line.c_str());
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
