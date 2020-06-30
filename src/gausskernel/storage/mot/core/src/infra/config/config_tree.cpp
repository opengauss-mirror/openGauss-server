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
 * config_tree.cpp
 *    Configuration tree implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/infra/config/config_tree.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <vector>

#include "config_file_parser.h"
#include "config_tree.h"

namespace MOT {
IMPLEMENT_CLASS_LOGGER(ConfigTree, Configuration)

/** @define Limit the depth of a section. */
#define MAX_SECTION_DEPTH 10

bool ConfigTree::Build(mot_list<ConfigSection*>& parsedSections)
{
    bool result = true;
    MOT_LOG_DEBUG("Building config tree");
    // insert all section to map according to full path name
    ConfigSectionMap sectionMap;
    mot_list<ConfigSection*>::iterator listItr = parsedSections.begin();
    while (listItr != parsedSections.end()) {
        ConfigSection* section = *listItr;

        // special case: root section
        if (section->GetDepth() == 0) {
            if (!m_rootSection.Merge(section)) {
                MOT_LOG_WARN("Malformed configuration file: failed to merge root section");
                result = false;
            }
            // anyway must cleanup, since this section is not added to tree
            delete section;
            listItr = parsedSections.erase(listItr);
            continue;
        }

        ConfigSectionMap::iterator itr2 = sectionMap.find(section->GetFullPathName());
        if (itr2 == sectionMap.end()) {
            // insert new section
            MOT_LOG_DEBUG("Adding section: %s (name: %s [%p])",
                section->GetFullPathName(),
                section->GetName(),
                section->GetName());
            if (!sectionMap.insert(ConfigSectionMap::value_type(section->GetFullPathName(), section)).second) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to build configuration tree, cannot add section %s",
                        section->GetFullPathName());
                    result = false;  // we continue so we can cleanup all sections
                } else {
                    MOT_LOG_WARN("Malformed configuration file: duplicate section %s", section->GetFullPathName());
                }
            }
        } else {
            // merge into existing section
            ConfigSection* existingSection = itr2->second;
            MOT_LOG_DEBUG("Merging section %s to section %s by path %s",
                section->GetName(),
                existingSection->GetName(),
                section->GetFullPathName());
            if (!existingSection->Merge(section)) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Load Configuration", "Failed to merge section %s", section->GetFullPathName());
                result = false;  // we continue so we can cleanup all sections
            }
            delete section;
        }
        ++listItr;
    }
    parsedSections.clear();
    if (!result) {
        return result;
    }

    // traverse all sections, and link each one to its parent
    MOT_LOG_DEBUG("Linking sections");
    ConfigSectionMap::iterator mapItr = sectionMap.begin();
    while (mapItr != sectionMap.end()) {
        ConfigSection* section = mapItr->second;
        MOT_LOG_DEBUG("Linking section %s with depth %u", section->GetFullPathName(), section->GetDepth());
        if (section->GetDepth() == 1) {
            MOT_LOG_DEBUG("Setting main section: %s", section->GetName());
            if (!AddMainSection(section)) {
                MOT_LOG_ERROR("Ignoring duplicate section %s: parent root section already has such a sub-section",
                    section->GetName());
            }
        } else {
            // find parent section (create an empty parent if non was found)
            ConfigSectionMap::iterator itr2 = sectionMap.find(section->GetPath());
            ConfigSection* parentSection = nullptr;
            if (itr2 != sectionMap.end()) {
                parentSection = itr2->second;
            } else {
                MOT_LOG_WARN("Malformed configuration file: found section %s without a parent section (Searched "
                             "parent path: %s)",
                    section->GetFullPathName(),
                    section->GetPath());
                parentSection = CreateSectionPath(sectionMap, section->GetPath(), 0);
            }
            MOT_LOG_DEBUG("linking section %s to parent section %s by parent path %s",
                section->GetName(),
                parentSection->GetName(),
                section->GetPath());
            if (!parentSection->AddConfigItem(section)) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to add configuration section %s to parent section %s",
                        section->GetName(),
                        parentSection->GetFullPathName());
                    return false;
                }
                MOT_LOG_WARN("Ignoring duplicate section %s: parent section %s already has such a sub-section",
                    section->GetName(),
                    parentSection->GetName());
            }
        }
        ++mapItr;
    }

    MOT_LOG_DEBUG("Finished building configuration tree %p %s with priority %u", this, GetSource(), m_priority);
    m_rootSection.Print(LogLevel::LL_DEBUG, true);
    MOT_LOG_DEBUG("=================================");
    Print(LogLevel::LL_DEBUG);
    MOT_LOG_DEBUG("=================================");
    return true;
}

const ConfigItem* ConfigTree::GetConfigItem(const char* fullPathName) const
{
    MOT_LOG_DEBUG("Getting from tree %s config item %s: starting", GetSource(), fullPathName);
    const ConfigItem* result = nullptr;

    // break path name to components
    mot_string_list pathComponents;
    if (!ConfigFileParser::Split(fullPathName, ConfigItem::PATH_SEP, pathComponents)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to split configuration path %s into components",
            fullPathName);
        return nullptr;
    }
    MOT_LOG_DEBUG("Getting from tree %s config item %s: finished split", GetSource(), fullPathName);

    // dig into the tree
    const ConfigSection* currentSection = &m_rootSection;
    mot_string_list::iterator itr = pathComponents.begin();
    while (itr != pathComponents.end() && currentSection) {
        MOT_LOG_DEBUG("Getting from section %s config item %s", currentSection->GetFullPathName(), itr->c_str());
        result = currentSection->GetConfigItem(itr->c_str());
        if (result == nullptr) {
            break;
        }
        if (result->GetClass() == ConfigItemClass::CONFIG_ITEM_SECTION) {
            currentSection = static_cast<const ConfigSection*>(result);
        } else {
            currentSection = nullptr;
        }
        ++itr;
    }

    if (itr != pathComponents.end()) {
        result = nullptr;
    }

    return result;
}

ConfigSection* ConfigTree::CreateSectionPath(ConfigSectionMap& sectionMap, const char* fullPathName, int depth) const
{
    ConfigSection* result = nullptr;
    mot_string sectionPath;
    mot_string sectionName;
    if (depth >= MAX_SECTION_DEPTH) {
        MOT_LOG_ERROR("Failed to create section, section depth is too big");
        return nullptr;
    }
    if (!ConfigFileParser::BreakSectionName(fullPathName, sectionPath, sectionName)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse section name");
    } else {
        result = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
        if (result == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration section");
        } else {
            // make sure parent section exists (recursive call)
            if ((result->GetDepth() > 0) && (sectionMap.find(result->GetPath()) == sectionMap.end())) {
                ConfigSection* parentSection = CreateSectionPath(sectionMap, result->GetPath(), depth + 1);
                if (parentSection == nullptr) {
                    MOT_REPORT_ERROR(
                        MOT_ERROR_OOM, "Load Configuration", "Failed to allocate empty parent configuration section");
                    delete result;
                    result = nullptr;
                } else {
                    if (!sectionMap.insert(ConfigSectionMap::value_type(result->GetFullPathName(), result)).second) {
                        if (MOT_IS_SEVERE()) {
                            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                                "Load Configuration",
                                "Failed to build configuration tree, cannot add section %s",
                                result->GetFullPathName());
                            delete result;
                            result = nullptr;  // we continue so we can cleanup all sections
                        } else {
                            MOT_LOG_WARN(
                                "Malformed configuration file: duplicate section %s", result->GetFullPathName());
                        }
                    }
                }
            }
        }
    }
    return result;
}
}  // namespace MOT
