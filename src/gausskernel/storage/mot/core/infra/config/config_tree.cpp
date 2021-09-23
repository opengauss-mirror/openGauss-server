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
 *    src/gausskernel/storage/mot/core/infra/config/config_tree.cpp
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

bool ConfigTree::ConsolidateParsedSections(mot_list<ConfigSection*>& parsedSections, ConfigSectionMap& sectionMap)
{
    // consolidate all parsed section into a map:
    // 1. new sections are added to map and removed from the list (need to delete section from map in case of error)
    // 2. duplicate sections are merged and deleted (no need to delete merged section in case of error)
    // 3. iteration stops on first error, and all sections in section list and map are deleted
    // 4. on successful execution the section list should be empty
    bool result = true;
    mot_list<ConfigSection*>::iterator listItr = parsedSections.begin();
    while (listItr != parsedSections.end()) {
        ConfigSection* section = *listItr;

        // special case: root section
        if (section->GetDepth() == 0) {
            MOT_LOG_DEBUG("Merging root section");
            if (!m_rootSection.Merge(section)) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Malformed configuration file: failed to merge root section");
                result = false;
                break;  // go to cleanup after error (currently iterated section will be cleaned up from list)
            }
            // cleanup merged section now, even if we fail later (because if we don't fail, this is a memory leak)
            delete section;
        } else {
            ConfigSectionMap::iterator mapItr = sectionMap.find(section->GetFullPathName());
            if (mapItr == sectionMap.end()) {
                // insert new section
                MOT_LOG_DEBUG("Adding section: %s (name: %s)", section->GetFullPathName(), section->GetName());
                if (!sectionMap.insert(ConfigSectionMap::value_type(section->GetFullPathName(), section)).second) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to build configuration tree, cannot add section %s",
                        section->GetFullPathName());
                    result = false;
                    break;  // go to cleanup after error (currently iterated section will be cleaned up from list)
                }
            } else {
                // merge into existing section
                ConfigSection* existingSection = mapItr->second;
                MOT_LOG_DEBUG("Merging section: %s (name: %s)", section->GetFullPathName(), section->GetName());
                if (!existingSection->Merge(section)) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                        "Load Configuration",
                        "Failed to merge section %s",
                        section->GetFullPathName());
                    result = false;
                    break;  // go to cleanup after error (currently iterated section will be cleaned up from list)
                }
                // cleanup merged section now, even if we fail later (because if we don't fail, this is a memory leak)
                delete section;
            }
        }
        // whether section added to map or merged, we do not want it to be deleted twice in case of error
        listItr = parsedSections.erase(listItr);
    }

    // upon successful execution the section list must be empty
    MOT_ASSERT(!result || parsedSections.empty());

    // delete all section from list, only if error occurred (otherwise they were added to tree or merged and deleted)
    if (!result) {
        // cleanup parsed sections and section map
        listItr = parsedSections.begin();
        while (listItr != parsedSections.end()) {
            ConfigSection* section = *listItr;
            delete section;
            ++listItr;
        }
        parsedSections.clear();
        ConfigSectionMap::iterator mapItr = sectionMap.begin();
        while (mapItr != sectionMap.end()) {
            ConfigSection* section = mapItr->second;
            delete section;
            ++mapItr;
        }
        sectionMap.clear();
    }
    return result;
}

bool ConfigTree::LinkConsolidatedSectionMap(ConfigSectionMap& sectionMap)
{
    // link consolidated section map. for each iterated section:
    // 1. find or create parent section
    // 2. new sub-sections are added to parent and removed from the map (no need to delete from map in case of error)
    // 3. duplicate sub-sections are merged and deleted (no need to delete merged section in case of error)
    // 4. iteration stops on first error, and all sections in section map are deleted
    // 5. on successful execution the section map should be empty
    bool result = true;
    MOT_LOG_DEBUG("Linking sections");
    ConfigSectionMap::iterator mapItr = sectionMap.begin();
    while (mapItr != sectionMap.end()) {
        ConfigSection* section = mapItr->second;
        MOT_LOG_DEBUG("Linking section %s with depth %u", section->GetFullPathName(), section->GetDepth());
        bool created = false;

        // get the parent or create a new one (recursively)
        ConfigSection* parent = GetOrCreateParent(section, 0, created);
        if (parent == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to get or create parent of section %s",
                section->GetFullPathName());
            result = false;
            break;
        }

        // add or merge the section into its parent
        if (!parent->ContainsConfigSection(section->GetName())) {
            if (!parent->AddConfigItem(section)) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to add section %s to new parent %s",
                    section->GetFullPathName(),
                    parent->GetFullPathName());
                result = false;
                break;
            }
        } else {
            ConfigSection* existingSection = (ConfigSection*)parent->GetConfigSection(section->GetName());
            if (!existingSection->Merge(section)) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to merge existing section %s",
                    section->GetFullPathName());
                result = false;
                break;
            }
            // cleanup merged section now, even if we fail later (because if we don't fail, this is a memory leak)
            delete section;
        }
        // whether section added to tree or merged, we do not want it to be deleted twice in case of error
        mapItr = sectionMap.erase(mapItr);
    }

    // upon successful execution the section map must be empty
    MOT_ASSERT(!result || sectionMap.empty());

    // cleanup if failed
    if (!result) {
        mapItr = sectionMap.begin();
        while (mapItr != sectionMap.end()) {
            ConfigSection* section = mapItr->second;
            delete section;
            ++mapItr;
        }
        sectionMap.clear();
    }
    return result;
}

bool ConfigTree::Build(mot_list<ConfigSection*>& parsedSections)
{
    bool result = true;
    MOT_LOG_DEBUG("Building configuration tree");

    // insert all section to map according to full path name
    ConfigSectionMap sectionMap;
    if (!ConsolidateParsedSections(parsedSections, sectionMap)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to consolidate parsed section list");
        result = false;
    } else {
        if (!LinkConsolidatedSectionMap(sectionMap)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to build configuration tree");
            result = false;
        }
    }

    if (result) {
        MOT_LOG_DEBUG("Finished building configuration tree %p %s with priority %u", this, GetSource(), m_priority);
        m_rootSection.Print(LogLevel::LL_DEBUG, true);
        MOT_LOG_DEBUG("=================================");
        Print(LogLevel::LL_DEBUG);
        MOT_LOG_DEBUG("=================================");
    }
    return result;
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

ConfigSection* ConfigTree::GetOrCreateParent(ConfigSection* section, int depth, bool& created)
{
    MOT_LOG_DEBUG("Building recursive parent for section: %s", section->GetFullPathName());

    // guard against endless recurring calls
    if (depth >= MAX_SECTION_DEPTH) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
            "Load Configuration"
            "Failed to get or create parent of section %s: section depth exceeds maximum allowed (%u)",
            section->GetFullPathName(),
            (unsigned)MAX_SECTION_DEPTH);
        return nullptr;
    }

    // get parent or create it (recursively)
    created = false;
    ConfigSection* parent = nullptr;
    if (section->GetDepth() == 1) {
        MOT_LOG_DEBUG("Found root parent of section %s", section->GetFullPathName());
        parent = &m_rootSection;
    } else {
        parent = (ConfigSection*)GetConfigSection(section->GetPath());
    }
    if (parent == nullptr) {
        MOT_LOG_DEBUG("Parent of section %s not found, creating", section->GetFullPathName());
        parent = CreateParent(section, depth);
        if (parent == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to create section path: %s",
                section->GetFullPathName());
            return nullptr;
        }
        created = true;
    }

    // if this is not the first recurring call, then link the parent to this section
    // (otherwise caller links the section to its parent)
    if (depth > 0) {
        MOT_LOG_TRACE("Linking section %s to its parent %s", section->GetFullPathName(), parent->GetFullPathName());
        if (!parent->AddConfigItem(section)) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to add section %s to parent %s (call depth: %d)",
                section->GetFullPathName(),
                parent->GetFullPathName(),
                depth);
            // if the parent was created, then we should not clean it up, since it is already linked to the tree
            return nullptr;
        }
    }

    return parent;
}

ConfigSection* ConfigTree::CreateParent(ConfigSection* section, int depth)
{
    ConfigSection* parent = nullptr;
    mot_string sectionPath;
    mot_string sectionName;

    MOT_LOG_DEBUG("Creating recursive parent for section: %s", section->GetFullPathName());

    // guard against endless recurring calls
    if (depth >= MAX_SECTION_DEPTH) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
            "Load Configuration"
            "Failed to create parent of section %s: section depth exceeds maximum allowed (%u)",
            section->GetFullPathName(),
            (unsigned)MAX_SECTION_DEPTH);
        return nullptr;
    }

    // parse full section name of parent and create the parent
    if (!ConfigFileParser::BreakSectionName(section->GetPath(), sectionPath, sectionName)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_INTERNAL, "Load Configuration", "Failed to parse section name: %s", section->GetPath());
        return nullptr;
    }
    parent = ConfigSection::CreateConfigSection(sectionPath.c_str(), sectionName.c_str());
    if (parent == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration section");
        return nullptr;
    }

    // get the parent of the new parent section (make sure it exists and linked to the parent we just created)
    bool parentCreated = false;
    ConfigSection* grandParent = GetOrCreateParent(parent, depth + 1, parentCreated);
    if (grandParent == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to get or create parent of parent section %s",
            parent->GetFullPathName());
        delete parent;
        return nullptr;
    }

    return parent;
}
}  // namespace MOT
