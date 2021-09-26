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
 * layered_config_tree.cpp
 *    Implements layering of prioritized configuration trees.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/layered_config_tree.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "layered_config_tree.h"
#include "mot_error.h"

#include <string.h>

#include <algorithm>

namespace MOT {
typedef LayeredConfigTree::ConfigTreeList ConfigTreeList;
typedef LayeredConfigTree::LayeredConfigTrees LayeredConfigTrees;

IMPLEMENT_CLASS_LOGGER(LayeredConfigTree, Configuration)

// Helper class for building the print list
class PrintVisitor : public ConfigItemVisitor {
public:
    /** @var The configuration list to build. */
    ConfigItemList* m_printList;

    /** @brief The set of all configuration items that were added. */
    mot_set<mot_string> m_itemsAdded;

    /** @var The operation status. */
    bool m_status;

    /**
     * @brief Constructor.
     * @param printList The configuration list to build.
     */
    explicit PrintVisitor(ConfigItemList* printList) : m_printList(printList), m_status(true)
    {}

    ~PrintVisitor() override
    {}

    /** @brief Queries whether status is ok or error occurred. */
    inline bool GetStatus() const
    {
        return m_status;
    }

    /**
     * @brief Adds configuration item to print list.
     * @param configItem The configuration item.
     */
    void OnConfigItem(const ConfigItem* configItem) override
    {
        if (!m_status) {
            return;
        }

        if (configItem->GetClass() == ConfigItemClass::CONFIG_ITEM_VALUE) {
            if (m_itemsAdded.find(configItem->GetFullPathName()) == m_itemsAdded.end()) {
                if (!m_printList->push_back(const_cast<ConfigItem*>(configItem))) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to add item to print list");
                    m_status = false;
                    return;
                }
                if (!m_itemsAdded.insert(configItem->GetFullPathName()).second) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to add item to items-added set");
                    m_status = false;
                    return;
                }
                if (MOT_CHECK_LOG_LEVEL(LogLevel::LL_DEBUG)) {
                    MOT_LOG_DEBUG("Adding config item to print list:");
                    configItem->Print(LogLevel::LL_DEBUG, true);
                }
            }
        }
    }

private:
    DECLARE_CLASS_LOGGER()
};

IMPLEMENT_CLASS_LOGGER(PrintVisitor, Configuration)

LayeredConfigTree::~LayeredConfigTree()
{
    ClearConfigTrees();
}

bool LayeredConfigTree::AddConfigTree(ConfigTree* configTree)
{
    bool configTreeAdded = false;
    MOT_LOG_DEBUG("Adding configuration tree %p %s with priority %d",
        configTree,
        configTree->GetSource(),
        configTree->GetPriority());

    // insert sorted
    LayeredConfigTrees::iterator itr = m_configTrees.begin();
    while (itr != m_configTrees.end()) {
        // check next list (get priority of first tree in the list (all trees in the list have same priority)
        ConfigTreeList& configTreeList = *itr;
        MOT_ASSERT(!configTreeList.empty());
        ConfigTree* currConfigTree = *configTreeList.begin();
        if (currConfigTree->GetPriority() == configTree->GetPriority()) {
            // matching list found, we are done
            configTreeAdded = AddConfigTreeToList(configTree, configTreeList);
            return configTreeAdded;  // whether succeeded or not, we are done
        } else if (currConfigTree->GetPriority() < configTree->GetPriority()) {
            // not found yet, go on to next list
            ++itr;
        } else {
            // the priority of this list is too large, so we need to add a new list before the current one
            configTreeAdded = AddNewConfigTreeAt(itr, configTree);
            return configTreeAdded;  // whether succeeded or not, we are done
        }
    }

    if (!configTreeAdded) {
        // create a new list for this tree and put it as last (it has the worse priority)
        configTreeAdded = AddNewConfigTreeAt(m_configTrees.end(), configTree);
    }

    return configTreeAdded;
}

void LayeredConfigTree::RemoveConfigTree(ConfigTree* configTree)
{
    int priority = 0;
    bool found = false;
    MOT_LOG_DEBUG("Removing configuration tree %p %s with priority %d",
        configTree,
        configTree->GetSource(),
        configTree->GetPriority());
    LayeredConfigTrees::iterator itr = m_configTrees.begin();
    while (itr != m_configTrees.end()) {
        ConfigTreeList& configTreeList = *itr;
        MOT_ASSERT(!configTreeList.empty());
        ConfigTreeList::iterator itr2 = std::find(configTreeList.begin(), configTreeList.end(), configTree);
        if (itr2 == configTreeList.end()) {
            MOT_LOG_TRACE(
                "Configuration tree %s not found in list priority %d in layered configuration tree (search continues)",
                configTree->GetSource(),
                priority);
            ++itr;       // keep searching
            ++priority;  // for debug printing
        } else {
            MOT_LOG_TRACE("Removing configuration tree %s from layered configuration tree", configTree->GetSource());
            configTreeList.erase(itr2);
            if (configTreeList.empty()) {
                m_configTrees.erase(itr);
            }
            found = true;
            break;  // stop searching
        }
    }
    if (!found) {
        MOT_LOG_WARN("Failed to remove configuration tree %s from layered configuration tree: not found",
            configTree->GetSource());
    }
}

void LayeredConfigTree::Print(LogLevel logLevel) const
{
    MOT_LOG(logLevel, "Loaded configuration:");
    if (m_printList.empty()) {
        if (!const_cast<LayeredConfigTree*>(this)->BuildPrintList()) {
            MOT_LOG_ERROR_STACK("Failed to build print list for layered configuration tree");
            ClearErrorStack();
        }
    }
    ConfigItemList::const_iterator itr = m_printList.begin();
    while (itr != m_printList.end()) {
        const ConfigItem* configItem = *itr;
        configItem->Print(logLevel, true);
        ++itr;
    }
}

const ConfigItem* LayeredConfigTree::GetConfigItem(const char* fullPathName) const
{
    const ConfigItem* result = nullptr;

    // search item layer by layer
    LayeredConfigTrees::const_iterator itr = m_configTrees.begin();
    while (result == nullptr && itr != m_configTrees.end()) {
        const ConfigTreeList& configTreeList = *itr;
        ConfigTreeList::const_iterator itr2 = configTreeList.begin();
        while (result == nullptr && itr2 != configTreeList.end()) {
            const ConfigTree* configTree = *itr2;
            result = configTree->GetConfigItem(fullPathName);
            if (result != nullptr) {
                MOT_LOG_DEBUG("*** --> Found %s", fullPathName);
                if (MOT_CHECK_LOG_LEVEL(LogLevel::LL_DEBUG)) {
                    result->Print(LogLevel::LL_DEBUG, true);
                }
            } else {
                ++itr2;
            }
        }
        ++itr;
    }

    return result;
}

bool LayeredConfigTree::BuildPrintList()
{
    // add configuration items layer by layer, only if not added already
    // 1. define item visitor
    // 2. visit all trees layer by layer
    MOT_LOG_DEBUG("Building layered config tree print list");
    PrintVisitor pv(&m_printList);
    LayeredConfigTrees::const_iterator itr = m_configTrees.cbegin();
    while (itr != m_configTrees.cend()) {
        const ConfigTreeList& configTreeList = *itr;
        ConfigTreeList::const_iterator itr2 = configTreeList.begin();
        while (itr2 != configTreeList.end()) {
            const ConfigTree* configTree = *itr2;
            MOT_LOG_DEBUG("Scanning config tree %p %s with priority %d",
                configTree,
                configTree->GetSource(),
                configTree->GetPriority());
            configTree->ForEach(pv);
            if (!pv.GetStatus()) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Load Configuration",
                    "Failed to build print list for configuration tree %s",
                    configTree->GetSource());
                return false;
            }
            ++itr2;
        }
        ++itr;
    }
    return true;
}

void LayeredConfigTree::ClearConfigTrees()
{
    MOT_LOG_DEBUG("Clearing all configuration trees from layered configuration tree");
    LayeredConfigTrees::iterator itr = m_configTrees.begin();
    while (itr != m_configTrees.end()) {
        ConfigTreeList& configTreeList = *itr;
        ConfigTreeList::iterator litr = configTreeList.begin();
        while (litr != configTreeList.end()) {
            ConfigTree* configTree = *litr;
            if (!configTree->IsStatic()) {
                MOT_LOG_DEBUG("Destroying configuration tree %p %s", configTree, configTree->GetSource());
                delete configTree;
            }
            ++litr;
        }
        configTreeList.clear();
        ++itr;
    }
    m_configTrees.clear();
    m_printList.clear();
}

bool LayeredConfigTree::AddNewConfigTreeAt(LayeredConfigTrees::iterator itr, ConfigTree* configTree)
{
    bool configTreeAdded = false;
    LayeredConfigTrees::pairib pb = m_configTrees.insert(itr, ConfigTreeList());
    if (!pb.second) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to insert configuration tree list");
    } else {
        ConfigTreeList& configTreeList = *pb.first;
        configTreeAdded = AddConfigTreeToList(configTree, configTreeList);
    }
    return configTreeAdded;
}

bool LayeredConfigTree::AddConfigTreeToList(ConfigTree* configTree, ConfigTreeList& configTreeList)
{
    bool configTreeAdded = configTreeList.push_back(configTree);
    if (!configTreeAdded) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to add configuration tree to new list");
    }
    return configTreeAdded;
}
}  // namespace MOT
