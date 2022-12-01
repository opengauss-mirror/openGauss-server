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
 * config_manager.cpp
 *    Configuration manager to manage all system configuration.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_manager.h"
#include "mot_configuration.h"
#include "cmdline_config_loader.h"
#include "cycles.h"
#include "props_config_file_loader.h"
#include "mot_error.h"
#include "log_level_formatter.h"

#include <unistd.h>
#include <cstdlib>
#include <algorithm>

namespace MOT {
DECLARE_LOGGER(ConfigManager, Configuration)

ConfigManager* ConfigManager::m_manager = nullptr;

ConfigManager::ConfigManager() : m_configLoaderCount(0), m_listenerCount(0)
{
    errno_t erc = memset_s(m_configLoaders, sizeof(m_configLoaders), 0, sizeof(m_configLoaders));
    securec_check(erc, "\0", "\0");
    erc = memset_s(m_listeners, sizeof(m_listeners), 0, sizeof(m_listeners));
    securec_check(erc, "\0", "\0");
}

ConfigManager::~ConfigManager()
{
    // cleanup all configuration loaders
    for (uint32_t i = 0; i < m_configLoaderCount; ++i) {
        if (m_configLoaders[i]) {
            // remove configuration tree carefully (some might have failed to load)
            if (m_configLoaders[i]->GetConfig() != nullptr) {
                m_layeredConfigTree.RemoveConfigTree(m_configLoaders[i]->GetConfig());
            }
            delete m_configLoaders[i];
            m_configLoaders[i] = nullptr;
        }
    }
}

bool ConfigManager::CreateInstance(char** argv /* = nullptr */, int argc /* = 0 */)
{
    bool result = false;
    MOT_ASSERT(m_manager == nullptr);
    if (m_manager == nullptr) {
        m_manager = new (std::nothrow) ConfigManager();
        if (m_manager == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Load Configuration", "Failed to allocate memory for configuration manager, aborting");
        } else {
            result = m_manager->Initialize(argv, argc);
            if (!result) {
                delete m_manager;
                m_manager = nullptr;
            }
        }
    }
    return result;
}

void ConfigManager::DestroyInstance()
{
    MOT_ASSERT(m_manager != nullptr);
    if (m_manager != nullptr) {
        delete m_manager;
        m_manager = nullptr;
    }
}

ConfigManager& ConfigManager::GetInstance()
{
    MOT_ASSERT(m_manager != nullptr);
    return *m_manager;
}

bool ConfigManager::AddConfigFile(const char* configFilePath, const char* name /* = "Main" */,
    ConfigFileFormat configFileFormat /* = ConfigFileFormat::CONFIG_FILE_FORMAT_NONE */)
{
    bool result = false;
    ConfigFileLoader* cfgFileLoader = nullptr;

    switch (configFileFormat) {
        case ConfigFileFormat::CONFIG_FILE_PROPS:
            cfgFileLoader = new (std::nothrow) PropsConfigFileLoader(name, CFG_FILE_CONFIG_PRIORITY, configFilePath);
            break;

        case ConfigFileFormat::CONFIG_FILE_FORMAT_NONE:
            cfgFileLoader = CreateConfigFileLoader(configFilePath, name);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Load Configuration",
                "Invalid configuration file format specification %d",
                (int)configFileFormat);
            return false;
    }

    if (cfgFileLoader != nullptr) {
        result = AddConfigLoader(cfgFileLoader);
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Load Configuration",
                "Failed to add configuration file loader by name %s (duplicate?)",
                name);
            delete cfgFileLoader;
        }
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration file loader");
        return false;
    }

    return result;
}

static bool CompareConfgLoaders(ConfigLoader* lhs, ConfigLoader* rhs)
{
    int lhsPriority = lhs->GetPriority();
    int rhsPriority = rhs->GetPriority();
    // higher priority number (i.e. lesser real priority) should go first
    if (lhsPriority < rhsPriority) {
        // lhs has lower priority number (i.e. higher real priority) so it should go after rhs
        return false;
    }
    return true;
}

bool ConfigManager::AddConfigLoader(ConfigLoader* configLoader)
{
    bool result = false;
    MOT_LOG_TRACE("Adding configuration loader %s", configLoader->GetName());

    if (m_configLoaderCount == MAX_CONFIG_LOADER_COUNT) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Load Configuration",
            "Cannot add configuration loader %s: Reached limit %u",
            configLoader->GetName(),
            (unsigned)MAX_CONFIG_LOADER_COUNT);
    } else {
        // insert sorted in descending order - we want lower priority loaders to load configuration first, so higher
        // priority loaders can impose other limits based on configuration loaded already (as in external loaders that
        // impose envelope configuration)
        m_configLoaders[m_configLoaderCount++] = configLoader;
        std::sort(m_configLoaders, m_configLoaders + m_configLoaderCount, CompareConfgLoaders);
        uint32_t configLoaderPos =
            std::find(m_configLoaders, m_configLoaders + m_configLoaderCount, configLoader) - m_configLoaders;
        MOT_LOG_TRACE(
            "Configuration loader %s added successfully at slot %u", configLoader->GetName(), configLoaderPos);
        result = true;
    }

    return result;
}

bool ConfigManager::RemoveConfigLoader(const char* configName)
{
    bool result = false;
    MOT_LOG_TRACE("Removing configuration loader %s [%u registered]", configName, m_configLoaderCount);

    uint32_t i = 0;
    for (; i < m_configLoaderCount; ++i) {
        if (strcmp(m_configLoaders[i]->GetName(), configName) == 0) {
            MOT_LOG_TRACE("Configuration loader %s found at slot %u", configName, i);
            m_layeredConfigTree.RemoveConfigTree(m_configLoaders[i]->GetConfig());
            m_configLoaders[i] = nullptr;  // make sure loader is nullified, just for safety
            result = true;
            break;
        }
    }

    if (result) {
        MOT_LOG_TRACE("Removing configuration loader %s from slot %u", configName, i);
        for (; i < m_configLoaderCount - 1; ++i) {
            m_configLoaders[i] = m_configLoaders[i + 1];
        }
        --m_configLoaderCount;
        MOT_LOG_TRACE("Configuration loader %s removed successfully [%u registered]", configName, m_configLoaderCount);
    } else {
        MOT_LOG_ERROR("Failed to remove configuration loader [%s]: not found", configName);
    }

    return result;
}

bool ConfigManager::AddConfigChangeListener(IConfigChangeListener* listener)
{
    bool result = false;

    if (m_listenerCount == MAX_CONFIG_LISTENER_COUNT) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "Load Configuration",
            "Cannot add configuration listener: Reached limit %u",
            (unsigned)MAX_CONFIG_LISTENER_COUNT);
    } else {
        m_listeners[m_listenerCount++] = listener;
        result = true;
    }

    return result;
}

bool ConfigManager::RemoveConfigChangeListener(IConfigChangeListener* listener)
{
    bool result = false;

    uint32_t i = 0;
    for (; i < m_listenerCount; ++i) {
        if (m_listeners[i] == listener) {
            result = true;
            break;
        }
    }
    if (result) {
        for (; i < m_listenerCount - 1; ++i) {
            m_listeners[i] = m_listeners[i + 1];
        }
    }

    if (!result) {
        MOT_LOG_ERROR("Failed to remove configuration change listener: not found");
    }

    return result;
}

bool ConfigManager::InitLoad()
{
    bool result = false;

    // load configuration from all configuration loaders
    if (!ReloadConfig(false)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Load Configuration", "Failed to reload configuration for the first time");
        if (MOT_CHECK_LOG_LEVEL(LogLevel::LL_TRACE)) {
            MOT_LOG_ERROR_STACK("Failed to load configuration (MOT engine continues to load):");
        }
        ClearErrorStack();  // reset error state

        // nevertheless we continue with default configuration
    }

    // trigger update from just-loaded configuration (only to this specific listener).
    MOT_LOG_DEBUG("Reloaded MOTConfiguration from main configuration");
    MOTConfiguration& motCfg = GetGlobalConfiguration();
    motCfg.OnConfigChange();

    // now the main configuration is fully loaded, so let's validate it
    if (motCfg.IsValid()) {
        result = true;
    }
    return result;
}

const ConfigTree* ConfigManager::GetConfigTree(const char* configName) const
{
    const ConfigTree* result = nullptr;
    for (uint32_t i = 0; i < m_configLoaderCount; ++i) {
        if (strcmp(m_configLoaders[i]->GetName(), configName) == 0) {
            result = m_configLoaders[i]->GetConfig();
            break;
        }
    }
    return result;
}

bool ConfigManager::Initialize(char** argv, int argc)
{
    bool result = true;

    char* envvar = getenv("MOT_DEBUG_CFG_LOAD");
    if (envvar && (strcmp(envvar, "TRUE") == 0)) {
        (void)SetLogComponentLogLevel("Configuration", LogLevel::LL_DEBUG);
    }

    // add command line configuration loader
    if (argv != nullptr && argc != 0) {
        result = AddCmdLineConfigLoader(argv, argc);
        if (!result) {
            MOT_REPORT_PANIC(MOT_ERROR_INTERNAL,
                "Configuration Manager Initialization",
                "Failed to create command line configuration loader");
        }
    }

    return result;
}

bool ConfigManager::AddCmdLineConfigLoader(char** argv, int argc)
{
    bool result = true;
    ConfigLoader* cmdLineCfgLoader = new (std::nothrow) CmdLineConfigLoader(argv, argc);
    if (cmdLineCfgLoader == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate command line configuration loader");
        result = false;
    } else {
        result = AddConfigLoader(cmdLineCfgLoader);
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Load Configuration",
                "Failed to add command line configuration loader (duplicate?)");
            delete cmdLineCfgLoader;
        }
    }
    return result;
}

ConfigFileLoader* ConfigManager::CreateConfigFileLoader(const char* configFilePath, const char* name)
{
    ConfigFileLoader* cfgFileLoader = nullptr;
    std::string cfgPath(configFilePath);
    std::string::size_type dotPos = cfgPath.find_last_of('.');
    if (dotPos == std::string::npos) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Load Configuration",
            "Unable to load configuration file. Format not specified and configuration file does not contain suffix: "
            "%s",
            cfgPath.c_str());
        return nullptr;
    } else {
        std::string suffix = cfgPath.substr(dotPos + 1);
        if (suffix.compare("conf") == 0) {
            cfgFileLoader = new (std::nothrow) PropsConfigFileLoader(name, CFG_FILE_CONFIG_PRIORITY, configFilePath);
        } else {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "Load Configuration",
                "Unable to load configuration file: Format not specified and cannot be inferred from file suffix: %s",
                suffix.c_str());
            return nullptr;
        }
    }

    if (cfgFileLoader == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Load Configuration", "Failed to allocate configuration file loader");
    }

    return cfgFileLoader;
}

bool ConfigManager::ReloadConfig(bool ignoreErrors /* = true */)
{
    bool result = true;
    MOT_LOG_DEBUG("Reloading configuration");
    m_layeredConfigTree.Clear();
    for (uint32_t i = 0; i < m_configLoaderCount; ++i) {
        ConfigLoader* configLoader = m_configLoaders[i];
        ConfigTree* configTree = configLoader->Load();
        if (configTree == nullptr) {
            MOT_LOG_WARN("Failed to load configuration tree from configuration loader %s, configuration from this "
                         "loader will be ignored. Please fix configuration and reload.",
                configLoader->GetName());
            if (!ignoreErrors) {
                result = false;
            }
            continue;
        }
        MOT_LOG_DEBUG("Adding configuration tree %p with priority %u from configuration loader %s",
            configTree,
            configTree->GetPriority(),
            configLoader->GetName());
        if (!m_layeredConfigTree.AddConfigTree(configTree)) {
            MOT_LOG_WARN("Failed to add configuration tree from configuration loader %s, configuration from this "
                         "loader will be ignored. Please fix configuration and reload.",
                configLoader->GetName());
            if (!ignoreErrors) {
                result = false;
            }
        }
    }

    // print loaded configuration
    LogLevel logLevel = LogLevel::LL_TRACE;
    logLevel = GetLayeredConfigTree()->GetUserConfigValue("Log.cfg_startup_log_level", logLevel, false);
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {
        GetLayeredConfigTree()->Print(logLevel);
    }

    return result;
}

void ConfigManager::NotifyConfigChange()
{
    MOT_LOG_INFO("Propagating configuration changes");
    for (uint32_t i = 0; i < m_listenerCount; ++i) {
        m_listeners[i]->OnConfigChange();
    }
}
}  // namespace MOT
