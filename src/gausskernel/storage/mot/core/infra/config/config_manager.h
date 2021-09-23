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
 * config_manager.h
 *    Configuration manager to manage all system configuration.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_MANAGER_H
#define CONFIG_MANAGER_H

#include "config_file_loader.h"
#include "iconfig_change_listener.h"
#include "layered_config_tree.h"
#include "config_file_format.h"

/** @define The maximum number of configuration loaders. */
#define MAX_CONFIG_LOADER_COUNT 16

/** @define The maximum number of configuration listeners. */
#define MAX_CONFIG_LISTENER_COUNT 256

namespace MOT {
/**
 * @class ConfigManager
 * @brief Configuration manager to manage all system configuration.
 * @details Configuration may come from different sources. For each such source type there exists an @ref ConfigLoader
 * implementation. Each configuration loader should be registered at the configuration manager, such that it may be
 * accessed easily within the application. Each configuration source is identified by a unique configuration name. The
 * resulting configuration tree of each configuration loader may be accessed through the manager. All registered
 * configuration loaders may be ordered to reload configuration. Following is an example of correct usage:
 *
 * <code>
 * ConfigManager::CreateInstance();
 * ...
 * ConfigManager& mgr = ConfigManager::GetInstance();
 * ...
 * // register configuration loaders and load configuration
 * mgr.AddConfigLoader("name1", configLoader1);
 * mgr.AddConfigLoader("name2", configLoader2);
 * mgr.InitLoad();
 * ...
 * // initialize applicaiton with loaded configuration
 * initApp(mgr.GetLayeredConfigTree());
 * </code>
 *
 * Application shutdown should look like this:
 *
 * <code>
 * ConfigManager::DestroyInstance();
 * </code>
 */
class ConfigManager {
private:
    /** Constructor */
    ConfigManager();

    /** Destructor */
    ~ConfigManager();

public:
    /**
     * @brief Creates singleton instance using command line arguments as configuration source. Must be called once
     * during engine startup.
     * @param[opt] argc Command line argument count (excluding program name first argument).
     * @param[opt] argv Command line argument array (excluding program name first argument).
     * @return True if instance creation succeeded.
     */
    static bool CreateInstance(char** argv = nullptr, int argc = 0);

    /**
     * @brief Creates singleton instance using configuration file as source. Must be called once during engine startup.
     * @param configFile The configuration file from which to load configuration.
     * @return True if instance creation succeeded.
     */
    static bool CreateInstance(const char* configFile);

    /**
     * @brief Destroys singleton instance. Must be called once during engine shutdown.
     */
    static void DestroyInstance();

    /**
     * @brief Retrieves reference to singleton instance.
     * @return Configuration manager
     */
    static ConfigManager& GetInstance();

    /**
     * @brief Registers a file configuration loader.
     * @param configFilePath The configuration file path.
     * @param name The name of the new configuration loader.
     * @param configFileFormat The format of the configuration file.
     * @return True if the configuration loader was registered, or false if another is already
     * registered by the same name.
     */
    bool AddConfigFile(const char* configFilePath, const char* name = "Main",
        ConfigFileFormat configFileFormat = ConfigFileFormat::CONFIG_FILE_FORMAT_NONE);

    /**
     * @brief Registers a named configuration loader.
     * @param configLoader The configuration loader.
     * @return True if the configuration loader was registered, or false if
     * another is already registered by the same name.
     */
    bool AddConfigLoader(ConfigLoader* configLoader);

    /**
     * @brief Unregisters a configuration loader by name.
     * @param configName The configuration name.
     * @return True if the configuration loader was unregistered, or false if none
     * was found by the same name.
     */
    bool RemoveConfigLoader(const char* configName);

    /**
     * @brief Adds a configuration change listener.
     * @param listener The listener to add.
     * @return True if the configuration listener was registered, or false if ran out of room.
     * @note Duplicates are not checked.
     */
    bool AddConfigChangeListener(IConfigChangeListener* listener);

    /**
     * @brief Removes a configuration change listener.
     * @param listener The listener to remove.
     * @return True if the configuration listener was found and removed, otherwise false.
     */
    bool RemoveConfigChangeListener(IConfigChangeListener* listener);

    /**
     * @brief Perform initial loading. This consists of loading from all configuration loaders and
     * notifying only the main configuration. The configuration monitor is not started.
     * @return True if configuration loading succeeded and configuration is valid.
     */
    bool InitLoad();

    /**
     * @brief Retrieves the configuration tree of the given source.
     * @param configName The unique configuration name identifying the source.
     * @return The configuration tree, or null pointer of none was found by the
     * given name.
     */
    const ConfigTree* GetConfigTree(const char* configName) const;

    /**
     * @brief Retrieves The layered configuration tree from all configuration loaders.
     * @details Each configuration loader has a priority. Configuration trees from loaders
     * with higher priority mask configuration trees from lower priority.
     * @return The layered configuration tree.
     */
    const LayeredConfigTree* GetLayeredConfigTree() const
    {
        return &m_layeredConfigTree;
    }

private:
    /** @var The single instance. */
    static ConfigManager* m_manager;

    /** @var The configuration loader map. */
    ConfigLoader* m_configLoaders[MAX_CONFIG_LOADER_COUNT];

    /** @var Number of registered configuration loaders. */
    uint32_t m_configLoaderCount;

    /* @var Configuration change listener list. */
    IConfigChangeListener* m_listeners[MAX_CONFIG_LISTENER_COUNT];

    /** @var Number of registered configuration listeners. */
    uint32_t m_listenerCount;

    /** @var The unified configuration tree. */
    LayeredConfigTree m_layeredConfigTree;

    /** @var Perform application startup. */
    bool Initialize(char** argv, int argc);

    /**
     * @brief Adds a command line configuration loader.
     * @param argv The command line argument array.
     * @param argc The command line argument count.
     * @return True if succeeded, otherwise false.
     */
    bool AddCmdLineConfigLoader(char** argv, int argc);

    /**
     * @brief Create a configuration file loader.
     * @param configFilePath The configuration file path.
     * @param name The name of the new configuration loader.
     * @return The configuration file loader or nullptr if failed.
     */
    ConfigFileLoader* CreateConfigFileLoader(const char* configFilePath, const char* name);

    /**
     * @brief Reloads configuration from all configuration sources. All registered changed listeners
     * on all configuration loaders will be notified about any changes.
     * @param ignoreErrors Specifies whether to ignore errors. This is the default behavior while
     * reloading configuration. During system initialization errors are not ignored.
     * @return True if configuration reloading succeeded, otherwise false.
     */
    bool ReloadConfig(bool ignoreErrors = true);

    /** @brief Notifies all registered listeners that configuration changed. */
    void NotifyConfigChange();
};
}  // namespace MOT

#endif /* CONFIG_MANAGER_H */
