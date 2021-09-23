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
 * config_loader.h
 *    The base interface for all configuration loaders.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_LOADER_H
#define CONFIG_LOADER_H

#include "config_tree.h"

// predefined priorities

/** @var Priority of the external configuration loaded from the envelope (always highest). */
#define EXT_CONFIG_PRIORITY 0

/** @define Priority of configuration loaded from command line. */
#define CMDLINE_CONFIG_PRIORITY 1

/** @var Priority of configuration loaded from environment variables. */
#define ENV_VAR_CONFIG_PRIORITY 2

/** @var Priority of engine configuration loaded from file. */
#define CFG_FILE_CONFIG_PRIORITY 3

/** @define Upper bound on configuration loader name (including terminating null). */
#define MAX_CONFIG_LOADER_NAME 256

namespace MOT {
/**
 * @class IConfigLoader
 * @brief The base interface for all configuration loaders.
 */
class ConfigLoader {
protected:
    /**
     * @brief Constructor.
     * @param name The name of the configuration loader. Must be unique among all loaders.
     * @param priority The priority of the configuration loader. Lower numbers mean higher priority.
     * This is for managing layered configuration and it determines precedence while merging
     * configuration trees.
     */
    ConfigLoader(const char* name, uint32_t priority);

public:
    /** Destructor. */
    virtual ~ConfigLoader()
    {
        if (m_configTree != NULL) {
            delete m_configTree;
            m_configTree = NULL;
        }
    }

    /**
     * @brief Retrieves the unique name by which this loader is identified.
     * @return The unique name of the configuration loader.
     */
    inline const char* GetName() const
    {
        return m_name;
    }

    /**
     * @brief Retrieves the priority of the loader.
     */
    inline uint32_t GetPriority() const
    {
        return m_priority;
    }

    /**
     * @brief Retrieves the configuration source name associated with the loaded configuration tree.
     * @return The configuration source name.
     */
    inline const char* GetConfigTreeSource() const
    {
        return m_configTree->GetSource();
    }

    /**
     * @brief Queries whether the configuration changed and therefore needs to be reloaded.
     * @return True if configuration changed.
     * @note This call has a side effect such that calling this function twice surely returns false.
     * For instance, for a configuration file loader, the last modification time is updated, and
     * therefore a subsequent call would return false.
     */
    virtual bool HasChanged()
    {
        return false;
    }

    /**
     * @brief Loads configuration from some configuration source.
     * @return The resulting configuration tree or null pointer if failed.
     */
    inline ConfigTree* Load()
    {
        m_configTree = LoadConfig();
        return m_configTree;
    }

    /**
     * @brief Reloads configuration and fires change notifications to all registered listeners.
     */
    inline void Reload()
    {
        ConfigTree* tmp = m_configTree;
        (void)Load();
        if (tmp != NULL) {
            delete tmp;
        }
    }

    /**
     * @brief Retrieves the recently loaded configuration.
     * @return The configuration tree.
     */
    inline ConfigTree* GetConfig()
    {
        return m_configTree;
    }

protected:
    /**
     * @brief Implement configuration loading.
     * @return The resulting configuration tree or null pointer if failed.
     */
    virtual ConfigTree* LoadConfig() = 0;

private:
    /** @var Unique loader name. */
    char m_name[MAX_CONFIG_LOADER_NAME];

    /** @var The priority of the resulting configuration tree. */
    uint32_t m_priority;

    /** @var Configuration tree. */
    ConfigTree* m_configTree;
};
}  // namespace MOT

#endif /* CONFIG_LOADER_H */
