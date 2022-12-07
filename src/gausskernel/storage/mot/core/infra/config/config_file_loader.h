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
 * config_file_loader.h
 *    Interface for all file-based configuration loaders.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_file_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_FILE_LOADER_H
#define CONFIG_FILE_LOADER_H

#include "config_loader.h"
#include "mot_string.h"

namespace MOT {
/**
 * @class IConfigFileLoader
 * @brief Interface for all file-based configuration loaders.
 */
class ConfigFileLoader : public ConfigLoader {
protected:
    /**
     * @brief Constructor.
     * @param typeName The type name of the class. Used for loader name formatting.
     * @param name The name of the configuration loader. Must be unique among all loaders.
     * @param priority The priority of the configuration loader. Lower numbers mean higher priority.
     * This is for managing layered configuration and it determines precedence while merging
     * configuration trees.
     * @param configFilePath The path to the configuration file.
     */
    ConfigFileLoader(const char* typeName, const char* name, uint32_t priority, const char* configFilePath);

public:
    /** Destructor. */
    ~ConfigFileLoader() override
    {}

    /**
     * @brief Retrieves the configuration file path.
     * @return The configuration file path.
     */
    inline const char* GetConfigFilePath() const
    {
        return m_configFilePath.c_str();
    }

    /**
     * @brief Queries whether the configuration changed and therefore needs to be reloaded.
     * @return True if configuration changed.
     */
    bool HasChanged() override;

protected:
    /**
     * @brief Loads a configuration from some configuration source.
     * @return The resulting configuration tree or null pointer if failed.
     */
    ConfigTree* LoadConfig() override
    {
        return LoadConfigFile(m_configFilePath.c_str());
    }
    /**
     * @brief Loads a configuration from a configuration file.
     * @param configFilePath The path to the configuration file.
     * @return The resulting configuration tree or null pointer if failed.
     */
    virtual ConfigTree* LoadConfigFile(const char* configFilePath) = 0;

private:
    /** @var The path to the configuration file. */
    mot_string m_configFilePath;

    /** @var Last seen modification time of the configuration file. */
    uint64_t m_lastModTime;

    /**
     * @brief Helper method for composing loader name.
     * @param typeName The type name of the loader class.
     * @param name The name of the loader.
     * @param configFilePath The configuration file path.
     * @return The full name of the loader.
     */
    static mot_string ComposeFullName(const char* typeName, const char* name, const char* configFilePath);

    /**
     * @brief Retrieves the last modification time of the configuration file.
     * @return The last modification time of the configuration file or zero if failed.
     */
    uint64_t GetFileModificationTime() const;
};
}  // namespace MOT

#endif /* CONFIG_FILE_LOADER_H */
