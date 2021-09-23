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
 * props_config_file_loader.h
 *    Configuration loader that loads from a properties file (Java style).
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/props_config_file_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PROPS_CONFIG_FILE_LOADER_H
#define PROPS_CONFIG_FILE_LOADER_H

#include "config_file_loader.h"

namespace MOT {
/**
 * @class PropsConfigFileLoader
 * @brief Class for loading configuration from a properties file (Java style).
 */
class PropsConfigFileLoader : public ConfigFileLoader {
public:
    /**
     * Constructor.
     * @param name The name of the configuration loader. Must be unique among all loaders.
     * @param priority The priority of the configuration loader. Lower numbers mean higher priority.
     * This is for managing layered configuration and it determines precedence while merging
     * configuration trees.
     * @param configFilePath The configuration file path.
     */
    PropsConfigFileLoader(const char* name, uint32_t priority, const char* configFilePath)
        : ConfigFileLoader("PropsConfigFileLoader", name, priority, configFilePath)
    {}

    /** Destructor. */
    ~PropsConfigFileLoader() final
    {}

protected:
    /**
     * @brief Loads configuration from a properties configuration file.
     * @param configFilePath The path to the configuration file.
     * @return The resulting configuration tree or null pointer if failed.
     */
    ConfigTree* LoadConfigFile(const char* configFilePath) final;
};
}  // namespace MOT

#endif /* PROPS_CONFIG_FILE_LOADER_H */
