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
 * cmdline_config_loader.h
 *    Configuration loader that loads from command line arguments.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/cmdline_config_loader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMDLINE_CONFIG_LOADER_H
#define CMDLINE_CONFIG_LOADER_H

#include "config_loader.h"

namespace MOT {
/**
 * @class CmdLineConfigLoader
 * @brief Configuration loader that loads from command line arguments. This loader has the highest
 * priority of all loaders.
 */
class CmdLineConfigLoader : public ConfigLoader {
public:
    /**
     * @brief Constructor.
     * @param argv Command-line argument string array.
     * @param argc Number of command-line arguments.
     */
    CmdLineConfigLoader(char** argv, int argc)
        : ConfigLoader("CmdLine", CMDLINE_CONFIG_PRIORITY), m_cmdlineConfigTree(ParseCmdLine(argv, argc))
    {}

    /** @brief Destructor. */
    ~CmdLineConfigLoader() override
    {}

protected:
    /**
     * @brief Implements configuration loading.
     * @return The resulting configuration tree or a null pointer on failure.
     */
    ConfigTree* LoadConfig() override
    {
        return m_cmdlineConfigTree;
    }

private:
    /** @var Cached configuration, loaded only once and never modified. */
    ConfigTree* m_cmdlineConfigTree;

    /**
     * @brief Parses the command line arguments.
     * @param argv Command-line argument string array.
     * @param argc Number of command-line arguments.
     */
    ConfigTree* ParseCmdLine(char** argv, int argc);
};
}  // namespace MOT

#endif /* CMDLINE_CONFIG_LOADER_H */
