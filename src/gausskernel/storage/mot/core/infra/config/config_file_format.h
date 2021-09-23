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
 * config_file_format.h
 *    Configuration file format types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_file_format.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CONFIG_FILE_FORMAT_H
#define CONFIG_FILE_FORMAT_H

namespace MOT {
/**
 * @enum ConfigFileFormat
 * @brief The type of configuration file format.
 */
enum class ConfigFileFormat : uint32_t {
    /** @var Unspecified configuration file format. */
    CONFIG_FILE_FORMAT_NONE,

    /** @var Java-like properties configuration file format. */
    CONFIG_FILE_PROPS
};
}  // namespace MOT

#endif /* CONFIG_FILE_FORMAT_H */
