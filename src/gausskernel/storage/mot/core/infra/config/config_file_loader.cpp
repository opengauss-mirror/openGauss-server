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
 * config_file_loader.cpp
 *    Interface for all file-based configuration loaders.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/config_file_loader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "config_file_loader.h"

#include <sys/stat.h>

namespace MOT {
DECLARE_LOGGER(ConfigFileLoader, Configuration)

static void FormatTime(uint64_t timeVal, char* buf, uint32_t len)
{
    timespec ts = {(long int)(timeVal / 1000000000ULL), (long int)(timeVal % 1000000000ULL)};
    struct tm t;

    if (localtime_r(&(ts.tv_sec), &t) != NULL) {
        size_t ret = strftime(buf, len, "%F %T", &t);
        if (ret != 0) {
            len -= ret;
            errno_t erc = snprintf_s(buf + ret, len, len - 1, ".%09ld", ts.tv_nsec);
            securec_check_ss(erc, "\0", "\0");
        }
    }
}

ConfigFileLoader::ConfigFileLoader(
    const char* typeName, const char* name, uint32_t priority, const char* configFilePath)
    : ConfigLoader(ComposeFullName(typeName, name, configFilePath).c_str(), priority),
      m_configFilePath(configFilePath),
      m_lastModTime(GetFileModificationTime())
{}

bool ConfigFileLoader::HasChanged()
{
    bool result = false;
    uint64_t modTime = GetFileModificationTime();
    if (modTime > m_lastModTime) {
        char lastDate[32];
        char newDate[32];
        FormatTime(m_lastModTime, lastDate, sizeof(lastDate));
        FormatTime(modTime, newDate, sizeof(newDate));
        MOT_LOG_INFO("Detected change in configuration file: %s (modification time changed: %" PRIu64 " --> %" PRIu64
                     " [%s --> %s])",
            m_configFilePath.c_str(),
            m_lastModTime,
            modTime,
            lastDate,
            newDate);
        m_lastModTime = modTime;
        result = true;
    }
    return result;
}

mot_string ConfigFileLoader::ComposeFullName(const char* typeName, const char* name, const char* configFilePath)
{
    mot_string result;
    if (!result.format("%s[%s]@%s", typeName, name, configFilePath)) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Load Configuration",
            "Failed to format configuration file loader name: %s",
            configFilePath);
        // this is not a critical error, we continue with ill-formatted configuration loader name
    }
    return result;
}

uint64_t ConfigFileLoader::GetFileModificationTime() const
{
    uint64_t filetime = 0;
    struct stat buf;
    if (stat(m_configFilePath.c_str(), &buf) == 0) {
        filetime = buf.st_mtim.tv_sec * 1000000000ULL + buf.st_mtim.tv_nsec;
    }
    return filetime;
}
}  // namespace MOT
