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
 * logger.cpp
 *    A logger descriptor record to control log level of a single logger.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/logger.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "logger.h"
#include "mot_string.h"
#include "mot_list.h"
#include "mot_map.h"

#include <algorithm>

namespace MOT {
DECLARE_LOGGER(Logger, Utilities)

/** @typedef List of  loggers. */
typedef mot_list<Logger*> LoggerList;

/** @typedef Map of logger lists belonging to the same component.   */
typedef mot_map<mot_string, mot_pair<LogLevel, LoggerList> > LoggerMap;

// use pointer to avoid static initialization race
static LoggerMap* loggerMap = nullptr;

static void RegisterLogger(Logger* logger);
static void UnregisterLogger(Logger* logger);

Logger::Logger(const char* loggeName, const char* componentName) : m_logLevel(LogLevel::LL_INFO)
{
    errno_t erc = snprintf_s(m_fullName, MOT_LOGGER_MAX_NAME_LEN, MOT_LOGGER_MAX_NAME_LEN - 1, "<%s>", componentName);
    securec_check_ss(erc, "\0", "\0");
    erc = snprintf_s(m_loggerName, MOT_LOGGER_MAX_NAME_LEN, MOT_LOGGER_MAX_NAME_LEN - 1, "%s", loggeName);
    securec_check_ss(erc, "\0", "\0");
    erc = snprintf_s(m_componentName, MOT_LOGGER_MAX_NAME_LEN, MOT_LOGGER_MAX_NAME_LEN - 1, "%s", componentName);
    securec_check_ss(erc, "\0", "\0");
    erc = snprintf_s(
        m_qualifiedName, MOT_LOGGER_MAX_NAME_LEN, MOT_LOGGER_MAX_NAME_LEN - 1, "%s.%s", componentName, loggeName);
    securec_check_ss(erc, "\0", "\0");
    RegisterLogger(this);
}

Logger::~Logger()
{
    UnregisterLogger(this);
}

/**
 * @brief Registers a logger for dynamic log level setting from configuration.
 * @param logger The logger to register.
 */
static void RegisterLogger(Logger* logger)
{
    MOT_LOG_DEBUG("Registering logger %s in component %s", logger->m_loggerName, logger->m_componentName);
    if (loggerMap == nullptr) {
        loggerMap = new (std::nothrow) LoggerMap();
        if (loggerMap == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "System Startup", "Failed to allocate logger map");
            return;
        }
        MOT_LOG_DEBUG("Starting logger registration");
    }
    LoggerMap::iterator itr = loggerMap->find(logger->m_componentName);
    if (itr == loggerMap->end()) {
        LoggerMap::pairis pairis = loggerMap->insert(
            LoggerMap::value_type(logger->m_componentName, mot_make_pair(LogLevel::LL_INFO, LoggerList())));
        switch (pairis.second) {
            case INSERT_SUCCESS:
                break;
            case INSERT_FAILED:
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "System Startup",
                    "Failed to insert component %s for logger %s to loggerMap",
                    logger->m_componentName,
                    logger->m_loggerName);
                return;
            case INSERT_EXISTS:
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "System Startup",
                    "Failed to insert component %s for logger %s to loggerMap. Element already exists",
                    logger->m_componentName,
                    logger->m_loggerName);
                return;
            default:
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "System Startup",
                    "Unknown error code %d while inserting component %s logger %s to loggerMap",
                    pairis.second,
                    logger->m_componentName,
                    logger->m_loggerName);
                return;
        }

        itr = pairis.first;
        MOT_LOG_DEBUG("Registered log component: %s", logger->m_componentName);
    }
    LoggerList& loggerList = itr->second.second;
    LoggerList::iterator itr2 = find(loggerList.begin(), loggerList.end(), logger);
    MOT_ASSERT(itr2 == loggerList.end());
    if (!loggerList.push_back(logger)) {
        MOT_LOG_ERROR("Failed to add log %s to logger's list", logger->m_loggerName);
    }
}

/**
 * @brief Unregisters a logger for dynamic log level setting from configuration.
 * @param logger The logger to unregister.
 */
static void UnregisterLogger(Logger* logger)
{
    // we silently ignore the request if initialization failed
    if (loggerMap == nullptr) {
        return;
    }
    LoggerMap::iterator itr = loggerMap->find(logger->m_componentName);
    MOT_ASSERT(itr != loggerMap->end());
    LoggerList& loggerList = itr->second.second;
    LoggerList::iterator itr2 = find(loggerList.begin(), loggerList.end(), logger);
    MOT_ASSERT(itr2 != loggerList.end());
    (void)loggerList.erase(itr2);
    if (loggerList.empty()) {
        MOT_LOG_DEBUG("Unregistered log component: %s", logger->m_componentName);
        (void)loggerMap->erase(itr);
        if (loggerMap->empty()) {
            delete loggerMap;
            loggerMap = nullptr;
            MOT_LOG_DEBUG("Finished log component cleanup");
        }
    }
}

extern LogLevel GetLogComponentLogLevel(const char* componentName)
{
    LogLevel result = LogLevel::LL_INFO;
    MOT_ASSERT(loggerMap != nullptr);
    LoggerMap::iterator itr = loggerMap->find(componentName);
    if (itr == loggerMap->end()) {
        MOT_LOG_WARN("Cannot retrieve log level of log component %s: component not found.", componentName);
    } else {
        result = itr->second.first;
    }
    return result;
}

extern LogLevel SetLogComponentLogLevel(const char* componentName, LogLevel logLevel)
{
    if (loggerMap == nullptr) {
        return LogLevel::LL_INVALID;
    }
    LogLevel prevLogLevel = LogLevel::LL_INVALID;
    LoggerMap::iterator itr = loggerMap->find(componentName);
    if (itr == loggerMap->end()) {
        MOT_LOG_WARN("Invalid log component %s: name not found.", componentName);
    } else {
        prevLogLevel = itr->second.first;
        itr->second.first = logLevel;  // remember the log level for getLogComponentLogLevel()
        LoggerList& loggerList = itr->second.second;

        // induce the component log level on all sub-loggers (right afterwards the log level of specific
        // loggers will be set and override component settings (see NgConfiguration::updateComponentLogLevel())
        LoggerList::iterator itr2 = loggerList.begin();
        while (itr2 != loggerList.end()) {
            Logger* logger = *itr2;
            logger->m_logLevel = logLevel;
            MOT_LOG_DEBUG("Setting the log level of logger %s in component %s to %s by component configuration",
                logger->m_loggerName,
                componentName,
                LogLevelToString(logLevel));
            ++itr2;
        }
    }
    return prevLogLevel;
}

struct LoggerNameFinder {
    const char* m_loggerName;

    explicit LoggerNameFinder(const char* loggerName) : m_loggerName(loggerName)
    {}

    inline bool operator()(Logger*& logger) const
    {
        return (strcmp(logger->m_loggerName, m_loggerName) == 0);
    }
};

extern LogLevel GetLoggerLogLevel(const char* componentName, const char* loggerName)
{
    if (loggerMap == nullptr) {
        return LogLevel::LL_INVALID;
    }
    LogLevel result = LogLevel::LL_INFO;
    LoggerMap::iterator itr = loggerMap->find(componentName);
    if (itr == loggerMap->end()) {
        MOT_LOG_WARN("Cannot retrieve log level of log component %s: component not found.", componentName);
    } else {
        LoggerList& loggerList = itr->second.second;
        LoggerList::iterator itr2 = find_if(loggerList.begin(), loggerList.end(), LoggerNameFinder(loggerName));
        if (itr2 == loggerList.end()) {
            MOT_LOG_WARN(
                "Cannot retrieve log level of logger %s in component %s: logger not found.", loggerName, componentName);
        } else {
            Logger* logger = *itr2;
            result = logger->m_logLevel;
            ++itr2;
        }
    }
    return result;
}

extern LogLevel SetLoggerLogLevel(const char* componentName, const char* loggerName, LogLevel logLevel)
{
    if (loggerMap == nullptr) {
        return LogLevel::LL_INVALID;
    }
    LogLevel prevLogLevel = LogLevel::LL_INFO;
    MOT_LOG_DEBUG("Setting the log level of logger %s in component %s to %s",
        loggerName,
        componentName,
        LogLevelToString(logLevel));
    LoggerMap::iterator itr = loggerMap->find(componentName);
    if (itr == loggerMap->end()) {
        MOT_LOG_WARN("Cannot set log level of log component %s: log component not found.", componentName);
    } else {
        LoggerList& loggerList = itr->second.second;
        LoggerList::iterator itr2 = find_if(loggerList.begin(), loggerList.end(), LoggerNameFinder(loggerName));
        if (itr2 == loggerList.end()) {
            MOT_LOG_WARN(
                "Cannot retrieve log level of logger %s in component %s: logger not found.", loggerName, componentName);
        } else {
            Logger* logger = *itr2;
            prevLogLevel = logger->m_logLevel;
            logger->m_logLevel = logLevel;
            ++itr2;
        }
    }
    return prevLogLevel;
}
}  // namespace MOT
