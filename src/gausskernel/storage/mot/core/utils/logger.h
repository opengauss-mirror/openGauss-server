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
 * logger.h
 *    A logger descriptor record to control log level of a single logger.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/logger.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOGGER_H
#define LOGGER_H

#include "log_level.h"

namespace MOT {
/** @define Maximum size of logger name. */
#define MOT_LOGGER_MAX_NAME_LEN 64

/**
 * @struct Logger
 * @brief A logger descriptor record to control log level of a single logger.
 */
struct Logger {
    /** @brief Constructor. */
    /**
     * @brief Constructor.
     * @param loggerName The name of the logger.
     * @param componentName The name of the component to which the logger belongs.
     */
    Logger(const char* loggerName, const char* componentName);

    /** @brief Destructor. */
    ~Logger();

    /** @var The full name of the logger as it would appear in the log file. */
    char m_fullName[MOT_LOGGER_MAX_NAME_LEN];

    /** @var The name of the logger under its component. */
    char m_loggerName[MOT_LOGGER_MAX_NAME_LEN];

    /** @var The name of the component to which the logger belongs. */
    char m_componentName[MOT_LOGGER_MAX_NAME_LEN];

    /** @var The qualified name of the logger. */
    char m_qualifiedName[MOT_LOGGER_MAX_NAME_LEN];

    /** @var The log level of this logger. */
    LogLevel m_logLevel;
};

/** @define DECLARE_LOGGER Use this macro to declare a logger at a source file. */
#define DECLARE_LOGGER(LoggerName, ComponentName) static MOT::Logger _logger(#LoggerName, #ComponentName);

/** @define DECLARE_CLASS_LOGGER Use this macro to declare a logger at a class level in its header file. */
#define DECLARE_CLASS_LOGGER() static MOT::Logger _logger;

/** @define IMPLEMENT_CLASS_LOGGER Use this macro to define the class logger at its source file. */
#define IMPLEMENT_CLASS_LOGGER(Class, Component) MOT::Logger Class::_logger(#Class, #Component);

/** @define IMPLEMENT_TEMPLATE_LOGGER Use this macro to define a class logger declared in a template class. */
#define IMPLEMENT_TEMPLATE_LOGGER(Class, Component) \
    template <>                                     \
    IMPLEMENT_CLASS_LOGGER(Class, Component)

/** @define LOGGER_FULL_NAME The full name of the current logger as it would appear in the log file. */
#define MOT_LOGGER_FULL_NAME _logger.m_fullName

/** @define LOGGER_LEVEL The log level of the current logger. */
#define MOT_LOGGER_LEVEL _logger.m_logLevel

/** @define LOGGER_NAME The name of the current logger. */
#define MOT_LOGGER_NAME _logger.m_loggerName

/** @define LOGGER_COMPONENT_NAME The name of the component to which the current logger belongs. */
#define MOT_LOGGER_COMPONENT_NAME _logger.m_componentName

/**
 * @brief Retrieves the log level of all loggers belonging to a specific component.
 * @param componentName The name of the component.
 * @return The log level of the component.
 */
extern LogLevel GetLogComponentLogLevel(const char* componentName);

/**
 * @brief Retrieves the log level of a specific logger.
 * @param componentName The name of the component to which the logger belongs.
 * @param loggerName The name of the logger to configure its log level.
 * @return The log level of the logger.
 */
extern LogLevel GetLoggerLogLevel(const char* componentName, const char* loggerName);

/**
 * @brief Sets the log level of all loggers belonging to a specific component. To be called when configuration changes.
 * @param componentName The name of the component.
 * @param logLevel The new log level.
 * @return The previous log level of the component.
 */
extern LogLevel SetLogComponentLogLevel(const char* componentName, LogLevel logLevel);

/**
 * @brief Sets the log level of a specific logger. To be called when configuration changes.
 * @param componenetName The name of the component to which the logger belongs.
 * @param loggerName The name of the logger to configure its log level.
 * @param logLevel The new log level.
 * @return The previous log level of the logger.
 */
extern LogLevel SetLoggerLogLevel(const char* componenetName, const char* loggerName, LogLevel logLevel);
}  // namespace MOT

#endif /* LOGGER_H */
