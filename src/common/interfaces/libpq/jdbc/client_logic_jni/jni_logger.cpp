/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * jni_logger.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_logger.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jni_logger.h"
#include <syslog.h>
#include <stdarg.h>
#include "securec.h"

static const int DEAFULT_MIN_LOG_LEVEL = LOG_DEBUG;
static const size_t LOG_BUFFER_LEN = 8192;
static const char *ERROR_FMT = "ERROR: %.60s:%04d ";
static const char *WARN_FMT = "WARNING: %.60s:%04d ";
static const char *INFO_FMT = "INFO: %.60s:%04d ";
static const char *DEBUG_FMT = "DEBUG: %.60s:%04d ";

int JNILogger::m_min_log_level = DEAFULT_MIN_LOG_LEVEL;

JNILogger::JNILogger() {}

JNILogger::~JNILogger() {}
/*
 * @return the the singleton instance
 */
JNILogger &JNILogger::get_instance()
{
    static JNILogger instance;
    return instance;
}

/*
 * Implementation of the log function using syslog
 * @param[in] priority log priority
 * @param[in] buffer log message
 */
void JNILogger::log_impl(int priority, const char *buffer) const
{
    syslog(priority, "%s", buffer ? buffer : "empty log");
}

/*
 * sets the global log level
 * @param[in] log_level
 */
void JNILogger::set_log_level(int log_level)
{
    if (log_level == LOG_ERR || log_level == LOG_WARNING || log_level == LOG_DEBUG) {
        JNILogger::m_min_log_level = log_level;
    }
}

/*
 * outer log function
 * @param[in] priority log message priority
 * @param[in] filename file name
 * @param[in] lineno line number
 * @param[in] fmt the log message formats, the "..." parameters following it are the message parameters
 */
void JNILogger::log(int priority, const char *filename, int lineno, const char *fmt, ...)
{
    if (filename == NULL || fmt == NULL) {
        return;
    }
    if (priority > JNILogger::m_min_log_level) {
        return;
    }
    int level = LOG_ERR;
    const char *line_format;
    switch (priority) {
        case LOG_ERR:
            line_format = ERROR_FMT;
            break;
        case LOG_WARNING:
            line_format = WARN_FMT;
            break;
        case LOG_INFO:
            line_format = INFO_FMT;
            break;
        case LOG_DEBUG:
        default:
            line_format = DEBUG_FMT;
            break;
    }

    char buff[LOG_BUFFER_LEN] = {0};
    int len_used = snprintf_s(buff, LOG_BUFFER_LEN, LOG_BUFFER_LEN, line_format, filename, lineno);
    if (len_used < 0) {
        return;
    } else {
        va_list va;
        va_start(va, fmt);
        int len_written = vsnprintf_s(buff + len_used, sizeof(buff) - len_used, sizeof(buff) - len_used, fmt, va);
        va_end(va);
        if (len_written > 0) {
            const JNILogger &logger = get_instance();
            logger.log_impl(level, buff);
        } else {
            return;
        }
    }
}
