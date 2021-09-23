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
 * jni_logger.h
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_logger.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JNI_LOGGER_H_
#define JNI_LOGGER_H_

#include <syslog.h>

/* ! \class Logger
    \brief singleton that encapsulate logger functionality for the JNI functions, currently wrapping syslog
*/
class JNILogger {
public:
    JNILogger(const JNILogger &) = delete;
    JNILogger(JNILogger &&) = delete;
    JNILogger &operator = (const JNILogger &) = delete;
    JNILogger &operator = (JNILogger &&) = delete;

    static void log(int priority, const char *filename, int lineno, const char *fmt, ...);
    static void set_log_level(int logLevel);

private:
    JNILogger(); /* singleton */
    ~JNILogger();
    void log_impl(int priority, const char *buffer) const;
    static JNILogger &get_instance();
    static int m_min_log_level;
};

#define JNI_LOG_ERROR(...) JNILogger::log(LOG_ERR, __FILE__, __LINE__, __VA_ARGS__)
#define JNI_LOG_WARN(...) JNILogger::log(LOG_WARNING, __FILE__, __LINE__, __VA_ARGS__)
#define JNI_LOG_INFO(...) JNILogger::log(LOG_INFO, __FILE__, __LINE__, __VA_ARGS__)
#define JNI_LOG_DEBUG(...) JNILogger::log(LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)

#endif /* JNI_LOGGER_H_ */
