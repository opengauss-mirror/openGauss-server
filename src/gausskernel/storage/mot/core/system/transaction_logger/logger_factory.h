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
 * logger_factory.h
 *    The factory used to generate a logger based on configuration.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/transaction_logger/logger_factory.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOGGER_FACTORY_H
#define LOGGER_FACTORY_H

#include "ilogger.h"

namespace MOT {
/**
 * @class LoggerFactory
 * @brief The factory used to generate a logger based on configuration.
 */
class LoggerFactory {
private:
    LoggerFactory()
    {}

    ~LoggerFactory()
    {}

public:
    /**
     * @brief Creates logger based on configuration.
     * @return The created logger or NULL if failed.
     */
    static ILogger* CreateLogger();
};
}  // namespace MOT

#endif /* LOGGER_FACTORY_H */
