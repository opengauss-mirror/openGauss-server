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
 * statistics_generator.h
 *    Interface for all typed statistics generators.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/statistics_generator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STATISTICS_GENERATOR_H
#define STATISTICS_GENERATOR_H

#include "global_statistics.h"
#include "thread_statistics.h"

namespace MOT {
/**
 * @class StatisticsGenerator
 * @brief Interface for all typed statistics generators (i.e. object factories).
 * @see StatisticsProvider.
 */
class StatisticsGenerator {
public:
    /** @brief Destructor. */
    virtual ~StatisticsGenerator()
    {}

    /**
     * @brief Retrieves the size of the created object (required for in-place
     * allocation).
     * @return The object size.
     */
    virtual size_t GetObjectSize() const = 0;

    /**
     * @brief Creates a ThreadStatistics object for per-thread component statistics.
     * @param threadId The logical identifier of the thread to which the created
     * thread statistics object belongs.
     * @return The created ThreadStatistics object.
     */
    virtual ThreadStatistics* CreateThreadStatistics(uint64_t threadId) const = 0;

    /**
     * @brief Creates a ThreadStatistics object using in-place buffer.
     * @param thread_id The logical identifier of the thread to which the created
     * thread statistics object belongs.
     * @param buffer In-place buffer for placement new operator.
     * @return The created ThreadStatistics object.
     */
    virtual ThreadStatistics* CreateThreadStatistics(uint64_t threadId, void* buffer) const = 0;

    /**
     * @brief Creates a GlobalStatistics object for global component statistics.
     * @param namingScheme The naming scheme identifier. This identifier may be
     * used in the name formation of each statistic variable.
     * @return The created GlobalStatistics object.
     */
    virtual GlobalStatistics* CreateGlobalStatistics(GlobalStatistics::NamingScheme namingScheme) const = 0;

protected:
    /** @brief Constructor. */
    StatisticsGenerator()
    {}
};
}  // namespace MOT
#endif /* STATISTICS_GENERATOR_H */
