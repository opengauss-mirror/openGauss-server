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
 * typed_statistics_generator.h
 *    Utility template class for implementing StatisticsGenerator.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/typed_statistics_generator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef TYPED_STATISTICS_GENERATOR_H
#define TYPED_STATISTICS_GENERATOR_H

#include "statistics_generator.h"

namespace MOT {
/**
 * @class TypedStatisticsGenerator
 * @brief Utility template class for implementing StatisticsGenerator.
 * @tparam T Thread statistics type (expected to derive from ThreadStatistics).
 * @tparam G Global statistics type (expected to derive from GlobalStatistics).
 */
template <typename T, typename G>
class TypedStatisticsGenerator : public StatisticsGenerator {
public:
    /** @brief COnstructor. */
    TypedStatisticsGenerator()
    {}

    /** @brief Destructor. */
    ~TypedStatisticsGenerator() override
    {}

    /**
     * @brief Retrieves the size of the created object (required for in-place
     * allocation).
     * @return The object size.
     */
    size_t GetObjectSize() const override
    {
        return sizeof(T);
    }

    /**
     * @brief Creates a ThreadStatistics object.
     * @param threadId The logical identifier of the thread to which the created
     * thread statistics object belongs.
     * @return The created ThreadStatistics object.
     */
    ThreadStatistics* CreateThreadStatistics(uint64_t threadId) const override
    {
        return new (std::nothrow) T(threadId);
    }

    /**
     * @brief Creates a ThreadStatistics object using in-place buffer.
     * @param threadId The logical identifier of the thread to which the created
     * thread statistics object belongs.
     * @param buffer In-place buffer for placement new operator.
     * @return The created ThreadStatistics object.
     */
    ThreadStatistics* CreateThreadStatistics(uint64_t threadId, void* buffer) const override
    {
        return new (buffer) T(threadId, buffer);
    }

    /**
     * @brief Creates a GlobalStatistics object for global component statistics.
     * @param namingScheme The naming scheme identifier. This identifier may be
     * used in the name formation of each statistic variable.
     * @return The created GlobalStatistics object.
     */
    GlobalStatistics* CreateGlobalStatistics(GlobalStatistics::NamingScheme namingScheme) const override
    {
        return new (std::nothrow) G(namingScheme);
    }
};
}  // namespace MOT
#endif /* TYPED_STATISTICS_GENERATOR_H */
