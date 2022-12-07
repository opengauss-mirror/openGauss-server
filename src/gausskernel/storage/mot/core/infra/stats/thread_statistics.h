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
 * thread_statistics.h
 *    A class used to manage per-thread set of statistic variables for a single component.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/thread_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef THREAD_STATISTICS_H
#define THREAD_STATISTICS_H

#include "mot_string.h"
#include "mot_vector.h"
#include "statistic_variable.h"
#include "session_context.h"

namespace MOT {
/**
 * @class ThreadStatistics
 * @brief A class used to manage per-thread set of statistic variables for a
 * single component. Each instance manages a set of statistics for a single thread.
 * @detail Derived classes should define their own static variable members, and
 * register them in the constructor by calling RegisterStatistics().
 * This class is also used for computing aggregates statistics for all threads
 * of a single statistics provider.
 * @see StatisticsProvider
 */
class ThreadStatistics {
public:
    /**
     * @brief Constructor.
     * @param threadId The logical identifier of the thread that these
     * statistics describe. This identifier may be used in the name formation of
     * each statistic variable.
     * @see MakeName().
     */
    explicit ThreadStatistics(uint64_t threadId, void* inplaceBuffer = nullptr)
        : m_threadId(threadId), m_node(MOTCurrentNumaNodeId), m_inplaceBuffer(inplaceBuffer)
    {}

    /** @brief Destructor. */
    virtual ~ThreadStatistics()
    {
        // m_inplaceBuffer is not owned by this class
        m_inplaceBuffer = nullptr;
    }

    /**
     * @brief Retrieves the amount of statistic variables contained by this object.
     * @return Number of static variables.
     */
    inline uint32_t GetStatCount() const
    {
        return m_statVars.size();
    }

    /**
     * @brief Retrieves the logical identifier of the thread that these statistics describe.
     * @return The logical thread identifier.
     */
    inline uint64_t GetThreadId() const
    {
        return m_threadId;
    }

    /**
     * @brief Retrieves the identifier of the NUMA node from which this object's memory is allocated.
     * @return The NUMA node identifier.
     */
    inline int GetNodeId() const
    {
        return m_node;
    }

    /**
     * @brief Retrieves the in-place buffer used to allocate this object.
     * @return The in-place buffer.
     */
    inline void* GetInPlaceBuffer() const
    {
        return m_inplaceBuffer;
    }

    /**
     * @brief Summarizes all contained statistic variables for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp);

    /**
     * @brief Prints a contained statistic variable to log file using the given log level.
     * @param statId The statistic variable identifier.
     * @param log_levelThe log level to use.
     */
    void Print(uint32_t statId, LogLevel logLevel) const;

    /**
     * @brief Resets all contained statistic variables values to zero.
     */
    void Reset();

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    void Assign(const ThreadStatistics& rhs);

    /**
     * @brief Adds statistics of another object to this object. This method is
     * used to aggregate thread statistics from all thread of a single statistic
     * provider.
     * @param rhs The right-hand-side object to add from.
     */
    void Add(const ThreadStatistics& rhs);

    /**
     * @brief Adds statistics of another object to this object. This method is
     * used to aggregate thread statistics from all thread of a single statistic
     * provider.
     * @param rhs The right-hand-side object to add from.
     */
    void Subtract(const ThreadStatistics& rhs);

    /**
     * @brief Normalizes all statistic variables after aggregation.
     * @see Add().
     */
    void Normalize();

    /** @brief Queries whether there are valid samples in this statistics container. */
    bool HasValidSamples() const;

    /**
     * @brief Utility method for generating per-thread statistic variable names.
     * @param baseName The name of the statistic variable.
     * @param threadId The logical identifier of the thread to which the
     * statistic variable belongs. Special values are used to produce special
     * names (-1 for TOTAL, -2 for AVG, -3 for DIFF, -4 for DIFF-AVG).
     * @return The generated name.
     */
    static mot_string MakeName(const char* baseName, uint64_t threadId);

    /** @var Constant for [TOTAL] thread statistics name. */
    static constexpr uint64_t THREAD_ID_TOTAL = (uint64_t)-1;

    /** @var Constant for [AVG] thread statistics name. */
    static constexpr uint64_t THREAD_ID_AVG = (uint64_t)-2;

    /** @var Constant for [DIFF] thread statistics name. */
    static constexpr uint64_t THREAD_ID_DIFF = (uint64_t)-3;

    /** @var Constant for [DIFF-AVG] thread statistics name. */
    static constexpr uint64_t THREAD_ID_DIFF_AVG = (uint64_t)-4;

protected:
    /**
     * @brief Allows derived classes to register their statistic variables.
     * @param statVar The statistic variable to register.
     * @note In case of failure the system continues to operate, but the
     * statistics variable will not be periodically reported to the log.
     */
    void RegisterStatistics(StatisticVariable* statVar);

private:
    /** @var The logical identifier of the thread to which this set of statistic variables belong. */
    uint64_t m_threadId;

    /** @var The identifier of the NUMA node from which this object's memory is allocated. */
    int m_node;

    /** @var The set of managed statistic variables. */
    mot_vector<StatisticVariable*> m_statVars;

    /**
     * @var Counts the number of threads contributing samples for each managed statistic variables. Used during to
     * normalize results after aggregation.
     */
    mot_vector<uint32_t> m_validThreads;

    /** @var Helper member to simplify life-cycle of a pooled-object. */
    void* m_inplaceBuffer;
};
}  // namespace MOT
#endif /* THREAD_STATISTICS_H */
