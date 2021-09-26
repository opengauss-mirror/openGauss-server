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
 * global_statistics.h
 *    A class used to manage global set of statistic variables for a single component.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/global_statistics.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GLOBAL_STATISTICS_H
#define GLOBAL_STATISTICS_H

#include "mot_string.h"
#include "mot_vector.h"
#include "statistic_variable.h"

namespace MOT {
/**
 * @class GlobalStatistics
 * @brief A class used to manage global set of statistic variables for a
 * single component.
 * @detail Derived classes should define their own static variable members, and
 * register them in the constructor by calling RegisterStatistics().
 * @see StatisticsProvider
 */
class GlobalStatistics {
public:
    /**
     * Constructor.
     */
    GlobalStatistics()
    {}

    /** Destructor. */
    virtual ~GlobalStatistics()
    {}

    enum class NamingScheme : uint32_t {
        /** @var Constant for [TOTAL] global statistics name. */
        NAMING_SCHEME_TOTAL = 1,

        /** @var Constant for [TOTAL] global statistics name, but designates the previous total. */
        NAMING_SCHEME_TOTAL_PREV,

        /** @var Constant for [DIFF] global statistics name. */
        NAMING_SCHEME_DIFF
    };

    /**
     * @brief Retrieves the amount of statistic variables contained by this object.
     * @return Number of static variables.
     */
    inline uint32_t GetStatCount() const
    {
        return m_statVars.size();
    }

    /**
     * @brief Summarizes all contained statistic variables for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp);

    /**
     * @brief Prints all contained statistic variables to log file using the given
     * log level.
     * @param log_levelThe log level to use.
     * @param[opt] ss Alternate output string stream (instead of envelope logger).
     */
    /**
     * @brief Prints a contained statistic variables to log file using the given log level.
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
    void Assign(const GlobalStatistics& rhs);

    /**
     * @brief Adds statistics of another object to this object. This method is
     * used to aggregate thread statistics from all thread of a single statistic
     * provider.
     * @param rhs The right-hand-side object to add from.
     */
    void Subtract(const GlobalStatistics& rhs);

    /** @brief Queries whether there are valid samples in this statistics container. */
    bool HasValidSamples() const;

    /**
     * @brief Utility method for generating global statistic variable names.
     * @param base_name The name of the statistic variable.
     * @param naming_scheme The naming scheme identifier. Special values are used
     * to produce special names.
     * @return The generated name.
     */
    static mot_string MakeName(const char* baseName, NamingScheme namingScheme);

protected:
    /**
     * Allows derived classes to register their statistic variables.
     * @param stat_var The statistic variable to register.
     * @note In case of failure the system continues to operate, but the
     * statistics variable will not be periodically reported to the log.
     */
    void RegisterStatistics(StatisticVariable* statVar);

private:
    /** @var The set of managed statistic variables. */
    mot_vector<StatisticVariable*> m_statVars;
};

/**
 * @class EmptyGlobalStatistics Stub for statistics providers that have no global statitiscs.
 */
class EmptyGlobalStatistics : public GlobalStatistics {
public:
    /**
     * @brief constructor.
     * @param naming_scheme Unused.
     */
    explicit EmptyGlobalStatistics(GlobalStatistics::NamingScheme /* naming_scheme */)
    {}

    /** @brief Destructor. */
    ~EmptyGlobalStatistics() final
    {}
};
}  // namespace MOT

#endif /* GLOBAL_STATISTICS_H */
