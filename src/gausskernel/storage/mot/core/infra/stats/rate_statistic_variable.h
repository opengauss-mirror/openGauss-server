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
 * rate_statistic_variable.h
 *    A statistic variable for rate types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/rate_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RATE_STATISTIC_VARIABLE_H
#define RATE_STATISTIC_VARIABLE_H

#include "statistic_variable.h"
#include "cycles.h"

namespace MOT {
/**
 * @class RateStatisticVariable
 * @brief A statistic variable for rate types. It computes volume over time. A @ref RateStatisticVariable
 * differs from a @ref FrequencyStatisticVariable since the latter observes the occurrence frequency
 * of an event over time, whereas the former observes the average value of an event in a time unit.
 */
class RateStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     * @param factor Unit factor. Used to scale down collected data.
     * @param units Unit name for display.
     */
    explicit RateStatisticVariable(const char* name = "", uint64_t factor = 1, const char* units = "")
        : StatisticVariable(name),
          m_sum(0),
          m_factor(factor),
          m_tstamp(0),
          m_initTstamp(0),
          m_intervalSeconds(0.0f),
          m_rate(0.0f),
          m_frequency(0.0f),
          m_countSaved(0),
          m_sumSaved(0)
    {
        errno_t erc = snprintf_s(m_units, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", units);
        securec_check_ss(erc, "\0", "\0");
    }

    /** @brief Destructor. */
    ~RateStatisticVariable() override
    {}

    /**
     * @brief Configures string representation of the variable.
     * @param name Name used for printing.
     * @param factor The scale-down factor to use in display.
     * @param units The unit name to use in display.
     */
    inline void Configure(const char* name, uint64_t factor, const char* units)
    {
        SetName(name);
        m_factor = factor;
        errno_t erc = snprintf_s(m_units, STAT_VAR_MAX_NAME_LEN, STAT_VAR_MAX_NAME_LEN - 1, "%s", units);
        securec_check_ss(erc, "\0", "\0");
    }

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp) override;

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param logLevel The log level to use.
     */
    void Print(LogLevel logLevel) const override;

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    void Assign(const StatisticVariable& rhs) override;

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    void Add(const StatisticVariable& rhs) override;

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    void Subtract(const StatisticVariable& rhs) override;

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    void Divide(uint32_t factor) override;

    /**
     * @brief Resets all statistic values to zero.
     */
    void Reset() override;

    /**
     * @brief Adds a sample to the statistics.
     * @param value The sample value
     */
    inline void AddSample(uint64_t value)
    {
        ++m_count;
        m_sum += value;
        if (m_initTstamp == 0) {
            m_initTstamp = CpuCyclesLevelTime::Rdtscp();
        }
    }

    /**
     * @brief Retrieves the rate during the last call to summarize.
     * @return The data rate.
     */
    inline double GetRate() const
    {
        return m_rate;
    }

    /**
     * @brief Computes the up-to-date event frequency in the last measured interval (i.e. consecutive
     * calls to summarize()).
     * @return The up-to-date rate.
     */
    inline double GetUpToDateRate() const
    {
        uint64_t tstamp = CpuCyclesLevelTime::Rdtscp();
        double rate = ((double)(m_sum)) / ((double)CpuCyclesLevelTime::CyclesToSeconds(tstamp - m_initTstamp));
        return rate / ((double)m_factor);  // scale down
    }

    /**
     * Retrieves the frequency during the last call to summarize.
     * @return The event frequency.
     */
    inline double GetFrequency() const
    {
        return m_frequency;
    }

    /**
     * @brief Computes the up-to-date event frequency in the last measured interval (i.e.
     * consecutive calls to summarize()).
     * @return The up-to-date frequency.
     */
    inline double GetUpToDateFrequency() const
    {
        uint64_t tstamp = CpuCyclesLevelTime::Rdtscp();
        return ((double)(m_count)) / ((double)CpuCyclesLevelTime::CyclesToSeconds(tstamp - m_initTstamp));
    }

private:
    /** @var The sum of all the samples. */
    uint64_t m_sum;

    /** @var The factor to scale down all samples. */
    uint64_t m_factor;

    /** @var Time stamp of last recorded event. */
    uint64_t m_tstamp;

    /** @var Initial time stamp starting from the first recorded event. */
    uint64_t m_initTstamp;

    /** @var The time interval in seconds. */
    double m_intervalSeconds;

    /** @var Last computed rate. */
    double m_rate;

    /** @var Last computed frequency. */
    double m_frequency;

    /** @var Name of units for printing. */
    char m_units[STAT_VAR_MAX_NAME_LEN];

    /** @var Last seen sample count. */
    uint64_t m_countSaved;

    /** @var Last computed sum. */
    uint64_t m_sumSaved;
};

/**
 * @brief Rate statistic variable for data volumes in mega-bytes.
 */
class DataRateStatisticVariable : public RateStatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name The statistic variable name.
     */
    explicit DataRateStatisticVariable(const char* name) : RateStatisticVariable(name, 1024 * 1024, "MB")
    {}

    /** @brief Destructor. */
    ~DataRateStatisticVariable() override
    {}
};
}  // namespace MOT
#endif /* RATE_STATISTIC_VARIABLE_H */
