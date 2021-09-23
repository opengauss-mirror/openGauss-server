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
 * frequency_statistic_variable.h
 *    A statistic variable for monitoring the frequency of an event.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/stats/frequency_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FREQUENCY_STATISTIC_VARIABLE_H
#define FREQUENCY_STATISTIC_VARIABLE_H

#include "cycles.h"
#include "statistic_variable.h"

namespace MOT {
/**
 * @class FrequencyStatisticVariable
 * @brief A statistic variable for monitoring the frequency of an event. It
 * computes how many times did the event occurred between two points in time.
 */
class FrequencyStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief Constructor.
     * @param name Name used for printing.
     */
    explicit FrequencyStatisticVariable(const char* name = "")
        : StatisticVariable(name),
          m_tstamp(0),
          m_initTstamp(0),
          m_intervalSeconds(0.0f),
          m_frequency(0.0f),
          m_countSaved(0)
    {}

    /** @brief Destructor. */
    ~FrequencyStatisticVariable() final
    {}

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    void Summarize(bool updateTstamp) final;

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param log_levelThe log level to use.
     */
    void Print(LogLevel logLevel) const final;

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    void Assign(const StatisticVariable& rhs) final;

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    void Add(const StatisticVariable& rhs) final;

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    void Subtract(const StatisticVariable& rhs) final;

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    void Divide(uint32_t factor) final;

    /**
     * @brief Resets all statistic values to zero.
     */
    void Reset() final;

    /** @brief Adds an event occurrence. */
    inline void AddSample()
    {
        ++m_count;
        if (m_initTstamp == 0)
            m_initTstamp = CpuCyclesLevelTime::Rdtscp();
    }

    /**
     * @brief Retrieves the frequency during the last call to summarize.
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
    /** @var Time stamp of last recorded event. */
    uint64_t m_tstamp;

    /** @var Initial time stamp starting from the first recorded event. */
    uint64_t m_initTstamp;

    /** @var The time interval in seconds. */
    double m_intervalSeconds;

    /** @var Last computed frequency. */
    double m_frequency;

    /** @var Last seen sample count. */
    uint64_t m_countSaved;
};
}  // namespace MOT

#endif /* FREQUENCYSTATISTICVARIABLE_H */
