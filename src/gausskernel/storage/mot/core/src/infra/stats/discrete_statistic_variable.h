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
 * discrete_statistic_variable.h
 *    A statistic variable used to collect statistics for a discrete value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/infra/stats/discrete_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DISCRETE_STATISTIC_VARIABLE_H
#define DISCRETE_STATISTIC_VARIABLE_H

#include "statistic_variable.h"
#include "mot_string.h"

#include <iomanip>

namespace MOT {
/**
 * @class DiscreteStatisticVariable<ItemCount>
 * @tparam ItemCount The number of items in the discrete value.
 * @brief A statistic variable used to collect statistics for a discrete value. In particular the
 * occurrence percentage of each value in the range [0, ItemCount) is computed.
 */
template <uint32_t ItemCount>
class DiscreteStatisticVariable : public StatisticVariable {
public:
    /**
     * @brief COnstructor.
     * @param name The statistic variable name.
     */
    DiscreteStatisticVariable(const char* name) : StatisticVariable(name)
    {
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_sum[i] = 0;
            m_percentage[i] = 0.0f;
            MOT_SNPRINTF(m_names[i], STAT_VAR_MAX_NAME_LEN, "%u", i);
        }
    }

    /** @brief Destructor. */
    virtual ~DiscreteStatisticVariable()
    {}

    /**
     * @brief Summarizes statistics for printing.
     * @param updateTstamp Specifies whether to update last time stamp (only for statistic
     * variables that provide time-based statistics, such as rate, frequency and level).
     */
    virtual void Summarize(bool updateTstamp)
    {
        (void)updateTstamp;
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_countSaved = m_count;
            m_percentage[i] = ((double)m_sum[i]) / ((double)m_countSaved);
        }
    }

    /**
     * @brief Prints statistics to log file using the given log level.
     * @param logLevel The log level to use.
     */
    virtual void Print(LogLevel logLevel) const
    {
        const unsigned int bufferSize = 256;
        mot_stirng str;
        char buf[bufferSize];
        MOT_SNPRINTF(buf, bufferSize, "%-20s ", LOGGER_FULL_NAME);
        str.append("%-20s %s={ samples: ", LOGGER_FULL_NAME, _name, m_countSaved);
        for (uint32_t i = 0; i < ItemCount; ++i) {
            if (m_sum[i] > 0) {
                str.append(", %s: %" PRIu64 " (%0.4f%%)", m_names[i], m_sum[i], m_percentage[i] * 100.0f);
            }
        }
        str.append(" }");
        MOTLog(logLevel, LOGGER_FULL_NAME, str.c_str());
    }

    /**
     * @brief Copies statistics from another object into this object.
     * @param rhs The right-hand-side object to copy from.
     */
    virtual void Assign(const StatisticVariable& rhs)
    {
        const DiscreteStatisticVariable<ItemCount>& disRhs = static_cast<const DiscreteStatisticVariable&>(rhs);
        m_count = disRhs.m_count;
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_sum[i] = disRhs.m_sum[i];
        }
    }

    /**
     * @brief Adds statistics of another object to this object.
     * @param rhs The right-hand-side object to add from.
     */
    virtual void Add(const StatisticVariable& rhs)
    {
        const DiscreteStatisticVariable<ItemCount>& disRhs = static_cast<const DiscreteStatisticVariable&>(rhs);
        m_count += disRhs.m_count;
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_sum[i] += disRhs.m_sum[i];
        }
    }

    /**
     * @brief Subtracts statistics of another object from this object.
     * @param rhs The right-hand-side subtrahend object.
     */
    virtual void Subtract(const StatisticVariable& rhs)
    {
        const DiscreteStatisticVariable<ItemCount>& disRhs = static_cast<const DiscreteStatisticVariable&>(rhs);
        m_count -= disRhs.m_count;
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_sum[i] -= disRhs.m_sum[i];
        }
    }

    /**
     * @brief Divides the statistics of this object by a given factor.
     * @param factor The division factor.
     */
    virtual void Divide(uint32_t factor)
    {
        if (factor > 0) {
            m_count /= factor;
            for (uint32_t i = 0; i < ItemCount; ++i) {
                m_sum[i] /= factor;
            }
        }
    }

    /**
     * @brief Resets all statistic values to zero.
     */
    virtual void Reset()
    {
        m_count = 0;
        for (uint32_t i = 0; i < ItemCount; ++i) {
            m_sum[i] = 0;
        }
    }

    /**
     * @brief Adds a sample.
     * @param item_id The item identifier. Must be in the range []0, ItemCount).
     */
    inline void AddSample(uint32_t item_id)
    {
        MOT_ASSERT(item_id < ItemCount);
        ++m_sum[item_id];
        ++m_count;
    }

    /**
     * @brief Retrieves the sample count for an item.
     * @param item_id The item identifier.
     * @return The sample count.
     */
    inline uint64_t getItemSampleCount(uint32_t item_id) const
    {
        return m_sum[item_id];
    }

    /**
     * @brief Retrieves the last seen occurrence percentage of an item.
     * @param item_id The item identifier.
     * @return The occurrence percentage.
     */
    inline double getPercentage(uint32_t item_id) const
    {
        return m_percentage[item_id];
    }

protected:
    /**
     * @brief Sets the name of an item.
     * @param item_id The item identifier.
     * @param name The item name.
     */
    inline void setName(uint32_t item_id, const char* name)
    {
        MOT_SNPRINTF(m_names[item_id], STAT_VAR_MAX_NAME_LEN, "%s", name);
    }

private:
    /** @var Number of times each discrete value was reported. */
    uint64_t m_sum[ItemCount];

    /** @var Last computed percentage of each discrete value. */
    double m_percentage[ItemCount];

    /** @var Last seen sample count. */
    uint64_t m_countSaved;

    /** @var Discrete value names. */
    char m_names[ItemCount][STAT_VAR_MAX_NAME_LEN];
};
}  // namespace MOT

#endif /* DISCRETE_STATISTIC_VARIABLE_H */
