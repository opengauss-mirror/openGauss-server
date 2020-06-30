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
 * enumerated_statistic_variable.h
 *    A statistic variable used to collect statistics for an enumerated value.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/infra/stats/enumerated_statistic_variable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ENUMERATED_STATISTIC_VARIABLE_H
#define ENUMERATED_STATISTIC_VARIABLE_H

#include "discrete_statistic_variable.h"
#include "type_formatter.h"

namespace MOT {
/**
 * @class EnumeratedStatisticVariable<EnumType, ItemCount>
 * @tparam EnumType The enumeration type. The enumeration must contain a special member ITEM_COUNT,
 * which specifies the number of items (in the semi-closed range [0, ItemCount)) used by the
 * enumeration.
 * @tparam ItemCount The number of items in the enumeration type.
 * @brief A statistic variable used to collect statistics for an enumerated value. In particular the
 * occurrence percentage of each enumeration value in the type EnumType is computed.
 */
template <typename EnumType, uint32_t ItemCount = (uint32_t)EnumType::ITEM_COUNT>
class EnumeratedStatisticVariable : public DiscreteStatisticVariable<ItemCount> {
public:
    /** @typedef The base class. */
    typedef DiscreteStatisticVariable<ItemCount> BaseType;

    /**
     * @brief Constructor.
     * @param name The statistic variable name.
     */
    EnumeratedStatisticVariable(const char* name) : BaseType(name)
    {
        for (uint32_t i = 0; i < ItemCount; ++i) {
            BaseType::setName(i, TypeFormatter<EnumType>::ToString((EnumType)i));
        }
    }

    /** @brief Destructor. */
    virtual ~EnumeratedStatisticVariable()
    {}

    /**
     * @brief Adds a sample.
     * @param enum_id The enumeration value for which a sample is added.
     */
    inline void AddSample(EnumType enum_id)
    {
        BaseType::AddSample((uint32_t)enum_id);
    }

    /**
     * @brief Retrieves the sample count for an enumeration item.
     * @param enum_id The enumeration value.
     * @return The sample count.
     */
    inline uint64_t GetItemSampleCount(EnumType enum_id) const
    {
        return BaseType::GetItemSampleCount((uint32_t)enum_id);
    }

    /**
     * @brief Retrieves the last seen occurrence percentage of an enumeration item.
     * @param enum_id The enumeration value.
     * @return The occurrence percentage.
     */
    inline double getPercentage(EnumType enum_id) const
    {
        return BaseType::getPercentage((uint32_t)enum_id);
    }
};
}  // namespace MOT

#endif /* ENUMERATEDSTATISTICVARIABLE_H */
