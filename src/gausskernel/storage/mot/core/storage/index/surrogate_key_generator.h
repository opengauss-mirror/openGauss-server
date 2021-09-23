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
 * surrogate_key_generator.h
 *    Global unique key generator for fake-index and row-id.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/index/surrogate_key_generator.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SURROGATE_KEY_GENERATOR_H
#define SURROGATE_KEY_GENERATOR_H

#include "global.h"

namespace MOT {
/**
 * @class SurrogateKeyGenerator
 * @brief Generates global unique key for fake-index and row-id
 */
class SurrogateKeyGenerator {
public:
    explicit SurrogateKeyGenerator(uint64_t counter = INITIAL_KEY) : m_surrogateCounter(counter)
    {}

    SurrogateKeyGenerator(const SurrogateKeyGenerator& orig)
    {
        m_surrogateCounter = orig.GetCurrentCount();
    }

    ~SurrogateKeyGenerator()
    {}

    static constexpr uint64_t INITIAL_KEY = 1;
    static constexpr uint32_t KEY_BITS = 48;
    static constexpr uint64_t KEY_MASK = (1UL << KEY_BITS) - 1;

    inline void Set(uint64_t counter)
    {
        m_surrogateCounter = counter;
    }

    /**
     * @brief Generates unique-id based on connectionId
     * @param connectionId Current connection-id
     * @return unique global key
     */
    uint64_t GetSurrogateKey(uint64_t connectionId)
    {
        uint64_t key = connectionId;
        key = (key << KEY_BITS) | (m_surrogateCounter++ & KEY_MASK);
        return key;
    }

    inline uint64_t GetCurrentCount() const
    {
        return m_surrogateCounter;
    }

private:
    /** @var local counter   */
    uint64_t m_surrogateCounter = INITIAL_KEY;
};
}  // namespace MOT
#endif /* SURROGATE_KEY_GENERATOR_H */
