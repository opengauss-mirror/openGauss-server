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
 * cycles.h
 *    Provides static methods that access CPU cycle counter and
 *    translate cycle-level times to absolute time and vice versa.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/cycles.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_CPU_CYCLE_LEVEL_TIME_H
#define MOT_CPU_CYCLE_LEVEL_TIME_H

#include <stdint.h>

namespace MOT {
/**
 * @class cycles
 *
 * @brief This class provides static methods that access CPU cycle
 * counter and translate cycle-level times to absolute time and vice versa.
 */
class CpuCyclesLevelTime {
public:
    /** @brief Initializes the object. */
    static void Init();

    /**
     * @brief Retrieve the CPU cycle counter using rdtsc instruction
     * @detail Force memory barrier. In ARM architecture, processor barrier is also forced
     * @return The CPU cycle counter value.
     */
    static __inline __attribute__((always_inline)) uint64_t Rdtsc()
    {
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        uint32_t low, high;
        __asm__ __volatile__("rdtsc" : "=a"(low), "=d"(high));
        return (((uint64_t)high << 32) | low);
#elif defined(__aarch64__)
        unsigned long cval = 0;
        asm volatile("isb; mrs %0, cntvct_el0" : "=r"(cval) : : "memory");
        return cval;
#else
#error "Unsupported CPU architecture or compiler."
#endif
    }

    /**
     * @brief Retrieve the CPU cycle counter using rdtscp instruction
     * @detail Force processor barrier and memory barrier
     * @return The CPU cycle counter value.
     */
    static __inline __attribute__((always_inline)) uint64_t Rdtscp()
    {
#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
        uint32_t low, high;
        __asm__ __volatile__("rdtscp" : "=a"(low), "=d"(high) : : "%rcx");
        return (((uint64_t)high << 32) | low);
#elif defined(__aarch64__)
        unsigned long cval = 0;
        asm volatile("isb; mrs %0, cntvct_el0" : "=r"(cval) : : "memory");
        return cval;
#else
#error "Unsupported CPU architecture or compiler."
#endif
    }

    /**
     * @brief Returns the number of CPU cycles per second.
     * @return Cycles per second.
     */
    static double CyclesPerSecond();

    /**
     * @brief Converts CPU cycles to seconds.
     * @param cycles The cycle count.
     * @param cyclesPerSecond conversion ratio (optional).
     * @return The conversion result in seconds.
     */
    static double CyclesToSeconds(uint64_t cycles, double cyclesPerSecond = 0.0);

    /**
     * @brief Converts seconds to CPU cycles.
     * @param seconds The amount of seconds.
     * @param cyclesPerSecond Conversion ratio (optional).
     * @return The conversion result in CPU cycles.
     */
    static uint64_t CyclesFromSeconds(double seconds, double cyclesPerSecond = 0.0);

    /**
     * @brief Converts CPU cycles to microseconds.
     * @param cycles The cycle count.
     * @param cyclesPerSecond Conversion ratio (optional).
     * @return The conversion result in microseconds.
     */
    static uint64_t CyclesToMicroseconds(uint64_t cycles, double cyclesPerSecond = 0.0);

    /**
     * @brief Converts micro-seconds to CPU cycles.
     * @param us The amount of micro-seconds.
     * @param cyclesPerSecond Conversion ratio (optional).
     * @return The conversion result in CPU cycles.
     */
    static uint64_t CyclesFromMicroseconds(uint64_t us, double cyclesPerSecond = 0.0);

    /**
     * @brief Converts CPU cycles to nanoseconds.
     * @param cycles The cycle count.
     * @param cyclesPerSecond Conversion ratio (optional).
     * @return The conversion result in nanoseconds.
     */
    static uint64_t CyclesToNanoseconds(uint64_t cycles, double cyclesPerSecond = 0.0);

    /**
     * @brief Converts nanoseconds to CPU cycles.
     * @param ns The amount of nanoseconds.
     * @param cyclesPerSec Conversion ratio (optional).
     * @return The conversion result in CPU cycles.
     */
    static uint64_t CyclesFromNanoseconds(uint64_t ns, double cyclesPerSecond = 0.0);

    /**
     * @brief Performs a busy-wait sleep without yielding the CPU.
     * @param ms The number of microseconds to sleep.
     */
    static void Sleep(uint64_t ms);

private:
    /** @cond EXCLUDE_DOC */
    CpuCyclesLevelTime() = delete;
    /** @endcond */

    /** @var Conversion factor between cycles and the seconds. */
    static double m_cyclesPerSecond;

    static inline __attribute__((always_inline)) double GetCyclesPerSecond()
    {
        return m_cyclesPerSecond;
    }
};
}  // namespace MOT

/**
 * @brief Retrieves the current system clock value. Delegates to CpuCyclesLevelTime::Rdtscp().
 * @return The current system clock value.
 */
__inline __attribute__((always_inline)) uint64_t GetSysClock()
{
    return MOT::CpuCyclesLevelTime::Rdtscp();
}

#endif  // MOT_CPU_CYCLE_LEVEL_TIME_H
