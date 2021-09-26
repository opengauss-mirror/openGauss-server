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
 * cycles.cpp
 *    Provides static methods that access CPU cycle counter and
 *    translate cycle-level times to absolute time and vice versa.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/synchronization/cycles.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cycles.h"

#include <sys/time.h>
#include <math.h>

#include "utilities.h"

namespace MOT {
DECLARE_LOGGER(CpuCyclesLevelTime, System)

double CpuCyclesLevelTime::m_cyclesPerSecond = 1.0;
static constexpr double EPSILON = 1.0E-06;

void CpuCyclesLevelTime::Init()
{
    if (m_cyclesPerSecond > 1.0) {
        return;
    }

    struct timeval startTime;
    struct timeval stopTime;
    uint64_t startCycles = 0;
    uint64_t stopCycles = 0;
    uint64_t micros = 0;
    double oldCycles = 0.0f;

    while (true) {
        if (gettimeofday(&startTime, nullptr) != 0) {
            MOT_LOG_SYSTEM_ERROR(gettimeofday, "CpuCyclesLevelTime::init: gettimeofday failed");
        }
        startCycles = Rdtsc();
        while (true) {
            if (gettimeofday(&stopTime, nullptr) != 0) {
                MOT_LOG_SYSTEM_ERROR(gettimeofday, "CpuCyclesLevelTime::init: gettimeofday failed");
            }
            stopCycles = Rdtsc();
            micros = (stopTime.tv_usec - startTime.tv_usec) + (stopTime.tv_sec - startTime.tv_sec) * 1000000;
            if (micros > 10000) {
                m_cyclesPerSecond = static_cast<double>(stopCycles - startCycles);
                m_cyclesPerSecond = 1000000.0 * m_cyclesPerSecond / static_cast<double>(micros);
                break;
            }
        }
        double delta = m_cyclesPerSecond / 1000.0;
        if ((oldCycles > (m_cyclesPerSecond - delta)) && (oldCycles < (m_cyclesPerSecond + delta))) {
            return;
        }
        oldCycles = m_cyclesPerSecond;
    }
}

double CpuCyclesLevelTime::CyclesPerSecond()
{
    return GetCyclesPerSecond();
}

double CpuCyclesLevelTime::CyclesToSeconds(uint64_t cycles, double cyclesPerSecond)
{
    if (fabs(cyclesPerSecond) <= EPSILON) {
        cyclesPerSecond = GetCyclesPerSecond();
    }
    return static_cast<double>(cycles) / cyclesPerSecond;
}

uint64_t CpuCyclesLevelTime::CyclesFromSeconds(double seconds, double cyclesPerSecond)
{
    if (fabs(cyclesPerSecond) <= EPSILON) {
        cyclesPerSecond = GetCyclesPerSecond();
    }
    return (uint64_t)(seconds * cyclesPerSecond + 0.5);
}

uint64_t CpuCyclesLevelTime::CyclesToMicroseconds(uint64_t cycles, double cyclesPerSecond)
{
    return CyclesToNanoseconds(cycles, cyclesPerSecond) / 1000;
}

uint64_t CpuCyclesLevelTime::CyclesFromMicroseconds(uint64_t ms, double cyclesPerSecond)
{
    return CyclesFromNanoseconds(1000 * ms, cyclesPerSecond);
}

uint64_t CpuCyclesLevelTime::CyclesToNanoseconds(uint64_t cycles, double cyclesPerSecond)
{
    if (fabs(cyclesPerSecond) <= EPSILON) {
        cyclesPerSecond = GetCyclesPerSecond();
    }
    return (uint64_t)(1e09 * static_cast<double>(cycles) / cyclesPerSecond + 0.5);
}

uint64_t CpuCyclesLevelTime::CyclesFromNanoseconds(uint64_t ns, double cyclesPerSecond)
{
    if (fabs(cyclesPerSecond) <= EPSILON) {
        cyclesPerSecond = GetCyclesPerSecond();
    }
    return (uint64_t)(static_cast<double>(ns) * cyclesPerSecond / 1e09 + 0.5);
}

void CpuCyclesLevelTime::Sleep(uint64_t ms)
{
    uint64_t endCycles = CpuCyclesLevelTime::Rdtsc() + CpuCyclesLevelTime::CyclesFromNanoseconds(1000 * ms);
    while (CpuCyclesLevelTime::Rdtsc() < endCycles) {
    }
}
}  // namespace MOT
