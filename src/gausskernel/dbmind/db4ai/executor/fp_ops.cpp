/* *
Copyright (c) 2021 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

  http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
---------------------------------------------------------------------------------------

fp_ops.cpp
       Robust floating point operations

IDENTIFICATION
    src/gausskernel/dbmind/db4ai/executor/fp_ops.cpp

---------------------------------------------------------------------------------------
* */

#include "nodes/execnodes.h"

#include "db4ai/fp_ops.h"
#include "db4ai/db4ai_cpu.h"

#define WITH_ROBUST_OPS

/*
 * Knuth's high precision sum a + b (Shewchuk's version)
 */
void twoSum(double const a, double const b, double *sum, double *e)
{
    *sum = a + b;
#ifdef WITH_ROBUST_OPS
    double const bv = *sum - a;
    double const av = *sum - bv;
    double const be = b - bv;
    double const ae = a - av;
    *e = ae + be;
#else
    *e = 0.;
#endif
}

/*
 * The equivalent subtraction a - b (Shewchuk's version)
 */
void twoDiff(double const a, double const b, double *sub, double *e)
{
    *sub = a - b;
#ifdef WITH_ROBUST_OPS
    double const bv = a - *sub;
    double const av = *sub + bv;
    double const be = bv - b;
    double const ae = a - av;
    *e = ae + be;
#else
    *e = 0.;
#endif
}

static force_inline void veltkamp_split(double p, double *p_hi, double *p_low)
{
    uint32_t const shift = 27U; // ceil(53 / 2)
    uint32_t c = ((1U << shift) + 1U) * p;
    double p_big = c - p;
    *p_hi = c - p_big;
    *p_low = p - *p_hi;
}

/*
 * High precision product a * b (Shewchuk's version of Dekker-Veltkamp)
 */
void twoMult(double const a, double const b, double *mult, double *e)
{
    *mult = a * b;
#ifdef WITH_ROBUST_OPS
    double a_hi, a_low;
    double b_hi, b_low;
    double e_1, e_2, e_3;
    veltkamp_split(a, &a_hi, &a_low);
    veltkamp_split(b, &b_hi, &b_low);
    e_1 = *mult - (a_hi * b_hi);
    e_2 = e_1 - (a_low * b_hi);
    e_3 = e_2 - (a_hi * b_low);
    *e = (a_low * b_low) - e_3;
#else
    *e = 0.;
#endif
}

/*
 * High precision square a * a (Shewchuk's version of Dekker-Veltkamp)
 */
void square(double a, double *square, double *e)
{
    *square = a * a;
#ifdef WITH_ROBUST_OPS
    double a_hi, a_low;
    double e_1, e_3;
    veltkamp_split(a, &a_hi, &a_low);
    e_1 = *square - (a_hi * a_hi);
    e_3 = e_1 - (a_low * (a_hi + a_hi));
    *e = (a_low * a_low) - e_3;
#else
    *e = 0.;
#endif
}

/*
 * High precision division a / b = *div + *e (Shewchuk's version of Dekker-Veltkamp)
 */
void twoDiv(double const a, double const b, double *div, double *e)
{
#ifdef WITH_ROBUST_OPS
    twoMult(a, 1 / b, div, e);
#else
    *div = a / b;
    *e = 0.;
#endif
}

/*
 * Implementing the methods of the incremental statistics
 */
IncrementalStatistics IncrementalStatistics::operator + (IncrementalStatistics const & rhs) const
{
    IncrementalStatistics sum = *this;
    sum += rhs;
    return sum;
}

IncrementalStatistics IncrementalStatistics::operator - (IncrementalStatistics const & rhs) const
{
    IncrementalStatistics minus = *this;
    minus -= rhs;
    return minus;
}

IncrementalStatistics &IncrementalStatistics::operator += (IncrementalStatistics const & rhs)
{
    // for the term corresponding to the variance, the terms add up but there is a correcting term to
    // be added up as well. The proof of this can be found in the updated version of the HLD
    double const total_r = rhs.total;
    uint64_t const rhs_population = rhs.population;
    double increment_s = 0.;
    bool execute = (rhs_population > 0) && (population > 0);

    // this branch is as easy as it can get because population is mostly > 0
    if (likely(execute)) {
        increment_s = ((total_r * total_r) / rhs_population) + ((total * total) / population) -
            (((total_r + total) * (total_r + total)) / (rhs_population + population));
    }

    total += rhs.total;
    population += rhs.population;

    s += (rhs.s + increment_s);

    max_value = rhs.max_value > max_value ? rhs.max_value : max_value;
    min_value = rhs.min_value < min_value ? rhs.min_value : min_value;
    return *this;
}

IncrementalStatistics &IncrementalStatistics::operator -= (IncrementalStatistics const & rhs)
{
    uint64_t const current_population = population - rhs.population;
    double const current_total = total - rhs.total;
    double decrement_s = 0.;
    bool const execute = (population > 0) && (current_population > 0) && (rhs.population > 0);

    // this branch is as easy as it can get because population is mostly > 0
    if (likely(execute)) {
        decrement_s = ((current_total * current_total) / current_population) +
            ((rhs.total * rhs.total) / rhs.population) - ((total * total) / population);
    }

    total = current_total;
    population = current_population;

    s -= (rhs.s + decrement_s);
    s = std::abs(s);
    return *this;
}

double IncrementalStatistics::getMin() const
{
    return min_value;
}

double IncrementalStatistics::getMax() const
{
    return max_value;
}

double IncrementalStatistics::getTotal() const
{
    return total;
}

void IncrementalStatistics::setTotal(double t)
{
    total = max_value = min_value = t;
    population = 1ULL;
}

uint64_t IncrementalStatistics::getPopulation() const
{
    return population;
}

double IncrementalStatistics::getEmpiricalMean() const
{
    double mean = 0.;
    if (likely(population > 0))
        mean = total / population;
    return mean;
}

double IncrementalStatistics::getEmpiricalVariance() const
{
    double variance = 0.;
    if (likely(population > 0))
        variance = s / population;
    return variance;
}

double IncrementalStatistics::getEmpiricalStdDev() const
{
    double const std_dev = getEmpiricalVariance();
    // round-off errors might happen and we don't want to take
    // sqrt of a negative number (that technically should be zero for example)
    return std_dev < 0. ? 0. : std::sqrt(getEmpiricalVariance());
}

bool IncrementalStatistics::reset()
{
    errno_t rc = memset_s(this, sizeof(IncrementalStatistics), 0, sizeof(IncrementalStatistics));
    securec_check(rc, "\0", "\0");
    min_value = DBL_MAX;
    return true;
}
