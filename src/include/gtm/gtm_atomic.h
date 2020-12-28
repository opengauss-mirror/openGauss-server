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
 * ---------------------------------------------------------------------------------------
 *
 * gtm_atomic.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/gtm/gtm_atomic.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CODE_SRC_INCLUDE_GTM_GTM_ATOMIC_H_
#define CODE_SRC_INCLUDE_GTM_GTM_ATOMIC_H_

template <typename T>
struct GTM_AtomicStride {
    /* increment value when add it. */
    static const unsigned int unitSize = 1;
};
template <typename T>
struct GTM_AtomicStride<T *> {
    /* increment value when add it. */
    static const unsigned int unitSize = sizeof(T);
};

/**
 * atomic operations used in gtm
 */
template <typename ValueType>
class GTM_Atomic {
public:
    /**
     * Construct an GTM_Atomic use initial value
     */
    explicit GTM_Atomic(const ValueType value = 0) : m_value(value)
    {
    }
    ~GTM_Atomic(){};

    /**
     * atomic increment
     */
    void add(int64_t increment)
    {
        __sync_fetch_and_add(&m_value, increment);
    }

    /**
     * atomic compare exchange
     */
    ValueType compareExchange(ValueType tmpValue, ValueType newValue)
    {
        tmpValue = __sync_val_compare_and_swap(&m_value, tmpValue, newValue);
        return tmpValue;
    }

    /**
     * atomic exchange
     */
    ValueType exchange(ValueType newValue)
    {
        newValue = __sync_lock_test_and_set(&m_value, newValue);
        return newValue;
    }

    /**
     * atomic increment
     */
    void inc()
    {
        add(1);
    }

    /**
     * Return the current value.
     */
    ValueType load()
    {
        return m_value;
    }

    /**
     * overload operator =
     */
    GTM_Atomic<ValueType>& operator=(ValueType newValue)
    {
        store(newValue);
        return *this;
    }

    /**
     * Return the current value.
     */
    operator ValueType()
    {
        return load();
    }

    /**
     * overload operator ++
     */
    const GTM_Atomic<ValueType>& operator++()
    {
        inc();
        return *this;
    }
    const GTM_Atomic<ValueType> operator++(int)
    {
        GTM_Atomic<ValueType> tmp = *this;
        inc();
        return tmp;
    }

    /**
     * overload operator --
     */
    const GTM_Atomic<ValueType>& operator--()
    {
        add(-1);
        return *this;
    }
    const GTM_Atomic<ValueType> operator--(int)
    {
        GTM_Atomic<ValueType> tmp = *this;
        add(-1);
        return tmp;
    }

    /**
     * set value
     */
    void store(ValueType newValue)
    {
        m_value = newValue;
    }

protected:
    /* The value on which the atomic operations operate. */
    volatile ValueType m_value;
};

/**
 * This method provides appropriate fencing for the end of a critical
 * section.  It guarantees the following:
 * - Loads coming from code preceding this method will complete before the
 *   method returns, so they will not see any changes made to memory by other
 *   threads after the method is invoked.
 * - Stores coming from code preceding this method will be reflected
 *   in memory before the method returns, so when the next thread enters
 *   the critical section it is guaranteed to see any changes made in the
 *   current critical section.
 */
static inline void GTM_loadStoreFence()
{
#ifdef __aarch64__

    __asm__ __volatile__("DMB ish" ::: "memory");
#else
    __asm__ __volatile__("lfence" ::: "memory");
    __asm__ __volatile__("sfence" ::: "memory");
#endif
}

static inline void GTM_loadFence()
{
    __asm__ __volatile__("lfence" ::: "memory");
}

static inline void GTM_StoreFence()
{
    __asm__ __volatile__("sfence" ::: "memory");
}
#endif /* CODE_SRC_INCLUDE_GTM_GTM_ATOMIC_H_ */
