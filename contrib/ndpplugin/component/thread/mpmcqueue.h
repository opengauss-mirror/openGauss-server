/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: mpmcqueue info
 * Path: src/include/component/thread/mpmcqueue.h
 */


#ifndef LIBSMARTSCAN_MPMCQUEUE_H
#define LIBSMARTSCAN_MPMCQUEUE_H

#include <atomic>
#include <cassert>

#if defined(__aarch64__) /* 64 bit x86 */
constexpr int CACHE_LINE_SIZE = 128;
#else
constexpr int CACHE_LINE_SIZE = 64;
#endif

/** Multiple producer consumer, bounded queue
 Implementation of Dmitry Vyukov's MPMC algorithm
 http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue */
template<typename T>
class MpmcBoundedQueue {
public:
    /** Constructor
    @param[in]	nElems		Max number of elements allowed */
    explicit MpmcBoundedQueue(size_t nElems)
        : _mRing(reinterpret_cast<Cell *>(new Aligned[nElems])),
          _mCapacity(nElems - 1)
    {
        /* Should be a power of 2 */
        assert((nElems & (nElems - 1)) == 0);

        for (size_t i = 0; i < nElems; ++i) {
            _mRing[i]._mPos.store(i, std::memory_order_relaxed);
        }

        _mEnqueuePos.store(0, std::memory_order_relaxed);
        _mDequeuePos.store(0, std::memory_order_relaxed);
    }

    /** Destructor */
    ~MpmcBoundedQueue()
    {
        delete[] _mRing;
    }

    /** Enqueue an element
    @param[in]	data		Element to insert, it will be copied
    @return true on success */
    bool Enqueue(T const& data)
        {
            /* _mEnqueuePos only wraps at MAX(_mEnqueuePos), instead
            we use the capacity to convert the sequence to an array
            index. This is why the ring buffer must be a size which
            is a power of 2. This also allows the sequence to double
            as a ticket/lock. */

            size_t pos = _mEnqueuePos.load(std::memory_order_relaxed);

        Cell *cell = NULL;

        for (;;) {
            cell = &_mRing[pos & _mCapacity];

            size_t seq;

            seq = cell->_mPos.load(std::memory_order_acquire);

            intptr_t diff = (intptr_t)seq - (intptr_t)pos;

            /* If they are the same then it means this cell is empty */

            if (diff == 0) {
                /* Claim our spot by moving head. If head isn't the same as we last
                checked then that means someone beat us to the punch. Weak compare is
                faster, but can return spurious results which in this instance is OK,
                because it's in the loop */

                if (_mEnqueuePos.compare_exchange_weak(pos, pos + 1,
                                                       std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                /* The queue is full */

                return (false);
            } else {
                pos = _mEnqueuePos.load(std::memory_order_relaxed);
            }
        }

        cell->_mData = data;

        /* Increment the sequence so that the tail knows it's accessible */

        cell->_mPos.store(pos + 1, std::memory_order_release);

        return (true);
        }

    /** Dequeue an element
    @param[out]	data		Element read from the queue
    @return true on success */
    bool Dequeue(T& data)
        {
            Cell *cell = NULL;
        size_t pos = _mDequeuePos.load(std::memory_order_relaxed);

        for (;;) {
            cell = &_mRing[pos & _mCapacity];

            size_t seq = cell->_mPos.load(std::memory_order_acquire);

            auto diff = (intptr_t)seq - (intptr_t)(pos + 1);

            if (diff == 0) {
                /* Claim our spot by moving the head. If head isn't the same as we last
                checked then that means someone beat us to the punch. Weak compare is
                faster, but can return spurious results. Which in this instance is
                OK, because it's in the loop. */

                if (_mDequeuePos.compare_exchange_weak(pos, pos + 1,
                                                       std::memory_order_relaxed)) {
                    break;
                }
            } else if (diff < 0) {
                /* The queue is empty */
                return (false);
            } else {
                /* Under normal circumstances this branch should never be taken. */
                pos = _mDequeuePos.load(std::memory_order_relaxed);
            }
        }

        data = cell->_mData;

        /* Set the sequence to what the head sequence should be next
        time around */

        cell->_mPos.store(pos + _mCapacity + 1, std::memory_order_release);

        return (true);
        }

    /** @return the capacity of the queue */
    size_t Capacity() const
        {
            return (_mCapacity + 1);
        }

    /** @return true if the queue is empty. */
    bool Empty() const
        {
            size_t pos = _mDequeuePos.load(std::memory_order_relaxed);

        for (;;) {
            auto cell = &_mRing[pos & _mCapacity];

            size_t seq = cell->_mPos.load(std::memory_order_acquire);

            auto diff = (intptr_t)seq - (intptr_t)(pos + 1);

            if (diff == 0) {
                return (false);
            } else if (diff < 0) {
                return (true);
            } else {
                pos = _mDequeuePos.load(std::memory_order_relaxed);
            }
        }

        return (false);
        }

private:
    using Pad = char[CACHE_LINE_SIZE];

    struct Cell {
        std::atomic<size_t> _mPos;
        T _mData;
    };

    using Aligned =
    typename std::aligned_storage<sizeof(Cell),
        std::alignment_of<Cell>::value>::type;

    Pad m_pad0;
    Cell *const _mRing;
    size_t const _mCapacity;
    Pad _mPad1;
    std::atomic<size_t> _mEnqueuePos;
    Pad _mPad2;
    std::atomic<size_t> _mDequeuePos;
    Pad _mPad3;

    MpmcBoundedQueue(MpmcBoundedQueue&&) = delete;

    MpmcBoundedQueue(const MpmcBoundedQueue&) = delete;

    MpmcBoundedQueue& operator=(MpmcBoundedQueue&&) = delete;

    MpmcBoundedQueue& operator=(const MpmcBoundedQueue&) = delete;
};

#endif //LIBSMARTSCAN_MPMCQUEUE_H
