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
 * sctp_lock_free_queue.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_lock_free_queue.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SCTP_LOCK_FREE_QUEUE_H__
#define __SCTP_LOCK_FREE_QUEUE_H__

#include <stdint.h>  // uint32_t

#undef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE

/* @brief Lock-free queue based on a circular array */
template<typename ELEM_T>
class ArrayLockFreeQueue {
public:
    ArrayLockFreeQueue();
    virtual ~ArrayLockFreeQueue();
    int initialize(uint32_t q_size);
    /// @brief returns the current number of items in the queue
    /// It tries to take a snapshot of the size of the queue
    /// this function might return bogus values.
    ///
    /// If a reliable queue size must be kept you might want to have a look at
    /// the preprocessor variable in this header file called 'ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE'
    /// it enables a reliable size though it hits overall performance of the queue
    /// (when the reliable size variable is on it's got an impact of about 20% in time)
    uint32_t size();

    /// @brief push an element at the tail of the queue
    /// @param the element to insert in the queue
    /// Note that the element is not a pointer or a reference, so if you are using large data
    /// structures to be inserted in the queue you should think of instantiate the template
    /// of the queue as a pointer to that large structure
    /// @returns true if the element was inserted in the queue. False if the queue was full
    bool push(ELEM_T* a_data);

    /// @brief pop the element at the head of the queue
    /// @param a reference where the element in the head of the queue will be saved to
    /// Note that the a_data parameter might contain rubbish if the function returns false
    /// @returns true if the element was successfully extracted from the queue. False if the queue was empty
    ELEM_T* pop(ELEM_T* a_data);

private:
    /// @brief array to keep the elements
    ELEM_T** m_theQueue;
    char* m_written;
    volatile uint32_t m_qsize;
    /* m_index_compare use for count_to_index */
    volatile uint32_t m_index_compare;

#ifdef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
    /// @brief number of elements in the queue
    volatile uint32_t m_count;
#endif

    /// @brief where a new element will be inserted
    volatile uint32_t m_write_index;

    /// @brief where the next element where be extracted from
    volatile uint32_t m_read_index;

    /// @brief maximum read index for multiple producer queues
    /// If it's not the same as m_write_index it means
    /// there are writes pending to be "committed" to the queue, that means,
    /// the place for the data was reserved (the index in the array) but
    /// data is still not in the queue, so the thread trying to read will have
    /// to wait for those other threads to save the data into the queue
    ///
    /// note this index is only used for multiple producer thread queues
    volatile uint32_t m_maximum_read_index;

    /// @brief calculate the index in the circular array that corresponds
    /// to a particular "count" value
    inline uint32_t count_to_index(uint32_t a_count);
};

#include "libcomm_lock_free_queue.cpp"
#endif  // __SCTP_LOCK_FREE_QUEUE_H__
