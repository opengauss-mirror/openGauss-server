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
 * sctp_lock_free_queue.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_lock_free_queue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <assert.h>
#include "libcomm_atomics.h"
#include "libcomm_lock_free_queue.h"
#include "libcomm_common.h"

template<typename ELEM_T>
ArrayLockFreeQueue<ELEM_T>::ArrayLockFreeQueue()
    : m_theQueue(NULL),
      m_written(NULL),
      m_qsize(0),
      m_index_compare(0),
      m_write_index(0),
      m_read_index(0),
      m_maximum_read_index(0)
{
#ifdef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
    m_count = 0;
#endif
}

template<typename ELEM_T>
int ArrayLockFreeQueue<ELEM_T>::initialize(uint32_t qsize)
{
    const int UINT_BIT_OFFSET_NUM = 31;
    if (qsize == 0 || (qsize > (uint32_t)(1 << UINT_BIT_OFFSET_NUM))) {
        return -1;
    }

    uint32_t real_size = 1;
    uint32_t q_size, w_size;

    /* real_size is a power of 2, and >= qsize */
    while (real_size < qsize) {
        real_size = (real_size << 1);
    }

    /* save the real_size */
    m_qsize = real_size;
    /* m_index_compare use for count_to_index */
    m_index_compare = m_qsize - 1;
    /* m_theQueue[] save the pointer */
    q_size = real_size * (sizeof(ELEM_T*));
    LIBCOMM_MALLOC(m_theQueue, q_size, ELEM_T*);
    /* m_written[] set 1 when m_theQueue[m_write_index] is written and m_maximum_read_index not updated yet */
    w_size = real_size * (sizeof(char));
    LIBCOMM_MALLOC(m_written, w_size, char);

    if (m_theQueue == NULL || m_written == NULL) {
        if (m_theQueue != NULL) {
            LIBCOMM_FREE(m_theQueue, q_size);
        }
        if (m_written != NULL) {
            LIBCOMM_FREE(m_written, w_size);
        }
        return -1;
    }
    return 0;
}

template<typename ELEM_T>
ArrayLockFreeQueue<ELEM_T>::~ArrayLockFreeQueue()
{
    uint32_t q_size = m_qsize * (uint32_t)(sizeof(ELEM_T*));
    uint32_t w_size = m_qsize * (uint32_t)(sizeof(char));
    LIBCOMM_FREE(m_theQueue, q_size);
    LIBCOMM_FREE(m_written, w_size);
}

template<typename ELEM_T>
inline uint32_t ArrayLockFreeQueue<ELEM_T>::count_to_index(uint32_t a_count)
{
    /* if m_qsize is a power of 2 this statement could be also written as (a_count & m_qsize - 1); */
    /* use m_index_compare replace m_qsize - 1 */
    return (a_count & m_index_compare);
}

template<typename ELEM_T>
uint32_t ArrayLockFreeQueue<ELEM_T>::size()
{
#ifdef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
    return m_count;
#else
    uint32_t currentWriteIndex = m_write_index;
    uint32_t currentReadIndex = m_read_index;

    // let's think of a scenario where this function returns bogus data
    // 1. when the statement 'currentWriteIndex = m_write_index' is run
    // m_write_index is 3 and m_read_index is 2. Real size is 1
    // 2. afterwards this thread is preemted. While this thread is inactive 2
    // elements are inserted and removed from the queue, so m_write_index is 5
    // m_read_index 4. Real size is still 1
    // 3. Now the current thread comes back from preemption and reads m_read_index.
    // currentReadIndex is 4
    // 4. currentReadIndex is bigger than currentWriteIndex, so
    // m_total_size + currentWriteIndex - currentReadIndex is returned, that is,
    // it returns that the queue is almost full, when it is almost empty
    if (currentWriteIndex >= currentReadIndex) {
        return (currentWriteIndex - currentReadIndex);
    } else {
        return (m_qsize + currentWriteIndex - currentReadIndex);
    }
#endif  // ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
}

template<typename ELEM_T>
bool ArrayLockFreeQueue<ELEM_T>::push(ELEM_T* a_data)
{
    uint32_t currentReadIndex;
    uint32_t currentWriteIndex;
    uint32_t currentArrayIndex;

    for (;;) {
        currentWriteIndex = m_write_index;
        currentReadIndex = m_read_index;
        if (count_to_index(currentWriteIndex + 1) == count_to_index(currentReadIndex)) {
            // the queue is full
            return false;
        }

        if (true == COMPARE_AND_SWAP(&m_write_index, currentWriteIndex, currentWriteIndex + 1)) {
            break;
        }
    }

    currentArrayIndex = count_to_index(currentWriteIndex);

    m_theQueue[currentArrayIndex] = a_data;

    /*
     * m_written falg is 0 means other thread use it,
     * wait flag is 0, then set this flag is 1
     */
    while (!COMPARE_AND_SWAP(&m_written[currentArrayIndex], 0, 1)) {
        sched_yield();
    }

    do {
        if (COMPARE_AND_SWAP(&m_maximum_read_index, currentWriteIndex, currentWriteIndex + 1)) {
            /* m_maximum_read_index has update, clean m_written flag */
            m_written[currentArrayIndex] = 0;
        } else {
            /*
             * update m_maximum_read_index failed, must be other thread has not updated m_maximum_read_index yet,
             * this thread no need to wait other thread update m_maximum_read_index first,
             * exit now, and other thread can help this thread to update m_maximum_read_index with m_written flag.
             */
            break;
        }

        /* get next currentWriteIndex and   currentArrayIndex */
        currentWriteIndex = currentWriteIndex + 1;
        /* update currentArrayIndex when currentWriteIndex changed */
        currentArrayIndex = count_to_index(currentWriteIndex);
        /* if m_written flag has been set, help other thread to update m_maximum_read_index */
    } while (m_written[currentArrayIndex] == 1);

    // The value was successfully inserted into the queue
#ifdef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
    atomic_add(&m_count, 1);
#endif

    return true;
}

template<typename ELEM_T>
// bool ArrayLockFreeQueue<ELEM_T, Q_SIZE>::pop(ELEM_T *a_data)
ELEM_T* ArrayLockFreeQueue<ELEM_T>::pop(ELEM_T* a_data)
{
    uint32_t currentMaximumReadIndex;
    uint32_t currentReadIndex;

    do {
        // to ensure thread-safety when there is more than 1 producer thread
        // a second index is defined (m_maximum_read_index)
        currentReadIndex = m_read_index;
        currentMaximumReadIndex = m_maximum_read_index;

        if (count_to_index(currentReadIndex) == count_to_index(currentMaximumReadIndex)) {
            // the queue is empty or
            // a producer thread has allocate space in the queue but is
            // waiting to commit the data into it
            return NULL;
        }

        // retrieve the data from the queue
        a_data = m_theQueue[count_to_index(currentReadIndex)];

        // try to perfrom now the CAS operation on the read index. If we succeed
        // a_data already contains what m_read_index pointed to before we
        // increased it
        if (COMPARE_AND_SWAP(&m_read_index, currentReadIndex, currentReadIndex + 1)) {
            // got here. The value was retrieved from the queue. Note that the
            // data inside the m_queue array is not deleted nor reseted
#ifdef ARRAY_LOCK_FREE_Q_KEEP_REAL_SIZE
            atomic_sub(&m_count, 1);
#endif
            return a_data;
        }

        // it failed retrieving the element off the queue. Someone else must
        // have read the element stored at count_to_index(currentReadIndex)
        // before we could perform the CAS operation
    } while (1);  // keep looping to try again!

    // Something went wrong. it shouldn't be possible to reach here
    Assert(0);

    // Add this return statement to avoid compiler warnings
    return NULL;
}
