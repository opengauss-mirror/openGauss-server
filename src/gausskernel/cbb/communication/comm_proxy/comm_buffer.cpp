/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * comm_buffer.cpp
 *        TODO add contents
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_buffer.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <time.h>
#include <sys/time.h>
#ifdef __USE_NUMA
#include <numa.h>
#endif
#include <arpa/inet.h>

#include "communication/commproxy_interface.h"
#include "communication/commproxy_dfx.h"
#include "comm_core.h"
#include "comm_proxy.h"
#include "comm_connection.h"
#include "executor/executor.h"

CommRingBuffer::CommRingBuffer()
{
}

CommRingBuffer::~CommRingBuffer()
{
    Assert(m_buffer == NULL);
}

void CommRingBuffer::Init(int fd, CommQueueChannel type)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    m_buffer = (RingBufferGroup *)comm_malloc(sizeof(RingBufferGroup));
    m_buffer->m_comm_buffer_size = DefaultSocketRingBufferSize;
    m_buffer->m_read_pointer = 0;
    m_buffer->m_write_pointer = 0;
    m_buffer->m_comm_buffer = (char *)comm_malloc(m_buffer->m_comm_buffer_size);
    int rc = memset_s(m_buffer->m_comm_buffer, m_buffer->m_comm_buffer_size,
                    '\0', m_buffer->m_comm_buffer_size);
    securec_check(rc, "\0", "\0");

    m_fd = fd;
    m_type = type;
    m_debug_level = COMM_DEBUG2;
    m_isempty = true;
    m_status = TransportNormal;
    m_notify_mode = g_comm_controller->m_notify_mode;
    init_commsock_recv_delay(&m_delay);

    m_enlarge_buffer = NULL;

    (void)pthread_mutex_init(&m_mutex, 0);
    (void)pthread_cond_init(&m_cv, 0);
}

void CommRingBuffer::DeInit()
{
    if (m_buffer != NULL) {
         if (m_buffer->m_comm_buffer != NULL) {
            comm_free_ext(m_buffer->m_comm_buffer);
        }
        comm_free_ext(m_buffer);
    }

    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cv);
}

bool CommRingBuffer::EnlargeBufferSize(uint32 length)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    uint32 required = 0;
    /* If capacity expansion is ongoing, we return directly */
    if (m_status == TransportChange) {
        ereport(LOG, (errmodule(MOD_COMM_PROXY), errmsg("m_status is TransportChange.")));
        return false;
    }

    if (UINT32_MAX - m_buffer->m_comm_buffer_size < length ||
        MaxSocketRingBufferSize < m_buffer->m_comm_buffer_size + length) {
        ereport(LOG, (errmodule(MOD_COMM_PROXY), errmsg("length in EnlargeBufferSize is oversize.")));
        return false;
    }

    /*
     * NOTICE: we enlarge ring buffer size, this operation affects the processing of the agent thread
     * so, we need set a exclusive operation
     */
    required = m_buffer->m_comm_buffer_size + length;

    m_enlarge_buffer = (RingBufferGroup *)comm_malloc(sizeof(RingBufferGroup));
    m_enlarge_buffer->m_comm_buffer = (char *)comm_malloc(required);
    m_enlarge_buffer->m_read_pointer = 0;
    m_enlarge_buffer->m_write_pointer = 0;
    m_enlarge_buffer->m_comm_buffer_size = required;

    while (!comm_compare_and_swap_32((int32 *)&m_status, TransportNormal, TransportChange)) {
    }

    return true;
}

bool CommRingBuffer::CheckBufferEmpty()
{
    return m_buffer->m_read_pointer == m_buffer->m_write_pointer;
}

bool CommRingBuffer::CheckBufferFull()
{
    return (m_buffer->m_write_pointer + 1) % m_buffer->m_comm_buffer_size == m_buffer->m_read_pointer;
}

size_t CommRingBuffer::GetBufferFreeBytes(int r, int w, int size)
{
    if (r == w) {
        return size - 1;
    } else if (w > r) {
        return (r - w + size - 1);
    } else {
        return (r - w - 1);
    }
}

size_t CommRingBuffer::GetBufferDataSize()
{
    uint32_t r = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    uint32_t size = m_buffer->m_comm_buffer_size;

    if (r == w) {
        return 0;
    } else if (w > r) {
        return (w - r);
    } else {
        return (w + size - r);
    }
}

void CommRingBuffer::PrintBufferDetail(const char *info)
{
    COMM_DEBUG_EXEC(
        bool is_empty = CheckBufferEmpty();
        bool is_full = CheckBufferFull();
        ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
            errmsg("ringbuffer[fd:%d, type:%d] [r:%u, w:%u]. now %s%s%s, is %sblock mode, %s.",
            m_fd,
            m_type,
            m_buffer->m_read_pointer,
            m_buffer->m_write_pointer,
            is_empty ? "empty" : "",
            is_full ? "full": "",
            (!is_empty && !is_full) ? "has part data" : "",
            g_comm_controller->FdGetCommSockDesc(m_fd)->m_block ? "" : "not ",
            info)));
    )
}


/*
 * for worker recv data
 */
int CommRingBuffer::CopyDataFromBuffer(char* s, size_t length, size_t offset)
{
    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    uint32_t r_newpos = m_buffer->m_comm_buffer_size;
    char *buffer = m_buffer->m_comm_buffer;
    uint32_t buffer_size = m_buffer->m_comm_buffer_size;

    int rc = 0;

    size_t data_len = m_buffer->m_comm_buffer_size - GetBufferFreeBytes(r_pos, w_pos, buffer_size) - 1;
    if (data_len < length + offset) {
        return EOF;
    }

    /* Memory barrier to synchronizing variables of m_buffer */
    gaussdb_read_barrier();

    /* we need add offset to readpointer */
    if (r_pos < w_pos) {
        r_newpos = r_pos + offset;
    } else {
        Assert(r_pos != w_pos);
        size_t tail_len = buffer_size - r_pos;
        if (tail_len > offset) {
            r_newpos = r_pos + offset;
        } else {
            r_newpos = offset - tail_len;
        }
    }

    /* data_len - offset >= length */
    Assert(data_len - offset >= length);

    if (r_newpos < w_pos) {
        rc = memcpy_s(s, length, buffer + r_newpos, length);
        securec_check(rc, "\0", "\0");
    } else {
        size_t tail_len = buffer_size - r_newpos;
        if (tail_len > length) {
            rc = memcpy_s(s, length, buffer + r_newpos, length);
            securec_check(rc, "\0", "\0");
        } else {
            rc = memcpy_s(s, tail_len, buffer + r_newpos, tail_len);
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(s + tail_len, length - tail_len, buffer, length - tail_len);
            securec_check(rc, "\0", "\0");
        }
    }

    return length;
}

/*
 * for worker recv data
 */
int CommRingBuffer::GetDataFromBuffer(char* s, size_t length)
{
    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    uint32_t r_newpos = m_buffer->m_comm_buffer_size;
    char *buffer = m_buffer->m_comm_buffer;
    uint32_t buffer_size = m_buffer->m_comm_buffer_size;

    int readlen = 0;
    int rc = 0;

    size_t data_len = m_buffer->m_comm_buffer_size - GetBufferFreeBytes(r_pos, w_pos, buffer_size) - 1;
    if (data_len == 0) {
        return 0;
    }
    gaussdb_read_barrier();

    if (data_len < length) {
        if (r_pos > w_pos) {
            rc = memcpy_s(s, buffer_size - r_pos, buffer + r_pos, buffer_size - r_pos);
            securec_check(rc, "\0", "\0");
            rc = memcpy_s(s + buffer_size - r_pos, w_pos, buffer, w_pos);
            securec_check(rc, "\0", "\0");
            readlen = buffer_size - r_pos + w_pos;
        } else {
            rc = memcpy_s(s, w_pos - r_pos, buffer + r_pos, w_pos - r_pos);
            securec_check(rc, "\0", "\0");
            readlen = w_pos - r_pos;
        }
        r_newpos = w_pos;
    } else {
        if (r_pos < w_pos) {
            rc = memcpy_s(s, length, buffer + r_pos, length);
            securec_check(rc, "\0", "\0");
            r_newpos = r_pos + length;
        } else {
            size_t tail_len = buffer_size - r_pos;
            if (tail_len > length) {
                rc = memcpy_s(s, length, buffer + r_pos, length);
                securec_check(rc, "\0", "\0");
                r_newpos = r_pos + length;
            } else {
                rc = memcpy_s(s, tail_len, buffer + r_pos, tail_len);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s(s + tail_len, length - tail_len, buffer, length - tail_len);
                securec_check(rc, "\0", "\0");
                r_newpos = length - tail_len;
            }
        }

        readlen = length;
    }

    comm_atomic_wirte_u32(&m_buffer->m_read_pointer, r_newpos);
    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("worker recv fd:[%d] nbytes:%d, first char:%c.    [r:%u, w:%u]->[r:%u, w:%u].",
        m_fd,
        readlen,
        s[0],
        r_pos,
        w_pos,
        m_buffer->m_read_pointer,
        m_buffer->m_write_pointer)));
    ResetDelay();
    return readlen;
}

/*
 * for worker send data
 */
int CommRingBuffer::PutDataToBuffer(const char *s, size_t len)
{
    uint32_t r_pos;
    uint32_t w_pos;
    uint32_t w_newpos;
    errno_t errorno = EOK;

    RingBufferGroup *ring_buffer = NULL;

    if (m_status == TransportNormal) {
        ring_buffer = m_buffer;
        w_newpos = ring_buffer->m_comm_buffer_size;
    } else {
        ring_buffer = m_enlarge_buffer;
        w_newpos = ring_buffer->m_comm_buffer_size;
    }

    r_pos = comm_atomic_read_u32(&ring_buffer->m_read_pointer);
    w_pos = comm_atomic_read_u32(&ring_buffer->m_write_pointer);

    size_t free_len = GetBufferFreeBytes(r_pos, w_pos, ring_buffer->m_comm_buffer_size);
    size_t write_len = free_len >= len ? len : free_len;

    size_t tail_len = 0;
    if (w_pos < r_pos) {
        errorno = memcpy_s(ring_buffer->m_comm_buffer + w_pos, write_len, s, write_len);
        securec_check(errorno, "\0", "\0");
        w_newpos = w_pos + write_len;
    } else {
        tail_len = ring_buffer->m_comm_buffer_size - w_pos;
        if (write_len <= tail_len) {
            errorno = memcpy_s(ring_buffer->m_comm_buffer + w_pos, write_len, s, write_len);
            securec_check(errorno, "\0", "\0");
            w_newpos = w_pos + write_len;
        } else {
            if (tail_len != 0) {
                errorno = memcpy_s(ring_buffer->m_comm_buffer + w_pos, tail_len, s, tail_len);
                securec_check(errorno, "\0", "\0");
            }
            errorno = memcpy_s(ring_buffer->m_comm_buffer, write_len - tail_len, s + tail_len, write_len - tail_len);
            securec_check(errorno, "\0", "\0");
            w_newpos = write_len - tail_len;
        }
    }

    gaussdb_write_barrier();
    comm_atomic_wirte_u32(&ring_buffer->m_write_pointer, w_newpos);

    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("worker send fd:[%d] nbytes:%lu, first char:%c.    [r:%u, w:%u]->[r:%u, w:%u].",
        m_fd,
        write_len,
        s[0],
        r_pos,
        w_pos,
        r_pos,
        w_newpos)));

    return write_len;
}

/*
 * for proxy recv data
 */
SockRecvStatus CommRingBuffer::RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm)
{
    gaussdb_memory_barrier();

    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    uint32_t w_newpos = 0x7fffffff;

    SockRecvStatus rtn_status = RecvStatusSuccess;
    errno = 0;

    size_t recv_exp_len = GetBufferFreeBytes(r_pos, w_pos, m_buffer->m_comm_buffer_size);
    if (recv_exp_len == 0) {
        return RecvStatusRetry;
    }

    size_t tail_len = 0;
    int nbytes = 0;

    if (g_comm_proxy_config.s_enable_dfx) {
        comm_atomic_add_fetch_u64(&(comm->m_thread_status.s_recv_packet_num), 1);
    }

    if (w_pos < r_pos) {
        nbytes = comm->m_comm_api.recv_fn(fd, m_buffer->m_comm_buffer + w_pos, recv_exp_len, 0);
        if (nbytes > 0) {
            w_newpos = w_pos + nbytes;
            rtn_status = RecvStatusSuccess;
            goto RTN_LABEL;
        }

        if (nbytes < 0 && IgnoreErrorNo(errno)) {
            rtn_status = RecvStatusRetry;
            goto RTN_LABEL;
        }

        if (nbytes == 0) {
            rtn_status = RecvStatusClose;
            goto RTN_LABEL;
        }

        rtn_status = RecvStatusError;
        goto RTN_LABEL;
    } else {
        /* first step: recv tail */
        tail_len = m_buffer->m_comm_buffer_size - w_pos;

        if (recv_exp_len <= tail_len) {
            nbytes = comm->m_comm_api.recv_fn(fd, m_buffer->m_comm_buffer + w_pos, recv_exp_len, 0);
            if (nbytes > 0) {
                w_newpos = w_pos + nbytes;
                rtn_status = RecvStatusSuccess;
                goto RTN_LABEL;
            }

            if (nbytes < 0 && IgnoreErrorNo(errno)) {
                rtn_status = RecvStatusRetry;
                goto RTN_LABEL;
            }

            if (nbytes == 0) {
                rtn_status = RecvStatusClose;
                goto RTN_LABEL;
            }

            rtn_status = RecvStatusError;
            goto RTN_LABEL;
        } else {
            if (tail_len != 0) {
                nbytes = comm->m_comm_api.recv_fn(fd, m_buffer->m_comm_buffer + w_pos, tail_len, 0);
                if (nbytes > 0) {
                    /* socket buff is recv over, immediately return */
                    if (nbytes < (int)tail_len) {
                        w_newpos = w_pos + nbytes;
                        rtn_status = RecvStatusSuccess;
                        goto RTN_LABEL;
                    }
                } else {
                    if (nbytes < 0 && IgnoreErrorNo(errno)) {
                        rtn_status = RecvStatusRetry;
                        goto RTN_LABEL;
                    }

                    if (nbytes == 0) {
                        rtn_status = RecvStatusClose;
                        goto RTN_LABEL;
                    }

                    rtn_status = RecvStatusError;
                    goto RTN_LABEL;
                }
            }

            /*
             * second step: recv head
             * if first recv is valid, we return success whatever this recv result
             * if first recv is not happen, we return with real case
             */
            nbytes = comm->m_comm_api.recv_fn(fd, m_buffer->m_comm_buffer, recv_exp_len - tail_len, 0);
            if (nbytes > 0) {
                /* tail is recv end, so second recv is start from pos 0, we can set point with recv len */
                w_newpos = nbytes;
                rtn_status = RecvStatusSuccess;
                goto RTN_LABEL;
            }

            if (nbytes < 0 && IgnoreErrorNo(errno)) {
                /* first step recv data is valid, and full to tail */
                w_newpos = w_pos + tail_len;
                rtn_status = (tail_len == 0) ? RecvStatusRetry : RecvStatusSuccess;
                goto RTN_LABEL;
            }

            if (nbytes == 0) {
                w_newpos = w_pos + tail_len;
                rtn_status = (tail_len == 0) ? RecvStatusClose : RecvStatusSuccess;
                goto RTN_LABEL;
            }

            w_newpos = w_pos + tail_len;
            rtn_status = (tail_len == 0) ? RecvStatusError : RecvStatusSuccess;
            goto RTN_LABEL;

        }
    }

RTN_LABEL:
    if (w_newpos != 0x7fffffff) {
        gaussdb_write_barrier();
        comm_atomic_wirte_u32(&m_buffer->m_write_pointer, w_newpos);
    }
    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("proxy recv fd:[%d] nbytes:%d, first char:%c>>expect len:%lu,[r:%u, w:%u]->[r:%u, w:%u], errno:%d, %m.",
        fd,
        nbytes,
        m_buffer->m_comm_buffer[w_pos],
        recv_exp_len,
        r_pos,
        w_pos,
        m_buffer->m_read_pointer,
        m_buffer->m_write_pointer,
        errno)));

        NotifyWorker(rtn_status);

        return rtn_status;
}

/*
 * for proxy send data
 */
SendStatus CommRingBuffer::SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm)
{
    SendStatus status = SendStatusReady;

    int send_len = 0;
    int nbytes = 0;
    size_t tail_len = 0;
    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    RingBufferGroup *old_ring_buffer = NULL;

    if (r_pos == w_pos) {
        if (m_status == TransportChange) {
            /* The proxy thread has sent all the old data, so we can exchange enlarge buffer to buffer */
            old_ring_buffer = m_buffer;
            m_buffer = m_enlarge_buffer;
            gaussdb_write_barrier();
            r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);;
            w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);;
            m_status = TransportNormal;

            comm_free(old_ring_buffer->m_comm_buffer);
            comm_free(old_ring_buffer);

            if (r_pos == w_pos) {
                status = SendStatusSuccess;
                goto RTN_LABEL;
            }
        } else {
            status = SendStatusSuccess;
            goto RTN_LABEL;
        }
    }

    gaussdb_read_barrier();

    if (r_pos > w_pos) {
        /* first step: send data tail */
        tail_len = m_buffer->m_comm_buffer_size - r_pos;
        if (tail_len > 0) {
            nbytes = comm->m_comm_api.send_fn(fd, m_buffer->m_comm_buffer + r_pos, tail_len, 0);
            if (g_comm_proxy_config.s_enable_dfx) {
                comm_atomic_add_fetch_u64(&(comm->m_thread_status.s_send_packet_num), 1);
            }

            ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
                errmsg("proxy send fd:[%d] nbytes:%d, data:%c.    [r:%u, w:%u]->[r:%u, w:%u].",
                fd,
                nbytes,
                m_buffer->m_comm_buffer[m_buffer->m_read_pointer],
                r_pos,
                w_pos,
                m_buffer->m_read_pointer,
                m_buffer->m_write_pointer)));

            if (nbytes > 0) {
                send_len = nbytes;
                if (nbytes < (int)tail_len) {
                    comm_atomic_wirte_u32(&m_buffer->m_read_pointer, r_pos + nbytes);
                    status = SendStatusPart;
                    goto RTN_LABEL;
                }
            } else {
                if (nbytes == 0) {
                    status = SendStatusError;
                }
                if (nbytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                    status = SendStatusError;
                }
                if (nbytes < 0) {
                    status = SendStatusPart;
                }
                goto RTN_LABEL;
            }
        }

        /* second step: send data head */
        nbytes = comm->m_comm_api.send_fn(fd, m_buffer->m_comm_buffer, w_pos, 0);
        if (g_comm_proxy_config.s_enable_dfx) {
            comm_atomic_add_fetch_u64(&(comm->m_thread_status.s_send_packet_num), 1);
        }

        ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
            errmsg("proxy send fd:[%d] nbytes:%d, data:%c.    [r:%u, w:%u]->[r:%u, w:%u].",
            fd,
            nbytes,
            m_buffer->m_comm_buffer[0],
            r_pos,
            w_pos,
            m_buffer->m_read_pointer,
            m_buffer->m_write_pointer)));

        if (nbytes > 0) {
            send_len += nbytes;
            if (nbytes < (int)w_pos) {
                comm_atomic_wirte_u32(&m_buffer->m_read_pointer, nbytes);
                status = SendStatusPart;
                goto RTN_LABEL;
            }

            comm_atomic_wirte_u32(&m_buffer->m_read_pointer, w_pos);
            status = SendStatusSuccess;
            goto RTN_LABEL;
        }

        if (nbytes < 0 && IgnoreErrorNo(errno)) {
            comm_atomic_wirte_u32(&m_buffer->m_read_pointer, r_pos + tail_len);
            status = SendStatusPart;
            goto RTN_LABEL;
        }

        /*
         * nbyte == 0: connection close by peer, all data is invalid
         * nbyte < 0, other erro
         */
        status = SendStatusError;
        goto RTN_LABEL;
    } else {
        nbytes = comm->m_comm_api.send_fn(fd, m_buffer->m_comm_buffer + r_pos, w_pos - r_pos, 0);
        if (g_comm_proxy_config.s_enable_dfx) {
            comm_atomic_add_fetch_u64(&(comm->m_thread_status.s_send_packet_num), 1);
        }

        ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
            errmsg("proxy send fd:[%d] nbytes:%d, data:%c    [r:%u, w:%u]->[r:%u, w:%u].",
            fd,
            nbytes,
            m_buffer->m_comm_buffer[m_buffer->m_read_pointer],
            r_pos,
            w_pos,
            m_buffer->m_read_pointer,
            m_buffer->m_write_pointer)));

        if (nbytes > 0) {
            if (nbytes < (int)(w_pos - r_pos)) {
                comm_atomic_wirte_u32(&m_buffer->m_read_pointer, r_pos + nbytes);
                status = SendStatusPart;
                goto RTN_LABEL;
            }
            comm_atomic_wirte_u32(&m_buffer->m_read_pointer, w_pos);
            status = SendStatusSuccess;
            goto RTN_LABEL;
        }

        if (nbytes < 0 && IgnoreErrorNo(errno)) {
            status = SendStatusPart;
            goto RTN_LABEL;
        }

        status = SendStatusError;
        goto RTN_LABEL;
    }

RTN_LABEL:

    /* compile error */
    return status;
}

bool CommRingBuffer::NeedWait(bool block)
{
    if (!block) {
        return false;
    }

    if (m_notify_mode == CommProxyNotifyModeSem) {
        struct timespec time_to_wait;
        AutoMutexLock wait_lock(&m_mutex);

        m_isempty = true;
        while (m_isempty) {
            clock_gettime(CLOCK_REALTIME, &time_to_wait);
            time_to_wait.tv_sec += 2;
            time_to_wait.tv_nsec = 0;
            (void)pthread_cond_timedwait(&m_cv, &m_mutex, &time_to_wait);
        }
    } else if (m_notify_mode == CommProxyNotifyModeSpinLoop) {
        perform_commsock_recv_delay(&m_delay);
    } else if (m_notify_mode == CommProxyNotifyModePolling) {
        while (CheckBufferEmpty()) {
            continue;
        }
    } else {
        ereport(WARNING, (errmodule(MOD_COMM_PROXY), errmsg("m_notify_mode set error. %s.", __FUNCTION__)));
    }

    return true;
}

void CommRingBuffer::NotifyWorker(SockRecvStatus rtn_status)
{
    if (m_notify_mode != CommProxyNotifyModeSem) {
        return;
    }

    if (rtn_status == RecvStatusSuccess && m_isempty) {
        AutoMutexLock wait_lock(&m_mutex);
        (void)pthread_cond_signal(&m_cv);
    }
}

void CommRingBuffer::SetNotifyMode(CommProxyNotifyMode mode)
{
    m_notify_mode = mode;
}

void CommRingBuffer::WaitBufferEmpty()
{
    if (m_type == ChannelRX) {
        return;
    }

    while (!CheckBufferEmpty()) {
        CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(m_fd);
        if (comm_sock == NULL) {
            ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("CommSockDesc is null in WaitBufferEmpty, fd:%d.", m_fd),
                errdetail("N/A"),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }

        if (comm_sock->m_status == CommSockStatusClosedByPeer) {
            return;
        }
        /* if send queue is not empty, wait 100us */
        pg_usleep(100);
    }

}

#ifdef USE_LIBNET
CommRingBufferLibnet::CommRingBufferLibnet()
{
    m_comm_buffer = NULL;
    m_buffer = NULL;
}

CommRingBufferLibnet::~CommRingBufferLibnet()
{
    Assert(m_comm_buffer == NULL);
    Assert(m_buffer == NULL);
}

void CommRingBufferLibnet::Init(int fd, CommQueueChannel type)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    m_buffer = (RingBufferGroup *)comm_malloc(sizeof(RingBufferGroup));
    m_buffer->m_comm_buffer_size = MaxPbufNum;
    m_buffer->m_read_pointer = 0;
    m_buffer->m_write_pointer = 0;
    m_buffaddr_pointer = 0;
    m_local_read_pointer = 0;
    m_minus_buff_num_counter = 0;
    m_curr_data_len = 0;
    m_fd = fd;
    m_type = type;
    m_notify_mode = g_comm_controller->m_notify_mode;
    m_comm_buffer =  (struct pkt_buff_info *)comm_malloc(sizeof(struct pkt_buff_info) * m_buffer->m_comm_buffer_size);
    int rc = memset_s(m_comm_buffer, MaxPbufNum * sizeof(struct pkt_buff_info),
                     '\0', MaxPbufNum * sizeof(struct pkt_buff_info));
    securec_check(rc, "\0", "\0");

    m_debug_level = COMM_DEBUG1;
    m_isempty = true;
    init_commsock_recv_delay(&m_delay);
    m_notify_mode = g_comm_controller->m_notify_mode;
}

void CommRingBufferLibnet::DeInit()
{
    if (m_comm_buffer != NULL) {
        comm_free_ext(m_comm_buffer);
    }

    if (m_buffer != NULL) {
        comm_free_ext(m_buffer);
    }
}

/*
 * for worker send data
 */
int CommRingBufferLibnet::PutDataToBuffer(const char *s, size_t len)
{
    Assert(0);
    return 0;
}

/*
 * for proxy send data
 */
SendStatus CommRingBufferLibnet::SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm)
{
    Assert(0);
    return SendStatusSuccess;
}

size_t CommRingBufferLibnet::GetBufferDataSize()
{
    return m_curr_data_len;
}

int CommRingBufferLibnet::CopyDataFromBuffer(char* s, size_t length, size_t offset)
{
    return 0;
}


int CommRingBufferLibnet::GetDataFromBuffer(char* s, size_t length)
{
    gaussdb_memory_barrier();
    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    if (r_pos == w_pos) {
        return 0;
    }
    int readlen = 0;

    struct ring_buff_desc ring_buff = {
        .buff = m_comm_buffer,
        .begin = r_pos,
        .end = w_pos,
        .buffer_size = m_buffer->m_comm_buffer_size
    };

    readlen = lwip_get_data_from_buff_addr((void *)s, length, &ring_buff);

    comm_atomic_fetch_sub_u32(&m_curr_data_len, readlen);

    comm_atomic_wirte_u32(&m_buffer->m_read_pointer, ring_buff.begin);

    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("worker recv fd:[%d] nbytes:%d, first char:%c.    [r:%u, w:%u]->[r:%u, w:%u], m_buffaddr_pointer:%u.",
        m_fd,
        readlen,
        s[0],
        r_pos,
        w_pos,
        m_buffer->m_read_pointer,
        m_buffer->m_write_pointer,
        m_buffaddr_pointer)));

    gaussdb_memory_barrier();
    ResetDelay();
    return readlen;
}

/*
 * for proxy recv data
 */
SockRecvStatus CommRingBufferLibnet::RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm)
{
    gaussdb_memory_barrier();

    uint32_t r_pos = comm_atomic_read_u32(&m_buffer->m_read_pointer);
    uint32_t w_pos = comm_atomic_read_u32(&m_buffer->m_write_pointer);
    struct ring_buff_desc ring_buff = {
        .buff = m_comm_buffer,
        .begin = m_buffaddr_pointer,
        .end = r_pos,
        .buffer_size = m_buffer->m_comm_buffer_size
    };

    lwip_release_pkt_buf_info((void *)&ring_buff);
    m_buffaddr_pointer = r_pos;

    SockRecvStatus rtn_status = RecvStatusSuccess;
    errno = 0;

    int nbytes = 0;
    ring_buff.begin = w_pos;
    ring_buff.end = m_buffaddr_pointer;

    nbytes = comm->m_comm_api.addr_recv_fn(fd, (void *)&ring_buff, 0, 0);

    if (g_comm_proxy_config.s_enable_dfx) {
        comm_atomic_add_fetch_u64(&(comm->m_thread_status.s_recv_packet_num), 1);
    }

    if (nbytes > 0) {
        gaussdb_memory_barrier();
        comm_atomic_wirte_u32(&m_buffer->m_write_pointer, ring_buff.begin);
        comm_atomic_add_fetch_u32(&m_curr_data_len, nbytes);
    } else {
        if (nbytes == 0) {
            rtn_status = RecvStatusClose;
            goto RTN_LABEL;
        }
        if (nbytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            rtn_status = RecvStatusError;
            goto RTN_LABEL;
        }
        if (nbytes < 0) {
            rtn_status = RecvStatusRetry;
            goto RTN_LABEL;
        }
    }


RTN_LABEL:
    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("proxy recv fd:[%d] nbytes:%d, [r:%u, w:%u]->[r:%u, w:%u], m_buffaddr_pointer:%u, errno:%d, %m.",
        fd,
        nbytes,
        r_pos,
        w_pos,
        m_buffer->m_read_pointer,
        m_buffer->m_write_pointer,
        m_buffaddr_pointer,
        errno)));

    NotifyWorker(rtn_status);

    return rtn_status;
}
#endif

CommPacketBuffer::CommPacketBuffer()
{
}

CommPacketBuffer::~CommPacketBuffer()
{
    sem_destroy(&m_data_queue_sem);
    Assert(m_data_queue == NULL);
}

void CommPacketBuffer::Init(int fd, CommQueueChannel type)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    m_fd = fd;
    m_type = type;
    m_debug_level = COMM_DEBUG1;
    m_recv_packet = NULL;
    m_data_queue = new boost::lockfree::queue<Packet*>(1024);
    sem_init(&m_data_queue_sem, 0, 0);
}

void CommPacketBuffer::DeInit()
{
    sem_destroy(&m_data_queue_sem);
    delete m_data_queue;
    m_data_queue = NULL;
}

bool CommPacketBuffer::CheckBufferEmpty()
{
    if (m_type == ChannelRX) {
        if (m_recv_packet != NULL && (m_recv_packet->cur_size - m_recv_packet->cur_off > 0)) {
            return false;
        }
    }

    return m_data_queue->empty();
}

bool CommPacketBuffer::CheckBufferFull()
{
    return false;
}

size_t CommPacketBuffer::GetBufferFreeBytes(int r, int w)
{
    return 0;
}
size_t CommPacketBuffer::GetBufferDataSize()
{
    return 0;
}

void CommPacketBuffer::PrintBufferDetail(const char *info)
{
}

/*
 * for worker recv data
 */
int CommPacketBuffer::GetDataFromBuffer(char* s, size_t length)
{
    int recv_length = 0;
    int rc = 0;

    /* Polling the receive data from rx_queue */
    if (m_recv_packet == NULL) {
        if ((m_recv_packet = RemoveHeadPacket()) == NULL) {
            return 0;
        }
    }

    Packet* recv_packet = m_recv_packet;
    if (recv_packet->comm_errno > 0) {
        /* if pack errno > 0. we not recv anything to buffer */
        m_recv_packet = NULL;
        ereport(WARNING, (errmodule(MOD_COMM_PROXY),
            errmsg("fd[%d] recv data error:%d,%m,pack->cur_off:%d,pack->cur_size:%d,pack->exp_size:%d,pack->data:%s.",
            recv_packet->sockfd, recv_packet->comm_errno,
            recv_packet->cur_off,
            recv_packet->cur_size,
            recv_packet->exp_size,
            recv_packet->data)));
        BackPktBuff(recv_packet);
        return 0;
    }

    if (recv_packet->cur_size - recv_packet->cur_off > (int)length) {
        recv_length = length;
        rc = memcpy_s(s, recv_length, recv_packet->data + recv_packet->cur_off, recv_length);
        securec_check(rc, "\0", "\0");
        recv_packet->cur_off += length;
    } else {
        recv_length = recv_packet->cur_size - recv_packet->cur_off;
        rc = memcpy_s(s, recv_length, recv_packet->data + recv_packet->cur_off, recv_length);
        securec_check(rc, "\0", "\0");
        recv_packet->cur_off = recv_packet->cur_size;
    }

    ereport(DEBUG4, (errmodule(MOD_COMM_PROXY), errmsg("worker recv from fd[%d] len[%d] data:%s.",
        recv_packet->sockfd,
        recv_length,
        s)));

    if (recv_packet->cur_off >= recv_packet->cur_size) {
        m_recv_packet = NULL;
        BackPktBuff(recv_packet);
    }

    return recv_length;
}

/*
 * for worker send data
 */
int CommPacketBuffer::PutDataToBuffer(const char *s, size_t len)
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);

    Packet* pack = NULL;
    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(m_fd);
    if (comm_sock == NULL || comm_sock->m_communicator == NULL ||
        comm_sock->m_communicator->m_packet_buf[ChannelTX] == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("CommSockDesc or m_communicator or m_packet_buf is null in PutDataToBuffer, fd:%d.", m_fd),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }

    ThreadPoolCommunicator* comm = comm_sock->m_communicator;
    int fd = m_fd;

    /* if length > MAX_LENGTH, we need loop to send  */
    int ret = comm->m_packet_buf[ChannelTX]->pop(pack);
    if (!ret) {
        pack = CreatePacket(fd, s, NULL, len, PKT_TYPE_SEND);
    } else {
        pack->sockfd = fd;
        if (pack->outbuff != NULL) {
            comm_free_ext(pack->outbuff);
        }
    }
    pack->exp_size = len;
    pack->outbuff = (char*)comm_malloc(sizeof(char) * len);
    int rc = memcpy_s(pack->outbuff, len, s, len);
    securec_check(rc, "\0", "\0");

    AddTailPacket(pack);

    return len;
}

/*
 * for proxy recv data
 */
SockRecvStatus CommPacketBuffer::RecvDataToBuffer(int fd, ThreadPoolCommunicator *comm)
{
    gaussdb_memory_barrier();

    int nbytes = 0;
    SockRecvStatus rtn_status = RecvStatusSuccess;
    Packet* pack = NULL;
    int ret = comm->m_packet_buf[ChannelRX]->pop(pack);
    if (!ret) {
        pack = CreatePacket(m_fd, NULL, NULL, 512, PKT_TYPE_RECV);
    } else {
        pack->sockfd = m_fd;
    }

    Assert(pack->data != NULL);
    nbytes = comm->m_comm_api.recv_fn(pack->sockfd, pack->data, pack->exp_size, 0);

    /* Handle expeptional case */
    if (nbytes <= 0) {
        pack->comm_errno = errno;
        if (nbytes == 0) {
             rtn_status = RecvStatusClose;
             goto RTN_LABEL;
        }

        if (nbytes < 0 &&
             pack->comm_errno != EAGAIN && pack->comm_errno != EWOULDBLOCK && pack->comm_errno != EINTR) {
            rtn_status = RecvStatusError;
            goto RTN_LABEL;
        }
    }

    /* Handle regular cases for full-packet read */
    pack->cur_size = nbytes;

    pack->done = true;

    AddTailPacket(pack);

RTN_LABEL:
    ereport(DEBUG5, (errmodule(MOD_COMM_PROXY),
        errmsg("%s recv pack->fd:%d pack->exp_size:%d pack->cur_size:%d pack->data:%s.",
        t_thrd.proxy_cxt.identifier,
        pack->sockfd,
        pack->exp_size,
        pack->cur_size,
        pack->data)));

        return rtn_status;
}

/*
 * for proxy send data
 */
SendStatus CommPacketBuffer::SendDataFromBuffer(int fd, ThreadPoolCommunicator *comm)
{
    Packet* pack = NULL;
    SendStatus status = SendStatusReady;
    int start_pos = 0;
    int send_len = 0;

    while ((pack = GetHeadPacket()) != NULL) {
        start_pos = pack->cur_off;
        status = comm_one_pack_send_by_doublequeue(pack);
        if (status != SendStatusSuccess) {
            return status;
        }

        send_len = pack->cur_off - start_pos;

        PopHeadPacket();
        BackPktBuff(pack);
    }

    return status;
}

bool CommPacketBuffer::NeedWait(bool block)
{
    if (block) {
        if (m_recv_packet != NULL) {
            return true;
        }
        while ((m_recv_packet = RemoveHeadPacket()) == NULL) {
            struct timeval now;
            struct timespec outtime;
            gettimeofday(&now, NULL);
            outtime.tv_sec = now.tv_sec;
            outtime.tv_nsec = now.tv_usec * 1000 + 100 * 1000;
            sem_timedwait(&m_data_queue_sem, &outtime);
            gaussdb_memory_barrier();
        }
        return true;
    } else {
        return false;
    }
}

void CommPacketBuffer::WaitBufferEmpty()
{
    if (m_type == ChannelRX) {
                /*
         * first, clear last processed packet(m_recv_packet), m_recv_packet maybe null
         * then, clear all packet in recv queue
         */
        BackPktBuff(m_recv_packet);
        ClearQueuePacket();
    } else {
        while (m_data_queue->empty() != 0) {
            CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(m_fd);
            if (comm_sock == NULL) {
                ereport(ERROR, (errmodule(MOD_COMM_PROXY),
                    errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("CommSockDesc is null in WaitBufferEmpty, fd:%d.", m_fd),
                    errdetail("N/A"),
                    errcause("System error."),
                    erraction("Contact Huawei Engineer.")));
            }

            if (comm_sock->m_status == CommSockStatusClosedByPeer) {
                ClearQueuePacket();
                return;
            }
            /* if send queue is not empty, wait 100us */
            pg_usleep(100);
        }
    }
}

void CommPacketBuffer::ClearQueuePacket()
{
    Packet *pack = NULL;
    while ((pack = RemoveHeadPacket()) != NULL) {
        BackPktBuff(pack);
    }

}

void CommPacketBuffer::BackPktBuff(Packet* pack)
{
    if (pack == NULL) {
        return;
    }

    ClearPacket(&pack);

    CommSockDesc* comm_sock = g_comm_controller->FdGetCommSockDesc(m_fd);
    if (comm_sock == NULL || comm_sock->m_communicator == NULL ||
        comm_sock->m_communicator->m_packet_buf[m_type] == NULL) {
        ereport(ERROR, (errmodule(MOD_COMM_PROXY),
            errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("Can't find CommSockDesc or m_communicator is null or m_packet_buf is null in BackPktBuff, "
                "fd:%d, type:%d.", m_fd, m_type),
            errdetail("N/A"),
            errcause("System error."),
            erraction("Contact Huawei Engineer.")));
    }
    comm_sock->m_communicator->m_packet_buf[m_type]->push(pack);
}
