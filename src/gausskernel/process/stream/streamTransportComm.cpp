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
 * streamTransportComm.cpp
 *	  Support methods for class StreamCOMM.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/streamTransportComm.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libcomm/libcomm.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamTransportCore.h"
#include "distributelayer/streamTransportComm.h"

StreamCOMM::StreamCOMM(libcommaddrinfo* addr, bool flag) : m_addr(addr)
{
    m_nodeName[0] = '\0';
    m_nodeoid = InvalidOid;
    m_type = STREAM_COMM;
    m_sendSide = flag;
    m_port = NULL;
    m_buffer = NULL;
}

StreamCOMM::~StreamCOMM()
{
    m_addr = NULL;
}

/*
 * @Description: Send a normal message
 *
 * @param[IN] msgtype:  message type
 * @param[IN] msg:  pointer to message body
 * @param[IN] len:  length of the message
 * @return: 0 if OK, EOF if trouble
 */
int StreamCOMM::send(char msgtype, const char* msg, size_t len)
{
    return pq_putmessage(msgtype, msg, len);
}

/*
 * @Description: Flush pending output
 *
 * @return: void
 */
void StreamCOMM::flush()
{
    pq_flush();
}

/*
 * @Description: Close stream
 *
 * @return: void
 */
void StreamCOMM::release()
{
    gs_close_gsocket(&(m_addr->gs_sock));
}

/*
 * @Description: Init stream port
 * @param[IN] dbname: database name inherited from StreamProducer.
 * @param[IN] usrname: user name inherited from StreamProducer.
 * @return: void
 */
void StreamCOMM::init(char* dbname, char* usrname)
{
    m_port->sock = NO_SOCKET;
    m_port->libcomm_addrinfo = m_addr;
    m_port->database_name = dbname;
    m_port->user_name = usrname;
}

/*
 * @Description: Allocate net buffer for stream port
 *
 * @return: void
 */
void StreamCOMM::allocNetBuffer()
{
    m_port = (Port*)palloc0(sizeof(Port));

    if (m_sendSide) {
        m_buffer = (StreamBuffer*)palloc0(sizeof(StreamBuffer));
        m_buffer->PqSendBufferSize = STREAM_BUFFER_SIZE;
        m_buffer->PqSendPointer = 0;
        m_buffer->PqSendStart = 0;
        m_buffer->PqCommBusy = false;
    }
}

/*
 * @Description: Set send buffer active
 *
 * @return: void
 */
bool StreamCOMM::setActive()
{
    /*
     * if we use parallel send mode,
     * and the head of address info list is already close,
     * we must continue to send,
     * and gs_broadcast can send to other node in address info list.
     */
    if (m_addr->parallel_send_mode == true) {
        /*
         * if we use parallel send mode,
         * we only send to head node of address info list,
         * and do not care other node in address info list,
         * gs_broadcast can parallel send to other node.
         */
        if (m_addr->addr_list_size == 0)
            return false;
    } else if (m_addr->gs_sock.type == GSOCK_INVALID) {
        return false;
    }

    u_sess->proc_cxt.MyProcPort = m_port;

    t_thrd.libpq_cxt.PqSendBuffer = &m_buffer->PqSendBuffer[0];
    t_thrd.libpq_cxt.PqSendPointer = m_buffer->PqSendPointer;
    t_thrd.libpq_cxt.PqSendBufferSize = m_buffer->PqSendBufferSize;
    t_thrd.libpq_cxt.PqSendStart = m_buffer->PqSendStart;
    t_thrd.libpq_cxt.PqCommBusy = m_buffer->PqCommBusy;

    return true;
}

/*
 * @Description: Is stream closed?
 *
 * @return: true if already closed
 */
bool StreamCOMM::isClosed()
{
    return (m_addr->gs_sock.type == GSOCK_INVALID);
}

/*
 * @Description: Set send buffer inactive
 *
 * @return: void
 */
void StreamCOMM::setInActive()
{
    m_buffer->PqSendPointer = t_thrd.libpq_cxt.PqSendPointer;
    m_buffer->PqSendStart = t_thrd.libpq_cxt.PqSendStart;
    m_buffer->PqCommBusy = t_thrd.libpq_cxt.PqCommBusy;
}

/*
 * @Description: Update connection info
 *
 * @param[IN] connInfo:  connection info
 * @return: void
 */
void StreamCOMM::updateInfo(StreamConnInfo* connInfo)
{
    int nodeNameLen = strlen(connInfo->nodeName);
    errno_t rc = EOK;

    m_addr->gs_sock = connInfo->port.libcomm_layer.gsock;
    rc = strncpy_s(m_nodeName, NAMEDATALEN, connInfo->nodeName, nodeNameLen + 1);
    securec_check(rc, "\0", "\0");
    m_addr->streamKey.producerSmpId = connInfo->producerSmpId;
}
