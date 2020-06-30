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
 * streamTransportTcp.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/distributelayer/streamTransportTcp.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STREAMTRANSPORTTCP_H_
#define STREAMTRANSPORTTCP_H_

class StreamTCP : public StreamTransport {
public:
    StreamTCP(int fd, bool flag);
    ~StreamTCP();

    /* Send a normal message. */
    int send(char msgtype, const char* msg, size_t len);

    /* Flush pending output. */
    void flush();

    /* Close fd. */
    void release();

    /* Init stream port. */
    void init(char* dbname, char* usrname);

    /* Is stream closed? */
    bool isClosed();

    /* Close fd with linger. */
    void releaseSocketWithLinger();

    /* Allocate net buffer for stream port. */
    void allocNetBuffer();

    /* Set send buffer active. */
    bool setActive();

    /* Set send buffer inactive. */
    void setInActive();

    /* Update connection info. */
    void updateInfo(StreamConnInfo* connInfo);

    /* Socket fd. */
    int m_fd;
};

#endif /* STREAMTRANSPORTTCP_H_ */

