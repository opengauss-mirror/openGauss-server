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
 * streamTransportCore.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/distributelayer/streamTransportCore.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STREAMTRANSPORTCORE_H_
#define STREAMTRANSPORTCORE_H_

enum StreamTransType { STREAM_COMM, STREAM_MEM };

class StreamTransport : public BaseObject {
public:
    StreamTransport()
    {}

    virtual ~StreamTransport()
    {}

    virtual int send(char msgtype, const char* msg, size_t len)
    {
        Assert(false);
        return 0;
    }

    virtual void flush()
    {
        Assert(false);
    }

    virtual void release()
    {
        Assert(false);
    }

    virtual bool isClosed()
    {
        Assert(false);
        return false;
    }

    virtual void init(char* dbname, char* usrname)
    {
        Assert(false);
    }

    virtual void allocNetBuffer()
    {
        Assert(false);
    }

    virtual bool setActive()
    {
        Assert(false);
        return false;
    }

    virtual void setInActive()
    {
        Assert(false);
    }

    virtual void updateInfo(StreamConnInfo* connInfo)
    {
        Assert(false);
    }

public:
    /* Node name of data node sending data, only for consumer currently. */
    char m_nodeName[NAMEDATALEN];

    /* Oid of data node sending data, only for consumer currently. */
    Oid m_nodeoid;

    /* Transport type. */
    StreamTransType m_type;

    /* Mark if transport is for producer */
    bool m_sendSide;

    /* Communication port. */
    Port* m_port;

    /* Stream buffer. */
    StreamBuffer* m_buffer;
};

#endif /* STREAMTRANSPORTCORE_H_ */
