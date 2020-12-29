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
 * gds_stream.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/gds_stream.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GDS_STREAM_H
#define GDS_STREAM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "lib/stringinfo.h"
#include "bulkload/utils.h"
#include "ssl/gs_openssl_client.h"

/*
 * Bulkload base class
 */
class BulkLoadStream : public BaseObject {
public:
    /* Bulkload stream close */
    virtual void Close() = 0;

    /* Bulkload stream read */
    virtual int Read() = 0;

    virtual int InternalRead() = 0;

    virtual int ReadMessage(StringInfoData& dst) = 0;

    /* Bulkload stream write */
    virtual int Write(void* src, Size len) = 0;

    virtual void Flush() = 0;

    virtual void VerifyAddr() = 0;
};

class GDSUri : public BaseObject {
public:
    GDSUri()
    {
        m_uri = NULL;
        m_protocol = NULL;
        m_host = NULL;
        m_port = -1;
        m_path = NULL;
    }

    ~GDSUri()
    {
        if (m_uri)
            pfree(m_uri);
        if (m_protocol)
            pfree(m_protocol);
        if (m_host)
            pfree(m_host);
        if (m_path)
            pfree(m_path);
    }

    void Parse(const char* uri);

    const char* ToString()
    {
        return m_uri;
    }

    static void Trim(char* str);

    char* m_uri;
    char* m_protocol;
    char* m_host;
    int m_port;
    char* m_path;
};

class GDSStream : public BulkLoadStream {
public:
    GDSStream();

    virtual ~GDSStream();

    void Initialize(const char* uri);

    void Close();

    int Read();

    int ReadMessage(StringInfoData& dst);

    int Write(void* src, Size len);

    void Flush();

    void VerifyAddr();

    GDSUri* m_uri;
    int m_fd;
    bool m_ssl_enable;
    gs_openssl_cli m_ssl;
    StringInfo m_inBuf;
    StringInfo m_outBuf;

private:
    /* function type pointer defination */
    typedef int (GDSStream::*readfun)(void);
    readfun m_read;

    void PrepareReadBuf(void);
    int InternalRead(void);
    int InternalReadSSL(void);

    void InitSSL(void);
};

extern void SerializeCmd(CmdBase* cmd, StringInfo buf);
extern CmdBase* DeserializeCmd(StringInfo buf);
extern void PackData(StringInfo data, StringInfo dst);

#endif
