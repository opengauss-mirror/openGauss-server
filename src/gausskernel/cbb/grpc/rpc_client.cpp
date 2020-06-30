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
 * rpc_clinet.cpp
 *     c++ code
 *     Don't include any of PG header file.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_lib.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <iostream>
#include <memory>
#include <string>

#include "remote_read.grpc.pb.h"
#include "securec.h"
#include "service/rpc_client.h"
#include "utils/elog.h"

using gauss::CURequest;
using gauss::CUResponse;
using gauss::PageRequest;
using gauss::PageResponse;
using gauss::RemoteRead;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::StatusCode;

/*
 * rpc timeout, T timeout = 30s
 * T timeout = T wait_redo + T wait_read + T wait_network + T other
 * T wait_redo = 5s ,
 * T wait_read = 10s,  read 1GB (max CU size) on 1 SATA disk 100MB/s,
 * T wait_network = 10s, transfer 1GB (max CU size) on GE network 100MB/s,
 * T other = 5s, rpc function call  and other.
 */
const static int RPC_CLIENT_TIMEOUT_SECOND = 30;
/* same as max CU size */
const static int RPC_MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;

class RemoteReadClient {
public:
    explicit RemoteReadClient(std::shared_ptr<Channel> channel) : stub_(RemoteRead::NewStub(channel))
    {}

    virtual ~RemoteReadClient()
    {}

    /*
     * Assembles the client's payload, sends it and presents the response back
     * from the server.
     */
    int GetCU(uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset, int32 size, uint64 lsn,
        char* cu_data)
    {
        /* Data we are sending to the server. */
        CURequest request;
        request.set_spcnode(spcnode);
        request.set_dbnode(dbnode);
        request.set_relnode(relnode);
        request.set_colid(colid);
        request.set_offset(offset);
        request.set_size(size);
        request.set_lsn(lsn);

        /* Container for the data we expect from the server. */
        CUResponse response;

        int ret_code = REMOTE_READ_OK;

        /*
         * Context for the client. It could be used to convey extra information to
         * the server and/or tweak certain RPC behaviors.
         */
        ClientContext context;

        /* set PRC timeout */
        context.set_deadline(gpr_time_from_seconds(RPC_CLIENT_TIMEOUT_SECOND, GPR_TIMESPAN));

        /* The actual RPC. */
        Status status = stub_->GetCU(&context, request, &response);

        /* Act upon its status. */
        if (status.ok()) {
            ret_code = response.return_code();

            /* check if remote read has error */
            if (ret_code != REMOTE_READ_OK)
                return ret_code;

            int resp_size = response.size();

            std::string resp_data = response.cudata();
            int resp_data_size = resp_data.size();
            const char* resp_data_ptr = resp_data.c_str();

            if (resp_size != resp_data_size || size != resp_data_size) {
                ::OutputMsgforRPC(LOG,
                    "%s, request.size = %d, response.size = %d, response.cudata.size = %d",
                    RemoteReadErrMsg(REMOTE_READ_SIZE_ERROR),
                    size,
                    resp_size,
                    resp_data_size);

                return REMOTE_READ_SIZE_ERROR;
            } else {
                /* copy CU data */
                errno_t rc = memcpy_s(cu_data, size, resp_data_ptr, size);

                if (EOK != rc)
                    return REMOTE_READ_SIZE_ERROR;
            }
        } else {
            /* rpc error */
            ret_code = (status.error_code() == StatusCode::DEADLINE_EXCEEDED) ? REMOTE_READ_RPC_TIMEOUT
                                                                              : REMOTE_READ_RPC_ERROR;

            ::OutputMsgforRPC(LOG,
                "%s, error_code = %d, %s",
                RemoteReadErrMsg(ret_code),
                status.error_code(),
                status.error_message().c_str());
        }
        return ret_code;
    }

    int GetPage(uint32 spcnode, uint32 dbnode, uint32 relnode, int16 bucketnode, int32 forknum, uint32 blocknum,
        uint32 blocksize, uint64 lsn, char* page_data)
    {
        /* Data we are sending to the server. */
        PageRequest request;
        request.set_spcnode(spcnode);
        request.set_dbnode(dbnode);
        request.set_relnode(relnode);
        request.set_bucketnode(bucketnode);
        request.set_forknum(forknum);
        request.set_blocknum(blocknum);
        request.set_blocksize(blocksize);
        request.set_lsn(lsn);

        /* Container for the data we expect from the server. */
        PageResponse response;

        int ret_code = REMOTE_READ_OK;

        /*
         * Context for the client. It could be used to convey extra information to
         * the server and/or tweak certain RPC behaviors.
         */
        ClientContext context;

        /* set PRC timeout */
        context.set_deadline(gpr_time_from_seconds(RPC_CLIENT_TIMEOUT_SECOND, GPR_TIMESPAN));

        /* The actual RPC. */
        Status status = stub_->GetPage(&context, request, &response);

        /* Act upon its status. */
        if (status.ok()) {
            ret_code = response.return_code();

            /* check if remote read has error */
            if (ret_code != REMOTE_READ_OK)
                return ret_code;

            int resp_size = response.size();

            std::string resp_data = response.pagedata();
            int resp_data_size = resp_data.size();
            const char* resp_data_ptr = resp_data.c_str();

            if (resp_size != resp_data_size || BLCKSZ != resp_data_size) {
                ::OutputMsgforRPC(LOG,
                    "%s, request.size = %u, response.size = %d, response.cudata.size = %d",
                    RemoteReadErrMsg(REMOTE_READ_SIZE_ERROR),
                    blocksize,
                    resp_size,
                    resp_data_size);

                return REMOTE_READ_SIZE_ERROR;
            } else {
                /* copy page data */
                errno_t rc = memcpy_s(page_data, blocksize, resp_data_ptr, blocksize);

                if (EOK != rc)
                    return REMOTE_READ_SIZE_ERROR;
            }
        } else {
            /* rpc error */
            ret_code = (status.error_code() == StatusCode::DEADLINE_EXCEEDED) ? REMOTE_READ_RPC_TIMEOUT
                                                                              : REMOTE_READ_RPC_ERROR;

            ::OutputMsgforRPC(LOG,
                "%s, error_code = %d, %s",
                RemoteReadErrMsg(ret_code),
                status.error_code(),
                status.error_message().c_str());
        }
        return ret_code;
    }

private:
    std::unique_ptr<RemoteRead::Stub> stub_;
};

std::shared_ptr<grpc::ChannelCredentials> GetSSlCredential()
{
    grpc::SslCredentialsOptions sslOpts;
    std::shared_ptr<grpc::ChannelCredentials> channelCred;
    TlsCertPath clientCert;
    char gaussHome[MAXPGPATH] = {0};
    char* clientCaBuf = NULL;
    char* clientKeyBuf = NULL;
    char* clientCertBuf = NULL;
    int len = 0;
    int rc = 0;

    if (GetCertEnv("GAUSSHOME", gaussHome, MAXPGPATH)) {
        rc = snprintf_s(
            clientCert.caPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/cacertnew.pem", gaussHome);
        securec_check_ss(rc, "", "");
        rc = snprintf_s(
            clientCert.keyPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/clientnew.key", gaussHome);
        securec_check_ss(rc, "", "");
        rc = snprintf_s(
            clientCert.certPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/clientnew.crt", gaussHome);
        securec_check_ss(rc, "", "");

        if ((clientCaBuf = GetCertStr(clientCert.caPath, &len)) == NULL) {
            ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\".", clientCert.caPath);
        }
        if ((clientKeyBuf = GetCertStr(clientCert.keyPath, &len)) == NULL) {
            ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\".", clientCert.keyPath);
        }
        if ((clientCertBuf = GetCertStr(clientCert.certPath, &len)) == NULL) {
            ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\".", clientCert.certPath);
        }
    }

    if (clientCaBuf == NULL || clientKeyBuf == NULL || clientCertBuf == NULL) {
        Free(clientCaBuf);
        Free(clientKeyBuf);
        Free(clientCertBuf);
        ::OutputMsgforRPC(ERROR, "Client Credential is abnormal.");
        return grpc::InsecureChannelCredentials();
    }

    ::OutputMsgforRPC(LOG, "Will connect remote service in SSL...");

    sslOpts = grpc::SslCredentialsOptions();
    sslOpts.pem_root_certs = clientCaBuf;
    sslOpts.pem_private_key = clientKeyBuf;
    sslOpts.pem_cert_chain = clientCertBuf;
    channelCred = grpc::SslCredentials(sslOpts);

    Free(clientCaBuf);
    Free(clientKeyBuf);
    Free(clientCertBuf);

    return channelCred;
}

/*
 * @Description: remote read cu
 * @IN remote_address: remote address
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN colid: column id
 * @IN offset: cu offset
 * @IN size: cu size
 * @IN lsn: current lsn
 * @OUT cu_data: pointer of cu data
 * @Return: remote read error code
 * @See also:
 */
int RemoteGetCU(const char* remoteAddress, uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset,
    int32 size, uint64 lsn, char* cuData)
{
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(RPC_MAX_MESSAGE_SIZE);
    std::shared_ptr<grpc::ChannelCredentials> channelCred;

    if (IsRemoteReadModeAuth()) {
        channelCred = GetSSlCredential();
    } else {
        channelCred = grpc::InsecureChannelCredentials();
    }

    RemoteReadClient remoteread(grpc::CreateCustomChannel(remoteAddress, channelCred, args));
    int errCode = remoteread.GetCU(spcnode, dbnode, relnode, colid, offset, size, lsn, cuData);

    return errCode;
}

/*
 * @Description: remote read page
 * @IN/OUT remote_address:remote address
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN/OUT forknum: forknum
 * @IN/OUT blocknum: block number
 * @IN/OUT blocksize: block size
 * @IN/OUT lsn: current lsn
 * @IN/OUT page_data: pointer of page data
 * @Return: remote read error code
 * @See also:
 */
int RemoteGetPage(const char* remoteAddress, uint32 spcnode, uint32 dbnode, uint32 relnode, int2 bucketnode,
    int32 forknum, uint32 blocknum, uint32 blocksize, uint64 lsn, char* pageData)
{
    std::shared_ptr<grpc::ChannelCredentials> channelCred;

    if (IsRemoteReadModeAuth()) {
        channelCred = GetSSlCredential();
    } else {
        channelCred = grpc::InsecureChannelCredentials();
    }

    RemoteReadClient remoteread(grpc::CreateChannel(remoteAddress, channelCred));
    int errCode = remoteread.GetPage(spcnode, dbnode, relnode, bucketnode, forknum, blocknum, blocksize, lsn, pageData);

    return errCode;
}
