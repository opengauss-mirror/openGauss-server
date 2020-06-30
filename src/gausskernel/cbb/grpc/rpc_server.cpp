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
 * rpc_server.cpp
 *
 * c++ code
 * Don't include any of PG header file.
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/grpc/rpc_server.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "remote_read.grpc.pb.h"
#include "service/rpc_server.h"
#include "securec.h"
#include "securec_check.h"
#include "utils/elog.h"
#include "storage/remote_adapter.h"

using gauss::CURequest;
using gauss::CUResponse;
using gauss::PageRequest;
using gauss::PageResponse;
using gauss::RemoteRead;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

/* Logic and data behind the server's behavior. */
class RemoteReadServiceImpl final : public RemoteRead::Service {
    Status GetCU(ServerContext* context, const CURequest* request, CUResponse* response) override
    {
        uint32 spcnode = request->spcnode();
        uint32 dbnode = request->dbnode();
        uint32 relnode = request->relnode();
        int32 colid = request->colid();
        uint64 offset = request->offset();
        int32 size = request->size();
        uint64 lsn = request->lsn();

        char* cudata = NULL;
        int ret_code = REMOTE_READ_OK;

        /* check rpc client still waiting for */
        if (context->IsCancelled())
            return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");

        /* init  remote read context */
        RemoteReadContext* remote_read_context = InitRemoteReadContext();

        /* read cu */
        ret_code =
            ::StandbyReadCUforPrimary(spcnode, dbnode, relnode, colid, offset, size, lsn, remote_read_context, &cudata);

        if (ret_code == REMOTE_READ_OK) {
            response->set_cudata(cudata, size);
            response->set_size(size);
        }

        response->set_return_code(ret_code);

        /* release remote read context */
        ReleaseRemoteReadContext(remote_read_context);
        remote_read_context = NULL;

        return Status::OK;
    }

    Status GetPage(ServerContext* context, const PageRequest* request, PageResponse* response) override
    {
        uint32 spcnode = request->spcnode();
        uint32 dbnode = request->dbnode();
        uint32 relnode = request->relnode();
        int16 bucketnode = request->bucketnode();
        int32 forknum = request->forknum();
        uint32 blocknum = request->blocknum();
        uint32 blocksize = request->blocksize();
        uint64 lsn = request->lsn();

        char* pagedata = NULL;
        int ret_code = REMOTE_READ_OK;

        /* check rpc client still waiting for */
        if (context->IsCancelled())
            return Status(StatusCode::CANCELLED, "Deadline exceeded or Client cancelled, abandoning.");

        /* init  remote read context */
        RemoteReadContext* remote_read_context = InitRemoteReadContext();

        /* read page */
        ret_code = ::StandbyReadPageforPrimary(
            spcnode, dbnode, relnode, bucketnode, forknum, blocknum, blocksize, lsn, remote_read_context, &pagedata);

        if (ret_code == REMOTE_READ_OK) {
            response->set_pagedata(pagedata, blocksize);
            response->set_size(blocksize);
        }

        response->set_return_code(ret_code);

        /* release remote read context */
        ReleaseRemoteReadContext(remote_read_context);
        remote_read_context = NULL;

        return Status::OK;
    }
};

typedef struct RPCServerContext {
    RPCServerContext() : cleanup(CleanWorkEnv), threadWrapper(0)
    {
        isPthreadCreate = false;
    }

    ~RPCServerContext()
    {
        if (server) {
            try {
                server->Shutdown();
            } catch(...) {
                ::OutputMsgforRPC(ERROR, "grpc RPCServer shutdown failed.");
            }
        }
    }

    std::function<void()> cleanup;
    ServerBuilder builder;
    RemoteReadServiceImpl service;
    std::unique_ptr<Server> server; /* thread safe in grpc, detail in thread_manager.cc */
    pthread_t threadWrapper;
    bool isPthreadCreate;
} RPCServerContext;

/*
 * @Description: thread for wrapper server->Wait();
 * @IN/OUT arg: thread arg
 */
static void* RPCServerWait(void* arg)
{
    if (arg != NULL) {
        RPCServerContext* serverContext = (RPCServerContext*)arg;

        if (serverContext->server) {
            serverContext->server->Wait();
        }
    }

    return NULL;
}

/*
 * @Description: build a rpc server and start it
 * @IN listen_address: listen address and port
 * @Return: rpc server context
 */
RPCServerContext* BuildAndStartServer(const char* listenAddress)
{
    RPCServerContext* serverContext = new RPCServerContext();
    grpc::SslServerCredentialsOptions sslOpts;
    char gaussHome[MAXPGPATH] = {0};
    char* serverCaBuf = NULL;
    char* serverKeyBuf = NULL;
    char* serverCertBuf = NULL;
    TlsCertPath serverCert;
    int rc = 0;
    int len = 0;
    std::shared_ptr<grpc::ServerCredentials> creds;

    std::string server_address(listenAddress);

    /*
     * In REMOTE_READ_WITH_SSL mode will identify the client, the cluster
     * can not start up if server credentials is abnormal.
     */
    if (IsRemoteReadModeAuth()) {
        if (GetCertEnv("GAUSSHOME", gaussHome, MAXPGPATH)) {
            rc = snprintf_s(
                serverCert.caPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/cacertnew.pem", gaussHome);
            securec_check_ss(rc, "", "");
            rc = snprintf_s(
                serverCert.keyPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/servernew.key", gaussHome);
            securec_check_ss(rc, "", "");
            rc = snprintf_s(
                serverCert.certPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/share/sslcert/grpc/servernew.crt", gaussHome);
            securec_check_ss(rc, "", "");

            if ((serverCaBuf = GetCertStr(serverCert.caPath, &len)) == NULL) {
                ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\". Please check the file.", serverCert.caPath);
            }
            if ((serverKeyBuf = GetCertStr(serverCert.keyPath, &len)) == NULL) {
                ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\". Please check the file.", serverCert.keyPath);
            }
            if ((serverCertBuf = GetCertStr(serverCert.certPath, &len)) == NULL) {
                ::OutputMsgforRPC(WARNING, "Could not read file: \"%s\". Please check the file.", serverCert.certPath);
            }
        }
        if (serverCaBuf != NULL && serverKeyBuf != NULL && serverCertBuf != NULL) {
            grpc::SslServerCredentialsOptions::PemKeyCertPair pkcp = {serverKeyBuf, serverCertBuf};
            sslOpts.pem_root_certs = serverCaBuf;
            sslOpts.pem_key_cert_pairs.push_back(pkcp);
            creds = grpc::SslServerCredentials(sslOpts);
            ::OutputMsgforRPC(LOG, "remote service will start in SSL...");
        } else {
            ::OutputMsgforRPC(ERROR,
                "Can not start remote service without ssl certificate. Check ssl certificate or adjust "
                "remote_read_mode.");
        }
    } else {
        creds = grpc::InsecureServerCredentials();
    }
    /* Listen on the given address without any authentication mechanism. */
    serverContext->builder.AddListeningPort(server_address, creds);

    /*
     * Register "service" as the instance through which we'll communicate with
     * clients. In this case it corresponds to an *synchronous* service.
     */
    serverContext->builder.RegisterService(&serverContext->service);

    /*
     * min polling threads 1, max polling threads 2.
     * if need more threads, still will be create, after use then will be cleanup
     */
    serverContext->builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MIN_POLLERS, 1);
    serverContext->builder.SetSyncServerOption(ServerBuilder::SyncServerOption::MAX_POLLERS, 2);

    /* set work thread cleanup function. */
    serverContext->builder.SetThreadCleanupFunc(serverContext->cleanup);

    /* Finally assemble the server. */
    serverContext->server = serverContext->builder.BuildAndStart();

    serverContext->isPthreadCreate = false;

    /*
     * Wait for the server to shutdown. Note that some other thread must be
     * responsible for shutting down the server for this call to ever return.
     */
    if (serverContext->server) {
        ::OutputMsgforRPC(LOG, "remote service started, listening on %s", server_address.c_str());

        /* using thread wrapper for call server->Wait(); */
        int err = 0;
        err = pthread_create(&serverContext->threadWrapper, NULL, RPCServerWait, (void*)serverContext);
        serverContext->isPthreadCreate = true;
        if (err != 0)
            ::OutputMsgforRPC(LOG, "remote service start failed, thread wrapper failed");
    } else {
        ::OutputMsgforRPC(LOG, "remote service start failed.");
    }

    Free(serverCaBuf);
    Free(serverKeyBuf);
    Free(serverCertBuf);

    return serverContext;
}

/*
 * @Description: shutdown and release rpc server
 * @IN/OUT server_context: rpc server context
 */
void ShutdownAndReleaseServer(RPCServerContext* serverContext)
{
    if (serverContext != NULL) {
        if (serverContext->server) {
            serverContext->server->Shutdown();
        }

        if (serverContext->isPthreadCreate) {
            (void)pthread_join(serverContext->threadWrapper, NULL);
            serverContext->isPthreadCreate = false;
        }

        delete serverContext;
    }
}

/*
 * @Description: force release server, not safe,use  for quick quit .
 * @IN/OUT server_context: rpc server context
 * @See also: ShutdownAndReleaseServer(),  to safe shutdown and release rpc sever
 */
void ForceReleaseServer(RPCServerContext* serverContext)
{
    if (serverContext != NULL) {
        delete serverContext;
    }
}
