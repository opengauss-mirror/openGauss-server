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
 * rpc_server.h
 *     Don't include any of RPC or PG header file
 *     Just using simple C API interface
 * 
 * IDENTIFICATION
 *        src/include/service/rpc_server.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef RPC_SERVER_H
#define RPC_SERVER_H

typedef struct RPCServerContext RPCServerContext;

extern RPCServerContext* BuildAndStartServer(const char* listen_address);

extern void ShutdownAndReleaseServer(RPCServerContext* server_context);

extern void ForceReleaseServer(RPCServerContext* server_context);

#endif /* RPC_SERVER_H */
