/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * comm.h
 *
 * IDENTIFICATION
 * src/include/optimizer/comm.h
 *
 * DESCRIPTION
 * Declaration of externel APIs of Code/src/backend/utils/learn/comm.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef COMM_H
#define COMM_H

#include <curl/curl.h>

#define CURL_BUF_SIZE 4096
/* For ssl certificates */
#define CAHOME "/CA"
#define CA_PATH "/CA/demoCA/cacert.pem"
#define CERT_PATH "/CA/client.crt"
#define KEY_PATH "/CA/client.key"

typedef struct AiEngineConnInfo {
    bool accessible;
    char* host;
    char* port;
    char* url;
    char* request_api;
    char* file_tag;
    char* file_path;
    char* header;
    char* json_string;
} AiEngineConnInfo;

typedef struct AiConn {
    CURL* curl;      // the connection handler
    char* rec_buf;   // the buffer of receive data
    int rec_len;     // the length of the buffer
    int rec_cursor;  // the end of current data in the buffer
} AiConn;

extern AiConn* MakeAiConnHandle();
extern bool InitAiConnHandle(AiConn* connHandle);
extern void SetOptForCurl(AiConn* connHandle, AiEngineConnInfo* conninfo, int timeout);
extern void DestoryAiHandle(AiConn* connHandle);
extern bool TryConnectRemoteServer(AiEngineConnInfo* conninfo, char** buf);
extern void DestroyConnInfo(AiEngineConnInfo* conninfo);
#endif /* COMM_H */
