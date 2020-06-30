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
 * HttpRestfulClient.h
 *     Implementation of Exchanging Data with FI KMS Restful API
 * 
 * 
 * IDENTIFICATION
 *        src/include/keymanagement/HttpRestfulClient.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KEY_HTTPRESTFULCLIENT_H
#define SEC_KEY_HTTPRESTFULCLIENT_H

#include <string>
#include <curl/curl.h>
#include <vector>

/*
 * This class is used to complete the network connection and realize the transmission and reception of network data.
 * Support GSS-Negotiate authentication.
 */
class HttpRestfulClient {
public:
    HttpRestfulClient();
    ~HttpRestfulClient();
    std::string post(const char* res, const char* data, const long length);
    std::string get(const char* res);

private:
    std::string host;
    std::string port;
    std::string http_protocol;
    std::vector<std::string> hosts;
    bool is_ReportAlarmAbnormalKMSConnt;
    CURL* curl;
    CURLcode response_code;
    struct curl_slist* headers;

    bool send();
    void update_kms_url();
};

#endif  // SEC_KEY_HTTPRESTFULCLIENT_H
