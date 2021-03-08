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
 * HttpRestfulClient.cpp
 *    encrypt and decrypt functions for MPPDB
 *
 * IDENTIFICATION
 *    src/gausskernel/security/keymanagement/HttpRestfulClient.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <iostream>
#include <sstream>
#include <vector>

#include "keymanagement/HttpRestfulClient.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "alarm/alarm.h"

#define KRB_USER "mppdb:"
#define MAX_PORT_NUM 65536

extern char* g_fi_ca_path;

void ReportAlarmAbnormalKMSConnt()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;

    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalConnToKMS, ALM_AS_Reported, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "",
        "",
        alarmItem,
        ALM_AT_Fault,
        g_instance.attr.attr_common.PGXCNodeName);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeAbnormalKMSConnt()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;

    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalConnToKMS, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

size_t write_net_data(void* ptr, size_t size, size_t nmemb, void* stream)
{
    std::string data((const char*)ptr, (size_t)size * nmemb);

    *((std::stringstream*)stream) << data << std::endl;

    return size * nmemb;
}

HttpRestfulClient::HttpRestfulClient()
{
    host = "";
    port = ":29800";             // FI KMS default value
    http_protocol = "https://";  // FI KMS default value
    headers = NULL;
    curl = curl_easy_init();
    headers = curl_slist_append(headers, "Content-Type: application/json");
    response_code = CURL_LAST;
    is_ReportAlarmAbnormalKMSConnt = false;
}

HttpRestfulClient::~HttpRestfulClient()
{
    if (curl) {
        curl_easy_cleanup(curl);
        curl_global_cleanup();
        curl = NULL;
    }

    if (headers) {
        curl_slist_free_all(headers);
        headers = NULL;
    }
}

/*
 * The format of the GUC parameter transparent_encrypt_kms_url is "kms://https@host0;host1:29800/kms".
 * If the user resets the value of transparent_encrypt_kms_url, the connection address of KMS will be refreshed every
 * This function converts the string to the vector stack and assigns a default host.

 * time the user restarts the instance.

 * The format of the GUC parameter transparent_encrypt_kms_url is "kms://https@host0;host1:29800/kms".
 * This function converts the string to the vector stack and assigns a default host.
 *
 *     kms://https@host0;host1:29800/kms
 *     hosts = [<host0>,<host1>]
 *
 */
void HttpRestfulClient::update_kms_url()
{
    if (g_instance.attr.attr_common.transparent_encrypt_kms_url == NULL ||
        *g_instance.attr.attr_common.transparent_encrypt_kms_url == '\0') {
        std::cout << "keymanagement transparent_encrypt_kms_url is null." << std::endl;
        return;
    }

    std::string tmp = std::string(g_instance.attr.attr_common.transparent_encrypt_kms_url);
    if (tmp.length() <= sizeof("kms://")) {
        std::cout << "keymanagement transparent_encrypt_kms_url is error." << tmp << std::endl;
        return;
    }
    tmp.erase(0, sizeof("kms://") - 1);

    std::string::size_type pos1 = tmp.find("@");
    std::string::size_type pos2 = tmp.find(":");
    if (std::string::npos == pos1 || std::string::npos == pos2 || pos2 <= pos1) {
        std::cout << "keymanagement KMS url Error:" << tmp << std::endl;
        return;
    }

    std::string tmp_hosts = tmp.substr(pos1 + 1, pos2 - pos1 - 1);
    std::string port_str_tmp = tmp.substr(pos2 + 1, tmp.length());
    std::string::size_type pos3 = port_str_tmp.find("/");
    std::string port_str = port_str_tmp.substr(0, pos3);

    /* Check whether the port is between 0 and 65536. */
    int port_unchecked = atoi(port_str.c_str());
    if (port_unchecked > 0 && port_unchecked < MAX_PORT_NUM) {
        /* set port value. */
        port = ":" + port_str;
    }

    /* set http_protocol value. */
    http_protocol = tmp.substr(0, pos1) + "://";
    pos1 = 0;
    pos2 = tmp_hosts.find(";");

    while (std::string::npos != pos2) {
        hosts.push_back(tmp_hosts.substr(pos1, pos2 - pos1));
        pos1 = pos2 + 1;
        pos2 = tmp_hosts.find(";", pos1);
    }

    if (pos1 != tmp_hosts.length()) {
        /* set hosts value. */
        hosts.push_back(tmp_hosts.substr(pos1));
    }
    host = hosts.at(0);
}

std::string HttpRestfulClient::post(const char* res, const char* data, const long length)
{
    if (curl == NULL) {
        return "";
    }
    update_kms_url();
    if (http_protocol.empty()) {
        return "";
    }

    std::stringstream out;
    std::string url = http_protocol + host + port + res;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    /* time out 1s */
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 1);
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, length);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_net_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

    /* SSL VERIFY */
    if (g_fi_ca_path != NULL) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, g_fi_ca_path);
    }
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, false);

    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_GSSNEGOTIATE);
    curl_easy_setopt(curl, CURLOPT_USERPWD, KRB_USER);  // GAUSSDB default value

    if (send()) {
        return std::string(out.str());
    }
    std::cout << "keymanagement conn post failed, Url:" << url << std::endl;
    std::cout << "keymanagement conn post failed, Data:" << std::string(out.str()) << std::endl;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);  // for net infomation

    for (unsigned int i = 0; i < hosts.size(); i++) {
        std::string urlItem = http_protocol + hosts.at(i) + port + res;
        curl_easy_setopt(curl, CURLOPT_URL, urlItem.c_str());
        /* try 3 times */
        if (send() || send() || send()) {
            return std::string(out.str());
        }
    }

    return "";
}

std::string HttpRestfulClient::get(const char* res)
{
    if (curl == NULL) {
        return "";
    }
    update_kms_url();
    if (http_protocol.empty()) {
        return "";
    }
    std::string url = http_protocol + host + port + res;

    std::stringstream out;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 1);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_net_data);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);

    /* SSL VERIFY */
    if (g_fi_ca_path != NULL) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, g_fi_ca_path);
    }
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, false);

    curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_GSSNEGOTIATE);
    curl_easy_setopt(curl, CURLOPT_USERPWD, KRB_USER);  // GAUSSDB default value

    if (send()) {
        return std::string(out.str());
    }

    std::cout << "keymanagement conn get failed, Url:" << url << std::endl;
    std::cout << "keymanagement conn get failed, Data:" << std::string(out.str()) << std::endl;
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);  // for net infomation

    for (unsigned int i = 0; i < hosts.size(); i++) {
        std::string urlItem = http_protocol + hosts.at(i) + port + res;
        curl_easy_setopt(curl, CURLOPT_URL, urlItem.c_str());
        /* try 3 times */
        if (send() || send() || send()) {
            return std::string(out.str());
        }
    }

    return "";
}

bool HttpRestfulClient::send()
{
    // Do not do the transfer - only connect to host
    response_code = curl_easy_perform(curl);
    if (response_code != CURLE_OK) {
        if (!is_ReportAlarmAbnormalKMSConnt) {
            ReportAlarmAbnormalKMSConnt();
            is_ReportAlarmAbnormalKMSConnt = true;
        }
        std::cout << "keymanagement Curl_easy_perform() failed:" << curl_easy_strerror(response_code) << std::endl;
        return false;
    }

    long http_code = 0;
    response_code = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    const int HTTP_REQUEST_SUCCESS = 200;
    const int HTTP_REQUEST_REALIZE = 201;
    if ((response_code != CURLE_OK) || ((http_code != HTTP_REQUEST_SUCCESS) && (http_code != HTTP_REQUEST_REALIZE))) {
        std::cout << "keymanagement Http code:" << http_code << std::endl;
        return false;
    }

    ReportResumeAbnormalKMSConnt();
    is_ReportAlarmAbnormalKMSConnt = false;
    return true;
}

