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
 * curl_utils.cpp
 *	  routines for curl connection with remote server
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/gs_policy/curl_utils.cpp

 * -------------------------------------------------------------------------
 */

#include <fstream>
#include <streambuf>
#include <thread>
#include <mutex>
#include <vector>
#include <string>
#include "curl/curl.h"
#include "gs_policy/curl_utils.h"
#include "utils/elog.h"


static std::mutex g_i_mutex;

CurlUtils::CurlUtils() : m_withSSL(false), 
                         m_certificate(""),
                         m_user(""),
                         m_password(""),
                         m_curlForPost(NULL)
{
}

CurlUtils::~CurlUtils()
{
    curl_easy_cleanup(m_curlForPost);
}

void CurlUtils::initialize(bool withSSL, const std::string certificate, const std::string user,
    const std::string password)
{
    m_withSSL = withSSL;
    m_certificate = certificate;
    m_user = user;
    m_password = password;
    m_curlForPost = curl_easy_init();
}

/*
 * Send file to remote web server as rest interface
 */
bool CurlUtils::http_post_file_request(const std::string url, const std::string fileName, bool connection_testing)
{
    ereport(INFO, (errmsg("Url = %s, fileName = %s", url.c_str(), fileName.c_str())));
    std::ifstream t(fileName);
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());

    if (m_curlForPost != NULL) {
        struct curl_slist *slist1 = NULL;
        slist1 = curl_slist_append(slist1, "Content-Type: application/json");
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_URL, url.c_str()); 
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_NOPROGRESS, 1L);
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_POSTFIELDS, str.c_str());
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_POSTFIELDSIZE_LARGE, str.length());
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_USERAGENT, "curl/7.29.0");
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_HTTPHEADER, slist1);
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_MAXREDIRS, 50L);

        /* 
         * as cert from server maybe certificated by self we ignore the verification
         */
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_SSL_VERIFYPEER, 0L);
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_SSL_VERIFYHOST, 0L);
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_CUSTOMREQUEST, "POST");
        (void)curl_easy_setopt(m_curlForPost, CURLOPT_TCP_KEEPALIVE, 1L);

        /* a simply connection test to server, just verify the connection without any data transfer */
        if (connection_testing) {
            (void)curl_easy_setopt(m_curlForPost, CURLOPT_CONNECT_ONLY, 1L);
        }

        /* perform a file transfer */
        CURLcode res = curl_easy_perform(m_curlForPost);
        if (res != CURLE_OK) {
            /*
             * we will not generate error which will make the audit thread restarted
             * but make an error and free related resource to make user deal with the connection issue
             */
            if (connection_testing) {
                ereport(PANIC, 
                    (errmsg("make sure connection to elastic_search_ip_addr, error info: %s\n", 
                        curl_easy_strerror(res))));
            }

            curl_slist_free_all(slist1);
            curl_easy_reset(m_curlForPost);
            ereport(WARNING, (errmsg("Connection issue happended, post file error: %s\n", curl_easy_strerror(res))));
            return false;
        }
        curl_slist_free_all(slist1);
        curl_easy_reset(m_curlForPost);
    }

    return true;
}
