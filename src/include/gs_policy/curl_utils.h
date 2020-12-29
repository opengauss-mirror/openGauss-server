/* -------------------------------------------------------------------------
 *
 * CurlUtils.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/gs_policy/CurlUtils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SRC_LIBS_LIBCURL_CURLUTILS_H_
#define SRC_LIBS_LIBCURL_CURLUTILS_H_

#include <string>
#include <iostream>
#include "curl/curl.h"

class CurlUtils
{
public:
    CurlUtils();
    ~CurlUtils();
    void initialize(bool withSSL, const std::string certificate, const std::string user, const std::string password);
    bool http_post_file_request(const std::string url, const std::string fileName, bool connection_testing = false);
private:
    bool m_withSSL;
    std::string m_certificate;
    std::string m_user;
    std::string m_password;
    CURL* m_curlForPost;
};

#endif /* SRC_LIBS_LIBCURL_CURLUTILS_H_ */
