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
 * comm.cpp
 *
 * IDENTIFICATION
 * src/gausskernel/optimizer/util/comm.cpp
 * DESCRIPTION
 * Use libcurl apis reach remote ai engine, examples:
 * 1) sending files: "curl -X POST -F file=@file_path 'https://ip:port/api'"
 * setting params before calling TryConnectRemoteServer():
 * AiEngineConnInfo *conninfo = (AiEngineConnInfo *)palloc0(sizeof(AiEngineConnInfo));
 * conninfo->host = ip;
 * conninfo->port = port;
 * conninfo->request_api = pstrdup(api); // with '/' like "/predict"
 * conninfo->file_tag = pstrdup("file");
 * conninfo->file_path = pstrdup(file_path);
 * get result from conn->rec_buf before DestoryAiHandle is called
 * 2) sending json: "curl -X POST -d json_string -H 'Content-Type: application/json' 'https://ip:port/api'"
 * setting params before calling TryConnectRemoteServer():
 * AiEngineConnInfo *conninfo = (AiEngineConnInfo *)palloc0(sizeof(AiEngineConnInfo));
 * conninfo->host = ip;
 * conninfo->port = port;
 * conninfo->request_api = pstrdup(api); // with '/' like "/check"
 * conninfo->header = pstrdup("Content-Type: application/json");
 * conninfo->json_string = pstrdup(json_string);
 * get result from conn->rec_buf before DestoryAiHandle is called
 * 3) after calling TryConnectRemoteServer(), remember to delete conninfo by calling DestroyConnInfo()
 *
 * -------------------------------------------------------------------------
 */

#include "cipher.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "optimizer/comm.h"
#include "postgres.h"
#include "regex.h"
#include "securec.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"

static bool ValidIpAddress(const char* ipAddress);
static bool ValidatePortNumber(const char* portNumber);

const int G_ONE_SECOND_TO_MICRO_SECOND = 1000000;

/* *
 * Description: free members of AiConn safely
 * @in: connHandle to free
 */
void DestoryAiHandle(AiConn* connHandle)
{
    if (connHandle != NULL) {
        if (connHandle->curl) {
            curl_easy_cleanup(connHandle->curl);
        }

        pfree_ext(connHandle->rec_buf);

        pfree(connHandle);
    }
}

/* *
 * Description: init members of AiConn safely
 * @in: connHandle to init
 */
bool InitAiConnHandle(AiConn* connHandle)
{
    connHandle->curl = curl_easy_init();
    if (!connHandle->curl) {
        pfree(connHandle);
        return false;
    }
    connHandle->rec_buf = NULL;
    connHandle->rec_len = 0;
    connHandle->rec_cursor = 0;
    return true;
}

/* *
 * Description: make a new AiConn
 * @return: a new AiConn
 */
AiConn* MakeAiConnHandle()
{
    AiConn* connHandle = (AiConn*)palloc0(sizeof(AiConn));
    return connHandle;
}

/* *
 * Description: the callback function to get the data from CURL's buffer
 * @in: buffer: the pointer to the data in CURL's buffer
 * size * nmemb: the length of the data in CURL's buffer
 * userp: the pointer to the GraphConn
 * @out: received data is saved in conn->rec_buf
 * @return: the number of bytes received
 */
static size_t WriteData(const void* buffer, size_t size, size_t nmemb, void* userp)
{
    if (nmemb == 0) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("nmemb should not be zero.")));
    }
    AiConn* conn = (AiConn*)userp;
    if (unlikely(size >= PG_INT32_MAX / nmemb)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid value, size:%lu, mem block num:%lu", size, nmemb)));
    }
    int segsize = size * nmemb;
    errno_t rc;

    /* if the rec_buf is NULL, alloc memory size of segsize */
    if (conn->rec_buf == NULL) {
        conn->rec_buf = (char*)palloc0(segsize + 1);
        conn->rec_len = segsize + 1;
    }

    /* *
     * Check to see if this data exceeds the size of our buffer.
     * If so, enlarge the size of our buffer.
     */
    if (conn->rec_cursor + segsize + 1 > conn->rec_len) {
        conn->rec_len = conn->rec_cursor + segsize + 1;
        conn->rec_buf = (char*)repalloc(conn->rec_buf, conn->rec_len);
    }

    /* Copy the data from the curl buffer into our buffer */
    rc = memcpy_s(conn->rec_buf + conn->rec_cursor, conn->rec_len - conn->rec_cursor, buffer, segsize);
    securec_check(rc, "\0", "\0");

    /* Update the write index */
    conn->rec_cursor += segsize;

    /* Null terminate the buffer */
    conn->rec_buf[conn->rec_cursor] = 0;

    ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("%s", conn->rec_buf)));

    /* Return the number of bytes received, indicating to curl that all is okay */
    return segsize;
}

static bool ReadKeyContentFromFile(const char* cipherkeyfile, const char* randfile,
    CipherkeyFile* cipherFileContent, RandkeyFile* randFileContent)
{
        if (!ReadContentFromFile(cipherkeyfile, cipherFileContent, sizeof(CipherkeyFile)) ||
            !ReadContentFromFile(randfile, randFileContent, sizeof(RandkeyFile))) {
            ereport(DEBUG1, (errmodule(MOD_OPT_AI), 
                             errmsg("Read data source cipher file or random parameter file failed.")));
            return false;
        }
        if (!CipherFileIsValid(cipherFileContent) || !RandFileIsValid(randFileContent)) {
            ereport(DEBUG1, (errmodule(MOD_OPT_AI), 
                             errmsg("Data source cipher file or random parameter file is invalid.")));
            return false;
        }
        return true;
}

/* decrypt the cipher text to plain text */
static bool DecryptInputKey(GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* initrand, GS_UCHAR* initVector,
    GS_UCHAR* decryptVector, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen, GS_UCHAR* decryptKey)
{
    GS_UINT32 retval = 0;
    errno_t rc = EOK;

    if (pucCipherText == NULL) {
        ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("invalid cipher text, please check it!")));
        return false;
    }

    /* get the decrypt key value */
    retval = PKCS5_PBKDF2_HMAC((const char*)initrand,
        RANDOM_LEN,
        initVector,
        RANDOM_LEN,
        ITERATE_TIMES,
        EVP_sha256(),
        RANDOM_LEN,
        decryptKey);
    if (retval != 1) {
        rc = memset_s(decryptKey, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("Generate the derived key failed.")));
        return false;
    }

    /* decrypt the cipher */
    retval = CRYPT_decrypt(NID_aes_128_cbc,
        decryptKey,
        RANDOM_LEN,
        decryptVector,
        RANDOM_LEN,
        pucCipherText,
        ulCLen,
        pucPlainText,
        pulPLen);
    if (retval != 0) {
        rc = memset_s(decryptKey, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        ereport(DEBUG1, (errmodule(MOD_OPT_AI), errmsg("Decrypt cipher text to plain text failed.")));
        return false;
    }

    rc = memset_s(decryptKey, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");
    return true;
}

static GS_UCHAR* DecodeClientKey(StringInfo cahome)
{
    GS_UINT32 plainlen = 0;
    char cipherkeyfile[MAXPGPATH] = {0x00};
    char randfile[MAXPGPATH] = {0x00};
    RandkeyFile randFileContent;
    CipherkeyFile cipherFileContent;
    GS_UCHAR *plainpwd = (GS_UCHAR*)palloc((size_t)(CIPHER_LEN + 1));
    GS_UCHAR decryptKey[RANDOM_LEN] = {0};
    int ret;
    errno_t rc;

    ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", cahome->data, CIPHER_KEY_FILE);
    securec_check_ss_c(ret, "\0", "\0");
    ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", cahome->data, RAN_KEY_FILE);
    securec_check_ss_c(ret, "\0", "\0");
    rc = memset_s(plainpwd, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(rc, "\0", "\0");
    
    /* first,read the cipher file and rand file to buffer */
    if (!ReadKeyContentFromFile(cipherkeyfile,
                                randfile,
                                &cipherFileContent,
                                &randFileContent)) {
        ClearCipherKeyFile(&cipherFileContent);
        ClearRandKeyFile(&randFileContent);
        pfree(plainpwd);
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("Read from key file failed.")));
    }
    if (!DecryptInputKey(cipherFileContent.cipherkey, 
                         CIPHER_LEN, randFileContent.randkey, 
                         cipherFileContent.key_salt,
                         cipherFileContent.vector_salt,
                         plainpwd,
                         &plainlen,
                         decryptKey)) {
        ClearCipherKeyFile(&cipherFileContent);
        ClearRandKeyFile(&randFileContent);
        pfree(plainpwd);
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("decrypt input key failed.")));
        } 
    ClearCipherKeyFile(&cipherFileContent);
    ClearRandKeyFile(&randFileContent);
    return plainpwd;
}

static void CleanCertInfo(StringInfo str)
{
    errno_t rc = memset_s(str->data, str->len, 0, str->len);
    securec_check(rc, "\0", "\0");
    pfree_ext(str->data);
    pfree_ext(str);
}

static void GetCurlClientCerts(AiConn* connHandle)
{
    char* gausshome = getGaussHome();
    GS_UCHAR *plainpwd = NULL;
    StringInfo cahome = makeStringInfo();
    StringInfo caPath = makeStringInfo();
    StringInfo certPath = makeStringInfo();
    StringInfo keyPath = makeStringInfo();

    appendStringInfo(cahome, "%s/%s", gausshome, CAHOME);
    appendStringInfo(caPath, "%s/%s", gausshome, CA_PATH);
    appendStringInfo(certPath, "%s/%s", gausshome, CERT_PATH);
    appendStringInfo(keyPath, "%s/%s", gausshome, KEY_PATH);
    plainpwd = DecodeClientKey(cahome);
    if (plainpwd == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("Decode password for client key failed.")));
    }

    if (access(caPath->data, R_OK) == 0 && access(certPath->data, R_OK) == 0 && access(keyPath->data, R_OK) == 0) {
        curl_easy_setopt(connHandle->curl, CURLOPT_CAINFO, caPath->data);
        curl_easy_setopt(connHandle->curl, CURLOPT_SSLCERT, certPath->data);
        curl_easy_setopt(connHandle->curl, CURLOPT_SSLKEY, keyPath->data);
        curl_easy_setopt(connHandle->curl, CURLOPT_KEYPASSWD, plainpwd);
    } else {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errcode_for_file_access(), 
        errmsg("Read certificate files failed.")));
    }

    CleanCertInfo(caPath);
    CleanCertInfo(certPath);
    CleanCertInfo(keyPath);
    errno_t rc = memset_s(plainpwd, CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
    securec_check(rc, "\0", "\0");
    pfree_ext(plainpwd);
}

/* *
 * Description: set up curl setopts by calling curl_easy_setopt()
 * @in: connHandle: for CURLOPT_WRITEDATA
 * conninfo: holding params to set to curl
 * timeout: for CURLOPT_CONNECTTIMEOUT, maybe useless
 * @out: connHandle->curl with params
 */
void SetOptForCurl(AiConn* connHandle, AiEngineConnInfo* conninfo, int timeout)
{
    struct curl_httppost* post = NULL;
    struct curl_httppost* last = NULL;
    const int prefixStringLen = strlen("https://:/");

    if (conninfo->url == NULL) {
        int urlLen = prefixStringLen;
        urlLen += strlen(conninfo->host) + strlen(conninfo->port) + strlen(conninfo->request_api) + 1;
        conninfo->url = (char*)palloc0(urlLen);
        int rc =
            sprintf_s(conninfo->url, urlLen, "https://%s:%s%s", conninfo->host, conninfo->port, conninfo->request_api);
        securec_check_ss(rc, "\0", "\0");
        conninfo->url[rc] = '\0';
    }
    curl_easy_setopt(connHandle->curl, CURLOPT_URL, conninfo->url);

    if (timeout > 0) {
        curl_easy_setopt(connHandle->curl, CURLOPT_CONNECTTIMEOUT, timeout);
    }

    // default using post
    curl_easy_setopt(connHandle->curl, CURLOPT_POST, 1L);

    if (conninfo->file_tag && conninfo->file_path) {
        curl_formadd(
            &post, &last, CURLFORM_COPYNAME, conninfo->file_tag, CURLFORM_FILE, conninfo->file_path, CURLFORM_END);
        curl_easy_setopt(connHandle->curl, CURLOPT_HTTPPOST, post);
    }

    if (conninfo->header) {
        struct curl_slist* list = NULL;
        list = curl_slist_append(list, conninfo->header);
        curl_easy_setopt(connHandle->curl, CURLOPT_HTTPHEADER, list);
    }

    if (conninfo->json_string) {
        curl_easy_setopt(connHandle->curl, CURLOPT_POSTFIELDSIZE, strlen(conninfo->json_string));
        curl_easy_setopt(connHandle->curl, CURLOPT_POSTFIELDS, conninfo->json_string);
    }

    curl_easy_setopt(connHandle->curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(connHandle->curl, CURLOPT_SSL_VERIFYHOST, 2L);

    GetCurlClientCerts(connHandle);

    curl_easy_setopt(connHandle->curl, CURLOPT_WRITEDATA, connHandle);
    curl_easy_setopt(connHandle->curl, CURLOPT_WRITEFUNCTION, WriteData);
    curl_easy_setopt(connHandle->curl, CURLOPT_NOSIGNAL, 1);
}

static bool CheckConnParams(AiEngineConnInfo* conninfo)
{
    if (conninfo == NULL) {
        return false;
    }

    if (!ValidIpAddress(conninfo->host)) {
        ereport(INFO, (errmsg("ip address %s is not valid", conninfo->host)));
        return false;
    }

    if (!ValidatePortNumber(conninfo->port)) {
        ereport(INFO, (errmsg("port number %s is not valid", conninfo->port)));
        return false;
    }

    return true;
}

/* *
 * Description: try to connect remote server
 * @in: conninfo: holding params to set to curl
 * @return: true if connect successfully
 */
bool TryConnectRemoteServer(AiEngineConnInfo* conninfo, char** buf)
{
    AiConn* connHandle = NULL;
    bool immediateInterruptOKOld = t_thrd.int_cxt.ImmediateInterruptOK;
    // set timeout as 1 second, retry 2 times
    const int timeout = 1;
    const int retryTimes = 2;
    CURLcode ret;
    int loopCount = 0;

    if (!CheckConnParams(conninfo)) {
        return false;
    }
    bool exceptionCaught = false;

    PG_TRY();
    {
        CHECK_FOR_INTERRUPTS();
        t_thrd.int_cxt.ImmediateInterruptOK = true;

        connHandle = MakeAiConnHandle();
        if (!InitAiConnHandle(connHandle)) {
            t_thrd.int_cxt.ImmediateInterruptOK = immediateInterruptOKOld;
            DestoryAiHandle(connHandle);
            return false;
        }
        SetOptForCurl(connHandle, conninfo, timeout);

        do {
            ret = curl_easy_perform(connHandle->curl);
            if (ret == 0) {
                break;
            }
            // sleep 0.5 second
            pg_usleep(G_ONE_SECOND_TO_MICRO_SECOND / 2);
            if (loopCount++ >= retryTimes) {
                ereport(DEBUG1,
                    (errmsg("connect to ai engine failed, "
                            "error code is: %s.",
                        curl_easy_strerror(ret))));
                // jump to pg_catch
                ereport(ERROR, (errmsg("connect to ai engine failed")));
            }
        } while (ret != 0);

        t_thrd.int_cxt.ImmediateInterruptOK = immediateInterruptOKOld;
    }
    PG_CATCH();
    {
        exceptionCaught = true;
        t_thrd.int_cxt.ImmediateInterruptOK = immediateInterruptOKOld;
        DestoryAiHandle(connHandle);
        if (buf != NULL) {
            *buf = NULL;
        }
        FlushErrorState();
    }
    PG_END_TRY();
    if (exceptionCaught) {
        return false;
    }

    if (buf != NULL) {
        *buf = pstrdup(connHandle->rec_buf);
    }
    DestoryAiHandle(connHandle);
    return true;
}

/* *
 * Description: free members of AiEngineConnInfo safely
 * @in: conninfo Connection information container
 */
void DestroyConnInfo(AiEngineConnInfo* conninfo)
{
    if (conninfo == NULL) {
        return;
    }

    pfree_ext(conninfo->host);
    pfree_ext(conninfo->port);
    pfree_ext(conninfo->request_api);
    pfree_ext(conninfo->file_tag);
    pfree_ext(conninfo->file_path);
    pfree_ext(conninfo->header);
    pfree_ext(conninfo->json_string);
    pfree_ext(conninfo->url);

    pfree(conninfo);
}

/* *
 * Description: test remote engine running or not, a 'check' api should be implemented in engine
 * @in: ip address of remote engine
 * @in: port of remote engine
 * @return: success if connected
 */
Datum check_engine_status(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("AiEngine is not available in multipule nodes mode")));
#endif
    char* ipAddress = NULL;
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("IpAddress should not be NULL.")));
    } else {
        ipAddress = (char*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    }
    if (!ValidIpAddress(ipAddress)) {
        ereport(INFO, (errmsg("ip address %s is not valid", ipAddress)));
        pfree_ext(ipAddress);
        PG_RETURN_TEXT_P(cstring_to_text("Failed"));
    }

    char* portNumber = NULL;
    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errmodule(MOD_OPT_AI), errmsg("Port should not be NULL.")));
    } else {
        portNumber = (char*)(text_to_cstring(PG_GETARG_TEXT_P(1)));
    }
    if (!ValidatePortNumber(portNumber)) {
        ereport(INFO, (errmsg("port number %s is not valid", portNumber)));
        pfree_ext(portNumber);
        pfree_ext(ipAddress);
        PG_RETURN_TEXT_P(cstring_to_text("Failed"));
    }

    AiEngineConnInfo* conninfo = (AiEngineConnInfo*)palloc0(sizeof(AiEngineConnInfo));
    // ipAddress and portNumber will be freed in DestroyConnInfo()
    conninfo->host = ipAddress;
    conninfo->port = portNumber;
    conninfo->request_api = pstrdup("/check");
    char** buf = NULL;
    if (TryConnectRemoteServer(conninfo, buf)) {
        DestroyConnInfo(conninfo);
        ereport(INFO, (errmsg("AI engine @%s:%s is running", ipAddress, portNumber)));
        PG_RETURN_TEXT_P(cstring_to_text("Success"));
    } else {
        DestroyConnInfo(conninfo);
        ereport(INFO, (errmsg("AI engine @%s:%s is not running", ipAddress, portNumber)));
        PG_RETURN_TEXT_P(cstring_to_text("Failed"));
    }
}

static bool RegMatch(const char* pattern, const char* src)
{
    // compare src string with regular expression defined by pattern
    regex_t reg;
    regmatch_t pmatch[1];
    const size_t nMatch = 1;
    int cflags = REG_EXTENDED;
    int status;
    regcomp(&reg, pattern, cflags);
    status = regexec(&reg, src, nMatch, pmatch, 0);
    if (status == REG_NOMATCH) {
        return false;
    }
    return true;
}

static bool ValidIpAddress(const char* ipAddress)
{
    const char* ipPattern = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9]).){3}"
                            "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$";
    return RegMatch(ipPattern, ipAddress);
}

static bool ValidatePortNumber(const char* portNumber)
{
    const char* portPattern = "^[1-9]$|(^[1-9][0-9]$)|(^[1-9][0-9][0-9]$)|(^[1-9][0-9][0-9][0-9]$)|"
                              "(^[1-5][0-9][0-9][0-9][0-9]$)|(^[6][0-5][0-5][0-3][0-5]$)";
    return RegMatch(portPattern, portNumber);
}
