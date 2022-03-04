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
 * utils.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/bulkload/utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_H_
#define UTILS_H_

#ifndef WIN32
#include "postgres.h"
#include "knl/knl_variable.h"
#endif

#ifndef GDS_SERVER
#include "cjson/cJSON.h"
#else
#include "gds.h"
#endif

typedef enum {
    CMD_TYPE_BEGIN,            // 0db->gds
    CMD_TYPE_END,              // 1db<->gds
    CMD_TYPE_EXPLAIN,          // 2not support
    CMD_TYPE_RESPONSE,         // 3db<-gds
    CMD_TYPE_CANCEL,           // 4not support
    CMD_TYPE_REQ,              // 5db->gds
    CMD_TYPE_ERROR,            // 6db<-gds
    CMD_TYPE_FILE_SWITCH,      // 7db<-gds
    CMD_TYPE_DATA,             // 8db<->gds
    CMD_TYPE_QUERY_RESULT,     // 9db<-gds
    CMD_TYPE_REMOTELOG,        // 10db->gds
    CMD_TYPE_DATA_SEG,         // 11indicate a segment of line, db<-gds
    CMD_TYPE_QUERY_RESULT_V1,  // 12update version of QR, db<-gds
    CMD_TYPE_INVALID
} GDSCmdType;

struct CmdBase {
    char m_type;

    // explicit constructor
    CmdBase() : m_type(0)
    {
    }

    virtual ~CmdBase(){};
};

struct CmdData : public CmdBase {
    char* m_data;
    uint32 m_len;

    CmdData() : m_data(NULL), m_len(0)
    {
    }
};

struct CmdBegin : public CmdBase {
    char* m_url;
    uint64 m_id;
    char m_format;
    char m_nodeType;
    char m_quote;
    char m_escape;
    char* m_eol;
    bool m_header;
    int m_nodeNum;
    char* m_nodeName;
    int m_fixSize;
    char* m_prefix;
    char* m_fileheader;

    CmdBegin()
    {
        m_url = NULL;
        m_id = 0;
        m_format = -1;
        m_nodeType = 'C';
        m_quote = '"';
        m_escape = '"';
        m_eol = NULL;
        m_header = false;
        m_nodeNum = 0;
        m_nodeName = NULL;
        m_fixSize = 0;
        m_prefix = NULL;
        m_fileheader = NULL;
    }
    ~CmdBegin()
    {
        if (m_url)
            cJSON_free(m_url);
        if (m_eol)
            cJSON_free(m_eol);
        if (m_nodeName)
            cJSON_free(m_nodeName);
        if (m_prefix)
            cJSON_free(m_prefix);
        if (m_fileheader)
            cJSON_free(m_fileheader);
    }
};

struct CmdResponse : public CmdBase {
    int m_result;
    char* m_reason;
    CmdResponse() : m_result(0), m_reason(NULL)
    {
    }

    ~CmdResponse()
    {
        if (m_reason)
            cJSON_free(m_reason);
    }
};

struct CmdError : public CmdBase {
    char m_level;
    char* m_detail;

    CmdError() : m_level(0), m_detail(NULL)
    {
    }

    ~CmdError()
    {
        if (m_detail)
            cJSON_free(m_detail);
    }
};

struct CmdRemoteLog : public CmdBase {
    int m_datasize;
    char* m_data;
    char* m_name;
    CmdRemoteLog() : m_datasize(0), m_data(NULL), m_name(NULL)
    {
    }

    ~CmdRemoteLog()
    {
        if (m_name)
            cJSON_free(m_name);
    }
};

struct CmdFileSwitch : public CmdBase {
    char* m_fileName;

    CmdFileSwitch() : m_fileName(NULL)
    {
    }
    ~CmdFileSwitch()
    {
        if (m_fileName)
            cJSON_free(m_fileName);
    }
};

struct CmdQueryResult : public CmdBase {
    int m_result;
    const char* m_version_num;

    // explicit constructor
    CmdQueryResult() : m_result(0)
    {
    }
};

inline void U64ToString(uint64 in, char* out, int len)
{
#if __WORDSIZE == 64
    int rc = snprintf_s(out, len, len - 1, "%lu", in);
    securec_check_ss(rc, "\0", "\0");
#else
    int rc = snprintf_s(out, len, len - 1, "%llu", in);
    securec_check_ss(rc, "\0", "\0");
#endif
}

#define WRITE_JSON_START(_type, obj) do { \
    cJSON *json = cJSON_CreateObject(); \
    char *print = NULL;                 \
    _type *tmp = obj;                   \
    if (json == NULL)                   \
break
#define WRITE_JSON_INT(_field) cJSON_AddNumberToObject(json, #_field, tmp->_field)
#define WRITE_JSON_BOOL(_field) cJSON_AddBoolToObject(json, #_field, tmp->_field)
#define WRITE_JSON_STRING(_field) do { \
    if (tmp->_field)                                         \
        cJSON_AddStringToObject(json, #_field, tmp->_field); \
} while (0)

#define WRITE_JSON_UINT64(_field) do { \
    char tmpstr[128];                               \
    U64ToString(tmp->_field, tmpstr, 128);          \
    cJSON_AddStringToObject(json, #_field, tmpstr); \
} while (0)

#define WRITE_JSON_END()       \
    print = cJSON_Print(json); \
    cJSON_Delete(json);        \
    return print;              \
    }                          \
    while (0)                  \
        ;                      \
    return NULL

#define READ_JSON_START(_type, obj, msg) do { \
    cJSON *json = cJSON_Parse(msg);  \
    _type *tmp = obj;                \
    cJSON *tmpObj = NULL;            \
    if (json == NULL)                \
return -1

#define READ_JSON_INT(_field)                           \
    do {                                                \
        tmpObj = cJSON_GetObjectItem(json, #_field);    \
        if (tmpObj)                                     \
            tmp->_field = tmpObj->valueint;             \
        else                                            \
            return -1;                                  \
    } while (0)

#define READ_JSON_STRING(_field)                                                          \
    do {                                                                                  \
        tmpObj = cJSON_GetObjectItem(json, #_field);                                      \
        if (tmpObj) {                                                                     \
            cJSON *_tmpStringItem = cJSON_CreateString(tmpObj->valuestring);              \
            if (_tmpStringItem != NULL) {                                                 \
                tmp->_field = _tmpStringItem->valuestring;                                \
                cJSON_free(_tmpStringItem); /* shallow free */                            \
            } else {                                                                      \
                tmp->_field = NULL;                                                       \
            }                                                                             \
        } else {                                                                          \
            tmp->_field = NULL;                                                           \
        }                                                                                 \
    } while (0)

#define READ_JSON_BOOL(_field)                          \
    do {                                                \
        tmpObj = cJSON_GetObjectItem(json, #_field);    \
        if (tmpObj)                                     \
            tmp->_field = tmpObj->type == cJSON_True;   \
        else                                            \
            return -1;                                  \
    } while (0)

#define READ_JSON_UINT64(_field)                                                \
    do {                                                                        \
        tmpObj = cJSON_GetObjectItem(json, #_field);                            \
        if (tmpObj) {                                                           \
            char* str = tmpObj->valuestring;                                    \
            if (str) {                                                          \
                char* _end;                                                     \
                tmp->_field = strtoul(str, &_end, 10);                          \
            } else {                                                            \
                return -1;                                                      \
            }                                                                   \
        } else {                                                                \
            return -1;                                                          \
        }                                                                       \
    } while (0)

#define READ_JSON_END() \
    cJSON_Delete(json); \
    }                   \
    while (0)           \
        ;               \
    return 0

typedef enum {
    LEVEL_STATUS = 17,
    LEVEL_LOG,
    LEVEL_WARNING,
    LEVEL_ERROR
} LogLevel;

#define GDSCmdHeaderSize 5 /*length of Cmd header*/

typedef enum {
    FORMAT_UNKNOWN,
    FORMAT_BINARY,
    FORMAT_CSV,
    FORMAT_TEXT,
    FORMAT_FIXED,
    FORMAT_REMOTEWRITE
} FileFormat;

typedef enum {
    NOTIFY_CMD_ADD_SESSION,  /* used to add a new session just for worker thread */
    NOTIFY_CMD_DEL_WORKLOAD, /* used to delete a work load just for main thread */
    NOTIFY_CMD_ERROR_LOG,    /* used to commit all kinds of error log for main thread and worker threads */
    NOTIFY_CMD_EXIT,         /* used to notify threads to exit */
    NOTIFY_CMD_UNKNOWN       /* a  boundary */
} NotifyCmdType;

#endif /* UTILS_H_ */
