/*
 * Copyright (c) 2019 Huawei Technologies Co.,Ltd.
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
 *---------------------------------------------------------------------------------------
 *
 *  profile_log_private.h
 *	        core interface and implements of log dumping.
 *	        split control flow, data processing and output view.
 *          tools for binary logs, now including profile log.
 *
 * IDENTIFICATION
 *        src/bin/gs_log/profile_log_private.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_BIN_PRFLOG_PRIVATE_H_
#define SRC_BIN_PRFLOG_PRIVATE_H_

#include "pgtime.h"

#ifdef _GSLOG_ITERATOR_MODE_
#include "utils/plog.h"
#include "utils/elog.h"
#endif /* _GSLOG_ITERATOR_MODE_ */

#define FORMATTED_TS_LEN 128
#define GSLOG_ERRNO(_err) (-1 * (_err))
#define GSLOG_EITEM(_err) (-1 * (_err))

enum gslog_errlist {
    GSLOG_ERROR_START = 0,
    FILENAME_TOO_LONG,
    FILESIZE_TOO_SMALL,
    FILEDATA_BAD_MAGIC1,
    FILEDATA_BAD_MAGIC2,
    FILEDATA_INVALID_VER,
    LOGENTRY_INVALID_DS,
    GAUSSENV_HOME_MISS,
    GAUSSENV_TZ_MISS,
    FILEHEAD_TOO_BIG
    /* define error no abover */
    ,
    GSLOG_MAX_ERROR
};

static const char* datasource_type[DS_VALID_NUM + 1] = {
    "md",     /* => DS_MD */
    "obs",    /* => DS_OBS */
    "hadoop", /* => DS_HADOOP */
    "remote", /* => DS_REMOTE_DATANODE */
    ""        /* => DS_VALID_NUM */
};

static const char* ds_require_type[DSRQ_VALID_NUM + 1] = {
    "list",  /* => DSRQ_LIST */
    "read",  /* => DSRQ_READ */
    "write", /* => DSRQ_WRITE */
    "open",  /* => DSRQ_OPEN */
    ""       /* => DSRQ_VALID_NUM */
};

/* compute the entire length of this log message */
static int plog_msglen_md(PLogEntry* entry)
{
    return (offsetof(PLogEntry, item) + sizeof(PLogEntryItem) * entry->head.item_num);
}

/* compute the entire length of this log message.
 * now it has the same struct with MD line.
 */
static int plog_msglen_obs(PLogEntry* entry)
{
    return (offsetof(PLogEntry, item) + sizeof(PLogEntryItem) * entry->head.item_num);
}

/* compute the entire length of this log message.
 * now it has the same struct with MD line.
 */
static int plog_msglen_hdp(PLogEntry* entry)
{
    return (offsetof(PLogEntry, item) + sizeof(PLogEntryItem) * entry->head.item_num);
}

/* compute the entire length of this log message.
 * now it has the same struct with MD line.
 */
static int plog_msglen_remote(PLogEntry* entry)
{
    return (offsetof(PLogEntry, item) + sizeof(PLogEntryItem) * entry->head.item_num);
}

typedef int (*plog_msglen_f)(PLogEntry*);

static const plog_msglen_f plog_msglen_tbl[DS_VALID_NUM + 1] = {
    &plog_msglen_md,     /* => DS_MD */
    &plog_msglen_obs,    /* => DS_OBS */
    &plog_msglen_hdp,    /* => DS_HADOOP */
    &plog_msglen_remote, /* => DS_REMOTE_DATANODE */
    0                    /* => DS_VALID_NUM */
};

/*
 * @Description: format time data according to current time zone.
 * @Param[IN] fd: fd about output log file
 * @Param[IN] tv: time data
 * @Return: void
 * @See also: setup_formatted_log_time()
 */
static bool format_log_reqtime(char* time_txt, const struct timeval& tv)
{
    pg_time_t stamp_time = 0;
    pg_tm* localtm = NULL;
    char msbuf[8];
    int ret = 0;

    stamp_time = (pg_time_t)tv.tv_sec;

    /*
     * NULL point may be returned by pg_localtime().
     * it's decised by inputing stamp_time value.
     * if this value is out of bound or not a valid time range,
     * NULL will be returned. software bug can cause this problem.
     */
    localtm = pg_localtime(&stamp_time, log_timezone);
    if (NULL != localtm) {
        (void)pg_strftime(time_txt,
            FORMATTED_TS_LEN,
            /* leave room for milliseconds... */
            "%Y-%m-%d %H:%M:%S	   %Z",
            localtm);

        /* 'paste' milliseconds into place... */
        ret = sprintf_s(msbuf, sizeof(msbuf), ".%03d", (int)(tv.tv_usec / 1000));
#ifdef _GSLOG_ITERATOR_MODE_
        securec_check_ss(ret, "\0", "");
#else
        secure_check_ss(ret);
#endif /* _GSLOG_ITERATOR_MODE_ */

        /* 19 means the offset to US value */
        ret = strncpy_s(time_txt + 19, (FORMATTED_TS_LEN - 19), msbuf, 4);
#ifdef _GSLOG_ITERATOR_MODE_
        securec_check(ret, "", "");
#else
        secure_check_ret(ret);
#endif /* _GSLOG_ITERATOR_MODE_ */

        return true;
    }
    return false;
}

#endif /* SRC_BIN_PRFLOG_PRIVATE_H_ */
