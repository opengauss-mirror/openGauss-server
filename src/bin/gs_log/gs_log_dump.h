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
 *---------------------------------------------------------------------------------------
 *
 *  gs_log_dump.h
 *	        core interface and implements of log dumping.
 *	        split control flow, data processing and output view.
 *          tools for binary logs, now including profile log.
 *
 * IDENTIFICATION
 *        src/bin/gs_log/gs_log_dump.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_BIN_GSLOG_DUMP_H_
#define SRC_BIN_GSLOG_DUMP_H_

#include <stdio.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgtz.h"
#include "postmaster/syslogger.h"

#define GAUSS_ID "GaussDB Kernel"
#define GAUSS_HOME_ENV "GAUSSHOME"
#define GAUSS_TZ_SUBDIR "/share/postgresql/"

/* valid value for logtype_txt[]/logdump_tbl[] */
#define LOG_TYPE_NUM (LOG_TYPE_MAXVALID - (LOG_TYPE_ELOG + 1))

#define GSLOG_MAX_BUFSIZE (8192 * 2)
#define GSLOG_OUT_SUFFIX ".txt"

/* Only used in sprintf_s or scanf_s cluster function */
#define secure_check_ss(_errno)                                                                                    \
    {                                                                                                              \
        if (-1 == (_errno)) {                                                                                      \
            fprintf(stderr,                                                                                        \
                "%s : %d : The destination buffer or format is a NULL pointer or the invalid parameter handle is " \
                "invoked.",                                                                                        \
                __FILE__,                                                                                          \
                __LINE__);                                                                                         \
            _exit(2);                                                                                              \
        }                                                                                                          \
    }

/* macro for checking secure function returned value */
#define secure_check_ret(_errno)                                                                                       \
    {                                                                                                                  \
        if (EOK != (_errno)) {                                                                                         \
            switch ((_errno)) {                                                                                        \
                case EINVAL:                                                                                           \
                    fprintf(stderr,                                                                                    \
                        "%s : %d : The destination buffer is NULL or not terminated. The second case only occures in " \
                        "function strcat_s/strncat_s.\n",                                                              \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    _exit(2);                                                                                          \
                case EINVAL_AND_RESET:                                                                                 \
                    fprintf(stderr, "%s : %d : The Source Buffer is NULL.\n", __FILE__, __LINE__);                     \
                    _exit(2);                                                                                          \
                case ERANGE:                                                                                           \
                    fprintf(stderr,                                                                                    \
                        "%s : %d : The parameter destMax is equal to zero or larger than the macro : "                 \
                        "SECUREC_STRING_MAX_LEN.\n",                                                                   \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    _exit(2);                                                                                          \
                case ERANGE_AND_RESET:                                                                                 \
                    fprintf(stderr,                                                                                    \
                        "%s : %d : The parameter destMax is too small or parameter count is larger than macro "        \
                        "parameter SECUREC_STRING_MAX_LEN. The second case only occures in functions "                 \
                        "strncat_s/strncpy_s.\n",                                                                      \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    _exit(2);                                                                                          \
                case EOVERLAP_AND_RESET:                                                                               \
                    fprintf(stderr,                                                                                    \
                        "%s : %d : The destination buffer and source buffer are overlapped.\n",                        \
                        __FILE__,                                                                                      \
                        __LINE__);                                                                                     \
                    _exit(2);                                                                                          \
                default:                                                                                               \
                    fprintf(stderr, "%s : %d : Unrecognized return type.\n", __FILE__, __LINE__);                      \
                    _exit(2);                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
    }

class gslog_dumper;

/* internal function declaration */
extern int parse_plog(gslog_dumper*);
extern void gslog_detailed_errinfo(gslog_dumper*);

class gslog_dumper {
public:
    gslog_dumper();
    ~gslog_dumper();

    /* set output fd, may be a file or stdout */
    void set_out_fd(FILE* outfd);

    /* mian body for dumping binary log */
    int dump(void);

    /* if running failed, get detail message */
    void print_detailed_errinfo(void);

    /* friend functions area */
    friend int parse_plog(gslog_dumper*);
    friend void gslog_detailed_errinfo(gslog_dumper*);

    /* set log file name */
    int set_logfile(const char* logfile);
    void close_logfile(void);

    /* set log type excluding elog */
    void set_logtype(const int t);

protected:
private:
    int parse_file_head(void);
    int get_file_size(void);
    void reset_before_dump(void);

    /* input log file name */
    char m_in_logfile[MAXPGPATH];

    /* file handler */
    FILE* m_infd;
    FILE* m_outfd;

    /* data offset in log file which corresponds to the start of log buffer */
    long m_file_offset;

    /* total size of log file, must > m_file_offset */
    long m_file_size;

    /* local buffer */
    char m_log_buffer[GSLOG_MAX_BUFSIZE];
    int m_log_buflen;

    /* global names fetched from log file head */
    char m_host_name[LOG_MAX_NODENAME_LEN];
    char m_node_name[LOG_MAX_NODENAME_LEN];

    /* all binary log maybe have different log version */
    uint16 m_log_version;

    int m_logtype;

    /* about error information */
    int m_error_no;
    int m_error_ds;

    /* some data discarded info */
    bool m_discard_data;
};

extern void set_threadlocal_vars(void);
extern int set_timezone_dirpath(void);
extern void gslog_detailed_errinfo(int error_no);

#endif /* SRC_BIN_GSLOG_DUMP_H_ */
