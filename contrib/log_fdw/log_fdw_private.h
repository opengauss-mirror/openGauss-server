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
 * log_fdw_private.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        contrib/log_fdw/log_fdw_private.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _LOG_FDW_PRIVATE_H_
#define _LOG_FDW_PRIVATE_H_

#include "postgres.h"
#include "knl/knl_variable.h"

#include <string.h>
#include <time.h>

#include "log_fdw.h"
#include "prflog_dump.h"
#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "storage/lock/lock.h"
#include "miscadmin.h"
#include "zlib.h"
#include "unzip.h"
#include "ioapi.h"
#include "pgtime.h"

/* attr position for pg_log attribute, starting from 0 */
#define PGLOG_ATTR_DIRNAME 0
#define PGLOG_ATTR_FILENAME 1
#define PGLOG_ATTR_HOSTNAME 2
#define PGLOG_ATTR_MATCHED 3
#define PGLOG_ATTR_LOGTIME 4
#define PGLOG_ATTR_NODENAME 5
#define PGLOG_ATTR_APPNAME 6
#define PGLOG_ATTR_SESSION_START 7
#define PGLOG_ATTR_SESSION_ID 8
#define PGLOG_ATTR_DBNAME 9
#define PGLOG_ATTR_REMOTE 10
#define PGLOG_ATTR_CMDTAG 11
#define PGLOG_ATTR_USERNAME 12
#define PGLOG_ATTR_VIRTUAL_XID 13
#define PGLOG_ATTR_PID 14
#define PGLOG_ATTR_LINENO 15
#define PGLOG_ATTR_XID 16
#define PGLOG_ATTR_QID 17
#define PGLOG_ATTR_ECODE 18
#define PGLOG_ATTR_MODNAME 19
#define PGLOG_ATTR_LEVEL 20
#define PGLOG_ATTR_MESSAGE 21
#define PGLOG_ATTR_MAX 22

#define PGLOG_ATTRNAME_DIRNAME "dirname"
#define PGLOG_ATTRNAME_FILENAME "filename"
#define PGLOG_ATTRNAME_HOSTNAME "hostname"
#define PGLOG_ATTRNAME_MATCHED "match"
#define PGLOG_ATTRNAME_LOGTIME "logtime"
#define PGLOG_ATTRNAME_NODENAME "nodename"
#define PGLOG_ATTRNAME_APPNAME "app"
#define PGLOG_ATTRNAME_SESSION_START "session_start"
#define PGLOG_ATTRNAME_SESSION_ID "session_id"
#define PGLOG_ATTRNAME_DBNAME "db"
#define PGLOG_ATTRNAME_REMOTE "remote"
#define PGLOG_ATTRNAME_CMDTAG "cmdtag"
#define PGLOG_ATTRNAME_USERNAME "username"
#define PGLOG_ATTRNAME_VIRTUAL_XID "vxid"
#define PGLOG_ATTRNAME_PID "pid"
#define PGLOG_ATTRNAME_LINENO "lineno"
#define PGLOG_ATTRNAME_XID "xid"
#define PGLOG_ATTRNAME_QID "qid"
#define PGLOG_ATTRNAME_ECODE "ecode"
#define PGLOG_ATTRNAME_MODNAME "mod"
#define PGLOG_ATTRNAME_LEVEL "level"
#define PGLOG_ATTRNAME_MESSAGE "msg"

#define PROFILELOG_ATTR_DIRNAME PGLOG_ATTR_DIRNAME
#define PROFILELOG_ATTR_FILENAME PGLOG_ATTR_FILENAME
#define PROFILELOG_ATTR_HOSTNAME PGLOG_ATTR_HOSTNAME
#define PROFILELOG_ATTR_LOGTIME 3
#define PROFILELOG_ATTR_NODENAME 4
#define PROFILELOG_ATTR_THREAD 5
#define PROFILELOG_ATTR_XID 6
#define PROFILELOG_ATTR_QID 7
#define PROFILELOG_ATTR_REQSRC 8
#define PROFILELOG_ATTR_REQTYPE 9
#define PROFILELOG_ATTR_REQOK 10
#define PROFILELOG_ATTR_REQCOUNT 11
#define PROFILELOG_ATTR_REQSIZE 12
#define PROFILELOG_ATTR_REQUSEC 13
#define PROFILELOG_ATTR_MAX 14

#define LOGFDW_MAX_INT32 (2147483600)
#define LOGFDW_LOCAL_IP_LEN 20
#define LOGFDW_HOSTNAME_MAXLEN 256

#define LOGFDW_TUPLE_CACHE_SIZE (int)(128)

/* dirname and filename before */
#define PRFLOG_SYSATTR_NUM 2

/* max size of one log line */
#define MAX_LINE_LEN 8192

enum FdwLogType { FLT_PG_LOG = 0, FLT_GS_PROFILE, FLT_UNKNOWN };

/* reading buffer size for log data */
#define LOGFDW_BUFFER_SZ (1024 * 1024)

/* info tag transformed from CN to datanodes */
#define logft_opt_masters_info "masters_info"

/* options for log foreign tables */
#define OPTION_LOGTYPE "logtype"
#define OPTION_MASTER_ONLY "master_only"
#define OPTION_LATEST_FILES "latest_files"

typedef struct log_file_info {
    char* name;            /* log file name */
    time_t tm_create;      /* create time */
    time_t tm_last_modify; /* last modify time */
} log_file_info;

typedef struct {
    NameData ip; /* ip adress */
} hkey_ip;

typedef enum unreachable_host_type {
    UHT_OK,           /* reachable */
    UHT_DEPLOY_LIMIT, /* because deploy limitation */
    UHT_NO_MASTER,    /* because master switch-over */
    UHT_UNKNOWN
} unr_host_type;

typedef struct {
    hkey_ip host_ip;
    List* nodenames;
    unr_host_type reachable; /* whether this host is accessed by master datanode ? */
    bool has_datanode;       /* is there at least one datanode on this host ? */
} hentry_reachable_host;

typedef bool (*dirname_match)(const char*);
typedef dirname_match fname_match;
typedef int (*cmp_func)(const void*, const void*);

struct ld_scanner {
    MemoryContext m_memcnxt;
    char* top_dir;           /* len < 1024, top directory */
    List* dir_list;          /* total directories to scan */
    List* dir_list_copy;     /* for rescan support */
    char* cur_dir;           /* len < 1024, sub-directory under top_dir */
    log_file_info* log_file; /* len < 1024, log file under cur_dir */
    List* log_file_list;     /* list of log files */
    dirname_match dir_cb;    /* callback for dir name */
    fname_match fname_cb;    /* callback for file name */
    int m_latest_files_num;  /* apply for each directory */
};
typedef struct ld_scanner* logdir_scanner;

typedef struct {
    hkey_ip host_ip;
    List* nodenames;
} hvalue_nodes;

typedef void* (*logfile_open)(const char* logfile);
typedef int (*logfile_read)(void* reader, char* buf, int bufsize);
typedef void (*logfile_close)(void* reader);

typedef struct pglogPlanState {
    Datum regrex;

    /*
     * in the order of column definations of log foreign table
     * see file Code/contrib/log_fdw/log_fdw--1.0.sql
     */
    FmgrInfo allattr_fmgrinfo[PGLOG_ATTR_MAX];
    int32 allattr_typmod[PGLOG_ATTR_MAX];
    Oid allattr_typioparam[PGLOG_ATTR_MAX];

    /* attribute map from real log data to log relation */
    int attmap[PGLOG_ATTR_MAX];

    /*
     * the real number of attributes in real log data
     * now it's determinated by GUC Log_line_prefix.
     */
    int nattrs;
    Datum real_values[PGLOG_ATTR_MAX];
    bool real_isnull[PGLOG_ATTR_MAX];
} pglogPlanState;

typedef struct gsprofilePlanState {
    prflog_parse* parser; /* parse profile log */
} gsprofilePlanState;

typedef struct logFdwPlanState {
    /*
     * private memory manager.
     */

    /* all the members should be under this private memory context. */
    MemoryContext m_memcnxt;

    /* per tuple memory context */
    MemoryContext m_pertup_memcnxt;

    /* functions for different log type */
    GetForeignRelSize_function get_ftrel_size;
    GetForeignPaths_function get_foreign_paths;
    GetForeignPlan_function get_foreign_plan;
    BeginForeignScan_function begin_foreign_scan;
    IterateForeignScan_function iterate_foreign_scan;
    ReScanForeignScan_function rescan_foreign_scan;
    EndForeignScan_function end_foreign_scan;
    ExplainForeignScan_function explain_foreign_scan;

    /* common data type */

    /* datum to form a tuple */
    bool* m_isnull;
    Datum* m_values;

    union {
        pglogPlanState pg_log;
        gsprofilePlanState profile_log;
    } log_state;

    /* members for log dir */
    text* dname; /* log directory name */
    logdir_scanner dir_scan;

    /* members for log file */
    log_file_info* logfile_info; /* file name with absolute path */
    text* filename;              /* only file name */
    void* m_vfd;
    logfile_open m_open;
    logfile_read m_read;
    logfile_close m_close;

    /* file data buffer */
    logdata_buf m_log_buf;
    int64 m_lineno_of_file; /* line no in each file */

    /* only care N latest files, not the total set */
    int m_latest_files;

    /* needn't to read data from log file */
    bool m_need_read_logdata;

    /* just care log data of master DN */
    bool m_master_only;

    /* whether to check host name/dir name/log name/log time and do filter */
    bool m_need_check_hostname;
    bool m_need_check_dirname;
    bool m_need_check_logname;
    bool m_need_check_logtime;

    /* EXPLAIN analyze/performance */
    bool m_refuted_by_hostname; /* whether to ignore my host */
    bool m_need_timing;         /* need timing */
    uint32 m_total_dirname_num; /* dirname info */
    uint32 m_refuted_dirname_num;
    uint32 m_total_logfile_num; /* log file info */
    uint32 m_refuted_logfile_num_by_name;
    uint32 m_refuted_logfile_num_by_time;
    uint32 m_incompleted_files;
    time_t m_last_modify_tm_maxval; /* always set now */
    int m_plan_node_id;             /* plan node id */

    Var m_tmp_hostname_var;
    Var m_tmp_dirname_var;
    Var m_tmp_logname_var;
    Var m_tmp_logtime_var;
    Datum m_tmp_hostname_text;

    /* tuples cache */
    HeapTuple m_tuple_buf[LOGFDW_TUPLE_CACHE_SIZE];
    int m_tuple_num;
    int m_tuple_cur;

    inline char* get_next_line(void)
    {
        Assert(m_log_buf.has_unhandled_data());
        char* newline =
            (char*)memchr((m_log_buf.m_buf + m_log_buf.m_buf_cur), '\n', (m_log_buf.m_buf_len - m_log_buf.m_buf_cur));
        if (NULL != newline) {
            *newline = '\0';                                      /* c string */
            char* line = (m_log_buf.m_buf + m_log_buf.m_buf_cur); /* this line */
            m_log_buf.m_buf_cur += (newline - line + 1);          /* move to next line position */
            return line;
        }
        return NULL;
    }

    inline bool continue_to_load_logdata(void)
    {
        Assert(m_log_buf.m_buf_len < m_log_buf.m_buf_maxlen);
        int read_size =
            m_read(m_vfd, (m_log_buf.m_buf + m_log_buf.m_buf_len), m_log_buf.m_buf_maxlen - m_log_buf.m_buf_len);
        if (read_size > 0) {
            m_log_buf.m_buf_len += read_size;
            return true;
        }
        return false; /* end of file */
    }
} logFdwPlanState;

struct logFdwOption {
    const char* optname;
    Oid optcontext; /* Oid of catalog in which option may appear */
};

#endif /* _LOG_FDW_PRIVATE_H_ */
