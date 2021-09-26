/* ---------------------------------------------------------------------------------------
 * 
 * elog.h
 *        openGauss error reporting/logging definitions.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group 
 * 
 * IDENTIFICATION
 *        src/include/cm/elog.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ELOG_H
#define ELOG_H

#include "c.h"
#include "pgtime.h"
#include <time.h>
#include "cm/be_module.h"
#include "cm/cm_errcodes.h"
#include "securec.h"
#include <initializer_list>

#ifdef HAVE_SYSLOG
#ifndef MONITOR_SYSLOG_LIMIT
#define MONITOR_SYSLOG_LIMIT 900
#endif
#endif

#define LOG_DESTION_STDERR 0
#define LOG_DESTION_SYSLOG 1
#define LOG_DESTION_FILE 2
#define LOG_DESTION_DEV_NULL 3

extern int log_destion_choice;
extern volatile int log_min_messages;
extern volatile int maxLogFileSize;
extern volatile int curLogFileNum;
extern volatile bool logInitFlag;
/* unify log style */
extern THR_LOCAL const char* thread_name;

/* Error level codes */
#define DEBUG5                                 \
    10 /* Debugging messages, in categories of \
        * decreasing detail. */

#define DEBUG1 14 /* used by GUC debug_* variables */
#define LOG                                         \
    15 /* Server operational messages; sent only to \
        * server log by default. */

#define WARNING                                      \
    19 /* Warnings.  NOTICE is for expected messages \
        * like implicit sequence creation by SERIAL. \
        * WARNING is for unexpected messages. */
#define ERROR                                                 \
    20           /* user error - abort transaction; return to \
                  * known state */
#define FATAL 22 /* fatal error - abort process */

/*
 * express means you want to free the space alloced
 * by yourself. e.g. express could be "free(str1);free(str2)"
 */

#define PGSIXBIT(ch) (((ch) - '0') & 0x3F)
#define PGUNSIXBIT(val) (((val)&0x3F) + '0')

#define MAKE_SQLSTATE(ch1, ch2, ch3, ch4, ch5) \
    (PGSIXBIT(ch1) + (PGSIXBIT(ch2) << 6) + (PGSIXBIT(ch3) << 12) + (PGSIXBIT(ch4) << 18) + (PGSIXBIT(ch5) << 24))

#define check_errno(errno, express, file, line)                                                               \
    {                                                                                                         \
        if (EOK != errno) {                                                                                   \
            express;                                                                                          \
            write_runlog(ERROR, "%s : %d failed on calling security function, err:%d.\n", file, line, errno); \
            exit(1);                                                                                          \
        }                                                                                                     \
    }

#define securec_check_errno(errno, express) check_errno(errno, express, __FILE__, __LINE__)

/* Only used in sprintf_s or scanf_s cluster function */
#define check_intval(errno, express, file, line)                                                            \
    {                                                                                                       \
        if (errno == -1) {                                                                                  \
            express;                                                                                        \
            write_runlog(ERROR, "check_intval %s : %d failed on calling security function.\n", file, line); \
            exit(1);                                                                                        \
        }                                                                                                   \
    }

#define securec_check_intval(errno, express) check_intval(errno, express, __FILE__, __LINE__)

#define check_sscanf_s_result(rc, expect_rc)                                                                \
    {                                                                                                       \
        if (rc != expect_rc) {                                                                              \
            write_runlog(ERROR, "get value by sscanf_s return error:%d, %s:%d \n", rc, __FILE__, __LINE__); \
        }                                                                                                   \
    }

#define PG_CTL_NAME "gs_ctl"
#define PG_REWIND_NAME "gs_rewind"
#define GTM_CTL_NAME "gtm_ctl"
#define CM_CTL_NAME "cm_ctl"

#define COORDINATE_BIN_NAME "gaussdb"
#define DATANODE_BIN_NAME "gaussdb"
#define GTM_BIN_NAME "gs_gtm"
#define CM_SERVER_BIN_NAME "cm_server"
#define CM_AGENT_NAME "cm_agent"
#define FENCED_MASTER_BIN_NAME "gaussdb"
#define KERBEROS_BIN_NAME "krb5kdc"

#define CM_AUTH_REJECT (0)
#define CM_AUTH_TRUST (1)
#define CM_AUTH_GSS (2)

#define EREPORT_BUF_LEN 1024
#define MAX_EREPORT_BUF_LEN 2048

extern void write_runlog(int elevel, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
extern char *errmsg(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
extern char* errdetail(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
extern char* errcause(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
extern char* erraction(const char* fmt, ...) __attribute__((format(printf, 1, 2)));
extern char* errcode(int sql_state);
extern char* errmodule(ModuleId id);
extern int add_message_string(char* errmsg_tmp, char* errdetail_tmp, char* errmodule_tmp,
    char* errcode_tmp, char* errcause_tmp, char* erraction_tmp, const char* fmt);
template<typename... T>
extern void write_runlog2(int elevel, T... args);

FILE* logfile_open(const char* filename, const char* mode);
void write_runlog(int elevel, const char* fmt, ...);
void add_log_prefix(char* str);

void write_log_file(const char* buffer, int count);

char* errmsg(const char* fmt, ...);
char* errdetail(const char* fmt, ...);
char* errcode(int sql_state);
char* errmodule(ModuleId id);


#define curLogFileMark ("-current.log")

int logfile_init();
void openLogFile(void);
void switchLogFile(void);

void get_log_paramter(const char* confDir);
int get_cm_thread_count(const char* config_file);
void get_build_mode(const char* config_file);
/*trim blank characters on both ends*/
char* trim(char* src);
int is_comment_line(const char* str);
int is_digit_string(char* str);
int SetFdCloseExecFlag(FILE* fp);
int add_message_string(char* errmsg_tmp, char* errdetail_tmp, char* errmodule_tmp, char* errcode_tmp, const char* fmt);
void write_runlog3(int elevel, const char* errmodule_tmp, const char* errcode_tmp, const char* fmt, ...)
    __attribute__((format(printf, 4, 5)));
/*
 * @Description:  get value of paramater from configuration file
 *
 * @in config_file: configuration file path
 * @in key: name of paramater
 * @in defaultValue: default value of parameter
 *
 * @out: value of parameter
 */
int get_int_value_from_config(const char* config_file, const char* key, int defaultValue);
int64 get_int64_value_from_config(const char* config_file, const char* key, int64 defaultValue);
uint32 get_uint32_value_from_config(const char* config_file, const char* key, uint32 defaultValue);
extern int get_authentication_type(const char* config_file);
extern void get_krb_server_keyfile(const char* config_file);

extern const char* prefix_name;

#define LOG_MAX_TIMELEN 80

/* inplace upgrade stuffs */
extern volatile uint32 undocumentedVersion;
#define INPLACE_UPGRADE_PRECOMMIT_VERSION 1

template<typename... T>
void write_runlog2(int elevel, T... args)
{
    char errmsg_tmp[EREPORT_BUF_LEN] = {0};
    char errdetail_tmp[EREPORT_BUF_LEN] = {0};
    char errmodule_tmp[EREPORT_BUF_LEN] = {0};
    char errcode_tmp[EREPORT_BUF_LEN] = {0};
    char errcause_tmp[EREPORT_BUF_LEN] = {0};
    char erraction_tmp[EREPORT_BUF_LEN] = {0};
    char errbuf[MAX_EREPORT_BUF_LEN] = {0};
    int rcs;
    for (auto x : {args...}) {
        rcs = add_message_string(errmsg_tmp, errdetail_tmp, errmodule_tmp,
            errcode_tmp, errcause_tmp, erraction_tmp, x);
    }

    if (errmsg_tmp[0]) {
        if (errbuf[0]) {
            rcs = memcpy_s(errbuf + strlen(errbuf), MAX_EREPORT_BUF_LEN - strlen(errbuf),
                           errmsg_tmp, strlen(errmsg_tmp));
            securec_check_errno(rcs, (void)rcs);
        } else {
            rcs = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, errmsg_tmp);
            securec_check_intval(rcs, );
        }
    }

    if (errdetail_tmp[0]) {
        if (errbuf[0]) {
            rcs = memcpy_s(errbuf + strlen(errbuf), MAX_EREPORT_BUF_LEN - strlen(errbuf),
                           errdetail_tmp, strlen(errdetail_tmp));
            securec_check_errno(rcs, (void)rcs);
        } else {
            rcs = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, errdetail_tmp);
            securec_check_intval(rcs, );
        }
    }

    if (errcause_tmp[0]) {
        if (errbuf[0]) {
            rcs = memcpy_s(errbuf + strlen(errbuf), MAX_EREPORT_BUF_LEN - strlen(errbuf),
                           errcause_tmp, strlen(errcause_tmp));
            securec_check_errno(rcs, (void)rcs);
        } else {
            rcs = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, errcause_tmp);
            securec_check_intval(rcs, );
        }
    }
    if (erraction_tmp[0]) {
        if (errbuf[0]) {
            rcs = memcpy_s(errbuf + strlen(errbuf), MAX_EREPORT_BUF_LEN - strlen(errbuf),
                           erraction_tmp, strlen(erraction_tmp));
            securec_check_errno(rcs, (void)rcs);
        } else {
            rcs = snprintf_s(errbuf, sizeof(errbuf), sizeof(errbuf) - 1, erraction_tmp);
            securec_check_intval(rcs, );
        }
    }
    if (errbuf[0]) {
        write_runlog3(elevel, errmodule_tmp, errcode_tmp, "%s\n", errbuf);
    }
}

#endif /* GTM_ELOG_H */
