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
 *  gs_log_dump.cpp
 *		core interface and implements of log dumping.
 *		split control flow, data processing and output view.
 *          tools for binary logs, now including profile log.
 *
 * IDENTIFICATION
 *        src/bin/gs_log/gs_log_dump.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <assert.h>
#include <sys/time.h>
#include <fcntl.h>

#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgtime.h"
#include "utils/plog.h"
#include "gs_log_dump.h"
#include "prflog_private.h"
#include "securec.h"
#include "bin/elog.h"
#include "port.h"
#define PRINT_DELIM(_fd) fprintf((_fd), ",")
#define PRINT_NEWLN(_fd) fprintf((_fd), "\n")

#ifndef USE_ASSERT_CHECKING
#define ASSERT(condition)
#else
#define ASSERT(condition) assert(condition)
#endif

typedef int (*logdump_f)(gslog_dumper*);
typedef void (*plog_dump_line_f)(FILE* fd, char* buf);

/* see select_default_timezone() in file findtimezone.cpp */
extern const char* select_default_timezone(const char* share_path);

/*
 * <log_timezone> is defined here and will be used by the others.
 * so we use a THR_LOCAL memory to store its value and state.
 * it's set by set_threadlocal_vars() function.
 */
static THR_LOCAL pg_tz g_log_timezone_data;
THR_LOCAL pg_tz* log_timezone = NULL;

THR_LOCAL char* g_log_hostname = NULL;
THR_LOCAL char* g_log_nodename = NULL;

static bool set_timezone(const char* name);
static void timezone_initialize(void);
static void plog_dump_line_md(FILE* fd, char* buf);
static void plog_dump_line_obs(FILE* fd, char* buf);
static void plog_dump_line_hdp(FILE* fd, char* buf);

static const logdump_f logdump_tbl[PROFILE_LOG_VERSION][LOG_TYPE_NUM] = {
    /* version 1 */
    {
        NULL,       /* => LOG_TYPE_ELOG */
        &parse_plog /* => LOG_TYPE_PLOG */
    }
    /* version 2 */
};

static const plog_dump_line_f plog_dump_line_tbl[DS_VALID_NUM + 1] = {
    plog_dump_line_md,  /* => DS_MD */
    plog_dump_line_obs, /* => DS_OBS */
    plog_dump_line_hdp, /* => DS_HADOOP */
    NULL                /* => DS_VALID_NUM */
};

/*
 * @Description: make log_timezone pointer valid.
 * @Return: void
 * @See also:
 */
void set_threadlocal_vars(void)
{
    log_timezone = &g_log_timezone_data;
}

/*
 * @Description: this function set correct directory for timezone data.
 *	   it relies on "GAUSSHOME" and "TZ" env info. so this function
 *	   must be called before any parse action.
 *	   notice, this function will change and set tzdirpath global var.
 * @Return: void
 * @See also:
 */
int set_timezone_dirpath(void)
{
    /*
     * $GAUSSHOME env must not be an empty string.
     * if so, log directory will be under root dir '/' and permition denied.
     */
    char* homedir = getenv(GAUSS_HOME_ENV);
    check_env_value_c(homedir);
    if (homedir == NULL || *homedir == '\0') {
        return GSLOG_ERRNO(GAUSSENV_HOME_MISS);
    }

    char shared_path[MAXPGPATH] = {0};
    int ret = snprintf_s(shared_path, MAXPGPATH, MAXPGPATH - 1, "%s" GAUSS_TZ_SUBDIR, homedir);
    secure_check_ss(ret);

    /* set tzdirpath */
    if (NULL == select_default_timezone(shared_path)) {
        return GSLOG_ERRNO(GAUSSENV_TZ_MISS);
    }
    return 0;
}

gslog_dumper::gslog_dumper()
    : m_infd(NULL),
      m_outfd(NULL),
      m_file_offset(0),
      m_file_size(0),
      m_log_buflen(0),
      m_log_version(0),
      m_logtype(0),
      m_error_no(0),
      m_error_ds(-1),
      m_discard_data(false)
{
    m_in_logfile[0] = '\0';
    m_log_buffer[0] = '\0';
    m_node_name[0] = '\0';
    m_host_name[0] = '\0';
}

gslog_dumper::~gslog_dumper()
{
    close_logfile();

    /* caller must clost it */
    m_outfd = NULL;
}

/*
 * this method must be called before dumping.
 * because dump object may be reused repeatedly after creating,
 * so we should reset and cleanup dumper members.
 */
void gslog_dumper::reset_before_dump(void)
{
    m_file_offset = 0;
    m_file_size = 0;
    m_log_buflen = 0; /* Must reset buffer length */
    m_log_version = 0;
    m_error_ds = -1;
    m_discard_data = false;
}

/*
 * @Description: set input log file to parse.
 *	   this function is reenter.
 *	   this function will check existing of input file, and open
 *	   open it if there is.
 * @Param[IN] logfile: input log file
 * @Return: 0, success; otherwise see m_error_no.
 * @See also:
 */
int gslog_dumper::set_logfile(const char* logfile)
{
    if (NULL == logfile) {
        m_error_no = GSLOG_ERRNO(FILEDATA_INVALID_VER);
        return m_error_no;
    }

    /* check the length of file name */
    size_t len = strlen(logfile);
    if (len >= MAXPGPATH) {
        m_error_no = GSLOG_ERRNO(FILENAME_TOO_LONG);
        return m_error_no;
    }

    int ret = memcpy_s(m_in_logfile, MAXPGPATH - 1, logfile, len);
    secure_check_ret(ret);
    m_in_logfile[len] = '\0';

    close_logfile();

    /* check whether log file exists */
    canonicalize_path(m_in_logfile);
    m_infd = fopen(m_in_logfile, "rb");
    if (NULL == m_infd) {
        m_error_no = errno;
        return m_error_no;
    }

    return 0;
}

void gslog_dumper::close_logfile(void)
{
    if (NULL != m_infd) {
        /* ignore the returned value, because we cannot do
         * anything for this IO error.
         */
        (void)fclose(m_infd);
        m_infd = NULL;
    }
}

/* set the type of log file */
void gslog_dumper::set_logtype(const int t)
{
    ASSERT(LOG_TYPE_ELOG != t);
    m_logtype = t;
}

/*
 * set the fd of output file.
 * it's the caller's responsibility to close this fd, and we only use it.
 */
void gslog_dumper::set_out_fd(FILE* outfd)
{
    ASSERT(outfd);
    m_outfd = outfd;
}

/*
 * @Description: output the detailed error message
 * @Return: void
 * @See also:
 */
void gslog_dumper::print_detailed_errinfo(void)
{
    if (m_error_no > 0) {
        fprintf(stderr, "%s\n", strerror(m_error_no));
    } else if (m_error_no < 0 && m_error_no > GSLOG_ERRNO(GSLOG_MAX_ERROR)) {
        gslog_detailed_errinfo(this);
    } else {
        fprintf(stderr, "Unknown error no\n");
    }
}

/*
 * parse and dump a binary log file into given output file.
 * if error happens, store errorno into m_error_no and return
 * it. caller can get details by calling print_detailed_errinfo().
 */
int gslog_dumper::dump(void)
{
    ASSERT(m_infd);
    ASSERT(m_outfd);

    /* cleanup important info before dumping */
    reset_before_dump();

    if (0 != parse_file_head()) {
        return m_error_no;
    }

    /* set correct timezone */
    timezone_initialize();

    size_t result = 0;

    while (true) {
        /* reset errno before reading data */
        errno = 0;
        /* read a block of data */
        result = fread(m_log_buffer + m_log_buflen, 1, (GSLOG_MAX_BUFSIZE - m_log_buflen), m_infd);

        if ((0 == result) || (0 != errno)) {
            if (feof(m_infd) != 0) {
                /* reach the end of this file */
                ASSERT(GSLOG_MAX_BUFSIZE != m_log_buflen);
                break;
            } else {
                m_error_no = fileno(m_infd);
                return m_error_no;
            }
        }
        m_log_buflen += result;

        /* dump different log type */
        if (logdump_tbl[m_log_version][m_logtype] == NULL ||
            (*logdump_tbl[m_log_version][m_logtype])(this) != 0) {
            return m_error_no;
        }
    }

    if (m_log_buflen > 0) {
        /* the last dumping */
        if ((*logdump_tbl[m_log_version][m_logtype])(this) != 0) {
            return m_error_no;
        }

        /*
         * update this flag whether some data is discarded.
         * (1) discard size is (m_file_size - m_file_offset)
         * (2) discard offset is m_file_offset
         */
        m_discard_data = (m_log_buflen > 0);
    }

    (void)fclose(m_infd);
    m_infd = NULL;
    return 0;
}

/*
 * all binary log must have the same struct of file head.
 * we will read file head data, then parse, and check some
 * data including version, magic data, etc.
 */
int gslog_dumper::parse_file_head(void)
{
    int ret = 0;
    int off = 0;

    if (0 != get_file_size()) {
        return m_error_no;
    }

    /* clean errno before reading/writing IO */
    errno = 0;
    ASSERT(sizeof(LogFileHeader) < GSLOG_MAX_BUFSIZE);
    size_t relsize = fread(m_log_buffer, 1, sizeof(LogFileHeader), m_infd);
    if (relsize != sizeof(LogFileHeader)) {
        m_error_no = errno;
        return m_error_no;
    }

    /* the first magic data */
    unsigned long fst_magic = *(unsigned long*)(m_log_buffer + off);
    if (LOG_MAGICNUM != fst_magic) {
        m_error_no = GSLOG_ERRNO(FILEDATA_BAD_MAGIC1);
        return m_error_no;
    }
    off += sizeof(fst_magic);

    /* version info */
    uint16 version = *(uint16*)(m_log_buffer + off);
    if (version > 0 && version <= PROFILE_LOG_VERSION) {
        m_log_version = version - 1;
    } else {
        /* remember wrong version for error report */
        m_log_version = version;
        m_error_no = GSLOG_ERRNO(FILEDATA_INVALID_VER);
        return m_error_no;
    }
    off += sizeof(version);

    /* host name length */
    uint8 hostname_len = *(uint8*)(m_log_buffer + off);
    off += sizeof(hostname_len);

    if (hostname_len == 0) {
        return -1;
    }

    /* node name length */
    uint8 nodename_len = *(uint8*)(m_log_buffer + off);
    off += sizeof(nodename_len);

    if (nodename_len == 0) {
        return -1;
    }

    /* time zone length */
    uint16 timezone_len = *(uint16*)(m_log_buffer + off);
    off += sizeof(timezone_len);

    if (timezone_len == 0) {
        return -1;
    }

    /* compute the total length of file head */
    size_t hd_total_len = sizeof(LogFileHeader) + hostname_len + nodename_len + timezone_len;
    hd_total_len = MAXALIGN(hd_total_len);
    ASSERT(hd_total_len <= GSLOG_MAX_BUFSIZE);
    if (hd_total_len > GSLOG_MAX_BUFSIZE) {
        return -1;
    }
    /* reload all data of file head */
    rewind(m_infd);
    errno = 0;
    relsize = fread(m_log_buffer, 1, hd_total_len, m_infd);
    if (relsize != hd_total_len) {
        m_error_no = errno;
        return m_error_no;
    }

    /* host name */
    ret = memcpy_s(m_host_name, LOG_MAX_NODENAME_LEN, m_log_buffer + off, hostname_len);
    secure_check_ret(ret);
    ASSERT(m_host_name[hostname_len - 1] == '\0');
    if (m_host_name[hostname_len - 1] != '\0') {
        return -1;
    }
    off += hostname_len;

    g_log_hostname = m_host_name; /* set global host name */

    /* node name */
    ret = memcpy_s(m_node_name, LOG_MAX_NODENAME_LEN, m_log_buffer + off, nodename_len);
    secure_check_ret(ret);
    ASSERT(m_node_name[nodename_len - 1] == '\0');
    if (m_node_name[nodename_len - 1] != '\0') {
        return -1;
    }
    off += nodename_len;

    g_log_nodename = m_node_name; /* set global node name */

    /* time zone */
    ret = memcpy_s(log_timezone->TZname, TZ_STRLEN_MAX + 1, m_log_buffer + off, timezone_len);
    secure_check_ret(ret);
    ASSERT(log_timezone->TZname[timezone_len - 1] == '\0');
    off += timezone_len;

    /* last magic data */
    ASSERT(hd_total_len - off >= (int)sizeof(fst_magic));
    unsigned long lst_magic = *(unsigned long*)(m_log_buffer + hd_total_len - sizeof(fst_magic));
    if (LOG_MAGICNUM != lst_magic) {
        /* remember the offset when error happens */
        m_file_offset = hd_total_len - sizeof(unsigned long);
        m_error_no = GSLOG_ERRNO(FILEDATA_BAD_MAGIC2);
        return m_error_no;
    }

    /* update current file offset */
    m_file_offset = hd_total_len;

    return 0;
}

/*
 * get the total size of input log file.
 * we will check its size which should be equal to
 * or bigger than sizeof(LogFileHeader).
 */
int gslog_dumper::get_file_size(void)
{
    ASSERT(m_infd);

    /* set the total size of this log file */
    errno = 0;
    int ret = fseek(m_infd, 0, SEEK_END);
    if (ret != 0) {
        m_error_no = errno;
        return m_error_no;
    }

    /* get the total file size of this log file */
    m_file_size = ftell(m_infd);

    /* check log file size */
    if ((0 == m_file_size) || (m_file_size < (int)sizeof(LogFileHeader))) {
        m_error_no = GSLOG_ERRNO(FILESIZE_TOO_SMALL);
        return m_error_no;
    }

    /* reset fd position */
    rewind(m_infd);
    return 0;
}

/*
 * parse a block of data for profile log.
 * until remaing data is not a completed record, we will
 * move it to the begin of this buffer, and wait next parsing.
 */
int parse_plog(gslog_dumper* parser)
{
    if (parser->m_log_buflen < (int)sizeof(PLogEntry)) {
        /* incompleted log record */
        return 0;
    }

    int cur = 0;
    do {
        char* start = parser->m_log_buffer + cur;
        PLogEntry* entry = (PLogEntry*)start;
        char ds = entry->head.data_src;

        if (ds < 0 || ds >= DS_VALID_NUM) {
            /* unsupported data source */
            parser->m_file_offset += cur;
            parser->m_error_ds = ds;
            parser->m_error_no = GSLOG_ERRNO(LOGENTRY_INVALID_DS);
            return parser->m_error_no;
        }

        const int msglen = (*plog_msglen_tbl[(int)ds])(entry);
        if (msglen < 0) {
            return 0;
        }
        if (parser->m_log_buflen - cur < msglen) {
            break;
        }

        /* dump a complete record */
        (*plog_dump_line_tbl[(int)ds])(parser->m_outfd, start);

        cur += msglen;
    } while (parser->m_log_buflen - cur > (int)sizeof(PLogEntry));

    if (cur > 0) {
        if (parser->m_log_buflen > cur) {
            int ret = memmove_s(
                parser->m_log_buffer, GSLOG_MAX_BUFSIZE, parser->m_log_buffer + cur, parser->m_log_buflen - cur);
            secure_check_ret(ret);
            parser->m_log_buflen -= cur;
        } else {
            ASSERT(parser->m_log_buflen == cur);
            parser->m_log_buflen = 0;
        }
    }
    /* update current file offset */
    parser->m_file_offset += cur;
    return 0;
}

/* copy from pg_tzset() in pgtz.cpp */
static bool set_timezone(const char* name)
{
    char namecopy[TZ_STRLEN_MAX + 1] = {0};
    char canonname[TZ_STRLEN_MAX + 1] = {0};
    int ret = 0;

    /* do a copy and don't modify name */
    ret = strcpy_s(namecopy, TZ_STRLEN_MAX + 1, name);
    secure_check_ret(ret);

    if (0 == strcmp(namecopy, "GMT")) {
        if (tzparse(namecopy, &(log_timezone->state), 1) != 0) {
            /* this error shouldn't happen, otherwise stop running */
            fprintf(stderr, "Failed to initialize GMT timezone\n");
            exit(1);
        }
    }
    /* tzload() needs the right tzdirpath. see set_timezone_dirpath() */
    else if (tzload(namecopy, canonname, &(log_timezone->state), 1) != 0) {
        if (namecopy[0] == ':' || tzparse(namecopy, &(log_timezone->state), 0) != 0) {
            /* Unknown timezone. Fail our call instead of loading GMT! */
            return false;
        }
    }
    return true;
}

/* see also pg_timezone_initialize() */
static void timezone_initialize(void)
{
    if (!set_timezone((const char*)log_timezone->TZname)) {
        fprintf(stderr, "WARNING: failed to set time zone %s, instead of loading GMT.\n", log_timezone->TZname);

        /* GMT must be set successfully */
        (void)set_timezone("GMT");
    }
}

/* dump meta data of this log line */
static inline void format_plog_line_meta(FILE* fd, PLogEntryMeta* meta)
{
    /* host name */
    fprintf(fd, "%s", g_log_hostname);
    PRINT_DELIM(fd);

    /* time info without year and time zone */
    char time_txt[FORMATTED_TS_LEN] = {0};
    if (format_log_reqtime(time_txt, meta->req_time)) {
        fprintf(fd, "%s", time_txt);
    } else {
        /* CANNOT enter this branch */
        fprintf(stderr,
            "timeval value (tv_sec: %ld, tv_usec: %ld) is out of time range.\n",
            meta->req_time.tv_sec,
            meta->req_time.tv_usec);
        fprintf(fd, " ");
    }
    PRINT_DELIM(fd);

    /* node name */
    fprintf(fd, "%s", g_log_nodename);
    PRINT_DELIM(fd);

    /* thread info */
    fprintf(fd, "%lu", meta->tid);
    PRINT_DELIM(fd);

    /* transaction id */
    fprintf(fd, "%u", meta->gxid);
    PRINT_DELIM(fd);

    /* plog magic num */
    fprintf(fd, "%u", meta->plog_magic);
    PRINT_DELIM(fd);

    /* query id */
    fprintf(fd, "%lu", meta->gqid);
    PRINT_DELIM(fd);
}

/* dump head data of this log line */
static inline void format_plog_line_head(FILE* fd, PLogEntryHead* head)
{
    /* profile head data */
    fprintf(fd, "%s", datasource_type[(int)head->data_src]);
    PRINT_DELIM(fd);
    fprintf(fd, "%s", ds_require_type[(int)head->req_type]);
    PRINT_DELIM(fd);
    fprintf(fd, "%d", head->ret_type);
    PRINT_DELIM(fd);
    /* ignore the total number of items within this line */
}

/* dump item data of this log line */
static inline void format_plog_line_item(FILE* fd, PLogEntryItem* item)
{
    fprintf(fd, "%u", item->sum_count);
    PRINT_DELIM(fd);
    fprintf(fd, "%u", item->sum_size);
    PRINT_DELIM(fd);
    fprintf(fd, "%u", item->sum_usec);
    PRINT_NEWLN(fd);
}

/* dump an entry of MD into fd */
static void plog_dump_line_md(FILE* fd, char* buf)
{
    PLogBasicEntry* md = (PLogBasicEntry*)buf;
    PLogEntryItem* item = &md->basic.item[0];
    const int n = md->basic.head.item_num;

    /* multi items is written into within a single entry.
     * now we dump them into seperated lines in order to
     * batch-insert into databases.
     */
    for (int i = 0; i < n; ++i) {
        format_plog_line_meta(fd, &md->basic.meta);
        format_plog_line_head(fd, &md->basic.head);
        format_plog_line_item(fd, item);
        item++;
    }
}

/* dump an entry of OBS into fd.
 * Now it has the same struct with MD line.
 */
static void plog_dump_line_obs(FILE* fd, char* buf)
{
    plog_dump_line_md(fd, buf);
}

/* dump an entry of Hadoop into fd.
 * Now it has the same struct with MD line.
 */
static void plog_dump_line_hdp(FILE* fd, char* buf)
{
    plog_dump_line_md(fd, buf);
}

/*
 * @Description: output the detailed error message about user defined error number.
 * @Param[IN] error_no: input error no
 * @Return: void
 * @See also:
 */
void gslog_detailed_errinfo(int error_no)
{
    switch (GSLOG_EITEM(error_no)) {
        case GAUSSENV_HOME_MISS:
            fprintf(stderr, "\"%s\" is empty or not correct.\n", GAUSS_HOME_ENV);
            break;
        case GAUSSENV_TZ_MISS:
            fprintf(stderr, "\"TZ\" is empty or not correct.\n");
            break;
        default:
            break;
    }
}

/*
 * @Description: output the detailed error message during log dumping
 * @Param[IN] parser: log dump object
 * @Return: void
 * @See also:
 */
void gslog_detailed_errinfo(gslog_dumper* parser)
{
    switch (GSLOG_EITEM(parser->m_error_no)) {
        case FILENAME_TOO_LONG:
            fprintf(stderr, "File name is too long(>%d).\n", MAXPGPATH);
            break;
        case FILESIZE_TOO_SMALL:
            fprintf(stderr, "File size(%ld) is too small, at least %zu.\n", parser->m_file_size, sizeof(LogFileHeader));
            break;
        case FILEDATA_BAD_MAGIC1:
            fprintf(stderr, "The first magic of file head mismatched, offset 0, size %zu \n", sizeof(unsigned long));
            break;
        case FILEDATA_BAD_MAGIC2:
            fprintf(stderr,
                "The tail magic of file head mismatched, offset %ld, size %zu \n",
                parser->m_file_offset,
                sizeof(unsigned long));
            break;
        case FILEDATA_INVALID_VER:
            fprintf(stderr,
                "The log version(%u) in file head invalid, current version %c \n",
                parser->m_log_version,
                PROFILE_LOG_VERSION);
            break;
        case LOGENTRY_INVALID_DS:
            fprintf(stderr,
                "Invalid data source(%d), range [0, %d), file offset %ld \n",
                parser->m_error_ds,
                DS_VALID_NUM,
                parser->m_file_offset);
            break;
        case GAUSSENV_HOME_MISS:
        case GAUSSENV_TZ_MISS:
        default:
            break;
    }
}
