/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * security_httpscan.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_httpscan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_httpscan.h"
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "keymgr/comm/security_utils.h"

/*
 * when we parse http response message, We judge field we are parsing based on special characters.
 */
static ResAction action_get(const char *buf, size_t bufsz)
{
    if (buf == NULL || bufsz == 0) {
        return RA_NULL;
    } else if (strcmp(buf, "0") == 0) {
        return RA_ZERO;
    } else if (strcmp(buf, "\n") == 0) {
        return RA_NEWLINE;
    } else if (strcmp(buf, "\r\n") == 0) { /* act of curl with diefferent version is different */
        return RA_NEWLINE;
    } else if (strncmp(buf, "HTTP/", strlen("HTTP/")) == 0) {
        return RA_START;
    } else {
        return RA_TEXT;
    }
}

#define RESBODY_BUF_SZ 4096
/*
 * remain[i] : if copy field in http response
 *      if 1: copy, and store in http->resscan
 *      if 0: do not copy, which means we will lose this field
 */
ResScan *resscan_new(KmErr *errbuf)
{
    ResScan *scan;

    scan = (ResScan *)km_alloc_zero(sizeof(ResScan));
    if (scan == NULL) {
        return NULL;
    }

    scan->err = errbuf;
    scan->stat = RS_INIT;

    return scan;
}

void resscan_free(ResScan *scan)
{
    if (scan == NULL) {
        return;
    }

    km_safe_free(scan->line);
    km_safe_free(scan->header);
    km_safe_free(scan->hdrkey);
    km_safe_free(scan->file);
    km_safe_free(scan->body);

    if (scan->fd != NULL) {
        fclose(scan->fd);
        scan->fd = NULL;
    }

    km_free(scan);
}

void resscan_set_receive(ResScan *scan, char remain[3], const char *hdrkey)
{
    errno_t rc;
    
    if (scan == NULL) {
        return;
    }
    
    scan->remain[RR_LINE] = remain[RR_LINE];
    scan->remain[RR_HEADER] = remain[RR_HEADER];
    scan->remain[RR_BODY] = remain[RR_BODY];

    if (remain[RR_BODY] == 1) {
        if (scan->body == NULL) {
            scan->body = (char *)km_alloc(RESBODY_BUF_SZ);
        }
        if (scan->body == NULL) {
            km_err_msg(scan->err, "faild to malloc memory for http response message.");
            return;
        }
        rc = memset_s(scan->body, RESBODY_BUF_SZ, 0, RESBODY_BUF_SZ);
        km_securec_check(rc, "", "");
    }

    if (scan->hdrkey != NULL) {
        km_free(scan->hdrkey);
    }
    scan->hdrkey = km_strdup(hdrkey);
}

#ifdef ENABLE_KM_DEBUG
void resscan_set_output(ResScan *scan, const char *file)
{
    struct stat filestat;
    int fd;
    
    if (scan == NULL || file == NULL) {
        return;
    }

    scan->file = km_strdup(file);

    if (lstat(file, &filestat) < 0) {
        fd = open(file, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
        if (fd >= 0) {
            close(fd);
        }
    }
    
    scan->fd = fopen(scan->file, "a");
    if (scan->fd == NULL) {
        km_err_msg(scan->err, "failed to open file: '%s'.", file);
    }
}

void resscan_output(ResScan *scan, const char *line)
{
    if (line == NULL) {
        return;
    }
    if (scan->file == NULL) {
        return;
    }
    if (scan->fd != NULL) {
        (void)fputs(line, scan->fd);
    }
}

void resscan_finish(ResScan *scan)
{
    if (scan->fd != NULL) {
        fclose(scan->fd);
        scan->fd = NULL;
    }
}
#endif

#define RES_SCAN_ERR_MSG                                                  \
    "failed to parse http response message. we only acceept format like " \
    "'HTTP/...\\nResponseHeaders\\r\\nResponseBody\\n'. "                 \
    "this error may be caused by a network or proxy error."

/* see 'state machine' */
void resscan_line(ResScan *scan, const char *line, size_t linelen)
{
#define RES_SCAN_ERR(scan)                         \
    do {                                           \
        km_err_msg((scan)->err, RES_SCAN_ERR_MSG); \
        (scan)->stat = RS_ERROR;                   \
    } while (0)

    errno_t rc;
    size_t catlen;

    if (scan->stat == RS_ENDEARLY) {
        return;
    }

    ResAction action = action_get(line, linelen);

#ifdef ENABLE_KM_DEBUG
    resscan_output(scan, line);
#endif

    switch (scan->stat) {
        case RS_INIT:
            if (action == RA_START) {
                scan->stat = RS_LINE;
                /* no break here */
            } else {
                RES_SCAN_ERR(scan);
                break;
            }
        case RS_LINE:
            /* in some strange scenes, there several pieces of http response line, such as: proxy... */
            if (action == RA_START) {
                if (scan->remain[RR_LINE] == 1) {
                    scan->line = km_strdup(line);
                }
                scan->stat = RS_HEADER;
                break;
            }
            RES_SCAN_ERR(scan);
            break;
        case RS_HEADER:
            if (action == RA_TEXT) {
                if (scan->remain[RR_HEADER] != 1) {
                    break;
                }
                if (strncmp(line, scan->hdrkey, strlen(scan->hdrkey)) == 0) {
                    scan->header = km_strdup(line);
                    if (scan->remain[RR_BODY] != 1) {
                        scan->stat = RS_ENDEARLY;
                    }
                }
                break;
            } else if (action == RA_NEWLINE) {
                scan->stat = RS_BODY;
                break;
            }
            RES_SCAN_ERR(scan);
            break;
        case RS_BODY:
            if (action == RA_TEXT) {
                if (scan->remain[RR_BODY] == 1) {
                    catlen = (RESBODY_BUF_SZ - scan->bodylen) - 1 > linelen ? linelen : 0;
                    rc = strncat_s(scan->body, RESBODY_BUF_SZ, line, catlen);
                    km_securec_check(rc, "", "");
                    scan->bodylen += catlen;
                }
                break;
            } else if (action == RA_NEWLINE) {
                scan->stat = RS_END;
                break;
            } else if (action == RA_START) {
                /*
                 * if thereis an proxy-agent, the format of http response message will be like
                 *   HTTP/1.1 xxx xxx
                 *   Proxy-agent: xxx
                 *
                 *   HTTP/1.1 xxx xxx
                 */
                if (scan->remain[RR_BODY] == 1) {
                    rc = memset_s(scan->body, RESBODY_BUF_SZ, 0, RESBODY_BUF_SZ);
                    km_securec_check(rc, "", "");
                    scan->bodylen = 0;
                }
                scan->stat = RS_LINE;
                resscan_line(scan, line, linelen);
                return;
            }
            RES_SCAN_ERR(scan);
            break;
        case RS_END:
            if (action == RA_NEWLINE) {
                /* do nothing */
                break;
            }
            RES_SCAN_ERR(scan);
            break;
        case RS_ENDEARLY:
            /* ignore everything */
            break;
        case RS_ERROR:
            break;
        default:
            scan->stat = RS_ERROR;
            break;
    }
}
