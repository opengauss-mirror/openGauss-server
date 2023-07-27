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
 * security_httpscan.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_httpscan.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KM_HTTPSCAN_H_
#define __KM_HTTPSCAN_H_

#include <stdlib.h>
#include <stdio.h>
#include "keymgr/comm/security_error.h"

typedef enum {
    RS_INIT = 0,
    RS_END,
    RS_ENDEARLY,
    RS_ERROR,

    RS_LINE,
    RS_HEADER,
    RS_BODY,
} ResState;

/*
 * state machine:
 *      RS_INIT
 *        | RA_START
 *      RS_LINE  <-- RA_START
 *        | RA_TEXT
 *      RS_HEADER <-- RA_TEXT
 *        | RA_NEWLINE
 *      RS_BODY <-- RA_TEXT
 *        | RA_ZERO/RA_NEWLINE
 *      RS_END
 *
 * see ScanLine.
 */

typedef enum {
    RA_START, /* start with ‘HTTP/’ */
    RA_NEWLINE, /* /n */
    RA_ZERO, /* '0' */
    RA_TEXT, /* else */
    RA_NULL,
} ResAction;

typedef enum {
    RR_LINE = 0,
    RR_HEADER,
    RR_BODY,
} ResRemain;

typedef struct {
    ResState stat;
    char remain[3]; /* ResponseLine, ResponseHeader, ResponseBody */

    char *line;
    char *header; /* for now, we just need 1 header, so we don't store all headers in a list. */
    char *body;
    size_t bodylen;

    char *hdrkey; /* the header we need */

    char *file; /* if != NULL, will echo all http response message into the file */
    FILE *fd;

    KmErr *err;
} ResScan;

ResScan *resscan_new(KmErr *errbuf);
void resscan_free(ResScan *scan);
void resscan_set_receive(ResScan *scan, char remain[3], const char *hdrkey);
void resscan_line(ResScan *scan, const char *line, size_t linelen);
#ifdef ENABLE_KM_DEBUG
void resscan_set_output(ResScan *scan, const char *file);
void resscan_output(ResScan *scan, const char *line);
void resscan_finish(ResScan *scan);
#endif
#endif