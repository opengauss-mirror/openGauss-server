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
 * -------------------------------------------------------------------------
 *
 * instr_slow_query_log.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/slow_query/instr_slow_query_log.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "postmaster/syslogger.h"
#include "instruments/instr_slow_query_log.h"

static void write_slow_log_chunks(char *data, int len, bool end)
{
    LogPipeProtoChunk p;
    int fd = fileno(stderr);
    int rc;

    Assert(len > 0);

    p.proto.nuls[0] = p.proto.nuls[1] = '\0';
    p.proto.pid = t_thrd.proc_cxt.MyProcPid;
    p.proto.logtype = LOG_TYPE_PLAN_LOG;
    p.proto.magic = PROTO_HEADER_MAGICNUM;

    /* write all but the last chunk */
    while (len > LOGPIPE_MAX_PAYLOAD) {
        p.proto.is_last = 'F';
        p.proto.len = LOGPIPE_MAX_PAYLOAD;
        rc = memcpy_s(p.proto.data, LOGPIPE_MAX_PAYLOAD, data, LOGPIPE_MAX_PAYLOAD);
        securec_check(rc, "\0", "\0");
        rc = write(fd, &p, LOGPIPE_HEADER_SIZE + LOGPIPE_MAX_PAYLOAD);
        (void) rc;
        data += LOGPIPE_MAX_PAYLOAD;
        len -= LOGPIPE_MAX_PAYLOAD;
    }

    /* write the last chunk */
    p.proto.is_last = end ? 'T' : 'F';
    p.proto.len = len;
    rc = memcpy_s(p.proto.data, LOGPIPE_MAX_PAYLOAD, data, len);
    securec_check(rc, "\0", "\0");
    rc = write(fd, &p, LOGPIPE_HEADER_SIZE + len);
    (void) rc;
}

void write_local_slow_log(char *data, int len, bool end)
{
    // send data to syslogger
    if (t_thrd.role == SYSLOGGER || !t_thrd.postmaster_cxt.redirection_done) {
        write_syslogger_file(data, len, LOG_DESTINATION_QUERYLOG);
    } else {
        write_slow_log_chunks(data, len, end);
    }
}

