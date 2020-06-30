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
 * prflog_dump.h
 *  core interface and implements of profile log dumping.
 * 
 * 
 * IDENTIFICATION
 *        contrib/log_fdw/prflog_dump.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_BIN_PRFLOG_DUMP_H_
#define SRC_BIN_PRFLOG_DUMP_H_

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/elog.h"
#include "utils/palloc.h"

typedef struct logdata_buf {
    char* m_buf;      /* data buffer */
    int m_buf_cur;    /* 0 <= m_buf_cur < m_buf_len */
    int m_buf_len;    /* m_buf_len <= m_buf_maxlen  */
    int m_buf_maxlen; /* const value */

    static inline void init(logdata_buf* logbuf, const int maxlen)
    {
        logbuf->m_buf = (char*)palloc(maxlen);
        logbuf->m_buf_cur = 0;
        logbuf->m_buf_len = 0;
        logbuf->m_buf_maxlen = maxlen;
    }
    static inline void deinit(logdata_buf* logbuf)
    {
        if (logbuf->m_buf != NULL) {
            pfree_ext(logbuf->m_buf);
        }
    }

    inline void reset_data_buf(void)
    {
        m_buf_len = 0;
        m_buf_cur = 0;
    }

    inline bool has_unhandled_data(void)
    {
        return (m_buf_cur < m_buf_len);
    }

    inline int unhandled_data_size(void)
    {
        return (m_buf_len - m_buf_cur);
    }

    void handle_buffered_data(void)
    {
        if (m_buf_cur > 0) {
            if (unhandled_data_size() > 0) {
                int left = m_buf_len - m_buf_cur;
                int ret = memmove_s(m_buf, m_buf_maxlen, m_buf + m_buf_cur, left);
                securec_check(ret, "\0", "\0");
                m_buf_len = left;
            } else {
                Assert(m_buf_len == m_buf_cur);
                m_buf_len = 0;
            }
            m_buf_cur = 0;
        }
    }
} logdata_buf;

typedef struct prflog_parse {
public:
    void init(void);

    /* set log type */
    void set(const int logtype);

    /*
     * iter all the data for one file.
     */

    /* call iter_begin() after the first read from log file */
    int iter_begin(logdata_buf* datbuf);

    /* fetch next tuple */
    bool iter_next(logdata_buf* datbuf, bool* isnull, Datum* values, int nvalues);

    /* call iter_end() after end of this log file */
    void iter_end(void);

private:
    inline bool have_unhandle_items(void)
    {
        Assert(m_items_cur <= m_items_num);
        return (m_items_cur < m_items_num);
    }

    inline int get_msg_len_with_check(logdata_buf* datbuf);
    inline void advance_items_cursor(logdata_buf* datbuf);
    inline void reset(void);

    /* global names fetched from log file head */
    Datum m_host_name;
    Datum m_node_name;

    /* all binary log maybe have different log version */
    uint16 m_log_version;

    /* log type */
    int m_logtype;

    /* about error information */
    int m_error_no;
    int m_error_ds;

    /*
     * keep the same output with gs_log dump.
     * there are a few items within a record.
     */
    int m_items_num;
    int m_items_cur;

    /* data offset in log file which corresponds to the start of log buffer */
    long m_file_offset;

    /* some data discarded info */
    bool m_discard_data;

} prflog_parse;

#endif /* SRC_BIN_PRFLOG_DUMP_H_ */
