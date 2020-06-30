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
 * prflog_dump.cpp
 *  core interface and implements of log dumping.
 *  split control flow, data processing and output view.
 * 
 * IDENTIFICATION
 *        contrib/log_fdw/prflog_dump.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "prflog_dump.h"
#include "prflog_private.h"
#include "utils/builtins.h"
#include "utils/plog.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "pgtz.h"

static int deform_line_meta(PLogEntryMeta* meta, bool* isnull, Datum* values, int nvalues)
{
    int idx = 0;
    char time_txt[FORMATTED_TS_LEN] = {0};

    /* host name */
    isnull[idx] = false;
    values[idx++] = t_thrd.contrib_cxt.g_log_hostname;

    /* time info without year and time zone */
    if (format_log_reqtime(time_txt, meta->req_time)) {
        isnull[idx] = false;
        values[idx++] = DirectFunctionCall3(timestamptz_in, CStringGetDatum(time_txt), (Datum)0, (Datum)-1);
    } else {
        isnull[idx] = true;
        values[idx++] = (Datum)0;
    }

    /* node name */
    isnull[idx] = false;
    values[idx++] = t_thrd.contrib_cxt.g_log_nodename;

    /* thread info */
    isnull[idx] = false;
    values[idx++] = Int64GetDatum(meta->tid);

    /* transaction id */
    isnull[idx] = false;
    values[idx++] = UInt32GetDatum(meta->gxid);

    /* query id */
    isnull[idx] = false;
    values[idx++] = Int64GetDatum(meta->gqid);

    Assert(nvalues >= idx);
    return idx;
}

static inline int deform_line_head(PLogEntryHead* head, bool* isnull, Datum* values, int nvalues)
{
    int idx = 0;

    /* profile head data */
    isnull[idx] = false;
    values[idx++] = PointerGetDatum(cstring_to_text(datasource_type[(int)head->data_src]));

    isnull[idx] = false;
    values[idx++] = PointerGetDatum(cstring_to_text(ds_require_type[(int)head->req_type]));

    isnull[idx] = false;
    values[idx++] = Int32GetDatum(head->ret_type);

    /* ignore the total number of items within this line */
    Assert(nvalues >= idx);
    return idx;
}

static inline int deform_line_item(PLogEntryItem* item, bool* isnull, Datum* values, int nvalues)
{
    int idx = 0;

    isnull[idx] = false;
    values[idx++] = UInt32GetDatum(item->sum_count);

    isnull[idx] = false;
    values[idx++] = UInt32GetDatum(item->sum_size);

    isnull[idx] = false;
    values[idx++] = UInt32GetDatum(item->sum_usec);

    Assert(nvalues >= idx);
    return idx;
}

static void deform_record_to_values_md(bool* isnull, Datum* values, int nvalues, int items_cur, void* entry)
{
    PLogBasicEntry* md = (PLogBasicEntry*)entry;

    /* multi items is written into within a single entry.
     * now we dump them into seperated lines in order to
     * batch-insert into databases.
     */
    int idx = deform_line_meta(&md->basic.meta, isnull, values, nvalues);
    idx += deform_line_head(&md->basic.head, isnull + idx, values + idx, (nvalues - idx));
    idx += deform_line_item(&md->basic.item[items_cur], isnull + idx, values + idx, (nvalues - idx));
    Assert(nvalues == idx);
}

void prflog_parse::init(void)
{
    m_host_name = (Datum)0;
    m_node_name = (Datum)0;
    reset();
}

void prflog_parse::reset(void)
{
    m_log_version = 0;
    m_logtype = 0;
    m_error_no = 0;
    m_error_ds = -1;
    m_items_num = 0;
    m_items_cur = 0;
    m_file_offset = 0;
    m_discard_data = false;
}

void prflog_parse::set(const int logtype)
{
    /* set log type */
    Assert(LOG_TYPE_ELOG != logtype);
    m_logtype = logtype;
}

int prflog_parse::iter_begin(logdata_buf* datbuf)
{
    Assert(0 == datbuf->m_buf_cur);
    if ((0 == datbuf->m_buf_len) || (datbuf->m_buf_len < (int)sizeof(LogFileHeader))) {
        m_error_no = GSLOG_ERRNO(FILESIZE_TOO_SMALL);
        return m_error_no;
    }

    int off = 0;

    /* the first magic data */
    Assert(datbuf->m_buf_len >= (int)sizeof(LogFileHeader));
    unsigned long fst_magic = *(unsigned long*)(datbuf->m_buf + off);
    if (LOG_MAGICNUM != fst_magic) {
        m_error_no = GSLOG_ERRNO(FILEDATA_BAD_MAGIC1);
        return m_error_no;
    }
    off += sizeof(fst_magic);

    /* version info */
    uint16 version = *(uint16*)(datbuf->m_buf + off);
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
    uint8 hostname_len = *(uint8*)(datbuf->m_buf + off);
    off += sizeof(hostname_len);

    /* node name length */
    uint8 nodename_len = *(uint8*)(datbuf->m_buf + off);
    off += sizeof(nodename_len);

    /* time zone length */
    uint16 timezone_len = *(uint16*)(datbuf->m_buf + off);
    off += sizeof(timezone_len);

    /* compute the total length of file head */
    size_t hd_total_len = sizeof(LogFileHeader) + hostname_len + nodename_len + timezone_len;
    hd_total_len = MAXALIGN(hd_total_len);
    if (datbuf->m_buf_len < (int)hd_total_len) {
        m_error_no = GSLOG_ERRNO(FILEHEAD_TOO_BIG);
        return m_error_no;
    }

    /* host name */
    Assert('\0' == datbuf->m_buf[off + hostname_len - 1]);
    m_host_name = PointerGetDatum(cstring_to_text(datbuf->m_buf + off));
    off += hostname_len;
    /* set global host name */
    t_thrd.contrib_cxt.g_log_hostname = m_host_name;

    /* node name */
    Assert('\0' == datbuf->m_buf[off + nodename_len - 1]);
    m_node_name = PointerGetDatum(cstring_to_text(datbuf->m_buf + off));
    off += nodename_len;
    /* set global node name */
    t_thrd.contrib_cxt.g_log_nodename = m_node_name;

    /*
     * time zone which is a C string.
     * we don't copy and remember this timezone info, because timezone
     * info about this server backend will be used. this is different
     * from gs_log tool.
     */
    off += timezone_len;
    Assert('\0' == *(datbuf->m_buf + off - 1));

    /* last magic data */
    Assert(hd_total_len - off >= (int)sizeof(fst_magic));
    unsigned long lst_magic = *(unsigned long*)(datbuf->m_buf + hd_total_len - sizeof(fst_magic));
    if (LOG_MAGICNUM != lst_magic) {
        /* remember the offset when error happens */
        m_file_offset = hd_total_len - sizeof(unsigned long);
        m_error_no = GSLOG_ERRNO(FILEDATA_BAD_MAGIC2);
        return m_error_no;
    }

    /* update current file offset */
    m_file_offset = hd_total_len;

    /* update logdata_buf info */
    datbuf->m_buf_cur = hd_total_len;
    return 0;
}

bool prflog_parse::iter_next(logdata_buf* datbuf, bool* isnull, Datum* values, int nvalues)
{
    PLogBasicEntry* md = (PLogBasicEntry*)(datbuf->m_buf + datbuf->m_buf_cur);

    /*
     * deal with 64-bit debug query id in the log files.
     * If we encounter 32-bit debug query id in old-version log files, just skip.
     */
    if (md->basic.meta.plog_magic != PLOG_ENTRY_MAGICNUM) {
        return false;
    }

    if (have_unhandle_items()) {
        /* ok, return next item within this record */
        deform_record_to_values_md(isnull, values, nvalues, m_items_cur, (void*)md);
        advance_items_cursor(datbuf);
        return true;
    }
    Assert(0 == m_items_num && 0 == m_items_cur);

    if (datbuf->unhandled_data_size() >= (int)sizeof(PLogEntry)) {
        const int msglen = get_msg_len_with_check(datbuf);
        if (datbuf->unhandled_data_size() >= msglen) {
            /* there is a completed record */
            m_items_num = md->basic.head.item_num;
            Assert(m_items_num > 0);
            m_items_cur = 0;

            /* ok, return the first item with this completed record */
            deform_record_to_values_md(isnull, values, nvalues, m_items_cur, (void*)md);
            advance_items_cursor(datbuf);
            return true;
        }
    }
    return false; /* continue to read data from log file */
}

void prflog_parse::iter_end(void)
{
    char* ptr = DatumGetPointer(m_host_name);
    if (PointerIsValid(ptr)) {
        pfree(ptr);
        m_host_name = (Datum)0;
    }

    ptr = DatumGetPointer(m_node_name);
    if (ptr != NULL) {
        pfree(ptr);
        m_node_name = (Datum)0;
    }
    reset();
}

inline int prflog_parse::get_msg_len_with_check(logdata_buf* datbuf)
{
    char* start = datbuf->m_buf + datbuf->m_buf_cur;
    PLogEntry* entry = (PLogEntry*)start;
    char ds = entry->head.data_src;
    if (ds >= 0 && ds < DS_VALID_NUM) {
        return (*plog_msglen_tbl[(int)ds])(entry);
    }

    /* unsupported data source */
    m_file_offset += datbuf->m_buf_cur;
    elog(ERROR, "unexcepted data source(%d) in log, offset %ld, ", ds, m_file_offset);
    return 0;
}

inline void prflog_parse::advance_items_cursor(logdata_buf* datbuf)
{
    Assert(m_items_cur < m_items_num);
    if ((++m_items_cur) == m_items_num) {
        /* skip this handled record */
        const int msglen = get_msg_len_with_check(datbuf);
        datbuf->m_buf_cur += msglen;

        /* reset items infor */
        m_items_num = 0;
        m_items_cur = 0;
    }
}
