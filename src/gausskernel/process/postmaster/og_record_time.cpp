/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/og_record_time.cpp
 *
 * NOTES
 *	  Implements some of this code used to record sql execute time.
 *
 * -------------------------------------------------------------------------
 */

#include "og_record_time.h"
#include "tcop/tcopprot.h"
const char* TimeInfoTypeName[] = {
        "DB_TIME",
        "CPU_TIME",
        "EXECUTION_TIME",
        "PARSE_TIME",
        "PLAN_TIME",
        "REWRITE_TIME",   /*total elapsed time of rewrite stage.*/
        "PL_EXECUTION_TIME",   /*total elapsed time of plpgsql exection.*/
        "PL_COMPILATION_TIME", /*total elapsed time of plpgsql compilation.*/
        "NET_SEND_TIME",
        "DATA_IO_TIME",
        "SRT1_Q",
        "SRT2_SIMPLE_QUERY",
        "SRT3_ANALYZE_REWRITE",
        "SRT4_PLAN_QUERY",
        "SRT5_LIGHT_QUERY",
        "SRT6_P",
        "SRT7_B",
        "SRT8_E",
        "SRT9_D",
        "SRT10_S",
        "SRT11_C",
        "SRT12_U",
        "SRT13_BEFORE_QUERY",
        "SRT14_AFTER_QUERY",
        "RTT_UNKNOWN",
        "NET_SEND_TIMES",
        "NET_SEND_N_CALLS",
        "NET_SEND_SIZE",
        "NET_RECV_TIMES",
        "NET_RECV_N_CALLS",
        "NET_RECV_SIZE",
        "NET_STREAM_SEND_times",
        "NET_STREAM_SEND_n_calls",
        "NET_STREAM_SEND_size",
        "NET_STREAM_RECV_times",
        "NET_STREAM_RECV_n_calls",
        "NET_STREAM_RECV_size"
};

static inline knl_u_stat_context* get_record_cxt()
{
    return &(u_sess->stat_cxt);
}

static int og_get_time_record_level()
{
    Assert(u_sess != NULL);
    return u_sess->attr.attr_common.time_record_level;
}

OgRecordStat* og_get_record_stat()
{
    OgRecordStat* record_stat = (OgRecordStat*) get_record_cxt()->og_record_stat;
    Assert(record_stat != NULL);
    return record_stat;
}


void og_record_time_cleanup(int code, Datum arg)
{
    if (u_sess == NULL) {
        return;
    }
    ereport(DEBUG1,
        (errmsg("record(%ld): cleanup called!",
                get_record_cxt()->og_record_stat == NULL ? -1 : ((int64)(get_record_cxt()->og_record_stat)))));
    if (get_record_cxt()->og_record_stat != NULL) {
        DELETE_EX_TYPE(get_record_cxt()->og_record_stat, OgRecordStat);
    }
}

void og_record_time_reinit()
{
    if (u_sess == NULL || get_record_cxt()->og_record_stat == NULL) {
        return;
    }
    og_get_record_stat()->reinit();
    ResetMemory(u_sess->stat_cxt.localTimeInfoArray,
        sizeof(int64) * TOTAL_TIME_INFO_TYPES);
    ResetMemory(u_sess->stat_cxt.localNetInfo,
        sizeof(uint64) * TOTAL_NET_INFO_TYPES);
}

const char* og_record_time_type_str(const RecordType& time_type)
{
    return og_record_time_type_str(time_type.position());
}

const char* og_record_time_type_str(int pos)
{
    int max_size = sizeof(TimeInfoTypeName) / sizeof(TimeInfoTypeName[0]);
    if (pos < 0 || pos >= max_size) {
        pos = max_size - 1;
    }
    return TimeInfoTypeName[pos];
}

bool og_time_record_start()
{
    return og_get_record_stat()->start_first_record_opt();
}

bool og_time_record_end()
{
    return og_get_record_stat()->free_first_record_opt();
}

bool og_time_record_is_started()
{
    return og_get_record_stat()->already_start();
}

int64 og_get_time_unique_id()
{
    if (u_sess == NULL
    || ((OgRecordStat*) get_record_cxt()->og_record_stat == NULL)) {
        return 0;
    }
    return og_get_record_stat()->get_time_unique_id();
}

static inline void og_record_report_start(const OgTimeDataVo& record)
{
    og_get_record_stat()->report_start(record);
}

static inline void og_record_report_end(const OgTimeDataVo& record)
{
    og_get_record_stat()->report_end(record);
}

static inline void og_record_report_duplicate(OgTimeDataVo& record)
{
    og_get_record_stat()->report_duplicate(record);
}

RecordType::RecordType()
{
        type_code = (int) RTT_UNKNOWN;
        rtt_type = TIME_INFO;
}

RecordType::RecordType(TimeInfoType time_info_type)
{
    type_code = (int) time_info_type;
    rtt_type = TIME_INFO;
}
RecordType::RecordType(SelfRecordType self_typ)
{
    type_code = (int) self_typ;
    rtt_type = SELF_INFO;
}

RecordType::RecordType(NetInfoType net_info_type, ssize_t str_len)
{
    type_code = (int) net_info_type;
    rtt_type = NET_INFO;
    this->str_len = str_len;
}

RecordTimeType RecordType::get_record_time_type() const
{
    return rtt_type;
}

int RecordType::get_type_code() const
{
    return type_code;
}

ssize_t RecordType::get_str_len() const
{
    return str_len;
}

int RecordType::get_init_pos() const
{
    if (rtt_type == NET_INFO) {
        return (int) TOTAL_TIME_INFO_TYPES + (int) SRT_ALL;
    } else if (rtt_type == SELF_INFO) {
        return (int) TOTAL_TIME_INFO_TYPES;
    }
    return 0;
}

int RecordType::position() const
{
    return get_type_code() + get_init_pos();
}

bool RecordType::is_root_type() const
{
    return *this == DB_TIME;
}

bool RecordType::operator==(TimeInfoType time_type) const
{
    if (rtt_type != TIME_INFO) {
        return false;
    }
    return type_code == (int)time_type;
}
bool RecordType::operator!=(TimeInfoType time_type) const
{
    return !(*this == time_type);
}

bool RecordType::operator==(NetInfoType net_type) const
{
    if (rtt_type != NET_INFO) {
        return false;
    }
    return type_code == (int) net_type;
}

bool RecordType::operator!=(NetInfoType net_type) const
{
    return !(*this == net_type);
}

bool RecordType::operator==(SelfRecordType self_type) const
{
    if (rtt_type != SELF_INFO) {
        return false;
    }
    return type_code == (int) self_type;
}

bool RecordType::operator!=(SelfRecordType self_type) const
{
    return !(*this == self_type);
}

const char* OgTimeDataFormatHelper::format(const OgTimeDataVo& vo)
{
    int ret = snprintf_s(format_str, DEFAULT_FORMAT_LENGTH, DEFAULT_FORMAT_LENGTH - 1,
            "rd:{id=%lld,s=%lld,e=%lld,t=%lld,d=%d,na=%s}", vo.id, vo.begin,
            vo.end == 0 ? GetCurrentTimestamp(): vo.end, vo.end == 0 ? 0 : vo.total(),
            vo.depth, og_record_time_type_str(vo.record_type));
    securec_check_ss(ret, "\0", "\0");
    return format_str;
}

OgRecordOperator::OgRecordOperator(bool auto_record)
{
    init(auto_record, RecordType(RTT_UNKNOWN));
}

OgRecordOperator::OgRecordOperator(TimeInfoType time_info_type)
{
    init(true, RecordType(time_info_type));
}

OgRecordOperator::OgRecordOperator(NetInfoType net_info_type)
{
    init(true, RecordType(net_info_type, 0));
}

OgRecordOperator::OgRecordOperator(SelfRecordType self_type)
{
    init(true, RecordType(self_type));
}

OgRecordOperator::OgRecordOperator(bool auto_record, NetInfoType net_type) {
    init(auto_record, RecordType(net_type, 0));
}

OgRecordOperator::OgRecordOperator(bool auto_record, TimeInfoType time_type) {
    init(auto_record, RecordType(time_type));
}

OgRecordOperator::OgRecordOperator(bool auto_record, SelfRecordType self_type) {
    init(auto_record, RecordType(self_type));
}

OgRecordOperator::~OgRecordOperator()
{
    if (this->auto_record) {
        exit();
    }
}

void OgRecordOperator::enter(TimeInfoType time_type)
{
    enter(RecordType(time_type));
}

void OgRecordOperator::enter(NetInfoType net_type)
{
    enter(RecordType(net_type, 0));
}

void OgRecordOperator::enter(const RecordType& record_type)
{
    report_record = report_enable();
    if (!report_record) {
        return;
    }
    if (record_type != RTT_UNKNOWN) {
        base_record.record_type = record_type;
    }
    if (!base_record.record_type.is_root_type()) {
        base_record.begin = (int64)GetCurrentTimestamp();
    }
    og_record_report_start(base_record);
}

void OgRecordOperator::exit(TimeInfoType time_info_type)
{
    exit(RecordType(time_info_type));
}

void OgRecordOperator::exit(NetInfoType net_type, ssize_t str_len)
{
    exit(RecordType(net_type, str_len));
}

void OgRecordOperator::exit(const RecordType& record_type)
{
    if (!report_record || !report_enable()) {
        return;
    }
    if (record_type != RTT_UNKNOWN) {
        base_record.record_type = record_type;
    }
    og_record_report_end(base_record);
}

// this only called after exit() to report new record again.
void OgRecordOperator::report_duplicate(NetInfoType net_type, ssize_t str_len)
{
    RecordType old = base_record.record_type;
    base_record.record_type = RecordType(net_type, str_len);
    og_record_report_duplicate(base_record);
    base_record.record_type = old;
}

int64 OgRecordOperator::get_record_id() const
{
    return base_record.id;
}

void OgRecordOperator::update_record_id()
{
    base_record.id = og_get_time_unique_id();
}

bool OgRecordOperator::report_enable() const
{
    if (u_sess == NULL) {
        return false;
    }

    OgRecordStat* record_stat = (OgRecordStat*)get_record_cxt()->og_record_stat;
    if (record_stat == NULL) {
        return false;
    }
    // for future, we can control diff level record time.
    int level = record_stat->get_time_record_level();
    return level == 0;
}

void OgRecordOperator::init(bool auto_record, const RecordType& record_type)
{
    this->auto_record = auto_record;
    if (this->auto_record) {
        enter(record_type);
    } else {
        base_record.record_type = record_type;
    }
}

OgRecordAutoController::OgRecordAutoController(TimeInfoType time_info_type)
{
    time_info = time_info_type;
    bool report_enable = OgRecordAutoController::report_enable();
    if (report_enable) {
        MemoryContext old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        og_operator = New(CurrentMemoryContext) OgRecordOperator(time_info);
        MemoryContextSwitchTo(old);
    } else {
        og_operator = NULL;
    }
}

OgRecordAutoController::~OgRecordAutoController()
{
    if (og_operator != NULL && u_sess != NULL) {
        MemoryContext old = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        DELETE_EX_TYPE(og_operator,OgRecordOperator);
        MemoryContextSwitchTo(old);
    }
}

bool OgRecordAutoController::report_enable()
{
    if (u_sess == NULL) {
        return false;
    }

    OgRecordStat* record_stat = (OgRecordStat*)get_record_cxt()->og_record_stat;
    if (record_stat == NULL) {
        return false;
    }
    // for future, we can control diff level record time.
    int level = record_stat->get_time_record_level();
    return level == 0;
}

OgTimeDataVo& OgTimeDataStack::top()
{
    Assert(cur_pos >= 0 && cur_pos < DEFAULT_TIME_DATA_STACK_DEPTH);
    return data_list[cur_pos];
}

const OgTimeDataVo& OgTimeDataStack::top() const
{
    Assert(cur_pos >= 0 && cur_pos < DEFAULT_TIME_DATA_STACK_DEPTH);
    return data_list[cur_pos];
}

bool OgTimeDataStack::empty() const
{
    return cur_pos < 0;
}

size_t OgTimeDataStack::size() const {
    Assert(cur_pos >= -1 && cur_pos < DEFAULT_TIME_DATA_STACK_DEPTH);
    return cur_pos + 1;
}

bool OgTimeDataStack::push(const OgTimeDataVo &vo)
{
    if (cur_pos == DEFAULT_TIME_DATA_STACK_DEPTH - 1) {
        return false;
    }
    data_list[++ cur_pos] = vo;
    return true;
}

void OgTimeDataStack::pop()
{
    if (cur_pos < 0) {
        return;
    }
    cur_pos --;
}

void OgTimeDataStack::reset()
{
    cur_pos = -1;
}

void OgTimeDataStack::smart_reset()
{
    cur_pos = 0;
    data_list[0].reset();
}

OgRecordStat::OgRecordStat(int64* local_time_info, uint64* loca_net_info)
:first_record_opt(false, DB_TIME)
{
    log_trace_msg = makeStringInfo();
    record_start = false;
    this->local_time_info = local_time_info;
    this->local_net_info = loca_net_info;
    time_unique_id = 0;
    db_time_baseline = DEFAULT_DB_TIME_BASELINE;
    time_record_level = 0;
    reset();
}

OgRecordStat::~OgRecordStat()
{
    Destroy();
}

void OgRecordStat::Destroy()
{
    reset();
    if (log_trace_msg != NULL) {
        DestroyStringInfo(log_trace_msg);
        log_trace_msg = NULL;
    }
}

void OgRecordStat::reset()
{
    this->logtrace(DEBUG1, "reset");
    depth = INVALID_DEPTH;
    record_start = false;
    free_first_record_opt();
    records_stack.smart_reset();
}

void OgRecordStat::reinit()
{
    this->logtrace(DEBUG1, "reinit");
    reset();
    pre_records_stack.reset();
}

void OgRecordStat::report_start(const OgTimeDataVo& data_record)
{
    if (!records_stack.push(data_record)) {
        return;
    }
    records_stack.top().depth = increment_depth();
    log_vo("begin", records_stack.top());
    if (data_record.record_type.is_root_type()) {
        records_stack.top().begin = GetCurrentTimestamp();
    }
}

void OgRecordStat::report_end(const OgTimeDataVo& record)
{
    // assert not records_stack.is_empty()
    OgTimeDataVo& time_vo = records_stack.top();
    if (record != time_vo) {
        log_vo("unmatch_top:", time_vo);
        log_vo("unmatch_cur:", record);
        return;
    }
    records_stack.pop();
    decrement_depth();
    if (!already_start()) {
        return;
    }
    time_vo.record_type = record.record_type;
    if (time_vo.record_type.is_root_type()) {
        // We don't want debug time calc in root type.
        time_vo.end = GetCurrentTimestamp();
        log_vo("  end", time_vo);
    } else {
        log_vo("  end", time_vo);
        time_vo.end = GetCurrentTimestamp();
    }
    OgTimeDataVo& parent_time_vo = records_stack.top();
    int64 cost = time_vo.cost();
    int64 total = time_vo.total();
    if (time_vo.record_type.get_record_time_type() != NET_INFO) {
        parent_time_vo.update_other_cost(total);
    }
    update_record_time(time_vo.record_type, cost);
}

// some auto report can only report net record.
void OgRecordStat::report_duplicate(OgTimeDataVo& record)
{
    Assert(record.record_type.get_record_time_type() == NET_INFO);
    log_vo("duplicate", record);
    if (record.end == 0) {
        record.end = GetCurrentTimestamp();
    }
    update_record_time(record.record_type, record.cost());
}

int OgRecordStat::increment_depth()
{
    return ++ depth;
}

void OgRecordStat::decrement_depth()
{
    depth --;
}

int64 OgRecordStat::get_time_unique_id()
{
    time_unique_id ++;
    return time_unique_id;
}

void OgRecordStat::print_self() const
{
    int64 bind_total = 0;
    int64 bind_data = 0;
    if (!log_enable(DEBUG1)) {
        return;
    }
    for (int i = 0; i <  TOTAL_RECORD_TYPES; i ++)
    {
        if (i < TOTAL_TIME_INFO_TYPES) {
            bind_total += local_time_info[i];
            bind_data = local_time_info[i];
        } else if(i < TOTAL_TIME_INFO_TYPES + SRT_ALL) {
            bind_data = 0;
        } else if(i < TOTAL_RECORD_TYPES) {
            int tmp_pos = i - TOTAL_TIME_INFO_TYPES - SRT_ALL;
            bind_data = local_net_info[tmp_pos];
        } else {
            // nothing to do
        }
        logtrace(DEBUG1, "name:%d %30s %ld", i, og_record_time_type_str(i), bind_data);
    }
    int64 child_cost = bind_total - get_record_times(DB_TIME) - get_record_times(CPU_TIME);
    logtrace(DEBUG1, "%s,%s,diff(%lld) = total=(%lld) - child_cost(%lld)",
                 get_record_times(RTT_UNKNOWN) < db_time_baseline ? "rd_yes" : "rd_no ",
                 (child_cost == get_db_time()) ? "rd_eq  " : "rd_ne",
                 get_record_times(RTT_UNKNOWN),
                 get_db_time(), child_cost);
}

int OgRecordStat::get_time_record_level() const
{
    return time_record_level;
}

void OgRecordStat::update_time_record_level()
{
    time_record_level = og_get_time_record_level();
}

bool OgRecordStat::start_first_record_opt()
{
    if (!already_start()) {
        reset();
        update_time_record_level();
        first_record_opt.update_record_id();
        first_record_opt.enter(DB_TIME);
        record_start = true;
        while (!pre_records_stack.empty()) {
            OgTimeDataVo& vo = pre_records_stack.top();
            pre_records_stack.pop();
            vo.begin = GetCurrentTimestamp();
            vo.other_cost = 0;
            report_start(vo);
        }
        return true;
    }
    return false;
}

bool OgRecordStat::free_first_record_opt()
{
    if(already_start()) {
        int64 record_id = first_record_opt.get_record_id();
        while (records_stack.size() > 2 && (records_stack.top().id != record_id)) {
            records_stack.top().end = GetCurrentTimestamp();
            pre_records_stack.push(records_stack.top());
            log_vo("free_head", records_stack.top());
            report_end(records_stack.top());
        }
        first_record_opt.exit();
        record_start = false;
        logtrace(DEBUG1, "free_first_record_opt called, %d", record_id);
        local_time_info[RTT_UNKNOWN] += local_time_info[DB_TIME];
        local_time_info[DB_TIME] = get_db_time();
        return true;
    }
    return false;
}

const int64 OgRecordStat::get_record_times(TimeInfoType type_info) const
{
    return local_time_info[type_info];
}

const int64 OgRecordStat::get_db_time() const
{
    return records_stack.top().other_cost;
}

const OgTimeDataVo& OgRecordStat::get_root_time_data_vo() const
{
    return records_stack.top();
}

inline bool OgRecordStat::already_start() const
{
    return record_start;
}

void OgRecordStat::update_record_time(const RecordType& record_type, int64 cost)
{
    if (record_type.get_record_time_type() == TIME_INFO) {
        local_time_info[record_type.position()] += cost;
    } else if (record_type.get_record_time_type() == SELF_INFO) {
        // not use, only for add new time record to quick debug
    } else {
        ssize_t str_len = record_type.get_str_len();
        if (str_len != 0) {
            int type_code = record_type.get_type_code();
            local_net_info[type_code] += (uint64)cost;
            local_net_info[type_code + 1] ++;
            local_net_info[type_code + 2] += str_len;
        }
    }
}

// flow database system log config
bool OgRecordStat::log_enable(int level) const {
    return u_sess != NULL && u_sess->attr.attr_common.log_statement == LOGSTMT_ALL
           && level >= u_sess->attr.attr_common.log_min_messages;
}

void OgRecordStat::log_vo(const char* tag, const OgTimeDataVo& vo) const
{
    if (!log_enable(DEBUG1)) {
        return;
    }
    ereport(DEBUG1, (errmsg("record(%ld-%ld) %s %s", (uint64)this,
                           u_sess == NULL ? 0 : u_sess->session_id, tag,
                           FORMAT_VO(vo))));
}

void OgRecordStat::logtrace(int level, const char* fmt, ...) const
{
    if (!log_enable(level) || log_trace_msg == NULL) {
        return;
    }
    if (fmt != log_trace_msg->data) {
        va_list args;
        (void)va_start(args, fmt);
        // This place just is the message print. So there is't need check the value of vsnprintf_s function return. if
        // checked, when the message lengtn is over than log_trace_msg->maxlen, will be abnormal exit.
        (void)vsnprintf_s(log_trace_msg->data, log_trace_msg->maxlen, log_trace_msg->maxlen - 1, fmt, args);
        va_end(args);
    }
    ereport(level, (errmsg("record(%ld-%ld) %s", (uint64)this,
        u_sess == NULL ? 0 : u_sess->session_id, log_trace_msg->data)));
}
