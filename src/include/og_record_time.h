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
 *	  src/include/og_record_time.h
 *
 * NOTES
 *	  Some of this code used to record sql execute time.
 *
 * -------------------------------------------------------------------------
 */

#ifndef OG_RECORD_TIME_H
#define OG_RECORD_TIME_H
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "og_record_time_rely.h"
#include "knl/knl_session.h"

typedef enum TimeInfoType {
    DB_TIME = 0, /*total elapsed time while dealing user command.*/
    CPU_TIME,    /*total cpu time used while dealing user command.*/

    /*statistics of specific execution stage.*/
    EXECUTION_TIME, /*total elapsed time of execution stage.*/
    PARSE_TIME,     /*total elapsed time of parse stage.*/
    PLAN_TIME,      /*total elapsed time of plan stage.*/
    REWRITE_TIME,   /*total elapsed time of rewrite stage.*/

    /*statistics for plpgsql especially*/
    PL_EXECUTION_TIME,   /*total elapsed time of plpgsql exection.*/
    PL_COMPILATION_TIME, /*total elapsed time of plpgsql compilation.*/

    NET_SEND_TIME,
    DATA_IO_TIME,
    SRT1_Q,
    SRT2_SIMPLE_QUERY,
    SRT3_ANALYZE_REWRITE,
    SRT4_PLAN_QUERY,
    SRT5_LIGHT_QUERY,
    SRT6_P,
    SRT7_B,
    SRT8_E,
    SRT9_D,
    SRT10_S,
    SRT11_C,
    SRT12_U,
    SRT13_BEFORE_QUERY,
    SRT14_AFTER_QUERY,
    RTT_UNKNOWN,
    TOTAL_TIME_INFO_TYPES
} TimeInfoType;

// some procedure use old postion, so we define this.
const TimeInfoType TOTAL_TIME_INFO_TYPES_P1 = SRT1_Q;

typedef enum NetInfoType {
    NET_SEND_TIMES,
    NET_SEND_N_CALLS,
    NET_SEND_SIZE,

    NET_RECV_TIMES,
    NET_RECV_N_CALLS,
    NET_RECV_SIZE,

    NET_STREAM_SEND_TIMES,
    NET_STREAM_SEND_N_CALLS,
    NET_STREAM_SEND_SIZE,

    NET_STREAM_RECV_TIMES,
    NET_STREAM_RECV_N_CALLS,
    NET_STREAM_RECV_SIZE,

    TOTAL_NET_INFO_TYPES
} NetInfoType;

// this for easy add new record type for debug.
typedef enum SelfRecordType {
    SRT_ALL
} SelfRecordType;
// the type of record time
typedef enum RecordTimeType {
    TIME_INFO = 0,
    NET_INFO,
    SELF_INFO
} RecordTimeType;

const int TOTAL_RECORD_TYPES = TOTAL_TIME_INFO_TYPES + TOTAL_NET_INFO_TYPES + SRT_ALL;
// if left record time more than this, will print flag info.
const int64 DEFAULT_DB_TIME_BASELINE = 10; //ms
// the OgTimeDataStack depth
const int DEFAULT_TIME_DATA_STACK_DEPTH = 100;
// the default format length
const int DEFAULT_FORMAT_LENGTH = 1024;
const int INVALID_DEPTH = -1;
// the type of record.
class RecordType;
// the time base slice of type
class OgTimeDataVo;
// the time base vo format helper
class OgTimeDataFormatHelper;
// the time data vo stack
class OgTimeDataStack;
// the stat statics class
class OgRecordStat;

#define FORMAT_VO(vo) (OgTimeDataFormatHelper().format(vo))

#ifdef _cplusplus
extern "C" {
#endif
extern const char* TimeInfoTypeName[];
/**
 * Get record stat instance, must be already init session before use it!
 * */
OgRecordStat* og_get_record_stat();

/**
 * Clean RecordState instance memory.
 * @param code error code
 * @param arg  the input arg
 */
void og_record_time_cleanup(int code, Datum arg);

/**
 * Reinit time record stat after set_long_jump
 */
void og_record_time_reinit();

/**
 * Start time record, this will trigger DB_TIME begin event report.
 * @return true if first started else already startted.
 */
bool og_time_record_start();

/**
 * Stop time record, this will trigger DB_TIME end event report
 * @return true if success stopped else already stopped.
 */
bool og_time_record_end();

/**
 * Get if time record startted.
 * @return true if stattted.
 */
bool og_time_record_is_started();

/**
 * Get unique event id, id will continuously growing in a single statistic.
 * @return the new event it.
 */
int64 og_get_time_unique_id();

/**
 * Convert record_time to str.
 * @param record_type the record type.
 * @return const char* desc
 */
const char* og_record_time_type_str(const RecordType& record_type);

/**
 * Convert int pos to str
 * @param pos the postion of record type
 * @return const char* desc
 */
const char* og_record_time_type_str(int pos);

#ifdef _cplusplus
}
#endif

class RecordType {
public:
    explicit RecordType();
    explicit RecordType(TimeInfoType time_info_type);
    explicit RecordType(SelfRecordType self_typ);
    explicit RecordType(NetInfoType net_info_type, ssize_t str_len);
    /**
     * Get which type of record it is. match the construct.
     * @return
     */
    RecordTimeType get_record_time_type() const;

    /**
     * The enum type to int.
     * @return the enum type order.
     */
    int get_type_code() const;

    /**
     * Only use in NET_INFO_TYPE.
     * @return get the send net size len.
     */
    ssize_t get_str_len() const;

    /**
     * Match the RecordTimeType split postion.
     * @return matched RecordTimeType base pos.
     */
    int get_init_pos() const;

    /**
     * Match the `og_record_time_type_str(int pos);` pos
     * @return the pos of time_type_str
     */
    int position() const;

    /**
     * Is the DB_TIME type. DB_TIME can't be trigger by user.
     * @return true if current if DB_TIME event
     */
    bool is_root_type() const;

    bool operator==(TimeInfoType time_type) const;
    bool operator!=(TimeInfoType time_type) const;
    bool operator==(NetInfoType net_type) const;
    bool operator!=(NetInfoType net_type) const;
    bool operator==(SelfRecordType self_type) const;
    bool operator!=(SelfRecordType self_type) const;
private:
    RecordTimeType rtt_type;
    int type_code;
    ssize_t str_len;
};

class OgTimeDataVo {
public:
    explicit OgTimeDataVo(): begin(0), end(0), depth(INVALID_DEPTH)
            , record_type(RTT_UNKNOWN), other_cost(0), id(og_get_time_unique_id()) {}
    explicit OgTimeDataVo(const RecordType& cur_record_type): begin(0), end(0), depth(INVALID_DEPTH)
    , record_type(cur_record_type), other_cost(0), id(og_get_time_unique_id()) {}

    void reset()
    {
        begin = 0;
        end = 0;
        other_cost = 0;
        depth = INVALID_DEPTH;
    }

    /**
     * Get this stage total time.
     * @return total record time.
     */
    int64 total() const
    {
        return end - begin;
    }

    /**
     * Get this stage real time. sub child stage.
     * @return
     */
    int64 cost() const
    {
        return total() - other_cost;
    }

    /**
     * Update child stage cost.
     * @param cost
     */
    void update_other_cost(int64 cost) {
        this->other_cost += cost;
    }

    bool operator==(const OgTimeDataVo& compare) const {
        return this->id == compare.id;
    }

    bool operator!=(const OgTimeDataVo& compare) const {
        return !(*this == compare);
    }
    // NOTICE: we will push this class to stack(some std::stack use this to create new instance),
    // so must implementation this operator.
    OgTimeDataVo& operator=(const OgTimeDataVo& vo)
    {
        if (this != &vo) {
            this->begin = vo.begin;
            this->end = vo.end;
            this->depth = vo.depth;
            this->record_type = vo.record_type;
            this->other_cost = vo.other_cost;
            this->id = vo.id;
        }
        return *this;
    }

public:
    int64 begin;
    int64 end;
    int depth;
    RecordType record_type;
    int64 other_cost;
    int64 id;
};

class OgTimeDataFormatHelper {
public:
    const char* format(const OgTimeDataVo& vo);
private:
    char format_str[DEFAULT_FORMAT_LENGTH];
};

class OgTimeDataStack {
public:
    OgTimeDataStack(): cur_pos(-1) {}
    OgTimeDataVo& top();
    const OgTimeDataVo& top() const;
    bool push(const OgTimeDataVo& vo);
    void pop();
    bool empty() const;
    size_t size() const;
    void reset();
    void smart_reset();
private:
    int cur_pos;
    OgTimeDataVo data_list[DEFAULT_TIME_DATA_STACK_DEPTH];
};

class OgRecordOperator : public BaseObject {
public:
    /* You must be careful when auto_record = false, it means you need ensure enter/exit pair called!
     * The only situation is in loop, like in timeRecordStart/End.
     * if call stack not match, some time record will discard until `OgRecordStat.reset` is called!
     * We encourage this approach: OgRecordOperator _local_opt(TimeInfoType);
     */
    explicit OgRecordOperator(bool auto_record);
    // `NOTICE`: only TimeInfoType will record time, NetInfoType time will calculate for parent TimeInfoType stage.
    // SelfRecordType only for add new stage for debug.
    explicit OgRecordOperator(TimeInfoType time_info_type);
    explicit OgRecordOperator(NetInfoType net_info_type);
    explicit OgRecordOperator(SelfRecordType self_type);
    explicit OgRecordOperator(bool auto_record, NetInfoType net_type);
    explicit OgRecordOperator(bool auto_record, TimeInfoType time_type);
    explicit OgRecordOperator(bool auto_record, SelfRecordType self_type);
    virtual ~OgRecordOperator();
    void Destroy() {}
    void enter(TimeInfoType time_type=RTT_UNKNOWN);
    void enter(NetInfoType net_type);
    void enter(const RecordType& record_type);
    void exit(TimeInfoType time_info_type=RTT_UNKNOWN);
    void exit(NetInfoType net_type, ssize_t str_len);
    void exit(const RecordType& record_type);

    /**
     * Only TIME_INFO_TYPE will record time. this only called after exit() to report new record again.
     * @param net_type NET_INTO_TYPE type
     * @param str_len the send size
     */
    void report_duplicate(NetInfoType net_type, ssize_t str_len);

    /**
     * Get current record id.
     * @return
     */
    int64 get_record_id() const;

    /**
     * update new record id for reuse.
     */
    void update_record_id();

    /**
     * If enable report stage event. if record not start or `time_record_level` != 0 , this will false.
     * @return true if need report.
     */
    bool report_enable() const;
private:
    void init(bool auto_record, const RecordType& record_type);
private:
    OgTimeDataVo base_record;
    bool auto_record;
    bool report_record;
};

class OgRecordAutoController {
public:
    OgRecordAutoController(TimeInfoType time_info_type);
    ~OgRecordAutoController();
    static bool report_enable();
private:
    OgRecordOperator* og_operator;
    TimeInfoType time_info;
};

class OgRecordStat : public BaseObject {
public:
    /**
     * Bind old stat memory of local_time_info and loca_net_info
     * @param local_time_info the TimeInfoType memory
     * @param loca_net_info the NetInfoType memory
     */
    OgRecordStat(int64* local_time_info, uint64* loca_net_info);
    virtual ~OgRecordStat();
    /**
     * This used by DELETE_EX macro
     */
    void Destroy();

    /**
     * Reset this instance.
     */
    void reset();

    /**
     * Reinit after long jump
     */
    void reinit();

    /**
     * Process stage event from `OgRecordOperator.enter`.
     * @param data_record the record
     */
    void report_start(const OgTimeDataVo& data_record);

    /**
     * Process stage event from `OgRecordOperator.exit`.
     * @param record the record
     */
    void report_end(const OgTimeDataVo& record);

    /**
     * Process start record time. this will trigger DB_TIME stage event.
     * @return true if first startted.
     */
    bool start_first_record_opt();

    /**
     * Process stop record time. this will trigger DB_TIME stage event.
     * @return true if first stopped.
     */
    bool free_first_record_opt();

    /**
     * Get the stage record time.
     * @param type_info the stage
     * @return record time.
     */
    const int64 get_record_times(TimeInfoType type_info) const;

    /**
     * Get the DB_TIME stage record time.
     * @return the DB_TIME stage time.
     */
    const int64 get_db_time() const;

    /**
     * The root PgDataTimeVo instance, it's child_cost is equal DB_TIME stage.
     * @return the root PgDataTimeVo instance.
     */
    const OgTimeDataVo& get_root_time_data_vo() const;

    /**
     * Get if already start record.
     * @return true if already startted.
     */
    bool already_start() const;

    // only for NetInfoType report.
    void report_duplicate(OgTimeDataVo& record);

    /**
     * Incr record depth. eveny new report stage will increment it.
     * @return current depth
     */
    int increment_depth();

    /**
     * Decr record depth. after record end stage will decrement it.
     */
    void decrement_depth();

    /**
     * Get new unique PgDataTimeVo id.
     * @return the new id.
     */
    int64 get_time_unique_id();

    /**
     * Print str, only for debug use.
     */
    void print_self() const;

    /**
     * Get time record level for report_enable. 0-> default, all stage will report
     * 1~10-> not record time.
     * @return the level.
     */
    int get_time_record_level() const;

    /**
     * Update time record level from session.
     */
    void update_time_record_level();

    bool log_enable(int level = LOG) const;
    void log_vo(const char* tag, const OgTimeDataVo& vo) const;
    void logtrace(int level, const char* fmt, ...) const;
private:
    void update_record_time(const RecordType& record_type, int64 cost);

private:
    OgTimeDataStack records_stack;
    OgTimeDataStack pre_records_stack;
    int64* local_time_info;
    uint64* local_net_info;
    int depth;
    int64 time_unique_id;
    int64 db_time_baseline;
    int time_record_level;
    StringInfo log_trace_msg;
    OgRecordOperator first_record_opt;
    bool record_start;
};
#endif
