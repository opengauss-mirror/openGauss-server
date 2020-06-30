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
 * plog.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/plog.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_UTILS_PLOG_H_
#define SRC_INCLUDE_UTILS_PLOG_H_

#include "c.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "postmaster/syslogger.h"
#include "replication/replicainternal.h"

#define RET_TYPE_OK 1
#define RET_TYPE_FAIL 0
#define PLOG_ENTRY_MAGICNUM (0xFFFFFFFF)

enum DataSourceType {
    /* magnetic disk */
    DS_MD = 0,

    /* OBS data source */
    DS_OBS,

    /* Hadoop data source */
    DS_HADOOP,

    /* remote datanode */
    DS_REMOTE_DATANODE,

    /* current number of valid data source, maybe equal to DS_UPLIMIT */
    DS_VALID_NUM,

    /* max value of data source */
    DS_UPLIMIT = 127
};

/* data resource require type */
enum DataResReqType {
    /* OBS LIST */
    DSRQ_LIST = 0,

    /* read data from data source */
    DSRQ_READ,

    /* write data to data source  */
    DSRQ_WRITE,

    /* OBS OPEN */
    DSRQ_OPEN,

    /* current number of valid requiring type */
    DSRQ_VALID_NUM,

    /* max value of requiring data source */
    DSRQ_UPLIMIT = 127
};

/*
 * Add plog_magic to distinguish 32 bits u_sess->debug_query_id
 * from 64 bits u_sess->debug_query_id in log files of gs_profile.
 * In case of 32 bits u_sess->debug_query_id, gxid and gqid form a 8 byte, and align with 8-byte tid.
 * In case of 64 bits u_sess->debug_query_id, gxid and plog_magic form a 8 byte, and align with 8-byte tid.
 * Thus, plog_magic can be used to tell us which type of u_sess->debug_query_id is read from the log files.
 * Here, byte alignment feature is used. So be careful to maintain this byte alignment
 * when revising PLogEntryMeta.
 */
typedef struct {
    struct timeval req_time; /* record's time */
    ThreadId tid;            /* thread id */
    uint32 gxid;             /* global transaction id */
    uint32 plog_magic;       /* PLogEntryMeta MAGICNUM */
    uint64 gqid;             /* debug query id */
} PLogEntryMeta;             /* 8byte alligned */

typedef struct {
    uint8 data_src;
    uint8 req_type;
    uint8 ret_type; /* 1, success; 0, failed */
    uint8 item_num; /* <= PLOG_MD_ITEM_MAX_NUM */
} PLogEntryHead;    /* 4byte alligned */

typedef struct {
    uint32 sum_count;
    uint32 sum_size; /* how many data */
    uint32 sum_usec; /* time to run, upmost is 1 hour */
} PLogEntryItem;     /* 4byte alligned */

typedef struct {
    uint8 data_src;
    uint8 req_type;

    /* RET_TYPE_OK or RET_TYPE_FAIL */
    uint8 ret_type;

    /*
     * if ret_type is RET_TYPE_OK,
     *   dat_size means the real data amount offered by disk/OBS, etc
     * if ret_type is RET_TYPE_FAIL,
     *   dat_size means the data amount required by caller
     *
     * req_usec always mean the used time to require and server.
     */
    uint32 dat_size; /* data size */
    uint32 req_usec; /* required time with us */
} IndicatorItem;

typedef struct {
    PLogEntryMeta meta;
    PLogEntryHead head;
    PLogEntryItem item[FLEXIBLE_ARRAY_MEMBER];
    /* the other items */
} PLogEntry;

typedef struct {
    PLogEntry basic;
} PLogBasicEntry;

typedef struct {
    /* message protocol head */
    char msghead[LOGPIPE_HEADER_SIZE];
    union {
        PLogBasicEntry entry;
    } msgbody;
} PLogMsg;

extern void aggregate_profile_logs(IndicatorItem* new_item, struct timeval* nowtm);
extern void flush_plog(void);

#define SKIPPED_MODE()                                                          \
    (t_thrd.xlog_cxt.InRecovery || (t_thrd.postmaster_cxt.HaShmData == NULL) || \
        (t_thrd.postmaster_cxt.HaShmData->current_mode != PRIMARY_MODE &&       \
            t_thrd.postmaster_cxt.HaShmData->current_mode != NORMAL_MODE))

/* if plog is enable */
#define ENABLE_PLOG()                                                      \
    (IsUnderPostmaster && g_instance.attr.attr_common.Logging_collector && \
        u_sess->attr.attr_storage.plog_merge_age > 0 && !SKIPPED_MODE())

#define PROFILING_START_TIMER()                    \
    instr_time __startTime;                        \
    bool enable_plog = ENABLE_PLOG();              \
    if (enable_plog) {                             \
        (void)INSTR_TIME_SET_CURRENT(__startTime); \
    }

#define PROFILING_END_TIMER()                \
    instr_time __endTime;                    \
    instr_time __difTime;                    \
    (void)INSTR_TIME_SET_CURRENT(__endTime); \
    __difTime = __endTime;                   \
    INSTR_TIME_SUBTRACT(__difTime, __startTime)

/* time X > time Y ? */
#define TM_IS_BIGGER(x, y) (((x).tv_sec == (y).tv_sec) ? ((x).tv_usec > (y).tv_usec) : ((x).tv_sec > (y).tv_sec))

/* interface macro about MD(magetic disk) IO monitor */
#define PROFILING_MDIO_START() PROFILING_START_TIMER()

#define PROFILING_MDIO_END_READ(__reqsize, __retsize)               \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_MD;                              \
            new_item.req_type = DSRQ_READ;                          \
            if ((__retsize) >= 0) {                                 \
                new_item.ret_type = RET_TYPE_OK;                    \
                new_item.dat_size = (__retsize);                    \
            } else {                                                \
                new_item.ret_type = RET_TYPE_FAIL;                  \
                new_item.dat_size = (__reqsize);                    \
            }                                                       \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

#define PROFILING_MDIO_END_WRITE(__reqsize, __retsize)              \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_MD;                              \
            new_item.req_type = DSRQ_WRITE;                         \
            if ((__retsize) >= 0) {                                 \
                new_item.ret_type = RET_TYPE_OK;                    \
                new_item.dat_size = (__retsize);                    \
            } else {                                                \
                new_item.ret_type = RET_TYPE_FAIL;                  \
                new_item.dat_size = (__reqsize);                    \
            }                                                       \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

/* interface macro about OBS(Object Storage Service) IO monitor */
#define PROFILING_OBS_ERROR(__reqsize, __reqtype)        \
    do {                                                 \
        IndicatorItem new_item;                          \
        instr_time __nowtime;                            \
        new_item.data_src = DS_OBS;                      \
        new_item.req_type = (__reqtype);                 \
        new_item.ret_type = RET_TYPE_FAIL;               \
        new_item.dat_size = (__reqsize);                 \
        new_item.req_usec = 0;                           \
        (void)INSTR_TIME_SET_CURRENT(__nowtime);         \
        aggregate_profile_logs(&new_item, &(__nowtime)); \
    } while (0)

#define PROFILING_OBS_START() PROFILING_START_TIMER()

#define PROFILING_OBS_END_LIST(__retsize)                           \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_OBS;                             \
            new_item.req_type = DSRQ_LIST;                          \
            new_item.ret_type = RET_TYPE_OK;                        \
            new_item.dat_size = (__retsize);                        \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

#define PROFILING_OBS_END_READ(__retsize)                           \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_OBS;                             \
            new_item.req_type = DSRQ_READ;                          \
            new_item.ret_type = RET_TYPE_OK;                        \
            new_item.dat_size = (__retsize);                        \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

#define PROFILING_OBS_END_WRITE(__retsize)                          \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_OBS;                             \
            new_item.req_type = DSRQ_WRITE;                         \
            new_item.ret_type = RET_TYPE_OK;                        \
            new_item.dat_size = (__retsize);                        \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

/* interface macro about Hadoop IO monitor */

#define PROFILING_HDP_ERROR(__reqsize, __reqtype)        \
    do {                                                 \
        IndicatorItem new_item;                          \
        instr_time __nowtime;                            \
        new_item.data_src = DS_HADOOP;                   \
        new_item.req_type = (__reqtype);                 \
        new_item.ret_type = RET_TYPE_FAIL;               \
        new_item.dat_size = (__reqsize);                 \
        new_item.req_usec = 0;                           \
        (void)INSTR_TIME_SET_CURRENT(__nowtime);         \
        aggregate_profile_logs(&new_item, &(__nowtime)); \
    } while (0)

#define PROFILING_HDP_START() PROFILING_START_TIMER()

#define PROFILING_HDP_END_OPEN(__rettype)                           \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_HADOOP;                          \
            new_item.req_type = DSRQ_OPEN;                          \
            new_item.ret_type = (__rettype);                        \
            new_item.dat_size = 1; /* only one file */              \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

#define PROFILING_HDP_END_READ(__retsize)                           \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_HADOOP;                          \
            new_item.req_type = DSRQ_READ;                          \
            new_item.ret_type = RET_TYPE_OK;                        \
            new_item.dat_size = (__retsize);                        \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

#define PROFILING_HDP_END_WRITE(__retsize)                          \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_HADOOP;                          \
            new_item.req_type = DSRQ_WRITE;                         \
            new_item.ret_type = RET_TYPE_OK;                        \
            new_item.dat_size = (__retsize);                        \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

/* interface macro about remote datanode read monitor */
#define PROFILING_REMOTE_START() PROFILING_START_TIMER()

#define PROFILING_REMOTE_END_READ(__reqsize, __is_success)          \
    do {                                                            \
        if (enable_plog) {                                          \
            PROFILING_END_TIMER();                                  \
            IndicatorItem new_item;                                 \
            new_item.data_src = DS_REMOTE_DATANODE;                 \
            new_item.req_type = DSRQ_READ;                          \
            if (__is_success) {                                     \
                new_item.ret_type = RET_TYPE_OK;                    \
                new_item.dat_size = (__reqsize);                    \
            } else {                                                \
                new_item.ret_type = RET_TYPE_FAIL;                  \
                new_item.dat_size = (__reqsize);                    \
            }                                                       \
            new_item.req_usec = INSTR_TIME_GET_MICROSEC(__difTime); \
            aggregate_profile_logs(&new_item, &__endTime);          \
        }                                                           \
    } while (0)

extern void init_plog_global_mem(void);

#endif /* SRC_INCLUDE_UTILS_PLOG_H_ */
