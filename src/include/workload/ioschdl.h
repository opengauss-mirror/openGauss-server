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
 * ioschdl.h
 *     definitions for IO control parameters 
 * 
 * IDENTIFICATION
 *        src/include/workload/ioschdl.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef IOSCHDL_H
#define IOSCHDL_H

#include "postgres.h"
#include "knl/knl_variable.h"
/* IO types */
#define IO_TYPE_WRITE 0   // IO type write
#define IO_TYPE_READ 1    // IO type read
#define IO_TYPE_COLUMN 0  // IO type for column
#define IO_TYPE_ROW 1     // IO type for row

/* iostat */
#define MAX_DEVICE_DIR 256
#define UTIL_COUNT_LEN 3

/* util threshold */
#define UTIL_HIGH_THRESHOLD 50
#define UTIL_LOW_THRESHOLD 30

/* util state */
#define UTIL_LOW 0
#define UTIL_MEDIUM 1
#define UTIL_HIGH 2

#define FETCH_IOSTAT 0
#define RESET_IOSTAT 1

#define IO_MIN_COST 50000

#define IOCONTROL_UNIT 6000

// write take 30%
#define W_RATIO_THRESHOLD 30
// iops cannot be lower than 5
#define IOPS_LOW_LIMIT 5

#define FULL_PERCENT 100

#define IOPRIORITY_NONE 0
#define IOPRIORITY_LOW 10
#define IOPRIORITY_MEDIUM 20
#define IOPRIORITY_HIGH 50

#define WLM_AUTOWAKE_INTERVAL 1

#define IOLIMIT_WAKE_TIME 1
#define CPUSTAT_WAKE_TIME 8

#define IOPRIORITY_LOW_WAKE_TIME 3
#define IOPRIORITY_MEDIUM_WAKE_TIME 2
#define IOPRIORITY_HIGH_WAKE_TIME 1

#define FILE_DISKSTAT "/proc/diskstats"
#define FILE_CPUSTAT "/proc/stat"
#define FILE_MOUNTS "/proc/mounts"

#define AVERAGE_DISK_VALUE(m, n, p, u) (((double)((n) - (m))) / (p)*u)

typedef struct IORequestEntry {
    int amount;     // IO amount
    int rqst_type;  // IO type
    int count;      // iocount

    int count_down;  // count of time wlmmonitor thread is wakened
    pthread_cond_t io_proceed_cond;
} IORequestEntry;

struct blkio_info {
    int used;
    uint64 rd_ios;     /* Read I/O operations */
    uint64 wr_ios;     /* Write I/O operations */
    uint64 rd_sectors; /* Sectors read */
    uint64 wr_sectors; /* Sectors written */
    uint64 tot_ticks;  /* Time of requests in queue */
};

typedef struct WLMmonitor_iostat {
    // updated on DN
    char device[MAX_DEVICE_DIR]; /* device name */

    // iostat
    double rs;      /* r/s */
    double ws;      /* w/s */
    double util;    /* %util */
    double w_ratio; /* write proportion */

    // cpu util
    double cpu_util;
    int cpu_util_count;
    int cpu_count;

    // for io scheduler
    unsigned char device_util_tbl[UTIL_COUNT_LEN]; /* whether it is below the threshold*/
    int total_tbl_util;
    int tick_count;

    // updated on CN
    int maxIOUtil;
    int minIOUtil;
    int maxCpuUtil;
    int minCpuUtil;

    int node_count; /* count of DN */
} WLMmonitor_iostat;

typedef struct WLMIOContext {
    /* waiting list and mutex */
    List* waiting_list;
    pthread_mutex_t waiting_list_mutex;

    /* cpu cores */
    int cpu_nr;

    /*
     * cpu total time calculated from /proc/stat,
     * the first element stores the last one,
     * and the second stores the current
     */
    uint64 uptime[2];
    /* cpu0 total calculated from /proc/stat */
    uint64 uptime0[2];
    /* cpu user time */
    uint64 cputime_user[2];

    /* timestamp of the previous and the current */
    TimestampTz prev_timestamp;
    TimestampTz cur_timestamp;

    /* hz per second */
    uint32 hz;

    /* blockio info, got from /proc/diskstats */
    struct blkio_info new_blkio;
    struct blkio_info old_blkio;

    /* device iostat */
    WLMmonitor_iostat WLMmonitorDeviceStat;

    /* device init */
    bool device_init;
} WLMIOContext;

extern void IOStatistics(int type, int count, int size);
extern void IOSchedulerAndUpdate(int type, int count, int store_type);
extern void WLMmonitor_check_and_update_IOCost(PlannerInfo* root, NodeTag node, Cost IOcost);

/* monitoring thread use  */
extern ThreadId StartWLMmonitor(void);

/* arbiter thread use  */
extern ThreadId StartWLMarbiter(void);

#ifdef EXEC_BACKEND
extern void WLMmonitorMain();
extern void WLMarbiterMain();
#endif

#endif
