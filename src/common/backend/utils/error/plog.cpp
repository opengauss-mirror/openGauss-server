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
 * plog.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/utils/error/plog.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plog.h"
#include "utils/palloc.h"
#include "access/xact.h"
#include "tcop/tcopprot.h"
#include "miscadmin.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#define PLOG_NOT_USED (char)(-1)

#define PLOG_ENTRY_MAX_NUM 32

/*
 * judge whether the sum of two UINT32 overflow.
 * (_sum) and (_val) must be both of UINT32 date type.
 */
#define UINT32_SUM_OVERFLOW(_sum, _val) ((_sum) + (_val) < (_sum))

/* max size of plog single line */
#define PLOG_ENTRY_MAX_SIZE \
    (LOGPIPE_HEADER_SIZE + sizeof(PLogEntryMeta) + sizeof(PLogEntryHead) + sizeof(PLogEntryItem) * PLOG_ENTRY_MAX_NUM)

typedef int (*plog_get_slot)(IndicatorItem*);

static inline void plog_fill_fixed_proto(LogPipeProtoHeader*);
static inline void plog_record_init_meta(PLogEntryMeta*, struct timeval*);
static inline void plog_record_init_head(PLogEntryHead*, int, char, char, char);
static inline void plog_record_init_items(PLogEntryItem*, int);
static inline bool plog_switch_new_msg(struct timeval*, struct timeval);
static inline void write_logmsg_with_one_item(IndicatorItem*, struct timeval*);
static int plog_md_get_slot(IndicatorItem*);
static int plog_obs_get_slot(IndicatorItem*);
static int plog_hdp_get_slot(IndicatorItem*);
static int plog_remote_get_slot(IndicatorItem*);
static void write_logmsg_with_multi_items(PLogMsg*);

static plog_get_slot g_plog_slot_array[DS_VALID_NUM][DSRQ_VALID_NUM] = {
    /* DS_MD */
    {plog_md_get_slot, plog_md_get_slot, plog_md_get_slot, plog_md_get_slot},
    /* DS_OBS */
    {plog_obs_get_slot, plog_obs_get_slot, plog_obs_get_slot, plog_obs_get_slot},
    /* DS_HADOOP */
    {plog_hdp_get_slot, plog_hdp_get_slot, plog_hdp_get_slot, plog_hdp_get_slot},
    /* DS_REMOTE_DATANODE */
    {plog_remote_get_slot, plog_remote_get_slot, plog_remote_get_slot, plog_remote_get_slot}};

/*
 * this function MUST be called immediately after t_thrd.mem_cxt.profile_log_mem_cxt
 * memory context is created. see its caller.
 *
 * Maybe these global memory must be malloced in the begin of worker
 * thread running, including the followings:
 * 1. t_thrd.log_cxt.plog_md_read_entry
 * 2. t_thrd.log_cxt.plog_md_write_entry
 */
void init_plog_global_mem(void)
{
    /*
     * we use "t_thrd.log_cxt.plog_md_read_entry is null pointer" as the IF judgement.
     * so all memories like t_thrd.log_cxt.plog_md_read_entry MUST be malloced in IF
     * branch.
     */
    if (NULL == t_thrd.log_cxt.plog_md_read_entry) {
        MemoryContext oldMemCnxt = MemoryContextSwitchTo(t_thrd.mem_cxt.profile_log_mem_cxt);

        t_thrd.log_cxt.plog_md_read_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_md_read_entry[0] = t_thrd.log_cxt.plog_md_read_entry[1] = PLOG_NOT_USED;
        t_thrd.log_cxt.plog_md_write_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_md_write_entry[0] = t_thrd.log_cxt.plog_md_write_entry[1] = PLOG_NOT_USED;

        t_thrd.log_cxt.plog_obs_list_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_obs_list_entry[0] = t_thrd.log_cxt.plog_obs_list_entry[1] = PLOG_NOT_USED;
        t_thrd.log_cxt.plog_obs_read_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_obs_read_entry[0] = t_thrd.log_cxt.plog_obs_read_entry[1] = PLOG_NOT_USED;
        t_thrd.log_cxt.plog_obs_write_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_obs_write_entry[0] = t_thrd.log_cxt.plog_obs_write_entry[1] = PLOG_NOT_USED;

        t_thrd.log_cxt.plog_hdp_read_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_hdp_read_entry[0] = t_thrd.log_cxt.plog_hdp_read_entry[1] = PLOG_NOT_USED;
        t_thrd.log_cxt.plog_hdp_write_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_hdp_write_entry[0] = t_thrd.log_cxt.plog_hdp_write_entry[1] = PLOG_NOT_USED;
        t_thrd.log_cxt.plog_hdp_open_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_hdp_open_entry[0] = t_thrd.log_cxt.plog_hdp_open_entry[1] = PLOG_NOT_USED;

        t_thrd.log_cxt.plog_remote_read_entry = (char*)palloc0(PLOG_ENTRY_MAX_SIZE);
        t_thrd.log_cxt.plog_remote_read_entry[0] = t_thrd.log_cxt.plog_remote_read_entry[1] = PLOG_NOT_USED;

        (void)MemoryContextSwitchTo(oldMemCnxt);

        t_thrd.log_cxt.g_plog_msgmem_array[DS_MD][DSRQ_LIST] = NULL;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_MD][DSRQ_READ] = t_thrd.log_cxt.plog_md_read_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_MD][DSRQ_WRITE] = t_thrd.log_cxt.plog_md_write_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_MD][DSRQ_OPEN] = NULL;

        t_thrd.log_cxt.g_plog_msgmem_array[DS_OBS][DSRQ_LIST] = t_thrd.log_cxt.plog_obs_list_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_OBS][DSRQ_READ] = t_thrd.log_cxt.plog_obs_read_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_OBS][DSRQ_WRITE] = t_thrd.log_cxt.plog_obs_write_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_OBS][DSRQ_OPEN] = NULL;

        t_thrd.log_cxt.g_plog_msgmem_array[DS_HADOOP][DSRQ_LIST] = NULL;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_HADOOP][DSRQ_READ] = t_thrd.log_cxt.plog_hdp_read_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_HADOOP][DSRQ_WRITE] = t_thrd.log_cxt.plog_hdp_write_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_HADOOP][DSRQ_OPEN] = t_thrd.log_cxt.plog_hdp_open_entry;

        t_thrd.log_cxt.g_plog_msgmem_array[DS_REMOTE_DATANODE][DSRQ_LIST] = NULL;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_REMOTE_DATANODE][DSRQ_READ] = t_thrd.log_cxt.plog_remote_read_entry;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_REMOTE_DATANODE][DSRQ_WRITE] = NULL;
        t_thrd.log_cxt.g_plog_msgmem_array[DS_REMOTE_DATANODE][DSRQ_OPEN] = NULL;
    }
}

/*
 * force to flush all the profile logs into syslogger and write them.
 * it's triggered before this backend thread exits.
 * notice this is not forced when a transaction is committed or aborted,
 * just deferred when the next new transaction starts. see also plog_switch_new_msg()
 */
void flush_plog(void)
{
    /*
     * this interface may be called under bootstrap/standalone mode,
     * where all logs are output directly to standard output, including
     * profile log data.
     * we don't expect this happen, so restrict it when IsUnderPostmaster = true.
     */
    if (GTM_FREE_MODE) {
        return;
    }

    if (!IsUnderPostmaster) {
        return;
    }

    for (int i = 0; i < DS_VALID_NUM; i++) {
        for (int j = 0; j < DSRQ_VALID_NUM; j++) {
            if (t_thrd.log_cxt.g_plog_msgmem_array[i][j]) {
                write_logmsg_with_multi_items((PLogMsg*)t_thrd.log_cxt.g_plog_msgmem_array[i][j]);
            }
        }
    }
}

/*
 * @Description: aggregate profile logs about MD IO, and write the existing
 *   data if necessary ( new transaction starts, global query id changes,
 *   aggregation time is enough, etc).
 * @Param[IN] new_item: a new input indicator about read/write item
 * @Param[IN] nowtm: the current time
 * @Return: void
 * @See also:
 */
void aggregate_profile_logs(IndicatorItem* new_item, struct timeval* nowtm)
{
    /*
     * disk IO error should not be common states, so make it a
     * single one record.
     */
    if (RET_TYPE_FAIL == new_item->ret_type) {
        write_logmsg_with_one_item(new_item, nowtm);
        return;
    }

    PLogMsg* prfdata = (PLogMsg*)t_thrd.log_cxt.g_plog_msgmem_array[new_item->data_src][new_item->req_type];
    LogPipeProtoHeader* proto = (LogPipeProtoHeader*)prfdata->msghead;
    PLogBasicEntry* entry = &prfdata->msgbody.entry;
    PLogEntryMeta* meta = &(entry->basic.meta);
    PLogEntryHead* head = &(entry->basic.head);
    PLogEntryItem* item = &(entry->basic.item[0]);
    PLogEntryItem* tmp_item = NULL;
    int slotno = 0;

    /* the following codes will be hit at 99% case. */

    slotno = (*(g_plog_slot_array[new_item->data_src][new_item->req_type]))(new_item);
    Assert(slotno < (int)PLOG_ENTRY_MAX_NUM);
    tmp_item = item + slotno;

    if (PLOG_NOT_USED == proto->nuls[0] || PLOG_NOT_USED == proto->nuls[1]) {
        /* the first time to log profiling data in this thread */
        plog_fill_fixed_proto(proto);

        plog_record_init_meta(meta, nowtm);
        plog_record_init_head(head, 0, new_item->data_src, new_item->req_type, RET_TYPE_OK);
        plog_record_init_items(item, PLOG_ENTRY_MAX_NUM);
    } else if (plog_switch_new_msg(nowtm, meta->req_time) || (meta->gxid != GetTopTransactionIdIfAny()) ||
               (meta->gqid != (uint64)(u_sess->debug_query_id)) ||
               UINT32_SUM_OVERFLOW(tmp_item->sum_usec, new_item->req_usec) ||
               UINT32_SUM_OVERFLOW(tmp_item->sum_size, new_item->dat_size)
        /* needn't to check tmp_item->sum_count, because it's added only by 1 */
    ) {
        /* first write this plog */
        write_logmsg_with_multi_items(prfdata);

        /* prepare for the next plog */
        proto->len = 0; /* updated later */
        plog_record_init_meta(meta, nowtm);
        plog_record_init_head(head, 0, new_item->data_src, new_item->req_type, RET_TYPE_OK);
        plog_record_init_items(item, PLOG_ENTRY_MAX_NUM);
    }
    Assert(head->item_num <= PLOG_ENTRY_MAX_NUM);

    if (0 == tmp_item->sum_count) {
        /* update the number of this record */
        ++head->item_num;
        tmp_item->sum_count = 1;
    } else {
        ++tmp_item->sum_count;
    }
    tmp_item->sum_usec += new_item->req_usec;
    tmp_item->sum_size += new_item->dat_size;
}

/*
 * @Description: init logging protocol information.
 *	  LogPipeProtoHeader::len must be updated later before writing.
 * @Param[IN] proto: protocol data
 * @Return: void
 * @See also:
 */
static inline void plog_fill_fixed_proto(LogPipeProtoHeader* proto)
{
    proto->nuls[0] = proto->nuls[1] = '\0';
    proto->len = 0; /* updated later */
    proto->pid = t_thrd.proc_cxt.MyProcPid;
    proto->logtype = (char)LOG_TYPE_PLOG;
    proto->is_last = 't';
    proto->magic = PROTO_HEADER_MAGICNUM;
}

/*
 * @Description: init meta data of this log record.
 * @Param[IN] meta: meta memory of log record
 * @Param[IN] tm: time info about requiring service
 * @Return: void
 * @See also:
 */
static inline void plog_record_init_meta(PLogEntryMeta* meta, struct timeval* tm)
{
    meta->req_time = *tm;
    meta->tid = t_thrd.proc_cxt.MyProcPid;
    meta->gxid = GetTopTransactionIdIfAny();
    meta->plog_magic = PLOG_ENTRY_MAGICNUM;
    meta->gqid = u_sess->debug_query_id;
}

/*
 * @Description: init head data of this log record.
 * @Param[IN] head: head memory of log record
 * @Param[IN] nitems: how many items within this log record
 * @Param[IN] ds: data source id
 * @Param[IN] req_type: required type, read/write etc.
 * @Param[IN] ret_type: result type, success or failed.
 * @Return: void
 * @See also:
 */
static inline void plog_record_init_head(PLogEntryHead* head, int nitems, char ds, char req_type, char ret_type)
{
    head->data_src = ds;
    head->req_type = req_type;
    head->item_num = nitems;
    head->ret_type = ret_type;
}

/*
 * @Description: init/reset items array
 * @Param[IN] item: items array
 * @Param[IN] n:    items number
 * @Return: void
 * @See also:
 */
static inline void plog_record_init_items(PLogEntryItem* item, int n)
{
    int rc = memset_s(item, sizeof(PLogEntryItem) * n, 0, sizeof(PLogEntryItem) * n);
    securec_check_c(rc, "\0", "\0");
}

/*
 * @Description: judge whether switch to a new log record
 * @Param[IN] current: current timestamp
 * @Param[IN] last: last timestamp
 * @Return: void
 * @See also:
 */
static inline bool plog_switch_new_msg(struct timeval* current, struct timeval last)
{
    struct timeval nowtime = *current;
    INSTR_TIME_SUBTRACT(nowtime, last);
    return TM_IS_BIGGER(nowtime, t_thrd.log_cxt.plog_msg_switch_tm);
}

/*
 * @Description: write a single profile log directly into log file.
 * @Param[IN] item: an input indicator item
 * @Param[IN] nowtm: current timestamp
 * @Return: void
 * @See also:
 */
static inline void write_logmsg_with_one_item(IndicatorItem* item, struct timeval* nowtm)
{
    int size = offsetof(PLogMsg, msgbody) + offsetof(PLogBasicEntry, basic) + offsetof(PLogEntry, item) +
               sizeof(PLogEntryItem);
    PLogMsg* msg = (PLogMsg*)palloc0(size);
    /* fill protocol part */
    LogPipeProtoHeader* proto = (LogPipeProtoHeader*)&msg->msghead;
    plog_fill_fixed_proto(proto);
    proto->len = sizeof(PLogEntry);

    /* fill meta part */
    plog_record_init_meta(&(msg->msgbody.entry.basic.meta), nowtm);

    /* fill head part */
    plog_record_init_head(&(msg->msgbody.entry.basic.head), 1, item->data_src, item->req_type, item->ret_type);

    /* fill items part */
    PLogEntryItem* tmp_item = &(msg->msgbody.entry.basic.item[0]);
    tmp_item->sum_count = 1;
    tmp_item->sum_size = item->dat_size;
    tmp_item->sum_usec = item->req_usec;

    /* write this log into stderr */
    {
        Assert(int(LOGPIPE_HEADER_SIZE + proto->len) <= size);
        int fd = fileno(stderr);
        (void)write(fd, (char*)msg, LOGPIPE_HEADER_SIZE + proto->len);
    }
    pfree(msg);
    msg = NULL;
}

/*
 * there are all 32 slots avaiable, and we these ranges:
 * R1: step = 16us,
 *	   0 ~ 16, < 32, < 48, ..., < 320
 * R2:
 *	   < 512, < 1024
 * R3: step = 2 ms
 *	   < 2ms, < 4ms, ..., < 16ms, < 18ms
 * R4:
 *	   the others time
 */
#define MD_R1_STEP 16   /* us */
#define MD_R1_BOUND 320 /* us */
#define MD_SLOT_BASE1 (MD_R1_BOUND / MD_R1_STEP)
#define MD_R2_BOUND1 512  /* us */
#define MD_R2_BOUND2 1024 /* us */
#define MD_SLOT_BASE2 (MD_SLOT_BASE1 + 2)
#define MD_R3_STEP 2000   /* us */
#define MD_R3_BOUND 18000 /* us */
#define MD_SLOT_BASE3 (MD_SLOT_BASE2 + MD_R3_BOUND / MD_R3_STEP)

/*
 * @Description: compute which slot to store this new item.
 *      Core codes about how to collect profile data.
 * @Param[IN] item: an input indicator item to search
 * @Return: which slot to store this new item
 * @See also: macro defination above
 */
static int plog_md_get_slot(IndicatorItem* item)
{
    Assert(MD_SLOT_BASE3 < PLOG_ENTRY_MAX_NUM);
    if (item->req_usec < MD_R1_BOUND) {
        /* slot range: 0 ~ 19 */
        return (item->req_usec / MD_R1_STEP);
    }

    if (item->req_usec < MD_R2_BOUND2) {
        /* slot range: 20, 21 */
        return (item->req_usec < MD_R2_BOUND1) ? MD_SLOT_BASE1 : (MD_SLOT_BASE1 + 1);
    }

    if (item->req_usec < MD_R3_BOUND) {
        /* slot range: 22 ~ 30 */
        return MD_SLOT_BASE2 + (item->req_usec / MD_R3_STEP);
    }

    /* slot: 31 */
    return MD_SLOT_BASE3;
}

#define OBS_R1_BOUND 900000 /* 900ms */
#define OBS_R1_STEP 30000   /*  30ms */
#define OBS_R1_BASE1 (OBS_R1_BOUND / OBS_R1_STEP)
#define OBS_R2_BOUND 2000000 /* 2s */

static int plog_obs_get_slot(IndicatorItem* item)
{
    Assert(OBS_R1_BASE1 < PLOG_ENTRY_MAX_NUM);
    if (item->req_usec < OBS_R1_BOUND) {
        /* slot range: 0 ~ 29 */
        return (item->req_usec / OBS_R1_STEP);
    }
    /* slot range: 30, 31 */
    return OBS_R1_BASE1 + ((item->req_usec < OBS_R2_BOUND) ? 0 : 1);
}

/* results of log2 whose rang is 0~1023 */
#define LOG2_MAP_SIZE 1024
static const int log2_map[LOG2_MAP_SIZE] = {0,
    0,
    1,
    2,
    2,
    3,
    3,
    3,
    3,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    4,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    5,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    9,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10,
    10};

/*
 * there are all 32 slots avaiable, and we these ranges:
 * R1: step = 50us,
 *	   0 ~ 50, < 100, < 150, ..., < 1000
 * R2: step = 2^n ms
 *	   < 2ms, < 4ms, <8ms..., < 512ms, < 1024ms
 * R3:
 *	   the other times
 */
#define HDP_R1_STEP 50    /* us */
#define HDP_R1_BOUND 1000 /* us */
#define HDP_SLOT_BASE1 (HDP_R1_BOUND / HDP_R1_STEP)
#define HDP_R2_BOUND 1024000 /* us */
#define HDP_SLOT_BASE2 (HDP_SLOT_BASE1 + 11)

static int plog_hdp_get_slot(IndicatorItem* item)
{
    Assert(HDP_SLOT_BASE2 < PLOG_ENTRY_MAX_NUM);
    if (item->req_usec < HDP_R1_BOUND) {
        /* slot range: 0 ~ 19 */
        return (item->req_usec / HDP_R1_STEP);
    }

    if (item->req_usec < HDP_R2_BOUND) {
        /* slot range: 20 ~ 30 */
        uint32 idx = item->req_usec / 1000;
        Assert(idx < LOG2_MAP_SIZE);
        return HDP_SLOT_BASE1 + log2_map[idx];
    }

    /* slot: 31 */
    return HDP_SLOT_BASE2;
}

/*
 * there are all 21 slots avaiable, and we these ranges:
 * R1: step = 100ms,
 *	   0 ~ 100, < 200, < 300, ..., < 1000
 * R2: step = 1000ms
 *	   <1000, <2000,..., < 10000
 * R3:
 *	   the others time
 */
#define REMOTE_R1_STEP 100000   /* 100 ms */
#define REMOTE_R1_BOUND 1000000 /* 1 s */
#define REMOTE_SLOT_BASE1 (REMOTE_R1_BOUND / REMOTE_R1_STEP)
#define REMOTE_R2_STEP 1000000   /* 1 s */
#define REMOTE_R2_BOUND 10000000 /* 10 s */
#define REMOTE_SLOT_BASE2 (REMOTE_SLOT_BASE1 + (REMOTE_R2_BOUND / REMOTE_R2_STEP))

/*
 * @Description: compute which slot to store this new item.
 *      Core codes about how to collect profile data.
 * @Param[IN] item: an input indicator item to search
 * @Return: which slot to store this new item
 * @See also: macro defination above
 */
static int plog_remote_get_slot(IndicatorItem* item)
{
    Assert(REMOTE_SLOT_BASE2 < PLOG_ENTRY_MAX_NUM);
    if (item->req_usec < REMOTE_R1_BOUND) {
        /* slot range: 0 ~ 9 */
        return (item->req_usec / REMOTE_R1_STEP);
    }

    if (item->req_usec < REMOTE_R2_BOUND) {
        /* slot range: 10, 19 */
        return REMOTE_SLOT_BASE1 + (item->req_usec / REMOTE_R2_STEP);
    }

    /* slot: 20 */
    return REMOTE_SLOT_BASE2;
}

/*
 * @Description: write a plog message into syslogger thread,
 *      which will flush this message into files/disk.
 * @Param[IN] plog_msg: a log message data
 * @Return: void
 * @See also: write_logmsg_with_one_item()
 */
static void write_logmsg_with_multi_items(PLogMsg* plog_msg)
{
    LogPipeProtoHeader* proto = (LogPipeProtoHeader*)plog_msg->msghead;
    PLogBasicEntry* entry = &plog_msg->msgbody.entry;
    PLogEntryItem* items = &(entry->basic.item[0]);
    int const nitems = entry->basic.head.item_num;

    if (PLOG_NOT_USED == proto->nuls[0] || PLOG_NOT_USED == proto->nuls[1] || 0 == nitems) {
        return; /* needn't do anything */
    }

    int left_items_num = 0;
    PLogEntryItem* left = items;
    PLogEntryItem* right = items + (PLOG_ENTRY_MAX_NUM - 1);
    PLogEntryItem* const soldier = items + nitems;

    do {
        /* find the next empty slot from the left */
        while (left->sum_count != 0 && left < soldier) {
            ++left_items_num;
            ++left;
        }

        /* we expect all the items are placed on the left side, no empty slot */
        if (left == soldier) {
            break;
        }

        /* find the next used slot from the right */
        while (right->sum_count == 0 && right > soldier) {
            --right;
        }
        Assert(left < right);

        /* move the right used-slot to the left empty-slot */
        left->sum_count = right->sum_count;
        left->sum_size = right->sum_size;
        left->sum_usec = right->sum_usec;

        /* update all these vars */
        ++left_items_num;
        ++left;
        --right;
    } while (left < soldier);
    Assert(left_items_num == nitems);

    proto->len = offsetof(PLogEntry, item) + sizeof(PLogEntryItem) * nitems;
    {
        /* write this log into stderr */
        int fd = fileno(stderr);
        (void)write(fd, (char*)plog_msg, LOGPIPE_HEADER_SIZE + proto->len);
    }

    /* set the number of entries be 0 */
    entry->basic.head.item_num = 0;
}
