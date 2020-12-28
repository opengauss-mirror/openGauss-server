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
 * gs_bbox.h
 *
 * IDENTIFICATION
 *        src/include/gs_bbox.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __GS_BBOX_H__
#define __GS_BBOX_H__

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/guc.h"

/*
 * all kinds of blacklist items.
 *
 * NOTE: at most 64 different blacklist items are supported now. We can enlarge this upper limits
 * by changing g_instance.attr.attr_common.bbox_blacklist_mask from uint64 to Bitmap.
 */
typedef enum {
    SHARED_BUFFER = 0,
    XLOG_BUFFER,
    DW_BUFFER,
    XLOG_MESSAGE_SEND,
    DATA_MESSAGE_SEND,
    WALRECIVER_CTL_BLOCK,
    DATA_WRITER_QUEUE
} BlacklistIndex;

/* blacklist item */
typedef struct {
    /* blacklist item's ID */
    BlacklistIndex blacklist_ID;

    /* blacklist item's name */
    char *blacklist_name;

    /* whether or not only added by postmaster. */
    bool pm_only;
} BlacklistItem;

/* all information about blacklist items. */
extern BlacklistItem g_blacklist_items[];

/* whether or not bbox is enabled. */
#define BBOX_ENABLED (u_sess->attr.attr_common.enable_bbox_dump)

/* fetch instance's blacklist items' mask. */
#define BBOX_BLACKLIST (g_instance.attr.attr_common.bbox_blacklist_mask)

/* fetch one blacklist item's bits mask by ID (internal) */
#define BLACKLIST_ITEM_MASK(blacklist_ID) ((uint64)1 << (unsigned int)(blacklist_ID))

/* t_thrd.storage_cxt.BufferBlocks */
#define BBOX_BLACKLIST_SHARE_BUFFER (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(SHARED_BUFFER)))

/* t_thrd.shemem_ptr_cxt.XLogCtl->pages */
#define BBOX_BLACKLIST_XLOG_BUFFER (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(XLOG_BUFFER)))

/* g_instance.dw_batch_cxt.buf */
#define BBOX_BLACKLIST_DW_BUFFER (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(DW_BUFFER)))

/* t_thrd.walsender_cxt.output_xlog_message*/
#define BBOX_BLACKLIST_XLOG_MESSAGE_SEND (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(XLOG_MESSAGE_SEND)))

/* t_thrd.datasender_cxt.output_message */
#define BBOX_BLACKLIST_DATA_MESSAGE_SEND (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(DATA_MESSAGE_SEND)))

/* t_thrd.walreceiver_cxt.walRcvCtlBlock */
#define BBOX_BLACKLIST_WALREC_CTL_BLOCK (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(WALRECIVER_CTL_BLOCK)))

/*t_thrd.dataqueue_cxt.DataWriterQueue*/
#define BBOX_BLACKLIST_DATA_WRITER_QUEUE (BBOX_ENABLED && (BBOX_BLACKLIST & BLACKLIST_ITEM_MASK(DATA_WRITER_QUEUE)))

// t_thrd.walsender_cxt.output_data_message, LogicalDecodingContext

// t_thrd.libwalreceiver_cxt.recvBuf  (malloc each time)
// t_thrd.datareceiver_cxt.recvBuf
// XLogReaderState->readbuf

#ifdef ENABLE_UT
#include "../gausskernel/cbb/bbox/bbox.h"
extern int bbox_handler_exit;
extern void bbox_handler(SIGNAL_ARGS);
extern s32 BBOX_CreateCoredump(char* file_name);
#endif

/* do initilaization for dumping core file */
extern void bbox_initialize();

/* add an blacklist item to exclude it from core file. */
extern void bbox_blacklist_add(BlacklistIndex item, void *addr, uint64 size);

/* remove an blacklist item. */
extern void bbox_blacklist_remove(BlacklistIndex item, void *addr);

/* for GUC parameter - bbox_dump_path */
extern bool check_bbox_corepath(char** newval, void** extra, GucSource source);
extern void assign_bbox_corepath(const char* newval, void* extra);
extern const char* show_bbox_dump_path(void);

/* for GUC parameter - bbox_blanklist_items */
extern bool check_bbox_blacklist(char** newval, void** extra, GucSource source);
extern void assign_bbox_blacklist(const char* newval, void* extra);
extern const char* show_bbox_blacklist();

/* for GUC parameter - enable_bbox_dump */
extern void assign_bbox_coredump(const bool newval, void* extra);
extern int CheckFilenameValid(const char* inputEnvValue);
#endif

