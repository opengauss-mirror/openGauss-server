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
 * lsn_info_double_list.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/extreme_rto/standby_read/lsn_info_double_list.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LSN_INFO_DOUBLE_LIST_H
#define LSN_INFO_DOUBLE_LIST_H

#include "gs_thread.h"
#include "postgres.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"

namespace extreme_rto_standby_read {
typedef uint64 LsnInfoPosition;

static const LsnInfoPosition LSN_INFO_LIST_HEAD = 0xFFFFFFFFFFFFFFFFL;

#define INFO_POSITION_IS_VALID(p) ((p) != 0xFFFFFFFFFFFFFFFFL)
#define INFO_POSITION_IS_INVALID(p) ((p) == 0xFFFFFFFFFFFFFFFFL)
typedef struct _LsnInfoDoubleList {
    LsnInfoPosition prev;  // not pointer, is position in lsn info meta table
    LsnInfoPosition next;  // not pointer, is position in lsn info meta table
} LsnInfoDoubleList;

void lsn_info_list_init(LsnInfoDoubleList* node);
void info_list_modify_old_tail(StandbyReadMetaInfo *meta_info, LsnInfoPosition old_tail_pos,
    LsnInfoPosition insert_pos, XLogRecPtr current_page_lsn, XLogRecPtr next_lsn, bool is_lsn_info);
}  // namespace extreme_rto_standby_read
#endif