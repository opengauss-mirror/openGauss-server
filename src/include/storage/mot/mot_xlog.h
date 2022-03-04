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
 * mot_xlog.h
 *    Definitions of MOT xlog interface
 *
 * IDENTIFICATION
 *    src/include/storage/mot/mot_xlog.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef MOT_XLOG_H
#define MOT_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

extern void MOTRedo(XLogReaderState* record);
extern void MOTDesc(StringInfo buf, XLogReaderState* record);
extern const char* MOT_type_name(uint8 subtype);

#endif /* MOT_XLOG_H */
