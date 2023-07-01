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
 * xlog_read.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/xlog_read.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_XLOG_READ_H
#define ONDEMAND_EXTREME_RTO_XLOG_READ_H

#include "access/xlog_basic.h"

namespace ondemand_extreme_rto {
XLogRecord* XLogParallelReadNextRecord(XLogReaderState* xlogreader);
XLogRecord *ReadNextXLogRecord(XLogReaderState **xlogreaderptr, int emode);

}  // namespace ondemand_extreme_rto
#endif /* ONDEMAND_EXTREME_RTO_XLOG_READ_H */