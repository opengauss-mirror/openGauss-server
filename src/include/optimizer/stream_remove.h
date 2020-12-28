/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN ""AS IS"" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * stream_remove.h
 *
 * IDENTIFICATION
 *    src/include/optimizer/stream_remove.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STREAM_REMOVE_H
#define STREAM_REMOVE_H
#include "nodes/plannodes.h"
#include "optimizer/pgxcplan.h"
void delete_redundant_streams_of_append_plan(const Append *append);
void delete_redundant_streams_of_remotequery(RemoteQuery *top_plan);

#endif