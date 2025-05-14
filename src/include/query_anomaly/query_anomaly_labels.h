/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * IDENTIFICATION
 *   src/include/gs_policy/query_anomaly_labels.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef QUERY_ANOMALY_LABELS_H_
#define QUERY_ANOMALY_LABELS_H_
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"

const bool get_query_anomaly_labels(const List *relationOids);
void load_query_anomaly_labels(const bool reload = false);
void finish_query_anomaly_labels();
#endif /* QUERY_ANOMALY_LABELS_H_ */
