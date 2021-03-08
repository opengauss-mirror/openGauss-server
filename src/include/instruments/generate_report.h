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
 * generate_report.h
 *        definitions for WDR report.
 *
 *   Before generating a report, you need to generate at least two WDR snapshot.
 *   A WDR report is used to show the performance of the database between snapshots.
 *
 *
 * IDENTIFICATION
 *        src/include/instruments/generate_report.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef GENER_REPORT
#define GENER_REPORT

#include <vector>

extern Datum generate_wdr_report(PG_FUNCTION_ARGS);
extern char* Datum_to_string(Datum value, Oid type, bool isnull);
extern void DeepListFree(List* deepList, bool deep);

#endif
