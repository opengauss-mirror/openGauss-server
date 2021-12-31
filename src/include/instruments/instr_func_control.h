/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * instr_func_control.h
 *        definitions for dynamic function control manager
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/instr_func_control.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef INSTR_FUNC_CONTROL_H
#define INSTR_FUNC_CONTROL_H
#include "instruments/instr_statement.h"

extern void InitTrackStmtControl();
extern StatLevel instr_track_stmt_find_level();

#endif
