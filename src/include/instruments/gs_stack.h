/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * gs_stack.h
 *
 * IDENTIFICATION
 *        src/include/instruments/gs_stack.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_STACK_H
#define GS_STACK_H

#define STACK_PRINT_LIMIT 128

typedef struct BACKTRACE_INFO {
    void *backtrace_buff[STACK_PRINT_LIMIT];
    int number_pc;
    volatile uint32 refresh_flag;
} BACKTRACE_INFO_t;

Datum gs_stack(PG_FUNCTION_ARGS);
extern void print_stack(SIGNAL_ARGS);
void InitGsStack();
void print_all_stack();
void get_stack_and_write_result();
void check_and_process_gs_stack();
void get_stack_according_to_tid(ThreadId tid, StringInfoData* call_stack);
NON_EXEC_STATIC void stack_perf_main();
#endif

