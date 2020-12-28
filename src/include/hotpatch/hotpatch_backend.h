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
 * hotpatch_backend.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/hotpatch/hotpatch_backend.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef HOTPATCH_BACKEND_H
#define HOTPATCH_BACKEND_H

typedef struct knl_hotpatch_context {
    bool under_patching;
} knl_hotpatch_context_t;

void check_and_process_hotpatch(void);
int hotpatch_remove_signal_file(const char* data_dir);
#endif
