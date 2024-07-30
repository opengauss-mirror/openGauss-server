/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * elog.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/bin/elog.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COMMON_H
#define COMMON_H

extern void write_stderr(const char* fmt, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
extern void write_log(const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
extern void init_log(char* prefix_name);
extern void check_env_value_c(const char* input_env_value);
extern void check_env_name_c(const char* input_env_value);
extern void GenerateProgressBar(int percent, char* progressBar);

struct auditConfig
{
    bool has_init;
    bool is_success;
    const char *process_name;
    int argc;
    char **argv;
};

extern void init_audit(const char* prefix_name, int argc, char** argv); //register audit callback on exit
extern void audit_success();
#endif /* COMMON_H */
