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
 * hotpatch_client.cpp
 *
 *
 * IDENTIFICATION
 *        src/lib/hotpatch/client/hotpatch_client.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <stddef.h>
#include "securec.h"
#include "securec_check.h"

#include "hotpatch/hotpatch_client.h"
#include "hotpatch/hotpatch.h"
#include "hotpatch/hotpatch_inner.h"

static char* g_hotpatch_return_ok = "[PATCH-SUCCESS]";

char* strip_path_from_pathname(const char* name_withpath)
{
    char* name_without_path = NULL;

    if (name_withpath == NULL) {
        return NULL;
    }

    // no "/"
    name_without_path = strrchr((char*)name_withpath, '/');
    if (name_without_path == NULL) {
        return (char*)name_withpath;
    }

    // only "/"
    if (strlen(name_without_path) <= 1) {
        return NULL;
    }

    return (char*)(name_without_path + 1);
}

void hotpatch_patch_state_to_string(unsigned int state, char* state_string, int length_state_string)
{
    int ret = 0;

    const char* string_state[] = {
        "UNKNOWN",
        "UNLOAD",
        "DEACTIVE",
        "ACTIVE",
    };

    switch (state) {
        case HP_STATE_ACTIVED:
        case HP_STATE_DEACTIVE:
        case HP_STATE_UNLOAD:
            ret = snprintf_s(state_string, length_state_string, length_state_string - 1, "%s", string_state[state]);
            securec_check_ss_c(ret, "\0", "\0");
            break;

        default:
            ret = snprintf_s(state_string, length_state_string, length_state_string - 1, "%s", string_state[0]);
            securec_check_ss_c(ret, "\0", "\0");
            break;
    }

    return;
}

int hotpatch_check(const char* path, const char* command, bool* is_list)
{
    int i, cmd_number;
    const char* support_action[] = {"list", "load", "unload", "active", "deactive", "info"};

    if (command == NULL) {
        return -1;
    }

    cmd_number = sizeof(support_action) / sizeof(char*);
    for (i = 0; i < cmd_number; i++) {
        if (strncmp(support_action[i], command, g_max_length_act) == 0) {
            if (i == 0) {
                *is_list = true;
            } else if (path == NULL) {
                return -1;
            }
            return 0;
        }
    }

    return -1;
}

static FILE* hotpatch_open_patch_info_file(const char* data_dir, size_t dir_length, void (*canonicalize_path)(char*))
{
    int ret;
    char info_file[g_max_length_path] = {0};
    FILE* fp = NULL;

    if (dir_length >= g_max_length_path) {
        return NULL;
    }

    ret = snprintf_s(info_file, g_max_length_path, g_max_length_path - 1, "%s/hotpatch/patch.info", data_dir);
    securec_check_ss_c(ret, "\0", "\0");

    canonicalize_path(info_file);
    fp = fopen(info_file, "r");

    return fp;
}

static int hotpatch_output_list_from_patch_info(FILE* fp, LogFunc log_func)
{
    int ret;
    int i;
    int max_patch_number;
    char* patch_name = NULL;
    char patch_state_string[g_length_statstr] = {0};
    PATCH_INFO_HEADER_T hp_header = {0};
    PATCH_INFO_T patch_info = {0};

    if (fp == NULL) {
        return -1;
    }

    ret = fread(&hp_header, sizeof(PATCH_INFO_HEADER_T), 1, fp);
    if (ret != 1) {
        return -1;
    }

    max_patch_number = hp_header.max_patch_number;
    if ((max_patch_number < 0) || (max_patch_number > g_max_number_patch)) {
        return -1;
    }

    for (i = 0; i < max_patch_number; i++) {
        ret = fread(&patch_info, sizeof(PATCH_INFO_T), 1, fp);
        if (ret != 1) {
            return -1;
        }
        hotpatch_patch_state_to_string(patch_info.patch_state, patch_state_string, sizeof(patch_state_string));
        patch_info.patch_name[g_max_length_path - 1] = '\0';
        patch_name = strip_path_from_pathname(patch_info.patch_name);
        if (patch_name == NULL) {
            log_func("PATCH: UNKNOWN STATE: %s\n", patch_state_string);
            continue;
        }
        log_func("PATCH: %s STATE: %s \n", patch_name, patch_state_string);
    }

    return 0;
}

void hotpatch_process_list(const char* return_string, int string_length, const char* data_dir, size_t dir_length,
    void (*canonicalize_path)(char*), LogFunc log_func)
{
    FILE* fp = NULL;
    int ret = -1;

    if (strncmp(return_string, g_hotpatch_return_ok, g_length_okstr) == 0) {
        fp = hotpatch_open_patch_info_file(data_dir, dir_length, canonicalize_path);
        if (fp != NULL) {
            ret = hotpatch_output_list_from_patch_info(fp, log_func);
            fclose(fp);
            fp = NULL;
        }
        if (ret != 0) {
            log_func("[PATCH-ERROR] HOTPATCH LIST FAILED\n");
        } else {
            log_func("[PATCH-SUCCESS] LIST PATCH\n");
        }
    } else {
        if (strstr(return_string, "No patch loaded now") != NULL) {
            log_func("[PATCH-SUCCESS] LIST PATCH, NO PATCH LOADED!\n");
        } else {
            log_func("%s\n", return_string);
        }
    }

    return;
}
