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
 *---------------------------------------------------------------------------------------
 *
 *  encrypt.cpp
 *        Add encrypt function
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/encrypt.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include "securec.h"
#include "securec_check.h"
#include "cipher.h"
#include "crypt.h"
#include "bin/elog.h"

#define PROG_NAME "gs_encrypt"

static int check_key_num(const char* password);
static void create_child_dir(const char* pathdir);

void check_path(const char *path_name)
{
    const char* danger_character_list[] = {"|",
        ";",
        "&",
        "$",
        "<",
        ">",
        "`",
        "\\",
        "'",
        "\"",
        "{",
        "}",
        "(",
        ")",
        "[",
        "]",
        "~",
        "*",
        "?",
        "!",
        "\n",
        NULL};
    int i = 0;

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(path_name, danger_character_list[i]) != NULL) {
            fprintf(
                    stderr, "invalid token \"%s\" in path name: (%s)\n", danger_character_list[i], path_name);
            exit(1);
        }
    }
}

static int check_key_num(const char* password)
{
    int key_len = 0;
    if (password == NULL) {
        (void)fprintf(stderr, _("Invalid password,please check it\n"));
        return 0;
    }
    key_len = strlen(password);
    if (key_len > MAX_CRYPT_LEN) {
        (void)fprintf(stderr, _("Invalid password,the length exceed %d\n"), MAX_CRYPT_LEN);
        return 0;
    }
    return key_len;
}

static void create_child_dir(const char* pathdir)
{
    if (pathdir == NULL) {
        (void)fprintf(stderr, _("ERROR: creat directory %s failed: invalid path <NULL>\n"), pathdir);
        exit(EXIT_FAILURE);
    }
    /* check whether directory is exits or not, if not exit then mkdir it */
    if (-1 == access(pathdir, F_OK)) {
        if (mkdir(pathdir, S_IRWXU) < 0) {
            (void)fprintf(stderr, _("ERROR: creat directory %s failed: %s\n"), pathdir, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    if (-1 == access(pathdir, R_OK | W_OK)) {
        (void)fprintf(stderr, _("ERROR: Could not access the path: %s\n"), pathdir);
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char* argv[])
{
    int result = -1;
    int key_num = 0;
    int key_child_num = 0;
    int i = 0;
    int j = 0;
    char* key_child[MAX_CHILD_NUM];
    char* path_child[MAX_CHILD_NUM];
    char* keyword = NULL;
    errno_t rc = EOK;
    FILE* cmd_fp = NULL;
    char* mv_cmd = NULL;
    char* keypath[2];   /* we can specify 2 path,the first is the cipher path,the second is the rand path. */

    keypath[0] = NULL;
    keypath[1] = NULL;

    for (i = 0; i < MAX_CHILD_NUM; i++) {
        key_child[i] = NULL;
        path_child[i] = NULL;
    }

    if (argc != 3 && argc != 4) {
        (void)fprintf(stderr, _("ERROR: invalid parameter\n"));
        return result;
    }

    key_num = check_key_num(argv[1]);
    if (key_num == 0) {
        (void)fprintf(stderr, _("ERROR: invalid passwd length\n"));
        return result;
    }

    key_child_num = key_num / KEY_SPLIT_LEN + 1;
    keyword = (char*)crypt_malloc_zero(KEY_SPLIT_LEN * key_child_num);
    if (NULL == keyword) {
        (void)fprintf(stderr, _("out of memory\n"));
        return result;
    }
    rc = memcpy_s(keyword, KEY_SPLIT_LEN * key_child_num, argv[1], key_num + 1);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(argv[1], key_num, 0, key_num);
    securec_check_c(rc, "\0", "\0");

    for (i = 2, j = 0; i < argc; i++, j++) {
        if (strlen(argv[i]) > MAX_CHILD_PATH) {
            (void)fprintf(stderr, _("ERROR: path %s length is more then %d\n"), argv[i], MAX_CHILD_PATH);
            goto END;
        }

        keypath[j] = strdup(argv[i]);
        if (NULL == keypath[j]) {
            (void)write_stderr(_("%s: out of memory\n"), "gs_guc");
            exit(1);
        }
        canonicalize_path(keypath[j]);
        if (-1 == access(keypath[j], R_OK | W_OK)) {
            (void)fprintf(stderr, _("ERROR: Could not access the path %s\n"), keypath[j]);
            goto END;
        }
        check_path(keypath[j]);
    }

    init_log((char*)PROG_NAME);

    for (i = 0; i < key_child_num; i++) {
        key_child[i] = (char*)crypt_malloc_zero(KEY_SPLIT_LEN + 1);
        rc = memcpy_s(key_child[i], KEY_SPLIT_LEN, keyword + (i * KEY_SPLIT_LEN), KEY_SPLIT_LEN);
        securec_check_c(rc, "\0", "\0");

        path_child[i] = (char*)crypt_malloc_zero(MAX_CHILD_PATH + 1);
        rc = snprintf_s(path_child[i], MAX_CHILD_PATH, MAX_CHILD_PATH - 1, "%s/key_%d", keypath[0], i);
        securec_check_ss_c(rc, "\0", "\0");

        create_child_dir(path_child[i]);
        gen_cipher_rand_files(SERVER_MODE, key_child[i], "newsql", path_child[i], NULL);

        if (argc == 4) {
            CRYPT_FREE(path_child[i]);
            path_child[i] = (char*)crypt_malloc_zero(MAX_CHILD_PATH + 1);
            rc = snprintf_s(path_child[i], MAX_CHILD_PATH, MAX_CHILD_PATH - 1, "%s/key_%d", keypath[1], i);
            securec_check_ss_c(rc, "\0", "\0");

            create_child_dir(path_child[i]);

            mv_cmd = (char*)crypt_malloc_zero(MAX_COMMAND_LEN + 1);
            rc = snprintf_s(mv_cmd, MAX_COMMAND_LEN, MAX_COMMAND_LEN - 1, "mv %s/key_%d/*.rand %s/key_%d/",
                            keypath[0], i, keypath[1], i);
            securec_check_ss_c(rc, "\0", "\0");

            cmd_fp = popen(mv_cmd, "r");
            if (NULL == cmd_fp) {
                perror("could not open mv command.\n");
                CRYPT_FREE(mv_cmd);
                CRYPT_FREE(key_child[i]);
                CRYPT_FREE(path_child[i]);
                goto END;
            }
            pclose(cmd_fp);
            CRYPT_FREE(mv_cmd);
        }
        CRYPT_FREE(key_child[i]);
        CRYPT_FREE(path_child[i]);
    }

    printf("encrypt success.\n");
    result = 0;

END:
    if (keyword != NULL) {
        rc = memset_s(keyword, KEY_SPLIT_LEN * key_child_num, 0, KEY_SPLIT_LEN * key_child_num);
        securec_check_c(rc, "\0", "\0");
        CRYPT_FREE(keyword);
    }
    CRYPT_FREE(keypath[0]);
    CRYPT_FREE(keypath[1]);
    return result;
}
