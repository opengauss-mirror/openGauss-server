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
 * -------------------------------------------------------------------------
 *
 * convjson_common.cpp
 *      functions to create/read/write/remove files, used for convert json file into code file.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/convert_jsonfile/convjson_common.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "convjson_common.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include "securec.h"
#include "securec_check.h"

CmkemErrCode check_file_path(const char *file_path)
{
    const char danger_char_list[] = {'|', ';', '&', '$', '<', '>', '`', '\\', '\'', '\"', '{', '}', '(', ')', '[',']',
        '~', '*', '?', '!'};

    for (size_t i = 0; i < strlen(file_path); i++) {
        for (size_t j = 0; j < sizeof(danger_char_list); j++) {
            if (file_path[i] == danger_char_list[j]) {
                cmkem_errmsg("the path '%s' contains invalid character '%c'.", file_path, file_path[i]);
                return CMKEM_CHECK_ENV_VAL_ERR;
            }
        }
    }
    
    return CMKEM_SUCCEED;
}

CmkemErrCode check_and_realpath(const char *file_path, char *real_file_path, size_t real_file_path_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (real_file_path_len < PATH_MAX) {
        cmkem_errmsg("failed to convert '%s' to real path, the length of dest buffer is shorter than PATH_MAX.",
            file_path);
        return CMKEM_CHECK_BUF_LEN_ERR;
    }

    ret = check_file_path(file_path);
    check_kmsret(ret, ret);

    realpath(file_path, real_file_path);

    return ret;
}

CmkemErrCode create_file(const char *file_path)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    int fd = open(real_file_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        cmkem_errmsg("failed to create file '%s'.", real_file_path);
        return CMKEM_CREATE_FILE_ERR;
    }

    close(fd);
    return ret;
}

CmkemErrCode read_file_size(const char *file_path, size_t *file_size)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    struct stat file_stat = {0};
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    if (stat(real_file_path, &file_stat) != 0) {
        cmkem_errmsg("failed to read status of file '%s'.", real_file_path);
        return CMKEM_READ_FILE_STATUS_ERR;
    }

    *file_size = file_stat.st_size;
    return ret;
}

CmkemErrCode read_file_content(const char *file_path, size_t read_len, char *buf, size_t buf_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int fd = 0;
    char real_file_path[PATH_MAX] = {0};

    if (buf_len < read_len) {
        cmkem_errmsg("faield to read file '%s', content buffer is too short.", file_path);
        return CMKEM_CHECK_BUF_LEN_ERR;
    }

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    fd = open(real_file_path, O_RDONLY, 0);
    if (fd < 0) {
        cmkem_errmsg("failed to open file '%s'.", real_file_path);
        return CMKEM_OPEN_FILE_ERR;
    }

    if (read(fd, buf, read_len) < 0) {
        cmkem_errmsg("failed to read file '%s'.", real_file_path);
        close(fd);
        return CMKEM_READ_FILE_ERR;
    }

    close(fd);
    return ret;
}

CmkemStr *read_file(const char *file_path)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int fd = 0;
    char real_file_path[PATH_MAX] = {0};
    size_t file_size = 0;
    CmkemStr *file_content = NULL;

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, NULL);

    ret = read_file_size(real_file_path, &file_size);
    check_kmsret(ret, NULL);

    file_content = malloc_cmkem_str(file_size + 1);
    if (file_content == NULL) {
        return NULL;
    }

    fd = open(real_file_path, O_RDONLY, 0);
    if (fd < 0) {
        cmkem_errmsg("failed to open file '%s'.", real_file_path);
        return NULL;
    }

    if (read(fd, file_content->str_val, file_size) < 0) {
        cmkem_errmsg("failed to read file '%s'.\n", real_file_path);
        close(fd);
        return NULL;
    }

    close(fd);
    file_content->str_len = file_size;
    return file_content;
}

CmkemErrCode write_content(const char *file_path, const char* buf, size_t buf_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int fd = 0;
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    fd = open(real_file_path, O_WRONLY, 0);
    if (fd < 0) {
        cmkem_errmsg("failed to open file '%s'.", real_file_path);
        return CMKEM_OPEN_FILE_ERR;
    }

    if (write(fd, buf, buf_len) < 0) {
        cmkem_errmsg("failed to write to file '%s'.\n", real_file_path);
        close(fd);
        return CMKEM_WRITE_FILE_ERR;
    }

    close(fd);
    return ret;
}

CmkemErrCode write_content_with_create(const char *file_path, const char* buf, size_t buf_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int fd = 0;
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    fd = open(real_file_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        cmkem_errmsg("failed to create file '%s'.", real_file_path);
        return CMKEM_CREATE_FILE_ERR;
    }

    if (write(fd, buf, buf_len) < 0) {
        cmkem_errmsg("failed to write to file '%s'.\n", real_file_path);
        close(fd);
        return CMKEM_WRITE_FILE_ERR;
    }

    close(fd);
    return ret;
}

CmkemErrCode write_content_to_tail(const char *file_path, const char* buf, size_t buf_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int fd = 0;
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    fd = open(real_file_path, O_WRONLY | O_APPEND, 0);
    if (fd < 0) {
        cmkem_errmsg("failed to open file '%s'.", real_file_path);
        return CMKEM_OPEN_FILE_ERR;
    }

    if (write(fd, buf, buf_len) < 0) {
        cmkem_errmsg("failed to write to file '%s'.\n", real_file_path);
        close(fd);
        return CMKEM_WRITE_FILE_ERR;
    }

    close(fd);
    return ret;
}

CmkemErrCode remove_file(const char *file_path)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    char real_file_path[PATH_MAX] = {0};

    ret = check_and_realpath(file_path, real_file_path, PATH_MAX);
    check_kmsret(ret, ret);

    if (remove(real_file_path) != 0) {
        cmkem_errmsg("failed to remove file '%s' ", real_file_path);
    }

    return ret;
}
