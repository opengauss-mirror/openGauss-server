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
 * pg_format_cu.cpp
 *
 * IDENTIFICATION
 *    src/bin/pg_upgrade/pg_format_cu.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef __cplusplus
extern "C" {
#endif

extern char* optarg;

const int ALIGN_SIZE = 8192;
const int PERMISSION = 0600;
const int FILE_MAX_SIZE = 1073741824;  //  1024*1024*1024
const int FILE_MAX_PATH = 1024;        //  reffer MAXPGPATH
const int PATH_DELIMITER_SIZE = 1;
const int STRING_TERMINATOR = 1;

#define CONFIRM_PATH(file_path)                           \
    do {                                                  \
        if (NULL == (file_path)) {                        \
            printf("the file or path does not exist.\n"); \
            exit(1);                                      \
        }                                                 \
    } while (0)

typedef int (*oper_dir_entry_proc)(
    const char* root, int buf_size, int root_len, const char* entry_name, const void* args);

static void help(const char* progname)
{
    printf("%s is an upgrade tool about align column store files for openGauss.\n\n"
           "Usage:\n"
           "  %s [OPTION]...\n"
           "\nOptions:\n"
           "  -d DIRECTORY       database directory absolutely path, align all the named file \n"
           "  -f FILE_NAME       database file absolutely path, align the files \n"
           "  -a ALIGN_SIZE      size to align \n"
           "  -?, -h, --help     show this help, then exit\n",
        progname,
        progname);
}

/*
 * @Description: check file exist
 * @Param[IN/OUT] file_path: file path
 * @Return: true exist; false not exist
 * @See also:
 */
bool file_exist(const char* file_path)
{
    int ret = 0;
    struct stat stat_buf {};

    CONFIRM_PATH(file_path);
    ret = stat(file_path, &stat_buf);
    if (ret != 0) {
        printf("file(%s) not exist, OS errno=%d.\n", file_path, errno);
        return false;
    }

    /* S_ISREG: judge whether it's a regular file or not by the flag */
    if (S_ISREG(stat_buf.st_mode)) {
        return true;
    }
    printf("file(%s) not regular file.\n", file_path);
    return false;
}

/*
 * @Description: check file name and judge whether is column store file
 * @Param[IN/OUT] file_path: file path
 * @Return: true cloumn store style file
 * @See also:
 */
bool file_check_column_style(char* file_path)
{
    // relate to CUStorage::InitFileNamePrefix
    char* file_name = NULL;
    char* str = NULL;
    char* str1 = NULL;

    CONFIRM_PATH(file_path);
    file_name = strrchr(file_path, '/');
    if (file_name == NULL) {
        printf("failed to get file(%s) name.\n", file_path);
        return false;
    }

    /* here can check number in file name */
    str = strstr(file_name, "_C");
    if (str == NULL) {
        printf("file(%s) maybe not column store file.\n", file_path);
        return false;
    }

    str1 = strchr(str, '.');
    if (str1 == NULL) {
        printf("file(%s) maybe not column store file.\n", file_path);
        return false;
    }
    /* here can check number in file name */
    return true;
}

/*
 * @Description: get file size
 * @Param[IN] file_path:file size
 * @Param[IN/OUT] size: file size
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int file_size(const char* file_path, uint64* size)
{
    int ret = 0;
    struct stat64 stat_buf {};

    CONFIRM_PATH(file_path);
    ret = stat64(file_path, &stat_buf);
    if (ret != 0) {
        printf("failed to get file(%s) size, OS errno=%d.\n", file_path, errno);
        return ret;
    }

    *size = (uint64)(stat_buf.st_size); /* size is long long */

    if (*size > FILE_MAX_SIZE) {
        printf("file(%s) is not database file, size(%lu).\n", file_path, *size);
        *size = 0;
        return 1;
    }
    return ret;
}

/*
 * @Description: open file
 * @Param[IN] fd: fd
 * @Param[IN/OUT] file_path: file path
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int open_file(const char* file_path, int* fd)
{
    int file_handle = 0;
    int file_flag = O_RDWR;  // relate to CUStorage::OpenFile
    const int permission = PERMISSION;

    CONFIRM_PATH(file_path);
    if (fd == NULL) {
        printf("the fd does not exist.\n");
        exit(1);
    }

    file_handle = open(file_path, file_flag, permission);
    if (file_handle == -1) {
        printf("open file failed, file(%s), open flag(%d), OS errno=%d.\n", file_path, file_flag, errno);
        return 1;
    }
    *fd = file_handle;
    return 0;
}

/*
 * @Description: wite file, atmost try 3 times
 * @Param[IN] buf: write buffer
 * @Param[IN] count: buffer count
 * @Param[IN] fd: fd
 * @Param[IN] offset: offset
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int write_file(int fd, const char* buf, int count, int offset)
{
    int written_count = 0;
    const int write_times = 3;
    int try_times = 0;

    for (; try_times < write_times; try_times++) {
        written_count = pwrite(fd, buf, count, offset);
        if (written_count == -1) {
            printf("write file failed, request written count(%d), OS errno=%d.\n", count, errno);
            return 1;
        }

        if (written_count == count) {
            break;
        } else if (written_count == 0) {
            printf("write file 0 byte, request written count(%d), try again.\n", count);
            (void)sleep(3);
            continue;
        } else {
            printf(
                "write file failed (%d BYTE), request written count(%d), OS errno=%d.\n", written_count, count, errno);
            return 1;
        }
    }

    if (write_times == try_times) {
        printf("write file failed (%d BYTE), request written count(%d).\n", written_count, count);
        return 1;
    }

    return 0;
}

/*
 * @Description: append file
 * @Param[IN] align_size: align size
 * @Param[IN] file_path: file path
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int append_file(const char* file_path, int align_size)
{
    int ret = 0;
    int fd = 0;
    int rc = 0;
    char* buf = NULL;
    int buf_size = 0;
    uint64 size = 0;
    uint64 remainder = 0;

    CONFIRM_PATH(file_path);

    ret = file_size(file_path, &size);
    if (ret != 0) {
        return ret;
    }

    remainder = size % align_size;
    if (remainder == 0) {
        return ret;
    }

    buf_size = align_size - remainder;
    buf = (char*)malloc(buf_size);
    if (buf == NULL) {
        printf("failed to malloc size(%d) for file(%s).\n", buf_size, file_path);
        return 1;
    }
    rc = memset_s(buf, buf_size, 0, buf_size);
    securec_check_c(rc, "\0", "\0");

    ret = open_file(file_path, &fd);
    if (ret != 0) {
        free(buf);
        return ret;
    }

    ret = write_file(fd, buf, buf_size, size);
    (void)close(fd);
    free(buf);
    if (ret != 0) {
        return ret;
    }

    printf("successfully to align file(%s), original size(%lu) padding size(%d).\n", file_path, size, buf_size);
    return ret;
}

/*
 * @Description: recursive open file
 * @Param[IN] args: args, not used
 * @Param[IN] buf_size:  dir path length
 * @Param[IN] oper_proc: operate function
 * @Param[IN] root: relative dir path
 * @Param[IN] root_len: relative dir path length
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int oper_dir_recursive(const char* root, int buf_size, int root_len, oper_dir_entry_proc oper_proc, const void* args)
{
    int ret = 0;
    int tmp_ret = 0;
    struct dirent* dir_entry = NULL;
    struct dirent entry {};
    DIR* dir = NULL;

    CONFIRM_PATH(root);
    if (oper_proc == NULL) {
        printf("function pointer oper_dir_entry_proc not exist.\n");
        exit(1);
    }

    /* this function do not care about this parameter, pass directory to oper_proc */
    (void)args;

    dir = opendir(root);
    if (dir == NULL) {
        printf("open directory failed, path=%s, OS errno=%d.\n", root, errno);
        return 1;
    }

    dir_entry = &entry;
    do {
        /* in arm environment, readdir_r warning about deprecated-declarations,
            but for thread safe keep using readdir_r. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        tmp_ret = readdir_r(dir, &entry, &dir_entry);
#pragma GCC diagnostic pop

        if (dir_entry == NULL) {
            break;
        }

        if (tmp_ret != 0) {
            ret = 1;
            break;
        }

        // skip "." and ".." entry
        if ((0 == strcmp(".", dir_entry->d_name)) || (0 == strcmp("..", dir_entry->d_name))) {
            continue;
        }

        tmp_ret = (*oper_proc)(root, buf_size, root_len, dir_entry->d_name, args);
        if (tmp_ret != 0) {
            ret = tmp_ret;
        }
    } while (true);

    (void)closedir(dir);

    return ret;
}

/*
 * @Description: do align file
 * @Param[IN] align_size: align size
 * @Param[IN] file_path: file path
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
static int do_align_file(char* file_path, int align_size)
{
    // relate to CUStorage::OpenFile
    CONFIRM_PATH(file_path);
    if (!file_exist(file_path)) {
        return 1;
    }

    if (file_check_column_style(file_path)) {
        return append_file(file_path, align_size);
    }

    return 0;
}

/*
 * @Description: filter file
 * @Param[IN] args: not used
 * @Param[IN] buf_size: entry path length
 * @Param[IN] entry_name:entry path
 * @Param[IN] root: relative dir path
 * @Param[IN] root_len: relative dir path length
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
int fliter_append_file_proc(const char* root, int buf_size, int root_len, const char* entry_name, const void* args)
{
    int align_size = 0;
    int rc = 0;
    char file_path[FILE_MAX_PATH] = {0};

    align_size = *(int*)args;
    (void)buf_size;

    if ((root_len + strlen(entry_name) + PATH_DELIMITER_SIZE + STRING_TERMINATOR) > FILE_MAX_PATH) {
        printf("file path \"%s%c%s\"is too long, max file path is %d.\n", root, '/', entry_name, FILE_MAX_PATH);
        return 1;
    }

    rc = snprintf_s(file_path, FILE_MAX_PATH, FILE_MAX_PATH - 1, "%s%c%s", root, '/', entry_name);
    securec_check_ss_c(rc, "\0", "\0");

    return do_align_file(file_path, align_size);
}

/*
 * @Description: align files which in the directory recursive
 * @Param[IN] align_size: align size
 * @Param[IN] dir_path: dir path
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
static int do_align_dir(const char* dir_path, int align_size)
{
    int ret = 0;

    CONFIRM_PATH(dir_path);
    ret = oper_dir_recursive(
        dir_path, (int)strlen(dir_path), (int)strlen(dir_path), fliter_append_file_proc, (const void*)&align_size);
    if (ret != 0) {
        printf("failed to get directory path(%s).\n", dir_path);
    }
    return ret;
}

/*
 * @Description: align api function
 * @Param[IN] align_size: align size
 * @Param[IN] dir_path: dir path
 * @Param[IN] file_path: file path
 * @Return: 0 :op succeed; others failed
 * @See also:
 */
static int do_align(const char* dir_path, char* file_path, int align_size)
{
    int align = (align_size == 0) ? ALIGN_SIZE : align_size;

    if (dir_path != NULL) {
        return do_align_dir(dir_path, align);
    }

    if (file_path != NULL) {
        return do_align_file(file_path, align);
    }

    return 0;
}

static void check_env_value(const char* input_env_value)
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
        if (strstr(input_env_value, danger_character_list[i]) != NULL) {
            fprintf(stderr,
                _("ERROR: Failed to check environment value: invalid token \"%s\".\n"),
                danger_character_list[i]);
            exit(1);
        }
    }
}

/*
 * @Description: main entry
 * @Param[IN] argc: param count
 * @Param[IN] argv: param list
 * @Return:
 * @See also:
 */
int main(int argc, char* argv[])
{
    int c;
    const char* progname = "pg_format_cu";
    char* dir_path = NULL;
    char* file_path = NULL;
    int align_size = 0;
    bool conflit = false;

    if (argc == 1) {
        help(progname);
        exit(0);
    }

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0 || strcmp(argv[1], "-h") == 0) {
            help(progname);
            exit(0);
        }
    }

    while ((c = getopt(argc, argv, "d:f:a:")) != -1) {
        switch (c) {
            case 'd':
                if (conflit) {
                    printf("can not support combination -d and -f.\n");
                    exit(1);
                }
                check_env_value(optarg);
                dir_path = optarg;
                conflit = true;
                break;

            case 'f':
                if (conflit) {
                    printf("can not support combination -d and -f.\n");
                    exit(1);
                }
                check_env_value(optarg);
                file_path = optarg;
                conflit = true;
                break;

            case 'a':
                check_env_value(optarg);
                align_size = atoi(optarg);
                break;
            default:
                break;
        }
    }

    if (dir_path == NULL && file_path == NULL) {
        printf("you must give the direct path or file path.\n");
        exit(1);
    }

    if (align_size < 0) {
        printf("you must give the correct align size.\n");
        exit(1);
    }

    if (align_size % ALIGN_SIZE != 0) {
        printf("you must give the align size %d integer multiple .\n", ALIGN_SIZE);
        exit(1);
    }

    if (do_align(dir_path, file_path, align_size) != 0) {
        exit(1);
    }
    printf("successfully to align all files.\n");
    return 0;
}

#ifdef __cplusplus
}
#endif

