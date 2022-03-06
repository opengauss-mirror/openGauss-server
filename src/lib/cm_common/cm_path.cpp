/**
 * @file cm_path.cpp
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-06
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#include <ctype.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "cm/cm_c.h"

#define IS_DIR_SEP_GTM(ch) ((ch) == '/' || (ch) == '\\')

#define skip_drive(path) (path)

void trim_directory(char* path);

/**
 * @brief  Trim trailing directory from path, that is, remove any trailing slashes,
 *	the last pathname component, and the slash just ahead of it --- but never
 *	remove a leading slash.
 * 
 * @param  path             Check the path validity. 
 */
void trim_directory(char* path)
{
    char* p = NULL;

    path = skip_drive(path);

    if (path[0] == '\0') {
        return;
    }

    /* back up over trailing slash(es) */
    for (p = path + strlen(path) - 1; IS_DIR_SEP_GTM(*p) && p > path; p--) {
        ;
    }
    /* back up over directory name */
    for (; !IS_DIR_SEP_GTM(*p) && p > path; p--) {
        ;
    }
    /* if multiple slashes before directory name, remove 'em all */
    for (; p > path && IS_DIR_SEP_GTM(*(p - 1)); p--) {
        ;
    }
    /* don't erase a leading slash */
    if (p == path && IS_DIR_SEP_GTM(*p)) {
        p++;
    }
    *p = '\0';
}
