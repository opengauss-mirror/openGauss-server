/* ---------------------------------------------------------------------------------------
 *
 * build_query.cpp
 *      functions used by cm_ctl and gs_ctl to display build progress
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/lib/build_query/build_query.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/build_query/build_query.h"
#include "securec.h"
#include "securec_check.h"

/*
 * strdup() replacements that prints an error and exits
 * if something goes wrong. Can never return NULL.
 */
static char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (result == NULL) {
        printf("out of memory\n");
        exit(1);
    }
    return result;
}

char* show_estimated_time(int estimated_time)
{
    char time_string[MAXPGPATH] = {0};
    int hour = 0;
    int min = 0;
    int sec = 0;
    int nRet = 0;

    if (estimated_time == -1)
        return xstrdup("--:--:--");

    hour = estimated_time / S_PER_H;
    min = (estimated_time % S_PER_H) / S_PER_MIN;
    sec = (estimated_time % S_PER_H) % S_PER_MIN;

    nRet = snprintf_s(time_string, MAXPGPATH, MAXPGPATH - 1, "%.2d:%.2d:%.2d", hour, min, sec);
    securec_check_ss_c(nRet, "\0", "\0");

    return xstrdup(time_string);
}

char* show_datasize(uint64 size)
{
    char size_string[MAXPGPATH] = {0};
    float showsize = 0;
    const char* unit = NULL;
    int nRet = 0;

    if (size / KB_PER_TB != 0) {
        showsize = (float)size / KB_PER_TB;
        unit = "TB";
    } else if (size / KB_PER_GB != 0) {
        showsize = (float)size / KB_PER_GB;
        unit = "GB";
    } else if (size / KB_PER_MB != 0) {
        showsize = (float)size / KB_PER_MB;
        unit = "MB";
    } else {
        showsize = (float)size;
        unit = "kB";
    }

    nRet = snprintf_s(size_string, MAXPGPATH, MAXPGPATH - 1, "%.2f%s", showsize, unit);
    securec_check_ss_c(nRet, "\0", "\0");

    return xstrdup(size_string);
}

void UpdateDBStateFile(char* path, GaussState* state)
{
    FILE* statef = NULL;
    char temppath[MAXPGPATH] = {0};
    int ret;

    if (NULL == state || path == NULL) {
        return;
    }

    ret = snprintf_s(temppath, MAXPGPATH, MAXPGPATH - 1, "%s.temp", path);
    securec_check_ss_c(ret, "\0", "\0");

    statef = fopen(temppath, "w");
    if (statef == NULL) {
        return;
    }
    if (0 == (fwrite(state, 1, sizeof(GaussState), statef))) {
    }
    fclose(statef);

    (void)rename(temppath, path);
}
