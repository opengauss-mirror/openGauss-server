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
 * gs_getopt_r.cpp
 *
 * IDENTIFICATION
 *    src/common/port/gs_getopt_r.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"

#define BADCH (int)'?'
#define BADARG (int)':'
#define EMSG ""

void initOptParseContext(OptParseContext* pOptCtxt)
{
    pOptCtxt->place = "";
    pOptCtxt->opterr = 1;
    pOptCtxt->optind = 1;
    pOptCtxt->optopt = 0;
    pOptCtxt->optarg = NULL;
}

/*
 * Parse argc/argv argument vector reentrant safe.
 *
 * This implementation is revised from current getopt.cpp in exiting code
 * base for Win32 port.
 *
 * This implementation does not use optreset.  Instead, we guarantee that
 * it can be restarted on a new argv array after a previous call returned -1,
 * if the caller resets optind to 1 before the first call of the new series.
 * (Internally, this means we must be sure to reset "place" to EMSG before
 * returning -1.)
 */
int getopt_r(int nargc,         // number of argruments
    char* const* nargv,         // array of argruments
    const char* ostr,           // argument separators
    OptParseContext* pOptCtxt)  // parsing context
{
    char* place = pOptCtxt->place; /* option letter processing */
    char* oli = NULL;              /* option letter list index */

    if (!*place) {
        /* update scanning pointer */
        if (pOptCtxt->optind >= nargc || *(place = nargv[pOptCtxt->optind]) != '-') {
            place = EMSG;
            return -1;
        }
        if (place[1] && *++place == '-' && place[1] == '\0') {
            /* found "--" */
            ++pOptCtxt->optind;
            place = EMSG;
            return -1;
        }
    }
    /* option letter okay? */
    if ((pOptCtxt->optopt = (int)*place++) == (int)':' || ((oli = strchr((char*)ostr, pOptCtxt->optopt)) == NULL)) {
        /*
         * if the user didn't specify '-' as an option, assume it means -1.
         */
        if (pOptCtxt->optopt == (int)'-') {
            place = EMSG;
            return -1;
        }
        if (!*place) {
            ++pOptCtxt->optind;
        }
        if (pOptCtxt->opterr && *ostr != ':') {
            (void)fprintf(stderr, "illegal option -- %c\n", pOptCtxt->optopt);
        }
        return BADCH;
    }
    if (*++oli != ':') {
        /* don't need argument */
        pOptCtxt->optarg = NULL;
        if (!*place)
            ++pOptCtxt->optind;
    } else {
        /* need an argument */
        if (*place)
            /* no white space */
            pOptCtxt->optarg = place;
        else if (nargc <= ++pOptCtxt->optind) {
            /* no arg */
            place = EMSG;
            if (*ostr == ':')
                return BADARG;
            if (pOptCtxt->opterr)
                (void)fprintf(stderr, "option requires an argument -- %c\n", pOptCtxt->optopt);
            return BADCH;
        } else
            /* white space */
            pOptCtxt->optarg = nargv[pOptCtxt->optind];
        place = EMSG;
        ++pOptCtxt->optind;
    }
    return pOptCtxt->optopt; /* dump back option letter */
}
