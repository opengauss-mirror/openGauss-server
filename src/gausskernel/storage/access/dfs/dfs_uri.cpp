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
 *  dfs_uri.cpp
 *        URI encode/decode functions
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/dfs/dfs_uri.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_insert.h"

const char DEC2HEX[16 + 1] = "0123456789ABCDEF";
const char URIDEC[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 0-19 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 20-39 */
    -1, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  -1, -1, /* 40-59 */
    -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 60-79 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, /* 80-99 */
    13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 100-119 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 120-139 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 140-159 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 160-179 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 180-199 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 200-219 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, /* 220-239 */
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1                  /* 240-255 */
};

const char URISAFE[256] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /*  0-19   */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, /* 20-39   */
    0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, /* 40-59   */
    0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 60-79   */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 1, /* 80-99   */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, /* 100-119 */
    1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 120-139 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 140-159 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 160-179 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 180-199 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 200-219 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* 220-239 */
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0              /* 240-255 */
};

/*
 * Encode the uri char. Like "#" -> "%23", used in partition filter.
 *
 * @_in param pSrc: The uri format string to be encoded.
 * @return Return the encoded string.
 */
char *UriEncode(const char *pSrc)
{
    size_t len = strlen(pSrc);
    if (len > size_t((unsigned int)(MAX_PARSIG_LENGTH / 3))) {
        ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED), errmodule(MOD_DFS),
                        errmsg("Before encoding, the length of data as partition directory name must be less than "
                               "dfs_partition_directory_length(%d)/3.",
                               u_sess->attr.attr_storage.dfs_max_parsig_length)));
    }

    char *pStr = (char *)palloc0(3 * len + 1);
    char *pEnd = pStr;

    for (size_t i = 0; i < len; i++) {
        int srcChar = (int)pSrc[i];
        if (srcChar >= 0 && !URISAFE[srcChar]) {
            *pEnd++ = '%';
            *pEnd++ = DEC2HEX[(unsigned char)pSrc[i] >> 4];
            *pEnd++ = DEC2HEX[(unsigned char)pSrc[i] & 0x0F];
        } else
            *pEnd++ = pSrc[i];
    }

    return pStr;
}

/*
 * Decode the uri char. Like "%23" -> "#", used in partition filter.
 *
 * @_in param pSrc: The uri format string to be decoded.
 * @return Return the decoded string.
 */
char *UriDecode(const char *pSrc)
{
    uint32 i = 0;

    Assert(pSrc != NULL);
    const uint32 src_len = strlen(pSrc) + 1;
    char *pret = (char *)palloc0(sizeof(char) * src_len);
    char *pCursor = pret;

    for (i = 0; i < src_len - 2; i++) {
        if (*pSrc == '%') {
            char dec1 = URIDEC[(int)(*(pSrc + 1))];
            char dec2 = URIDEC[(int)(*(pSrc + 2))];
            if (-1 != dec1 && -1 != dec2) {
                *pCursor++ = ((unsigned char)dec1 << 4) + dec2;
                pSrc += 3;
                i += 2;
                continue;
            }
        }

        *pCursor++ = *pSrc++;
    }

    // the last 2- chars
    for (; i < src_len; i++) {
        *pCursor++ = *pSrc++;
    }

    return pret;
}
