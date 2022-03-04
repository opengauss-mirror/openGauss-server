/* ---------------------------------------------------------------------------------------
 * *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * pageparse.h
 *
 * IDENTIFICATION
 * src/gausskernel/storage/page/pageparse.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef OPENGAUSS_PAGEPARSE_H
#define OPENGAUSS_PAGEPARSE_H

extern void CheckUser(const char *fName);

extern void PrepForRead(char *path, int64 blocknum, char *relation_type, char *outputFilename, RelFileNode *relnode,
    bool parse_page);

extern void ValidateParameterPath(RelFileNode rnode, char *str);

extern char *ParsePage(char *path, int64 blocknum, char *relation_type, bool read_memory);

extern void CheckOpenFile(FILE *outputfile, char *outputFilename);

extern void CheckWriteFile(int result, int cnt_len, char *outputFilename);

extern void CheckCloseFile(int result, char *outputFilename);

extern void validate_xlog_location(char *str);

#define MAXFILENAME 4096
#define MAXFNAMELEN 64
#define MAXOUTPUTLEN 1048576
#define TENBASE 10
#define XIDTHIRTYTWO 32
#define TWO 2
#define FIVE 5
#define SHORTOUTPUTLEN 200

#endif /* OPENGAUSS_PAGEPARSE_H */
