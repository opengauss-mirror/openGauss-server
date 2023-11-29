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
 * ss_initdb.h
 *
 *
 * IDENTIFICATION
 *        src/bin/initdb/ss_initdb.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SS_INITDB_H_
#define SS_INITTDB_H_

#ifndef ERROR_LIMIT_LEN
#define ERROR_LIMIT_LEN 256
#endif

#define ERROR_INSTANCEDIR_EXISTS 0x01
#define ERROR_CLUSTERDIR_EXISTS_BY_PRIMARY 0x02
#define ERROR_CLUSTERDIR_NO_EXISTS_BY_STANDBY 0x04
#define ERROR_CLUSTERDIR_INCOMPLETE 0x10

#define ARRAY_NUM(a) (sizeof(a) / sizeof((a)[0]))

extern char **replace_token(char **lines, const char *token, const char *replacement);
extern void exit_nicely(void);
extern void *pg_malloc(size_t size);

extern char* ss_nodedatainfo;
extern int32 ss_nodeid;

extern bool ss_issharedstorage;
extern bool ss_need_mkclusterdir;
extern bool ss_need_mkspecialdir;

/* check dms url when gs_initdb */

extern bool ss_check_nodedatainfo(bool enable_dss);
extern int ss_check_shareddir(char* path, int32 node_id, bool *ss_need_mkclusterdir);
extern bool ss_check_specialdir(char *path);

extern void ss_createdir(const char** ss_dirs, int32 num, int32 node_id, const char* pg_data, const char* vgdata_dir, const char* vglog_dir);
extern void ss_mkdirdir(int32 node_id, const char* pg_data, const char* vgdata_dir, const char* vglog_dir, bool need_mkclusterdir, bool need_mkspecialdir);
extern char** ss_addnodeparmater(char** conflines);


#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr) && reinterpret_cast<char*>(ptr) != static_cast<char*>("")) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)

#endif /* SS_INITDB_H */