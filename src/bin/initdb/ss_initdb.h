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

#define ERROR_CLUSTERDIR_EXISTS_BY_PRIMARY 0x01
#define ERROR_CLUSTERDIR_NO_EXISTS_BY_STANDBY 0x02
#define ERROR_CLUSTERDIR_INCOMPLETE 0x04

#define ARRAY_NUM(a) (sizeof(a) / sizeof((a)[0]))

extern char **replace_token(char **lines, const char *token, const char *replacement);
extern void exit_nicely(void);
extern void *pg_malloc(size_t size);

extern char *ss_nodedatainfo;
extern int32 ss_nodeid;

extern bool ss_issharedstorage;
extern bool need_create_data;

/* check dms url when gs_initdb */
extern bool ss_check_nodedatainfo(bool enable_dss);
extern int ss_check_shareddir(char *path, int32 node_id, bool *need_create_data);
extern void ss_mkdirdir(const char *pg_data, const char *vgdata_dir, const char *vglog_dir, bool need_mkclusterdir);
extern char **ss_addnodeparmater(char **conflines);

#endif /* SS_INITDB_H */