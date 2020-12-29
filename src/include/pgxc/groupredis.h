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
 * groupredis.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/groupredis.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GROUP_REDIS_H
#define GROUP_REDIS_H

#include "c.h"

extern void PgxcChangeRedistribution(Oid group_oid, char in_redistribution);
extern oidvector* PgxcGetRedisNodes(Relation rel, char redist_kind);
extern void PgxcUpdateRedistSrcGroup(Relation rel, oidvector* gmember,
                                     text* bucket_str, char* group_name);
extern void PgxcGroupResize(const char* src_group_name, const char* dest_group_name);
extern void PgxcGroupResizeComplete();

#endif
