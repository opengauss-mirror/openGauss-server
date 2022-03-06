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
 * csnminsync.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/csnminsync.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CSNMINSYNC_H
#define CSNMINSYNC_H

#define DEFAULT_CLEANUP_TIME 50000

extern void csnminsync_main(void);
extern void csnminsync_thread_shutdown(void);
extern bool csnminsync_is_first_cn_or_ccn(const char *node_name);
#endif /* CSNMINSYNC_H */