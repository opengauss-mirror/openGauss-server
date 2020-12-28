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
 * --------------------------------------------------------------------------------------
 *
 * globalplancache.h
 *
 *        global plan cache
 *
 * IDENTIFICATION
 *        src/include/utils/globalplancache.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef GLOBALPRESTMT_H
#define GLOBALPRESTMT_H

#include "knl/knl_variable.h"

#include "pgxc/pgxc.h"
#include "storage/sinval.h"
#include "utils/globalplancore.h"

class GlobalPrepareStmt : public BaseObject
{
public:
	GlobalPrepareStmt();
    ~GlobalPrepareStmt();
    
    /* cache invalid */

    void Init();
    void InitCnTimelineHTAB();
    void Store(const char *stmt_name,CachedPlanSource *plansource, bool from_sql, bool is_share);
    PreparedStatement *Fetch(const char *stmt_name, bool throwError);
    void Drop(const char *stmt_name, bool showError);
    void DropAll(sess_orient* key, bool need_lock);
    void Clean(uint32 cn_id);

    /* transaction */
    void PrepareCleanUpByTime(bool needCheckTime);
    void CleanSessionGPC(knl_session_context* currentSession);
    void UpdateUseTime(sess_orient* key, bool sess_detach);


    void CheckTimeline();

    /* system function */

    void* GetStatus(uint32 *num);

private:

    GPCHashCtl *m_array;
    HTAB* m_cn_timeline;
};


#endif   /* PLANCACHE_H */
