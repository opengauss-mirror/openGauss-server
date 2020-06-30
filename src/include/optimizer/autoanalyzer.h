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
 * autoanalyzer.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/autoanalyzer.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AUTOANALYZER_H
#define AUTOANALYZER_H

#include "libpq/libpq-fe.h"
#include "lib/stringinfo.h"
#include "utils/relcache.h"

/* Auto-Analyze */
class AutoAnaProcess : public BaseObject {
public:
    // constructor
    AutoAnaProcess(Relation rel);

    ~AutoAnaProcess();

    static bool runAutoAnalyze(Relation rel);

    static void cancelAutoAnalyze();

protected:
    PGconn* m_pgconn;

    PGresult* m_res;

    List* m_query;

private:
    static bool check_conditions(Relation rel);

    bool run();

    bool executeSQLCommand(char* cmd);

    static void tear_down();
};

#endif /* AUTOANALYZER_H */
