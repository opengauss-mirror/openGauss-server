/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * mot_fdw_helpers.cpp
 *    MOT Foreign Data Wrapper helpers.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_helpers.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_FDW_HELPERS_H
#define MOT_FDW_HELPERS_H

#include "catalog_column_types.h"
#include "nodes/primnodes.h"
#include "global.h"
#include "mot_engine.h"
#include "mot_match_index.h"

void DestroySession(MOT::SessionContext* sessionContext);
void InitSessionDetailsMap();
void DestroySessionDetailsMap();
void RecordSessionDetails();
void ClearSessionDetails(MOT::SessionId sessionId);
void ClearCurrentSessionDetails();
void GetSessionDetails(MOT::SessionId sessionId, ::ThreadId* gaussSessionId, pg_time_t* sessionStartTime);
void InitSessionCleanup();
void DestroySessionCleanup();
void ScheduleSessionCleanup();
void CancelSessionCleanup();
void DestroySessionJitContexts();
void MOTOnThreadShutdown();
void MOTCleanupThread(int status, Datum ptr);

bool IsMOTExpr(
    RelOptInfo* baserel, MOTFdwStateSt* state, MatchIndexArr* marr, Expr* expr, Expr** result, bool setLocal);
bool IsNotEqualOper(OpExpr* op);

uint16_t MOTTimestampToStr(uintptr_t src, char* destBuf, size_t len);
uint16_t MOTTimestampTzToStr(uintptr_t src, char* destBuf, size_t len);
uint16_t MOTDateToStr(uintptr_t src, char* destBuf, size_t len);

/**
 * @brief Initializes MOT query state.
 * @param fdwState list of nodes
 * @param fdwExpr list of additional nodes added during update query
 * @param exTableId PG table id
 */
MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableID);

/**
 * @brief Serializes MOT query state into list of nodes.
 * @param state MOT query state
 */
void* SerializeFdwState(MOTFdwStateSt* state);

/**
 * @brief Releases MOT query state.
 * @param state MOT query state
 */
void ReleaseFdwState(MOTFdwStateSt* state);

/**
 * @brief Converts MOT column type to PG column type .
 * @param motColumnType MOT column type
 */
Oid ConvertMotColumnTypeToOid(MOT::MOT_CATALOG_FIELD_TYPES motColumnType);

#endif /* MOT_FDW_HELPERS_H */
