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
 * mot_match_index.cpp
 *    Helper class to store the matched index information during planning phase.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_match_index.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mot_match_index.h"
#include "mot_internal.h"
#include "nodes/makefuncs.h"

static KEY_OPER g_keyOperStateMachine[static_cast<uint8_t>(KEY_OPER::READ_INVALID) + 1]
                                     [static_cast<uint8_t>(KEY_OPER::READ_INVALID)];

void InitKeyOperStateMachine()
{
    // fill key operation matrix
    for (uint8_t i = 0; i <= static_cast<uint8_t>(KEY_OPER::READ_INVALID); i++) {
        for (uint8_t j = 0; j < static_cast<uint8_t>(KEY_OPER::READ_INVALID); j++) {
            switch ((KEY_OPER)i) {
                case KEY_OPER::READ_KEY_EXACT:  // = : allows all operations
                    g_keyOperStateMachine[i][j] = (KEY_OPER)j;
                    break;
                case KEY_OPER::READ_KEY_OR_NEXT:  // >= : allows nothing
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
                case KEY_OPER::READ_KEY_AFTER:  // > : allows nothing
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
                case KEY_OPER::READ_KEY_OR_PREV:  // <= : allows nothing
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
                case KEY_OPER::READ_KEY_BEFORE:  // < : allows nothing
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;

                case KEY_OPER::READ_KEY_LIKE:  // like: allows nothing
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;

                case KEY_OPER::READ_INVALID:  // = : allows all operations
                    g_keyOperStateMachine[i][j] = (KEY_OPER)j;
                    break;

                default:
                    g_keyOperStateMachine[i][j] = KEY_OPER::READ_INVALID;
                    break;
            }
        }
    }
}

bool MatchIndex::IsSameOper(KEY_OPER op1, KEY_OPER op2) const
{
    bool res = true;
    if (op1 == op2) {
        return res;
    }

    switch (op1) {
        case KEY_OPER::READ_KEY_EXACT: {
            res = true;
            break;
        }
        case KEY_OPER::READ_KEY_LIKE: {
            res = true;
            break;
        }
        case KEY_OPER::READ_KEY_OR_NEXT:
        case KEY_OPER::READ_KEY_AFTER: {
            switch (op2) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_KEY_OR_NEXT:
                case KEY_OPER::READ_KEY_AFTER:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        case KEY_OPER::READ_KEY_OR_PREV:
        case KEY_OPER::READ_KEY_BEFORE: {
            switch (op2) {
                case KEY_OPER::READ_KEY_EXACT:
                case KEY_OPER::READ_KEY_LIKE:
                case KEY_OPER::READ_KEY_OR_PREV:
                case KEY_OPER::READ_KEY_BEFORE:
                    break;
                default:
                    res = false;
                    break;
            }
            break;
        }
        default:
            break;
    }

    return res;
}

void MatchIndex::ClearPreviousMatch(MOTFdwStateSt* state, bool setLocal, int i, int j)
{
    if (m_parentColMatch[i][j] != nullptr) {
        if (setLocal) {
            if (!list_member(state->m_localConds, m_parentColMatch[i][j])) {
                state->m_localConds = lappend(state->m_localConds, m_parentColMatch[i][j]);
            }
        }

        m_parentColMatch[i][j] = nullptr;
        m_colMatch[i][j] = nullptr;
        m_opers[i][j] = KEY_OPER::READ_INVALID;
        m_numMatches[i]--;
    }
    // in case second bound is set, move it to be a first bound
    if (i == 0 && m_parentColMatch[1][j] != nullptr) {
        m_parentColMatch[0][j] = m_parentColMatch[1][j];
        m_colMatch[0][j] = m_colMatch[1][j];
        m_opers[0][i] = m_opers[1][i];
        m_parentColMatch[1][j] = nullptr;
        m_colMatch[1][j] = nullptr;
        m_opers[1][j] = KEY_OPER::READ_INVALID;
    }
}

bool MatchIndex::SetIndexColumn(
    MOTFdwStateSt* state, int16_t colNum, KEY_OPER op, Expr* expr, Expr* parent, bool setLocal)
{
    bool res = false;
    int i = 0;
    const int16_t* cols = m_ix->GetColumnKeyFields();
    int16_t numKeyCols = m_ix->GetNumFields();
    bool sameOper = false;

    for (; i < numKeyCols; i++) {
        if (cols[i] == colNum) {
            if (m_colMatch[0][i] == nullptr) {
                m_parentColMatch[0][i] = parent;
                m_colMatch[0][i] = expr;
                m_opers[0][i] = op;
                m_numMatches[0]++;
                res = true;
                break;
            } else {
                sameOper = IsSameOper(m_opers[0][i], op);
                // do not use current and previous expression for this column
                // in index selection
                // we can not differentiate between a > 2 and a > 1
                if (sameOper) {
                    if (op == KEY_OPER::READ_KEY_EXACT && m_opers[0][i] != op) {
                        ClearPreviousMatch(state, setLocal, 0, i);
                        ClearPreviousMatch(state, setLocal, 1, i);
                        m_parentColMatch[0][i] = parent;
                        m_colMatch[0][i] = expr;
                        m_opers[0][i] = op;
                        m_numMatches[0]++;
                        res = true;
                    } else if (m_opers[0][i] != KEY_OPER::READ_KEY_EXACT) {
                        ClearPreviousMatch(state, setLocal, 0, i);
                    }
                    break;
                }
            }

            sameOper = false;
            if (m_colMatch[1][i] == nullptr) {
                m_parentColMatch[1][i] = parent;
                m_colMatch[1][i] = expr;
                m_opers[1][i] = op;
                m_numMatches[1]++;
                res = true;
            } else {
                sameOper = IsSameOper(m_opers[1][i], op);
                // do not use current and previous expression for this column
                // in index selection
                // we can not differentiate between a > 2 and a > 1
                if (sameOper) {
                    if (op == KEY_OPER::READ_KEY_EXACT && m_opers[1][i] != op) {
                        ClearPreviousMatch(state, setLocal, 1, i);
                        m_parentColMatch[1][i] = parent;
                        m_colMatch[1][i] = expr;
                        m_opers[1][i] = op;
                        m_numMatches[1]++;
                        res = true;
                    } else if (m_opers[0][i] != KEY_OPER::READ_KEY_EXACT) {
                        ClearPreviousMatch(state, setLocal, 1, i);
                    }
                    break;
                }
            }

            break;
        }
    }

    return res;
}

double MatchIndex::GetCost(int numClauses)
{
    if (m_costs[0] == 0) {
        int partialIxMulti = 100;
        int notUsed[2] = {0, 0};
        int used[2] = {0, 0};

        m_costs[0] = 1.0;
        m_costs[1] = 1.0;

        for (int i = 0; i < m_ix->GetNumFields(); i++) {
            // we had same operations on the column, clean all settings
            if (m_parentColMatch[0][i] == nullptr && m_parentColMatch[1][i] != nullptr) {
                m_parentColMatch[0][i] = m_parentColMatch[1][i];
                m_colMatch[0][i] = m_colMatch[1][i];
                m_opers[0][i] = m_opers[1][i];

                m_parentColMatch[1][i] = nullptr;
                m_colMatch[1][i] = nullptr;
                m_opers[1][i] = KEY_OPER::READ_INVALID;

                m_numMatches[0]++;
                m_numMatches[1]--;
            } else if (m_parentColMatch[0][i] == nullptr) {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                m_colMatch[0][i] = nullptr;
            } else if (m_parentColMatch[1][i] == nullptr) {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                m_colMatch[1][i] = nullptr;
            }
        }

        for (int i = 0; i < m_ix->GetNumFields(); i++) {
            if (m_colMatch[0][i] == nullptr || notUsed[0] > 0) {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                notUsed[0]++;
            } else if (m_opers[0][i] < KEY_OPER::READ_INVALID) {
                KEY_OPER curr =
                    g_keyOperStateMachine[static_cast<uint8_t>(m_ixOpers[0])][static_cast<uint8_t>(m_opers[0][i])];

                if (curr < KEY_OPER::READ_INVALID) {
                    if (m_ixOpers[0] != KEY_OPER::READ_INVALID) {
                        if (curr > m_ixOpers[0]) {
                            m_ixOpers[0] = curr;
                        }
                    } else {
                        m_ixOpers[0] = curr;
                    }
                    used[0]++;
                    if (m_colMatch[1][i] == nullptr &&
                        (m_opers[0][i] == KEY_OPER::READ_KEY_EXACT || m_opers[0][i] == KEY_OPER::READ_KEY_LIKE)) {
                        m_colMatch[1][i] = m_colMatch[0][i];
                        m_opers[1][i] = m_opers[0][i];
                        m_parentColMatch[1][i] = m_parentColMatch[0][i];
                    }
                } else {
                    m_opers[0][i] = KEY_OPER::READ_INVALID;
                    notUsed[0]++;
                }
            } else {
                m_opers[0][i] = KEY_OPER::READ_INVALID;
                notUsed[0]++;
            }

            if (m_colMatch[1][i] == nullptr || notUsed[1] > 0) {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                notUsed[1]++;
            } else if (m_opers[1][i] < KEY_OPER::READ_INVALID) {
                KEY_OPER curr =
                    g_keyOperStateMachine[static_cast<uint8_t>(m_ixOpers[1])][static_cast<uint8_t>(m_opers[1][i])];

                if (curr < KEY_OPER::READ_INVALID) {
                    m_ixOpers[1] = curr;
                    used[1]++;
                } else {
                    m_opers[1][i] = KEY_OPER::READ_INVALID;
                    notUsed[1]++;
                }
            } else {
                m_opers[1][i] = KEY_OPER::READ_INVALID;
                notUsed[1]++;
            }
        }

        for (int i = 0; i < 2; i++) {
            int estimatedRows = 0;
            if (notUsed[i] > 0) {
                if (m_ixOpers[i] < KEY_OPER::READ_INVALID) {
                    m_ixOpers[i] = (KEY_OPER)((uint8_t)m_ixOpers[i] | KEY_OPER_PREFIX_BITMASK);
                }
                estimatedRows += (notUsed[i] * partialIxMulti);
            }

            // we assume that that all other operations will bring 10 times rows
            if (m_ixOpers[i] != KEY_OPER::READ_KEY_EXACT) {
                estimatedRows += 10;
            }

            // we assume that using partial key will bring 10 times more rows per not used column
            if (used[i] < numClauses) {
                estimatedRows += (numClauses - used[i]) * 10;
            }

            m_costs[i] += estimatedRows;
        }

        if (!m_ix->GetUnique()) {
            if (m_ixOpers[0] == KEY_OPER::READ_KEY_EXACT) {
                m_ixOpers[0] = KEY_OPER::READ_PREFIX;
            } else if (m_ixOpers[1] == KEY_OPER::READ_KEY_EXACT) {
                m_ixOpers[1] = KEY_OPER::READ_PREFIX;
            }
        }

        if (m_costs[0] <= m_costs[1]) {
            m_cost = m_costs[0];
            m_start = 0;

            if (m_colMatch[1][0]) {
                m_end = 1;
                if (m_ixOpers[1] == KEY_OPER::READ_PREFIX || m_ixOpers[1] == KEY_OPER::READ_PREFIX_LIKE) {
                    if (((static_cast<uint8_t>(m_ixOpers[0]) & ~KEY_OPER_PREFIX_BITMASK) <
                            static_cast<uint8_t>(KEY_OPER::READ_KEY_OR_PREV))) {
                        m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
                    } else {
                        m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_NEXT;
                    }
                }
            }
            if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_EXACT && m_ix->GetUnique()) {
                m_end = -1;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_EXACT) {
                m_end = m_start;
                m_ixOpers[1] = m_ixOpers[m_start];
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_PREFIX) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_KEY_LIKE) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_KEY_OR_PREV;
            } else if (m_ixOpers[m_start] == KEY_OPER::READ_PREFIX_LIKE) {
                m_end = m_start;
                m_ixOpers[1] = KEY_OPER::READ_PREFIX_OR_PREV;
            }
        } else {
            m_cost = m_costs[1];
            m_start = 1;
            m_end = 0;

            KEY_OPER tmp = m_ixOpers[0];
            m_ixOpers[0] = m_ixOpers[1];
            m_ixOpers[1] = tmp;
        }
    }

    if (m_ixOpers[0] == KEY_OPER::READ_INVALID && m_ixOpers[1] == KEY_OPER::READ_INVALID) {
        return INT_MAX;
    }

    return m_cost;
}

bool MatchIndex::AdjustForOrdering(bool desc)
{
    if (m_end == -1 && m_ixOpers[0] == KEY_OPER::READ_KEY_EXACT) {  // READ_KEY_EXACT
        return true;
    }

    KEY_OPER curr = (KEY_OPER)(static_cast<uint8_t>(m_ixOpers[0]) & ~KEY_OPER_PREFIX_BITMASK);
    bool hasBoth = (m_start != -1 && m_end != -1);
    bool currDesc = !(curr < KEY_OPER::READ_KEY_OR_PREV);

    if (desc == currDesc) {
        return true;
    } else if (!hasBoth) {
        return false;
    }

    KEY_OPER tmpo = m_ixOpers[0];
    m_ixOpers[0] = m_ixOpers[1];
    m_ixOpers[1] = tmpo;

    int32_t tmp = m_start;
    m_start = m_end;
    m_end = tmp;

    return true;
}

bool MatchIndex::CanApplyOrdering(const int* orderCols) const
{
    int16_t numKeyCols = m_ix->GetNumFields();

    // check if order columns are overlap index matched columns or are suffix for it
    for (int16_t i = 0; i < numKeyCols; i++) {
        // overlap: we can use index ordering
        if (m_colMatch[0][i] != nullptr && orderCols[i] == 1) {
            return true;
        }

        // ordering does not include all index columns from the start
        if (m_colMatch[0][i] != nullptr && orderCols[i] == 0) {
            return false;
        }

        // suffix: the order columns are continuation of index columns, we can use index ordering
        if (m_colMatch[0][i] == nullptr && orderCols[i] == 1) {
            return true;
        }

        // we have gap between matched index columns and order columns
        if (m_colMatch[0][i] == nullptr && orderCols[i] == 0) {
            return false;
        }
    }

    return false;
}

void MatchIndex::Serialize(List** list) const
{
    List* ixlist = nullptr;

    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_ixPosition), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_start), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_end), false, true));
    ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_cost), false, true));
    ixlist = lappend(ixlist, makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(m_fullScan), false, true));
    ixlist = lappend(ixlist, makeConst(INT1OID, -1, InvalidOid, 1, Int8GetDatum(m_order), false, true));

    for (int i = 0; i < 2; i++) {
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_numMatches[i]), false, true));
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_costs[i]), false, true));
        ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_ixOpers[i]), false, true));

        for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
            ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, UInt32GetDatum(m_opers[i][j]), false, true));
            ixlist = lappend(ixlist, makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum(m_params[i][j]), false, true));
        }
    }

    *list = list_concat(*list, ixlist);
}

void MatchIndex::Deserialize(ListCell* cell, uint64_t exTableID)
{
    MOT::TxnManager* txn = GetSafeTxn(__FUNCTION__);

    m_ixPosition = (int32_t)((Const*)lfirst(cell))->constvalue;
    MOT::Table* table = txn->GetTableByExternalId(exTableID);
    if (table != nullptr) {
        m_ix = table->GetIndex(m_ixPosition);
    }

    cell = lnext(cell);

    m_start = (int32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_end = (int32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_cost = (uint32_t)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_fullScan = (bool)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    m_order = (SortDir)((Const*)lfirst(cell))->constvalue;
    cell = lnext(cell);

    for (int i = 0; i < 2; i++) {
        m_numMatches[i] = (int32_t)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        m_costs[i] = (uint32_t)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);

        m_ixOpers[i] = (KEY_OPER)((Const*)lfirst(cell))->constvalue;
        cell = lnext(cell);
        for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
            m_opers[i][j] = (KEY_OPER)((Const*)lfirst(cell))->constvalue;
            cell = lnext(cell);

            m_params[i][j] = (int32_t)((Const*)lfirst(cell))->constvalue;
            cell = lnext(cell);
        }
    }
}

void MatchIndex::Clean(MOTFdwStateSt* state)
{
    for (int k = 0; k < 2; k++) {
        for (int j = 0; j < m_ix->GetNumFields(); j++) {
            if (m_colMatch[k][j]) {
                if (!list_member(state->m_localConds, m_parentColMatch[k][j]) &&
                    !(m_remoteCondsOrig != nullptr && list_member(m_remoteCondsOrig, m_parentColMatch[k][j])))
                    state->m_localConds = lappend(state->m_localConds, m_parentColMatch[k][j]);
                m_colMatch[k][j] = nullptr;
                m_parentColMatch[k][j] = nullptr;
            }
        }
    }
}
