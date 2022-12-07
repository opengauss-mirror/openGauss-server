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
 * mot_match_index.h
 *    Helper class to store the matched index information during planning phase.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_match_index.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_MATCH_INDEX_H
#define MOT_MATCH_INDEX_H

#include "index.h"
#include "logger.h"
#include "nodes/primnodes.h"

typedef struct MOTFdwState_St MOTFdwStateSt;

#define KEY_OPER_PREFIX_BITMASK static_cast<uint8_t>(0x10)

enum class KEY_OPER : uint8_t {
    READ_KEY_EXACT = 0,    // equal
    READ_KEY_LIKE = 1,     // like
    READ_KEY_OR_NEXT = 2,  // ge
    READ_KEY_AFTER = 3,    // gt
    READ_KEY_OR_PREV = 4,  // le
    READ_KEY_BEFORE = 5,   // lt
    READ_INVALID = 6,

    // partial key eq
    READ_PREFIX = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_EXACT,
    // partial key ge
    READ_PREFIX_OR_NEXT = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_OR_NEXT,
    // partial key gt
    READ_PREFIX_AFTER = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_AFTER,
    // partial key le
    READ_PREFIX_OR_PREV = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_OR_PREV,
    // partial key lt
    READ_PREFIX_BEFORE = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_BEFORE,
    // partial key like
    READ_PREFIX_LIKE = KEY_OPER_PREFIX_BITMASK | KEY_OPER::READ_KEY_LIKE,
};

enum class SortDir : uint8_t { SORTDIR_NONE = 0, SORTDIR_ASC = 1, SORTDIR_DESC = 2 };

class MatchIndex {
public:
    MatchIndex()
    {
        Init();
    }

    ~MatchIndex()
    {
        m_remoteConds = nullptr;
        m_remoteCondsOrig = nullptr;
        m_ix = nullptr;
    }

    List* m_remoteConds = nullptr;
    List* m_remoteCondsOrig = nullptr;
    MOT::Index* m_ix = nullptr;
    int32_t m_ixPosition = 0;
    Expr* m_colMatch[2][MAX_KEY_COLUMNS];
    Expr* m_parentColMatch[2][MAX_KEY_COLUMNS];
    KEY_OPER m_opers[2][MAX_KEY_COLUMNS];
    int32_t m_params[2][MAX_KEY_COLUMNS];
    int32_t m_numMatches[2] = {0, 0};
    double m_costs[2] = {0, 0};
    KEY_OPER m_ixOpers[2] = {KEY_OPER::READ_INVALID, KEY_OPER::READ_INVALID};

    // this is for iteration start condition
    int32_t m_start = -1;
    int32_t m_end = -1;
    double m_cost = 0;
    bool m_fullScan = false;
    SortDir m_order;

    void Init()
    {
        for (int i = 0; i < 2; i++) {
            for (uint j = 0; j < MAX_KEY_COLUMNS; j++) {
                m_colMatch[i][j] = nullptr;
                m_parentColMatch[i][j] = nullptr;
                m_opers[i][j] = KEY_OPER::READ_INVALID;
                m_params[i][j] = -1;
            }
            m_ixOpers[i] = KEY_OPER::READ_INVALID;
            m_costs[i] = 0;
            m_numMatches[i] = 0;
        }
        m_start = m_end = -1;
        m_fullScan = false;
        m_order = SortDir::SORTDIR_ASC;
    }

    bool IsSameOper(KEY_OPER op1, KEY_OPER op2) const;
    void ClearPreviousMatch(MOTFdwStateSt* state, bool setLocal, int i, int j);
    bool SetIndexColumn(MOTFdwStateSt* state, int16_t colNum, KEY_OPER op, Expr* expr, Expr* parent, bool setLocal);

    inline bool IsUsable() const
    {
        return (m_colMatch[0][0] != nullptr);
    }

    inline bool IsFullMatch() const
    {
        return (m_numMatches[0] == m_ix->GetNumFields() || m_numMatches[1] == m_ix->GetNumFields());
    }

    inline int32_t GetNumMatchedCols() const
    {
        return m_numMatches[0];
    }

    double GetCost(int numClauses);
    bool CanApplyOrdering(const int* orderCols) const;
    bool AdjustForOrdering(bool desc);
    void Serialize(List** list) const;
    void Deserialize(ListCell* cell, uint64_t exTableID);
    void Clean(MOTFdwStateSt* state);
};

class MatchIndexArr {
public:
    MatchIndexArr()
    {
        m_ixOid = InvalidOid;
        for (uint i = 0; i < MAX_NUM_INDEXES; i++) {
            m_idx[i] = nullptr;
        }
    }

    ~MatchIndexArr()
    {}

    void Clear(bool release = false)
    {
        m_ixOid = InvalidOid;
        for (uint i = 0; i < MAX_NUM_INDEXES; i++) {
            if (m_idx[i]) {
                if (release) {
                    pfree(m_idx[i]);
                }
                m_idx[i] = nullptr;
            }
        }
    }
    Oid m_ixOid;
    MatchIndex* m_idx[MAX_NUM_INDEXES];
};

void InitKeyOperStateMachine();

#endif /* MOT_MATCH_INDEX_H */
