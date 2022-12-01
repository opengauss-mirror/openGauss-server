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
 * recovery_stats.h
 *    Table recovery stats collector.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/system/recovery/recovery_stats.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef RECOVERY_STATS_H
#define RECOVERY_STATS_H

#include <atomic>
#include <map>
#include <vector>

namespace MOT {
/**
 * @class RecoveryStats
 * @brief Recovery statistics collector
 */
class RecoveryStats {
public:
    /**
     * @struct Entry
     * @brief A per-table entry to collect recovery statistics.
     */
    struct TableEntry {
        explicit TableEntry(uint64_t tableId) : m_inserts(0), m_updates(0), m_deletes(0), m_id(tableId)
        {}

        void IncInsert()
        {
            (void)++m_inserts;
        }

        void IncUpdate()
        {
            (void)++m_updates;
        }

        void IncDelete()
        {
            (void)++m_deletes;
        }

        std::atomic<uint64_t> m_inserts;

        std::atomic<uint64_t> m_updates;

        std::atomic<uint64_t> m_deletes;

        uint64_t m_id;
    };

    RecoveryStats() : m_commits(0), m_numEntries(0)
    {}

    ~RecoveryStats()
    {
        Clear();
    }

    void IncInsert(uint64_t id)
    {
        uint32_t idx = 0;
        if (FindIdx(id, idx)) {
            m_tableStats[idx]->IncInsert();
        }
    }

    void IncUpdate(uint64_t id)
    {
        uint32_t idx = 0;
        if (FindIdx(id, idx)) {
            m_tableStats[idx]->IncUpdate();
        }
    }

    void IncDelete(uint64_t id)
    {
        uint32_t idx = 0;
        if (FindIdx(id, idx)) {
            m_tableStats[idx]->IncDelete();
        }
    }

    inline void IncCommit()
    {
        (void)++m_commits;
    }

    /**
     * @brief Prints the stats data to the log
     */
    void Print()
    {
        MOT_LOG_INFO("\n>> Log Recovery Stats >>");
        for (uint32_t i = 0; i < m_numEntries; i++) {
            MOT_LOG_INFO("TableId %lu, Inserts: %lu, Updates: %lu, Deletes: %lu",
                m_tableStats[i]->m_id,
                m_tableStats[i]->m_inserts.load(),
                m_tableStats[i]->m_updates.load(),
                m_tableStats[i]->m_deletes.load());
        }
        MOT_LOG_INFO("Overall commit ops: %lu\n", m_commits.load());
    }

    /**
     * @brief Clears all the statistics.
     */
    void Clear()
    {
        m_idToIdx.clear();
        for (std::vector<TableEntry*>::iterator it = m_tableStats.begin(); it != m_tableStats.end(); (void)++it) {
            if (*it) {
                delete *it;
            }
        }
        m_tableStats.clear();
        m_numEntries = 0;
        m_commits = 0;
    }

private:
    /**
     * @brief Returns a table id array index. it will create
     * a new table entry if necessary.
     * @param tableId The id of the table.
     * @param id The returned array index.
     * @return Boolean value denoting success or failure.
     */
    bool FindIdx(uint64_t tableId, uint32_t& idx)
    {
        TableIdToStatsIdxMap::iterator it;
        std::lock_guard<spin_lock> lock(m_slock);
        idx = m_numEntries;
        it = m_idToIdx.find(tableId);
        if (it == m_idToIdx.end()) {
            TableEntry* newEntry = new (std::nothrow) TableEntry(tableId);
            if (newEntry == nullptr) {
                return false;
            }
            m_tableStats.push_back(newEntry);
            (void)m_idToIdx.insert(std::pair<uint64_t, uint32_t>(tableId, m_numEntries));
            m_numEntries++;
        } else {
            idx = it->second;
        }
        return true;
    }

    using TableIdToStatsIdxMap = std::map<uint64_t, uint32_t>;

    TableIdToStatsIdxMap m_idToIdx;

    std::vector<TableEntry*> m_tableStats;

    std::atomic<uint64_t> m_commits;

    spin_lock m_slock;

    uint32_t m_numEntries;

    DECLARE_CLASS_LOGGER();
};
}  // namespace MOT

#endif /* RECOVERY_STATS_H */
