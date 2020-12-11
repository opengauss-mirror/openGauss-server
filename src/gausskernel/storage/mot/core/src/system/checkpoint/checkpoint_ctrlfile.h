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
 * checkpoint_ctrlfile.h
 *    Checkpoint control file implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/src/system/checkpoint/checkpoint_ctrlfile.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CHECKPOINT_CTRLFILE_H
#define CHECKPOINT_CTRLFILE_H

namespace MOT {

/**
 * @class CheckpointControlFile
 * @brief This class implements the checkpoint control file logic that
 * currently includes the last good valid checkpoint id.
 */
class CheckpointControlFile {
public:
    CheckpointControlFile() : m_valid(false)
    {}

    ~CheckpointControlFile()
    {}

    bool Init();

    struct CtrlFileElem {
        CtrlFileElem(uint64_t id = invalidId, uint64_t lsn = invalidId, uint64_t replay = invalidId)
            : checkpointId(id), lsn(lsn), lastReplayLsn(replay)
        {}

        void Init()
        {
            checkpointId = invalidId;
            lsn = invalidId;
            lastReplayLsn = invalidId;
        }

        uint64_t checkpointId;
        uint64_t lsn;
        uint64_t lastReplayLsn;
    };

    static CheckpointControlFile* GetCtrlFile();

    uint64_t GetId() const
    {
        return m_ctrlFileData.entry[0].checkpointId;
    }

    uint64_t GetLsn() const
    {
        return m_ctrlFileData.entry[0].lsn;
    }

    uint64_t GetLastReplayLsn() const
    {
        return m_ctrlFileData.entry[0].lastReplayLsn;
    }

    /**
     * @brief Performs a durable update of the checkpoint id in the file
     * @param id The checkpoint's id.
     * @return  Boolean value denoting success or failure.
     */
    bool Update(uint64_t id, uint64_t lsn, uint64_t lastReplayLsn);

    bool IsValid() const
    {
        return m_valid;
    }

    void Print();

    static const uint64_t invalidId = (uint64_t)(-1);

    static constexpr const char* CTRL_FILE_NAME = "mot.ctrl";

private:
    static constexpr int NUM_ELEMS = 1;

    struct CtrlFileData {
        void Init()
        {
            for (int i = 0; i < NUM_ELEMS; ++i) {
                entry[i].Init();
            }
        }

        CtrlFileElem entry[NUM_ELEMS];
    };

    static bool initialized;

    static CheckpointControlFile* ctrlfileInst;

    const char* m_defaultDir = "/tmp";

    std::string m_fullPath;

    struct CtrlFileData m_ctrlFileData;

    bool m_valid;
};
}  // namespace MOT

#endif  // CHECKPOINT_CTRLFILE_H
