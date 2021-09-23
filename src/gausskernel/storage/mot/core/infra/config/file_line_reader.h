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
 * file_line_reader.h
 *    Utility class for parsing file into lines.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/file_line_reader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FILE_LINE_READER_H
#define FILE_LINE_READER_H

#include <fstream>
#include "mot_list.h"
#include "mot_string.h"

namespace MOT {
/**
 * @class Utility class for parsing file into lines.
 */
class FileLineReader {
public:
    /**
     * @brief Constructor.
     * @param configFilePath The configuration file path.
     */
    explicit FileLineReader(const char* configFilePath);

    /** Destructor. */
    ~FileLineReader();

    /**
     * @brief Queries whether the configuration file was opened and parsed successfully.
     * @return True if the reader is valid.
     */
    inline bool IsValid() const
    {
        return m_configFile.is_open();
    }

    /**
     * @brief Retrieves the configuration file path.
     * @return The configuration file path.
     */
    inline const char* GetConfigFilePath() const
    {
        return m_configFilePath.c_str();
    }

    /**
     * @brief Queries whether end-of-file was encountered.
     * @return True if end-of-file was encountered.
     */
    inline bool Eof() const
    {
        return (m_lineItr == m_lines.end());
    }

    /**
     * @brief Moves the file reader to the next line.
     */
    inline void NextLine()
    {
        if (!Eof()) {
            ++m_lineItr;
            ++m_lineNumber;
        }
    }

    /**
     * @brief Retrieves the current line.
     * @return The current line.
     */
    inline const mot_string& GetLine() const
    {
        return *m_lineItr;
    }

    /**
     * @brief Retrieves the current line number.
     * @return The current line number.
     */
    inline uint32_t GetLineNumber() const
    {
        return m_lineNumber;
    }

private:
    /** @var The configuration file path. */
    mot_string m_configFilePath;

    /** @var The configuration file stream. */
    std::ifstream m_configFile;

    /** @var All configuration file lines. */
    mot_list<mot_string> m_lines;

    /** @var An iterator pointing to the currently parsed line. */
    mot_list<mot_string>::const_iterator m_lineItr;

    /** @var The currently parsed line number. */
    uint32_t m_lineNumber;

    /** @brief Parses the configuration file, breaking it into lines. */
    void ParseFile();
};
}  // namespace MOT

#endif /* FILE_LINE_READER_H */
