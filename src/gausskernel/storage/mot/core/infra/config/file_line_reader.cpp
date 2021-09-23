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
 * file_line_reader.cpp
 *    Utility class for parsing file into lines.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/config/file_line_reader.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "file_line_reader.h"

namespace MOT {
FileLineReader::FileLineReader(const char* configFilePath)
    : m_configFilePath(configFilePath), m_configFile(configFilePath), m_lineItr(NULL), m_lineNumber(0)
{
    ParseFile();
}

FileLineReader::~FileLineReader()
{}

void FileLineReader::ParseFile()
{
    if (m_configFile) {
        std::string line;
        while (std::getline(m_configFile, line)) {
            m_lines.push_back(line.c_str());
        }

        m_lineItr = m_lines.cbegin();
        m_lineNumber = 1;
    } else {
        m_lineItr = m_lines.cend();
    }
}
}  // namespace MOT
