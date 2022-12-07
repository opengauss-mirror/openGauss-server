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
 * utilities.cpp
 *    MOT utilities.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/utils/utilities.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <arpa/inet.h>
#include <cerrno>
#include "sys_numa_api.h"
#include <string.h>
#include <time.h>
#include <math.h>
#include <sys/syscall.h>
#include <execinfo.h>
#include <algorithm>
#include <cstdarg>

#include "table.h"
#include "utilities.h"
#include "table.h"

#include "mot_list.h"
#include "mot_map.h"
#include "mot_string.h"

#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <array>

namespace MOT {
constexpr char hexMap[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

std::string ExecOsCommand(const string& cmd)
{
    return ExecOsCommand(cmd.c_str());
}

std::string ExecOsCommand(const char* cmd)
{
    const int bufSize = 128;
    char buffer[bufSize];
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) {
        return nullptr;
    }
    while (!feof(pipe.get())) {
        if (fgets(buffer, bufSize, pipe.get()) != nullptr) {
            result += buffer;
        }
    }
    return result;
}

std::string HexStr(const uint8_t* data, uint16_t len)
{
    std::string outStr(len * 2, ' ');
    uint32_t pos = 0;
    for (int i = 0; i < len; ++i) {
        outStr[pos++] = hexMap[HIGH_NIBBLE(data[i])];
        outStr[pos++] = hexMap[LOW_NIBBLE(data[i])];
    }
    return outStr;
}
}  // namespace MOT
