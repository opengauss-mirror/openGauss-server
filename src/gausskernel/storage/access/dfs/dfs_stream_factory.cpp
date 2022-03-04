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
 *  dfs_stream_factory.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_stream_factory.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <sstream>

#include "orc/OrcObsFile.h"

#include "access/dfs/dfs_stream_factory.h"

#ifdef USE_ASSERT_CHECKING
#define FOREIGNTABLEFILEID (-1)
#endif

namespace dfs {
/*
 * @Description: orc reader InputStream factory
 * @IN/OUT conn: dfs connector
 * @IN/OUT path: rea file path
 * @IN/OUT readerState: reader state
 * @Return: InputStream pointer
 */
DFS_UNIQUE_PTR<dfs::GSInputStream> InputStreamFactory(dfs::DFSConnector *conn, const std::string &path,
                                                      dfs::reader::ReaderState *readerState, bool use_cache)
{
    Assert(conn && readerState);

    int32_t connect_type = conn->getType();
    if (OBS_CONNECTOR == connect_type) {
#ifndef ENABLE_LITE_MODE
        /*
         * when we read file through foreign table,
         * readerState->currentFileID value is -1.
         * Now we only support OBS foreign table
         * access method. Be carefully when enbale_orc_cache
         * for OBS foreign table, we think the data on
         * OBS should be immutable.
         */
        Assert(FOREIGNTABLEFILEID == readerState->currentFileID);
        OBSReadWriteHandler* reader_handler = static_cast<OBSReadWriteHandler *>(conn->getHandler());
        if (reader_handler == NULL) {
            ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION), errmodule(MOD_ORC),
                        errmsg("obs readwrite handler is null")));
        }
        if (use_cache) {
            return dfs::readObsFileWithCache(reader_handler, path, readerState);
        } else {
            return dfs::readObsFile(reader_handler, path, readerState);
        }
#else
        FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
    } else {
        ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION), errmodule(MOD_ORC),
                        errmsg("unsupport connector type %d", connect_type)));
    }

    return DFS_UNIQUE_PTR<GSInputStream>();
}
}  // namespace dfs
