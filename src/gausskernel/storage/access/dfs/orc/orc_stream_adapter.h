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
 * orc_stream_adapter.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/orc_stream_adapter.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SRC_BACKEND_ACCESS_DFS_ORC_ORC_STREAM_ADAPTER_H
#define SRC_BACKEND_ACCESS_DFS_ORC_ORC_STREAM_ADAPTER_H
#include <string>

#ifndef ENABLE_LITE_MODE
/* Becareful: liborc header file must before  openGauss header file */
#include "orc/Adaptor.hh"
#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"
#endif

namespace orc {
class OrcInputStreamAdapter : public InputStream {
public:
    OrcInputStreamAdapter(std::unique_ptr<dfs::GSInputStream> gsinputstream) : m_gsinputstream(std::move(gsinputstream))
    {
    }

    ~OrcInputStreamAdapter()
    {
    }

    /*
     * @Description:  get file size
     * @Return: file size
     * @See also:
     */
    uint64_t getLength() const
    {
        return m_gsinputstream->getLength();
    }

    /*
     * @Description: get natural read size
     * @Return: natural read size
     * @See also:
     */
    uint64_t getNaturalReadSize() const override
    {
        return m_gsinputstream->getNaturalReadSize();
    }

    /*
     * @Description: read file to buffer
     * @IN buf: dest buffer
     * @IN length: read length
     * @IN offset: read offset
     * @See also:
     */
    void read(void *buf, uint64_t length, uint64_t offset) override
    {
        return m_gsinputstream->read(buf, length, offset);
    }

    /*
     * @Description: get file name
     * @Return: file name
     * @See also:
     */
    const std::string &getName() const override
    {
        return m_gsinputstream->getName();
    }

private:
    std::unique_ptr<dfs::GSInputStream> m_gsinputstream;
};
}  // namespace orc

#endif  // SRC_BACKEND_ACCESS_DFS_ORC_ORC_STREAM_ADAPTER_H