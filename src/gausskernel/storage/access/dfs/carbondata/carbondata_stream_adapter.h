#ifndef CARBONDATA_INPUT_STREAM_ADAPTER_H
#define CARBONDATA_INPUT_STREAM_ADAPTER_H

#include "carbondata/inputstream.h"

#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"

#include "utils/memutils.h"

namespace carbondata {
class CarbondataInputStreamAdapter : public InputStream {
public:
    CarbondataInputStreamAdapter(std::unique_ptr<dfs::GSInputStream> gsinputstream)
        : m_gsinputstream(std::move(gsinputstream)), m_total_size(m_gsinputstream->getLength())
    {
    }

    ~CarbondataInputStreamAdapter()
    {
    }

    /*
     * @Description: read file to buffer
     * @IN buf: dest buffer
     * @IN length: read length
     * @IN offset: read offset
     * @See also:
     */
    uint64_t Read(void *buf, uint64_t offset, uint64_t len) override
    {
        uint64_t real_read_length = 0;

        if (m_total_size <= offset)
            return 0;

        if ((offset + len) > m_total_size) {
            real_read_length = m_total_size - offset;
        } else {
            real_read_length = len;
        }
        m_gsinputstream->read(buf, real_read_length, offset);

        return real_read_length;
    }

    /*
     * @Description:  get file size
     * @Return: file size
     * @See also:
     */
    uint64_t Size() const override
    {
        return m_total_size;
    }

    /*
     * @Description: get natural read size
     * @Return: natural read size
     * @See also:
     */
    uint64_t RecommendedReadSize() const override
    {
        return m_gsinputstream->getNaturalReadSize();
    }

    /*
     * @Description: get file name
     * @Return: file name
     * @See also:
     */
    const std::string &GetName() const override
    {
        return m_gsinputstream->getName();
    }

private:
    std::unique_ptr<dfs::GSInputStream> m_gsinputstream;

    uint64_t m_total_size;
};
}  // namespace carbondata

#endif
