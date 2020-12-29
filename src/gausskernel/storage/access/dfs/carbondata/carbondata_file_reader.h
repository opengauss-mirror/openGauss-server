#ifndef CARBONDATA_FILE_READER_H
#define CARBONDATA_FILE_READER_H

#include "carbondata/data_file_reader.h"

#include "utils/dfs_vector.h"
#include "utils/bloom_filter.h"
#include "vecexecutor/vectorbatch.h"
#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"

#include "carbondata_column_reader.h"

extern THR_LOCAL char *PGXCNodeName;

namespace dfs {
namespace reader {
class CarbondataFileReader : public BaseObject {
public:
    static CarbondataFileReader *create(ReaderState *readerState, std::unique_ptr<GSInputStream> gsInputStream);

    ~CarbondataFileReader();

    uint64_t getFooterSize();

    void getFooterCache(unsigned char *footerCache);

    uint64_t getHeaderSize();

    void getHeaderCache(unsigned char *headerCache);

    void setFileMetaData(const unsigned char *footer, const unsigned char *header, uint64_t footerSize,
                         uint64_t headerSize);

    void begin();
    void Destroy();

    uint64_t getNumRows() const;
    uint64_t getNumColumns() const;
    uint64_t getNumBlocklets() const;
    uint64_t getNumFields() const;

    bool isCurrentBlockletValid() const;

    bool isCurrentPageValid() const;

    int64_t readRows(VectorBatch *batch, uint64_t maxRowsReadOnce, uint64_t curRowsInFile, uint64_t &rowsInFile,
                     uint64_t &rowsSkip, uint64_t &rowsCross, FileOffset *fileOffset);
    void initializeColumnReaders(GSInputStream *gsInputStream);
    void setColBloomFilter(filter::BloomFilter **bloomFilters);

private:
    CarbondataFileReader(ReaderState *readerState,
                         std::unique_ptr<carbondata::CarbondataFileReader> realCarbondataFileReader);

    void destroyColumnReaders();
    void setBlockletIndexForColumnReaders();
    void setPageIndexForColumnReaders();
    void createRowGroupReaderWhenNecessary();
    uint64_t calculateRowsToRead(uint64_t maxRowsReadOnce);

    bool checkPredicateOnCurrentBlocklet();
    bool checkPredicateOnCurrentPage();
    List *buildRestriction(CarbonRestrictionType type, uint64_t blockletIndex);

    bool readAndFilter(uint64_t rowsSkip, uint64_t numRowsToRead);
    void fillVectorBatch(VectorBatch *batch, uint64_t numRowsToRead, FileOffset *fileOffset, uint64_t rowsInFile);
    void fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset);

    bool tryToSkipCurrentBlocklet(uint64_t &rowsSkip, uint64_t &rowsCross);
    bool isStartOfCurrentBlocklet();
    void skipCurrentBlocklet(uint64_t &rowsSkip, uint64_t &rowsCross);
    bool trySwitchToNextBlocklet(uint64_t numRowsRead);

    bool tryToSkipCurrentPage(uint64_t &rowsSkip, uint64_t &rowsCross);
    bool isStartOfCurrentPage();
    void skipCurrentPage(uint64_t &rowsSkip, uint64_t &rowsCross);
    void trySwitchToNextPage(uint64_t numRowsRead);

    bool hasRestriction();

    inline uint32_t numberOfColumns()
    {
        return m_readerState->relAttrNum;
    }

    inline bool isColumnRequiredToRead(uint32_t columnIndexInRead)
    {
        return m_readerState->readRequired[columnIndexInRead];
    }

    inline uint32_t columnIndexInFile(uint32_t columnIndexInRead)
    {
        return m_readerState->readColIDs[columnIndexInRead];
    }

    inline void switchToNextBlocklet()
    {
        m_currentBlockletIndex++;
        m_currentPageIndex = 0;

        m_totalRowsInCurrentBlocklet = 0;
        m_totalPagesInCurrentBlocklet = 0;
        m_totalRowsInCurrentPage = 0;

        m_rowsReadInCurrentBlocklet = 0;
        m_rowsReadInCurrentPage = 0;
    }

    inline void switchToNextPage()
    {
        m_currentPageIndex++;

        m_rowsReadInCurrentPage = 0;
        m_totalRowsInCurrentPage = 0;
    }

    inline bool canSkipBatch(uint64_t rowsToRead) const
    {
        for (uint64_t i = 0; i < rowsToRead; i++) {
            if (isSelected[i]) {
                return false;
            }
        }
        return true;
    }

    inline void resetIsSelectedBuffer(uint64_t rowsToRead)
    {
        errno_t rc = memset_s(isSelected, sizeof(bool) * BatchMaxSize, true, rowsToRead);
        securec_check(rc, "\0", "\0");
    }

    inline void initSchemaOrdinalList()
    {
        m_schemaOrdinalList = (uint32_t *)palloc0(sizeof(uint32_t) * m_numberOfColumns);
        for (uint32_t i = 0; i < m_fileMetaData.m_columnSpecList.size(); i++) {
            if (m_fileMetaData.m_columnSpecList[i].m_schemaOrdinal < m_numberOfColumns) {
                m_schemaOrdinalList[m_fileMetaData.m_columnSpecList[i].m_schemaOrdinal] = i;
            }
        }
    }

    ReaderState *m_readerState;

    std::unique_ptr<carbondata::CarbondataFileReader> m_realCarbondataFileReader;
    carbondata::CarbondataFileMeta m_fileMetaData;
    carbondata::BlockletInfo3 *m_blockletInfo3;

    Vector<CarbondataColumnReader *> m_columnReaders;

    bool *isSelected;
    uint32_t *m_schemaOrdinalList;
    uint64_t m_num_rows;
    uint64_t m_num_columns;
    uint64_t m_num_blocklets;
    uint64_t m_numberOfColumns;
    uint64_t m_numberOfFields;
    uint64_t m_currentBlockletIndex;
    uint64_t m_totalRowsInCurrentBlocklet;
    uint64_t m_rowsReadInCurrentBlocklet;
    uint64_t m_currentPageIndex;
    uint64_t m_totalPagesInCurrentBlocklet;
    uint64_t m_rowsReadInCurrentPage;
    uint64_t m_totalRowsInCurrentPage;
    bool m_hasRowCountInPage;
};
}  // namespace reader
}  // namespace dfs

#endif
