#ifndef CARBONDATA_MPPDB_COLUMN_READER_H
#define CARBONDATA_MPPDB_COLUMN_READER_H

#include "carbondata/data_file_reader.h"
#include "carbondata/format/schema_types.h"

#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/cash.h"
#include "utils/dfs_vector.h"
#include "vecexecutor/vectorbatch.h"
#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"

extern THR_LOCAL char *PGXCNodeName;

namespace dfs {
#define CARBONDATA_PSQL_EPOCH_IN_DAYS (10957)
#define CARBONDATA_MICROSECOND_IN_EACH_DAY (24L * 3600 * 1000 * 1000)

constexpr int64_t SECONDS_PER_DAY = INT64_C(60 * 60 * 24);
constexpr int64_t MILLISECONDS_PER_DAY = SECONDS_PER_DAY * INT64_C(1000);
constexpr int64_t MICROSECONDS_PER_DAY = MILLISECONDS_PER_DAY * INT64_C(1000);
constexpr int64_t NANOSECONDS_PER_DAY = MICROSECONDS_PER_DAY * INT64_C(1000);
constexpr int64_t epochOffsetDiff = (8 * MICROSECONDS_PER_DAY / 24);

/* The restriction types of CARBONDATA file:blocklet, page. */
enum CarbonRestrictionType {
    BLOCKLET = 1,
    PAGE = 2
};

typedef Datum (*convertToDatum)(void *, uint64, carbondata::DataType::type, Oid, int32_t, int32, int32_t, bool &, int32,
                                int32, bool &);

namespace reader {
class CarbondataColumnReader : public BaseObject {
public:
    CarbondataColumnReader()
    {
    }

    virtual ~CarbondataColumnReader()
    {
    }

    virtual void begin(std::unique_ptr<GSInputStream> gsInputStream,
                       const carbondata::CarbondataFileMeta *fileMetaData) = 0;

    virtual void Destroy() = 0;

    virtual void setBlockletIndex(uint64_t blockletIndex) = 0;

    virtual uint64_t setPageIndex(uint64_t pageIndex) = 0;

    virtual void setCurrentRowsInPage(uint64_t currentRowsInPage) = 0;

    virtual void readNewPage() = 0;

    virtual int fillScalarVector(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *scalorVector) = 0;

    /*
     * Check if the current column of Carbondata file has predicates.
     * @return true: The predicate of the column exists;
     *		false: The predicate of the column does not exists;
     */
    virtual bool hasPredicate() const = 0;

    /*
     * Filter the obtained data with the predicates on the column
     * and set isSelected flags.
     * @_in_param numValues: The number of rows to be filtered.
     * @_in_out_param isSelected: The flag array to indicate which
     *		row is selected or not.
     */
    virtual void predicateFilter(uint64_t numValues, bool *isSelected) = 0;

    virtual Node *buildColRestriction(CarbonRestrictionType type, uint64_t index) const = 0;
};

CarbondataColumnReader *createCarbondataColumnReader(const carbondata::ColumnSpec *columnSpec, uint32_t columnIndex,
                                                     uint32_t mppColumnIndex, ReaderState *readerState, const Var *var,
                                                     uint64_t footerOffset);
}  // namespace reader
}  // namespace dfs

#endif
