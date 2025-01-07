#include "storage/smgr/storage_converter.h"

LocationConvert g_location_convert[INVALID_EXTEND_STORAGE];

class StorageConverterRegister
{
public:
    StorageConverterRegister() noexcept
    {
        g_location_convert[COMMON_STORAGE] = StorageConvert;
    }
};

ExtentLocation StorageConvert(SMgrRelation sRel, const RelFileNode& relNode, int fd,
                              int extent_size, ForkNumber forknum, BlockNumber logicBlockNumber)
{
    RelFileCompressOption option;
    TransCompressOptions(relNode, &option);

    BlockNumber extentNumber = logicBlockNumber / CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentOffset = logicBlockNumber % CFS_LOGIC_BLOCKS_PER_EXTENT;
    BlockNumber extentStart = (extentNumber * CFS_EXTENT_SIZE) % CFS_MAX_BLOCK_PER_FILE;  // 0   129      129*2 129*3
    BlockNumber extentHeader = extentStart + CFS_LOGIC_BLOCKS_PER_EXTENT;  //              128 129+128  129*2+128

    return {.fd = fd,
            .relFileNode = relNode,
            .extentNumber = extentNumber,
            .extentStart = extentStart,
            .extentOffset = extentOffset,
            .headerNum = extentHeader,
            .chunk_size = (uint16)CHUNK_SIZE_LIST[option.compressChunkSize],
            .algorithm = (uint8)option.compressAlgorithm,
            .is_segment_page = false,
            .is_compress_allowed = true
    };
}
StorageConverterRegister g_storage_converter_register;