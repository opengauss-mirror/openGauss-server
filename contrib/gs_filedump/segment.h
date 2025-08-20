#ifndef GS_FILEDUMP_SEGMENT_H_
#define GS_FILEDUMP_SEGMENT_H_

using BlockLocationT = struct BlockLocationT {
    unsigned int blockId;
    unsigned int extentId;      // extent id
    unsigned int fileno;        // file no [ 2 - 5 ]
    bool isLevel1;              // level1 is used
    unsigned int level1Id;      // level1 index [ 0 - 256 )
    unsigned int level0Id;      // level0 index
    unsigned int extentOffset;  // extent offset
    unsigned int blockOffset;   // block offset
};

char *SliceFilename(char *filename, int sliceno);
template <typename T>
int ReadHeadPage(FILE *fp, unsigned int relfilenode, T *page, long int magic);

/* Calculate the position of the block (level1, level0, blockOffset) through blockNo. */
void ConvertBlockLocation(unsigned int blockId, BlockLocationT *blockLocation);
void DumpSegmentContent(SegmentHead *segmentHead, unsigned int blockStart, unsigned int blockEnd);

bool CopyFileChunkC(const char *srcPath, const char *destPath, long offset, long size);

void InitSegmentInfo(FILE *fp, FILE *fpToast);

#endif
