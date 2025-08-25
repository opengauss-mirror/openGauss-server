#include <cstdio>
#include <string>

#include "gs_filedump.h"
#include "storage/smgr/segment.h"
#include "segment.h"

/* Program exit code */
static int g_exitCode = 0;
int g_blockSize = 8192;

/* Return a palloc string, and callers should free it */
char *SliceFilename(char *filename, int sliceno)
{
    char *res = nullptr;
    int len = strlen(filename);
    if (len == 0 || len > MAXPGPATH) {
        fprintf(stderr, "Error: get length of filename error.\n");
    }
    if (sliceno == 0) {
        res = (char *)malloc(len + 1);
        errno_t rc = sprintf_s(res, len + 1, "%s", filename);
        securec_check(rc, "\0", "\0");
    } else {
        res = (char *)malloc(len + MAX_LEN_OF_MAXINTRANGE);
        errno_t rc = sprintf_s(res, len + MAX_LEN_OF_MAXINTRANGE, "%s.%d", filename, sliceno);
        securec_check(rc, "\0", "\0");
    }
    return res;
}

template <typename T>
int ReadHeadPage(FILE *fp, unsigned int relfilenode, T *page, long int magic)
{
    char *block = (char *)malloc(g_blockSize);
    if (fp == nullptr || page == nullptr) {
        fprintf(stderr, "Error: Invalid input parameters.\n");
        return MEMORY_ALL_FAILED;
    }

    if (!block) {
        fprintf(stderr, "Error: Memory allocation failed.\n");
        return MEMORY_ALL_FAILED;
    }
    unsigned int position = relfilenode * g_blockSize;
    if (fseek(fp, position, SEEK_SET) != 0) {
        fprintf(stderr, "Error: Failed to fseek file position.\n");
        return ATTRIBUTE_SIZE_ERROR;
    }

    /* Read block from file */
    int gBytesToFormat = fread(block, 1, g_blockSize, fp);
    if (gBytesToFormat != g_blockSize) {
        fprintf(stderr, "Error: Failed to read block from file.\n");
        free(block);
        return ATTRIBUTE_SIZE_ERROR;
    }
    char *bufferTemp = static_cast<char *>(memmem(block, g_blockSize, &magic, sizeof(magic)));
    if (bufferTemp == nullptr) {
        printf("Warning: SEGMENT_HEAD_MAGIC not found\n");
        free(block);
        return ATTRIBUTE_SIZE_ERROR;
    }
    
    bool isMagicMatched = false;
    if (magic == BMTLEVEL0_MAGIC) {
        isMagicMatched = IsBMTLevel0Block(reinterpret_cast<T *>(bufferTemp));
    } else {
        isMagicMatched = IsNormalSegmentHead(reinterpret_cast<T *>(bufferTemp));
    }

    if (!isMagicMatched) {
        printf("Warning: Is not a NormalSegmentHead\n");
        free(block);
        return ATTRIBUTE_SIZE_ERROR;
    }

    errno_t rc = memcpy_s(page, sizeof(T), (T *)bufferTemp, sizeof(T));
    securec_check(rc, "\0", "\0");
    free(block);
    return RETURN_SUCCESS;
}

void InitSegmentInfo(FILE *fp, FILE *fpToast)
{
    if (!fp) {
        printf("Error: Input file pointer is null.\n");
        exit(-1);
    }
    SegmentHead *seg_head = new SegmentHead();
    int res = ReadHeadPage<SegmentHead>(fp, (g_tableRelfilenode % DF_FILE_SLICE_BLOCKS), seg_head, SEGMENT_HEAD_MAGIC);
    if (res != 0) {
        printf("Error: SEGMENT_HEAD_MAGIC not found\n");
        exit(-1);
    }
    if (g_blockStart < 0) {
        g_blockStart = 0;
    }
    if (g_blockEnd < 0) {
        g_blockEnd = seg_head->nblocks - 1;
    }
    printf("\n* Block Number: <%d> -----\n", seg_head->nblocks);
    if (g_blockEnd >= (int)seg_head->nblocks) {
        printf("* Error: Block Number <%d> is out of range, max is <%d>\n", g_blockEnd, seg_head->nblocks);
        exit(-1);
    }

    /* Path to directory with TOAST realtion file */
    char *toastRelationPath = strdup(g_fileName);
    get_parent_directory(toastRelationPath);

    /* target segment toast filename */
    char toast_relation_filename[MAXPGPATH];
    /* filename of source of segment toast data file */
    char segmentToastDataFile[MAXPGPATH];
    char *segmentToastDataFileTag;
    /* Filename of TOAST relation file */
    FILE *toastRelFp;
    errno_t rc;

    /* copy toast of segment to a new file if g_toastRelfilenode is given. */
    if (g_toastRelfilenode >= 0) {
        rc = sprintf_s(toast_relation_filename, MAXPGPATH, "%s/%d_%s", *toastRelationPath ? toastRelationPath : ".",
                       g_toastRelfilenode, SEGTOASTTAG);
        securec_check(rc, "\0", "\0");
        if (access(toast_relation_filename, F_OK) == -1) {
            toastRelFp = fopen(toast_relation_filename, "wb");
        } else {
            toastRelFp = fopen(toast_relation_filename, "w");
        }
        if (toastRelFp) {
            fclose(toastRelFp);
        } else {
            printf("Error: Failed to open or create file %s\n", toast_relation_filename);
            exit(-1);
        }

        SegmentHead *seg_toast_head = new SegmentHead();
        res = ReadHeadPage<SegmentHead>(fpToast, (g_toastRelfilenode % DF_FILE_SLICE_BLOCKS), seg_toast_head,
                                        SEGMENT_HEAD_MAGIC);
        if (res != 0) {
            printf("Error: SEGMENT_HEAD_MAGIC not found\n");
            exit(-1);
        }
        /* copy SegmentHead.level0_slots */
        for (unsigned int l0slot = 0; l0slot < BMT_HEADER_LEVEL0_SLOTS; l0slot++) {
            unsigned int blocknum = seg_toast_head->level0_slots[l0slot];
            if (blocknum == 0) {
                break;
            }
            unsigned int extentPageCount = ExtentSizeByCount(l0slot);
            unsigned int seg_fileno = EXTENT_SIZE_TO_TYPE(extentPageCount);
            rc = snprintf_s(segmentToastDataFile, MAXPGPATH, MAXPGPATH, "%s/%u", toastRelationPath, seg_fileno);
            securec_check(rc, "\0", "\0");
            segmentToastDataFileTag = SliceFilename(segmentToastDataFile, (blocknum / DF_FILE_SLICE_BLOCKS));

            CopyFileChunkC(segmentToastDataFileTag, toast_relation_filename,
                           (blocknum % DF_FILE_SLICE_BLOCKS) * g_blockSize, extentPageCount * g_blockSize);
        }

        /* copy SegmentHead.level1_slots */
        for (unsigned int l1_slot = 0; l1_slot < BMT_HEADER_LEVEL1_SLOTS; l1_slot++) {
            unsigned int mbt_blocknum = seg_toast_head->level1_slots[l1_slot];
            if (mbt_blocknum == 0) {
                break;
            }
            FILE *mbtFp = fopen(SliceFilename(g_fileName, (mbt_blocknum / DF_FILE_SLICE_BLOCKS)), "rb");
            if (mbtFp) {
                BMTLevel0Page *bmt_level0_page = new BMTLevel0Page();
                res = ReadHeadPage<BMTLevel0Page>(mbtFp, (mbt_blocknum % DF_FILE_SLICE_BLOCKS), bmt_level0_page,
                                                  BMTLEVEL0_MAGIC);
                if (res != 0) {
                    printf("Error: BMTLEVEL0_MAGIC not found\n");
                    exit(-1);
                }
                for (unsigned int mbt_slot = 0; mbt_slot < BMT_LEVEL0_SLOTS; mbt_slot++) {
                    unsigned int mbtSlotBlocknum = bmt_level0_page->slots[mbt_slot];
                    if (mbtSlotBlocknum == 0) {
                        break;
                    }
                    rc =
                        snprintf_s(segmentToastDataFile, MAXPGPATH, MAXPGPATH, "%s/%d", toastRelationPath, EXTENT_8192);
                    securec_check(rc, "\0", "\0");
                    segmentToastDataFileTag =
                        SliceFilename(segmentToastDataFile, (mbtSlotBlocknum / DF_FILE_SLICE_BLOCKS));
                    CopyFileChunkC(segmentToastDataFileTag, toast_relation_filename,
                                   (mbtSlotBlocknum % DF_FILE_SLICE_BLOCKS) * g_blockSize, EXT_SIZE_8192 * g_blockSize);
                }
            }
        }
    }

    DumpSegmentContent(seg_head, g_blockStart, g_blockEnd);

    /* remove temp toast file */
    if (access(toast_relation_filename, F_OK) == 0 && strstr(toast_relation_filename, SEGTOASTTAG)) {
        if (remove(toast_relation_filename) != 0) {
            printf("Error: toastfile: %s remove failed\n", toast_relation_filename);
        }
    }
}

void DumpSegmentContent(SegmentHead *segmentHead, unsigned int blockStart, unsigned int blockEnd)
{
    if (!segmentHead) {
        printf("Error: SegmentHead is null.\n");
        return;
    }
    /* Filename of TOAST relation file */
    FILE *toastRelFp;
    errno_t rc;
    char *segmentDirectory;
    char segmentDataFile[MAXPGPATH];
    char *segmentDataFileTag;

    unsigned int blkstart = 0;
    unsigned int blkend = 0;
    int currentLevel1Index = -1;
    BMTLevel0Page *bmt_level0_page = new BMTLevel0Page();

    BlockLocationT *blockLocationStart = new BlockLocationT();
    BlockLocationT *blockLocationEnd = new BlockLocationT();
    ConvertBlockLocation(blockStart, blockLocationStart);
    ConvertBlockLocation(blockEnd, blockLocationEnd);
    BlockLocationT *blockLocationCurrent = blockLocationStart;
    segmentDirectory = strdup(g_fileName);
    get_parent_directory(segmentDirectory);

    while (blockLocationCurrent->blockId <= blockLocationEnd->blockId) {
        if (blockLocationCurrent->isLevel1) {
            if (currentLevel1Index != static_cast<int>(blockLocationCurrent->level1Id)) {
                currentLevel1Index = blockLocationCurrent->level1Id;
                unsigned int mbtSlotBlocknum = segmentHead->level1_slots[currentLevel1Index];
                FILE *mbtFp = fopen(SliceFilename(g_fileName, (mbtSlotBlocknum / DF_FILE_SLICE_BLOCKS)), "rb");
                if (mbtFp) {
                    int res = ReadHeadPage<BMTLevel0Page>(mbtFp, (mbtSlotBlocknum % DF_FILE_SLICE_BLOCKS),
                                                          bmt_level0_page, BMTLEVEL0_MAGIC);
                    if (res != 0) {
                        printf("Error: BMTLEVEL0_MAGIC not found\n");
                        exit(-1);
                    }
                }
            }
            blkstart = blkend = bmt_level0_page->slots[blockLocationCurrent->level0Id];
            rc = snprintf_s(segmentDataFile, MAXPGPATH, MAXPGPATH, "%s/%d", *segmentDirectory ? segmentDirectory : ".",
                            EXTENT_8192);
            securec_check(rc, "\0", "\0");
        } else {
            blkstart = blkend = segmentHead->level0_slots[blockLocationCurrent->level0Id];
            rc = snprintf_s(segmentDataFile, MAXPGPATH, MAXPGPATH, "%s/%d", *segmentDirectory ? segmentDirectory : ".",
                            blockLocationCurrent->fileno);
            securec_check(rc, "\0", "\0");
        }

        blkstart += blockLocationCurrent->blockOffset;
        if (blockLocationCurrent->extentId == blockLocationEnd->extentId) {
            blkend += blockLocationEnd->blockOffset;
        } else {
            blkend += (ExtentSizeByCount(blockLocationCurrent->extentId) - 1);
        }
        segmentDataFileTag = SliceFilename(segmentDataFile, (blkstart / DF_FILE_SLICE_BLOCKS));
        toastRelFp = fopen(segmentDataFileTag, "rb");
        if (toastRelFp) {
            g_exitCode = DumpFileContents(g_blockOptions, g_controlOptions, toastRelFp, g_blockSize,
                                          blkstart % DF_FILE_SLICE_BLOCKS, blkend % DF_FILE_SLICE_BLOCKS,
                                          false, /* is toast realtion */
                                          0,     /* no toast Oid */
                                          0,     /* no toast external size */
                                          NULL); /* no out toast value */
        } else {
            rc = OPT_RC_FILE;
            printf("Error: Could not open file <%s>.\n", segmentDataFile);
            g_exitCode = 1;
        }
        ConvertBlockLocation(ExtentIdToLogicBlockNum(++blockLocationCurrent->extentId),
            blockLocationCurrent);
    }
}

/* Calculate the position of the block (level1, level0, blockOffset) through blockNo. */
void ConvertBlockLocation(unsigned int blockId, BlockLocationT *blockLocation)
{
    if (blockLocation == nullptr) {
        return;
    }

    blockLocation->isLevel1 = false;
    blockLocation->level1Id = 0;
    blockLocation->blockId = blockId;

    if (blockId < EXT_SIZE_8_TOTAL_PAGES) {
        blockLocation->fileno = EXTENT_8;
        blockLocation->extentId = blockId / EXT_SIZE_8;
        blockLocation->extentOffset = blockLocation->extentId;
        blockLocation->blockOffset = blockId % EXT_SIZE_8;
    } else if (blockId < EXT_SIZE_128_TOTAL_PAGES) {
        blockLocation->fileno = EXTENT_128;
        blockId -= EXT_SIZE_8_TOTAL_PAGES;
        blockLocation->extentOffset = blockId / EXT_SIZE_128;
        blockLocation->extentId = EXT_SIZE_8_BOUNDARY + blockLocation->extentOffset;
        blockLocation->blockOffset = blockId % EXT_SIZE_128;
    } else if (blockId < EXT_SIZE_1024_TOTAL_PAGES) {
        blockLocation->fileno = EXTENT_1024;
        blockId -= EXT_SIZE_128_TOTAL_PAGES;
        blockLocation->extentOffset = blockId / EXT_SIZE_1024;
        blockLocation->extentId = EXT_SIZE_128_BOUNDARY + blockLocation->extentOffset;
        blockLocation->blockOffset = blockId % EXT_SIZE_1024;
    } else {
        blockLocation->fileno = EXTENT_8192;
        blockId -= EXT_SIZE_1024_TOTAL_PAGES;
        blockLocation->extentOffset = blockId / EXT_SIZE_8192;
        blockLocation->extentId = EXT_SIZE_1024_BOUNDARY + blockLocation->extentOffset;
        blockLocation->blockOffset = blockId % EXT_SIZE_8192;
    }

    if (blockLocation->extentId < BMT_HEADER_LEVEL0_SLOTS) {
        blockLocation->level0Id = blockLocation->extentId;
    } else {
        unsigned int leftExtents = blockLocation->extentId - BMT_HEADER_LEVEL0_SLOTS;
        blockLocation->level1Id = leftExtents / BMT_LEVEL0_SLOTS;
        blockLocation->level0Id = leftExtents % BMT_LEVEL0_SLOTS;
        blockLocation->isLevel1 = true;
    }
}

/**
 * @brief copy file chunk
 * @param srcPath source file path
 * @param destPath destination file path
 * @param offset source file offset
 * @param size size to copy
 * @return if success, return true; otherwise, return false
 */
bool CopyFileChunkC(const char *srcPath, const char *destPath, long offset, long size)
{
    if (!srcPath || !destPath || size <= 0) {
        fprintf(stderr, "Invalid input parameters.\n");
        return false;
    }
    FILE *srcFile = fopen(srcPath, "rb");
    if (!srcFile) {
        fprintf(stderr, "Failed to open source file: %s\n", strerror(errno));
        return false;
    }
    /* get destination directory */
    std::string destDir(destPath);
    size_t lastSlash = destDir.find_last_of('/');
    if (lastSlash != std::string::npos) {
        destDir = destDir.substr(0, lastSlash);
        /* check if directory exists */
        struct stat st;
        if (stat(destDir.c_str(), &st) != 0) {
            if (mkdir(destDir.c_str(), 0777) != 0) {
                fprintf(stderr, "Failed to create directory: %s\n", strerror(errno));
                fclose(srcFile);
                return false;
            }
        } else if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory: %s\n", destDir.c_str());
            fclose(srcFile);
            return false;
        }
    }

    FILE *destFile = fopen(destPath, "ab");
    if (!destFile) {
        fprintf(stderr, "Failed to open destination file: %s\n", strerror(errno));
        fclose(srcFile);
        return false;
    }

    if (fseek(srcFile, offset, SEEK_SET) != 0) {
        fprintf(stderr, "fseek failed: %s\n", strerror(errno));
        fclose(srcFile);
        fclose(destFile);
        return false;
    }

    const size_t bufferSize = g_blockSize;
    char buffer[bufferSize];
    long remaining = size;
    while (remaining > 0) {
        size_t readSize = (remaining > static_cast<long>(bufferSize)) ? bufferSize : remaining;
        size_t bytesRead = fread(buffer, 1, readSize, srcFile);
        if (bytesRead == 0) {
            if (feof(srcFile)) {
                break;
            } else {
                fprintf(stderr, "fread error: %s\n", strerror(errno));
                fclose(srcFile);
                fclose(destFile);
                return false;
            }
        }

        size_t bytesWritten = fwrite(buffer, 1, bytesRead, destFile);
        if (bytesWritten != bytesRead) {
            fprintf(stderr, "fwrite error: %s\n", strerror(errno));
            fclose(srcFile);
            fclose(destFile);
            return false;
        }
        remaining -= bytesRead;
    }
    if (fclose(srcFile) != 0) {
        fprintf(stderr, "error: %s\n", strerror(errno));
    }
    if (fclose(destFile) != 0) {
        fprintf(stderr, "error: %s\n", strerror(errno));
    }
    return true;
}
