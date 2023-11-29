/* ---------------------------------------------------------------------------------------
 * 
 * xlogreader.h
 *        Definitions for the generic XLog reading facility
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2013-2015, PostgreSQL Global Development Group 
 *
 * 
 * IDENTIFICATION
 *        src/include/access/xlogreader.h
 *
 * NOTES
 * 		See the definition of the XLogReaderState struct for instructions on
 * 		how to use the XLogReader infrastructure.
 *
 * 		The basic idea is to allocate an XLogReaderState via
 * 		XLogReaderAllocate(), and call XLogReadRecord() until it returns NULL.
 *
 *		After reading a record with XLogReadRecord(), it's decomposed into
 *		the per-block and main data parts, and the parts can be accessed
 *		with the XLogRec* macros and functions. You can also decode a
 *		record that's already constructed in memory, without reading from
 *		disk, by calling the DecodeXLogRecord() function.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef XLOGREADER_H
#define XLOGREADER_H

#include "access/xlogrecord.h"
#include "securec_check.h"
#include "access/xlog_basic.h"

#define offsetof_twoversion(type, typeold, field) (offsetof(type, field) - offsetof(typeold, field))
#define XLOG_READER_MAX_MSGLENTH 1024

/* Get a new XLogReader */
extern XLogReaderState* XLogReaderAllocate(XLogPageReadCB pagereadfunc, void* private_data, Size alignedSize = 0);

/* Free an XLogReader */
extern void XLogReaderFree(XLogReaderState* state);
/* Read the next XLog record. Returns NULL on end-of-WAL or failure */
extern struct XLogRecord* XLogReadRecord(
    XLogReaderState* state, XLogRecPtr recptr, char** errormsg, bool doDecode = true, char* xlog_path = NULL);
extern struct XLogRecord* XLogReadRecordFromAllDir(
    char** xlogDirs, int xlogDirNum, XLogReaderState *xlogReader, XLogRecPtr curLsn, char** errorMsg);

extern bool XLogRecGetBlockTag(XLogReaderState *record, uint8 block_id, RelFileNode *rnode, ForkNumber *forknum,
    BlockNumber *blknum, XLogPhyBlock *pblk = NULL);
extern bool XLogRecGetBlockLastLsn(XLogReaderState* record, uint8 block_id, XLogRecPtr* lsn);
extern char* XLogRecGetBlockImage(XLogReaderState* record, uint8 block_id, uint16* hole_offset, uint16* hole_length);
extern void XLogRecGetPhysicalBlock(const XLogReaderState *record, uint8 blockId,
                                    uint8 *segFileno, BlockNumber *segBlockno);
extern void XLogRecGetVMPhysicalBlock(const XLogReaderState *record, uint8 blockId,
                                    uint8 *vmFileno, BlockNumber *vmblock, bool *has_vm_loc);

/* Invalidate read state */
extern void XLogReaderInvalReadState(XLogReaderState* state);

extern XLogRecPtr XLogFindNextRecord(XLogReaderState* state, XLogRecPtr RecPtr, XLogRecPtr *endPtr = NULL, char* xlog_path = NULL);
extern XLogRecPtr SSFindMaxLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *maxLsnCrc, char** xlogDirs, int xlogDirNum);
extern XLogRecPtr FindMaxLSN(char* workingpath, char* returnmsg, int msg_len, pg_crc32* maxLsnCrc, 
    uint32 *maxLsnLen = NULL, TimeLineID *returnTli = NULL, char* xlog_path = NULL);
extern XLogRecPtr FindMinLSN(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *minLsnCrc);
extern void CloseXlogFile(void);
extern int SimpleXLogPageRead(XLogReaderState* xlogreader, XLogRecPtr targetPagePtr, int reqLen,
    XLogRecPtr targetRecPtr, char* readBuf, TimeLineID* pageTLI, char* xlog_path = NULL);
extern void CloseXlogFile(void);

/* Functions for decoding an XLogRecord */
extern bool DecodeXLogRecord(XLogReaderState* state, XLogRecord* record, char** errmsg);

#define XLogRecGetTotalLen(decoder) ((decoder)->decoded_record->xl_tot_len)
#define XLogRecGetPrev(decoder) ((decoder)->decoded_record->xl_prev)
#define XLogRecGetInfo(decoder) ((decoder)->decoded_record->xl_info)
#define XLogRecGetTdeInfo(decoder) ((decoder)->isTde)
#define XLogRecGetRmid(decoder) ((decoder)->decoded_record->xl_rmid)
#define XLogRecGetXid(decoder) ((decoder)->decoded_record->xl_xid)
#define XLogRecGetTerm(decoder) ((decoder)->decoded_record->xl_term & XLOG_MASK_TERM)
#define XLogRecGetBucketId(decoder) ((decoder)->decoded_record->xl_bucket_id - 1)
#define XLogRecGetCrc(decoder) ((decoder)->decoded_record->xl_crc)
#define XLogRecGetOrigin(decoder) ((decoder)->record_origin)
#define XLogRecGetData(decoder) ((decoder)->main_data)
#define XLogRecGetDataLen(decoder) ((decoder)->main_data_len)
#define XLogRecHasAnyBlockRefs(decoder) ((decoder)->max_block_id >= 0)
#define XLogRecHasBlockRef(decoder, block_id) ((decoder)->blocks[block_id].in_use)
#define XLogRecHasBlockImage(decoder, block_id) ((decoder)->blocks[block_id].has_image)

extern void RestoreBlockImage(const char* bkp_image, uint16 hole_offset, uint16 hole_length, char* page);
extern char* XLogRecGetBlockData(XLogReaderState* record, uint8 block_id, Size* len);
extern bool allocate_recordbuf(XLogReaderState* state, uint32 reclength);
extern bool XlogFileIsExisted(const char* workingPath, XLogRecPtr inputLsn, TimeLineID timeLine);
extern void ResetDecoder(XLogReaderState* state);
bool ValidXLogPageHeader(XLogReaderState* state, XLogRecPtr recptr, XLogPageHeader hdr);
void report_invalid_record(XLogReaderState* state, const char* fmt, ...)
    /*
     * This extension allows gcc to check the format string for consistency with
     * the supplied arguments.
     */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

bool ValidXLogRecordHeader(
    XLogReaderState* state, XLogRecPtr RecPtr, XLogRecPtr PrevRecPtr, XLogRecord* record, bool randAccess);
bool ValidXLogRecord(XLogReaderState* state, XLogRecord* record, XLogRecPtr recptr);
Size SimpleValidatePage(XLogRecPtr targetPagePtr, char* page,  XLogPageReadCB pagereadfunc);
extern int read_library(char *bufptr, int nlibrary);
extern char *GetRepOriginPtr(char *xnodes, uint64 xinfo, int nsubxacts, int nmsgs, int nrels, int nlibrary, bool compress);
#endif /* XLOGREADER_H */
