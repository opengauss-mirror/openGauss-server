/*-------------------------------------------------------------------------
 *
 * generic_xlog.h
 *	  Generic xlog API definition.
 *
 * src/include/access/generic_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENERIC_XLOG_H
#define GENERIC_XLOG_H

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "storage/buf/bufpage.h"
#include "utils/rel.h"

#define MAX_GENERIC_XLOG_PAGES	XLR_NORMAL_MAX_BLOCK_ID
#define GENERIC_XLOG_FULL_IMAGE 0x0001 /* write full-page image */

/* state of generic xlog record construction */
struct GenericXLogState;
typedef struct GenericXLogState GenericXLogState;

/* API for construction of generic xlog records */
extern GenericXLogState *GenericXLogStart(Relation relation);
extern Page GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer,
				int flags);
extern void GenericXLogUnregister(GenericXLogState *state, Buffer buffer);
extern XLogRecPtr GenericXLogFinish(GenericXLogState *state);
extern void GenericXLogAbort(GenericXLogState *state);

/* functions defined for rmgr */
extern void generic_redo(XLogReaderState *record);
extern const char *generic_identify(uint8 info);
extern void generic_desc(StringInfo buf, XLogReaderState *record);

#endif   /* GENERIC_XLOG_H */
