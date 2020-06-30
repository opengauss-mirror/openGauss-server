/* -------------------------------------------------------------------------
 *
 * printtup.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/printtup.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include "utils/portal.h"
#include "distributelayer/streamProducer.h"

extern DestReceiver* printtup_create_DR(CommandDest dest);
extern DestReceiver* createStreamDestReceiver(CommandDest dest);
extern void SetStreamReceiverParams(DestReceiver* self, StreamProducer* arg, Portal portal);
extern void SetRemoteDestReceiverParams(DestReceiver* self, Portal portal);

extern void SendRowDescriptionMessage(StringInfo buf, TupleDesc typeinfo, List* targetlist, int16* formats);

extern void debugStartup(DestReceiver* self, int operation, TupleDesc typeinfo);
extern void debugtup(TupleTableSlot* slot, DestReceiver* self);

/* XXX these are really in executor/spi.c */
extern void spi_dest_startup(DestReceiver* self, int operation, TupleDesc typeinfo);
extern void spi_printtup(TupleTableSlot* slot, DestReceiver* self);

extern void printBatch(VectorBatch* batch, DestReceiver* self);
extern void printtup(TupleTableSlot* slot, DestReceiver* self);
extern void printbatchStream(VectorBatch* batch, DestReceiver* self);
extern void printtupStream(TupleTableSlot* slot, DestReceiver* self);
extern void assembleStreamMessage(TupleTableSlot* slot, DestReceiver* self, StringInfo buf);
extern void assembleStreamBatchMessage(BatchCompressType ctype, VectorBatch* batch, StringInfo buf);

typedef struct {       /* Per-attribute information */
    Oid typoutput;     /* Oid for the type's text output fn */
    Oid typsend;       /* Oid for the type's binary output fn */
    bool typisvarlena; /* is it varlena (ie possibly toastable)? */
    int16 format;      /* format code for this column */
    FmgrInfo finfo;    /* Precomputed call info for output fn */
} PrinttupAttrInfo;

typedef struct {
    DestReceiver pub;   /* publicly-known function pointers */
    StringInfoData buf; /* output buffer */
    Portal portal;      /* the Portal we are printing from */
    bool sendDescrip;   /* send RowDescription at startup? */
    TupleDesc attrinfo; /* The attr info we are set up for */
    int nattrs;
    PrinttupAttrInfo* myinfo; /* Cached info about each attr */
    int16* formats;     /* format code for each column */
} DR_printtup;

typedef struct {
    DestReceiver pub;   /* publicly-known function pointers */
    StringInfoData buf; /* output buffer */
    Portal portal;      /* the Portal we are printing from */
    bool sendDescrip;   /* send RowDescription at startup? */
    TupleDesc attrinfo; /* The attr info we are set up for */
    int nattrs;
    PrinttupAttrInfo* myinfo; /* Cached info about each attr */
    StreamProducer* arg;
} streamReceiver;

#endif /* PRINTTUP_H */
