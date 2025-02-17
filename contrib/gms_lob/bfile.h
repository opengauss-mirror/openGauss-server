/*---------------------------------------------------------------------------------------
 * bfile.h
 *
 *  Definition about bfile type.
 *
 * IDENTIFICATION
 *        contrib/gms_lob/bfile.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_LOB_BFILE_H
#define GMS_LOB_BFILE_H
/* from bfile.c */
extern "C" Datum bfileopen(PG_FUNCTION_ARGS);
extern "C" Datum bfileclose(PG_FUNCTION_ARGS);
extern "C" Datum bfileread(PG_FUNCTION_ARGS);
extern "C" Datum getlength(PG_FUNCTION_ARGS);
#endif