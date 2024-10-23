/*---------------------------------------------------------------------------------------*
 * gms_lob.h
 *
 *  Definition about gms_lob package.
 *
 * IDENTIFICATION
 *        contrib/gms_stats/gms_lob.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_LOB_H
#define GMS_LOB_H
/* blob/clob最大存储长度1G*/
#define LOBMAXSIZE (int64)0x3fffffff
#define AMOUNT_MAX_SIZE 32767
typedef struct GmsLobContext {
    struct HTAB* gmsLobNameHash;
} GmsLobContext;

extern "C" Datum gms_lob_og_createtemporary(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_freetemporary(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_read_blob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_read_clob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_write_blob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_write_clob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_isopen(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_open(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_append_blob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_append_clob(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_close(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_cloblength(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_bloblength(PG_FUNCTION_ARGS);
extern "C" Datum gms_lob_og_null(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);
extern "C" GmsLobContext* get_session_context();
#endif
