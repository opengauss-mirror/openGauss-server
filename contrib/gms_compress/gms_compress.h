#ifndef __GMS_COMPRESS__
#define __GMS_COMPRESS__


#include "postgres.h"

#define GZIP_COMPRESS_EXTRA_LENGTH 18
#define GZIP_MIN_LENGTH 14
#define GZIP_HEADER_1 ((unsigned char)0x1f)
#define GZIP_HEADER_2 ((unsigned char)0x8b)
#define UTLCOMP_MAX_HANDLE 5

typedef struct gms_context {
    void *uncompressed_data; /* data to be compressed */
    void *compressed_data; /* data after compressed or data to be uncompressed */
    int2 compress_level; /* 0 for uncompress, 1~9 for compress */
    bool used;
} gms_context;
typedef struct gms_compress_context {
    gms_context context[UTLCOMP_MAX_HANDLE];
} gms_compress_context;

extern "C" Datum gms_lz_compress(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_uncompress(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_compress_open(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_compress_close(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_compress_add(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_uncompress_open(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_uncompress_close(PG_FUNCTION_ARGS);
extern "C" Datum gms_lz_uncompress_extract(PG_FUNCTION_ARGS);
extern "C" Datum gms_isopen(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);


#endif // __GMS_COMPRESS__