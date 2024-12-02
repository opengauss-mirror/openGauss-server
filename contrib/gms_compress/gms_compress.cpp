#include <cstdlib>
#include <zlib.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "commands/extension.h"
#include "utils/builtins.h"
#include "utils/varbit.h"
#include "utils/memutils.h"

#include "gms_compress.h"

PG_MODULE_MAGIC;

#define MIN_QUALITY 1
#define MAX_QUALITY 9
#define MIN_HANDLE 0
#define MAX_HANDLE 4
#define HANDLE_OFFSET 1
#define UNCOMPRESS_LEVEL 0

PG_FUNCTION_INFO_V1(gms_lz_compress);
PG_FUNCTION_INFO_V1(gms_lz_uncompress);
PG_FUNCTION_INFO_V1(gms_lz_compress_open);
PG_FUNCTION_INFO_V1(gms_lz_compress_close);
PG_FUNCTION_INFO_V1(gms_lz_compress_add);
PG_FUNCTION_INFO_V1(gms_lz_uncompress_open);
PG_FUNCTION_INFO_V1(gms_lz_uncompress_close);
PG_FUNCTION_INFO_V1(gms_lz_uncompress_extract);
PG_FUNCTION_INFO_V1(gms_isopen);

static void gzip_compress(void* src, Size src_len, void** dst, Size* dst_len, int quality);
static void gzip_uncompress(void* src, Size src_len, void** dst, Size* dst_len);
static void free_context(int handle);
static inline void Check_Invalid_Input(gms_compress_context* compress_cxt, int handle);

static uint32 compress_index;

void set_extension_index(uint32 index)
{
    compress_index = index;
}

void init_session_vars(void) {
    RepallocSessionVarsArrayIfNecessary();
    gms_compress_context* psc = 
        (gms_compress_context*)MemoryContextAlloc(u_sess->self_mem_cxt, sizeof(gms_compress_context));
    u_sess->attr.attr_common.extension_session_vars_array[compress_index] = psc;

    for (int i =0;i < UTLCOMP_MAX_HANDLE;i++) {
        psc->context[i].compress_level = -1;
        psc->context[i].compressed_data = NULL;
        psc->context[i].uncompressed_data = NULL;
        psc->context[i].used = false;
    }
}

gms_compress_context* get_session_context() {
    if (u_sess->attr.attr_common.extension_session_vars_array[compress_index] == NULL) {
        init_session_vars();
    }
    return (gms_compress_context*)u_sess->attr.attr_common.extension_session_vars_array[compress_index];
}

Datum gms_lz_compress(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmsg("compressed data cannot be NULL")));
    }

    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    int quality = PG_GETARG_INT32(1);
    Size src_len = VARSIZE_ANY_EXHDR(input_bytea);
    bytea *dst = NULL;
    Size dst_len = 0;

    /* Call gzip_compress function for compression */
    gzip_compress(input_bytea, src_len, (void**)&dst, &dst_len, quality);

    SET_VARSIZE(dst, VARHDRSZ + dst_len);
    PG_RETURN_BYTEA_P(dst);
}

Datum gms_lz_uncompress(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmsg("uncompressed data cannot be NULL")));
    }

    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    Size src_len = VARSIZE_ANY_EXHDR(input_bytea);
    bytea *dst = NULL;
    Size dst_len = 0;

    /* Call gzip_uncompress function for uncompression */
    gzip_uncompress(input_bytea, src_len, (void**)&dst, &dst_len);

    SET_VARSIZE(dst, VARHDRSZ + dst_len);
    PG_RETURN_TEXT_P(dst);
}

/* Open a handle and initialize it */
Datum gms_lz_compress_open(PG_FUNCTION_ARGS)
{
    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    int quality = PG_GETARG_INT32(1);

    if (quality < MIN_QUALITY || quality > MAX_QUALITY) {
        ereport(ERROR, (errmsg("compression quality must be within the range of %d to %d", MIN_QUALITY, MAX_QUALITY)));
    }

    gms_compress_context *compress_cxt = get_session_context();
    for(int i = 0;i < UTLCOMP_MAX_HANDLE;i++) {
        if (!compress_cxt->context[i].used) {
            compress_cxt->context[i].compress_level = quality;
            compress_cxt->context[i].used = true;
            PG_RETURN_INT32(i + HANDLE_OFFSET);
        }
    }

    ereport(ERROR, (errmsg("no handle free, the maximum number of handles is %d", MAX_HANDLE + HANDLE_OFFSET)));
    PG_RETURN_VOID();
}

/* Close a handle and compress the data back */
Datum gms_lz_compress_close(PG_FUNCTION_ARGS)
{
    int handle = PG_GETARG_INT32(0) - HANDLE_OFFSET;
    gms_compress_context *compress_cxt = get_session_context();

    Check_Invalid_Input(compress_cxt, handle);

    if (compress_cxt->context[handle].compress_level == UNCOMPRESS_LEVEL) {
        ereport(ERROR, (errmsg("handle %d is a uncompressed handle", handle + HANDLE_OFFSET)));
    }

    if (compress_cxt->context[handle].uncompressed_data == NULL) {
        free_context(handle);
        PG_RETURN_NULL();
    }

    bytea *input_bytea = (bytea *)compress_cxt->context[handle].uncompressed_data;
    Assert((char*)input_bytea + VARHDRSZ == VARDATA_ANY(input_bytea));
    Size src_len = VARSIZE_ANY_EXHDR(input_bytea);
    if (src_len > MaxAllocSize - VARHDRSZ) {
        free_context(handle);
        ereport(ERROR, (errmsg("data too long, data size cannot exceed 1GB")));
    }

    bytea *dst = NULL;
    Size dst_len = 0;
    int quality = compress_cxt->context[handle].compress_level;
    /* Call gzip_compress function for compression */
    gzip_compress(input_bytea, src_len, (void**)&dst, &dst_len, quality);

    SET_VARSIZE(dst, VARHDRSZ + dst_len);

    free_context(handle);
    PG_RETURN_BYTEA_P(dst);
}

/* Open a handle and store data into it */
Datum gms_lz_compress_add(PG_FUNCTION_ARGS)
{
    int handle = PG_GETARG_INT32(0) - HANDLE_OFFSET;
    gms_compress_context *compress_cxt = get_session_context();
    bytea *input_bytea = PG_GETARG_BYTEA_PP(1);
    Size src_len = VARSIZE_ANY_EXHDR(input_bytea);

    Check_Invalid_Input(compress_cxt, handle);

    if (compress_cxt->context[handle].compress_level == UNCOMPRESS_LEVEL) {
        ereport(ERROR, (errmsg("handle %d is a uncompressed handle", handle + HANDLE_OFFSET)));
    }

    if (src_len > MaxAllocSize - VARHDRSZ) {
        ereport(ERROR, (errmsg("data too long, data size cannot exceed 1GB")));
    }

    bytea *new_data = (bytea *)compress_cxt->context[handle].uncompressed_data;
    if (new_data == NULL) {
        new_data = (bytea *)MemoryContextAlloc(u_sess->self_mem_cxt, src_len + VARHDRSZ);
        SET_VARSIZE(new_data, src_len + VARHDRSZ);
        Assert((char*)new_data + VARHDRSZ == VARDATA_ANY(new_data));
        errno_t rc = memcpy_s(VARDATA(new_data), src_len, VARDATA(input_bytea), src_len);
        securec_check(rc, "\0", "\0");
        compress_cxt->context[handle].uncompressed_data = new_data;
    } else {
        Size dst_len = VARSIZE_ANY_EXHDR(new_data);
        Size uncompressed_size = src_len + dst_len + VARHDRSZ;
        if (uncompressed_size > MaxAllocSize) {
            ereport(ERROR, (errmsg("data too long, data size cannot exceed 1GB")));
        }
        new_data = (bytea *)repalloc(new_data, uncompressed_size);
        SET_VARSIZE(new_data, uncompressed_size);
        Assert((char*)new_data + VARHDRSZ == VARDATA_ANY(new_data));
        errno_t rc = memcpy_s(VARDATA(new_data) + dst_len, src_len, VARDATA(input_bytea), src_len);
        securec_check(rc, "\0", "\0");
        compress_cxt->context[handle].uncompressed_data = new_data;
    }
    PG_RETURN_VOID();
}

/* Open a handle and initialize it */
Datum gms_lz_uncompress_open(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmsg("uncompress_data can not be NULL")));
    }

    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    gms_compress_context *compress_cxt = get_session_context();

    for(int i = 0;i < UTLCOMP_MAX_HANDLE;i++) {
        if (!compress_cxt->context[i].used) {
            compress_cxt->context[i].compressed_data = input_bytea;
            compress_cxt->context[i].compress_level = UNCOMPRESS_LEVEL;
            compress_cxt->context[i].used = true;

            PG_RETURN_INT32(i + HANDLE_OFFSET);
        }
    }

    ereport(ERROR, (errmsg("no handle free, the maximum number of handles is %d", MAX_HANDLE + HANDLE_OFFSET)));
    PG_RETURN_VOID();
}

/* Close and free the handle */
Datum gms_lz_uncompress_close(PG_FUNCTION_ARGS)
{
    int handle = PG_GETARG_INT32(0) - HANDLE_OFFSET;
    gms_compress_context *compress_cxt = get_session_context();

    Check_Invalid_Input(compress_cxt, handle);

    if (compress_cxt->context[handle].compress_level != UNCOMPRESS_LEVEL) {
        ereport(ERROR, (errmsg("handle %d is a compressed handle", handle + HANDLE_OFFSET)));
    }

    free_context(handle);
    PG_RETURN_VOID();
}

/* Open a handle and uncompress the data back  */
Datum gms_lz_uncompress_extract(PG_FUNCTION_ARGS)
{
    int handle = PG_GETARG_INT32(0) - HANDLE_OFFSET;
    gms_compress_context *compress_cxt = get_session_context();

    Check_Invalid_Input(compress_cxt, handle);

    if (compress_cxt->context[handle].compressed_data == NULL) {
        ereport(ERROR, (errmsg("no compressed data found in handle %d", handle + HANDLE_OFFSET)));
    }

    if (compress_cxt->context[handle].compress_level != UNCOMPRESS_LEVEL) {
        ereport(ERROR, (errmsg("handle %d is a compressed handle", handle + HANDLE_OFFSET)));
    }

    bytea *src = (bytea *)compress_cxt->context[handle].compressed_data;
    Size src_len = VARSIZE_ANY_EXHDR(src);
    Size dst_len = 0;
    bytea *dst = NULL;
    
    compress_cxt->context[handle].compressed_data = NULL;
    gzip_uncompress(src, src_len, (void**)&dst, &dst_len);

    SET_VARSIZE(dst, VARHDRSZ + dst_len);

    PG_RETURN_BYTEA_P(dst);
}

/* Check if a handle has been opened */
Datum gms_isopen(PG_FUNCTION_ARGS)
{
    int handle = PG_GETARG_INT32(0) - HANDLE_OFFSET;
    if (handle < MIN_HANDLE || handle > MAX_HANDLE) {
        return false;
    }
    
    return get_session_context()->context[handle].used;
}

/* Using the zlib library and Lemoel Xiv algorithm to implement compression functionality */
static void gzip_compress(void* src, Size src_len, void** dst, Size* dst_len, int quality)
{
    /* The compression quality is limited between 1 and 9 */
    if (quality < MIN_QUALITY || quality > MAX_QUALITY) {
        ereport(ERROR, (errmsg("compression quality must be within the range of %d to %d", MIN_QUALITY, MAX_QUALITY)));
    }

    bytea *input_bytea = (bytea*)src;
    Size compressed_size = compressBound(src_len) + GZIP_COMPRESS_EXTRA_LENGTH;
    bytea *result = (bytea*)palloc(VARHDRSZ + compressed_size);
    SET_VARSIZE(result, VARHDRSZ + compressed_size);

    z_stream c_stream;
    c_stream.zalloc = NULL;
    c_stream.zfree = NULL;
    c_stream.opaque = NULL;
    // MAX_WBITS + 16 for gzip
    if (deflateInit2(&c_stream, quality, Z_DEFLATED, MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib compression initialization failed")));
    }

    c_stream.avail_out = compressed_size; // output size  
    c_stream.next_out = (Bytef*)VARDATA_ANY(result); // output buffer  
    c_stream.avail_in = src_len; // input size  
    c_stream.next_in = (Bytef*)VARDATA_ANY(input_bytea); // input buffer  

    if (deflate(&c_stream, Z_FINISH) != Z_STREAM_END) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib compression failed")));
    }

    if (deflateEnd(&c_stream) != Z_OK) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib cleaning up compression stream failed")));
    }
    *dst_len = c_stream.total_out;
    *dst = result;
}

/* Using the zlib library and Lemoel Xiv algorithm to implement uncompression functionality */
static void gzip_uncompress(void* src, Size src_len, void** dst, Size* dst_len)
{
    bytea *input_bytea = (bytea*)src;
    if (src_len < GZIP_MIN_LENGTH) {
        ereport(ERROR, (errmsg("too small, minimum length of gzip format is %d bytes", GZIP_MIN_LENGTH)));
    }
    unsigned char *gzip_content = (unsigned char*)VARDATA_ANY(input_bytea);
    if (gzip_content[0] != GZIP_HEADER_1 || gzip_content[1] != GZIP_HEADER_2) {
        ereport(ERROR, (errmsg("data corrupt, invalid compressed data head")));
    }
    uint4 uncompressed_size = *(uint4*)(gzip_content + src_len - sizeof(int));
#ifdef WORDS_BIGENDIAN
    uncompressed_size = BSWAP32(uncompressed_size);
#endif
    if (uncompressed_size > MaxAllocSize - VARHDRSZ) {
        ereport(ERROR, (errmsg("data too long, data size cannot exceed 1GB")));
    }
    bytea *result = (bytea*)palloc(VARHDRSZ + uncompressed_size);
    SET_VARSIZE(result, VARHDRSZ + uncompressed_size);

    z_stream d_stream = { 0 };
    d_stream.zalloc = NULL;
    d_stream.zfree = NULL;
    d_stream.opaque = NULL;
    d_stream.next_in = (Bytef*)VARDATA_ANY(input_bytea);
    d_stream.avail_in = src_len;
    d_stream.avail_out = VARSIZE_ANY_EXHDR(result);
    d_stream.next_out = (Bytef*)VARDATA_ANY(result);
    // MAX_WBITS + 16 for gzip
    if (inflateInit2(&d_stream, 16 + MAX_WBITS) != Z_OK) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib uncompression initialization failed")));
    }
    if (inflate(&d_stream, Z_FINISH) != Z_STREAM_END) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib uncompression failed")));
    }

    if (inflateEnd(&d_stream) != Z_OK) {
        pfree_ext(result);
        ereport(ERROR, (errmsg("zlib cleaning up uncompression stream failed")));
    }
    *dst_len = d_stream.total_out;
    *dst = result;
    return;
}

/**
 * Because compressed_data is the data source address and 
 * uncompressed_data is the newly created data address, 
 * it is necessary to point compressed_data to null and release uncompressed_data
*/
static void free_context(int handle)
{
    if (handle < MIN_HANDLE || handle > MAX_HANDLE)
    {
        return;
    }
    gms_compress_context *compress_cxt = get_session_context();
    compress_cxt->context[handle].compressed_data =NULL;
    pfree_ext(compress_cxt->context[handle].uncompressed_data);
    compress_cxt->context[handle].compress_level = -1;
    compress_cxt->context[handle].used = false;
    return;
}

static inline void Check_Invalid_Input(gms_compress_context* compress_cxt, int handle) {
    if (handle < MIN_HANDLE || handle > MAX_HANDLE) {
        ereport(ERROR, (errmsg("invalid handle, it be within the range of %d to %d",
            MIN_HANDLE + HANDLE_OFFSET, MAX_HANDLE + HANDLE_OFFSET)));
    }

    if (!compress_cxt->context[handle].used) {
        ereport(ERROR, (errmsg("handle %d is not be used", handle + HANDLE_OFFSET)));
    }
}
