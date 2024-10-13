/*---------------------------------------------------------------------------------------*
 * gms_raw.h
 *
 *  Definition about gms_raw package.
 *
 * IDENTIFICATION
 *        contrib/gms_raw/gms_raw.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_RAW_H
#define GMS_RAW_H

#define BIG_ENDIAN_FLAG 1
#define LITTLE_ENDIAN_FLAG 2
#define MACHINE_ENDIAN_FLAG 3
#define BINARY_DOUBLE_SIZE 8
#define BINARY_FLOAT_INTEGER_SIZE 4
#define MAX_RAW_SIZE 1024 * 1024 * 1024 - 8203 - 4 //1GB-8203字节-4字节header

extern "C" Datum bit_and(PG_FUNCTION_ARGS);
extern "C" Datum bit_complement(PG_FUNCTION_ARGS);
extern "C" Datum bit_or(PG_FUNCTION_ARGS);
extern "C" Datum bit_xor(PG_FUNCTION_ARGS);
extern "C" Datum cast_from_binary_double(PG_FUNCTION_ARGS);
extern "C" Datum cast_from_binary_float(PG_FUNCTION_ARGS);
extern "C" Datum cast_from_binary_integer(PG_FUNCTION_ARGS);
extern "C" Datum cast_from_number(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_binary_double(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_binary_float(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_binary_integer(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_number(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_nvarchar2(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_raw(PG_FUNCTION_ARGS);
extern "C" Datum cast_to_varchar2(PG_FUNCTION_ARGS);
extern "C" Datum compare(PG_FUNCTION_ARGS);
extern "C" Datum concat(PG_FUNCTION_ARGS);
extern "C" Datum convert(PG_FUNCTION_ARGS);
extern "C" Datum copies(PG_FUNCTION_ARGS);
extern "C" Datum reverse(PG_FUNCTION_ARGS);
extern "C" Datum func_translate(PG_FUNCTION_ARGS);
extern "C" Datum transliterate(PG_FUNCTION_ARGS);
extern "C" Datum xrange(PG_FUNCTION_ARGS);

#endif
