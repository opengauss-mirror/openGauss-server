/* -------------------------------------------------------------------------
 *
 * builtins.h
 *	  Declarations for operations on built-in types.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/utils/builtins.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BUILTINS_H
#define BUILTINS_H

#ifndef FRONTEND_PARSER
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#ifdef PGXC
#include "lib/stringinfo.h"
#endif
#include "utils/sortsupport.h"

/*
 *		Defined in adt/
 */

/* acl.c */
extern Datum has_any_column_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_any_column_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_any_column_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_any_column_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_any_column_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_any_column_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_name_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_name_attnum(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_id_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_id_attnum(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_name_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_name_attnum(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_id_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_id_attnum(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_name_attnum(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_column_privilege_id_attnum(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_table_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_sequence_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_database_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_directory_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_foreign_data_wrapper_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_function_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_language_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_nodegroup_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_schema_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_server_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_tablespace_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_type_privilege_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id_id(PG_FUNCTION_ARGS);
extern Datum pg_has_role_name(PG_FUNCTION_ARGS);
extern Datum pg_has_role_id(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_cmk_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_name_name(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_name_id(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_id_name(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_id_id(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_name(PG_FUNCTION_ARGS);
extern Datum has_cek_privilege_id(PG_FUNCTION_ARGS);
extern Datum has_any_privilege(PG_FUNCTION_ARGS);

/* bool.c */
extern Datum boolin(PG_FUNCTION_ARGS);
extern Datum boolout(PG_FUNCTION_ARGS);
extern Datum boolrecv(PG_FUNCTION_ARGS);
extern Datum boolsend(PG_FUNCTION_ARGS);
extern Datum booltext(PG_FUNCTION_ARGS);
extern Datum booleq(PG_FUNCTION_ARGS);
extern Datum boolne(PG_FUNCTION_ARGS);
extern Datum boollt(PG_FUNCTION_ARGS);
extern Datum boolgt(PG_FUNCTION_ARGS);
extern Datum boolle(PG_FUNCTION_ARGS);
extern Datum boolge(PG_FUNCTION_ARGS);
extern Datum disable_conn(PG_FUNCTION_ARGS);
extern Datum read_disable_conn_file(void);
extern Datum booland_statefunc(PG_FUNCTION_ARGS);
extern Datum boolor_statefunc(PG_FUNCTION_ARGS);
extern bool parse_bool(const char* value, bool* result);
extern bool parse_bool_with_len(const char* value, size_t len, bool* result);

/* char.c */
extern Datum charin(PG_FUNCTION_ARGS);
extern Datum charout(PG_FUNCTION_ARGS);
extern Datum charrecv(PG_FUNCTION_ARGS);
extern Datum charsend(PG_FUNCTION_ARGS);
extern Datum chareq(PG_FUNCTION_ARGS);
extern Datum charne(PG_FUNCTION_ARGS);
extern Datum charlt(PG_FUNCTION_ARGS);
extern Datum charle(PG_FUNCTION_ARGS);
extern Datum chargt(PG_FUNCTION_ARGS);
extern Datum charge(PG_FUNCTION_ARGS);
extern Datum chartoi4(PG_FUNCTION_ARGS);
extern Datum i4tochar(PG_FUNCTION_ARGS);
extern Datum text_char(PG_FUNCTION_ARGS);
extern Datum char_text(PG_FUNCTION_ARGS);

/* domains.c */
extern Datum domain_in(PG_FUNCTION_ARGS);
extern Datum domain_recv(PG_FUNCTION_ARGS);
extern void domain_check(Datum value, bool isnull, Oid domainType, void** extra, MemoryContext mcxt);

/* encode.c */
extern Datum binary_encode(PG_FUNCTION_ARGS);
extern Datum binary_decode(PG_FUNCTION_ARGS);
extern unsigned hex_encode(const char* src, unsigned len, char* dst);
extern unsigned hex_decode(const char* src, unsigned len, char* dst);

/* encrypt_decrypt.c */
extern Datum aes_encrypt(PG_FUNCTION_ARGS);
extern Datum aes_decrypt(PG_FUNCTION_ARGS);

/* enum.c */
extern Datum enum_in(PG_FUNCTION_ARGS);
extern Datum enum_out(PG_FUNCTION_ARGS);
extern Datum enum_recv(PG_FUNCTION_ARGS);
extern Datum enum_send(PG_FUNCTION_ARGS);
extern Datum enum_lt(PG_FUNCTION_ARGS);
extern Datum enum_le(PG_FUNCTION_ARGS);
extern Datum enum_eq(PG_FUNCTION_ARGS);
extern Datum enum_ne(PG_FUNCTION_ARGS);
extern Datum enum_ge(PG_FUNCTION_ARGS);
extern Datum enum_gt(PG_FUNCTION_ARGS);
extern Datum enum_cmp(PG_FUNCTION_ARGS);
extern Datum enum_smaller(PG_FUNCTION_ARGS);
extern Datum enum_larger(PG_FUNCTION_ARGS);
extern Datum enum_first(PG_FUNCTION_ARGS);
extern Datum enum_last(PG_FUNCTION_ARGS);
extern Datum enum_range_bounds(PG_FUNCTION_ARGS);
extern Datum enum_range_all(PG_FUNCTION_ARGS);

/* set.c */
extern Datum set_in(PG_FUNCTION_ARGS);
extern Datum set_out(PG_FUNCTION_ARGS);
extern Datum set_recv(PG_FUNCTION_ARGS);
extern Datum set_send(PG_FUNCTION_ARGS);
extern Datum setint2(PG_FUNCTION_ARGS);
extern Datum setint4(PG_FUNCTION_ARGS);
extern Datum setint8(PG_FUNCTION_ARGS);
extern Datum settof(PG_FUNCTION_ARGS);
extern Datum settod(PG_FUNCTION_ARGS);
extern Datum settonumber(PG_FUNCTION_ARGS);
extern Datum settobpchar(PG_FUNCTION_ARGS);
extern Datum settovarchar(PG_FUNCTION_ARGS);
extern Datum settotext(PG_FUNCTION_ARGS);
extern Datum settonvarchar2(PG_FUNCTION_ARGS);
extern Datum i8toset(PG_FUNCTION_ARGS);
extern Datum i4toset(PG_FUNCTION_ARGS);
extern Datum i2toset(PG_FUNCTION_ARGS);
extern Datum ftoset(PG_FUNCTION_ARGS);
extern Datum dtoset(PG_FUNCTION_ARGS);
extern Datum numbertoset(PG_FUNCTION_ARGS);
extern Datum bpchartoset(PG_FUNCTION_ARGS);
extern Datum varchartoset(PG_FUNCTION_ARGS);
extern Datum texttoset(PG_FUNCTION_ARGS);
extern Datum nvarchar2toset(PG_FUNCTION_ARGS);
extern Datum btsetcmp(PG_FUNCTION_ARGS);
extern Datum seteq(PG_FUNCTION_ARGS);
extern Datum setne(PG_FUNCTION_ARGS);
extern Datum setlt(PG_FUNCTION_ARGS);
extern Datum setgt(PG_FUNCTION_ARGS);
extern Datum setle(PG_FUNCTION_ARGS);
extern Datum setge(PG_FUNCTION_ARGS);
extern Datum setint8eq(PG_FUNCTION_ARGS);
extern Datum setint8ne(PG_FUNCTION_ARGS);
extern Datum setint8lt(PG_FUNCTION_ARGS);
extern Datum setint8gt(PG_FUNCTION_ARGS);
extern Datum setint8le(PG_FUNCTION_ARGS);
extern Datum setint8ge(PG_FUNCTION_ARGS);
extern Datum setint2eq(PG_FUNCTION_ARGS);
extern Datum setint2ne(PG_FUNCTION_ARGS);
extern Datum setint2lt(PG_FUNCTION_ARGS);
extern Datum setint2gt(PG_FUNCTION_ARGS);
extern Datum setint2le(PG_FUNCTION_ARGS);
extern Datum setint2ge(PG_FUNCTION_ARGS);
extern Datum setint4eq(PG_FUNCTION_ARGS);
extern Datum setint4ne(PG_FUNCTION_ARGS);
extern Datum setint4lt(PG_FUNCTION_ARGS);
extern Datum setint4gt(PG_FUNCTION_ARGS);
extern Datum setint4le(PG_FUNCTION_ARGS);
extern Datum setint4ge(PG_FUNCTION_ARGS);
extern Datum int8seteq(PG_FUNCTION_ARGS);
extern Datum int8setne(PG_FUNCTION_ARGS);
extern Datum int8setlt(PG_FUNCTION_ARGS);
extern Datum int8setgt(PG_FUNCTION_ARGS);
extern Datum int8setle(PG_FUNCTION_ARGS);
extern Datum int8setge(PG_FUNCTION_ARGS);
extern Datum int2seteq(PG_FUNCTION_ARGS);
extern Datum int2setne(PG_FUNCTION_ARGS);
extern Datum int2setlt(PG_FUNCTION_ARGS);
extern Datum int2setgt(PG_FUNCTION_ARGS);
extern Datum int2setle(PG_FUNCTION_ARGS);
extern Datum int2setge(PG_FUNCTION_ARGS);
extern Datum int4seteq(PG_FUNCTION_ARGS);
extern Datum int4setne(PG_FUNCTION_ARGS);
extern Datum int4setlt(PG_FUNCTION_ARGS);
extern Datum int4setgt(PG_FUNCTION_ARGS);
extern Datum int4setle(PG_FUNCTION_ARGS);
extern Datum int4setge(PG_FUNCTION_ARGS);
extern Datum findinset(PG_FUNCTION_ARGS);
extern Datum btint8sortsupport(PG_FUNCTION_ARGS);

/* int.c */
extern Datum int2in(PG_FUNCTION_ARGS);
extern Datum int2out(PG_FUNCTION_ARGS);
extern Datum int2recv(PG_FUNCTION_ARGS);
extern Datum int2send(PG_FUNCTION_ARGS);
extern Datum int2vectorin(PG_FUNCTION_ARGS);
extern Datum int2vectorout(PG_FUNCTION_ARGS);
extern Datum int2vectorrecv(PG_FUNCTION_ARGS);
extern Datum int2vectorsend(PG_FUNCTION_ARGS);
extern Datum int2vectorin_extend(PG_FUNCTION_ARGS);
extern Datum int2vectorout_extend(PG_FUNCTION_ARGS);
extern Datum int2vectorrecv_extend(PG_FUNCTION_ARGS);
extern Datum int2vectorsend_extend(PG_FUNCTION_ARGS);
extern Datum int2vectoreq(PG_FUNCTION_ARGS);
extern Datum int4in(PG_FUNCTION_ARGS);
extern Datum int4out(PG_FUNCTION_ARGS);
extern Datum int4recv(PG_FUNCTION_ARGS);
extern Datum int4send(PG_FUNCTION_ARGS);
extern Datum i2toi4(PG_FUNCTION_ARGS);
extern Datum i4toi2(PG_FUNCTION_ARGS);
extern Datum int4_bool(PG_FUNCTION_ARGS);
extern Datum bool_int4(PG_FUNCTION_ARGS);
extern Datum int2_bool(PG_FUNCTION_ARGS);
extern Datum bool_int2(PG_FUNCTION_ARGS);
extern Datum int4eq(PG_FUNCTION_ARGS);
extern Datum int4ne(PG_FUNCTION_ARGS);
extern Datum int4lt(PG_FUNCTION_ARGS);
extern Datum int4le(PG_FUNCTION_ARGS);
extern Datum int4gt(PG_FUNCTION_ARGS);
extern Datum int4ge(PG_FUNCTION_ARGS);
extern Datum int2eq(PG_FUNCTION_ARGS);
extern Datum int2ne(PG_FUNCTION_ARGS);
extern Datum int2lt(PG_FUNCTION_ARGS);
extern Datum int2le(PG_FUNCTION_ARGS);
extern Datum int2gt(PG_FUNCTION_ARGS);
extern Datum int2ge(PG_FUNCTION_ARGS);
extern Datum int24eq(PG_FUNCTION_ARGS);
extern Datum int24ne(PG_FUNCTION_ARGS);
extern Datum int24lt(PG_FUNCTION_ARGS);
extern Datum int24le(PG_FUNCTION_ARGS);
extern Datum int24gt(PG_FUNCTION_ARGS);
extern Datum int24ge(PG_FUNCTION_ARGS);
extern Datum int42eq(PG_FUNCTION_ARGS);
extern Datum int42ne(PG_FUNCTION_ARGS);
extern Datum int42lt(PG_FUNCTION_ARGS);
extern Datum int42le(PG_FUNCTION_ARGS);
extern Datum int42gt(PG_FUNCTION_ARGS);
extern Datum int42ge(PG_FUNCTION_ARGS);
extern Datum int4um(PG_FUNCTION_ARGS);
extern Datum int4up(PG_FUNCTION_ARGS);
extern Datum int4pl(PG_FUNCTION_ARGS);
extern Datum int4mi(PG_FUNCTION_ARGS);
extern Datum int4mul(PG_FUNCTION_ARGS);
extern Datum int4div(PG_FUNCTION_ARGS);
extern Datum int4abs(PG_FUNCTION_ARGS);
extern Datum int4inc(PG_FUNCTION_ARGS);
extern Datum int2um(PG_FUNCTION_ARGS);
extern Datum int2up(PG_FUNCTION_ARGS);
extern Datum int2pl(PG_FUNCTION_ARGS);
extern Datum int2mi(PG_FUNCTION_ARGS);
extern Datum int2mul(PG_FUNCTION_ARGS);
extern Datum int2div(PG_FUNCTION_ARGS);
extern Datum int2abs(PG_FUNCTION_ARGS);
extern Datum int24pl(PG_FUNCTION_ARGS);
extern Datum int24mi(PG_FUNCTION_ARGS);
extern Datum int24mul(PG_FUNCTION_ARGS);
extern Datum int24div(PG_FUNCTION_ARGS);
extern Datum int42pl(PG_FUNCTION_ARGS);
extern Datum int42mi(PG_FUNCTION_ARGS);
extern Datum int42mul(PG_FUNCTION_ARGS);
extern Datum int42div(PG_FUNCTION_ARGS);
extern Datum int4mod(PG_FUNCTION_ARGS);
extern Datum int2mod(PG_FUNCTION_ARGS);
extern Datum int2larger(PG_FUNCTION_ARGS);
extern Datum int2smaller(PG_FUNCTION_ARGS);
extern Datum int4larger(PG_FUNCTION_ARGS);
extern Datum int4smaller(PG_FUNCTION_ARGS);
extern Datum int1in(PG_FUNCTION_ARGS);
extern Datum int1out(PG_FUNCTION_ARGS);
extern Datum int1recv(PG_FUNCTION_ARGS);
extern Datum int1send(PG_FUNCTION_ARGS);
extern Datum int1eq(PG_FUNCTION_ARGS);
extern Datum int1ne(PG_FUNCTION_ARGS);
extern Datum int1lt(PG_FUNCTION_ARGS);
extern Datum int1le(PG_FUNCTION_ARGS);
extern Datum int1gt(PG_FUNCTION_ARGS);
extern Datum int1ge(PG_FUNCTION_ARGS);
extern Datum int1cmp(PG_FUNCTION_ARGS);
extern Datum i1toi2(PG_FUNCTION_ARGS);
extern Datum i2toi1(PG_FUNCTION_ARGS);
extern Datum i1toi4(PG_FUNCTION_ARGS);
extern Datum i4toi1(PG_FUNCTION_ARGS);
extern Datum i1toi8(PG_FUNCTION_ARGS);
extern Datum i8toi1(PG_FUNCTION_ARGS);
extern Datum i1tof4(PG_FUNCTION_ARGS);
extern Datum f4toi1(PG_FUNCTION_ARGS);
extern Datum i1tof8(PG_FUNCTION_ARGS);
extern Datum f8toi1(PG_FUNCTION_ARGS);
extern Datum int1_bool(PG_FUNCTION_ARGS);
extern Datum bool_int1(PG_FUNCTION_ARGS);
extern Datum boolum(PG_FUNCTION_ARGS);
extern Datum int4_bpchar(PG_FUNCTION_ARGS);
extern Datum int1_bpchar(PG_FUNCTION_ARGS);
extern Datum int2_bpchar(PG_FUNCTION_ARGS);
extern Datum float4_bpchar(PG_FUNCTION_ARGS);
extern Datum float8_bpchar(PG_FUNCTION_ARGS);
extern Datum numeric_bpchar(PG_FUNCTION_ARGS);
extern Datum int1and(PG_FUNCTION_ARGS);
extern Datum int1or(PG_FUNCTION_ARGS);
extern Datum int1xor(PG_FUNCTION_ARGS);
extern Datum int1not(PG_FUNCTION_ARGS);
extern Datum int1shl(PG_FUNCTION_ARGS);
extern Datum int1shr(PG_FUNCTION_ARGS);
extern Datum int1um(PG_FUNCTION_ARGS);
extern Datum int1up(PG_FUNCTION_ARGS);
extern Datum int1pl(PG_FUNCTION_ARGS);
extern Datum int1mi(PG_FUNCTION_ARGS);
extern Datum int1mul(PG_FUNCTION_ARGS);
extern Datum int1div(PG_FUNCTION_ARGS);
extern Datum int1abs(PG_FUNCTION_ARGS);
extern Datum int1mod(PG_FUNCTION_ARGS);
extern Datum int1larger(PG_FUNCTION_ARGS);
extern Datum int1smaller(PG_FUNCTION_ARGS);
extern Datum int1inc(PG_FUNCTION_ARGS);

extern Datum int4and(PG_FUNCTION_ARGS);
extern Datum int4or(PG_FUNCTION_ARGS);
extern Datum int4xor(PG_FUNCTION_ARGS);
extern Datum int4not(PG_FUNCTION_ARGS);
extern Datum int4shl(PG_FUNCTION_ARGS);
extern Datum int4shr(PG_FUNCTION_ARGS);
extern Datum int2and(PG_FUNCTION_ARGS);
extern Datum int2or(PG_FUNCTION_ARGS);
extern Datum int2xor(PG_FUNCTION_ARGS);
extern Datum int2not(PG_FUNCTION_ARGS);
extern Datum int2shl(PG_FUNCTION_ARGS);
extern Datum int2shr(PG_FUNCTION_ARGS);
extern Datum generate_series_int4(PG_FUNCTION_ARGS);
extern Datum generate_series_step_int4(PG_FUNCTION_ARGS);
extern int2vector* buildint2vector(const int2* int2s, int n);
extern int2vector* int2vectorCopy(int2vector* from);
/* encoding.cpp */
extern Datum encode_plan_node(PG_FUNCTION_ARGS);
/* ml_model.cpp */
extern Datum model_train_opt(PG_FUNCTION_ARGS);
extern Datum track_model_train_opt(PG_FUNCTION_ARGS);

/* lsyscache.cpp */
extern Datum node_oid_name(PG_FUNCTION_ARGS);
/* tablespace.cpp */
extern Datum tablespace_oid_name(PG_FUNCTION_ARGS);
/* namespace.cpp */
extern Datum get_schema_oid(PG_FUNCTION_ARGS);
/* name.c */
extern Datum namein(PG_FUNCTION_ARGS);
extern Datum nameout(PG_FUNCTION_ARGS);
extern Datum namerecv(PG_FUNCTION_ARGS);
extern Datum namesend(PG_FUNCTION_ARGS);
extern Datum nameeq(PG_FUNCTION_ARGS);
extern Datum nameeq_withhead(PG_FUNCTION_ARGS);
extern Datum namene(PG_FUNCTION_ARGS);
extern Datum namelt(PG_FUNCTION_ARGS);
extern Datum namele(PG_FUNCTION_ARGS);
extern Datum namegt(PG_FUNCTION_ARGS);
extern Datum namege(PG_FUNCTION_ARGS);
extern int namestrcpy(Name name, const char* str);
extern int namestrcmp(Name name, const char* str);
extern int namestrcasecmp(Name name, const char* str);
extern Datum current_user(PG_FUNCTION_ARGS);
extern Datum session_user(PG_FUNCTION_ARGS);
extern Datum current_schema(PG_FUNCTION_ARGS);
extern Datum current_schemas(PG_FUNCTION_ARGS);
extern Datum pseudo_current_user(PG_FUNCTION_ARGS);
extern uint64 pg_strtouint64(const char* str, char** endptr, int base);

/* numutils.c */
extern int32 pg_atoi(char* s, int size, int c, bool can_ignore);
extern int16 pg_strtoint16(const char* s, bool can_ignore = false);
extern int32 pg_strtoint32(const char* s, bool can_ignore);
extern void pg_itoa(int16 i, char* a);
extern void pg_ltoa(int32 l, char* a);
extern char* pg_ltoa2(int32 l, int* len);
extern void pg_ctoa(uint8 i, char* a);
extern void pg_lltoa(int64 ll, char* a);
extern char* pg_lltoa2(int64 ll, int* len);
extern void pg_i128toa(int128 value, char* a, int length);

/*
 *		Per-opclass comparison functions for new btrees.  These are
 *		stored in pg_amproc; most are defined in access/nbtree/nbtcompare.c
 */
extern Datum btboolcmp(PG_FUNCTION_ARGS);
extern Datum btint2cmp(PG_FUNCTION_ARGS);
extern Datum btint4cmp(PG_FUNCTION_ARGS);
extern Datum btint8cmp(PG_FUNCTION_ARGS);
extern Datum btfloat4cmp(PG_FUNCTION_ARGS);
extern Datum btfloat8cmp(PG_FUNCTION_ARGS);
extern Datum btint48cmp(PG_FUNCTION_ARGS);
extern Datum btint84cmp(PG_FUNCTION_ARGS);
extern Datum btint24cmp(PG_FUNCTION_ARGS);
extern Datum btint42cmp(PG_FUNCTION_ARGS);
extern Datum btint28cmp(PG_FUNCTION_ARGS);
extern Datum btint82cmp(PG_FUNCTION_ARGS);
extern Datum btfloat48cmp(PG_FUNCTION_ARGS);
extern Datum btfloat84cmp(PG_FUNCTION_ARGS);
extern Datum btoidcmp(PG_FUNCTION_ARGS);
extern Datum btoidvectorcmp(PG_FUNCTION_ARGS);
extern Datum btabstimecmp(PG_FUNCTION_ARGS);
extern Datum btreltimecmp(PG_FUNCTION_ARGS);
extern Datum bttintervalcmp(PG_FUNCTION_ARGS);
extern Datum btcharcmp(PG_FUNCTION_ARGS);
extern Datum btnamecmp(PG_FUNCTION_ARGS);
extern Datum bttextcmp(PG_FUNCTION_ARGS);
extern Datum bpchar_sortsupport(PG_FUNCTION_ARGS);
extern Datum bttextsortsupport(PG_FUNCTION_ARGS);
/*
 *		Per-opclass sort support functions for new btrees.	Like the
 *		functions above, these are stored in pg_amproc; most are defined in
 *		access/nbtree/nbtcompare.c
 */
extern Datum btint2sortsupport(PG_FUNCTION_ARGS);
extern Datum btint4sortsupport(PG_FUNCTION_ARGS);
extern Datum btint8sortsupport(PG_FUNCTION_ARGS);
extern Datum btfloat4sortsupport(PG_FUNCTION_ARGS);
extern Datum btfloat8sortsupport(PG_FUNCTION_ARGS);
extern Datum btoidsortsupport(PG_FUNCTION_ARGS);
extern Datum btnamesortsupport(PG_FUNCTION_ARGS);

extern double get_float8_infinity(void);
extern float get_float4_infinity(void);
extern double get_float8_nan(void);
extern float get_float4_nan(void);
extern int is_infinite(double val);
extern double float8in_internal(char* str, char** s, bool* hasError);

extern Datum float4in(PG_FUNCTION_ARGS);
extern Datum float4out(PG_FUNCTION_ARGS);
extern Datum float4recv(PG_FUNCTION_ARGS);
extern Datum float4send(PG_FUNCTION_ARGS);
extern Datum float8in(PG_FUNCTION_ARGS);
extern Datum float8out(PG_FUNCTION_ARGS);
extern Datum float8recv(PG_FUNCTION_ARGS);
extern Datum float8send(PG_FUNCTION_ARGS);
extern Datum float4abs(PG_FUNCTION_ARGS);
extern Datum float4um(PG_FUNCTION_ARGS);
extern Datum float4up(PG_FUNCTION_ARGS);
extern Datum float4larger(PG_FUNCTION_ARGS);
extern Datum float4smaller(PG_FUNCTION_ARGS);
extern Datum float8abs(PG_FUNCTION_ARGS);
extern Datum float8um(PG_FUNCTION_ARGS);
extern Datum float8up(PG_FUNCTION_ARGS);
extern Datum float8larger(PG_FUNCTION_ARGS);
extern Datum float8smaller(PG_FUNCTION_ARGS);
extern Datum float4pl(PG_FUNCTION_ARGS);
extern Datum float4mi(PG_FUNCTION_ARGS);
extern Datum float4mul(PG_FUNCTION_ARGS);
extern Datum float4div(PG_FUNCTION_ARGS);
extern Datum float8pl(PG_FUNCTION_ARGS);
extern Datum float8mi(PG_FUNCTION_ARGS);
extern Datum float8mul(PG_FUNCTION_ARGS);
extern Datum float8div(PG_FUNCTION_ARGS);
extern Datum float4eq(PG_FUNCTION_ARGS);
extern Datum float4ne(PG_FUNCTION_ARGS);
extern Datum float4lt(PG_FUNCTION_ARGS);
extern Datum float4le(PG_FUNCTION_ARGS);
extern Datum float4gt(PG_FUNCTION_ARGS);
extern Datum float4ge(PG_FUNCTION_ARGS);
extern Datum float8eq(PG_FUNCTION_ARGS);
extern Datum float8ne(PG_FUNCTION_ARGS);
extern Datum float8lt(PG_FUNCTION_ARGS);
extern Datum float8le(PG_FUNCTION_ARGS);
extern Datum float8gt(PG_FUNCTION_ARGS);
extern Datum float8ge(PG_FUNCTION_ARGS);
extern Datum ftod(PG_FUNCTION_ARGS);
extern Datum i4tod(PG_FUNCTION_ARGS);
extern Datum i2tod(PG_FUNCTION_ARGS);
extern Datum dtof(PG_FUNCTION_ARGS);
extern Datum dtoi4(PG_FUNCTION_ARGS);
extern Datum dtoi2(PG_FUNCTION_ARGS);
extern Datum i4tof(PG_FUNCTION_ARGS);
extern Datum i2tof(PG_FUNCTION_ARGS);
extern Datum ftoi4(PG_FUNCTION_ARGS);
extern Datum ftoi2(PG_FUNCTION_ARGS);
extern Datum dround(PG_FUNCTION_ARGS);
extern Datum dceil(PG_FUNCTION_ARGS);
extern Datum dfloor(PG_FUNCTION_ARGS);
extern Datum dsign(PG_FUNCTION_ARGS);
extern Datum dtrunc(PG_FUNCTION_ARGS);
extern Datum dsqrt(PG_FUNCTION_ARGS);
extern Datum dcbrt(PG_FUNCTION_ARGS);
extern Datum dpow(PG_FUNCTION_ARGS);
extern Datum dexp(PG_FUNCTION_ARGS);
extern Datum dlog1(PG_FUNCTION_ARGS);
extern Datum dlog10(PG_FUNCTION_ARGS);
extern Datum dacos(PG_FUNCTION_ARGS);
extern Datum dasin(PG_FUNCTION_ARGS);
extern Datum datan(PG_FUNCTION_ARGS);
extern Datum datan2(PG_FUNCTION_ARGS);
extern Datum dcos(PG_FUNCTION_ARGS);
extern Datum dcot(PG_FUNCTION_ARGS);
extern Datum dsin(PG_FUNCTION_ARGS);
extern Datum dtan(PG_FUNCTION_ARGS);
extern Datum degrees(PG_FUNCTION_ARGS);
extern Datum dpi(PG_FUNCTION_ARGS);
extern Datum radians(PG_FUNCTION_ARGS);
extern Datum drandom(PG_FUNCTION_ARGS);
extern Datum setseed(PG_FUNCTION_ARGS);
extern Datum float8_accum(PG_FUNCTION_ARGS);
extern Datum float4_accum(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum float8_collect(PG_FUNCTION_ARGS);
#endif
extern Datum float8_avg(PG_FUNCTION_ARGS);
extern Datum float8_var_pop(PG_FUNCTION_ARGS);
extern Datum float8_var_samp(PG_FUNCTION_ARGS);
extern Datum float8_stddev_pop(PG_FUNCTION_ARGS);
extern Datum float8_stddev_samp(PG_FUNCTION_ARGS);
extern Datum float8_regr_accum(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum float8_regr_collect(PG_FUNCTION_ARGS);
#endif
extern Datum float8_regr_sxx(PG_FUNCTION_ARGS);
extern Datum float8_regr_syy(PG_FUNCTION_ARGS);
extern Datum float8_regr_sxy(PG_FUNCTION_ARGS);
extern Datum float8_regr_avgx(PG_FUNCTION_ARGS);
extern Datum float8_regr_avgy(PG_FUNCTION_ARGS);
extern Datum float8_covar_pop(PG_FUNCTION_ARGS);
extern Datum float8_covar_samp(PG_FUNCTION_ARGS);
extern Datum float8_corr(PG_FUNCTION_ARGS);
extern Datum float8_regr_r2(PG_FUNCTION_ARGS);
extern Datum float8_regr_slope(PG_FUNCTION_ARGS);
extern Datum float8_regr_intercept(PG_FUNCTION_ARGS);
extern Datum float48pl(PG_FUNCTION_ARGS);
extern Datum float48mi(PG_FUNCTION_ARGS);
extern Datum float48mul(PG_FUNCTION_ARGS);
extern Datum float48div(PG_FUNCTION_ARGS);
extern Datum float84pl(PG_FUNCTION_ARGS);
extern Datum float84mi(PG_FUNCTION_ARGS);
extern Datum float84mul(PG_FUNCTION_ARGS);
extern Datum float84div(PG_FUNCTION_ARGS);
extern Datum float48eq(PG_FUNCTION_ARGS);
extern Datum float48ne(PG_FUNCTION_ARGS);
extern Datum float48lt(PG_FUNCTION_ARGS);
extern Datum float48le(PG_FUNCTION_ARGS);
extern Datum float48gt(PG_FUNCTION_ARGS);
extern Datum float48ge(PG_FUNCTION_ARGS);
extern Datum float84eq(PG_FUNCTION_ARGS);
extern Datum float84ne(PG_FUNCTION_ARGS);
extern Datum float84lt(PG_FUNCTION_ARGS);
extern Datum float84le(PG_FUNCTION_ARGS);
extern Datum float84gt(PG_FUNCTION_ARGS);
extern Datum float84ge(PG_FUNCTION_ARGS);
extern Datum width_bucket_float8(PG_FUNCTION_ARGS);
Datum float8_multiply_text(PG_FUNCTION_ARGS);
Datum text_multiply_float8(PG_FUNCTION_ARGS);

/* dbsize.c */
extern Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_tablespace_size_name(PG_FUNCTION_ARGS);
extern Datum pg_database_size_oid(PG_FUNCTION_ARGS);
extern Datum pg_database_size_name(PG_FUNCTION_ARGS);
extern Datum get_db_source_datasize(PG_FUNCTION_ARGS);
extern Datum pg_switch_relfilenode_name(PG_FUNCTION_ARGS);
extern Datum pg_relation_size(PG_FUNCTION_ARGS);
extern Datum pg_total_relation_size(PG_FUNCTION_ARGS);
extern Datum pg_size_pretty(PG_FUNCTION_ARGS);
extern Datum pg_size_pretty_numeric(PG_FUNCTION_ARGS);
extern Datum pg_table_size(PG_FUNCTION_ARGS);
extern Datum pg_partition_size(PG_FUNCTION_ARGS);
extern Datum pg_partition_indexes_size(PG_FUNCTION_ARGS);
extern Datum pg_indexes_size(PG_FUNCTION_ARGS);
extern Datum pg_relation_filenode(PG_FUNCTION_ARGS);
extern Datum pg_filenode_relation(PG_FUNCTION_ARGS);
extern Datum pg_relation_filepath(PG_FUNCTION_ARGS);
extern Datum pg_relation_is_scannable(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum pg_relation_with_compression(PG_FUNCTION_ARGS);
extern Datum pg_relation_compression_ratio(PG_FUNCTION_ARGS);
#endif
extern Datum pg_partition_filenode(PG_FUNCTION_ARGS);
extern Datum pg_partition_filepath(PG_FUNCTION_ARGS);
/* regioninfo.cpp */
extern Datum pg_clean_region_info(PG_FUNCTION_ARGS);

/* genfile.c */
extern bytea* read_binary_file(
    const char* filename, int64 seek_offset, int64 bytes_to_read, bool missing_ok, bool need_check = false);
extern Datum pg_stat_file(PG_FUNCTION_ARGS);
extern Datum pg_read_file(PG_FUNCTION_ARGS);
extern Datum pg_read_file_all(PG_FUNCTION_ARGS);
extern Datum pg_read_binary_file(PG_FUNCTION_ARGS);
extern Datum pg_read_binary_file_all(PG_FUNCTION_ARGS);
extern Datum pg_read_binary_file_blocks(PG_FUNCTION_ARGS);
extern Datum pg_ls_dir(PG_FUNCTION_ARGS);
extern Datum pg_stat_file_recursive(PG_FUNCTION_ARGS);

/* misc.c */
extern Datum current_database(PG_FUNCTION_ARGS);
extern Datum current_query(PG_FUNCTION_ARGS);
extern Datum pg_cancel_backend(PG_FUNCTION_ARGS);
extern Datum pg_cancel_session(PG_FUNCTION_ARGS);
extern Datum pg_cancel_invalid_query(PG_FUNCTION_ARGS);
extern Datum pg_terminate_backend(PG_FUNCTION_ARGS);
extern Datum pg_terminate_session(PG_FUNCTION_ARGS);
extern Datum pg_reload_conf(PG_FUNCTION_ARGS);
extern Datum pg_tablespace_databases(PG_FUNCTION_ARGS);
extern Datum pg_tablespace_location(PG_FUNCTION_ARGS);
extern Datum pg_rotate_logfile(PG_FUNCTION_ARGS);
extern Datum pg_sleep(PG_FUNCTION_ARGS);
extern Datum pg_get_keywords(PG_FUNCTION_ARGS);
extern Datum pg_typeof(PG_FUNCTION_ARGS);
extern Datum pg_collation_for(PG_FUNCTION_ARGS);
extern Datum pg_sync_cstore_delta(PG_FUNCTION_ARGS);
extern Datum pg_sync_all_cstore_delta(PG_FUNCTION_ARGS);
extern Datum pg_test_err_contain_err(PG_FUNCTION_ARGS);
extern Datum pg_relation_is_updatable(PG_FUNCTION_ARGS);
extern Datum pg_column_is_updatable(PG_FUNCTION_ARGS);

/* oid.c */
extern Datum oidin(PG_FUNCTION_ARGS);
extern Datum oidout(PG_FUNCTION_ARGS);
extern Datum oidrecv(PG_FUNCTION_ARGS);
extern Datum oidsend(PG_FUNCTION_ARGS);
extern Datum oideq(PG_FUNCTION_ARGS);
extern Datum oidne(PG_FUNCTION_ARGS);
extern Datum oidlt(PG_FUNCTION_ARGS);
extern Datum oidle(PG_FUNCTION_ARGS);
extern Datum oidge(PG_FUNCTION_ARGS);
extern Datum oidgt(PG_FUNCTION_ARGS);
extern Datum oidlarger(PG_FUNCTION_ARGS);
extern Datum oidsmaller(PG_FUNCTION_ARGS);
extern Datum oidvectorin(PG_FUNCTION_ARGS);
extern Datum oidvectorout(PG_FUNCTION_ARGS);
extern Datum oidvectorrecv(PG_FUNCTION_ARGS);
extern Datum oidvectorsend(PG_FUNCTION_ARGS);
extern Datum oidvectoreq(PG_FUNCTION_ARGS);
extern Datum oidvectorne(PG_FUNCTION_ARGS);
extern Datum oidvectorlt(PG_FUNCTION_ARGS);
extern Datum oidvectorle(PG_FUNCTION_ARGS);
extern Datum oidvectorge(PG_FUNCTION_ARGS);
extern Datum oidvectorgt(PG_FUNCTION_ARGS);
extern oidvector* buildoidvector(const Oid* oids, int n);
extern Oid oidparse(Node* node);
extern int oid_cmp(const void *p1, const void *p2);

/* pseudotypes.c */
extern Datum cstring_in(PG_FUNCTION_ARGS);
extern Datum cstring_out(PG_FUNCTION_ARGS);
extern Datum cstring_recv(PG_FUNCTION_ARGS);
extern Datum cstring_send(PG_FUNCTION_ARGS);
extern Datum any_in(PG_FUNCTION_ARGS);
extern Datum any_out(PG_FUNCTION_ARGS);
extern Datum anyarray_in(PG_FUNCTION_ARGS);
extern Datum anyarray_out(PG_FUNCTION_ARGS);
extern Datum anyarray_recv(PG_FUNCTION_ARGS);
extern Datum anyarray_send(PG_FUNCTION_ARGS);
extern Datum anynonarray_in(PG_FUNCTION_ARGS);
extern Datum anynonarray_out(PG_FUNCTION_ARGS);
extern Datum anyenum_in(PG_FUNCTION_ARGS);
extern Datum anyenum_out(PG_FUNCTION_ARGS);
extern Datum anyrange_in(PG_FUNCTION_ARGS);
extern Datum anyrange_out(PG_FUNCTION_ARGS);
extern Datum void_in(PG_FUNCTION_ARGS);
extern Datum void_out(PG_FUNCTION_ARGS);
extern Datum void_recv(PG_FUNCTION_ARGS);
extern Datum void_send(PG_FUNCTION_ARGS);
extern Datum anyset_in(PG_FUNCTION_ARGS);
extern Datum anyset_out(PG_FUNCTION_ARGS);

#ifdef PGXC
extern Datum pgxc_node_str(PG_FUNCTION_ARGS);
extern Datum pgxc_lock_for_backup(PG_FUNCTION_ARGS);
extern Datum pgxc_lock_for_sp_database(PG_FUNCTION_ARGS);
extern Datum pgxc_unlock_for_sp_database(PG_FUNCTION_ARGS);
extern Datum pgxc_lock_for_transfer(PG_FUNCTION_ARGS);
extern Datum pgxc_unlock_for_transfer(PG_FUNCTION_ARGS);
#endif
extern Datum trigger_in(PG_FUNCTION_ARGS);
extern Datum trigger_out(PG_FUNCTION_ARGS);
extern Datum event_trigger_in(PG_FUNCTION_ARGS);
extern Datum event_trigger_out(PG_FUNCTION_ARGS);

extern Datum language_handler_in(PG_FUNCTION_ARGS);
extern Datum language_handler_out(PG_FUNCTION_ARGS);
extern Datum fdw_handler_in(PG_FUNCTION_ARGS);
extern Datum fdw_handler_out(PG_FUNCTION_ARGS);
extern Datum internal_in(PG_FUNCTION_ARGS);
extern Datum internal_out(PG_FUNCTION_ARGS);
extern Datum opaque_in(PG_FUNCTION_ARGS);
extern Datum opaque_out(PG_FUNCTION_ARGS);
extern Datum anyelement_in(PG_FUNCTION_ARGS);
extern Datum anyelement_out(PG_FUNCTION_ARGS);
extern Datum shell_in(PG_FUNCTION_ARGS);
extern Datum shell_out(PG_FUNCTION_ARGS);
extern Datum pg_node_tree_in(PG_FUNCTION_ARGS);
extern Datum pg_node_tree_out(PG_FUNCTION_ARGS);
extern Datum pg_node_tree_recv(PG_FUNCTION_ARGS);
extern Datum pg_node_tree_send(PG_FUNCTION_ARGS);

/* regexp.c */
extern Datum textregexsubstr_enforce_a(PG_FUNCTION_ARGS);
extern Datum nameregexeq(PG_FUNCTION_ARGS);
extern Datum nameregexne(PG_FUNCTION_ARGS);
extern Datum textregexeq(PG_FUNCTION_ARGS);
extern Datum textregexne(PG_FUNCTION_ARGS);
extern Datum nameicregexeq(PG_FUNCTION_ARGS);
extern Datum nameicregexne(PG_FUNCTION_ARGS);
extern Datum texticregexeq(PG_FUNCTION_ARGS);
extern Datum texticregexne(PG_FUNCTION_ARGS);
extern Datum textregexsubstr(PG_FUNCTION_ARGS);
extern Datum textregexreplace_noopt(PG_FUNCTION_ARGS);
extern Datum textregexreplace(PG_FUNCTION_ARGS);
extern Datum regexp_replace_noopt(PG_FUNCTION_ARGS);
extern Datum regexp_replace_with_position(PG_FUNCTION_ARGS);
extern Datum regexp_replace_with_occur(PG_FUNCTION_ARGS);
extern Datum regexp_replace_with_opt(PG_FUNCTION_ARGS);
extern Datum similar_escape(PG_FUNCTION_ARGS);
extern Datum regexp_count_noopt(PG_FUNCTION_ARGS);
extern Datum regexp_count_position(PG_FUNCTION_ARGS);
extern Datum regexp_count_matchopt(PG_FUNCTION_ARGS);
extern Datum regexp_instr_noopt(PG_FUNCTION_ARGS);
extern Datum regexp_instr_position(PG_FUNCTION_ARGS);
extern Datum regexp_instr_occurren(PG_FUNCTION_ARGS);
extern Datum regexp_instr_returnopt(PG_FUNCTION_ARGS);
extern Datum regexp_instr_matchopt(PG_FUNCTION_ARGS);
extern Datum regexp_substr_with_position(PG_FUNCTION_ARGS);
extern Datum regexp_substr_with_occurrence(PG_FUNCTION_ARGS);
extern Datum regexp_substr_with_opt(PG_FUNCTION_ARGS);
extern Datum regexp_matches(PG_FUNCTION_ARGS);
extern Datum regexp_matches_no_flags(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_table(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_table_no_flags(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_array(PG_FUNCTION_ARGS);
extern Datum regexp_match_to_array(PG_FUNCTION_ARGS);
extern Datum regexp_split_to_array_no_flags(PG_FUNCTION_ARGS);
extern char* regexp_fixed_prefix(text* text_re, bool case_insensitive, Oid collation, bool* exact);

/* regproc.c */
extern Datum regprocin(PG_FUNCTION_ARGS);
extern Datum regprocout(PG_FUNCTION_ARGS);
extern Datum regprocrecv(PG_FUNCTION_ARGS);
extern Datum regprocsend(PG_FUNCTION_ARGS);
extern Datum regprocedurein(PG_FUNCTION_ARGS);
extern Datum regprocedureout(PG_FUNCTION_ARGS);
extern Datum regprocedurerecv(PG_FUNCTION_ARGS);
extern Datum regproceduresend(PG_FUNCTION_ARGS);
extern Datum regoperin(PG_FUNCTION_ARGS);
extern Datum regoperout(PG_FUNCTION_ARGS);
extern Datum regoperrecv(PG_FUNCTION_ARGS);
extern Datum regopersend(PG_FUNCTION_ARGS);
extern Datum regoperatorin(PG_FUNCTION_ARGS);
extern Datum regoperatorout(PG_FUNCTION_ARGS);
extern Datum regoperatorrecv(PG_FUNCTION_ARGS);
extern Datum regoperatorsend(PG_FUNCTION_ARGS);
extern Datum regclassin(PG_FUNCTION_ARGS);
extern Datum regclassout(PG_FUNCTION_ARGS);
extern Datum regclassrecv(PG_FUNCTION_ARGS);
extern Datum regclasssend(PG_FUNCTION_ARGS);
extern Datum regtypein(PG_FUNCTION_ARGS);
extern Datum regtypeout(PG_FUNCTION_ARGS);
extern Datum regtyperecv(PG_FUNCTION_ARGS);
extern Datum regtypesend(PG_FUNCTION_ARGS);
extern Datum regconfigin(PG_FUNCTION_ARGS);
extern Datum regconfigout(PG_FUNCTION_ARGS);
extern Datum regconfigrecv(PG_FUNCTION_ARGS);
extern Datum regconfigsend(PG_FUNCTION_ARGS);
extern Datum regdictionaryin(PG_FUNCTION_ARGS);
extern Datum regdictionaryout(PG_FUNCTION_ARGS);
extern Datum regdictionaryrecv(PG_FUNCTION_ARGS);
extern Datum regdictionarysend(PG_FUNCTION_ARGS);
extern Datum text_regclass(PG_FUNCTION_ARGS);
extern List* stringToQualifiedNameList(const char* string);
extern char* format_procedure(Oid procedure_oid);
extern char* format_operator(Oid operator_oid);
extern char *format_procedure_qualified(Oid procedure_oid);
extern char *format_operator_qualified(Oid operator_oid);
extern void format_procedure_parts(Oid procedure_oid, List **objnames, List **objargs);

/* rowtypes.c */
extern Datum record_in(PG_FUNCTION_ARGS);
extern Datum record_out(PG_FUNCTION_ARGS);
extern Datum record_recv(PG_FUNCTION_ARGS);
extern Datum record_send(PG_FUNCTION_ARGS);
extern Datum record_eq(PG_FUNCTION_ARGS);
extern Datum record_ne(PG_FUNCTION_ARGS);
extern Datum record_lt(PG_FUNCTION_ARGS);
extern Datum record_gt(PG_FUNCTION_ARGS);
extern Datum record_le(PG_FUNCTION_ARGS);
extern Datum record_ge(PG_FUNCTION_ARGS);
extern Datum btrecordcmp(PG_FUNCTION_ARGS);

/* ruleutils.c */
extern Datum pg_get_ruledef(PG_FUNCTION_ARGS);
extern Datum pg_get_ruledef_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_wrap(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_name(PG_FUNCTION_ARGS);
extern Datum pg_get_viewdef_name_ext(PG_FUNCTION_ARGS);
extern char* pg_get_viewdef_string(Oid viewid);
extern Datum pg_get_indexdef(PG_FUNCTION_ARGS);
extern Datum pg_get_indexdef_for_dump(PG_FUNCTION_ARGS);
extern Datum pg_get_indexdef_ext(PG_FUNCTION_ARGS);
extern char* pg_get_indexdef_string(Oid indexrelid);
extern char* pg_get_indexdef_columns(Oid indexrelid, bool pretty);
extern Datum pg_get_triggerdef(PG_FUNCTION_ARGS);
extern Datum pg_get_triggerdef_ext(PG_FUNCTION_ARGS);
extern char* pg_get_triggerdef_string(Oid trigid);
extern Datum pg_get_constraintdef(PG_FUNCTION_ARGS);
extern Datum pg_get_constraintdef_ext(PG_FUNCTION_ARGS);
extern char* pg_get_constraintdef_string(Oid constraintId);
extern Datum pg_get_expr(PG_FUNCTION_ARGS);
extern Datum pg_get_expr_ext(PG_FUNCTION_ARGS);
extern Datum pg_get_userbyid(PG_FUNCTION_ARGS);
extern Datum pg_get_serial_sequence(PG_FUNCTION_ARGS);
extern Datum pg_get_functiondef(PG_FUNCTION_ARGS);
extern Datum pg_get_function_arguments(PG_FUNCTION_ARGS);
extern Datum pg_get_function_identity_arguments(PG_FUNCTION_ARGS);
extern Datum pg_get_function_result(PG_FUNCTION_ARGS);
extern char* deparse_expression(
    Node* expr, List* dpcontext, bool forceprefix, bool showimplicit, bool no_alias = false);
extern void get_query_def(Query* query, StringInfo buf, List* parentnamespace, TupleDesc resultDesc, int prettyFlags,
    int wrapColumn, int startIndent,
#ifdef PGXC
    bool finalise_aggregates, bool sortgroup_colno, void* parserArg = NULL,
#endif /* PGXC */
    bool qrw_phase = false, bool viewdef = false, bool is_fqs = false);
extern char* deparse_create_sequence(Node* stmt, bool owned_by_none = false);
extern char* deparse_alter_sequence(Node* stmt, bool owned_by_none = false);

#ifdef PGXC
extern void get_hint_string(HintState* hstate, StringInfo buf);
extern void deparse_query(Query* query, StringInfo buf, List* parentnamespace, bool finalise_aggs, bool sortgroup_colno,
    void* parserArg = NULL, bool qrw_phase = false, bool is_fqs = false);
extern void deparse_targetlist(Query* query, List* targetList, StringInfo buf);
#endif
extern List* deparse_context_for(const char* aliasname, Oid relid);
extern List* deparse_context_for_planstate(Node* planstate, List* ancestors, List* rtable);
#ifdef PGXC
extern List* deparse_context_for_plan(Node* plan, List* ancestors, List* rtable);
#endif
extern const char* quote_identifier(const char* ident);
extern char* quote_qualified_identifier(const char* qualifier, const char* ident1, const char* ident2 = NULL);
extern char* generate_collation_name(Oid collid);
extern void get_utility_stmt_def(AlterTableStmt* stmt, StringInfo buf);

/* tid.c */
extern Datum tidin(PG_FUNCTION_ARGS);
extern Datum tidout(PG_FUNCTION_ARGS);
extern Datum tidrecv(PG_FUNCTION_ARGS);
extern Datum tidsend(PG_FUNCTION_ARGS);
extern Datum tideq(PG_FUNCTION_ARGS);
extern Datum tidne(PG_FUNCTION_ARGS);
extern Datum tidlt(PG_FUNCTION_ARGS);
extern Datum tidle(PG_FUNCTION_ARGS);
extern Datum tidgt(PG_FUNCTION_ARGS);
extern Datum tidge(PG_FUNCTION_ARGS);
extern Datum bttidcmp(PG_FUNCTION_ARGS);
extern Datum tidlarger(PG_FUNCTION_ARGS);
extern Datum tidsmaller(PG_FUNCTION_ARGS);
extern Datum bigint_tid(PG_FUNCTION_ARGS);
extern Datum cstore_tid_out(PG_FUNCTION_ARGS);
extern Datum currtid_byreloid(PG_FUNCTION_ARGS);
extern Datum currtid_byrelname(PG_FUNCTION_ARGS);

/* varchar.c */
extern Datum bpcharlenb(PG_FUNCTION_ARGS);
extern Datum bpcharin(PG_FUNCTION_ARGS);
extern Datum input_bpcharin(char* str, Oid typioparam, int32 atttypmod);
extern Datum bpcharout(PG_FUNCTION_ARGS);
extern Datum bpcharrecv(PG_FUNCTION_ARGS);
extern Datum bpcharsend(PG_FUNCTION_ARGS);
extern Datum bpchartypmodin(PG_FUNCTION_ARGS);
extern Datum bpchartypmodout(PG_FUNCTION_ARGS);
extern Datum bpchar(PG_FUNCTION_ARGS);
extern Datum opfusion_bpchar(Datum arg1, Datum arg2, Datum arg3);
extern Datum char_bpchar(PG_FUNCTION_ARGS);
extern Datum name_bpchar(PG_FUNCTION_ARGS);
extern Datum bpchar_name(PG_FUNCTION_ARGS);
extern Datum bpchareq(PG_FUNCTION_ARGS);
extern Datum bpcharne(PG_FUNCTION_ARGS);
extern Datum bpcharlt(PG_FUNCTION_ARGS);
extern Datum bpcharle(PG_FUNCTION_ARGS);
extern Datum bpchargt(PG_FUNCTION_ARGS);
extern Datum bpcharge(PG_FUNCTION_ARGS);
extern Datum bpcharcmp(PG_FUNCTION_ARGS);
extern Datum bpchar_larger(PG_FUNCTION_ARGS);
extern Datum bpchar_smaller(PG_FUNCTION_ARGS);
extern int bpchartruelen(const char* s, int len);
extern Datum bpcharlen(PG_FUNCTION_ARGS);
extern int bcTruelen(BpChar* arg);
extern Datum bpcharoctetlen(PG_FUNCTION_ARGS);
extern Datum hashbpchar(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_lt(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_le(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_gt(PG_FUNCTION_ARGS);
extern Datum bpchar_pattern_ge(PG_FUNCTION_ARGS);
extern Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS);

extern Datum varcharin(PG_FUNCTION_ARGS);
extern Datum input_varcharin(char* str, Oid typioparam, int32 atttypmod);
extern Datum varcharout(PG_FUNCTION_ARGS);
extern Datum varcharrecv(PG_FUNCTION_ARGS);
extern Datum varcharsend(PG_FUNCTION_ARGS);
extern Datum varchartypmodin(PG_FUNCTION_ARGS);
extern Datum varchartypmodout(PG_FUNCTION_ARGS);
extern Datum varchar_transform(PG_FUNCTION_ARGS);
extern Datum varchar(PG_FUNCTION_ARGS);
extern Datum opfusion_varchar(Datum arg1, Datum arg2, Datum arg3);

extern Datum nvarchar2in(PG_FUNCTION_ARGS);
extern Datum nvarchar2out(PG_FUNCTION_ARGS);
extern Datum nvarchar2recv(PG_FUNCTION_ARGS);
extern Datum nvarchar2send(PG_FUNCTION_ARGS);
extern Datum nvarchar2typmodin(PG_FUNCTION_ARGS);
extern Datum nvarchar2typmodout(PG_FUNCTION_ARGS);
extern Datum nvarchar2(PG_FUNCTION_ARGS);

/* varlena.c */
extern text* cstring_to_text(const char* s);
extern text* cstring_to_text_with_len(const char* s, size_t len);
extern bytea *cstring_to_bytea_with_len(const char *s, int len);
extern BpChar* cstring_to_bpchar_with_len(const char* s, int len);
extern char* text_to_cstring(const text* t);
extern char* output_text_to_cstring(const text* t);
extern char* output_int32_to_cstring(int32 value, int* len);
extern char* output_int64_to_cstring(int64 value, int* len);
extern char* output_int128_to_cstring(int128 value);
extern char* output_date_out(int32 date);
extern void text_to_cstring_buffer(const text* src, char* dst, size_t dst_len);
extern int text_instr_3args(text* textStr, text* textStrToSearch, int32 beginIndex);
extern int text_instr_4args(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes);
extern int32 text_length(Datum str);
extern int text_cmp(text* arg1, text* arg2, Oid collid);
extern text* text_substring(Datum str, int32 start, int32 length, bool length_not_specified);
extern Datum instr_3args(PG_FUNCTION_ARGS);
extern Datum instr_4args(PG_FUNCTION_ARGS);
extern Datum byteain(PG_FUNCTION_ARGS);
extern void text_to_bktmap(text* gbucket, uint2* bktmap, int *bktlen);
extern bytea* bytea_substring(Datum str, int S, int L, bool length_not_specified);

#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#define TextDatumGetCString(d) text_to_cstring((text*)DatumGetPointer(d))

#define CStringGetByteaDatum(s, len) PointerGetDatum(cstring_to_bytea_with_len(s, len))

extern Datum rawin(PG_FUNCTION_ARGS);
extern Datum rawout(PG_FUNCTION_ARGS);
extern Datum rawtotext(PG_FUNCTION_ARGS);
extern Datum texttoraw(PG_FUNCTION_ARGS);
extern Datum raweq(PG_FUNCTION_ARGS);
extern Datum rawne(PG_FUNCTION_ARGS);
extern Datum rawlt(PG_FUNCTION_ARGS);
extern Datum rawle(PG_FUNCTION_ARGS);
extern Datum rawgt(PG_FUNCTION_ARGS);
extern Datum rawge(PG_FUNCTION_ARGS);
extern Datum rawcmp(PG_FUNCTION_ARGS);
extern Datum rawcat(PG_FUNCTION_ARGS);
extern Datum rawlike(PG_FUNCTION_ARGS);
extern Datum rawnlike(PG_FUNCTION_ARGS);
extern Datum textin(PG_FUNCTION_ARGS);
extern Datum textout(PG_FUNCTION_ARGS);
extern Datum textrecv(PG_FUNCTION_ARGS);
extern Datum textsend(PG_FUNCTION_ARGS);
extern Datum textcat(PG_FUNCTION_ARGS);
extern Datum texteq(PG_FUNCTION_ARGS);
extern Datum textne(PG_FUNCTION_ARGS);
extern Datum text_lt(PG_FUNCTION_ARGS);
extern Datum text_le(PG_FUNCTION_ARGS);
extern Datum text_gt(PG_FUNCTION_ARGS);
extern Datum text_ge(PG_FUNCTION_ARGS);
extern Datum text_larger(PG_FUNCTION_ARGS);
extern Datum text_smaller(PG_FUNCTION_ARGS);
extern Datum text_pattern_lt(PG_FUNCTION_ARGS);
extern Datum text_pattern_le(PG_FUNCTION_ARGS);
extern Datum text_pattern_gt(PG_FUNCTION_ARGS);
extern Datum text_pattern_ge(PG_FUNCTION_ARGS);
extern Datum bttext_pattern_cmp(PG_FUNCTION_ARGS);
extern Datum textlen(PG_FUNCTION_ARGS);
extern Datum textoctetlen(PG_FUNCTION_ARGS);
extern Datum textpos(PG_FUNCTION_ARGS);
extern Datum text_substr(PG_FUNCTION_ARGS);
extern Datum text_substr_no_len(PG_FUNCTION_ARGS);
extern Datum text_substr_null(PG_FUNCTION_ARGS);
extern Datum text_substr_no_len_null(PG_FUNCTION_ARGS);
extern Datum text_substr_orclcompat(PG_FUNCTION_ARGS);
extern Datum text_substr_no_len_orclcompat(PG_FUNCTION_ARGS);
extern Datum bytea_substr_orclcompat(PG_FUNCTION_ARGS);
extern Datum bytea_substr_no_len_orclcompat(PG_FUNCTION_ARGS);
extern Datum textoverlay(PG_FUNCTION_ARGS);
extern Datum textoverlay_no_len(PG_FUNCTION_ARGS);
extern Datum name_text(PG_FUNCTION_ARGS);
extern Datum text_name(PG_FUNCTION_ARGS);
extern int varstr_cmp(char* arg1, int len1, char* arg2, int len2, Oid collid);
extern void varstr_sortsupport(SortSupport ssup, Oid collid, bool bpchar);
extern List* textToQualifiedNameList(text* textval);
extern bool SplitIdentifierString(char* rawstring, char separator, List** namelist, bool downCase = true, bool truncateToolong = true);
extern bool SplitIdentifierInteger(char* rawstring, char separator, List** namelist);
extern Datum replace_text(PG_FUNCTION_ARGS);
extern Datum replace_text_with_two_args(PG_FUNCTION_ARGS);
extern text* replace_text_regexp(text* src_text, void* regexp, text* replace_text, int position, int occur);
extern Datum split_text(PG_FUNCTION_ARGS);
extern Datum text_to_array(PG_FUNCTION_ARGS);
extern Datum array_to_text(PG_FUNCTION_ARGS);
extern Datum text_to_array_null(PG_FUNCTION_ARGS);
extern Datum array_to_text_null(PG_FUNCTION_ARGS);
extern Datum to_hex32(PG_FUNCTION_ARGS);
extern Datum to_hex64(PG_FUNCTION_ARGS);
extern Datum md5_text(PG_FUNCTION_ARGS);
extern Datum md5_bytea(PG_FUNCTION_ARGS);
extern Datum sha(PG_FUNCTION_ARGS);
extern Datum sha1(PG_FUNCTION_ARGS);
extern Datum sha2(PG_FUNCTION_ARGS);

extern Datum unknownin(PG_FUNCTION_ARGS);
extern Datum unknownout(PG_FUNCTION_ARGS);
extern Datum unknownrecv(PG_FUNCTION_ARGS);
extern Datum unknownsend(PG_FUNCTION_ARGS);

extern Datum pg_column_size(PG_FUNCTION_ARGS);
extern Datum datalength(PG_FUNCTION_ARGS);

extern Datum bytea_string_agg_transfn(PG_FUNCTION_ARGS);
extern Datum bytea_string_agg_finalfn(PG_FUNCTION_ARGS);
extern Datum string_agg_transfn(PG_FUNCTION_ARGS);
extern Datum string_agg_finalfn(PG_FUNCTION_ARGS);
extern Datum checksumtext_agg_transfn(PG_FUNCTION_ARGS);

extern Datum group_concat_transfn(PG_FUNCTION_ARGS);
extern Datum group_concat_finalfn(PG_FUNCTION_ARGS);

extern Datum list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum list_agg_finalfn(PG_FUNCTION_ARGS);
extern Datum list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum int2_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum int2_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum int4_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum int4_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum int8_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum int8_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum float4_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum float4_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum float8_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum float8_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum numeric_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum numeric_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum date_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum date_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum timestamp_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum timestamp_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum timestamptz_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum timestamptz_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);
extern Datum interval_list_agg_transfn(PG_FUNCTION_ARGS);
extern Datum interval_list_agg_noarg2_transfn(PG_FUNCTION_ARGS);

extern Datum text_concat(PG_FUNCTION_ARGS);
extern Datum text_concat_ws(PG_FUNCTION_ARGS);
extern Datum text_left(PG_FUNCTION_ARGS);
extern Datum text_right(PG_FUNCTION_ARGS);
extern Datum text_reverse(PG_FUNCTION_ARGS);
extern Datum text_format(PG_FUNCTION_ARGS);
extern Datum text_format_nv(PG_FUNCTION_ARGS);

/* byteawithoutorderwithequalcol.cpp */
extern Datum byteawithoutordercolin(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolin(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoltypmodin(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcoltypmodout(PG_FUNCTION_ARGS);

/* version.c */
extern Datum pgsql_version(PG_FUNCTION_ARGS);
extern Datum opengauss_version(PG_FUNCTION_ARGS);
extern Datum gs_deployment(PG_FUNCTION_ARGS);

/* xid.c */
extern Datum xidin(PG_FUNCTION_ARGS);
extern Datum xidout(PG_FUNCTION_ARGS);
extern Datum xidrecv(PG_FUNCTION_ARGS);
extern Datum xidsend(PG_FUNCTION_ARGS);
extern Datum xideq(PG_FUNCTION_ARGS);
extern Datum xidlt(PG_FUNCTION_ARGS);
extern Datum xid_age(PG_FUNCTION_ARGS);
extern Datum xidin4(PG_FUNCTION_ARGS);
extern Datum xidout4(PG_FUNCTION_ARGS);
extern Datum xidrecv4(PG_FUNCTION_ARGS);
extern Datum xidsend4(PG_FUNCTION_ARGS);
extern Datum xideq4(PG_FUNCTION_ARGS);
extern Datum xidlt4(PG_FUNCTION_ARGS);
extern int xidComparator(const void* arg1, const void* arg2);
extern Datum cidin(PG_FUNCTION_ARGS);
extern Datum cidout(PG_FUNCTION_ARGS);
extern Datum cidrecv(PG_FUNCTION_ARGS);
extern Datum cidsend(PG_FUNCTION_ARGS);
extern Datum cideq(PG_FUNCTION_ARGS);
extern int RemoveRepetitiveXids(TransactionId* xids, int nxid);

/* like.c */
#define LIKE_TRUE 1
#define LIKE_FALSE 0
#define LIKE_ABORT (-1)

extern Datum namelike(PG_FUNCTION_ARGS);
extern Datum namenlike(PG_FUNCTION_ARGS);
extern Datum nameiclike(PG_FUNCTION_ARGS);
extern Datum nameicnlike(PG_FUNCTION_ARGS);
extern Datum textlike(PG_FUNCTION_ARGS);
extern Datum textnlike(PG_FUNCTION_ARGS);
extern Datum texticlike(PG_FUNCTION_ARGS);
extern Datum texticnlike(PG_FUNCTION_ARGS);
extern Datum bytealike(PG_FUNCTION_ARGS);
extern Datum byteanlike(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollike(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolnlike(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollikebytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolnlikebytear(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcollikebyteal(PG_FUNCTION_ARGS);
extern Datum byteawithoutorderwithequalcolnlikebyteal(PG_FUNCTION_ARGS);
extern Datum like_escape(PG_FUNCTION_ARGS);
extern Datum like_escape_bytea(PG_FUNCTION_ARGS);
extern int GenericMatchText(char* s, int slen, char* p, int plen);

/* a_compat.c */
extern Datum lower(PG_FUNCTION_ARGS);
extern Datum upper(PG_FUNCTION_ARGS);
extern Datum initcap(PG_FUNCTION_ARGS);
extern Datum lpad(PG_FUNCTION_ARGS);
extern Datum rpad(PG_FUNCTION_ARGS);
extern Datum btrim(PG_FUNCTION_ARGS);
extern Datum btrim1(PG_FUNCTION_ARGS);
extern Datum byteatrim(PG_FUNCTION_ARGS);
extern Datum ltrim(PG_FUNCTION_ARGS);
extern Datum ltrim1(PG_FUNCTION_ARGS);
extern Datum rtrim(PG_FUNCTION_ARGS);
extern Datum rtrim1(PG_FUNCTION_ARGS);
extern Datum translate(PG_FUNCTION_ARGS);
extern Datum chr(PG_FUNCTION_ARGS);
extern Datum repeat(PG_FUNCTION_ARGS);
extern Datum ascii(PG_FUNCTION_ARGS);

/* inet_cidr_ntop.c */
extern char* inet_cidr_ntop(int af, const void* src, int bits, char* dst, size_t size);

/* inet_net_pton.c */
extern int inet_net_pton(int af, const char* src, void* dst, size_t size);

/* network.c */
extern Datum inet_in(PG_FUNCTION_ARGS);
extern Datum inet_out(PG_FUNCTION_ARGS);
extern Datum inet_recv(PG_FUNCTION_ARGS);
extern Datum inet_send(PG_FUNCTION_ARGS);
extern Datum cidr_in(PG_FUNCTION_ARGS);
extern Datum cidr_out(PG_FUNCTION_ARGS);
extern Datum cidr_recv(PG_FUNCTION_ARGS);
extern Datum cidr_send(PG_FUNCTION_ARGS);
extern Datum network_cmp(PG_FUNCTION_ARGS);
extern Datum network_lt(PG_FUNCTION_ARGS);
extern Datum network_le(PG_FUNCTION_ARGS);
extern Datum network_eq(PG_FUNCTION_ARGS);
extern Datum network_ge(PG_FUNCTION_ARGS);
extern Datum network_gt(PG_FUNCTION_ARGS);
extern Datum network_ne(PG_FUNCTION_ARGS);
extern Datum network_smaller(PG_FUNCTION_ARGS);
extern Datum network_larger(PG_FUNCTION_ARGS);
extern Datum hashinet(PG_FUNCTION_ARGS);
extern Datum network_sub(PG_FUNCTION_ARGS);
extern Datum network_subeq(PG_FUNCTION_ARGS);
extern Datum network_sup(PG_FUNCTION_ARGS);
extern Datum network_supeq(PG_FUNCTION_ARGS);
extern Datum network_network(PG_FUNCTION_ARGS);
extern Datum network_netmask(PG_FUNCTION_ARGS);
extern Datum network_hostmask(PG_FUNCTION_ARGS);
extern Datum network_masklen(PG_FUNCTION_ARGS);
extern Datum network_family(PG_FUNCTION_ARGS);
extern Datum network_broadcast(PG_FUNCTION_ARGS);
extern Datum network_host(PG_FUNCTION_ARGS);
extern Datum network_show(PG_FUNCTION_ARGS);
extern Datum inet_abbrev(PG_FUNCTION_ARGS);
extern Datum cidr_abbrev(PG_FUNCTION_ARGS);
extern double convert_network_to_scalar(Datum value, Oid typid);
extern Datum inet_to_cidr(PG_FUNCTION_ARGS);
extern Datum inet_set_masklen(PG_FUNCTION_ARGS);
extern Datum cidr_set_masklen(PG_FUNCTION_ARGS);
extern Datum network_scan_first(Datum in);
extern Datum network_scan_last(Datum in);
extern Datum inet_client_addr(PG_FUNCTION_ARGS);
extern Datum inet_client_port(PG_FUNCTION_ARGS);
extern Datum inet_server_addr(PG_FUNCTION_ARGS);
extern Datum inet_server_port(PG_FUNCTION_ARGS);
extern Datum inetnot(PG_FUNCTION_ARGS);
extern Datum inetand(PG_FUNCTION_ARGS);
extern Datum inetor(PG_FUNCTION_ARGS);
extern Datum inetpl(PG_FUNCTION_ARGS);
extern Datum inetmi_int8(PG_FUNCTION_ARGS);
extern Datum inetmi(PG_FUNCTION_ARGS);
extern void clean_ipv6_addr(int addr_family, char* addr);

/* mac.c */
extern Datum macaddr_in(PG_FUNCTION_ARGS);
extern Datum macaddr_out(PG_FUNCTION_ARGS);
extern Datum macaddr_recv(PG_FUNCTION_ARGS);
extern Datum macaddr_send(PG_FUNCTION_ARGS);
extern Datum macaddr_cmp(PG_FUNCTION_ARGS);
extern Datum macaddr_lt(PG_FUNCTION_ARGS);
extern Datum macaddr_le(PG_FUNCTION_ARGS);
extern Datum macaddr_eq(PG_FUNCTION_ARGS);
extern Datum macaddr_ge(PG_FUNCTION_ARGS);
extern Datum macaddr_gt(PG_FUNCTION_ARGS);
extern Datum macaddr_ne(PG_FUNCTION_ARGS);
extern Datum macaddr_not(PG_FUNCTION_ARGS);
extern Datum macaddr_and(PG_FUNCTION_ARGS);
extern Datum macaddr_or(PG_FUNCTION_ARGS);
extern Datum macaddr_trunc(PG_FUNCTION_ARGS);
extern Datum hashmacaddr(PG_FUNCTION_ARGS);

/* numeric.c */
extern Datum numeric_in(PG_FUNCTION_ARGS);
extern Datum numeric_out(PG_FUNCTION_ARGS);
extern Datum numeric_out_with_zero(PG_FUNCTION_ARGS);
extern Datum numeric_recv(PG_FUNCTION_ARGS);
extern Datum numeric_send(PG_FUNCTION_ARGS);
extern Datum numerictypmodin(PG_FUNCTION_ARGS);
extern Datum numerictypmodout(PG_FUNCTION_ARGS);
extern Datum numeric_transform(PG_FUNCTION_ARGS);
extern Datum numeric(PG_FUNCTION_ARGS);
extern Datum numeric_abs(PG_FUNCTION_ARGS);
extern Datum numeric_uminus(PG_FUNCTION_ARGS);
extern Datum numeric_uplus(PG_FUNCTION_ARGS);
extern Datum numeric_sign(PG_FUNCTION_ARGS);
extern Datum numeric_round(PG_FUNCTION_ARGS);
extern Datum numeric_trunc(PG_FUNCTION_ARGS);
extern Datum numeric_ceil(PG_FUNCTION_ARGS);
extern Datum numeric_floor(PG_FUNCTION_ARGS);
extern Datum numeric_cmp(PG_FUNCTION_ARGS);
extern Datum numeric_eq(PG_FUNCTION_ARGS);
extern Datum numeric_ne(PG_FUNCTION_ARGS);
extern Datum numeric_gt(PG_FUNCTION_ARGS);
extern Datum numeric_ge(PG_FUNCTION_ARGS);
extern Datum numeric_lt(PG_FUNCTION_ARGS);
extern Datum numeric_le(PG_FUNCTION_ARGS);
extern Datum numeric_add(PG_FUNCTION_ARGS);
extern Datum numeric_sub(PG_FUNCTION_ARGS);
extern Datum numeric_mul(PG_FUNCTION_ARGS);
extern Datum numeric_div(PG_FUNCTION_ARGS);
extern Datum numeric_div_trunc(PG_FUNCTION_ARGS);
extern Datum numeric_mod(PG_FUNCTION_ARGS);
extern Datum numeric_inc(PG_FUNCTION_ARGS);
extern Datum numeric_smaller(PG_FUNCTION_ARGS);
extern Datum numeric_larger(PG_FUNCTION_ARGS);
extern Datum numeric_fac(PG_FUNCTION_ARGS);
extern Datum numeric_sqrt(PG_FUNCTION_ARGS);
extern Datum numeric_exp(PG_FUNCTION_ARGS);
extern Datum numeric_ln(PG_FUNCTION_ARGS);
extern Datum numeric_log(PG_FUNCTION_ARGS);
extern Datum numeric_power(PG_FUNCTION_ARGS);
extern Datum int4_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int4(PG_FUNCTION_ARGS);
extern Datum int8_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int8(PG_FUNCTION_ARGS);
extern Datum int2_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int2(PG_FUNCTION_ARGS);
extern Datum int1_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int1(PG_FUNCTION_ARGS);
extern Datum float8_numeric(PG_FUNCTION_ARGS);
extern Datum float8_interval(PG_FUNCTION_ARGS);
extern Datum numeric_float8(PG_FUNCTION_ARGS);
extern Datum numeric_float8_no_overflow(PG_FUNCTION_ARGS);
extern Datum float4_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_float4(PG_FUNCTION_ARGS);
extern Datum numeric_accum(PG_FUNCTION_ARGS);
extern Datum numeric_avg_accum(PG_FUNCTION_ARGS);
extern Datum int2_accum(PG_FUNCTION_ARGS);
extern Datum int4_accum(PG_FUNCTION_ARGS);
extern Datum int8_accum(PG_FUNCTION_ARGS);
extern Datum numeric_bool(PG_FUNCTION_ARGS);
extern Datum bool_numeric(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum numeric_collect(PG_FUNCTION_ARGS);
#endif
extern Datum int8_avg_accum(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum numeric_avg_collect(PG_FUNCTION_ARGS);
#endif
#ifndef ENABLE_MULTIPLE_NODES
extern Datum int8_avg_accum_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_sum(PG_FUNCTION_ARGS);
extern Datum numeric_accum_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_avg_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_avg_accum_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_var_pop_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_var_samp_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_pop_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_samp_numeric(PG_FUNCTION_ARGS);
#endif
extern Datum numeric_avg(PG_FUNCTION_ARGS);
extern Datum numeric_var_pop(PG_FUNCTION_ARGS);
extern Datum numeric_var_samp(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_pop(PG_FUNCTION_ARGS);
extern Datum numeric_stddev_samp(PG_FUNCTION_ARGS);
extern Datum int2_sum(PG_FUNCTION_ARGS);
extern Datum int4_sum(PG_FUNCTION_ARGS);
extern Datum int8_sum(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum int8_sum_to_int8(PG_FUNCTION_ARGS);
#endif
extern Datum int1_avg_accum(PG_FUNCTION_ARGS);
extern Datum int2_avg_accum(PG_FUNCTION_ARGS);
extern Datum int4_avg_accum(PG_FUNCTION_ARGS);
#ifdef PGXC
extern Datum int8_avg_collect(PG_FUNCTION_ARGS);
#endif
extern Datum int8_avg(PG_FUNCTION_ARGS);
extern Datum width_bucket_numeric(PG_FUNCTION_ARGS);
extern Datum hash_numeric(PG_FUNCTION_ARGS);
extern Datum numtodsinterval(PG_FUNCTION_ARGS);
extern Datum numeric_interval(PG_FUNCTION_ARGS);
extern Datum int1_interval(PG_FUNCTION_ARGS);
extern Datum int2_interval(PG_FUNCTION_ARGS);
extern Datum int4_interval(PG_FUNCTION_ARGS);

/* ri_triggers.c */
extern Datum RI_FKey_check_ins(PG_FUNCTION_ARGS);
extern Datum RI_FKey_check_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_noaction_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_noaction_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_cascade_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_cascade_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_restrict_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_restrict_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setnull_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setnull_upd(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setdefault_del(PG_FUNCTION_ARGS);
extern Datum RI_FKey_setdefault_upd(PG_FUNCTION_ARGS);

/* trigfuncs.c */
extern Datum suppress_redundant_updates_trigger(PG_FUNCTION_ARGS);

/* encoding support functions */
extern Datum getdatabaseencoding(PG_FUNCTION_ARGS);
extern Datum database_character_set(PG_FUNCTION_ARGS);
extern Datum pg_client_encoding(PG_FUNCTION_ARGS);
extern Datum PG_encoding_to_char(PG_FUNCTION_ARGS);
extern Datum PG_char_to_encoding(PG_FUNCTION_ARGS);
extern Datum PG_character_set_name(PG_FUNCTION_ARGS);
extern Datum PG_character_set_id(PG_FUNCTION_ARGS);
extern Datum pg_convert(PG_FUNCTION_ARGS);
extern Datum pg_convert_to(PG_FUNCTION_ARGS);
extern Datum pg_convert_nocase(PG_FUNCTION_ARGS);
extern Datum pg_convert_to_nocase(PG_FUNCTION_ARGS);
extern Datum pg_convert_from(PG_FUNCTION_ARGS);
extern Datum length_in_encoding(PG_FUNCTION_ARGS);
extern Datum pg_encoding_max_length_sql(PG_FUNCTION_ARGS);

/* format_type.c */
extern Datum format_type(PG_FUNCTION_ARGS);
extern char* format_type_be(Oid type_oid);
extern char* format_type_with_typemod(Oid type_oid, int32 typemod);
extern Datum oidvectortypes(PG_FUNCTION_ARGS);
extern int32 type_maximum_size(Oid type_oid, int32 typemod);
extern char *format_type_be_qualified(Oid type_oid);

/* quote.c */
extern Datum quote_ident(PG_FUNCTION_ARGS);
extern Datum quote_literal(PG_FUNCTION_ARGS);
extern char* quote_literal_cstr(const char* rawstr);
extern Datum quote_nullable(PG_FUNCTION_ARGS);

/* guc.c */
extern Datum show_config_by_name(PG_FUNCTION_ARGS);
extern Datum set_config_by_name(PG_FUNCTION_ARGS);
extern Datum show_all_settings(PG_FUNCTION_ARGS);

/* lockfuncs.c */
extern Datum pg_lock_status(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_try_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS);
extern Datum pg_advisory_unlock_all(PG_FUNCTION_ARGS);

/* pgstatfuncs.cpp */
extern Datum gs_stack(PG_FUNCTION_ARGS);

/* txid.c */
extern Datum txid_snapshot_in(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_out(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_recv(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_send(PG_FUNCTION_ARGS);
extern Datum txid_current(PG_FUNCTION_ARGS);
extern Datum txid_current_snapshot(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xmin(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xmax(PG_FUNCTION_ARGS);
extern Datum txid_snapshot_xip(PG_FUNCTION_ARGS);
extern Datum txid_visible_in_snapshot(PG_FUNCTION_ARGS);
extern Datum pgxc_snapshot_status(PG_FUNCTION_ARGS);
extern Datum gs_txid_oldestxmin(PG_FUNCTION_ARGS);

/* uuid.c */
extern Datum uuid_in(PG_FUNCTION_ARGS);
extern Datum uuid_out(PG_FUNCTION_ARGS);
extern Datum uuid_send(PG_FUNCTION_ARGS);
extern Datum uuid_recv(PG_FUNCTION_ARGS);
extern Datum uuid_lt(PG_FUNCTION_ARGS);
extern Datum uuid_le(PG_FUNCTION_ARGS);
extern Datum uuid_eq(PG_FUNCTION_ARGS);
extern Datum uuid_ge(PG_FUNCTION_ARGS);
extern Datum uuid_gt(PG_FUNCTION_ARGS);
extern Datum uuid_ne(PG_FUNCTION_ARGS);
extern Datum uuid_cmp(PG_FUNCTION_ARGS);
extern Datum uuid_hash(PG_FUNCTION_ARGS);

/* hash16 */
extern Datum hash16in(PG_FUNCTION_ARGS);
extern Datum hash16out(PG_FUNCTION_ARGS);
/* hash32 */
extern Datum hash32in(PG_FUNCTION_ARGS);
extern Datum hash32out(PG_FUNCTION_ARGS);

/* windowfuncs.c */
extern Datum window_row_number(PG_FUNCTION_ARGS);
extern Datum window_rank(PG_FUNCTION_ARGS);
extern Datum window_dense_rank(PG_FUNCTION_ARGS);
extern Datum window_percent_rank(PG_FUNCTION_ARGS);
extern Datum window_cume_dist(PG_FUNCTION_ARGS);
extern Datum window_ntile(PG_FUNCTION_ARGS);
extern Datum window_lag(PG_FUNCTION_ARGS);
extern Datum window_lag_with_offset(PG_FUNCTION_ARGS);
extern Datum window_lag_with_offset_and_default(PG_FUNCTION_ARGS);
extern Datum window_lead(PG_FUNCTION_ARGS);
extern Datum window_lead_with_offset(PG_FUNCTION_ARGS);
extern Datum window_lead_with_offset_and_default(PG_FUNCTION_ARGS);
extern Datum window_first_value(PG_FUNCTION_ARGS);
extern Datum window_last_value(PG_FUNCTION_ARGS);
extern Datum window_nth_value(PG_FUNCTION_ARGS);
extern Datum window_delta(PG_FUNCTION_ARGS);

/* access/spgist/spgquadtreeproc.c */
extern Datum spg_quad_config(PG_FUNCTION_ARGS);
extern Datum spg_quad_choose(PG_FUNCTION_ARGS);
extern Datum spg_quad_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_quad_inner_consistent(PG_FUNCTION_ARGS);
extern Datum spg_quad_leaf_consistent(PG_FUNCTION_ARGS);

/* access/spgist/spgkdtreeproc.c */
extern Datum spg_kd_config(PG_FUNCTION_ARGS);
extern Datum spg_kd_choose(PG_FUNCTION_ARGS);
extern Datum spg_kd_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_kd_inner_consistent(PG_FUNCTION_ARGS);

/* access/spgist/spgtextproc.c */
extern Datum spg_text_config(PG_FUNCTION_ARGS);
extern Datum spg_text_choose(PG_FUNCTION_ARGS);
extern Datum spg_text_picksplit(PG_FUNCTION_ARGS);
extern Datum spg_text_inner_consistent(PG_FUNCTION_ARGS);
extern Datum spg_text_leaf_consistent(PG_FUNCTION_ARGS);

/* access/gin/ginarrayproc.c */
extern Datum ginarrayextract(PG_FUNCTION_ARGS);
extern Datum ginarrayextract_2args(PG_FUNCTION_ARGS);
extern Datum ginqueryarrayextract(PG_FUNCTION_ARGS);
extern Datum ginarrayconsistent(PG_FUNCTION_ARGS);
extern Datum ginarraytriconsistent(PG_FUNCTION_ARGS);

/* storage/ipc/procarray.c */
extern Datum pg_get_running_xacts(PG_FUNCTION_ARGS);
extern Datum pgxc_gtm_snapshot_status(PG_FUNCTION_ARGS);
extern Datum get_gtm_lite_status(PG_FUNCTION_ARGS);

/* access/transam/twophase.c */
extern Datum pg_prepared_xact(PG_FUNCTION_ARGS);
extern Datum pg_parse_clog(PG_FUNCTION_ARGS);

/* postmaster.c  */
extern Datum pg_log_comm_status(PG_FUNCTION_ARGS);
extern Datum set_working_grand_version_num_manually(PG_FUNCTION_ARGS);

/* access/transam/varsup.c */
extern Datum pg_check_xidlimit(PG_FUNCTION_ARGS);
extern Datum pg_get_xidlimit(PG_FUNCTION_ARGS);
extern Datum pg_get_variable_info(PG_FUNCTION_ARGS);

/* catalogs/dependency.c */
extern Datum pg_describe_object(PG_FUNCTION_ARGS);
extern Datum pg_identify_object(PG_FUNCTION_ARGS);

/* commands/constraint.c */
extern Datum unique_key_recheck(PG_FUNCTION_ARGS);

/* commands/extension.c */
extern Datum pg_available_extensions(PG_FUNCTION_ARGS);
extern Datum pg_available_extension_versions(PG_FUNCTION_ARGS);
extern Datum pg_extension_update_paths(PG_FUNCTION_ARGS);
extern Datum pg_extension_config_dump(PG_FUNCTION_ARGS);

/* commands/prepare.c */
extern Datum pg_prepared_statement(PG_FUNCTION_ARGS);

/* utils/mmgr/portalmem.c */
extern Datum pg_cursor(PG_FUNCTION_ARGS);

/* utils/adt/pgstatfuncs.c */
extern Datum pg_stat_get_dead_tuples(PG_FUNCTION_ARGS);
extern Datum pg_stat_get_tuples_changed(PG_FUNCTION_ARGS);
extern Datum gs_stat_ustore(PG_FUNCTION_ARGS);

#ifdef PGXC
/* backend/pgxc/pool/poolutils.c */
extern Datum pgxc_pool_check(PG_FUNCTION_ARGS);
extern Datum pgxc_pool_reload(PG_FUNCTION_ARGS);
extern Datum pgxc_pool_connection_status(PG_FUNCTION_ARGS);
extern Datum pg_pool_validate(PG_FUNCTION_ARGS);
extern Datum pg_pool_ping(PG_FUNCTION_ARGS);
extern Datum comm_check_connection_status(PG_FUNCTION_ARGS);
extern Datum pgxc_disaster_read_set(PG_FUNCTION_ARGS);
extern Datum pgxc_disaster_read_init(PG_FUNCTION_ARGS);
extern Datum pgxc_disaster_read_clear(PG_FUNCTION_ARGS);
extern Datum pgxc_disaster_read_status(PG_FUNCTION_ARGS);
#endif

/* comm_proxy.cpp */
extern Datum gs_comm_proxy_thread_status(PG_FUNCTION_ARGS);

/* backend/access/transam/transam.c */
#ifdef PGXC
extern Datum pgxc_is_committed(PG_FUNCTION_ARGS);
extern Datum pgxc_get_csn(PG_FUNCTION_ARGS);
#endif

/*adapt A db's empty_blob*/
extern Datum get_empty_blob(PG_FUNCTION_ARGS);
/*adapt A db's substr*/
extern Datum substrb_with_lenth(PG_FUNCTION_ARGS);
extern Datum substrb_without_lenth(PG_FUNCTION_ARGS);
extern Datum gs_encrypt(PG_FUNCTION_ARGS);
extern Datum gs_decrypt(PG_FUNCTION_ARGS);
extern ScalarVector* vtimestamp_part(PG_FUNCTION_ARGS);
extern ScalarVector* vint4mul(PG_FUNCTION_ARGS);
extern ScalarVector* vint4mi(PG_FUNCTION_ARGS);
extern ScalarVector* vint4pl(PG_FUNCTION_ARGS);
extern ScalarVector* vtextlike(PG_FUNCTION_ARGS);
extern ScalarVector* vtextnlike(PG_FUNCTION_ARGS);
extern ScalarVector* vtextne(PG_FUNCTION_ARGS);
extern ScalarVector* vlower(PG_FUNCTION_ARGS);
extern ScalarVector* vupper(PG_FUNCTION_ARGS);
extern ScalarVector* vlpad(PG_FUNCTION_ARGS);
extern ScalarVector* vnumeric_sum(PG_FUNCTION_ARGS);
extern ScalarVector* vsnumeric_sum(PG_FUNCTION_ARGS);
extern ScalarVector* vnumeric_abs(PG_FUNCTION_ARGS);
extern ScalarVector* vnumeric_fac(PG_FUNCTION_ARGS);
extern ScalarVector* vnumeric_ne(PG_FUNCTION_ARGS);
extern ScalarVector* vinterval_sum(PG_FUNCTION_ARGS);
extern ScalarVector* vcash_sum(PG_FUNCTION_ARGS);
extern ScalarVector* vintervalpl(PG_FUNCTION_ARGS);
extern ScalarVector* vbpcharlen(PG_FUNCTION_ARGS);

extern Datum int1_text(PG_FUNCTION_ARGS);
extern Datum int2_text(PG_FUNCTION_ARGS);
extern Datum int4_text(PG_FUNCTION_ARGS);
extern Datum int8_text(PG_FUNCTION_ARGS);
extern Datum float4_text(PG_FUNCTION_ARGS);
extern Datum float8_text(PG_FUNCTION_ARGS);
extern Datum numeric_text(PG_FUNCTION_ARGS);
extern Datum bpchar_numeric(PG_FUNCTION_ARGS);
extern Datum varchar_numeric(PG_FUNCTION_ARGS);
extern Datum varchar_int4(PG_FUNCTION_ARGS);
extern Datum bpchar_int4(PG_FUNCTION_ARGS);
extern Datum varchar_int8(PG_FUNCTION_ARGS);
extern Datum timestampzone_text(PG_FUNCTION_ARGS);
extern Datum timestamp_text(PG_FUNCTION_ARGS);
extern Datum timestamp_varchar(PG_FUNCTION_ARGS);
extern Datum timestamp_diff(PG_FUNCTION_ARGS);
extern Datum int1_varchar(PG_FUNCTION_ARGS);
extern Datum int2_varchar(PG_FUNCTION_ARGS);
extern Datum int4_varchar(PG_FUNCTION_ARGS);
extern Datum int8_varchar(PG_FUNCTION_ARGS);
extern Datum int8_bpchar(PG_FUNCTION_ARGS);
extern Datum numeric_varchar(PG_FUNCTION_ARGS);
extern Datum numeric_bpchar(PG_FUNCTION_ARGS);
extern Datum float4_varchar(PG_FUNCTION_ARGS);
extern Datum float8_varchar(PG_FUNCTION_ARGS);
extern Datum int1_nvarchar2(PG_FUNCTION_ARGS);
extern Datum varchar_timestamp(PG_FUNCTION_ARGS);
extern Datum bpchar_timestamp(PG_FUNCTION_ARGS);
extern Datum text_int1(PG_FUNCTION_ARGS);
extern Datum text_int2(PG_FUNCTION_ARGS);
extern Datum text_int4(PG_FUNCTION_ARGS);
extern Datum text_int8(PG_FUNCTION_ARGS);
extern Datum text_float4(PG_FUNCTION_ARGS);
extern Datum text_float8(PG_FUNCTION_ARGS);
extern Datum text_numeric(PG_FUNCTION_ARGS);
extern Datum getDistributeKey(PG_FUNCTION_ARGS);
extern Datum text_timestamp(PG_FUNCTION_ARGS);
extern void encryptOBS(char* srcplaintext, char destciphertext[], uint32 destcipherlength);
extern void decryptOBS(
    const char* srcciphertext, char destplaintext[], uint32 destplainlength, const char* obskey = NULL);
extern char *encryptECString(char* src_plain_text, int mode);
extern bool decryptECString(const char* src_cipher_text, char** dest_plain_text, int mode);
extern bool IsECEncryptedString(const char* src_cipher_text);
extern void EncryptGenericOptions(List* options, const char** sensitiveOptionsArray,
                                         int arrayLength, int mode);
extern void DecryptOptions(List *options, const char** sensitiveOptionsArray, int arrayLength, int mode);

#define EC_CIPHER_TEXT_LENGTH 1024

/* fencedudf.cpp */
extern Datum fenced_udf_process(PG_FUNCTION_ARGS);
extern Datum gs_extend_library(PG_FUNCTION_ARGS);

/* cstore_am.cpp */
extern Datum cupointer_bigint(PG_FUNCTION_ARGS);
extern void transformTdeInfoFromPage(TdeInfo* tde_info, TdePageInfo* tde_page_info);
extern void transformTdeInfoToPage(TdeInfo* tde_info, TdePageInfo* tde_page_info);
extern void encryptBlockOrCUData(
    const char* plainText, const size_t plainLength, char* cipherText, size_t* cipherLength, TdeInfo* tdeinfo = NULL);
extern void decryptBlockOrCUData(
    const char* cipherText, const size_t cipherLength, char* plainText, size_t* plainLength, TdeInfo* tdeinfo = NULL);
extern bool isEncryptedCluster();

/* pg_lsn.cpp */
extern Datum pg_lsn_in(PG_FUNCTION_ARGS);

/* nlssort.cpp */
extern Datum nlssort(PG_FUNCTION_ARGS);
extern char *remove_trailing_spaces(const char *src_str);

// template function implementation
//

/* client logic */
extern Datum globalsettingin(PG_FUNCTION_ARGS);
extern Datum columnsettingin(PG_FUNCTION_ARGS);

/* tsdb */
extern Datum add_job_class_depend_internal(PG_FUNCTION_ARGS);
extern Datum remove_job_class_depend_internal(PG_FUNCTION_ARGS);
extern Datum series_internal(PG_FUNCTION_ARGS);
extern Datum top_key_internal(PG_FUNCTION_ARGS);
extern Datum job_cancel(PG_FUNCTION_ARGS);
extern void job_update(PG_FUNCTION_ARGS);
extern Datum submit_job_on_nodes(PG_FUNCTION_ARGS);
extern Datum isubmit_job_on_nodes(PG_FUNCTION_ARGS);
extern Datum isubmit_job_on_nodes_internal(PG_FUNCTION_ARGS);

extern Datum tdigest_merge(PG_FUNCTION_ARGS);
extern Datum tdigest_merge_to_one(PG_FUNCTION_ARGS);
extern Datum calculate_quantile_of(PG_FUNCTION_ARGS);
extern Datum tdigest_mergep(PG_FUNCTION_ARGS);
extern Datum calculate_value_at(PG_FUNCTION_ARGS);
extern Datum tdigest_out(PG_FUNCTION_ARGS);
extern Datum tdigest_in(PG_FUNCTION_ARGS);

/* AI */
extern Datum db4ai_predict_by(PG_FUNCTION_ARGS);
extern Datum db4ai_explain_model(PG_FUNCTION_ARGS);
extern Datum gs_index_advise(PG_FUNCTION_ARGS);
extern Datum hypopg_create_index(PG_FUNCTION_ARGS);                             
extern Datum hypopg_display_index(PG_FUNCTION_ARGS);                            
extern Datum hypopg_drop_index(PG_FUNCTION_ARGS);                               
extern Datum hypopg_estimate_size(PG_FUNCTION_ARGS);                            
extern Datum hypopg_reset_index(PG_FUNCTION_ARGS);

/* MOT */
extern Datum mot_global_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_local_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_session_memory_detail(PG_FUNCTION_ARGS);
extern Datum mot_jit_detail(PG_FUNCTION_ARGS);
extern Datum mot_jit_profile(PG_FUNCTION_ARGS);

/* UBtree index */
Datum gs_index_verify(PG_FUNCTION_ARGS);
Datum gs_index_recycle_queue(PG_FUNCTION_ARGS);

/* undo meta */
extern Datum gs_undo_meta_dump_zone(PG_FUNCTION_ARGS);
extern Datum gs_undo_meta_dump_spaces(PG_FUNCTION_ARGS);
extern Datum gs_undo_meta_dump_slot(PG_FUNCTION_ARGS);
extern Datum gs_undo_translot_dump_slot(PG_FUNCTION_ARGS);
extern Datum gs_undo_translot_dump_xid(PG_FUNCTION_ARGS);
extern Datum gs_undo_dump_record(PG_FUNCTION_ARGS);
extern Datum gs_undo_dump_xid(PG_FUNCTION_ARGS);
extern Datum gs_undo_meta(PG_FUNCTION_ARGS);
extern Datum gs_stat_undo(PG_FUNCTION_ARGS);
extern Datum gs_undo_translot(PG_FUNCTION_ARGS);
extern Datum gs_undo_record(PG_FUNCTION_ARGS);

/* Xlog write/flush */
extern Datum gs_stat_wal_entrytable(PG_FUNCTION_ARGS);
extern Datum gs_walwriter_flush_position(PG_FUNCTION_ARGS);
extern Datum gs_walwriter_flush_stat(PG_FUNCTION_ARGS);

/* Ledger */
extern Datum get_dn_hist_relhash(PG_FUNCTION_ARGS);
extern Datum ledger_hist_check(PG_FUNCTION_ARGS);
extern Datum ledger_hist_repair(PG_FUNCTION_ARGS);
extern Datum ledger_hist_archive(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_check(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_repair(PG_FUNCTION_ARGS);
extern Datum ledger_gchain_archive(PG_FUNCTION_ARGS);
extern Datum gs_is_recycle_object(PG_FUNCTION_ARGS);
extern Datum gs_is_recycle_obj(PG_FUNCTION_ARGS);

/* Oracle connect by */
extern Datum sys_connect_by_path(PG_FUNCTION_ARGS);
extern Datum connect_by_root(PG_FUNCTION_ARGS);

/* Sequence update */
Datum large_sequence_upgrade_node_tree(PG_FUNCTION_ARGS);
Datum large_sequence_rollback_node_tree(PG_FUNCTION_ARGS);

/* Create Index Concurrently for Distribution */
#ifdef ENABLE_MULTIPLE_NODES
extern Datum gs_mark_indisvalid(PG_FUNCTION_ARGS);
#endif

/* origin.cpp */
extern Datum pg_replication_origin_advance(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_create(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_drop(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_oid(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_progress(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_session_is_setup(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_session_progress(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_session_reset(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_session_setup(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_xact_reset(PG_FUNCTION_ARGS);
extern Datum pg_replication_origin_xact_setup(PG_FUNCTION_ARGS);
extern Datum pg_show_replication_origin_status(PG_FUNCTION_ARGS);

/* datum.cpp */
extern Datum btequalimage(PG_FUNCTION_ARGS);
/* varlena.cpp */
extern Datum btvarstrequalimage(PG_FUNCTION_ARGS);

/* pg_publication.cpp */
extern Datum pg_get_publication_tables(PG_FUNCTION_ARGS);

/* launcher.cpp */
extern Datum pg_stat_get_subscription(PG_FUNCTION_ARGS);

/* sqlpatch.cpp */
extern Datum create_sql_patch_by_id_hint(PG_FUNCTION_ARGS);
extern Datum enable_sql_patch(PG_FUNCTION_ARGS);
extern Datum disable_sql_patch(PG_FUNCTION_ARGS);
extern Datum drop_sql_patch(PG_FUNCTION_ARGS);
extern Datum create_abort_patch_by_id(PG_FUNCTION_ARGS);
extern Datum show_sql_patch(PG_FUNCTION_ARGS);

/* row-compression genfile.c */
extern Datum compress_address_header(PG_FUNCTION_ARGS);
extern Datum compress_address_details(PG_FUNCTION_ARGS);
extern Datum compress_buffer_stat_info(PG_FUNCTION_ARGS);
extern Datum compress_ratio_info(PG_FUNCTION_ARGS);
extern Datum compress_statistic_info(PG_FUNCTION_ARGS);
extern Datum pg_read_binary_file_blocks(PG_FUNCTION_ARGS);

#else
#endif
extern char *pg_ultostr(char *str, uint32 value);
extern char *pg_ultostr_zeropad(char *str, uint32 value, int32 minwidth);

/* float.cpp */
extern int float8_cmp_internal(float8 a, float8 b);

#endif /* BUILTINS_H */
