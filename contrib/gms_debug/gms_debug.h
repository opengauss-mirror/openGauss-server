/*---------------------------------------------------------------------------------------*
 * gms_debug.h
 *
 *  Definition about gms_debug package.
 *
 * IDENTIFICATION
 *        contrib/gms_debug/gms_debug.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_DEBUG_H
#define GMS_DEBUG_H

#define BREAK_NEXT_LINE 2
#define BREAK_ANY_CALL 4
#define BREAK_ANY_RETURN 8
#define BREAK_RETURN 16 
#define BREAK_EXCEPTION 32 
#define BREAK_HANDLER 64 
#define BREAK_ABORT_EXECUTION 128 

#define ERROR_ILLEGAL_LINE -1
#define ERROR_BAD_HANDLE -2
#define ERROR_ALREADY_EXISTS -3
#define ERROR_COMMUNICATION -4
#define ERROR_FUNC_NOT_ATTACHED -9
 
/* from gms_debug.cpp */
extern "C"  Datum gms_debug_attach_session(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_detach_session(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_get_runtime_info(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_probe_version(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_initialize(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_on(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_off(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_continue(PG_FUNCTION_ARGS);
extern "C"  Datum gms_debug_set_breakpoint(PG_FUNCTION_ARGS);


#define CHECK_RETURN_DATUM(mask_string) \
    do {                                \
        if (mask_string == NULL)        \
            PG_RETURN_DATUM(0);         \
    } while (0)

#endif
