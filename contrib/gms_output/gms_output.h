#ifndef __GMS_OUTPUT__
#define __GMS_OUTPUT__

#include "postgres.h"

#define OUTPUTBUF_LEN 10240

typedef struct OutputContext {
    bool is_server_output = false;
    char *buffer = NULL;
    int  buffer_size = 0;    /* allocated bytes in buffer */
    int  buffer_len = 0;    /* used bytes in buffer */
    int  buffer_get = 0;    /* retrieved bytes in buffer */
    uint32      gms_buf_get;    /* retrieved bytes in buffer */
    bool        gms_getline;      /*  in order to clean buffer */
    int64       gms_valid_num;
    char        output_buf[OUTPUTBUF_LEN];
    bool        gms_serveroutput;
} OutputContext;

extern "C" Datum gms_output_enable_default(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_enable(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_disable(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_serveroutput(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_put(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_put_line(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_new_line(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_get_line(PG_FUNCTION_ARGS);
extern "C" Datum gms_output_get_lines(PG_FUNCTION_ARGS);
extern "C" void set_extension_index(uint32 index);
extern "C" void init_session_vars(void);
extern "C" OutputContext* get_session_context();

#endif // __GMS_OUTPUT__