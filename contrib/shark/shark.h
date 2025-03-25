#include "postgres.h"
#include "knl/knl_instance.h"

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);
extern "C" void init_session_vars(void);
extern "C" void set_extension_index(uint32 index);
extern "C" Datum fetch_status(PG_FUNCTION_ARGS);
extern "C" Datum rowcount(PG_FUNCTION_ARGS);
extern "C" Datum rowcount_big(PG_FUNCTION_ARGS);
static void fetch_cursor_end_hook(int fetch_status);
static void rowcount_hook(int64 rowcount);

typedef struct SharkContext {
    bool dialect_sql;
    int fetch_status;
    int64 rowcount;
} sharkContext;

SharkContext* GetSessionContext();

typedef enum {
    FETCH_STATUS_SUCCESS = 0,    // The FETCH statement was successful.
    FETCH_STATUS_FAIL = -1,      // The FETCH statement failed or the row was beyond the result set.
    FETCH_STATUS_NOT_EXIST = -2, // The row fetched is missing.
    FETCH_STATUS_NOT_FETCH = -9  // The cursor is not performing a fetch operation.
} CursorFetchStatus;
