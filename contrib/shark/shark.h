#include "postgres.h"
#include "knl/knl_instance.h"

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);
extern "C" void init_session_vars(void);
extern "C" void set_extension_index(uint32 index);
extern "C" Datum fetch_status(PG_FUNCTION_ARGS);
extern "C" Datum rowcount(PG_FUNCTION_ARGS);
extern "C" Datum rowcount_big(PG_FUNCTION_ARGS);
extern "C" Datum procid(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantin(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantout(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantrecv(PG_FUNCTION_ARGS);
extern "C" Datum sql_variantsend(PG_FUNCTION_ARGS);
extern "C" Datum databasepropertyex(PG_FUNCTION_ARGS);
extern "C" Datum suser_name(PG_FUNCTION_ARGS);
extern "C" Datum suser_id(PG_FUNCTION_ARGS);
extern "C" Datum get_scope_identity(PG_FUNCTION_ARGS);
extern "C" Datum get_ident_current(PG_FUNCTION_ARGS);
extern "C" Datum numeric_log10(PG_FUNCTION_ARGS);
extern "C" Datum shark_timestamp_diff(PG_FUNCTION_ARGS);
extern "C" Datum shark_timestamp_diff_big(PG_FUNCTION_ARGS);
static void fetch_cursor_end_hook(int fetch_status);
static void rowcount_hook(int64 rowcount);
extern void set_procid(Oid oid);
extern Oid get_procid();
extern int PltsqlNewScopeIdentityNestLevel();
extern void PltsqlRevertLastScopeIdentity(int nestLevel);
extern int128 last_scope_identity_value();
extern char* GetPhysicalSchemaName(char *dbName, const char *schemaName);
extern void AssignIdentitycmdsHook(void);
extern void InitIntervalLookup(void);

typedef struct SeqTableIdentityData {
    Oid relid;                /* pg_class OID of this sequence */
    bool lastIdentityValid; /* check value validity */
    int128 lastIdentity;     /* sequence identity value */
} SeqTableIdentityData;

typedef struct ScopeIdentityStack {
    struct ScopeIdentityStack* prev;                      /* previous stack item if any */
    int nestLevel;                                       /* nesting depth at which we made entry */
    SeqTableIdentityData last_used_seq_identity_in_scope; /* current scope identity value */
} ScopeIdentityStack;
typedef struct SharkContext {
    bool dialect_sql;
    int fetch_status;
    int64 rowcount;
    Oid procid;
    ScopeIdentityStack *lastUsedScopeSeqIdentity;
    int pltsqlScopeIdentityNestLevel;
} sharkContext;

SharkContext* GetSessionContext();

typedef enum {
    FETCH_STATUS_SUCCESS = 0,    // The FETCH statement was successful.
    FETCH_STATUS_FAIL = -1,      // The FETCH statement failed or the row was beyond the result set.
    FETCH_STATUS_NOT_EXIST = -2, // The row fetched is missing.
    FETCH_STATUS_NOT_FETCH = -9  // The cursor is not performing a fetch operation.
} CursorFetchStatus;
