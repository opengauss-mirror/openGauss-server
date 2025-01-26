#include "postgres.h"
#include "knl/knl_instance.h"

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);
extern "C" void init_session_vars(void);
