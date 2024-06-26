#ifndef PLPYTHON_INIT_H_
#define PLPYTHON_INIT_H_
#include "postgres.h"

/* forward declarations */
struct PlyGlobalsCtx;
struct PlySessionCtx;

typedef struct PlPythonState
{
    bool is_init;
    /* Record the session that the current lock is granted to */
    uint64 granted_session_id;
    /* Callback function to clean up session memory */
    void (*release_PlySessionCtx_callback) (PlySessionCtx* ctx);
    struct PlyGlobalsCtx* PlyGlobalsCtx;
} PlPythonState;

extern PlPythonState* plpython_state;

extern Size PlpythonShmemSize(void);
extern void PlpythonShmemInit(void);
extern void PlPyGilAcquire(void);

#endif /* PLPYTHON_INIT_H_ */