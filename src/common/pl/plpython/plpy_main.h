/*
 * src/common/pl/plpython/plpy_main.h
 */

#ifndef PLPY_MAIN_H
#define PLPY_MAIN_H

#include "plpy_procedure.h"

class PyLock {
public:
    explicit PyLock(int* lockLevel) : m_lockLevel(lockLevel)
    {
        (*m_lockLevel)++;
    }

    ~PyLock()
    {
        (*m_lockLevel)--;
    }
    void Reset()
    {
        (*m_lockLevel) = 0;
    }

private:
    int* m_lockLevel;
};

/*
 * A stack of PL/Python execution contexts. Each time user-defined Python code
 * is called, an execution context is created and put on the stack. After the
 * Python code returns, the context is destroyed.
 */
typedef struct PLyExecutionContext {
    PLyProcedure* curr_proc;          /* the currently executing procedure */
    MemoryContext scratch_ctx;        /* a context for things like type I/O */
    struct PLyExecutionContext* next; /* previous stack level */
} PLyExecutionContext;

/* Get the current execution context */
extern PLyExecutionContext* PLy_current_execution_context(void);

#endif /* PLPY_MAIN_H */
