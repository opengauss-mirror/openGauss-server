/*
 * src/common/pl/plpython/plpy_plpymodule.h
 */

#ifndef PLPY_PLPYMODULE_H
#define PLPY_PLPYMODULE_H

#include "utils/hsearch.h"

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit_plpy(void);
#endif
extern void PLy_init_plpy(void);

#endif /* PLPY_PLPYMODULE_H */
