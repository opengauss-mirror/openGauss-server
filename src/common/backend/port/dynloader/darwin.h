/* src/backend/port/dynloader/darwin.h */
#ifndef DYNLOADER_DARWIN_H
#define DYNLOADER_DARWIN_H

#include "fmgr.h"

void* pg_dlopen(char* filename);
PGFunction pg_dlsym(void* handle, char* funcname);
void pg_dlclose(void* handle);
char* pg_dlerror(void);

#endif /* DYNLOADER_DARWIN_H */
