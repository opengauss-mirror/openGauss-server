
/* This file was generated automatically by the Snowball to ANSI C compiler */

#include "header.h"

#ifdef __cplusplus
extern "C" {
#endif
extern int italian_UTF_8_stem(struct SN_env* z);
extern struct SN_env* italian_UTF_8_create_env(void);
extern void italian_UTF_8_close_env(struct SN_env* z);
#ifdef __cplusplus
}
#endif

extern int italian_UTF_8_stem(struct SN_env* z)
{
    ereport(ERROR,
        (errmodule(MOD_TS),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Text search for Italian is not supported!")));
    return 1;
}

extern struct SN_env* italian_UTF_8_create_env(void)
{
    ereport(ERROR,
        (errmodule(MOD_TS),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Text search for Italian is not supported!")));
    return NULL;
}

extern void italian_UTF_8_close_env(struct SN_env* z)
{
    ereport(ERROR,
        (errmodule(MOD_TS),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Text search for Italian is not supported!")));
}