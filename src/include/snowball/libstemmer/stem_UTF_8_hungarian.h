
/* This file was generated automatically by the Snowball to ANSI C compiler */
#ifndef STEM_UTF_8_HUNGARIAN_H
#define STEM_UTF_8_HUNGARIAN_H

#ifdef __cplusplus
extern "C" {
#endif

extern struct SN_env* hungarian_UTF_8_create_env(void);
extern void hungarian_UTF_8_close_env(struct SN_env* z);

extern int hungarian_UTF_8_stem(struct SN_env* z);

#ifdef __cplusplus
}
#endif

#endif /* STEM_UTF_8_HUNGARIAN_H */
