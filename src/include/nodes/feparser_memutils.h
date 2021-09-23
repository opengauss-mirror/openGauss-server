/*
 * fe_memutils.h
 * memory management support for frontend code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2003-2015, PostgreSQL Global Development Group
 *
 * src/include/common/fe_memutils.h
 */
#ifndef FEPARSER_MEMUTILS_H
#define FEPARSER_MEMUTILS_H
#ifdef FRONTEND_PARSER
/*
 * "Safe" memory allocation functions --- these exit(1) on failure
 */
char *feparser_strdup(const char *in);
char *feparser_strndup(const char *in, const size_t n);
void *feparser_malloc(size_t size);
void *feparser_malloc0(size_t size);
void *feparser_malloc_extended(size_t size, int flags);
void *feparser_realloc(void *pointer, size_t size);
void feparser_free(void *pointer);
void free_memory();
#endif
#endif /* FEPARSER_MEMUTILS_H */
