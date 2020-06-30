/* -------------------------------------------------------------------------
 *
 * logging.h
 *	 prototypes for logging functions
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_REWIND_LOGGING_H
#define PG_REWIND_LOGGING_H

/* progress counters */
extern uint64 fetch_size;
extern uint64 fetch_done;

/*
 * Enumeration to denote pg_log modes
 */
typedef enum {
    PG_DEBUG,
    PG_PROGRESS,
    PG_WARNING,
    PG_PRINT,
    /*
     * increment build error type, it will change 'increment_return_code' value.
     */
    PG_ERROR, /* Error LEVEL1 for increment build, will retry */
    PG_FATAL  /* Error LEVEL2 for increment build, must do full build */
} eLogType;

extern void pg_log(eLogType type, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
extern void pg_fatal(const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
extern void progress_report(bool force);

#endif /* PG_REWIND_LOGGING_H */

