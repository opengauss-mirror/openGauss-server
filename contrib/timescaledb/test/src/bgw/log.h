/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TEST_BGW_LOG_H
#define TEST_BGW_LOG_H

extern void ts_bgw_log_set_application_name(char *name);
extern void ts_register_emit_log_hook(void);

/* Hook for intercepting messages before they are sent to the server log */
typedef void (*emit_log_hook_type) (ErrorData *edata);
extern PGDLLIMPORT emit_log_hook_type emit_log_hook;

#endif /* TEST_BGW_LOG_H */
