/*---------------------------------------------------------------------------------------*
 * gms_i18n.h
 *
 *  Definition about gms_i18n package.
 *
 * IDENTIFICATION
 *        contrib/gms_i18n/gms_i18n.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GMS_I18N__
#define __GMS_I18N__

#include "postgres.h"

extern "C" Datum gms_i18n_raw_to_char(PG_FUNCTION_ARGS);
extern "C" Datum gms_i18n_string_to_raw(PG_FUNCTION_ARGS);

#endif // __GMS_I18N__