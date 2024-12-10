/*---------------------------------------------------------------------------------------*
 * gms_match.h
 *
 *  Definition about gms_match package.
 *
 * IDENTIFICATION
 *        contrib/gms_match/gms_match.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GMS_MATCH_H
#define GMS_MATCH_H

extern "C" Datum edit_distance(PG_FUNCTION_ARGS);
extern "C" Datum edit_distance_similarity(PG_FUNCTION_ARGS);

#endif
