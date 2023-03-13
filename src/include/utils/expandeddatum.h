/*-------------------------------------------------------------------------
 *
 * expandeddatum.h
 *	  Declarations for access to "expanded" value representations.
 *
 * Complex data types, particularly container types such as arrays and
 * records, usually have on-disk representations that are compact but not
 * especially convenient to modify.  What's more, when we do modify them,
 * having to recopy all the rest of the value can be extremely inefficient.
 * Therefore, we provide a notion of an "expanded" representation that is used
 * only in memory and is optimized more for computation than storage.
 * The format appearing on disk is called the data type's "flattened"
 * representation, since it is required to be a contiguous blob of bytes --
 * but the type can have an expanded representation that is not.  Data types
 * must provide means to translate an expanded representation back to
 * flattened form.
 *
 * An expanded object is meant to survive across multiple operations, but
 * not to be enormously long-lived; for example it might be a local variable
 * in a PL/pgSQL procedure.  So its extra bulk compared to the on-disk format
 * is a worthwhile trade-off.
 *
 * References to expanded objects are a type of TOAST pointer.
 * Because of longstanding conventions in Postgres, this means that the
 * flattened form of such an object must always be a varlena object.
 * Fortunately that's no restriction in practice.
 *
 * There are actually two kinds of TOAST pointers for expanded objects:
 * read-only and read-write pointers.  Possession of one of the latter
 * authorizes a function to modify the value in-place rather than copying it
 * as would normally be required.  Functions should always return a read-write
 * pointer to any new expanded object they create.  Functions that modify an
 * argument value in-place must take care that they do not corrupt the old
 * value if they fail partway through.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/expandeddatum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPANDEDDATUM_H
#define EXPANDEDDATUM_H


extern Datum MakeExpandedObjectReadOnlyInternal(Datum d);


#endif							/* EXPANDEDDATUM_H */