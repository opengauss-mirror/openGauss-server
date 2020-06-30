/* -------------------------------------------------------------------------
 *
 * like_match.c
 * LIKE pattern matching internal code.
 *
 * This file is included by like.c four times, to provide matching code for
 * (1) single-byte encodings, (2) UTF8, (3) other multi-byte encodings,
 * and (4) case insensitive matches in single-byte encodings.
 * (UTF8 is a special case because we can use a much more efficient version
 * of NextChar than can be used for general multi-byte encodings.)
 *
 * Before the inclusion, we need to define the following macros:
 *
 * NextChar
 * MatchText - to name of function wanted
 * do_like_escape - name of function if wanted - needs CHAREQ and CopyAdvChar
 * MATCH_LOWER - define for case (4) to specify case folding for 1-byte chars
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * src/backend/utils/adt/like_match.c
 *
 * -------------------------------------------------------------------------
 */

/*
 * Originally written by Rich $alz, mirror!rs, Wed Nov 26 19:03:17 EST 1986.
 * Rich $alz is now <rsalz@bbn.com>.
 * Special thanks to Lars Mathiesen <thorinn@diku.dk> for the LABORT code.
 *
 * This code was shamelessly stolen from the "pql" code by myself and
 * slightly modified :)
 *
 * All references to the word "star" were replaced by "percent"
 * All references to the word "wild" were replaced by "like"
 *
 * All the nice shell RE matching stuff was replaced by just "_" and "%"
 *
 * As I don't have a copy of the SQL standard handy I wasn't sure whether
 * to leave in the '\' escape character handling.
 *
 * Keith Parks. <keith@mtcc.demon.co.uk>
 *
 * SQL92 lets you specify the escape character by saying
 * LIKE <pattern> ESCAPE <escape character>. We are a small operation
 * so we force you to use '\'. - ay 7/95
 *
 * Now we have the like_escape() function that converts patterns with
 * any specified escape character (or none at all) to the internal
 * default escape character, which is still '\'. - tgl 9/2000
 *
 * The code is rewritten to avoid requiring null-terminated strings,
 * which in turn allows us to leave out some memcpy() operations.
 * This code should be faster and take less memory, but no promises...
 * - thomas 2000-08-06
 */

/* --------------------
 * Match text and pattern, return LIKE_TRUE, LIKE_FALSE, or LIKE_ABORT.
 *
 * LIKE_TRUE: they match
 * LIKE_FALSE: they don't match
 * LIKE_ABORT: not only don't they match, but the text is too short.
 *
 * If LIKE_ABORT is returned, then no suffix of the text can match the
 * pattern either, so an upper-level % scan can stop scanning now.
 * --------------------
 */

#ifdef MATCH_LOWER
#define GETCHAR(t) MATCH_LOWER(t)
#else
#define GETCHAR(t) (t)
#endif

static int MatchText(char* t, int t_len, char* p, int p_len, pg_locale_t locale, bool locale_is_c)
{
    /* Fast path for match-everything pattern */
    if (p_len == 1 && *p == '%') {
        return LIKE_TRUE;
    }

    /* Since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /*
     * In this loop, we advance by char when matching wildcards (and thus on
     * recursive entry to this function we are properly char-synced). On other
     * occasions it is safe to advance by byte, as the text and pattern will
     * be in lockstep. This allows us to perform all comparisons between the
     * text and pattern on a byte by byte basis, even for multi-byte
     * encodings.
     */
    while (t_len > 0 && p_len > 0) {
        if (*p == '\\') {
            /* Next pattern byte must match literally, whatever it is */
            NextByte(p, p_len);
            /* ... and there had better be one, per SQL standard */
            if (p_len <= 0) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                    errmsg("LIKE pattern must not end with escape character")));
            }
            if (GETCHAR(*p) != GETCHAR(*t)) {
                return LIKE_FALSE;
            }
        } else if (*p == '%') {
            char first_pat;

            /*
             * % processing is essentially a search for a text position at
             * which the remainder of the text matches the remainder of the
             * pattern, using a recursive call to check each potential match.
             *
             * If there are wildcards immediately following the %, we can skip
             * over them first, using the idea that any sequence of N _'s and
             * one or more %'s is equivalent to N _'s and one % (ie, it will
             * match any sequence of at least N text characters).  In this way
             * we will always run the recursive search loop using a pattern
             * fragment that begins with a literal character-to-match, thereby
             * not recursing more than we have to.
             */
            NextByte(p, p_len);

            while (p_len > 0) {
                if (*p == '%') {
                    NextByte(p, p_len);
                } else if (*p == '_') {
                    /* If not enough text left to match the pattern, ABORT */
                    if (t_len <= 0) {
                        return LIKE_ABORT;
                    }
                    NextChar(t, t_len);
                    NextByte(p, p_len);
                } else {
                    break; /* Reached a non-wildcard pattern char */
                }
            }

            /*
             * If we're at end of pattern, match: we have a trailing % which
             * matches any remaining text string.
             */
            if (p_len <= 0) {
                return LIKE_TRUE;
            }
            /*
             * Otherwise, scan for a text position at which we can match the
             * rest of the pattern.  The first remaining pattern char is known
             * to be a regular or escaped literal character, so we can compare
             * the first pattern byte to each text byte to avoid recursing
             * more than we have to.  This fact also guarantees that we don't
             * have to consider a match to the zero-length substring at the
             * end of the text.
             */
            if (*p == '\\') {
                if (p_len < 2) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                        errmsg("LIKE pattern must not end with escape character")));
                }
                first_pat = GETCHAR(p[1]);
            } else {
                first_pat = GETCHAR(*p);
            }

            while (t_len > 0) {
                if (GETCHAR(*t) == first_pat) {
                    int matched = MatchText(t, t_len, p, p_len, locale, locale_is_c);
                    if (matched != LIKE_FALSE) {
                        return matched; /* TRUE or ABORT */
                    }
                }

                NextChar(t, t_len);
            }

            /*
             * End of text with no match, so no point in trying later places
             * to start matching this pattern.
             */
            return LIKE_ABORT;
        } else if (*p == '_') {
            /* _ matches any single character, and we know there is one */
            NextChar(t, t_len);
            NextByte(p, p_len);
            continue;
        } else if (!CHAREQ(p, t)) {
            /* non-wildcard pattern char fails to match text char */
            return LIKE_FALSE;
        }

        /*
         * Pattern and text match, so advance.
         *
         * It is safe to use NextByte instead of NextChar here, even for
         * multi-byte character sets, because we are not following immediately
         * after a wildcard character. If we are in the middle of a multibyte
         * character, we must already have matched at least one byte of the
         * character from both text and pattern; so we cannot get out-of-sync
         * on character boundaries.  And we know that no backend-legal
         * encoding allows ASCII characters such as '%' to appear as non-first
         * bytes of characters, so we won't mistakenly detect a new wildcard.
         */
        NextChar(t, t_len);
        NextChar(p, p_len);
    }

    if (t_len > 0) {
        return LIKE_FALSE; /* end of pattern, but not of text */
    }

    /*
     * End of text, but perhaps not of pattern.  Match iff the remaining
     * pattern can match a zero-length string, ie, it's zero or more %'s.
     */
    while (p_len > 0 && *p == '%') {
        NextByte(p, p_len);
    }
    if (p_len <= 0) {
        return LIKE_TRUE;
    }

    /*
     * End of text with no match, so no point in trying later places to start
     * matching this pattern.
     */
    return LIKE_ABORT;
}

/*
 * like_escape() --- given a pattern and an ESCAPE string,
 * convert the pattern to use Postgres' standard backslash escape convention.
 */
#ifdef do_like_escape

static text* do_like_escape(text* pat, text* esc)
{
    text* result = NULL;
    char *p = NULL;
    char *e = NULL;
    char *r = NULL;
    bool after_escape = false;

    p = VARDATA_ANY(pat);
    int p_len = VARSIZE_ANY_EXHDR(pat);
    e = VARDATA_ANY(esc);
    int e_len = VARSIZE_ANY_EXHDR(esc);

    /*
     * Worst-case pattern growth is 2x --- unlikely, but it's hardly worth
     * trying to calculate the size more accurately than that.
     */
    result = (text*)palloc(p_len * 2 + VARHDRSZ);
    r = VARDATA(result);

    if (e_len == 0) {
        /*
         * No escape character is wanted.  Double any backslashes in the
         * pattern to make them act like ordinary characters.
         */
        while (p_len > 0) {
            if (*p == '\\') {
                *r++ = '\\';
            }
            CopyAdvChar(r, p, p_len);
        }
    } else {
        /*
         * The specified escape must be only a single character.
         */
        NextChar(e, e_len);
        if (e_len != 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                errmsg("invalid escape string"),
                errhint("Escape string must be empty or one character.")));
        }

        /*
         * If specified escape is '\', just copy the pattern as-is.
         */
        e = VARDATA_ANY(esc);
        if (*e == '\\') {
            errno_t ss_rc = memcpy_s(result, p_len * 2 + VARHDRSZ, pat, VARSIZE_ANY(pat));
            securec_check(ss_rc, "\0", "\0");
            return result;
        }

        /*
         * Otherwise, convert occurrences of the specified escape character to
         * '\', and double occurrences of '\' --- unless they immediately
         * follow an escape character!
         */
        after_escape = false;
        while (p_len > 0) {
            if (CHAREQ(p, e) && !after_escape) {
                *r++ = '\\';
                NextChar(p, p_len);
                after_escape = true;
            } else if (*p == '\\') {
                *r++ = '\\';
                if (!after_escape) {
                    *r++ = '\\';
                }
                NextChar(p, p_len);
                after_escape = false;
            } else {
                CopyAdvChar(r, p, p_len);
                after_escape = false;
            }
        }
    }

    SET_VARSIZE(result, r - ((char*)result));

    return result;
}
#endif /* do_like_escape */

#ifdef CHAREQ
#undef CHAREQ
#endif

#undef NextChar
#undef CopyAdvChar
#undef MatchText

#ifdef do_like_escape
#undef do_like_escape
#endif

#undef GETCHAR

#ifdef MATCH_LOWER
#undef MATCH_LOWER

#endif
