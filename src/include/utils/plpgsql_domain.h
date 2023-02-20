#ifndef PLPGSQL_DOMAIN_H
#define PLPGSQL_DOMAIN_H
/*
 * Move the define to here from plpgsql.h, because many cpp include plpgsql.h,
 * make the background unable to translate normally,
 * now only pl*cpp include this header
 */
/* define our text domain for translations */
#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("plpgsql")

#undef _
#define _(x) dgettext(TEXTDOMAIN, x)

#endif