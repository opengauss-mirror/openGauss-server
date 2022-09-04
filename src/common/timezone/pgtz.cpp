/* -------------------------------------------------------------------------
 *
 * pgtz.cpp
 *	  Timezone Library Integration Functions
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/timezone/pgtz.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "miscadmin.h"
#include "pgtz.h"
#include "storage/smgr/fd.h"
#include "utils/hsearch.h"

#ifdef ENABLE_UT
#define static
#endif

/* Current session timezone (controlled by TimeZone GUC) */
THR_LOCAL pg_tz* session_timezone = NULL;

/* Current log timezone (controlled by log_timezone GUC) */
THR_LOCAL pg_tz* log_timezone = NULL;

static bool scan_directory_ci(const char* dirname, const char* fname, int fnamelen, char* canonname, int canonnamelen);

/*
 * Return full pathname of timezone data directory
 */
static const char* pg_TZDIR(void)
{
#ifndef SYSTEMTZDIR
    /* normal case: timezone stuff is under our share dir */
    static THR_LOCAL bool done_tzdir = false;
    static THR_LOCAL char tzdir[MAXPGPATH] = {0};

    if (done_tzdir) {
        return tzdir;
    }

    get_share_path(my_exec_path, tzdir);
    strlcpy(tzdir + strlen(tzdir), "/timezone", MAXPGPATH - strlen(tzdir));

    done_tzdir = true;
    return tzdir;
#else
    /* we're configured to use system's timezone database */
    return SYSTEMTZDIR;
#endif
}

/*
 * Given a timezone name, open() the timezone data file.  Return the
 * file descriptor if successful, -1 if not.
 *
 * The input name is searched for case-insensitively (we assume that the
 * timezone database does not contain case-equivalent names).
 *
 * If "canonname" is not NULL, then on success the canonical spelling of the
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 */
int pg_open_tzfile(const char* name, char* canonname)
{
    const char* fname = NULL;
    char fullname[MAXPGPATH];
    int fullnamelen;
    int orignamelen;

    /*
     * Loop to split the given name into directory levels; for each level,
     * search using scan_directory_ci().
     */
    strlcpy(fullname, pg_TZDIR(), sizeof(fullname));
    orignamelen = fullnamelen = strlen(fullname);
    fname = name;
    for (;;) {
        const char* slashptr = NULL;
        int fnamelen;

        slashptr = strchr(fname, '/');
        if (slashptr != NULL) {
            fnamelen = slashptr - fname;
        } else {
            fnamelen = strlen(fname);
        }
        if (fullnamelen + 1 + fnamelen >= MAXPGPATH) {
            return -1; /* not gonna fit */
        }
        if (!scan_directory_ci(fullname, fname, fnamelen, fullname + fullnamelen + 1, MAXPGPATH - fullnamelen - 1)) {
            return -1;
        }
        fullname[fullnamelen++] = '/';
        fullnamelen += strlen(fullname + fullnamelen);
        if (slashptr != NULL) {
            fname = slashptr + 1;
        } else {
            break;
        }
    }

    if (canonname != NULL) {
        strlcpy(canonname, fullname + orignamelen + 1, TZ_STRLEN_MAX + 1);
    }

    return open(fullname, O_RDONLY | PG_BINARY, 0);
}

/*
 * Scan specified directory for a case-insensitive match to fname
 * (of length fnamelen --- fname may not be null terminated!).	If found,
 * copy the actual filename into canonname and return true.
 */
static bool scan_directory_ci(const char* dirname, const char* fname, int fnamelen, char* canonname, int canonnamelen)
{
    bool found = false;
    DIR* dirdesc = NULL;
    struct dirent* direntry = NULL;

    dirdesc = AllocateDir(dirname);
    if (dirdesc == NULL) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirname)));
        return false;
    }

    while ((direntry = ReadDir(dirdesc, dirname)) != NULL) {
        /*
         * Ignore . and .., plus any other "hidden" files.	This is a security
         * measure to prevent access to files outside the timezone directory.
         */
        if (direntry->d_name[0] == '.') {
            continue;
        }

        if (strlen(direntry->d_name) == (size_t)(unsigned int)fnamelen &&
            pg_strncasecmp(direntry->d_name, fname, fnamelen) == 0) {
            /* Found our match */
            strlcpy(canonname, direntry->d_name, canonnamelen);
            found = true;
            break;
        }
    }

    FreeDir(dirdesc);

    return found;
}

/*
 * We keep loaded timezones in a hashtable so we don't have to
 * load and parse the TZ definition file every time one is selected.
 * Because we want timezone names to be found case-insensitively,
 * the hash key is the uppercased name of the zone.
 */
typedef struct {
    /* tznameupper contains the all-upper-case name of the timezone */
    char tznameupper[TZ_STRLEN_MAX + 1];
    pg_tz tz;
} pg_tz_cache;

static bool init_timezone_hashtable(void)
{
    HASHCTL hash_ctl;
    errno_t rc = EOK;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = TZ_STRLEN_MAX + 1;
    hash_ctl.entrysize = sizeof(pg_tz_cache);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;

    u_sess->time_cxt.timezone_cache = hash_create("Timezones", 4, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
    if (u_sess->time_cxt.timezone_cache == NULL) {
        return false;
    }

    return true;
}

/*
 * Load a timezone from file or from cache.
 * Does not verify that the timezone is acceptable!
 *
 * "GMT" is always interpreted as the tzparse() definition, without attempting
 * to load a definition from the filesystem.  This has a number of benefits:
 * 1. It's guaranteed to succeed, so we don't have the failure mode wherein
 * the bootstrap default timezone setting doesn't work (as could happen if
 * the OS attempts to supply a leap-second-aware version of "GMT").
 * 2. Because we aren't accessing the filesystem, we can safely initialize
 * the "GMT" zone definition before my_exec_path is known.
 * 3. It's quick enough that we don't waste much time when the bootstrap
 * default timezone setting is later overridden from postgresql.conf.
 */
pg_tz* pg_tzset(const char* name)
{
    pg_tz_cache* tzp = NULL;
    struct state tzstate;
    char uppername[TZ_STRLEN_MAX + 1];
    char canonname[TZ_STRLEN_MAX + 1];
    char* p = NULL;
    errno_t rc = EOK;

    if (strlen(name) > TZ_STRLEN_MAX) {
        return NULL; /* not going to fit */
    }

    if (u_sess->time_cxt.timezone_cache == NULL) {
        if (!init_timezone_hashtable()) {
            return NULL;
        }
    }

    /*
     * Upcase the given name to perform a case-insensitive hashtable search.
     * (We could alternatively downcase it, but we prefer upcase so that we
     * can get consistently upcased results from tzparse() in case the name is
     * a POSIX-style timezone spec.)
     */
    p = uppername;
    while (*name) {
        *p++ = pg_toupper((unsigned char)*name++);
    }
    *p = '\0';

    tzp = (pg_tz_cache*)hash_search(u_sess->time_cxt.timezone_cache, uppername, HASH_FIND, NULL);
    if (tzp != NULL) {
        /* Timezone found in cache, nothing more to do */
        return &tzp->tz;
    }

    /*
     * "GMT" is always sent to tzparse(), as per discussion above.
     */
    if (strcmp(uppername, "GMT") == 0) {
        if (tzparse(uppername, &tzstate, TRUE) != 0) {
            /* This really, really should not happen ... */
            elog(ERROR, "could not initialize GMT time zone");
        }
        /* Use uppercase name as canonical */
        rc = strcpy_s(canonname, TZ_STRLEN_MAX + 1, uppername);
        securec_check(rc, "\0", "\0");
    } else if (tzload(uppername, canonname, &tzstate, TRUE) != 0) {
        if (uppername[0] == ':' || tzparse(uppername, &tzstate, FALSE) != 0) {
            /* Unknown timezone. Fail our call instead of loading GMT! */
            return NULL;
        }
        /* For POSIX timezone specs, use uppercase name as canonical */
        rc = strcpy_s(canonname, TZ_STRLEN_MAX + 1, uppername);
        securec_check(rc, "\0", "\0");
    }

    /* Save timezone in the cache */
    tzp = (pg_tz_cache*)hash_search(u_sess->time_cxt.timezone_cache, uppername, HASH_ENTER, NULL);

    /* hash_search already copied uppername into the hash key */
    rc = strcpy_s(tzp->tz.TZname, TZ_STRLEN_MAX + 1, canonname);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(&tzp->tz.state, sizeof(tzp->tz.state), &tzstate, sizeof(tzstate));
    securec_check(rc, "\0", "\0");

    return &tzp->tz;
}

/*
 * Initialize timezone library
 *
 * This is called before GUC variable initialization begins.  Its purpose
 * is to ensure that log_timezone has a valid value before any logging GUC
 * variables could become set to values that require elog.c to provide
 * timestamps (e.g., log_line_prefix).	We may as well initialize
 * session_timestamp to something valid, too.
 */
void pg_timezone_initialize(void)
{
    /*
     * We may not yet know where PGSHAREDIR is (in particular this is true in
     * an EXEC_BACKEND subprocess).  So use "GMT", which pg_tzset forces to be
     * interpreted without reference to the filesystem.  This corresponds to
     * the bootstrap default for these variables in guc.c, although in
     * principle it could be different.
     */
    session_timezone = pg_tzset("GMT");
    log_timezone = session_timezone;
}

/*
 * Functions to enumerate available timezones
 *
 * Note that pg_tzenumerate_next() will return a pointer into the pg_tzenum
 * structure, so the data is only valid up to the next call.
 *
 * All data is allocated using palloc in the current context.
 */
#define MAX_TZDIR_DEPTH 10

struct pg_tzenum {
    int baselen;
    int depth;
    DIR* dirdesc[MAX_TZDIR_DEPTH];
    char* dirname[MAX_TZDIR_DEPTH];
    struct pg_tz tz;
};

/* typedef pg_tzenum is declared in pgtime.h */
pg_tzenum* pg_tzenumerate_start(void)
{
    pg_tzenum* ret = (pg_tzenum*)palloc0(sizeof(pg_tzenum));
    if (ret == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not alloc memory ")));
    }

    char* startdir = pstrdup(pg_TZDIR());
    if (startdir != NULL) {
        ret->baselen = strlen(startdir) + 1;
        ret->depth = 0;
        ret->dirname[0] = startdir;
        ret->dirdesc[0] = AllocateDir(startdir);
    }
    if (ret->dirdesc[0] == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", startdir)));
    }
    return ret;
}

void pg_tzenumerate_end(pg_tzenum* dir)
{
    while (dir->depth >= 0) {
        FreeDir(dir->dirdesc[dir->depth]);
        pfree_ext(dir->dirname[dir->depth]);
        dir->depth--;
    }
    pfree_ext(dir);
}

pg_tz* pg_tzenumerate_next(pg_tzenum* dir)
{
    while (dir->depth >= 0) {
        struct dirent* direntry = NULL;
        char fullname[MAXPGPATH];
        struct stat statbuf;
        errno_t rc = EOK;

        direntry = ReadDir(dir->dirdesc[dir->depth], dir->dirname[dir->depth]);

        if (direntry == NULL) {
            /* End of this directory */
            FreeDir(dir->dirdesc[dir->depth]);
            pfree_ext(dir->dirname[dir->depth]);
            dir->depth--;
            continue;
        }

        if (direntry->d_name[0] == '.') {
            continue;
        }

        rc = snprintf_s(fullname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dir->dirname[dir->depth], direntry->d_name);
        securec_check_ss(rc, "\0", "\0");
        if (stat(fullname, &statbuf) != 0) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat \"%s\": %m", fullname)));
        }
        if (S_ISDIR(statbuf.st_mode)) {
            /* Step into the subdirectory */
            if (dir->depth >= MAX_TZDIR_DEPTH - 1) {
                ereport(ERROR, (errcode(ERRCODE_IO_ERROR), errmsg_internal("timezone directory stack overflow")));
            }
            dir->depth++;
            dir->dirname[dir->depth] = pstrdup(fullname);
            dir->dirdesc[dir->depth] = AllocateDir(fullname);
            if (dir->dirdesc[dir->depth] == NULL) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", fullname)));
            }

            /* Start over reading in the new directory */
            continue;
        }

        /*
         * Load this timezone using tzload() not pg_tzset(), so we don't fill
         * the cache
         */
        if (tzload(fullname + dir->baselen, dir->tz.TZname, &dir->tz.state, TRUE) != 0) {
            /* Zone could not be loaded, ignore it */
            continue;
        }

        if (!pg_tz_acceptable(&dir->tz)) {
            /* Ignore leap-second zones */
            continue;
        }

        /* Timezone loaded OK. */
        return &dir->tz;
    }

    /* Nothing more found */
    return NULL;
}
