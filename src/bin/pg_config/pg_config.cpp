/* -------------------------------------------------------------------------
 *
 * pg_config.c
 *
 * This program reports various pieces of information about the
 * installed version of PostgreSQL.  Packages that interface to
 * PostgreSQL can use it to configure their build.
 *
 * This is a C implementation of the previous shell script written by
 * Peter Eisentraut <peter_e@gmx.net>, with adjustments made to
 * accommodate the possibility that the installation has been relocated from
 * the place originally configured.
 *
 * author of C translation: Andrew Dunstan	   mailto:andrew@dunslane.net
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * src/bin/pg_config/pg_config.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char* progname;
static char* homepath;

/*
 * For each piece of information known to pg_config, we define a subroutine
 * to print it.  This is probably overkill, but it avoids code duplication
 * and accidentally omitting items from the "all" display.
 */
static void show_bindir(int alls)
{
    if (alls) {
        printf("BINDIR = ");
    }

    /* assume we are located in the bindir */
    printf("%s/bin\n", homepath);
}

static void show_docdir(int alls)
{
    if (alls) {
        printf("DOCDIR = ");
    }

    printf("%s/share/doc/postgresql\n", homepath);
}

static void show_htmldir(int alls)
{
    if (alls) {
        printf("HTMLDIR = ");
    }

    printf("%s/share/doc/postgresql\n", homepath);
}

static void show_includedir(int alls)
{
    if (alls) {
        printf("INCLUDEDIR = ");
    }

    printf("%s/include\n", homepath);
}

static void show_pkgincludedir(int alls)
{
    if (alls) {
        printf("PKGINCLUDEDIR = ");
    }

    printf("%s/include/postgresql\n", homepath);
}

static void show_includedir_server(int alls)
{
    if (alls) {
        printf("INCLUDEDIR-SERVER = ");
    }

    printf("%s/include/postgresql/server\n", homepath);
}

static void show_libdir(int alls)
{
    if (alls) {
        printf("LIBDIR = ");
    }

    printf("%s/lib\n", homepath);
}

static void show_pkglibdir(int alls)
{
    if (alls) {
        printf("PKGLIBDIR = ");
    }

    printf("%s/lib/postgresql\n", homepath);
}

static void show_localedir(int alls)
{
    if (alls) {
        printf("LOCALEDIR = ");
    }

    printf("%s/share/locale\n", homepath);
}

static void show_mandir(int alls)
{
    if (alls) {
        printf("MANDIR = ");
    }

    printf("%s/share/man\n", homepath);
}

static void show_sharedir(int alls)
{
    if (alls) {
        printf("SHAREDIR = ");
    }

    printf("%s/share/postgresql\n", homepath);
}

static void show_sysconfdir(int alls)
{
    if (alls) {
        printf("SYSCONFDIR = ");
    }

    printf("%s/etc/postgresql\n", homepath);
}

static void show_pgxs(int alls)
{
    if (alls) {
        printf("PGXS = ");
    }

    printf("%s/lib/postgresql/pgxs/src/makefiles/pgxs.mk\n", homepath);
}

static void show_configure(int alls)
{
#ifdef VAL_CONFIGURE
    if (alls) {
        printf("CONFIGURE = ");
    }
    printf("%s\n", VAL_CONFIGURE);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_cc(int alls)
{
#ifdef VAL_CC
    if (alls) {
        printf("CC = ");
    }
    printf("g++\n");
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_cppflags(int alls)
{
#ifdef VAL_CPPFLAGS
    if (alls) {
        printf("CPPFLAGS = ");
    }
    printf("-D_GNU_SOURCE -I%s/include\n", homepath);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_cflags(int alls)
{
#ifdef VAL_CFLAGS
    if (alls) {
        printf("CFLAGS = ");
    }
    printf("%s\n", VAL_CFLAGS);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_cflags_sl(int alls)
{
#ifdef VAL_CFLAGS_SL
    if (alls) {
        printf("CFLAGS_SL = ");
    }
    printf("%s\n", VAL_CFLAGS_SL);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_ldflags(int alls)
{
#ifdef VAL_LDFLAGS
    if (alls) {
        printf("LDFLAGS = ");
    }
    printf("%s\n", VAL_LDFLAGS);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_ldflags_ex(int alls)
{
#ifdef VAL_LDFLAGS_EX
    if (alls) {
        printf("LDFLAGS_EX = ");
    }
    printf("%s\n", VAL_LDFLAGS_EX);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_ldflags_sl(int alls)
{
#ifdef VAL_LDFLAGS_SL
    if (alls) {
        printf("LDFLAGS_SL = ");
    }
    printf("%s\n", VAL_LDFLAGS_SL);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_libs(int alls)
{
#ifdef VAL_LIBS
    if (alls) {
        printf("LIBS = ");
    }
    printf("%s\n", VAL_LIBS);
#else
    if (!alls) {
        fprintf(stderr, ("not recorded\n"));
        exit(1);
    }
#endif
}

static void show_version(int alls)
{
    if (alls) {
        printf("VERSION = ");
    }
    printf("PostgreSQL 9.2.4\n");
#ifndef ENABLE_MULTIPLE_NODES
    if (alls) {
	printf("OPENGAUSS_VERSION = ");
    }
    printf("openGauss 6.0.0-RC1\n");
#endif
}

/*
 * Table of known information items
 *
 * Be careful to keep this in sync with the help() display.
 */
typedef struct {
    const char* switchname;
    void (*show_func)(int alls);
} InfoItem;

static const InfoItem info_items[] = {{"--bindir", show_bindir},
    {"--docdir", show_docdir},
    {"--htmldir", show_htmldir},
    {"--includedir", show_includedir},
    {"--pkgincludedir", show_pkgincludedir},
    {"--includedir-server", show_includedir_server},
    {"--libdir", show_libdir},
    {"--pkglibdir", show_pkglibdir},
    {"--localedir", show_localedir},
    {"--mandir", show_mandir},
    {"--sharedir", show_sharedir},
    {"--sysconfdir", show_sysconfdir},
    {"--pgxs", show_pgxs},
    {"--configure", show_configure},
    {"--cc", show_cc},
    {"--cppflags", show_cppflags},
    {"--cflags", show_cflags},
    {"--cflags_sl", show_cflags_sl},
    {"--ldflags", show_ldflags},
    {"--ldflags_ex", show_ldflags_ex},
    {"--ldflags_sl", show_ldflags_sl},
    {"--libs", show_libs},
    {"--version", show_version},
    {NULL, NULL}};

static void help(void)
{
    printf(("\n%s provides information about the installed version of openGauss.\n\n"), progname);
    printf(("Usage:\n"));
    printf(("  %s [OPTION]...\n\n"), progname);
    printf(("Options:\n"));
    printf(("  --bindir              show location of user executables\n"));
    printf(("  --docdir              show location of documentation files\n"));
    printf(("  --htmldir             show location of HTML documentation files\n"));
    printf(("  --includedir          show location of C header files of the client\n"
            "                        interfaces\n"));
    printf(("  --pkgincludedir       show location of other C header files\n"));
    printf(("  --includedir-server   show location of C header files for the server\n"));
    printf(("  --libdir              show location of object code libraries\n"));
    printf(("  --pkglibdir           show location of dynamically loadable modules\n"));
    printf(("  --localedir           show location of locale support files\n"));
    printf(("  --mandir              show location of manual pages\n"));
    printf(("  --sharedir            show location of architecture-independent support files\n"));
    printf(("  --sysconfdir          show location of system-wide configuration files\n"));
    printf(("  --pgxs                show location of extension makefile\n"));
    printf(("  --configure           show options given to \"configure\" script when\n"
            "                        openGauss was built\n"));
    printf(("  --cc                  show CC value used when openGauss was built\n"));
    printf(("  --cppflags            show CPPFLAGS value used when openGauss was built\n"));
    printf(("  --cflags              show CFLAGS value used when openGauss was built\n"));
    printf(("  --cflags_sl           show CFLAGS_SL value used when openGauss was built\n"));
    printf(("  --ldflags             show LDFLAGS value used when openGauss was built\n"));
    printf(("  --ldflags_ex          show LDFLAGS_EX value used when openGauss was built\n"));
    printf(("  --ldflags_sl          show LDFLAGS_SL value used when openGauss was built\n"));
    printf(("  --libs                show LIBS value used when openGauss was built\n"));
    printf(("  --version             show the openGauss version\n"));
    printf(("  -?, --help            show this help, then exit\n"));
    printf(("\nWith no arguments, all known items are shown.\n\n"));

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf("\nReport bugs to GaussDB support.\n");
#else
    printf("\nReport bugs to community@opengauss.org> or join opengauss community <https://opengauss.org>.\n");
#endif
}

static void show_all(void)
{
    int i;
    int alls = 1;

    for (i = 0; info_items[i].switchname != NULL; i++) {
        (*info_items[i].show_func)(alls);
    }
}

int main(int argc, char** argv)
{
    int i;
    int j;
    int alls = 0;

    /* Work directory is derived from $GAUSSHOME */
    homepath = getenv("GAUSSHOME");

    if (homepath == NULL) {
        fprintf(stderr, "system variable $GAUSSHOME is not specified\n");
        exit(0);
    }
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_config"));
    progname = get_progname(argv[0]);
    /* check for --help */
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0) {
            help();
            exit(0);
        }
    }

    /* no arguments -> print everything */
    if (argc < 2) {
        show_all();
        exit(0);
    }

    for (i = 1; i < argc; i++) {
        for (j = 0; info_items[j].switchname != NULL; j++) {
            if (strcmp(argv[i], info_items[j].switchname) == 0) {
                (*info_items[j].show_func)(alls);
                break;
            }
        }
        if (info_items[j].switchname == NULL) {
            fprintf(stderr, ("%s: invalid argument: %s\n"), progname, argv[i]);
            exit(1);
        }
    }

    return 0;
}
