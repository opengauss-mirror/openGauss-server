/* -------------------------------------------------------------------------
 *
 * port.h
 *	  Header for src/port/ compatibility functions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/port.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_PORT_H
#define PG_PORT_H

#include <ctype.h>
#include <netdb.h>
#include <pwd.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include "gs_threadlocal.h"

/* socket has a different definition on WIN32 */
#ifndef WIN32
typedef int pgsocket;

#define PGINVALID_SOCKET (-1)
#else
typedef SOCKET pgsocket;

#define PGINVALID_SOCKET INVALID_SOCKET
#endif

/* non-blocking */
extern bool pg_set_noblock(pgsocket sock);
extern bool pg_set_block(pgsocket sock);

/* Portable path handling for Unix/Win32 (in path.c) */

extern bool has_drive_prefix(const char* filename);
extern char* first_dir_separator(const char* filename);
extern char* last_dir_separator(const char* filename);
extern char* first_path_var_separator(const char* pathlist);
extern void join_path_components(char* ret_path, const char* head, const char* tail);
extern void canonicalize_path(char* path);
extern void make_native_path(char* path);
extern bool path_contains_parent_reference(const char* path);
extern bool path_is_relative_and_below_cwd(const char* path);
extern bool path_is_prefix_of_path(const char* path1, const char* path2);
extern const char* get_progname(const char* argv0);
extern void get_share_path(const char* my_exec_path, char* ret_path);
extern void get_etc_path(const char* my_exec_path, char* ret_path, size_t ret_path_len);
extern void get_include_path(const char* my_exec_path, char* ret_path);
extern void get_pkginclude_path(const char* my_exec_path, char* ret_path);
extern void get_includeserver_path(const char* my_exec_path, char* ret_path);
extern void get_lib_path(const char* my_exec_path, char* ret_path);
extern void get_pkglib_path(const char* my_exec_path, char* ret_path);
extern void get_locale_path(const char* my_exec_path, char* ret_path);
extern void get_doc_path(const char* my_exec_path, char* ret_path);
extern void get_html_path(const char* my_exec_path, char* ret_path);
extern void get_man_path(const char* my_exec_path, char* ret_path);
extern bool get_home_path(char* ret_path, size_t sz);
extern void get_parent_directory(char* path);
extern char* pg_strtolower(char* str);
extern char* pg_strtoupper(char* str);
extern int mask_single_passwd(char* passwd);

/* port/dirmod.c */
extern char** pgfnames(const char* path);
extern void pgfnames_cleanup(char** filenames);

/*
 *	is_absolute_path
 *
 *	By making this a macro we avoid needing to include path.c in libpq.
 */
#ifndef WIN32
#define IS_DIR_SEP(ch) ((ch) == '/')

#define is_absolute_path(filename) (IS_DIR_SEP((filename)[0]))
#else
#define IS_DIR_SEP(ch) ((ch) == '/' || (ch) == '\\')

/* See path_is_relative_and_below_cwd() for how we handle 'E:abc'. */
#define is_absolute_path(filename) \
    (IS_DIR_SEP((filename)[0]) ||  \
        (isalpha((unsigned char)((filename)[0])) && (filename)[1] == ':' && IS_DIR_SEP((filename)[2])))
#endif

/* Portable locale initialization (in exec.c) */
extern void set_pglocale_pgservice(const char* argv0, const char* app);

/* Portable way to find binaries (in exec.c) */
extern int find_my_exec(const char* argv0, char* retpath);
extern int find_other_exec(const char* argv0, const char* target, const char* versionstr, char* retpath);

/* Windows security token manipulation (in exec.c) */
#ifdef WIN32
extern BOOL AddUserToTokenDacl(HANDLE hToken);
#endif

#if defined(WIN32) || defined(__CYGWIN__)
#define EXE ".exe"
#else
#define EXE ""
#endif

#if defined(WIN32) && !defined(__CYGWIN__)
#define DEVNULL "nul"
#else
#define DEVNULL "/dev/null"
#endif

/*
 *	Win32 needs double quotes at the beginning and end of system()
 *	strings.  If not, it gets confused with multiple quoted strings.
 *	It also requires double-quotes around the executable name and
 *	any files used for redirection.  Other args can use single-quotes.
 *
 *	Generated using Win32 "CMD /?":
 *
 *	1. If all of the following conditions are met, then quote characters
 *	on the command line are preserved:
 *
 *	 - no /S switch
 *	 - exactly two quote characters
 *	 - no special characters between the two quote characters, where special
 *	   is one of: &<>()@^|
 *	 - there are one or more whitespace characters between the two quote
 *	   characters
 *	 - the string between the two quote characters is the name of an
 *	   executable file.
 *
 *	 2. Otherwise, old behavior is to see if the first character is a quote
 *	 character and if so, strip the leading character and remove the last
 *	 quote character on the command line, preserving any text after the last
 *	 quote character.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
#define SYSTEMQUOTE "\""
#else
#define SYSTEMQUOTE ""
#endif

/* Portable delay handling */
extern void pg_usleep(long microsec);
extern void pg_usleep_retry(long microsec, int retry_times);

/* Portable SQL-like case-independent comparisons and conversions */
extern int pg_strcasecmp(const char* s1, const char* s2);
extern int pg_strncasecmp(const char* s1, const char* s2, size_t n);
extern unsigned char pg_toupper(unsigned char ch);
extern unsigned char pg_tolower(unsigned char ch);
extern unsigned char pg_ascii_toupper(unsigned char ch);
extern unsigned char pg_ascii_tolower(unsigned char ch);
#ifndef WIN32
extern bool IsMyPostmasterPid(pid_t pid, const char* data);
#endif

#if defined(WIN32)
/*
 * Versions of libintl >= 0.18? try to replace setlocale() with a macro
 * to their own versions.  Remove the macro, if it exists, because it
 * ends up calling the wrong version when the backend and libintl use
 * different versions of msvcrt.
 */
#if defined(setlocale)
#undef setlocale
#endif

/*
 * Define our own wrapper macro around setlocale() to work around bugs in
 * Windows' native setlocale() function.
 */
extern char* pgwin32_setlocale(int category, const char* locale);

#define setlocale(a, b) pgwin32_setlocale(a, b)
#endif /* WIN32 */

/* Portable prompt handling */
extern char* simple_prompt(const char* prompt, int maxlen, bool echo);

#ifdef WIN32
#define PG_SIGNAL_COUNT 32
#define kill(pid, sig) pgkill(pid, sig)
extern int pgkill(int pid, int sig);
#endif

extern int pclose_check(FILE* stream);

/* Global variable holding time zone information. */
#ifndef __CYGWIN__
#define TIMEZONE_GLOBAL timezone
#define TZNAME_GLOBAL tzname
#else
#define TIMEZONE_GLOBAL _timezone
#define TZNAME_GLOBAL _tzname
#endif

#if defined(WIN32) || defined(__CYGWIN__)
/*
 *	Win32 doesn't have reliable rename/unlink during concurrent access.
 */
extern int pgrename(const char* from, const char* to);
extern int pgunlink(const char* path);

/* Include this first so later includes don't see these defines */
#ifdef WIN32_ONLY_COMPILER
#include <io.h>
#endif

#define rename(from, to) pgrename(from, to)
#define unlink(path) pgunlink(path)
#endif /* defined(WIN32) || defined(__CYGWIN__) */

/*
 *	Win32 also doesn't have symlinks, but we can emulate them with
 *	junction points on newer Win32 versions.
 *
 *	Cygwin has its own symlinks which work on Win95/98/ME where
 *	junction points don't, so use those instead.  We have no way of
 *	knowing what type of system Cygwin binaries will be run on.
 *		Note: Some CYGWIN includes might #define WIN32.
 */
#if defined(WIN32) && !defined(__CYGWIN__)
extern int pgreadlink(const char* path, char* buf, size_t size);
extern bool pgwin32_is_junction(char* path);

#define readlink(path, buf, size) pgreadlink(path, buf, size)
#endif

extern bool rmtree(const char* path, bool rmtopdir, bool noent_ok = false);

/*
 * stat() is not guaranteed to set the st_size field on win32, so we
 * redefine it to our own implementation that is.
 *
 * We must pull in sys/stat.h here so the system header definition
 * goes in first, and we redefine that, and not the other way around.
 *
 * Some frontends don't need the size from stat, so if UNSAFE_STAT_OK
 * is defined we don't bother with this.
 */
#if defined(WIN32) && !defined(__CYGWIN__) && !defined(UNSAFE_STAT_OK)
#include <sys/stat.h>
extern int pgwin32_safestat(const char* path, struct stat* buf);

#define stat(a, b) pgwin32_safestat(a, b)
#endif

#if defined(WIN32) && !defined(__CYGWIN__)

/*
 * open() and fopen() replacements to allow deletion of open files and
 * passing of other special options.
 */
#define O_DIRECT 0x80000000
extern int pgwin32_open(const char*, int, ...);
extern FILE* pgwin32_fopen(const char*, const char*);

#ifndef FRONTEND
#define open(a, b, c) pgwin32_open(a, b, c)
#define fopen(a, b) pgwin32_fopen(a, b)
#endif

#ifndef popen
#define popen(a, b) _popen(a, b)
#endif
#ifndef pclose
#define pclose(a) _pclose(a)
#endif

/* New versions of MingW have gettimeofday, old mingw and msvc don't */
#ifndef HAVE_GETTIMEOFDAY
/* Last parameter not used */
extern int gettimeofday(struct timeval* tp, struct timezone* tzp);
#endif
#else /* !WIN32 */

/*
 *	Win32 requires a special close for sockets and pipes, while on Unix
 *	close() does them all.
 */
#define closesocket close
#endif /* WIN32 */

/*
 * Default "extern" declarations or macro substitutes for library routines.
 * When necessary, these routines are provided by files in src/port/.
 */
#ifndef HAVE_CRYPT
extern char* crypt(const char* key, const char* setting);
#endif

/* WIN32 handled in port/win32.h */
#ifndef WIN32
#define pgoff_t off_t
#ifdef __NetBSD__
extern int fseeko(FILE* stream, off_t offset, int whence);
extern off_t ftello(FILE* stream);
#endif
#endif

extern double pg_erand48(unsigned short xseed[3]);
extern long pg_lrand48(void);
extern void pg_srand48(long seed);
extern long free_list_lrand48(void);
extern void free_list_srand48(long seed);
extern void pg_reset_srand48(unsigned short xseed[3]);
extern unsigned short* pg_get_srand48();

#ifndef HAVE_FLS
extern int fls(int mask);
#endif

#ifndef HAVE_FSEEKO
#define fseeko(a, b, c) fseek(a, b, c)
#define ftello(a) ftell(a)
#endif

#ifndef HAVE_GETOPT
extern int getopt(int nargc, char* const* nargv, const char* ostr);
#endif

#if !defined(HAVE_GETPEEREID) && !defined(WIN32)
extern int getpeereid(int sock, uid_t* uid, gid_t* gid);
#endif

#ifndef HAVE_ISINF
extern int isinf(double x);
#endif

#ifndef HAVE_RINT
extern double rint(double x);
#endif

#ifndef HAVE_INET_ATON
#include <netinet/in.h>
#include <arpa/inet.h>
extern int inet_aton(const char* cp, struct in_addr* addr);
#endif

#if !HAVE_DECL_STRLCAT
extern size_t strlcat(char* dst, const char* src, size_t siz);
#endif

#if !HAVE_DECL_STRLCPY
extern size_t strlcpy(char* dst, const char* src, size_t siz);
#endif

#if !defined(HAVE_RANDOM) && !defined(__BORLANDC__)
extern long random(void);
#endif

#ifndef HAVE_UNSETENV
extern void unsetenv(const char* name);
#endif

#ifndef HAVE_SRANDOM
extern void srandom(unsigned int seed);
#endif

#define gs_random() pg_lrand48()
#define gs_srandom(seed) pg_srand48((long int)(seed))


/* thread.h */
extern char* pqStrerror(int errnum, char* strerrbuf, size_t buflen);

#if !defined(WIN32) || defined(__CYGWIN__)
extern int pqGetpwuid(uid_t uid, struct passwd* resultbuf, char* buffer, size_t buflen, struct passwd** result);
#endif

extern int pqGethostbyname(
    const char* name, struct hostent* resultbuf, char* buffer, size_t buflen, struct hostent** result, int* herrno);

extern void pg_qsort(void* base, size_t nel, size_t elsize, int (*cmp)(const void*, const void*));

#define qsort(a, b, c, d) pg_qsort(a, b, c, d)

typedef int (*qsort_arg_comparator)(const void* a, const void* b, void* arg);

extern void qsort_arg(void* base, size_t nel, size_t elsize, qsort_arg_comparator cmp, void* arg);

/* port/chklocale.c */
extern int pg_get_encoding_from_locale(const char* ctype, bool write_message);

/* port/inet_net_ntop.c */

/* port/pgcheckdir.c */
extern int pg_check_dir(const char* dir);

/* port/pgmkdirp.c */
extern int pg_mkdir_p(char* path, int omode);
extern int pg_mkdir_p_used_by_gaussdb(char* path, int omode);

typedef struct OptParseContext {
    char* place;
    int opterr;
    int optind;
    int optopt;
    char* optarg;
} OptParseContext;

extern void initOptParseContext(OptParseContext* pOptCtxt);
extern int getopt_r(int nargc, char* const* nargv, const char* ostr, OptParseContext* pOptArgs);

/* port/gs_system.c */
extern int gs_system(const char* command);

/* port/gs_system_security */
extern int gs_system_security(const char* command);
extern FILE* popen_security(const char* command, char type);
extern int pclose_security(FILE* fp);
extern int gs_popen_security(const char* command);

/* port/gs_strerror.c */

extern THR_LOCAL char gs_error_buf[];
extern char* gs_strerror(int errnum);

/* port/gs_readdir.c */

extern THR_LOCAL char gs_dir_buf[];
extern struct dirent* gs_readdir(DIR* dir);

/*env thread safe version*/
extern int gs_putenv_r(char* envvar);
extern char* gs_getenv_r(const char* name);

#endif /* PG_PORT_H */
