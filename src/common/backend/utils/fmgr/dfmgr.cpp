/* -------------------------------------------------------------------------
 *
 * dfmgr.c
 *	  Dynamic function manager code.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/fmgr/dfmgr.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <sys/stat.h>

#ifndef WIN32_ONLY_COMPILER
#include "dynloader.h"
#else
#include "port/dynloader/win32.h"
#endif
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/dynamic_loader.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/syscall_lock.h"

/* Max size of error message of dlopen   */
#define DLERROR_MSG_MAX_LEN 512
/* signatures for openGauss library init/fini functions */
typedef void (*PG_init_t)(void);
typedef void (*PG_fini_t)(void);

/* hashtable entry for rendezvous variables */
typedef struct {
    char varName[NAMEDATALEN]; /* hash key (must be first) */
    void* varValue;
} rendezvousHashEntry;

DynamicFileList* file_list = NULL;
DynamicFileList* file_tail = NULL;

/* stat() call under Win32 returns an st_ino field, but it has no meaning */
#ifndef WIN32
#define SAME_INODE(A, B) ((A).st_ino == (B).inode && (A).st_dev == (B).device)
#else
#define SAME_INODE(A, B) false
#endif

void* internal_load_library(const char* libname);
static void incompatible_module_error(const char* libname, const Pg_magic_struct* module_magic_data);
static void internal_unload_library(const char* libname);
static void check_restricted_library_name(const char* name);
static char* substitute_libpath_macro(const char* name);
static char* find_in_dynamic_libpath(const char* basename);

/* Magic structure that module needs to match to be accepted */
static const Pg_magic_struct magic_data = PG_MODULE_MAGIC_DATA;

void check_external_function(const char* filepath, const char* filename, const char* funcname)
{
    if (!file_exists(filepath))
        return;

    check_backend_env(filepath);

    /* Load the shared library, unless we already did */
    void* lib_handle = internal_load_library(filepath);

    /* Look up the function within the library */
    PGFunction retval = (PGFunction)pg_dlsym(lib_handle, funcname);

    if (retval == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("could not find function \"%s\" in file \"%s\"", funcname, filename)));
    }
}

/*
 * Load the specified dynamic-link library file, and look for a function
 * named funcname in it.
 *
 * If the function is not found, we raise an error if signalNotFound is true,
 * else return (PGFunction) NULL.  Note that errors in loading the library
 * will provoke ereport() regardless of signalNotFound.
 *
 * If filehandle is not NULL, then *filehandle will be set to a handle
 * identifying the library file.  The filehandle can be used with
 * lookup_external_function to lookup additional functions in the same file
 * at less cost than repeating load_external_function.
 */
CFunInfo load_external_function(const char* filename, char* funcname, bool signalNotFound, bool isValidate)
{
    char* fullname = NULL;
    void* lib_handle = NULL;
    PGFunction retval;
    const Pg_finfo_record* record = NULL;
    CFunInfo funInfo;

    /* Expand the possibly-abbreviated filename to an exact path name */
    fullname = expand_dynamic_library_name(filename);
    check_backend_env(fullname);

    /* Load the shared library, unless we already did */
    lib_handle = internal_load_library(fullname);

    /* Look up the function within the library */
    retval = (PGFunction)pg_dlsym(lib_handle, funcname);

    if (retval == NULL && signalNotFound) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("could not find function \"%s\" in file \"%s\"", funcname, fullname)));
    }

    record = fetch_finfo_record(lib_handle, funcname, isValidate);

    funInfo.user_fn = retval;
    funInfo.inforec = record;
    pfree_ext(fullname);

    return funInfo;
}

/*
 * This function loads a shlib file without looking up any particular
 * function in it.	If the same shlib has previously been loaded,
 * unload and reload it.
 *
 * When 'restricted' is true, only libraries in the presumed-secure
 * directory $libdir/plugins may be referenced.
 */
void load_file(const char* filename, bool restricted)
{
    char* fullname = NULL;

    /* Apply security restriction if requested */
    if (restricted)
        check_restricted_library_name(filename);

    /* Expand the possibly-abbreviated filename to an exact path name */
    fullname = expand_dynamic_library_name(filename);

    /* Unload the library if currently loaded */
    internal_unload_library(fullname);

    AutoMutexLock libraryLock(&dlerror_lock);
    libraryLock.lock();
    /* Load the shared library */
    (void)internal_load_library(fullname);
    libraryLock.unLock();

    pfree_ext(fullname);
}

/*
 * Lookup a function whose library file is already loaded.
 * Return (PGFunction) NULL if not found.
 */
PGFunction lookup_external_function(void* filehandle, const char* funcname)
{
    return (PGFunction)pg_dlsym(filehandle, funcname);
}

/*
 * Load the specified dynamic-link library file, unless it already is
 * loaded.	Return the pg_dl* handle for the file.
 *
 * Note: libname is expected to be an exact name for the library file.
 */
void* internal_load_library(const char* libname)
{
    Assert(libname != NULL);

    DynamicFileList* file_scanner = NULL;
    FileListInit* file_init_scanner = NULL;
    PGModuleMagicFunction magic_func;
    void (*set_extension_index)(uint32);
    struct stat stat_buf;
    PG_init_t PG_init = NULL;
    char* file = last_dir_separator(libname);
    file = (file == NULL) ? ((char*)libname) : (file + 1);

    /*
     * Scan the list of loaded FILES to see if the file has been loaded.
     */
    for (file_scanner = file_list; file_scanner != NULL && strcmp(libname, file_scanner->filename) != 0;
         file_scanner = file_scanner->next)
        ;

    if (file_scanner == NULL) {
        /*
         * Check for same files - different paths (ie, symlink or link)
         */
        if (stat(libname, &stat_buf) == -1) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not access file \"%s\": %m", file)));
        }

        for (file_scanner = file_list; file_scanner != NULL && !SAME_INODE(stat_buf, *file_scanner);
             file_scanner = file_scanner->next)
            ;
    }

    if (file_scanner == NULL) {
        size_t tmplen = 0;
        errno_t rc = EOK;

        /*
         * File not loaded yet.
         */
        tmplen = offsetof(DynamicFileList, filename) + strlen(libname) + 1;

        file_scanner = (DynamicFileList*)MemoryContextAlloc(
            INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), tmplen);

        if (file_scanner == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }

        rc = memset_s(file_scanner, tmplen, 0, sizeof(DynamicFileList));
        securec_check(rc, "\0", "\0");
        rc = strncpy_s(file_scanner->filename, strlen(libname) + 1, libname, strlen(libname));
        securec_check_c(rc, "\0", "\0");
        file_scanner->device = stat_buf.st_dev;
#ifndef WIN32
        file_scanner->inode = stat_buf.st_ino;
#endif
        file_scanner->next = NULL;

        /*
         * Call pg_dlopen.
         */
#if ((defined ENABLE_PYTHON2) || (defined ENABLE_PYTHON3))
#ifdef ENABLE_PYTHON2
    #define PYTHON_LIB_NAME "libpython2.7.so"
#endif

#ifdef ENABLE_PYTHON3
    #define PYTHON_LIB_NAME "libpython3.so"
#endif
        /*
         * In C++, when you try to open a shared library use dlopen with flag RTLD_NOW,
         * it will search dependent shared library of main program for the undefined symbol.
         */
        if (strstr(file_scanner->filename, "plpython") != NULL) {
            /*
             * dlopen will find *.so in LD_LIBRARY_PATH, /etc/ld.so.cache, /lib, /usr/lib ..
             * And we must set the Flag to "RTLD_NOW | RTLD_GLOBAL"
             */
            if (dlopen(PYTHON_LIB_NAME, RTLD_NOW | RTLD_GLOBAL) == NULL) {
                pfree((char*)file_scanner);
                file_scanner = NULL;
                ereport(ERROR,
                    (errcode_for_file_access(),
                        errmsg("could not load library \"%s\", get error report failed", PYTHON_LIB_NAME)));
            }
        }
#endif
        file_scanner->handle = pg_dlopen(file_scanner->filename);

        if (file_scanner->handle == NULL) {
            char* load_error = NULL;
            char error[DLERROR_MSG_MAX_LEN + 1];
            load_error = (char*)pg_dlerror();
            if (load_error == NULL) {
                pfree((char*)file_scanner);
                file_scanner = NULL;
                ereport(ERROR,
                    (errcode_for_file_access(),
                        errmsg("could not load library \"%s\", get error report failed", file)));
            }
            rc = strncpy_s(error, DLERROR_MSG_MAX_LEN + 1, load_error, strlen(load_error));
            securec_check(rc, "\0", "\0");
            pfree((char*)file_scanner);
            file_scanner = NULL;
            /* errcode_for_file_access might not be appropriate here? */
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not load library \"%s\": %s", file, error)));
        }

        /* Check the magic function to determine compatibility */
        magic_func = (PGModuleMagicFunction)pg_dlsym(file_scanner->handle, PG_MAGIC_FUNCTION_NAME_STRING);

        if (magic_func) {
            const Pg_magic_struct* magic_data_ptr = (*magic_func)();

            if (magic_data_ptr->len != magic_data.len || memcmp(magic_data_ptr, &magic_data, magic_data.len) != 0) {
                /* copy data block before unlinking library */
                Pg_magic_struct module_magic_data = *magic_data_ptr;

                /* try to unlink library */
                (void)pg_dlclose(file_scanner->handle);
                pfree((char*)file_scanner);
                file_scanner = NULL;

                /* issue suitable complaint */
                incompatible_module_error(libname, &module_magic_data);
            }
        } else {
            /* try to unlink library */
            (void)pg_dlclose(file_scanner->handle);
            pfree((char*)file_scanner);
            file_scanner = NULL;

            /* complain */
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("incompatible library \"%s\": missing magic block", file),
                    errhint("Extension libraries are required to use the PG_MODULE_MAGIC macro.")));
        }

        /* Look up the set_extension_index function within extension's so */
        set_extension_index = (void(*)(uint32))pg_dlsym(file_scanner->handle, "set_extension_index");
        if (set_extension_index) {
            uint32 idx = pg_atomic_fetch_add_u32(&g_instance.extensionNum, 1);
            (*set_extension_index)(idx);
        }

        /* call init_session_vars after set_extension_index */
        if (IsUnderPostmaster) {
            void (*init_session_vars)(void) = (void(*)(void))pg_dlsym(file_scanner->handle, "init_session_vars");
            if (init_session_vars) {
                (*init_session_vars)();
            }
        }

        /* OK to link it into list */
        if (file_list == NULL)
            file_list = file_scanner;
        else
            file_tail->next = file_scanner;

        file_tail = file_scanner;
    }

    /*
     * check if the file_scanner has been registered
     */
    for (file_init_scanner = u_sess->fmgr_cxt.file_list_init;
         file_init_scanner != NULL && file_init_scanner->file_list != file_scanner;
         file_init_scanner = file_init_scanner->next) {
    }

    /* If file_scanner doesn't exist, register it and call __PG_init() */
    if (file_init_scanner == NULL) {
        file_init_scanner = (FileListInit*)MemoryContextAllocZero(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), sizeof(FileListInit));
        file_init_scanner->file_list = file_scanner;
        file_init_scanner->next = NULL;

        /*
         * If the library has a _PG_init() function, call it.
         */
        PG_init = (PG_init_t)pg_dlsym(file_scanner->handle, "_PG_init");

        if (PG_init != NULL)
            (*PG_init)();

        if (u_sess->fmgr_cxt.file_list_init == NULL)
            u_sess->fmgr_cxt.file_list_init = file_init_scanner;
        else
            u_sess->fmgr_cxt.file_init_tail->next = file_init_scanner;
        u_sess->fmgr_cxt.file_init_tail = file_init_scanner;
    }

    return file_scanner->handle;
}

/*
 * Report a suitable error for an incompatible magic block.
 */
static void incompatible_module_error(const char* libname, const Pg_magic_struct* module_magic_data)
{
    StringInfoData details;

    /*
     * If the version doesn't match, just report that, because the rest of the
     * block might not even have the fields we expect.
     */
    if (magic_data.version != module_magic_data->version)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("incompatible library \"%s\": version mismatch", libname),
                errdetail("Server is version %d.%d, library is version %d.%d.",
                    magic_data.version / 100,
                    magic_data.version % 100,
                    module_magic_data->version / 100,
                    module_magic_data->version % 100)));

    /*
     * Otherwise, spell out which fields don't agree.
     *
     * XXX this code has to be adjusted any time the set of fields in a magic
     * block change!
     */
    initStringInfo(&details);

    if (module_magic_data->funcmaxargs != magic_data.funcmaxargs) {
        if (details.len)
            appendStringInfoChar(&details, '\n');

        appendStringInfo(&details,
            _("Server has FUNC_MAX_ARGS = %d, library has %d."),
            magic_data.funcmaxargs,
            module_magic_data->funcmaxargs);
    }

    if (module_magic_data->indexmaxkeys != magic_data.indexmaxkeys) {
        if (details.len)
            appendStringInfoChar(&details, '\n');

        appendStringInfo(&details,
            _("Server has INDEX_MAX_KEYS = %d, library has %d."),
            magic_data.indexmaxkeys,
            module_magic_data->indexmaxkeys);
    }

    if (module_magic_data->namedatalen != magic_data.namedatalen) {
        if (details.len)
            appendStringInfoChar(&details, '\n');

        appendStringInfo(&details,
            _("Server has NAMEDATALEN = %d, library has %d."),
            magic_data.namedatalen,
            module_magic_data->namedatalen);
    }

    if (module_magic_data->float4byval != magic_data.float4byval) {
        if (details.len)
            appendStringInfoChar(&details, '\n');

        appendStringInfo(&details,
            _("Server has FLOAT4PASSBYVAL = %s, library has %s."),
            magic_data.float4byval ? "true" : "false",
            module_magic_data->float4byval ? "true" : "false");
    }

    if (module_magic_data->float8byval != magic_data.float8byval) {
        if (details.len)
            appendStringInfoChar(&details, '\n');

        appendStringInfo(&details,
            _("Server has FLOAT8PASSBYVAL = %s, library has %s."),
            magic_data.float8byval ? "true" : "false",
            module_magic_data->float8byval ? "true" : "false");
    }

    if (details.len == 0)
        appendStringInfo(&details, _("Magic block has unexpected length or padding difference."));

    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("incompatible library \"%s\": magic block mismatch", libname),
            errdetail_internal("%s", details.data)));
}

/*
 * Unload the specified dynamic-link library file, if it is loaded.
 *
 * Note: libname is expected to be an exact name for the library file.
 *
 * XXX for the moment, this is disabled, resulting in LOAD of an already-loaded
 * library always being a no-op.  We might re-enable it someday if we can
 * convince ourselves we have safe protocols for un-hooking from hook function
 * pointers, releasing custom GUC variables, and perhaps other things that
 * are definitely unsafe currently.
 */
static void internal_unload_library(const char* libname)
{
#ifdef NOT_USED
    DynamicFileList *file_scanner = NULL, *prv = NULL, *nxt = NULL;
    struct stat stat_buf;
    PG_fini_t PG_fini = NULL;

    /*
     * We need to do stat() in order to determine whether this is the same
     * file as a previously loaded file; it's also handy so as to give a good
     * error message if bogus file name given.
     */
    if (stat(libname, &stat_buf) == -1) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not access file \"%s\": %m", libname)));
    }

    /*
     * We have to zap all entries in the list that match on either filename or
     * inode, else internal_load_library() will still think it's present.
     */

    AutoMutexLock libraryLock(&dlerror_lock);

    libraryLock.lock();

    for (file_scanner = file_list; file_scanner != NULL; file_scanner = nxt) {
        nxt = file_scanner->next;

        if (strcmp(libname, file_scanner->filename) == 0 || SAME_INODE(stat_buf, *file_scanner)) {
            if (prv != NULL) {
                prv->next = nxt;
            } else {
                file_list = nxt;
            }

            /*
             * If the library has a _PG_fini() function, call it.
             */
            PG_fini = (PG_fini_t)pg_dlsym(file_scanner->handle, "_PG_fini");

            if (PG_fini != NULL) {
                (*PG_fini)();
            }

            clear_external_function_hash(file_scanner->handle);
            pg_dlclose(file_scanner->handle);
            pfree_ext((char*)file_scanner);
            /* prv does not change */
        } else {
            prv = file_scanner;
        }
    }

    libraryLock.unLock();

#endif /* NOT_USED */
}

bool file_exists(const char* name)
{
    struct stat st;

    AssertArg(name != NULL);

    if (stat(name, &st) == 0) {
        return S_ISDIR(st.st_mode) ? false : true;
    } else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES)) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not access file \"%s\": %m", name)));
    }

    return false;
}

/* Example format: ".so" */
#ifndef DLSUFFIX
#error "DLSUFFIX must be defined to compile this file."
#endif

/*
 * If name contains a slash, check if the file exists, if so return
 * the name.  Else (no slash) try to expand using search path (see
 * find_in_dynamic_libpath below); if that works, return the fully
 * expanded file name.	If the previous failed, append DLSUFFIX and
 * try again.  If all fails, just return the original name.
 *
 * The result will always be freshly palloc'd.
 */
char* expand_dynamic_library_name(const char* name)
{
    bool have_slash = false;
    char* newm = NULL;
    char* full = NULL;

    AssertArg(name);

    have_slash = (first_dir_separator(name) != NULL);

    if (!have_slash) {
        full = find_in_dynamic_libpath(name);

        if (full != NULL)
            return full;
    } else {
        full = substitute_libpath_macro(name);

        if (file_exists(full)) {
            return full;
        }

        pfree_ext(full);
    }

    newm = (char*)palloc(strlen(name) + strlen(DLSUFFIX) + 1);
    errno_t rc = strcpy_s(newm, strlen(name) + strlen(DLSUFFIX) + 1, name);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(newm, strlen(name) + strlen(DLSUFFIX) + 1, DLSUFFIX);
    securec_check(rc, "\0", "\0");

    if (!have_slash) {
        full = find_in_dynamic_libpath(newm);
        pfree_ext(newm);

        if (full != NULL) {
            return full;
        }
    } else {
        full = substitute_libpath_macro(newm);
        pfree_ext(newm);

        if (file_exists(full)) {
            return full;
        }

        pfree_ext(full);
    }

    /*
     * If we can't find the file, just return the string as-is. The ensuing
     * load attempt will fail and report a suitable message.
     */
    return pstrdup(name);
}

/*
 * Check a restricted library name.  It must begin with "$libdir/plugins/"
 * and there must not be any directory separators after that (this is
 * sufficient to prevent ".." style attacks).
 */
static void check_restricted_library_name(const char* name)
{
    if (strncmp(name, "$libdir/plugins/", 16) != 0 || first_dir_separator(name + 16) != NULL)
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("access to library \"%s\" is not allowed", name)));
}

/*
 * Substitute for any macros appearing in the given string.
 * Result is always freshly palloc'd.
 */
static char* substitute_libpath_macro(const char* name)
{
    const char* sep_ptr = NULL;
    char* ret = NULL;
    errno_t rc = EOK;

    AssertArg(name != NULL);

    /* Currently, we only recognize $libdir at the start of the string */
    if (name[0] != '$')
        return pstrdup(name);

    if ((sep_ptr = first_dir_separator(name)) == NULL)
        sep_ptr = name + strlen(name);

    if (strlen("$libdir") != (size_t)(sep_ptr - name) || strncmp(name, "$libdir", strlen("$libdir")) != 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid macro name in dynamic library path: %s", name)));

    ret = (char*)palloc(strlen(t_thrd.proc_cxt.pkglib_path) + strlen(sep_ptr) + 1);

    rc = strcpy_s(ret, strlen(t_thrd.proc_cxt.pkglib_path) + strlen(sep_ptr) + 1, t_thrd.proc_cxt.pkglib_path);
    securec_check(rc, "\0", "\0");
    rc = strcat_s(ret, strlen(t_thrd.proc_cxt.pkglib_path) + strlen(sep_ptr) + 1, sep_ptr);
    securec_check(rc, "\0", "\0");

    return ret;
}

/*
 * Search for a file called 'basename' in the colon-separated search
 * path u_sess->attr.attr_common.Dynamic_library_path.  If the file is found, the full file name
 * is returned in freshly palloc'd memory.  If the file is not found,
 * return NULL.
 */
static char* find_in_dynamic_libpath(const char* basename)
{
    const char* p = NULL;
    size_t baselen;

    AssertArg(basename != NULL);
    AssertArg(first_dir_separator(basename) == NULL);
    AssertState(u_sess->attr.attr_common.Dynamic_library_path != NULL);

    p = u_sess->attr.attr_common.Dynamic_library_path;

    if (strlen(p) == 0)
        return NULL;

    baselen = strlen(basename);

    for (;;) {
        size_t len;
        char* piece = NULL;
        char* mangled = NULL;
        char* full = NULL;

        piece = first_path_var_separator(p);

        if (piece == p)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME), errmsg("zero-length component in parameter \"dynamic_library_path\"")));

        if (piece == NULL)
            len = strlen(p);
        else
            len = piece - p;

        piece = (char*)palloc(len + 1);
        strlcpy(piece, p, len + 1);

        mangled = substitute_libpath_macro(piece);
        pfree_ext(piece);

        canonicalize_path(mangled);

        /* only absolute paths */
        if (!is_absolute_path(mangled))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_NAME),
                    errmsg("component in parameter \"dynamic_library_path\" is not an absolute path")));

        const size_t fulllen = strlen(mangled) + 1 + baselen + 1;
        full = (char*)palloc(fulllen);
        errno_t rc = sprintf_s(full, fulllen, "%s/%s", mangled, basename);
        securec_check_ss(rc, "", "");
        pfree_ext(mangled);

        elog(DEBUG3, "find_in_dynamic_libpath: trying \"%s\"", full);

        if (file_exists(full)) {
            return full;
        }

        pfree_ext(full);

        if (p[len] == '\0') {
            break;
        } else {
            p += len + 1;
        }
    }

    return NULL;
}

/*
 * Find (or create) a rendezvous variable that one dynamically
 * loaded library can use to meet up with another.
 *
 * On the first call of this function for a particular varName,
 * a "rendezvous variable" is created with the given name.
 * The value of the variable is a void pointer (initially set to NULL).
 * Subsequent calls with the same varName just return the address of
 * the existing variable.  Once created, a rendezvous variable lasts
 * for the life of the process.
 *
 * Dynamically loaded libraries can use rendezvous variables
 * to find each other and share information: they just need to agree
 * on the variable name and the data it will point to.
 */
void** find_rendezvous_variable(const char* varName)
{
    rendezvousHashEntry* hentry = NULL;
    bool found = false;

    /* Create a hashtable if we haven't already done so in this process */
    if (u_sess->plsql_cxt.rendezvousHash == NULL) {
        HASHCTL ctl;

        errno_t ret = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(ret, "\0", "\0");
        ctl.keysize = NAMEDATALEN;
        ctl.entrysize = sizeof(rendezvousHashEntry);
        ctl.hcxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
        u_sess->plsql_cxt.rendezvousHash = hash_create("Rendezvous variable hash", 16, &ctl, HASH_ELEM | HASH_CONTEXT);
    }

    /* Find or create the hashtable entry for this varName */
    hentry = (rendezvousHashEntry*)hash_search(u_sess->plsql_cxt.rendezvousHash, varName, HASH_ENTER, &found);

    /* Initialize to NULL if first time */
    if (!found)
        hentry->varValue = NULL;

    return &hentry->varValue;
}
