/* -------------------------------------------------------------------------
 *
 * ts_utils.c
 *		various support functions
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/tsearch/ts_utils.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>

#include "gaussdb_version.h"
#include "miscadmin.h"
#include "access/transam.h"
#include "catalog/pg_ts_template.h"
#include "commands/defrem.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"

#define FILEPATH_PREFIX_LOCAL "file://"
#define FILEPATH_PREFIX_OBS "obs://"

#define SENDTOOTHERNODE 1
#define SENDTOBACKUP 2

extern char* get_transfer_path();
extern bool copy_library_file(const char* sourceFile, const char* targetFile);
extern void check_file_path(char* absolutePath);
extern char* trim(char* src);
extern char* get_obsfile_local(char* pathname, const char* basename, const char* extension);

/*
 * @Description: Get temporary dictionary file names for copy to remote node.
 * @in isExecCN: if sql is from user directly
 * @out: temporary dictionary file name prefix
 */
char* get_tsfile_prefix_tmp(bool isExecCN)
{
    StringInfoData strinfo;
    char tmpSharepath[MAXPGPATH];
    initStringInfo(&strinfo);
    get_share_path(my_exec_path, tmpSharepath);

    appendStringInfo(&strinfo,
        "%s/tsearch_data/%ld%lu",
        tmpSharepath,
        GetCurrentTransactionStartTimestamp(),
        (isExecCN ? GetCurrentTransactionId() : t_thrd.xact_cxt.cn_xid));

    /* We need to check for my_exec_path */
    check_backend_env(strinfo.data);

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Get tsfile temp prefix: %s", strinfo.data)));

    return strinfo.data;
}

/*
 * @Description: Expand internal dictionary file prefix.
 * @in dictprefix: internal dictionary file name prefix.
 * @out: internal dictionary file prefix start with absolute path.
 */
char* expand_tsfile_prefix_internal(const char* dictprefix)
{
    StringInfoData strinfo;
    char sharepath[MAXPGPATH];

    initStringInfo(&strinfo);
    get_share_path(my_exec_path, sharepath);
    appendStringInfo(&strinfo, "%s/tsearch_data/%s", sharepath, dictprefix);

    if (strinfo.len > MAXPGPATH - 1) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of internal dictionary file is too long")));
    }
    /* We need to check for my_exec_path */
    check_backend_env(strinfo.data);

    ereport(DEBUG2,
        (errmodule(MOD_TS), errmsg("Expand tsfile internal prefix \"%s\" to \"%s\"", dictprefix, strinfo.data)));

    return strinfo.data;
}

/*
 * @Description: Get the absolute_path of dictionary file for create/alter.
 * @in pathname: user-defined path of dictionary file start with
 *               prefix "file://" or "obs://".
 * @in basename: user-defined dictionary file name.
 * @in extension: user-defined dictionary file extension.
 * @out: a palloc'd string of absolute file path without ending '/'.
 */
char* get_tsfile_absolute_path(char* pathname, const char* basename, const char* extension)
{
    char* pname = NULL;
    int local_plen = strlen(FILEPATH_PREFIX_LOCAL);
    int obs_plen = strlen(FILEPATH_PREFIX_OBS);

    if (strncmp(pathname, FILEPATH_PREFIX_LOCAL, local_plen) == 0) {
        /* Skip prefix and trim */
        pname = trim(pathname + local_plen);
        if (strlen(pname) == 0 || pname[0] != '/') {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("FilePath must set an absolute path followed by \"file://\".")));
        }

        /* Make a copy and check user-defined path */
        pname = pstrdup(pname);
        check_file_path(pname);
    } else if (strncmp(pathname, FILEPATH_PREFIX_OBS, obs_plen) == 0) {
        /* Here, isSecurityMode is just used to judge whether we are on DWS. */
        if (!isSecurityMode) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("obs path can be accepted only in security mode.")));
        }
        /* Skip prefix and trim */
        pname = trim(pathname + obs_plen);
        if (strlen(pname) == 0) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("FilePath must set an obs path info followed by \"obs://\".")));
        }

        /* Download obs file and get the tmp path */
        pname = get_obsfile_local(pname, basename, extension);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("FilePath must start with \"file://\" or \"obs://\": %s", pathname)));
    }
    canonicalize_path(pname);
    return pname;
}

/*
 * Given the base name and extension of a tsearch config file, return
 * its full path name.	The base name is assumed to be user-supplied,
 * and is checked to prevent pathname attacks.	The extension is assumed
 * to be safe.
 *
 * The result is a palloc'd string.
 */
char* get_tsearch_config_filename(const char* basename, char* pathname, const char* extension, bool newfile)
{
    char* absfname = NULL;
    bool deletefile = false;
    char* result = NULL;
    errno_t errorno = EOK;

    ereport(DEBUG2,
        (errmodule(MOD_TS),
            errmsg("Get tsfile filename with \"%s\" \"%s\" \"%s\", %d", pathname, basename, extension, newfile)));

    if (newfile) {
        /*
         * If user-defined dictionary file, we need to check it carefully.
         * We do this check when sql is from user directly or it is not a cluster.
         */
        if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || !inCluster()) {
            /*
             * We limit the basename to contain a-z, 0-9, and underscores.	This may
             * be overly restrictive, but we don't want to allow access to anything
             * outside the tsearch_data directory, so for instance '/' *must* be
             * rejected, and on some platforms '\' and ':' are risky as well. Allowing
             * uppercase might result in incompatible behavior between case-sensitive
             * and case-insensitive filesystems, and non-ASCII characters create other
             * interesting risks, so on the whole a tight policy seems best.
             */
            if (strspn(basename, "abcdefghijklmnopqrstuvwxyz0123456789_") != strlen(basename))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid text search configuration file name \"%s\"", basename)));

            StringInfoData strinfo;
            initStringInfo(&strinfo);
            if (pathname != NULL) {
                /* Check user-defined path */
                char* pname = trim(pathname);
                pname = get_tsfile_absolute_path(pname, basename, extension);
                appendStringInfo(&strinfo, "%s", pname);
                pfree_ext(pname);
            } else {
                /* original logic as openGauss */
                char sharepath[MAXPGPATH];
                get_share_path(my_exec_path, sharepath);
                appendStringInfo(&strinfo, "%s/tsearch_data", sharepath);
                check_backend_env(strinfo.data);
            }

            appendStringInfo(&strinfo, "/%s", basename);
            absfname = strinfo.data;
        } else {
            /* Temp dictionary file copied from remote CN */
            absfname = get_tsfile_prefix_tmp(false);
            deletefile = true;
        }
    } else {
        StringInfoData strinfo;
        char sharepath[MAXPGPATH];

        initStringInfo(&strinfo);
        get_share_path(my_exec_path, sharepath);
        check_backend_env(sharepath);
        appendStringInfo(&strinfo, "%s/tsearch_data/%s", sharepath, basename);
        absfname = strinfo.data;
    }

    /* result format: %s.%s */
    int resultlen = strlen(absfname) + 1 + strlen(extension);
    if (resultlen > MAXPGPATH - 1) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of dictionary file is too long")));
    }

    result = (char*)palloc(resultlen + 1);
    errorno = snprintf_s(result, resultlen + 1, resultlen, "%s.%s", absfname, extension);
    securec_check_ss(errorno, "\0", "\0");

    /* Delete temp dictionary file (copied from remote CN) when commit or abort. */
    if (deletefile) {
        InsertIntoPendingLibraryDelete(result, true);
        InsertIntoPendingLibraryDelete(result, false);
    }

    /* Check if exists */
    if (!file_exists(result)) {
        ereport(ERROR,
            (errcode_for_file_access(),
                errmsg("Dictionary file \"%s\" does not exist or cannot access the directory.", result)));
    }

    /* Check if has read permission */
    if (-1 == access(result, R_OK)) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("Dictionary File \"%s\" does not have READ permission.", result)));
    }

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Get tsfile filename ok: \"%s\"", result)));

    pfree_ext(absfname);
    return result;
}

static int comparestr(const void* a, const void* b)
{
    return strcmp(*(char* const*)a, *(char* const*)b);
}

/*
 * Reads a stop-word file. Each word is run through 'wordop'
 * function, if given.	wordop may either modify the input in-place,
 * or palloc a new version.
 */
void readstoplist(const char* fname, StopList* s, char* (*wordop)(const char*))
{
    char** stop = NULL;

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Read stopword list from \"%s\"", fname)));

    s->len = 0;
    if (fname && *fname) {
        tsearch_readline_state trst;
        char* line = NULL;
        int reallen = 0;

        if (!tsearch_readline_begin(&trst, fname)) {
            ereport(
                ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("could not open stop-word file \"%s\": %m", fname)));
        }

        while ((line = tsearch_readline(&trst)) != NULL) {
            char* pbuf = line;

            /* Trim trailing space */
            while (*pbuf && !t_isspace(pbuf))
                pbuf += pg_mblen(pbuf);
            *pbuf = '\0';

            /* Skip empty lines */
            if (*line == '\0') {
                pfree_ext(line);
                continue;
            }

            if (s->len >= reallen) {
                if (reallen == 0) {
                    reallen = 64;
                    stop = (char**)palloc(sizeof(char*) * reallen);
                } else {
                    reallen *= 2;
                    stop = (char**)repalloc((void*)stop, sizeof(char*) * reallen);
                }
            }

            if (wordop != NULL) {
                stop[s->len] = wordop(line);
                if (stop[s->len] != line)
                    pfree_ext(line);
            } else {
                stop[s->len] = line;
            }

            (s->len)++;
        }

        tsearch_readline_end(&trst);
    }

    s->stop = stop;

    /* Sort to allow binary searching */
    if (s->stop && s->len > 0) {
        qsort(s->stop, s->len, sizeof(char*), comparestr);
    }
}

bool searchstoplist(StopList* s, const char* key)
{
    return (s->stop && s->len > 0 && bsearch(&key, s->stop, s->len, sizeof(char*), comparestr)) ? true : false;
}

void ts_check_feature_disable()
{
    if (!IsInitdb && is_feature_disabled(FULL_TEXT_INDEX)) {
        /* full text index is disabled under 300 deployment */
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("TEXT SEARCH is not yet supported.")));
    }
}

/*
 * @Description: Generate internal dictionary file prefix.
 * @in: void
 * @out: internal dictionary file prefix name, e.g. "cn1_1234567890".
 */
char* get_tsfile_prefix_internal()
{
    /* no internal prefix for build-in text search dictionary */
    if (IsInitdb) {
        return NULL;
    }

    StringInfoData prefixstr;
    initStringInfo(&prefixstr);
    appendStringInfo(&prefixstr,
        "%s%c%ld%lu",
        g_instance.attr.attr_common.PGXCNodeName,
        DICT_SEPARATOR,
        GetCurrentTimestamp(),
        GetCurrentTransactionId());

    if (prefixstr.len > MAXPGPATH - 1) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of internal dictionary file is too long")));
    }	

    ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Get tsfile internal prefix: \"%s\"", prefixstr.data)));

    return prefixstr.data;
}

/*
 * @Description: Get dictionary files' postfix by given separator.
 * @in filenames: dictionary file names.
 * @in sep: separator
 * @out: postfix list (not palloc).
 */
List* get_tsfile_postfix(List* filenames, char sep)
{
    ListCell* cell = NULL;
    char* fname = NULL;
    List* results = NIL;
    int i;

    foreach (cell, filenames) {
        fname = (char*)lfirst(cell);
        for (i = strlen(fname) - 1; i >= 0; i--) {
            if (fname[i] == sep)
                break;
        }

        if (i < 0) {
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Dictionary file name value \"%s\" contain no %c", fname, sep)));
        }

        results = lappend(results, &fname[i]);
    }

    return results;
}

/*
 * @Description: Copy user-defined dictionary files to local.
 * @in filenames: user-defined dictionary file names.
 * @in postfix: file name postfixes corresponding with filenames.
 * @in dictprefix: internal dictionary file prefix.
 * @out: internal dictionary file names with absolute path.
 */
List* copy_tsfile_to_local(List* filenames, List* postfixes, const char* dictprefix)
{
    if (filenames == NIL || postfixes == NIL || dictprefix == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Copy dictionary files (local) failed for invalid inputs")));
    }
	
    /* Expand internal dictionary file prefix start with "$dictdir/" */
    char* absprefix = expand_tsfile_prefix_internal(dictprefix);
    int absprefixlen = strlen(absprefix);
    int targetpathlen = 0;
    ListCell *cell1 = NULL;
    ListCell *cell2 = NULL;
    char *fname = NULL;
    char *postfix = NULL;
    char targetpath[MAXPGPATH];
    errno_t errorno = EOK;
    List* internalfiles = NIL;

    forboth(cell1, filenames, cell2, postfixes)
    {
        fname = (char*)lfirst(cell1);
        postfix = (char*)lfirst(cell2);
        targetpathlen = absprefixlen + strlen(postfix);
        if (targetpathlen > MAXPGPATH - 1) {
            ereport(
                ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of internal dictionary file is too long")));
        }
        errorno = snprintf_s(targetpath, MAXPGPATH, targetpathlen, "%s%s", absprefix, postfix);
        securec_check_ss(errorno, "\0", "\0");

        if (!copy_library_file(fname, targetpath)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("Copy dictionary file \"%s\" failed: %m", fname)));
        }
        /* Delete the internal dictionary file when rollback. */
        InsertIntoPendingLibraryDelete(targetpath, false);
        internalfiles = lappend(internalfiles, pstrdup(targetpath));

        ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Copy tsfile \"%s\" to local \"%s\"", fname, targetpath)));
    }

    /* Clean up */
    pfree_ext(absprefix);
    return internalfiles;
}

/*
 * @Description: Send dictionary files to other remote nodes.
 * @in filenames: names of dictionary files with absolute path.
 * @in postfixes: dictionary file name postfixes corresponding with filenames.
 * @out: void
 */
void copy_tsfile_to_remote(List* filenames, List* postfixes)
{
    if (!IS_SINGLE_NODE && list_length(filenames) > 0 && list_length(postfixes) > 0) {
        char* transfer_path = get_transfer_path();
        if (!file_exists(transfer_path)) {
            /* transer file is not exixts, that be not clusters. */
            ereport(LOG, (errcode_for_file_access(), errmsg("File transfer.py does not exist.")));
            pfree_ext(transfer_path);
            return;
        }

        /* Generate remote file name */
        char* prefix = get_tsfile_prefix_tmp(true);
        int prefixlen = strlen(prefix);
        ListCell *cell1 = NULL;
        ListCell *cell2 = NULL;
        char *fname = NULL;
        char *postfix = NULL;
        StringInfoData strinfo;
        initStringInfo(&strinfo);

        forboth(cell1, filenames, cell2, postfixes)
        {
            fname = (char*)lfirst(cell1);
            postfix = (char*)lfirst(cell2);
            if (prefixlen + strlen(postfix) > MAXPGPATH - 1) {
                ereport(ERROR,
                    (errcode(ERRCODE_NAME_TOO_LONG), errmsg("The name of internal dictionary file is too long")));
            }
            appendStringInfo(&strinfo, "%s %d %s %s%s", transfer_path, SENDTOOTHERNODE, fname, prefix, postfix);

            if (system(strinfo.data) != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Send dictionary file to node fail: %m, command %s", strinfo.data)));
            }
            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Copy tsfile to remote: \"%s\"", strinfo.data)));

            resetStringInfo(&strinfo);
        }

        /* Clean up */
        pfree_ext(prefix);
        pfree_ext(strinfo.data);
        pfree_ext(transfer_path);
    }
}

/*
 * @Description: Send dictionary files to backup node.
 * @in filenames: names of dictionary files.
 * @out: void
 */
void copy_tsfile_to_backup(List* filenames)
{
    if (!IS_SINGLE_NODE && list_length(filenames) > 0) {
        char* transfer_path = get_transfer_path();
        if (!file_exists(transfer_path)) {
            /* transer file is not exixts, that be not clusters. */
            ereport(LOG, (errcode_for_file_access(), errmsg("File transfer.py does not exist.")));
            pfree_ext(transfer_path);
            return;
        }

        char* fname = NULL;
        ListCell* cell = NULL;
        StringInfoData strinfo;
        initStringInfo(&strinfo);

        foreach (cell, filenames) {
            fname = (char*)lfirst(cell);
            appendStringInfo(&strinfo,
                "%s %d %s %s",
                transfer_path,
                SENDTOBACKUP,
                fname,
                g_instance.attr.attr_common.PGXCNodeName);

            if (system(strinfo.data) != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYSTEM_ERROR),
                        errmsg("Send dictionary file to backup fail: %m, command %s", strinfo.data)));
            }
            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Copy tsfile to backup: \"%s\"", strinfo.data)));

            resetStringInfo(&strinfo);
        }

        /* Clean up */
        pfree_ext(strinfo.data);
        pfree_ext(transfer_path);
    }
}

/*
 * @Description: Delete internal dictionary files of one dictionary.
 * @in dictprefix: internal dictionary file prefix.
 * @in tmplId: template oid
 * @out: void
 */
void delete_tsfile_internal(const char* dictprefix, Oid tmplId)
{
    if (dictprefix == NULL || tmplId == InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("Delete internal dictionary files failed for invalid inputs")));
    }
    /* Expand to get absolute path */
    char* prefix = expand_tsfile_prefix_internal(dictprefix);
    int prefixlen = strlen(prefix);
    char result[MAXPGPATH];
    errno_t errorno = EOK;

    /*
     * For now, we only support 5 built-in text search templates (shown in
     * pg_ts_template), and do not allowed user-defined template.
     */
    switch (tmplId) {
        case TSTemplateSynonymId: {
            /* only syn */
            errorno = snprintf_s(
                result, MAXPGPATH, prefixlen + 1 + strlen(FILE_POSTFIX_SYN), "%s.%s", prefix, FILE_POSTFIX_SYN);
            securec_check_ss(errorno, "\0", "\0");
            InsertIntoPendingLibraryDelete(result, true);

            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete internal tsfile: \"%s\"", result)));
            break;
        }

        case TSTemplateThesaurusId: {
            /* only ths */
            errorno = snprintf_s(
                result, MAXPGPATH, prefixlen + 1 + strlen(FILE_POSTFIX_THS), "%s.%s", prefix, FILE_POSTFIX_THS);
            securec_check_ss(errorno, "\0", "\0");
            InsertIntoPendingLibraryDelete(result, true);

            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete internal tsfile: \"%s\"", result)));
            break;
        }

        case TSTemplateIspellId: {
            /* dict and affix, maybe stop */
            errorno = snprintf_s(
                result, MAXPGPATH, prefixlen + 1 + strlen(FILE_POSTFIX_DICT), "%s.%s", prefix, FILE_POSTFIX_DICT);
            securec_check_ss(errorno, "\0", "\0");
            InsertIntoPendingLibraryDelete(result, true);
            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete internal tsfile: \"%s\"", result)));

            errorno = snprintf_s(
                result, MAXPGPATH, prefixlen + 1 + strlen(FILE_POSTFIX_AFFIX), "%s.%s", prefix, FILE_POSTFIX_AFFIX);
            securec_check_ss(errorno, "\0", "\0");
            InsertIntoPendingLibraryDelete(result, true);
            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete internal tsfile: \"%s\"", result)));
        }
            /* stop file is added as below, do not break */
        default: {
            /* only stop for simple and snowball */
            errorno = snprintf_s(
                result, MAXPGPATH, prefixlen + 1 + strlen(FILE_POSTFIX_STOP), "%s.%s", prefix, FILE_POSTFIX_STOP);
            securec_check_ss(errorno, "\0", "\0");
            InsertIntoPendingLibraryDelete(result, true);

            ereport(DEBUG2, (errmodule(MOD_TS), errmsg("Delete internal tsfile: \"%s\"", result)));
            break;
        }
    }

    /* Clean up */
    pfree(prefix);
}

/*
 * @Description: Delete internal dictionary files of multiple dictionaries.
 * @in objfiles: list of internal dictionary file names with "_templateOid"
 *               (format in pg_shdepend's objfile).
 * @out: void
 */
void delete_tsfiles(List* objfiles)
{
    /* Separate the template oids */
    List* tmplIds = get_tsfile_postfix(objfiles, DICT_SEPARATOR);
    Oid tmplId = InvalidOid;
    char *dictprefix = NULL;
    char *tmplIdstr = NULL;
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    int dictprefixlen;

    forboth(lc1, objfiles, lc2, tmplIds)
    {
        dictprefix = (char*)lfirst(lc1);
        /* skip DICT_SEPARATOR */
        tmplIdstr = (char*)lfirst(lc2) + 1;
        /* get template oid */
        tmplId = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(tmplIdstr)));
        /* get general internal dictionary file names */
        dictprefixlen = strlen(dictprefix) - strlen(tmplIdstr) - 1;
        dictprefix[dictprefixlen] = '\0';

        ereport(
            DEBUG2, (errmodule(MOD_TS), errmsg("Delete db tsfile: prefix \"%s\", template %u", dictprefix, tmplId)));

        /* delete dictionary files for one dictionary */
        delete_tsfile_internal(dictprefix, tmplId);
    }

    /* Clean up */
    list_free(tmplIds);
}

/*
 * @Description: Check if we are in a cluster or not.
 * @in: void
 * @out: if we are in a cluster or not
 */
bool inCluster()
{
    if (IS_SINGLE_NODE) {
        return false;
    }
    return file_exists(get_transfer_path());
}

