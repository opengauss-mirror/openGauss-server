/*
 *	relfilenode.c
 *
 *	relfilenode functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/relfilenode.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"

#include "catalog/pg_class.h"
#include "access/transam.h"

typedef enum tag_Transfer_operation {
    TRANSFER_GEN_UNDO_AND_DEL_NEW_DIRS = 0,
    TRANSFER_BACKUP_PG_CATALOG = 1,
    TRANSFER_ADJUST_DB_DIR_FILES = 2
} TRANSFER_OP_E;

#define ROLLBACK_SCRIPT_FILE "rollback_full_upgrade.sh"
#define CLEANUP_SCRIPT_FILE "cleanup_full_upgrade.sh"

static void rename_dir(char* dir_src, char* dir_dest)
{
    FILE* out_dir = NULL;
    pg_log(PG_VERBOSE, "moving folder \"%s\" to \"%s\"\n", dir_src, dir_dest);

    if (0 != rename(dir_src, dir_dest)) {
        pg_log(PG_FATAL, "error while moving folder (%s to %s) : %s.\n", dir_src, dir_dest, getErrorText(errno));
    }

    if ((out_dir = fopen(dir_dest, "r")) == NULL) {
        pg_log(PG_FATAL, "faile to fopen dir %s : %s.\n", dir_dest, getErrorText(errno));
    }

    /* fsync the file_dest dir immediately, in case of an unfortunate system crash */
    if (fsync(fileno(out_dir)) != 0) {
        (void)fclose(out_dir);
        pg_log(PG_FATAL, "faile to fsync dir %s : %s.\n", dir_dest, getErrorText(errno));
    }

    if (fclose(out_dir) != 0) {
        pg_log(PG_FATAL, "faile to fclose dir %s : %s.\n", dir_dest, getErrorText(errno));
    }
}

static void rename_file(char* file_src, char* file_dest)
{
    FILE* out_file = NULL;

    pg_log(PG_VERBOSE, "moving file \"%s\" to \"%s\"\n", file_src, file_dest);

    if (0 != rename(file_src, file_dest)) {
        pg_log(PG_FATAL, "error while moving file (%s to %s) : %s.\n", file_src, file_dest, getErrorText(errno));
    }

    if ((out_file = fopen(file_dest, "r")) == NULL) {
        pg_log(PG_FATAL, "faile to fopen file %s : %s.\n", file_dest, getErrorText(errno));
    }

    /* fsync the file_dest file immediately, in case of an unfortunate system crash */
    if (fsync(fileno(out_file)) != 0) {
        (void)fclose(out_file);
        pg_log(PG_FATAL, "faile to fsync file %s : %s.\n", file_dest, getErrorText(errno));
    }

    if (fclose(out_file) != 0) {
        pg_log(PG_FATAL, "faile to fclose file %s : %s.\n", file_dest, getErrorText(errno));
    }
}

static int32 mark_db_folders_for_deletion(DbInfo* new_db, ClusterInfo* cluster, FILE* cleanup_file)
{
    int tblnum;
    char* src_dir = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_dir = (char*)pg_malloc(MAXPGPATH + 1);
    int32 ulRet;

    /* check for base directory */
    ulRet = snprintf_s(src_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", cluster->pgdata, new_db->db_oid);
    securec_check_ss_c(ulRet, "\0", "\0");

    if (isDirExists(src_dir)) {
        ulRet = snprintf_s(dest_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u_del", cluster->pgdata, new_db->db_oid);
        securec_check_ss_c(ulRet, "\0", "\0");

        fprintf(cleanup_file, "rm -rf '%s'\n", dest_dir);
        rename_dir(src_dir, dest_dir);
    }

    /* check in all table spaces */
    for (tblnum = 0; tblnum < os_info.num_tablespaces; tblnum++) {
        snprintf_s(src_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            os_info.tablespaces[tblnum],
            cluster->tablespace_suffix,
            new_db->db_oid);
        securec_check_ss_c(ulRet, "\0", "\0");

        if (isDirExists(src_dir)) {
            ulRet = snprintf_s(dest_dir,
                MAXPGPATH + 1,
                MAXPGPATH,
                "%s%s/%u_del",
                os_info.tablespaces[tblnum],
                cluster->tablespace_suffix,
                new_db->db_oid);
            securec_check_ss_c(ulRet, "\0", "\0");

            fprintf(cleanup_file, "rm -rf '%s'\n", dest_dir);
            rename_dir(src_dir, dest_dir);
        }
    }

    free(src_dir);
    free(dest_dir);

    return 0;
}

static int32 genrate_undo_remove_new_pg_catalog_files(DbInfo* new_db, ClusterInfo* newcluster, FILE* rollback_script)
{
    char* pg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);

    char* src_file = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_file = (char*)pg_malloc(MAXPGPATH + 1);
    int32 nRet;
    int idx;
    int seg_idx;

    if ('\0' == new_db->db_tblspace[0]) {
        nRet =
            snprintf_s(pg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", newcluster->pgdata, new_db->db_oid);
    } else {
        nRet = snprintf_s(pg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            new_db->db_tblspace,
            newcluster->tablespace_suffix,
            new_db->db_oid);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "if [ -d \"%s\" ] ;\nthen\n", pg_catalog_base_dir);

    for (idx = 0; idx < new_db->rel_arr.nrels; idx++) {
        RelInfo* new_rel = &new_db->rel_arr.rels[idx];

        /*main/vm/fsm  -- main.1 .. */
        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
        }

        seg_idx = 1;
        do {
            nRet = snprintf_s(
                src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u.%u", pg_catalog_base_dir, new_rel->relfilenode, seg_idx);
            securec_check_ss_c(nRet, "\0", "\0");

            if (isFileExists(src_file)) {
                fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
            } else {
                break;
            }

            seg_idx++;
        } while (true);

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
        }

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
        }
    }

    /* special files pg_filenode.map pg_internal.init PG_VERSION */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_filenode.map", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);

    /*pg_internal.init file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    if (isFileExists(src_file)) {
        fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
    }

    /*PG_VERSION file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/PG_VERSION", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "  rm -rf \"%s\" ;\n", src_file);
    fprintf(rollback_script, "fi\n\n");

    free(src_file);
    free(dest_file);
    free(pg_catalog_base_dir);

    return 0;
}

static int32 genrate_undo_restore_old_pg_catalog_files(DbInfo* old_db, ClusterInfo* newcluster, FILE* rollback_script)
{
    char* pg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);

    char* src_file = (char*)pg_malloc(MAXPGPATH + 1);
    int32 nRet;
    int idx;
    int seg_idx;

    if ('\0' == old_db->db_tblspace[0]) {
        nRet =
            snprintf_s(pg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", newcluster->pgdata, old_db->db_oid);
    } else {
        nRet = snprintf_s(pg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            old_db->db_tblspace,
            newcluster->tablespace_suffix,
            old_db->db_oid);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "\n#restoring pg_catalog entries for %s from %s\n", old_db->db_name, pg_catalog_base_dir);

    fprintf(rollback_script, "if [ -d \"%s\" ] ;\nthen\n", pg_catalog_base_dir);

    for (idx = 0; idx < old_db->rel_arr.nrels; idx++) {
        RelInfo* new_rel = &old_db->rel_arr.rels[idx];

        /*main/vm/fsm  -- main.1 .. */
        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
            fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);
        }

        seg_idx = 1;
        do {
            nRet = snprintf_s(
                src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u.%u", pg_catalog_base_dir, new_rel->relfilenode, seg_idx);
            securec_check_ss_c(nRet, "\0", "\0");

            if (isFileExists(src_file)) {
                fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
                fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);
            } else {
                break;
            }

            seg_idx++;
        } while (true);

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
            fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);
        }

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
            fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);
        }
    }

    /* special files pg_filenode.map pg_internal.init PG_VERSION */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_filenode.map", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
    fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);

    /*pg_internal.init file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    if (isFileExists(src_file)) {
        fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
        fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);
    }

    /*PG_VERSION file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/PG_VERSION", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    fprintf(rollback_script, "  if [ -f '%s_bak' ] ;\n  then\n", src_file);
    fprintf(rollback_script, "    mv '%s_bak' '%s'\n  fi\n", src_file, src_file);

    fprintf(rollback_script, "fi\n\n");

    free(src_file);
    free(pg_catalog_base_dir);

    return 0;
}

static int32 generate_safe_undo_log(
    DbInfo* old_db, DbInfo* new_db, ClusterInfo* oldcluster, ClusterInfo* newcluster, FILE* rollback_script)
{
    int tblnum;
    char* src_dir = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_dir = (char*)pg_malloc(MAXPGPATH + 1);
    int32 ulRet;
    int32 nRet;

    fprintf(rollback_script, "\n\n#\n#rollback operation started for %s \n#\n", new_db->db_name);

    genrate_undo_remove_new_pg_catalog_files(new_db, newcluster, rollback_script);

    /* check for base directory */
    ulRet = snprintf_s(src_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", oldcluster->pgdata, old_db->db_oid);
    securec_check_ss_c(ulRet, "\0", "\0");

    if (isDirExists(src_dir)) {
        ulRet = snprintf_s(dest_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", newcluster->pgdata, new_db->db_oid);
        securec_check_ss_c(ulRet, "\0", "\0");

        fprintf(rollback_script, "\n#mapping %s => %s\n", src_dir, dest_dir);
        fprintf(rollback_script, "if [ ! -d \"%s\" ];\nthen\n", src_dir);
        fprintf(rollback_script, "  mv \"%s\" \"%s\" \n", dest_dir, src_dir);
        fprintf(rollback_script,
            "  if [ \"$?\" -ne 0 ];\n"
            "  then\n"
            "    echo \"move command failed for '%s' to '%s'\"\n"
            "    script_error=1\n"
            "  fi\n"
            "fi\n",
            dest_dir,
            src_dir);
    }

    /* check in all table spaces */
    for (tblnum = 0; tblnum < os_info.num_tablespaces; tblnum++) {
        nRet = snprintf_s(src_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            os_info.tablespaces[tblnum],
            oldcluster->tablespace_suffix,
            old_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isDirExists(src_dir)) {
            nRet = snprintf_s(dest_dir,
                MAXPGPATH + 1,
                MAXPGPATH,
                "%s%s/%u",
                os_info.tablespaces[tblnum],
                newcluster->tablespace_suffix,
                new_db->db_oid);
            securec_check_ss_c(nRet, "\0", "\0");

            fprintf(rollback_script, "\n#mapping %s => %s\n", src_dir, dest_dir);
            fprintf(rollback_script, "if [ ! -d \"%s\" ];\n", src_dir);
            fprintf(rollback_script,
                "then\n"
                "  mv \"%s\" \"%s\" \n",
                dest_dir,
                src_dir);
            fprintf(rollback_script,
                "  if [ \"$?\" -ne 0 ];\n"
                "  then\n"
                "    echo \"move command failed for '%s' to '%s'\"\n"
                "    script_error=1\n"
                "  fi\n"
                "fi\n",
                dest_dir,
                src_dir);
        }
    }

    genrate_undo_restore_old_pg_catalog_files(old_db, oldcluster, rollback_script);
    fprintf(rollback_script, "\n#\n#rollback operation ended for %s \n#\n\n", new_db->db_name);

    free(src_dir);
    free(dest_dir);

    return 0;
}

static int32 cleanup_db_pg_catalog_files(
    DbInfo* old_db, DbInfo* new_db, ClusterInfo* oldcluster, ClusterInfo* newcluster, FILE* cleanup_file)
{
    char* oldpg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);
    char* newpg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);
    char* src_file = (char*)pg_malloc(MAXPGPATH + 1);

    int32 nRet;
    int idx;
    int seg_idx;

    if ('\0' == old_db->db_tblspace[0]) {
        nRet = snprintf_s(
            oldpg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", oldcluster->pgdata, old_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(
            newpg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", newcluster->pgdata, new_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        nRet = snprintf_s(oldpg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            old_db->db_tblspace,
            oldcluster->tablespace_suffix,
            old_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(newpg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            new_db->db_tblspace,
            newcluster->tablespace_suffix,
            new_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    for (idx = 0; idx < old_db->rel_arr.nrels; idx++) {
        RelInfo* old_rel = &old_db->rel_arr.rels[idx];

        /*main/vm/fsm  -- main.1 .. */
        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u", oldpg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(cleanup_file, "rm -f '%s/%u_bak'\n", newpg_catalog_base_dir, old_rel->relfilenode);
        }

        seg_idx = 1;
        do {
            nRet = snprintf_s(
                src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u.%u", oldpg_catalog_base_dir, old_rel->relfilenode, seg_idx);
            securec_check_ss_c(nRet, "\0", "\0");

            if (isFileExists(src_file)) {
                fprintf(cleanup_file, "rm -f '%s/%u.%u_bak'\n", newpg_catalog_base_dir, old_rel->relfilenode, seg_idx);
            } else {
                break;
            }
            seg_idx++;
        } while (true);

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm", oldpg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            fprintf(cleanup_file, "rm -f '%s/%u_vm_bak'\n", newpg_catalog_base_dir, old_rel->relfilenode);
        }

        nRet =
            snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm", oldpg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");
        if (isFileExists(src_file)) {
            fprintf(cleanup_file, "rm -f '%s/%u_fsm_bak'\n", newpg_catalog_base_dir, old_rel->relfilenode);
        }
    }

    /* special files pg_filenode.map pg_internal.init PG_VERSION */
    fprintf(cleanup_file, "rm -f '%s/pg_filenode.map_bak'\n", newpg_catalog_base_dir);

    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init", oldpg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    if (isFileExists(src_file)) {
        fprintf(cleanup_file, "rm -f '%s/pg_internal.init_bak'\n", newpg_catalog_base_dir);
    }

    fprintf(cleanup_file, "rm -f '%s/PG_VERSION_bak'\n", newpg_catalog_base_dir);

    free(src_file);
    free(newpg_catalog_base_dir);
    free(oldpg_catalog_base_dir);

    return 0;
}

static int32 backup_db_pg_catalog_files(DbInfo* old_db, ClusterInfo* cluster)
{
    char* pg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);

    char* src_file = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_file = (char*)pg_malloc(MAXPGPATH + 1);
    int32 nRet;
    int idx;
    int seg_idx;

    if ('\0' == old_db->db_tblspace[0]) {
        nRet = snprintf_s(pg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", cluster->pgdata, old_db->db_oid);
    } else {
        nRet = snprintf_s(pg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            old_db->db_tblspace,
            cluster->tablespace_suffix,
            old_db->db_oid);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    for (idx = 0; idx < old_db->rel_arr.nrels; idx++) {
        RelInfo* old_rel = &old_db->rel_arr.rels[idx];

        /*main/vm/fsm  -- main.1 .. */
        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u", pg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            nRet =
                snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_bak", pg_catalog_base_dir, old_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");

            rename_file(src_file, dest_file);
        }

        seg_idx = 1;
        do {
            nRet = snprintf_s(
                src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u.%u", pg_catalog_base_dir, old_rel->relfilenode, seg_idx);
            securec_check_ss_c(nRet, "\0", "\0");

            if (isFileExists(src_file)) {
                nRet = snprintf_s(dest_file,
                    MAXPGPATH + 1,
                    MAXPGPATH,
                    "%s/%u.%u_bak",
                    pg_catalog_base_dir,
                    old_rel->relfilenode,
                    seg_idx);
                securec_check_ss_c(nRet, "\0", "\0");
                rename_file(src_file, dest_file);
            } else {
                break;
            }
            seg_idx++;
        } while (true);

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm", pg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            nRet = snprintf_s(
                dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm_bak", pg_catalog_base_dir, old_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");
            rename_file(src_file, dest_file);
        }

        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm", pg_catalog_base_dir, old_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");
        if (isFileExists(src_file)) {
            nRet = snprintf_s(
                dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm_bak", pg_catalog_base_dir, old_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");
            rename_file(src_file, dest_file);
        }
    }

    /* special files pg_filenode.map pg_internal.init PG_VERSION */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_filenode.map", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_filenode.map_bak", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    rename_file(src_file, dest_file);

    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    if (isFileExists(src_file)) {
        nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init_bak", pg_catalog_base_dir);
        securec_check_ss_c(nRet, "\0", "\0");
        rename_file(src_file, dest_file);
    }

    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s/PG_VERSION", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/PG_VERSION_bak", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");
    rename_file(src_file, dest_file);

    free(src_file);
    free(dest_file);
    free(pg_catalog_base_dir);

    return 0;
}

static int32 transfer_db_folders(DbInfo* old_db, DbInfo* new_db, ClusterInfo* oldcluster, ClusterInfo* newcluster)
{
    int tblnum;
    char* src_dir = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_dir = (char*)pg_malloc(MAXPGPATH + 1);
    int32 ulRet;

    /* check for base directory */
    ulRet = snprintf_s(src_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", oldcluster->pgdata, old_db->db_oid);
    securec_check_ss_c(ulRet, "\0", "\0");

    if (isDirExists(src_dir)) {
        ulRet = snprintf_s(dest_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", newcluster->pgdata, new_db->db_oid);
        securec_check_ss_c(ulRet, "\0", "\0");

        rename_dir(src_dir, dest_dir);
    }

    /* check in all table spaces */
    for (tblnum = 0; tblnum < os_info.num_tablespaces; tblnum++) {
        snprintf_s(src_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            os_info.tablespaces[tblnum],
            oldcluster->tablespace_suffix,
            old_db->db_oid);
        securec_check_ss_c(ulRet, "\0", "\0");

        if (isDirExists(src_dir)) {
            ulRet = snprintf_s(dest_dir,
                MAXPGPATH + 1,
                MAXPGPATH,
                "%s%s/%u",
                os_info.tablespaces[tblnum],
                newcluster->tablespace_suffix,
                new_db->db_oid);
            securec_check_ss_c(ulRet, "\0", "\0");

            rename_dir(src_dir, dest_dir);
        }
    }

    free(src_dir);
    free(dest_dir);

    return 0;
}

static int32 restore_db_pg_catalog_files(DbInfo* new_db, ClusterInfo* cluster)
{
    char* pg_catalog_base_dir = (char*)pg_malloc(MAXPGPATH + 1);

    char* src_file = (char*)pg_malloc(MAXPGPATH + 1);
    char* dest_file = (char*)pg_malloc(MAXPGPATH + 1);
    int32 nRet = 0;
    int idx;
    int seg_idx;

    if ('\0' == new_db->db_tblspace[0]) {
        nRet = snprintf_s(pg_catalog_base_dir, MAXPGPATH + 1, MAXPGPATH, "%s/base/%u", cluster->pgdata, new_db->db_oid);
    } else {
        nRet = snprintf_s(pg_catalog_base_dir,
            MAXPGPATH + 1,
            MAXPGPATH,
            "%s%s/%u",
            new_db->db_tblspace,
            cluster->tablespace_suffix,
            new_db->db_oid);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    for (idx = 0; idx < new_db->rel_arr.nrels; idx++) {
        RelInfo* new_rel = &new_db->rel_arr.rels[idx];

        /*main/vm/fsm  -- main.1 .. */
        nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/%u", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");
        if (isFileExists(src_file)) {
            nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u", pg_catalog_base_dir, new_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");
            rename_file(src_file, dest_file);
        }

        seg_idx = 1;
        do {
            nRet = snprintf_s(
                src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/%u.%u", pg_catalog_base_dir, new_rel->relfilenode, seg_idx);
            securec_check_ss_c(nRet, "\0", "\0");

            if (isFileExists(src_file)) {
                nRet = snprintf_s(dest_file,
                    MAXPGPATH + 1,
                    MAXPGPATH,
                    "%s/%u.%u",
                    pg_catalog_base_dir,
                    new_rel->relfilenode,
                    seg_idx);
                securec_check_ss_c(nRet, "\0", "\0");

                rename_file(src_file, dest_file);
            } else {
                break;
            }
            seg_idx++;
        } while (true);

        nRet =
            snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/%u_vm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            nRet =
                snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_vm", pg_catalog_base_dir, new_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");

            rename_file(src_file, dest_file);
        }

        nRet =
            snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/%u_fsm", pg_catalog_base_dir, new_rel->relfilenode);
        securec_check_ss_c(nRet, "\0", "\0");

        if (isFileExists(src_file)) {
            nRet =
                snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/%u_fsm", pg_catalog_base_dir, new_rel->relfilenode);
            securec_check_ss_c(nRet, "\0", "\0");

            rename_file(src_file, dest_file);
        }
    }

    /* special files pg_filenode.map pg_internal.init PG_VERSION */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/pg_filenode.map", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_filenode.map", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    rename_file(src_file, dest_file);

    /*pg_internal.init file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/pg_internal.init", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    if (isFileExists(src_file)) {
        nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/pg_internal.init", pg_catalog_base_dir);
        securec_check_ss_c(nRet, "\0", "\0");

        rename_file(src_file, dest_file);
    }

    /*PG_VERSION file */
    nRet = snprintf_s(src_file, MAXPGPATH + 1, MAXPGPATH, "%s_del/PG_VERSION", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = snprintf_s(dest_file, MAXPGPATH + 1, MAXPGPATH, "%s/PG_VERSION", pg_catalog_base_dir);
    securec_check_ss_c(nRet, "\0", "\0");

    rename_file(src_file, dest_file);

    free(src_file);
    free(dest_file);
    free(pg_catalog_base_dir);

    return 0;
}

static int32 prepare_transfer_all_dbs(
    TRANSFER_OP_E operation, DbInfoArr* old_db_arr, DbInfoArr* new_db_arr, FILE* rollback_fp, FILE* clean_file_fp)
{
    int dbnum;
    int new_dbnum;
    int nRet = 0;

    for (dbnum = 0, new_dbnum = 0; dbnum < old_db_arr->ndbs; dbnum++) {
        DbInfo *old_db = &old_db_arr->dbs[dbnum], *new_db = NULL;
        /*
         * Advance past any databases that exist in the new cluster but not in
         * the old, e.g. "postgres".  (The user might have removed the
         * 'postgres' database from the old cluster.)
         */
        for (; new_dbnum < new_db_arr->ndbs; new_dbnum++) {
            new_db = &new_db_arr->dbs[new_dbnum];
            if (strcmp(old_db->db_name, new_db->db_name) == 0)
                break;
        }

        switch (operation) {
            case TRANSFER_GEN_UNDO_AND_DEL_NEW_DIRS: {
                nRet = generate_safe_undo_log(old_db, new_db, &old_cluster, &new_cluster, rollback_fp);
                if (nRet != 0) {
                    return nRet;
                }

                nRet = mark_db_folders_for_deletion(new_db, &new_cluster, clean_file_fp);
                if (nRet != 0) {
                    return nRet;
                }

                (void)cleanup_db_pg_catalog_files(old_db, new_db, &old_cluster, &new_cluster, clean_file_fp);
                break;
            }
            case TRANSFER_BACKUP_PG_CATALOG: {
                nRet = backup_db_pg_catalog_files(old_db, &old_cluster);
                if (nRet != 0) {
                    return nRet;
                }
                break;
            }
            case TRANSFER_ADJUST_DB_DIR_FILES: {
                nRet = transfer_db_folders(old_db, new_db, &old_cluster, &new_cluster);
                if (nRet != 0) {
                    return nRet;
                }

                nRet = restore_db_pg_catalog_files(new_db, &new_cluster);
                if (nRet != 0) {
                    return nRet;
                }
                break;
            }
        }
    }

    return nRet;
}

/*
 * transfer_all_new_dbs()
 *
 * Responsible for upgrading all database. invokes routines to generate mappings and then
 * physically link the databases.
 */
const char* transfer_all_new_dbs(DbInfoArr* old_db_arr, DbInfoArr* new_db_arr, char* old_pgdata, char* new_pgdata)
{
    char file[MAXPGPATH];
    int32 nRet;

    FILE* rollback_file = NULL;
    FILE* clean_file = NULL;

    snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, ROLLBACK_SCRIPT_FILE);
    rollback_file = fopen(file, "w");
    if (NULL == rollback_file) {
        pg_log(PG_FATAL, "Not able to create the %s file\n", file);
    }

    snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, CLEANUP_SCRIPT_FILE);
    clean_file = fopen(file, "w");
    if (NULL == clean_file) {
        pg_log(PG_FATAL, "Not able to create the %s file\n", file);
    }

    fprintf(rollback_file, "#FULL-UPGRADE-ROLLBACK START OF FILE\n");
    fprintf(clean_file, "#FULL-UPGRADE-CLEAN START OF FILE\n");

    nRet =
        prepare_transfer_all_dbs(TRANSFER_GEN_UNDO_AND_DEL_NEW_DIRS, old_db_arr, new_db_arr, rollback_file, clean_file);
    if (nRet != 0) {
        fclose(rollback_file);
        fclose(clean_file);
        snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, ROLLBACK_SCRIPT_FILE);
        unlink(file);
        snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, CLEANUP_SCRIPT_FILE);
        unlink(file);
        pg_log(PG_FATAL, "Writing failed %s file\n", file);
    }

    nRet = fprintf(rollback_file, "\n#FULL-UPGRADE-ROLLBACK END OF FILE");
    if (nRet == -1) {
        fclose(rollback_file);
        fclose(clean_file);
        snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, ROLLBACK_SCRIPT_FILE);
        unlink(file);
        snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, CLEANUP_SCRIPT_FILE);
        unlink(file);
        pg_log(PG_FATAL, "Write failed for %s file\n", file);
    }

    /* wait till file is flushed to disk */
    while (true) {
        errno = 0;
        nRet = fflush(rollback_file);
        if (nRet != 0) {
            if ((errno == EAGAIN) || (errno == EINTR)) {
                continue;
            }

            fclose(rollback_file);
            snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, ROLLBACK_SCRIPT_FILE);
            unlink(file);

            /*LOG fatal*/
            pg_log(PG_FATAL, "Write failed for %s file\n", file);
            break;
        }
        break;
    }

    fclose(rollback_file);

    nRet = prepare_transfer_all_dbs(TRANSFER_BACKUP_PG_CATALOG, old_db_arr, new_db_arr, NULL, clean_file);

    nRet = prepare_transfer_all_dbs(TRANSFER_ADJUST_DB_DIR_FILES, old_db_arr, new_db_arr, NULL, clean_file);

    nRet = fprintf(clean_file, "\n#FULL-UPGRADE-CLEANUP END OF FILE");
    if (nRet == -1) {
        fclose(clean_file);
        snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_pgdata, CLEANUP_SCRIPT_FILE);
        unlink(file);
        pg_log(PG_FATAL, "Write failed for %s file\n", file);
    }

    (void)fflush(clean_file);
    fclose(clean_file);

    prep_status(" "); /* in case nothing printed; pass a space so
                       * gcc doesn't complain about empty format
                       * string */
    check_ok();

    return "success";
}

/*
 * get_pg_database_relfilenode()
 *
 *	Retrieves the relfilenode for a few system-catalog tables.	We need these
 *	relfilenodes later in the upgrade process.
 */
void get_pg_database_relfilenode(ClusterInfo* cluster)
{
    PGconn* conn = connectToServer(cluster, "template1");
    PGresult* res = NULL;
    int i_relfile;

    res = executeQueryOrDie(conn,
        "SELECT c.relname, c.relfilenode "
        "FROM	pg_catalog.pg_class c, "
        "		pg_catalog.pg_namespace n "
        "WHERE	c.relnamespace = n.oid AND "
        "		n.nspname = 'pg_catalog' AND "
        "		c.relname = 'pg_database' "
        "ORDER BY c.relname");

    i_relfile = PQfnumber(res, "relfilenode");
    cluster->pg_database_oid = atooid(PQgetvalue(res, 0, i_relfile));

    PQclear(res);
    PQfinish(conn);
}
