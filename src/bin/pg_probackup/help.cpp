/*-------------------------------------------------------------------------
 *
 * help.c
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2017-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

static void help_init(void);
static void help_backup(void);
static void help_restore(void);
static void help_validate(void);
static void help_show(void);
static void help_delete(void);
static void help_merge(void);
static void help_set_backup(void);
static void help_set_config(void);
static void help_show_config(void);
static void help_add_instance(void);
static void help_del_instance(void);

bool is_no_help_command(const char *command)
{
    bool no_help = false;
    
    no_help = strcmp(command, "--help") == 0
         || strcmp(command, "help") == 0
         || strcmp(command, "-?") == 0
         || strcmp(command, "--version") == 0
         || strcmp(command, "version") == 0
         || strcmp(command, "-V") == 0;
         
    return no_help;
}

void help_command(const char *command)
{
    if (strcmp(command, "init") == 0)
        help_init();
    else if (strcmp(command, "backup") == 0)
        help_backup();
    else if (strcmp(command, "restore") == 0)
        help_restore();
    else if (strcmp(command, "validate") == 0)
        help_validate();
    else if (strcmp(command, "show") == 0)
        help_show();
    else if (strcmp(command, "delete") == 0)
        help_delete();
    else if (strcmp(command, "merge") == 0)
        help_merge();
    else if (strcmp(command, "set-backup") == 0)
        help_set_backup();
    else if (strcmp(command, "set-config") == 0)
        help_set_config();
    else if (strcmp(command, "show-config") == 0)
        help_show_config();
    else if (strcmp(command, "add-instance") == 0)
        help_add_instance();
    else if (strcmp(command, "del-instance") == 0)
        help_del_instance();
    else if (is_no_help_command(command))
        printf(_("No help page for \"%s\" command. Try gs_probackup help\n"), command);
    else
        printf(_("Unknown command \"%s\". Try gs_probackup help\n"), command);
    exit(0);
}

void help_pg_probackup(void)
{
    printf(_("\n%s - utility to manage backup/recovery of openGauss database.\n\n"), PROGRAM_NAME);

    printf(_("  %s help [COMMAND]\n"), PROGRAM_NAME);

    printf(_("\n  %s version\n"), PROGRAM_NAME);

    printf(_("\n  %s init -B backup-path [--help]\n"), PROGRAM_NAME);

    printf(_("\n  %s add-instance -B backup-path -D pgdata-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-E external-directories-paths]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s del-instance -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [--help]\n"));

    printf(_("\n  %s set-config -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-E external-directories-paths]\n"));
    printf(_("                 [--archive-timeout=timeout]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth]\n"));
    printf(_("                 [--compress-algorithm=compress-algorithm]\n"));
    printf(_("                 [--compress-level=compress-level]\n"));
    printf(_("                 [-d dbname] [-h host] [-p port] [-U username]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s set-backup -B backup-path --instance=instance_name -i backup-id\n"), PROGRAM_NAME);
    printf(_("                 [--note=text] [--ttl=interval] [--expire-time=time]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s show-config -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [--format=plain|json]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s show -B backup-path\n"), PROGRAM_NAME);
    printf(_("                 [--instance=instance_name [-i backup-id]]\n"));
    printf(_("                 [--archive] [--format=plain|json]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s backup -B backup-path --instance=instance_name -b backup-mode\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-C] [-S slot-name] [--temp-slot]\n"));
    printf(_("                 [--backup-pg-log] [-j threads_num] [--progress]\n"));
    printf(_("                 [--no-validate] [--skip-block-validation]\n"));
    printf(_("                 [-E external-directories-paths]\n"));
    printf(_("                 [--no-sync] [--note=text]\n"));
    printf(_("                 [--archive-timeout=timeout]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--delete-expired] [--delete-wal] [--merge-expired]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth] [--dry-run]\n"));
    printf(_("                 [--compress-algorithm=compress-algorithm]\n"));
    printf(_("                 [--compress-level=compress-level]\n"));
    printf(_("                 [--compress]\n"));
    printf(_("                 [-d dbname] [-h host] [-p port] [-U username] [-w] [-W password]\n"));
    printf(_("                 [-t rwtimeout]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--ttl=interval] [--expire-time=time]\n"));
    printf(_("                 [--backup-pg-replslot]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s restore -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-i backup-id] [-j threads_num] [--progress]\n"));
    printf(_("                 [--force] [--no-sync] [--no-validate] [--skip-block-validation]\n"));
    printf(_("                 [--external-mapping=OLDDIR=NEWDIR] [-T OLDDIR=NEWDIR]\n"));
    printf(_("                 [--skip-external-dirs] [-I incremental_mode]\n"));
    printf(_("                 [--recovery-target-time=time|--recovery-target-xid=xid\n"));
    printf(_("                  |--recovery-target-lsn=lsn|--recovery-target-name=target-name]\n"));
    printf(_("                 [--recovery-target-inclusive=boolean]\n"));    
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s merge -B backup-path --instance=instance_name -i backup-id\n"), PROGRAM_NAME);
    printf(_("                 [-j threads_num] [--progress]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s delete -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-i backup-id | --delete-expired | --merge-expired | --status=backup_status]\n"));
    printf(_("                 [--delete-wal] [-j threads_num] [--progress]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth] [--dry-run]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--help]\n"));

    printf(_("\n  %s validate -B backup-path\n"), PROGRAM_NAME);
    printf(_("                 [--instance=instance_name] [-i backup-id]\n"));
    printf(_("                 [-j threads-num] [--progress] [--skip-block-validation]\n"));
    printf(_("                 [--recovery-target-time=time|--recovery-target-xid=xid\n"));
    printf(_("                  |--recovery-target-lsn=lsn|--recovery-target-name=target-name]\n"));
    printf(_("                 [--recovery-target-inclusive=boolean]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--help]\n"));

    exit(0);
}

static void help_init(void)
{
    printf(_("\n%s init -B backup-path\n\n"), PROGRAM_NAME);

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n\n"));
}

static void help_add_instance(void)
{
    printf(_("\n%s add-instance -B backup-path -D pgdata-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-E external-directories-paths]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("  -D, --pgdata=pgdata-path         location of the database storage area\n"));
    printf(_("      --instance=instance_name     name of the new instance\n"));
    printf(_("  -E, --external-dirs=external-directories-paths\n"));
    printf(_("                                   backup some directories not from pgdata \n"));
    printf(_("                                   (example: --external-dirs=/tmp/dir1:/tmp/dir2)\n"));

    printf(_("\n  Remote options:\n"));
    printf(_("      --remote-proto=protocol      remote protocol to use\n"));
    printf(_("                                   available options: 'ssh', 'none' (default: ssh)\n"));
    printf(_("      --remote-host=destination    remote host address or hostname\n"));
    printf(_("      --remote-port=port           remote host port (default: 22)\n"));
    printf(_("      --remote-path=path           path to directory with gs_probackup binary on remote host\n"));
    printf(_("                                   (default: current binary path)\n"));
    printf(_("      --remote-user=username       user name for ssh connection (default: current user)\n"));
    printf(_("      --remote-libpath=libpath         library path on remote host\n"));
    printf(_("      --ssh-options=ssh_options    additional ssh options (default: none)\n"));
    printf(_("                                   (example: --ssh-options='-c cipher_spec -F configfile')\n\n"));
}

static void help_del_instance(void)
{
    printf(_("\n%s del-instance -B backup-path --instance=instance_name\n\n"), PROGRAM_NAME);

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance to delete\n\n"));
}

static void help_set_config(void)
{
    printf(_("\n%s set-config -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-E external-directories-paths]\n"));
    printf(_("                 [--archive-timeout=timeout]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth]\n"));
    printf(_("                 [--compress-algorithm=compress-algorithm]\n"));
    printf(_("                 [--compress-level=compress-level]\n"));
    printf(_("                 [-d dbname] [-h host] [-p port] [-U username]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -D, --pgdata=pgdata-path         location of the database storage area\n"));
    printf(_("  -E, --external-dirs=external-directories-paths\n"));
    printf(_("                                   backup some directories not from pgdata \n"));
    printf(_("                                   (example: --external-dirs=/tmp/dir1:/tmp/dir2)\n"));
    printf(_("      --archive-timeout=timeout    wait timeout for WAL segment archiving (default: 5min)\n"));

    printf(_("\n  Retention options:\n"));
    printf(_("      --retention-redundancy=retention-redundancy\n"));
    printf(_("                                   number of full backups to keep; 0 disables; (default: 0)\n"));
    printf(_("      --retention-window=retention-window\n"));
    printf(_("                                   number of days of recoverability; 0 disables; (default: 0)\n"));
    printf(_("      --wal-depth=wal-depth        number of latest valid backups with ability to perform\n"));
    printf(_("                                   the point in time recovery;  disables; (default: 0)\n"));

    printf(_("\n  Compression options:\n"));
    printf(_("      --compress-algorithm=compress-algorithm\n"));
    printf(_("                                   available options: 'zlib','pglz','none' (default: 'none')\n"));
    printf(_("      --compress-level=compress-level\n"));
    printf(_("                                   level of compression [0-9] (default: 1)\n"));

    printf(_("\n  Connection options:\n"));
    printf(_("  -U, --pguser=username            user name to connect as (default: current local user)\n"));
    printf(_("  -d, --pgdatabase=dbname          database to connect (default: username)\n"));
    printf(_("  -h, --pghost=hostname            database server host or socket directory(default: 'local socket')\n"));
    printf(_("  -p, --pgport=port                database server port (default: 5432)\n"));

    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n"));

    printf(_("\n  Remote options:\n"));
    printf(_("      --remote-proto=protocol      remote protocol to use\n"));
    printf(_("                                   available options: 'ssh', 'none' (default: ssh)\n"));
    printf(_("      --remote-host=destination    remote host address or hostname\n"));
    printf(_("      --remote-port=port           remote host port (default: 22)\n"));
    printf(_("      --remote-path=path           path to directory with gs_probackup binary on remote host\n"));
    printf(_("                                   (default: current binary path)\n"));
    printf(_("      --remote-user=username       user name for ssh connection (default: current user)\n"));
    printf(_("      --remote-libpath=libpath         library path on remote host\n"));
    printf(_("      --ssh-options=ssh_options    additional ssh options (default: none)\n"));
    printf(_("                                   (example: --ssh-options='-c cipher_spec -F configfile')\n\n"));
}

static void help_set_backup(void)
{
    printf(_("\n%s set-backup -B backup-path --instance=instance_name -i backup-id\n"), PROGRAM_NAME);
    printf(_("                 [--note=text] [--ttl=interval] [--expire-time=time]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -i, --backup-id=backup-id        backup id\n"));
    printf(_("      --note=text                  add note to backup; 'none' to remove note\n"));
    printf(_("                                   (example: --note='backup before app update to v13.1')\n"));
    printf(_("      --ttl=interval               pin backup for specified amount of time; 0 unpin\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: s)\n"));
    printf(_("                                   (example: --ttl=20d)\n"));
    printf(_("      --expire-time=time           pin backup until specified time stamp\n"));
    printf(_("                                   (example: --expire-time='2024-01-01 00:00:00+03')\n\n"));
}

static void help_show_config(void)
{
    printf(_("\n%s show-config -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [--format=format]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("      --format=format              show format=PLAIN|JSON\n\n"));
}

static void help_show(void)
{
    printf(_("\n%s show -B backup-path\n"), PROGRAM_NAME);
    printf(_("                 [--instance=instance_name [-i backup-id]]\n"));
    printf(_("                 [--archive] [--format=format]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     show info about specific instance\n"));
    printf(_("  -i, --backup-id=backup-id        show info about specific backups\n"));
    printf(_("      --archive                    show WAL archive information\n"));
    printf(_("      --format=format              show format=PLAIN|JSON\n\n"));
}

static void help_backup(void)
{
    printf(_("\n%s backup -B backup-path --instance=instance_name -b backup-mode\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-C] [-S slot-name] [--temp-slot]\n"));
    printf(_("                 [--backup-pg-log] [-j threads_num] [--progress]\n"));
    printf(_("                 [--no-validate] [--skip-block-validation]\n"));
    printf(_("                 [-E external-directories-paths]\n"));
    printf(_("                 [--no-sync] [--note=text]\n"));
    printf(_("                 [--archive-timeout=timeout]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n"));
    printf(_("                 [--delete-expired] [--delete-wal] [--merge-expired]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth] [--dry-run]\n"));
    printf(_("                 [--compress-algorithm=compress-algorithm]\n"));
    printf(_("                 [--compress-level=compress-level]\n"));
    printf(_("                 [--compress]\n"));
    printf(_("                 [-d dbname] [-h host] [-p port] [-U username] [-w] [-W password]\n"));
    printf(_("                 [-t rw-timeout]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--ttl=interval] [--expire-time=time]\n\n"));
    printf(_("                 [--backup-pg-replslot]\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -b, --backup-mode=backup-mode    backup mode=FULL|PTRACK\n"));
    printf(_("  -D, --pgdata=pgdata-path         location of the database storage area\n"));
    printf(_("  -C, --smooth-checkpoint          do smooth checkpoint before backup\n"));
    printf(_("  -S, --slot=slot-name             replication slot to use\n"));
    printf(_("      --temp-slot                  use temporary replication slot\n"));
    printf(_("      --backup-pg-log              backup of '%s' directory\n"), PG_LOG_DIR);
    printf(_("  -j, --threads=threads_num        number of parallel threads\n"));
    printf(_("  -t, --rw-timeout=rw-timeout      read-write timeout during idle connection\n"));
    printf(_("      --progress                   show progress\n"));
    printf(_("      --no-validate                disable validation after backup\n"));
    printf(_("      --skip-block-validation      set to validate only file-level checksum\n"));
    printf(_("  -E, --external-dirs=external-directories-paths\n"));
    printf(_("                                   backup some directories not from pgdata \n"));
    printf(_("                                   (example: --external-dirs=/tmp/dir1:/tmp/dir2)\n"));
    printf(_("      --no-sync                    do not sync backed up files to disk\n"));
    printf(_("      --note=text                  add note to backup\n"));
    printf(_("                                   (example: --note='backup before app update to v13.1')\n"));
    printf(_("      --archive-timeout=timeout    wait timeout for WAL segment archiving (default: 5min)\n"));
    printf(_("      --backup-pg-replslot]        backup of '%s' directory\n"), PG_REPLSLOT_DIR);

    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n"));

    printf(_("\n  Retention options:\n"));
    printf(_("      --delete-expired             delete backups expired according to current\n"));
    printf(_("                                   retention policy after successful backup completion\n"));
    printf(_("      --merge-expired              merge backups expired according to current\n"));
    printf(_("                                   retention policy after successful backup completion\n"));
    printf(_("      --delete-wal                 remove redundant files in WAL archive\n"));
    printf(_("      --retention-redundancy=retention-redundancy\n"));
    printf(_("                                   number of full backups to keep; 0 disables; (default: 0)\n"));
    printf(_("      --retention-window=retention-window\n"));
    printf(_("                                   number of days of recoverability; 0 disables; (default: 0)\n"));
    printf(_("      --wal-depth=wal-depth        number of latest valid backups per timeline that must\n"));
    printf(_("                                   retain the ability to perform PITR; 0 disables; (default: 0)\n"));
    printf(_("      --dry-run                    perform a trial run without any changes\n"));

    printf(_("\n  Compression options:\n"));
    printf(_("      --compress-algorithm=compress-algorithm\n"));
    printf(_("                                   available options: 'zlib', 'pglz', 'none' (default: none)\n"));
    printf(_("      --compress-level=compress-level\n"));
    printf(_("                                   level of compression [0-9] (default: 1)\n"));
    printf(_("      --compress                   alias for --compress-algorithm='zlib' and --compress-level=1\n"));

    printf(_("\n  Connection options:\n"));
    printf(_("  -U, --pguser=username            user name to connect as (default: current local user)\n"));
    printf(_("  -d, --pgdatabase=dbname          database to connect (default: username)\n"));
    printf(_("  -h, --pghost=hostname            database server host or socket directory(default: 'local socket')\n"));
    printf(_("  -p, --pgport=port                database server port (default: 5432)\n"));
    printf(_("  -w, --no-password                never prompt for password\n"));
    printf(_("  -W, --password=password          the password of specified database user\n"));

    printf(_("\n  Remote options:\n"));
    printf(_("      --remote-proto=protocol      remote protocol to use\n"));
    printf(_("                                   available options: 'ssh', 'none' (default: ssh)\n"));
    printf(_("      --remote-host=destination    remote host address or hostname\n"));
    printf(_("      --remote-port=port           remote host port (default: 22)\n"));
    printf(_("      --remote-path=path           path to directory with gs_probackup binary on remote host\n"));
    printf(_("                                   (default: current binary path)\n"));
    printf(_("      --remote-user=username       user name for ssh connection (default: current user)\n"));
    printf(_("      --remote-libpath=libpath         library path on remote host\n"));
    printf(_("      --ssh-options=ssh_options    additional ssh options (default: none)\n"));
    printf(_("                                   (example: --ssh-options='-c cipher_spec -F configfile')\n"));

    printf(_("\n  Pinning options:\n"));
    printf(_("      --ttl=interval               pin backup for specified amount of time; 0 unpin\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: s)\n"));
    printf(_("                                   (example: --ttl=20d)\n"));
    printf(_("      --expire-time=time           pin backup until specified time stamp\n"));
    printf(_("                                   (example: --expire-time='2024-01-01 00:00:00+03')\n\n"));
}

static void help_restore(void)
{
    printf(_("\n%s restore -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-D pgdata-path] [-i backup-id] [-j threads_num] [--progress]\n"));
    printf(_("                 [--force] [--no-sync] [--no-validate] [--skip-block-validation]\n"));
    printf(_("                 [--external-mapping=OLDDIR=NEWDIR] [-T OLDDIR=NEWDIR]\n"));
    printf(_("                 [--skip-external-dirs] [-I incremental_mode]\n"));
    printf(_("                 [--recovery-target-time=time|--recovery-target-xid=xid\n"));
    printf(_("                  |--recovery-target-lsn=lsn|--recovery-target-name=target-name]\n"));
    printf(_("                 [--recovery-target-inclusive=boolean]\n"));
    printf(_("                 [--remote-proto=protocol] [--remote-host=destination]\n"));
    printf(_("                 [--remote-path=path] [--remote-user=username]\n"));
    printf(_("                 [--remote-port=port] [--ssh-options=ssh_options]\n"));
    printf(_("                 [--remote-libpath=libpath]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -D, --pgdata=pgdata-path         location of the database storage area\n"));
    printf(_("  -i, --backup-id=backup-id        backup to restore\n"));
    printf(_("  -j, --threads=threads_num        number of parallel threads\n"));
    printf(_("      --progress                   show progress\n"));
    printf(_("      --force                      ignore invalid status of the restored backup\n"));
    printf(_("      --no-sync                    do not sync restored files to disk\n"));
    printf(_("      --no-validate                disable backup validation during restore\n"));
    printf(_("      --skip-block-validation      set to validate only file-level checksum\n"));
    printf(_("      --external-mapping=OLDDIR=NEWDIR\n"));
    printf(_("                                   relocate the external directory from OLDDIR to NEWDIR\n"));
    printf(_("  -T, --tablespace-mapping=OLDDIR=NEWDIR\n"));
    printf(_("                                   relocate the tablespace from directory OLDDIR to NEWDIR\n"));
    printf(_("      --skip-external-dirs         do not restore all external directories\n"));
    printf(_("  -I, --incremental-mode=none|checksum|lsn\n"));
    printf(_("                                   reuse valid pages available in PGDATA if they have not changed\n"));
    printf(_("                                   (default: none)\n"));

    printf(_("\n  Recovery options:\n"));
    printf(_("      --recovery-target-time=time  time stamp up to which recovery will proceed\n"));
    printf(_("      --recovery-target-xid=xid    transaction ID up to which recovery will proceed\n"));
    printf(_("      --recovery-target-lsn=lsn    LSN of the write-ahead log location up to which recovery will proceed\n"));
    printf(_("      --recovery-target-name=target-name\n"));
    printf(_("                                   the named restore point to which recovery will proceed\n"));
    printf(_("      --recovery-target-inclusive=boolean\n"));
    printf(_("                                   whether we stop just after the recovery target\n"));

    printf(_("\n  Remote options:\n"));
    printf(_("      --remote-proto=protocol      remote protocol to use\n"));
    printf(_("                                   available options: 'ssh', 'none' (default: ssh)\n"));
    printf(_("      --remote-host=destination    remote host address or hostname\n"));
    printf(_("      --remote-port=port           remote host port (default: 22)\n"));
    printf(_("      --remote-path=path           path to directory with gs_probackup binary on remote host\n"));
    printf(_("                                   (default: current binary path)\n"));
    printf(_("      --remote-user=username       user name for ssh connection (default: current user)\n"));
    printf(_("      --remote-libpath=libpath         library path on remote host\n"));
    printf(_("      --ssh-options=ssh_options    additional ssh options (default: none)\n"));
    printf(_("                                   (example: --ssh-options='-c cipher_spec -F configfile')\n"));

    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n\n"));
}

static void help_merge(void)
{
    printf(_("\n%s merge -B backup-path --instance=instance_name -i backup-id\n"), PROGRAM_NAME);
    printf(_("                 [-j threads_num] [--progress]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -i, --backup-id=backup-id        backup to merge\n"));
    printf(_("  -j, --threads=threads_num        number of parallel threads\n"));
    printf(_("      --progress                   show progress\n"));

    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n\n"));
}

static void help_delete(void)
{
    printf(_("\n%s delete -B backup-path --instance=instance_name\n"), PROGRAM_NAME);
    printf(_("                 [-i backup-id | --delete-expired | --merge-expired | --status=backup_status]\n"));
    printf(_("                 [--delete-wal] [-j threads_num] [--progress]\n"));
    printf(_("                 [--retention-redundancy=retention-redundancy]\n"));
    printf(_("                 [--retention-window=retention-window]\n"));
    printf(_("                 [--wal-depth=wal-depth] [--dry-run]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -i, --backup-id=backup-id        backup to delete\n"));
    printf(_("      --status=backup_status       delete all backups with specified status\n"));
    printf(_("  -j, --threads=threads_num        number of parallel threads\n"));
    printf(_("      --progress                   show progress\n"));

    printf(_("\n  Retention options:\n"));
    printf(_("      --delete-expired             delete backups expired according to current\n"));
    printf(_("                                   retention policy\n"));
    printf(_("      --merge-expired              merge backups expired according to current\n"));
    printf(_("                                   retention policy\n"));
    printf(_("      --delete-wal                 remove redundant files in WAL archive\n"));
    printf(_("      --retention-redundancy=retention-redundancy\n"));
    printf(_("                                   number of full backups to keep; 0 disables; (default: 0)\n"));
    printf(_("      --retention-window=retention-window\n"));
    printf(_("                                   number of days of recoverability; 0 disables; (default: 0)\n"));
    printf(_("      --wal-depth=wal-depth        number of latest valid backups per timeline that must\n"));
    printf(_("                                   retain the ability to perform PITR; 0 disables; (default: 0)\n"));
    printf(_("      --dry-run                    perform a trial run without any changes\n"));

    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n\n"));
}

static void help_validate(void)
{
    printf(_("\n%s validate -B backup-path\n"), PROGRAM_NAME);
    printf(_("                 [--instance=instance_name] [-i backup-id]\n"));
    printf(_("                 [-j threads_num] [--progress] [--skip-block-validation]\n"));
    printf(_("                 [--recovery-target-time=time|--recovery-target-xid=xid\n"));
    printf(_("                  |--recovery-target-lsn=lsn|--recovery-target-name=target-name]\n"));
    printf(_("                 [--recovery-target-inclusive=boolean]\n"));
    printf(_("                 [--log-level-console=log-level-console]\n"));
    printf(_("                 [--log-level-file=log-level-file]\n"));
    printf(_("                 [--log-filename=log-filename]\n"));
    printf(_("                 [--error-log-filename=error-log-filename]\n"));
    printf(_("                 [--log-directory=log-directory]\n"));
    printf(_("                 [--log-rotation-size=log-rotation-size]\n"));
    printf(_("                 [--log-rotation-age=log-rotation-age]\n\n"));

    printf(_("  -B, --backup-path=backup-path    location of the backup storage area\n"));
    printf(_("      --instance=instance_name     name of the instance\n"));
    printf(_("  -i, --backup-id=backup-id        backup to validate\n"));
    printf(_("  -j, --threads=threads_num        number of parallel threads\n"));
    printf(_("      --progress                   show progress\n"));
    printf(_("      --skip-block-validation      set to validate only file-level checksum\n"));

    printf(_("\n  Recovery options:\n"));
    printf(_("      --recovery-target-time=time  time stamp up to which recovery will proceed\n"));
    printf(_("      --recovery-target-xid=xid    transaction ID up to which recovery will proceed\n"));
    printf(_("      --recovery-target-lsn=lsn    LSN of the write-ahead log location up to which recovery will proceed\n"));
    printf(_("      --recovery-target-name=target-name\n"));
    printf(_("                                   the named restore point to which recovery will proceed\n"));
    printf(_("      --recovery-target-inclusive=boolean\n"));
    printf(_("                                   whether we stop just after the recovery target\n"));
    
    printf(_("\n  Logging options:\n"));
    printf(_("      --log-level-console=log-level-console\n"));
    printf(_("                                   level for console logging (default: info)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-level-file=log-level-file\n"));
    printf(_("                                   level for file logging (default: off)\n"));
    printf(_("                                   available options: 'off', 'error', 'warning', 'info', 'log', 'verbose'\n"));
    printf(_("      --log-filename=log-filename\n"));
    printf(_("                                   filename for file logging (default: 'pg_probackup.log')\n"));
    printf(_("                                   support strftime format (example: pg_probackup-%%Y-%%m-%%d_%%H%%M%%S.log)\n"));
    printf(_("      --error-log-filename=error-log-filename\n"));
    printf(_("                                   filename for error logging (default: none)\n"));
    printf(_("      --log-directory=log-directory\n"));
    printf(_("                                   directory for file logging (default: BACKUP_PATH/log)\n"));
    printf(_("      --log-rotation-size=log-rotation-size\n"));
    printf(_("                                   rotate logfile if its size exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'kB', 'MB', 'GB', 'TB' (default: kB)\n"));
    printf(_("      --log-rotation-age=log-rotation-age\n"));
    printf(_("                                   rotate logfile if its age exceeds this value; 0 disables; (default: 0)\n"));
    printf(_("                                   available units: 'ms', 's', 'min', 'h', 'd' (default: min)\n\n"));
}
