# src/bin/pg_dump/nls.mk
CATALOG_NAME     = pg_dump
AVAIL_LANGUAGES  = cs de es fr it ja pl pt_BR ru zh_CN
GETTEXT_FILES    = pg_backup_archiver.cpp pg_backup_db.cpp pg_backup_custom.cpp \
                   pg_backup_null.cpp pg_backup_tar.cpp \
                   pg_backup_directory.cpp dumpmem.cpp dumputils.cpp compress_io.cpp \
                   pg_dump.cpp common.cpp pg_dump_sort.cpp \
                   pg_restore.cpp pg_dumpall.cpp \
                   ../../port/exec.cpp
GETTEXT_TRIGGERS = write_msg:2 exit_horribly:2 simple_prompt \
                   ExecuteSqlCommand:3 ahlog:3 warn_or_exit_horribly:3
GETTEXT_FLAGS  = \
    write_msg:2:c-format \
    exit_horribly:2:c-format \
    ahlog:3:c-format \
    warn_or_exit_horribly:3:c-format
