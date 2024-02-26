# src/bin/pg_rewind/nls.mk
CATALOG_NAME     = pg_rewind
AVAIL_LANGUAGES  =de
GETTEXT_FILES    = datapagemap.cpp fetch.cpp filemap.cpp file_ops.cpp logging.cpp parsexlog.cpp pg_rewind.cpp
#copy_fetch.cpp datapagemap.cpp fetch.cpp file_ops.cpp filemap.cpp libpq_fetch.cpp logging.cpp parsexlog.cpp pg_rewind.cpp compressed_rewind.cpp timeline.cpp ../../common/fe_memutils.cpp ../../common/restricted_token.cpp ../../../src/backend/access/transam/xlogreader.cpp

GETTEXT_TRIGGERS = pg_log:2 pg_fatal report_invalid_record:2
GETTEXT_FLAGS    = pg_log:2:c-format \
    pg_fatal:1:c-format \
    report_invalid_record:2:c-format
