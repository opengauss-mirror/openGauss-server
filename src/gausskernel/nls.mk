# src/gausskernel/nls.mk
CATALOG_NAME     = gaussdb
AVAIL_LANGUAGES  = de es fr it ja pl pt_BR ru zh_CN zh_TW
GETTEXT_FILES    = + gettext-files
GETTEXT_TRIGGERS = $(BACKEND_COMMON_GETTEXT_TRIGGERS) \
    GUC_check_errmsg GUC_check_errdetail GUC_check_errhint \
    write_stderr yyerror parser_yyerror AlarmLog:2 report_invalid_record:2
GETTEXT_FLAGS    = $(BACKEND_COMMON_GETTEXT_FLAGS) \
    GUC_check_errmsg:1:c-format \
    GUC_check_errdetail:1:c-format \
    GUC_check_errhint:1:c-format \
    write_stderr:1:c-format \
    AlarmLog:2:c-format \
    report_invalid_record:2:c-format

gettext-files: distprep
	find $(srcdir)/ $(srcdir)/../common/backend/  $(srcdir)/../common/port/ -name '*.cpp' -o -name '*.l'| LC_ALL=C sort >$@

my-clean:
	rm -f gettext-files

.PHONY: my-clean
clean: my-clean
