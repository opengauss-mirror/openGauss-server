# src/bin/psql/nls.mk
CATALOG_NAME     = psql
AVAIL_LANGUAGES  = cs de es fr it ja pl pt_BR ru zh_CN zh_TW
GETTEXT_FILES    = command.cpp common.cpp copy.cpp help.cpp input.cpp large_obj.cpp \
                   mainloop.cpp print.cpp psqlscan.inc startup.cpp describe.cpp sql_help.h sql_help.cpp \
                   tab-complete.cpp variables.cpp \
                   ../../common/port/exec.cpp 

GETTEXT_TRIGGERS = N_ psql_error simple_prompt
GETTEXT_FLAGS    = psql_error:1:c-format
