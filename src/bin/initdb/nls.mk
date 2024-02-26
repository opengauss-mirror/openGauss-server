# src/bin/initdb/nls.mk
CATALOG_NAME     = initdb
AVAIL_LANGUAGES  = cs de es fr it ja ko pl pt_BR ro ru sv tr zh_CN zh_TW
GETTEXT_FILES    = findtimezone.cpp initdb.cpp ../../common/port/dirmod.cpp ../../common/port/exec.cpp
GETTEXT_TRIGGERS = simple_prompt
