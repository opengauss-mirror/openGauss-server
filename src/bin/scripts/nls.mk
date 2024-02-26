# src/bin/.cppripts/nls.mk
CATALOG_NAME     = pg.cppripts
AVAIL_LANGUAGES  = cs de es fr it ja ko pl pt_BR ro ru sv tr zh_CN zh_TW
GETTEXT_FILES    = createdb.cpp createlang.cpp createuser.cpp \
                   dropdb.cpp droplang.cpp dropuser.cpp \
                   dropdb.cpp droplang.cpp dropuser.cpp \
                   common.cpp
GETTEXT_TRIGGERS = simple_prompt yesno_prompt
