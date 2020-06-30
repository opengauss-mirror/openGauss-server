# src/interfaces/libpq/nls.mk
CATALOG_NAME     = libpq
AVAIL_LANGUAGES  = cs de es fr it ja pl pt_BR ru tr zh_CN zh_TW
GETTEXT_FILES    = fe-auth.cpp fe-connect.cpp fe-exec.cpp fe-lobj.cpp fe-misc.cpp fe-protocol2.cpp fe-protocol3.cpp fe-secure.cpp win32.cpp
GETTEXT_TRIGGERS = libpq_gettext pqInternalNotice:2
GETTEXT_FLAGS    = libpq_gettext:1:pass-c-format pqInternalNotice:2:c-format
