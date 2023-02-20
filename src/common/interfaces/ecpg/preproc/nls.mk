# src/interfaces/ecpg/preproc/nls.mk
CATALOG_NAME     = ecpg
AVAIL_LANGUAGES  = cs de es fr it ja ko pl pt_BR ru tr zh_CN zh_TW
GETTEXT_FILES    = descriptor.cpp ecpg.cpp pgc.cpp preproc.cpp type.cpp variable.cpp
GETTEXT_TRIGGERS = mmerror:3
GETTEXT_FLAGS    = mmerror:3:c-format
