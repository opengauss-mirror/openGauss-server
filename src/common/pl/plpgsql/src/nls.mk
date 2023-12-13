# src/pl/plpgsql/s.cpp/nls.mk
CATALOG_NAME     = plpgsql
AVAIL_LANGUAGES  = cs de es fr it ja pl pt_BR ro ru zh_CN zh_TW
GETTEXT_FILES    = pl_comp.cpp pl_exec.cpp pl_gram.cpp pl_funcs.cpp pl_handler.cpp pl_scanner.cpp
GETTEXT_TRIGGERS = $(BACKEND_COMMON_GETTEXT_TRIGGERS) yyerror plpgsql_yyerror
GETTEXT_FLAGS    = $(BACKEND_COMMON_GETTEXT_FLAGS)
