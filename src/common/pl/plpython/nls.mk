# src/pl/plpython/nls.mk
CATALOG_NAME     = plpython
AVAIL_LANGUAGES  = cs de es fr it ja pl pt_BR ro ru zh_CN
GETTEXT_FILES    = plpy_cursorobject.cpp plpy_elog.cpp plpy_exec.cpp plpy_main.cpp plpy_planobject.cpp plpy_plpymodule.cpp \
                   plpy_procedure.cpp plpy_resultobject.cpp plpy_spi.cpp plpy_subxactobject.cpp plpy_typeio.cpp plpy_util.cpp
GETTEXT_TRIGGERS = $(BACKEND_COMMON_GETTEXT_TRIGGERS) PLy_elog:2 PLy_exception_set:2 PLy_exception_set_plural:2,3
GETTEXT_FLAGS    = $(BACKEND_COMMON_GETTEXT_FLAGS) \
    PLy_elog:2:c-format \
    PLy_exception_set:2:c-format \
    PLy_exception_set_plural:2:c-format \
    PLy_exception_set_plural:3:c-format
