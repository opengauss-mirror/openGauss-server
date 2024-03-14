subdir = src/common/interfaces/ecpg/include
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I../include -I$(top_srcdir)/src/common/interfaces/ecpg/include \
	-I$(libpq_srcdir) -I$(top_builddir)/src/common/port $(CPPFLAGS)

informix_esql_dir = $(pkgincludedir)/informix/esql
ecpg_config_h = $(top_builddir)/src/common/interfaces/ecpg/include/ecpg_config.h

all: $(ecpg_config_h)

install: all installdirs install-headers

.PHONY: install-headers
ecpg_headers = ecpgerrno.h ecpglib.h ecpgtype.h sqlca.h sql3types.h ecpg_informix.h \
	pgtypes_error.h pgtypes_numeric.h pgtypes_timestamp.h pgtypes_date.h pgtypes_interval.h \
	sqlda.h sqlda-compat.h sqlda-native.h
informix_headers = datetime.h decimal.h sqltypes.h

install-headers: $(ecpg_headers) $(informix_headers)
	$(INSTALL_DATA) $(addprefix $(srcdir)/,$(ecpg_headers)) '$(DESTDIR)$(includedir)/'
	$(INSTALL_DATA) $(addprefix $(srcdir)/,$(informix_headers)) '$(DESTDIR)$(informix_esql_dir)/'
	$(INSTALL_DATA) $(ecpg_config_h) '$(DESTDIR)$(includedir)'

	$(MKDIR_P) '$(DESTDIR)$(includedir_server)/ecpg'
	for header_file in $(wildcard $(srcdir)/*.h); do \
		$(INSTALL_DATA) $$header_file '$(DESTDIR)$(includedir_server)/ecpg' || exit; \
	done

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(includedir)' '$(DESTDIR)$(informix_esql_dir)'


uninstall:
	rm -f $(addprefix '$(DESTDIR)$(includedir)'/, $(ecpg_headers))
	rm -f $(addprefix '$(DESTDIR)$(informix_esql_dir)'/, $(informix_headers))
	rm -f '$(DESTDIR)$(includedir)'/$(notdir $(ecpg_config_h))

distclean maintainer-clean:
	rm -f ecpg_config.h stamp-h
	rm -r '$(DESTDIR)$(includedir_server)/ecpg'
