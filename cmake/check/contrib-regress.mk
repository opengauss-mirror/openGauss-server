# Keep pg_regress in the extension directory and copy the existing CMake install tree.

CMAKE_CHECK_SOURCE_DIR = $(abspath ../..)
CMAKE_CHECK_INPUT_DIR = $(CURDIR)
CMAKE_CHECK_PG_REGRESS ?= $(CMAKE_CHECK_BUILD_DIR)/src/test/regress/pg_regress_single
CMAKE_CHECK_LOCALKMS_DIR = $(CMAKE_CHECK_INSTALL_PREFIX)/etc/localkms

.PHONY: check plugin_check install clean

plugin_check: check

install:
	if [ -n "$(DESTDIR)" ]; then mkdir -p "$(DESTDIR)$(CMAKE_CHECK_INSTALL_PREFIX)" && cp -R "$(CMAKE_CHECK_INSTALL_PREFIX)/." "$(DESTDIR)$(CMAKE_CHECK_INSTALL_PREFIX)/"; else $(MAKE) -C "$(CMAKE_CHECK_BUILD_DIR)" install; fi

check: $(CMAKE_CHECK_PREP)
	test -f "$(CMAKE_CHECK_BUILD_DIR)/Makefile" || { echo "missing CMake-generated Makefile: $(CMAKE_CHECK_BUILD_DIR)/Makefile"; exit 1; }
	test -x "$(CMAKE_CHECK_PG_REGRESS)" || { echo "missing pg_regress_single: $(CMAKE_CHECK_PG_REGRESS)"; exit 1; }
	test -d "$(CMAKE_CHECK_INSTALL_PREFIX)" || { echo "missing CMake install: $(CMAKE_CHECK_INSTALL_PREFIX)"; exit 1; }
	mkdir -p "$(CMAKE_CHECK_LOCALKMS_DIR)"
	LOCALKMS_FILE_PATH="$(CMAKE_CHECK_LOCALKMS_DIR)" GAUSSHOME="$(CMAKE_CHECK_INSTALL_PREFIX)" LD_LIBRARY_PATH="$(CMAKE_CHECK_INSTALL_PREFIX)/lib:$(CMAKE_CHECK_INSTALL_PREFIX)/lib/postgresql:$$LD_LIBRARY_PATH" CODE_BASE_SRC="$(CMAKE_CHECK_SOURCE_DIR)/src" PREFIX_HOME="$(CMAKE_CHECK_INSTALL_PREFIX)" "$(CMAKE_CHECK_PG_REGRESS)" --inputdir="$(CMAKE_CHECK_INPUT_DIR)" --outputdir="$(CMAKE_CHECK_INPUT_DIR)" --temp-install="$(CMAKE_CHECK_INPUT_DIR)/tmp_check" --top-builddir="$(CMAKE_CHECK_BUILD_DIR)" $(REGRESS_OPTS) $(REGRESS)

clean:
	rm -rf tmp_check log regression.diffs regression.out results $(CMAKE_CHECK_CLEAN_FILES)
