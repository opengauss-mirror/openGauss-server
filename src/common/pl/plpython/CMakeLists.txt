cmake_minimum_required(VERSION 3.16.5)
project(plpython CXX)

# ==============================================================================
# Configuration Options
# ==============================================================================
option(ENABLE_PYTHON_EMBED "Install Python runtime into PG directory (non-standard)" OFF)

# ==============================================================================
# Find Python
# ==============================================================================
# Default to Python 3 (Python 2 is obsolete as of 2026)
set(3RD_PATH_PY "$ENV{THIRD_BIN_PATH}/kernel/platform/python3.7/")

message("3RD_PATH_PY is ${3RD_PATH_PY}")
if(IS_DIRECTORY ${3RD_PATH_PY})
    message("${3RD_PATH_PY} is found")
    set(Python3_ROOT_DIR ${3RD_PATH_PY})
    set(Python3_EXECUTABLE "${3RD_PATH_PY}/bin/python3")
    set(Python3_LIBRARY "${3RD_PATH_PY}/lib/libpython3.7m.so")
endif()
    
find_package(Python3 3.7 REQUIRED COMPONENTS Interpreter Development)

# Get Python version information
set(PYTHON_MAJOR_VERSION ${Python3_VERSION_MAJOR})
set(PYTHON_VERSION ${Python3_VERSION})
set(PYTHON_INCLUDE_SPEC -I${Python3_INCLUDE_DIRS})
set(PYTHON_LIB_DIR ${Python3_LIBRARY_DIRS})

# Determine extension name (e.g., plpython3u)
set(PLPYTHON_NAME "plpython${PYTHON_MAJOR_VERSION}")

message(NOTICE "Python3_LIBRARY = ${Python3_LIBRARY}")
message(NOTICE "Python3_LIBRARY_DIRS = ${Python3_LIBRARY_DIRS}")
message(NOTICE "Python3_INCLUDE_DIRS = ${Python3_INCLUDE_DIRS}")

# ==============================================================================
# Platform Specific Checks (对应 original Makefile shared_libpython check)
# ==============================================================================
set(SHARED_LIBPYTHON FALSE)

# Check if libpython is a shared library
# CMake's FindPython usually handles this, but we ensure we don't link static libs to shared modules
if(NOT Python3_LIBRARY MATCHES "\\.(a|lib)$")
    set(SHARED_LIBPYTHON TRUE)
endif()

# Windows specific handling
if(WIN32)
    set(SHARED_LIBPYTHON TRUE)
    # Convert backslashes to forward slashes
    string(REPLACE "\\" "/" PYTHON_INCLUDE_SPEC "${PYTHON_INCLUDE_SPEC}")
endif()

# Darwin (macOS) specific handling
if(APPLE)
    set(SHARED_LIBPYTHON TRUE)
    # CMake FindPython on macOS usually handles Frameworks automatically
endif()

if(NOT SHARED_LIBPYTHON)
    message(FATAL_ERROR "Cannot build PL/Python because libpython is not a shared library. You might have to rebuild your Python installation.")
endif()

# ==============================================================================
# Source Files
# ==============================================================================
# Note: User specified source files are .cpp
set(PLPYTHON_SRCS
    plpy_cursorobject.cpp
    plpy_elog.cpp
    plpy_exec.cpp
    plpy_main.cpp
    plpy_planobject.cpp
    plpy_plpymodule.cpp
    plpy_procedure.cpp
    plpy_resultobject.cpp
    plpy_spi.cpp
    plpy_subxactobject.cpp
    plpy_typeio.cpp
    plpy_util.cpp
)

# ==============================================================================
# Windows Import Library Generation (MinGW + MSVC Python special case)
# ==============================================================================
if(WIN32 AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    # Simulate original Makefile dlltool logic
    string(REPLACE "." "" PYTVERSTR "${PYTHON_VERSION}")
    set(PYTHON_DEF_FILE "python${PYTVERSTR}.def")
    set(PYTHON_IMPORT_LIB "libpython${PYTVERSTR}.a")

    # Find python.dll location (usually alongside Python3_EXECUTABLE or in System32)
    get_filename_component(PYTHON_BIN_DIR ${Python3_EXECUTABLE} DIRECTORY)
    find_file(PYTHON_DLL_PATH NAMES python${PYTVERSTR}.dll PATHS ${PYTHON_BIN_DIR} $ENV{WINDIR}/System32)

    if(PYTHON_DLL_PATH)
        add_custom_command(
            OUTPUT ${PYTHON_DEF_FILE}
            COMMAND pexports ${PYTHON_DLL_PATH} > ${PYTHON_DEF_FILE}
            DEPENDS ${PYTHON_DLL_PATH}
            COMMENT "Generating python.def from python.dll"
        )
        add_custom_command(
            OUTPUT ${PYTHON_IMPORT_LIB}
            COMMAND dlltool --dllname python${PYTVERSTR}.dll --def ${PYTHON_DEF_FILE} --output-lib ${PYTHON_IMPORT_LIB}
            DEPENDS ${PYTHON_DEF_FILE}
            COMMENT "Generating libpython.a import library"
        )
        list(APPEND PLPYTHON_SRCS ${PYTHON_IMPORT_LIB})
        set(PYTHON_LIB_SPEC "") # Included in OBJS
    else()
        message(WARNING "Could not find python.dll for import lib generation. Standard linking will be attempted.")
        set(PYTHON_LIB_SPEC ${Python3_LIBRARIES})
    endif()
else()
    set(PYTHON_LIB_SPEC ${Python3_LIBRARIES})
endif()

# ==============================================================================
# Generate spiexceptions.h
# ==============================================================================
# Original Makefile depends on generate-spiexceptions.pl and errcodes.txt
find_package(Perl REQUIRED)

set(ERRCODES_TXT "${CMAKE_CURRENT_SOURCE_DIR}/../../../../src/common/backend/utils/errcodes.txt")
set(GEN_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/generate-spiexceptions.pl")

if(EXISTS ${ERRCODES_TXT} AND EXISTS ${GEN_SCRIPT})
    add_custom_command(
        OUTPUT spiexceptions.h
        COMMAND ${PERL_EXECUTABLE} ${GEN_SCRIPT} ${ERRCODES_TXT} > ${CMAKE_CURRENT_SOURCE_DIR}/spiexceptions.h
        DEPENDS ${ERRCODES_TXT} ${GEN_SCRIPT}
        COMMENT "Generating spiexceptions.h"
    )
    set(GENERATED_HEADERS spiexceptions.h)
else()
    # If building independently without PG source tree, try to find existing or skip
    message(WARNING "spiexceptions.h generation sources not found. Assuming pre-generated or system provided.")
    set(GENERATED_HEADERS "")
endif()

# ==============================================================================
# Build Shared Library
# ==============================================================================
add_library(${PLPYTHON_NAME} MODULE ${PLPYTHON_SRCS} ${GENERATED_HEADERS})

# Set include directories
#target_include_directories(${PLPYTHON_NAME} PRIVATE
#    ${CMAKE_CURRENT_SOURCE_DIR}
#    ${PG_INCLUDE_DIR}
#    ${CMAKE_INCLUDE_PATH}
#    ${Python3_INCLUDE_DIRS}
#)

#INCLUDE_DIRECTORIES(${Python3_INCLUDE_DIRS},  ${PROJECT_SRC_DIR}/include, ${PROJECT_SRC_DIR}/include/nodes)

set(TGT_INC
    ${PROJECT_SRC_DIR}/include
    ${PROJECT_SRC_DIR}/include/nodes
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_INCLUDE_PATH}
)

# Compile definitions
target_compile_definitions(${PLPYTHON_NAME} PRIVATE
    -DPLPYTHON_NAME="${PLPYTHON_NAME}"
    PGXC
    _REENTRANT
    _THREAD_SAFE
    _POSIX_PTHREAD_SEMANTICS
    USE_SPQ
)

target_include_directories(${PLPYTHON_NAME} PRIVATE ${TGT_INC})
# Link libraries
target_link_libraries(${PLPYTHON_NAME} PRIVATE
    Python3::Python -ldl -lm
)

# Set output properties (PG extensions usually do not need 'lib' prefix)
set_target_properties(${PLPYTHON_NAME} PROPERTIES
    PREFIX ""
    OUTPUT_NAME ${PLPYTHON_NAME}
)

# Ensure Position Independent Code (PIC)
set_target_properties(${PLPYTHON_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# macOS Framework special link flags
if(APPLE)
    set_target_properties(${PLPYTHON_NAME} PROPERTIES
        LINK_FLAGS "-framework Python"
    )
endif()

# ==============================================================================
# Data Files (SQL & Control)
# ==============================================================================
set(PLPYTHON_DATA
    "${PLPYTHON_NAME}u.control"
    "${PLPYTHON_NAME}u--1.0.sql"
    "${PLPYTHON_NAME}u--unpackaged--1.0.sql"
)

# ==============================================================================
# Append test cases to PARTA
# ==============================================================================
# 1. Configuration parameters (replace with actual file paths)
set(TARGET_FILE "${CMAKE_CURRENT_SOURCE_DIR}/../../../../src/test/regress/parallel_schedule0A")
set(MARKER_LINE "#support plpython3u")

# Define the content to append (keep \n at the beginning and end to prevent merging with existing lines)
set(CONTENT_TO_APPEND "\n${MARKER_LINE}
test: plpython3u/plpython_schema
test: plpython3u/plpython_populate
test: plpython3u/plpython_test
test: plpython3u/plpython_do
test: plpython3u/plpython_global
test: plpython3u/plpython_global_session
test: plpython3u/plpython_import
test: plpython3u/plpython_spi
test: plpython3u/plpython_newline plpython3u/plpython_void plpython3u/plpython_params plpython3u/plpython_setof plpython3u/plpython_record
test: plpython3u/plpython_types
test: plpython3u/plpython_error
test: plpython3u/plpython_unicode plpython3u/plpython_quote
test: plpython3u/plpython_composite
test: plpython3u/plpython_subtransaction
test: plpython3u/plpython_gms_xslprocessor plpython3u/plpython_gms_xmldom
test: plpython3u/plpython_drop")

# 2. Core logic
if(EXISTS "${TARGET_FILE}")
    # Read the entire file content
    file(READ "${TARGET_FILE}" FILE_CONTENT)
    
    # Check if the marker line exists
    string(FIND "${FILE_CONTENT}" "${MARKER_LINE}" MARKER_POS)
    
    # If not found (returns -1), append the content
    if(MARKER_POS EQUAL -1)
        file(APPEND "${TARGET_FILE}" "${CONTENT_TO_APPEND}")
        message(STATUS "ppended plpython3u support content to ${TARGET_FILE}")
    else()
        message(STATUS "Marker '${MARKER_LINE}' already exists in the file, skipping append")
    endif()
else()
    message(FATAL_ERROR "Target file does not exist, please check the path: ${TARGET_FILE}")
endif()

# ==============================================================================
# Test Configuration (pg_regress)
# ==============================================================================

#if(PG_REGRESS)
if(FALSE)
    find_program(PG_REGRESS pg_regress PATHS ${PG_BIN_DIR})
    # Prepare Python 3 test files (simulating prep3 target)
    set(TEST_SQL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/sql")
    set(TEST_EXPECTED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/expected")
    set(TEST_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/python3")

    file(MAKE_DIRECTORY "${TEST_BUILD_DIR}/sql")
    file(MAKE_DIRECTORY "${TEST_BUILD_DIR}/expected")

    # Find sed for text transformation
    find_program(SED_EXECUTABLE NAMES sed gsed)
    find_program(SHELL_EXECUTABLE NAMES sh bash)

    if(SED_EXECUTABLE AND SHELL_EXECUTABLE)
        # Create a script to process files to avoid complex CMake loops
        set(PREP_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/prep_tests.sh")
        file(WRITE ${PREP_SCRIPT}
            "#!/bin/sh\n"
            "mkdir -p ${TEST_BUILD_DIR}/sql ${TEST_BUILD_DIR}/expected\n"
            "for file in ${TEST_SQL_DIR}/*.sql ${TEST_EXPECTED_DIR}/*.out; do\n"
            "  if [ -f \"$file\" ]; then\n"
            "    outfile=\"${TEST_BUILD_DIR}/$file\"\n"
            "    mkdir -p $(dirname \"$outfile\")\n"
            "    sed -e 's/except \\([[:alpha:]][[:alpha:].]*\\), *\\([[:alpha:]][[:alpha:]]*\\):/except \\1 as \\2:/g' \\\n"
            "        -e \"s/<type 'exceptions\\.\\([[:alpha:]]*\\)'>/<class '\\1'>/g\" \\\n"
            "        -e \"s/<type 'long'>/<class 'int'>/g\" \\\n"
            "        -e \"s/\\([0-9][0-9]*\\)L/\\1/g\" \\\n"
            "        -e 's/\\([ [{]\\)u\"/\\1\"/g' \\\n"
            "        -e \"s/\\([ [{]\\)u'/\\1'/g\" \\\n"
            "        -e \"s/def next/def __next__/g\" \\\n"
            "        -e \"s/LANGUAGE plpythonu/LANGUAGE ${PLPYTHON_NAME}/g\" \\\n"
            "        -e \"s/LANGUAGE plpython2u/LANGUAGE ${PLPYTHON_NAME}/g\" \\\n"
            "        -e \"s/EXTENSION plpythonu/EXTENSION ${PLPYTHON_NAME}/g\" \\\n"
            "        -e \"s/EXTENSION plpython2u/EXTENSION ${PLPYTHON_NAME}/g\" \\\n"
            "        \"$file\" > \"$outfile\"\n"
            "  fi\n"
            "done\n"
        )

        add_custom_target(prep3
            COMMAND ${SHELL_EXECUTABLE} ${PREP_SCRIPT}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMENT "Preparing Python 3 regression tests"
        )

        # Define test
        add_test(NAME plpython_regress
            COMMAND ${PG_REGRESS} --inputdir=./python3 --outputdir=./python3 --dbname=postgres ${PLPYTHON_NAME}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        )
        set_tests_properties(plpython_regress PROPERTIES
            DEPENDS prep3
        )
    else()
        message(WARNING "sed or shell not found. Regression test preparation skipped.")
    endif()
endif()

# ==============================================================================
# Install Rules
# ==============================================================================
install(TARGETS ${PLPYTHON_NAME}
    LIBRARY DESTINATION lib/postgresql/
)

install(FILES ${PLPYTHON_DATA}
    DESTINATION share/postgresql/extension/
)

if(ENABLE_PYTHON3)
    get_filename_component(PYTHON_BIN_DIR ${Python3_EXECUTABLE} DIRECTORY)
    get_filename_component(PYTHON_LIB_DIR ${Python3_LIBRARY} DIRECTORY)
    #get_filename_component(PYTHON_PARENT_DIR ${PYTHON_BIN_DIR} DIRECTORY)
    set(PYTHON_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/python")
    message(NOTICE "PYTHON_PARENT_DIR = ${PYTHON_PARENT_DIR}")
    file(GLOB python_bins "${PYTHON_BIN_DIR}/python*")
    file(GLOB python_libs "${PYTHON_LIB_DIR}/libpython3.*")
    file(GLOB python_lib_dirs LIST_DIRECTORIES true "${PYTHON_LIB_DIR}/python3.*")
    message(NOTICE "python_bins = ${python_bins}")
    message(NOTICE "python_libs = ${python_libs}")
    message(NOTICE "python_lib_dirs = ${python_lib_dirs}")

    install(DIRECTORY DESTINATION "${PYTHON_INSTALL_DIR}/bin")
    install(DIRECTORY DESTINATION "${PYTHON_INSTALL_DIR}/lib64")
    install(FILES ${python_bins} DESTINATION "${PYTHON_INSTALL_DIR}/bin")
    install(FILES ${python_libs} DESTINATION "${PYTHON_INSTALL_DIR}/lib64")
    install(DIRECTORY ${python_lib_dirs} DESTINATION "${PYTHON_INSTALL_DIR}/lib64")
endif()

# ==============================================================================
# Clean / Distclean
# ==============================================================================
set_directory_properties(PROPERTIES
    ADDITIONAL_CLEAN_FILES
    "spiexceptions.h;${TEST_BUILD_DIR};python*.def;libpython*;${PREP_SCRIPT}"
)
