#This is the main CMAKE for build libstrncoll.so.
#
# LGPL-licensed custom strncoll() (adapted from glibc).  Built as a standalone
# shared library so it can be replaced/relinked independently by end users, as
# required by the LGPL.  Dynamically linked into gaussdb; see
# src/gausskernel/CMakeLists.txt for the -lstrncoll link.
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_strncoll_SRC)
set(TGT_strncoll_INC
    ${PROJECT_SRC_DIR}/include
)
set(strncoll_DEF_OPTIONS ${MACRO_OPTIONS} -DHAVE_LOCALE_T=1)
set(strncoll_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(strncoll_LINK_OPTIONS ${LIB_LINK_OPTIONS})
add_shared_libtarget(strncoll TGT_strncoll_SRC TGT_strncoll_INC "${strncoll_DEF_OPTIONS}" "${strncoll_COMPILE_OPTIONS}" "${strncoll_LINK_OPTIONS}")
install(TARGETS strncoll LIBRARY DESTINATION lib)
