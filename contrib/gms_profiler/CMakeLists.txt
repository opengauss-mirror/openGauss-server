#This is the main CMAKE for build all gms_profiler.
# gms_profiler.so

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} TGT_gms_profiler_SRC)

SET(TGT_gms_profiler_INC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

set(gms_profiler_DEF_OPTIONS ${MACRO_OPTIONS} -DNDP_CLIENT -DGlobalCache)
set(gms_profiler_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(gms_profiler_LINK_OPTIONS ${LIB_LINK_OPTIONS})

add_shared_libtarget(gms_profiler TGT_gms_profiler_SRC TGT_gms_profiler_INC "${gms_profiler_DEF_OPTIONS}" "${gms_profiler_COMPILE_OPTIONS}" "${gms_profiler_LINK_OPTIONS}")
set_target_properties(gms_profiler PROPERTIES PREFIX "")

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_profiler.control
    DESTINATION share/postgresql/extension/
)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/gms_profiler--1.0.sql
    DESTINATION share/postgresql/extension/
)

install(TARGETS gms_profiler DESTINATION lib/postgresql)
