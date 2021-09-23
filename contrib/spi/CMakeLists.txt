#This is the main CMAKE for build all spi.
# spi
set(spi_DEF_OPTIONS ${MACRO_OPTIONS} -DREFINT_VERBOSE)
set(spi_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS})
set(spi_LINK_OPTIONS ${LIB_LINK_OPTIONS})
AUX_SOURCE_DIRECTORY(${PROJECT_OPENGS_DIR}/contrib/spi TGT_spi_SRC)
foreach(subfile ${TGT_spi_SRC})
    execute_process(COMMAND basename ${subfile} .cpp OUTPUT_VARIABLE targetname OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(TGT_${targetname}_SRC subfile)
    add_shared_libtarget(${targetname} ${TGT_${targetname}_SRC} "" "${spi_DEF_OPTIONS}" "${spi_COMPILE_OPTIONS}" "${spi_LINK_OPTIONS}")
    target_link_directories(${targetname} PUBLIC ${CMAKE_BINARY_DIR}/lib)
    target_link_libraries(${targetname} PUBLIC pgport)
    add_dependencies(${targetname} pgport_static)
endforeach(subfile)

set_target_properties(autoinc PROPERTIES PREFIX "")
set_target_properties(refint PROPERTIES PREFIX "")

