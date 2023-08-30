#This is the main CMAKE for build all components.
SET(unicode_cmd_src
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_most.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_EUC_JP.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_EUC_CN.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_EUC_KR.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_EUC_TW.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_SJIS.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_GB18030.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_BIG5.pl|"
    "${PROJECT_SRC_DIR}/common/backend/utils/mb/Unicode|||perl UCS_to_GB18030_2022.pl|"
)

add_cmd_gen_when_configure(perl_target unicode_cmd_src)

execute_process(COMMAND ${CMAKE_SOURCE_DIR}/${openGauss}/cmake/src/buildfunction.sh --create_conversionfile ${PROJECT_TRUNK_DIR} ${CMAKE_BINARY_DIR})

set(common_DEF_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0 -DENABLE_GSTRACE -D_GNU_SOURCE -DPGXC -DSTREAMPLAN)
set(common_COMPILE_OPTIONS ${OPTIMIZE_OPTIONS} ${OS_OPTIONS} ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${LIB_SECURE_OPTIONS} ${CHECK_OPTIONS} -fstack-protector-all)
list(REMOVE_ITEM common_COMPILE_OPTIONS -fstack-protector)
set(common_LINK_OPTIONS ${LIB_LINK_OPTIONS})

SUBDIRLIST(mb_conversion_procs ${CMAKE_CURRENT_SOURCE_DIR})
foreach(subdir ${mb_conversion_procs})
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/${subdir} TGT_${subdir}_SRC)
    SET(TGT_${subdir}_INC ${CMAKE_CURRENT_SOURCE_DIR}/${subdir})
    add_shared_libtarget(${subdir} TGT_${subdir}_SRC TGT_${subdir}_INC "${common_DEF_OPTIONS}" "${common_COMPILE_OPTIONS}" "${common_LINK_OPTIONS}")
endforeach(subdir)

set_target_properties(ascii_and_mic          PROPERTIES PREFIX "")
set_target_properties(cyrillic_and_mic       PROPERTIES PREFIX "")
set_target_properties(euc2004_sjis2004       PROPERTIES PREFIX "")
set_target_properties(euc_cn_and_mic         PROPERTIES PREFIX "")
set_target_properties(euc_jp_and_sjis        PROPERTIES PREFIX "")
set_target_properties(euc_kr_and_mic         PROPERTIES PREFIX "")
set_target_properties(euc_tw_and_big5        PROPERTIES PREFIX "")
set_target_properties(latin2_and_win1250     PROPERTIES PREFIX "")
set_target_properties(latin_and_mic          PROPERTIES PREFIX "")
set_target_properties(utf8_and_ascii         PROPERTIES PREFIX "")
set_target_properties(utf8_and_big5          PROPERTIES PREFIX "")
set_target_properties(utf8_and_cyrillic      PROPERTIES PREFIX "")
set_target_properties(utf8_and_euc2004       PROPERTIES PREFIX "")
set_target_properties(utf8_and_euc_cn        PROPERTIES PREFIX "")
set_target_properties(utf8_and_euc_jp        PROPERTIES PREFIX "")
set_target_properties(utf8_and_euc_kr        PROPERTIES PREFIX "")
set_target_properties(utf8_and_euc_tw        PROPERTIES PREFIX "")
set_target_properties(utf8_and_gb18030       PROPERTIES PREFIX "")
set_target_properties(utf8_and_gbk           PROPERTIES PREFIX "")
set_target_properties(utf8_and_iso8859       PROPERTIES PREFIX "")
set_target_properties(utf8_and_iso8859_1     PROPERTIES PREFIX "")
set_target_properties(utf8_and_johab         PROPERTIES PREFIX "")
set_target_properties(utf8_and_sjis          PROPERTIES PREFIX "")
set_target_properties(utf8_and_sjis2004      PROPERTIES PREFIX "")
set_target_properties(utf8_and_uhc           PROPERTIES PREFIX "")
set_target_properties(utf8_and_win           PROPERTIES PREFIX "")

install(FILES ${CMAKE_BINARY_DIR}/conversion_create.sql
    DESTINATION share/postgresql/
)
install(TARGETS utf8_and_euc2004 LIBRARY DESTINATION lib/postgresql)
install(TARGETS utf8_and_uhc LIBRARY DESTINATION lib/postgresql)
install(TARGETS cyrillic_and_mic LIBRARY DESTINATION lib/postgresql)
install(TARGETS latin2_and_win1250 LIBRARY DESTINATION lib/postgresql)
install(TARGETS euc_cn_and_mic LIBRARY DESTINATION lib/postgresql)
install(TARGETS utf8_and_gbk LIBRARY DESTINATION lib/postgresql)
install(TARGETS euc2004_sjis2004 LIBRARY DESTINATION lib/postgresql)
install(TARGETS euc_jp_and_sjis LIBRARY DESTINATION lib/postgresql)
install(TARGETS utf8_and_ascii LIBRARY DESTINATION lib/postgresql)
install(TARGETS utf8_and_euc_jp DESTINATION lib/postgresql)
install(TARGETS utf8_and_win DESTINATION lib/postgresql)
install(TARGETS utf8_and_iso8859 DESTINATION lib/postgresql)
install(TARGETS euc_kr_and_mic DESTINATION lib/postgresql)
install(TARGETS latin_and_mic DESTINATION lib/postgresql)
install(TARGETS utf8_and_gb18030 DESTINATION lib/postgresql)
install(TARGETS utf8_and_euc_cn DESTINATION lib/postgresql)
install(TARGETS utf8_and_euc_kr DESTINATION lib/postgresql)
install(TARGETS utf8_and_iso8859_1 DESTINATION lib/postgresql)
install(TARGETS ascii_and_mic DESTINATION lib/postgresql)
install(TARGETS utf8_and_johab DESTINATION lib/postgresql)
install(TARGETS utf8_and_euc_tw DESTINATION lib/postgresql)
install(TARGETS utf8_and_sjis DESTINATION lib/postgresql)
install(TARGETS utf8_and_sjis2004 DESTINATION lib/postgresql)
install(TARGETS utf8_and_cyrillic DESTINATION lib/postgresql)
install(TARGETS utf8_and_big5 DESTINATION lib/postgresql)
install(TARGETS euc_tw_and_big5 DESTINATION lib/postgresql)
