#This is the main CMAKE for build all components.

SET(CMAKE_VERBOSE_MAKEFILE ON) #显示详细的原始编译信息
SET(CMAKE_RULE_MESSAGES OFF)   #减少一些不必要的输出
SET(CMAKE_SKIP_RPATH TRUE)     #TRUE表示在编译及安装阶段忽略RPATH

set(CMAKE_MODULE_PATH 
    ${PROJECT_SRC_DIR}/common/pl/plpgsql
)

add_subdirectory(plpgsql)