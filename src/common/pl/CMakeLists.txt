# Main CMakeLists.txt for building all components.

SET(CMAKE_VERBOSE_MAKEFILE ON) # Show detailed original compilation information
SET(CMAKE_RULE_MESSAGES OFF)   # Reduce unnecessary build output
SET(CMAKE_SKIP_RPATH TRUE)     # TRUE ignores RPATH during build and install phases


# Append plpython module path only when Python3 is enabled
if(ENABLE_PYTHON3)
    set(CMAKE_MODULE_PATH ${PROJECT_SRC_DIR}/common/pl/plpython ${PROJECT_SRC_DIR}/common/pl/plpgsql)
else()
    set(CMAKE_MODULE_PATH ${PROJECT_SRC_DIR}/common/pl/plpgsql)
endif()

add_subdirectory(plpgsql)

# Build plpython subdirectory only when Python3 is enabled
if(ENABLE_PYTHON3)
    add_subdirectory(plpython)
endif()

#set(CMAKE_MODULE_PATH ${PROJECT_SRC_DIR}/common/pl/plpgsql)
#add_subdirectory(plpgsql)
