set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/array.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/compression.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/create.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_utils.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/datum_serialize.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/deltadelta.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dictionary.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/gorilla.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/segment_meta.cpp
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
