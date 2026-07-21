# Add all *.c to sources in upperlevel directory
set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/decompress_chunk.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/exec.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/planner.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/qual_pushdown.cpp
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
