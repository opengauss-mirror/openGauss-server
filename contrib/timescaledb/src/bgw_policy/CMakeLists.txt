set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/reorder.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/drop_chunks.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/compress_chunks.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/policy.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/chunk_stats.cpp
)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
