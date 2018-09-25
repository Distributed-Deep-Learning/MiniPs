include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB base-src-files
        serialization.cpp
        range_partition_manager.hpp
        abstract_partition_manager.hpp
        node.hpp
        node_utils.hpp
        magic.hpp)

add_library(base-objs OBJECT ${base-src-files})
set_property(TARGET base-objs PROPERTY CXX_STANDARD 11)
add_dependencies(base-objs ${external_project_dependencies})
