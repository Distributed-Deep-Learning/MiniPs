include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB lib-src-files
        abstract_data_loader.hpp
        abstract_aync_data_loader.hpp
        labeled_sample.hpp
        parser.hpp
        )

add_library(lib-objs OBJECT ${lib-src-files})
set_property(TARGET lib-objs PROPERTY CXX_STANDARD 11)
add_dependencies(lib-objs ${external_project_dependencies})
