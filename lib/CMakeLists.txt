include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB_RECURSE lib-src-files *.hpp *.cpp)

add_library(lib-objs OBJECT ${lib-src-files})
set_property(TARGET lib-objs PROPERTY CXX_STANDARD 11)
add_dependencies(lib-objs ${external_project_dependencies})