include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB_RECURSE server-src-files *.cpp *.hpp)

add_library(server-objs OBJECT ${server-src-files})
set_property(TARGET server-objs PROPERTY CXX_STANDARD 11)
add_dependencies(server-objs ${external_project_dependencies})
