include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB_RECURSE master-src-files *.cpp *.hpp)

add_library(master-objs OBJECT ${master-src-files})
set_property(TARGET master-objs PROPERTY CXX_STANDARD 11)
add_dependencies(master-objs ${external_project_dependencies})