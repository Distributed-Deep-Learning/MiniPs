include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB_RECURSE driver-src-files *.hpp *.cpp)

add_library(driver-objs OBJECT ${driver-src-files})
set_property(TARGET driver-objs PROPERTY CXX_STANDARD 11)
add_dependencies(driver-objs ${external_project_dependencies})
