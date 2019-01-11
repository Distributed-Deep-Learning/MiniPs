include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

if (LIBHDFS3_FOUND)
    file(GLOB_RECURSE lr-src-files *.hpp *.cpp)

    add_executable(LRExample lr_example.cpp)
    target_link_libraries(LRExample minips)
    target_link_libraries(LRExample ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET LRExample PROPERTY CXX_STANDARD 11)
    add_dependencies(LRExample ${external_project_dependencies})
endif (LIBHDFS3_FOUND)
