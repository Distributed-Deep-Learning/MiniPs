include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

if (LIBHDFS3_FOUND)
    file(GLOB_RECURSE lr-src-files *.hpp *.cpp)

    add_executable(LR_Example lr_example.cpp)
    target_link_libraries(LR_Example csci5570)
    target_link_libraries(LR_Example ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET LR_Example PROPERTY CXX_STANDARD 11)
    add_dependencies(LR_Example ${external_project_dependencies})
endif (LIBHDFS3_FOUND)
