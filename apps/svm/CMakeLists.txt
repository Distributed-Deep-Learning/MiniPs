include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

if (LIBHDFS3_FOUND)
    file(GLOB_RECURSE svm-src-files *.hpp *.cpp)

    add_executable(SVM_Example svm_example.cpp)
    target_link_libraries(SVM_Example csci5570)
    target_link_libraries(SVM_Example ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET SVM_Example PROPERTY CXX_STANDARD 11)
    add_dependencies(SVM_Example ${external_project_dependencies})
endif (LIBHDFS3_FOUND)
