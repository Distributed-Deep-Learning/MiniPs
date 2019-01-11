include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

if (LIBHDFS3_FOUND)
    file(GLOB_RECURSE kmeans-src-files *.hpp *.cpp)

    add_executable(KMeans kmeans.cpp)
    target_link_libraries(KMeans minips)
    target_link_libraries(KMeans ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET KMeans PROPERTY CXX_STANDARD 11)
    add_dependencies(KMeans ${external_project_dependencies})
endif (LIBHDFS3_FOUND)
