include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

if (LIBHDFS3_FOUND)
    file(GLOB mnist-src-files
            mnist_example.cpp
            )

    add_executable(MNIST_Example mnist_example.cpp)
    target_link_libraries(MNIST_Example csci5570)
    target_link_libraries(MNIST_Example ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET MNIST_Example PROPERTY CXX_STANDARD 11)
    add_dependencies(MNIST_Example ${external_project_dependencies})
endif (LIBHDFS3_FOUND)
