# MiniPs

## Install & Run
Git clone this repository by
```sh
git clone https://github.com/RickAi/MiniPs.git
cd MiniPs
```
Create a directory for putting compiled files, and configure cmake.
```
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
```
See the make list by `make help`, and make anything in the list by `make ${ANYTHING}`

```
cd build/     # cd to the build directory
make -j4      # build all the targets
./HuskyUnitTest  # run all unit tests
./HuskyUnitTest --gtest_filter=TestServerThread.RegisterModel  # run a specific test
```
### Some tools: 
* [glog](http://rpg.ifi.uzh.ch/docs/glog.html). You may use `GLOG_logtostderr=1 ./HuskyUnitTest` to print the `LOG(INFO)` information to the console.
* [gtest](https://github.com/google/googletest/blob/master/googletest/docs/Primer.md).
* Actor model
* cmake
* C++ (C++11, multi-threading, std::move, rvalue reference, classes...)

### More

Project details refers to: [HERE](report/report.md)
