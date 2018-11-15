
//
// Created by aiyongbiao on 2018/9/25.
//

#pragma once

#include <iostream>
#include <vector>
#include <base/third_party/sarray.h>

namespace csci5570 {

    template<typename T>
    std::vector<T> SArrayToVector(third_party::SArray <T> &sArray) {
        std::vector<T> vec;
        for (T item : sArray) {
            vec.push_back(item);
        }
        return vec;
    }

};
