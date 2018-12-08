
//
// Created by aiyongbiao on 2018/9/25.
//

#pragma once

#include <iostream>
#include <vector>
#include <base/third_party/sarray.h>
#include <ctime>

namespace csci5570 {

    template<typename T>
    std::vector<T> SArrayToVector(third_party::SArray <T> &sArray) {
        std::vector<T> vec;
        for (T item : sArray) {
            vec.push_back(item);
        }
        return vec;
    }

    inline void CheckFaultTolerance(int phase) {
        auto now = std::time(0);
        std::string output;
        output.append("[Fault Tolerance]");
        output.append("[Phase");
        output.append(std::to_string(phase));
        output.append("]");
        output.append("[");
        output.append(std::to_string(now));
        output.append("] ");

        switch (phase) {
            case 2:
                output.append("Master Detect Process Failure");
                break;
            case 3:
                output.append("Failed Process Restart Success");
                break;
            case 4:
                output.append("Failed Process Recover Success");
                break;
            case 5:
                output.append("Master Send RollBack Message");
                break;
            case 6:
                output.append("Other Process Recover Success");
                break;
            default:
                break;
        }

        LOG(INFO) << output;
    }

};
