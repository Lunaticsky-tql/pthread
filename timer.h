//
// Created by 田佳业 on 2022/4/27.
//

#include <ctime>
using namespace std;
#ifndef PTHREAD_TIMER_H
#define PTHREAD_TIMER_H

#endif //PTHREAD_TIMER_H

class MyTimer {
private:
    struct timespec sts{},ets{};
    double timeMS;

public:
    MyTimer() {
        timeMS = 0.0;
    }

    void start() {
        timespec_get(&sts, TIME_UTC);
    }

    void finish() {
        timespec_get(&ets, TIME_UTC);
    }

    void get_duration(const string& str) {
        // get the time span (in milliseconds)
        timeMS = (ets.tv_sec - sts.tv_sec) * 1000.0 + (ets.tv_nsec - sts.tv_nsec) / 1000000.0;
        printf("%s: %f ms\n", str.c_str(), timeMS);
    }
};