#ifndef TRIGGER_HPP
#define TRIGGER_HPP

#include "pipeline.hpp"
#include <thread>
#include <chrono>

using namespace std;

class TimerTrigger {
private:
    shared_ptr<Pipeline> pipeline;
    int interval;

public:
    TimerTrigger(shared_ptr<Pipeline> p, int i);
    void start();
};

#endif
