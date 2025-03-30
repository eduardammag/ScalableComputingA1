#include "triggers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;

TimerTrigger::TimerTrigger(shared_ptr<Pipeline> p, int i) : pipeline(p), interval(i) {}

void TimerTrigger::start() {
    while (true) {
        cout << "TimerTrigger acionado!" << endl;
        pipeline->run();
        this_thread::sleep_for(chrono::seconds(interval));
    }
}
