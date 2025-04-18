#ifndef TRIGGER_HPP
#define TRIGGER_HPP

#include <functional>
#include <thread>
#include <chrono>
#include <atomic>

using namespace std;

class Trigger 
{
    private:
        function<void()> callback;
        int interval;
        bool isTimer;
        atomic<bool> running;

    public:
        Trigger(function<void()> func, int intervalMs, bool isTimer);
        void start();
        void stop();          
        void request();   
};

#endif
