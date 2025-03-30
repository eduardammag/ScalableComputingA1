#ifndef HANDLERS_HPP
#define HANDLERS_HPP

#include "pipeline.hpp"
#include <thread>
#include <chrono>

using namespace std;

class DataCleaner : public Handler {
public:
    shared_ptr<DataFrame> process(shared_ptr<DataFrame> input) override;
};

class EpidemiologicalStats : public Handler {
public:
    shared_ptr<DataFrame> process(shared_ptr<DataFrame> input) override;
};

class OutbreakDetector : public Handler {
public:
    shared_ptr<DataFrame> process(shared_ptr<DataFrame> input) override;
};

#endif
