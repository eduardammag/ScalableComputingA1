#include "Repository.hpp"
using namespace std;

MemoryRepository::MemoryRepository() : df(make_shared<DataFrame>()) {}

shared_ptr<DataFrame> MemoryRepository::loadData() {
    return df;
}

void MemoryRepository::saveData(shared_ptr<DataFrame> df) {
    this->df = df;
}
