#include "pipeline.hpp"
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;


// Métodos de DataFrame
void DataFrame::addRow(const vector<string>& row) {
    // Implementação simples de armazenamento de dados
}

void DataFrame::display() const {
    // Exibe os dados armazenados
}

// Construtor do repositório
MemoryRepository::MemoryRepository() : df(make_shared<DataFrame>()) {}

shared_ptr<DataFrame> MemoryRepository::loadData() {
    lock_guard<mutex> lock(mtx);
    return df;
}

void MemoryRepository::saveData(shared_ptr<DataFrame> df) {
    lock_guard<mutex> lock(mtx);
    this->df = df;
}

// Construtor do pipeline
Pipeline::Pipeline(shared_ptr<Repository> repo) : repository(repo) {}

void Pipeline::addHandler(shared_ptr<Handler> handler) {
    handlers.push_back(handler);
}

// Executa os handlers em paralelo
void Pipeline::run() {
    lock_guard<mutex> lock(mtx);
    auto df = repository->loadData();
    vector<thread> threads;

    for (auto& handler : handlers) {
        threads.emplace_back([&handler, df]() {
            handler->process(df);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    repository->saveData(df);
}
