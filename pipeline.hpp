#ifndef PIPELINE_HPP
#define PIPELINE_HPP

#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <mutex>

using namespace std;

// Classe para armazenar dados
class DataFrame {
public:
    void addRow(const vector<string>& row);
    void display() const;
};

// Interface para repositórios de dados
class Repository {
public:
    virtual shared_ptr<DataFrame> loadData() = 0;
    virtual void saveData(shared_ptr<DataFrame> df) = 0;
    virtual ~Repository() = default;
};

// Implementação de repositório na memória
class MemoryRepository : public Repository {
private:
    shared_ptr<DataFrame> df;
    mutex mtx;

public:
    MemoryRepository();
    shared_ptr<DataFrame> loadData() override;
    void saveData(shared_ptr<DataFrame> df) override;
};

// Interface para processadores de dados
class Handler {
public:
    virtual shared_ptr<DataFrame> process(shared_ptr<DataFrame> input) = 0;
    virtual ~Handler() = default;
};

// Pipeline de processamento
class Pipeline {
private:
    vector<shared_ptr<Handler>> handlers;
    shared_ptr<Repository> repository;
    mutex mtx;

public:
    Pipeline(shared_ptr<Repository> repo);
    void addHandler(shared_ptr<Handler> handler);
    void run();
};

#endif
