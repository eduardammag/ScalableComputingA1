#ifndef REPOSITORY_HPP
#define REPOSITORY_HPP

#include "DataFrame.hpp"
#include <memory>

using namespace std;

// Interface para repositórios de dados (extração e carga)
class Repository {
public:
    virtual shared_ptr<DataFrame> loadData() = 0; // Carrega os dados
    virtual void saveData(shared_ptr<DataFrame> df) = 0; // Salva os dados
    virtual ~Repository() = default;
};

// Implementação de um repositório que trabalha com memória
class MemoryRepository : public Repository {
private:
    shared_ptr<DataFrame> df; // Armazena os dados em memória

public:
    MemoryRepository();

    shared_ptr<DataFrame> loadData() override;

    void saveData(shared_ptr<DataFrame> df) override;
};

#endif // REPOSITORY_HPP
