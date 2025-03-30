#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <mutex>

// Classe base para representar um DataFrame (estrutura tabular de dados)
class DataFrame {
private:
    std::vector<std::vector<std::string>> data; // Armazena os dados tabulares

public:
    // Adiciona uma linha ao DataFrame
    void addRow(const std::vector<std::string>& row) {
        data.push_back(row);
    }

    // Exibe os dados do DataFrame
    void display() const {
        for (const auto& row : data) {
            for (const auto& value : row) {
                std::cout << value << " ";
            }
            std::cout << std::endl;
        }
    }
};

// Interface para repositórios de dados (extração e carga)
class Repository {
public:
    virtual std::shared_ptr<DataFrame> loadData() = 0; // Carrega os dados
    virtual void saveData(std::shared_ptr<DataFrame> df) = 0; // Salva os dados
    virtual ~Repository() = default;
};

// Implementação de um repositório que trabalha com memória
class MemoryRepository : public Repository {
private:
    std::shared_ptr<DataFrame> df; // Armazena os dados em memória

public:
    MemoryRepository() : df(std::make_shared<DataFrame>()) {}

    std::shared_ptr<DataFrame> loadData() override {
        return df;
    }

    void saveData(std::shared_ptr<DataFrame> df) override {
        this->df = df;
    }
};

// Classe base para handlers (processadores de dados)
class Handler {
public:
    virtual std::shared_ptr<DataFrame> process(std::shared_ptr<DataFrame> input) = 0;
    virtual ~Handler() = default;
};

// Exemplo de um handler que processa dados (simplesmente retorna o mesmo DataFrame)
class ExampleHandler : public Handler {
public:
    std::shared_ptr<DataFrame> process(std::shared_ptr<DataFrame> input) override {
        std::cout << "Processando dados..." << std::endl;
        return input;
    }
};

// Classe base para triggers (mecanismos de disparo do pipeline)
class Trigger {
public:
    virtual void execute() = 0;
    virtual ~Trigger() = default;
};

// Implementação de um RequestTrigger (dispara sob demanda)
class RequestTrigger : public Trigger {
private:
    std::shared_ptr<Handler> handler;
    std::shared_ptr<Repository> repository;

public:
    RequestTrigger(std::shared_ptr<Handler> h, std::shared_ptr<Repository> r) : handler(h), repository(r) {}

    void execute() override {
        std::cout << "Trigger acionado!" << std::endl;
        auto df = repository->loadData();
        auto processedDf = handler->process(df);
        repository->saveData(processedDf);
    }
};

// Classe que representa um pipeline de processamento
class Pipeline {
private:
    std::vector<std::shared_ptr<Handler>> handlers;
    std::shared_ptr<Repository> repository;
    std::mutex mtx; // Mutex para garantir concorrência segura

public:
    Pipeline(std::shared_ptr<Repository> repo) : repository(repo) {}

    // Adiciona um handler ao pipeline
    void addHandler(std::shared_ptr<Handler> handler) {
        handlers.push_back(handler);
    }

    // Executa o pipeline de forma sequencial
    void run() {
        std::lock_guard<std::mutex> lock(mtx); // Protege o acesso concorrente
        auto df = repository->loadData();

        for (auto& handler : handlers) {
            df = handler->process(df);
        }

        repository->saveData(df);
    }
};

int main() {
    // Criando um repositório na memória
    auto repo = std::make_shared<MemoryRepository>();

    // Criando um handler simples
    auto handler = std::make_shared<ExampleHandler>();

    // Criando um pipeline e adicionando o handler
    Pipeline pipeline(repo);
    pipeline.addHandler(handler);

    // Criando um trigger e executando o pipeline 
    auto trigger = std::make_shared<RequestTrigger>(handler, repo);
    trigger->execute();

    return 0;
}
