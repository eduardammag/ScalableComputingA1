// pipeline.cpp
#include "pipeline.hpp"
#include "../etl/extrator.hpp"

#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>

using namespace std;


// Fila compartilhada entre produtores e consumidores
queue<string> filaArquivos;
mutex filaMutex;
condition_variable condVar;
atomic<bool> encerrado(false);

// PRODUTOR: adiciona arquivos à fila
void produtor(const vector<string>& arquivos) {
    for (const auto& arquivo : arquivos) {
        {
            lock_guard<mutex> lock(filaMutex);
            filaArquivos.push(arquivo);
            cout << "[Produtor] Arquivo enfileirado: " << arquivo << endl;
        }
        condVar.notify_one(); // Avisa os consumidores
    }

    // Sinaliza fim
    {
        lock_guard<mutex> lock(filaMutex);
        encerrado = true;
    }
    condVar.notify_all();
}

// CONSUMIDOR: consome da fila e processa
void consumidor(int id) {
    Extrator extrator;

    while (true) {
        string arquivo;

        {
            unique_lock<mutex> lock(filaMutex);
            condVar.wait(lock, [] {
                return !filaArquivos.empty() || encerrado;
            });

            if (filaArquivos.empty() && encerrado)
                break;

            if (!filaArquivos.empty()) {
                arquivo = filaArquivos.front();
                filaArquivos.pop();
            } else {
                continue;
            }
        }

        try {
            DataFrame df = extrator.carregar(arquivo);
            cout << "[Consumidor " << id << "] Processando: " << arquivo << endl;
            df.display();
        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << arquivo << ": " << e.what() << endl;
        }
    }

    cout << "[Consumidor " << id << "] Encerrando.\n";
}

// Função que orquestra o pipeline
void executarPipeline(int numConsumidores) {
    // Reinicia estados globais (caso a função seja chamada várias vezes)
    encerrado = false;
    {
        lock_guard<mutex> lock(filaMutex);
        queue<string> empty;
        swap(filaArquivos, empty);
    }

    vector<string> arquivos = {
        "oms_mock.txt",
        "hospital_mock_1.csv",
        "hospital_mock_2.csv",
        "hospital_mock_3.csv",
        "secretary_data.db"
    };

    thread prod(produtor, arquivos);

    vector<thread> consumidores;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidores.emplace_back(consumidor, i + 1);
    }

    prod.join();
    for (auto& t : consumidores) {
        t.join();
    }

    cout << "[Pipeline] Execução completa com " << numConsumidores << " consumidor(es).\n";
}