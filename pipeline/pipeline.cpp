// pipeline.cpp
#include "pipeline.hpp"
#include "../etl/extrator.hpp"
#include "../etl/loader.hpp"
#include "../etl/handlers.hpp"


#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <vector>

using namespace std;


// Fila compartilhada entre fila de arquivos (produtor) e extrator (consumidor)
queue<string> filaArquivos;
mutex filaMutex;
condition_variable condVar;
atomic<bool> encerrado(false);

// Fila compartilhada entre extrator(produtor) e tratador(consumidor)
queue<DataFrame> extratorTratadorFila;
mutex extTratMutex;
condition_variable extTratcondVar;
atomic<bool> extTratencerrado(false);

// Fila compartilhada entre handler (produtor) e loader (consumidor)
queue<LoaderItem> tratadorLoaderFila;
mutex tratLoadMutex;
condition_variable tratLoadCondVar;
atomic<bool> tratadorEncerrado(false);

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
void consumidorExtrator(int id) {
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
            // extrai os arquivos 
            DataFrame df = extrator.carregar(arquivo);
            cout << "[Consumidor " << id << "] Processando: " << arquivo << endl;
            // df.display();

            //e joga na fila para os tratadores
            {
                unique_lock<mutex> lock(extTratMutex);
                extratorTratadorFila.push(std::move(df));
            }
            // avisa os tratadores
            extTratcondVar.notify_one();
            
            //usar no consumidor do handler quando estiver pronto. Aqui é só um teste do loader.
            string nomeBase = arquivo.substr(0, arquivo.find_last_of('.'));
            string saidaCsv = "database/saida_" + to_string(id) + "_" + nomeBase + ".csv";
            save_as_csv(df, saidaCsv);
            cout << "[Consumidor " << id << "] Arquivo salvo como: " << saidaCsv << endl;

        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << arquivo << ": " << e.what() << endl;
        }
    }

    cout << "[Consumidor " << id << "] Encerrando.\n";
}

// CONSUMIDOR TRATADOR: consome da fila extraída e joga para o tratador
void consumidorTrat(int id, string nomeCol, int numThreads) 
{
    Handler handler;

    while (true) {
        // df dummy só para inicializar o objeto
        DataFrame dfExtraido({"ID"}, {ColumnType::INTEGER});

        // esprando até ter pelo menos um elemento na fila ou o produtor terminar
        {
            unique_lock<mutex> lock(extTratMutex);
            extTratcondVar.wait(lock, [] {
                return !extratorTratadorFila.empty() || extTratencerrado;
            });

            if (extratorTratadorFila.empty() && extTratencerrado)
                break;

            if (!extratorTratadorFila.empty()) {
                DataFrame dfExtraido = extratorTratadorFila.front();
                extratorTratadorFila.pop();
            } else {
                continue;
            }
        }

        try {
            // processando o dataframe extraído
            DataFrame tratado = handler.meanAlert(dfExtraido, nomeCol, numThreads);
            cout << "[Consumidor Tratador " << id << endl;

            LoaderItem item{
                std::move(tratado),
                "saida_tratada_" + to_string(id) + ".csv",
                id
            };

            {
                lock_guard<mutex> lock(tratLoadMutex);
                tratadorLoaderFila.push(std::move(item));
            }
            tratLoadCondVar.notify_one();

        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << e.what() << endl;
        }
    }

    cout << "[Consumidor Tratador " << id << "] Encerrando.\n";
}

// CONSUMIDOR LOADER: consome da fila tratada e joga para o loader
void consumidorLoader(int id) {
    while (true) {
        LoaderItem item = [&] {
            unique_lock<mutex> lock(tratLoadMutex);
            tratLoadCondVar.wait(lock, [] {
                return !tratadorLoaderFila.empty() || tratadorEncerrado;
            });

            if (tratadorLoaderFila.empty() && tratadorEncerrado)
                return LoaderItem{DataFrame({"ID"}, {ColumnType::INTEGER}), "", -1};

            if (!tratadorLoaderFila.empty()) {
                LoaderItem i = std::move(tratadorLoaderFila.front());
                tratadorLoaderFila.pop();
                return i;
            }

            // fallback
            return LoaderItem{DataFrame({"ID"}, {ColumnType::INTEGER}), "", -1};
        }();

        // checa se é um item "falso" (fim do loop)
        if (item.nomeArquivoOriginal == "" && item.threadId == -1)
            break;

        try {
            save_as_csv(item.df, "database/" + item.nomeArquivoOriginal);
            cout << "[Loader " << id << "] Arquivo salvo como: " << item.nomeArquivoOriginal << endl;
        } catch (const exception& e) {
            cerr << "[Erro Loader " << id << "] ao salvar arquivo: " << e.what() << endl;
        }
    }

    cout << "[Loader " << id << "] Encerrando.\n";
}

// Função que orquestra o pipeline
void executarPipeline(int numConsumidores) {
    // Reinicia estados globais (caso a função seja chamada várias vezes)
    encerrado = false;
    extTratencerrado = false;
    tratadorEncerrado = false;  

    // entre fila e extrator
    {
        lock_guard<mutex> lock(filaMutex);
        queue<string> empty;
        swap(filaArquivos, empty);
    }
    // entre extrator e tratador
    {
        lock_guard<mutex> lock(extTratMutex);
        queue<DataFrame> empty;
        swap(extratorTratadorFila, empty);
    }

    // entre tratador e loader
    {
        lock_guard<mutex> lock(tratLoadMutex);
        queue<LoaderItem> empty;
        swap(tratadorLoaderFila, empty);
    }  

    vector<string> arquivos = {
        "oms_mock.txt",
        "hospital_mock_1.csv",
        "hospital_mock_2.csv",
        "hospital_mock_3.csv",
        "secretary_data.db"
    };

    // Cria produtor e inializa-o
    thread prod(produtor, arquivos);

    // Cria consumidores do extrator
    vector<thread> consumidoresExtrator;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresExtrator.emplace_back(consumidorExtrator, i + 1);
    }

    // Esperar produtor e consumidores extratores
    prod.join();
    for (auto& t : consumidoresExtrator) {
        t.join();
    }

    // Cria consumidores dos tratadores
    vector<thread> consumidoresTratador;
    for (int i = 0; i < 2; ++i) {
        consumidoresTratador.emplace_back(consumidorTrat, i + 1, "infectados", 4);
    }

    // Sinaliza que extração terminou e notifica tratadores
    {
        lock_guard<mutex> lock(extTratMutex);
        extTratencerrado = true;
    }
    extTratcondVar.notify_all();

    // Cria consumidores finais (loader)
    vector<thread> consumidoresLoader;
    for (int i = 0; i < 2; ++i) {
        consumidoresLoader.emplace_back(consumidorLoader, i + 1);
    }

    // Esperar tratadores
    for (auto& t : consumidoresTratador) {
        t.join();
    }

    // Sinalizar fim do tratamento e notificar loaders
    {
        lock_guard<mutex> lock(tratLoadMutex);
        tratadorEncerrado = true;
    }
    tratLoadCondVar.notify_all();
    
    // Esperar loaders
    for (auto& t : consumidoresLoader) {
        t.join();
    }
    
    cout << "[Pipeline] Execução completa com " << numConsumidores << " consumidor(es).\n";
}