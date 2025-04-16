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
            // cout<< "[Produtor] Arquivo enfileirado: " << arquivo << endl;
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
            }
        }

        try {
            // extrai os arquivos 
            DataFrame df = extrator.carregar(arquivo);
            // cout<< "[Consumidor " << id << "] Processando: " << arquivo << endl;
            if (df.empty()) {
                cerr << "[Consumidor " << id << "] DataFrame VAZIO após extração de " << arquivo << endl;
                continue;
            } else if (df.getColumnNames().size() != static_cast<size_t>(df.numCols()))
            {
                cerr << "[Consumidor " << id << "] Inconsistência no DataFrame: " << arquivo << endl;
                continue;
            }


            //e joga na fila para os tratadores
            {
                unique_lock<mutex> lock(extTratMutex);
                extratorTratadorFila.push(std::move(df));
            }
            // avisa os tratadores
            extTratcondVar.notify_one();

        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << arquivo << ": " << e.what() << endl;
        }
    }
}

// CONSUMIDOR TRATADOR: consome da fila extraída e joga para o tratador
void consumidorTrat(int id, string nomeCol, int numThreads) 
{
    Handler handler;

    while (true) {
        // df dummy só para inicializar o objeto
        DataFrame dfExtraido({"ID"}, {});

        // esprando até ter pelo menos um elemento na fila ou o produtor terminar
        {
            unique_lock<mutex> lock(extTratMutex);
            extTratcondVar.wait(lock, [] {
                return !extratorTratadorFila.empty() || extTratencerrado;
            });

            if (extratorTratadorFila.empty() && extTratencerrado)
                break;

            if (!extratorTratadorFila.empty()) {
                dfExtraido = extratorTratadorFila.front();
                extratorTratadorFila.pop();
            } else {
                continue;
            }
        }

        try {
            // processando o dataframe extraído
            DataFrame tratado = handler.meanAlert(dfExtraido, nomeCol, numThreads);
            if (tratado.empty()) {
                cerr << "[Tratador " << id << "] DataFrame TRATADO está vazio!\n";
            } else {
                // cout<< "[Tratador " << id << "] Tratamento completo. Linhas: " << tratado.size() 
                    // << ", Colunas: " << tratado.numCols() << "\n";
                // tratado.display();
            }


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

    // cout<< "[Consumidor Tratador " << id << "] Encerrando.\n";
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
                return LoaderItem{DataFrame({"ID"}, {}), "", -1};

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
            // cout<< "[Loader " << id << "] Arquivo salvo como: " << item.nomeArquivoOriginal << endl;
            if (item.df.empty()) {
                cerr << "[Loader " << id << "] AVISO: DataFrame salvo está VAZIO!\n";
            } else {
                // cout<< "[Loader " << id << "] DataFrame salvo com " << item.df.size() 
                    // << " linhas e " << item.df.numCols() << " colunas.\n";
            }

        } catch (const exception& e) {
            cerr << "[Erro Loader " << id << "] ao salvar arquivo: " << e.what() << endl;
        }
    }

    // cout<< "[Loader " << id << "] Encerrando.\n";
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

    // "secretary_data.db"
    vector<string> arquivos = {
        "oms_mock.txt",
        // "hospital_mock_1.csv",
        // "hospital_mock_2.csv",
        // "hospital_mock_3.csv",
    };

    // Cria produtor e inializa-o
    thread prod(produtor, arquivos);

    // Cria consumidores do extrator
    vector<thread> consumidoresExtrator;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresExtrator.emplace_back(consumidorExtrator, i + 1);
    }

    // Cria consumidores dos tratadores
    vector<thread> consumidoresTratador;
    for (int i = 0; i < 2; ++i) {
        consumidoresTratador.emplace_back(consumidorTrat, i + 1, "Nº óbitos", numConsumidores);
    }

    // Cria consumidores finais (loader)
    vector<thread> consumidoresLoader;
    for (int i = 0; i < 2; ++i) {
        consumidoresLoader.emplace_back(consumidorLoader, i + 1);
    }

    // Aguarda o produtor
    prod.join();

    // Aguarda extratores
    for (auto& t : consumidoresExtrator) t.join();

    // Sinaliza que extração terminou e notifica tratadores
    {
        lock_guard<mutex> lock(extTratMutex);
        extTratencerrado = true;
    }
    extTratcondVar.notify_all();

    // Aguarda tratadores
    for (auto& t : consumidoresTratador) t.join();

    // Sinaliza fim do tratamento e notifica loaders
    {
        lock_guard<mutex> lock(tratLoadMutex);
        tratadorEncerrado = true;
    }
    tratLoadCondVar.notify_all();
    
    // Aguarda loaders
    for (auto& t : consumidoresLoader) t.join();
    
    // cout<< "[Pipeline] Execução completa com " << numConsumidores << " consumidor(es).\n";
}