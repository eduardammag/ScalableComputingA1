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
                extratorTratadorFila.push(move(df));
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

            // cout << "Quantidade de threads" << numThreads << endl;
            auto startCall = chrono::high_resolution_clock::now();
            handler.meanAlert(dfExtraido, nomeCol, numThreads);
            auto endCall = chrono::high_resolution_clock::now();
            chrono::duration<double> durFunc = endCall - startCall;
            // cout << "[consumidorTrat] Tempo função: " << durFunc.count() << " s\n";
            if (dfExtraido.empty()) {
                cerr << "[Tratador " << id << "] DataFrame TRATADO está vazio!\n";
            } else {
                // cout<< "[Tratador " << id << "] Tratamento completo. Linhas: " << tratado.size() 
                    // << ", Colunas: " << tratado.numCols() << "\n";
                // tratado.display();
            }


            LoaderItem item{
                std::move(dfExtraido),
                "saida_tratada_" + to_string(id) + ".csv",
                id
            };

            {
                lock_guard<mutex> lock(tratLoadMutex);
                tratadorLoaderFila.push(move(item));
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
                LoaderItem i = move(tratadorLoaderFila.front());
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
            save_as_csv(item.df, "database_loader/" + item.nomeArquivoOriginal);
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
        "databases_mock/secretary_data.db",
        "databases_mock/oms_mock.txt",
        "databases_mock/hospital_mock_1.csv",
        "databases_mock/hospital_mock_2.csv",
        "databases_mock/hospital_mock_3.csv",
        "databases_mock/hospital_mock_4.csv",
        "databases_mock/hospital_mock_5.csv",
        "databases_mock/hospital_mock_6.csv",
        "databases_mock/hospital_mock_7.csv",
        "databases_mock/hospital_mock_8.csv",
        "databases_mock/hospital_mock_9.csv",
        "databases_mock/hospital_mock_10.csv"
    };

    // Variáveis para medição de tempo
    chrono::time_point<chrono::high_resolution_clock> start, end;
    chrono::duration<double> tempoTotal, tempoExtracao, tempoTratamento, tempoLoader;

    // Início do pipeline
    start = chrono::high_resolution_clock::now();

    // ---- Estágio 1: Extração ----
    auto startExtracao = chrono::high_resolution_clock::now();

    // Cria produtor e inializa-o
    thread prod(produtor, arquivos);

    // Cria consumidores do extrator
    vector<thread> consumidoresExtrator;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresExtrator.emplace_back(consumidorExtrator, i + 1);
    }
    // Aguarda o produtor
    prod.join();

    // Aguarda extratores
    for (auto& t : consumidoresExtrator) t.join();

    end = chrono::high_resolution_clock::now();
    tempoExtracao = end - startExtracao;

    // ---- Estágio 2: Tratamento ----
    auto startTratamento = chrono::high_resolution_clock::now();

    // Sinaliza que extração terminou e notifica tratadores
    {
        lock_guard<mutex> lock(extTratMutex);
        extTratencerrado = true;
    }
    extTratcondVar.notify_all();

    // Cria consumidores dos tratadores
    vector<thread> consumidoresTratador;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresTratador.emplace_back(consumidorTrat, i + 1, "Nº óbitos", numConsumidores);
    }

    // Aguarda tratadores
    for (auto& t : consumidoresTratador) t.join();

    end = chrono::high_resolution_clock::now();
    tempoTratamento = end - startTratamento;

    // ---- Estágio 3: Loader ----
    auto startLoader = chrono::high_resolution_clock::now();

    // Sinaliza fim do tratamento e notifica loaders
    {
        lock_guard<mutex> lock(tratLoadMutex);
        tratadorEncerrado = true;
    }
    tratLoadCondVar.notify_all();

    // Cria consumidores finais (loader)
    vector<thread> consumidoresLoader;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresLoader.emplace_back(consumidorLoader, i + 1);
    }
    // Aguarda loaders
    for (auto& t : consumidoresLoader) t.join();

    end = chrono::high_resolution_clock::now();
    tempoLoader = end - startLoader;

    // Tempo total
    tempoTotal = end - start;
    
    // ---- Exibição dos tempos ----
    cout << "\n=== Análise de Tempo por Estágio ===" << endl;
    cout << "1. Extração:    " << tempoExtracao.count() << " segundos" << endl;
    cout << "2. Tratamento: " << tempoTratamento.count() << " segundos" << endl;
    cout << "3. Loader:      " << tempoLoader.count() << " segundos" << endl;
    cout << "---------------------------------" << endl;
    cout << "Tempo Total:   " << tempoTotal.count() << " segundos\n" << endl;
}