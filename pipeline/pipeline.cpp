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
#include <ostream>

#include <functional> // Para std::ref

using namespace std;


// Fila compartilhada entre fila de arquivos (produtor) e extrator (consumidor)
queue<string> filaArquivos;
mutex filaMutex;
condition_variable condVar;
atomic<bool> encerrado(false);

// Fila compartilhada entre extrator(produtor) e tratador(consumidor)
queue<pair<string, DataFrame>> extratorTratadorFila;
mutex extTratMutex;
condition_variable extTratcondVar;
atomic<bool> extTratencerrado(false);

// Fila compartilhada entre handler (produtor) e loader (consumidor)
queue<LoaderItem> tratadorLoaderFila;
mutex tratLoadMutex;
condition_variable tratLoadCondVar;
atomic<bool> tratadorEncerrado(false);

// variáveis para a pipeline de merge
queue<DataFrame> extratMergeFila;
mutex mergeMtx;
condition_variable condVarMerge;
condition_variable extTratcondVarMerge;
condition_variable tratLoadCondVarMerge;

atomic<bool> encerradoMerge(false);

// PRODUTOR: adiciona arquivos à fila
void produtor(const vector<string>& arquivos, bool merge) {
    for (const auto& arquivo : arquivos) {
        {
            lock_guard<mutex> lock(filaMutex);
            filaArquivos.push(arquivo);
        }

        // Avisa os consumidores
        if (merge){condVarMerge.notify_one();}
        else{condVar.notify_one();}
    }

    // Sinaliza fim
    {
        lock_guard<mutex> lock(filaMutex);
        encerrado = true;
    }
    if (merge){condVarMerge.notify_all();}
        else{condVar.notify_all();}
}

// CONSUMIDOR: consome da fila e processa
void consumidorExtrator(int id, bool merge) {
    Extrator extrator;

    while (true) {
        string arquivo;

        {
            // esperando ter aruivos para extrair
            unique_lock<mutex> lock(filaMutex);
            if (merge)
            {
                condVarMerge.wait(lock, [] {
                    return !filaArquivos.empty() || encerrado;
                });
            }
            else
            {
                condVar.wait(lock, [] {
                    return !filaArquivos.empty() || encerrado;
                });
            }

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

            if (df.empty()) {
                cerr << "[Consumidor " << id << "] DataFrame VAZIO após extração de " << arquivo << endl;
                continue;
            } else if (df.getColumnNames().size() != static_cast<size_t>(df.numCols()))
            {
                cerr << "[Consumidor " << id << "] Inconsistência no DataFrame: " << arquivo << endl;
                continue;
            }
            if (merge)
            {
                unique_lock<mutex> lock(extTratMutex);
                extratMergeFila.push(move(df));
            }
            else
            {
                unique_lock<mutex> lock(extTratMutex);
                extratorTratadorFila.push({arquivo, move(df)});
            }
            // avisa os tratadores
            extTratcondVar.notify_one();

        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << arquivo << ": " << e.what() << endl;
        }
    }
}

// CONSUMIDOR TRATADOR: consome da fila extraída e joga para o tratador
void consumidorTrat(int id, string meanCol, string groupedCol, string aggCol,  int numThreads) 
{
    // int count = 0;

    while (true) {
        // df dummy só para inicializar o objeto
        DataFrame dfExtraido({"ID"}, {});
        DataFrame grouping({"ID"}, {});

        pair<string, DataFrame> item("hospital", DataFrame({"ID"}, {}));

        {
            // espera ter arquivos extraídos
            unique_lock<mutex> lock(extTratMutex);
            extTratcondVar.wait(lock, [] {
                return !extratorTratadorFila.empty() || extTratencerrado;
            });

            if (extratorTratadorFila.empty() && extTratencerrado)
                break;

            if (!extratorTratadorFila.empty()) {
                item = extratorTratadorFila.front();
                extratorTratadorFila.pop();
            } else {
                continue;
            }
        }

        // extrai a origem para conseguir fazer tratar diferente arquivos
        string origem = item.first;
        dfExtraido = item.second;

        Handler handler;

        auto startCall = chrono::high_resolution_clock::now();
            
            // se for hospital agrupa
            if (origem.find("hospital") != string::npos) 
            {
                handler.dataCleaner(dfExtraido);
                handler.validateDataFrame(dfExtraido, numThreads);
                grouping = handler.groupedDf(dfExtraido, groupedCol, aggCol, numThreads, false);
                
                LoaderItem l_item{
                    
                    std::move(grouping),
                    // "saida_tratada_hospital" + to_string(count++) + ".csv", id
                    "saida_tratada_hospital.csv", id
            };
            // coloca na fila do loader o df tratado
            {
                lock_guard<mutex> lock(tratLoadMutex);
                tratadorLoaderFila.push(move(l_item));
            }
            tratLoadCondVar.notify_one();
        }
        // se é oms então agrupa e faz média
        else if (origem.find("oms") != string::npos) 
        {
            handler.dataCleaner(dfExtraido);
            handler.validateDataFrame(dfExtraido, numThreads);
            grouping = handler.groupedDf(dfExtraido, "cep", meanCol, numThreads, false);
            handler.meanAlert(grouping, "Total_" + meanCol, numThreads);
            LoaderItem l_item{

                std::move(grouping),
                // "saida_tratada_oms" + to_string(count++) + to_string(numThreads) + ".csv",
                "saida_tratada_oms.csv",
                id
            };

            // adiciona na fila do loader
            {
                lock_guard<mutex> lock(tratLoadMutex);
                tratadorLoaderFila.push(move(l_item));
            }
            tratLoadCondVar.notify_one();
        }
        else if (origem.find("secretaria") != string::npos) 
        {
            handler.dataCleaner(dfExtraido);
            handler.validateDataFrame(dfExtraido, numThreads);
            grouping = handler.groupedDf(dfExtraido, "cep", "vacinado", numThreads, true);

            LoaderItem l_item{
                std::move(grouping),
                // "saida_tratada_secretaria" + to_string(count++) + ".csv",
                "saida_tratada_secretaria.csv",
                id
            };

            {
                lock_guard<mutex> lock(tratLoadMutex);
                tratadorLoaderFila.push(move(l_item));
            }
            tratLoadCondVar.notify_one();
        }


        else 
        {
            cerr << "[Tratador " << id << "] Origem desconhecida: " << origem << endl;
            continue;
        }

        auto endCall = chrono::high_resolution_clock::now();
        chrono::duration<double> durFunc = endCall - startCall;        
    }
}

// consome fazendo o merge
void consumidorMerge(int id, DataFrame& dfB, DataFrame& dfC, const string& cepColName,
    const string& colA, const string& colB, const string& colC, int numThreads) 
{
    Handler handler;
    
    while (true) {
        // df dummy só para inicializar o objeto
        DataFrame dfExtraido({"ID"}, {});
        
        {
            // espera ter arquivos para tratar
            unique_lock<mutex> lock(mergeMtx);
            extTratcondVarMerge.wait(lock, [] {
                return !extratMergeFila.empty() || extTratencerrado;
            });
            
            if (extratMergeFila.empty() && extTratencerrado)
            break;
            
            if (!extratMergeFila.empty()) {
                dfExtraido = extratMergeFila.front();
                extratMergeFila.pop();
            } else {
                continue;
            }
        }


        try {
            // processando o dataframe extraído;
            auto startCall = chrono::high_resolution_clock::now();
            handler.dataCleaner(dfExtraido);
            handler.validateDataFrame(dfExtraido, numThreads);
            dfExtraido =  handler.groupedDf(dfExtraido, cepColName, colA, numThreads, true);
            
            // fazendo cópia pois o merge modifica inplace
            DataFrame copyB = dfB;
            DataFrame copyC = dfC;

            auto merged = handler.mergeByCEP(dfExtraido, copyB, copyC, cepColName, colB, colC, numThreads); 
            auto endCall = chrono::high_resolution_clock::now();
            chrono::duration<double> durFunc = endCall - startCall;
            int count = 0;
            
            for (const auto& [nome, dfMerge] : merged) 
            {
                LoaderItem item{
                std::move(dfMerge),
                "saida_merge_" + to_string(count++) + to_string(numThreads) + ".csv",
                id
                };
                // colocando na fila do loader
                {
                lock_guard<mutex> lock(mergeMtx);
                tratadorLoaderFila.push(move(item));
                }
                tratLoadCondVarMerge.notify_one();
            }

        } catch (const exception& e) {
            cerr << "[Erro Consumidor " << id << "] ao processar " << e.what() << endl;
        }
    }
}



// CONSUMIDOR LOADER: consome da fila tratada e joga para o loader
void consumidorLoader(int id, bool merge) {
    while (true) {
        LoaderItem item = [&] {
            unique_lock<mutex> lock(tratLoadMutex);

            // se for loarder de merge olha para a condição de merge
            if (merge)
            {
                tratLoadCondVarMerge.wait(lock, [] {
                    return !tratadorLoaderFila.empty() || tratadorEncerrado;
                });
            }
            else
            {
                tratLoadCondVar.wait(lock, [] {
                    return !tratadorLoaderFila.empty() || tratadorEncerrado;
                });
            }

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
            if (item.df.empty()) {
                cerr << "[Loader " << id << "] AVISO: DataFrame salvo está VAZIO!\n";
            } else {  }

        } catch (const exception& e) {
            cerr << "[Erro Loader " << id << "] ao salvar arquivo: " << e.what() << endl;
        }
    }
}

// Função que orquestra o pipeline
void executarPipeline(int numConsumidores, const string& arquivoOmsJson, const string& arquivoSecretariaJson, const string& arquivoHospitalJson) 
{
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
        queue<pair<string, DataFrame>> empty;
        swap(extratorTratadorFila, empty);
    }

    // entre tratador e loader
    {
        lock_guard<mutex> lock(tratLoadMutex);
        queue<LoaderItem> empty;
        swap(tratadorLoaderFila, empty);
    }  

    // arquivos
    vector<string> arquivos = {arquivoOmsJson, arquivoHospitalJson, arquivoSecretariaJson};

    // Variáveis para medição de tempo
    chrono::time_point<chrono::high_resolution_clock> start, end;
    chrono::duration<double> tempoTotal, tempoExtracao, tempoTratamento, tempoLoader;
    
    // Início do pipeline
    start = chrono::high_resolution_clock::now();

    // ---- Estágio 1: Extração ----
    auto startExtracao = chrono::high_resolution_clock::now();
    
    // Cria produtor e inializa-o
    thread prod(produtor, arquivos, false);
    
    // Cria consumidores do extrator
    vector<thread> consumidoresExtrator;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresExtrator.emplace_back(consumidorExtrator, i + 1, false);
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
    for (int i = 0; i < numConsumidores; ++i) 
    {
        consumidoresTratador.emplace_back(consumidorTrat, i + 1, "num_obitos", "id_hospital", "internado", numConsumidores);
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
        consumidoresLoader.emplace_back(consumidorLoader, i + 1, false);
    }
    // Aguarda loaders
    for (auto& t : consumidoresLoader) t.join();
    end = chrono::high_resolution_clock::now();
    tempoLoader = end - startLoader;
    
    //inicia pipeline do merge
    
    // Reinicia estados globais
    encerrado = false;
    extTratencerrado = false;
    tratadorEncerrado = false;  
    
    // entre fila e extrator (Merge)
    {
        lock_guard<mutex> lock(filaMutex);
        queue<string> empty;
        swap(filaArquivos, empty);
    }
    // entre extrator e tratador (Merge)
    {
        lock_guard<mutex> lock(extTratMutex);
        queue<DataFrame> empty;
        swap(extratMergeFila, empty);
    }
    // entre tratador e loader (Merge)
    {
        lock_guard<mutex> lock(tratLoadMutex);
        queue<LoaderItem> empty;
        swap(tratadorLoaderFila, empty);
    }  
    
    // arquivos variados para o merge
    vector<string> arquivoMerge = {arquivoHospitalJson};
    Extrator extra;
    Handler handler;

    // arquivos fixos para o merge
    DataFrame oms = extra.carregar(arquivoOmsJson);
    DataFrame oms_agrup = handler.groupedDf(oms, "cep" , "num_obitos", 4, false);
    
    DataFrame ss = extra.carregar(arquivoSecretariaJson);
    DataFrame ss_agrup = handler.groupedDf(ss, "cep" , "vacinado", 4, true);

    auto startmerge = chrono::high_resolution_clock::now();
    // Cria produtor e inializa-o
    thread prodMerge(produtor, arquivoMerge, true);
    
    // Cria consumidores do extrator
    vector<thread> consumidoresExtratorMerge;
    for (int i = 0; i < numConsumidores; ++i) 
    {
        consumidoresExtratorMerge.emplace_back(consumidorExtrator, i + 1, true);
    }
    // Aguarda o produtor
    prodMerge.join();
    
    // Aguarda extratores
    for (auto& t : consumidoresExtratorMerge) t.join();
    
    // Sinaliza que extração terminou e notifica tratadores (Merge)
    {
        lock_guard<mutex> lock(extTratMutex);
        extTratencerrado = true;
    }
    extTratcondVarMerge.notify_all();

    // Cria consumidores dos tratadores
    vector<thread> consumidoresTratadorMerge;
    for (int i = 0; i < numConsumidores; ++i) 
    {
        consumidoresTratadorMerge.emplace_back(consumidorMerge, i + 1, ref(oms_agrup), 
        ref(ss_agrup), "cep","internado", "num_obitos", "Total_Vacinado", numConsumidores);
    }
    
    // Aguarda tratadores
    for (auto& t : consumidoresTratadorMerge) t.join();
    
    {
        lock_guard<mutex> lock(tratLoadMutex);
        tratadorEncerrado = true;
    }
    tratLoadCondVarMerge.notify_all();
    
    vector<thread> consumidoresLoaderMerge;
    for (int i = 0; i < numConsumidores; ++i) {
        consumidoresLoaderMerge.emplace_back(consumidorLoader, i + 1, true);
    }
    
    // Aguarda loaders
    for (auto& t : consumidoresLoaderMerge) t.join();
    
    end = chrono::high_resolution_clock::now();
    chrono::duration<double> tempomerge;
    tempomerge = end - startmerge;
    
    
    // Tempo total
    tempoTotal = end - start;
    
    // ---- Exibição dos tempos ----
    cout << "\n=== Análise de Tempo por Estágio ===" << endl;
    cout << "1. Extração:    " << tempoExtracao.count() << " segundos" << endl;
    cout << "2. Tratamento: " << tempoTratamento.count() << " segundos" << endl;
    cout << "3. Loader:      " << tempoLoader.count() << " segundos" << endl;
    cout << "4. Merge:      " << tempomerge.count() << " segundos" << endl;
    cout << "---------------------------------" << endl;


    cout << "Tempo Total:   " << tempoTotal.count() << " segundos\n" << endl;
}