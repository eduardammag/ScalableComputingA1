#include "handlers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>

//para fazer unique e sort
#include <algorithm>

using namespace std;

// soma parcial de uma coluna (uma thread processa uma parte da coluna)
void Handler::partialSum(const vector<string>& values, size_t start, size_t end, double& sum, double& count, mutex& mtx) 
{
    //variáveis da thread local
    double localSum = 0.0;
    double localCount = 0.0;

    for (size_t i = start; i < end; ++i) 
    {
        localSum += stoi(values[i]);
        localCount += 1.0;
    }

    //protege a variável compartilhada
    lock_guard<mutex> lock(mtx);
    sum += localSum;
    count += localCount;
}

// função para verificar quais linhas estão acima da média (uma thread processa uma parte da coluna)
void Handler::partialAlert(const vector<string>& values, size_t start, size_t end, double mean, vector<string>& alertas, mutex& mtx) {
    // variáveis da thread local
    vector<string> localAlerts;
    localAlerts.reserve(end - start);

    //percorre as linhas do df e verifica se estão acima da média
    for (size_t i = start; i < end; ++i) {
        if (stoi(values[i]) > mean)
            localAlerts.push_back("True");
        else
            localAlerts.push_back("False");
    }
    
    // protegendo região crítica
    lock_guard<mutex> lock(mtx);
    for (size_t i = 0; i < localAlerts.size(); ++i) {
        alertas[start + i] = localAlerts[i];
    }
}

// monta uma única coluna em paralelo
// o dataframe esta estruturado por linhas, então devemos acessar as linhas e pegar o elemento no índice da coluna
void Handler::getSingleColPar(const DataFrame& input, size_t start, size_t end, size_t colIndex, 
    mutex& mtx, vector<string>& colValues) 
{   
    vector<string> localColValues;
    localColValues.reserve(end - start);

    // percorre as linhas do df e acessa a coluna passada
    for (size_t i = start; i < end; ++i) 
    {
        localColValues.push_back(input.getRow(i)[colIndex]);
    }

    //adicicionando os valores locais ao vetor compartilhado
    lock_guard<mutex> lock(mtx);
    for (size_t i = 0; i < localColValues.size(); ++i) 
    {
        colValues[start + i] = localColValues[i];
    }
}

// função de gerar alertas para registros acima da média
DataFrame Handler::meanAlert(const DataFrame& input, const string& nameCol, int numThreads) 
{
    // inicialização
    DataFrame output = input;
    size_t colIndex = input.colIdx(nameCol);
    
    if (input.typeCol(colIndex) == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return output;
    }

    size_t n = input.size();
    size_t chunkSize = n / numThreads;
    // vetor compartilhado
    vector<string> colValues(n);

    // inicialização do as threads
    mutex mtxCol;
    vector<thread> threads;
    
    Handler handler;
    // paralelização 0 - montar a coluna e pegar os valores
    for (int i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(&Handler::getSingleColPar, &handler, cref(input), start, end, colIndex, ref(mtxCol), ref(colValues));
    }

    for (auto& t : threads) t.join();
    threads.clear();

    // paralelização 1 - média
    double sum = 0.0;
    double count = 0.0;
    mutex mtxSum;

    //percorre pedaços da coluna somando
    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(&Handler::partialSum, &handler, cref(colValues), start, end, ref(sum), ref(count), ref(mtxSum));
    }

    for (auto& t : threads) t.join();
    threads.clear();

    //média calculada totalmente
    double mean = sum / count;

    // paralelização 2 - identificar linhas acima da média
    // nova coluna de True/False
    //vetor compartilhado
    vector<string> alertas(n);
    mutex mtxAlerts;

    // percorre a coluna original identificando as linhas acima da média
    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(&Handler::partialAlert, &handler, cref(colValues), start, end, mean, ref(alertas), ref(mtxAlerts));
    }

    for (auto& t : threads) t.join();

    //adiciona a nova coluna de alertas ao dataframe
    output.addColumn("Alertas", ColumnType::STRING, alertas);
    // cout << "Alertas criados" << endl;
    return output;
}

//pega duas colunas ao mesmo tempo, uma para o agrupamento e outra para a agregação
void Handler::getColGroup(const DataFrame& input, size_t start, size_t end, size_t colIndexGroup, 
                size_t colIndexAgg, mutex& mtx, vector<string>& colValues, vector<string>& colValuesAgg) 
{
    //variáveis da thread corrente
    vector<string> localColValues;
    vector<string> localColValuesAgg;
    localColValues.reserve(end - start);
    localColValuesAgg.reserve(end - start);

    // acessando os valores das colunas passadas
    for (size_t i = start; i < end; ++i) 
    {
        localColValues.push_back(input.getRow(i)[colIndexGroup]);
        localColValuesAgg.push_back(input.getRow(i)[colIndexAgg]);
    }

    //protege a variável compartilhada
    lock_guard<mutex> lock(mtx);
    for (size_t i = 0; i < localColValues.size(); ++i) 
    {
        colValues[start + i] = localColValues[i];
        colValuesAgg[start + i] = localColValuesAgg[i];
    }
}

// agregação de grupos de uma mesma thread
<<<<<<< HEAD
void agregarGrupoPar(const vector<int>& grupos,
=======
void Handler::agregarGrupoPar(const vector<int>& grupos,
>>>>>>> 788ce7245c4f4c5fa096752c61703c88178c8343
    const vector<int>& ColOriginal,
    const vector<int>& aggColOriginal, mutex& totalsMutex, 
    unordered_map<string, double>& regionTotals) 
{   
    // mapemaento local para armazenar os totais por grupo
    unordered_map<string, double> localMap;

    // iterando sobre os grupos dessa thread
    for (int groupValue : grupos) 
    {
        double total = 0.0;
        // somando os valores
        for (size_t i = 0; i < ColOriginal.size(); ++i) 
        {
            if (ColOriginal[i] == groupValue) 
            {
            total += aggColOriginal[i];
            }
        }
        localMap[to_string(groupValue)] = total;
    }

    // Atualiza o mapa global com mutex
    lock_guard<mutex> lock(totalsMutex);
    for (const auto& [grupo, total] : localMap) 
    {
        regionTotals[grupo] = total;
    }
}

// faz o agrupamento e a agregação de duas colunas
DataFrame Handler::groupedDf(const DataFrame& input, const string& groupedCol, const string& aggCol, int numThreads) 
{
    auto idxGroup = input.colIdx(groupedCol);
    auto idxAgg = input.colIdx(aggCol);

    if (input.typeCol(idxGroup) == ColumnType::STRING || input.typeCol(idxAgg) == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return input;
    }

    // inicialização das threads
    size_t n = input.size();
    size_t chunkSize = n / numThreads;
    vector<thread> threads;
    mutex mtxCol;

    // paralelização 1 - pegar os valores das colunas de agregação e agrupamento
    vector<string> colValuesGroup(n);
    vector<string> colValuesAgg(n);
    Handler handler;
    // montando as duas colunas passadas
    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(&Handler::getColGroup, &handler, cref(input), start, end, idxGroup, idxAgg, ref(mtxCol), ref(colValuesGroup), ref(colValuesAgg));
    }

    for (auto& t : threads) t.join();
    threads.clear();

    // convertendo a coluna para inteiros
    vector<int> groupColOriginal, aggColOriginal;
    for (const auto& val : colValuesGroup) groupColOriginal.push_back(stoi(val));
    for (const auto& val : colValuesAgg) aggColOriginal.push_back(stoi(val));

    // tirando as duplicatas dos grupos, primeiro ordena e depois tira as duplicatas consecutivas
    vector<int> intGroupValues = groupColOriginal;
    sort(intGroupValues.begin(), intGroupValues.end()); 
    auto last = unique(intGroupValues.begin(), intGroupValues.end());
    intGroupValues.erase(last, intGroupValues.end());

    mutex totalsMutex;
    unordered_map<string, double> regionTotals;

    // paralelização 2 - cada thread agrega uma certa quantidade de grupos
    size_t step = intGroupValues.size() / numThreads;
    for (int t = 0; t < numThreads; ++t) 
    {
        size_t start = t * step;
        size_t end = (t == numThreads - 1) ? intGroupValues.size() : start + step;

        vector<int> gruposLocais(intGroupValues.begin() + start, intGroupValues.begin() + end);

        threads.emplace_back(&Handler::agregarGrupoPar, &handler, gruposLocais, cref(groupColOriginal),
                             cref(aggColOriginal), ref(totalsMutex), ref(regionTotals));
    }

    for (auto& t : threads) t.join();

    // inicialização um novo dataframe só com a coluna de grupos e agregação 
    DataFrame output({ "Grupos", "Agregação" }, { ColumnType::STRING, ColumnType::DOUBLE });

    for (const auto& [grupo, total] : regionTotals) 
    {
        output.addRow({ grupo, to_string(total) });
    }

    cout << "DataFrame agrupado criado" << endl;

    return output;
}


/*

teste da main

Extrator extrator;
        DataFrame df_csv = extrator.carregar("hospital_mock_1.csv");
        cout << "\nArquivo CSV carregado com sucesso:\n";
        DataFrame teste = groupedDf(df_csv, "CEP", "Idade",5);
        // df_csv.display();
        for (int n = 1; n <= 8; n += 1) {
                cout << "\n--- Testando com " << n << " consumidor(es) ---\n";
                auto inicio = chrono::high_resolution_clock::now();
        
                // executarPipeline(n);  // Pipeline com n consumidores
                teste = groupedDf(df_csv, "CEP", "Idade",n);
        
                auto fim = chrono::high_resolution_clock::now();
                chrono::duration<double> duracao = fim - inicio;
                teste.display();
        
                cout << "Tempo: " << duracao.count() << " segundos.\n";
            }


// 1. DataCleaner - Remove nulos e duplicatas
DataFrame DataCleaner::process(const DataFrame& input) {
    DataFrame output = input;
    output.removeNulls();
    output.removeDuplicates();
    return output;
}

// 2. OutlierDetector - Detecta valores anômalos
DataFrame OutlierDetector::process(const DataFrame& input) {
    DataFrame output = input;
    output.detectOutliers();
    return output;
}

// 3. TimeAggregator - Agrega dados por tempo
DataFrame TimeAggregator::process(const DataFrame& input) {
    DataFrame output = input;
    output.aggregateByTime("weekly");  // Suponha que esse método exista
    return output;
}

// 4. EpidemiologyAnalyzer - Analisa padrões epidemiológicos
DataFrame EpidemiologyAnalyzer::process(const DataFrame& input) {
    DataFrame output = input;
    output.analyzeCorrelations();  // Suponha que esse método exista
    return output;
}

// 5. AlertGenerator - Gera alertas de surtos
DataFrame AlertGenerator::process(const DataFrame& input) {
    DataFrame output = input;
    output.generateAlerts();
    return output;
}

*/