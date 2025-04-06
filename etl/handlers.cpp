#include "handlers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <string>

using namespace std;

// soma parcial de uma coluna (uma thread processa uma parte da coluna)
void partialSum(const vector<string>& values, size_t start, size_t end, double& sum, double& count, mutex& mtx) 
{
    //variáveis da thread corrente
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
void partialAlert(const vector<string>& values, size_t start, size_t end, double mean, vector<string>& alertas, mutex& mtx) {
    // variáveis da thread local
    vector<string> localAlerts;
    localAlerts.reserve(end - start);

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

DataFrame meanAlert(const DataFrame& input, const string& nameCol, int numThreads) 
{
    DataFrame output = input;
    auto [colValues, colType] = input.getColumn(nameCol);

    if (colType == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return output;
    }

    size_t n = colValues.size();
    size_t chunkSize = n / numThreads;

    // paralelização 1 - média
    double sum = 0.0;
    double count = 0.0;
    mutex mtxSum;

    vector<thread> threads;

    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(partialSum, cref(colValues), start, end, ref(sum), ref(count), ref(mtxSum));
    }

    for (auto& t : threads) t.join();
    threads.clear();

    //média calculada0 totalmente
    double mean = sum / count;

    // paralelização 2 - identificar linhas acima da média
    vector<string> alertas(n);
    mutex mtxAlerts;

    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(partialAlert, cref(colValues), start, end, mean, ref(alertas), ref(mtxAlerts));
    }

    for (auto& t : threads) t.join();

    output.addColumn("Alertas", ColumnType::STRING, alertas);
    cout << "Alertas criados" << endl;
    return output;
}

// soma parcial de uma coluna (uma thread processa uma parte da coluna)
void getColPar(const DataFrame& input, size_t start, size_t end, size_t colIndexGroup, 
                size_t colIndexAgg, mutex& mtx, vector<string>& colValues, vector<string>& colValuesAgg) 
{
    //variáveis da thread corrente
    vector<string> localColValues;
    vector<string> localColValuesAgg;
    localColValues.reserve(end - start);
    localColValuesAgg.reserve(end - start);

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

void agregarGrupo(const std::vector<int>& grupos,
    const std::vector<int>& ColOriginal,
    const std::vector<int>& aggColOriginal, mutex& totalsMutex, 
    std::unordered_map<std::string, double>& regionTotals) 
{
    std::unordered_map<std::string, double> localMap;

    for (int groupValue : grupos) 
    {
        double total = 0.0;
        for (size_t i = 0; i < ColOriginal.size(); ++i) 
        {
            if (ColOriginal[i] == groupValue) 
            {
            total += aggColOriginal[i];
            }
        }
        localMap[std::to_string(groupValue)] = total;
    }

    // Atualiza o mapa global com mutex
    std::lock_guard<std::mutex> lock(totalsMutex);
    for (const auto& [grupo, total] : localMap) 
    {
        regionTotals[grupo] = total;
    }
}


DataFrame groupedDf(const DataFrame& input, const string& groupedCol, const string& aggCol, int numThreads) 
{
    // DataFrame output = input;
    auto idxGroup = input.colIdx(groupedCol);
    auto idxAgg = input.colIdx(aggCol);

    if (input.typeCol(idxGroup) == ColumnType::STRING || input.typeCol(idxAgg) == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return input;
    }

    // size_t numThreads = thread::hardware_concurrency();
    size_t n = input.size();
    size_t chunkSize = n / numThreads;

    // paralelização 1 - pegar os valores das colunas de agregação e agrupamento
    vector<string> colValuesGroup(n);
    vector<string> colValuesAgg(n);
    mutex mtxCol;

    vector<thread> threads;

    for (size_t i = 0; i < numThreads; ++i) 
    {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? n : start + chunkSize;
        threads.emplace_back(getColPar, cref(input), start, end, idxGroup, idxAgg, ref(mtxCol), ref(colValuesGroup), ref(colValuesAgg));
    }

    for (auto& t : threads) t.join();
    threads.clear();

    vector<int> groupColOriginal, aggColOriginal;
    for (const auto& val : colValuesGroup) groupColOriginal.push_back(stoi(val));
    for (const auto& val : colValuesAgg) aggColOriginal.push_back(stoi(val));

    vector<int> intGroupValues = groupColOriginal;
    sort(intGroupValues.begin(), intGroupValues.end()); 
    auto last = unique(intGroupValues.begin(), intGroupValues.end());
    intGroupValues.erase(last, intGroupValues.end());

    mutex totalsMutex;
    unordered_map<string, double> regionTotals;

    size_t step = intGroupValues.size() / numThreads;
    for (int t = 0; t < numThreads; ++t) {
        size_t start = t * step;
        size_t end = (t == numThreads - 1) ? intGroupValues.size() : start + step;

        vector<int> gruposResponsaveis(intGroupValues.begin() + start, intGroupValues.begin() + end);

        threads.emplace_back(agregarGrupo, gruposResponsaveis,
                             cref(groupColOriginal),
                             cref(aggColOriginal),
                             ref(totalsMutex), ref(regionTotals));
    }

    for (auto& t : threads) t.join();

    DataFrame output({ "Grupos", "Agregação" }, { ColumnType::STRING, ColumnType::DOUBLE });
    for (const auto& [grupo, total] : regionTotals) {
        output.addRow({ grupo, to_string(total) });
    }

    cout << "DataFrame agrupado criado" << endl;

    return output;
}




//TODO 
// uma thread vai receber um conjunto de linhas e deve pegar o elemento que está na posição da coluna passada
// e adicionar a um vetor compartilhada de valores dessa coluna

// com todos os valores reunidos fazer um sort e unique para tirar as duplicatas

// cria threads que ficam responsáveis de agregar os valores de uma certa quantidade de regiões 
// depois de agregados juntar esses valores um um novo dataframe só com as regiões e seu total 
/*
DataFrame groupRegions(const DataFrame& input, const string& groupedCol, const string& aggCol) 
{
    tuple<vector<string>, ColumnType> coluna;
    
    auto [groupValues, groupType] = input.getColumn(groupedCol);
    auto [aggValues, aggType] = input.getColumn(aggCol);
    
    if (groupType == ColumnType::STRING || aggType == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return input;
    }
    
    vector<int> intGroupValues = stringToInt(groupValues);
    vector<int> intAggValues = stringToInt(aggValues);
    
    // ordenar e tirar duplicatas consecutivas
    sort(intGroupValues.begin(), intGroupValues.end()); 
    auto last = unique(intGroupValues.begin(), intGroupValues.end());
    intGroupValues.erase(last, intGroupValues.end());
    
    // Cria um mapa para armazenar os totais por região
    std::unordered_map<string, double> regionTotals;
    
    for (const auto& groupValue : intGroupValues) 
    {
        double total = 0.0;
        for (size_t i = 0; i < intGroupValues.size(); ++i) 
        {
            if (intGroupValues[i] == groupValue) 
            {
                total += intAggValues[i];
            }
        }
        regionTotals[to_string(groupValue)] = total;
    }
    
    // Adiciona os resultados ao DataFrame de saída
    vector<string> nomesColunas;
    nomesColunas.push_back("Grupos");
    nomesColunas.push_back("Agregação");
    
    vector<ColumnType> tipos;
    tipos.push_back(ColumnType::STRING);
    tipos.push_back(ColumnType::DOUBLE);
    
    DataFrame output = DataFrame(nomesColunas, tipos);
    
    for (const auto& pair : regionTotals) 
    {
        output.addRow({pair.first, to_string(pair.second)});
    }
    
    return output;
}

vector<int> stringToInt(const vector<string>& strVec) {
    vector<int> intVec;
    for (const auto& str : strVec) {
        intVec.push_back(stoi(str));
    }
    return intVec;
}

*/



/*

teste da main

Extrator extrator;

// Teste com um arquivo .csv (como um dos hospitais)
DataFrame df_csv = extrator.carregar("hospital_mock_1.csv");
std::cout << "\nArquivo CSV carregado com sucesso:\n";
df_csv.display();
DataFrame teste = meanAlert(df_csv, "Idade");
teste.display();

teste = groupRegions(df_csv, "CEP", "Idade");
teste.display();

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