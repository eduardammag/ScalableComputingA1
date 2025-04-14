#include "handlers.hpp"
#include "dataframe.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <variant>

//para fazer unique e sort
#include <algorithm>

using namespace std;

// soma parcial de uma coluna (uma thread processa uma parte da coluna)
void Handler::partialSum(const vector<Cell>& values, size_t start, size_t end, double& sum, double& count, mutex& mtx) 
{
    //variáveis da thread local
    double localSum = 0.0;
    double localCount = 0.0;

    for (size_t i = start; i < end; ++i) 
    {
        try
        {
            localSum += toDouble(values[i]);
            localCount += 1.0;
        } catch (...) {}
    }

    //protege a variável compartilhada
    lock_guard<mutex> lock(mtx);
    sum += localSum;
    count += localCount;
}

// função para verificar quais linhas estão acima da média (uma thread processa uma parte da coluna)
void Handler::partialAlert(const vector<Cell>& values, size_t start, size_t end, double mean, vector<string>& alertas, mutex& mtx)
{
    // variáveis da thread local
    vector<string> localAlerts;
    localAlerts.reserve(end - start);

    //percorre as linhas do df e verifica se estão acima da média
    for (size_t i = start; i < end; ++i)
    {
        try
        {
            localAlerts.push_back(toDouble(values[i]) > mean ? "True" : "False");
        } catch (...)
        {
            localAlerts.push_back("False"); //ou "Erro"
        }
    }
    
    // protegendo região crítica
    lock_guard<mutex> lock(mtx);
    for (size_t i = 0; i < localAlerts.size(); ++i)
    {
        alertas[start + i] = localAlerts[i];
    }
}

// monta uma única coluna em paralelo
// o dataframe esta estruturado por linhas, então devemos acessar as linhas e pegar o elemento no índice da coluna
void Handler::getSingleColPar(const DataFrame& input, size_t start, size_t end, size_t colIndex, 
    mutex& mtx, vector<Cell>& colValues) 
{   
    vector<Cell> localColValues;
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
    DataFrame output = input;
    int colIndex = input.colIdx(nameCol);
    
    if (input.typeCol(colIndex) == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return output;
    }

    int n = input.size();
    int chunkSize = n / numThreads;
    //vetor compartilhado
    vector<Cell> colValues(n);

    //Paralelização 0 - obter os valores da coluna
    mutex mtxCol;
    vector<thread> threads;
    
    for (int i = 0; i < numThreads; ++i) 
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;

        threads.emplace_back([&input, &colValues, colIndex, start, end, &mtxCol]()
        {
            vector<Cell> localCol;
            localCol.reserve(end - start);
            for (int j = start; j < end; ++j)
            {
                localCol.push_back(input.getRow(j)[colIndex]);
            }
            lock_guard<mutex> lock(mtxCol);
            for (int j = 0; j < end - start; ++j)
            {
                colValues[start + j] = localCol[j];
            }
        });
    }

    for (auto& t : threads) t.join();
    threads.clear();

    // paralelização 1 - média
    double sum = 0.0;
    double count = 0.0;
    mutex mtxSum;

    //percorre pedaços da coluna somando
    for (int i = 0; i < numThreads; ++i) 
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;

        threads.emplace_back([&colValues, start, end, &sum, &count, &mtxSum]()
        {
            double localSum = 0.0;
            double localCount = 0.0;
            for (int j = start; j < end; ++j)
            {
                const Cell& val = colValues[j];
                try
                {
                    localSum += toDouble(val);
                    localCount += 1.0;
                } catch (...) {}
            }
            lock_guard<mutex> lock(mtxSum);
            sum += localSum;
            count += localCount;
        });
    }

    for (auto& t : threads) t.join();
    threads.clear();

    double mean = (count > 0.0) ? sum / count : 0.0;

    // paralelização 2 - Gerar vetor de alertas (True se valor > média)
    //vetor compartilhado
    vector<Cell> alertas(n);
    mutex mtxAlerts;

    // percorre a coluna original identificando as linhas acima da média
    for (int i = 0; i < numThreads; ++i)
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;

        threads.emplace_back([&colValues, start, end, mean, &alertas, &mtxAlerts]()
        {
            vector<Cell> localAlerts(end - start);

            for (int j = start; j < end; ++j)
            {
                const Cell& val = colValues[j];

                try
                {
                    localAlerts[j - start] = toDouble(val) > mean ? "True" : "False";
                } catch (...)
                {
                    localAlerts[j - start] = "False"; //ou "Erro"
                }    
            }
            
            lock_guard<mutex> lock(mtxAlerts);
            for (size_t j = 0; j < localAlerts.size(); ++j)
            {
                alertas[start + j] = localAlerts[j];
            }
        });
    }

    for (auto& t : threads) t.join();

    //adiciona a nova coluna de alertas ao dataframe
    output.addColumn("Alertas", ColumnType::STRING, alertas);
    return output;
}

//pega duas colunas ao mesmo tempo, uma para o agrupamento e outra para a agregação
void Handler::getColGroup(const DataFrame& input, size_t start, size_t end, size_t colIndexGroup, 
                size_t colIndexAgg, mutex& mtx, vector<Cell>& colValues, vector<Cell>& colValuesAgg) 
{
    //variáveis da thread corrente
    vector<Cell> localColValues;
    vector<Cell> localColValuesAgg;
    localColValues.reserve(end - start);
    localColValuesAgg.reserve(end - start);

    // acessando os valores das colunas passadas
    for (size_t i = start; i < end; ++i) 
    {
        const auto& row = input.getRow(i);
        localColValues.push_back(row[colIndexGroup]);
        localColValuesAgg.push_back(row[colIndexAgg]);
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
void Handler::agregarGrupoPar(const vector<Cell>& uniqueGroups,
    const vector<Cell>& ColOriginal,
    const vector<Cell>& aggColOriginal, mutex& totalsMutex, 
    unordered_map<string, double>& regionTotals) 
{   
    // mapemaento local para armazenar os totais por grupo
    unordered_map<string, double> localMap;

    // iterando sobre os grupos dessa thread
    for (const auto& group : uniqueGroups) 
    {
        string strGroup = toString(group);

        double total = 0.0;

        // somando os valores
        for (size_t i = 0; i < ColOriginal.size(); ++i)
        {
            if (ColOriginal[i] == group)
            {
                total += visit([](auto&& val) -> double
                {
                    using T = decay_t<decltype(val)>;
                    if constexpr (is_same_v<T, string>) return stod(val);
                    else return static_cast<double>(val);
                }, aggColOriginal[i]);
            }
        }
        localMap[strGroup] = total;
    }

    // Atualiza o mapa global com mutex
    lock_guard<mutex> lock(totalsMutex);
    for (const auto& [group, total] : localMap) 
    {
        regionTotals[group] = total;
    }
}

// faz o agrupamento e a agregação de duas colunas
DataFrame Handler::groupedDf(const DataFrame& input, const string& groupedCol, const string& aggCol, int numThreads) 
{
    const int colIdxGroup = input.colIdx(groupedCol);
    const int colIdxAgg = input.colIdx(aggCol);
    const int numRows = input.size();

    if (numThreads == 0) throw invalid_argument("Número de threads deve ser maior que zero.");

    const int chunkSize = (numRows + numThreads - 1) / numThreads;

    // Fase 1: Extração paralela
    vector<int> groupKeys(numRows);
    vector<double> aggValues(numRows);
    vector<thread> threads;

    for (int t = 0; t < numThreads; ++t) 
    {
        threads.emplace_back([&, t]()
        {
            const int start = t * chunkSize;
            const int end = min(start + chunkSize, numRows);

            for (int i = start; i < end; ++i)
            {
                const auto& row = input.getRow(i);
                const Cell& groupCell = row[colIdxGroup];
                const Cell& aggCell = row[colIdxAgg];

                int groupKey;
                if (holds_alternative<int>(groupCell)) groupKey = stoi(get<string>(groupCell));
                else if (holds_alternative<double>(groupCell)) groupKey = static_cast<int>(get<double>(groupCell));
                else groupKey = stoi(get<string>(groupCell));

                groupKeys[i] = groupKey;
                aggValues[i] = toDouble(aggCell);
            }
        });
    }

    for (auto& thread : threads) thread.join();
    threads.clear();

    // Fase 2: Agregação paralela
    vector<unordered_map<int, double>> partialSums(numThreads);
    
    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            const int start = t* chunkSize;
            const int end = min(start + chunkSize, numRows);

            for (int i = start; i < end; ++i)
            {
                partialSums[t][groupKeys[i]] += aggValues[i];
            }
        });
    }
    for (auto& thread : threads) thread.join();

    //Combinando resultados
    unordered_map<int, double> totalSums;
    for (const auto& map : partialSums)
    {
        for (const auto& [key, value] : map)
        {
            totalSums[key] += value;
        }
    }

    //Construindo o DataFrame de saída
    DataFrame output({groupedCol, "Total_" + aggCol}, { ColumnType::STRING, ColumnType::DOUBLE});

    for (const auto& [key, sum] : totalSums) 
    {
        output.addRow({to_string(key), to_string(sum)});
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