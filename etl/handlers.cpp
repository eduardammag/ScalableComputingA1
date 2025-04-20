#include "handlers.hpp"
#include "dataframe.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <variant>
#include <algorithm>

// soma parcial de uma coluna (uma thread processa uma parte da coluna)
void Handler::partialSum(const vector<Cell>& values, size_t start, size_t end, double& sum, mutex& mtx) 
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
}

// função para verificar quais linhas estão acima da média (uma thread processa uma parte da coluna)
void Handler::partialAlert(const vector<Cell>& values, size_t start, size_t end, double mean, vector<string>& alertas)
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
            localAlerts.push_back("False"); 
        }
    }
    
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

// função de gerar alertas para registros acima da média, inplace
void Handler::meanAlert(DataFrame& input, const string& nameCol, int numThreads) 
{
    int colIndex = input.colIdx(nameCol);
    
    if (input.typeCol(colIndex) == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
    }
    
    int n = input.size();
    //Garante chunkSize mínimo de 1
    int chunkSize = max(n / numThreads, 1); 
    
    // paralelização 1 - média
    double sum = 0.0;
    mutex mtxSum;
    vector<thread> threads;
    
    //percorre pedaços da coluna somando
    for (int i = 0; i < numThreads; ++i) 
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;
        
        threads.emplace_back([&input, colIndex, start, end, &sum, &mtxSum]()
        {
            double localSum = 0.0;
            for (int j = start; j < end; ++j)
            {
                //Acesso direto sem cópia
                const Cell& val = input.getRow(j)[colIndex]; 
                try
                {
                    localSum += toDouble(val);
                } catch (...) {}
            }
            lock_guard<mutex> lock(mtxSum);
            sum += localSum;
        });
    }
    
    for (auto& t : threads) t.join();
    threads.clear();
    
    double mean = sum / n;
    
    // paralelização 2 - Gerar vetor de alertas (True se valor > média)
    //vetor compartilhado
    vector<Cell> alertas(n);
    mutex mtxAlerts;
    
    // percorre a coluna original identificando as linhas acima da média
    for (int i = 0; i < numThreads; ++i)
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;
        
        threads.emplace_back([&input, colIndex, start, end, mean, &alertas, &mtxAlerts]()
        {
            vector<Cell> localAlerts(end - start);
            
            for (int j = start; j < end; ++j)
            {
                const Cell& val = input.getRow(j)[colIndex]; 
                
                try
                {
                    localAlerts[j - start] = toDouble(val) > mean ? "True" : "False";
                } catch (...)
                {
                    localAlerts[j - start] = "False"; 
                }    
            }

            // não tem concorreência pois cada thread modifica o seu munícipio
            for (size_t j = 0; j < localAlerts.size(); ++j)
            {
                alertas[start + j] = localAlerts[j];
            }
        });
    }
    
    for (auto& t : threads) t.join();

    input.addColumn("Alertas", ColumnType::STRING, alertas, numThreads);

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
                if (holds_alternative<int>(groupCell)) {
                    groupKey = get<int>(groupCell);
                } else if (holds_alternative<double>(groupCell)) {
                    groupKey = static_cast<int>(get<double>(groupCell));
                } else if (holds_alternative<string>(groupCell)) {
                    groupKey = stoi(get<string>(groupCell));
                } else {
                    throw runtime_error("Tipo inválido em coluna de agrupamento.");
                }

                groupKeys[i] = groupKey;
                aggValues[i] = toDouble(aggCell);
            }
        });
    }

    for (auto& thread : threads) thread.join();
    threads.clear();

    // Fase 2: Agregação paralela
    vector<unordered_map<int, double>> partialSums(numThreads);
    
    std::mutex sumMutex;

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
    std::mutex outputMutex;

    for (const auto& [key, sum] : totalSums) 
    {
        std::lock_guard<std::mutex> lock(outputMutex);
        output.addRow({to_string(key), sum});
    }

    return output;
}


// Handler para limpeza de dados - remove duplicatas e linhas/colunas com muitos valores nulos
void Handler::dataCleaner(DataFrame& input, int numThreads)
{
    // Primeiro passo: remover linhas duplicadas
    removeDuplicateRows(input, numThreads);
    
    // Segundo passo: remover linhas com muitos valores nulos
    removeSparseRows(input, 0.9, numThreads);
    
    // Terceiro passo: remover colunas com muitos valores nulos
    removeSparseColumns(input, 0.9, numThreads);
}

// Função auxiliar para verificar se uma célula é nula
bool Handler::isNullCell(const Cell& cell)
{
    // Verifica se é string vazia (assumindo que strings vazias representam nulos)
    if (holds_alternative<string>(cell)) return get<string>(cell).empty();
    else if (holds_alternative<int>(cell)) return get<int>(cell) == 0;
    else if (holds_alternative<double>(cell)) return get<double>(cell) == 0.0;
    return false; // Outros tipos não são considerados nulos nesta implementação
}

// Função auxiliar para remover linhas duplicadas
void Handler::removeDuplicateRows(DataFrame& input, int numThreads)
{
    const int numRows = input.size();
    if (numRows == 0) return;

    const int numCols = input.numCols();
    vector<bool> toKeep(numRows, true);
    mutex mtx;

    // Dividir o trabalho entre threads para encontrar duplicatas
    int chunkSize = max(numRows / numThreads, 1);
    vector<thread> threads;

    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]() {
            const int start = t * chunkSize;
            const int end = min(start + chunkSize, numRows);

            for (int i = start; i < end; ++i)
            {
                if (!toKeep[i]) continue; // Já marcado para remoção

                const auto& currentRow = input.getRow(i);
                
                // Verificar duplicatas nas linhas seguintes
                for (int j = i + 1; j < numRows; ++j)
                {
                    if (!toKeep[j]) continue;

                    bool isDuplicate = true;
                    const auto& compareRow = input.getRow(j);
                    
                    for (int col = 0; col < numCols; ++col)
                    {
                        if (currentRow[col] != compareRow[col])
                        {
                            isDuplicate = false;
                            break;
                        }
                    }

                    if (isDuplicate) {
                        lock_guard<mutex> lock(mtx);
                        toKeep[j] = false;
                    }
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    // Obter nomes e tipos das colunas originais
    vector<string> colNames = input.getColumnNames();
    vector<ColumnType> colTypes;
    for (int i = 0; i < numCols; ++i)
    {
        colTypes.push_back(input.typeCol(i));
    }

    // Criar novo DataFrame sem as linhas duplicadas
    DataFrame cleaned{colNames, colTypes};
    for (int i = 0; i < numRows; ++i)
    {
        if (toKeep[i]) cleaned.addRow(input.getRow(i));
    }

    input = move(cleaned);
}

// Função auxiliar para remover linhas com muitos valores nulos
void Handler::removeSparseRows(DataFrame& input, double nullThreshold, int numThreads)
{
    const int numRows = input.size();
    if (numRows == 0) return;

    const int numCols = input.numCols();
    const int thresholdCount = numCols * nullThreshold;
    vector<bool> toKeep(numRows, true);
    mutex mtx;

    // Dividir o trabalho entre threads para verificar linhas esparsas
    int chunkSize = max(numRows / numThreads, 1);
    vector<thread> threads;

    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            const int start = t * chunkSize;
            const int end = min(start + chunkSize, numRows);

            for (int i = start; i < end; ++i)
            {
                int nullCount = 0;
                const auto& row = input.getRow(i);
                
                for (int col = 0; col < numCols; ++col)
                {
                    if (isNullCell(row[col]))
                    {
                        nullCount++;
                    }
                }

                if (nullCount >= thresholdCount)
                {
                    lock_guard<mutex> lock(mtx);
                    toKeep[i] = false;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    // Obter nomes e tipos das colunas originais
    vector<string> colNames = input.getColumnNames();
    vector<ColumnType> colTypes;
    for (int i = 0; i < numCols; ++i)
    {
        colTypes.push_back(input.typeCol(i));
    }

    // Criar novo DataFrame sem as linhas esparsas
    DataFrame cleaned(colNames, colTypes);
    for (int i = 0; i < numRows; ++i)
    {
        if (toKeep[i])
        {
            cleaned.addRow(input.getRow(i));
        }
    }

    input = move(cleaned);
}

// Função auxiliar para remover colunas com muitos valores nulos
void Handler::removeSparseColumns(DataFrame& input, double nullThreshold, int numThreads)
{
    const int numRows = input.size();
    if (numRows == 0) return;

    const int numCols = input.numCols();
    const int thresholdCount = numRows * nullThreshold;
    vector<bool> toKeep(numCols, true);
    vector<int> nullCounts(numCols, 0);
    mutex mtx;

    // Dividir o trabalho entre threads para contar nulos por coluna
    int chunkSize = max(numRows / numThreads, 1);
    vector<thread> threads;

    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            const int start = t * chunkSize;
            const int end = min(start + chunkSize, numRows);

            vector<int> localNullCounts(numCols, 0);

            for (int i = start; i < end; ++i)
            {
                const auto& row = input.getRow(i);
                
                for (int col = 0; col < numCols; ++col)
                {
                    if (isNullCell(row[col]))
                    {
                        localNullCounts[col]++;
                    }
                }
            }

            // Atualizar contagens globais
            lock_guard<mutex> lock(mtx);
            for (int col = 0; col < numCols; ++col)
            {
                nullCounts[col] += localNullCounts[col];
            }
        });
    }

    for (auto& t : threads) t.join();

    // Determinar quais colunas manter
    for (int col = 0; col < numCols; ++col)
    {
        if (nullCounts[col] >= thresholdCount) toKeep[col] = false;
    }

    // Obter nomes e tipos das colunas que serão mantidas
    vector<string> colNames = input.getColumnNames();
    vector<string> newColNames;
    vector<ColumnType> newColTypes;
    
    for (int col = 0; col < numCols; ++col)
    {
        if (toKeep[col])
        {
            newColNames.push_back(colNames[col]);
            newColTypes.push_back(input.typeCol(col));
        }
    }

    // Cria DataFrame sem as colunas esparsas
    DataFrame cleaned(newColNames, newColTypes);
    for (int i = 0; i < numRows; ++i) 
    {
        const auto& row = input.getRow(i);
        vector<Cell> newRow;
        
        for (int col = 0; col < numCols; ++col)
        {
            if (toKeep[col]) newRow.push_back(row[col]);
        }
        
        cleaned.addRow(newRow);
    }

    input = move(cleaned);
}
/*

teste da main

Extrator extrator;
        DataFrame df_csv = extrator.carregar("hospital_mock_1.csv");
        // cout << "\nArquivo CSV carregado com sucesso:\n";
        DataFrame teste = groupedDf(df_csv, "CEP", "Idade",5);
        // df_csv.display();
        for (int n = 1; n <= 8; n += 1) {
                // cout << "\n--- Testando com " << n << " consumidor(es) ---\n";
                auto inicio = chrono::high_resolution_clock::now();
        
                // executarPipeline(n);  // Pipeline com n consumidores
                teste = groupedDf(df_csv, "CEP", "Idade",n);
        
                auto fim = chrono::high_resolution_clock::now();
                chrono::duration<double> duracao = fim - inicio;
                teste.display();
        
                // cout << "Tempo: " << duracao.count() << " segundos.\n";
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