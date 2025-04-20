#include "handlers.hpp"
#include "dataframe.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <vector>
#include <variant>
#include <algorithm>
#include <cctype>
#include <unordered_map>

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
    
    // cout << "DataFrame agrupado criado" << endl;
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

// Tratador para validação de dados
void Handler::validateDataFrame(DataFrame& input, int numThreads)
{
    if (input.empty()) return;
    
    const int numRows = input.size();
    vector<bool> validRows(numRows, true);
    
    // Analisa as colunas para determinar regras de validação
    vector<ColumnValidationRules> rules = analyzeColumnsForValidation(input, min(20, numRows));
    
    // Valida as linhas em paralelo
    validateRows(input, rules, validRows, numThreads);
    
    // Cria novo DataFrame apenas com linhas válidas
    createValidatedDataFrame(input, validRows);
}

vector<Handler::ColumnValidationRules> Handler::analyzeColumnsForValidation(const DataFrame& df, int sampleSize)
{
    vector<ColumnValidationRules> rules;
    const int numCols = df.numCols();
    const int actualSampleSize = min(sampleSize, df.size());
    
    for (int col = 0; col < numCols; ++col)
    {
        ColumnValidationRules rule;
        rule.name = df.getColumnNames()[col];
        rule.type = df.typeCol(col);
        
        analyzeColumnPattern(df, col, actualSampleSize, rule);
        rules.push_back(rule);
    }
    
    return rules;
}

void Handler::analyzeColumnPattern(const DataFrame& df, int colIndex, int sampleSize, ColumnValidationRules& rule)
{
    set<string> uniqueValues;
    bool only01 = true;
    bool onlyTwoDigits = true;
    bool onlyFiveDigits = true;
    
    // Analisa os valores da coluna
    for (int i = 0; i < sampleSize; ++i)
    {
        const Cell& cell = df.getRow(i)[colIndex];
        string strVal = toString(cell);
        
        // Verifica padrões nos valores
        try
        {
            double val = toDouble(cell);
            if (val != 0 && val != 1) only01 = false;
            if (strVal.length() != 2) onlyTwoDigits = false;
            if (strVal.length() != 5) onlyFiveDigits = false;
        } catch (...)
        {
            only01 = false;
            onlyTwoDigits = false;
            onlyFiveDigits = false;
        }
        
        if (holds_alternative<string>(cell)) uniqueValues.insert(get<string>(cell));
    }
    
    string lowerName = toLower(rule.name);
    
    // Regras para CEPs
    if (contains(lowerName, "cep"))
    {
        if (contains(lowerName, "ilha") || contains(lowerName, "island"))
        {
            rule.isIslandCEP = true;
            rule.minLength = 2;
            rule.maxLength = 2;
        } 
        else if (contains(lowerName, "região") || contains(lowerName, "region") || contains(lowerName, "bairro")) {
            rule.isRegionCEP = true;
            rule.minLength = 5;
            rule.maxLength = 5;
        }
        else if (onlyTwoDigits && sampleSize >= 5)
        {
            rule.isIslandCEP = true;
            rule.minLength = 2;
            rule.maxLength = 2;
        }
        else if (onlyFiveDigits && sampleSize >= 5)
        {
            rule.isRegionCEP = true;
            rule.minLength = 5;
            rule.maxLength = 5;
        }
    }
    
    // Regras para outros tipos de colunas
    if (contains(lowerName, "idade") || contains(lowerName, "age"))
    {
        rule.allowNegative = false;
        rule.minValue = 0;
        rule.maxValue = 120;
    }
    else if (contains(lowerName, "óbitos") || contains(lowerName, "death"))
    {
        rule.allowNegative = false;
        rule.minValue = 0;
    }
    else if (contains(lowerName, "população") || contains(lowerName, "population"))
    {
        rule.allowNegative = false;
        rule.minValue = 0;
    }
    else if (only01 && sampleSize >= 5)
    {
        rule.isBoolean = true;
        rule.allowedStrings = {"0", "1", "false", "true"};
    }
}

void Handler::validateRows(DataFrame& input, const vector<ColumnValidationRules>& rules, vector<bool>& validRows, int numThreads)
{
    const int numRows = input.size();
    mutex mtx;
    
    int chunkSize = max(numRows / numThreads, 1);
    vector<thread> threads;
    
    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            const int start = t * chunkSize;
            const int end = (t == numThreads - 1) ? numRows : start + chunkSize;
            
            for (int i = start; i < end; ++i)
            {
                bool rowValid = true;
                const auto& row = input.getRow(i);
                
                for (size_t col = 0; col < rules.size() && rowValid; ++col)
                {
                    if (!isValidCell(row[col], rules[col])) rowValid = false;
                }
                
                if (!rowValid)
                {
                    lock_guard<mutex> lock(mtx);
                    validRows[i] = false;
                }
            }
        });
    }
    
    for (auto& t : threads) t.join();
}

bool Handler::isValidCell(const Cell& cell, const ColumnValidationRules& rule) 
{
    if (isNullCell(cell)) return true;
    
    string strVal = toString(cell);
    
    // Validação para CEPs
    if (rule.isIslandCEP || rule.isRegionCEP)
    {
        if (int(strVal.length()) != rule.maxLength) return false;
        if (!all_of(strVal.begin(), strVal.end(), ::isdigit)) return false;
        return true;
    }
    
    // Validação para booleanos
    if (rule.isBoolean) return rule.allowedStrings.find(strVal) != rule.allowedStrings.end();
    
    // Validação numérica
    try
    {
        double val = toDouble(cell);
        if (!rule.allowNegative && val < 0) return false;
        if (val < rule.minValue || val > rule.maxValue) return false;
    } catch (...)
    {
        return false;
    }
    
    return true;
}

void Handler::createValidatedDataFrame(DataFrame& input, const vector<bool>& validRows)
{
    vector<string> colNames = input.getColumnNames();
    vector<ColumnType> colTypes;
    for (int i = 0; i < input.numCols(); ++i)
    {
        colTypes.push_back(input.typeCol(i));
    }
    
    DataFrame cleaned(colNames, colTypes);
    for (size_t i = 0; i < validRows.size(); ++i)
    {
        if (validRows[i])
        {
            cleaned.addRow(input.getRow(i));
        }
    }
    
    input = move(cleaned);
}

void Handler::filterInvalidAges(DataFrame& input, const string& ageColumnName, int maxAge, int numThreads)
{
    int colIndex = input.colIdx(ageColumnName);
    if (colIndex == -1) return;
    
    const int numRows = input.size();
    vector<bool> validRows(numRows, true);
    mutex mtx;
    
    int chunkSize = max(numRows / numThreads, 1);
    vector<thread> threads;
    
    for (int t = 0; t < numThreads; ++t)
    {
        threads.emplace_back([&, t]() {
            const int start = t * chunkSize;
            const int end = (t == numThreads - 1) ? numRows : start + chunkSize;
            
            for (int i = start; i < end; ++i)
            {
                const Cell& cell = input.getRow(i)[colIndex];
                
                try 
                {
                    double age = toDouble(cell);
                    if (age < 0 || age > maxAge)
                    {
                        lock_guard<mutex> lock(mtx);
                        validRows[i] = false;
                    }
                } catch (...)
                {
                    lock_guard<mutex> lock(mtx);
                    validRows[i] = false;
                }
            }
        });
    }
    
    for (auto& t : threads) t.join();
    createValidatedDataFrame(input, validRows);
}

// Funções auxiliares

string Handler::toLower(const string& s)
{
    string result = s;
    transform(result.begin(), result.end(), result.begin(), [](unsigned char c)
    {
        return tolower(c);
    });
    return result;
}

bool Handler::contains(const string& str, const string& substr)
{
    return str.find(substr) != string::npos;
}

// Função auxiliar para extrair o código da ilha (primeiros 2 dígitos)
string extractIslandCode(const Cell& cepCell)
{
    string cepStr;
    
    // Converte a célula para string
    if (holds_alternative<string>(cepCell)) cepStr = get<string>(cepCell);
    else if (holds_alternative<int>(cepCell)) cepStr = to_string(get<int>(cepCell));
    else if (holds_alternative<double>(cepCell)) cepStr = to_string(static_cast<int>(get<double>(cepCell)));
    
    // Remove caracteres não numéricos
    cepStr.erase(remove_if(cepStr.begin(), cepStr.end(), [](char c) { return !isdigit(c); }), cepStr.end());
    
    // Pega os primeiros 2 dígitos (preenche com zero se necessário)
    if (cepStr.empty()) return "00";
    if (cepStr.length() == 1) return "0" + cepStr.substr(0, 1);
    return cepStr.substr(0, 2);
}

// Função para fazer merge de dois DataFrames
DataFrame mergeTwoDataFrames(const DataFrame& df1, const DataFrame& df2, const string& cepColName, const string& suffix1, const string& suffix2)
{
    // Encontra o índice da coluna CEP em ambos DataFrames
    int cepIdx1 = df1.colIdx(cepColName);
    int cepIdx2 = df2.colIdx(cepColName);
    
    if (cepIdx1 == -1 || cepIdx2 == -1)
    {
        throw invalid_argument("Coluna CEP não encontrada em um dos DataFrames");
    }

    // Mapeia códigos de ilha para linhas
    unordered_map<string, vector<size_t>> islandMap1;
    unordered_map<string, vector<size_t>> islandMap2;

    // Preenche o primeiro mapa
    for (int i = 0; i < df1.size(); ++i)
    {
        const auto& row = df1.getRow(i);
        string islandCode = extractIslandCode(row[cepIdx1]);
        islandMap1[islandCode].push_back(i);
    }

    // Preenche o segundo mapa
    for (int i = 0; i < df2.size(); ++i)
    {
        const auto& row = df2.getRow(i);
        string islandCode = extractIslandCode(row[cepIdx2]);
        islandMap2[islandCode].push_back(i);
    }

    // Prepara o DataFrame resultante
    vector<string> newColNames;
    vector<ColumnType> newColTypes;

    // Adiciona colunas do primeiro DataFrame
    auto colNames1 = df1.getColumnNames();
    for (int i = 0; i < df1.numCols(); ++i)
    {
        if (colNames1[i] == cepColName)
        {
        newColNames.push_back(colNames1[i]);  // sem sufixo
        } else 
        {
        newColNames.push_back(colNames1[i] + suffix1);
        }
        newColTypes.push_back(df1.typeCol(i));
    }

    // Adiciona colunas do segundo DataFrame
    auto colNames2 = df2.getColumnNames();
    for (int i = 0; i < df2.numCols(); ++i)
    {
        // Não repete a coluna CEP
        if (colNames2[i] != cepColName)
        {
            newColNames.push_back(colNames2[i] + suffix2);
            newColTypes.push_back(df2.typeCol(i));
        }
    }

    DataFrame result(newColNames, newColTypes);

    // Faz o merge
    for (const auto& pair1 : islandMap1)
    {
        const string& islandCode = pair1.first;
        auto it = islandMap2.find(islandCode);
        
        if (it != islandMap2.end())
        {
            // Combina todas as linhas com o mesmo código de ilha
            for (size_t i : pair1.second)
            {
                for (size_t j : it->second)
                {
                    const auto& row1 = df1.getRow(i);
                    const auto& row2 = df2.getRow(j);
                    
                    vector<Cell> newRow;
                    // Adiciona todas as células do primeiro DataFrame
                    newRow.insert(newRow.end(), row1.begin(), row1.end());
                    
                    // Adiciona células do segundo DataFrame, exceto a coluna CEP
                    int cepColIdx = df2.colIdx(cepColName);
                    for (size_t k = 0; k < row2.size(); ++k)
                    {
                        if (k != static_cast<size_t>(cepColIdx))
                        {
                            newRow.push_back(row2[k]);
                        }
                    }
                    
                    result.addRow(newRow);
                }
            }
        }
    }

    return result;
}

map<string, DataFrame> Handler::mergeByCEP(const DataFrame& dfA, const DataFrame& dfB, const DataFrame& dfC, const string& cepColName)
{
    map<string, DataFrame> results;

    // Verifica se a coluna CEP existe em todos os DataFrames
    if (int(dfA.colIdx(cepColName)) == -1 || int(dfB.colIdx(cepColName)) == -1 || int(dfC.colIdx(cepColName)) == -1)
    {
        throw invalid_argument("Coluna CEP não encontrada em um dos DataFrames");
    }

    // Merge dos três DataFrames
    DataFrame mergeAB = mergeTwoDataFrames(dfA, dfB, cepColName, "_A", "_B");
    DataFrame mergeABC = mergeTwoDataFrames(mergeAB, dfC, cepColName, "", "_C");
    results.insert({"ABC", mergeABC});

    // Todas as permutações dois a dois
    results.insert({"AB", mergeTwoDataFrames(dfA, dfB, cepColName, "_A", "_B")});
    results.insert({"AC", mergeTwoDataFrames(dfA, dfC, cepColName, "_A", "_C")});
    results.insert({"BC", mergeTwoDataFrames(dfB, dfC, cepColName, "_B", "_C")});

    return results;
}

/*

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