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
#include <unordered_set>

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
    // return cepStr;
}

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
    // Verifica se a coluna existe
    int colIndex = input.colIdx(nameCol);
    if (colIndex == -1) {
        throw invalid_argument("Coluna '" + nameCol + "' não encontrada no DataFrame.");
    }
    
    // Verifica se a coluna é numérica
    ColumnType colType = input.typeCol(colIndex);
    if (colType == ColumnType::STRING) {
        throw invalid_argument("Coluna '" + nameCol + "' é do tipo STRING. Apenas colunas numéricas são suportadas.");
    }
    
    int n = input.size();
    if (n == 0) return; // DataFrame vazio, nada a fazer
    
    // Garante chunkSize mínimo de 1
    int chunkSize = max(n / numThreads, 1); 
    
    // Fase 1: Cálculo da média
    double sum = 0.0;
    mutex mtxSum;
    vector<thread> threads;
    
    // Percorre pedaços da coluna somando
    for (int i = 0; i < numThreads; ++i) 
    {
        int start = i * chunkSize;
        int end = (i == numThreads - 1) ? n : start + chunkSize;
        
        threads.emplace_back([&input, colIndex, start, end, &sum, &mtxSum]()
        {
            double localSum = 0.0;
            for (int j = start; j < end; ++j)
            {
                const Cell& val = input.getRow(j)[colIndex]; 
                try
                {
                    localSum += toDouble(val);
                } 
                catch (const exception& e)
                {
                    cerr << "Erro ao converter valor para double: " << e.what() << endl;
                    // Continua sem adicionar ao somatório
                }
            }
            lock_guard<mutex> lock(mtxSum);
            sum += localSum;
        });
    }
    
    for (auto& t : threads) t.join();
    threads.clear();
    
    double mean = sum / n;
    
    // Fase 2: Gerar vetor de alertas ("Vermelho" se valor > média, "Verde" caso contrário)
    vector<Cell> alertas(n);
    mutex mtxAlerts;
    
    // Percorre a coluna original identificando as linhas acima/abaixo da média
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
                    double currentVal = toDouble(val);
                    localAlerts[j - start] = (currentVal > mean) ? "Vermelho" : "Verde";
                } 
                catch (const exception& e)
                {
                    cerr << "Erro ao converter valor para double: " << e.what() << endl;
                    localAlerts[j - start] = "Verde"; // Valor inválido considerado abaixo da média
                }    
            }

            // Atualiza o vetor compartilhado
            for (size_t j = 0; j < localAlerts.size(); ++j)
            {
                alertas[start + j] = localAlerts[j];
            }
        });
    }
    
    for (auto& t : threads) t.join();

    // Adiciona a nova coluna ao DataFrame
    input.addColumn("Alertas" , ColumnType::STRING, alertas, numThreads);
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

DataFrame Handler::groupedDf(const DataFrame& input, const string& groupedCol, const string& aggCol, int numThreads, bool groupIlha) 
{
    const int colIdxGroup = input.colIdx(groupedCol);
    const int colIdxAgg = input.colIdx(aggCol);
    const int numRows = input.size();
    // std::cout << groupedCol << " e " << aggCol << std::endl;
    // std::cout << colIdxGroup << " e " << colIdxAgg << std::endl;

    if (colIdxGroup < 0 || colIdxAgg < 0)
        throw std::invalid_argument("Coluna de agrupamento ou agregação não encontrada no DataFrame.");

    if (numThreads <= 0)
        throw std::invalid_argument("Número de threads deve ser maior que zero.");

    const int chunkSize = (numRows + numThreads - 1) / numThreads;

    // Fase 1: Extração paralela
    vector<int> groupKeys(numRows);
    vector<double> aggValues(numRows);
    vector<thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            try {
                const int start = t * chunkSize;
                const int end = min(start + chunkSize, numRows);

                for (int i = start; i < end; ++i) {
                    const auto& row = input.getRow(i);

                    if (colIdxGroup >= int(row.size()) || colIdxAgg >= int(row.size()))
                        throw runtime_error("Índice fora do intervalo em linha do DataFrame.");

                    const Cell& groupCell = row[colIdxGroup];
                    const Cell& aggCell = row[colIdxAgg];

                    int groupKey;

                    // Conversão segura do valor da coluna de agrupamento para int
                    if (holds_alternative<int>(groupCell)) {
                        groupKey = get<int>(groupCell);
                    } else if (holds_alternative<double>(groupCell)) {
                        groupKey = static_cast<int>(get<double>(groupCell));
                    } else if (holds_alternative<string>(groupCell)) {
                        try {
                            groupKey = stoi(get<string>(groupCell));
                        } catch (...) {
                            throw runtime_error("Falha ao converter string para int em coluna de agrupamento.");
                        }
                    } else {
                        throw runtime_error("Tipo inválido em coluna de agrupamento.");
                    }

                    if (groupIlha) {
                        string groupStr;
                        if (holds_alternative<string>(groupCell)) {
                            groupStr = get<string>(groupCell);
                        } else if (holds_alternative<int>(groupCell)) {
                            groupStr = to_string(get<int>(groupCell));
                        } else if (holds_alternative<double>(groupCell)) {
                            groupStr = to_string(static_cast<int>(get<double>(groupCell)));
                        } else {
                            throw runtime_error("Tipo inválido para extração do código da ilha.");
                        }

                        string islandCodeStr = extractIslandCode(groupStr);
                        try {
                            groupKeys[i] = stoi(islandCodeStr);
                        } catch (...) {
                            throw runtime_error("Falha ao converter código de ilha para inteiro.");
                        }
                    } else {
                        groupKeys[i] = groupKey;
                    }

                    aggValues[i] = toDouble(aggCell);
                }
            } catch (const std::exception& e) {
                cerr << "[Erro Thread Fase 1 " << t << "] " << e.what() << endl;
            }
        });
    }

    for (auto& thread : threads) thread.join();
    threads.clear();

    // Fase 2: Agregação paralela
    vector<unordered_map<int, double>> partialSums(numThreads);

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t]() {
            const int start = t * chunkSize;
            const int end = min(start + chunkSize, numRows);

            for (int i = start; i < end; ++i) {
                partialSums[t][groupKeys[i]] += aggValues[i];
            }
        });
    }

    for (auto& thread : threads) thread.join();

    // Combinando resultados
    unordered_map<int, double> totalSums;
    for (const auto& map : partialSums) {
        for (const auto& [key, value] : map) {
            totalSums[key] += value;
        }
    }

    // Construindo o DataFrame de saída
    DataFrame output({groupedCol, "Total_" + aggCol}, { ColumnType::STRING, ColumnType::DOUBLE });

    for (const auto& [key, sum] : totalSums) {
        output.addRow({to_string(key), sum});
    }

    return output;
}


// Handler para limpeza de dados - remove duplicatas e linhas/colunas com muitos valores nulos
void Handler::dataCleaner(DataFrame& input)
{
    // Primeiro passo: remover linhas duplicadas
    removeDuplicateRows(input);
    
    // Segundo passo: remover linhas com muitos valores nulos
    removeSparseRows(input, 0.9);
    
    // Terceiro passo: remover colunas com muitos valores nulos
    removeSparseColumns(input, 0.9);
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

// Hash de uma linha para detecção rápida de duplicatas
size_t hashRow(const vector<Cell>& row)
{
    size_t seed = row.size();
    for (const auto& cell : row)
    {
        seed ^= std::visit([](auto&& val) {
            return std::hash<std::decay_t<decltype(val)>>{}(val);
        }, cell) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

// Função auxiliar para remover linhas duplicadas
void Handler::removeDuplicateRows(DataFrame& input)
{

    unordered_set<size_t> seenHashes;
    vector<vector<Cell>> uniqueRows;
    const int numRows = input.size();

    for (int i = 0; i < numRows; ++i)
    {
        const auto& row = input.getRow(i);
        size_t h = hashRow(row);
        if (seenHashes.insert(h).second)
        {
            uniqueRows.push_back(row);
        }
    }
}

// Função auxiliar para remover linhas com muitos valores nulos
void Handler::removeSparseRows(DataFrame& input, double nullThreshold)
{
    const int numRows = input.size();
    const int numCols = input.numCols();
    const int thresholdCount = static_cast<int>(numRows * nullThreshold);

    vector<int> nullCounts(numCols, 0);

    for (int i = 0; i < numRows; ++i)
    {
        const auto& row = input.getRow(i);
        for (int col = 0; col < numCols; ++col)
        {
            if (isNullCell(row[col])) nullCounts[col]++;
        }
    }

    vector<int> keepIndices;
    for (int col = 0; col < numCols; ++col)
    {
        if (nullCounts[col] < thresholdCount) keepIndices.push_back(col);
    }

    vector<string> newColNames;
    vector<ColumnType> newColTypes;
    for (int col : keepIndices)
    {
        newColNames.push_back(input.getColumnNames()[col]);
        newColTypes.push_back(input.typeCol(col));
    }

    DataFrame cleaned(newColNames, newColTypes);
    for (int i = 0; i < numRows; ++i)
    {
        const auto& row = input.getRow(i);
        vector<Cell> newRow;
        newRow.reserve(keepIndices.size());
        for (int col : keepIndices) newRow.push_back(row[col]);
        cleaned.addRow(std::move(newRow));
    }

    input = std::move(cleaned);
}

// Função auxiliar para remover colunas com muitos valores nulos
void Handler::removeSparseColumns(DataFrame& input, double nullThreshold)
{
    const int numRows = input.size();
    const int numCols = input.numCols();
    const int thresholdCount = static_cast<int>(numRows * nullThreshold);

    vector<int> nullCounts(numCols, 0);

    for (int i = 0; i < numRows; ++i)
    {
        const auto& row = input.getRow(i);
        for (int col = 0; col < numCols; ++col)
        {
            if (isNullCell(row[col])) nullCounts[col]++;
        }
    }

    vector<int> keepIndices;
    for (int col = 0; col < numCols; ++col)
    {
        if (nullCounts[col] < thresholdCount) keepIndices.push_back(col);
    }

    vector<string> newColNames;
    vector<ColumnType> newColTypes;
    for (int col : keepIndices)
    {
        newColNames.push_back(input.getColumnNames()[col]);
        newColTypes.push_back(input.typeCol(col));
    }

    DataFrame cleaned(newColNames, newColTypes);
    for (int i = 0; i < numRows; ++i)
    {
        const auto& row = input.getRow(i);
        vector<Cell> newRow;
        newRow.reserve(keepIndices.size());
        for (int col : keepIndices) newRow.push_back(row[col]);
        cleaned.addRow(std::move(newRow));
    }

    input = std::move(cleaned);
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

    // Ignora validação se o nome da coluna for "data"
    if (toLower(rule.name) == "data") return true;
    
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

void mergeTwoDataFrames(
    DataFrame& df1, const DataFrame& df2,
    const string& cepColName, const string& valueCol2, const string& suffix,
    int numThreads = 1
) {
    // indíces das colunas desejadas
    int cepIdx1 = df1.colIdx(cepColName);
    int cepIdx2 = df2.colIdx(cepColName);
    int valIdx2 = df2.colIdx(valueCol2);

    // valores referentes do df1 no df2
    unordered_map<string, Cell> valueMap;
    for (int i = 0; i < df2.size(); ++i) 
    {
        // itera a linha e desbore o CEP
        const auto& row = df2.getRow(i);
        string islandCode = extractIslandCode(row[cepIdx2]);
        // acha o valor de CEP na coluna desejada do df2
        Cell valor = row[valIdx2];
        valueMap[islandCode] = valor;  
    }

    // inicializa a coluna a ser adicionada
    string newColName = valueCol2 + suffix;
    ColumnType newColType = df2.typeCol(valIdx2);

    // preenche a coluna com os valores na ordem certa
    vector<Cell> newColValues;
    for (int i = 0; i < df1.size(); ++i) 
    {
        const auto& row = df1.getRow(i);
        string islandCode = extractIslandCode(row[cepIdx1]);

        if (valueMap.count(islandCode)) 
        {
            newColValues.push_back(valueMap[islandCode]);
        } else 
        {
            // Caso não encontre, coloca valor vazio
            newColValues.push_back(Cell());  
        }
    }
    // Adiciona a nova coluna ao df1
    df1.addColumn(newColName, newColType, newColValues, numThreads);
}


                    
map<string, DataFrame> Handler::mergeByCEP(DataFrame& dfA, DataFrame& dfB, DataFrame& dfC, const string& cepColName,
    const string& colB, const string& colC, int numThreads)
{
    map<string, DataFrame> results;
    
    // Verifica se a coluna CEP existe em todos os DataFrames
    if (int(dfA.colIdx(cepColName)) == -1 || int(dfB.colIdx(cepColName)) == -1 || int(dfC.colIdx(cepColName)) == -1)
    {
        throw invalid_argument("Coluna CEP não encontrada em um dos DataFrames");
    }
    DataFrame guardaA = dfA;
    
    mergeTwoDataFrames(dfA, dfB,cepColName,colB, "_B", numThreads);
    results.insert({"AB", dfA});
    mergeTwoDataFrames(dfB, dfC,cepColName,colC, "_C", numThreads);
    results.insert({"BC", dfB});
    mergeTwoDataFrames(guardaA, dfC, cepColName,colC,  "_C", numThreads);
    results.insert({"AC", guardaA});
    mergeTwoDataFrames(dfA, dfC,cepColName,colC, "_B", numThreads);
    results.insert({"ABC", dfA});
    
    return results;
}