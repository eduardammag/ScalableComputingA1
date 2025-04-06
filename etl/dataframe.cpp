#include "dataframe.hpp" // Inclui o cabeçalho com a definição da classe DataFrame
#include <algorithm>
#include <tuple> 

// Construtor da classe DataFrame: inicializa nomes e tipos das colunas
DataFrame::DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes)
    : columnNames(colNames), columnTypes(colTypes) {
    // Verifica se o número de nomes de colunas é igual ao número de tipos
    if (colNames.size() != colTypes.size()) {
        throw invalid_argument("Number of column names and types must match."); // Lança erro se estiverem desbalanceados
    }
}

// Função para adicionar uma nova linha ao DataFrame
void DataFrame::addRow(const vector<string>& row) {
    // Verifica se o número de elementos na linha corresponde ao número de colunas
    if (row.size() != columnNames.size()) {
        cerr << "Row size doesn't match number of columns.\n"; // Mensagem de erro
        return;
    }

    // Valida cada valor da linha de acordo com o tipo da coluna correspondente
    for (size_t i = 0; i < row.size(); ++i) {
        if (isNull(row[i])) {
            continue; // Se o valor é nulo, pula validação de tipo
        }

        if (columnTypes[i] == ColumnType::INTEGER) {
            try {
                stoi(row[i]); // Tenta converter o valor para inteiro
            } catch (...) {
                cerr << "Invalid integer at column " << columnNames[i] << ".\n"; // Erro se falhar
                return;
            }
        } else if (columnTypes[i] == ColumnType::DOUBLE) {
            try {
                stod(row[i]); // Tenta converter o valor para double
            } catch (...) {
                cerr << "Invalid double at column " << columnNames[i] << ".\n"; // Erro se falhar
                return;
            }
        }
        // Se for string, nenhuma validação necessária
    }

    // Se passou por todas as validações, adiciona a linha ao DataFrame
    data.push_back(row);
}

// Remove uma linha com base no índice
void DataFrame::removeRow(int index) {
    // Verifica se o índice é válido
    if (index >= 0 && index < data.size()) {
        data.erase(data.begin() + index); // Remove a linha na posição indicada
    }
}

// Adiciona uma nova coluna ao DataFrame
void DataFrame::addColumn(const string& name, ColumnType type, const vector<string>& values) {
    // Verifica se o tamanho da nova coluna corresponde ao número de linhas já existentes
    if (!data.empty() && values.size() != data.size()) {
        cerr << "Column size doesn't match number of rows.\n"; // Erro se houver discrepância
        return;
    }

    // Adiciona o nome e tipo da nova coluna
    columnNames.push_back(name);
    columnTypes.push_back(type);

    if (data.empty()) {
        // Se ainda não há nenhuma linha, cria uma nova linha para cada valor
        for (const auto& val : values) {
            data.push_back({val});
        }
    } else {
        // Se já existem linhas, adiciona os valores às linhas correspondentes
        for (size_t i = 0; i < data.size(); ++i) {
            data[i].push_back(values[i]);
        }
    }
}

// Remove uma coluna com base no nome da coluna
void DataFrame::removeColumn(const string& name) {
    // Procura a posição do nome da coluna
    auto it = find(columnNames.begin(), columnNames.end(), name);
    if (it != columnNames.end()) {
        int idx = distance(columnNames.begin(), it); // Índice da coluna

        // Remove nome e tipo da coluna
        columnNames.erase(it);
        columnTypes.erase(columnTypes.begin() + idx);

        // Remove o valor correspondente em todas as linhas
        for (auto& row : data) {
            row.erase(row.begin() + idx);
        }
    }
}

// Exibe o conteúdo completo do DataFrame
void DataFrame::display() const {
    // Imprime os nomes das colunas com tabulação
    for (const auto& name : columnNames) {
        cout << name << "\t";
    }
    cout << endl;

    // Imprime os valores de cada linha com tabulação
    for (const auto& row : data) {
        for (const auto& val : row) {
            cout << val << "\t";
        }
        cout << endl;
    }
}


tuple<vector<string>, ColumnType> DataFrame::getColumn(const string& name) const {
    auto it = find(columnNames.begin(), columnNames.end(), name);
    if (it == columnNames.end()) {
        cerr << "Column '" << name << "' not found.\n";
        return std::make_tuple(vector<string>{}, ColumnType::STRING); // valor padrão
    }

    size_t index = distance(columnNames.begin(), it);
    vector<string> column;

    for (const auto& row : data) {
        column.push_back(row[index]);
    }

    return make_tuple(column, columnTypes[index]);
}
