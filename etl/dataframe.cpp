#include "dataframe.hpp" // Inclui o cabeçalho com a definição da classe DataFrame
#include <algorithm>
#include <variant>
#include <tuple>
#include <iomanip> //Para formatação

using namespace std;

// Construtor da classe DataFrame: inicializa nomes e tipos das colunas
DataFrame::DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes)
    : columnNames(colNames), columnTypes(colTypes) 
{
    // Verificação robusta de tamanhos
    if (colNames.size() != colTypes.size()) {
        if (colTypes.empty()) {
            // Se não há tipos, define todos como STRING
            columnTypes = vector<ColumnType>(colNames.size(), ColumnType::STRING);
        } else {
            // Se há tipos mas não correspondem, ajusta ou lança exceção
            throw invalid_argument("Number of column names (" + to_string(colNames.size()) + 
                                 ") and types (" + to_string(colTypes.size()) + ") must match.");
        }
    }
}

// Função para adicionar uma nova linha ao DataFrame
void DataFrame::addRow(const vector<Cell>& row) 
{
    // Verifica se o número de elementos na linha corresponde ao número de colunas
    if (row.size() != columnNames.size()) 
    {
        cerr << "Row size doesn't match number of columns.\n"; 
        return;
    }

    for (size_t i = 0; i < row.size(); ++i) {
        const auto& val = row[i];
        switch (columnTypes[i]) {
            case ColumnType::INTEGER:
                if (!holds_alternative<int>(val)) {
                    cerr << "Invalid INTEGER at column " << columnNames[i] << ".\n";
                    return;
                }
                break;
            case ColumnType::DOUBLE:
                if (!holds_alternative<double>(val)) {
                    cerr << "Invalid DOUBLE at column " << columnNames[i] << ".\n";
                    return;
                }
                break;
            case ColumnType::STRING:
                if (!holds_alternative<std::string>(val)) {
                    cerr << "Invalid STRING at column " << columnNames[i] << ".\n";
                    return;
                }
                break;
        }
    }

    data.push_back(row);
}

// Remove uma linha com base no índice
void DataFrame::removeRow(int index) 
{
    // Verifica se o índice é válido
    if (index >= 0 && index < static_cast<int>(data.size())) 
    {
        //remoção da linha
        data.erase(data.begin() + index); 
    }
}

// Adiciona uma nova coluna ao DataFrame
void DataFrame::addColumn(const string& name, ColumnType type, const vector<Cell>& values) 
{
    // Verifica se o tamanho da nova coluna corresponde ao número de linhas já existentes
    if (!data.empty() && values.size() != data.size()) 
    {
        cerr << "Column size doesn't match number of rows.\n";
        return;
    }

    // Adiciona o nome e tipo da nova coluna
    columnNames.push_back(name);
    columnTypes.push_back(type);

    if (data.empty())
    {
        // Se ainda não há nenhuma linha, cria uma nova linha para cada valor
        for (const auto& val : values)  data.push_back({val});
    } else 
    {
        // Se já existem linhas, adiciona os valores às linhas correspondentes
        for (size_t i = 0; i < data.size(); ++i)  data[i].push_back(values[i]);
    }
}

// Remove uma coluna com base no nome dela
void DataFrame::removeColumn(const string& name) 
{
    // Procura a posição do nome da coluna
    auto it = find(columnNames.begin(), columnNames.end(), name);
    if (it != columnNames.end())
     {
        int idx = distance(columnNames.begin(), it);

        // Remove nome e tipo da coluna
        columnNames.erase(it);
        columnTypes.erase(columnTypes.begin() + idx);

        // Remove o valor correspondente em todas as linhas
        for (auto& row : data)  row.erase(row.begin() + idx);
    }
}

// Exibe o conteúdo completo do DataFrame
void DataFrame::display() const 
{
    // Imprime os nomes das colunas com tabulação
    for (const auto& name : columnNames)  cout << name << "\t";
    cout << endl;

    // Imprime os valores de cada linha com tabulação
    for (const auto& row : data) 
    {
        for (const auto& cell : row)
        {
            visit([](auto&& val)
            {
                cout << val << "\t";
            }, cell);
        }
        cout << endl;
    }
}

// retorna o indice da coluna desejada
size_t DataFrame::colIdx(const string& name) const 
{
    auto it = find(columnNames.begin(), columnNames.end(), name);
    if (it != columnNames.end()) 
    {
        return distance(columnNames.begin(), it);
    }
    return static_cast<size_t>(-1);
}

// retorna o tipo da coluna na posição desejada
ColumnType DataFrame::typeCol(size_t idx) const 
{
    return columnTypes[idx];
}

// acessa uma linha especifica do DataFrame
const vector<Cell>& DataFrame::getRow(size_t index) const
{
    // verificação
    if (index >= data.size()) 
    {
        throw out_of_range("Índice fora do intervalo.");
    }
    // retorno se passou na verificação
    return data[index];
}


// saber a quantidade de linhas no dataframe
int DataFrame::size() const 
{
    return static_cast<int>(data.size());
}

// Nomes das colunas
const std::vector<std::string>& DataFrame::getColumnNames() const {
    return columnNames;
}

// Verifica se está vazio
bool DataFrame::empty() const {
    return data.empty();
}

// Retorna o número de colunas no DataFrame
int DataFrame::numCols() const {
    return static_cast<int>(columnNames.size());
}

// Retorna linhas do DataFrame
const vector<vector<Cell>>& DataFrame::getLinhas() const
{
    return data;
}