#include "dataframe.hpp" // Inclui o cabeçalho com a definição da classe DataFrame
#include <algorithm>
#include <tuple> 

// Construtor padrão
// DataFrame::DataFrame() {
    // Não faz nada — inicializa tudo vazio
// }

// Construtor da classe DataFrame: inicializa nomes e tipos das colunas
DataFrame::DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes)
    : columnNames(colNames), columnTypes(colTypes) 
    {
    // verifica se o número de nomes de colunas é igual ao número de tipos
    if (colNames.size() != colTypes.size()) 
    {
        // erro se estiverem desbalanceados
        throw invalid_argument("Number of column names and types must match."); 
    }
}

// Função para adicionar uma nova linha ao DataFrame
void DataFrame::addRow(const vector<string>& row) 
{
    // Verifica se o número de elementos na linha corresponde ao número de colunas
    if (row.size() != columnNames.size()) 
    {
        cerr << "Row size doesn't match number of columns.\n"; 
        return;
    }

    // Valida cada valor da linha de acordo com o tipo da coluna correspondente
    for (size_t i = 0; i < row.size(); ++i) 
    {
        if (isNull(row[i])) 
        {
            // Se o valor é nulo, pula validação de tipo
            continue; 
        }

        if (columnTypes[i] == ColumnType::INTEGER) 
        {
            try 
            {
                // Tenta converter o valor para inteiro
                stoi(row[i]); 
            } 
            catch (...) 
            {
                cerr << "Invalid integer at column " << columnNames[i] << ".\n"; 
                return;
            }
        } 
        else if (columnTypes[i] == ColumnType::DOUBLE) 
        {
            try 
            {
                stod(row[i]); // Tenta converter o valor para double
            } 
            catch (...) 
            {
                cerr << "Invalid double at column " << columnNames[i] << ".\n";
                return;
            }
        }
        // Se for string, nenhuma validação necessária
    }

    // Se passou por todas as validações, adiciona a linha ao DataFrame
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
void DataFrame::addColumn(const string& name, ColumnType type, const vector<string>& values) 
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
        for (const auto& val : values) 
        {
            data.push_back({val});
        }
    } else 
    {
        // Se já existem linhas, adiciona os valores às linhas correspondentes
        for (size_t i = 0; i < data.size(); ++i) 
        {
            data[i].push_back(values[i]);
        }
    }
}

// Remove uma coluna com base no nome da coluna
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
        for (auto& row : data) 
        {
            row.erase(row.begin() + idx);
        }
    }
}

// Exibe o conteúdo completo do DataFrame
void DataFrame::display() const 
{
    // Imprime os nomes das colunas com tabulação
    for (const auto& name : columnNames) 
    {
        cout << name << "\t";
    }
    cout << endl;

    // Imprime os valores de cada linha com tabulação
    for (const auto& row : data) 
    {
        for (const auto& val : row) 
        {
            cout << val << "\t";
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
        // posição ordinal da coluna
        return distance(columnNames.begin(), it);
    }
    return -1; 
}

// retorna o tipo da coluna na posição desejada
ColumnType DataFrame::typeCol(size_t idx) const 
{
    return columnTypes[idx];
}

// acessa uma linha especifica do DataFrame
const vector<string>& DataFrame::getRow(size_t index) const
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
    return data.size();
}

// Implementação do método getColumnNames
const std::vector<std::string>& DataFrame::getColumnNames() const {
    return columnNames;
}