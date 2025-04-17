#ifndef DATAFRAME_HPP              
#define DATAFRAME_HPP              
#include <iostream>                
#include <vector>                  
#include <string>
#include <variant>        

using namespace std;  

// Enum para representar os tipos de dados das colunas
enum class ColumnType {INTEGER, DOUBLE, STRING};

//Alias para tipo de célula (campo de uma tabela)
using Cell = variant<int, double, string>;

// Função auxiliar para extrair um double de um Cell
inline double toDouble(const Cell& cell)
{
    return visit([](auto&& arg) -> double
    {
        using T = decay_t<decltype(arg)>;
        if constexpr (is_same_v<T, int> || is_same_v<T, double>) 
            return static_cast<double>(arg);
        else
            throw invalid_argument("Valor não numérico em toDouble.");
    }, cell);
}

inline string toString(const Cell& cell)
{
    return visit([](auto&& arg) -> string
    {
        // Obtém o tipo real de 'arg'
        using T = decay_t<decltype(arg)>;
        // Se for string, retorna diretamente
        if constexpr (is_same_v<T, string>) return arg;
        // Converte const char* para string
        else if constexpr (is_same_v<T, const char*>) return string(arg);
        // Converte booleano para "true" ou "false"
        else if constexpr (is_same_v<T, bool>) return arg ? "true" : "false";
        // Caso seja um tipo aritmético (como int, float, double, etc.)
        else if constexpr (is_arithmetic_v<T>) return to_string(arg);
        // Para outros tipos não esperados
        else return "unknown";
    }, cell);
}

class DataFrame {
private:

    // Vetor com os nomes das colunas
    vector<string> columnNames;            
    
    // Vetor com os tipos de dados de cada coluna
    vector<ColumnType> columnTypes; 
    
    // Matriz com os dados (usando variant para identificar seus tipos)
    vector<vector<Cell>> data;            

    // Função auxiliar para verificar se um valor é considerado nulo
    bool isNull(const string& val) const {
        return val.empty() || val == "null" || val == "NULL" || val == "NaN";
    }

public:

    // Construtor que recebe nomes e tipos das colunas
    DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes);

    // Adiciona uma linha de dados ao DataFrame
    void addRow(const vector<Cell>& row);

    // Remove a linha no índice especificado
    void removeRow(int index);

    // Adiciona uma nova coluna com nome, tipo e valores
    void addColumn(const string&, ColumnType, const vector<Cell>&, int);

    // Remove uma coluna com base no nome
    void removeColumn(const string& name);

    // Exibe o DataFrame no console
    void display() const;

    // índice de uma coluna existente
    size_t colIdx(const string& name) const;

    // retorna o tipo da coluna
    ColumnType typeCol(size_t idx) const;

    // retorna linha desejada
    const vector<Cell>& getRow(size_t index) const;

    // retorna a quantidade de linhas do DataFrame
    int size() const;

    // Retorna o número de colunas no DataFrame
    int numCols() const;

    // Retorna os nomes das colunas
    const vector<string>& getColumnNames() const;

    // Retorna true se o DataFrame não tiver nenhuma linha
    bool empty() const;

    // Retorna as linhas do Dataframe
    const vector<vector<Cell>>& getLinhas() const;
};

#endif 