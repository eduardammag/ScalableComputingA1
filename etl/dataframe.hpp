#ifndef DATAFRAME_HPP              
#define DATAFRAME_HPP              
#include <iostream>                
#include <vector>                  
#include <string>
#include <variant>        

using std::string, std::vector;               

// Enum para representar os tipos de dados das colunas
enum class ColumnType {INTEGER, DOUBLE, STRING};

//Alias para tipo de célula (campo de uma tabela)
using Cell = std::variant<int, double, std::string>;

class DataFrame {
private:

    // Vetor com os nomes das colunas
    std::vector<std::string> columnNames;            
    
    // Vetor com os tipos de dados de cada coluna
    std::vector<ColumnType> columnTypes; 
    
    // Matriz com os dados (usando variant para identificar seus tipos)
    std::vector<std::vector<Cell>> data;            

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
    void addColumn(const string& name, ColumnType type, const vector<Cell>& values);

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
};

#endif 