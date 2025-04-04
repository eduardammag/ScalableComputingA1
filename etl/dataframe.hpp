#ifndef DATAFRAME_HPP              
#define DATAFRAME_HPP              
#include <iostream>                
#include <vector>                  
#include <string>                  
#include <unordered_set>          

using namespace std;              

// Enum para representar os tipos de dados das colunas
enum class ColumnType {
    INTEGER,                     
    DOUBLE,                       
    STRING                       
};

class DataFrame {
private:

    // Vetor com os nomes das colunas
    vector<string> columnNames;            
    
    // Vetor com os tipos de dados de cada coluna
    vector<ColumnType> columnTypes; 
    
    // Matriz com os dados (armazenados como strings) ?????????
    vector<vector<string>> data;              

    // Função auxiliar para verificar se um valor é considerado nulo
    bool isNull(const string& val) const {
        return val.empty() || val == "null" || val == "NULL" || val == "NaN";
    }

public:
    // Construtor que recebe nomes e tipos das colunas
    DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes);

    // Adiciona uma linha de dados ao DataFrame
    void addRow(const vector<string>& row);

    // Remove a linha no índice especificado
    void removeRow(int index);

    // Adiciona uma nova coluna com nome, tipo e valores
    void addColumn(const string& name, ColumnType type, const vector<string>& values);

    // Remove uma coluna com base no nome
    void removeColumn(const string& name);

    // Exibe o DataFrame no console
    void display() const;
};

#endif 