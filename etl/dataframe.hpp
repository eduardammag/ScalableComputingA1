// Impede que o arquivo seja incluído mais de uma vez durante a compilação
#ifndef DATAFRAME_HPP
#define DATAFRAME_HPP

#include <iostream>             // Para entrada e saída (cout, cin)
#include <vector>               // Para usar vetores dinâmicos
#include <string>               // Para manipular strings
#include <unordered_set>        // Para conjuntos sem ordenação (útil para eliminar duplicatas)
#include <unordered_map>        // Para mapas rápidos, se necessário
#include <algorithm>            // Para funções utilitárias como sort, find, etc.
#include <cmath>                // Para funções matemáticas (ex: std::abs, std::pow)
#include <fstream>              // Para leitura de arquivos (ex: CSV)
#include <sstream>              // Para manipulação de strings como fluxos (útil para parse)

using namespace std;            

// Enum class que define os tipos possíveis de dados das colunas
enum class ColumnType {
    INTEGER,    
    DOUBLE,     
    STRING      
};


class DataFrame {
private:

    // Lista com os nomes das colunas
    vector<string> columnNames;
    
    // Lista com os tipos das colunas, na mesma ordem
    vector<ColumnType> columnTypes;   

    // ======== Funções auxiliares privadas =========

    // Verifica se um valor é considerado nulo (ex: "NULL" ou vazio)
    bool isNull(const string& value) const;

    // Verifica se uma linha é duplicada comparando com as já existentes
    bool isDuplicateRow(const vector<string>& row) const;

    // Verifica se uma string representa um número (usado para validação)
    bool isNumber(const string& s) const;

    // Converte número double para string (com precisão padrão)
    string toString(double d) const;

    // Converte inteiro para string
    string toString(int i) const;

public:
    // ========= Construtor =========

    // Cria o DataFrame com os nomes e tipos das colunas definidos
    DataFrame(const vector<string>& colNames, const vector<ColumnType>& colTypes);

    // ========= Manipulação de linhas =========

    // Adiciona uma nova linha ao DataFrame (valores como strings)
    void addRow(const vector<string>& row);

    // Remove uma linha com base no índice (0-based)
    void removeRow(int index);

    // ========= Manipulação de colunas =========

    // Adiciona uma nova coluna com valores e tipo correspondente
    void addColumn(const string& name, ColumnType type, const vector<string>& values);

    // Remove uma coluna com base no nome
    void removeColumn(const string& name);

    // ========= Exibição =========

    // Mostra o conteúdo do DataFrame no terminal
    void display() const;

    // ========= Limpeza =========

    // Remove todas as linhas que contenham valores nulos
    void removeNulls();

    // Remove todas as linhas duplicadas (com valores idênticos)
    void removeDuplicates();
};

// Fim da proteção contra múltiplas inclusões
#endif // DATAFRAME_HPP
