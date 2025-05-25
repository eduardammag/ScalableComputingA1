#ifndef EXTRATOR_HPP  
#define EXTRATOR_HPP 

#include <string>     
#include <vector>
#include "dataframe.hpp" // Inclui o cabeçalho do DataFrame, que é uma estrutura para armazenar os dados carregados

using namespace std; 

class Extrator { 
public:
    // Função pública para carregar um arquivo, detectando o tipo automaticamente
    DataFrame carregar(const string&);

private:
    // Função auxiliar privada para obter a extensão de um arquivo (ex: csv, txt, sqlite)
    string obterExtensao(const string&);

    vector<string> dividirLinha(const string&, char);

    // Função privada para carregar arquivos CSV ou TXT, recebendo o caminho e o separador (vírgula ou tab)
    DataFrame carregarCSVouTXT(const string&, char);

    // Função privada para carregar dados a partir de um banco SQLite
    DataFrame carregarSQLite(const string&);

    // Função privada para carregar dados a partir de um json
    DataFrame carregarJSON(const string&);

    // Função privada para inferir os tipos de dados das colunas a partir de amostras das linhas (ex: int, double, string)
    vector<ColumnType> inferirTipos(const vector<vector<string>>&);
};


#endif