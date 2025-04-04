#ifndef EXTRATOR_HPP
#define EXTRATOR_HPP

#include <string>
#include <vector>
#include "dataframe.hpp"

using namespace std; 

class Extrator {
public:
    DataFrame carregar(const string& caminhoArquivo);

private:
    string obterExtensao(const string& nomeArquivo);

    DataFrame carregarCSVouTXT(const string& caminho, char separador);
    DataFrame carregarSQLite(const string& caminho);

    vector<ColumnType> inferirTipos(const vector<string>& primeiraLinha);
};

#endif
