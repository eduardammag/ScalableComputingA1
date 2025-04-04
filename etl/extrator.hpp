#ifndef EXTRATOR_HPP
#define EXTRATOR_HPP

#include <string>
#include <vector>
#include "dataframe.hpp"

class Extrator {
public:
    DataFrame carregar(const std::string& caminhoArquivo);

private:
    std::string obterExtensao(const std::string& nomeArquivo);

    DataFrame carregarCSVouTXT(const std::string& caminho, char separador);
    DataFrame carregarSQLite(const std::string& caminho);

    std::vector<ColumnType> inferirTipos(const std::vector<std::string>& primeiraLinha);
};

#endif
