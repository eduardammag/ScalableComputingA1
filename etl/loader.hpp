#ifndef LOADER_HPP
#define LOADER_HPP

#include "dataframe.hpp"
#include <string>

// Função para salvar um DataFrame como arquivo CSV
void save_as_csv(const DataFrame& df, const std::string& filename);

// Struct usada no pipeline para comunicação entre handler (produtor) e loader (consumidor)
struct LoaderItem {
    DataFrame df;
    std::string nomeArquivoOriginal;
    int threadId;
};

#endif // LOADER_HPP