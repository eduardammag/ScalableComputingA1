#ifndef LOADER_HPP
#define LOADER_HPP

#include "dataframe.hpp"
#include <string>

// Função para salvar um DataFrame como arquivo CSV
void save_as_csv(const DataFrame& df, const std::string& filename);

#endif // LOADER_HPP