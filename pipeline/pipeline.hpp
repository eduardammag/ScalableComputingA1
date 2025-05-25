// pipeline.hpp
#pragma once

#include <string>
#include <vector>

using std::string;

// Declara as funções utilizadas no pipeline

// Função que será chamada para iniciar o pipeline de extração com threads
void executarPipeline(int numConsumidores, const string&, const string&, const string&);

// Funções produtor e consumidor (podem ser usadas para testes ou extensões)
void produtor(const std::vector<std::string>& arquivos, bool);