#ifndef DASHBOARD_HPP
#define DASHBOARD_HPP

#include <string>
#include <vector>
#include <map>
#include <set>

using namespace std; 

// Função para exibir alertas da semana com base em frequência de 'True'
void exibirAlertasTratados(const string& caminhoArquivo);

// Função para processar um único arquivo e armazenar os valores
void processarArquivo(const string& caminhoArquivo);

// Função principal que varre todos os arquivos hospitalares e computa estatísticas
void calcularEstatisticasHospitalares();

// Função que processa um arquivo individual e armazena os dados por hospital
void processarArquivoPorHospital(const string& caminhoArquivo);

// Função principal para calcular estatísticas por hospital
void calcularEstatisticasPorHospital();

#endif // DASHBOARD_HPP
