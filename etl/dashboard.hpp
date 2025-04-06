#ifndef DASHBOARD_HPP
#define DASHBOARD_HPP

#include <string>
#include <unordered_map>
#include <mutex>

using namespace std;

// Dados agregados por região
struct DadosHospital {
    int internados = 0;
    int totalPacientes = 0;
};

// Mapa compartilhado e mutex
extern unordered_map<string, DadosHospital> dadosPorRegiao;
extern mutex mtx;

// Funções principais
void processarCSV(const string& caminho);
void iniciarMonitoramento(const string& pasta);

#endif
