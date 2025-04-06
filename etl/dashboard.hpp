#ifndef DASHBOARD_HPP
#define DASHBOARD_HPP

#include <string>
#include <map>
#include <array>

using namespace std;

// Estrutura que armazena estatísticas para cada CEP ou ilha
struct Estatisticas {
    // Dados da fonte hospital
    int totalInternados = 0;
    int somaIdadesInternados = 0;
    array<int, 4> totalSintomas = {0, 0, 0, 0};
    int totalRegistrosHospital = 0;

    // Dados da secretaria de saúde
    int totalPositivos = 0;
    int totalVacinados = 0;
    int totalRegistrosSecretaria = 0;

    // Dados da OMS
    int totalObitos = 0;
    int totalRecuperados = 0;
    int totalVacinadosOMS = 0;
    int populacaoIlha = 0;
    int totalRegistrosOMS = 0;
};

// Função que inicia o monitoramento de uma pasta para arquivos .csv novos
void iniciarMonitoramento(const string& pasta);


void processarArquivos(const string& pastaDatabase);
#endif // DASHBOARD_HPP
