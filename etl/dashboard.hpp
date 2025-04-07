#ifndef DASHBOARD_HPP
#define DASHBOARD_HPP

#include <string>
#include <map>

using namespace std;

struct Estatisticas {
    int totalObitos = 0;
    int populacaoIlha = 0;
    int totalRegistrosOMS = 0;
};

void iniciarMonitoramento(const string& pasta);

#endif // DASHBOARD_HPP