#include <iostream>
#include <chrono>
#include "pipeline/pipeline.hpp"
#include "etl/dashboard.hpp"
#include "etl/dashboard.hpp"


#include <filesystem>

using namespace std;

int main() {
    
    int vezes = 1;
    for (int n = 1; n <= 12; n += 1) 
    {   cout << "\n--- Testando com " << n << " consumidor(es) ---\n";

        for (int j = 0; j < vezes; j++)
        {            
            executarPipeline(n);  // Pipeline com n consumidores
        }
    }

    cout << "========== DASHBOARD ILHAS ==========\n";

    // ANÁLISE 1: Alertas semanais por CEP
    cout << "\n>> Análise 1: Alertas semanais por CEP\n";
    exibirAlertasTratados("database_loader/saida_tratada_oms04.csv");

    // ANÁLISE 2: Estatísticas gerais de internados (média e desvio padrão)
    cout << "\n>> Análise 2: Estatísticas gerais dos hospitais\n";
    calcularEstatisticasHospitalares();

    // ANÁLISE 3: Estatísticas por hospital
    cout << "\n>> Análise 3: Estatísticas individuais por hospital\n";
    calcularEstatisticasPorHospital();

    // ANÁLISE 4: Correlação entre vacinação e internação
    cout << "\n>> Análise 4: Correlação entre vacinação e internação\n";
    analyzeCorrelation();
    // ANÁLISE 5: Taxa de mortalidade por população
    cout << "\n>> Análise 5: Regressão Linear para estimar o número de internados com base na quantidade de vacinados.\n";
    regressionInternadoVsVacinado();
    cout << "\n============ FIM DO DASHBOARD ============\n";

    return 0;
}