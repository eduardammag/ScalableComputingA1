#include "pipeline/pipeline.hpp"
#include <iostream>
#include <chrono>
#include "etl/dashboard.hpp"
#include "etl/dataframe.hpp"
#include "etl/extrator.hpp"
#include "etl/handlers.hpp"
#include <filesystem>

using namespace std;

int main() {

    Extrator extra;
    Handler hand;
    DataFrame oms = extra.carregar("databases_mock/oms_mock.txt");
    DataFrame hosp = extra.carregar("databases_mock/hospital_mock_7.csv");
    DataFrame ss = extra.carregar("databases_mock/secretary_data.db");

    DataFrame oms_agrup = hand.groupedDf(oms, "CEP" , "Nº óbitos", 4, false);
    DataFrame hosp_agrup = hand.groupedDf(hosp, "CEP" , "Internado", 4, true);
    DataFrame ss_agrup = hand.groupedDf(ss, "CEP" , "Vacinado", 4, true);
    auto merged = hand.mergeByCEP(oms_agrup, hosp_agrup, ss_agrup, "CEP", "Total_Internado", "Total_Vacinado", 4);
    cout << "Deu certo" << endl;
    
    int vezes = 1;
    for (int n = 1; n <= 4; n += 1) 
    {   cout << "\n--- Testando com " << n << " consumidor(es) ---\n";

        for (int j = 0; j < vezes; j++)
        {            
            executarPipeline(n);  // Pipeline com n consumidores
        }
    }

    cout << "========== DASHBOARD ILHAS ==========\n";

    // ANÁLISE 1: Alertas semanais por CEP
    cout << "\n>> Análise 1: Alertas semanais por CEP\n";
    exibirAlertasTratados("database_loader/saida_tratada_oms0.csv");

    // ANÁLISE 2: Estatísticas gerais de internados (média e desvio padrão)
    cout << "\n>> Análise 2: Estatísticas gerais dos hospitais\n";
    calcularEstatisticasHospitalares();

    // ANÁLISE 3: Estatísticas por hospital
    cout << "\n>> Análise 3: Estatísticas individuais por hospital\n";
    calcularEstatisticasPorHospital();

    // ANÁLISE 4: Correlação entre vacinação e internação
    cout << "\n>> Análise 4: Correlação entre vacinação e internação\n";
    // chamar a função de análise 4 aqui quando pronta

    // ANÁLISE 5: Taxa de mortalidade por população
    cout << "\n>> Análise 5: Taxa de mortalidade por CEP (óbitos / população)\n";
    // chamar a função de análise 5 aqui quando pronta

    cout << "\n============ FIM DO DASHBOARD ============\n";

    return 0;
}
