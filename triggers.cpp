#include "triggers.hpp"
#include "etl/dashboard.hpp"
#include <iostream>

void rodarScriptPython() {
    std::string comando;

#ifdef _WIN32
    // Windows
    comando = "python \"mock/simulator.py\"";
#else
    // Linux / Mac
    comando = "python3 mock/simulator.py";
#endif

    int result = system(comando.c_str());
    if (result != 0) {
        std::cerr << "Erro ao executar o script Python!" << std::endl;
    }
}

Trigger::Trigger(function<void()> func, int interval, bool timer)
    : callback(func), interval(interval), isTimer(timer), running(false) {}

void Trigger::start() 
{
    // se for timer executa no intervalo de tempo 
    if (isTimer) 
    {
        running = true;
        // cria uma thread para cada ativação
        thread([this]() 
        {
            while (running) 
            {
                
                auto start_time = chrono::steady_clock::now();
                // executa a pipeline
                callback();
                
                // faz as análises depois da pipeline ser chamada
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
                
                auto end_time = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::milliseconds>(end_time - start_time);
                auto sleep_time = chrono::milliseconds(interval) - elapsed;
                
                // se treminou antes de chegar a próxima etapa dorme
                if (sleep_time.count() > 0) 
                {
                    this_thread::sleep_for(sleep_time);
                }
                // populando as bases usadas
                rodarScriptPython();
            }
        }).detach();
    } 
}

void Trigger::stop() 
{
    running = false;
}

void Trigger::request() 
{
    if (!isTimer) 
    {   // executa uma vez por requisição
        callback();  
    }
}