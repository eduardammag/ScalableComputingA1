#include "dashboard.hpp"
#include "csv.h"

#include <iostream>
#include <filesystem>
#include <thread>
#include <mutex>
#include <set>
#include <chrono>
#include <vector>

namespace fs = filesystem;

// Globais
unordered_map<string, DadosHospital> dadosPorRegiao;
mutex mtx;

// Processa um arquivo CSV
void processarCSV(const string& caminho) {
    io::CSVReader<18> in(caminho);
    in.read_header(io::ignore_extra_column,
        "Data", "Número de óbitos", "População", "CEP da ilha",
        "Número de recuperações", "Número de vacinados", "Vacinado (sim/não)",
        "Diagnóstico (positivo/negativo)", "Nível de escolaridade do paciente",
        "População regional", "CEP (ilha+região)", "Internado (sim/não)",
        "Idade", "Sexo (1 para FEM e 0 para MAS)",
        "Sintoma 1 (teve/não teve)", "Sintoma 2 (teve/não teve)",
        "Sintoma 3 (teve/não teve)", "Sintoma 4 (teve/não teve)");

    string data, cepIlha, vacinado, diagnostico, cepRegiao, internadoStr, sexoStr;
    int obitos, populacao, recuperacoes, vacinados, escolaridade, populacaoRegional, idade;
    int sintoma1, sintoma2, sintoma3, sintoma4;

    while (in.read_row(data, obitos, populacao, cepIlha, recuperacoes, vacinados,
                       vacinado, diagnostico, escolaridade, populacaoRegional,
                       cepRegiao, internadoStr, idade, sexoStr,
                       sintoma1, sintoma2, sintoma3, sintoma4)) {
        if (!cepRegiao.empty()) {
            lock_guard<mutex> lock(mtx);
            auto& dados = dadosPorRegiao[cepRegiao];
            dados.totalPacientes++;
            if (internadoStr == "sim" || internadoStr == "1") {
                dados.internados++;
            }
        }
    }
}

// Exibe os dados do dashboard no console
void exibirDashboard() {
    lock_guard<mutex> lock(mtx);
    cout << "\n===== DASHBOARD =====\n";
    for (const auto& [regiao, dados] : dadosPorRegiao) {
        double pct = (dados.totalPacientes > 0)
                        ? (100.0 * dados.internados / dados.totalPacientes)
                        : 0.0;
        cout << "Região: " << regiao
                  << " | Total: " << dados.totalPacientes
                  << " | Internados: " << dados.internados
                  << " | % Internados: " << pct << "%\n";
    }
    cout << "======================\n";
}

// Inicia o monitoramento da pasta e executa threads por arquivo
void iniciarMonitoramento(const string& pasta) {
    set<string> arquivosProcessados;
    vector<thread> threads;

    cout << "🟢 Monitorando pasta: " << pasta << "\n";

    while (true) {
        for (const auto& entry : fs::directory_iterator(pasta)) {
            string caminho = entry.path().string();
            if (arquivosProcessados.find(caminho) == arquivosProcessados.end()) {
                arquivosProcessados.insert(caminho);
                cout << "📂 Novo arquivo detectado: " << caminho << "\n";
                threads.emplace_back(thread(processarCSV, caminho));
            }
        }

        // Espera e atualiza dashboard a cada 5 segundos
        this_thread::sleep_for(chrono::seconds(5));
        exibirDashboard();
    }

    // Finaliza threads (em caso de saída futura)
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
}
