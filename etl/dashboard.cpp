#include "dashboard.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <thread>
#include <mutex>
#include <vector>
#include <map>
#include <set>
#include <algorithm>

using namespace std;
namespace fs = filesystem;

mutex mtx;
map<string, Estatisticas> mapaEstatisticas;

vector<string> dividirLinhaCSV(const string& linha) {
    vector<string> resultado;
    stringstream ss(linha);
    string item;

    while (getline(ss, item, ',')) {
        resultado.push_back(item);
    }
    return resultado;
}

string detectarFonte(const vector<string>& colunas) {
    if (find(colunas.begin(), colunas.end(), "Nº óbitos") != colunas.end()) return "oms";
    return "desconhecido";
}

void processarArquivoCSV(const string& caminhoArquivo) {
    ifstream arquivo(caminhoArquivo);
    if (!arquivo.is_open()) {
        cerr << "[ERRO] Falha ao abrir: " << caminhoArquivo << endl;
        return;
    }

    string linha;
    if (!getline(arquivo, linha)) return;

    auto colunas = dividirLinhaCSV(linha);
    string tipoFonte = detectarFonte(colunas);
    if (tipoFonte != "oms") return;

    while (getline(arquivo, linha)) {
        auto campos = dividirLinhaCSV(linha);
        if (campos.size() != colunas.size()) continue;

        map<string, string> linhaMap;
        for (size_t i = 0; i < colunas.size(); ++i) {
            linhaMap[colunas[i]] = campos[i];
        }

        string cepIlha = linhaMap["CEP da ilha"];

        lock_guard<mutex> lock(mtx);
        Estatisticas& est = mapaEstatisticas[cepIlha];
        est.totalObitos += stoi(linhaMap["Nº óbitos"]);
        est.populacaoIlha = stoi(linhaMap["População"]);
        est.totalRegistrosOMS++;
    }
}

void exibirDashboard() {
    lock_guard<mutex> lock(mtx);

    cout << "\n========== DASHBOARD ATUALIZADO ==========\n";

    if (mapaEstatisticas.empty()) {
        cout << "Nenhum dado disponível ainda.\n";
        return;
    }

    double somaGlobalObitos = 0.0;
    int totalIlhas = 0;

    for (const auto& [cep, est] : mapaEstatisticas) {
        if (est.totalRegistrosOMS > 0 && est.populacaoIlha > 0) {
            somaGlobalObitos += static_cast<double>(est.totalObitos) / est.populacaoIlha;
            totalIlhas++;
        }
    }

    double mediaGlobalObitos = (totalIlhas > 0) ? somaGlobalObitos / totalIlhas : 0.0;

    for (const auto& [cep, est] : mapaEstatisticas) {
        if (est.totalRegistrosOMS == 0 || est.populacaoIlha == 0) continue;

        double mediaIlha = static_cast<double>(est.totalObitos) / est.populacaoIlha;
        string alerta = (mediaIlha > mediaGlobalObitos) ? "VERMELHO" : "VERDE";

        cout << "CEP da Ilha: " << cep << " | Alerta: " << alerta << "\n";
    }

    cout << "==========================================\n";
}

void iniciarMonitoramento(const string& pasta) {
    static set<string> arquivosProcessados;
    vector<thread> threads;

    {
        lock_guard<mutex> lock(mtx);
        mapaEstatisticas.clear();
    }

    bool novoArquivo = false;

    for (const auto& entry : fs::directory_iterator(pasta)) {
        string caminho = entry.path().string();
        if (entry.path().extension() == ".csv" && arquivosProcessados.find(caminho) == arquivosProcessados.end()) {
            threads.emplace_back([caminho]() {
                processarArquivoCSV(caminho);
            });
            arquivosProcessados.insert(caminho);
            novoArquivo = true;
        }
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    if (novoArquivo || arquivosProcessados.empty()) {
        exibirDashboard();
    } else {
        cout << "[INFO] Nenhum novo arquivo para processar.\n";
    }
}