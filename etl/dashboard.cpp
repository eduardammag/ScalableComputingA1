#include "dashboard.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <thread>
#include <mutex>
#include <chrono>
#include <vector>
#include <map>
#include <algorithm>

using namespace std;
namespace fs = filesystem; // Alias para trabalhar com arquivos e diretórios

// Mutex para proteger o acesso compartilhado às estatísticas
mutex mtx;

// Mapa que associa cada CEP ou ilha às estatísticas coletadas
map<string, Estatisticas> mapaEstatisticas;

// Função utilitária que divide uma linha CSV em colunas, baseado na vírgula
vector<string> dividirLinhaCSV(const string& linha) {
    vector<string> resultado;
    stringstream ss(linha);
    string item;

    while (getline(ss, item, ',')) {
        resultado.push_back(item);
    }
    return resultado;
}

// Detecta a fonte dos dados (hospital, secretaria, OMS) baseado nas colunas do CSV
string detectarFonte(const vector<string>& colunas) {
    if (find(colunas.begin(), colunas.end(), "ID_Hospital") != colunas.end()) return "hospital";
    if (find(colunas.begin(), colunas.end(), "Nº óbitos") != colunas.end()) return "oms";
    if (find(colunas.begin(), colunas.end(), "Diagnostico") != colunas.end()) return "secretaria";
    return "desconhecido";
}

// Processa um único arquivo CSV (dados de hospital, secretaria ou OMS)
void processarArquivoCSV(const string& caminhoArquivo) {
    cout << "[INFO] Lendo arquivo: " << caminhoArquivo << endl;

    ifstream arquivo(caminhoArquivo);
    if (!arquivo.is_open()) {
        cerr << "[ERRO] Falha ao abrir: " << caminhoArquivo << endl;
        return;
    }

    string linha;
    // Lê a primeira linha (cabeçalho)
    if (!getline(arquivo, linha)) {
        cerr << "[ERRO] Arquivo vazio: " << caminhoArquivo << endl;
        return;
    }

    auto colunas = dividirLinhaCSV(linha);
    string tipoFonte = detectarFonte(colunas);
    cout << "[INFO] Fonte detectada: " << tipoFonte << endl;

    int linhasLidas = 0;

    // Lê as linhas seguintes e processa os dados
    while (getline(arquivo, linha)) {
        auto campos = dividirLinhaCSV(linha);
        if (campos.size() != colunas.size()) continue;

        map<string, string> linhaMap;
        for (size_t i = 0; i < colunas.size(); ++i) {
            linhaMap[colunas[i]] = campos[i];
        }

        // Extrai o CEP (ou CEP da ilha) para agrupar os dados corretamente
        string cep = linhaMap.count("CEP") ? linhaMap["CEP"] :
                     (linhaMap.count("CEP da ilha") ? linhaMap["CEP da ilha"] : "desconhecido");

        {
            // Protege acesso ao mapa com mutex
            lock_guard<mutex> lock(mtx);
            Estatisticas& est = mapaEstatisticas[cep];

            // Fonte hospitalar
            if (tipoFonte == "hospital") {
                if (linhaMap["Internado"] == "sim") {
                    est.totalInternados++;
                    est.somaIdadesInternados += stoi(linhaMap["Idade"]);
                }

                for (int i = 1; i <= 4; ++i) {
                    if (linhaMap["Sintoma" + to_string(i)] == "1") {
                        est.totalSintomas[i - 1]++;
                    }
                }

                est.totalRegistrosHospital++;

            // Fonte da secretaria de saúde
            } else if (tipoFonte == "secretaria") {
                if (linhaMap["Diagnostico"] == "positivo") est.totalPositivos++;
                if (linhaMap["Vacinado"] == "sim") est.totalVacinados++;
                est.totalRegistrosSecretaria++;

            // Fonte da OMS
            } else if (tipoFonte == "oms") {
                est.totalObitos += stoi(linhaMap["Nº óbitos"]);
                est.totalRecuperados += stoi(linhaMap["Nº recuperados"]);
                est.totalVacinadosOMS += stoi(linhaMap["Nº de vacinados"]);
                est.populacaoIlha = stoi(linhaMap["População"]);
                est.totalRegistrosOMS++;
            }
        }

        linhasLidas++;
    }

    cout << "[INFO] Linhas processadas: " << linhasLidas << endl;
}

// Imprime no terminal um resumo das estatísticas atuais por região/ilha
void exibirDashboard() {
    lock_guard<mutex> lock(mtx); // Protege o acesso ao mapa compartilhado

    cout << "\n========== DASHBOARD ATUALIZADO ==========\n";

    if (mapaEstatisticas.empty()) {
        cout << "Nenhum dado disponível ainda.\n";
        return;
    }

    // Calcula a média global de óbitos por ilha com base nos dados da OMS
    double somaGlobalObitos = 0.0;
    int totalIlhasOMS = 0;

    for (const auto& [cep, est] : mapaEstatisticas) {
        if (est.totalRegistrosOMS > 0 && est.populacaoIlha > 0) {
            somaGlobalObitos += static_cast<double>(est.totalObitos) / est.populacaoIlha;
            totalIlhasOMS++;
        }
    }

    double mediaGlobalObitos = (totalIlhasOMS > 0) ? somaGlobalObitos / totalIlhasOMS : 0.0;

    cout << "Média global de óbitos por habitante (OMS): " << mediaGlobalObitos << "\n";
    cout << "------------------------------------------\n";

    for (const auto& [cep, est] : mapaEstatisticas) {
        if (est.totalRegistrosOMS == 0 || est.populacaoIlha == 0) continue;

        double mediaIlha = static_cast<double>(est.totalObitos) / est.populacaoIlha;
        string alerta;

        if (mediaIlha > mediaGlobalObitos) {
            alerta = "ALERTA VERMELHO: Óbitos acima da média global!";
        } else if (mediaIlha == mediaGlobalObitos) {
            alerta = "ALERTA AMARELO: Óbitos iguais à média global.";
        } else {
            alerta = "ALERTA VERDE: Óbitos abaixo da média global.";
        }

        cout << ">>> Ilha " << cep << ": " << alerta << "\n";
    }

    cout << "==========================================\n";
}


// Função que monitora continuamente a pasta de arquivos CSV
void iniciarMonitoramento(const string& pasta) {
    cout << "[INFO] Monitorando pasta: " << pasta << endl;

    while (true) {
        vector<thread> threads;

        // Limpa as estatísticas para reprocessar tudo
        {
            lock_guard<mutex> lock(mtx);
            mapaEstatisticas.clear();
        }

        // Percorre todos os arquivos CSV da pasta e processa cada um em uma thread separada
        for (const auto& entry : fs::directory_iterator(pasta)) {
            string caminhoCompleto = entry.path().string();

            if (entry.path().extension() == ".csv") {
                cout << "[INFO] Arquivo detectado: " << caminhoCompleto << endl;
                threads.emplace_back([caminhoCompleto]() {
                    processarArquivoCSV(caminhoCompleto);
                });
            }
        }

        // Espera todas as threads terminarem
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }

        // Exibe o resultado do processamento
        exibirDashboard();

        // Aguarda 5 segundos antes de repetir
        this_thread::sleep_for(chrono::seconds(5));
    }
}


