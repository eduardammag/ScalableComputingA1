#include "dashboard.hpp"
#include "dataframe.hpp"  //Para Cell, toDouble, toString
#include "extrator.hpp"  //Para Extrator
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
#include <string>
#include <cmath>

using namespace std;
namespace fs = filesystem;

mutex mtx;
map<int, vector<double>> dadosPorHospital;
vector<double> todosInternados;

////////////////// ANÁLISE 1 ////////////

// Função para exibir alertas da semana com base em frequência de 'True'
void exibirAlertasTratados(const string& caminhoArquivo) {
    Extrator extrator;
    DataFrame df = extrator.carregar(caminhoArquivo);

    if (df.empty()) {
        cerr << "[ERRO] DataFrame vazio: " << caminhoArquivo << endl;
        return;
    }

    vector<string> colunasNecessarias = {"CEP", "Alertas"};
    for (const auto& col : colunasNecessarias) {
        if (df.colIdx(col) == static_cast<size_t>(-1)) {
            cerr << "[ERRO] Coluna necessária não encontrada: " << col << endl;
            return;
        }
    }

    // Mapa que conta quantas datas diferentes deram True por CEP
    map<string, set<string>> datasTruePorCep;

    for (const auto& linha : df.getLinhas()) {
        try {
            string cep = toString(linha[df.colIdx("CEP")]);
            string alertaStr = toString(linha[df.colIdx("Alertas")]);
            } catch (const exception& e) {
            cerr << "[AVISO] Erro ao processar linha: " << e.what() << endl;
            continue;
        }
    }

    // Separar CEPs entre vermelho e verde
    set<string> alertaVermelho;
    set<string> alertaVerde;

    // Pega todos os CEPs distintos do DataFrame
    set<string> todosCEPs;
    for (const auto& linha : df.getLinhas()) {
        string cep = toString(linha[df.colIdx("CEP")]);
        todosCEPs.insert(cep);
    }

    for (const auto& cep : todosCEPs) {
        size_t qtdTrue = datasTruePorCep[cep].size();
        if (qtdTrue > 4) {
            alertaVermelho.insert(cep);
        } else {
            alertaVerde.insert(cep);
        }
    }

    // Impressão final
    auto formatarCEPs = [](const set<string>& ceps) -> string {
        if (ceps.empty()) return "(nenhum)";
        ostringstream oss;
        for (auto it = ceps.begin(); it != ceps.end(); ++it) {
            if (it != ceps.begin()) oss << ", ";
            oss << *it;
        }
        return oss.str();
    };

    cout << "ALERTA DA SEMANA\n";
    cout << "   CEP de ilhas de alerta vermelho: " << formatarCEPs(alertaVermelho) << "\n";
    cout << "   CEP de ilhas de alerta verde: " << formatarCEPs(alertaVerde) << "\n";
}


//////////////////////////////////////// ANÁLISE 2 /////////////////////////////////////////////////

// Função para processar um único arquivo e armazenar os valores
void processarArquivo(const string& caminhoArquivo) {
    ifstream file(caminhoArquivo);
    if (!file.is_open()) {
        cerr << "[ERRO] Não foi possível abrir o arquivo: " << caminhoArquivo << endl;
        return;
    }

    string linha;
    getline(file, linha); // Ignora o cabeçalho

    while (getline(file, linha)) {
        istringstream ss(linha);
        string id, valorStr;
        getline(ss, id, ',');
        getline(ss, valorStr, ',');

        try {
            double valor = stod(valorStr);
            lock_guard<mutex> lock(mtx);
            todosInternados.push_back(valor);
        } catch (...) {
            cerr << "[AVISO] Erro ao processar linha em: " << caminhoArquivo << endl;
            continue;
        }
    }

    file.close();
}

// Função principal que varre todos os arquivos hospitalares e computa estatísticas
void calcularEstatisticasHospitalares() {
    vector<thread> threads;

    for (int i = 0; i <= 10; ++i) {
        stringstream ss;
        ss << "database_loader/saida_tratada_hospital" << i << ".csv";
        threads.emplace_back(processarArquivo, ss.str());
    }

    for (auto& t : threads) {
        t.join();
    }

    if (todosInternados.empty()) {
        cerr << "[ERRO] Nenhum dado carregado para estatísticas.\n";
        return;
    }

    // Cálculo da média
    double soma = 0.0;
    for (double x : todosInternados) soma += x;
    double media = soma / todosInternados.size();

    // Cálculo do desvio padrão
    double variancia = 0.0;
    for (double x : todosInternados) variancia += pow(x - media, 2);
    variancia /= todosInternados.size();
    double desvioPadrao = sqrt(variancia);

    cout << "\n=== Estatísticas Hospitalares ===\n";
    cout << "Total de registros: " << todosInternados.size() << "\n";
    cout << "Média de internados: " << media << "\n";
    cout << "Desvio padrão: " << desvioPadrao << "\n";
    cout << "=================================\n";
}




///////////////////////////////////////// ANÁLISE 3 //////////////////////////////////////////////

// Função que processa um arquivo individual e armazena os dados por hospital
void processarArquivoPorHospital(const string& caminhoArquivo) {
    ifstream file(caminhoArquivo);
    if (!file.is_open()) {
        cerr << "[ERRO] Não foi possível abrir o arquivo: " << caminhoArquivo << endl;
        return;
    }

    string linha;
    getline(file, linha); // Ignora o cabeçalho

    while (getline(file, linha)) {
        istringstream ss(linha);
        string idStr, valorStr;
        getline(ss, idStr, ',');
        getline(ss, valorStr, ',');

        try {
            int id = stoi(idStr);
            double valor = stod(valorStr);

            lock_guard<mutex> lock(mtx);
            dadosPorHospital[id].push_back(valor);
        } catch (...) {
            cerr << "[AVISO] Erro ao processar linha em: " << caminhoArquivo << endl;
            continue;
        }
    }

    file.close();
}

// Função principal para calcular estatísticas por hospital
void calcularEstatisticasPorHospital() {
    vector<thread> threads;

    for (int i = 0; i <= 10; ++i) {
        stringstream ss;
        ss << "database_loader/saida_tratada_hospital" << i << ".csv";
        threads.emplace_back(processarArquivoPorHospital, ss.str());
    }

    for (auto& t : threads) {
        t.join();
    }

    if (dadosPorHospital.empty()) {
        cerr << "[ERRO] Nenhum dado carregado para estatísticas por hospital.\n";
        return;
    }

    cout << "\n=== Estatísticas por Hospital ===\n";

    for (const auto& [id, valores] : dadosPorHospital) {
        if (valores.empty()) continue;

        double soma = 0.0;
        for (double v : valores) soma += v;
        double media = soma / valores.size();

        double variancia = 0.0;
        for (double v : valores) variancia += pow(v - media, 2);
        variancia /= valores.size();
        double desvioPadrao = sqrt(variancia);

        cout << "Hospital ID: " << id << "\n";
        cout << "   Média de internados: " << media << "\n";
        cout << "   Desvio padrão: " << desvioPadrao << "\n";
    }

    cout << "==================================\n";
}


//////////////////////////////////////// ANÁLISE 4 ////////////////////////////////////////////////




























//////////////////////////////////////// ANÁLISE 5 ////////////////////////////////////////////////