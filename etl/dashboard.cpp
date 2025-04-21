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
#include <numeric>
using namespace std;
namespace fs = filesystem;

mutex mtx;
map<int, vector<double>> dadosPorHospital;
vector<double> todosInternados;

////////////////// ANÁLISE 1 ////////////

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

    set<string> alertaVermelho;
    set<string> alertaVerde;

    for (const auto& linha : df.getLinhas()) {
        try {
            string cep = toString(linha[df.colIdx("CEP")]);
            string alerta = toString(linha[df.colIdx("Alertas")]);

            if (alerta == "Vermelho") {
                alertaVermelho.insert(cep);
            } else if (alerta == "Verde") {
                alertaVerde.insert(cep);
            }
        } catch (const exception& e) {
            cerr << "[AVISO] Erro ao processar linha: " << e.what() << endl;
            continue;
        }
    }

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

// Correlação de Pearson
void analyzeCorrelation() {
    string filepath;

    // Percorre os arquivos dentro da pasta 'database_loader'
    for (const auto& entry : filesystem::directory_iterator("database_loader")) {
        // Verifica se o nome do arquivo começa com "saida_merge_n3"
        if (entry.path().filename().string().rfind("saida_merge_34", 0) == 0) {
            filepath = entry.path().string();  // Armazena o caminho do arquivo encontrado
            break;  // Para a busca após encontrar o primeiro correspondente
        }
    }

    // Se nenhum arquivo foi encontrado, emite erro e retorna
    if (filepath.empty()) {
        cerr << "Arquivo 'saida_merge_34' não encontrado.\n";
        return;
    }

    // Usa o Extrator para carregar o conteúdo CSV em um DataFrame
    Extrator extrator;
    DataFrame df = extrator.carregar(filepath);  // Carrega os dados do arquivo em estrutura de DataFrame

    // Obtém os índices das colunas desejadas
    int col1 = df.colIdx("Total_Nº óbitos");
    int col2 = df.colIdx("Total_Vacinado_C");

    // Vetores para armazenar os valores numéricos das colunas
    vector<double> x, y;

    // Itera sobre cada linha do DataFrame
    for (int i = 0; i < df.size(); ++i) {
        try {
            // Converte os valores das colunas para double e armazena nos vetores
            x.push_back(toDouble(df.getRow(i)[col1]));
            y.push_back(toDouble(df.getRow(i)[col2]));
        } catch (...) {
            // Ignora qualquer erro de conversão silenciosamente
        }
    }

    // Define n como o número de pares válidos
    int n = min(x.size(), y.size());
    if (n == 0) {
        cerr << "Colunas vazias ou inválidas.\n";
        return;
    }

    // Calcula as médias das duas variáveis
    double mean_x = accumulate(x.begin(), x.end(), 0.0) / n;
    double mean_y = accumulate(y.begin(), y.end(), 0.0) / n;

    // Variáveis auxiliares para o cálculo da correlação
    double num = 0.0, den1 = 0.0, den2 = 0.0;

    // Cálculo do numerador e denominadores da fórmula de Pearson
    for (int i = 0; i < n; ++i) {
        double dx = x[i] - mean_x;
        double dy = y[i] - mean_y;
        num += dx * dy;
        den1 += dx * dx;
        den2 += dy * dy;
    }

    // Correlação de Pearson
    double corr = num / sqrt(den1 * den2);

    // Exibe o resultado
    cout << "Correlação de Pearson (Total_Nº óbitos vs Total_Vacinado_C): " << corr << endl;
}


























//////////////////////////////////////// ANÁLISE 5 ////////////////////////////////////////////////


// Função 2: Regressão Linear
void regressionInternadoVsVacinado() {
    string filepath;
    for (const auto& entry : filesystem::directory_iterator("database_loader")) {
        if (entry.path().filename().string().rfind("saida_merge_24", 0) == 0) {
            filepath = entry.path().string();
            break;
        }
    }

    if (filepath.empty()) {
        cerr << "Arquivo 'saida_merge_24' não encontrado.\n";
        return;
    }

    Extrator extrator;
    DataFrame df = extrator.carregar(filepath);  // <--- substituição feita aqui

    int colY = df.colIdx("Total_Internado");
    int colX = df.colIdx("Total_Vacinado_C");

    vector<double> x, y;
    for (int i = 0; i < df.size(); ++i) {
        try {
            x.push_back(toDouble(df.getRow(i)[colX]));
            y.push_back(toDouble(df.getRow(i)[colY]));
        } catch (...) {}
    }

    int n = min(x.size(), y.size());
    if (n == 0) {
        cerr << "Colunas vazias ou inválidas.\n";
        return;
    }

    double mean_x = accumulate(x.begin(), x.end(), 0.0) / n;
    double mean_y = accumulate(y.begin(), y.end(), 0.0) / n;

    double num = 0.0, den = 0.0;
    for (int i = 0; i < n; ++i) {
        num += (x[i] - mean_x) * (y[i] - mean_y);
        den += (x[i] - mean_x) * (x[i] - mean_x);
    }

    double beta1 = num / den;
    double beta0 = mean_y - beta1 * mean_x;

    cout << "Regressão Linear (Total_Internado ~ Total_Vacinado_C):\n";
    cout << "  β0 (intercepto): " << beta0 << "\n";
    cout << "  β1 (coeficiente): " << beta1 << "\n";
}
