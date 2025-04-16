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

using namespace std;
namespace fs = filesystem;

mutex mtx;
map<string, Estatisticas> mapaEstatisticas;

string detectarFonte(const vector<string>& colunas) 
{
    // Verifica se todas as colunas necessárias estão presentes
    bool temObitos = find(colunas.begin(), colunas.end(), "Nº óbitos") != colunas.end();
    bool temCep = find(colunas.begin(), colunas.end(), "CEP da ilha") != colunas.end();
    bool temPopulacao = find(colunas.begin(), colunas.end(), "População") != colunas.end();
        
    return (temObitos && temCep && temPopulacao) ? "oms" : "desconhecido";
}

void processarArquivoCSV(const string& caminhoArquivo) {
    try {
        Extrator extrator;
        DataFrame df = extrator.carregar(caminhoArquivo);

        if (df.empty())
        {
            cerr << "[AVISO] DataFrame vazio para o arquivo: " << caminhoArquivo << endl;
            return;
        }
        const vector<string>& colunas = df.getColumnNames();
        string fonte = detectarFonte(colunas);

        if (fonte != "oms") 
        {
            cerr << "[AVISO] Fonte desconhecida ou incompatível: " << caminhoArquivo << endl;
            return;
        }

        // Verifique se todas as colunas necessárias existem
        vector<string> colunasNecessarias = {"CEP da ilha", "Nº óbitos", "População"};
        for (const auto& col : colunasNecessarias)
        {
            if (df.colIdx(col) == static_cast<size_t>(-1))
            {
                cerr << "[ERRO] Coluna necessária não encontrada: " << col << " no arquivo " << caminhoArquivo << endl;
                return;
            }
        }

        for (const auto& linha : df.getLinhas())
        {
            try 
            {
                string cepIlha = toString(linha[df.colIdx("CEP da ilha")]);
                int obitos = static_cast<int>(toDouble(linha[df.colIdx("Nº óbitos")]));
                int populacao = static_cast<int>(toDouble(linha[df.colIdx("População")]));

                lock_guard<mutex> lock(mtx);
                Estatisticas& est = mapaEstatisticas[cepIlha];
                est.totalObitos += obitos;
                est.populacaoIlha = populacao;
                est.totalRegistrosOMS++;
            } catch (const exception& e) {
                cerr << "[ERRO] Linha ignorada no arquivo " << caminhoArquivo << ": " << e.what() << endl;
                continue;
            }
        }
    } catch (const exception& e) {
        cerr << "[ERRO CRÍTICO] Falha ao abrir ou processar: " << caminhoArquivo << " - " << e.what() << endl;
        return;
    }
}

void exibirDashboard() {
    lock_guard<mutex> lock(mtx);

    cout << "\n========== DASHBOARD DE ANALISE DE OBITOS (OMS) ==========\n";
    cout << "   Comparando a media de obitos por populacao das ilhas\n";
    cout << "   com os dados individuais de cada uma para gerar alertas.\n";
    cout << "============================================================\n";
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