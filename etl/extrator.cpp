#include "extrator.hpp"
#include <fstream>
#include <sstream>
#include <iostream>
#include <sqlite3.h>
#include <algorithm>
#include <cctype>

using namespace std;

// Utilitário para obter extensão de um arquivo
string Extrator::obterExtensao(const string& nomeArquivo) {
    size_t pos = nomeArquivo.rfind('.');
    return (pos != string::npos) ? nomeArquivo.substr(pos + 1) : "";
}

// Função pública para carregar qualquer arquivo
DataFrame Extrator::carregar(const string& caminhoArquivo) {
    string ext = obterExtensao(caminhoArquivo);

    if (ext == "csv") return carregarCSVouTXT(caminhoArquivo, ',');
    else if (ext == "txt") return carregarCSVouTXT(caminhoArquivo, '\t');
    else if (ext == "sqlite" || ext == "db") return carregarSQLite(caminhoArquivo);
    else throw invalid_argument("Formato de arquivo não suportado.");
}

// Leitor de arquivos .csv ou .txt
DataFrame Extrator::carregarCSVouTXT(const string& caminho, char separador) {
    ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        throw runtime_error("Não foi possível abrir o arquivo.");
    }

    string linha;
    getline(arquivo, linha); // Cabeçalho
    stringstream ss(linha);
    string celula;
    vector<string> nomesColunas;
    while (getline(ss, celula, separador)) {
        nomesColunas.push_back(celula);
    }

    vector<vector<string>> dados;
    while (getline(arquivo, linha)) {
        vector<string> row;
        stringstream ssLinha(linha);
        while (getline(ssLinha, celula, separador)) {
            row.push_back(celula);
        }
        dados.push_back(row);
    }

    // Inferir tipos a partir da primeira linha de dados
    vector<ColumnType> tipos = inferirTipos(dados[0]);

    DataFrame df(nomesColunas, tipos);
    for (const auto& row : dados) {
        df.addRow(row);
    }

    return df;
}

// Inferência de tipo básico
vector<ColumnType> Extrator::inferirTipos(const vector<string>& linha) {
    vector<ColumnType> tipos;
    for (const auto& valor : linha) {
        try {
            stoi(valor);
            tipos.push_back(ColumnType::INTEGER);
        } catch (...) {
            try {
                stod(valor);
                tipos.push_back(ColumnType::DOUBLE);
            } catch (...) {
                tipos.push_back(ColumnType::STRING);
            }
        }
    }
    return tipos;
}

// Leitor de banco SQLite
DataFrame Extrator::carregarSQLite(const string& caminho) {
    sqlite3* db;
    if (sqlite3_open(caminho.c_str(), &db) != SQLITE_OK) {
        throw runtime_error("Erro ao abrir o banco SQLite.");
    }

    string sql = "SELECT name FROM sqlite_master WHERE type='table';";
    sqlite3_stmt* stmt;
    string nomeTabela;

    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            nomeTabela = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    if (nomeTabela.empty()) {
        sqlite3_close(db);
        throw runtime_error("Nenhuma tabela encontrada.");
    }

    sql = "SELECT * FROM " + nomeTabela + ";";
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        sqlite3_close(db);
        throw runtime_error("Erro ao preparar SELECT.");
    }

    int numCols = sqlite3_column_count(stmt);
    vector<string> nomesColunas;
    vector<ColumnType> tipos;

    for (int i = 0; i < numCols; ++i) {
        nomesColunas.push_back(sqlite3_column_name(stmt, i));
        // Aqui simplificamos a inferência como STRING (poderia ser melhorado com tipos SQLite)
        tipos.push_back(ColumnType::STRING);
    }

    DataFrame df(nomesColunas, tipos);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        vector<string> linha;
        for (int i = 0; i < numCols; ++i) {
            const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
            linha.push_back(val ? val : "NULL");
        }
        df.addRow(linha);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return df;
}
