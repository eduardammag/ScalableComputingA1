#include "extrator.hpp"
#include <fstream>
#include <sstream>
#include <iostream>
#include <sqlite3.h>
#include <algorithm>
#include <cctype>

// Utilitário para obter extensão de um arquivo
std::string Extrator::obterExtensao(const std::string& nomeArquivo) {
    size_t pos = nomeArquivo.rfind('.');
    return (pos != std::string::npos) ? nomeArquivo.substr(pos + 1) : "";
}

// Função pública para carregar qualquer arquivo
DataFrame Extrator::carregar(const std::string& caminhoArquivo) {
    std::string ext = obterExtensao(caminhoArquivo);

    if (ext == "csv") return carregarCSVouTXT(caminhoArquivo, ',');
    else if (ext == "txt") return carregarCSVouTXT(caminhoArquivo, '\t');
    else if (ext == "sqlite" || ext == "db") return carregarSQLite(caminhoArquivo);
    else throw std::invalid_argument("Formato de arquivo não suportado.");
}

// Leitor de arquivos .csv ou .txt
DataFrame Extrator::carregarCSVouTXT(const std::string& caminho, char separador) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        throw std::runtime_error("Não foi possível abrir o arquivo.");
    }

    std::string linha;
    std::getline(arquivo, linha); // Cabeçalho
    std::stringstream ss(linha);
    std::string celula;
    std::vector<std::string> nomesColunas;
    while (std::getline(ss, celula, separador)) {
        nomesColunas.push_back(celula);
    }

    std::vector<std::vector<std::string>> dados;
    while (std::getline(arquivo, linha)) {
        std::vector<std::string> row;
        std::stringstream ssLinha(linha);
        while (std::getline(ssLinha, celula, separador)) {
            row.push_back(celula);
        }
        dados.push_back(row);
    }

    // Inferir tipos a partir da primeira linha de dados
    std::vector<ColumnType> tipos = inferirTipos(dados[0]);

    DataFrame df(nomesColunas, tipos);
    for (const auto& row : dados) {
        df.addRow(row);
    }

    return df;
}

// Inferência de tipo básico
std::vector<ColumnType> Extrator::inferirTipos(const std::vector<std::string>& linha) {
    std::vector<ColumnType> tipos;
    for (const auto& valor : linha) {
        try {
            std::stoi(valor);
            tipos.push_back(ColumnType::INTEGER);
        } catch (...) {
            try {
                std::stod(valor);
                tipos.push_back(ColumnType::DOUBLE);
            } catch (...) {
                tipos.push_back(ColumnType::STRING);
            }
        }
    }
    return tipos;
}

// Leitor de banco SQLite
DataFrame Extrator::carregarSQLite(const std::string& caminho) {
    sqlite3* db;
    if (sqlite3_open(caminho.c_str(), &db) != SQLITE_OK) {
        throw std::runtime_error("Erro ao abrir o banco SQLite.");
    }

    std::string sql = "SELECT name FROM sqlite_master WHERE type='table';";
    sqlite3_stmt* stmt;
    std::string nomeTabela;

    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            nomeTabela = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    if (nomeTabela.empty()) {
        sqlite3_close(db);
        throw std::runtime_error("Nenhuma tabela encontrada.");
    }

    sql = "SELECT * FROM " + nomeTabela + ";";
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        sqlite3_close(db);
        throw std::runtime_error("Erro ao preparar SELECT.");
    }

    int numCols = sqlite3_column_count(stmt);
    std::vector<std::string> nomesColunas;
    std::vector<ColumnType> tipos;

    for (int i = 0; i < numCols; ++i) {
        nomesColunas.push_back(sqlite3_column_name(stmt, i));
        // Aqui simplificamos a inferência como STRING (poderia ser melhorado com tipos SQLite)
        tipos.push_back(ColumnType::STRING);
    }

    DataFrame df(nomesColunas, tipos);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        std::vector<std::string> linha;
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
