#include "extrator.hpp"      // Declaração da classe Extrator
#include <fstream>           // Para leitura de arquivos
#include <sstream>           // Para manipulação de strings com stream
#include <iostream>          // Para mensagens de erro e debug
#include <sqlite3.h>         // Biblioteca SQLite
#include <algorithm>         // Para funções utilitárias como rfind
#include <cctype>            // Para funções com caracteres (não usada diretamente aqui)

using namespace std;

// Função utilitária privada da classe Extrator
// Obtém a extensão do arquivo (por ex: "csv", "txt", "db")
string Extrator::obterExtensao(const string& nomeArquivo) {
    size_t pos = nomeArquivo.rfind('.'); // Encontra a última ocorrência de ponto
    return (pos != string::npos) ? nomeArquivo.substr(pos + 1) : ""; // Retorna extensão se houver ponto
}

// Função pública principal: escolhe qual carregador usar baseado na extensão
DataFrame Extrator::carregar(const string& caminhoArquivo) {
    string ext = obterExtensao(caminhoArquivo); // Pega extensão do arquivo

    if (ext == "csv") return carregarCSVouTXT(caminhoArquivo, ',');         // CSV usa vírgula
    else if (ext == "txt") return carregarCSVouTXT(caminhoArquivo, '\t');   // TXT usa tabulação
    else if (ext == "sqlite" || ext == "db") return carregarSQLite(caminhoArquivo); // Banco SQLite
    else throw invalid_argument("Formato de arquivo não suportado.");       // Erro para outros formatos
}

// Carregador genérico para arquivos CSV e TXT
DataFrame Extrator::carregarCSVouTXT(const string& caminho, char separador) {
    ifstream arquivo(caminho); // Abre arquivo para leitura
    if (!arquivo.is_open()) {
        throw runtime_error("Não foi possível abrir o arquivo."); // Erro se falhar
    }

    // Lê a primeira linha do arquivo (cabeçalho com os nomes das colunas)
    string linha;
    getline(arquivo, linha);
    stringstream ss(linha);
    string celula;
    vector<string> nomesColunas;
    
    // Separa o cabeçalho pelo separador e armazena os nomes das colunas
    while (getline(ss, celula, separador)) {
        nomesColunas.push_back(celula);
    }

    vector<vector<string>> dados; // Armazena todas as linhas de dados

    // Lê cada linha subsequente (os dados)
    while (getline(arquivo, linha)) {
        vector<string> row;
        stringstream ssLinha(linha);
        while (getline(ssLinha, celula, separador)) {
            row.push_back(celula); // Adiciona célula à linha
        }
        dados.push_back(row); // Adiciona linha ao vetor de dados
    }

    // Infere os tipos das colunas com base na primeira linha de dados
    vector<ColumnType> tipos = inferirTipos(dados[0]);

    // Cria o DataFrame com os nomes e tipos de colunas inferidos
    DataFrame df(nomesColunas, tipos);

    // Adiciona cada linha ao DataFrame
    for (const auto& row : dados) {
        df.addRow(row);
    }

    return df;
}

// Função auxiliar: infere os tipos de colunas com base nos valores de uma linha
vector<ColumnType> Extrator::inferirTipos(const vector<string>& linha) {
    vector<ColumnType> tipos;

    for (const auto& valor : linha) {
        try {
            stoi(valor); // Tenta converter para inteiro
            tipos.push_back(ColumnType::INTEGER);
        } catch (...) {
            try {
                stod(valor); // Tenta converter para double
                tipos.push_back(ColumnType::DOUBLE);
            } catch (...) {
                tipos.push_back(ColumnType::STRING); // Se não for número, é string
            }
        }
    }

    return tipos;
}

// Função que carrega dados de um arquivo SQLite
DataFrame Extrator::carregarSQLite(const string& caminho) {
    sqlite3* db;
    
    // Abre conexão com o banco de dados SQLite
    if (sqlite3_open(caminho.c_str(), &db) != SQLITE_OK) {
        throw runtime_error("Erro ao abrir o banco SQLite.");
    }

    // Consulta para pegar o nome da primeira tabela encontrada no banco
    string sql = "SELECT name FROM sqlite_master WHERE type='table';";
    sqlite3_stmt* stmt;
    string nomeTabela;

    // Executa a consulta
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            // Pega o nome da tabela
            nomeTabela = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    if (nomeTabela.empty()) {
        sqlite3_close(db);
        throw runtime_error("Nenhuma tabela encontrada."); // Se não achou tabela, erro
    }

    // Consulta todos os dados da tabela
    sql = "SELECT * FROM " + nomeTabela + ";";
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
        sqlite3_close(db);
        throw runtime_error("Erro ao preparar SELECT.");
    }

    int numCols = sqlite3_column_count(stmt); // Número de colunas da tabela
    vector<string> nomesColunas;
    vector<ColumnType> tipos;

    // Lê os nomes das colunas da tabela
    for (int i = 0; i < numCols; ++i) {
        nomesColunas.push_back(sqlite3_column_name(stmt, i));
        tipos.push_back(ColumnType::STRING); // Aqui, por simplicidade, assume tudo como STRING
    }

    // Cria o DataFrame
    DataFrame df(nomesColunas, tipos);

    // Lê linha por linha dos resultados da consulta
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        vector<string> linha;
        for (int i = 0; i < numCols; ++i) {
            // Lê o valor da célula como string
            const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
            linha.push_back(val ? val : "NULL"); // Se for nulo, usa string "NULL"
        }
        df.addRow(linha); // Adiciona linha ao DataFrame
    }

    // Libera recursos e fecha conexão
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return df;
}
