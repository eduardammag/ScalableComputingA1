#include "extrator.hpp"      
#include <fstream>           
#include <sstream>        
#include <iostream>
#include <filesystem>
#include <sqlite3.h>
#include <cctype> 

using std::string, std::vector, std::ifstream, std::stringstream, std::runtime_error;
// using std::stoi, std::stod;

// Função utilitária privada da classe Extrator
// Obtém a extensão do arquivo (por ex: "csv", "txt", "db")
string Extrator::obterExtensao(const string& nomeArquivo) 
{
    size_t pos = nomeArquivo.find_last_of('.'); // Encontra a última ocorrência de ponto
    return (pos != string::npos) ? nomeArquivo.substr(pos + 1) : ""; // Retorna extensão se houver ponto
}

// Função pública principal: escolhe qual carregador usar baseado na extensão
DataFrame Extrator::carregar(const string& caminhoArquivo) 
{
    if (!std::filesystem::exists(caminhoArquivo))
    {
        throw runtime_error("Arquivo não encontrado: " + caminhoArquivo);
    }

    string ext = obterExtensao(caminhoArquivo);
    if (ext == "csv") return carregarCSVouTXT(caminhoArquivo, ',');         // CSV usa vírgula
    else if (ext == "txt") return carregarCSVouTXT(caminhoArquivo, '\t');   // TXT usa tabulação
    else if (ext == "sqlite" || ext == "db") return carregarSQLite(caminhoArquivo); // Banco SQLite
    else throw runtime_error("Formato não suportado: " + ext);       // Erro para outros formatos
}

// Carregador genérico para arquivos CSV e TXT
DataFrame Extrator::carregarCSVouTXT(const string& caminho, char separador)
{
    ifstream arquivo(caminho); // Abre arquivo para leitura
    if (!arquivo.is_open()) 
    {
        throw runtime_error("Não foi possível abrir o arquivo: " + caminho); // Erro se falhar
    }

    // Lê a primeira linha do arquivo (cabeçalho com os nomes das colunas)
    string linha;
    vector<string> colunas;
    vector<vector<string>> linhasTemporarias;
    int maxAmostras = 8;

    if (getline(arquivo, linha))
    {
        stringstream ss(linha);
        string valor;
        while (getline(ss, valor, separador))
        {
            colunas.push_back(valor);
        }
    }
    
    // Lê até 10 linhas para inferência de tipo
    while(getline(arquivo, linha) && linhasTemporarias.size() < static_cast<size_t>(maxAmostras))
    {
        vector<string> valores;
        stringstream ss(linha);
        string valor;
        while (getline(ss, valor, separador))
        {
            valores.push_back(valor);
        }
        linhasTemporarias.push_back(valores);
    }

    vector<ColumnType> tipos = inferirTipos(linhasTemporarias);

    // Cria o DataFrame com os nomes e tipos de colunas inferidos
    DataFrame df(colunas, tipos);

    // Adiciona as linhas lidas anteriormente
    for (const auto& linha : linhasTemporarias)
    {
        vector<Cell> row;
        for (size_t i = 0; i < linha.size(); ++i)
        {
            const string& val = linha[i];
            if (val.empty()) row.push_back(string(""));
            else if (tipos[i] == ColumnType::INTEGER) row.push_back(stoi(val));
            else if (tipos[i] == ColumnType::DOUBLE) row.push_back(stod(val));
            else row.push_back(val);
        }
        df.addRow(row);
    }

    // Continua lendo o restante do arquivo
    while(getline(arquivo, linha))
    {
        vector<Cell> row;
        stringstream ss(linha);
        string valor;
        size_t i = 0;
        while (getline(ss, valor, separador) && i < tipos.size())
        {
            if (valor.empty()) row.push_back(string(""));
            else if (tipos[i] == ColumnType::INTEGER) row.push_back(stoi(valor));
            else if (tipos[i] == ColumnType::DOUBLE) row.push_back(stod(valor));
            else row.push_back(valor);
            ++i;
        }
        df.addRow(row);
    }
    return df;    
}

// Função auxiliar: infere os tipos de colunas com base nos valores de uma linha
vector<ColumnType> Extrator::inferirTipos(const vector<vector<string>>& amostras) 
{
    if (amostras.empty()) return {};

    size_t numColunas = amostras[0].size();
    vector<ColumnType> tipos(numColunas, ColumnType::INTEGER);

    for (size_t col = 0; col < numColunas; ++col) {
        bool ehInteiro = true;
        bool ehDouble = true;

        for (const auto& linha : amostras) {
            if (col >= linha.size()) continue;

            const string& val = linha[col];
            if (val.empty()) continue;

            try {
                size_t pos;
                stoi(val, &pos);
                if (pos != val.size()) {
                    ehInteiro = false;
                    stod(val, &pos);  // testa double
                    if (pos != val.size()) {
                        ehDouble = false;
                        break;
                    }
                }
            } catch (...) {
                try {
                    size_t pos;
                    stod(val, &pos);
                    if (pos != val.size()) {
                        ehDouble = false;
                        break;
                    }
                    ehInteiro = false;
                } catch (...) {
                    ehInteiro = false;
                    ehDouble = false;
                    break;
                }
            }
        }

        if (ehInteiro) {
            tipos[col] = ColumnType::INTEGER;
        } else if (ehDouble) {
            tipos[col] = ColumnType::DOUBLE;
        } else {
            tipos[col] = ColumnType::STRING;
        }
    }

    return tipos;
}

// Função que carrega dados de um arquivo SQLite
DataFrame Extrator::carregarSQLite(const string& caminho)
{
    sqlite3* db;
    
    // Abre conexão com o banco de dados SQLite
    if (sqlite3_open(caminho.c_str(), &db) != SQLITE_OK)
    {
        throw runtime_error("Erro ao abrir o banco SQLite.");
    }

    // Consulta para pegar o nome da primeira tabela encontrada no banco
    string sql = "SELECT name FROM sqlite_master WHERE type='table';";
    sqlite3_stmt* stmt;
    string nomeTabela;

    // Executa a consulta
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
    {
        if (sqlite3_step(stmt) == SQLITE_ROW)
        {
            nomeTabela = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
    }

    if (nomeTabela.empty())
    {
        sqlite3_close(db);
        throw runtime_error("Nenhuma tabela encontrada."); // Se não achou tabela, erro
    }

    // Consulta todos os dados da tabela
    sql = "SELECT * FROM " + nomeTabela + ";";
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
    {
        sqlite3_close(db);
        throw runtime_error("Erro ao preparar SELECT.");
    }

    int numCols = sqlite3_column_count(stmt); // Número de colunas da tabela
    vector<string> nomesColunas;
    vector<ColumnType> tipos;

    // Lê os nomes das colunas da tabela
    for (int i = 0; i < numCols; ++i) 
    {
        nomesColunas.push_back(sqlite3_column_name(stmt, i));
    }

    // Inferência de tipos com base nos primeiros valores não nulos
    vector<bool> tipoInferido(numCols, false);
    tipos.resize(numCols, ColumnType::STRING);

    // Armazenar linhas temporárias para adicionar depois da inferência
    vector<vector<Cell>> linhas;

    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        vector<Cell> linha;
        for (int i = 0; i < numCols; ++i)
        {
            int tipo = sqlite3_column_type(stmt, i);

            if (!tipoInferido[i])
            {
                switch (tipo)
                {
                    case SQLITE_INTEGER:
                        tipos[i] = ColumnType::INTEGER;
                        break;
                    case SQLITE_FLOAT:
                        tipos[i] = ColumnType::DOUBLE;
                        break;
                    case SQLITE_TEXT:
                        tipos[i] = ColumnType::STRING;
                        break;
                    case SQLITE_NULL:
                        break; // Mantém o tipo como STRING por enquanto
                    default:
                        tipos[i] = ColumnType::STRING;
                        break;
                }
                if (tipo != SQLITE_NULL) tipoInferido[i] = true;
            }

            switch (tipo) 
            {
                case SQLITE_INTEGER:
                    linha.push_back(static_cast<int>(sqlite3_column_int(stmt, i)));
                    break;
                case SQLITE_FLOAT:
                    linha.push_back(static_cast<double>(sqlite3_column_double(stmt, i)));
                    break;
                case SQLITE_TEXT: 
                {
                    const char* val = reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
                    linha.push_back(val ? string(val) : "");
                    break;
                }
                case SQLITE_NULL:
                    linha.push_back(string("")); // Representa NULL como string vazia
                    break;
                default:
                    linha.push_back(string("")); // Fallback
                    break;
            }
        }
        linhas.push_back(linha);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    // Cria o DataFrame
    DataFrame df(nomesColunas, tipos);
    for (const auto& linha : linhas) df.addRow(linha);

    return df;
}

