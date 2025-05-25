#include "extrator.hpp"      
#include <fstream>           
#include <sstream>        
#include <iostream>
#include <filesystem>
#include <sqlite3.h>
#include <cctype>
#include "../json.hpp"

using json = nlohmann::json;
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
    else if (ext == "json") return carregarJSON(caminhoArquivo);  // json
    else throw runtime_error("Formato não suportado: " + ext);       // Erro para outros formatos
}


vector<string> Extrator::dividirLinha(const string& linha, char separador) {
    vector<string> resultado;
    string item;
    bool entreAspas = false;

    for (char c : linha)
    {
        if (c == '"')
        {
            entreAspas = !entreAspas;
        } else if (c == separador && !entreAspas)
        {
            //Remove aspas extras do item
            if (!item.empty() && item.front() == '"' && item.back() == '"')
            {
                item = item.substr(1, item.size() - 2);
            }
            resultado.push_back(item);
            item.clear();
        } else item += c;
    }

    //Adiciona o último item
    if (!item.empty() && item.front() == '"' && item.back() == '"')
    {
        item = item.substr(1, item.size() - 2);
    }
    resultado.push_back(item);

    return resultado;
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
    if (!getline(arquivo, linha)) 
    {
        throw runtime_error("Arquivo está vazio ou o cabeçalho está ausente.");
    }

    vector<string> colunas = dividirLinha(linha, separador);
    vector<vector<string>> linhasTemporarias;
    int maxAmostras = 8;
    
    // Lê até 8 linhas para inferência de tipo
    while(getline(arquivo, linha) && linhasTemporarias.size() < static_cast<size_t>(maxAmostras))
    {
        vector<string> valores = dividirLinha(linha, separador);

        //Remove linhas vazias
        if (valores.empty()) continue;

        // Valida se a linha tem o número certo de colunas
        if (valores.size() != colunas.size()) 
        {
            cerr << "[AVISO] Linha ignorada - esperado: " << colunas.size() << " colunas, encontrado: " << valores.size() << endl;
            continue;
        }
        
        linhasTemporarias.push_back(valores);
    }

    // Verificação adicional de consistência
    if (linhasTemporarias.empty()) {
        throw runtime_error("Nenhuma linha válida encontrada para inferência de tipos.");
    }

    // Debug: mostra informações antes da inferência
    // cout << "Debug - Antes da inferência:\n";
    // cout << "Número de colunas no cabeçalho: " << colunas.size() << endl;
    // cout << "Número de linhas amostrais: " << linhasTemporarias.size() << endl;
    // for (size_t i = 0; i < linhasTemporarias.size(); ++i) {
    //     cout << "Linha " << i << ": " << linhasTemporarias[i].size() << " colunas\n";
    // }

    vector<ColumnType> tipos = inferirTipos(linhasTemporarias);

    if (colunas.size() != tipos.size())
    {
        cerr << "[ERRO] Numero de colunas: " << colunas.size() << " | Numero de tipos inferidos: " << tipos.size() << endl;
        // throw invalid_argument("Number of column names and types must match.");
        tipos = vector<ColumnType>(colunas.size(), ColumnType::STRING);
    }

    // Cria o DataFrame com os nomes e tipos de colunas inferidos
    DataFrame df(colunas, tipos);

    // Adiciona as linhas lidas anteriormente
    for (const auto& valores : linhasTemporarias)
    {
        vector<Cell> row;
        for (size_t i = 0; i < valores.size(); ++i)
        {
            const string& val = valores[i];
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
        vector<string> valores = dividirLinha(linha, separador);

        // Se a linha for incompleta ou maior, ajusta ao tamanho esperado
        if (valores.size() != tipos.size())
        {
            cerr << "[AVISO] Linha ignorada - tamanho inesperado: " << valores.size() << " vs " << tipos.size()  << endl;
            continue;
        }

        vector<Cell> row;
        
        for (size_t i = 0; i < valores.size() && i < tipos.size(); ++i)
        {
            const string& val = valores[i];
            if (val.empty()) row.push_back(string(""));
            else if (tipos[i] == ColumnType::INTEGER) row.push_back(stoi(val));
            else if (tipos[i] == ColumnType::DOUBLE) row.push_back(stod(val));
            else row.push_back(val);
        }
        df.addRow(row);
    }
    return df;    
}

DataFrame Extrator::carregarJSON(const string& caminhoArquivo)
{
    ifstream arquivo(caminhoArquivo);
    if (!arquivo.is_open()) {
        throw runtime_error("Não foi possível abrir o arquivo JSON: " + caminhoArquivo);
    }

    json j;
    arquivo >> j;

    if (!j.is_array()) {
        throw runtime_error("O arquivo JSON deve conter uma lista de objetos.");
    }

    vector<string> colunas;
    vector<ColumnType> tipos;
    vector<vector<Cell>> linhas;

    // Detecta colunas com base no primeiro objeto
    if (!j.empty() && j[0].is_object()) {
        for (auto& [chave, valor] : j[0].items()) {
            colunas.push_back(chave);
            if (valor.is_number_integer()) tipos.push_back(ColumnType::INTEGER);
            else if (valor.is_number_float()) tipos.push_back(ColumnType::DOUBLE);
            else tipos.push_back(ColumnType::STRING);
        }
    }

    for (const auto& obj : j) {
        vector<Cell> linha;
        for (size_t i = 0; i < colunas.size(); ++i) {
            const string& key = colunas[i];
            if (!obj.contains(key) || obj[key].is_null()) {
                linha.push_back("");
            } else if (tipos[i] == ColumnType::INTEGER) {
                linha.push_back(obj[key].get<int>());
            } else if (tipos[i] == ColumnType::DOUBLE) {
                linha.push_back(obj[key].get<double>());
            } else {
                linha.push_back(obj[key].is_string() ? obj[key].get<string>() : obj[key].dump());
            }
        }
        linhas.push_back(linha);
    }

    DataFrame df(colunas, tipos);
    for (const auto& linha : linhas) {
        df.addRow(linha);
    }
    return df;
}

// Função auxiliar: infere os tipos de colunas com base nos valores de uma linha
vector<ColumnType> Extrator::inferirTipos(const vector<vector<string>>& amostras) {
    if (amostras.empty() || amostras[0].empty()) {
        return {};  // Retorna vetor vazio para ser tratado posteriormente
    }

    size_t numColunas = amostras[0].size();
    vector<ColumnType> tipos(numColunas, ColumnType::STRING);  // Padrão para STRING

    for (size_t col = 0; col < numColunas; ++col) {
        bool ehInteiro = true;
        bool ehDouble = true;

        for (const auto& linha : amostras) {
            if (col >= linha.size()) continue;

            const string& val = linha[col];
            if (val.empty()) continue;

            // Testa se é inteiro
            try {
                size_t pos;
                stoi(val, &pos);
                if (pos != val.size()) {
                    ehInteiro = false;
                }
            } catch (...) {
                ehInteiro = false;
            }

            // Testa se é double
            try {
                size_t pos;
                stod(val, &pos);
                if (pos != val.size()) {
                    ehDouble = false;
                }
            } catch (...) {
                ehDouble = false;
            }

            if (!ehInteiro && !ehDouble) break;
        }

        if (ehInteiro) {
            tipos[col] = ColumnType::INTEGER;
        } else if (ehDouble) {
            tipos[col] = ColumnType::DOUBLE;
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

