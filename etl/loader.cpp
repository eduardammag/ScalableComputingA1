#include "loader.hpp"
#include <fstream>
#include <stdexcept>
#include <sstream>

void save_as_csv(const DataFrame& df, const std::string& filename) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        throw std::runtime_error("Não foi possível abrir o arquivo: " + filename);
    }

    const auto& nomesColunas = df.getColumnNames();

    // Escreve os nomes das colunas
    for (size_t i = 0; i < nomesColunas.size(); ++i) {
        out << nomesColunas[i];
        if (i < nomesColunas.size() - 1)
            out << ",";
    }
    out << "\n";

    // Escreve os dados
    for (int i = 0; i < df.size(); ++i) {
        const auto& row = df.getRow(i);
        for (size_t j = 0; j < row.size(); ++j) {
            out << row[j];
            if (j < row.size() - 1)
                out << ",";
        }
        out << "\n";
    }

    out.close();
}