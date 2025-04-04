#include "dataframe.hpp"
#include <cstdlib>
#include <iostream>
#include <vector>
#include <sstream> // Para conversão de string para double
#include <cmath>
#include <algorithm>
#include <unordered_set>

using namespace std;

// Função para converter string em double de forma segura
double stringToDouble(const string& str) {
    stringstream ss(str);
    double num;
    if (!(ss >> num)) {
        return 0.0; // Retorna 0.0 se a conversão falhar
    }
    return num;
}

// Adiciona uma linha ao DataFrame
void DataFrame::addRow(const vector<string>& row) {
    data.push_back(row);
}

// Exibe os dados no terminal
void DataFrame::display() const {
    for (const auto& row : data) {
        for (const auto& value : row) {
            cout << value << " ";
        }
        cout << endl;
    }
}

// Remove linhas contendo valores nulos ("")
void DataFrame::removeNulls() {
    data.erase(remove_if(data.begin(), data.end(),
        [](const vector<string>& row) {
            return any_of(row.begin(), row.end(), [](const string& value) {
                return value.empty();
            });
        }), data.end());
}

// Remove linhas duplicadas
void DataFrame::removeDuplicates() {
    unordered_set<string> seen;
    data.erase(remove_if(data.begin(), data.end(),
        [&seen](const vector<string>& row) {
            string key = "";
            for (const auto& value : row) {
                key += value + "|";
            }
            if (seen.find(key) != seen.end()) return true;
            seen.insert(key);
            return false;
        }), data.end());
}


