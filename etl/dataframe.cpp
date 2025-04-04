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

// Detecta outliers na 2ª coluna (considerando valores numéricos)
void DataFrame::detectOutliers() {
    if (data.empty()) return;

    vector<double> values;
    for (const auto& row : data) {
        values.push_back(stringToDouble(row[1]));  // Usando a função personalizada
    }

    double sum = 0.0, mean = 0.0;
    for (double v : values) sum += v;
    mean = sum / values.size();

    double variance = 0.0;
    for (double v : values) variance += (v - mean) * (v - mean);
    variance /= values.size();
    double stddev = sqrt(variance);

    // Remove outliers (definição: valores acima de 3 desvios-padrão)
    data.erase(remove_if(data.begin(), data.end(),
        [mean, stddev](const vector<string>& row) {
            double val = stringToDouble(row[1]);
            return val > mean + 3 * stddev || val < mean - 3 * stddev;
        }), data.end());
}

// Agrega dados por período (exemplo simplificado)
void DataFrame::aggregateByTime(const string& period) {
    cout << "Agregando dados por: " << period << endl;
}

// Analisa padrões epidemiológicos (placeholder)
void DataFrame::analyzeCorrelations() {
    cout << "Analisando padrões epidemiológicos..." << endl;
}

// Gera alertas para surtos de doenças
void DataFrame::generateAlerts() {
    cout << "Gerando alertas para possíveis surtos!" << endl;
}
