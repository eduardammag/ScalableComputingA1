#ifndef DATAFRAME_HPP
#define DATAFRAME_HPP

#include <iostream>
#include <vector>
#include <string>
#include <unordered_set>
#include <algorithm>
#include <cmath>

using namespace std;

class DataFrame {
private:
    vector<vector<string>> data;

public:
    // Adiciona uma linha ao DataFrame
    void addRow(const vector<string>& row);

    // Exibe os dados no terminal
    void display() const;

    // Remove linhas contendo valores nulos
    void removeNulls();

    // Remove linhas duplicadas
    void removeDuplicates();

    // Detecta outliers na 2ª coluna (considerando valores numéricos)
    void detectOutliers();

    // Agrega dados por período (método placeholder)
    void aggregateByTime(const string& period);

    // Analisa padrões epidemiológicos (método placeholder)
    void analyzeCorrelations();

    // Gera alertas para surtos de doenças
    void generateAlerts();
};

#endif // DATAFRAME_HPP
