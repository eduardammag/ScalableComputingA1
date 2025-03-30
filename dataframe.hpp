#ifndef DATAFRAME_HPP
#define DATAFRAME_HPP

#include <iostream>
#include <vector>
#include <string>

using namespace std;

class DataFrame {
private:
    vector<vector<string>> data; // Armazena os dados tabulares

public:
    // Adiciona uma linha ao DataFrame
    void addRow(const vector<string>& row);

    // Exibe os dados do DataFrame
    void display() const;
};

#endif // DATAFRAME_HPP
