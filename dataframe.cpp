#include "DataFrame.hpp"
using namespace std;

// Adiciona uma linha ao DataFrame
void DataFrame::addRow(const vector<string>& row) {
    data.push_back(row);
}

// Exibe os dados do DataFrame
void DataFrame::display() const {
    for (const auto& row : data) {
        for (const auto& value : row) {
            cout << value << " ";
        }
        cout << endl;
    }
}
