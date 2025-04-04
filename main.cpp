#include "dataframe.hpp"
#include "extrator.hpp"
#include <iostream>

int main() {
    // Define colunas: Nome, Idade, Nota, Curso, Email, Status
    vector<string> colNames = {"Nome", "Idade", "Nota", "Curso", "Email", "Status"};
    vector<ColumnType> colTypes = {
        ColumnType::STRING,
        ColumnType::INTEGER,
        ColumnType::DOUBLE,
        ColumnType::STRING,
        ColumnType::STRING,
        ColumnType::STRING
    };

    // Cria o DataFrame
    DataFrame df(colNames, colTypes);

    // Adiciona linhas ao DataFrame
    df.addRow({"Alice", "22", "8.5", "Engenharia", "alice@email.com", "Ativa"});
    df.addRow({"Bob", "30", "7.0", "Matemática", "bob@email.com", "Inativa"});
    df.addRow({"Carla", "25", "9.2", "Física", "carla@email.com", "Ativa"});
    df.addRow({"Daniel", "NULL", "10.0", "Química", "", "Ativa"});
    df.addRow({"Eva", "28", "NaN", "Computação", "eva@email.com", "Trancada"});
    df.addRow({"Fábio", "27", "6.5", "História", "fabio@email.com", "Ativa"});

    // Exibe o DataFrame original
    cout << "=== DataFrame Original ===" << endl;
    df.display();

    // Remove a 2ª linha (índice 1, ou seja, Bob)
    df.removeRow(1);

    // Remove a coluna "Email"
    df.removeColumn("Email");

    // Exibe o DataFrame após remoções
    cout << "\n=== DataFrame Após Remoções ===" << endl;
    df.display();


    
     Extrator extrator;

    vector<string> arquivos = {
        "hospital_mock.csv",
        "mock_data.csv",
        "oms_mock.txt",
        "mock_data.db"
    };

    for (const auto& arquivo : arquivos) {
        cout << "\n====================" << endl;
        cout << "Carregando: " << arquivo << endl;

        try {
            DataFrame df = extrator.carregar(arquivo);
            df.display(); // ou outro método de visualização que você tenha
        } catch (const std::exception& e) {
            cerr << "Erro ao carregar " << arquivo << ": " << e.what() << endl;
        }
    }

    return 0;
}
