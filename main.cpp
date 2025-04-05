#include "etl/dataframe.hpp"  // Corrigido com caminho relativo
#include "etl/extrator.hpp"
#include "pipeline/pipeline.hpp"
#include <iostream>
#include <chrono>




int main() {
    // // Define colunas: Nome, Idade, Nota, Curso, Email, Status
    // vector<string> colNames = {"Nome", "Idade", "Nota", "Curso", "Email", "Status"};
    // vector<ColumnType> colTypes = {
    //     ColumnType::STRING,
    //     ColumnType::INTEGER,
    //     ColumnType::DOUBLE,
    //     ColumnType::STRING,
    //     ColumnType::STRING,
    //     ColumnType::STRING
    // };

    // // Cria o DataFrame
    // DataFrame df(colNames, colTypes);

    // // Adiciona linhas ao DataFrame
    // df.addRow({"Alice", "22", "8.5", "Engenharia", "alice@email.com", "Ativa"});
    // df.addRow({"Bob", "30", "7.0", "Matemática", "bob@email.com", "Inativa"});
    // df.addRow({"Carla", "25", "9.2", "Física", "carla@email.com", "Ativa"});
    // df.addRow({"Daniel", "NULL", "10.0", "Química", "", "Ativa"});
    // df.addRow({"Eva", "28", "NaN", "Computação", "eva@email.com", "Trancada"});
    // df.addRow({"Fábio", "27", "6.5", "História", "fabio@email.com", "Ativa"});

    // // Exibe o DataFrame original
    // cout << "=== DataFrame Original ===" << endl;
    // df.display();

    // // Remove a 2ª linha (índice 1, ou seja, Bob)
    // df.removeRow(1);

    // // Remove a coluna "Email"
    // df.removeColumn("Email");

    // // Exibe o DataFrame após remoções
    // cout << "\n=== DataFrame Após Remoções ===" << endl;
    // df.display();


    
    // Extrator extrator;

    // vector<string> arquivos = {
    //     "hospital_mock.csv",
    //     "mock_data.csv",
    //     "oms_mock.txt",
    //     "mock_data.db"
    // };

    // for (const auto& arquivo : arquivos) {
    //     cout << "\n====================" << endl;
    //     cout << "Carregando: " << arquivo << endl;

    //     try {
    //         DataFrame df = extrator.carregar(arquivo);
    //         df.display(); // ou outro método de visualização que você tenha
    //     } catch (const exception& e) {
    //         cerr << "Erro ao carregar " << arquivo << ": " << e.what() << endl;
    //     }
    // }

////////////////////////////////////////////////Teste funções de extração e df//////////////////////////////////////////////////////////////////////
// O seguinte prompt compila a main no arquivo extrator_test
 // g++ -std=c++17 -I./etl -o extrator_test main.cpp etl/extrator.cpp etl/dataframe.cpp -lsqlite3

 //Para executar o binário compilado ussamos: ./extrator_test

    // try {
    //     Extrator extrator;

    //     // Teste com um arquivo .txt (como o gerado para a OMS)
    //     DataFrame df_txt = extrator.carregar("oms_mock.txt");
    //     std::cout << "Arquivo TXT carregado com sucesso:\n";
    //     df_txt.display(); 

    //     // Teste com um arquivo .csv (como um dos hospitais)
    //     DataFrame df_csv = extrator.carregar("hospital_mock_1.csv");
    //     std::cout << "\nArquivo CSV carregado com sucesso:\n";
    //     df_csv.display();

    //     // Teste com banco de dados SQLite
    //     DataFrame df_db = extrator.carregar("secretary_data.db");
    //     std::cout << "\nBanco de dados SQLite carregado com sucesso:\n";
    //     df_db.display();

    // } catch (const std::exception& e) {
    //     std::cerr << "Erro: " << e.what() << std::endl;
    // }


    // return 0;

//////////////////////////////////////////////////////////teste extração - produtor consummidor/////////////////////////////////////////////////////////////////////

// g++ -std=c++17 -I./etl -o pipeline_exec main.cpp pipeline/pipeline.cpp etl/extrator.cpp etl/dataframe.cpp -lsqlite3
// ./pipeline_exec

    for (int n = 1; n <= 8; n *= 2) {
        std::cout << "\n--- Testando com " << n << " consumidor(es) ---\n";
        auto inicio = std::chrono::high_resolution_clock::now();

        executarPipeline(n);  // Pipeline com n consumidores

        auto fim = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duracao = fim - inicio;

        std::cout << "Tempo: " << duracao.count() << " segundos.\n";
    }
    return 0;


}
