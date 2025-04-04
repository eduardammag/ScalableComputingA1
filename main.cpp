#include "pipeline.hpp"
#include "handlers.hpp"
#include "triggers.hpp"
#include "dataframe.hpp"
#include "repository.hpp"
#include <iostream>
#include <memory>
#include <thread>

using namespace std;


int main() {

       // Define colunas: Nome (string), Idade (int), Nota (double)
        vector<string> colNames = {"Nome", "Idade", "Nota"};
        vector<ColumnType> colTypes = {ColumnType::STRING, ColumnType::INTEGER, ColumnType::DOUBLE};
    
        // Cria o DataFrame
        DataFrame df(colNames, colTypes);
    
        // Adiciona algumas linhas
        df.addRow({"Alice", "22", "8.5"});
        df.addRow({"Bob", "30", "7.0"});
        df.addRow({"Carla", "25", "9.2"});
        df.addRow({"Daniel", "NULL", "10.0"});   // Essa linha tem valor nulo
        df.addRow({"Bob", "30", "7.0"});         // Essa linha é duplicada
    
        // Exibe o DataFrame inicial
        cout << "=== DataFrame Original ===" << endl;
        df.display();
    
        // Remove linhas com valores nulos
        df.removeNulls();
        cout << "\n=== Após Remoção de Nulos ===" << endl;
        df.display();
    
        // Remove duplicatas
        df.removeDuplicates();
        cout << "\n=== Após Remoção de Duplicatas ===" << endl;
        df.display();
    
        // Detecta outliers na coluna de índice 2 ("Nota")
        cout << "\n=== Outliers em 'Nota' ===" << endl;
        df.detectOutliers(2);
    
        // Ordena pela coluna "Idade"
        df.sortByColumn("Idade", true);
        cout << "\n=== DataFrame Ordenado por Idade (Ascendente) ===" << endl;
        df.display();
    
        // Filtra onde Nome == "Bob"
        DataFrame bobDF = df.filterByValue("Nome", "Bob");
        cout << "\n=== Apenas linhas onde Nome == 'Bob' ===" << endl;
        bobDF.display();
    
        // Carrega dados de um arquivo CSV (certifique-se de ter esse arquivo no mesmo diretório)
        cout << "\n=== Carregando dados de 'dados.csv' ===" << endl;
        DataFrame dfCSV(colNames, colTypes);
        dfCSV.loadCSV("dados.csv");
        dfCSV.display();

    

    
    // auto repo = make_shared<MemoryRepository>();

    // auto cleaner = make_shared<DataCleaner>();
    // auto stats = make_shared<EpidemiologicalStats>();
    // auto outbreakDetector = make_shared<OutbreakDetector>();

    // auto pipeline = make_shared<Pipeline>(repo);
    // pipeline->addHandler(cleaner);
    // pipeline->addHandler(stats);
    // pipeline->addHandler(outbreakDetector);

    // auto timerTrigger = make_shared<TimerTrigger>(pipeline, 10);

    // // Rodar trigger em uma thread separada
    // thread triggerThread(&TimerTrigger::start, timerTrigger);
    // triggerThread.detach();

    // cout << "Monitoramento iniciado! Pressione Ctrl+C para sair." << endl;
    // this_thread::sleep_for(chrono::minutes(2));

    return 0;
}
