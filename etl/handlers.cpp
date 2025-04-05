#include "handlers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

#include <algorithm>
#include <vector>
#include <unordered_map>
#include <string>

using namespace std;

//Tratadores a serem feitos 
// tira a média de uma coluna e identifica linhas que estão acima da média
// agrupa por região e calcula o total de casos

// Alertas de acima da média
DataFrame meanAlert(const DataFrame& input, const string& nameCol) 
{
    DataFrame output = input;
    tuple<vector<string>, ColumnType> coluna;

    auto [colValues, colType] = input.getColumn(nameCol);

    if (colType == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return output;
    }

    double sum = 0.0;
    double count = 0.0;

    for (const auto& elem : colValues) 
    {
        sum += stoi(elem);
        count++;        
    }

    auto mean = sum / count;

    vector<string> alertas;

    for (const auto& elem : colValues) 
    {
        if (stoi(elem) > mean)
        {
            alertas.push_back("True");
        }
        else
        {
            alertas.push_back("False");
        }        
    }

    output.addColumn("Alertas", ColumnType::STRING, alertas);
    cout<< "Alertas criados" << endl;
    return output;
}

DataFrame groupRegions(const DataFrame& input, const string& groupedCol, const string& aggCol) 
{
    tuple<vector<string>, ColumnType> coluna;
    
    auto [groupValues, groupType] = input.getColumn(groupedCol);
    auto [aggValues, aggType] = input.getColumn(aggCol);
    
    if (groupType == ColumnType::STRING || aggType == ColumnType::STRING)
    {
        cout << "Coluna de texto" << endl;
        return input;
    }
    
    vector<int> intGroupValues = stringToInt(groupValues);
    vector<int> intAggValues = stringToInt(aggValues);
    
    // ordenar e tirar duplicatas consecutivas
    sort(intGroupValues.begin(), intGroupValues.end()); 
    auto last = unique(intGroupValues.begin(), intGroupValues.end());
    intGroupValues.erase(last, intGroupValues.end());
    
    // Cria um mapa para armazenar os totais por região
    std::unordered_map<string, double> regionTotals;
    
    for (const auto& groupValue : intGroupValues) 
    {
        double total = 0.0;
        for (size_t i = 0; i < intGroupValues.size(); ++i) 
        {
            if (intGroupValues[i] == groupValue) 
            {
                total += intAggValues[i];
            }
        }
        regionTotals[to_string(groupValue)] = total;
    }
    
    // Adiciona os resultados ao DataFrame de saída
    vector<string> nomesColunas;
    nomesColunas.push_back("Grupos");
    nomesColunas.push_back("Agregação");

    vector<ColumnType> tipos;
    tipos.push_back(ColumnType::STRING);
    tipos.push_back(ColumnType::DOUBLE);

    DataFrame output = DataFrame(nomesColunas, tipos);

    for (const auto& pair : regionTotals) 
    {
        output.addRow({pair.first, to_string(pair.second)});
    }

    return output;
}

vector<int> stringToInt(const vector<string>& strVec) {
    vector<int> intVec;
    for (const auto& str : strVec) {
        intVec.push_back(stoi(str));
    }
    return intVec;
}




/*

teste da main

Extrator extrator;

        // Teste com um arquivo .csv (como um dos hospitais)
        DataFrame df_csv = extrator.carregar("hospital_mock_1.csv");
        std::cout << "\nArquivo CSV carregado com sucesso:\n";
        df_csv.display();
        DataFrame teste = meanAlert(df_csv, "Idade");
        teste.display();

        teste = groupRegions(df_csv, "CEP", "Idade");
        teste.display();

// 1. DataCleaner - Remove nulos e duplicatas
DataFrame DataCleaner::process(const DataFrame& input) {
    DataFrame output = input;
    output.removeNulls();
    output.removeDuplicates();
    return output;
}

// 2. OutlierDetector - Detecta valores anômalos
DataFrame OutlierDetector::process(const DataFrame& input) {
    DataFrame output = input;
    output.detectOutliers();
    return output;
}

// 3. TimeAggregator - Agrega dados por tempo
DataFrame TimeAggregator::process(const DataFrame& input) {
    DataFrame output = input;
    output.aggregateByTime("weekly");  // Suponha que esse método exista
    return output;
}

// 4. EpidemiologyAnalyzer - Analisa padrões epidemiológicos
DataFrame EpidemiologyAnalyzer::process(const DataFrame& input) {
    DataFrame output = input;
    output.analyzeCorrelations();  // Suponha que esse método exista
    return output;
}

// 5. AlertGenerator - Gera alertas de surtos
DataFrame AlertGenerator::process(const DataFrame& input) {
    DataFrame output = input;
    output.generateAlerts();
    return output;
}

*/