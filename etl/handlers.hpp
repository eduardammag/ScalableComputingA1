#ifndef HANDLERS_HPP
#define HANDLERS_HPP

// #include "pipeline.hpp"
#include <thread>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include "dataframe.hpp"

using namespace std;

class Handler
{
public:
    // alerta para registros acima da média
    DataFrame meanAlert(const DataFrame& , const string& , int );

    // função para agregar duas colunas
    DataFrame groupedDf(const DataFrame& , const string& , const string& , int );

    // virtual DataFrame process(const DataFrame& input) = 0;
    // virtual ~Handler() {};
private:
    // funções para paralelização

    // soma parcial para calcular a média
    void partialSum(const vector<Cell>& , size_t , size_t , double& , double& , mutex& );

    // verificação parcial de linhas acima da média
    void partialAlert(const vector<Cell>& , size_t , size_t , double , vector<string>& , mutex& );

    // monta a coluna que será feita a média
    void getSingleColPar(const DataFrame& , size_t , size_t , size_t , mutex& , vector<Cell>& ) ;

    // monta as duas colunas passadas em paralelo
    void getColGroup(const DataFrame& , size_t , size_t , size_t , 
        size_t , mutex& , vector<Cell>& , vector<Cell>& );
    // agrega a coluna de agregação de acordo com os grupos
    void agregarGrupoPar(const std::vector<Cell>&, const std::vector<Cell>&,
        const std::vector<Cell>&, mutex&, std::unordered_map<std::string, double>&); 
};


// funções para paralelização

// DataFrame groupRegions(const DataFrame& , const string& , const string& ) ;
/*
// 1. Tratador de Limpeza de Dados
class DataCleaner : public Handler {
public:
    DataFrame process(const DataFrame& input) override;
};

// 2. Tratador de Detecção de Outliers
class OutlierDetector : public Handler {
public:
    DataFrame process(const DataFrame& input) override;
};

// 3. Tratador de Agregação Temporal
class TimeAggregator : public Handler {
public:
    DataFrame process(const DataFrame& input) override;
};

// 4. Tratador de Correlação Epidemiológica
class EpidemiologyAnalyzer : public Handler {
public:
    DataFrame process(const DataFrame& input) override;
};

// 5. Tratador de Geração de Alertas
class AlertGenerator : public Handler {
public:
    DataFrame process(const DataFrame& input) override;
};

*/

#endif // HANDLERS_HPP