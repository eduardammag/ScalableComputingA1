#ifndef HANDLERS_HPP
#define HANDLERS_HPP

// #include "pipeline.hpp"
#include <thread>
#include <chrono>

using namespace std;


#include "dataframe.hpp"

class Handler {
public:
    virtual DataFrame process(const DataFrame& input) = 0;
    virtual ~Handler() {}
};

DataFrame meanAlert(const DataFrame& , const string& ); 
vector<int> stringToInt(const vector<string>&);
DataFrame groupRegions(const DataFrame& , const string& , const string& ) ;
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