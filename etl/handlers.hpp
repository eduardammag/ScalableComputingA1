#ifndef HANDLERS_HPP
#define HANDLERS_HPP

#include <vector>
#include <string>
#include <set>
#include <limits>
#include <thread>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <map>
#include "dataframe.hpp"

using namespace std;

class Handler
{
public:
    // alerta para registros acima da média
    void meanAlert(DataFrame& , const string& , int );

    // função para agregar duas colunas
    DataFrame groupedDf(const DataFrame& , const string& , const string& , int , bool);

    // Handler para limpeza de dados - remove duplicatas e linhas/colunas com muitos valores nulos
    void dataCleaner(DataFrame&);

    // Função auxiliar para verificar se uma célula é nula
    bool isNullCell(const Cell&);

    // Handler para validar dados
    void validateDataFrame(DataFrame&, int);

    // Handler para merge de 3 DataFrames por CEP
    map<string, DataFrame> mergeByCEP(DataFrame&, DataFrame&, DataFrame&, const string&, const string&, const string&, int);

private:
    // Estrutura para regras de validação
    struct ColumnValidationRules {
        std::string name;
        ColumnType type;
        bool allowNegative = true;
        double minValue = -std::numeric_limits<double>::max();
        double maxValue = std::numeric_limits<double>::max();
        int maxLength = std::numeric_limits<int>::max();
        int minLength = 0;
        std::set<std::string> allowedStrings;
        bool isBoolean = false;
        bool isID = false;
        bool isDate = false;
        bool isIslandCEP = false;
        bool isRegionCEP = false;
    };
    
    // funções para paralelização

    // soma parcial para calcular a média
    void partialSum(const vector<Cell>& , size_t , size_t , double&, mutex& );

    // verificação parcial de linhas acima da média
    void partialAlert(const vector<Cell>& , size_t , size_t , double , vector<string>&);

    // monta a coluna que será feita a média
    void getSingleColPar(const DataFrame& , size_t , size_t , size_t , mutex& , vector<Cell>& ) ;

    // monta as duas colunas passadas em paralelo
    void getColGroup(const DataFrame& , size_t , size_t , size_t , 
        size_t , mutex& , vector<Cell>& , vector<Cell>& );

    // agrega a coluna de agregação de acordo com os grupos
    void agregarGrupoPar(const std::vector<Cell>&, const std::vector<Cell>&,
        const std::vector<Cell>&, mutex&, std::unordered_map<std::string, double>&);
    
    // Função auxiliar para remover linhas duplicadas
    void removeDuplicateRows(DataFrame&);

    // Função auxiliar para remover linhas com muitos valores nulos
    void removeSparseRows(DataFrame&, double);

    // Função auxiliar para remover colunas com muitos valores nulos
    void removeSparseColumns(DataFrame&, double);

    // Métodos auxiliares para validação
    vector<ColumnValidationRules> analyzeColumnsForValidation(const DataFrame&, int);
    void analyzeColumnPattern(const DataFrame&, int, int, ColumnValidationRules&);
    void validateRows(DataFrame&, const vector<ColumnValidationRules>&, vector<bool>&, int);
    bool isValidCell(const Cell&, const ColumnValidationRules&);
    void createValidatedDataFrame(DataFrame&, const vector<bool>&);
    void filterInvalidAges(DataFrame&, const string&, int, int);

    // Funções auxiliares genéricas
    string toLower(const string&);
    bool contains(const string&, const string&);
};

#endif // HANDLERS_HPP