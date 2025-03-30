#include "handlers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;

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
