#include "handlers.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>

using namespace std;

shared_ptr<DataFrame> DataCleaner::process(shared_ptr<DataFrame> input) {
    cout << "Limpando os dados..." << endl;
    this_thread::sleep_for(chrono::seconds(2));
    return input;
}

shared_ptr<DataFrame> EpidemiologicalStats::process(shared_ptr<DataFrame> input) {
    cout << "Calculando estatísticas epidemiológicas..." << endl;
    this_thread::sleep_for(chrono::seconds(3));
    return input;
}

shared_ptr<DataFrame> OutbreakDetector::process(shared_ptr<DataFrame> input) {
    cout << "Verificando aumento de casos suspeitos..." << endl;
    this_thread::sleep_for(chrono::seconds(4));
    return input;
}
