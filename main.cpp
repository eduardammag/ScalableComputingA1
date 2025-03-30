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
    auto repo = make_shared<MemoryRepository>();

    auto cleaner = make_shared<DataCleaner>();
    auto stats = make_shared<EpidemiologicalStats>();
    auto outbreakDetector = make_shared<OutbreakDetector>();

    auto pipeline = make_shared<Pipeline>(repo);
    pipeline->addHandler(cleaner);
    pipeline->addHandler(stats);
    pipeline->addHandler(outbreakDetector);

    auto timerTrigger = make_shared<TimerTrigger>(pipeline, 10);

    // Rodar trigger em uma thread separada
    thread triggerThread(&TimerTrigger::start, timerTrigger);
    triggerThread.detach();

    cout << "Monitoramento iniciado! Pressione Ctrl+C para sair." << endl;
    this_thread::sleep_for(chrono::minutes(2));

    return 0;
}
