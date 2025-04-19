#include "triggers.hpp"

Trigger::Trigger(function<void()> func, int interval, bool timer)
    : callback(func), interval(interval), isTimer(timer), running(false) {}

void Trigger::start() 
{
    // se for timer executa no intervalo de tempo 
    if (isTimer) 
    {
        running = true;
        // cria uma thread para cada ativação
        thread([this]() 
        {
            while (running) 
            {
                // populando as bases usadas
                system(R"( python "mock/simulator.py")");

                auto start_time = chrono::steady_clock::now();
                // executa a pipeline
                callback();

                auto end_time = chrono::steady_clock::now();
                auto elapsed = chrono::duration_cast<chrono::milliseconds>(end_time - start_time);
                auto sleep_time = chrono::milliseconds(interval) - elapsed;
                
                // se treminou antes de chegar a próxima etapa dorme
                if (sleep_time.count() > 0) 
                {
                    this_thread::sleep_for(sleep_time);
                }
            }
        }).detach();
    } 
}

void Trigger::stop() 
{
    running = false;
}

void Trigger::request() 
{
    if (!isTimer) 
    {   // executa uma vez por requisição
        callback();  
    }
}



/* teste da main
    bool usarTimer = true;

  // Intervalo em milissegundos (só será usado se for timer)
  int intervalo = 60000;

  // Cria o trigger
  Trigger trigger([] {executarPipeline(2);}, intervalo, usarTimer);

  if (usarTimer) {
      cout << "Modo timer: executando a cada " << intervalo / 1000 << " segundos..." << endl;
      trigger.start();
      this_thread::sleep_for(chrono::seconds(60));  // Deixa rodar por 20 segundos
      trigger.stop();
      cout << "Timer parado." << endl;
  } else {
      cout << "Modo por requisição: pressione ENTER para executar o pipeline, 'q' para sair." << endl;
      string input;
      while (true) {
          getline(cin, input);
          if (input == "q") break;

          trigger.request();  // Executa sob demanda
      }
  }
    return 0;
*/