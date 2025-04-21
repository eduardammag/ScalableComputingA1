#include "pipeline/pipeline.hpp"
#include <iostream>
#include <chrono>
#include <filesystem>
#include "triggers.hpp"

using namespace std;

int main() {
    bool usarTimer = true;

  // Intervalo em milissegundos (só será usado se for timer)
  int intervalo = 20000;

  // Cria o trigger
  Trigger trigger([] {executarPipeline(8);}, intervalo, usarTimer);

  if (usarTimer) 
  {
      cout << "Modo timer: executando a cada " << intervalo / 1000 << " segundos..." << endl;
      trigger.start();
      this_thread::sleep_for(chrono::seconds(120));
      trigger.stop();
      cout << "Timer parado." << endl;
  } 
  else 
  {
      cout << "Modo por requisição: pressione ENTER para executar o pipeline, 'q' para sair." << endl;
      string input;
      while (true)
      {
          getline(cin, input);
          if (input == "q") break;

          // Executa sob demanda
          trigger.request();  
      }
  }
    return 0;
}
