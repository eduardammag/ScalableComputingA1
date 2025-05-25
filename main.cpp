#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "json.hpp"
#include "pipeline/pipeline.hpp"

using json = nlohmann::json;

void processarOMS(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo OMS: " << caminho << std::endl;
        return;
    }

    json dados;
    arquivo >> dados;

    for (const auto& linha : dados) {
        std::cout << "[OMS] CEP: " << linha["cep"]
                  << ", Data: " << linha["data"] << std::endl;
    }
}

void processarHospital(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo Hospital: " << caminho << std::endl;
        return;
    }

    json dados;
    arquivo >> dados;

    for (const auto& linha : dados) {
        std::cout << "[Hospital] ID: " << linha["id_hospital"]
                  << ", Data: " << linha["data"] << std::endl;
    }
}

void processarSecretaria(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo Secretaria: " << caminho << std::endl;
        return;
    }

    json dados;
    arquivo >> dados;

    for (const auto& linha : dados) {
        std::cout << "[Secretaria] CEP: " << linha["cep"]
                  << ", Diagnóstico: " << linha["diagnostico"] << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Uso: programa.exe <oms.json> <hospital.json> <secretaria.json>\n";
        return 1;
    }

    std::string arquivoOms = argv[1];
    std::string arquivoHospital = argv[2];
    std::string arquivoSecretaria = argv[3];

    executarPipeline(4, arquivoOms, arquivoSecretaria, arquivoHospital);

    return 0;
}