#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

void processarOMS(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo OMS: " << caminho << std::endl;
        return;
    }

    std::string linha;
    while (std::getline(arquivo, linha)) {
        std::istringstream ss(linha);
        std::string cep, num_obitos, populacao, num_recuperados, num_vacinados, data;
        std::getline(ss, cep, ',');
        std::getline(ss, num_obitos, ',');
        std::getline(ss, populacao, ',');
        std::getline(ss, num_recuperados, ',');
        std::getline(ss, num_vacinados, ',');
        std::getline(ss, data, ',');

        std::cout << "[OMS] CEP: " << cep << ", Data: " << data << std::endl;
    }
}

void processarHospital(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo Hospital: " << caminho << std::endl;
        return;
    }

    std::string linha;
    while (std::getline(arquivo, linha)) {
        std::istringstream ss(linha);
        std::string id_hospital, data, internado, idade, sexo, cep, sintoma1, sintoma2, sintoma3, sintoma4;
        std::getline(ss, id_hospital, ',');
        std::getline(ss, data, ',');
        std::getline(ss, internado, ',');
        std::getline(ss, idade, ',');
        std::getline(ss, sexo, ',');
        std::getline(ss, cep, ',');
        std::getline(ss, sintoma1, ',');
        std::getline(ss, sintoma2, ',');
        std::getline(ss, sintoma3, ',');
        std::getline(ss, sintoma4, ',');

        std::cout << "[Hospital] ID: " << id_hospital << ", Data: " << data << std::endl;
    }
}

void processarSecretaria(const std::string& caminho) {
    std::ifstream arquivo(caminho);
    if (!arquivo.is_open()) {
        std::cerr << "Erro ao abrir arquivo Secretaria: " << caminho << std::endl;
        return;
    }

    std::string linha;
    while (std::getline(arquivo, linha)) {
        std::istringstream ss(linha);
        std::string diagnostico, vacinado, cep, escolaridade, populacao, data;
        std::getline(ss, diagnostico, ',');
        std::getline(ss, vacinado, ',');
        std::getline(ss, cep, ',');
        std::getline(ss, escolaridade, ',');
        std::getline(ss, populacao, ',');
        std::getline(ss, data, ',');

        std::cout << "[Secretaria] CEP: " << cep << ", Diagnóstico: " << diagnostico << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Uso: " << argv[0] << " <origem> <caminho_arquivo>" << std::endl;
        return 1;
    }

    std::string origem = argv[1];
    std::string caminho = argv[2];

    if (origem == "oms") {
        processarOMS(caminho);
    } else if (origem == "hospital") {
        processarHospital(caminho);
    } else if (origem == "secretaria") {
        processarSecretaria(caminho);
    } else {
        std::cerr << "Origem inválida: " << origem << std::endl;
        return 1;
    }

    return 0;
}