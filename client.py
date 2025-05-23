# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter client."""
import grpc
import csv
import sqlite3
import os
import random
from datetime import datetime, timedelta
import json

import etl_pb2
import etl_pb2_grpc

SERVER_ADDRESS = "localhost:50051"
STATE_FILE = "mock/simulator_state.json"

minLinhas = 50000
maxLinhas = 75000

cep_ilhas = list(range(11, 31))  # 20 ilhas
cep_regioes = []
for cep_ilha in cep_ilhas:
    cep_regioes.extend([int(f"{cep_ilha:02d}{i:03d}") for i in range(1, 6)])

def carregar_estado():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"semana_atual": 0}

def salvar_estado(estado):
    with open(STATE_FILE, "w") as f:
        json.dump(estado, f)

def oms_generate_mock(rows=random.randint(minLinhas, maxLinhas), output_file="databases_mock/oms_mock.txt"):
    headers = ["Nº óbitos", "População", "CEP", "Nº recuperados", "Nº de vacinados", "Data"]
    start_date = datetime.today() - timedelta(days=7 * (estado["semana_atual"] + 1))
    with open(output_file, mode="w", encoding="utf-8") as file:
        file.write("\t".join(headers) + "\n")
        for _ in range(rows):
            num_obitos = random.randint(0, 1000)
            populacao = random.randint(1000, 1000000)
            cep_ilha = random.choice(cep_ilhas)
            num_recuperados = random.randint(0, 5000)
            num_vacinados = random.randint(0, populacao)
            data = (start_date + timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
            file.write(f"{num_obitos}\t{populacao}\t{cep_ilha}\t{num_recuperados}\t{num_vacinados}\t{data}\n")

def hospital_generate_mock(rows=100, output_file="databases_mock/hospital_mock.csv"):
    headers = ["ID_Hospital", "Data", "Internado", "Idade", "Sexo", "CEP", "Sintoma1", "Sintoma2", "Sintoma3", "Sintoma4"]
    start_date = datetime.today() - timedelta(days=7 * (estado["semana_atual"] + 1))
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for _ in range(rows):
            id_hospital = random.randint(1, 5)
            data = (start_date + timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
            internado = random.choice([0, 1])
            idade = random.randint(0, 100)
            sexo = random.choice([0, 1])
            cep = random.choice(cep_regioes)
            sintomas = [random.randint(0, 1) for _ in range(4)]
            writer.writerow([id_hospital, data, internado, idade, sexo, cep] + sintomas)

def gerar_multiplos_arquivos_hospital(qtde_arquivos=3, min_linhas=80, max_linhas=150):
    for i in range(1, qtde_arquivos + 1):
        num_linhas = random.randint(min_linhas, max_linhas)
        nome_arquivo = f"databases_mock/hospital_mock_{i}.csv"
        hospital_generate_mock(rows=num_linhas, output_file=nome_arquivo)

def create_database(db_name="databases_mock/secretary_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pacientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Diagnostico BOOLEAN,
            Vacinado BOOLEAN,
            CEP INTEGER,
            Escolaridade INTEGER,
            Populacao INTEGER,
            Data TEXT
        )
    ''')
    conn.commit()
    conn.close()

def secretary_generate_mock(rows=random.randint(minLinhas, maxLinhas), db_name="databases_mock/secretary_data.db"):
    start_date = datetime.today() - timedelta(days=7 * (estado["semana_atual"] + 1))
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for _ in range(rows):
        diagnostico = random.choice([0, 1])
        vacinado = random.choice([0, 1])
        cep = random.choice(cep_regioes)
        escolaridade = random.randint(0, 5)
        populacao = random.randint(1000, 1000000)
        data = (start_date + timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
        cursor.execute('''
            INSERT INTO pacientes (Diagnostico, Vacinado, CEP, Escolaridade, Populacao, Data)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (diagnostico, vacinado, cep, escolaridade, populacao, data))
    conn.commit()
    conn.close()

def ler_arquivo_oms(caminho_arquivo):
    linhas = []
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        next(f)  # pula cabeçalho
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) != 6:
                continue
            num_obitos, populacao, cep, num_recuperados, num_vacinados, data = parts
            linha = etl_pb2.Linha(
                linha_oms=etl_pb2.LinhaOMS(
                    num_obitos=int(num_obitos),
                    populacao=int(populacao),
                    cep=int(cep),
                    num_recuperados=int(num_recuperados),
                    num_vacinados=int(num_vacinados),
                    data=data,
                )
            )
            linhas.append(linha)
    return linhas

def ler_arquivo_hospital(caminho_arquivo):
    linhas = []
    with open(caminho_arquivo, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            linha = etl_pb2.Linha(
                linha_hospital=etl_pb2.LinhaHospital(
                    id_hospital=int(row["ID_Hospital"]),
                    data=row["Data"],
                    internado=bool(int(row["Internado"])),
                    idade=int(row["Idade"]),
                    sexo=int(row["Sexo"]),
                    cep=int(row["CEP"]),
                    sintoma1=bool(int(row["Sintoma1"])),
                    sintoma2=bool(int(row["Sintoma2"])),
                    sintoma3=bool(int(row["Sintoma3"])),
                    sintoma4=bool(int(row["Sintoma4"])),
                )
            )
            linhas.append(linha)
    return linhas

def ler_banco_secretaria(caminho_banco):
    linhas = []
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    cursor.execute("SELECT Diagnostico, Vacinado, CEP, Escolaridade, Populacao, Data FROM pacientes")
    for row in cursor.fetchall():
        linha = etl_pb2.Linha(
            linha_secretaria=etl_pb2.LinhaSecretaria(
                diagnostico=bool(row[0]),
                vacinado=bool(row[1]),
                cep=int(row[2]),
                escolaridade=int(row[3]),
                populacao=int(row[4]),
                data=row[5],
            )
        )
        linhas.append(linha)
    conn.close()
    return linhas

def enviar_arquivo(stub, origem, nome_arquivo, linhas):
    print(f"Enviando arquivo '{nome_arquivo}' de origem '{origem}' com {len(linhas)} linhas...")
    request = etl_pb2.DadosRequest(
        origem=origem,
        nome_arquivo=nome_arquivo,
        dados=linhas
    )
    response = stub.EnviarDados(request)
    print("Resposta do servidor:", response.mensagem)

def main():
    global estado
    estado = carregar_estado()
    os.makedirs("databases_mock", exist_ok=True)

    # Remove banco antigo e arquivos antigos se quiser (opcional)
    if os.path.exists("databases_mock/secretary_data.db"):
        os.remove("databases_mock/secretary_data.db")
    for i in range(1, 11):
        arquivo_hospital = f"databases_mock/hospital_mock_{i}.csv"
        if os.path.exists(arquivo_hospital):
            os.remove(arquivo_hospital)
    arquivo_oms = "databases_mock/oms_mock.txt"
    if os.path.exists(arquivo_oms):
        os.remove(arquivo_oms)

    # Gerar arquivos mock
    oms_generate_mock(output_file=arquivo_oms)
    gerar_multiplos_arquivos_hospital(qtde_arquivos=10, min_linhas=minLinhas, max_linhas=maxLinhas)
    create_database()
    secretary_generate_mock()

    # Conectar no servidor
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = etl_pb2_grpc.ETLServiceStub(channel)

    # Enviar OMS
    linhas_oms = ler_arquivo_oms(arquivo_oms)
    enviar_arquivo(stub, origem="OMS", nome_arquivo=os.path.basename(arquivo_oms), linhas=linhas_oms)

    # Enviar hospitais
    for i in range(1, 11):
        arquivo = f"databases_mock/hospital_mock_{i}.csv"
        linhas_hospital = ler_arquivo_hospital(arquivo)
        enviar_arquivo(stub, origem="Hospital", nome_arquivo=os.path.basename(arquivo), linhas=linhas_hospital)

    # Enviar secretaria
    linhas_secretaria = ler_banco_secretaria("databases_mock/secretary_data.db")
    enviar_arquivo(stub, origem="Secretaria", nome_arquivo="secretary_data.db", linhas=linhas_secretaria)

    # Atualizar estado
    estado["semana_atual"] += 1
    salvar_estado(estado)

if __name__ == "__main__":
    main()

