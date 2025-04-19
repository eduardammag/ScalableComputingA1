import csv
import sqlite3
import random
from datetime import datetime, timedelta
import os
import json

STATE_FILE = "mock/simulator_state.json"

def carregar_estado():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"semana_atual": 0}

def salvar_estado(estado):
    with open(STATE_FILE, "w") as f:
        json.dump(estado, f)


estado = carregar_estado()
semana_atual = estado["semana_atual"]

# Definindo intervalo da semana simulada
start_date = datetime.today() - timedelta(days=7 * (semana_atual + 1))
end_date = start_date + timedelta(days=6)

print(f"Simulando semana {semana_atual} ({start_date.strftime('%d-%m-%Y')} a {end_date.strftime('%d-%m-%Y')})")


# 20 ilhas, 5 regiões em cada, totalizando 100 regiões

# cep ilha - 2 dígitos (sortear de 1200 a 1299) 
# cep região - 5 dígitos, sendo os 2 primeiros o da ilha

# OMS dá dados por ilha, ou seja, o cep tem 2 digítos
# SS dá dados por região de cada ilha, ou seja, o cada tabela o cep terá 5 dígitos sendo os 2 primeiro iguais na mesma tabela
# O hospital segue o cep da mesma forma da SS

cep_ilhas = list(range(11, 31))  # 11 até 30, totalizando 20 ilhas


# Escolhe uma ilha aleatória para gerar os dados das regiões
cep_ilha_escolhida = random.choice(cep_ilhas)

# Gera CEPs de regiões da ilha escolhida: 5 regiões com CEPs 5 dígitos, do tipo 12001, 12002...
cep_regioes = [int(f"{cep_ilha_escolhida:02d}{i:03d}") for i in range(1, 6)] 


############################################### OMS-CSV ##############################################################



def oms_generate_mock(rows=random.randint(500_000, 600_000), output_file="databases_mock/oms_mock.txt"):
    """Gera um arquivo .txt com dados fictícios no formato da tabela da OMS."""
    headers = ["Nº óbitos", "População", "CEP da ilha", "Nº recuperados", "Nº de vacinados", "Data"]


    # arquivo_existe = os.path.exists(output_file)

    

    with open(output_file, mode="w") as file:  # modo append ("a") para adicionar, "w" para sobrescrever
        # if not arquivo_existe:
        #     file.write("\t".join(headers) + "\n")  # só escreve cabeçalho se for um novo arquivo
        file.write("\t".join(headers) + "\n")

        for _ in range(rows):
            num_obitos = random.randint(0, 1000)  
            populacao = random.randint(1000, 1000000) 
            cep_ilha = random.choice(cep_ilhas)  # Escolhe aleatoriamente um dos 20 CEPs
            num_recuperados = random.randint(0, 5000)  
            num_vacinados = random.randint(0, populacao)  
            data = (start_date + timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")

            file.write(f"{num_obitos}\t{populacao}\t{cep_ilha}\t{num_recuperados}\t{num_vacinados}\t{data}\n")

    print(f"Arquivo TXT gerado: {output_file}")

oms_generate_mock()

#teste pequeno
# oms_generate_mock(50)

############################################### HOSPITAL-CSV ##############################################################
def hospital_generate_mock(rows=100, output_file="databases_mock/hospital_mock.csv"):
    headers = ["ID_Hospital", "Data", "Internado", "Idade", "Sexo", "CEP", "Sintoma1", "Sintoma2", "Sintoma3", "Sintoma4"]
    # arquivo_existe = os.path.exists(output_file)

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        # if not arquivo_existe:
        #     writer.writerow(headers)  # Escreve o cabeçalho só uma vez

        for _ in range(rows):
            id_hospital = random.randint(1, 5)  # ID de 1 a 5
            data = (start_date + timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
            internado = random.choice([True, False])
            idade = random.randint(0, 100)
            sexo = random.choice([0, 1]) # 1 = Feminino, 0 = Masculino
            cep = random.choice(cep_regioes)  # Escolhe aleatoriamente uma das 5 regiões, já que temos dados de uma só ilha em cada tabela
            sintomas = [random.randint(0, 1) for _ in range(4)]
            
            writer.writerow([id_hospital, data, internado, idade, sexo, cep] + sintomas)

    print(f"Arquivo CSV atualizado: {output_file}")

def gerar_multiplos_arquivos_hospital(qtde_arquivos=3, min_linhas=80, max_linhas=150):
    for i in range(1, qtde_arquivos + 1):
        num_linhas = random.randint(min_linhas, max_linhas)
        nome_arquivo = f"databases_mock/hospital_mock_{i}.csv"
        hospital_generate_mock(rows=num_linhas, output_file=nome_arquivo)

    print(f"\n{qtde_arquivos} arquivos hospitalares gerados com números de linhas aleatórios entre {min_linhas} e {max_linhas}.")


# hospital_generate_mock(100)
gerar_multiplos_arquivos_hospital(qtde_arquivos=10, min_linhas=500_000, max_linhas=600_000)

#teste pequeno
#gerar_multiplos_arquivos_hospital(qtde_arquivos=3, min_linhas=5, max_linhas=20)

############################################### SECRETARIA-SQlite ##############################################################

# # Remove o banco antigo, se existir
# if os.path.exists("secretary_data.db"):
#     os.remove("secretary_data.db")

def create_database(db_name="databases_mock/secretary_data.db"):
    """Cria o banco de dados e a tabela se não existir."""
   
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

def secretary_generate_mock(rows=random.randint(500_000, 600_000), db_name="databases_mock/secretary_data.db"):
    """Gera dados fictícios e insere no banco de dados."""
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
    print(f"Banco de dados '{db_name}' gerado com {rows} registros!")


# Remover banco antigo se existir
if os.path.exists("databases_mock/secretary_data.db"):
    os.remove("databases_mock/secretary_data.db")

# Criar novo banco e popular com dados mockados
create_database()
secretary_generate_mock()

# AVANÇA PARA A PRÓXIMA SEMANA
estado["semana_atual"] += 1
salvar_estado(estado)


#teste pequeno
#secretary_generate_mock(50)


# Para verificação

# Instale sqlite3

# entre no bd com 'sqlite3 secretary_data.db'

# SELECT * FROM pacientes LIMIT 10;

# 1|1|1|88653367|5|562533|20-02-2025
# 2|0|1|56178855|3|541846|19-07-2024
# 3|0|1|49898257|2|893789|05-11-2024
# 4|1|0|79357639|2|790822|09-01-2025
# 5|1|0|77270001|0|364791|11-08-2024
# 6|0|0|95246216|0|777697|02-12-2024
# 7|0|0|46973560|2|483722|02-06-2024
# 8|1|1|91754706|5|575897|04-05-2024
# 9|0|1|65043545|5|61922|10-09-2024
# 10|1|0|16786481|2|386127|24-08-2024


# PRAGMA table_info(pacientes);

# 0|id|INTEGER|0||1
# 1|Diagnostico|BOOLEAN|0||0
# 2|Vacinado|BOOLEAN|0||0
# 3|CEP|INTEGER|0||0
# 4|Escolaridade|INTEGER|0||0
# 5|Populacao|INTEGER|0||0
# 6|Data|TEXT|0||0


# SELECT COUNT(*) FROM pacientes;

# 100

# SELECT Diagnostico, COUNT(*) 
# FROM pacientes 
# GROUP BY Diagnostico;

# 0|49
# 1|51




