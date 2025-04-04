import csv
import sqlite3
import random
from datetime import datetime, timedelta

############################################### HOSPITAL-CSV ##############################################################

def hospital_generate_mock(rows=100, output_file="hospital_mock.csv"):
    headers = ["Data", "Internado", "Idade", "Sexo", "CEP", "Sintoma1", "Sintoma2", "Sintoma3", "Sintoma4"]
    
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        for _ in range(rows):

            random_days = random.randint(0, 365)
            data = (datetime.today() - timedelta(days=random_days)).strftime("%d-%m-%Y")

            internado = random.choice([True, False])
            idade = random.randint(0, 100)
            sexo = random.choice([0, 1])  # 0 = Feminino, 1 = Masculino
            cep = random.randint(10000000, 99999999)
            sintoma1 = random.randint(0, 1)
            sintoma2 = random.randint(0, 1)
            sintoma3 = random.randint(0, 1)
            sintoma4 = random.randint(0, 1)
            
            writer.writerow([data, internado, idade, sexo, cep, sintoma1, sintoma2, sintoma3, sintoma4])
    
    print(f"Arquivo CSV gerado: {output_file}")

hospital_generate_mock(100)

############################################### SECRETARIA-CSV ##############################################################


def create_database(db_name="secretary_data.db"):
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

def secretary_generate_mock(rows=100, db_name="secretary_data.db"):
    """Gera dados fictícios e insere no banco de dados."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    for _ in range(rows):
        diagnostico = random.choice([0, 1])
        vacinado = random.choice([0, 1])
        cep = random.randint(10000000, 99999999)
        escolaridade = random.randint(0, 5)
        populacao = random.randint(1000, 1000000)
        data = (datetime.today() - timedelta(days=random.randint(0, 365))).strftime("%d-%m-%Y")

        cursor.execute('''
            INSERT INTO pacientes (Diagnostico, Vacinado, CEP, Escolaridade, Populacao, Data) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (diagnostico, vacinado, cep, escolaridade, populacao, data))

    conn.commit()
    conn.close()
    print(f"Banco de dados '{db_name}' gerado com {rows} registros!")

create_database()
secretary_generate_mock(100)


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


############################################### OMS-CSV ##############################################################



def oms_generate_mock(rows=100, output_file="oms_mock.txt"):
    """Gera um arquivo .txt com dados fictícios no formato da tabela da OMS."""
    headers = ["Nº óbitos", "População", "CEP", "Nº recuperados", "Nº de vacinados", "Data"]

    with open(output_file, mode="w") as file:
        file.write("\t".join(headers) + "\n")  # Escreve o cabeçalho com tabulação

        for _ in range(rows):
            num_obitos = random.randint(0, 1000)  
            populacao = random.randint(1000, 1000000) 
            cep = random.randint(10000000, 99999999)  
            num_recuperados = random.randint(0, 5000)  
            num_vacinados = random.randint(0, populacao)  
            data = (datetime.today() - timedelta(days=random.randint(0, 365))).strftime("%d-%m-%Y")  

            file.write(f"{num_obitos}\t{populacao}\t{cep}\t{num_recuperados}\t{num_vacinados}\t{data}\n")

    print(f"Arquivo TXT gerado: {output_file}")

oms_generate_mock(100)
