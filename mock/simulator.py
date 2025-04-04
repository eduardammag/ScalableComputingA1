import csv
import random
from datetime import datetime, timedelta

def hospital_generate_mock(rows=100, output_file="mock_data.csv"):
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

# Gerar 100 linhas de dados fictícios
hospital_generate_mock(100)
