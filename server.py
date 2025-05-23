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
import grpc
from concurrent import futures
import sqlite3
import os

import etl_pb2
import etl_pb2_grpc

DB_FILE = "etl_data.db"

def criar_tabelas():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS oms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            num_obitos INTEGER,
            populacao INTEGER,
            cep INTEGER,
            num_recuperados INTEGER,
            num_vacinados INTEGER,
            data TEXT
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS hospital (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_hospital INTEGER,
            data TEXT,
            internado BOOLEAN,
            idade INTEGER,
            sexo INTEGER,
            cep INTEGER,
            sintoma1 BOOLEAN,
            sintoma2 BOOLEAN,
            sintoma3 BOOLEAN,
            sintoma4 BOOLEAN
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS secretaria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            diagnostico BOOLEAN,
            vacinado BOOLEAN,
            cep INTEGER,
            escolaridade INTEGER,
            populacao INTEGER,
            data TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

class ETLServiceServicer(etl_pb2_grpc.ETLServiceServicer):
    def EnviarDados(self, request, context):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        origem = request.origem.lower()
        nome_arquivo = request.nome_arquivo
        print(f"Recebendo arquivo '{nome_arquivo}' da origem '{origem}' com {len(request.dados)} linhas.")

        try:
            for linha in request.dados:
                tipo = linha.WhichOneof("linha")

                if tipo == "linha_oms":
                    d = linha.linha_oms
                    c.execute('''
                        INSERT INTO oms (num_obitos, populacao, cep, num_recuperados, num_vacinados, data)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (d.num_obitos, d.populacao, d.cep, d.num_recuperados, d.num_vacinados, d.data))

                elif tipo == "linha_hospital":
                    d = linha.linha_hospital
                    c.execute('''
                        INSERT INTO hospital (id_hospital, data, internado, idade, sexo, cep, sintoma1, sintoma2, sintoma3, sintoma4)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (d.id_hospital, d.data, d.internado, d.idade, d.sexo, d.cep, d.sintoma1, d.sintoma2, d.sintoma3, d.sintoma4))

                elif tipo == "linha_secretaria":
                    d = linha.linha_secretaria
                    c.execute('''
                        INSERT INTO secretaria (diagnostico, vacinado, cep, escolaridade, populacao, data)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (d.diagnostico, d.vacinado, d.cep, d.escolaridade, d.populacao, d.data))

                else:
                    print(f"Linha desconhecida: {linha}")

            conn.commit()
            mensagem = f"Arquivo '{nome_arquivo}' processado com sucesso."
            print(mensagem)
            return etl_pb2.DadosResponse(mensagem=mensagem)

        except Exception as e:
            conn.rollback()
            mensagem = f"Erro ao processar o arquivo '{nome_arquivo}': {e}"
            print(mensagem)
            context.set_details(mensagem)
            context.set_code(grpc.StatusCode.INTERNAL)
            return etl_pb2.DadosResponse(mensagem=mensagem)
        finally:
            conn.close()

def serve():
    criar_tabelas()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    etl_pb2_grpc.add_ETLServiceServicer_to_server(ETLServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Servidor gRPC rodando na porta 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
