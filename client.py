import grpc
import random
from datetime import datetime, timedelta

import etl_pb2
import etl_pb2_grpc

SERVER_ADDRESS = "localhost:50051"

cep_ilhas = list(range(11, 31))
cep_regioes = [int(f"{ilha:02d}{r:03d}") for ilha in cep_ilhas for r in range(1, 6)]

def gerar_dados_oms(rows=1000):
    linhas = []
    base_data = datetime.today()
    for _ in range(rows):
        linha = etl_pb2.Linha(
            linha_oms=etl_pb2.LinhaOMS(
                num_obitos=random.randint(0, 1000),
                populacao=random.randint(1000, 1000000),
                cep=random.choice(cep_ilhas),
                num_recuperados=random.randint(0, 5000),
                num_vacinados=random.randint(0, 500000),
                data=(base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y")
            )
        )
        linhas.append(linha)
    return linhas

def gerar_dados_hospital(rows=500):
    linhas = []
    base_data = datetime.today()
    for _ in range(rows):
        linha = etl_pb2.Linha(
            linha_hospital=etl_pb2.LinhaHospital(
                id_hospital=random.randint(1, 5),
                data=(base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y"),
                internado=random.choice([1, 0]),
                idade=random.randint(0, 100),
                sexo=random.randint(0, 1),
                cep=random.choice(cep_regioes),
                sintoma1=random.choice([1, 0]),
                sintoma2=random.choice([1, 0]),
                sintoma3=random.choice([1, 0]),
                sintoma4=random.choice([1, 0]),
            )
        )
        linhas.append(linha)
    return linhas

def gerar_dados_secretaria(rows=1000):
    linhas = []
    base_data = datetime.today()
    for _ in range(rows):
        linha = etl_pb2.Linha(
            linha_secretaria=etl_pb2.LinhaSecretaria(
                diagnostico=random.choice([1, 0]),
                vacinado=random.choice([1, 0]),
                cep=random.choice(cep_regioes),
                escolaridade=random.randint(0, 5),
                populacao=random.randint(1000, 1000000),
                data=(base_data - timedelta(days=random.randint(0, 6))).strftime("%d-%m-%Y"),
            )
        )
        linhas.append(linha)
    return linhas

def main():
    channel = grpc.insecure_channel(SERVER_ADDRESS)
    stub = etl_pb2_grpc.ETLServiceStub(channel)

    try:
        # Enviar OMS
        linhas_oms = gerar_dados_oms(10000)
        response = stub.EnviarDados(etl_pb2.DadosRequest(origem="oms", nome_arquivo="oms_virtual.txt", dados=linhas_oms))
        print(f"Resposta OMS: {response.mensagem}")

        # Enviar hospitais (vários arquivos simulados)
        # for i in range(1, 4):
        linhas_hospital = gerar_dados_hospital(random.randint(5000, 8000))
        response = stub.EnviarDados(etl_pb2.DadosRequest(origem="hospital", nome_arquivo=f"hospital_virtual.csv", dados=linhas_hospital))
        print(f"Resposta Hospital: {response.mensagem}")

        # Enviar secretaria
        linhas_secretaria = gerar_dados_secretaria(20000)
        response = stub.EnviarDados(etl_pb2.DadosRequest(origem="secretaria", nome_arquivo="secretaria_virtual.db", dados=linhas_secretaria))
        print(f"Resposta Secretaria: {response.mensagem}")

    except grpc.RpcError as e:
        print(f"Erro gRPC: {e}")


if __name__ == "__main__":
    main()