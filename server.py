import grpc
from concurrent import futures
import threading
import subprocess
import json
import time
import os
import uuid

import etl_pb2
import etl_pb2_grpc

TIPOS_ESPERADOS = {"oms", "hospital", "secretaria"}

# Armazenamento de dados por tipo
dados_agrupados = {tipo: [] for tipo in TIPOS_ESPERADOS}
arquivos_recebidos = {}
lock = threading.Lock()
inicio_pipeline = None

class PipelineServicer(etl_pb2_grpc.ETLServiceServicer):
    def EnviarDados(self, request, context):
        global inicio_pipeline
        origem = request.origem.lower()
        nome_arquivo = request.nome_arquivo
        dados = request.dados

        if origem not in TIPOS_ESPERADOS:
            return etl_pb2.DadosResponse(mensagem=f"Origem inválida: {origem}")

        linhas_json = []

        for linha in dados:
            if linha.HasField("linha_oms"):
                linhas_json.append({
                    "num_obitos": linha.linha_oms.num_obitos,
                    "populacao": linha.linha_oms.populacao,
                    "cep": linha.linha_oms.cep,
                    "num_recuperados": linha.linha_oms.num_recuperados,
                    "num_vacinados": linha.linha_oms.num_vacinados,
                    "data": linha.linha_oms.data
                })
            elif linha.HasField("linha_hospital"):
                linhas_json.append({
                    "id_hospital": linha.linha_hospital.id_hospital,
                    "data": linha.linha_hospital.data,
                    "internado": linha.linha_hospital.internado,
                    "idade": linha.linha_hospital.idade,
                    "sexo": linha.linha_hospital.sexo,
                    "cep": linha.linha_hospital.cep,
                    "sintoma1": int(linha.linha_hospital.sintoma1),
                    "sintoma2": int(linha.linha_hospital.sintoma2),
                    "sintoma3": int(linha.linha_hospital.sintoma3),
                    "sintoma4": int(linha.linha_hospital.sintoma4)
                })
            elif linha.HasField("linha_secretaria"):
                linhas_json.append({
                    "diagnostico": int(linha.linha_secretaria.diagnostico),
                    "vacinado": int(linha.linha_secretaria.vacinado),
                    "cep": linha.linha_secretaria.cep,
                    "escolaridade": linha.linha_secretaria.escolaridade,
                    "populacao": linha.linha_secretaria.populacao,
                    "data": linha.linha_secretaria.data
                })

        with lock:
            dados_agrupados[origem].extend(linhas_json)

            if inicio_pipeline is None:
                inicio_pipeline = time.time()

            mensagem = f"Recebido {len(linhas_json)} registros do tipo {origem}."

            # Se já recebemos todos os tipos, executa a pipeline
            if all(dados_agrupados[tipo] for tipo in TIPOS_ESPERADOS):
                arquivos_temp = {}
                for tipo in TIPOS_ESPERADOS:
                    caminho_temp = f"temp_{tipo}.json"
                    with open(caminho_temp, "w") as f:
                        json.dump(dados_agrupados[tipo], f, indent=2)
                    arquivos_temp[tipo] = caminho_temp
                    print(arquivos_temp)
                    arquivos_recebidos[tipo] = caminho_temp  # para limpeza futura

                print("Todos os arquivos recebidos. Executando pipeline:", list(arquivos_temp.values()))
                try:
                    subprocess.run(["./programa.exe"] + list(arquivos_temp.values()), check=True)
                    duracao = time.time() - inicio_pipeline
                    print(f"Pipeline executada com sucesso em {duracao:.2f} segundos (tempo de latência dos clientes).")
                    mensagem += f" Pipeline executada com sucesso em {duracao:.2f} segundos."
                except subprocess.CalledProcessError as e:
                    mensagem += f" Erro ao executar pipeline: {e}"

                # Limpa estado para próxima rodada
                for caminho in arquivos_recebidos.values():
                    try:
                        os.remove(caminho)
                    except FileNotFoundError:
                        pass

                dados_agrupados.clear()
                dados_agrupados.update({tipo: [] for tipo in TIPOS_ESPERADOS})
                arquivos_recebidos.clear()
                inicio_pipeline = None

        return etl_pb2.DadosResponse(mensagem=mensagem)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    etl_pb2_grpc.add_ETLServiceServicer_to_server(PipelineServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Servidor gRPC rodando na porta 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
