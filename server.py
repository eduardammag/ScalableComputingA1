import grpc
from concurrent import futures
import threading
import subprocess
import json
import optparse

import etl_pb2
import etl_pb2_grpc
# from etl_pipeline import executarPipeline

# Dicionário para armazenar arquivos temporários por tipo de origem
arquivos_recebidos = {}
lock_arquivos = threading.Lock()

TIPOS_ESPERADOS = {"oms", "hospital", "secretaria"}

class PipelineServicer(etl_pb2_grpc.ETLServiceServicer):
    def EnviarDados(self, request, context):
        origem = request.origem.lower()
        nome_arquivo = request.nome_arquivo
        dados = request.dados

        if origem not in TIPOS_ESPERADOS:
            return etl_pb2.DadosResponse(mensagem=f"Origem inválida: {origem}")

        # Transforma os dados em dicionários JSON
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
                    "sintoma1": linha.linha_hospital.sintoma1,
                    "sintoma2": linha.linha_hospital.sintoma2,
                    "sintoma3": linha.linha_hospital.sintoma3,
                    "sintoma4": linha.linha_hospital.sintoma4
                })
            elif linha.HasField("linha_secretaria"):
                linhas_json.append({
                    "diagnostico": linha.linha_secretaria.diagnostico,
                    "vacinado": linha.linha_secretaria.vacinado,
                    "cep": linha.linha_secretaria.cep,
                    "escolaridade": linha.linha_secretaria.escolaridade,
                    "populacao": linha.linha_secretaria.populacao,
                    "data": linha.linha_secretaria.data
                })

        # Salva os dados recebidos num arquivo temporário
        caminho_temp = f"temp_{origem}.json"
        with open(caminho_temp, "w") as f:
            json.dump(linhas_json, f, indent=2)

        mensagem = f"Recebido {len(linhas_json)} registros do tipo {origem}."

        with lock_arquivos:
            arquivos_recebidos[origem] = caminho_temp
            print(f"{origem} recebido e salvo em {caminho_temp}")

            if TIPOS_ESPERADOS.issubset(arquivos_recebidos.keys()):
                # Temos os 3 arquivos necessários, podemos executar o pipeline
                lista_arquivos = [arquivos_recebidos[t] for t in ['oms', 'hospital', 'secretaria']]
                print("Todos os arquivos recebidos. Executando pipeline:", lista_arquivos)

                try:
                    subprocess.run(["./programa.exe"] + lista_arquivos, check=True)
                    mensagem += " Pipeline executada com sucesso."
                except subprocess.CalledProcessError as e:
                    mensagem += f" Erro ao executar pipeline: {e}"
                
                # Limpa para próxima rodada
                arquivos_recebidos.clear()

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