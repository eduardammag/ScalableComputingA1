import grpc
from concurrent import futures
import threading
import subprocess

import etl_pb2
import etl_pb2_grpc
from etl_pipeline import executarPipeline

# Dicionário para armazenar temporariamente os arquivos recebidos
# Chave: tipo do arquivo ('csv', 'txt', 'sql'), valor: caminho temporário salvo
arquivos_recebidos = {}
lock_arquivos = threading.Lock()

class PipelineServicer(etl_pb2_grpc.PipelineServicer):
    def EnviarArquivo(self, request, context):
        tipo = request.tipo  # Ex: "csv", "txt", "sql"
        conteudo = request.conteudo  # bytes do arquivo

        caminho_arquivo = f"temp_{tipo}.file"
        with open(caminho_arquivo, "wb") as f:
            f.write(conteudo)

        with lock_arquivos:
            arquivos_recebidos[tipo] = caminho_arquivo
            if len(arquivos_recebidos) == 3:
                # Tem os 3 arquivos? Executa pipeline e limpa dicionário
                lista_arquivos = [arquivos_recebidos[t] for t in ['txt', 'csv', 'sql']]
                print("Todos arquivos recebidos, iniciando pipeline:", lista_arquivos)

                # Chama a pipeline C++ passando os arquivos como argumentos (exemplo)
                args = ["./programa.exe"] + lista_arquivos
                processo = subprocess.Popen(args)
                processo.wait()

                arquivos_recebidos.clear()

        return etl_pb2.Resposta(status="Arquivo recebido e pipeline executada se conjunto completo.")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    etl_pb2_grpc.add_PipelineServicer_to_server(PipelineServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Servidor gRPC rodando...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()