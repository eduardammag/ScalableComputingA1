import tempfile
import subprocess

def executarPipeline(origem, dados):
    """
    Executa a pipeline com os dados recebidos diretamente da mensagem gRPC.

    - Cria um arquivo temporário com os dados serializados conforme origem.
    - Chama o programa C++ (main) passando a origem e o caminho do arquivo.
    - Retorna a saída do programa ou lança erro em caso de falha.
    """
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".txt") as f:
        for linha in dados:
            tipo = linha.WhichOneof("linha")
            if tipo == "linha_oms":
                l = linha.linha_oms
                f.write(f"{l.cep},{l.num_obitos},{l.populacao},{l.num_recuperados},{l.num_vacinados},{l.data}\n")
            elif tipo == "linha_hospital":
                l = linha.linha_hospital
                f.write(f"{l.id_hospital},{l.data},{l.internado},{l.idade},{l.sexo},{l.cep},{l.sintoma1},{l.sintoma2},{l.sintoma3},{l.sintoma4}\n")
            elif tipo == "linha_secretaria":
                l = linha.linha_secretaria
                f.write(f"{l.diagnostico},{l.vacinado},{l.cep},{l.escolaridade},{l.populacao},{l.data}\n")

        temp_path = f.name

    try:
        result = subprocess.run(["./main", origem, temp_path], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Erro na execução de main.cpp: {e.stderr.strip()}")
