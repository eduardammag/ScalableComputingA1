#  Micro-Framework de Processamento Paralelo de Dados

Projeto desenvolvido para a disciplina de **Computação Escalável**, com o objetivo de construir um pipeline modular e eficiente para ingestão, tratamento e carregamento de dados médicos simulados. A estrutura do pipeline é genérica, com suporte à execução paralela e personalização por meio de triggers e tratadores.

---

##  Instalação

Clone o repositório e compile o projeto com `make`:

```bash
make

> Requisitos: `g++`, `make`, `sqlite3`, `Python 3.x`

---

## 📁 Estrutura do Projeto

```
.
├── etl/                    # Componentes C++ do ETL (extrator, handler, loader)
├── pipeline/               # Estrutura do pipeline
├── simulator.py            # Geração de dados mock semanais
├── databases_mock/         # Arquivos gerados pelo mock
├── triggers.cpp            # Implementação dos disparadores do pipeline
├── main.cpp                # Entrada principal do pipeline
├── Makefile                # Build do projeto
├── simulator_state.json    # Armazena os dados processados

```

---

##  Como Executar

1. Gerar os dados da semana atual com o script em Python:

```bash
python3 simulator.py
```

2. Compilar e executar o pipeline:

```bash
make
./programa
```

> A cada execução do mock, será gerada uma nova semana de dados com base na semana anterior. O controle é feito automaticamente pelo arquivo `simulator_state.json`.

---

## 📊 Fontes de Dados Simulados

O mock em Python gera três tipos de dados:

- **OMS** (`oms_mock.txt`)  
  Arquivo `.txt` tabulado com dados agregados por ilha: óbitos, população, recuperados, vacinados, data.

- **Hospitais** (`hospital_mock_*.csv`)  
  Arquivos `.csv` com registros de pacientes por hospital, incluindo idade, sintomas, sexo, internação, CEP e data.

- **Secretaria de Saúde** (`secretary_data.db`)  
  Banco SQLite com a tabela `pacientes`, contendo diagnóstico, vacinação, escolaridade, população e data.

---

##  Arquitetura do Pipeline

- **Trigger**: ativa o pipeline por tempo ou requisição.
- **Extrator**: detecta e lê arquivos `.txt`, `.csv`, ou `.db`, padronizando em `DataFrame`.
- **Tratadores gerais**: filtram inválidos, removem linhas e colunas.
- **Tratadores específicos**: agregam por colunas, calculam médias, concatenam dataframes.
- **Loader**: armazena os dados tratados.
- **Display**: relatório semanal da doença para visualização de resultados.

---

##  Paralelismo

O pipeline implementa a lógica Produtor-Consumidor com:

- Fila de arquivos protegida por `mutex`
- Várias threads de extração executando em paralelo
- Comunicação por `condition_variable` entre etapas

O número de threads pode ser ajustado na chamada da função `executarPipeline`.

---



## Alunas

- Ana Júlia Amaro Pereira Rocha  
- Maria Eduarda Mesquita Magalhães  
- Mariana Fernandes Rocha  
- Paula Eduarda de Lima  

**Curso**: Ciência de Dados e Inteligência Artificial – FGV EMAp

---
