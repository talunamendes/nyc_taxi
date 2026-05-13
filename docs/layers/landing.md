# Landing Layer - Analise de Implementacao

## Escopo analisado

- Arquivo principal: `src/nyc_taxi/lakehouse/landing/main.py`
- Dependencias diretas:
  - `src/nyc_taxi/core/config.py`
  - `src/nyc_taxi/core/logging_utils.py`

## Papel da camada no pipeline

A camada `landing` e responsavel por:

1. baixar arquivos parquet de origem (NYC TLC);
2. persisti-los em `UC Volume` em padrao Hive-style (`year=YYYY/month=MM`);
3. registrar metadados de ingestao para rastreabilidade;
4. aplicar idempotencia basica para evitar reprocessamento desnecessario.

Em termos de arquitetura, essa camada materializa a fronteira entre fonte externa e o lakehouse interno.

## Fluxo ponta a ponta da implementacao atual

### 1) Entrada do job e parsing de argumentos

O entrypoint `main()` recebe argumentos de CLI (Databricks `python_wheel_task`) e transforma os parametros em um `PipelineConfig` imutavel.

Principais argumentos aceitos:

- `--target-year`
- `--target-months` (ex.: `1,2,3,4,5`)
- `--catalog`
- `--environment` (`dev`, `stg`, `prd`)

### 2) Bootstrap do runtime Databricks

A funcao `_get_spark_and_dbutils()` instancia:

- `SparkSession` via `SparkSession.builder.getOrCreate()`;
- `DBUtils` via `DBUtils(spark)`.

Se o processo rodar fora de um cluster Databricks (sem `pyspark`), a funcao falha com `RuntimeError`.

### 3) Preparacao de objetos UC (implementado, mas desativado)

A funcao `ensure_uc_objects()` cria catalog/schema/volume com `CREATE ... IF NOT EXISTS`.

No fluxo atual, a chamada no `main()` esta comentada. Isso indica que a criacao de objetos provavelmente esta sendo feita no bootstrap do ambiente (DDL em `docs/sql`) e nao no runtime da task.

### 4) Ingestao mensal (loop principal)

Para cada mes configurado:

1. monta URL de origem a partir de `cfg.tlc_url_template`;
2. define `partition_path` e `dest_path` no volume;
3. verifica idempotencia com `file_already_ingested()`;
4. cria diretorio alvo (`dbutils.fs.mkdirs`);
5. faz download com retry exponencial (`download_with_retry`);
6. calcula hash MD5 (`compute_md5`);
7. grava `_ingestion_metadata.json` ao lado do arquivo;
8. retorna resultado estruturado (`ingested`, `skipped` ou `failed`).

### 5) Sumario e politica de exit code

Ao final:

- contabiliza `ingested`, `skipped`, `failed`;
- escreve sumario em log estruturado e no stdout em JSON;
- retorna `exit code 1` apenas quando todos os meses falham;
- retorna `exit code 0` em cenarios com sucesso total ou parcial.

## Exemplos de execucao

### Exemplo 1 - Execucao nominal

Entrada:

```bash
ingest_landing --target-year=2023 --target-months=1,2 --catalog=nyc_taxi_dev --environment=dev
```

Comportamento esperado:

- mes 1 baixa e grava parquet em `/Volumes/.../year=2023/month=01/...`;
- mes 2 baixa e grava parquet em `/Volumes/.../year=2023/month=02/...`;
- metadados escritos em cada particao;
- retorno `0`.

### Exemplo 2 - Reexecucao (idempotencia)

Se o mes ja existe com arquivo de tamanho plausivel, `file_already_ingested()` marca como `skipped`.

Comportamento esperado:

- nao refaz download;
- status final mistura `skipped` e/ou `ingested`;
- retorno segue `0` se houver pelo menos um mes nao-falho.

### Exemplo 3 - Falha parcial

Se o CDN falha para `month=03`, mas `month=04` baixa com sucesso:

- `month=03` recebe `status=failed`;
- `month=04` recebe `status=ingested`;
- job finaliza com retorno `0` (politica de falha parcial).

### Exemplo 4 - Falha total

Se todos os meses falham apos retries:

- todos os resultados ficam com `status=failed`;
- retorno final `1`.

## Pontos fortes da implementacao atual

- Separacao razoavel de responsabilidades (CLI, bootstrap, download, loop, sumario).
- Configuracao centralizada em `PipelineConfig`.
- Logging estruturado com contexto (`log_with_context`).
- Retry exponencial para resiliencia de rede.
- Processamento por mes com tolerancia a falha parcial.

## Lacunas e riscos tecnicos

1. **Idempotencia por heuristica fraca**  
   `file_already_ingested()` usa existencia de arquivo com tamanho minimo. Isso pode mascarar arquivo corrompido/parcial.

2. **Validacao de meses insuficiente**  
   Nao ha validacao explicita para meses fora de `1..12` nem para lista vazia.

3. **Ambiguidade no contrato de retorno**  
   Docstring diz que `0` e retornado se ao menos um mes for processado, mas lista vazia tambem retorna `0`.

4. **Acoplamento alto com `dbutils` e IO**  
   A regra de negocio e o acesso a armazenamento/remoto estao fortemente misturados.

5. **Criacao de objetos UC com estado ambiguo**  
   `ensure_uc_objects()` existe, mas a chamada no fluxo principal esta comentada.

6. **Medição temporal de ingestao pouco precisa**  
   `ingested_at` usa o instante de inicio da ingestao; falta separar claramente inicio/fim.

## Refactorings sugeridos (priorizados)

### Prioridade alta

1. **Idempotencia deterministica por checksum**
   - Ler `_ingestion_metadata.json` quando existir.
   - Verificar `file_name` esperado e comparar `md5`.
   - Reprocessar apenas quando divergente.

2. **Validacao forte de parametros de entrada**
   - Rejeitar meses invalidos, duplicados e lista vazia.
   - Falhar cedo com mensagem clara.

3. **Ajustar contrato de sucesso/erro**
   - Definir comportamento para "nenhum mes selecionado".
   - Sincronizar implementacao com docstring e runbook operacional.

4. **Tornar criacao de UC objects explicita via flag**
   - Ex.: `--ensure-uc-objects=true|false` (default coerente com ambiente).

### Prioridade media

5. **Extrair camadas de adaptadores (ports/adapters)**
   - `TlcDownloader` para HTTP.
   - `LandingStorage` para operacoes de volume.
   - Facilita testes e desacoplamento.

6. **Padronizar resultado com tipo dedicado**
   - Criar `IngestionResult` (`dataclass` ou `TypedDict`) em vez de dict aberto.

7. **Adicionar trilha de auditoria em tabela Delta**
   - Manter JSON local e complementar com tabela de auditoria para consultas e alertas.

### Prioridade baixa

8. **Higienizar mensagens e logs de `ensure_uc_objects()`**
   - Corrigir labels inconsistentes de schema.

9. **Padronizar nomenclatura de entrypoint na documentacao**
   - Evitar divergencias entre nomes historicos no docstring.

## Direcao recomendada para evolucao

Evoluir a `landing` para um componente orientado a contratos:

- entrada validada e rastreavel;
- idempotencia forte (checksum + metadata versionada);
- observabilidade orientada a SLO (latencia, taxa de falha, bytes ingeridos);
- separacao clara entre logica de negocio e detalhes de infraestrutura Databricks.
