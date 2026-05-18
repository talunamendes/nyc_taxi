# Threat Model (Resumo)

## Escopo

Pipeline de ingestão e transformação de dados NYC Taxi (Yellow e Green) em Databricks
Serverless, com quatro camadas sequenciais: **landing → bronze → silver → gold**.
Execução via job serverless mensal (cron dia 2, 05h Sao Paulo) com artefato wheel distribuído
por Declarative Asset Bundles (DAB). Três ambientes rastreados em Git: `dev`, `stg`, `prd`.
Armazenamento em Unity Catalog (catálogo/schemas/volumes/tabelas Delta + view de consumo).

## Ativos Críticos

| Ativo | Localização UC | Impacto em caso de perda/comprometimento |
|---|---|---|
| Dados brutos (Parquet TLC) | `<catalog>.landing` (Volume) | Re-ingestão custosa; fonte externa pode não manter histórico |
| Tabelas Delta bronze| `<catalog>.bronze.{yellow,green}_taxi` | Perda de linhagem e checkpoint; bronze teria de ser re-criada do zero |
| Volumes de checkpoint/schema | `<catalog>.{checkpoints,schemas}` (Volumes) | Auto Loader relê todos os arquivos da landing; risco de duplicação se bronze não for recriada |
| Tabelas Delta silver | `<catalog>.silver.{yellow,green}_taxi_trips` | Dado curado perdido; MERGE idempotente permite reconstrução mas com custo O(bronze) |
| View de consumo gold | `<catalog>.gold.vw_taxi_trips` | DDL idempotente (`CREATE OR REPLACE VIEW`); reconstrução instantânea |
| Credenciais Databricks/CLI | workspace/token | Acesso irrestrito ao workspace e aos dados do catálogo |
| Bundle DAB + artefato wheel | `dist/*.whl` / Git | Execução arbitrária de código no contexto do job |
| Metadados de ingestão | `_ingestion_metadata.json` por partição | Perda de rastreabilidade de proveniência e MD5 por arquivo |

## Principais Ameaças

**Acesso não autorizado a dados**
Permissões excessivas em catálogo/schema/volume expõem dados a principals não autorizados.
Bronze e silver carregam metadados de passageiros (contagem, localização, pagamento) — marcados
como `pii = none` nos UC tags, mas o risco de re-identificação por combinação existe.

**Comprometimento do artefato wheel (supply chain)**
A dependência de produção é mínima (`requests>=2.33.1`); dependências de dev incluem
`databricks-connect`, `mypy`, `ruff`, `pytest`, `pre-commit`. Um pacote malicioso no PyPI
com qualquer uma dessas dependências poderia ser incluído no wheel implantado.

**Corrupção ou perda de checkpoint/schemaLocation**
Os volumes de checkpoint (`<catalog>.checkpoints`) e schema (`<catalog>.schemas`) são a
memória do Auto Loader. Sua perda faz o bronze relê tudo da landing, com risco de duplicação
caso a tabela Delta não seja recriada junto.

**Manipulação de execução via bundle**
Alteração indevida em `databricks.yml` ou `resources/nyc_taxi_job.yml` (por PR não revisado
ou credencial comprometida) pode redirecionar o job, alterar parâmetros de catálogo/ambiente
ou injetar código via wheel substituta.

**Perda de disponibilidade da fonte externa**
O CDN do TLC (endpoint HTTP externo) é ponto único de falha para a camada landing.
Falha transitória é mitigada por retry; falha permanente ou mudança de URL exige intervenção
manual.

**Drift silencioso de schema / data dictionary**
As regras de DQ da silver (enums de `VendorID`, `payment_type` etc.) são constantes
hardcoded ancoradas nos PDFs do TLC. Se o TLC publicar novo valor sem atualização das
constantes, registros válidos são silenciosamente descartados.

**Perda de observabilidade por drop silencioso**
A silver descarta registros inválidos via filtro `WHERE` sem escrever em quarentena.
Não é possível auditar retroativamente quais registros foram removidos e por qual regra.

**Snapshot inconsistente na gold**
A gold é uma `VIEW` sobre a silver; re-execução da silver (ex.: correção de dados TLC)
altera os resultados da view retroativamente sem versionamento ou snapshot imutável.

## Mitigações Adotadas

**Governança e controle de acesso**
Unity Catalog como plano único de controle de permissões. Todas as tabelas Delta têm UC
tags explícitas (`layer`, `domain`, `taxi_type`, `criticality`, `pii`), incluindo
`pii = none` — política de privacidade declarada no catálogo.

**Integridade de ingestão**
MD5 calculado e gravado em `_ingestion_metadata.json` por arquivo ingerido na landing.
Verificação de idempotência antes de re-download. Retry com backoff exponencial
(`download_with_retry`) para falhas transitórias do CDN TLC.

**Idempotência por camada**
- Landing: verificação de existência + tamanho mínimo antes de re-download.
- Bronze: Auto Loader com checkpoint — relê apenas arquivos novos.
- Silver: MERGE pela chave de negócio `(VendorID, pickup_datetime, dropoff_datetime)`.
- Gold: `CREATE OR REPLACE VIEW` — idempotente por natureza.

**Schema evolution segura**
Bronze usa Auto Loader com `schemaEvolutionMode = addNewColumns` + `max_retries: 2`
(ADR-010): schema novo é detectado, stream encerra com exceção, retry prossegue com
schema atualizado. Silver usa `.withSchemaEvolution()` no builder do MERGE Delta,
disponível em delta-spark 3.3+/DBR 15.3+ (compatível com Serverless atual).
`delta.columnMapping.mode = name` previne corrupção de colunas em renomeação.

**Deploy rastreável e isolado por ambiente**
DAB com `mode: development` pausa schedules em `dev` automaticamente. Separação de
catálogo por ambiente via `--var catalog=...`. Três targets (`dev`, `stg`, `prd`)
rastreados em Git. Bundle UUID fixo previne colisão de recursos entre deploys.

**Política de falha por camada**
- Landing e bronze: falha parcial tolerada (um taxi não bloqueia o outro); exit 1
  apenas se todos os taxis falharem.
- Silver e gold: fail-fast — qualquer falha interrompe o job, evitando snapshot
  inconsistente no contrato de consumo.

**Prevenção de execução concorrente**
`queue: enabled: true` no job Databricks previne execuções simultâneas que poderiam
causar conflitos de escrita nas tabelas Delta.

**Observabilidade de auditoria**
Logging estruturado em JSON (`JSONFormatter`) com campos `timestamp`, `level`, `module`,
`function`, `line` — compatível com Datadog/ELK. Change Data Feed (`delta.enableChangeDataFeed`)
habilitado em bronze e silver para rastreabilidade de mudanças.

**Qualidade de código e CI**
Mypy strict, Ruff (subset amplo de regras), pre-commit hooks e pytest com testes unitários
das camadas e do entry point. `test_enum_lists_match_data_dictionary` valida no CI as
constantes de DQ contra os dicionários oficiais do TLC.

**Dependências mínimas em produção**
O wheel de produção carrega apenas `requests>=2.33.1`. Dependências de dev (mypy, ruff,
databricks-connect) não entram no artefato implantado, reduzindo superfície de supply chain.

## Riscos Residuais

**DQ inline sem quarentena (silver)** — Registros inválidos são descartados silenciosamente.
Não há rastreabilidade retroativa do motivo de rejeição por linha. Pré-condição para
produção: materializar quarentena com DQX (previsto no `PipelineConfig` e em ADR-015).

**Leitura não-incremental da silver** — Cada execução varre toda a bronze do taxi via MERGE.
Funciona pelo MERGE idempotente, mas o custo é O(bronze) a cada run. Mitigação futura:
CDF da bronze ou watermark por `_ingestion_ts`.

**Snapshot mutável na gold** — A view gold reflete o estado corrente da silver; re-execução
da silver altera resultados retroativamente. Aceitável no escopo atual (analytics ad-hoc,
leitor único). Inadequado para auditoria financeira ou SLA de frescor.

**Idempotência heurística na landing** — `file_already_ingested` verifica existência e
tamanho mínimo, não checksum. Um arquivo corrompido com tamanho correto não é detectado.

**Enums de DQ podem ficar defasados** — Se o TLC publicar novo `VendorID` ou `payment_type`,
as constantes de `silver/main.py` ficam desatualizadas silenciosamente até alguém notar
aumento de rejeições. Mitigação parcial: CI (`test_enum_lists_match_data_dictionary`).

**`wheel_file` sem valor padrão útil** — O bundle define `wheel_file: CHANGE_ME_WHEEL_FILE.whl`
como default. Deploy sem override explícito falha em runtime ao tentar carregar a wheel.

**Checkpoint/schemaLocation sem backup** — Os volumes de checkpoint e schema estão no mesmo
catálogo que os dados. Perda do catálogo/volume exigiria recriar bronze do zero com risco de
duplicação se a tabela Delta não for deletada antes.

**Permissões configuradas manualmente** — Não há automação de IAM/permissões no bundle.
Configuração incorreta de permissões no workspace é risco operacional residual sem mitigação
automatizada atual.

**Ausência de gate formal para `stg`** — O target `stg` existe no bundle mas não há
definição de critérios de promoção `dev → stg → prd` nem automação de smoke tests pós-deploy.
