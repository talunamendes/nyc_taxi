# ADR-015: Governança e Qualidade de Dados — Validação Hardcoded como MVP

- Status: Accepted
- Date: 2026-05-18

## Context

A camada Silver é o ponto onde o pipeline aplica curadoria, contratos semânticos
e qualidade de dados antes que o dado chegue ao consumidor. Três decisões de
governança precisam ser tomadas explicitamente:

1. **Como as regras de DQ são expressas e executadas** — inline no código, em
   framework dedicado, ou externalizadas em arquivo de contrato?
2. **Como o contrato de schema entre camadas é formalizado** — via DDL Delta,
   via ferramenta de data contract, ou implicitamente?
3. **Como registros inválidos são tratados** — drop silencioso, quarentena
   observável, ou falha explícita do pipeline?

No momento em que essa decisão foi tomada, foram avaliadas duas ferramentas do
ecossistema moderno de governança de dados para Databricks/lakehouse:

**Databricks Labs DQX** é uma biblioteca open-source que permite definir regras
de qualidade de dados de forma declarativa (Python fluente ou YAML) e executá-las
como parte de um pipeline Spark, produzindo métricas estruturadas e separando
registros válidos de inválidos com rastreabilidade da regra que causou a rejeição.

**datacontract CLI** é uma ferramenta de linha de comando que trata o contrato
de dados como código: o schema, os SLAs e as regras de qualidade ficam em um
arquivo `datacontract.yaml` versionado no repositório, e a CLI valida a aderência
dos dados publicados ao contrato definido. Integra com plataformas como Databricks,
Great Expectations, dbt e outras via plugins.

Ambas as ferramentas endereçam problemas reais que surgem em produção:

- Regras de DQ espalhadas em código Python sem rastreabilidade de por que um
  registro foi rejeitado.
- Ausência de contrato explícito e testável entre produtor e consumidor de cada
  camada.
- Dificuldade de auditar drift de schema sem ferramenta dedicada.

No entanto, o contexto do projeto é determinante: trata-se de um **MVP** com
fonte externa única (TLC NYC), esquema estável (dicionários de dados oficiais
publicados em PDF), conjunto fixo de consumidores (gold view + análises ad-hoc)
e sem requisito de auditoria formal. A diretriz explícita do repositório é
**evitar over-engineering**.

## Decision

O pipeline adota **validação de DQ hardcoded inline na Silver**, sem framework
externo de DQ nem ferramenta de data contract nesta fase.

As regras vivem no módulo `src/nyc_taxi/lakehouse/silver/main.py` como constantes
Python ancoradas nos data dictionaries oficiais do TLC:

```python
_VALID_VENDOR_IDS: tuple[int, ...] = (1, 2, 6, 7)
_VALID_RATECODE_IDS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 99)
_VALID_PAYMENT_TYPES: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6)
_VALID_STORE_FWD_FLAGS: tuple[str, ...] = ("Y", "N")
_VALID_TRIP_TYPES: tuple[int, ...] = (1, 2)

_MAX_TRIP_DISTANCE_MILES: int = 200
_MAX_PASSENGER_COUNT: int = 9
_MIN_LOCATION_ID: int = 1
_MAX_LOCATION_ID: int = 265
```

A função `_validation_expression(taxi_type, fn)` monta a expressão booleana
Spark que combina todas as regras em um único `where()` inline. Registros
inválidos são descartados silenciosamente — sem tabela de quarentena nesta fase.

O contrato de schema é formalizado via DDL Delta explícito (`CREATE TABLE IF NOT EXISTS`
com lista de colunas e tipos em `_SILVER_COLUMN_TYPES`) e via Unity Catalog tags
(`layer`, `domain`, `taxi_type`, `criticality`, `pii`). Não há arquivo
`datacontract.yaml` externo.

Os thresholds de volume e taxa de rejeição vivem como atributos do `PipelineConfig`:

```python
dq_min_rows_per_month: int = 1_500_000
dq_max_rows_per_month: int = 5_000_000
dq_max_null_pct_critical: float = 0.001
dq_max_rejection_rate: float = 0.10
```

**Por que essa escolha para o MVP?**

Porque a complexidade de DQX e datacontract CLI não tem contrapartida de valor
no escopo atual:

- **Fonte estável**: o schema do TLC não mudou entre jan–mai/2023. As regras de
  DQ derivam diretamente dos PDFs oficiais — não há negociação contratual entre
  equipes nem drift esperado no curto prazo.
- **Pipeline de camada única por domínio**: DQX e datacontract CLI pagam dividendo
  quando há múltiplos produtores, múltiplos consumidores ou contratos entre
  squads. Aqui há um produtor (TLC), uma pipeline e um consumidor (analítica
  ad-hoc).
- **Ausência de quarentena operacional**: sem SLA de observabilidade de rejeições
  por parte de consumidores, materializar uma tabela de quarentena e publicar
  métricas estruturadas seria artefato sem leitor.
- **Custo de adoção não-trivial**: DQX exige dependência adicional no wheel e
  runtime Databricks; datacontract CLI exige manutenção de arquivo de contrato
  sincronizado com o DDL Delta — em um MVP, o custo de manutenção supera o
  benefício.
- **Testabilidade suficiente**: `_validation_expression` recebe `fn` como argumento
  explícito, permitindo testes unitários sem SparkSession. O CI valida as
  constantes de enums contra os dicionários do TLC via `test_enum_lists_match_data_dictionary`.

## Consequences

### Positivas

- **Zero dependências adicionais**: o wheel de produção não carrega DQX nem
  plugins de datacontract. Menos superfície de atualização e compatibilidade
  com o runtime Databricks.
- **Regras totalmente rastreáveis no código**: cada constante tem comentário
  referenciando a seção do data dictionary do TLC. Um PR que altera uma regra
  é automaticamente revisável e auditável via git.
- **Implementação mínima e legível**: toda a lógica de DQ cabe em ~120 linhas
  de Python; onboarding é imediato.
- **Schema como código já implementado**: DDL explícito com tipos em
  `_SILVER_COLUMN_TYPES` e Unity Catalog tags estabelecem um contrato de
  consumo funcional sem ferramentas externas.

### Negativas (trade-offs aceitos para o MVP)

- **Regras inauditáveis em operação**: registros rejeitados são descartados
  sem rastreabilidade do motivo. Em produção, isso impossibilita auditoria
  retroativa de "por que essa corrida sumiu".
- **Ausência de métricas de DQ estruturadas**: não há contagem de rejeitados
  por regra, por taxi type ou por período. Observabilidade depende de
  `DESCRIBE HISTORY` do Delta ou de `count()` antes/depois — ambos custosos.
- **Contrato de schema implícito entre camadas**: o DDL declara o schema da
  silver, mas não há validação automática de que a bronze entrega o que a
  silver espera. Drift silencioso só é detectado quando o pipeline quebra.
- **Manutenção manual de enums**: se o TLC publicar novo `VendorID` ou
  `payment_type`, as constantes ficam defasadas silenciosamente. Mitigação
  atual: teste no CI (`test_enum_lists_match_data_dictionary`), mas exige
  atenção humana no PR de novos dados.
- **Regras não reutilizáveis por outras camadas ou domínios**: a lógica de
  validação é específica do módulo silver. Se outro pipeline consumir os
  mesmos dados do TLC, precisará duplicar as regras.

## Alternatives

### Avaliada e rejeitada para o MVP: Databricks Labs DQX

DQX permitiria declarar as mesmas regras de forma fluente:

```python
from databricks.labs.dqx.col_functions import is_not_null, is_in_list

checks = [
    DQRowRule(criticality="error", name="vendor_id_valid",
              check=is_in_list("VendorID", [1, 2, 6, 7])),
    DQRowRule(criticality="error", name="pickup_not_null",
              check=is_not_null("tpep_pickup_datetime")),
]
```

E separar automaticamente `valid_df` de `invalid_df`, gravando a quarentena
com a coluna `_errors` descrevendo qual regra falhou.

**Por que não agora?** Adiciona dependência de runtime, exige configuração de
quarentena observável por algum consumidor e introduce abstração onde não há
ganho real no escopo MVP. O valor concreto (rastreabilidade da rejeição, métricas
por regra) só se materializa quando há SLA de observabilidade ou múltiplos
consumidores monitorando a qualidade.

### Avaliada e rejeitada para o MVP: datacontract CLI

O datacontract CLI permitiria formalizar o contrato da silver como:

```yaml
# datacontract.yaml
dataContractSpecification: 1.1.0
id: nyc-taxi-silver-yellow
info:
  title: NYC Yellow Taxi Silver
  version: 1.0.0
models:
  yellow_taxi_trips:
    fields:
      VendorID: { type: bigint, required: true }
      tpep_pickup_datetime: { type: timestamp_ntz, required: true }
      total_amount: { type: double, required: true, minimum: 0 }
```

E validar a aderência do dado publicado ao contrato via `datacontract test`.

**Por que não agora?** O contrato hoje é simples (um domínio, um produtor, um
consumidor) e já está implicitamente codificado no DDL Delta + `_SILVER_COLUMN_TYPES`.
Manter um `datacontract.yaml` sincronizado manualmente com o DDL seria overhead
sem leitor. O valor real do datacontract CLI emerge quando o contrato é negociado
entre squads diferentes — o que não é o caso no MVP.

## Quando essa decisão deve ser revisitada

Esta decisão deve ser evoluída **antes de qualquer promoção para produção**. Os
gatilhos específicos são:

**1. Quarentena e rastreabilidade de rejeições**

Quando houver consumidor (squad de dados, analista ou SLA) que precise entender
por que registros específicos foram rejeitados, a abordagem de drop silencioso
torna-se inadequada. O próximo passo é materializar a quarentena usando
`silver_quarantine_table_fqn_for` (já previsto no `PipelineConfig`) e adotar
DQX para separar `valid_df` de `invalid_df` com a coluna `_errors` rastreando
a regra responsável pela rejeição.

**2. Múltiplos consumidores ou múltiplos domínios**

Quando mais de uma squad ou pipeline consumir os dados do TLC (ex.: times de
pricing, fraude ou supply), o risco de drift de definição das regras torna-se
real. Nesse ponto, formalizar o contrato via datacontract CLI separa a
responsabilidade: o produtor publica um `datacontract.yaml` versionado, e o
consumidor valida contra ele no CI/CD — sem depender de inspeção manual do DDL.

**3. Observabilidade de DQ em produção**

Quando o pipeline rodar em produção com SLA de frescor, será necessário
monitorar métricas de DQ por execução (taxa de rejeição, distribuição de erros
por regra, variação entre períodos). DQX produz essas métricas como saída
estruturada naturalmente; a abordagem atual exigiria instrumentação manual
custosa.

**4. Drift de schema entre fonte e contrato**

Quando o TLC ou outro produtor puder alterar o schema sem aviso, validações
automáticas de schema drift passam a ser necessárias. O datacontract CLI oferece
integração com o Unity Catalog para validar que o schema publicado corresponde
ao contrato definido — eliminando a dependência de atenção humana no PR.

## Caminho de evolução recomendado para produção

A evolução deve ser incremental, sem redesign de uma vez:

**Passo 1 — Materializar quarentena com DQX (impacto na Silver)**

Substituir o `where(_validation_expression(...))` atual por DQX:

```python
from databricks.labs.dqx.engine import DQEngine

engine = DQEngine(spark)
valid_df, quarantine_df = engine.apply_checks_by_metadata_and_split(bronze_df, checks)

valid_df  # segue para o MERGE na silver
quarantine_df.write.format("delta").mode("append") \
    .saveAsTable(cfg.silver_quarantine_table_fqn_for(taxi_type))
```

As regras que hoje vivem em `_validation_expression` são mapeadas para
`DQRowRule` declarations — sem alterar a lógica, apenas a forma de expressão.

**Passo 2 — Externalizar o contrato de schema com datacontract CLI**

Criar um `datacontract.yaml` por camada de consumo (silver e gold), referenciando
o Unity Catalog como fonte de verdade do schema atual. Adicionar `datacontract test`
ao CI para detectar drift antes do merge de qualquer PR que toque DDL ou regras DQ.

**Passo 3 — Publicar métricas de DQ como tabela observável**

Gravar as métricas de DQ produzidas pelo DQEngine (rejeitados por regra, por taxi,
por período) em uma tabela `observability.dq_metrics` no Unity Catalog. Isso
elimina a necessidade de `DESCRIBE HISTORY` manual e permite alertas por threshold
(`dq_max_rejection_rate` já está no `PipelineConfig`).

Esses três passos podem ser adotados independentemente e em qualquer ordem,
sem invalidar as decisões anteriores. Cada um vira um ADR próprio quando
for implementado.

## Validation

Critérios de validação contínua para o MVP atual:

- `test_enum_lists_match_data_dictionary` passa no CI sem intervenção manual.
- `dq_max_rejection_rate` (10%) não é excedido em nenhum run de silver sem
  alerta explícito no log.
- Unity Catalog tags (`layer`, `domain`, `pii`, `criticality`) presentes em
  todas as tabelas silver via `DESCRIBE EXTENDED`.
- Schema físico da silver (`_SILVER_COLUMN_TYPES`) casa com o DDL declarado
  em `ensure_yellow_silver_table` / `ensure_green_silver_table`.

**Quando essa decisão deve ser revisitada?**

- Quando houver requisito de auditoria retroativa de rejeições (SLA ou compliance).
- Quando um segundo domínio ou squad consumir os dados do TLC.
- Quando o pipeline for promovido a `prd` com SLA formal de frescor e qualidade.
- Quando o TLC mudar o data dictionary e a detecção manual (via CI) for considerada
  insuficiente.
