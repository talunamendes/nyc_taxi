# ADR-013: Data model da Silver — uma tabela por tipo de taxi

- Status: Accepted
- Date: 2026-05-17

## Context

A camada Silver consolida os datasets do TLC (yellow e green) com schema explícito, limpeza e dedupe. Surge a decisão de **como modelar fisicamente** essa camada quando o pipeline passa a ingerir mais de um tipo de taxi.

Características relevantes das fontes:

- **Yellow** e **green** são datasets separados publicados pelo TLC, com cadência e regras independentes.
- Os schemas se **sobrepõem em parte** (`VendorID`, `passenger_count`, `total_amount`, `payment_type`, `trip_distance`, `fare_amount`, `tip_amount`, `PULocationID`, `DOLocationID`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`) e **divergem em parte**:
  - Yellow só tem `airport_fee`.
  - Green só tem `trip_type` (street-hail vs dispatch) e `ehail_fee`.
  - As colunas de timestamp se chamam `tpep_pickup_datetime`/`tpep_dropoff_datetime` em yellow e `lpep_pickup_datetime`/`lpep_dropoff_datetime` em green — mesma semântica, nomes distintos publicados pelo TLC.
- A geografia regulatória difere: yellow opera predominantemente em Manhattan/CBD e em aeroportos; green opera nos boroughs externos e tem restrição legal sobre street-hails na zona core de Manhattan.
- Políticas públicas afetam os dois tipos de forma **assimétrica** (ex.: a CBD Congestion Relief Zone, vigente desde jan/2025, incide quase totalmente em yellow).

A questão é: a Silver deve materializar **uma tabela unificada** com `taxi_type` como coluna, ou **uma tabela por taxi**?

## Decision

A Silver materializa **uma tabela Delta por tipo de taxi**:

- `silver.yellow_taxi_trips`
- `silver.green_taxi_trips`

Cada tabela **preserva o schema nativo** da sua fonte (yellow mantém `tpep_*`; green mantém `lpep_*`). Não há renomeação cross-taxi nem coluna `taxi_type` — a identidade do dataset já está no nome da tabela.

Não materializamos view unificada. Quando uma análise cross-taxi for necessária, o consumidor (gold ou ad-hoc) expressa o `UNION ALL` localmente, aliasando explicitamente os timestamps. Materializar a view sem caso de uso real seria especulação prematura.

**Por que essa escolha?**

Porque uma única tabela só ganha sentido quando o contrato semântico das fontes converge totalmente, e aqui ele **não converge**: yellow e green têm colunas exclusivas (`airport_fee`, `trip_type`) que carregam sinal analítico real. Unificar fisicamente forçaria uma de duas perdas:

1. **Descartar colunas exclusivas** ao reduzir ao subset interseccional — perdendo justamente os atributos que mais discriminam fraude (`airport_fee` em yellow, `trip_type` em green), gorjeta (yellow tem padrão distinto por `RatecodeID=JFK`), e impacto de política pública (CBD Congestion afeta yellow desproporcionalmente).
2. **Aceitar schema poluído com NULLs estruturais** — `airport_fee NULL` para toda corrida green e `trip_type NULL` para toda corrida yellow viraria padrão permanente, dificultando data quality (NULL deixa de sinalizar "ausência" e passa a sinalizar "não se aplica"), inflando armazenamento e fazendo o consumidor sempre filtrar por `taxi_type` antes de qualquer agregação.

Separação física resolve isso ao **manter o schema de cada fonte fiel ao TLC**, com lineage de coluna direto. Cenários de feature engineering downstream que dependem desses sinais (previsão de demanda por zona, detecção de fraude, modelagem de gorjeta, ETA, análise causal de políticas) ficam viabilizados sem reprocessar a bronze. Os custos da separação são pequenos:

- Duas tabelas Delta em vez de uma — gerenciamento marginalmente maior, mas o pipeline já loopa por `taxi_type` desde a bronze (ADR-009), então a complexidade de código é simétrica.
- Queries cross-taxi precisam de `UNION ALL` explícito — mais verbose, mas torna a operação **observável** (o consumidor escolhe quais colunas alinhar e como tratar as exclusivas) em vez de esconder a heterogeneidade.

## Consequences

### Positivas

- **Preservação de sinais analíticos**: `airport_fee`, `trip_type`, `ehail_fee` ficam disponíveis nas suas tabelas naturais sem schema bloat. Modelos futuros (fraude, gorjeta, ETA, impacto de política) podem usar essas colunas sem rework.
- **Lineage limpo**: o nome de coluna no silver bate com o nome publicado pelo TLC. Auditoria de "essa coluna veio de onde?" é trivial.
- **Schema evolution isolado**: se o TLC adicionar coluna nova em yellow, só `yellow_taxi_trips` muda — green não fica com NULL fantasma. Bronze já tem `mergeSchema=true` (ADR-010), e essa propriedade se propaga naturalmente para silver-por-taxi.
- **Falha isolada**: refresh de yellow pode falhar (schema evolution, falha de cluster) e green continua publicado. O loop principal trata cada taxi em try/except, mesma política do bronze.
- **Clustering otimizado por dataset**: `CLUSTER BY (pickup_date)` em cada tabela é o suficiente — `taxi_type` é implícito na tabela e não consome posição de cluster key. Se o padrão de query da gold variar entre taxis (yellow mais filtrado por aeroporto, green mais por zona), cada tabela pode ter chaves de cluster diferentes no futuro (`ALTER TABLE ... CLUSTER BY` é metadata-only — ver ADR-012).
- **Coerência com a bronze**: a bronze já é por-taxi (ADR-009: `yellow_taxi`, `green_taxi`). Silver-por-taxi mantém o paralelismo de modelagem, evitando estranha "convergência depois divergência" no fluxo.

### Negativas (trade-offs)

- **Queries cross-taxi exigem `UNION ALL` explícito**: análises que somam yellow + green precisam aliasing manual. O custo cognitivo recai sobre o consumidor da silver, não sobre o produtor. Mitigação: se um padrão recorrente emergir, materializar uma view ou tabela gold dedicada com schema reduzido (apenas colunas comuns + `taxi_type`).
- **Duas tabelas para governar**: tags, ACLs e checks de qualidade precisam ser definidos duas vezes. Mitigação: pipeline já aplica `ALTER TABLE ... SET TAGS` em loop por taxi; configuração é parametrizada via `_PICKUP_COLS`/`silver_table_pattern` e qualquer regra futura entra no mesmo loop.
- **Risco de drift de regras de DQ entre tabelas**: cleaning inline (`dropoff > pickup`, `total_amount >= 0`, etc.) precisa ser igual nas duas. Hoje a função `build_silver_dataframe` recebe `taxi_type` e aplica a mesma regra com colunas nativas — drift seria PR conspícuo. Se a complexidade crescer, vale extrair as regras para um módulo compartilhado.
- **Nomes de timestamp não-uniformes** (`tpep_*` vs `lpep_*`): consumidores ad-hoc precisam saber qual usar. Compensação: o nome do TLC é a fonte de verdade — renomear esconderia essa heterogeneidade real do dataset e geraria atrito quando alguém comparar com a documentação oficial do TLC.

## Alternatives

### Rejeitada: tabela única `silver.taxi_trips` com coluna `taxi_type`

Schema unificado, `lpep_*` aliasado em `tpep_*`, `taxi_type` como cluster key junto com `pickup_date`. Foi a implementação anterior (refeita por este ADR).

**Por que não a alternativa óbvia?**

Tem três problemas que pioram com o tempo:

1. **Schema interseccional**: para a tabela única ter um schema coerente, ou descarta colunas exclusivas (perde sinal) ou aceita NULLs estruturais permanentes (`airport_fee NULL` em todas as linhas green). NULL deixa de ser "missing data" e passa a ser "not applicable" — um sinal que precisa de outra coluna (`taxi_type`) para ser interpretado. Isso é exatamente o sintoma de schema mal modelado.
2. **Bottleneck de schema evolution**: yellow ganhando coluna nova mexe num esquema que green também usa. Bronze hoje suporta isso (ADR-010), mas o ônus na silver é amplificado porque silver é a camada de contrato — toda mudança vira coordenação cross-taxi.
3. **Renomeação `lpep_*` -> `tpep_*` perde lineage**: o nome do TLC desaparece. Quando um analista comparar a silver com a documentação oficial, vai precisar conhecer a convenção interna do pipeline. Para um data product que pode ser consumido por equipes externas, isso é fricção pura.

A alternativa unificada faria sentido **se yellow e green tivessem o mesmo schema TLC**. Eles não têm.

### Rejeitada: tabela única + view materializada por taxi

Manter `silver.taxi_trips` unificada e oferecer `silver.yellow_taxi_trips` / `silver.green_taxi_trips` como views filtradas. O leitor escolhe.

**Por que não essa alternativa?**

Resolve o sintoma (queries por taxi ficam ergonômicas), mas mantém o problema raiz (schema interseccional, NULLs estruturais, perda de lineage `lpep_*`). Vista do consumidor é uma camada de açúcar; vista da produção é um schema mal modelado. View também não resolve cluster keys: a base ainda precisa `CLUSTER BY (taxi_type, ...)` para skipping eficiente, então a divergência de padrão de query entre yellow e green continua acoplada.

### Rejeitada: separadas + view unificada (`silver.taxi_trips_unified`)

Manter as duas físicas e expor uma view com `UNION ALL` aliasando `lpep_*` para `tpep_*` e adicionando `taxi_type` literal.

**Por que não essa alternativa agora?**

É uma evolução plausível, **mas materializá-la sem caso de uso comprovado é especulação**. Hoje as análises do case são por taxi; as duas análises da gold (média mensal e média por hora em maio) podem rodar contra `yellow_taxi_trips` diretamente. Quando emergir uma análise cross-taxi recorrente, criar essa view é um `CREATE VIEW` barato e reversível — fazer agora seria over-engineering. O ADR explicitamente delega isso à gold/feature store quando a demanda existir.

### Outras consideradas

- **Tabela única partitioned by `taxi_type`**: variação do "schema único" — partition `taxi_type` em vez de cluster key. Não resolve schema interseccional; introduz skew de partição (yellow tem ~3× o volume de green); e ADR-012 já argumenta contra partitioning Hive em silver. Pior em todos os eixos.
- **Schema "wide" com todas as colunas de yellow + green em uma tabela**: explícito sobre NULLs estruturais como decisão. Maior pegada de armazenamento, queries lendo coluna que não se aplica, e ainda exige `taxi_type` filter para qualquer agregação significativa. Pior do que o cenário interseccional.

## Validation

Critérios de validação contínua:

- **Schema fidelity**: as colunas em `silver.yellow_taxi_trips` e `silver.green_taxi_trips` devem corresponder 1:1 com os nomes publicados pelo TLC para cada tipo. Se a silver ficar com colunas que não existem na fonte (ou vice-versa) sem ADR de override, é desvio.
- **Sem NULL estrutural**: nenhuma coluna deveria ser sempre-NULL em uma das tabelas. Métrica: `SELECT COUNT(*) FILTER (WHERE col IS NOT NULL)` deve ser > 0 para toda coluna declarada.
- **Independência operacional**: refresh de uma tabela não deve ter dependência de leitura da outra. O pipeline garante isso processando cada taxi em try/except isolado.
- **Tags UC consistentes**: cada tabela carrega `taxi_type` como tag para que governance/ACL possa filtrar por taxi sem inspecionar nome.

**Quando essa decisão deve ser revisitada?**

- Quando uma análise cross-taxi emergir como **padrão recorrente** (não pontual) — então faz sentido materializar uma view unificada ou uma tabela gold dedicada com schema reduzido. A decisão de ter feito separação física na silver continua válida, só ganha uma camada por cima.
- Quando o TLC convergir os schemas (improvável, mas vale monitorar release notes do TLC). Nesse caso, unificar passaria a fazer sentido naturalmente.
- Quando o pipeline adicionar um terceiro tipo de táxi (FHV/HVFHS) — neste momento, vale revisitar se a estratégia "uma tabela por taxi" escala bem para N tipos ou se uma abstração diferente é necessária. Hoje a parametrização `silver_table_pattern` + loop por taxi suporta N>2 sem código novo, mas a complexidade de governance pode justificar repensar a partir de 4+ taxis.
- Quando uma feature store for introduzida (cenário 8 da análise de feature engineering): a feature store consome diretamente as tabelas separadas, então a decisão atual ajuda; pode ser oportunidade de promover algumas agregações cross-taxi para a feature store em vez da silver.
