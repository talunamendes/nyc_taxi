# ADR-012: Estratégia de organização física por camada (Liquid Clustering a partir da Silver)

- Status: Accepted
- Date: 2026-05-14

## Context

Cada camada do Lakehouse (Bronze, Silver, Gold) tem padrão de escrita e leitura próprio, e a estratégia de organização física dos arquivos Parquet deve refletir esse padrão. Decisões aplicáveis incluem:

- **Sem organização** (deixar Delta gerenciar com auto-compaction).
- **Partitioning Hive-style** por colunas categóricas de baixa cardinalidade.
- **Liquid Clustering** (`CLUSTER BY`) — recurso do Delta Lake 3.0+, suporta múltiplas colunas e evolui sem rewrite.
- **Z-Order** (legado do Delta para multi-coluna skipping).

Esta ADR define **onde aplicar clustering ou não** ao longo do pipeline. Versão anterior desta ADR defendia Liquid Clustering na bronze — decisão revisitada após análise do padrão de leitura real da camada.

Padrões observados por camada no escopo atual:

| Camada | Padrão de escrita | Padrão de leitura | Volume típico |
|--------|-------------------|--------------------|---------------|
| Bronze | Append-only, ~1 arquivo Parquet/mês | Full-scan da janela nova pelo job da silver | ~3M linhas/mês |
| Silver | Overwrite ou merge da janela transformada | Ad-hoc, exploração, validações | Equivalente à bronze após dedupe |
| Gold | Overwrite de agregados, frequência menor | BI, dashboards, queries previsíveis com filtros estáveis | Reduzido (agregado) |

## Decision

A organização física da tabela varia por camada:

- **Bronze**: **sem clustering nem partitioning**. Apenas `optimizeWrite` + `autoCompact` para resolver small files.
- **Silver**: **Liquid Clustering** por colunas de filtro analítico típico (a definir com base nas queries reais — candidatos prováveis: `DATE(tpep_pickup_datetime)`, `PULocationID`).
- **Gold**: **Liquid Clustering** por dimensões dos dashboards/relatórios consumidores (definir junto com modelagem dimensional, ADR futura).

**Por que essa escolha?**
Porque organização física só paga dividendos quando há padrão de **leitura seletiva**: filtros que se beneficiam de file skipping. Na bronze, ninguém faz leitura seletiva — o consumidor real é o job da silver, que faz full scan da janela recente para transformar. Clustering ali seria reescrita de arquivos que serão lidos uma única vez sequencialmente: custo de `OPTIMIZE` sem ganho de query. A silver e a gold, por outro lado, são consultadas por exploração e dashboards com filtros multi-dimensionais — exatamente onde Liquid Clustering brilha (multi-coluna sem skew, evolução sem rewrite).

## Consequences

### Positivas

- **Bronze mais barata de manter**: sem `OPTIMIZE` reorganizando arquivos que serão lidos sequencialmente uma vez. Auto-compaction continua resolvendo small files com custo proporcional ao real benefício.
- **Silver/Gold otimizadas para seu uso real**: clustering multi-coluna acelera filtros analíticos que são o padrão de consumo dessas camadas.
- **Sem partition skew** em silver/gold — Liquid evita o problema clássico de Hive partitioning com cardinalidade desbalanceada.
- **Evolução barata**: padrão de query da silver/gold vai amadurecer com uso real; `ALTER TABLE ... CLUSTER BY` em Liquid é metadata-only, permite revisão sem rewrite.
- **Coerência com princípio "MVP simples, permitindo evolução"**: bronze fica com configuração mínima viável; sofisticação entra onde gera valor de fato.

### Negativas (trade-offs)

- **Configuração não-uniforme entre camadas**: requer documentação clara para quem entra no projeto. Cada camada tem TBLPROPERTIES e clustering próprios.
- **Silver/Gold dependem de reader v2+** (requisito do Liquid Clustering). Bronze não tem esse requisito *vindo do clustering*, mas mantém via `columnMapping.mode = 'name'` (ADR-010), então o piso de versão é o mesmo na prática.
- **Risco de subdimensionar bronze**: se um dia surgir caso de leitura ad-hoc frequente em bronze (ex: análise forense de qualidade), pode ser necessário adicionar clustering. Custo de adicionar depois: `ALTER TABLE ... CLUSTER BY (...)` + um `OPTIMIZE` — barato e reversível.
- **Definição de chaves de clustering na silver/gold fica em aberto**: depende do padrão de query real, que só emergirá após uso. Risco mitigado pela natureza metadata-only do `ALTER`.

## Alternatives

### Rejeitada: Liquid Clustering em todas as camadas, inclusive bronze

Versão anterior desta ADR. Argumentava multi-coluna sem skew e evolução barata como justificativa universal.

**Por que não a alternativa óbvia?**
Os argumentos a favor de Liquid Clustering são genuínos, mas **respondem "por que Liquid em vez de partitioning"**, não **"por que organização física aqui especificamente"**. Para a bronze, a pergunta certa é a segunda — e a resposta é "não há leitor que se beneficie". Clustering em bronze é otimização sem caso de uso: custo de `OPTIMIZE` sem ganho de query. Decisão revisitada honestamente após reflexão sobre o padrão real de consumo.

### Rejeitada: classic partitioning (`PARTITIONED BY`) em silver/gold

Estratégia tradicional, bem entendida, sem dependência de Delta reader v2+.

**Por que não essa alternativa?**

1. **Granularidade fixa**: partições mensais são ótimas para queries de mês inteiro, ruins para filtros finos em `pickup_datetime`. Liquid resolve isso clusterizando em múltiplas dimensões.
2. **Evolução cara**: se a chave de clustering precisar mudar com base no padrão de query observado, partitioning exige rewrite. Liquid é metadata-only.
3. **Small files** em silver/gold com volume modesto: partições por mês geram fragmentação onde Liquid consolidaria.

Vale notar que para datasets **muito grandes e estáveis** com padrão de query previsível desde o início, partitioning ainda pode ser superior — pruning mais determinístico. Não é o caso aqui.

### Rejeitada: Z-Order em silver/gold

Técnica anterior do Delta para multi-coluna skipping.

**Por que não essa alternativa?** Z-Order requer `OPTIMIZE ZORDER BY ...` periódico explícito (custo operacional contínuo); Liquid é mantido pelo autoCompact. Z-Order também entra em maintenance mode no roadmap do Delta — novas otimizações vão para Liquid. Para tabela nova, Liquid é estritamente melhor.

### Outras consideradas

- **Partitioning por `_source_year`/`_source_month` na bronze**: descartado pelo mesmo motivo do clustering — bronze não é lida seletivamente.
- **Clustering em bronze por `tpep_pickup_datetime` apenas** (chave única, hipótese mais barata): ainda não se justifica — quem lê bronze faz full scan, não filtro por intervalo de timestamp.
- **Partitioned generated columns** (`PARTITIONED BY (DATE(tpep_pickup_datetime))`): granularidade diária com custo de skew. Liquid em silver/gold cobre melhor o mesmo caso.

## Validation

Critérios de validação contínua:

- **Bronze**: tamanho médio de arquivo após `autoCompact` deve ficar entre 100–500 MB. Métrica `numFiles` em `DESCRIBE DETAIL` deve crescer linearmente com meses ingeridos, sem fragmentação anômala.
- **Silver/Gold**: após implementação, queries com filtros típicos devem ler proporção reduzida de arquivos (validar via `EXPLAIN FORMATTED` e métrica `numFiles read`). Razão filtrado/total < 50% indica clustering efetivo; razão próxima de 100% indica que as chaves não cobrem o padrão real.
- **Sinal de subdimensionamento da bronze**: se aparecerem queries ad-hoc recorrentes em bronze com tempo de execução crescente, é gatilho para reavaliar.

**Quando essa decisão deve ser revisitada?**

- Quando o padrão de leitura da bronze mudar — por exemplo, se análises de qualidade ou auditoria passarem a ser executadas regularmente direto na bronze.
- Quando o padrão de query da silver/gold ficar estável o suficiente para justificar revisão das chaves de clustering escolhidas inicialmente.
- Quando ferramentas externas (Trino, Athena, Snowflake Iceberg-compat) precisarem ler silver/gold: reader version 2+ pode ser limitante e forçar revisitar Liquid vs. partitioning.
- Quando uma versão futura do Delta tornar alguma propriedade incompatível com Liquid (improvável, mas vale monitorar release notes).