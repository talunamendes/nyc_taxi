# ADR-010: Permissive schema evolution na Bronze

- Status: Accepted
- Date: 2026-05-14

## Context

O schema dos Parquets publicados pelo NYC TLC **não é estável ao longo do tempo**. Casos conhecidos:

- `cbd_congestion_fee` adicionada em Jan/2025.
- `airport_fee` introduzida em 2021.
- `payment_type` mudou de tipo (INT → BIGINT) em ~2022.
- Esporadicamente, colunas mudam de ordem entre publicações.

O case atual usa apenas dados de 2023, mas o pipeline precisa lidar com a possibilidade real de TLC publicar uma coluna nova no futuro (ex: extensão para 2024, ou ingestão automática de novos meses). Precisamos decidir como a bronze reage quando o Parquet de origem tem schema diferente do esperado.

Há um espectro de políticas possíveis:

1. **Strict**: schema fixo na DDL, qualquer divergência quebra o pipeline (force manual intervention).
2. **Permissive with column addition**: colunas novas são aceitas, colunas faltantes ficam NULL, mudança de tipo quebra.
3. **Rescue**: colunas inéditas capturadas em coluna especial `_rescued_data`, schema da tabela inalterado.
4. **Fully permissive**: aceita tudo, inclusive mudança de tipo via cast implícito.

## Decision

A bronze adota **a permissive policy with automatic column addition** (opção 2), implementada via:

- `cloudFiles.schemaEvolutionMode = "addNewColumns"` no Auto Loader.
- `.option("mergeSchema", "true")` no writeStream.
- `max_retries >= 1` configurado no job do Workflow.

Mudanças de tipo (item 4) **continuam quebrando**, intencionalmente. Apenas adição de coluna evolui automaticamente.

**Por que essa escolha?**
Porque o objetivo da bronze é ser **fiel à fonte com o mínimo de fricção operacional**. Se o TLC adiciona `cbd_congestion_fee`, queremos que essa coluna apareça na bronze automaticamente, sem aguardar um humano editar DDL — o dado já está perdido nessa janela se exigirmos intervenção manual. Por outro lado, mudança de tipo (ex: `total_amount` virar STRING) **deve quebrar**: tipo diferente significa que algo conceitualmente mudou e processamento downstream pode estar corrompendo dados silenciosamente. A combinação dá o "permitir evolução" do case sem virar terra-de-ninguém.

## Consequences

### Positivas

- Pipeline sobrevive a adições de coluna no Parquet de origem sem intervenção manual — só requer retry automático do job (que já é boa prática em produção).
- Compatibilidade com histórico: registros antigos ficam NULL na coluna nova, comportamento Delta padrão e bem compreendido downstream.
- Sem necessidade de coordenar release de código com data de publicação do TLC.
- Auditoria preservada: `_ingestion_ts` registra quando a coluna nova foi incorporada à bronze (via comparação `MIN(_ingestion_ts) WHERE col IS NOT NULL`).
- Mudança de tipo ainda quebra → erro visível, não silencioso. Garante que mudanças de **semântica** passem por revisão humana.

### Negativas (trade-offs)

- **Comportamento contraintuitivo**: na primeira execução com schema novo, o stream **falha** com `UnknownFieldException`. É design intencional do Auto Loader para forçar restart com schema atualizado, mas pega quem não conhece. Requer `max_retries >= 1` no job, senão a "evolução automática" não se materializa.
- Coluna nova adicionada **silenciosamente** — sem alerta. Se um Parquet bug do TLC trouxer coluna fantasma (typo, ex: `total_ammount`), ela vira coluna na bronze e só descobrimos via review periódico do schema.
- `mergeSchema=true` força reescrita do log Delta a cada evolução; em alta frequência de mudanças, pode crescer mais que o esperado (irrelevante no volume atual).
- Se um Parquet vier sem coluna esperada (ex: `airport_fee` ausente), os registros são gravados com NULL sem alerta — perda silenciosa de dado que talvez devesse ser detectada.

## Alternatives

### Rejeitada: schema strict, falhar em qualquer divergência

DDL explícita, sem `mergeSchema`, sem evolution mode. Qualquer Parquet com schema diferente do declarado faz o stream morrer com erro.

**Por que não a alternativa óbvia?**
Strict é a política certa quando: (a) o produtor do dado tem contrato formal e estável; (b) há equipe disponível para responder a falhas dentro de SLA curto; (c) o custo de processar dado errado é maior que o custo de pular dado certo. Nenhuma das três se aplica aqui — TLC publica sem contrato formal, projeto é solo, e bronze append-only é fácil de reprocessar se descobrirmos divergência depois. Strict apenas trocaria evolução automática por backlog operacional.

### Rejeitada: `schemaEvolutionMode = "rescue"`

Auto Loader detecta colunas novas e as captura na coluna especial `_rescued_data` (JSON), sem mexer no schema da tabela. O dado fica **disponível mas isolado** — não vira coluna de primeira classe.

**Por que não essa alternativa?** Foi seriamente considerada e tem mérito: dá visibilidade explícita ("olha, apareceu coluna nova, mas eu não evolui") sem perder dado. Foi rejeitada por dois motivos:

1. **Consumo downstream complicado**: silver/gold precisariam parsear `_rescued_data` para usar o campo novo. Quebra o "fluxo natural" de tipagem.
2. **Requer ação humana para promover**: o ganho de "ver antes de aceitar" só vale se houver processo formal de review. Como solo developer no MVP, não há esse processo — `rescue` viraria caixa preta esquecida.

Para um projeto com governança formal de schema e equipe de plantão, `rescue` seria a escolha superior. Voltar a considerá-la quando esse cenário aparecer.

### Rejeitada: fully permissive (cast implícito)

Aceitar inclusive mudança de tipo, com cast automático onde possível (Delta suporta com `mergeSchema` + `overwriteSchema`).

**Por que não essa alternativa?** Mudança de tipo é tipicamente sintoma de mudança de **semântica**, não de schema. Aceitar silenciosamente mascara bugs do produtor e pode corromper análises downstream. Trade-off: ganhamos sobrevivência a mais tipos de mudança ao custo de perder o sinal "algo importante mudou na fonte". Decidimos manter esse sinal — strict for type changes, permissive for column addition.

### Outras consideradas

- **`failOnNewColumns` (default antigo do Auto Loader)**: mesmo comportamento que strict, mas configurável a posteriori. Equivalente prático a "rejeitada strict".
- **Validação custom via expectations**: rodar regras de schema antes de gravar (Great Expectations, Soda). Adiciona dependência e custo de manutenção que não se justifica para MVP.

## Validation

Critérios de validação contínua:

- Simular adição de coluna: criar Parquet manual com coluna fictícia, depositar na landing, rodar job. Primeira execução deve falhar com `UnknownFieldException` registrado no log; segunda execução (via retry automático) deve concluir com coluna nova presente na bronze.
- Simular mudança de tipo: criar Parquet com `total_amount` como STRING. Job **deve falhar persistentemente** — não deve ser auto-corrigido.
- Inspecionar schema da bronze periodicamente (mensal): comparar com `_BRONZE_SCHEMA_DDL` declarada no código. Divergência é sinal de evolução acontecida — verificar se foi intencional ou se TLC publicou typo.
- Logs do job devem distinguir claramente entre "primeira tentativa falhou por schema novo" (esperado) e "retry também falhou" (problema real).

**Quando essa decisão deve ser revisitada?**

- Quando o projeto adotar governança formal de schema (contrato com SLA do produtor, processo de aprovação de mudanças) — `rescue` mode passa a fazer mais sentido que `addNewColumns`.
- Quando o cost de processar dado errado aumentar substancialmente — por exemplo, se a bronze passar a alimentar modelos de pricing em produção, strict pode voltar a ser preferível com aviso manual.
- Quando ferramentas de observabilidade de schema (data contracts, OpenLineage com schema validation) estiverem maduras — possibilita o melhor dos dois mundos: permissive + alerts.
- Quando descobrirmos casos reais de typo do TLC sendo aceitos silenciosamente — sinal de que precisamos de validação intermediária.