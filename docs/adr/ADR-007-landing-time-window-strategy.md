# ADR-007: Time window strategy e parametrização da Landing

- Status: Accepted
- Date: 2026-05-14

## Context

A camada Landing tem três decisões inter-relacionadas sobre **como tratar a passagem do tempo** no pipeline:

1. **Onde mora a janela alvo (ano/meses a ingerir)** — no `PipelineConfig` (estável entre execuções) ou na CLI da execução (parâmetro variável)?
2. **Quem decide o que ingerir** — o caller passa a lista explícita, ou a landing descobre o que falta?
3. **Com que frequência o job roda** — diário, semanal, mensal?

O pipeline está sendo entregue como **case técnico avaliado**, com pedido explícito de ingerir Jan–Mai/2023. Mas a rubrica de avaliação e o bom senso de engenharia pedem que o pipeline **permita evolução natural** para outros meses e anos sem refator de código.

Versões iniciais tinham `target_year: int = 2023` e `target_months = (1,2,3,4,5)` como defaults no `PipelineConfig`. Isso resolve o caso pedido mas esconde dois problemas:

- O config "mente" sobre a natureza do pipeline — lendo o arquivo parece que ele "só sabe fazer 2023".
- Avançar para qualquer outra janela exige editar o config, um arquivo que deveria ser estável entre execuções.

A fonte (NYC TLC) publica mensalmente, com ~30-45 dias de atraso após o fim do mês. Não há contrato formal de SLA — atrasos esporádicos acontecem.

## Decision

Três decisões agrupadas, sustentadas por um princípio comum: **o pipeline não carrega hardcode temporal; a janela de ingestão é parametrizada via bundle e o sistema oferece dois operation modes coerentes com diferentes casos de uso.**

### Decisão 1 — Separar configuração de parâmetros de execução

`target_year` e `target_months` não pertencem ao `PipelineConfig`. Passam a ser argumentos obrigatórios da CLI da landing (em um dos dois modes descritos na Decisão 2) e, no schedule do job, vêm das variables `target_year` e `target_months` do bundle, com defaults do case (`"2023"` e `"1,2,3,4,5"`).

O `PipelineConfig` mantém apenas o que é **estável entre execuções**: catalog, schemas, URL template, thresholds de DQ, tags. Adiciona `tlc_publication_lag_months` (default 2) para modelar explicitamente o atraso esperado da fonte.

**Por que essa escolha?**
Porque "configuração" e "parâmetro de execução" são coisas diferentes que estavam misturadas. Catalog é configuração — não muda entre execuções. Janela alvo é parâmetro — muda quando a janela alvo muda. Tratá-los como a mesma coisa força edição de código (ou pelo menos do arquivo de config) para algo que deveria ser argumento de runtime. Separá-los torna o config honesto sobre o que ele é, e habilita override via variables do bundle: mudar de 2023 para 2024 é uma flag, não uma edição de código.

### Decisão 2 — Dois operation modes na Landing

A landing aceita dois conjuntos de args, mutuamente exclusivos:

- **Explicit mode**: `--target-year=YYYY --target-months=M1,M2,...`. O caller diz exatamente o que baixar. **É o mode usado pelo schedule do job**, com valores vindos das variables `target_year` e `target_months` do bundle.

- **Discovery mode**: `--discover --discover-from=YYYY-MM`. A landing compara o conteúdo do volume com a janela `[discover_from, hoje − tlc_publication_lag_months]` e baixa o que falta. **Disponível para execução manual** (via `databricks bundle run` ou `databricks jobs run-now` com override de parâmetros), tipicamente para backfill histórico ou recuperação após perda de volume.

Sem nenhum dos dois conjuntos, falha cedo com mensagem clara.

**Por que essa escolha?**
Porque os dois modes atendem casos de uso genuinamente diferentes:

- **Explicit mode** dá reprodutibilidade. O avaliador roda `make dab-deploy ENV=dev` e o pipeline entrega exatamente o que o case pediu, independente da data corrente. Janelas determinísticas tornam o pipeline testável (mesma entrada → mesma saída) e tornam debug previsível.

- **Discovery mode** dá auto-manutenção. Volume novo aparece, discovery percebe e baixa. Útil quando o pipeline migrar para produção contínua, e útil agora para casos como backfill ("preciso popular do zero um catálogo novo desde 2023-01") ou recuperação ("perdi o volume, preciso re-baixar tudo").

Defaultar a discovery no schedule introduziria comportamento dependente de data corrente — execução em Maio/2026 baixaria janela diferente de execução em Setembro/2026. Para case avaliado, isso prejudica reprodutibilidade. Manter explícito como default do schedule entrega previsibilidade hoje sem perder a opção de migrar para discovery quando o cenário mudar (mudança de YAML, não de código).

### Decisão 3 — Schedule mensal, dia 02

O job é orquestrado com schedule Quartz cron mensal: `0 0 5 2 * ?` em timezone `America/Sao_Paulo` (dia 02 de cada mês, 05:00). Não é diário nem semanal.

**Por que essa escolha?**
Porque a frequência de execução deve refletir a frequência da fonte. TLC publica mensalmente, então rodar diário/semanal seria overhead: a maioria das execuções não encontraria nada novo. Dia 02 (e não dia 01) dá margem para o TLC publicar o mês anterior — historicamente, publicações chegam até a primeira semana do mês seguinte. Combinado com `tlc_publication_lag_months=2`, evita-se tentativa de baixar arquivos ainda não publicados quando o pipeline migrar para discovery no schedule.

Com explicit mode como default do schedule, o schedule mensal não é estritamente necessário (re-executar a mesma janela mês após mês resulta em todos `skipped`). Mantemos por três motivos: (a) sinaliza intenção operacional (este é um pipeline mensal); (b) custa zero manter; (c) migrar para discovery no futuro torna-se trivial — basta trocar os args do `python_wheel_task`, schedule já está configurado.

## Consequences

### Positivas

- **`PipelineConfig` para de "mentir"**: lendo o arquivo, fica claro que o pipeline é genérico, não específico de 2023.
- **Avaliação reprodutível**: `make dab-deploy ENV=dev && make dab-run ENV=dev` entrega exatamente os 5 meses do case, independente da data de execução do avaliador.
- **Override sem código**: passar `--var="target_year=2024" --var="target_months=1,2,3"` no deploy muda a janela do schedule sem editar arquivos versionados.
- **Discovery preservado para evolução**: implementação completa e testável, pronta para virar default do schedule quando o pipeline migrar para produção contínua. Migração custa ~5 linhas de YAML.
- **Lag de publicação modelado explicitamente**: `tlc_publication_lag_months` no config substitui "conhecimento tribal" sobre o ciclo do TLC por valor versionado e configurável.
- **Falha cedo em args inválidos**: `_validate_args` rejeita combinações inconsistentes antes de qualquer chamada Databricks; erros de uso ficam visíveis em segundos, não após sessão Spark.

### Negativas (trade-offs)

- **Re-execução do schedule é no-op semântico**: sem mudar as variables do bundle entre execuções, cada execução mensal re-ingere os mesmos meses como `skipped`. Não é dano, mas é ruído nos logs. Resolvido quando migrarmos para discovery no schedule.
- **Dois modes = duas árvores de teste**: testes precisam cobrir explícito e discovery separadamente. Custo marginal, mas existe.
- **Discovery depende de listagem confiável do volume** (quando usado): se `dbutils.fs.ls` mentir (cache, permissão, eventual consistência), discovery pode pular um mês. Mitigação: re-execução manual é segura porque é idempotente.
- **Hardcode `tlc_publication_lag_months=2`**: o lag real varia (alguns meses chegam em 25 dias, outros em 50). Default conservador pode atrasar a ingestão em ~1 mês de um arquivo que já estaria disponível. Aceitável: pior caso é "espera mais um ciclo"; quem precisa de menor latência usa explicit mode.
- **Renomeação de target no bundle (`prod` → `prd`)** quebra deploy de quem já tinha `prod` ativo. Efeito colateral da Decisão 1 (consistência com `PipelineConfig.environment`). Migração documentada no README.

## Alternatives

### Rejeitada: Manter `target_year`/`target_months` no `PipelineConfig` como defaults

Status anterior ao trabalho desta ADR.

**Por que não a alternativa óbvia?**
Manter por inércia é tentador (não quebra nada), mas perpetua o problema. Os defaults `2023` + `(1,2,3,4,5)` parecem "exemplos sensatos" mas funcionam como **âncora silenciosa**: o pipeline carrega para sempre o footprint do case original. Em 2027, quando alguém abrir o repo, ainda vai estar lá. Tirar agora, com poucos arquivos consumindo isso, é trivial; tirar daqui a 2 anos é refactor de risco. Custo zero hoje, custo crescente amanhã — exatamente o tipo de débito que vale pagar cedo.

### Rejeitada: Discovery mode como default do schedule

Discovery rodando no schedule mensal, sem necessidade de definir janela alvo no bundle.

**Por que não essa alternativa?**
Discovery é o mode certo para produção contínua, onde auto-manutenção é mais valiosa que previsibilidade. Mas o pipeline atual é entregue como case avaliado: o avaliador roda o pipeline uma ou duas vezes, e precisa receber a mesma entrega independente do dia da execução. Discovery introduz comportamento dependente de data corrente — execução em Junho baixaria diferente da execução em Outubro. Para case avaliado, **reprodutibilidade vence auto-manutenção**. Discovery permanece implementado para casos manuais e como caminho de migração quando o pipeline virar produção real.

### Rejeitada: Modo único (apenas explícito, ou apenas discovery)

Implementar só um dos modes para simplificar.

**Por que não essa alternativa?**
Cada mode cobre casos que o outro não cobre bem. Sem explícito, perdemos: (a) reprodutibilidade para case e debug; (b) backfill direcionado ("re-ingere só Março/2024 que veio corrompido"). Sem discovery, perdemos: (a) caminho natural de migração para produção contínua; (b) recuperação eficiente após perda de volume (sem discovery, alguém precisa listar mês a mês o que faltava). Os dois modes custam pouco código adicional (a validação mutuamente-exclusiva resolve a complexidade) e habilitam casos suficientemente diferentes para coexistir.

### Rejeitada: Modos diferentes por ambiente (dev/stg explícito, prd discovery)

`databricks.yml` com override por target, fazendo dev/stg rodarem explícito e prd rodar discovery automaticamente.

**Por que não essa alternativa?**
Faz sentido conceitual — dev/stg querem reprodutibilidade, prd quer auto-manutenção — mas é prematuro. O pipeline ainda não tem prd ativo nem CI/CD elaborado em stg. Introduzir override por target agora custa ~10 linhas de YAML e cognitive load adicional ("o pipeline se comporta diferente dependendo do ambiente") sem ganho funcional atual. Listado em "Quando revisitar": quando o pipeline migrar para produção contínua, **esta passa a ser a forma natural de resolver** — não é "alternativa rejeitada para sempre", é "alternativa postergada com gatilho claro".

### Rejeitada: Schedule diário (com discovery)

Roda todo dia; na maioria dos dias, discovery não encontra nada novo.

**Por que não essa alternativa?**
Não há ganho funcional sobre mensal — TLC não publica diariamente. Execuções diárias seriam ~28 no-ops por mês para 1-2 execuções úteis. Em ambiente serverless, cada no-op incorre em tempo de bootstrap de cluster e ciclo de Workflow. Aumenta custo sem reduzir latência da ingestão. Vale para fontes de alta frequência (APIs com publicações intra-dia); não vale para TLC.

### Rejeitada: Schedule semanal

Meio termo entre diário e mensal.

**Por que não essa alternativa?**
Mesma análise do diário, atenuada. Reduz no-ops de 28 para 3 por mês, mas ainda há ganho zero — o mês novo do TLC sai uma vez por mês, não uma vez por semana. Semanal só faria sentido se houvesse expectativa de catch-up de meses atrasados chegando esporadicamente fora do ciclo principal, o que não é o comportamento observado do TLC.

### Rejeitada: Schedule dia 01 em vez de dia 02

**Por que não essa alternativa?**
TLC publica o mês N até o início do mês N+1. Rodar dia 01 às vezes pega o arquivo, às vezes não — fica dependente do horário exato de publicação do TLC, que não é contratual. Dia 02 (+ lag de 2 meses) tem margem confortável para todos os casos observados historicamente.

### Outras consideradas

- **`--lookback-months=N`** (janela rolante): "baixar últimos N meses sempre". Faz sentido quando o pipeline virar diário/semanal com janela móvel. Postergado — em pipeline mensal com fonte mensal, discovery já cobre o caso melhor (não re-baixa o que já está lá). Listado como evolução futura em `bronze.md` e `landing.md`.
- **Discovery com teto explícito (`--discover-max-months`)**: mecanismo de segurança contra erro humano em `--discover-from=2009-01` que faria o job tentar baixar 15 anos. Risco operacional baixo na prática (volume zerado é detectável antes do incidente). Postergado.
- **Trigger por evento na bronze (file notification mode do Auto Loader)**: aplicável só na bronze, e já coberto pelo ADR-009.

## Validation

Critérios de validação contínua:

- Execução com `--target-year=2023 --target-months=1,2,3,4,5` em volume vazio deve baixar os 5 meses.
- Execução subsequente sem mudança no volume deve resultar em todos `skipped`, retorno `0`.
- Override via bundle var (`--var="target_year=2024" --var="target_months=1,2,3"`) deve trocar a janela do schedule sem alteração de código.
- Discovery mode (`--discover --discover-from=YYYY-MM`) deve continuar funcional via execução manual com override de `python_wheel_task.parameters`.
- Execução com combinação inválida (ex: `--discover --target-year=2023`) deve falhar imediatamente com `ValueError`, antes de qualquer chamada Databricks.
- Schedule mensal no dia 02 deve ser visível como ativo no Databricks Workflows quando deployado em target `mode: production` (prd/stg); pausado em target `mode: development` (dev).
- `tlc_publication_lag_months` deve poder ser ajustado via override do `PipelineConfig` sem mudança de código.

**Quando essa decisão deve ser revisitada?**

- **Quando o pipeline migrar de "case entregue" para "produção contínua"**: trocar default do schedule para discovery. Custo: editar 2 linhas no `nyc_taxi_job.yml` (trocar `--target-year`/`--target-months` por `--discover`/`--discover-from`). Sem refator de código Python.
- **Quando aparecer necessidade real de modes diferentes por ambiente**: introduzir override por target no `databricks.yml` (dev/stg explícito, prd discovery).
- **Quando a fonte mudar de TLC para outra com cadência diferente**: API com publicação intra-dia troca todas as três decisões — schedule mais frequente, lag diferente, possivelmente discovery por cursor em vez de path.
- **Quando o pipeline expandir para múltiplos datasets** (Green Taxi, FHV, Citi Bike): considerar um schedule por dataset, ou um schedule único com fan-out via task matrix.
- **Quando aparecerem casos reais de erro operacional por falta de `--discover-max-months`**: adicionar o teto.
- **Quando o TLC mudar o ciclo de publicação** (improvável mas possível): revisar `tlc_publication_lag_months` e o `quartz_cron_expression`.
- **Quando o caso de uso de "janela rolante" surgir** (pipeline diário com janela móvel de N meses): considerar `--lookback-months` como terceiro mode.