# TCO Model

## Objetivo

Estimar o custo total de operação da solução no Databricks, considerando compute, armazenamento, I/O e custo operacional do time.

## Fórmula de Cálculo

`TCO_mensal = C_compute + C_storage + C_io + C_operacao`

Com:

- `C_compute = tempo_total_execucao_horas x custo_hora_compute`
- `C_storage = GB_medio_armazenado x custo_por_GB_mes`
- `C_io = (GB_lidos + GB_gravados) x custo_por_GB_io`
- `C_operacao = horas_engenharia_mes x custo_hora_engenharia`

## Premissas e Variáveis

- **Compute**
  - Duração média por execução (horas)
  - Quantidade de execuções por mês
  - Custo/hora do tipo de compute utilizado
- **Storage**
  - Volume médio armazenado (GB) em `landing`, `bronze`, `silver`, `gold`
  - Política de retenção por camada
- **I/O**
  - GB processados mensalmente (leitura + escrita)
  - Custo por GB movimentado (ou custo por request, quando aplicável)
- **Operação**
  - Horas mensais do time (runbook, monitoramento, incidentes, melhoria)
  - Custo/hora da equipe

## Exemplo Preenchido (valores fictícios)

Premissas hipotéticas para ilustrar o cálculo:

- `tempo_total_execucao_horas = 25 h/mês`
- `custo_hora_compute = US$ 0.40/h`
- `GB_medio_armazenado = 300 GB`
- `custo_por_GB_mes = US$ 0.023`
- `GB_lidos + GB_gravados = 500 GB/mês`
- `custo_por_GB_io = US$ 0.01`
- `horas_engenharia_mes = 8 h/mês`
- `custo_hora_engenharia = US$ 35/h`

Cálculo:

- `C_compute = 25 x 0.40 = US$ 10.00`
- `C_storage = 300 x 0.023 = US$ 6.90`
- `C_io = 500 x 0.01 = US$ 5.00`
- `C_operacao = 8 x 35 = US$ 280.00`

`TCO_mensal = 10.00 + 6.90 + 5.00 + 280.00 = US$ 301.90`

## Cenários de Referência (fictícios)

- **Otimista**: menor reprocessamento e menor esforço operacional.
- **Base**: operação estável com incidentes pontuais.
- **Pessimista**: maior volume, mais retries e maior esforço de suporte.

Uma forma simples de evoluir:

- Definir premissas por cenário para cada componente (`compute`, `storage`, `i/o`, `operação`).
- Recalcular o `TCO_mensal` para cada cenário e comparar sensibilidade.

## Estratégias de Otimização

- Evitar reprocessamento completo (idempotência por partição).
- Ajustar frequência de execução ao SLA real de negócio.
- Limitar retenção na landing e privilegiar camadas curadas.
- Automatizar deploy/rollback para reduzir custo operacional.

## Disclaimer

Este documento, nesta etapa do projeto, tem objetivo **orientativo**: chamar atenção para a necessidade do levantamento de custo total de propriedade e estruturar a forma de cálculo.

Os valores apresentados são **fictícios** e servem apenas como exemplo didático.  
Para decisões reais de arquitetura e investimento, o TCO deve ser recalculado com dados observados de produção, preços vigentes do provedor e premissas validadas com áreas de engenharia, finanças e operação.
