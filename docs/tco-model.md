# TCO Model (Resumo)

## Objetivo

Estimar custo total de operação da solução no Databricks, considerando compute, armazenamento e operação contínua.

## Componentes de Custo

- **Compute**: execução de jobs serverless (ingestão + transformações).
- **Storage**: dados em volumes/tabelas por camada (landing, bronze, silver, gold).
- **I/O e requests**: leituras/escritas e movimentação entre camadas.
- **Operação**: tempo de engenharia para manutenção, observabilidade e suporte.

## Drivers Principais

- Janela de processamento (jan-mai/2023 inicialmente).
- Frequência de execução e reprocessamentos.
- Volume mensal de dados e retenção histórica.
- Eficiência das transformações e tamanho dos arquivos.

## Modelo Simplificado

`TCO mensal ~= C_compute + C_storage + C_io + C_operacao`

Onde:

- `C_compute`: custo por tempo de execução das tasks.
- `C_storage`: custo por GB armazenado e retenção.
- `C_io`: custo de acessos/movimentação.
- `C_operacao`: horas do time para run, ajuste e incidentes.

## Estratégias de Otimização

- Evitar reprocessamento completo (idempotência por partição).
- Ajustar frequência de execução ao SLA real de negócio.
- Limitar retenção na landing e privilegiar camadas curadas.
- Automatizar deploy/rollback para reduzir custo operacional.
